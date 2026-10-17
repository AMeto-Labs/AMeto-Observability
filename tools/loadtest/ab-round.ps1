#Requires -Version 7
<#
.SYNOPSIS
  One round of an A/B load comparison: a fresh data directory, one server, one k6 run at a fixed
  offered rate, the server's own counters, a query per signal, and (traces) the compaction run.

.DESCRIPTION
  Every timer in the server starts with the process, and the trace compaction worker runs once,
  60 s after start, then hourly. A round whose load started before that first run had it inside
  the window (a third of the window's spans rewritten), one that started after did not, and the
  difference was larger than anything A and B differ by. So the load starts at a FIXED offset,
  -StartDelaySeconds after /health answers (70 s: after that first run, on an empty store), and
  every round of either side sees every timer at the same phase.

  Before the load the whole machine has to stay quiet (every 1-s sample of total CPU under
  -IdleMaxPercent, host RAM 2 points under -MaxHostMemoryPercent) for -IdleSeconds in a row,
  inside that delay. If it does not, the server is stopped and the round starts again on a fresh
  data directory (up to -MaxStartAttempts).

  While k6 runs, the script samples every second: total CPU (GetSystemTimes), the server's CPU,
  working set and commit (GetProcessTimes / GetProcessMemoryInfo), and every other process's
  CPU from one SystemProcessInformation snapshot, split into ours (server, k6, dotnet-counters,
  this script), the OS kernel processes the load itself drives, and "foreign" — everybody else.
  A 5-s mean of foreign plus unattributed CPU (a short-lived process is in no snapshot) above
  -ForeignMaxPercent, or host RAM reaching -MaxHostMemoryPercent, marks the round "overlapped".

  Allocations and collections are recorded twice over the same window (k6 start to the end of
  the settle period): from dotnet-counters (System.Runtime) and from the deltas of
  GET /api/diagnostics (managedTotalAllocated, gen0/1/2Collections).

  Traces only (unless -NoCompactionRun): after the queries the server is stopped and started
  again on the same data, and the compaction run it makes 60 s after start — merging the small
  segments the load left — is measured on its own: CPU, allocations, collections, peak memory.

  The data directory is deleted at the end (unless -KeepData); the server logs, k6 output, the
  samples and result.json stay in the round folder.

.EXAMPLE
  ./ab-round.ps1 -ServerDir C:\tmp\treeB\src\Ameto.Server\bin\Release\net10.0 -Label B `
                 -Signal traces -OutRoot C:\tmp\rounds -Rate 50 -Duration 120s
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $ServerDir,
    [Parameter(Mandatory)] [string] $Label,
    [Parameter(Mandatory)] [ValidateSet('traces', 'metrics')] [string] $Signal,
    [Parameter(Mandatory)] [string] $OutRoot,
    [int]    $Rate = 50,
    [int]    $Batch = 0,                 # traces only: spans per request; 0 = the script's 1000
    [string] $Duration = '120s',
    [int]    $Port = 18341,
    [string] $K6 = 'C:\Program Files\k6\k6.exe',
    [string] $ScriptDir = $PSScriptRoot,
    [string] $DotnetCounters = "$env:USERPROFILE\.dotnet\tools\dotnet-counters.exe",
    [int]    $StartDelaySeconds = 70,    # load starts this long after /health; see DESCRIPTION
    [double] $IdleMaxPercent = 15,       # every 1-s sample of total CPU must stay under this...
    [int]    $IdleSeconds = 30,          # ...for this many seconds in a row, inside the start delay
    [int]    $MaxStartAttempts = 6,
    [int]    $SettleSeconds = 15,        # after k6 exits, before the window closes
    [int]    $RamTargetPercent = 99,     # the host sits above the 75 % default; see TRACES-METRICS-RESULTS.md
    [double] $ForeignMaxPercent = 15,    # a 5-s mean of foreign + unattributed CPU above this: overlapped
    [double] $MaxHostMemoryPercent = 90, # host RAM in use: the gate wants 2 points under it, and a round that
                                         # reaches it is overlapped — past ~90 % the .NET GC switches to its
                                         # high-memory-load mode and collects differently, for reasons not ours
    [hashtable] $ExtraEnv = @{},
    [int]    $QueryRepeats = 5,
    [switch] $NoCompactionRun,
    [int]    $CompactionTimeoutSeconds = 900,
    [switch] $KeepData
)

$ErrorActionPreference = 'Stop'
[System.Threading.Thread]::CurrentThread.CurrentCulture   = [cultureinfo]::InvariantCulture
[System.Threading.Thread]::CurrentThread.CurrentUICulture = [cultureinfo]::InvariantCulture

if (-not ('AbRoundV3.Native' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
namespace AbRoundV3 {
public static class Native {
    [DllImport("kernel32.dll")] static extern bool GetSystemTimes(out long idle, out long kernel, out long user);
    [DllImport("kernel32.dll")] static extern bool GetProcessTimes(IntPtr h, out long create, out long exit, out long kernel, out long user);
    [StructLayout(LayoutKind.Sequential)]
    struct PMC {
        public uint cb, PageFaultCount;
        public UIntPtr PeakWorkingSetSize, WorkingSetSize, QuotaPeakPagedPoolUsage, QuotaPagedPoolUsage,
                       QuotaPeakNonPagedPoolUsage, QuotaNonPagedPoolUsage, PagefileUsage, PeakPagefileUsage, PrivateUsage;
    }
    [DllImport("psapi.dll")] static extern bool GetProcessMemoryInfo(IntPtr h, out PMC c, uint cb);

    /// idle, kernel (includes idle), user — 100-ns units, all CPUs summed.
    public static long[] SystemTimes() { long i, k, u; GetSystemTimes(out i, out k, out u); return new[] { i, k, u }; }

    /// kernel + user CPU of the process, in seconds.
    public static double CpuSeconds(IntPtr h) {
        long c, e, k, u;
        return GetProcessTimes(h, out c, out e, out k, out u) ? (k + u) / 1e7 : double.NaN;
    }

    /// working set, peak working set, commit (private bytes), peak commit — bytes.
    public static long[] Memory(IntPtr h) {
        PMC c; c = default(PMC); c.cb = (uint)Marshal.SizeOf(typeof(PMC));
        if (!GetProcessMemoryInfo(h, out c, c.cb)) return new long[] { -1, -1, -1, -1 };
        return new[] { (long)c.WorkingSetSize, (long)c.PeakWorkingSetSize, (long)c.PrivateUsage, (long)c.PeakPagefileUsage };
    }

    [StructLayout(LayoutKind.Sequential)]
    struct MSX {
        public uint dwLength, dwMemoryLoad;
        public ulong ullTotalPhys, ullAvailPhys, ullTotalPageFile, ullAvailPageFile, ullTotalVirtual, ullAvailVirtual, ullAvailExtendedVirtual;
    }
    [DllImport("kernel32.dll")] static extern bool GlobalMemoryStatusEx(ref MSX m);

    /// Physical memory in use on the whole host, in percent.
    public static double HostMemoryPercent() {
        var m = new MSX(); m.dwLength = (uint)Marshal.SizeOf(typeof(MSX));
        if (!GlobalMemoryStatusEx(ref m) || m.ullTotalPhys == 0) return double.NaN;
        return 100.0 * (m.ullTotalPhys - m.ullAvailPhys) / m.ullTotalPhys;
    }

    [DllImport("ntdll.dll")] static extern int NtQuerySystemInformation(int cls, IntPtr buf, int len, out int ret);

    /// Every process's kernel + user time (100-ns units) by pid, with its image name — one
    /// SystemProcessInformation snapshot, so it needs no handle and sees protected processes
    /// (System, Memory Compression, Defender) that GetProcessTimes cannot open without admin.
    public static System.Collections.Generic.Dictionary<int, System.Collections.Generic.KeyValuePair<string, long>> ProcessCpu() {
        int len = 1 << 20;
        while (true) {
            IntPtr buf = Marshal.AllocHGlobal(len);
            try {
                int ret;
                int st = NtQuerySystemInformation(5, buf, len, out ret);
                if (st == unchecked((int)0xC0000004)) { len = Math.Max(len * 2, ret + 65536); continue; }
                if (st != 0) throw new Exception("NtQuerySystemInformation 0x" + st.ToString("X"));
                var d = new System.Collections.Generic.Dictionary<int, System.Collections.Generic.KeyValuePair<string, long>>();
                long off = 0;
                while (true) {
                    IntPtr p = new IntPtr(buf.ToInt64() + off);
                    int next = Marshal.ReadInt32(p, 0);
                    long user = Marshal.ReadInt64(p, 40), kern = Marshal.ReadInt64(p, 48);
                    int nameLen = Marshal.ReadInt16(p, 56) & 0xFFFF;
                    IntPtr nameBuf = Marshal.ReadIntPtr(p, 64);
                    string name = nameBuf == IntPtr.Zero ? "Idle" : Marshal.PtrToStringUni(nameBuf, nameLen / 2);
                    int pid = (int)Marshal.ReadIntPtr(p, 80).ToInt64();
                    d[pid] = new System.Collections.Generic.KeyValuePair<string, long>(name, user + kern);
                    if (next == 0) break;
                    off += next;
                }
                return d;
            } finally { Marshal.FreeHGlobal(buf); }
        }
    }

    public sealed class CpuSplit {
        public double Server, K6, Counters, Harness, Kernel, Foreign, TopForeignPct;
        public string TopForeign = "";
        public System.Collections.Generic.Dictionary<string, double> ForeignByName =
            new System.Collections.Generic.Dictionary<string, double>();
    }

    /// The machine's CPU between two snapshots, in percent of all logical CPUs, split into ours,
    /// the OS kernel processes, and everybody else. A process absent from the first snapshot
    /// started inside the interval, so all of its time belongs to it.
    public static CpuSplit Split(
        System.Collections.Generic.Dictionary<int, System.Collections.Generic.KeyValuePair<string, long>> prev,
        System.Collections.Generic.Dictionary<int, System.Collections.Generic.KeyValuePair<string, long>> cur,
        double seconds, int nCpu, int server, int k6, int counters, int harness, string[] kernelNames) {
        var s = new CpuSplit();
        double norm = 100.0 / (seconds * 1e7 * nCpu);
        foreach (var kv in cur) {
            int pid = kv.Key;
            if (pid == 0) continue;
            long before = 0;
            System.Collections.Generic.KeyValuePair<string, long> p;
            if (prev.TryGetValue(pid, out p) && p.Key == kv.Value.Key) before = p.Value;
            double d = (kv.Value.Value - before) * norm;
            if (d <= 0) continue;
            if (pid == server) s.Server += d;
            else if (pid == k6) s.K6 += d;
            else if (pid == counters) s.Counters += d;
            else if (pid == harness) s.Harness += d;
            else if (Array.IndexOf(kernelNames, kv.Value.Key) >= 0) s.Kernel += d;
            else {
                s.Foreign += d;
                double x; s.ForeignByName.TryGetValue(kv.Value.Key, out x); s.ForeignByName[kv.Value.Key] = x + d;
                if (d > s.TopForeignPct) { s.TopForeignPct = d; s.TopForeign = kv.Value.Key; }
            }
        }
        return s;
    }
}
}
'@
}

# OS processes whose CPU this load drives itself (loopback TCP, file cache, page compression, the
# on-access scan of the files the server writes). Everything that is neither these nor ours is
# "foreign": another session's build, a browser, the Docker VM.
$KernelNames = @('System', 'Registry', 'Memory Compression', 'Secure System', 'MsMpEng.exe', 'MpDefenderCoreService.exe', 'NisSrv.exe')

$nCpu = [Environment]::ProcessorCount

function Get-BusyPercent([long[]] $a, [long[]] $b) {
    $idle = $b[0] - $a[0]; $total = ($b[1] - $a[1]) + ($b[2] - $a[2])
    if ($total -le 0) { return 0.0 }
    return 100.0 * (1.0 - $idle / $total)
}

function Wait-Idle([datetime] $Deadline) {
    # Every 1-s sample under $IdleMaxPercent of CPU, with host RAM 2 points under
    # $MaxHostMemoryPercent, for $IdleSeconds in a row, before $Deadline.
    $window = [System.Collections.Generic.List[double]]::new()
    $mems = [System.Collections.Generic.List[double]]::new()
    $prev = [AbRoundV3.Native]::SystemTimes()
    $waited = 0
    while ((Get-Date).AddSeconds(1) -lt $Deadline) {
        Start-Sleep -Milliseconds 1000
        $now = [AbRoundV3.Native]::SystemTimes()
        $busy = Get-BusyPercent $prev $now
        $mem = [AbRoundV3.Native]::HostMemoryPercent()
        $prev = $now
        $waited++
        if ($busy -lt $IdleMaxPercent -and $mem -lt ($MaxHostMemoryPercent - 2)) { $window.Add($busy); $mems.Add($mem) }
        else { $window.Clear(); $mems.Clear() }
        if ($window.Count -ge $IdleSeconds) {
            return [ordered]@{
                ok = $true; waitedSeconds = $waited
                meanPercent = [math]::Round(($window | Measure-Object -Average).Average, 1)
                maxPercent  = [math]::Round(($window | Measure-Object -Maximum).Maximum, 1)
                hostMemoryPercent = [math]::Round(($mems | Measure-Object -Maximum).Maximum, 1)
            }
        }
    }
    return [ordered]@{ ok = $false; waitedSeconds = $waited; meanPercent = $null; maxPercent = $null
                       hostMemoryPercent = [math]::Round([AbRoundV3.Native]::HostMemoryPercent(), 1) }
}

function Start-AmetoServer([string] $name) {
    # Returns the process once /health answers; $script:healthAt is when it did.
    foreach ($k in $envSet.Keys) { [Environment]::SetEnvironmentVariable($k, $envSet[$k], 'Process') }
    try {
        $p = Start-Process -FilePath $exe -WorkingDirectory $ServerDir -PassThru -WindowStyle Hidden `
                -RedirectStandardOutput (Join-Path $roundDir "$name.stdout.log") `
                -RedirectStandardError  (Join-Path $roundDir "$name.stderr.log")
    } finally {
        foreach ($k in $envSet.Keys) { [Environment]::SetEnvironmentVariable($k, $null, 'Process') }
    }
    $null = $p.Handle   # held from here on, so the exit code and CPU times stay readable
    for ($i = 0; $i -lt 480; $i++) {
        Start-Sleep -Milliseconds 250
        if ($p.HasExited) { throw "Server exited during startup (code $($p.ExitCode))" }
        try {
            $r = Invoke-WebRequest -Uri "$base/health" -TimeoutSec 2 -SkipHttpErrorCheck
            if ($r.StatusCode -eq 200) { $script:healthAt = Get-Date; return $p }
        } catch { }
    }
    Stop-AmetoServer $p
    throw 'Server did not answer /health within 120 s'
}

function Stop-AmetoServer($p) {
    if ($p -and -not $p.HasExited) {
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        $p.WaitForExit(30000) | Out-Null
    }
}

function Get-Token {
    (Invoke-RestMethod -Method Post -Uri "$base/api/auth/login" -ContentType 'application/json' `
        -Body (@{ username = 'admin'; password = '123123' } | ConvertTo-Json)).token
}

function Invoke-Diag([string] $token, [string] $file) {
    $d = Invoke-RestMethod -Uri "$base/api/diagnostics" -Headers @{ Authorization = "Bearer $token" } -TimeoutSec 60
    $d | ConvertTo-Json -Depth 5 | Set-Content -Path $file -Encoding utf8
    return $d
}

function Get-CurlTime([string] $token, [string] $path, [string] $bodyFile, [string] $outFile) {
    $w = & curl.exe -s -o $outFile -w '%{http_code} %{time_total} %{size_download}' `
            -H "Authorization: Bearer $token" -H 'Content-Type: application/json' `
            --data-binary "@$bodyFile" "$base$path"
    $parts = $w.Trim() -split ' '
    return [ordered]@{
        status = [int]$parts[0]
        ms     = [math]::Round([double]::Parse($parts[1].Replace(',', '.'), [cultureinfo]::InvariantCulture) * 1000, 1)
        bytes  = [long]$parts[2]
    }
}

function Get-GcDelta($d0, $d1) {
    # GC.CollectionCount(n) counts every collection of generation n OR HIGHER; restated as one
    # count per collection, at the highest generation it collected (what dotnet-counters reports).
    $c0 = [long]$d1.gen0Collections - [long]$d0.gen0Collections
    $c1 = [long]$d1.gen1Collections - [long]$d0.gen1Collections
    $c2 = [long]$d1.gen2Collections - [long]$d0.gen2Collections
    return [ordered]@{ gen0 = $c0 - $c1; gen1 = $c1 - $c2; gen2 = $c2 }
}

function Read-Counters([string] $csv, [datetime] $from, [datetime] $to, [long] $items) {
    # dotnet-counters CSV: a "Rate" row is the increment over the second before its stamp, a
    # "Metric" row a gauge reading. Rows stamped inside (floor(from), floor(to)] are the window.
    if (-not (Test-Path $csv)) { return $null }
    $inv = [cultureinfo]::InvariantCulture
    $lo = $from.Date.AddSeconds([math]::Floor($from.TimeOfDay.TotalSeconds))
    $hi = $to.Date.AddSeconds([math]::Floor($to.TimeOfDay.TotalSeconds))
    $sum = @{}; $max = @{}; $heapAt = @{}; $rows = 0; $first = $null; $last = $null
    foreach ($r in Import-Csv $csv) {
        $t = [datetime]::ParseExact($r.Timestamp, 'MM/dd/yyyy HH:mm:ss', $inv)
        if ($t -le $lo -or $t -gt $hi) { continue }
        if ($null -eq $first) { $first = $t }; $last = $t
        $rows++
        $name = $r.'Counter Name'
        $v = [double]::Parse($r.'Mean/Increment', $inv)
        if ($r.'Counter Type' -eq 'Rate') { $sum[$name] = $sum[$name] + $v }
        else {
            if (-not $max.ContainsKey($name) -or $v -gt $max[$name]) { $max[$name] = $v }
            if ($name.StartsWith('dotnet.gc.last_collection.heap.size')) { $heapAt[$t] = $heapAt[$t] + $v }
        }
    }
    $g = { param($gen) [long]$sum["dotnet.gc.collections ({collection} / 1 sec)[gc.heap.generation=$gen]"] }
    $alloc = [long]$sum['dotnet.gc.heap.total_allocated (By / 1 sec)']
    $exceptions = [ordered]@{}
    foreach ($k in ($sum.Keys | Where-Object { $_ -like 'dotnet.exceptions*' } | Sort-Object)) {
        $exceptions[($k -replace '^.*error\.type=([^\]]+)\].*$', '$1')] = [long]$sum[$k]
    }
    return [ordered]@{
        rows = $rows; firstStamp = "$first"; lastStamp = "$last"
        coversWindow = ($null -ne $last -and $last -ge $hi.AddSeconds(-1))
        allocBytes = $alloc
        allocBytesPerItem = [math]::Round($alloc / [math]::Max(1, $items), 1)
        # Each collection counted once, at the highest generation it collected — unlike
        # GC.CollectionCount(n), which counts every collection of generation n or higher.
        gen0 = & $g 'gen0'; gen1 = & $g 'gen1'; gen2 = & $g 'gen2'
        gcPauseSeconds = [math]::Round([double]$sum['dotnet.gc.pause.time (s / 1 sec)'], 3)
        cpuUserSeconds   = [math]::Round([double]$sum['dotnet.process.cpu.time (s / 1 sec)[cpu.mode=user]'], 2)
        cpuSystemSeconds = [math]::Round([double]$sum['dotnet.process.cpu.time (s / 1 sec)[cpu.mode=system]'], 2)
        lockContentions = [long]$sum['dotnet.monitor.lock_contentions ({contention} / 1 sec)']
        threadPoolWorkItems = [long]$sum['dotnet.thread_pool.work_item.count ({work_item} / 1 sec)']
        jitMethods = [long]$sum['dotnet.jit.compiled_methods ({method} / 1 sec)']
        maxWorkingSetBytes = [long]$max['dotnet.process.memory.working_set (By)']
        maxGcCommittedBytes = [long]$max['dotnet.gc.last_collection.memory.committed_size (By)']
        maxGcHeapAfterCollectionBytes = if ($heapAt.Count) { [long](($heapAt.Values | Measure-Object -Maximum).Maximum) } else { 0 }
        exceptions = $exceptions
    }
}

# ── per-second sampler: total CPU, the server, and everybody else ───────────────
$script:samples = $null
function Reset-Sampler($list) {
    $script:samples = $list
    $script:foreignCpuSeconds = @{}
    $script:t0 = Get-Date; $script:lastT = $script:t0
    $script:lastSys = [AbRoundV3.Native]::SystemTimes(); $script:lastSnap = [AbRoundV3.Native]::ProcessCpu()
    $script:cpuBase = [AbRoundV3.Native]::CpuSeconds($script:srvHandle)
}
function Add-Sample([string] $phase) {
    $now  = [AbRoundV3.Native]::SystemTimes()
    $snap = [AbRoundV3.Native]::ProcessCpu()
    $t = Get-Date
    $sc = [AbRoundV3.Native]::CpuSeconds($script:srvHandle)
    $mem = [AbRoundV3.Native]::Memory($script:srvHandle)
    $dt = ($t - $script:lastT).TotalSeconds
    $busy = Get-BusyPercent $script:lastSys $now
    $k6Id = if ($script:k6p) { $script:k6p.Id } else { -1 }
    $cId  = if ($script:counters) { $script:counters.Id } else { -1 }
    $s = [AbRoundV3.Native]::Split($script:lastSnap, $snap, $dt, $nCpu, $script:srvId, $k6Id, $cId, $PID, $KernelNames)
    if ($phase -in 'load', 'compaction') {
        foreach ($n in $s.ForeignByName.Keys) {
            $script:foreignCpuSeconds[$n] = $script:foreignCpuSeconds[$n] + $s.ForeignByName[$n] * $dt * $nCpu / 100.0
        }
    }
    $script:samples.Add([pscustomobject]@{
        t = [math]::Round(($t - $script:t0).TotalSeconds, 2); phase = $phase
        busyPct = [math]::Round($busy, 2); serverPct = [math]::Round($s.Server, 2); k6Pct = [math]::Round($s.K6, 2)
        countersPct = [math]::Round($s.Counters, 2); harnessPct = [math]::Round($s.Harness, 2)
        kernelPct = [math]::Round($s.Kernel, 2); foreignPct = [math]::Round($s.Foreign, 2)
        unattributedPct = [math]::Round($busy - $s.Server - $s.K6 - $s.Counters - $s.Harness - $s.Kernel - $s.Foreign, 2)
        topForeign = $s.TopForeign; topForeignPct = [math]::Round($s.TopForeignPct, 2)
        hostMemPct = [math]::Round([AbRoundV3.Native]::HostMemoryPercent(), 1)
        serverCpuS =[math]::Round($sc - $script:cpuBase, 3); ws = $mem[0]; peakWs = $mem[1]; priv = $mem[2]; peakPriv = $mem[3]
    })
    $script:lastSys = $now; $script:lastT = $t; $script:lastSnap = $snap
}
function Get-MachineSummary($list, [string] $phase) {
    $ph = @($list | Where-Object phase -eq $phase)
    # Foreign PLUS unattributed: a build's short-lived processes (a compiler, git, a test host that
    # starts and exits inside one second) are in no snapshot, only in the machine's total.
    $roll = @(); $rollF = @()
    for ($i = 4; $i -lt $ph.Count; $i++) {
        $w = $ph[($i - 4)..$i]
        $rollF += (($w | Measure-Object -Property foreignPct -Average).Average)
        $roll  += ((($w | Measure-Object -Property foreignPct -Average).Average) +
                   [math]::Max(0, ($w | Measure-Object -Property unattributedPct -Average).Average))
    }
    $max5 = if ($roll.Count) { ($roll | Measure-Object -Maximum).Maximum } else { 0 }
    $max5F = if ($rollF.Count) { ($rollF | Measure-Object -Maximum).Maximum } else { 0 }
    $top = $script:foreignCpuSeconds.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 4 |
           ForEach-Object { '{0} {1:N1}s' -f $_.Key, $_.Value }
    $mean = { param($p) if ($ph.Count) { [math]::Round(($ph | Measure-Object -Property $p -Average).Average, 1) } else { $null } }
    $memMax = if ($ph.Count) { [math]::Round(($ph | Measure-Object -Property hostMemPct -Maximum).Maximum, 1) } else { $null }
    return [ordered]@{
        busyMeanPercent         = & $mean 'busyPct'
        serverMeanPercent       = & $mean 'serverPct'
        k6MeanPercent           = & $mean 'k6Pct'
        kernelMeanPercent       = & $mean 'kernelPct'
        unattributedMeanPercent = & $mean 'unattributedPct'
        foreignMeanPercent      = & $mean 'foreignPct'
        foreignMax5sPercent     = [math]::Round($max5, 1)     # foreign + unattributed
        foreignOnlyMax5sPercent = [math]::Round($max5F, 1)
        topForeign              = @($top)
        hostMemoryMaxPercent    = $memMax
        overlapped              = ($max5 -gt $ForeignMaxPercent) -or ($null -ne $memMax -and $memMax -ge $MaxHostMemoryPercent)
    }
}

$exe = Join-Path $ServerDir 'Ameto.Server.exe'
if (-not (Test-Path $exe)) { throw "No server at $exe" }
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { throw "Port $Port is already taken" }

$sha = (& git -C $ServerDir rev-parse HEAD 2>$null)
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$tag = "$Signal-r$Rate" + $(if ($Batch -gt 0) { "-b$Batch" } else { '' }) + $(if ($ExtraEnv.Count -gt 0) { '-env' } else { '' })
$roundDir = Join-Path $OutRoot "$stamp-$tag-$Label"
$dataDir = Join-Path $roundDir 'data'
New-Item -ItemType Directory -Force -Path $roundDir | Out-Null
$base = "http://127.0.0.1:$Port"

$result = [ordered]@{
    label = $Label; signal = $Signal; rate = $Rate; batch = $Batch; duration = $Duration
    sha = $sha; serverDir = $ServerDir; startedAt = (Get-Date).ToString('o'); extraEnv = $ExtraEnv
    ramTargetPercent = $RamTargetPercent; startDelaySeconds = $StartDelaySeconds; status = 'started'
}

$server = $null; $server2 = $null; $script:counters = $null; $script:k6p = $null
$envSet = [ordered]@{
    'Ameto__HttpPort'            = "$Port"
    'Ameto__DataDirectory'       = $dataDir
    'Ameto__Auth__AdminPassword' = '123123'
    'Ameto__RamTargetPercent'    = "$RamTargetPercent"
}
foreach ($k in $ExtraEnv.Keys) { $envSet[$k] = "$($ExtraEnv[$k])" }

try {
    # ── server, key, quiet machine — all inside the fixed start delay ─────────────
    $attempts = @()
    for ($attempt = 1; $attempt -le $MaxStartAttempts; $attempt++) {
        if (Test-Path $dataDir) { Remove-Item -Path $dataDir -Recurse -Force }
        New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
        $server = Start-AmetoServer 'server'
        $loadAt = $script:healthAt.AddSeconds($StartDelaySeconds)
        $token = Get-Token
        $key = Invoke-RestMethod -Method Post -Uri "$base/api/auth/keys" -Headers @{ Authorization = "Bearer $token" } `
                    -ContentType 'application/json' -Body (@{ name = 'k6-ab'; permissions = 7 } | ConvertTo-Json)
        $apiKey = $key.key
        if (-not $apiKey) { throw 'API key creation returned no key' }
        $idle = Wait-Idle ($loadAt.AddSeconds(-5))
        $attempts += $idle
        if ($idle.ok) { break }
        Stop-AmetoServer $server; $server = $null
    }
    $result.startAttempts = $attempts
    if (-not $server) { $result.status = 'idle-timeout'; throw "The machine was not quiet for $IdleSeconds s in any of $MaxStartAttempts start delays" }
    $result.idle = $attempts[-1]
    $result.serverPid = $server.Id
    $script:srvId = $server.Id; $script:srvHandle = $server.Handle

    $os = Get-CimInstance Win32_OperatingSystem
    $result.hostMemoryUsedPercent = [math]::Round(100 * (1 - $os.FreePhysicalMemory / $os.TotalVisibleMemorySize), 1)

    # ── window opens at exactly healthAt + StartDelaySeconds ─────────────────────
    # dotnet-counters stops on its own (killing it loses the CSV tail), so it runs past the window
    # by k6's worst-case graceful stop; rows are cut to the window by their timestamps afterwards.
    $durSec = [int]($Duration.TrimEnd('s'))
    $countersSpan = [TimeSpan]::FromSeconds(4 + $durSec + $SettleSeconds + 30)
    $wait = ($loadAt.AddSeconds(-4) - (Get-Date)).TotalMilliseconds
    if ($wait -gt 0) { Start-Sleep -Milliseconds ([int]$wait) }
    $script:counters = Start-Process -FilePath $DotnetCounters -PassThru -WindowStyle Hidden `
        -ArgumentList @('collect', '--process-id', "$($server.Id)", '--refresh-interval', '1', '--format', 'csv',
                        '--output', (Join-Path $roundDir 'counters.csv'), '--counters', 'System.Runtime',
                        '--duration', ('{0:dd\:hh\:mm\:ss}' -f $countersSpan)) `
        -RedirectStandardOutput (Join-Path $roundDir 'counters.stdout.log') `
        -RedirectStandardError  (Join-Path $roundDir 'counters.stderr.log')
    $wait = ($loadAt.AddSeconds(-1) - (Get-Date)).TotalMilliseconds
    if ($wait -gt 0) { Start-Sleep -Milliseconds ([int]$wait) }

    $diag0 = Invoke-Diag $token (Join-Path $roundDir 'diag-0-before.json')
    $cpu0 = [AbRoundV3.Native]::CpuSeconds($script:srvHandle)

    $k6Script = Join-Path $ScriptDir "k6-$Signal.js"
    $k6args = @('run', '--quiet', '--no-usage-report',
                '--summary-export', (Join-Path $roundDir 'k6-summary.json'),
                '--summary-trend-stats', 'avg,min,med,max,p(90),p(95),p(99)',
                '-e', "AMETO_API_KEY=$apiKey", '-e', "AMETO_URL=$base", '-e', "RATE=$Rate", '-e', "DURATION=$Duration")
    if ($Batch -gt 0) { $k6args += @('-e', "BATCH=$Batch") }
    $k6args += $k6Script

    $samples = [System.Collections.Generic.List[object]]::new()
    $wait = ($loadAt - (Get-Date)).TotalMilliseconds
    if ($wait -gt 0) { Start-Sleep -Milliseconds ([int]$wait) }
    Reset-Sampler $samples
    $loadStart = Get-Date
    $result.loadStartAfterHealthSeconds = [math]::Round(($loadStart - $script:healthAt).TotalSeconds, 2)
    $script:k6p = Start-Process -FilePath $K6 -ArgumentList $k6args -PassThru -WindowStyle Hidden `
              -RedirectStandardOutput (Join-Path $roundDir 'k6.stdout.log') `
              -RedirectStandardError  (Join-Path $roundDir 'k6.stderr.log')
    $k6p = $script:k6p
    while (-not $k6p.HasExited) {
        Start-Sleep -Milliseconds 1000
        if ($server.HasExited) { throw "Server died during the load (code $($server.ExitCode))" }
        Add-Sample 'load'
    }
    $k6p.WaitForExit()
    $loadEnd = Get-Date
    $cpuLoad = [AbRoundV3.Native]::CpuSeconds($script:srvHandle) - $cpu0
    $diag1 = Invoke-Diag $token (Join-Path $roundDir 'diag-1-load-end.json')
    $loadMachine = Get-MachineSummary $samples 'load'

    $settleUntil = (Get-Date).AddSeconds($SettleSeconds)
    while ((Get-Date) -lt $settleUntil) {
        Start-Sleep -Milliseconds 1000
        if ($server.HasExited) { throw "Server died while settling (code $($server.ExitCode))" }
        Add-Sample 'settle'
    }
    $cpuWindow = [AbRoundV3.Native]::CpuSeconds($script:srvHandle) - $cpu0
    $memWindow = [AbRoundV3.Native]::Memory($script:srvHandle)
    $windowEnd = Get-Date
    $windowSeconds = ($windowEnd - $loadStart).TotalSeconds
    $diag2 = Invoke-Diag $token (Join-Path $roundDir 'diag-2-settled.json')

    $samples | Export-Csv -Path (Join-Path $roundDir 'samples.csv') -NoTypeInformation

    # ── k6 ───────────────────────────────────────────────────────────────────
    $sum = Get-Content (Join-Path $roundDir 'k6-summary.json') -Raw | ConvertFrom-Json
    $m = $sum.metrics
    $dur = $m.http_req_duration
    $k6res = [ordered]@{
        exitCode = $k6p.ExitCode
        ingested = [long]($m.ameto_ingested.count)
        dropped  = if ($m.ameto_dropped) { [long]$m.ameto_dropped.count } else { 0 }
        requests = [long]$m.http_reqs.count
        iterations = [long]$m.iterations.count
        droppedIterations = if ($m.dropped_iterations) { [long]$m.dropped_iterations.count } else { 0 }
        httpFailedRate = if ($m.http_req_failed) { $m.http_req_failed.value } else { 0 }
        p50Ms = [math]::Round($dur.med, 2); p95Ms = [math]::Round($dur.'p(95)', 2); p99Ms = [math]::Round($dur.'p(99)', 2)
        maxMs = [math]::Round($dur.max, 2); avgMs = [math]::Round($dur.avg, 2)
        vusMax = if ($m.vus_max) { [int]$m.vus_max.max } else { $null }
    }
    $result.k6 = $k6res

    # ── server, over the window [k6 start, k6 end + settle] ──────────────────
    $loadSamples = @($samples | Where-Object phase -eq 'load')
    $items = [math]::Max(1, $k6res.ingested)
    $alloc = [long]$diag2.managedTotalAllocated - [long]$diag0.managedTotalAllocated
    $gc = Get-GcDelta $diag0 $diag2
    $result.server = [ordered]@{
        windowSeconds    = [math]::Round($windowSeconds, 1)
        loadSeconds      = [math]::Round(($loadEnd - $loadStart).TotalSeconds, 1)
        cpuSecondsLoad   = [math]::Round($cpuLoad, 2)
        cpuSecondsWindow = [math]::Round($cpuWindow, 2)
        cpuUsPerItem     = [math]::Round(1e6 * $cpuWindow / $items, 3)   # = core-seconds per million items
        cpuCoresMeanLoad = [math]::Round($cpuLoad / ($loadEnd - $loadStart).TotalSeconds, 2)
        allocBytes       = $alloc
        allocBytesPerItem = [math]::Round($alloc / $items, 1)
        gen0 = $gc.gen0; gen1 = $gc.gen1; gen2 = $gc.gen2
        peakWorkingSetBytes   = $memWindow[1]
        peakPrivateBytes      = $memWindow[3]
        loadMeanWorkingSetBytes = [long](($loadSamples | Measure-Object -Property ws -Average).Average)
        loadMaxWorkingSetBytes  = [long](($loadSamples | Measure-Object -Property ws -Maximum).Maximum)
        loadMaxPrivateBytes     = [long](($loadSamples | Measure-Object -Property priv -Maximum).Maximum)
        endWorkingSetBytes    = $memWindow[0]
        endPrivateBytes       = $memWindow[2]
        gcHeapBytesEnd        = [long]$diag2.gcHeapBytes
        diagCpuSecondsDelta   = [math]::Round([double]$diag2.processCpuSeconds - [double]$diag0.processCpuSeconds, 1)
    }
    $result.machine = $loadMachine

    # Refusals by reason. Trace ring counters exist only where the server reports them (B);
    # the older server logged one warning per refused request instead, counted from its log.
    $refusals = [ordered]@{}
    foreach ($n in 'tracesRingRefusedForBytes', 'tracesRingRefusedNoSlot', 'tracesRingRefusedNoArena',
                   'metricsExemplarMetricsRefused', 'ingestDroppedRingFull', 'ingestDroppedNoSlab', 'ingestWriteErrorDrops') {
        if ($diag2.PSObject.Properties.Name -contains $n) {
            $refusals[$n] = [long]$diag2.$n - $(if ($diag0.PSObject.Properties.Name -contains $n -and $null -ne $diag0.$n) { [long]$diag0.$n } else { 0 })
        }
    }
    $result.refusals = $refusals

    # ── one representative query per signal, after the load ─────────────────
    Start-Sleep -Milliseconds 1500   # keeps the first query out of the last counters second
    $from = $loadStart.ToUniversalTime().AddMinutes(-1).ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
    $to   = (Get-Date).ToUniversalTime().AddMinutes(1).ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
    $queries = if ($Signal -eq 'traces') {
        [ordered]@{
            'traceql-scan'      = @{ path = '/api/traces/query'; body = @{ query = '{ .db.system = "mssql" && duration > 1s }'; from = $from; to = $to; limit = 200 } }
            'traceql-selective' = @{ path = '/api/traces/query'; body = @{ query = '{ .http.status_code = 500 }'; from = $from; to = $to; limit = 100 } }
        }
    } else {
        [ordered]@{
            'metric-rate-by-route' = @{ path = '/api/metrics/query'; body = @{ metric = 'ameto.loadtest.counter_0'; from = $from; to = $to; step = '15s'; aggregation = 'rate'; groupBy = @('http.route') } }
            'metric-p95-histogram' = @{ path = '/api/metrics/query'; body = @{ metric = 'ameto.loadtest.duration_0'; from = $from; to = $to; step = '15s'; aggregation = 'quantile'; quantile = 0.95 } }
        }
    }
    $qres = [ordered]@{}
    foreach ($qn in $queries.Keys) {
        $q = $queries[$qn]
        $bodyFile = Join-Path $roundDir "query-$qn.json"
        $q.body | ConvertTo-Json -Depth 4 -Compress | Set-Content -Path $bodyFile -Encoding utf8NoBOM
        $runs = @()
        for ($i = 0; $i -lt $QueryRepeats; $i++) {
            $runs += Get-CurlTime $token $q.path $bodyFile (Join-Path $roundDir "query-$qn.out.json")
        }
        $warm = @($runs | Select-Object -Skip 1 | ForEach-Object { $_.ms } | Sort-Object)
        $qres[$qn] = [ordered]@{
            status = $runs[0].status; bytes = $runs[0].bytes
            firstMs = $runs[0].ms
            warmMedianMs = if ($warm.Count) { $warm[[int][math]::Floor(($warm.Count - 1) / 2)] } else { $null }
            allMs = @($runs | ForEach-Object { $_.ms })
        }
    }
    $result.queries = $qres
    $memAfterQ = [AbRoundV3.Native]::Memory($script:srvHandle)
    $result.server.peakWorkingSetAfterQueriesBytes = $memAfterQ[1]
    $result.server.peakPrivateAfterQueriesBytes    = $memAfterQ[3]

    # dotnet-counters, cut to the window: the queries above ran after it and are not in it.
    if ($script:counters) {
        $left = [int]($countersSpan.TotalSeconds + 15 - ((Get-Date) - $loadStart).TotalSeconds)
        if (-not $script:counters.WaitForExit([math]::Max(1000, $left * 1000))) {
            Stop-Process -Id $script:counters.Id -Force -ErrorAction SilentlyContinue
            $result.countersKilled = $true
        }
        $result.counters = Read-Counters (Join-Path $roundDir 'counters.csv') $loadStart $windowEnd $items
    }
    $script:counters = $null; $script:k6p = $null

    # ── traces: the compaction run, on the data this load left ─────────────────
    if ($Signal -eq 'traces' -and -not $NoCompactionRun) { try {
        Stop-AmetoServer $server
        $server2 = Start-AmetoServer 'server-compaction'
        $script:srvId = $server2.Id; $script:srvHandle = $server2.Handle
        $token2 = Get-Token
        $cs = [System.Collections.Generic.List[object]]::new()
        Reset-Sampler $cs
        # The worker loads the cold segments, waits 60 s, then compacts. Snapshot just before.
        $runAt = $server2.StartTime.AddSeconds(57)
        while ((Get-Date) -lt $runAt) { Start-Sleep -Milliseconds 1000; Add-Sample 'wait' }
        $dC0 = Invoke-Diag $token2 (Join-Path $roundDir 'diag-c0.json')
        $cC0 = [AbRoundV3.Native]::CpuSeconds($script:srvHandle)
        $log2 = Join-Path $roundDir 'server-compaction.stdout.log'
        $deadline = (Get-Date).AddSeconds($CompactionTimeoutSeconds)
        $fin = $null
        while ((Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 1000
            if ($server2.HasExited) { throw "Server died while compacting (code $($server2.ExitCode))" }
            Add-Sample 'compaction'
            $fin = Select-String -Path $log2 -Pattern 'Compaction run finished: (\d+) pass\(es\), (\d+) cold segments remain' | Select-Object -First 1
            if ($fin) { break }
        }
        $finishedAt = Get-Date
        $cC1 = [AbRoundV3.Native]::CpuSeconds($script:srvHandle)
        $memC = [AbRoundV3.Native]::Memory($script:srvHandle)
        $dC1 = Invoke-Diag $token2 (Join-Path $roundDir 'diag-c1.json')
        $cs | Export-Csv -Path (Join-Path $roundDir 'samples-compaction.csv') -NoTypeInformation
        # The segment count from the restart's own load line: /api/diagnostics walks the directory
        # at most every 45 s, so its count can predate the load.
        $loaded = Select-String -Path $log2 -Pattern 'Loaded (\d+) cold span segments' | Select-Object -First 1
        $segBefore = if ($loaded) { [int]$loaded.Matches[0].Groups[1].Value } else { $null }
        $merged = @(Select-String -Path $log2 -Pattern 'Compacted (\d+) small segments.*\((\d+) spans\)')
        $rewritten = [long](($merged | ForEach-Object { [long]$_.Matches[0].Groups[2].Value } | Measure-Object -Sum).Sum)
        $cAlloc = [long]$dC1.managedTotalAllocated - [long]$dC0.managedTotalAllocated
        $cgc = Get-GcDelta $dC0 $dC1
        $result.compaction = [ordered]@{
            finished        = [bool]$fin
            segmentsBefore  = $segBefore
            passes          = if ($fin) { [int]$fin.Matches[0].Groups[1].Value } else { $null }
            segmentsAfter   = if ($fin) { [int]$fin.Matches[0].Groups[2].Value } else { $null }
            merges          = $merged.Count
            inputs          = [int](($merged | ForEach-Object { [int]$_.Matches[0].Groups[1].Value } | Measure-Object -Sum).Sum)
            spansRewritten  = $rewritten
            wallSeconds     = [math]::Round(($finishedAt - $server2.StartTime).TotalSeconds - 60, 1)
            cpuSeconds      = [math]::Round($cC1 - $cC0, 2)
            cpuUsPerSpan    = [math]::Round(1e6 * ($cC1 - $cC0) / [math]::Max(1, $rewritten), 3)
            allocBytes      = $cAlloc
            allocBytesPerSpan = [math]::Round($cAlloc / [math]::Max(1, $rewritten), 1)
            gen0 = $cgc.gen0; gen1 = $cgc.gen1; gen2 = $cgc.gen2
            peakWorkingSetBytes = $memC[1]
            peakPrivateBytes    = $memC[3]
            maxGcHeapBytes      = [long]$dC1.gcHeapBytes
            machine         = Get-MachineSummary $cs 'compaction'
        }
        if ($result.compaction.machine.overlapped) { $result.compactionOverlapped = $true }
    } catch {
        # The load's numbers stand on their own; a compaction run that failed is reported, not fatal.
        $result.compactionError = "$_"
        Write-Warning "Compaction run failed: $_"
    } }

    $result.status = if ($result.machine.overlapped) { 'overlapped' } else { 'ok' }
}
catch {
    $result.error = "$_"
    if ($result.status -eq 'started') { $result.status = 'failed' }
    Write-Warning "Round failed: $_"
}
finally {
    if ($script:k6p -and -not $script:k6p.HasExited) { Stop-Process -Id $script:k6p.Id -Force -ErrorAction SilentlyContinue }
    if ($script:counters -and -not $script:counters.HasExited) { Stop-Process -Id $script:counters.Id -Force -ErrorAction SilentlyContinue }
    Stop-AmetoServer $server
    Stop-AmetoServer $server2

    # What the round left on disk, then the server's own log, then the data itself.
    if (Test-Path $dataDir) {
        $sizes = [ordered]@{}; $files = [ordered]@{}
        Get-ChildItem $dataDir -Directory -ErrorAction SilentlyContinue | ForEach-Object {
            $m = Get-ChildItem $_.FullName -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum
            $sizes[$_.Name] = $m.Sum; $files[$_.Name] = $m.Count
        }
        $result.dataDirBytes = $sizes
        $result.dataDirFiles = $files
        $logs = Join-Path $dataDir 'logs'
        if (Test-Path $logs) { Copy-Item -Path $logs -Destination (Join-Path $roundDir 'server-logs') -Recurse -ErrorAction SilentlyContinue }
        if (-not $KeepData) { Remove-Item -Path $dataDir -Recurse -Force -ErrorAction SilentlyContinue }
    }
    # The older server logs one warning per refused trace batch rather than counting it. Counted
    # from the console log only: the file log carries the same lines and may lose its tail to the kill.
    $stdout = Join-Path $roundDir 'server.stdout.log'
    if (Test-Path $stdout) {
        $result.logRingBackPressureWarnings = @(Select-String -Path $stdout -SimpleMatch 'applying back-pressure').Count
        $result.logRamPressureWarnings      = @(Select-String -Path $stdout -SimpleMatch 'RAM pressure: system memory').Count
        $result.logWarnOrWorse              = @(Select-String -Path $stdout -Pattern '^(warn|fail|crit):').Count
        $result.logTraceFlushes             = @(Select-String -Path $stdout -Pattern 'Flushed \d+ spans').Count
        $result.logTraceMergesDuringLoad    = @(Select-String -Path $stdout -Pattern 'Compacted \d+ small segments').Count
    }
    $result.finishedAt = (Get-Date).ToString('o')
    $result | ConvertTo-Json -Depth 6 | Set-Content -Path (Join-Path $roundDir 'result.json') -Encoding utf8
    Write-Output (Join-Path $roundDir 'result.json')
}
