#Requires -Version 7
<#
.SYNOPSIS
  Tables from ab-round.ps1 results: per scenario, A against B, as median (min–max) over the kept
  rounds, plus every round on its own line.

.EXAMPLE
  ./ab-summary.ps1 -OutRoot C:\tmp\rounds > summary.md
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string[]] $OutRoot,
    [switch] $IncludeOverlapped
)

$ErrorActionPreference = 'Stop'
[System.Threading.Thread]::CurrentThread.CurrentCulture = [cultureinfo]::InvariantCulture

function Fmt([double] $v, [int] $dec = 0) {
    if ([double]::IsNaN($v)) { return '—' }
    $s = $v.ToString("N$dec", [cultureinfo]::InvariantCulture)
    return $s.Replace(',', ' ')
}
function Median([double[]] $xs) {
    $s = @($xs | Sort-Object); $n = $s.Count
    if ($n -eq 0) { return [double]::NaN }
    if ($n % 2) { return $s[($n - 1) / 2] }
    return ($s[$n / 2 - 1] + $s[$n / 2]) / 2
}
function Cell([double[]] $xs, [int] $dec) {
    if ($null -eq $xs -or $xs.Count -eq 0) { return "—" }
    $m = Median $xs
    if ($xs.Count -eq 1) { return Fmt $m $dec }
    $lo = ($xs | Measure-Object -Minimum).Minimum; $hi = ($xs | Measure-Object -Maximum).Maximum
    return '{0} ({1}–{2})' -f (Fmt $m $dec), (Fmt $lo $dec), (Fmt $hi $dec)
}
function Change([double[]] $a, [double[]] $b) {
    if ($null -eq $a -or $null -eq $b -or $a.Count -eq 0 -or $b.Count -eq 0) { return "—" }
    $ma = Median $a; $mb = Median $b
    if ($ma -eq 0) { return $(if ($mb -eq 0) { '0' } else { 'new' }) }
    $p = 100.0 * ($mb - $ma) / $ma
    return '{0}{1} %' -f $(if ($p -gt 0) { '+' } else { '−' }), (Fmt ([math]::Abs($p)) 1)
}

$rounds = foreach ($root in $OutRoot) {
    Get-ChildItem -Path $root -Directory | Where-Object Name -ne 'discarded' | ForEach-Object {
        $f = Join-Path $_.FullName 'result.json'
        if (Test-Path $f) {
            $r = Get-Content $f -Raw | ConvertFrom-Json
            $r | Add-Member -NotePropertyName dir -NotePropertyValue $_.Name
            $r
        }
    }
}
$rounds = @($rounds | Where-Object { $_.status -eq 'ok' -or ($IncludeOverlapped -and $_.status -eq 'overlapped') })

$MB = 1024.0 * 1024.0
# name, unit/decimals, value selector
$rows = @(
    @{ n = 'Spans or points ingested';            d = 0; f = { $_.k6.ingested } }
    @{ n = 'Dropped (k6 `ameto_dropped`)';         d = 0; f = { $_.k6.dropped } }
    @{ n = 'k6 dropped iterations';               d = 0; f = { $_.k6.droppedIterations } }
    @{ n = 'Batch latency p50, ms';               d = 2; f = { $_.k6.p50Ms } }
    @{ n = 'Batch latency p95, ms';               d = 2; f = { $_.k6.p95Ms } }
    @{ n = 'Batch latency p99, ms';               d = 2; f = { $_.k6.p99Ms } }
    @{ n = 'Server CPU, core-seconds';            d = 1; f = { $_.server.cpuSecondsWindow } }
    @{ n = 'CPU µs per item (= core-s per million)'; d = 2; f = { $_.server.cpuUsPerItem } }
    @{ n = '… of which kernel (system) time, core-s'; d = 1; f = { $_.counters.cpuSystemSeconds } }
    @{ n = 'Allocated, MB';                       d = 0; f = { $_.counters.allocBytes / $MB } }
    @{ n = 'Allocated bytes per item';            d = 0; f = { $_.counters.allocBytesPerItem } }
    @{ n = 'gen0 collections';                    d = 0; f = { $_.counters.gen0 } }
    @{ n = 'gen1 collections';                    d = 0; f = { $_.counters.gen1 } }
    @{ n = 'gen2 collections';                    d = 0; f = { $_.counters.gen2 } }
    @{ n = 'GC pause, s';                         d = 2; f = { $_.counters.gcPauseSeconds } }
    @{ n = 'GC heap after a collection, max MB';  d = 0; f = { $_.counters.maxGcHeapAfterCollectionBytes / $MB } }
    @{ n = 'Peak working set, MB';                d = 0; f = { $_.server.peakWorkingSetBytes / $MB } }
    @{ n = 'Peak private bytes, MB';              d = 0; f = { $_.server.peakPrivateBytes / $MB } }
    @{ n = 'Working set, mean under load, MB';    d = 0; f = { $_.server.loadMeanWorkingSetBytes / $MB } }
    @{ n = 'Thread-pool work items per item';     d = 3; f = { $_.counters.threadPoolWorkItems / [math]::Max(1, $_.k6.ingested) } }
    @{ n = 'Lock contentions';                    d = 0; f = { $_.counters.lockContentions } }
)
# Traces: the compaction run after a restart on the data the load left (ab-round.ps1).
$compactionRows = @(
    @{ n = 'Segments in → out';                   d = 0; f = { $_.compaction.segmentsBefore }; f2 = { $_.compaction.segmentsAfter } }
    @{ n = 'Spans rewritten';                     d = 0; f = { $_.compaction.spansRewritten } }
    @{ n = 'Wall time, s (approx.)';              d = 1; f = { $_.compaction.wallSeconds } }
    @{ n = 'CPU, core-seconds';                   d = 2; f = { $_.compaction.cpuSeconds } }
    @{ n = 'CPU µs per ingested span';            d = 2; f = { 1e6 * $_.compaction.cpuSeconds / [math]::Max(1, $_.k6.ingested) } }
    @{ n = 'Allocated, MB';                       d = 0; f = { $_.compaction.allocBytes / $MB } }
    @{ n = 'Allocated bytes per ingested span';   d = 0; f = { $_.compaction.allocBytes / [math]::Max(1, $_.k6.ingested) } }
    @{ n = 'gen2 collections';                    d = 0; f = { $_.compaction.gen2 } }
    @{ n = 'Peak working set since restart, MB';  d = 0; f = { $_.compaction.peakWorkingSetBytes / $MB } }
    @{ n = 'Peak private bytes since restart, MB'; d = 0; f = { $_.compaction.peakPrivateBytes / $MB } }
)

$groups = $rounds | Group-Object { '{0}|{1}|{2}|{3}' -f $_.signal, $_.rate, $_.batch, (($_.extraEnv.PSObject.Properties | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ',') }
foreach ($g in ($groups | Sort-Object Name)) {
    $first = $g.Group[0]
    $a = @($g.Group | Where-Object label -eq 'A' | Sort-Object startedAt)
    $b = @($g.Group | Where-Object label -eq 'B' | Sort-Object startedAt)
    $perReq = if ($first.signal -eq 'traces' -and $first.batch -gt 0) { $first.batch } else { 1000 }
    $env = ($first.extraEnv.PSObject.Properties | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ', '
    "### {0}, {1} {2}/s offered, {3}{4}" -f $first.signal, (Fmt ($first.rate * $perReq)), $(if ($first.signal -eq 'traces') { 'spans' } else { 'points' }),
        $first.duration, $(if ($env) { ", $env" } else { '' })
    ''
    "A rounds: $($a.Count), B rounds: $($b.Count). Cells are median (min–max) over the rounds."
    ''
    '| Metric | A | B | B vs A (median) |'
    '|---|---|---|---|'
    foreach ($row in $rows) {
        $va = [double[]]@($a | ForEach-Object $row.f); $vb = [double[]]@($b | ForEach-Object $row.f)
        '| {0} | {1} | {2} | {3} |' -f $row.n, (Cell $va $row.d), (Cell $vb $row.d), (Change $va $vb)
    }
    $qnames = @($g.Group | ForEach-Object { $_.queries.PSObject.Properties.Name } | Select-Object -Unique)
    # first run as recorded; "warm" = the median of the repeats after it, recomputed from allMs
    $qv = { param($r, $q, $k) if ($k -eq 'firstMs') { $r.queries.$q.firstMs } else { Median ([double[]]@($r.queries.$q.allMs | Select-Object -Skip 1)) } }
    foreach ($q in $qnames) {
        foreach ($k in 'firstMs', 'warmMedianMs') {
            $va = [double[]]@($a | ForEach-Object { & $qv $_ $q $k }); $vb = [double[]]@($b | ForEach-Object { & $qv $_ $q $k })
            $label = if ($k -eq 'firstMs') { "Query ``$q``, first run, ms" } else { "Query ``$q``, warm median, ms" }
            '| {0} | {1} | {2} | {3} |' -f $label, (Cell $va 1) , (Cell $vb 1), (Change $va $vb)
        }
    }
    $ca = @($a | Where-Object { $_.compaction -and $_.compaction.finished -and -not $_.compactionOverlapped })
    $cb = @($b | Where-Object { $_.compaction -and $_.compaction.finished -and -not $_.compactionOverlapped })
    if ($ca.Count -or $cb.Count) {
        ''
        "Compaction run after a restart on the same data — A rounds: $($ca.Count), B rounds: $($cb.Count)."
        ''
        '| Metric | A | B | B vs A (median) |'
        '|---|---|---|---|'
        foreach ($row in $compactionRows) {
            $va = [double[]]@($ca | ForEach-Object $row.f); $vb = [double[]]@($cb | ForEach-Object $row.f)
            if ($row.f2) {
                $va2 = [double[]]@($ca | ForEach-Object $row.f2); $vb2 = [double[]]@($cb | ForEach-Object $row.f2)
                '| {0} | {1} → {2} | {3} → {4} | |' -f $row.n, (Cell $va 0), (Cell $va2 0), (Cell $vb 0), (Cell $vb2 0)
            } else {
                '| {0} | {1} | {2} | {3} |' -f $row.n, (Cell $va $row.d), (Cell $vb $row.d), (Change $va $vb)
            }
        }
    }
    ''
    'Every round, in the order it ran:'
    ''
    '| Round | Side | CPU µs/item | Alloc B/item | gen2 | Peak WS MB | p50 / p99 ms | Dropped | Refusals | Idle before, mean (max) % | Foreign CPU, mean / 5-s max % | Host RAM max % |'
    '|---|---|---|---|---|---|---|---|---|---|---|---|'
    foreach ($r in ($g.Group | Sort-Object startedAt)) {
        $ref = ($r.refusals.PSObject.Properties | Where-Object { $_.Value -ne 0 -and $_.Name -like 'traces*' } | ForEach-Object { "$($_.Name -replace '^tracesRingRefused', '') $($_.Value)" }) -join ', '
        if ($r.logRingBackPressureWarnings -gt 0) { $ref = "$($r.logRingBackPressureWarnings) refused requests (log)" }
        elseif (-not $ref) { $ref = $(if ($r.refusals.PSObject.Properties.Name -contains 'tracesRingRefusedNoSlot') { 'none' } elseif ($r.signal -eq 'traces') { 'none logged' } else { '—' }) }
        $mem = if ($r.machine.hostMemoryMaxPercent) { $r.machine.hostMemoryMaxPercent } else { "$($r.hostMemoryUsedPercent) (start)" }
        '| {0} | {1} | {2} | {3} | {4} | {5} | {6} / {7} | {8} | {9} | {10} ({11}) | {12} / {13} | {14} |' -f $r.dir.Substring(0, 13), $r.label,
            (Fmt $r.server.cpuUsPerItem 2), (Fmt $r.counters.allocBytesPerItem 0), $r.counters.gen2, (Fmt ($r.server.peakWorkingSetBytes / $MB) 0),
            $r.k6.p50Ms, $r.k6.p99Ms, (Fmt $r.k6.dropped 0), $ref, $r.idle.meanPercent, $r.idle.maxPercent,
            $r.machine.foreignMeanPercent, $r.machine.foreignMax5sPercent, $mem
    }
    ''
}

# Rounds thrown away for overlapping a spike of foreign CPU, for the record.
$disc = foreach ($root in $OutRoot) {
    $d = Join-Path $root 'discarded'
    if (Test-Path $d) { Get-ChildItem $d -Directory | ForEach-Object { $f = Join-Path $_.FullName 'result.json'; if (Test-Path $f) { $r = Get-Content $f -Raw | ConvertFrom-Json; $r | Add-Member -NotePropertyName dir -NotePropertyValue $_.Name; $r } } }
}
if ($disc) {
    # A round moved by hand says why in ab-compare.log ("manual: <round> discarded - <reason>").
    $manual = @{}
    foreach ($root in $OutRoot) {
        $log = Join-Path $root 'ab-compare.log'
        if (Test-Path $log) {
            foreach ($m in (Select-String -Path $log -Pattern 'manual: (\S+) discarded - (.*)$')) { $manual[$m.Matches[0].Groups[1].Value] = $m.Matches[0].Groups[2].Value }
        }
    }
    '### Discarded rounds'
    ''
    '| Round | Why | Foreign + unattributed CPU, 5-s max % | Host RAM max % | Top foreign processes |'
    '|---|---|---|---|---|'
    foreach ($r in ($disc | Sort-Object startedAt)) {
        $why = if ($manual.ContainsKey($r.dir)) { $manual[$r.dir] } else { $r.status }
        '| {0} | {1} | {2} | {3} | {4} |' -f $r.dir, $why, $r.machine.foreignMax5sPercent, $r.machine.hostMemoryMaxPercent, ($r.machine.topForeign -join ', ')
    }
    ''
}
