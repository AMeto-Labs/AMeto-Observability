#Requires -Version 7
<#
.SYNOPSIS
  Interleaved A/B rounds at one offered rate: A, B, A, B, ... with every round that overlapped
  another process's CPU spike (or failed to start) thrown away and run again.

.DESCRIPTION
  Each round is ab-round.ps1. A round whose result says "overlapped" or "failed" or
  "idle-timeout" is moved to <OutRoot>\discarded\ and repeated, up to -MaxAttempts times per
  slot. The kept rounds stay in <OutRoot>; ab-summary.ps1 turns them into tables.

.EXAMPLE
  ./ab-compare.ps1 -A C:\tmp\treeA\src\Ameto.Server\bin\Release\net10.0 `
                   -B C:\tmp\treeB\src\Ameto.Server\bin\Release\net10.0 `
                   -OutRoot C:\tmp\rounds -Signal traces -Rate 50 -Pairs 3
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $A,
    [Parameter(Mandatory)] [string] $B,
    [Parameter(Mandatory)] [string] $OutRoot,
    [Parameter(Mandatory)] [ValidateSet('traces', 'metrics')] [string] $Signal,
    [int]    $Rate = 50,
    [int]    $Batch = 0,
    [string] $Duration = '120s',
    [int]    $Pairs = 2,
    [int]    $MaxAttempts = 12,
    [int]    $Port = 18341,
    [hashtable] $ExtraEnv = @{},
    [double] $ForeignMaxPercent = 15,
    [switch] $NoCompactionRun,
    [string] $Order = ''                 # explicit slots, e.g. 'B' to redo one; default: 'AB' x Pairs
)

$ErrorActionPreference = 'Stop'
$round = Join-Path $PSScriptRoot 'ab-round.ps1'
$discarded = Join-Path $OutRoot 'discarded'
New-Item -ItemType Directory -Force -Path $OutRoot, $discarded | Out-Null
$log = Join-Path $OutRoot 'ab-compare.log'

$slots = if ($Order) { $Order.ToCharArray() | ForEach-Object { "$_" } } else { for ($i = 1; $i -le $Pairs; $i++) { 'A'; 'B' } }
foreach ($label in $slots) {
    $dir = if ($label -eq 'A') { $A } else { $B }
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $resultFile = & $round -ServerDir $dir -Label $label -Signal $Signal -OutRoot $OutRoot -Rate $Rate `
                              -Batch $Batch -Duration $Duration -Port $Port -ExtraEnv $ExtraEnv `
                              -ForeignMaxPercent $ForeignMaxPercent -NoCompactionRun:$NoCompactionRun | Select-Object -Last 1
        $r = Get-Content $resultFile -Raw | ConvertFrom-Json
        $line = '{0:o} {1} {2} r{3} attempt {4}: {5} idle {6}% foreign(5s max) {7}% -> {8}' -f (Get-Date), $Signal, $label,
                $Rate, $attempt, $r.status, $r.idle.meanPercent, $r.machine.foreignMax5sPercent, (Split-Path $resultFile -Parent)
        Add-Content -Path $log -Value $line
        Write-Host $line
        if ($r.status -eq 'ok') { break }
        Move-Item -Path (Split-Path $resultFile -Parent) -Destination $discarded
    }
}
