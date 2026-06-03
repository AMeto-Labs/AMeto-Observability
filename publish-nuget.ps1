#Requires -Version 7
<#
.SYNOPSIS
    Pack and push the Rd.Log.Serilog NuGet package.

.DESCRIPTION
    1. dotnet pack -c Release  -> ./artifacts/nuget/Rd.Log.Serilog.<Version>.nupkg
    2. dotnet nuget push        -> nuget.org (or the source you pass via -Source)

    The API key is read from the parameter, the env var NUGET_API_KEY, or
    the NuGet config (`nuget config defaultPushSource`).

.PARAMETER Version
    Overrides the <Version> in the csproj. When omitted the csproj value is used.

.PARAMETER Configuration
    Build configuration. Default: Release.

.PARAMETER ApiKey
    NuGet API key. Falls back to $env:NUGET_API_KEY.

.PARAMETER Source
    NuGet feed URL. Default: https://api.nuget.org/v3/index.json

.PARAMETER OutputDir
    Folder for the produced .nupkg / .snupkg.  Default: ./artifacts/nuget

.PARAMETER NoPush
    Only pack, do not push.

.PARAMETER SkipBuild
    Reuse an existing .nupkg in $OutputDir (skip pack step).

.PARAMETER CopyTo
    After packing, copy the produced .nupkg / .snupkg to this folder as well.
    Example: -CopyTo "C:\path\to\local\feed"

.EXAMPLE
    .\publish-nuget.ps1                           # use csproj version + $env:NUGET_API_KEY
    .\publish-nuget.ps1 -Version 0.2.0            # bump version, pack & push
    .\publish-nuget.ps1 -NoPush                   # just produce the .nupkg
    .\publish-nuget.ps1 -NoPush -CopyTo "C:\local\nupkgs"  # pack + copy locally, no push
    .\publish-nuget.ps1 -ApiKey oy2... -Source https://api.nuget.org/v3/index.json
#>
param(
    [string]$Version,
    [string]$Configuration = "Release",
    [string]$ApiKey        = $env:NUGET_API_KEY,
    [string]$Source        = "https://api.nuget.org/v3/index.json",
    [string]$OutputDir     = "artifacts/nuget",
    [string]$CopyTo        = "C:\Users\ruslan.akhmetov\Desktop\Processing\NT.Processing-c\Services\nupkgs",
    [switch]$NoPush,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$root      = $PSScriptRoot
$proj      = Join-Path $root "src\Rd.Log.Serilog\Rd.Log.Serilog.csproj"
$outFull   = Join-Path $root $OutputDir

if (-not (Test-Path $proj)) { throw "Project not found: $proj" }
New-Item -ItemType Directory -Force -Path $outFull | Out-Null

Write-Host ""
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Rd.Log.Serilog  ·  NuGet publish" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# ── 1. Pack ───────────────────────────────────────────────────────────────────
if (-not $SkipBuild) {
    Write-Host "[ 1/2 ] dotnet pack ($Configuration)..." -ForegroundColor Yellow

    $packArgs = @(
        "pack", $proj,
        "-c", $Configuration,
        "-o", $outFull,
        "--include-symbols",
        "-p:SymbolPackageFormat=snupkg",
        "-p:ContinuousIntegrationBuild=true"
    )
    if ($Version) { $packArgs += "-p:Version=$Version" }

    & dotnet @packArgs
    if ($LASTEXITCODE -ne 0) { throw "dotnet pack failed" }
}
else {
    Write-Host "[ 1/2 ] Skipping pack (--SkipBuild)" -ForegroundColor DarkGray
}

# Resolve the .nupkg we are going to push
$nupkg = Get-ChildItem $outFull -Filter "Rd.Log.Serilog.*.nupkg" `
        | Where-Object { $_.Name -notlike "*.symbols.nupkg" } `
        | Sort-Object LastWriteTime -Descending `
        | Select-Object -First 1

if (-not $nupkg) { throw "No .nupkg produced in $outFull" }
Write-Host "        Package: $($nupkg.FullName)" -ForegroundColor Green

# ── 1b. Copy to local feed ────────────────────────────────────────────────────
if ($CopyTo) {
    New-Item -ItemType Directory -Force -Path $CopyTo | Out-Null
    Copy-Item $nupkg.FullName -Destination $CopyTo -Force
    Write-Host "        Copied .nupkg -> $CopyTo" -ForegroundColor Cyan
    $snupkg = Get-ChildItem $outFull -Filter "Rd.Log.Serilog.*.snupkg" `
              | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($snupkg) {
        Copy-Item $snupkg.FullName -Destination $CopyTo -Force
        Write-Host "        Copied .snupkg -> $CopyTo" -ForegroundColor Cyan
    }
}

# ── 2. Push ───────────────────────────────────────────────────────────────────
if ($NoPush) {
    Write-Host ""
    Write-Host "[ 2/2 ] --NoPush set; package left at $($nupkg.FullName)" -ForegroundColor DarkGray
    return
}

if (-not $ApiKey) {
    throw "No API key supplied. Pass -ApiKey or set `$env:NUGET_API_KEY."
}

Write-Host ""
Write-Host "[ 2/2 ] dotnet nuget push -> $Source" -ForegroundColor Yellow

& dotnet nuget push $nupkg.FullName `
    --api-key $ApiKey `
    --source  $Source `
    --skip-duplicate
if ($LASTEXITCODE -ne 0) { throw "dotnet nuget push failed" }

Write-Host ""
Write-Host "Done." -ForegroundColor Green
