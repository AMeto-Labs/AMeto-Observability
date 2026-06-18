#Requires -Version 7
<#
.SYNOPSIS
    Build a self-contained Ameto release for Windows or Linux.

.DESCRIPTION
    1. Builds the Angular SPA (production)
    2. Runs dotnet publish (self-contained) for the chosen target OS
    3. Bundles the OS-specific installer into release/<os>/

.PARAMETER OS
    Target OS: "windows", "linux", or "both".
    If omitted, an interactive menu is shown.

.PARAMETER Arch
    CPU architecture: "x64" (default) or "arm64".

.PARAMETER OutDir
    Root output directory. Default: ./release

.PARAMETER SkipClient
    Skip Angular build (useful when re-packaging after a client build).

.EXAMPLE
    .\build-release.ps1
    .\build-release.ps1 -OS windows
    .\build-release.ps1 -OS linux -Arch arm64
    .\build-release.ps1 -OS both
#>
param(
    [ValidateSet("windows", "linux", "both", "")]
    [string]$OS = "",

    [ValidateSet("x64", "arm64")]
    [string]$Arch = "x64",

    [string]$OutDir = "release",

    [switch]$SkipClient
)

$ErrorActionPreference = "Stop"
$root       = $PSScriptRoot
$clientDir  = Join-Path $root "client"
$serverProj = Join-Path $root "src\Ameto.Server\Ameto.Server.csproj"
$wwwroot    = Join-Path $root "src\Ameto.Server\wwwroot"

function Write-Banner([string]$msg) {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step([string]$n, [string]$msg) {
    Write-Host "[ $n ] $msg" -ForegroundColor Yellow
}

function Write-Ok([string]$msg) {
    Write-Host "      $msg" -ForegroundColor Green
}

# ── OS selection ──────────────────────────────────────────────────────────────
if (-not $OS) {
    Write-Banner "Ameto  ·  Build Release"
    Write-Host "  Target OS:" -ForegroundColor Cyan
    Write-Host "    [1] Windows  (win-$Arch)" -ForegroundColor Gray
    Write-Host "    [2] Linux    (linux-$Arch)" -ForegroundColor Gray
    Write-Host "    [3] Both" -ForegroundColor Gray
    Write-Host ""
    do { $choice = Read-Host "  Choice (1/2/3)" } while ($choice -notin '1','2','3')

    $OS = switch ($choice) { '1' { 'windows' } '2' { 'linux' } '3' { 'both' } }
}

$targets = if ($OS -eq "both") { @("windows","linux") } else { @($OS) }

Write-Banner "Ameto  ·  Build Release  [$($targets -join ', ')-$Arch]"

# ── Step 1: Build Angular ─────────────────────────────────────────────────────
if (-not $SkipClient) {
    Write-Step "1/3" "Building Angular client (production)..."
    Push-Location $clientDir
    try {
        npm install --prefer-offline --no-audit --no-fund 2>&1 | Out-Null
        ng build --configuration production --output-path $wwwroot
        if ($LASTEXITCODE -ne 0) { throw "ng build failed" }

        # Flatten Angular 17+ browser/ subdirectory
        $browserSub = Join-Path $wwwroot "browser"
        if (Test-Path $browserSub) {
            Get-ChildItem $browserSub | Move-Item -Destination $wwwroot -Force
            Remove-Item $browserSub -Recurse -Force
        }
    }
    finally { Pop-Location }
    Write-Ok "Angular build complete → $wwwroot"
}
else {
    Write-Step "1/3" "Skipping Angular build (-SkipClient)."
}

# ── Step 2: dotnet publish for each target ────────────────────────────────────
Write-Step "2/3" "Publishing .NET server (self-contained)..."

foreach ($targetOS in $targets) {
    $rid       = "$targetOS-$Arch"
    $targetDir = Join-Path $root $OutDir $targetOS
    $exe       = if ($targetOS -eq "windows") { "Ameto.Server.exe" } else { "Ameto.Server" }

    Write-Host ""
    Write-Host "      → $rid  →  $targetDir" -ForegroundColor DarkCyan

    # Clean previous output so stale files don't accumulate
    if (Test-Path $targetDir) { Remove-Item $targetDir -Recurse -Force }
    New-Item -ItemType Directory -Path $targetDir -Force | Out-Null

    dotnet publish $serverProj `
        --configuration Release `
        --output $targetDir `
        --runtime $rid `
        --self-contained true `
        /p:DebugType=None `
        /p:DebugSymbols=false `
        /p:GenerateDocumentationFile=false

    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed for $rid" }

    # Strip debug / xml / locale / web.config / appsettings
    Get-ChildItem -Path $targetDir -Recurse -Include *.pdb, *.xml -ErrorAction SilentlyContinue |
        Remove-Item -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $targetDir -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match '^[a-z]{2}(-[A-Z]{2})?$' } |
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $targetDir 'web.config')        -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $targetDir -Filter 'appsettings*.json' -ErrorAction SilentlyContinue |
        Remove-Item -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $targetDir 'dotnet-tools.json') -Force -ErrorAction SilentlyContinue

    Write-Ok "$rid publish complete → $targetDir"
}

# ── Step 3: Bundle installer into each target folder ─────────────────────────
Write-Step "3/3" "Bundling installers..."

foreach ($targetOS in $targets) {
    $targetDir = Join-Path $root $OutDir $targetOS

    if ($targetOS -eq "windows") {
        $installerSrc = Join-Path $root "install\windows\install.ps1"
        $installerDst = Join-Path $targetDir "install.ps1"
        Copy-Item $installerSrc $installerDst -Force
        Write-Ok "Windows: install.ps1  →  $installerDst"
    }
    else {
        $installerSrc = Join-Path $root "install\linux\install.sh"
        $installerDst = Join-Path $targetDir "install.sh"
        Copy-Item $installerSrc $installerDst -Force
        # Ensure Unix line endings so the script is executable on Linux
        $content = [System.IO.File]::ReadAllText($installerDst) -replace "`r`n", "`n"
        [System.IO.File]::WriteAllText($installerDst, $content, [System.Text.Encoding]::UTF8)
        Write-Ok "Linux:   install.sh   →  $installerDst"
    }
}

# ── Done ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "═══════════════════════════════════════════" -ForegroundColor Green
Write-Host "  Release ready!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════" -ForegroundColor Green
Write-Host ""

foreach ($targetOS in $targets) {
    $targetDir = Join-Path $root $OutDir $targetOS
    Write-Host "  $targetOS" -ForegroundColor Cyan
    Write-Host "    Folder:    $targetDir" -ForegroundColor Gray

    if ($targetOS -eq "windows") {
        Write-Host "    Install:   (Admin PowerShell) .\install.ps1" -ForegroundColor Gray
    }
    else {
        Write-Host "    Install:   sudo bash install.sh" -ForegroundColor Gray
    }
    Write-Host ""
}
