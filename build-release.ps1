#Requires -Version 7
<#
.SYNOPSIS
    Build a self-contained Ameto release for Windows or Linux.

.PARAMETER OutDir
    Root output directory. Default: ./release

.PARAMETER SkipClient
    Skip Angular build (useful when re-packaging after a client build).
#>
param(
    [string]$OutDir     = "release",
    [switch]$SkipClient
)

$ErrorActionPreference = "Stop"
$root       = $PSScriptRoot
$clientDir  = Join-Path $root "client"
$serverProj = Join-Path $root "src\Ameto.Server\Ameto.Server.csproj"
$wwwroot    = Join-Path $root "src\Ameto.Server\wwwroot"

# ── Arrow-key menu ────────────────────────────────────────────────────────────
function Invoke-Menu {
    param(
        [string]   $Title,
        [string[]] $Items
    )

    $selected = 0
    [Console]::CursorVisible = $false

    # Print title + blank line once (stays fixed above the menu)
    Write-Host ""
    Write-Host "  $Title" -ForegroundColor Cyan
    Write-Host ""

    # Remember where the list starts so we can redraw in-place
    $menuTop = [Console]::CursorTop

    function Render {
        [Console]::SetCursorPosition(0, $menuTop)
        for ($i = 0; $i -lt $Items.Count; $i++) {
            if ($i -eq $selected) {
                Write-Host ("  > " + $Items[$i]).PadRight([Console]::WindowWidth - 1) `
                    -ForegroundColor Black -BackgroundColor Cyan
            } else {
                Write-Host ("    " + $Items[$i]).PadRight([Console]::WindowWidth - 1) `
                    -ForegroundColor Gray
            }
        }
    }

    Render

    try {
        while ($true) {
            $key = [Console]::ReadKey($true)
            switch ($key.Key) {
                'UpArrow'   { if ($selected -gt 0)               { $selected-- }; Render }
                'DownArrow' { if ($selected -lt $Items.Count - 1) { $selected++ }; Render }
                'Enter'     {
                    # Print confirmed selection and move past the menu block
                    [Console]::SetCursorPosition(0, $menuTop)
                    for ($i = 0; $i -lt $Items.Count; $i++) {
                        if ($i -eq $selected) {
                            Write-Host ("  > " + $Items[$i]).PadRight([Console]::WindowWidth - 1) `
                                -ForegroundColor Cyan
                        } else {
                            Write-Host (" " * ([Console]::WindowWidth - 1))
                        }
                    }
                    Write-Host ""
                    return $selected
                }
                'Escape' { Write-Host ""; exit 0 }
            }
        }
    }
    finally {
        [Console]::CursorVisible = $true
    }
}

# ── Banner ────────────────────────────────────────────────────────────────────
Clear-Host
Write-Host ""
Write-Host "  ═══════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host "    Ameto  ·  Build Release" -ForegroundColor Cyan
Write-Host "  ═══════════════════════════════════════════" -ForegroundColor DarkCyan

# ── Select OS ─────────────────────────────────────────────────────────────────
$osItems = @(
    "Windows   (win-x64)"
    "Windows   (win-arm64)"
    "Linux     (linux-x64)"
    "Linux     (linux-arm64)"
    "Both      (win-x64 + linux-x64)"
)

$osIndex = Invoke-Menu -Title "Target OS / Architecture:" -Items $osItems

$targets = switch ($osIndex) {
    0 { @(@{ OS = "windows"; Arch = "x64"   }) }
    1 { @(@{ OS = "windows"; Arch = "arm64" }) }
    2 { @(@{ OS = "linux";   Arch = "x64"   }) }
    3 { @(@{ OS = "linux";   Arch = "arm64" }) }
    4 { @(@{ OS = "windows"; Arch = "x64" }; @{ OS = "linux"; Arch = "x64" }) }
}

Write-Host ""
Write-Host "  ═══════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host ""

function Write-Step([string]$n, [string]$msg) {
    Write-Host "[ $n ] $msg" -ForegroundColor Yellow
}
function Write-Ok([string]$msg) {
    Write-Host "      $msg" -ForegroundColor Green
}

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
} else {
    Write-Step "1/3" "Skipping Angular build (-SkipClient)."
}

# ── Step 2: dotnet publish ────────────────────────────────────────────────────
Write-Step "2/3" "Publishing .NET server (self-contained)..."

foreach ($t in $targets) {
    $rid       = "$($t.OS)-$($t.Arch)"
    $targetDir = Join-Path $root $OutDir $t.OS

    Write-Host ""
    Write-Host "      → $rid  →  $targetDir" -ForegroundColor DarkCyan

    if (Test-Path $targetDir) { Remove-Item $targetDir -Recurse -Force }
    New-Item -ItemType Directory -Path $targetDir -Force | Out-Null

    dotnet publish $serverProj `
        --configuration Release `
        --output $targetDir `
        --runtime $rid `
        --self-contained true `
        /p:PublishReadyToRun=true `
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

    Write-Ok "$rid complete → $targetDir"
}

# ── Step 3: Bundle installer ──────────────────────────────────────────────────
Write-Step "3/3" "Bundling installers..."

foreach ($t in $targets) {
    $targetDir = Join-Path $root $OutDir $t.OS

    if ($t.OS -eq "windows") {
        $src = Join-Path $root "install\windows\install.ps1"
        $dst = Join-Path $targetDir "install.ps1"
        Copy-Item $src $dst -Force
        Write-Ok "Windows: install.ps1  →  $dst"
    } else {
        $src = Join-Path $root "install\linux\install.sh"
        $dst = Join-Path $targetDir "install.sh"
        Copy-Item $src $dst -Force
        # Ensure Unix line endings AND no UTF-8 BOM. A BOM makes the kernel
        # fail to parse the shebang ("#!/usr/bin/env: not found") and the
        # script falls back to sh, breaking `set -o pipefail`.
        $content   = [System.IO.File]::ReadAllText($dst) -replace "`r`n", "`n"
        $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
        [System.IO.File]::WriteAllText($dst, $content, $utf8NoBom)
        Write-Ok "Linux:   install.sh   →  $dst"
    }
}

# ── Done ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  ═══════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host "    Release ready!" -ForegroundColor Green
Write-Host "  ═══════════════════════════════════════════" -ForegroundColor DarkCyan
Write-Host ""

foreach ($t in $targets) {
    $targetDir = Join-Path $root $OutDir $t.OS
    $rid = "$($t.OS)-$($t.Arch)"
    Write-Host "  $rid" -ForegroundColor Cyan
    Write-Host "    $targetDir" -ForegroundColor Gray
    if ($t.OS -eq "windows") {
        Write-Host "    Install: (Admin PS) .\install.ps1" -ForegroundColor DarkGray
    } else {
        Write-Host "    Install: sudo bash install.sh" -ForegroundColor DarkGray
    }
    Write-Host ""
}
