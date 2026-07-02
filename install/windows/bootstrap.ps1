#Requires -Version 7
#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Ameto — one-line Windows installer.
    Downloads the latest win-x64 release and runs the bundled service installer.

.EXAMPLE
    # In an elevated PowerShell 7 window:
    irm https://raw.githubusercontent.com/AMeto-Observability/AMeto-Observability/main/install/windows/bootstrap.ps1 | iex

.EXAMPLE
    # With custom options (download, then run):
    $b = irm https://raw.githubusercontent.com/AMeto-Observability/AMeto-Observability/main/install/windows/bootstrap.ps1
    & ([scriptblock]::Create($b)) -HttpPort 8080
#>
param(
    [string]$Version,                     # e.g. v0.1.0; empty = latest
    [int]   $HttpPort      = 5341,
    [string]$InstallDir    = "C:\Program Files\Ameto",
    [string]$DataDirectory = "C:\ProgramData\Ameto\data"
)

$ErrorActionPreference = 'Stop'
$repo = 'AMeto-Observability/AMeto-Observability'

if (-not $Version) {
    Write-Host ">> Resolving latest release ..." -ForegroundColor Cyan
    $Version = (Invoke-RestMethod "https://api.github.com/repos/$repo/releases/latest").tag_name
}
if (-not $Version) { throw "Could not resolve a release version." }

$asset = "ameto-$Version-win-x64.zip"
$url   = "https://github.com/$repo/releases/download/$Version/$asset"
$tmp   = Join-Path $env:TEMP ("ameto-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $tmp | Out-Null

try {
    Write-Host ">> Downloading $asset ..." -ForegroundColor Cyan
    Invoke-WebRequest -Uri $url -OutFile (Join-Path $tmp $asset)
    Expand-Archive -Path (Join-Path $tmp $asset) -DestinationPath $tmp -Force

    Write-Host ">> Running installer ..." -ForegroundColor Cyan
    & (Join-Path $tmp 'install.ps1') `
        -BinaryPath    (Join-Path $tmp 'Ameto.Server.exe') `
        -InstallDir    $InstallDir `
        -DataDirectory $DataDirectory `
        -HttpPort      $HttpPort
}
finally {
    Remove-Item $tmp -Recurse -Force -ErrorAction SilentlyContinue
}
