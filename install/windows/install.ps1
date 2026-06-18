#Requires -RunAsAdministrator
#Requires -Version 7
<#
.SYNOPSIS
    Install or uninstall Ameto as a Windows Service.

.PARAMETER BinaryPath
    Path to the Ameto.Server.exe binary.
    Defaults to .\Ameto.Server.exe (same directory as this script or its parent).

.PARAMETER InstallDir
    Directory where the service files will be placed.
    Defaults to C:\Program Files\Ameto.

.PARAMETER DataDirectory
    Directory for log data storage.
    Defaults to C:\ProgramData\Ameto\data.

.PARAMETER HttpPort
    HTTP port for the server to listen on. Default: 5341.

.PARAMETER ServiceName
    Windows Service name. Default: Ameto.

.PARAMETER Uninstall
    Remove the service and optionally its data.

.EXAMPLE
    .\install.ps1
    .\install.ps1 -BinaryPath .\Ameto.Server.exe -HttpPort 5341
    .\install.ps1 -Uninstall
#>
param(
    [string]$BinaryPath,
    [string]$InstallDir    = "C:\Program Files\Ameto",
    [string]$DataDirectory = "C:\ProgramData\Ameto\data",
    [int]   $HttpPort      = 5341,
    [string]$ServiceName   = "Ameto",
    [switch]$Uninstall
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$msg) { Write-Host "  >> $msg" -ForegroundColor Cyan }
function Write-Ok([string]$msg)   { Write-Host "     $msg" -ForegroundColor Green }
function Write-Warn([string]$msg) { Write-Host "     $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Ameto  ·  Windows Service Installer" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# ── Uninstall ─────────────────────────────────────────────────────────────────
if ($Uninstall) {
    Write-Step "Stopping service '$ServiceName'..."
    $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($svc) {
        if ($svc.Status -ne 'Stopped') {
            Stop-Service -Name $ServiceName -Force
            $svc.WaitForStatus('Stopped', [TimeSpan]::FromSeconds(15))
        }
        Write-Step "Removing service '$ServiceName'..."
        sc.exe delete $ServiceName | Out-Null
        Write-Ok "Service removed."
    }
    else {
        Write-Warn "Service '$ServiceName' not found — nothing to remove."
    }

    $removeData = Read-Host "  Remove data directory '$DataDirectory'? [y/N]"
    if ($removeData -eq 'y' -or $removeData -eq 'Y') {
        if (Test-Path $DataDirectory) {
            Remove-Item $DataDirectory -Recurse -Force
            Write-Ok "Data directory removed."
        }
    }

    Write-Host ""
    Write-Ok "Uninstall complete."
    Write-Host ""
    exit 0
}

# ── Locate binary ─────────────────────────────────────────────────────────────
if (-not $BinaryPath) {
    # Look next to this script, then one level up (e.g., if running from install\windows\)
    $candidates = @(
        Join-Path $PSScriptRoot "Ameto.Server.exe"
        Join-Path (Split-Path $PSScriptRoot -Parent) "Ameto.Server.exe"
        Join-Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) "publish\Ameto.Server.exe"
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { $BinaryPath = $c; break }
    }
}

if (-not $BinaryPath -or -not (Test-Path $BinaryPath)) {
    Write-Host ""
    Write-Host "  ERROR: Ameto.Server.exe not found." -ForegroundColor Red
    Write-Host "  Specify the path explicitly:" -ForegroundColor Red
    Write-Host "    .\install.ps1 -BinaryPath <path>\Ameto.Server.exe" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}

$BinaryPath = [System.IO.Path]::GetFullPath($BinaryPath)
Write-Ok "Binary: $BinaryPath"

# ── Check for existing service ────────────────────────────────────────────────
$existingSvc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingSvc) {
    Write-Warn "Service '$ServiceName' already exists."
    $overwrite = Read-Host "  Update existing installation? [y/N]"
    if ($overwrite -ne 'y' -and $overwrite -ne 'Y') { exit 0 }

    Write-Step "Stopping existing service..."
    if ($existingSvc.Status -ne 'Stopped') {
        Stop-Service -Name $ServiceName -Force
        $existingSvc.WaitForStatus('Stopped', [TimeSpan]::FromSeconds(15))
    }
}

# ── Copy binary to install directory ─────────────────────────────────────────
Write-Step "Installing to $InstallDir ..."
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null

# Copy everything from the binary's directory (runtime, assets, config.yml, etc.)
$sourceDir = Split-Path $BinaryPath -Parent
Write-Ok "Copying from $sourceDir ..."
Copy-Item "$sourceDir\*" -Destination $InstallDir -Recurse -Force

# ── Create / ensure data directory ────────────────────────────────────────────
Write-Step "Creating data directory: $DataDirectory"
New-Item -ItemType Directory -Path $DataDirectory -Force | Out-Null

# ── Write config.yml ──────────────────────────────────────────────────────────
$configPath = Join-Path $InstallDir "config.yml"
if (-not (Test-Path $configPath)) {
    Write-Step "Writing default config.yml ..."
    @"
Ameto:
  NodeId: 0
  DataDirectory: $($DataDirectory -replace '\\', '/')
  HttpPort: $HttpPort

  SslCertPath: ""
  SslCertPassword: ""

  RamTargetPercent: 99

  HotTier:
    MaxSizeBytes: 268435456  # 256 MB
    MaxAge: "00:05:00"

  Indexing:
    MaxPropertyFlattenDepth: 5

  Retention:
    VerboseDays: 3
    DebugDays: 3
    InformationDays: 90
    WarningDays: 90
    ErrorDays: 90
    FatalDays: 90

  Replication:
    Enabled: false
    SeedNodes: []
"@ | Set-Content $configPath -Encoding UTF8
    Write-Ok "Config written to $configPath"
}
else {
    Write-Warn "config.yml already exists — skipping (edit manually if needed)."
}

# ── Register Windows Service ──────────────────────────────────────────────────
Write-Step "Registering Windows Service '$ServiceName' ..."
$exePath = Join-Path $InstallDir "Ameto.Server.exe"

$svcParams = @{
    Name           = $ServiceName
    BinaryPathName = "`"$exePath`""
    DisplayName    = "Ameto Server"
    Description    = "High-performance structured log server (Ameto)"
    StartupType    = "Automatic"
}

if ($existingSvc) {
    Set-Service @svcParams
}
else {
    New-Service @svcParams | Out-Null
}

# Allow the service to write to the data directory
$acl = Get-Acl $DataDirectory
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
    "NT AUTHORITY\NetworkService", "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow"
)
$acl.SetAccessRule($rule)
Set-Acl -Path $DataDirectory -AclObject $acl

# ── Start the service ─────────────────────────────────────────────────────────
Write-Step "Starting service '$ServiceName' ..."
Start-Service -Name $ServiceName
$svc = Get-Service -Name $ServiceName
$svc.WaitForStatus('Running', [TimeSpan]::FromSeconds(15))
Write-Ok "Service is running."

Write-Host ""
Write-Host "═══════════════════════════════════════════" -ForegroundColor Green
Write-Host "  Installation complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  UI:      http://localhost:$HttpPort" -ForegroundColor Cyan
Write-Host "  Login:   admin / 123123  (change immediately!)" -ForegroundColor Yellow
Write-Host "  Config:  $configPath" -ForegroundColor Cyan
Write-Host "  Data:    $DataDirectory" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Manage:" -ForegroundColor Gray
Write-Host "    Start:     Start-Service $ServiceName" -ForegroundColor Gray
Write-Host "    Stop:      Stop-Service  $ServiceName" -ForegroundColor Gray
Write-Host "    Uninstall: .\install.ps1 -Uninstall" -ForegroundColor Gray
Write-Host ""
