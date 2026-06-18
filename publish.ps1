#Requires -Version 7
<#
.SYNOPSIS
    Build Angular client + .NET server and produce a self-contained publish folder.

.PARAMETER Output
    Destination directory for the published artifacts (default: ./publish)

.PARAMETER Configuration
    .NET build configuration (default: Release)

.PARAMETER NoRestart
    Don't auto-restart the server after publishing.

.EXAMPLE
    .\publish.ps1
    .\publish.ps1 -Output dist -Configuration Debug
    .\publish.ps1 -NoRestart
#>
param(
    [string]$Output        = "publish",
    [string]$Configuration = "Release",
    [string]$Runtime,
    [switch]$NoRestart,
    [switch]$Restart
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

function Resolve-DefaultRuntimeIdentifier {
    $arch = switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
        'X64'   { 'x64' }
        'Arm64' { 'arm64' }
        default { throw "Unsupported OS architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)" }
    }

    if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)) {
        return "win-$arch"
    }
    if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)) {
        return "linux-$arch"
    }
    if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::OSX)) {
        return "osx-$arch"
    }

    throw "Unsupported OS platform for self-contained publish."
}

$runtimeId = if ($Runtime) { $Runtime } else { Resolve-DefaultRuntimeIdentifier }

# ── Interactive mode prompt ──────────────────────────────────────────────────
# If neither switch was supplied, ask the user what to do.
if (-not $NoRestart -and -not $Restart) {
    Write-Host ""
    Write-Host "  Select publish mode:" -ForegroundColor Cyan
    Write-Host "    [1] Only build" -ForegroundColor Gray
    Write-Host "    [2] Build + restart server" -ForegroundColor Gray
    Write-Host ""
    do {
        $choice = Read-Host "  Choice (1/2)"
    } while ($choice -ne '1' -and $choice -ne '2')

    if ($choice -eq '1') { $NoRestart = $true } else { $Restart = $true }
}

$clientDir  = Join-Path $root "client"
$serverProj = Join-Path $root "src\Ameto.Server\Ameto.Server.csproj"
$wwwroot     = Join-Path $root "src\Ameto.Server\wwwroot"
$outputDir   = Join-Path $root $Output
$serverDll   = Join-Path $outputDir "Ameto.Server.dll"
$serverApp   = Join-Path $outputDir $(if ($runtimeId.StartsWith('win-')) { 'Ameto.Server.exe' } else { 'Ameto.Server' })

# Resolve full output dir path early so process matching works before publish.
$outputDirFull = [System.IO.Path]::GetFullPath($outputDir)
$serverDllFull = [System.IO.Path]::GetFullPath($serverDll)
$serverAppFull = [System.IO.Path]::GetFullPath($serverApp)

function Stop-AmetoServer {
    # Match by command line so we only kill OUR server, not unrelated dotnet processes.
    # Get-CimInstance reads Win32_Process.CommandLine which includes the full args.
    $procs = Get-CimInstance Win32_Process -Filter "Name = 'dotnet.exe' OR Name = 'Ameto.Server.exe' OR Name = 'Ameto.Server'" -ErrorAction SilentlyContinue |
        Where-Object {
            $_.CommandLine -and (
                $_.CommandLine -like "*Ameto.Server.dll*" -or
                $_.CommandLine -like "*Ameto.Server.exe*" -or
                $_.CommandLine -like "*Ameto.Server*"
            )
        }

    if (-not $procs) { return $false }

    foreach ($p in $procs) {
        Write-Host "        Stopping PID $($p.ProcessId)  ($($p.Name))" -ForegroundColor DarkGray
        try {
            Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
        }
        catch {
            Write-Warning "        Failed to stop PID $($p.ProcessId): $($_.Exception.Message)"
        }
    }

    # Give the OS a moment to release file locks on the publish dir.
    Start-Sleep -Milliseconds 800
    return $true
}

function Start-AmetoServer {
    if (-not (Test-Path $serverAppFull)) {
        Write-Warning "Published app not found at $serverAppFull — skipping start."
        return
    }
    # Launch detached so the publish script can exit while the server keeps running.
    $proc = Start-Process -FilePath $serverAppFull `
        -WorkingDirectory $outputDirFull `
        -PassThru
    Write-Host "        Started PID $($proc.Id)  → $serverAppFull" -ForegroundColor Green
}

Write-Host ""
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Ameto  ·  publish" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# ── 1. Build Angular ──────────────────────────────────────────────────────────
Write-Host "[ 1/4 ] Building Angular client..." -ForegroundColor Yellow
Push-Location $clientDir
try {
    npm install --prefer-offline --no-audit --no-fund 2>&1 | Out-Null
    ng build --configuration production --output-path $wwwroot
    if ($LASTEXITCODE -ne 0) { throw "ng build failed" }

    # Angular 17+ outputs browser assets to a 'browser/' subdirectory.
    # Flatten it so ASP.NET Core static file middleware finds index.html at wwwroot root.
    $browserSub = Join-Path $wwwroot "browser"
    if (Test-Path $browserSub) {
        Get-ChildItem $browserSub | Move-Item -Destination $wwwroot -Force
        Remove-Item $browserSub -Recurse -Force
    }
}
finally {
    Pop-Location
}
Write-Host "        Angular build complete → $wwwroot" -ForegroundColor Green

# ── 2. Stop running server (so we can overwrite the publish dir) ──────────────
Write-Host ""
Write-Host "[ 2/4 ] Stopping running Ameto.Server (if any)..." -ForegroundColor Yellow
$wasRunning = Stop-AmetoServer
if (-not $wasRunning) {
    Write-Host "        No running server found." -ForegroundColor DarkGray
}

# ── 3. Publish .NET Server ────────────────────────────────────────────────────
Write-Host ""
Write-Host "[ 3/4 ] Publishing .NET Server ($Configuration, $runtimeId, self-contained)..." -ForegroundColor Yellow
dotnet publish $serverProj `
    --configuration $Configuration `
    --output $outputDir `
    --runtime $runtimeId `
    --self-contained true `
    /p:DebugType=None `
    /p:DebugSymbols=false `
    /p:GenerateDocumentationFile=false
if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed" }

# Strip residual symbols / XML docs / locale folders that some packages still emit.
Get-ChildItem -Path $outputDir -Recurse -Include *.pdb, *.xml -ErrorAction SilentlyContinue |
    Remove-Item -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path $outputDir -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '^[a-z]{2}(-[A-Z]{2})?$' } |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item (Join-Path $outputDir 'web.config') -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path $outputDir -Filter 'appsettings*.json' -ErrorAction SilentlyContinue |
    Remove-Item -Force -ErrorAction SilentlyContinue
Remove-Item (Join-Path $outputDir 'dotnet-tools.json') -Force -ErrorAction SilentlyContinue

Write-Host "        .NET publish complete → $outputDir" -ForegroundColor Green

# ── 4. Restart server ─────────────────────────────────────────────────────────
Write-Host ""
if ($NoRestart) {
    Write-Host "[ 4/4 ] Skipping server start (-NoRestart)." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Run:  $serverApp" -ForegroundColor Cyan
}
else {
    Write-Host "[ 4/4 ] Starting Ameto.Server..." -ForegroundColor Yellow
    Start-AmetoServer
}

Write-Host ""
Write-Host "  UI:   http://localhost:5341" -ForegroundColor Cyan
Write-Host ""
