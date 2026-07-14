#Requires -Version 7
<#
.SYNOPSIS
    Build the Ameto Windows installer(s) with Inno Setup.

    Publishes the self-contained server for each requested architecture, builds
    the Angular UI into wwwroot, then compiles install\windows\ameto.iss into
    ameto-<version>-setup-<arch>.exe under install\windows\Output.

.PARAMETER Version
    Product / installer version (e.g. 1.0.1). Default: 1.0.1.

.PARAMETER Arch
    Architectures to build: x64, x86, or both. Default: both.

.PARAMETER SkipClient
    Reuse the existing src\Ameto.Server\wwwroot instead of rebuilding the Angular
    SPA (faster when iterating on the installer alone).

.PARAMETER Iscc
    Full path to ISCC.exe if it is not auto-detected.

.EXAMPLE
    .\build-installer.ps1
    .\build-installer.ps1 -Version 1.2.0 -Arch x64
    .\build-installer.ps1 -SkipClient
#>
param(
    [string]  $Version = '1.0.1',
    [ValidateSet('x64', 'x86', 'both')]
    [string]  $Arch    = 'both',
    [switch]  $SkipClient,
    [string]  $Iscc
)

$ErrorActionPreference = 'Stop'

$ScriptDir = $PSScriptRoot
$RepoRoot  = (Resolve-Path (Join-Path $ScriptDir '..\..')).Path
$Project   = Join-Path $RepoRoot 'src\Ameto.Server\Ameto.Server.csproj'
$Wwwroot   = Join-Path $RepoRoot 'src\Ameto.Server\wwwroot'
$OutRoot   = Join-Path $RepoRoot 'out'
$Iss       = Join-Path $ScriptDir 'ameto.iss'

function Write-Step($m) { Write-Host "`n==> $m" -ForegroundColor Cyan }

# ── Locate ISCC.exe ───────────────────────────────────────────────────────────
if (-not $Iscc) {
    $candidates = @(
        (Get-Command ISCC.exe -ErrorAction SilentlyContinue).Source
        "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe"
        "$env:ProgramFiles\Inno Setup 6\ISCC.exe"
        # winget installs Inno Setup per-user under LOCALAPPDATA by default.
        "$env:LOCALAPPDATA\Programs\Inno Setup 6\ISCC.exe"
    ) | Where-Object { $_ -and (Test-Path $_) }
    $Iscc = $candidates | Select-Object -First 1
}
if (-not $Iscc -or -not (Test-Path $Iscc)) {
    throw @"
ISCC.exe (Inno Setup 6 compiler) not found.
Install it, then re-run:
    winget install --id JRSoftware.InnoSetup --source winget
  or
    choco install innosetup
Or pass the path explicitly:  -Iscc 'C:\Path\To\ISCC.exe'
"@
}
Write-Host "Inno Setup compiler: $Iscc" -ForegroundColor DarkGray

$targets = if ($Arch -eq 'both') { @('x64', 'x86') } else { @($Arch) }

# ── Build the Angular SPA once, into the server's wwwroot ─────────────────────
if (-not $SkipClient) {
    Write-Step 'Building Angular client'
    Push-Location (Join-Path $RepoRoot 'client')
    try {
        npm ci --prefer-offline --no-audit --no-fund
        npx ng build --configuration production --output-path dist
        # Angular 17+ emits to dist\browser — flatten into wwwroot.
        if (Test-Path $Wwwroot) { Remove-Item $Wwwroot -Recurse -Force }
        New-Item -ItemType Directory -Path $Wwwroot | Out-Null
        $src = if (Test-Path 'dist\browser') { 'dist\browser\*' } else { 'dist\*' }
        Copy-Item $src -Destination $Wwwroot -Recurse -Force
    }
    finally { Pop-Location }
}
elseif (-not (Test-Path (Join-Path $Wwwroot 'index.html'))) {
    throw "-SkipClient set but $Wwwroot has no index.html — build the client first."
}

# ── Publish + compile per architecture ───────────────────────────────────────
$results = @()
foreach ($a in $targets) {
    $rid    = "win-$a"
    $outDir = Join-Path $OutRoot $a

    Write-Step "Publishing $rid (self-contained, single-file)"
    if (Test-Path $outDir) { Remove-Item $outDir -Recurse -Force }
    dotnet publish $Project -c Release -r $rid --self-contained true `
        -p:PublishSingleFile=true -p:EnableCompressionInSingleFile=true `
        -p:DebugType=None -p:DebugSymbols=false `
        -o $outDir
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed for $rid" }

    Write-Step "Compiling installer for $a"
    & $Iscc "/DAppVersion=$Version" "/DArch=$a" "/DSourceDir=$outDir" $Iss
    if ($LASTEXITCODE -ne 0) { throw "ISCC failed for $a" }

    $exe = Join-Path $ScriptDir "Output\ameto-$Version-setup-$a.exe"
    $results += [pscustomobject]@{ Arch = $a; Installer = $exe }
}

Write-Step 'Done'
foreach ($r in $results) {
    $size = '{0:N1} MB' -f ((Get-Item $r.Installer).Length / 1MB)
    Write-Host ("  {0,-4}  {1}  ({2})" -f $r.Arch, $r.Installer, $size) -ForegroundColor Green
}
