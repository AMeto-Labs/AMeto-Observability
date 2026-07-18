; ─────────────────────────────────────────────────────────────────────────────
;  Ameto — Windows installer (Inno Setup)
;
;  Builds a native setup.exe that deploys the self-contained Ameto server as a
;  Windows Service. One script drives both architectures; the caller picks the
;  target via /DArch and points /DSourceDir at the matching `dotnet publish`
;  output:
;
;    ISCC /DArch=x64 /DAppVersion=1.0.1 /DSourceDir=..\..\out\x64 ameto.iss
;    ISCC /DArch=x86 /DAppVersion=1.0.1 /DSourceDir=..\..\out\x86 ameto.iss
;
;  Or just run install\windows\build-installer.ps1 which publishes + compiles both.
; ─────────────────────────────────────────────────────────────────────────────

#ifndef AppVersion
  #define AppVersion "1.0.1"
#endif

#ifndef Arch
  #define Arch "x64"
#endif

; Publish output for {#Arch}; relative to this .iss file (install\windows).
#ifndef SourceDir
  #define SourceDir "..\..\out\" + Arch
#endif

; VersionInfoVersion must be a plain numeric x.x.x.x — strip a leading "v" so a
; git tag like v0.1.0 still compiles (AppVersion keeps the display form).
#if Copy(AppVersion, 1, 1) == "v"
  #define VersionInfo Copy(AppVersion, 2, Len(AppVersion) - 1)
#else
  #define VersionInfo AppVersion
#endif

#define AppName        "Ameto"
#define AppPublisher   "AMeto Observability"
#define AppURL         "https://github.com/AMeto-Observability/AMeto-Observability"
#define ServiceName    "Ameto"
#define ServiceDisplay "Ameto Server"
#define ExeName        "Ameto.Server.exe"

[Setup]
; A stable AppId keeps upgrades/uninstall pointing at the same install across
; versions. Do NOT change it once shipped. Same product for x86/x64 (only one
; can be installed at a time on a given machine).
AppId={{7B2A5F4C-9E1D-4A6B-8C3E-2F0A1B7D4E90}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
VersionInfoVersion={#VersionInfo}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
; Always show the install-location page. On an upgrade Inno pre-fills it with
; the EXISTING install directory (UsePreviousAppDir default), so the user sees
; where Ameto lives instead of the page being silently skipped.
DisableDirPage=no
UninstallDisplayName={#ServiceDisplay} {#AppVersion} ({#Arch})
UninstallDisplayIcon={app}\ameto.ico
OutputDir=Output
OutputBaseFilename=ameto-{#AppVersion}-setup-{#Arch}
SetupIconFile=assets\ameto.ico
WizardStyle=modern
; Branding: logo on white (the modern wizard's page background) — top-right on
; every page, and the large image on the Welcome/Finished pages.
WizardSmallImageFile=assets\wizard-small.png
WizardImageFile=assets\wizard-large.png
Compression=lzma2/max
SolidCompression=yes
; Program Files + service registration both require elevation.
PrivilegesRequired=admin
; .NET 10 supports Windows 10 1607+ / Server 2016+.
MinVersion=10.0.14393

#if Arch == "x64"
; 64-bit build: refuse to run on 32-bit Windows and install into the real
; Program Files (not the WOW64 x86 folder). "x64compatible" also covers ARM64
; Windows running the x64 binary under emulation.
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
#endif
; The x86 build sets neither, so it installs into Program Files (x86) and runs
; on both 32- and 64-bit Windows.

[Languages]
Name: "en"; MessagesFile: "compiler:Default.isl"

[Files]
; Everything from the publish output EXCEPT config.yml — the user's config is
; generated/preserved in code so an upgrade never clobbers a customised port.
Source: "{#SourceDir}\*"; DestDir: "{app}"; Excludes: "config.yml"; \
    Flags: recursesubdirs createallsubdirs ignoreversion
; Branding icon kept beside the app — referenced by UninstallDisplayIcon so the
; logo shows in Apps & features / Programs and Features.
Source: "assets\ameto.ico"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
; Internet shortcut to the local web UI (port is chosen on the wizard page).
Name: "{group}\Ameto Web UI"; Filename: "http://localhost:{code:GetPort}"
Name: "{group}\Ameto data folder"; Filename: "{code:GetDataDir}"
Name: "{group}\Uninstall Ameto"; Filename: "{uninstallexe}"

[Run]
; Offer to open the UI once the service is up.
Filename: "http://localhost:{code:GetPort}"; Description: "Open Ameto in your browser"; \
    Flags: postinstall shellexec skipifsilent nowait

[Code]
const
  DataSubDir = 'Ameto\data';   // relative to {commonappdata} = C:\ProgramData
  // Inno's own uninstall key for our AppId — used to detect an existing install
  // and locate its config.yml before the wizard shows.
  UninstKey  = 'Software\Microsoft\Windows\CurrentVersion\Uninstall\{7B2A5F4C-9E1D-4A6B-8C3E-2F0A1B7D4E90}_is1';
  // Newline for the generated config.yml. Kept global because Inno Pascal Script
  // has no local const sections; a leading #13#10 on a code line would also be
  // misread by the ISPP preprocessor as a directive, so we reference NL instead.
  NL = #13#10;

var
  PortPage:      TInputQueryWizardPage;
  DataDirPage:   TInputDirWizardPage;
  UninstDataDir: String;   // resolved in InitializeUninstall for the uninstall prompt
  PrevInstallDir: String;  // '' on a fresh install
  PrevPort:       String;  // port parsed from the existing config.yml ('' if unknown)
  PrevDataDir:    String;  // data dir of the existing install ('' if unknown)

function IsValidPort(const S: String): Boolean;
var
  N, I: Integer;
begin
  Result := False;
  if (Length(S) = 0) or (Length(S) > 5) then Exit;
  for I := 1 to Length(S) do
    if (S[I] < '0') or (S[I] > '9') then Exit;
  N := StrToIntDef(S, -1);
  Result := (N >= 1) and (N <= 65535);
end;

{ ── Existing-install detection (upgrade path) ─────────────────────────────── }

{ Reads `Key: value` from a YAML file (first match, comments stripped).
  Good enough for the flat scalars the installer itself writes. }
function TryReadYamlValue(const Path, Key: String; var Value: String): Boolean;
var
  Lines: TArrayOfString;
  I, P: Integer;
  L: String;
begin
  Result := False;
  if not LoadStringsFromFile(Path, Lines) then Exit;
  for I := 0 to GetArrayLength(Lines) - 1 do
  begin
    L := Trim(Lines[I]);
    if Pos(Key + ':', L) = 1 then
    begin
      L := Trim(Copy(L, Length(Key) + 2, Length(L)));
      P := Pos('#', L);
      if P > 0 then L := Trim(Copy(L, 1, P - 1));
      Value  := L;
      Result := L <> '';
      Exit;
    end;
  end;
end;

{ Replaces the value of `Key:` in-place, preserving indentation and the rest of
  the file — an upgrade must never clobber a hand-edited config. }
procedure UpdateYamlValue(const Path, Key, Value: String);
var
  Lines: TArrayOfString;
  I, P: Integer;
begin
  if not LoadStringsFromFile(Path, Lines) then Exit;
  for I := 0 to GetArrayLength(Lines) - 1 do
    if Pos(Key + ':', Trim(Lines[I])) = 1 then
    begin
      P := Pos(Key, Lines[I]);
      Lines[I] := Copy(Lines[I], 1, P - 1) + Key + ': ' + Value;
      SaveStringsToFile(Path, Lines, False);
      Exit;
    end;
end;

procedure DetectPreviousInstall;
var
  S: String;
begin
  PrevInstallDir := '';
  PrevPort       := '';
  PrevDataDir    := '';

  if RegQueryStringValue(HKLM, UninstKey, 'InstallLocation', S) and (S <> '') then
    PrevInstallDir := RemoveBackslashUnlessRoot(S);

  { Data dir marker written by every install (also covers custom locations). }
  RegQueryStringValue(HKLM, 'Software\Ameto', 'DataDirectory', PrevDataDir);

  if (PrevInstallDir <> '') and FileExists(PrevInstallDir + '\config.yml') then
  begin
    if TryReadYamlValue(PrevInstallDir + '\config.yml', 'HttpPort', S) and IsValidPort(S) then
      PrevPort := S;
    if (PrevDataDir = '') and TryReadYamlValue(PrevInstallDir + '\config.yml', 'DataDirectory', S) then
      PrevDataDir := S;
  end;
end;

{ ── Wizard: ask for the HTTP port + data directory ────────────────────────── }
procedure InitializeWizard;
var
  PortHint: String;
begin
  DetectPreviousInstall;

  PortHint := 'The web UI and the ingestion / OTLP API are served over this single HTTP port. ';
  if PrevPort <> '' then
    PortHint := PortHint + 'Pre-filled with the CURRENT setting of the installed server — ' +
                           'change it here and the upgrade updates config.yml for you.'
  else
    PortHint := PortHint + 'Leave the default unless it clashes with another service, then click Next.';

  PortPage := CreateInputQueryPage(wpSelectDir,
    'Server configuration',
    'On which port should Ameto listen?',
    PortHint);
  PortPage.Add('HTTP port:', False);
  if PrevPort <> '' then
    PortPage.Values[0] := PrevPort
  else
    PortPage.Values[0] := '5341';

  { Where to store logs/metrics/traces + Ameto.db. Kept separate from the program
    files (Program Files) so upgrades never touch it. On an upgrade the page is
    pre-filled with the data folder the installed server already uses. }
  DataDirPage := CreateInputDirPage(PortPage.ID,
    'Data location',
    'Where should Ameto store its data?',
    'Logs, metrics, traces and the SQLite database (Ameto.db) are written to this ' +
    'folder. It is preserved across upgrades; the uninstaller asks before deleting it.',
    False, '');
  DataDirPage.Add('Data folder:');
  if PrevDataDir <> '' then
    DataDirPage.Values[0] := PrevDataDir
  else
    DataDirPage.Values[0] := ExpandConstant('{commonappdata}\' + DataSubDir);
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;
  if (CurPageID = PortPage.ID) and (not IsValidPort(Trim(PortPage.Values[0]))) then
  begin
    MsgBox('Please enter a valid TCP port (1-65535).', mbError, MB_OK);
    Result := False;
  end;
end;

// Exposed to [Icons]/[Run] as code:GetPort / code:GetDataDir.
function GetPort(Param: String): String;
begin
  Result := Trim(PortPage.Values[0]);
  if not IsValidPort(Result) then Result := '5341';
end;

function GetDataDir(Param: String): String;
begin
  { The page default is set in InitializeWizard, so this is valid even in a
    /SILENT install where the page never shows. }
  Result := Trim(DataDirPage.Values[0]);
  if Result = '' then
    Result := ExpandConstant('{commonappdata}\' + DataSubDir);
end;

{ ── sc.exe / net.exe helpers ──────────────────────────────────────────────── }
function RunHidden(const Exe, Params: String): Integer;
begin
  if not Exec(Exe, Params, '', SW_HIDE, ewWaitUntilTerminated, Result) then
    Result := -1;
end;

function ServiceExists: Boolean;
var
  Code: Integer;
begin
  { sc query returns 1060 (ERROR_SERVICE_DOES_NOT_EXIST) when absent, 0 when present. }
  Code := RunHidden(ExpandConstant('{sys}\sc.exe'), 'query {#ServiceName}');
  Result := (Code = 0);
end;

procedure StopService;
begin
  { net stop blocks until the service has actually stopped, so the .exe is
    unlocked before we overwrite it. Ignore failures (already stopped/absent). }
  RunHidden(ExpandConstant('{sys}\net.exe'), 'stop {#ServiceName}');
  Sleep(1500);
end;

{ ── Free the binary before files are copied (upgrade path) ────────────────── }
function PrepareToInstall(var NeedsRestart: Boolean): String;
begin
  Result := '';
  if ServiceExists then
    StopService;
end;

{ ── Generate config.yml only when absent (preserve user edits on upgrade) ──── }
procedure WriteDefaultConfig(const Path, Port, DataDir: String);
var
  Yaml, D: String;
begin
  { YAML plain scalars keep backslashes literal, but forward slashes read
    cleaner and are what the Linux/PS installers write. }
  D := DataDir;
  StringChangeEx(D, '\', '/', True);
  Yaml :=
    'Ameto:' + NL +
    '  NodeId: 0' + NL +
    '  DataDirectory: ' + D + NL +
    '  HttpPort: ' + Port + NL +
    NL +
    '  SslCertPath: ""' + NL +
    '  SslCertPassword: ""' + NL +
    NL +
    '  RamTargetPercent: 75' + NL +
    NL +
    '  HotTier:' + NL +
    '    MaxSizeBytes: 67108864   # 64 MB' + NL +
    '    MaxAge: "00:05:00"' + NL +
    '    FlushConcurrency: 0      # 0 = auto (cores/2, 2-8). Lower = less RAM, higher = more throughput' + NL +
    NL +
    '  Indexing:' + NL +
    '    MaxPropertyFlattenDepth: 5' + NL +
    NL +
    '  Retention:' + NL +
    '    VerboseDays: 3' + NL +
    '    DebugDays: 3' + NL +
    '    InformationDays: 90' + NL +
    '    WarningDays: 90' + NL +
    '    ErrorDays: 90' + NL +
    '    FatalDays: 90' + NL +
    NL +
    '  Replication:' + NL +
    '    Enabled: false' + NL +
    '    SeedNodes: []' + NL;
  SaveStringToFile(Path, Yaml, False);
end;

{ ── Register + start the service after files are in place ─────────────────── }
procedure CurStepChanged(CurStep: TSetupStep);
var
  DataDir, ConfigPath, ExePath, Port, BinPath, D: String;
begin
  if CurStep <> ssPostInstall then Exit;

  Port       := GetPort('');
  DataDir    := GetDataDir('');
  ExePath    := ExpandConstant('{app}\{#ExeName}');
  ConfigPath := ExpandConstant('{app}\config.yml');

  { Create the chosen data directory (default or user-picked). }
  ForceDirectories(DataDir);

  { Remember it so the uninstaller can locate a non-default path. Written via code
    (not [Registry]) so InitializeUninstall reads it back from the same view. }
  RegWriteStringValue(HKLM, 'Software\Ameto', 'DataDirectory', DataDir);

  { Grant the NetworkService account (the service identity, SID S-1-5-20 — using
    the SID keeps this locale-independent) modify rights on the data tree. }
  RunHidden(ExpandConstant('{sys}\icacls.exe'),
    '"' + DataDir + '" /grant *S-1-5-20:(OI)(CI)M /T /C /Q');

  { config.yml: full defaults on a fresh install. On an upgrade the file is kept
    and only the values the user actually changed on the wizard pages are patched
    in-place (indentation and every other hand-edited key survive). The guards
    against the DETECTED previous values also make a silent self-update
    (/VERYSILENT from Settings → Updates) a strict no-op for the config. }
  if not FileExists(ConfigPath) then
    WriteDefaultConfig(ConfigPath, Port, DataDir)
  else
  begin
    if (PrevPort <> '') and (Port <> PrevPort) then
      UpdateYamlValue(ConfigPath, 'HttpPort', Port);
    if (PrevDataDir <> '') and (CompareText(DataDir, PrevDataDir) <> 0) then
    begin
      { Point the server at the new folder; the old data stays where it was. }
      D := DataDir;
      StringChangeEx(D, '\', '/', True);
      UpdateYamlValue(ConfigPath, 'DataDirectory', D);
    end;
  end;

  { Create the service if it doesn't exist yet. The binPath is quoted INSIDE the
    stored ImagePath (\" ... \") so a path with spaces isn't vulnerable to the
    unquoted-service-path hijack.
    The service runs as LocalSystem (sc default): the in-app self-update has the
    SERVICE launch the downloaded installer, and only LocalSystem can start an
    admin-manifested setup.exe silently — NetworkService gets ERROR_ELEVATION_
    REQUIRED and a service can never show a UAC prompt. }
  if not ServiceExists then
  begin
    BinPath := '"\"' + ExePath + '\""';
    RunHidden(ExpandConstant('{sys}\sc.exe'),
      'create {#ServiceName} binPath= ' + BinPath +
      ' start= auto DisplayName= "{#ServiceDisplay}"');
    RunHidden(ExpandConstant('{sys}\sc.exe'),
      'description {#ServiceName} "High-performance self-hosted structured log server (Ameto)"');
    { Auto-restart on crash: 5 s delay, reset the failure counter after a day. }
    RunHidden(ExpandConstant('{sys}\sc.exe'),
      'failure {#ServiceName} reset= 86400 actions= restart/5000/restart/5000/restart/5000');
  end
  else
    { Migrate older installs that ran as NetworkService (see comment above). }
    RunHidden(ExpandConstant('{sys}\sc.exe'), 'config {#ServiceName} obj= LocalSystem');

  { Trim RAM after ingest bursts: with ConserveMemory the GC compacts and returns
    the ballooned heap to the OS instead of keeping it committed until the next
    gen2. Same knob the Docker image sets via ENV; the SCM passes this
    multi-string value to the service process as environment variables. Written
    AFTER service creation (sc create must own the key) and on every upgrade so
    existing installs pick it up too. }
  RegWriteMultiStringValue(HKLM, 'SYSTEM\CurrentControlSet\Services\{#ServiceName}',
    'Environment', 'DOTNET_GCConserveMemory=5');

  { Start (or restart, after an upgrade stop) the service. }
  RunHidden(ExpandConstant('{sys}\net.exe'), 'start {#ServiceName}');
end;

{ Resolve the (possibly custom) data dir up-front, before any uninstall step. }
function InitializeUninstall(): Boolean;
begin
  Result := True;
  if not RegQueryStringValue(HKLM, 'Software\Ameto', 'DataDirectory', UninstDataDir) then
    UninstDataDir := ExpandConstant('{commonappdata}\' + DataSubDir);
end;

{ ── Uninstall: stop + remove the service, optionally the data ─────────────── }
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  DataDir: String;
begin
  if CurUninstallStep = usUninstall then
  begin
    RunHidden(ExpandConstant('{sys}\net.exe'), 'stop {#ServiceName}');
    Sleep(1000);
    RunHidden(ExpandConstant('{sys}\sc.exe'), 'delete {#ServiceName}');
  end
  else if CurUninstallStep = usPostUninstall then
  begin
    DataDir := UninstDataDir;
    if DirExists(DataDir) then
      if MsgBox('Remove the Ameto data directory too?' + #13#10 + #13#10 +
                DataDir + #13#10 + #13#10 +
                'This permanently deletes all stored logs, metrics and traces.',
                mbConfirmation, MB_YESNO or MB_DEFBUTTON2) = IDYES then
        DelTree(DataDir, True, True, True);
    { Remove our registry marker. }
    RegDeleteKeyIncludingSubkeys(HKLM, 'Software\Ameto');
  end;
end;
