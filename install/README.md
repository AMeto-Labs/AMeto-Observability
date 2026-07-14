# Installing Ameto

Ameto ships as a **single self-contained binary** (no runtime to install) plus the
Angular UI baked into it. Pick one of: **Windows service**, **Linux systemd service**,
or **Docker**. The server listens on **`http://localhost:5341`** by default and, on first
start, creates an admin login **`admin` / `123123`** — change it immediately under
**Settings → Users**.

| Method | Folder | Best for |
|--------|--------|----------|
| Windows service | [`windows/`](windows/) | Windows hosts (GUI installer or script) |
| Linux systemd   | [`linux/`](linux/)   | Linux servers |
| Docker Compose  | [`docker/`](docker/) | Containers / any OS with Docker |

Full configuration reference: [`../docs/CONFIGURATION.md`](../docs/CONFIGURATION.md).

---

## Requirements

**Runtime (all methods):** none — the published binary is self-contained. A 64-bit
host with a few hundred MB of RAM is enough to start; give it more for higher ingestion.

**To build from source** (needed unless you already have a published binary / image):

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Node.js 22+ / npm 10+](https://nodejs.org/) — builds the Angular UI into `wwwroot`
- Docker method: Docker Engine 24+ with Compose v2 (BuildKit is on by default)
- Windows `.exe` installer only: [Inno Setup 6](https://jrsoftware.org/isdl.php) (`winget install JRSoftware.InnoSetup`)

Produce a published server folder (used by the Windows/Linux scripts):

```bash
# from the repo root — build the UI, then publish a self-contained server
cd client && npm ci && npx ng build --configuration production --output-path dist
# copy dist/browser/* into src/Ameto.Server/wwwroot, then:
cd .. && dotnet publish src/Ameto.Server -c Release -r <win-x64|linux-x64> \
    --self-contained true -p:PublishSingleFile=true -o publish
```

The output `publish/` folder contains `Ameto.Server[.exe]` + `wwwroot` — point the
installer scripts below at it.

---

## Windows

### Option A — GUI installer (`.exe`)

Build the installer (requires Inno Setup), then run it:

```powershell
cd install\windows
.\build-installer.ps1 -Version 1.0.0 -Arch x64      # builds UI + server + Output\ameto-1.0.0-setup-x64.exe
.\Output\ameto-1.0.0-setup-x64.exe                  # run the installer (elevates for the service)
```

Installs the **`Ameto`** Windows service to `C:\Program Files\Ameto`, data under
`C:\ProgramData\Ameto`, config at `C:\Program Files\Ameto\config.yml`.
`-SkipClient` reuses an already-built `wwwroot` (faster when only the backend changed).

### Option B — script (register a published binary as a service)

From an **elevated PowerShell 7**, against a `publish/` folder (see Requirements):

```powershell
cd install\windows
.\install.ps1 -BinaryPath ..\..\publish\Ameto.Server.exe -HttpPort 5341
# uninstall:
.\install.ps1 -Uninstall
```

**Manage:** `Get-Service Ameto` · `Restart-Service Ameto` · `Stop-Service Ameto` ·
logs go to the Windows Event Log / the service's working directory.

---

## Linux (systemd)

Against a published Linux binary (see Requirements). The installer creates an
`ameto` system user, installs to `/opt/ameto`, data in `/var/lib/ameto/data`, and
registers/starts a hardened systemd unit:

```bash
cd install/linux
sudo ./install.sh --binary /path/to/publish/Ameto.Server
# options: --port 5341  --data /var/lib/ameto/data  --install-dir /opt/ameto
# uninstall:
sudo ./install.sh --uninstall
```

**Manage:**

```bash
systemctl status ameto
journalctl -u ameto -f          # follow logs
systemctl restart ameto
```

Config lives at `/opt/ameto/config.yml` (preserved across re-installs).

---

## Docker

Uses [`docker/docker-compose.example.yml`](docker/docker-compose.example.yml), which
documents **every** configuration option as an environment variable.

```bash
# copy the example, adjust, and start (builds the image from source by default)
cp install/docker/docker-compose.example.yml docker-compose.yml
docker compose up -d --build
```

Or run the example directly from the repo:

```bash
docker compose -f install/docker/docker-compose.example.yml up -d --build
```

- Data persists in the `ameto-data` volume (log/metric/trace segments + `Ameto.db`).
- To consume a **pre-built** image instead of building, set `image:` to your registry
  tag (e.g. `ghcr.io/…/ameto:latest`) and delete the `build:` block — see the comments
  in the example file.
- `mem_limit` matters: .NET sizes its heap to it and `RamTargetPercent` is measured
  against the container, not the host.

**Manage:** `docker compose logs -f ameto` · `docker compose restart ameto` ·
`docker compose down` (keeps the volume).

---

## After install — send data

1. Open `http://localhost:5341`, log in as `admin` / `123123`, **change the password**.
2. Create an API key under **Settings → API Keys**.
3. Point your logs/traces/metrics at the server:
   - **Serilog:** `WriteTo.Ameto("http://localhost:5341", apiKey: "…")` (or any Seq sink — the CLEF endpoint is Seq-compatible).
   - **OpenTelemetry:** OTLP exporter → `http://localhost:5341/otlp/v1/{logs,traces,metrics}`.

See [`../README.md`](../README.md) and [`../docs/API.md`](../docs/API.md) for details.
