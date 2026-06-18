# Ameto — Installation Guide

Ameto ships as a **self-contained single binary** — no .NET runtime or external dependencies required.

- Default port: **5341**
- Default admin credentials: **admin / 123123** — change immediately after first login via **Settings → Users**

---

## Contents

- [Windows](#windows)
- [Linux](#linux)
- [Docker](#docker)
- [Configuration reference](#configuration)
- [Serilog sink (log ingestion)](#serilog-sink)

---

## Windows

### Option A — Windows Service (recommended)

1. Download the latest Windows release archive from [GitHub Releases](https://github.com/AMeto-Observability/AMeto-Observability/releases) and extract it.
2. Open **PowerShell 7+ as Administrator** in the extracted directory.
3. Run the installer:

```powershell
.\install\windows\install.ps1
```

The script will:
- Copy the binary to `C:\Program Files\Ameto`
- Create a data directory at `C:\ProgramData\Ameto\data`
- Write a default `config.yml`
- Register and start the **Ameto** Windows Service (auto-start)

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-BinaryPath` | auto-detect | Path to `Ameto.Server.exe` |
| `-InstallDir` | `C:\Program Files\Ameto` | Service installation directory |
| `-DataDirectory` | `C:\ProgramData\Ameto\data` | Log data storage directory |
| `-HttpPort` | `5341` | HTTP port |
| `-ServiceName` | `Ameto` | Windows Service name |
| `-Uninstall` | — | Remove the service |

**Examples:**

```powershell
# Custom port and data location
.\install\windows\install.ps1 -HttpPort 8080 -DataDirectory D:\logs\Ameto

# Uninstall
.\install\windows\install.ps1 -Uninstall
```

**Service management:**

```powershell
Start-Service Ameto
Stop-Service  Ameto
Restart-Service Ameto
Get-Service   Ameto
```

### Option B — Run manually

```powershell
# Extract archive, then:
.\Ameto.Server.exe
```

The server reads `config.yml` from the same directory as the executable.

---

## Linux

### Option A — systemd service (recommended)

1. Download the latest Linux release archive from [GitHub Releases](https://github.com/AMeto-Observability/AMeto-Observability/releases) and extract it.
2. Run the installer as root:

```bash
sudo bash install/linux/install.sh
```

The script will:
- Create a dedicated `Ameto` system user
- Install the binary to `/opt/Ameto/`
- Create a data directory at `/var/lib/Ameto/data`
- Write a default `config.yml`
- Register and start the `Ameto` systemd unit

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--binary <path>` | auto-detect | Path to `Ameto.Server` binary |
| `--install-dir <path>` | `/opt/Ameto` | Installation directory |
| `--data <path>` | `/var/lib/Ameto/data` | Data directory |
| `--port <n>` | `5341` | HTTP port |
| `--service <name>` | `Ameto` | systemd service name |
| `--uninstall` | — | Remove service and files |

**Examples:**

```bash
# Custom data directory and port
sudo bash install/linux/install.sh --data /mnt/fast-disk/Ameto --port 8080

# Uninstall
sudo bash install/linux/install.sh --uninstall
```

**Service management:**

```bash
systemctl status  Ameto
systemctl start   Ameto
systemctl stop    Ameto
systemctl restart Ameto

# Follow logs
journalctl -u Ameto -f
```

### Option B — Run manually

```bash
chmod +x ./Ameto.Server
./Ameto.Server
```

The server reads `config.yml` from its working directory.

---

## Docker

### Quick start

```bash
cd install/docker
docker compose up -d
```

Open `http://localhost:5341`.

### Build the image

The Docker setup uses a **multi-stage build** (Node.js 22 for the Angular SPA, .NET 10 SDK for the server) producing a minimal runtime image based on `mcr.microsoft.com/dotnet/runtime-deps:10.0`.

```bash
# From the repository root:
docker build -f install/docker/Dockerfile -t Ameto:latest .

# Or via Compose (builds automatically on first run):
docker compose -f install/docker/docker-compose.yml up -d
```

### Custom configuration

Mount your own `config.yml` to override defaults:

1. Copy the sample config:

```bash
cp src/Ameto.Server/config.yml install/docker/config.yml
# Edit as needed
```

2. Uncomment the volume mount in `docker-compose.yml`:

```yaml
volumes:
  - Ameto-data:/data
  - ./config.yml:/app/config.yml:ro   # <-- uncomment this line
```

3. Restart:

```bash
docker compose restart Ameto
```

### Data persistence

Log data is stored in the named volume `Ameto-data` (mapped to `/data` inside the container). This volume persists across `docker compose down` and container restarts.

To back up or inspect data:

```bash
# Inspect volume path
docker volume inspect Ameto_Ameto-data

# Copy data out
docker cp Ameto:/data ./Ameto-backup
```

### Environment variables

Ameto is configured via `config.yml`, not environment variables. To customize settings in Docker, mount a config file (see above).

### Ports

| Container port | Host port (default) | Description |
|---------------|---------------------|-------------|
| 5341 | 5341 | HTTP (UI + API + ingestion) |

Change the host port in `docker-compose.yml`:

```yaml
ports:
  - "8080:5341"   # host:container
```

---

## Configuration

Configuration lives in `config.yml` next to the executable (or at `/app/config.yml` in Docker).

```yaml
Ameto:
  NodeId: 0
  DataDirectory: data          # relative to binary, or absolute path
  HttpPort: 5341

  # TLS — leave empty for plain HTTP
  SslCertPath: ""
  SslCertPassword: ""

  HotTier:
    MaxSizeBytes: 268435456    # 256 MB in-memory tier before flushing to disk
    MaxAge: "00:05:00"         # flush after 5 minutes regardless of size

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
    LocalAddress: ""           # e.g. "http://node0:5341"
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the full reference.

---

## Serilog sink

Install the NuGet package in your application:

```bash
dotnet add package Ameto.Serilog
```

Configure:

```csharp
Log.Logger = new LoggerConfiguration()
    .WriteTo.Seq("http://localhost:5341", apiKey: "<your-api-key>")
    .CreateLogger();
```

Create an API key via **Settings → API Keys** in the Ameto UI, or via the REST API:

```bash
curl -X POST http://localhost:5341/api/auth/keys \
     -H "Authorization: Bearer <jwt-token>" \
     -H "Content-Type: application/json" \
     -d '{"description": "my-service"}'
```

See [docs/API.md](docs/API.md) for the full REST API reference.
