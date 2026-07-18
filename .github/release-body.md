## Install

Self-contained binaries — **no runtime to install**. The server listens on
**http://localhost:5341** and, on first start, creates an admin login
**`admin` / `123123`** — change it immediately under **Settings → Users**.

> Verify a download (optional): `sha256sum -c SHA256SUMS.txt`
> (Windows installers: `sha256sum -c SHA256SUMS-win-installer.txt`.)

### Windows — installer (recommended)

1. Download **`ameto-__VERSION__-setup-x64.exe`** (32-bit host: `ameto-__VERSION__-setup-x86.exe`).
2. Run it — it elevates and installs the **`Ameto`** Windows service
   (app in `C:\Program Files\Ameto`, data in `C:\ProgramData\Ameto`).
3. Open <http://localhost:5341>.

Manage: `Restart-Service Ameto` · `Stop-Service Ameto`

### Windows — portable (zip)

```powershell
Expand-Archive ameto-__VERSION__-win-x64.zip -DestinationPath ameto
cd ameto
# register as a service (from an elevated PowerShell 7):
.\install.ps1 -BinaryPath .\Ameto.Server.exe -HttpPort 5341
# …or just run it in the foreground:
.\Ameto.Server.exe
```

### Linux (systemd)

```bash
mkdir ameto && tar -xzf ameto-__VERSION__-linux-x64.tar.gz -C ameto && cd ameto
# creates user `ameto`, installs to /opt/ameto, data in /var/lib/ameto/data, starts the service:
sudo ./install.sh --binary ./Ameto.Server
systemctl status ameto
journalctl -u ameto -f
```

### Docker

```bash
docker run -d --name ameto -p 5341:5341 -v ameto-data:/data \
  ghcr.io/ameto-labs/ameto:__VERSION__
```

The `__VERSION__` image is pushed to GHCR by the same release workflow (`:latest`
points at it too). `install/docker/docker-compose.example.yml` documents every
configuration option.

### After install — send data

1. Open <http://localhost:5341>, log in as `admin` / `123123`, **change the password**.
2. Create an API key under **Settings → API Keys**.
3. Point your telemetry at the server — Serilog / Seq sink → `http://localhost:5341`,
   or OpenTelemetry OTLP → `http://localhost:5341/otlp/v1/{logs,traces,metrics}`.

Full docs: [install guide](https://github.com/AMeto-Labs/AMeto-Observability/blob/main/install/README.md) ·
[configuration](https://github.com/AMeto-Labs/AMeto-Observability/blob/main/docs/CONFIGURATION.md)

---
