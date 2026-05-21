# Rd.Log

A high-performance, self-hosted structured log server — a full open-source alternative to [Datalust Seq](https://datalust.co).

## Features

- **100,000 events/second** sustained ingestion throughput
- **Seq-compatible Filter Expressions** — use the same query syntax you know
- **Off-heap storage** — ring buffer + hot-tier arena in `NativeMemory`, LZ4-compressed cold segments
- **Inverted index + Bloom filters** — fast segment-skip without decompressing blocks
- **Alert rules** — threshold-based signals with webhook / SMTP dispatch
- **Symmetric replication** — each node replicates its own flushed segments to all peers, no leader election
- **Angular SPA** — live tail, search, signals, retention settings, diagnostics
- **Single binary** per node, no external dependencies

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | .NET 10, C# 14 |
| HTTP server | Kestrel (HTTP/1.1 + HTTP/2) |
| Configuration | YAML (`config.yml`) via `NetEscapades.Configuration.Yaml` |
| Auth | JWT Bearer (8 h lifetime) + API-key cache for ingest hot path |
| Auth storage | SQLite (`Microsoft.Data.Sqlite`) |
| Wire format | MessagePack (`application/x-msgpack`) |
| Compression | LZ4 (K4os.Compression.LZ4) |
| Frontend | Angular 21, standalone components, Vitest |

## Quick Start

### 1. Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Node.js 22+ / npm 10+](https://nodejs.org/) — only required for UI development

### 2. Run the server

```bash
cd src/Rd.Log.Server
dotnet run -c Release
```

The server starts on `http://localhost:5341` by default.
Configuration lives in `src/Rd.Log.Server/config.yml` (see [docs/CONFIGURATION.md](docs/CONFIGURATION.md)).

On first start a seed admin account is created automatically:

| Field | Value |
|-------|-------|
| Username | `admin` |
| Password | `123123` |

Change the password immediately via **Settings → Users**.

### 3. Send a log event

Use any Seq-compatible sink. Example with **Serilog**:

```csharp
Log.Logger = new LoggerConfiguration()
    .WriteTo.Seq("http://localhost:5341",
                 apiKey: "<your-api-key>")
    .CreateLogger();

Log.Information("User {UserId} logged in from {Ip}", "alice", "10.0.0.1");
```

Create an API key first via **Settings → API Keys** (or `POST /api/auth/keys`).

### 4. Open the UI

The Angular SPA is served by the same Kestrel process at `http://localhost:5341`.

For local UI development (hot-reload):

```bash
cd client
npm install
npm start   # proxies /api/* to http://localhost:5341
```

### 5. Publish (production)

```powershell
.\publish.ps1            # build Angular + dotnet publish (self-contained), interactive prompt
.\publish.ps1 -NoRestart # build only
.\publish.ps1 -Restart   # build + restart running server
```

### 6. Run tests

```bash
dotnet test tests/Rd.Log.Core.Tests
dotnet test tests/Rd.Log.Query.Tests
dotnet test tests/Rd.Log.Storage.Tests
dotnet test tests/Rd.Log.Integration.Tests

# BenchmarkDotNet perf suite
cd tests/Rd.Log.Perf
dotnet run -c Release
```

## Configuration

Configuration lives in `src/Rd.Log.Server/config.yml`. See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the full reference.

Minimal example:

```yaml
RdLog:
  NodeId: 0
  DataDirectory: data
  HttpPort: 5341
  HotTier:
    MaxSizeBytes: 268435456
    MaxAge: "00:05:00"
```

## REST API

See [docs/API.md](docs/API.md) for the full API reference.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the detailed module breakdown.

## Project layout

```
Rd.Log.slnx
├── src/
│   ├── Rd.Log.Core/          — shared models, interfaces, serialization
│   ├── Rd.Log.Storage/       — hot-tier NativeMemory arena, WAL, cold LZ4 segments
│   ├── Rd.Log.Indexing/      — inverted index, trigram index, Bloom filter
│   ├── Rd.Log.Ingestion/     — MPMC ring buffer, HTTP ingest endpoint
│   ├── Rd.Log.Query/         — Seq Filter parser, evaluator, query executor
│   ├── Rd.Log.Alerts/        — threshold rules, webhook/SMTP dispatch
│   ├── Rd.Log.Replication/   — symmetric segment replication, peer probing
│   └── Rd.Log.Server/        — Kestrel host, REST/SSE endpoints, Angular SPA
├── client/                   — Angular 21 SPA (served from wwwroot at runtime)
├── tests/
│   ├── Rd.Log.Core.Tests/
│   ├── Rd.Log.Storage.Tests/
│   ├── Rd.Log.Query.Tests/
│   ├── Rd.Log.Integration.Tests/
│   └── Rd.Log.Perf/          — BenchmarkDotNet benchmarks
└── tools/
    ├── Rd.Log.DemoGen/        — demo event generator
    └── Rd.Log.LoadTest/       — load test harness
```

## License

MIT