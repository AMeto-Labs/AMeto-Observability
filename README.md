# Ameto

A high-performance, self-hosted observability server — **logs, traces and metrics** in a single binary. A full open-source alternative to [Datalust Seq](https://datalust.co) that also speaks [OpenTelemetry](https://opentelemetry.io).

## Features

- **150,000 events/second** measured sustained ingestion on a single node (99.97 % accepted; 100k/s runs with **zero drops**, graceful degradation up to a measured **456k/s** peak — see [tools/loadtest](tools/loadtest/README.md))
- **Three signals, one server** — structured logs, distributed traces and metrics share the same storage and query primitives
- **OTLP ingestion** — native `POST /otlp/v1/{logs,traces,metrics}` (protobuf), no collector required
- **Seq-compatible Filter Expressions** — use the same query syntax you know
- **Off-heap storage** — ring buffer + hot-tier arena in `NativeMemory`, LZ4-compressed cold segments
- **Inverted index + Bloom filters** — fast segment-skip without decompressing blocks
- **Alert rules** — threshold-based signals with webhook / SMTP dispatch
- **Symmetric replication** — each node replicates its own flushed segments to all peers, no leader election
- **Auth** — JWT + API keys, optional OAuth providers
- **Angular SPA** — live tail, log search, trace explorer, metric charts, retention settings, diagnostics
- **Single binary** per node, no external dependencies

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | .NET 10, C# 14 |
| HTTP server | Kestrel (HTTP/1.1 + HTTP/2) |
| Configuration | YAML (`config.yml`) via `NetEscapades.Configuration.Yaml` |
| Auth | JWT Bearer (8 h lifetime) + API-key cache for ingest hot path + OAuth |
| Auth storage | SQLite (`Microsoft.Data.Sqlite`) |
| Ingest protocols | Seq-compatible CLEF (MessagePack) + OTLP (Google.Protobuf) |
| Wire format | MessagePack (`application/x-msgpack`) |
| Compression | LZ4 (K4os.Compression.LZ4) |
| Frontend | Angular 21, standalone components, Vitest |
| Serilog sink | `Serilog.Sinks.Ameto` (net8.0 / net9.0 / net10.0, on NuGet) |

## Quick Start

### 1. Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Node.js 22+ / npm 10+](https://nodejs.org/) — only required for UI development

### 2. Run the server

```bash
cd src/Ameto.Server
dotnet run -c Release
```

The server starts on `http://localhost:5341` by default.
Configuration lives in `src/Ameto.Server/config.yml` (see [docs/CONFIGURATION.md](docs/CONFIGURATION.md)).

On first start a seed admin account is created automatically:

| Field | Value |
|-------|-------|
| Username | `admin` |
| Password | `123123` |

Change the password immediately via **Settings → Users**.

### 3. Send data

**Logs via Serilog** — use the dedicated [`Serilog.Sinks.Ameto`](src/Serilog.Sinks.Ameto) sink:

```csharp
Log.Logger = new LoggerConfiguration()
    .WriteTo.Ameto("http://localhost:5341",
                   apiKey: "<your-api-key>")
    .CreateLogger();

Log.Information("User {UserId} logged in from {Ip}", "alice", "10.0.0.1");
```

The Seq-compatible endpoint also works with any Seq sink (`WriteTo.Seq(...)`).

**Logs / traces / metrics via OpenTelemetry** — point your OTLP exporter at:

```
POST http://localhost:5341/otlp/v1/logs
POST http://localhost:5341/otlp/v1/traces
POST http://localhost:5341/otlp/v1/metrics
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

### 5. Deploy (production)

Ameto runs as a **Windows service**, a **Linux systemd service**, or in **Docker** — see
[**install/README.md**](install/README.md) for step-by-step instructions and requirements per OS.

To build a self-contained binary yourself (UI baked in):

```bash
# build the Angular UI into wwwroot, then publish a single self-contained binary
cd client && npm ci && npx ng build --configuration production --output-path dist
# copy dist/browser/* → src/Ameto.Server/wwwroot, then:
cd .. && dotnet publish src/Ameto.Server -c Release -r <win-x64|linux-x64> \
    --self-contained true -p:PublishSingleFile=true -o publish
```

Docker builds the UI + server in one step — no local .NET/Node needed:
`docker compose -f install/docker/docker-compose.example.yml up -d --build`.

### 6. Run tests

```bash
dotnet test tests/Ameto.Core.Tests
dotnet test tests/Ameto.Storage.Tests
dotnet test tests/Ameto.Indexing.Tests
dotnet test tests/Ameto.Query.Tests
dotnet test tests/Ameto.Integration.Tests

# BenchmarkDotNet perf suite + allocation/parity probes
cd tests/Ameto.Perf
dotnet run -c Release
```

## Configuration

Configuration lives in `src/Ameto.Server/config.yml`. See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the full reference.

Minimal example:

```yaml
Ameto:
  NodeId: 0
  DataDirectory: data
  HttpPort: 5341
  HotTier:
    MaxSizeBytes: 67108864   # 64 MB
    MaxAge: "00:05:00"
```

## REST API

See [docs/API.md](docs/API.md) for the full reference and [docs/FILTER_EXPRESSIONS.md](docs/FILTER_EXPRESSIONS.md) for the query syntax. Core surface:

| Area | Endpoints |
|------|-----------|
| Ingest (CLEF) | `POST /api/events` |
| Ingest (OTLP) | `POST /otlp/v1/logs`, `/otlp/v1/traces`, `/otlp/v1/metrics` |
| Logs | `GET /api/events`, `/api/events/live` (SSE), `/api/events/props`, `/api/events/services` |
| Traces | `GET /api/traces`, `/api/traces/stats`, `/api/traces/{traceId}` |
| Metrics | `GET /api/metrics`, `/api/metrics/names`, `/api/metrics/{name}` |
| Auth | `POST /api/auth/login`, `/api/auth/refresh`, `/api/auth/keys`, OAuth via `/api/auth/oauth/{provider}` |
| Ops | `GET /api/stats`, `/api/diagnostics`, `/api/retention`, `/api/replication/nodes`, `/health` |

## Architecture

See [docs/OBSERVABILITY_PLAN.md](docs/OBSERVABILITY_PLAN.md) for the logs/traces/metrics design overview, and the module breakdown below.

## Project layout

```
Ameto.slnx
├── src/
│   ├── Ameto.Core/          — shared models, interfaces, serialization
│   ├── Ameto.Storage/       — hot-tier NativeMemory arena, WAL, cold LZ4 segments
│   ├── Ameto.Indexing/      — inverted index, trigram index, Bloom filter
│   ├── Ameto.Ingestion/     — MPMC ring buffer, HTTP ingest endpoint
│   ├── Ameto.Query/         — Seq Filter parser, evaluator, query executor
│   ├── Ameto.Tracing/       — span storage and trace model
│   ├── Ameto.Metrics/       — metric point storage and aggregation
│   ├── Ameto.Otel/          — OTLP (protobuf) ingestion + trace/metric endpoints
│   ├── Ameto.Alerts/        — threshold rules, webhook/SMTP dispatch
│   ├── Ameto.Replication/   — symmetric segment replication, peer probing
│   ├── Ameto.Server/        — Kestrel host, REST/SSE endpoints, Angular SPA
│   └── Serilog.Sinks.Ameto/ — Serilog sink (NuGet package)
├── client/                   — Angular 21 SPA (served from wwwroot at runtime)
└── tests/
    ├── Ameto.Core.Tests/
    ├── Ameto.Storage.Tests/
    ├── Ameto.Indexing.Tests/ — posting-list codec, segment format, zero-alloc gates
    ├── Ameto.Query.Tests/
    ├── Ameto.Integration.Tests/
    └── Ameto.Perf/           — BenchmarkDotNet benchmarks + allocation/parity probes
```

## License

MIT
