# Configuration Reference

All settings live in `config.yml` (next to the server binary; `src/Ameto.Server/config.yml`
when running from source) under the `Ameto` key.

Precedence (later wins): **`config.yml` → environment variables → CLI args**.
Environment variables use `__` as the hierarchy separator:

```bash
Ameto__DataDirectory=/mnt/logs Ameto__HttpPort=5342 ./Ameto.Server
```

---

## Server options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `NodeId` | uint | `0` | Node identifier. Must be unique per node in a multi-node setup. |
| `DataDirectory` | string | `"data"` | Root directory for WAL files, cold segments, and the auth/retention SQLite database (`Ameto.db`). |
| `HttpPort` | int | `5341` | Kestrel listen port (serves the API, SSE, OTLP, and the SPA). |
| `SslCertPath` | string | `""` | Path to a `.pfx` TLS certificate. Empty = plain HTTP. |
| `SslCertPassword` | string | `""` | Password for the `.pfx` certificate. |
| `RamTargetPercent` | int | `85` | When host/container RAM load exceeds this, the hot tier is flushed to disk to release the write buffer. |

---

## Hot-tier options (`Ameto:HotTier`)

The hot tier is the in-RAM write buffer; it is flushed to a compressed cold segment on size, age, or memory pressure.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `MaxSizeBytes` | long | `67108864` (64 MB) | Flush is triggered when the hot tier reaches this size. Smaller tiers = smaller frozen tiers held while flushing, so the parallel-flush backlog can be deeper for the same RAM ceiling. |
| `MaxAge` | TimeSpan | `"00:05:00"` (5 min) | Flush a non-empty tier at least this often. Format `hh:mm:ss`. |
| `FlushConcurrency` | int | `0` | Concurrent cold-segment flushes (index build + compress + write). `0` = auto (≈ cores/2, capped 2–8). Lower = less peak RAM; higher = more flush throughput (fewer drops under burst) on many-core hosts. |

---

## Indexing options (`Ameto:Indexing`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `MaxPropertyFlattenDepth` | int | `5` | Max recursion depth when flattening nested structured properties into the inverted index. `0` = index only top-level keys. |

---

## Ingestion options (`Ameto:Ingestion`)

Request/size limits, in bytes. Oversized requests are rejected with `413` before parsing.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `MaxBatchBytes` | int | `4194304` (4 MB) | Max body for a CLEF batch (`POST /api/events`). |
| `MaxEventPayloadBytes` | int | `65536` (64 KB) | Max serialised properties for a single event (also the ring-buffer slab size). An oversized event is dropped (and logged) while the rest of the batch ingests. |
| `MaxOtlpBatchBytes` | int | `8388608` (8 MB) | Max body for the OTLP endpoints (`POST /otlp/v1/*`). |
| `RingCapacity` | int | `65536` | Ring-buffer slots between the HTTP ingest endpoints and the storage drainer (rounded up to a power of two, ~64 B each). Together with `PayloadPoolBytes` this is the absorption window for flush stalls before events drop. |
| `PayloadPoolBytes` | long | `536870912` (512 MB) | Payload slab arena budget: slab count = min(`RingCapacity`, this / `MaxEventPayloadBytes`). Reserved virtual memory — resident pages track the payload bytes actually written, not the budget. Slabs, not ring slots, are the true drop threshold under stall. |

---

## Retention (`Ameto:Retention`)

These seed the SQLite retention table **on first run only**. Afterwards, change them in the UI (**Settings → Retention**) or via `PUT /api/retention`.

| Key | Type | Default (days) |
|-----|------|----------------|
| `VerboseDays` | int | `90` |
| `DebugDays` | int | `3` |
| `InformationDays` | int | `90` |
| `WarningDays` | int | `90` |
| `ErrorDays` | int | `90` |
| `FatalDays` | int | `90` |
| `MetricsDays` | int | `30` |
| `TracesDays` | int | `14` |

---

## Replication options (`Ameto:Replication`)

Symmetric replication: each node replicates its own flushed cold segments to all healthy peers. No leader election. A node is **healthy** if its last successful ping was within 30 s.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | When `false` the node runs standalone (all replication endpoints/services skipped). |
| `SeedNodes` | string[] | `[]` | Peer base URLs to probe on startup. Further peers are discovered via ping exchange. |
| `LocalAddress` | string | `"http://localhost:5341"` | This node's publicly reachable base URL, used by peers to push segments/pings back. Set to the real hostname when clustering. |
| `ProbeInterval` | TimeSpan | `"00:00:10"` | How often to ping known peers. |
| `PushTimeout` | TimeSpan | `"00:01:00"` | Per-segment HTTP push timeout. |

### Example — two-node cluster

**Node 0**:
```yaml
Ameto:
  NodeId: 0
  HttpPort: 5341
  Replication:
    Enabled: true
    LocalAddress: "http://node0:5341"
    SeedNodes: ["http://node1:5341"]
```

**Node 1**:
```yaml
Ameto:
  NodeId: 1
  HttpPort: 5341
  Replication:
    Enabled: true
    LocalAddress: "http://node1:5341"
    SeedNodes: ["http://node0:5341"]
```

---

## TLS

```yaml
Ameto:
  HttpPort: 5341
  SslCertPath: "/etc/ameto/cert.pfx"
  SslCertPassword: "changeme"
```

The certificate is hot-reloaded on every new TLS handshake — replace the `.pfx` on disk and new connections pick it up without restarting.

---

## Full `config.yml` reference

```yaml
Ameto:
  NodeId: 0
  DataDirectory: data
  HttpPort: 5341
  SslCertPath: ""
  SslCertPassword: ""
  RamTargetPercent: 85

  HotTier:
    MaxSizeBytes: 67108864    # 64 MB
    MaxAge: "00:05:00"        # hh:mm:ss
    FlushConcurrency: 0       # 0 = auto (cores/2, 2-8)

  Indexing:
    MaxPropertyFlattenDepth: 5

  Ingestion:
    MaxBatchBytes: 4194304        # 4 MB  (CLEF /api/events)
    MaxEventPayloadBytes: 65536   # 64 KB (per-event properties)
    MaxOtlpBatchBytes: 8388608    # 8 MB  (/otlp/v1/*)

  Retention:
    VerboseDays: 90
    DebugDays: 3
    InformationDays: 90
    WarningDays: 90
    ErrorDays: 90
    FatalDays: 90
    MetricsDays: 30
    TracesDays: 14

  Replication:
    Enabled: false
    LocalAddress: "http://localhost:5341"
    SeedNodes: []             # e.g. ["http://node1:5341"]
    ProbeInterval: "00:00:10"
    PushTimeout: "00:01:00"
```
