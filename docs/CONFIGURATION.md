# Configuration Reference

All settings live in `src/Ameto.Server/config.yml` under the `Ameto` key.

Environment variables override config file values using `__` as the hierarchy separator:

```bash
Ameto__DataDirectory=/mnt/logs Ameto__HttpPort=5342 ./Ameto.Server
```

---

## Server options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `NodeId` | uint | `0` | Node identifier. Must be unique per node in a multi-node setup. |
| `DataDirectory` | string | `"data"` | Root directory for WAL files, cold segments, alert rules, and the auth SQLite database. |
| `HttpPort` | int | `5341` | Kestrel listen port. |
| `SslCertPath` | string | `""` | Path to a `.pfx` TLS certificate. Leave empty for plain HTTP. |
| `SslCertPassword` | string | `""` | Password for the `.pfx` certificate. |
| `RamTargetPercent` | int | `85` | When OS RAM utilisation exceeds this threshold the hot tier is flushed to disk automatically. |

---

## Hot-tier options (`Ameto:HotTier`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `MaxSizeBytes` | long | `268435456` (256 MB) | Hot-tier arena size. A flush is triggered when this threshold is reached. |
| `MaxAge` | TimeSpan string | `"00:05:00"` (5 min) | Maximum age of hot-tier data before an automatic flush. Format: `hh:mm:ss`. |

---

## Indexing options (`Ameto:Indexing`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `MaxPropertyFlattenDepth` | int | `5` | Maximum recursion depth when flattening nested structured properties into the inverted index. Set to `0` to index only top-level keys. |

---

## Retention (`Ameto:Retention`)

These values seed the SQLite retention table **on first run only**. After that, use `PUT /api/retention` to change them.

| Key | Type | Default |
|-----|------|---------|
| `VerboseDays` | int | `3` |
| `DebugDays` | int | `3` |
| `InformationDays` | int | `90` |
| `WarningDays` | int | `90` |
| `ErrorDays` | int | `90` |
| `FatalDays` | int | `90` |

---

## Replication options (`Ameto:Replication`)

Symmetric replication: each node replicates its own flushed cold segments to all healthy peers. There is no leader election.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `true` | Enable replication. When `false`, all replication endpoints and background services are skipped. |
| `LocalAddress` | string | `""` | This node's publicly reachable base URL, e.g. `"http://node0:5341"`. Used by peers to push segments and pings back. |
| `SeedNodes` | string[] | `[]` | Base URLs of peer nodes to probe on startup. Additional peers are discovered automatically via ping exchange. |
| `ProbeInterval` | TimeSpan string | `"00:00:10"` | How often to ping known peers. |
| `PushTimeout` | TimeSpan string | `"00:01:00"` | Per-segment HTTP push timeout. |

A node is considered **healthy** if its last successful ping was within 30 seconds.

### Example — two-node setup

**Node 0** `config.yml`:
```yaml
Ameto:
  NodeId: 0
  HttpPort: 5341
  Replication:
    Enabled: true
    LocalAddress: "http://node0:5341"
    SeedNodes:
      - "http://node1:5341"
```

**Node 1** `config.yml`:
```yaml
Ameto:
  NodeId: 1
  HttpPort: 5341
  Replication:
    Enabled: true
    LocalAddress: "http://node1:5341"
    SeedNodes:
      - "http://node0:5341"
```

---

## TLS

```yaml
Ameto:
  HttpPort: 5341
  SslCertPath: "/etc/Ameto/cert.pfx"
  SslCertPassword: "changeme"
```

The certificate is hot-reloaded on every new TLS handshake — replace the `.pfx` file on disk and new connections pick it up without restarting the process.

---

## Full `config.yml` reference

```yaml
Ameto:
  NodeId: 0
  DataDirectory: data
  HttpPort: 5341
  SslCertPath: ""
  SslCertPassword: ""
  RamTargetPercent: 99

  HotTier:
    MaxSizeBytes: 268435456   # 256 MB
    MaxAge: "00:05:00"        # hh:mm:ss

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
    Enabled: true
    LocalAddress: ""          # this node's public URL, e.g. "http://node0:5341"
    SeedNodes: []             # peer URLs, e.g. ["http://node1:5341"]
    ProbeInterval: "00:00:10"
    PushTimeout: "00:01:00"
```