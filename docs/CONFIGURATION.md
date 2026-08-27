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
| `BasePath` | string | `""` | URL prefix the whole server is served under, e.g. `"/ameto"`, for hosting behind a reverse proxy at `https://host/ameto`. Empty = served at `/`. Applied at runtime, so one build works under any prefix — see [Serving under a URL prefix](#serving-under-a-url-prefix). |
| `SslCertPath` | string | `""` | Path to a `.pfx` TLS certificate. Empty = plain HTTP. |
| `SslCertPassword` | string | `""` | Password for the `.pfx` certificate. |
| `TrustForwardedHeaders` | bool | `false` | Trust `X-Forwarded-Proto/Host/For` from a reverse proxy that terminates TLS. Required for correct OAuth redirect URIs behind nginx/traefik. Enable only when the server is reachable exclusively through the proxy. |
| `KnownProxies` | string[] | `[]` | IPs of the reverse proxies whose forwarded headers are trusted. Empty trusts any source and logs a startup warning — list your proxy here whenever `TrustForwardedHeaders` is on. |
| `RamTargetPercent` | int | `85` | When host/container RAM load exceeds this, the hot tier is flushed to disk to release the write buffer. |

---

## Serving under a URL prefix

To host Ameto at `https://logs.example.com/ameto` rather than at the root of a host, set:

```yaml
Ameto:
  BasePath: "/ameto"
```

`ameto`, `/ameto` and `/ameto/` all mean the same thing. The value is read once at startup, so
changing it needs a restart. A value that cannot be a path prefix — a whole URL, a query string,
a `..` segment — stops the server at startup with a message naming the setting, rather than
half-configuring a pipeline that then serves the UI from the wrong place.

The prefix is applied at **runtime**, not at build time: the SPA's `<base href>` is rewritten as
`index.html` is served. One build and one container image therefore work under any prefix, and
the Windows installer and the Docker image of the same version no longer disagree about where
they are hosted.

### The proxy must pass the prefix through

This is the one thing that has to be right. In nginx that means `proxy_pass` with **no trailing
slash** and no URI part:

```nginx
# Bare /ameto does not match `location /ameto/`, so send it to the canonical form.
location = /ameto { return 301 /ameto/; }

location /ameto/ {
    proxy_pass http://127.0.0.1:5341;      # NO trailing slash — the prefix reaches the server
    proxy_http_version 1.1;

    proxy_set_header Host              $host;
    proxy_set_header X-Real-IP         $remote_addr;
    proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Host  $host;
    proxy_set_header X-Forwarded-Proto $scheme;

    # Ameto streams the live tail over SSE. Without this, nginx buffers the response and the
    # tail looks frozen, then arrives in bursts.
    proxy_buffering off;
    proxy_read_timeout 900;

    # Ingest batches are up to 4 MB (Ameto:Ingestion:MaxBatchBytes). nginx defaults to 1 MB and
    # answers 413 — and a Serilog sink treats that as delivered and drops the batch.
    client_max_body_size 8m;
    proxy_request_buffering off;   # stream large batches rather than spooling them to disk
}
```

Add `TrustForwardedHeaders: true` with your proxy's IP in `KnownProxies` so the server builds
links and OAuth redirects with the public scheme and host rather than Kestrel's own.

A trailing slash on `proxy_pass` (`proxy_pass http://127.0.0.1:5341/;`) **strips** the prefix
instead. The UI, the API, SSE and ingest all still work that way — the `<base href>` comes from
configuration, not from the request — but everything the server *generates* loses the prefix,
because the request no longer carries it:

* the OAuth `redirect_uri` sent to Google/Microsoft, so sign-in fails at the provider;
* the post-sign-in redirects, which land outside the app and discard the freshly issued token;
* the `Location` header on `POST /api/alerts` and `POST /api/alerts/maintenance`.

If you only use local username/password sign-in, a stripping proxy is a workable setup. If you
use OAuth, pass the prefix through.

`X-Forwarded-Prefix` is deliberately **not** honoured. With `TrustForwardedHeaders` on and no
`KnownProxies`, any client could set it — and a path base of `//evil.com` is a valid one, which
would turn the post-sign-in redirect into a token handed to another host.

### What changes for callers

Nothing is moved; an address is added. `UsePathBase` strips the prefix when it is present and
passes every other request through untouched, so **every endpoint answers both at the root and
under the prefix**. That is what lets the container health check and any agent already pointed at
the bare address survive the change with no coordination — the upgrade is safe to do first and
reconfigure afterwards.

The consequence to be clear about: the prefix is **not an access boundary**. With
`BasePath: "/ameto"` set, `POST /api/auth/login` is still fully live at the origin root. If you
need the prefix to actually restrict what is reachable, enforce that at the proxy.

Callers that reach Kestrel directly (agents on the same host, the container health check) need no
change. Callers that go through a proxy scoped to the prefix must use it:

| | direct | through the proxy |
|---|---|---|
| UI | `http://host:5341/` | `https://host/ameto/` |
| Logs (CLEF) | `http://host:5341/api/events` | `https://host/ameto/api/events` |
| OTLP logs / traces / metrics | `http://host:5341/otlp/v1/…` | `https://host/ameto/otlp/v1/…` |
| Health | `http://host:5341/health` | `https://host/ameto/health` |
| OAuth callback to register | — | `https://host/ameto/api/auth/oauth/{provider}/callback` |

Replication `LocalAddress` and `SeedNodes` may carry a peer's prefix
(`http://node1:5341/ameto`), with or without a trailing slash.

---

## Sign-in providers (`Ameto:Auth`)

Local username/password login is on by default. Google / Microsoft OAuth buttons appear on the login page as soon as the matching credentials are set (empty = provider disabled). Secrets are best passed via environment variables rather than `config.yml`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `LocalEnabled` | bool | `true` | Allow local username/password login. |
| `Google:ClientId` / `Google:ClientSecret` | string | `""` | OAuth client (Google Cloud Console → Credentials → *Web application*). Redirect URI to register: `https://<host><BasePath>/api/auth/oauth/google/callback` — Google requires **https** for any non-localhost host. |
| `Microsoft:ClientId` / `Microsoft:ClientSecret` | string | `""` | Azure AD app registration. Redirect URI: `https://<host><BasePath>/api/auth/oauth/microsoft/callback`. |
| `Microsoft:TenantId` | string | `""` | **Required** for Microsoft sign-in: your Entra tenant id. Blank or a tenant-agnostic value (`common` / `organizations` / `consumers`) leaves the provider disabled unless `AllowMultiTenant` is set — see below. |
| `Microsoft:AllowMultiTenant` | bool | `false` | Accept a tenant-agnostic endpoint anyway. |

Who may sign in is controlled in **Settings → Users**: add an OAuth user by exact e-mail, or a per-domain rule ("anyone `@your-company.com` via google gets role X"). Unknown e-mails are rejected. Changing `Auth` settings requires a server restart (auth handlers are registered at startup).

### Why the tenant id is required

Sign-in is matched on the e-mail address the provider asserts, so that address has to be one only the right directory can claim. A tenant-scoped endpoint gives that: the claim can only come from the tenant you named. A tenant-agnostic one does not — anyone may register an Entra tenant and set any address on a user in it, including one already on your allowlist. Ameto therefore refuses to register the Microsoft provider unless you pin `TenantId` (or explicitly opt in with `AllowMultiTenant`), and logs an error at startup explaining the missing sign-in button.

Two further checks apply regardless: Google sign-ins must carry `email_verified`, and every OAuth account is bound to the provider's immutable subject id on first sign-in — after which a different identity asserting the same address is refused.

**One gap remains if you set `AllowMultiTenant`.** Subject binding protects an account only once it is bound, and binding happens at the *first* sign-in. So an entry you add ahead of time — the usual "add the new hire to the allowlist, they join next week" flow — belongs to whoever signs in as that address first. With the tenant pinned that can only be someone from your directory; with `AllowMultiTenant` it can be anyone who asserts the address from any tenant. The binding is recorded in the log at `Information` ("bound to subject … on first sign-in"), which is the only signal distinguishing that from a normal sign-in, so if you run multi-tenant, watch that line. Prefer pinning the tenant and adding allowlist entries close to when they will be used.

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

## Resource attributes (env, deployment id, …)

Attach shared attributes to everything a service sends by setting OTLP **resource attributes** on the sender — one env var, no code:

```bash
OTEL_RESOURCE_ATTRIBUTES=env=prod,prid=wallet
```

| Signal | Behaviour |
|--------|-----------|
| Logs | all resource attributes become event properties (filter: `env = 'prod'`) |
| Traces | all resource attributes are merged into every span's attributes (span's own keys win) |
| Metrics | all resource attributes become series labels (point attributes win). Note: adding a new resource attribute changes series identity, so existing series continue without it and new ones appear with it |

`service.name` is always extracted into the dedicated service field/label, and the SDK's self-description (`telemetry.sdk.*` / `telemetry.distro.*`) is excluded from metric labels — the sender never sets those explicitly, and an SDK upgrade would otherwise fork every series.

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

## Update check (`Ameto:Updates`)

The server polls the GitHub Releases API and surfaces "new version available" in **Settings → Updates** (admin only). On Windows installs and Linux systemd installs the same tab offers a two-step self-update: **Download** (with progress; SHA-256-verified) and then **Install & restart** on explicit approval. Windows runs the installer silently (the service runs as LocalSystem — no UAC); Linux swaps the binaries in place and exits non-zero so systemd's `Restart=on-failure` starts the new build. One conditional (ETag) request per interval; `304 Not Modified` responses don't count against GitHub's rate limit.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `true` | Set `false` on air-gapped hosts to skip the check entirely. |
| `CheckIntervalMinutes` | int | `60` | Minutes between checks (clamped to ≥ 15). |
| `GitHubRepository` | string | `"AMeto-Labs/AMeto-Observability"` | `owner/repo` whose Releases are polled. |

Docker installs can't self-update from inside the container — use a moving tag (`:latest`) plus the Watchtower service documented in `install/docker/docker-compose.example.yml`.

---

## Replication options (`Ameto:Replication`)

Symmetric replication: each node replicates its own flushed cold segments to all healthy peers. No leader election. A node is **healthy** if its last successful ping was within 30 s.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | When `false` the node runs standalone (all replication endpoints/services skipped). |
| `SeedNodes` | string[] | `[]` | Peer base URLs to probe on startup. Further peers are discovered via ping exchange. |
| `LocalAddress` | string | `"http://localhost:5341"` | This node's publicly reachable base URL, used by peers to push segments/pings back. Set to the real hostname when clustering. |
| `ProbeInterval` | TimeSpan | `"00:00:10"` | How often to ping known peers. |
| `PushTimeout` | TimeSpan | `"00:01:00"` | Per-segment HTTP push timeout, applied **separately** to the request/headers exchange and to reading a failed response's body — so a single push can take up to twice this value against a stalled peer. If the body read times out, the response classification is kept and the quoted body is marked `…(body read timed out)` rather than dropped. |
| `Secret` | string | `""` | Shared cluster secret, sent as `X-Ameto-Replication` on every ping and push and checked by every replication endpoint. **Required for any cluster:** with it empty the receiver refuses every replication request with `401`, so nodes configured without it will probe and push forever while replicating nothing. Set the same value on every node, preferably via an environment variable (`Ameto__Replication__Secret`) rather than `config.yml`. |
| `MaxSegmentBytes` | long | `536870912` (512 MB) | Largest segment body this node **accepts** from a peer. Read on the receiving side: a body over it is answered with `413` and nothing is written. It replaces the framework's 30 MB request-body default, which a pushed body can clear: a push carries a hot-tier level segment — merged segments are never offered to peers — and one flush starts from a 64 MB budget (`Ameto:HotTier:MaxSizeBytes`) with LZ4 the only thing between that and the wire. The 512 MB default is larger than anything this system builds, so a legitimate body is never what this limit refuses. |

### Example — two-node cluster

**Node 0**:
```yaml
Ameto:
  NodeId: 0
  HttpPort: 5341
  Replication:
    Enabled: true
    Secret: "one-shared-value-on-every-node"   # without it every ping and push is answered 401
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
    Secret: "one-shared-value-on-every-node"   # must MATCH node 0's
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
  BasePath: ""              # URL prefix, e.g. "/ameto"; empty = served at /
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
    MaxSegmentBytes: 536870912  # 512 MB — largest segment body this node ACCEPTS
```
