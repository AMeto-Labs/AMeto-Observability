# REST API Reference

Base URL: `http://localhost:5341` (configurable via `Ameto.HttpPort`).

All responses are JSON unless noted.

## Authentication

### Ingest endpoint (`POST /api/events`)

Uses an **API key** validated from the in-memory `ApiKeyCache` (no JWT, no DB hit on hot path).

Pass the key in any of these ways:

```
X-Seq-ApiKey: <key>
Authorization: apikey <key>
?apiKey=<key>
```

### All other endpoints

Use **JWT Bearer**. Obtain a token via `POST /api/auth/login`, then:

```
Authorization: Bearer <token>
```

SSE endpoints (`GET /api/events`, `GET /api/events/live`) also accept `?access_token=<token>` because browsers cannot set `Authorization` headers on `EventSource`.

---

## Auth

### `POST /api/auth/login`

Obtain a JWT token (expires in 8 h).

**Body:**
```json
{ "username": "admin", "password": "123123" }
```

**Response `200 OK`:**
```json
{ "token": "<jwt>", "expiresIn": 28800 }
```

### `POST /api/auth/refresh`

Refresh the current token (requires valid Bearer token).

**Response `200 OK`:** same shape as login.

---

## Users (admin role required)

### `GET /api/users`

List all users.

### `POST /api/users`

Create a user.

**Body:**
```json
{ "username": "bob", "password": "s3cret", "role": "manager" }
```

Roles: `admin`, `manager`.

**Response `200 OK`:** created user object.  
**Response `409 Conflict`:** username already exists.

### `DELETE /api/users/{id}`

Delete a user by ID. Cannot delete your own account.

**Response `204 No Content`** on success.

---

## API Keys

### `GET /api/auth/keys`

List all API keys. The full key is never returned after creation — only an 8-character preview.

### `POST /api/auth/keys`

Create an API key.

**Body:**
```json
{ "name": "serilog-prod", "key": null }
```

Omit `key` (or pass `null`) to auto-generate a `rdl_`-prefixed key. Provide a custom value to use your own.

**Response `200 OK`:**
```json
{
  "id": "abc12345",
  "name": "serilog-prod",
  "key": "rdl_AAAA...",
  "createdBy": "admin",
  "createdAt": "2026-05-20T10:00:00Z"
}
```

The full key is returned **only here** — store it now.

### `DELETE /api/auth/keys/{id}`

Delete an API key by ID.

**Response `204 No Content`** on success.

---

## Ingestion

### `POST /api/events`

Ingest a batch of log events.

**Auth:** API key (see above).  
**Content-Type:** `application/x-msgpack`  
**Body:** MessagePack array of CLEF maps. Max body size: 4 MB.

| Field | Type | Description |
|-------|------|-------------|
| `@t` | ISO-8601 string | Timestamp (UTC). |
| `@mt` | string | Message template, e.g. `"User {UserId} logged in"`. |
| `@l` | string | Level: `Verbose`, `Debug`, `Information`, `Warning`, `Error`, `Fatal`. |
| `@m` | string | Ingest-only fallback for `@mt`. Never re-emitted by the server. |
| `@x` | string or object | Exception. String is auto-wrapped; or `{type, message?, stack?, inner?}` recursive up to depth 3. |
| *(any)* | any | Structured properties. |

**Response `200 OK`:**
```json
{ "ingested": 42, "dropped": 0 }
```

**Response `413 Payload Too Large`:** body > 4 MB.  
**Response `400 Bad Request`:** invalid MessagePack.

### `POST /otlp/v1/logs`, `POST /otlp/v1/traces`, `POST /otlp/v1/metrics`

Native OpenTelemetry ingestion — point any OTLP exporter here (no collector required).

**Auth:** API key (same as `/api/events`).  
**Content-Type:** `application/json` (OTLP/JSON) or `application/x-protobuf` (OTLP/Protobuf).  
**Body:** the corresponding OTLP `Export…ServiceRequest` (`resourceLogs` / `resourceSpans` / `resourceMetrics`). Max body: 8 MB (`Ingestion.MaxOtlpBatchBytes`).

**Response `200 OK`:** `{ "ingested": N, "dropped": M }`.  
`resource.attributes["service.name"]` becomes the event's service; `traceId` / `spanId` are indexed for log↔trace correlation.

---

## Query

### `GET /api/events` (SSE stream)

**Auth:** JWT Bearer (or `?access_token=`).

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `filter` | string | *(match all)* | Seq Filter Expression. |
| `from` | ISO-8601 | *(unbounded)* | Start of time range (inclusive). |
| `to` | ISO-8601 | *(unbounded)* | End of time range (inclusive). |
| `count` | int | `500` | Max events to return (1–10 000). |
| `dir` | `backward`\|`forward` | `backward` | Sort direction. |
| `afterId` | string | *(none)* | Keyset pagination cursor — raw `EventId` uint64 from a previous response. |
| `afterTs` | long | *(none)* | Timestamp ticks paired with `afterId`. |
| `levels` | string | *(all)* | Comma-separated level filter, e.g. `Error,Fatal`. |

**Response:** `Content-Type: text/event-stream`. Each frame: `data: <json>\n\n`. Ends with `event: done\ndata: {}\n\n`.

Event JSON:
```json
{
  "@t":    "2026-05-20T10:00:00.0000000+00:00",
  "@mt":   "Request {Path} failed",
  "@l":    "Error",
  "@x":    { "type": "System.InvalidOperationException", "message": "Boom", "stack": "...", "inner": null },
  "id":    "123456789",
  "props": { "Path": "/api/users", "StatusCode": 500 }
}
```

### `GET /api/events/live` (SSE live tail)

**Auth:** JWT Bearer (or `?access_token=`).

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `filter` | string | *(match all)* | Filter expression applied to new events. |
| `from` | ISO-8601 | now | Tail start time. |

Streams matching events continuously. Sends `: keepalive` comments every 250 ms when idle to keep the connection open.

### `GET /api/events/props`

Returns a sorted array of distinct property names seen in the last 24 h (up to 5 000 events sampled).

**Auth:** JWT Bearer.

### `GET /api/events/services`

Returns the distinct `service.name` values seen, for the services dropdown/filter.

**Auth:** JWT Bearer.

### `GET /api/events/counts`

Header-only log-volume aggregation: bucketed `(time, level[, service])` counts over a time range, without materialising events (powers the volume histogram). Params: `from`, `to`, `filter`, `gb` (group-by), bucketing hints.

**Auth:** JWT Bearer. **Response:** JSON `{ buckets, series, … }`.

---

## Search history

Per-user recent + pinned filter queries (shown in the events Signals panel).

### `GET /api/search-history`
Returns `{ pinned: string[], recent: string[] }` for the caller.

### `POST /api/search-history`
Record a query — body `{ "query": "@l='Error'" }`. → `204`.

### `PUT /api/search-history/pin`
Pin/unpin — body `{ "query": "…", "pinned": true }`. → `204`.

### `DELETE /api/search-history?query=…`
Remove one entry. → `204`.

All require JWT Bearer.

---

## Statistics

### `GET /api/stats`

**Auth:** JWT Bearer.

**Response `200 OK`:**
```json
{
  "segments": 7,
  "totalEvents": 462345,
  "compressedBytes": 134217728
}
```

---

## Diagnostics

### `GET /api/diagnostics`

Server health snapshot.

**Auth:** JWT Bearer.

**Response `200 OK`:**
```json
{
  "diskFreeBytes": 10737418240,
  "diskTotalBytes": 107374182400,
  "systemRamPercent": 42,
  "ramTargetPercent": 85,
  "processWorkingSetBytes": 134217728,
  "processThreads": 18,
  "processStartedAt": "2026-05-20T09:00:00Z",
  "segmentCount": 7,
  "totalEventCount": 462345,
  "totalCompressedBytes": 134217728
}
```

---

## Retention

### `GET /api/retention`

Returns current per-level retention settings (days).

**Auth:** JWT Bearer.

### `PUT /api/retention`

Updates and persists retention settings to SQLite.

**Auth:** JWT Bearer, role `admin` — shortening a window deletes stored data irreversibly.

**Body:**
```json
{
  "verboseDays": 90,
  "debugDays": 3,
  "informationDays": 90,
  "warningDays": 90,
  "errorDays": 90,
  "fatalDays": 90,
  "metricsDays": 30,
  "tracesDays": 14
}
```

**Response `200 OK`:** updated retention object.

### `POST /api/retention/run`

Force-runs retention enforcement immediately.

**Auth:** JWT Bearer, role `admin` — this deletes every segment past the current horizon.

**Response `200 OK`:** enforcement result summary.

---

## Queries

Saved filter expressions. All endpoints require JWT Bearer auth.

A query can be **shared** (visible to all authenticated users) or **private** (visible only to the owner and admins).

### `GET /api/queries`

Returns all shared queries **plus** the caller's own private queries, sorted by name.

### `GET /api/queries/{id}`

Returns a single query if the caller is allowed to see it (shared, owner, or admin role).  
**`403 Forbidden`** if the query is private and the caller is not the owner/admin.

### `POST /api/queries`

Create a new saved query. `OwnerId` is set automatically from the JWT.

**Body:**
```json
{
  "name":        "All errors today",
  "filter":      "@l = 'Error'",
  "description": "Quick-access for production triage",
  "isShared":    true
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Display name. |
| `filter` | no | Seq Filter Expression. Empty matches everything. |
| `description` | no | Free-text label shown in the UI. |
| `isShared` | no (default `false`) | `true` → visible to all users; `false` → private. |

**Response `201 Created`:** created query object.

### `PUT /api/queries/{id}`

Update a saved query. Only the owner or an `admin` may update.

**Body:** same shape as `POST`.  
**Response `200 OK`:** updated query, **`403`** if not owner/admin, **`404`** if not found.

### `DELETE /api/queries/{id}`

Delete a saved query. Only the owner or an `admin` may delete.

**Response `204 No Content`** on success, **`403`** if not owner/admin, **`404`** if not found.

---

## Traces

Distributed-tracing query surface (spans ingested via OTLP). All require JWT Bearer.

| Endpoint | Purpose |
|----------|---------|
| `GET /api/traces` | List/search traces (root spans) by service, name, tag filter, duration, time range. |
| `GET /api/traces/query` | Same, richer query params (tag expressions like `{ db.system = 'mssql' && duration > 200ms }`). |
| `GET /api/traces/stats` | Aggregate trace stats (counts, error rate, latency) over a window. |
| `GET /api/traces/latency` | Latency distribution / percentiles by service or operation. |
| `GET /api/traces/service-graph` | Service dependency graph (edges + call counts) inferred from spans. |
| `GET /api/traces/compare` | Compare two traces / time windows. |
| `GET /api/traces/{traceId}` | Full span tree for one trace. |
| `GET /api/traces/{traceId}/flamegraph` | Flamegraph layout for the trace. |
| `GET /api/traces/{traceId}/logs` | Logs correlated to the trace (via `@tr`). |
| `GET /api/spans/{spanId}/logs` | Logs correlated to a single span (via `@sp`). |

---

## Metrics

Metric query surface (points ingested via OTLP). All require JWT Bearer.

| Endpoint | Purpose |
|----------|---------|
| `GET /api/metrics/names` | Distinct metric names. |
| `GET /api/metrics/catalog` | Metric catalog (name, type, unit, label keys). |
| `GET /api/metrics/query` | Time-series query: `metric`, `agg` (avg/sum/min/max/count), `gb` (group-by label), `filters`, time range. |
| `GET /api/metrics/expr` | Evaluate a metric expression (arithmetic over series). |
| `GET /api/metrics/{name}` | Series for one metric. |
| `GET /api/metrics/{name}/labels` | Label keys for a metric; `…/labels/{key}/values` for a key's values. |
| `GET /api/metrics/{name}/heatmap` | Heatmap buckets for a histogram/gauge metric. |
| `GET /api/metrics/{name}/exemplars` | Exemplars (trace-linked sample points) for a metric. |

---

## Alerts

Threshold rules over logs/metrics with webhook/SMTP dispatch and silences. Mapped under the alerts group; all require JWT Bearer. Key endpoints: rule CRUD (`GET/POST/PUT/DELETE`), `GET …/state` (firing state), `POST …/{id}/ack`, `POST …/test`, `POST …/preview`, `…/silences`, `…/maintenance`, `…/history`.

Three privilege tiers:

| Tier | Role | Endpoints |
|------|------|-----------|
| Read | any signed-in user | `GET` rule list/detail, `…/state`, `…/history`, `…/silences`, `…/maintenance` |
| Ops | `manager`+ | `POST/DELETE …/silences`, `POST/PUT/DELETE …/maintenance`, `POST/DELETE …/{id}/ack` |
| Manage | `admin` | `POST/PUT/DELETE` a rule, `POST …/test`, `POST …/preview` |

Rule writes are admin-only because a rule owns its channels, and a channel holds a credential and names the host the server dials on dispatch.

**Secrets in channels.** Every response redacts channel secrets to `********`. Sending that sentinel back on an upsert means "unchanged" and the stored value is merged in — but only while the channel's destination is unchanged too (webhook URL, SMTP host/port, Telegram chat id, or the whole HTTP-flow step list). Moving the destination while leaving a secret masked returns `400`; re-send the secret to point a channel somewhere new.

---

## Auth providers (OAuth)

| Endpoint | Purpose |
|----------|---------|
| `GET /api/auth/providers` | Configured OAuth providers (for the login screen). |
| `GET /api/auth/oauth/{provider}` | Begin the OAuth flow for a provider. |
| `GET/POST/DELETE /api/users/oauth-domains` | Admin: manage the OAuth email-domain allowlist / auto-provisioning. |

Per-user **view permissions** (Logs / Metrics / Traces / Stats) are set on the user object (`POST/PUT /api/users`) and enforced server-side per request.

---

## Replication

All replication endpoints are registered only when `Ameto.Replication.Enabled = true`.

### `GET /api/replication/nodes`

List all known peer nodes and their health status.

**Auth:** JWT Bearer.

**Response `200 OK`:**
```json
[
  {
    "id": 1,
    "address": "http://node1:5341",
    "lastSeenUtc": "2026-05-20T10:05:00Z",
    "healthy": true
  }
]
```

### `POST /api/replication/ping`

Peer-to-peer presence exchange. **Auth:** the `X-Ameto-Replication` header must carry the configured replication secret; a blank secret fails closed, so an enabled-but-unconfigured node accepts nothing.

**Body:**
```json
{ "nodeId": 1, "address": "http://node1:5341", "timestamp": "2026-05-20T10:05:00Z" }
```

**Response `200 OK`:** this node's own `PeerPayload`.

### `POST /api/replication/segments/{nodeId}/{segmentId}`

Receive a replicated cold-tier segment from a peer. **Auth:** the `X-Ameto-Replication` header must carry the configured replication secret (same as `/ping`).

**Body:** raw `.seg` file bytes (`application/octet-stream`).

| Status | Meaning | Retry? |
|---|---|---|
| `204 No Content` | Registered. A re-push of a segment this node already holds also returns 204: the entry and the bytes already held are **kept** and the pushed body is discarded — the push is idempotent, not a refresh. The header carries no digest, so "already holds" is decided on the header fields; keeping the served copy is the safe side of that comparison. | — |
| `400 Bad Request` | Either the route did not bind — `{nodeId}` is a `uint` and `{segmentId}` a `ulong`, and a URL carrying anything else is rejected by model binding **before the secret is checked**, with no body of ours — or the body was read but is not a segment file, in which case nothing was registered and the received bytes were discarded. | No — neither the URL nor the bytes change on their own. |
| `401 Unauthorized` | The `X-Ameto-Replication` header is missing or does not match the receiver's configured secret. Note the row above: an unroutable URL never reaches this check. | No. |
| `409 Conflict` | Three causes, and the response body names which one it was. Two mean two nodes are configured with the same `NodeId`: a **different** segment already holds this `(nodeId, segmentId)` on the receiver, or the id falls inside the span the receiver's own allocator has handed out — a local flush, merge or WAL replay holds it or is about to publish under it. The third says nothing about NodeId: the path is occupied by a file the receiver **could not read**, so it refused to adjudicate and kept what is on disk. In every case the pushed segment was **not** registered. | For the two NodeId causes: no — permanent until one of the nodes is renumbered. For the unreadable-incumbent cause: yes, after the file on the receiver is removed or repaired. |
| `413 Content Too Large` | The body is larger than `Ameto:Replication:MaxSegmentBytes` on the **receiver** (default 512 MB). Nothing was written. | No — the same bytes are the same size. Raise the limit on the receiver, or the sender will never place this segment. |
| `500` | The receiver could not take the body: a disk error writing or placing the file, or a body that stopped arriving mid-push. | Yes — both causes are transient, and the same body over a healthy connection will land. |

Segment ids are monotonic **per node**, so `(nodeId, segmentId)` — not `segmentId` alone —
identifies a segment across a cluster. A 409 is a deployment fault rather than a push failure:
the receiver cannot tell which of two senders claiming one `NodeId` is the stranger, so it
refuses the second and reports it to the sender, which is the only party positioned to tell a
duplicate-NodeId deployment from a healthy push.

The pair a 409 is decided on comes from the **file header**, not from the route. The receiver
reads the segment it was sent and takes `(nodeId, segmentId)` out of it, so a sender that
addresses a body to a URL the body itself disagrees with gets a verdict about what it sent — and
the message names the id in the URL, which is then not the id the receiver compared. A push
built by `SegmentReplicator` always addresses the segment it is sending, so route and header
agree; a hand-made push need not, and this is the one place that shows.

`413` is worth setting deliberately rather than leaving to the framework, whose own default is
30 MB. What a peer pushes is a hot-tier **level segment** — replication is triggered by a flush
publishing and by nothing else, so merged segments are not offered to peers — and one flush
starts from a 64 MB budget (`Ameto:HotTier:MaxSizeBytes`, configurable upward) with LZ4 the only
thing between that and the wire, so 30 MB is cleared without anything unusual happening. The
default ceiling of 512 MB is the merge target measured before compression: larger than anything
this system builds, so a legitimate body is never what it refuses. The receiver raises the limit
for this endpoint alone, and only after the secret matches.

The `Retry?` column states what a sender **should** do, not what this one does. `SegmentReplicator`
pushes each segment once per healthy peer, fire-and-forget; a non-success status is logged as a
warning and that segment is not offered to that peer again. So `500` is retryable in the sense
that the same bytes would land, not in the sense that this implementation will resend them.

---

## Health

### `GET /health`

No auth required. Always returns `200 OK`:

```json
{ "status": "ok", "utc": "2026-05-20T10:00:00Z" }
```
