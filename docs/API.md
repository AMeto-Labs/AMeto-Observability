# REST API Reference

Base URL: `http://localhost:5341` (configurable via `RdLog.HttpPort`).

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
  "ramTargetPercent": 99,
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

**Auth:** JWT Bearer.

**Body:**
```json
{
  "verboseDays": 3,
  "debugDays": 3,
  "informationDays": 90,
  "warningDays": 90,
  "errorDays": 90,
  "fatalDays": 90
}
```

**Response `200 OK`:** updated retention object.

### `POST /api/retention/run`

Force-runs retention enforcement immediately.

**Auth:** JWT Bearer.

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

## Replication

All replication endpoints are registered only when `RdLog.Replication.Enabled = true`.

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

Peer-to-peer presence exchange. **No auth required** (peer-to-peer).

**Body:**
```json
{ "nodeId": 1, "address": "http://node1:5341", "timestamp": "2026-05-20T10:05:00Z" }
```

**Response `200 OK`:** this node's own `PeerPayload`.

### `POST /api/replication/segments/{nodeId}/{segmentId}`

Receive a replicated cold-tier segment from a peer. **No auth required** (peer-to-peer).

**Body:** raw `.seg` file bytes (`application/octet-stream`).

**Response `204 No Content`** on success.

---

## Health

### `GET /health`

No auth required. Always returns `200 OK`:

```json
{ "status": "ok", "utc": "2026-05-20T10:00:00Z" }
```