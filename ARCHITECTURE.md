# Ameto — Architecture Document

> **For AI assistants**: This document is the single source of truth for the project. Read it fully before making any changes.

---

## Overview

Ameto is a high-performance, self-hosted structured log server — a full alternative to [Datalust Seq](https://datalust.co/docs/posting-raw-events).

**Goals:**
- Ingest **100,000 log events/second** sustained
- Minimal GC pressure (off-heap buffers, zero-copy hot path)
- Full **Seq Filter Expression** query compatibility
- Single-binary deployment per node, supports Leader + Read Replica clustering

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | **.NET 10** |
| Language | **C# 14** (with native interop where needed) |
| HTTP server | **Kestrel** (HTTP/1.1 + HTTP/2) |
| Wire serializer | **MessagePack-CSharp** — dynamic `Dictionary<string, object>` properties |
| Compression | **LZ4** (K4os.Compression.LZ4) for cold-tier segments |
| Bitmap indexes | **RoaringBitmap** (Roaring.Net or similar) |
| Bloom filters | Custom on `NativeMemory` (XOR filter) |
| Ring buffer | Custom MPMC lock-free (`NativeMemory` + `Interlocked`) — LMAX Disruptor pattern |
| Off-heap storage | `NativeMemory` + `mmap` files for hot-tier and WAL |
| UI | **Blazor Server** (.NET 10) |
| Benchmarks | **BenchmarkDotNet** |
| Tests | xUnit |

---

## Project Structure

```
Ameto.sln
├── src/
│   ├── Ameto.Core/
│   ├── Ameto.Ingestion/
│   ├── Ameto.Storage/
│   ├── Ameto.Indexing/
│   ├── Ameto.Query/
│   ├── Ameto.Cluster/
│   ├── Ameto.Alerts/
│   ├── Ameto.Server/
│   └── Ameto.UI/
└── tests/
    ├── Ameto.Core.Tests/
    ├── Ameto.Storage.Tests/
    ├── Ameto.Query.Tests/
    ├── Ameto.Integration.Tests/
    └── Ameto.Perf/
```

---

## Module Responsibilities

### `Ameto.Core`
- `LogEvent` — **struct** (value type, no heap alloc in hot path)
- `LogLevel` — enum: `Verbose | Debug | Information | Warning | Error | Fatal`
- Interfaces: `ILogStorage`, `ILogIndex`, `IQueryExecutor`, `ISegmentWriter`
- MessagePack serialization contracts for `LogEvent` and `Dictionary<string, object>` properties
- Shared value types: `EventId` (ulong), `SegmentId`, `NodeId`

**Key type:**
```csharp
// Stored off-heap; only heap-allocates MessageTemplate and Properties on demand
public readonly struct LogEvent
{
    public ulong Id;              // monotonic, node-local
    public long TimestampUtcTicks;
    public LogLevel Level;
    public int MessageTemplateOffset; // offset into string intern pool
    public int PropertiesOffset;      // offset into raw msgpack block
}
```

### `Ameto.Ingestion`
- **MPMC Ring Buffer**: power-of-2 slots, `NativeMemory`-backed, cache-line padded
- HTTP endpoint: `POST /api/events` — accepts `application/x-msgpack` batch
- Zero-copy parsing: `ReadOnlySequence<byte>` → `Span<byte>`, no intermediate arrays
- Dispatches to: **WAL Writer**, **Hot-Tier Writer**, **Index Writer** (all via ring buffer)
- Back-pressure: HTTP 503 when ring buffer > 90% full

**Ingestion payload format (CLEF-compatible over MessagePack):**
```
Array of events, each event = Map {
  "@t":  timestamp (string ISO8601 or int64 ticks),
  "@mt": message template (string),
  "@l":  level (string, optional, default = Information),
  "@m":  ingest-only fallback for "@mt" (server never re-emits),
  "@x":  exception — either string OR map { type, message?, stack?,
         inner? } recursive up to depth 3,
  ... arbitrary properties (string → any)
}
```

Rendered messages are **not** stored on the server — the client renders
`@mt` + properties on the fly.

### `Ameto.Storage`
Three-tier storage:

#### Hot-Tier
- `NativeMemory` segment, fixed size (configured: 256 MB **or** N minutes — whichever first)
- Stores raw msgpack event bytes contiguously
- In-memory inverted index (dictionary-based, not yet persisted)
- Thread-safe via segment-level spinlock on flush path only

#### WAL (Write-Ahead Log) — `RDWA` v2
- `mmap`-backed append-only file per active segment + companion `.pool`
- Entry header is 20 bytes: `timestamp i64 | level u8 | flags u8 (HasException=1) | _pad u16 | propsLen u32 | exceptionLen u32`
- Followed by UTF-8 message template, msgpack properties, optional msgpack `ExceptionInfo`
- Sequential writes only, `msync` async (no fsync per event)
- Used for crash recovery: on startup, replays WAL if cold segment is incomplete
- WAL file naming: `{node_id}-{segment_id}.wal`

#### Cold-Tier — `.seg` v3 (columnar)
- Immutable `.seg` files on disk; magic `RDLG`, version `3`
- Created by **Segment Flusher** when hot-tier flush triggers
- Each 64 KB block is LZ4-compressed and stores **6 columns** for `BlockEventCount` events:
  1. `@t`  — int64 delta-encoded timestamps
  2. `@l`  — uint8 level
  3. `@i`  — uint64 delta-encoded EventId
  4. `@mt` — UTF-8 string column (n+1 uint32 offsets + bytes)
  5. `@x`  — nullable msgpack `ExceptionInfo` column (offsets + bytes)
  6. `props` — nullable msgpack property-map column (offsets + bytes)
- Block frame: `uint32 uncompSize | uint32 compSize | LZ4 bytes`
- Embedded indexes (see Indexing section) and footer with block index
- File naming: `{node_id}-{segment_id}-{min_ts}-{max_ts}.seg`

> **Exception model:** `ExceptionInfo { Type, Message?, Stack?, Inner? }`
> serialized as msgpack with keys `type/msg/stk/inner`. Inner chains are
> truncated at `MaxDepth = 3`.

**Retention Engine** (background service):
| Level | TTL |
|-------|-----|
| Debug | 3 days |
| Verbose | 90 days |
| Information | 90 days |
| Warning | 90 days |
| Error | 90 days |
| Fatal | 90 days |

- Segments store `min_level` in header
- A segment is deleted only if **all** events in it are expired
- No defragmentation — whole-segment deletion only

### `Ameto.Indexing`
Per-segment indexes, built during flush and embedded in `.seg` files:

| Index | Purpose |
|-------|---------|
| **Inverted index** | `(propertyName, value) → RoaringBitmap(localEventOffset)` |
| **Timestamp skip-list** | O(log n) range queries by time |
| **Bloom filter** (XOR filter) | Per-segment fast-skip: "does this segment possibly contain value X?" |
| **Trigram index** | Full-text search on `@mt` (message template), `@x.type`, `@x.message` |

Hot-tier maintains a live in-memory version of all indexes above.

### `Ameto.Query`
- **Seq Filter Expression** compatible: full lexer + recursive descent parser → AST
- Supported operators: `=`, `<>`, `<`, `>`, `<=`, `>=`, `like`, `not like`, `in`, `not in`, `has`, `is null`, `is not null`, `and`, `or`, `not`
- Built-in properties: `@l` (level), `@t` (timestamp), `@mt`, `@m`, `@x`
- Query executor:
  1. Consults Bloom filter → skip segment entirely if no match possible
  2. Uses inverted index for equality/in predicates → get candidate bitmap
  3. Intersects bitmaps (RoaringBitmap AND/OR)
  4. Fetches and decodes only candidate event blocks (LZ4 decompress)
  5. Final filter: eval AST against decoded `LogEvent`
- Result streaming: `IAsyncEnumerable<LogEvent>` — no full result materialization
- Hot + cold tier queried in parallel, results merged by timestamp

### `Ameto.Cluster`
- **Topology**: Leader (writes) + Read Replicas (reads only)
- **Leader election**: Raft-lite — leader lease + heartbeat (no log replication for election)
- **Replication**:
  - Leader pushes completed cold `.seg` files to replicas after flush
  - Replicas also receive WAL tail for near-realtime hot events (configurable lag)
- **Node registry**: each node broadcasts via UDP multicast or configured static list
- **Health**: `/api/health` endpoint, read replicas redirect writes to leader

### `Ameto.Alerts`
- Alert rule: `{ Name, FilterExpression (Seq syntax), Threshold, Window, Channels }`
- **Evaluator**: sliding window counter per rule, runs on ingestion hot path (non-blocking)
- **Channels**: Webhook (HTTP POST JSON) and SMTP email
- Rules stored in local JSON config file, hot-reloaded

### `Ameto.Server`
- Kestrel composition root, DI wiring
- **REST API**:
  - `POST /api/events` — ingest batch (msgpack)
  - `GET  /api/events?filter=...&from=...&to=...&count=...` — search
  - `GET  /api/events/{id}` — single event
  - `GET  /api/stream?filter=...` — SSE live tail
  - `GET  /api/signals` — list alert rules
  - `POST /api/signals` — create/update alert rule
  - `GET  /api/nodes` — cluster node list
  - `GET  /api/health` — health + metrics
- API key authentication (`X-Api-Key` header), per-app keys
- `application/x-msgpack` preferred, `application/json` fallback

### `Ameto.UI`
- **Blazor Server** (.NET 10)
- Pages:
  - **Live Tail** — real-time event stream with filter bar
  - **Search** — time-range + filter, paginated results
  - **Event Detail** — expanded properties, exception trace
  - **Signals** — alert rule management
  - **Settings** — retention config, nodes, API keys

---

## Hot Path Data Flow (100k/sec)

```
POST /api/events  (msgpack batch, Content-Type: application/x-msgpack)
    │
    ▼  Kestrel pipeline — ReadOnlySequence<byte>, zero-copy
    │
    ▼  MessagePack.Deserialize  (ArrayPool-rented buffers, no heap per event)
    │
    ▼  MPMC Ring Buffer  [NativeMemory, pre-allocated, power-of-2 slots]
    │
    ├──► WAL Writer      (mmap, sequential write, msync async)
    │
    ├──► Hot-Tier Writer (NativeMemory segment, struct LogEvent array)
    │
    └──► Index Writer    (batched updates to live inverted index)
                │
                ▼  flush trigger: 256 MB consumed  OR  N minutes elapsed
           Segment Flusher  (background, non-blocking to hot path)
                │
                ├── serialize inverted index → RoaringBitmap
                ├── build Bloom filter
                ├── build trigram index
                └── write .seg file  (LZ4 blocks + all indexes + footer)
                        │
                        ▼
                 Cold-Tier  (.seg files on disk)
                        │
                        ▼  (async, after flush complete)
                 Replicate to Read Replicas
```

---

## `.seg` File Format

```
Offset  Content
──────  ───────────────────────────────────────────────────────
0       Magic: 0x52_44_4C_47 ("RDLG")
4       Version: uint16
6       NodeId: uint32
10      SegmentId: uint64
18      MinTimestampTicks: int64
26      MaxTimestampTicks: int64
34      EventCount: uint32
38      MinLevel: byte
39      Flags: byte (compressed=1, has_trigram=2, ...)
40      [Event Blocks]
            [Block Header: uncompressed_size uint32, compressed_size uint32]
            [Block Data: LZ4-compressed msgpack event array]
        [Inverted Index Section]
            [property_count uint32]
            [per-property: name_len, name_bytes, value_count,
                           per-value: value_msgpack, bitmap_bytes_len, RoaringBitmap]
        [Trigram Index Section]
            [trigram_count uint32]
            [per-trigram: trigram uint32, bitmap_bytes_len, RoaringBitmap]
        [Bloom Filter Section]
            [filter_bytes_len uint32, XOR filter bytes]
        [Block Index Section]
            [block_count uint32]
            [per-block: file_offset uint64, first_event_id uint64]
        [Footer]
            [inverted_index_offset uint64]
            [trigram_index_offset uint64]
            [bloom_filter_offset uint64]
            [block_index_offset uint64]
            [footer_magic: 0x52_44_46_54 "RDFT"]
```

---

## GC / Memory Strategy

| Technique | Where applied |
|-----------|--------------|
| `NativeMemory.Alloc` | Ring buffer slots, hot-tier event storage |
| `mmap` files | WAL, read-path of cold segments (OS-managed paging) |
| `ArrayPool<byte>` | HTTP read buffers, msgpack deserialization scratch, LZ4 decompress |
| `Span<T>` / `ReadOnlySequence<byte>` | All parsing, zero intermediate arrays |
| `struct LogEvent` | Value type, stored in unmanaged arrays |
| Pre-allocated ring buffer | No allocation on hot ingestion path after startup |
| Batch index updates | Index writer processes ring buffer in batches, amortizes allocs |
| `MemoryPool<byte>` | SSE / streaming response buffers |

**Target**: GC Gen0 collections < 1/sec during sustained 100k events/sec ingestion.

---

## Retention Config (appsettings.json)

```json
{
  "Retention": {
    "Rules": [
      { "Level": "Debug",       "MaxAgeDays": 3  },
      { "Level": "Verbose",     "MaxAgeDays": 90 },
      { "Level": "Information", "MaxAgeDays": 90 },
      { "Level": "Warning",     "MaxAgeDays": 90 },
      { "Level": "Error",       "MaxAgeDays": 90 },
      { "Level": "Fatal",       "MaxAgeDays": 90 }
    ]
  },
  "HotTier": {
    "MaxSizeBytes": 268435456,
    "MaxAgeMinutes": 5
  },
  "Cluster": {
    "Role": "Leader",
    "Replicas": []
  }
}
```

---

## Implementation Order

1. `Ameto.Core` — models, interfaces, MessagePack contracts
2. `Ameto.Storage` — hot-tier, WAL, cold-tier, retention
3. `Ameto.Indexing` — inverted index, bloom, trigram, RoaringBitmap
4. `Ameto.Ingestion` — ring buffer, HTTP endpoint
5. `Ameto.Query` — Seq Filter Expression parser + executor
6. `Ameto.Server` — Kestrel host, REST + SSE API
7. `Ameto.Alerts` — alert rules + dispatcher
8. `Ameto.Cluster` — leader election + replication
9. `Ameto.UI` — Blazor Server app
10. `Ameto.Perf` — BenchmarkDotNet suite

---

## Non-Goals (out of scope)

- Multi-tenancy (single org)
- Metrics / traces ingestion (logs only)
- Plugin system
- Kubernetes operator
