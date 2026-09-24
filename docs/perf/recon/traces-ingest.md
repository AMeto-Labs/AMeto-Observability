# Trace ingest path — CPU / allocation / resident-memory reconnaissance (worktree AMeto-recon-traces-ingest @ main 0a7389d)

Paths traced end to end:

* **OTLP/JSON** — `OtlpEndpointMapper.cs:52-98` (`/v1/traces`, `/otlp/v1/traces`) → `OtlpBodyReader.ReadAsync` (pooled) → `OtlpTraceStreamParser.Parse` → `List<SpanIngestItem>` → `SpanIngestionEndpoint.TryIngest` → `SpanRingBuffer` → `SpanDrainer` → `TraceStorageEngine.WriteSpan` → `SpanWriteAheadLog.Append` + `AddToHotTierLocked`.
* **OTLP/protobuf over HTTP** — same route, `OtlpEndpointMapper.cs:67-69`: `OtlpProtoDecoder.DecodeTraces` (DOM) → `OtlpTraceMapper.Map` → same tail.
* **OTLP/protobuf over gRPC** — `OtlpGrpcEndpointMapper.cs:53-63` → `OtlpGrpcFraming.TryUnframe` → `MessageSegment(… decodeReadsFromZero: true)` → the same DOM decoder → same tail.
* **Zipkin / Jaeger / anything else: none.** `grep -rn "MapPost|MapGet" src/` over every mapper returns exactly six trace-capable routes (the four OTLP/HTTP spellings and the two gRPC Export paths, `Program.cs:460-465`). There is no second trace receiver to audit.

**Prior work verified still in place (do not re-do):**
`IngestBufferPool` + `OtlpBodyReader` carry both trace routes' bodies (`OtlpEndpointMapper.cs:230-239`, `OtlpGrpcEndpointMapper.cs:122`), so the LOH-per-request item from the logs round does not recur here. gRPC gzip inflates into a size-hinted pooled buffer (`OtlpGrpcFraming.cs:107-116`) over the caller's array — no `ToArray()`. The `{"ingested":…}` reply is `Utf8Formatter` into `BodyWriter.GetSpan` (`OtlpEndpointMapper.cs:249-272`) — 0 B. `Program.cs:338` branches `UseRateLimiter/UseAuthentication/UseAuthorization` around ingest POSTs, and `/v1/traces` is in `IngestRoutes.Paths` (`Program.cs:463-464`), so JwtBearer does not run on this route. `ApiKeyCache` uses `Convert.ToHexStringLower` (`ApiKeyCache.cs:140`). `AmetoIngestEndpoints.Matches(ReadOnlySpan<byte>)` is allocation-free (`AmetoIngestEndpoints.cs:71-78`). The JSON streaming parser already exists and already avoids the OTLP object graph. **Nothing indexes, blooms or writes a sidecar per span** — `AddToHotTierLocked` touches only `_hotSpans`, `_traceIdx` and `_hotSince`; `_index`, `TraceSummarySidecar`, `ServiceGraphSidecar` and `SpanStats` are all flush-time only.

---

## Baseline numbers measured at main @ 0a7389d

All Release, this machine, foreground. Commands under "Existing probes" below.

| Stage | ns per span | B allocated / span | Notes |
|---|---|---|---|
| OTLP/JSON `OtlpTraceStreamParser.Parse` | **1 081** | **330** | 200 spans, 6 attrs, 120.6 KB body; best of 7 rounds × 200 iters |
| OTLP/proto `DecodeTraces` only | **3 012** | **6 821** | 200 spans, 6 attrs, 60.0 KB body |
| OTLP/proto `DecodeTraces` + `OtlpTraceMapper.Map` | **5 428** / 5 306 | **7 881** | existing `OtlpTraceProtoProbe`, two runs |
| `TraceStorageEngine.WriteSpan`, 8 attrs (222 B msgpack) | **4 557** | **1 399** alloc, **1 002 retained** | 38 000 spans, 10 spans/trace, index off |
| `TraceStorageEngine.WriteSpan`, 0 attrs | **293** | **151** alloc, **15 retained** | same shape, `AttributesBytes = []` |
| *reference:* `OtlpLogProtoParser` (the target shape) | 1 457 | **0** | existing `OtlpLogProtoProbe` |
| *reference:* logs proto DOM (what #79 replaced) | 6 842 | 8 031 | same probe |

Derived:

* **End-to-end protobuf: ≈ 9 985 ns/span** (5 428 decode on a request thread + 4 557 on the one drain thread) → **≈ 100 k spans/s** with a request core *and* the drainer both saturated.
* **End-to-end JSON: ≈ 5 638 ns/span**, drainer-bound at **219 k spans/s**.
* **Attribute deserialisation alone is 4 264 ns/span and 987 B/span retained** (4 557 − 293; 1 002 − 15). It is 94 % of `WriteSpan`.
* Span WAL on disk: **419 B/span** with 8 attrs (16.0 MB / 40 000), **209 B/span** bare (8.0 MB / 40 000).
* Hot tier at its flush threshold (`HotFlushThreshold = 50 000`, `TraceStorageEngine.cs:294`): **≈ 50 MB resident**, and up to **≈ 100 MB** while `_flushingSpans` still holds the previous snapshot (`TraceStorageEngine.cs:1720-1725`) and a new tier fills behind it. On the console.ntpayments 512 MB container that is 10–20 % of the process for the hot tier alone, before the 48 MB index cache and the 120 000-span compaction ceiling (`MaxSpansPerPass`, `TraceStorageEngine.cs:350`, ≈ 210 MB by the file's own comment).

---

## Findings (ranked by expected impact)

### 1. OTLP/protobuf traces still decode to a DOM — CONFIRMED (CPU + alloc, per span and per attribute)

`OtlpEndpointMapper.cs:63-69`, `OtlpGrpcEndpointMapper.cs:53-63`, `OtlpProtoDecoder.cs:60-73,289-379,451-514`, `OtlpTraceMapper.cs:14-109,149-188`.

MEASURED: **5 428 ns/span, 7 881 B/span** vs 1 081 ns / 330 B on the JSON path for the *same* span shape. The protobuf path — the one every SDK exporter and the collector actually use — is **5× slower and allocates 24× more** than the JSON path nobody uses in production.

Mechanism, per span: `SubStream()` (`OtlpProtoDecoder.cs:39-40`) is `ReadBytes().CreateCodedInput()` — one `byte[]` copy plus a `ByteString` plus a `CodedInputStream` for **every** nested message. For a 6-attribute span that is ScopeSpans + Span + 6 × KeyValue + 6 × AnyValue + Status = 15 nested messages ≈ 45 objects, every attribute byte copied 3–4×. On top:

* `HexFromBytes` (`:513-514`) = `Convert.ToHexString(...).ToLowerInvariant()` — **2 strings per id × 3 ids = 6 strings/span** (32-, 16-, 16-char), immediately re-parsed character-by-character by `TraceId.TryParseHex`/`SpanId.TryParseHex` (`SpanRecord.cs:50-58,110-123`). The raw wire bytes are already big-endian binary; the whole hex round trip is pure waste.
* `ReadFixed64().ToString()` for `start_time_unix_nano` and `end_time_unix_nano` (`:337-338`), re-parsed by `long.TryParse` in `ParseNanoString` (`OtlpTraceMapper.cs:301-305`) — 2 strings/span.
* `ReadInt64().ToString()` per int attribute (`:507`), re-parsed by `long.TryParse` (`OtlpTraceMapper.cs:231`).
* `cis.ReadString()` per key and per string value — ~13 strings/span at 6 attributes, plus `span.Name`.
* **`OtlpSpanEvent` objects are materialised and thrown away.** `ReadSpan` case 90 (`:340`) decodes every `events[]` entry into an `OtlpSpanEvent` with its own attribute `List<OtlpKeyValue>` — and `OtlpTraceMapper.MapSpan` never reads `span.Events`. Same for `status.Message` (`:356`) and `ReadScope`'s name/version (`:463-476`). `Traces_Realistic` has no events, so **the 7 881 B/span figure understates real SDK traffic**: an error span carries an `exception` event with 3–4 attributes ≈ another 12 objects and ~600 B, all discarded.
* Then `OtlpTraceMapper.SerializeAttributes` (`:149-188`) walks it all again, sizes an `ArrayBufferWriter`, transcodes every UTF-16 key/value back to UTF-8, and `WrittenMemory.ToArray()`s it into a fresh `byte[]`.

**Fix — `OtlpTraceProtoParser` on `ProtoReader`, mirroring `OtlpLogProtoParser` exactly.** Two passes over each `ResourceSpans` body (`OtlpLogProtoParser.cs:154-182`) so resource attributes are final before any span is read — proto does not guarantee field order. Field map (tag = (field << 3) | wire):

| Message | tag | field | action |
|---|---|---|---|
| ExportTraceServiceRequest | 10 | 1 resource_spans | recurse |
| ResourceSpans | 10 / 18 | 1 resource / 2 scope_spans | pass 1 / pass 2 |
| ScopeSpans | 18 | 2 spans | recurse (skip 10 scope, 26 schema_url) |
| Span | 10, 18, 34 | 1 trace_id, 2 span_id, 4 parent_span_id | keep the raw slices |
| Span | 42 | 5 name | `WriteUtf8`-validated span |
| Span | 48 | 6 kind | `(SpanKind)(varint & 0x07)` |
| Span | 57 / 65 | 7 / 8 start / end fixed64 | `ulong`; `≤ long.MaxValue ? (long)v : 0` |
| Span | 74 | 9 attributes | `TryWriteKeyValue` + promotion hooks |
| Span | 122 | 15 status | read field 3 (tag 24) only |
| Span | 26, 80, 90, 96, 106, 112, 133 | trace_state, dropped counts, **events**, **links**, flags | `SkipField` — never materialised |
| Status | 24 | 3 code | 1→Ok, 2→Error, else Unset (skip tag 18 message) |
| KeyValue | 10 / 18 | 1 key / 2 value | as in the log parser |
| AnyValue | 10,16,24,33,42,50 | string/bool/int/double/array/kvlist | see risk 4 |

Ids go `BinaryPrimitives.ReadUInt64BigEndian` straight off the slice (`OtlpLogProtoParser.cs:263-269`) — no hex anywhere on the trace path, because unlike logs the ids are *columns*, not `@tr`/`@sp` map entries. Attributes go through the same `[ThreadStatic] ArrayBufferWriter` triple (res / span / out) the JSON parser already uses.

Expected: **≈ 1 200–1 500 ns/span** (the logs round got 6 842 → 1 457, 4.7×) and 330 B/span — dropping to ~0 B/span once #3 lands. `decodeReadsFromZero` (finding #7) dies with it.

**Parity risks — what tests must pin:**
1. **Attribute order and the map header.** Resource pairs first, span pairs second, `WriteMapHeader(resCount + spanCount)` (`OtlpTraceMapper.cs:174-185`). Span attrs win on collision because they are written last.
2. **`service.name` is EXCLUDED from the pairs** on the trace path (`OtlpTraceMapper.cs:55,63`) — the opposite of the logs path, where it stays in the props map. Absent → the literal `"unknown"` (`:142,146`).
3. **First-wins vs last-wins on a duplicate `service.name`.** `OtlpTraceMapper.ExtractServiceName` returns the FIRST (`:143-146`); `OtlpTraceStreamParser.ReadResourceAttributes` overwrites, so JSON takes the LAST (`OtlpTraceStreamParser.cs:156`). Pick first-wins (`ServiceSeen`, `OtlpLogProtoParser.cs:78-79`) and add the case to the JSON parity test too — this is an existing JSON/DOM divergence nobody has pinned.
4. **array_value / kvlist_value are currently DROPPED on the protobuf path.** `OtlpProtoDecoder.ReadAnyValue` (`:495-509`) handles only fields 1–4; 42 and 50 fall to `SkipLastField`, leaving an all-null `OtlpAnyValue`, which `WriteAnyValue` (`OtlpTraceMapper.cs:253`) writes as **nil**. Encoding them is a deliberate divergence in the client's favour, exactly as `OtlpLogProtoParityTests` asserts for logs (see its class comment) — assert it, do not paper over it.
5. **Invalid UTF-8.** `CodedInputStream.ReadString()` replaces each invalid sequence with U+FFFD; a raw byte copy would store invalid UTF-8 in msgpack. Reuse `WriteUtf8` / `WriteReplacingInvalid` verbatim (`OtlpLogProtoParser.cs:504-528`) — this is the #79 fix and it applies unchanged.
6. **Depth cap.** `WriteArrayValue`/`WriteKvlistValue` recurse into `WriteAnyValue`; without a bound a small POST of nested `array_value` is a stack overflow — process death, no exception. Copy `MaxValueDepth = 64` + `EnterValue` (`OtlpLogProtoParser.cs:95-108,461-473`). The JSON parser gets this free from `Utf8JsonReader`'s default `MaxDepth`; a hand-rolled proto reader does not.
7. **Drop rules.** trace_id ≠ 16 bytes or span_id ≠ 8 bytes → drop the span (the DOM reaches the same outcome via `TryParseHex`'s exact-length check). parent_span_id: parse only at exactly 8 bytes, else `default`. `kind == 3 && IsAmetoInternalSpan` → drop (`OtlpTraceMapper.cs:81-82,123-138`); use `AmetoIngestEndpoints.Matches(ReadOnlySpan<byte>)` as the JSON parser does. **Note the existing hole:** the byte overload refuses any URL over 512 bytes (`AmetoIngestEndpoints.cs:74`) while the char overload the DOM uses has no cap — so a >512-byte self-ingest URL is dropped by JSON and kept by protobuf today. Follow the JSON behaviour and record the divergence.
8. **HTTP status promotion.** New key `http.response.status_code` beats old `http.status_code` regardless of order, both string and int forms, clamped to `short` (`OtlpTraceStreamParser.cs:456-463` vs `OtlpTraceMapper.cs:281-299`).
9. **Duration** `end > start ? end - start : 0`; a fixed64 past `long.MaxValue` must land as 0, matching the `long.TryParse` of the stringified value.
10. **Partial-batch semantics.** Today the whole batch is built then ingested, so a malformed tail yields 400 with nothing stored. A streaming parser leaves a prefix in the ring — which is fine here and needs no `NotifyBatchEnqueued` equivalent, because `SpanRingBuffer.TryEnqueue` releases the drainer's semaphore per item (`SpanRingBuffer.cs:111-115`). Say so in the code so nobody adds one.

**Measure:** extend `tests/Ameto.Perf/OtlpProtoPayloads.cs` `Span(i)` with an `events[]` entry (field 11) and an `array_value` attribute, add `OtlpTraceProtoParityTests` against `DecodeTraces` + `OtlpTraceMapper.Map` comparing every `SpanIngestItem` field and `AttributesBytes` **byte for byte**, and extend `OtlpTraceProtoProbe` with the parser arm (copy `OtlpLogProtoProbe.SpanParserBeatsDomPath`). `OtlpProtoDecoder.DecodeTraces` must stay in the tree as the reference, exactly as `DecodeLogs` did (`OtlpProtoDecoder.cs:75-81`).

---

### 2. The hot tier deserialises every span's attribute msgpack into a boxed `Dictionary<string, object?>` — under the exclusive write lock — CONFIRMED (CPU + resident memory + lock hold)

`TraceStorageEngine.cs:565-583` (`WriteSpan` holds `_lock.EnterWriteLock()` for the whole thing), `:603-605` (`DeserializeAttributes(item.AttributesBytes)`), `:2783-2793` (`MessagePackSerializer.Deserialize<Dictionary<string, object?>>`).

MEASURED, the cleanest number in this report: **4 557 ns/span with 8 attributes vs 293 ns/span with none** — `DeserializeAttributes` is **4 264 ns/span, 94 % of `WriteSpan`** — and **987 B/span retained** against a 222 B msgpack blob, a **4.5× expansion**. That matches the object graph by hand: `Dictionary` header + buckets + 8 entries ≈ 380 B, 8 key strings ≈ 480 B, 6 value strings ≈ 300 B, one boxed `long` 24 B.

Three consequences, all bad:

* **Resident memory — this is the traces OOM on the 512 MB stand.** 50 000 spans × 1 002 B ≈ **50 MB**, doubling to ≈ 100 MB across a flush. The blob-only alternative is 50 000 × 254 B ≈ **13 MB**. The engine's own comments already say a span count is the wrong unit (`TraceStorageEngine.cs:345-348`) — but the unit is not the whole story; the *per-span multiplier* is 4.5× larger than it needs to be, and it is a **gen2** multiplier because a tier lives minutes.
* **The single drain thread saturates at 219 k spans/s**, co-equal with the DOM decoder. Fixing #1 alone moves the bottleneck here and buys much less than the parser numbers suggest.
* **Queries block on it.** `_lock` is a `ReaderWriterLockSlim`; `GetTraceAsync`, `SearchSpansAsync`, the trace list and the stats aggregate all take the read lock. At 50 k spans/s the write lock is held **23 % of wall time** purely to run a msgpack `Deserialize` that the ingest path never reads.

`SpanRecord.Attributes` is even documented as "lazy — null until read" (`SpanRecord.cs:226`). It is not lazy; it is eager on the write path.

**Fix:** add `ReadOnlyMemory<byte> AttributesMsgpack` to `SpanRecord`, set it directly from `item.AttributesBytes` in `AddToHotTierLocked`, and decode on demand at the four read sites: `TraceQLAst.cs:149-150,356-357`, `TraceQLExecutor.cs:320-321`, `TraceStorageEngine.cs:3373-3374`, `TraceSummarySidecar.cs:127-128`, `TraceQueryEndpointMapper.cs:1301`. The last three are **root spans only**, so they decode a few hundred per flush instead of 50 000. Cheaper still for TraceQL: `AttrValue`/`AttrExists` want one key — a `MessagePackReader` scan over the blob comparing UTF-8 keys avoids the dictionary entirely on the filter path.

**Flag: this changes segment bytes.** `SpanWriter.cs:436-438` re-serialises the dictionary (`WriteAttributes`) — so today every attribute blob is decoded on write and **re-encoded** on flush. Handing the writer the original blob is both faster and one fewer re-encode, but the emitted bytes need not be identical to the current round trip (int width, map-header size). Still fully readable by the existing reader, which decodes a msgpack map either way; call it out as a byte-level, not format-level, change, and if strict byte identity is wanted, keep `WriteAttributes` and feed it a lazily-decoded dictionary.

**Risks / tests to pin:** `TraceDetailOrderingTests`, `TraceQLThreeValuedTests` (a span that *cannot* answer must not answer "no" — a lazy decode that fails must keep returning null, not false), `TraceQLHttpStatusAbsenceTests`, `TraceSummarySidecarTests`, `SpanSearchBoundTests`/`SpanSearchHotTierBoundTests` (their per-span byte assertions move), `SpanFormatV3Tests`/`SpanFormatV4Tests`, `SpanWalTests`, `TraceFlushVisibilityTests`.

**Measure:** the temporary probe in this recon (`WriteSpanCost`, `[InlineData(8)]` / `[InlineData(0)]`) — promote it to a permanent `SpanIngestProbe` in `tests/Ameto.Perf` (needs `<InternalsVisibleTo Include="Ameto.Perf" />` on `Ameto.Tracing.csproj` plus a project reference, or keep it in `Ameto.Storage.Tests`, which already has both).

---

### 3. No raw ingest sink for spans: one `SpanIngestItem` object + one `byte[]` per span, through a reference ring — CONFIRMED (alloc + resident, per span)

`Interfaces.cs:12,19-36` (`ISpanIngester.TryIngest(ReadOnlySpan<SpanIngestItem>)`, `SpanIngestItem` is a **sealed class**), `OtlpTraceStreamParser.cs:294,297-310` (`outBuf.WrittenSpan.ToArray()` + `new SpanIngestItem` + `result.Add`), `SpanRingBuffer.cs:58,71` (`SpanIngestItem?[65 536]`).

MEASURED: **330 B/span** on the JSON path is essentially all of this — the item (~88 B + header), the attribute `byte[]` (~254 B) and `List<SpanIngestItem>` growth. The logs path reaches **0 B/record** because `IOtlpLogSink.TryIngestRaw(…, ReadOnlySpan<byte> msgpackProps, …)` writes straight into a slab-backed ring. Traces have no such sink, so *the parser in #1 cannot get below 330 B/span no matter how good it is*.

Two extra costs of the reference ring: it is a **512 KB LOH pointer array** that the GC scans, and when the drainer lags it holds up to 65 536 live objects that get **promoted into gen1/gen2** — a 65 536-span backlog is ≈ 22 MB of survivors on a 512 MB container. The logs ring keeps payloads in a pooled `SlabArena` precisely to avoid this (`Options.cs:134-148`).

**Fix:** `ISpanSink.TryIngestRaw(TraceId, SpanId parent, long startNano, long durationNanos, ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8, SpanKind, SpanStatusCode, short httpStatus, ReadOnlySpan<byte> msgpackAttrs)`, implemented by `SpanIngestionEndpoint` over a slab-backed ring. `SpanHeader` (`SpanRecord.cs:166-206`) is **already written for exactly this** — 72 fixed bytes, pool indices for name and service, arena offset + length for the attribute blob — and is **currently dead code: zero references outside its own file** (`grep -rn SpanHeader src/ tests/` → 2 hits, both in `SpanRecord.cs`). Reuse `SlabArena` (`src/Ameto.Ingestion/SlabArena.cs`) and `PoolTrimPolicy`, and size the arena from `MemoryBudgets` rather than the hardcoded 65 536.

**Risk:** `SpanIngestItem` is public and is the WAL replay type (`SpanWriteAheadLog.ReadAll` → `RecoverFromWal`, `TraceStorageEngine.cs:525-561`); keep it for replay, or make replay drive the same raw sink. `OtlpTraceStreamingParityTests` compares `SpanIngestItem` fields, so the parity gate needs a capturing sink like `OtlpLogProtoParityTests.CapturingSink` (`OtlpLogProtoParityTests.cs:30-50`).

**Measure:** `OtlpTraceAllocProbe` (already asserts `stream * 3 < dom`; tighten to `stream * 20 < dom` once the sink lands) and the temp `JsonStreamingCostPerSpan` fact.

---

### 4. The trace hot tier has no byte budget and no configuration at all — CONFIRMED (resident memory)

`TraceStorageEngine.cs:294` (`HotFlushThreshold = 50_000` spans), `:359-360` (`MinSegmentSpans = 500`, `MaxHotAge = 1 h`), `SpanRingBuffer.cs:56` (`DefaultCapacity = 1 << 16`), `TracingServiceExtensions.cs:68` (`AddSingleton<SpanRingBuffer>()` — the default ctor, so the capacity is unreachable from config), `Options.cs:376-401` (`TracesOptions` has `IndexBackfill`, `SegmentFormatV4`, `IndexEnabled` — and nothing about memory).

Logs got `RingCapacity`, `PayloadPoolBytes`, `MemoryBudgets.IngestArenaFraction` and `HotTier.MaxSizeBytes`. Traces got a span count. Measured, that count is worth 50 MB at 8 ordinary attributes — and a span carrying a SQL statement or a stack frame is 5–10 KB, i.e. **250–500 MB for the same 50 000**. There is no operator lever between "fine" and OOM, which is what the console.ntpayments incident looked like.

**Fix:** flush on `max(spanCount ≥ N, hotBytes ≥ budget)` where `hotBytes` is accumulated from `nameLen + serviceLen + attrLen` as spans are added (free — the WAL already computes those three at `SpanWriteAheadLog.cs:222-224`), budget from `MemoryBudgets` as a fraction of the container limit, with `TracesOptions.HotTierMaxBytes` / `RingCapacity` overrides. No format change.

**Measure:** the temp probe's `hot tier retained` line at several attribute sizes; on the stand, gen2 size and `GC.CollectionCount(2)` over a sustained trace load via `dotnet-counters`.

---

### 5. Span names and service names are never interned — CONFIRMED (alloc + resident, per span)

`OtlpTraceStreamParser.cs:240` (`name = reader.GetString()`), `OtlpProtoDecoder.cs:335` (`cis.ReadString()`), `Interfaces.cs:26-27`, `TraceStorageEngine.cs:598-599`.

`grep -rln StringInternPool src/` lists ten files — **all in `Ameto.Storage`, `Ameto.Ingestion`, `Ameto.Indexing`. None in `Ameto.Tracing`.** Every span therefore carries a *fresh* `string` for its name, retained for the tier's whole life, although span names are route templates and repeat across essentially every span in a batch (`GET /api/v1/resource/{id}`). At ~30 chars that is **~80 B/span** of duplicate gen2 strings — ≈ 4 MB per full hot tier — plus the UTF-16 decode on the parse side and a second UTF-8 *re*-encode in `SpanWriteAheadLog.Append` (`:222-223,263-264`), so the same text is transcoded twice per span for nothing.

`ServiceName` is better by construction: one string per `resourceSpans` block, shared by reference (`OtlpTraceMapper.cs:26`, `OtlpTraceStreamParser.cs:73`) — but it is a *different* string object for every request, so a hot tier spanning 10 000 requests holds 10 000 copies of `"Wallet.API"`.

**Fix:** an engine-owned `StringInternPool` (reuse `src/Ameto.Storage/StringInternPool.cs` — array-backed, `Intern(ReadOnlySpan<byte>)` alloc-free on hit, `Claim`), `NamePoolIndex` / `ServiceNamePoolIndex` already reserved in `SpanHeader` (`SpanRecord.cs:184-188`). Intern the service **once per `ResourceSpans` block** (the log parser's `InternService` + `ServiceIdx`, `OtlpLogProtoParser.cs:174`), the name per span. Combines with #3: the raw sink takes `ReadOnlySpan<byte>` and interns without ever building a string.

**Risk:** the pool must be bounded and shed on flush or it becomes its own leak for high-cardinality names (`/api/user/12345`); `StringInternPool` saturates by design and answers −1, which must fall back to a plain string rather than dropping the span.

---

### 6. Span WAL: no per-entry CRC, a UTF-16→UTF-8 transcode per append, and a whole-mapping flush — CONFIRMED (durability gap + CPU, per span / per commit)

`SpanWriteAheadLog.cs:70-72` (`WalVersion = 1`, 32-byte file header, 64-byte entry header), `:217-270` (`Append`), `:204-208` (`FlushLocked`), `:543-560` (`Grow`).

* **No checksum anywhere.** The logs WAL is v4 with hardware CRC32C over header + payload (`src/Ameto.Storage/WriteAheadLog.cs:265-267,614-616`, `Ameto.Core/Crc32c.cs`). `SpanWalEntryHeader` has no CRC field, so `ReadAll` (`:459-527`) validates only that the declared lengths fit — a torn append replays as a span with garbage name/service/attribute bytes straight into the hot tier. Given how recently `MEMORY: metrics WAL poisoned on stand` closed, this is the same shape of exposure on the third signal. Cost of closing it is ~0.1 ns/byte (the logs round measured the CRC as not a hotspot).
* **Two `Encoding.UTF8` round trips per span.** `GetByteCount(name) + GetByteCount(service)` then `GetBytes` × 2 (`:222-223,263-264`) — on text that arrived as UTF-8 on the wire and was decoded to UTF-16 only because `SpanIngestItem` holds `string`. With #3 the sink hands the WAL the original UTF-8 slice and all four calls disappear.
* **`FlushLocked` flushes the whole mapping.** `_accessor.Flush()` (`:206`) is `FlushViewOfFile` over the entire view — 8 MB initially, doubling with `Grow` — plus `FlushFileBuffers`, under `_writeLock`, stalling every append for the duration. Same finding as logs #12 and the same fix: msync/`FlushViewOfFile` only `[lastFlushed, writeOffset)`, and take the file-handle flush outside the lock after capturing the offset. It fires only at `CommitFlush`/`AbandonFlush` (`:356,390`), so it is latency, not steady CPU — but it is a stall the ring must absorb, and the ring has no slab budget (#4).
* **No periodic fsync.** Unlike the logs WAL there is no timer flush; between segment flushes the log is only in the page cache. That is a deliberate-looking trade (the mapping survives process death, not machine death) but it is undocumented and worth stating explicitly.

**Fix:** WAL v2 = `SpanWalEntryHeader` + `uint Crc` over checksummed-header + name + service + attrs, written last; `ReadAll` stops at the first mismatch exactly as the logs WAL does. **This is a format change** — gated by the version byte, with v1 accepted read-only for one release (`:165` already rejects an unknown version by re-initialising, which would silently drop a v1 log; make it *read* v1 instead).

**Measure:** `SpanWalTests` for replay correctness; the temp probe's `WAL file` line for bytes/span; a CRC-cost arm on the same probe.

---

### 7. The gRPC trace route memmoves the whole message down five bytes, per request — CONFIRMED (CPU, per request)

`OtlpGrpcEndpointMapper.cs:63` (`decodeReadsFromZero: true`, the only `true` left) and `:222-226` (`body.AsSpan(HeaderBytes, messageLength).CopyTo(body)`).

An uncompressed gRPC message sits five bytes into the request buffer. Every other decoder takes the segment where it lies; `DecodeTraces(byte[], int)` reads from index 0, so the whole payload is copied down — **on a 1 MB batch that is a 1 MB memmove per request**, ~40 µs, for nothing. The parser in #1 takes a `ReadOnlySpan<byte>` and the flag, the branch and the `MessageSegment` `decodeReadsFromZero` parameter all go. `OtlpGrpcMessageSegmentTests` pins the behaviour; it will need the trace case retargeted at the new parser.

---

### 8. Per-request logger construction and an always-boxing `LogDebug` on the traces route — CONFIRMED (per request, minor)

`OtlpEndpointMapper.cs:55` `logFactory.CreateLogger("Ameto.Otel.Traces")` runs **per request** — `LoggerFactory.CreateLogger` takes a lock and walks providers. The metrics and logs handlers on the same file resolve nothing of the kind. `:87` `logger.LogDebug("… {SpanCount} spans", spans.Count)` goes through `LoggerExtensions.LogDebug(ILogger, string, params object?[])`, which allocates the `object[1]` **and boxes the int before `IsEnabled` is ever consulted** — so it costs on every request at production log levels.

**Fix:** hoist one `ILogger` into the closure at `MapOtlpEndpoints` time (it is a startup-scoped lambda, not a static one — the logs handler already avoids the issue by not logging), and replace `LogDebug` with a `[LoggerMessage]` source-generated method or an `IsEnabled` guard. ~1–2 KB/request saved on the busiest route in the process.

---

### 9. `ArrayBufferWriter.Clear()` zeroes the written span three times per span in the JSON parser — CONFIRMED (CPU, per span, ~2–3 %)

`OtlpTraceStreamParser.cs:75` (`resBuf.Clear()` per resource), `:226` (`attrBuf.Clear()` per span), `:288` (`outBuf.Clear()` per span). `Clear()` zeroes; `ResetWrittenCount()` just resets the index. With ~250 B of span pairs and ~500 B of assembled output that is ~750 B of pointless `memset` per span. The logs round already took this fix in `OtlpLogProtoParser` (`:120-122` uses `ResetWrittenCount`). One-line change per site; no behaviour change because every byte read back is bounded by `WrittenSpan`.

Two smaller ones in the same file: `WriteArrayValue`/`WriteKvlistValue` allocate **a fresh `ArrayBufferWriter<byte>(256)` per nested value** (`:499,527`) — thread-static scratch or a depth-indexed pool removes it; and `WriteJsonStringToMsgpack`/`CaptureString` rent from `ArrayPool<byte>.Shared` per *escaped* attribute (`:449,548`) where a `stackalloc byte[256]` with a pooled fallback would do.

---

### 10. `SpanDrainer` takes both locks once per span instead of once per batch — SUSPECTED (CPU, per span)

`SpanDrainer.cs:217-222` calls `_storage.WriteSpan(item)` in a loop over a batch of up to 512; each call does `EnterWriteLock`/`ExitWriteLock` (`TraceStorageEngine.cs:567,581`) and a nested `lock (_writeLock)` in the WAL (`SpanWriteAheadLog.cs:233`). Uncontended that is ~4 interlocked round trips per span; the 293 ns/span bare figure bounds the whole of `WriteSpan`, so the locks are maybe 80–150 ns of it — 25–50 % of the non-attribute cost, and every acquisition is a point at which a reader can interleave.

**Fix:** `WriteSpans(ReadOnlySpan<SpanIngestItem>)` taking the write lock and the WAL lock once per drain batch, with the flush check after the loop. **Risk:** the lock hold grows from ~300 ns to ~150 µs for a full 512-span batch, which delays readers — measure the read-side latency before and after, and cap the batch under one lock (64–128) if it bites. The two-phase WAL flush protocol (`BeginFlush`/`CommitFlush`) is unaffected because it already runs under the same `_writeLock`.

---

### 11. `SpanIngestionEndpoint` logs a warning per refused batch and refuses whole batches at 90 % — CONFIRMED (minor, but a log storm under the exact condition that caused the incident)

`SpanIngestionEndpoint.cs:27-31`: at `FillFraction ≥ 0.9` it emits `LogWarning("… {Threshold:P0} …")` — a structured format with a `P0` numeric format — **and returns false without enqueuing anything**, so the entire batch is dropped. Under sustained overload that is one formatted warning per request, written into the server's own log storage, on the path that is already over budget. The threshold is also checked once per batch: a batch admitted at 0.89 fill then enqueues until `TryEnqueue` fails and `break`s (`:33-39`), so partial acceptance happens anyway — the all-or-nothing refusal above it buys nothing.

**Fix:** rate-limit the warning (one per second, or a counter surfaced through `/api/diagnostics`), and drop the batch-level pre-check in favour of the per-item result that already exists.

---

### 12. `TraceStorageEngine` has no shutdown lifetime gate — CONFIRMED (correctness at shutdown, not CPU)

`TraceStorageEngine.Dispose` (`:3473-3497`) sets `_disposed`, flushes and disposes the WAL; nothing waits for the drainer, and nothing stops `WriteSpan` from running afterwards. `SpanWriteAheadLog.Append` answers a post-dispose append with `if (_disposed) return;` (`:235`) — **the span is silently dropped but still added to the hot tier** by the very next line in `WriteSpan`, so it becomes queryable and unrecoverable at once. `SpanDrainer.DisposeAsync` (`:254-271`) is idempotent and calls `FlushHotTier` after its own drain, but DI disposal order between the drainer singleton and the engine singleton is what decides which runs first. This is the same double-dispose shape `StorageEngine` closed with a write gate + heavy-phase count + reader wait; port that pattern.

---

## Aside (functional, not perf, but it is on this lens)

**HTTP OTLP still ignores `Content-Encoding`.** `grep -rn "Content-Encoding|UseRequestDecompression" src/` returns nothing. The collector's `otlphttp` exporter defaults to `compression: gzip`, so a gzipped `POST /v1/traces` fails the protobuf parse and gets 400. gRPC handles `grpc-encoding` (`OtlpGrpcFraming.cs:61-91`); HTTP does not. Same note as the logs recon — still open, and it applies to traces identically.

---

## Existing probes, tests, and how to run them

Run everything with `TMP`/`TEMP` pointed at a scratch dir; Release; from the worktree root.

```
dotnet build -c Release tests/Ameto.Perf/Ameto.Perf.csproj
dotnet test tests/Ameto.Perf/Ameto.Perf.csproj -c Release --no-build \
  --filter "FullyQualifiedName~OtlpTraceProtoProbe|FullyQualifiedName~OtlpTraceAllocProbe|FullyQualifiedName~OtlpTraceStreamingParityTests" \
  -l "console;verbosity=detailed"
```

* `tests/Ameto.Perf/OtlpTraceProtoProbe.cs` — DOM decode+map ns/span and B/span. **5 428 ns, 7 881 B.** Add the parser arm here.
* `tests/Ameto.Perf/OtlpTraceAllocProbe.cs` — streaming JSON vs reflection DOM, bytes/batch. **64.4 KB vs 456.8 KB per 200 spans (7.1×)**; guard is `stream * 3 < dom`.
* `tests/Ameto.Perf/OtlpTraceStreamingParityTests.cs` — streaming JSON vs DOM, every field plus `AttributesBytes` byte-for-byte, 6 spans of which 2 are dropped. **The template for the protobuf parity gate.**
* `tests/Ameto.Perf/OtlpProtoPayloads.cs:184-226` — `Traces_Realistic(spans)`: one resource, one scope, N SERVER spans with 6 attributes and a status. **No span events, no links, no array/kvlist attributes** — extend it before trusting #1's numbers as an upper bound.
* `tests/Ameto.Perf/OtlpLogProtoProbe.cs` / `OtlpLogProtoParityTests.cs` / `OtlpLogProtoLimitsTests.cs` / `OtlpLogProtoInvalidUtf8Tests.cs` — the four-file shape the trace parser should be delivered in (probe, parity, depth/limits, invalid UTF-8). `OtlpLogProtoProbe`: **1 457 ns/record, 0 B/record.**
* `tests/Ameto.Perf/OtlpMetricProtoProbe.cs` — 2 271 ns/point, 1 199 B/point (the metrics parser still allocates its ingest items; same gap as #3).
* `tests/Ameto.Integration.Tests/OtlpProtoDecoderTests.cs`, `OtlpGrpcFramingTests.cs`, `OtlpGrpcMessageSegmentTests.cs`, `OtlpHttpOversizedBodyStatusTests.cs`, `OtlpOversizedBodyTests.cs` — endpoint-level gates; `OtlpGrpcMessageSegmentTests` runs each signal's real decoder over `MessageSegment`'s output and is what #7 must not break.
* `tests/Ameto.Storage.Tests/SpanWalTests.cs`, `SpanFormatV3Tests.cs`, `SpanFormatV4Tests.cs`, `TraceFlushVisibilityTests.cs`, `SpanSearchBoundTests.cs`, `SpanSearchHotTierBoundTests.cs` — the storage-side gates for #2/#4/#6.

**Probes to add:** `OtlpTraceProtoParityTests` + a parser arm on `OtlpTraceProtoProbe` (#1); `SpanIngestProbe` — the temporary `WriteSpanCost` fact used for this report, measuring ns/span, B/span allocated and B/span retained at 0 and 8 attributes (#2, #4); an LOH/gen2 counter run under a sustained 100 k spans/s load on the stand (#3, #4).

*(Both temporary probes written for this recon — `tests/Ameto.Storage.Tests/ZzTempSpanWriteProbe.cs` and `tests/Ameto.Perf/ZzTempTraceParseProbe.cs` — were deleted; the tree is clean at 0a7389d.)*
