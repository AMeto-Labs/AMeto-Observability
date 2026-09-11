# Log ingest path — CPU / allocation reconnaissance (worktree AMeto-perf-logs @ 677b49f)

Paths traced end to end: OTLP/JSON 1000-record batch (`OtlpEndpointMapper.logs` → `OtlpLogStreamParser.Parse` → `IngestionEndpoint.TryIngestRaw` → ring → drainer → `StorageEngine.TryWrite` → hot tier + WAL + `EventWritten`), OTLP/protobuf over HTTP and gRPC (`OtlpProtoDecoder.DecodeLogs` → `OtlpLogMapper.Map` → `IngestEvents`), and CLEF `/api/events` (`IngestionEndpoint.HandleAsync` → `LogEventSerializer.DeserializeBatch` → `TryIngest`).

**Prior work verified still in place (not re-reported):** `TryIngestRaw`/`IOtlpLogSink` with no per-record object graph on JSON; `StringInternPool.Intern(ReadOnlySpan<byte>)` alloc-free on hit (alternate lookup); `[ThreadStatic]` msgpack scratch in parser and serializer; pooled parse copies in `LogEventSerializer.Deserialize(span)`; ring slab pool + `PayloadPoolBytes`; drainer pending-slot retry; `HotTierSegment` Dekker handshake; WAL v4 with hardware CRC32C (`Crc32c.cs` uses SSE4.2/ARM intrinsics — the per-event checksum is ~0.1 ns/byte, not a hotspot).

**Drainer verdict:** per event it does 3 memcpy (slab→`_payloadBuf`→chunk→mmap), one CRC pass, ~4 interlocked ops, one uncontended WAL lock, one delegate (`LiveEventSignal.Signal` = `Interlocked.Increment`). No `LogEvent`, no indexing, no logging per event. ≈0.5–1 µs/event → the single drain thread saturates around 1–2 M events/s; it is not the bottleneck. The request threads are.

## Findings (ranked by expected impact)

### 1. Protobuf logs still decode to a DOM — CONFIRMED (CPU + alloc, per record/attribute)
`src/Ameto.Otel/OtlpEndpointMapper.cs:150-153`, `OtlpGrpcEndpointMapper.cs:46-49`, `OtlpProtoDecoder.cs:40-41,408-440,472-507`, `OtlpLogMapper.cs:49-121`, `IngestionEndpoint.cs:233-262`.
`SubStream()` = `ReadBytes().CreateCodedInput()`: one `byte[]` copy + `ByteString` + `CodedInputStream` for every nested message — body AnyValue, and KeyValue + AnyValue per attribute (≈21 nested messages, ≈63 objects per 10-attr record; every byte copied 3–4×). Then: `ReadFixed64().ToString()` for the timestamp (re-parsed by `ParseNanoString`), `ReadInt64().ToString()` per int attr (re-parsed by `long.TryParse`), `HexFromBytes` = `Convert.ToHexString().ToLowerInvariant()` = 2 strings each for trace/span id (re-parsed with `ulong.TryParse HexNumber`), `cis.ReadString()` per key/value/severity (~25 strings), `OtlpLogRecord`/`OtlpKeyValue`×10/`OtlpAnyValue`×11 + `List` growth, `new ArrayBufferWriter(count*32)` + UTF-16→UTF-8 transcode of every key/value into msgpack, `new LogEvent`, `List<LogEvent>`; then `TryIngest` re-hashes the fresh template string for interning. Estimate: ~120 allocations / 6–8 KB and 3–5 µs per record → at 100k/s ≈ 600–800 MB/s gen0 churn and 30–50 % of a core. This is the encoding SDK exporters and the collector send, so it is the production path.
**Fix:** `OtlpLogProtoParser` on `ProtoReader` (mirror `OtlpMetricProtoParser`'s two-pass ResourceLogs shape), writing straight into the `[ThreadStatic]` `ArrayBufferWriter`s and `IOtlpLogSink.TryIngestRaw`: field 1 fixed64 → ticks arithmetic; 2 enum; 3 bytes → `MapSeverityText`; 5 body AnyValue → template bytes; 6 KeyValue → `MessagePackWriter.WriteString(ReadOnlySpan<byte>)` for key and string_value, `Write(long/bool/double)` from the wire, array/kvlist via count-then-splice as the JSON parser does; 9/10 raw 16/8 bytes → `BinaryPrimitives.ReadUInt64BigEndian` for trHi/trLo/spanId and a `stackalloc byte[32]` lowercase-hex for the `@tr`/`@sp` properties (keep the on-disk property format). Both HTTP and gRPC routes switch to `Parse(ReadOnlySpan<byte>, sink)`; the gRPC identity-frame memmove at `OtlpGrpcEndpointMapper.cs:169` goes away with it.
**Risk:** property order (resource attrs, record attrs, `@tr`, `@sp`) and the `service.name` capture must match `OtlpLogMapper` byte-for-byte; unknown/absent fields; `service.name` also stays inside the props map (JSON parity behaviour). Tests pinning it: `tests/Ameto.Integration.Tests/OtlpProtoDecoderTests.cs`, `AmetoIngestEndpointsTests.cs`.
**Measure:** add `OtlpLogProtoParityTests` + `OtlpLogProtoProbe` modelled on `OtlpMetricProtoParityTests`/`OtlpMetricProtoProbe` (needs a `Logs_Realistic()` builder in `tests/Ameto.Perf/OtlpProtoPayloads.cs`; none exists yet).

### 2. Hot tier retains a per-event duplicate template string (CLEF + proto paths) — CONFIRMED (resident memory, gen2)
`src/Ameto.Ingestion/IngestionEndpoint.cs:252-262` passes `ev.MessageTemplate` — a fresh string per event from `reader.ReadString()`/`cis.ReadString()` — into `_ring.TryEnqueue`, and `HotTierSegment.cs:374-378` stores it in `_chunkTemplates` for the tier's lifetime. The JSON path passes the canonical `_pool.Get(tmplIdx)` instead. ≈100–150 B/event promoted to gen2 (tiers live seconds–minutes): a 500k-event tier pins ~60–75 MB of duplicate strings, all released as gen2 garbage at flush → gen2 GCs and an inflated heap for Serilog-sink deployments.
**Fix (one line):** `string tmpl = tmplIdx >= 0 ? _pool.Get(tmplIdx) : ev.MessageTemplate;` (or make `Intern` return the canonical string). No format change.
**Measure:** gen2 size / `GC.CollectionCount(2)` over a 1M-event CLEF run on the stand (`dotnet-counters`), or a probe comparing `GC.GetTotalMemory` retained after a 100k-event `IngestEvents` into a real `HotTierSegment`.

### 3. CLEF `/api/events` materialises a `LogEvent` per event — CONFIRMED (alloc + CPU, per event)
`src/Ameto.Core/Serialization/LogEventSerializer.cs:159-298`. Per event: `ReadString()` for `@t` (~90 B), `@mt` (~100 B), `@l` (~30 B), `service.name`, `@tr` (86 B), `@sp` (54 B); a `combined` `byte[]` for the props (300–500 B); the `LogEvent` (~130 B); plus `DateTimeOffset.TryParse(string, null, RoundtripKind)` — the culture-aware general parser, ~300–500 ns. ≈8 objects / 0.8–1 KB / ~1.5 µs per event → 80–100 MB/s gen0 and ~15 % of a core at 100k/s, all garbage the moment `TryIngest` copies into the ring.
**Fix:** a streaming CLEF path mirroring `TryIngestRaw`: `MessagePackReader` over `bodyBuf.AsMemory(0, bodyLen)`; `TryReadStringSpan` + `ClassifyKey` (already there); `@t` → `Utf8Parser.TryParse(span, out DateTimeOffset, out _, 'O')` with the old parse as fallback (non-.NET Seq clients send 3-digit fractions); `@l` → UTF-8 compare; `@tr`/`@sp` → `TraceIdHelper.TryParseTraceId(ReadOnlySpan<byte>)`; `@mt`/`service.name` → `_pool.Intern(ReadOnlySpan<byte>)`; raw pairs into the existing `[ThreadStatic]` scratch, header + pairs → `_ring.TryEnqueue`. Keep `ExceptionInfo.Read` for `@x` (must be an object in the ring).
**Risk:** `@m`→`@mt` fallback, `service.name` consumed (not stored in props) on this path, oversized-drop marker path, `Properties=null` contract. Pinned by `tests/Ameto.Perf/ClefBatchScratchTests.cs`, `MsgPackJsonTranscoderParityTests`, `Ameto.Integration.Tests/FullIntegrationTests.cs`.
**Measure:** new `ClefAllocProbe` (bytes per 1000-event batch via `GC.GetAllocatedBytesForCurrentThread`, like `OtlpAllocProbe`).

### 4. Request bodies rented at full size from `ArrayPool<byte>.Shared` — CONFIRMED (LOH, per request)
`OtlpEndpointMapper.cs:244`, `OtlpGrpcEndpointMapper.cs:261`, `IngestionEndpoint.cs:90,103`. `Rent(1.4 MB)` rounds to the 2 MB bucket; the shared pool keeps 1 TLS + ≤8 per-core arrays per bucket and trims them on gen2 — under concurrent requests the overflow is a fresh, zeroed 2 MB LOH array per request (the earlier trace). Returns are correct on all paths (finally/catch), so this is pool depth, not leaks.
**Fix:** one `ArrayPool<byte>.Create(maxArrayLength: max(MaxOtlpBatchBytes, MaxBatchBytes), maxArraysPerBucket: 32–64)` singleton (`IngestBufferPool`) used by all three body readers and the gzip inflate target. Never trimmed, bounded. (Parsing straight from `BodyReader`'s `ReadOnlySequence` is blocked by Kestrel's 1 MB `MaxRequestBufferSize` pause threshold; an incremental `Utf8JsonReader` over 64 KB windows is a larger rewrite of the recursive parser — later.)
**Measure:** LOH allocated bytes (`GC.GetGCMemoryInfo().GenerationInfo[3]` / `dotnet-counters`) under 32 concurrent 1.4 MB posts.

### 5. gRPC gzip inflate rents `MaxOtlpBatchBytes` and copies the compressed payload — CONFIRMED (LOH + CPU, per compressed request)
`OtlpGrpcFraming.cs:387` `Rent(maxInflatedBytes)` = 8 MiB per request (the collector's OTLP/gRPC exporter defaults to gzip, so this is every request); `:393` `payload.ToArray()` (fresh `byte[]`, LOH above 85 KB) + `MemoryStream` + `GZipStream`. Inflate CPU itself (~10–15 % of a core at 50 MB/s output) is inherent.
**Fix:** rent `clamp(4 × compressedLen, 64 KB, max)` from the pool in #4 and grow by doubling (the "against the limit" check is unchanged); pass the rented body array + offset so `new MemoryStream(body, off, len, writable:false)` wraps it without a copy.
**Measure:** `tests/Ameto.Integration.Tests/OtlpGrpcFramingTests.cs` for correctness; alloc-per-call probe around `TryUnframe`.

### 6. Three intern-pool lookups per JSON record — SUSPECTED (CPU, per record)
`IngestionEndpoint.cs:214-216`: `Intern(templateUtf8)` (UTF-8→UTF-16 decode + Marvin hash), `Get(tmplIdx)`, `Intern(serviceUtf8)` — the service name is identical for every record in a `resourceLogs` block, and templates repeat. ≈200–300 ns/record → 2–3 % of a core at 100k/s.
**Fix:** intern `service.name` once per resource in `OtlpLogStreamParser.ParseResourceLogs` and pass an `int svcIdx` (add a `TryIngestRaw` overload); `Intern(ReadOnlySpan<byte>, out string canonical)` to fold the `Get`; optional per-thread last-template memo.
**Measure:** BenchmarkDotNet bench on `StringInternPool.Intern(span)` (extend `StringInternPoolBenchmark`), or `OtlpAllocProbe` with a real endpoint.

### 7. JwtBearer handler runs on every ingest request — CONFIRMED (per request)
`Program.cs:313-315` (`UseRateLimiter`, `UseAuthentication`, `UseAuthorization`) with `DefaultAuthenticateScheme = JwtBearer` (`AuthServiceExtensions.cs:138`). Per request: handler instance via `ActivatorUtilities`, `InitializeAsync`, `LoggerFactory.CreateLogger` (takes a lock), the `OnMessageReceived` event, `NoResult` — ~1–2 KB and ~2–5 µs, all before the endpoint's own `ApiKeyCache` check.
**Fix:** `app.UseWhen(ctx => !IsIngest(ctx), b => { b.UseRateLimiter(); b.UseAuthentication(); b.UseAuthorization(); })` with a static path+method check (`POST /api/events`, `/v1/logs`, `/otlp/v1/*`, gRPC Export). Risk: `GET /api/events` (SSE) shares the path — branch on method.

### 8. `ApiKeyCache.Validate` allocates two strings per request — CONFIRMED (per request, minor)
`ApiKeyCache.cs:85` `Convert.ToHexString(hash).ToLowerInvariant()` (2 × ~150 B) + `ImmutableDictionary` lookup. Fix: `Convert.ToHexStringLower`, or key a `FrozenDictionary` by a 32-byte `readonly record struct` (4 × ulong) — zero alloc. `ApiKeyHeader.Extract:365-367` also substrings on the `Authorization: apikey` form.

### 9. Response writing — minor (per request)
`OtlpEndpointMapper.cs:289` `new Utf8JsonWriter` per request; `IngestionEndpoint.cs:161-163` interpolated string + `WriteAsync(string)`. Fix: `Utf8Formatter` into `BodyWriter.GetSpan(48)` + `Advance` + `FlushAsync`, shared helper. ~300 B/request.

### 10. JSON parser scratch micro-costs — CONFIRMED (per record, ~1 % core)
`OtlpLogStreamParser.cs:182,246` `ArrayBufferWriter.Clear()` zeroes the written span (2 × ~400 B/record) → `ResetWrittenCount()`; `:249-250` rec→out copy could be avoided by writing a fixed 3-byte map16 header up front (changes bytes — `OtlpStreamingParityTests` compare props byte-for-byte, so probably leave it); `:430-433` per-escaped-attribute rent/return → `stackalloc` ≤256 B.

### 11. Per-chunk 128 KB LOH arrays in the hot tier — SUSPECTED (LOH churn, per 16 384 events)
`HotTierSegment.cs:376,381` `new string?[16384]` / `new ExceptionInfo?[16384]` (128 KB → LOH) per chunk, garbage after the tier flushes (~8 per 64 MB tier). Fix: recycle through an engine-owned stack or `ArrayPool<string?>.Shared.Rent(16384)` + `Return(clearArray:true)` on `Dispose`.

### 12. WAL msync holds the writer lock over the whole mapping — per timer tick (latency, not CPU)
`WriteAheadLog.cs:276-286`: `_accessor.Flush()` on the full 64 MB+ view + `FlushFileBuffers` every 2 s (`Options.cs:51`) under `_writeLock`, stalling the drainer for the duration (ms; the ring absorbs it). `FlushViewOfFile` cost scales with mapping size. Fix: P/Invoke `FlushViewOfFile`/`msync` on `[lastFlushed, writeOffset)` only, and do the file-handle flush outside the lock after capturing the offset.

## Aside (functional, not perf)
HTTP OTLP has no gzip handling at all: `OtlpEndpointMapper.ReadBodyAsync` ignores `Content-Encoding` and there is no `UseRequestDecompression` in `Program.cs`. The collector's `otlphttp` exporter defaults to `compression: gzip`, so such bodies fail the JSON/proto parse and get 400.

## Existing probes and how to run them
- Project: `tests/Ameto.Perf` — BenchmarkDotNet exe (`dotnet run -c Release --project tests/Ameto.Perf -- --filter *HotTierWrite*`) plus xUnit facts (`dotnet test tests/Ameto.Perf`); `AssemblyInfo.cs` serialises the facts so process-wide counters are not cross-contaminated.
- `OtlpAllocProbe` — bytes/batch, JSON streaming vs DOM; guard `streamBytes*8 < domBytes`.
- `OtlpStreamingParityTests` — JSON streaming parser vs DOM, byte-for-byte props.
- `OtlpMetricProtoProbe` / `OtlpMetricProtoParityTests` / `OtlpProtoPayloads` — the template for a logs proto parser probe and parity gate (#1).
- `ClefBatchScratchTests` — per-event `RawProperties` independence (#3).
- `Benchmarks.cs`: `HotTierWriteBenchmark`, `StringInternPoolBenchmark` (#6), `PerfSmokeTests`.
- `FlushCpuProbe` (cold flush), `HotTierScanAllocProbe`.
- Integration: `OtlpProtoDecoderTests`, `OtlpGrpcFramingTests`, `AmetoIngestEndpointsTests`, `FullIntegrationTests`.
Suggested new: `OtlpLogProtoProbe`/`OtlpLogProtoParityTests` (#1), `ClefAllocProbe` (#3), a retained-memory probe for #2, LOH counters under concurrent large posts for #4/#5.
