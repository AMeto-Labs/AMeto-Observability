# Traces + metrics — CPU / allocation / memory optimisation plan

Issue #83. Branch `perf/traces-metrics-cpu-alloc` (from `main` @ 0a7389d, after #79). Goals, in
priority order: resident memory on a 512 MB host (this round exists because traces OOM'd there),
CPU per span / per point, allocations per span / per point / per request (GC pressure, LOH churn).
On-disk formats stay byte-identical in this round; the one planned format change (span WAL v2) is
gated by its version byte and called out in its package.

Recon reports (evidence, file:line, fix sketches): `docs/perf/recon/traces-ingest.md`,
`docs/perf/recon/traces-store.md`, `docs/perf/recon/metrics.md`. Item ids below are
`TI#n` (traces-ingest), `TS#n` (traces-store), `M#n` (metrics).

## Baseline at `main` @ 0a7389d

Release, this machine, workstation GC. Every number below is measured, not estimated; the recon
reports carry the commands and the probe sources.

**Traces — ingest**

| Stage | ns/span | B/span alloc | Note |
|---|---|---|---|
| OTLP/proto `DecodeTraces` + `OtlpTraceMapper.Map` | **5 428** | **7 881** | the path every SDK exporter uses |
| OTLP/JSON `OtlpTraceStreamParser.Parse` | 1 081 | 330 | the path nobody uses in production |
| `TraceStorageEngine.WriteSpan`, 8 attrs | **4 557** | 1 399 alloc / **1 002 retained** | 94 % of it is `DeserializeAttributes` |
| `TraceStorageEngine.WriteSpan`, 0 attrs | 293 | 151 / 15 | |
| *reference:* `OtlpLogProtoParser` after #79 | 1 457 | **0** | the shape TI#1 must reach |

End to end protobuf ≈ **9 985 ns/span** (5 428 on a request core + 4 557 on the one drain thread)
≈ 100 k spans/s with both saturated. JSON ≈ 5 638 ns/span, drainer-bound at 219 k spans/s.

**Traces — storage, flush, query** (8-attribute SqlClient span, 375 B msgpack blob)

| Stage | wall | allocated | retained |
|---|---|---|---|
| hot tier, 49 000 spans through `WriteSpan` | 250.9 ms (5.12 µs/span) | 77.0 MB (1 647 B/span) | **52.2 MB (1 117 B/span)** |
| flush, 50 000 spans (`SpanWriter.Write`) | 598.9 ms wall / 859.4 ms CPU | 20.9 MB (439 B/span) | +3.8 MB LOH |
| `SpanReader.ReadAll`, one 50 000-span segment | 203.8 ms | 83.4 MB | **83.0 MB (1 740 B/span)** |
| TraceQL `{.db.system="mssql" && duration>1s}` limit 200 | 176.1 ms | 43.6 MB (913 B/span *scanned*) | |
| `GetAggregateStatsAsync` over a 49 000-span tier | 19.8 ms **inside the read lock** | 12.3 MB | |
| `GetServiceGraphAsync` | 26.7 ms **inside the read lock** | 16.1 MB | |
| span WAL after 49 000 spans | — | — | 32.0 MB mapped (685 B/span) |

**Metrics**

| Stage | ns/point | B/point | resident |
|---|---|---|---|
| OTLP/proto DOM decode+map | 6 511 | 6 701 | — |
| `OtlpMetricProtoParser` (span parser, already landed) | 2 348 | **1 199** | 8 000 label strings/batch, **26 distinct** |
| ingest (WAL+hot), gauge | 1 080 | 95 | 185 B/point in the tier |
| ingest (WAL+hot), 16-bucket histogram | 1 084 | 104 | 346 B/point in the tier |
| ingest lock scaling | 1 thr 716 k/s → 8 thr 971 k/s | — | **flat: 8 cores buy 1.36×** |
| flush, 120 k points | 159–212 ms | 35–52 MB (309–453 B/point) | writes 20–48 KB |
| cold query, 2 000 series × 60 points (48 KB on disk) | 111–187 ms | **28–36 MB** | **625× the bytes it reads** |
| hot tier retained by an **empty** tier, after a drain | — | — | **12 166 B per series ever seen** |

Suites green at baseline: Core / Indexing / Storage / Query / Integration / Perf.

## Why nine packages, in three waves

The three reports carry 38 findings. Six packages cannot hold them at the 2–5 day size without
either merging unrelated areas or dropping work that is load-bearing for the 512 MB story. The
shape of the code forces the count as much as the volume does: `TraceStorageEngine.cs` (3 500
lines) is wanted by eight separate findings and can be owned by exactly one package per wave, and
`MetricStorageEngine.cs` by five. Nine packages let each wave give those two files a single owner
while still running three agents in parallel.

Implementation runs in **waves of exactly three packages**. Each package is its own
worktree/branch off `perf/traces-metrics-cpu-alloc`. **Within a wave the three packages' owner
files are disjoint** — every file below appears in at most one package of its wave. Shared helpers
(`IngestBufferPool`, `MemoryBudgets`, `PoolTrimPolicy`, `MemoryShedRegistry`, `Crc32c`,
`StringInternPool`, `SlabArena`) are **read-only for everyone except the package that explicitly
owns them** in that wave. Across waves, later packages depend on earlier ones; each package states
its dependencies.

## Work packages

| WP | Wave | Branch | Items (recon id) | Owner files |
|----|------|--------|------------------|-------------|
| WP1 traces-proto | 1 | `perf/tm-traces-proto` | TI#1 zero-copy OTLP/protobuf trace parser (HTTP+gRPC); TI#7 gRPC memmove; TI#8 per-request logger + boxing `LogDebug`; TI#9 JSON parser micro-costs | `Ameto.Otel/OtlpTraceProtoParser.cs` (new), `OtlpTraceMapper.cs`, `OtlpTraceStreamParser.cs`, `OtlpProtoDecoder.cs`, `OtlpEndpointMapper.cs`, `OtlpGrpcEndpointMapper.cs`, `AmetoIngestEndpoints.cs`; `tests/Ameto.Perf/OtlpTraceProtoProbe.cs`, `OtlpProtoPayloads.cs`, new `OtlpTraceProto{Parity,Limits,InvalidUtf8}Tests.cs`; `tests/Ameto.Integration.Tests/OtlpGrpcMessageSegmentTests.cs` |
| WP2 traces-blob | 1 | `perf/tm-traces-blob` | TS#1 (=TI#2) hot tier keeps the msgpack blob; TS#5 reader keeps the blob; TS#13 `GetAttr` span-of-keys | `Ameto.Tracing/SpanRecord.cs`, `Storage/TraceStorageEngine.cs`, `Storage/SpanReader.cs`, `Storage/SpanWriter.cs`, `Storage/TraceSummarySidecar.cs`, `TraceQL/TraceQLAst.cs`, `TraceQL/TraceQLExecutor.cs`; new `tests/Ameto.Storage.Tests/{TraceHotTierProbe,TraceCompactionMemoryProbe,TraceQlScanProbe,SpanWriterAttrParityTests}.cs`; `tests/Ameto.Storage.Tests/SpanSearchBoundTests.cs` |
| WP3 metrics-hot | 1 | `perf/tm-metrics-hot` | M#2 hot tier releases point arrays + evicts stale series; M#6 every metrics cap from `MemoryBudgets`; M#7 `GetMetricNames` off `_hot.Keys`; M#11 `_coldLock` shutdown gate + `PruneAsync`; M#13 `UpdateMeta` / exemplar loop | `Ameto.Metrics/Storage/MetricStorageEngine.cs`, `Storage/MetricWriteAheadLog.cs` (initial-capacity parameter only), `MetricsServiceExtensions.cs`; `Ameto.Core/Options.cs`, `Ameto.Core/MemoryBudgets.cs`, `Ameto.Core/MemoryShedRegistry.cs`; new `tests/Ameto.Storage.Tests/{MetricHotTierRetentionProbe,MetricFlushAllocProbe}.cs` |
| WP4 traces-writepath | 2 | `perf/tm-traces-writepath` | TS#8 (=TI#12) shutdown gate; TS#4 (=TI#10) one engine lock per drained batch; TI#6 + TS#10 span WAL **v2** (CRC32C, UTF-8 append overload, msync range only, bounded growth); TS#6 aggregates off the read lock; TI#11 ingest-endpoint warning storm | `Ameto.Tracing/Storage/TraceStorageEngine.cs`, `Storage/SpanWriteAheadLog.cs`, `Ingestion/SpanDrainer.cs`, `Ingestion/SpanIngestionEndpoint.cs`, `Interfaces.cs`, `TracingServiceExtensions.cs`; new `tests/Ameto.Storage.Tests/{TraceShutdownSeamTests,SpanWalV2Tests,TraceAggregateLockProbe}.cs`; `tests/Ameto.Storage.Tests/SpanWalTests.cs` |
| WP5 traces-flush | 2 | `perf/tm-traces-flush` | TS#7(a) index sort without the 50 000-reference copy; (b) `svcBlockMap` without `SortedSet`; (c) trace index as a sorted array; (d) `.svcgraph` reuses the writer's span→service pass; (e) `.tracesum` one pooled buffer, no `ToArray`; (f) bloom fed from one decode; (g) LZ4 level — **measure only, do not adopt blind** | `Ameto.Tracing/Storage/SpanWriter.cs`, `Storage/ServiceGraphSidecar.cs`, `Storage/TraceSummarySidecar.cs`, `Storage/SpanStats.cs`; new `tests/Ameto.Storage.Tests/TraceFlushProbe.cs` |
| WP6 metrics-ingest | 2 | `perf/tm-metrics-ingest` | M#1 interned label keys/values (+ move `StringInternPool` to `Ameto.Core`); M#3 ingest stops serialising on the WAL lock; M#8 hot query touches only its own metric; M#10 perf half (`Grow` outside the append lock, bounded increment); M#13 exemplar ring | `Ameto.Otel/OtlpMetricProtoParser.cs`, `OtlpMetricMapper.cs`; `Ameto.Metrics/MetricTypes.cs`, `Storage/MetricWriteAheadLog.cs`, `Storage/MetricStorageEngine.cs`; `Ameto.Core/StringInternPool.cs` (moved in), `Ameto.Storage/StringInternPool.cs` (removed); `tests/Ameto.Perf/{MetricIngestContentionProbe,OtlpMetricProtoProbe,ServiceInternProbe}.cs`, `tests/Ameto.Storage.Tests/{StringInternPoolTests,StringInternPoolSlotArrayTests}.cs` |
| WP7 metrics-query | 3 | `perf/tm-metrics-query` | M#4(a–f) reader copy, msgpack key compare, matcher scan, `Downsample` de-LINQ, no `SortedDictionary`, pooled bucket arrays; M#9 source-generated JSON + streamed rows; M#12 rollup stops re-decoding every source per chunk; M#13 date parse, startup enumerate | `Ameto.Metrics/Storage/MetricReader.cs`, `MetricAggregator.cs`, `MetricQueryEndpointMapper.cs`, `Interfaces.cs`, `MetricJson.cs` (new), `Storage/MetricStorageEngine.cs`; new `tests/Ameto.Storage.Tests/{MetricQueryAllocProbe,MetricDownsampleGoldenTests}.cs`; new `tests/Ameto.Integration.Tests/MetricResponseShapeTests.cs` |
| WP8 traces-sink | 3 | `perf/tm-traces-sink` | TI#3 raw span sink over a slab arena (+ move `SlabArena` to `Ameto.Core`); TI#5 interned span + service names; TS#9 ring back-pressure by bytes; TS#3 (=TI#4) traces caps from `MemoryBudgets` | `Ameto.Tracing/Interfaces.cs`, `SpanRecord.cs`, `Ingestion/SpanRingBuffer.cs`, `Ingestion/SpanIngestionEndpoint.cs`, `Ingestion/SpanDrainer.cs`, `Storage/TraceStorageEngine.cs`, `TracingServiceExtensions.cs`; `Ameto.Core/SlabArena.cs` (moved in), `Ameto.Core/Options.cs`, `Ameto.Core/Ameto.Core.csproj`, `Ameto.Ingestion/SlabArena.cs` (removed), `Ameto.Ingestion/IngestionRingBuffer.cs`, `Ameto.Ingestion/IngestionServiceExtensions.cs`; `Ameto.Otel/OtlpTraceProtoParser.cs`, `OtlpTraceStreamParser.cs`, `OtlpEndpointMapper.cs`; new `tests/Ameto.Storage.Tests/SpanRingBytesProbe.cs`, `tests/Ameto.Perf/{SlabArenaCommitTests,HugePageOptOutDecisionTests}.cs`, `tests/Ameto.Integration.Tests/IngestArenaCommitFailureTests.cs` |
| WP9 traces-read | 3 | `perf/tm-traces-read` | TS#11 trace-detail/flamegraph without a dictionary + four strings per span, source-generated JSON, streamed; TS#13 `TraceIndexStore.Lookup` pooled lists; TraceQL row smalls; the SSE hot-tier re-walk (memo or document) | `Ameto.Tracing/TraceQueryEndpointMapper.cs`, `TraceQL/TraceQLExecutor.cs`, `TraceQL/TraceQLAst.cs`, `Storage/TraceIndexStore.cs`, `Storage/SpanReader.cs`; new `tests/Ameto.Perf/TraceDetailAllocProbe.cs`; new `tests/Ameto.Integration.Tests/TraceDetailShapeTests.cs` |

### Ownership rules that keep the waves disjoint

- `Ameto.Tracing/Storage/TraceStorageEngine.cs`: WP2 (w1) → WP4 (w2) → WP8 (w3). Never two in a wave.
- `Ameto.Metrics/Storage/MetricStorageEngine.cs`: WP3 (w1) → WP6 (w2) → WP7 (w3).
- `Ameto.Core/MemoryBudgets.cs` is owned **once in the whole round, by WP3**. The existing cut
  already claims 0.55 of the managed limit and 0.40 of the physical one for logs
  (`ManagedBuildFraction` 0.30 + `IndexCacheFraction` 0.15 + `IngestBufferFraction` 0.10;
  `NativeTierFraction` 0.25 + `IngestArenaFraction` 0.15). A traces and a metrics share are a
  **re-cut, not an append** — so WP3 defines *both* new fractions (`MetricHotTierFraction`,
  `TraceHotTierFraction`, `TraceMergeFraction`) and re-balances the existing ones in one commit
  with the logs budget tests green. WP8 only consumes the traces fractions.
- `Ameto.Core/Options.cs`: WP3 (w1, `MetricsOptions`) → WP8 (w3, `TracesOptions` memory knobs).
- `Ameto.Tracing/Storage/SpanWriteAheadLog.cs` is WP4's alone. WP4 lands the UTF-8
  `Append(… ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> serviceUtf8, ReadOnlySpan<byte> attrs)`
  overload as part of TI#6 (with the `SpanIngestItem` caller transcoding at the call site until
  WP8 removes it), so **WP8 never opens that file**.
- `tests/Ameto.Perf` does not reference `Ameto.Tracing` or `Ameto.Metrics` and neither grants it
  `InternalsVisibleTo`. Traces-storage and metrics-storage probes therefore live in
  `tests/Ameto.Storage.Tests`, which already has both — as `MetricReaderStreamingProbe` does today.
  **No package edits `tests/Ameto.Perf/Ameto.Perf.csproj`.** Decode-path probes (WP1, WP6) stay in
  `tests/Ameto.Perf`, which already reaches `Ameto.Otel`.

---

## WP1 — `perf/tm-traces-proto` (wave 1)

The protobuf trace path is 5× slower and allocates 24× more than the JSON path, and it is the one
every SDK exporter and the collector actually use.

| Item | Measured now | Target |
|---|---|---|
| TI#1 `OtlpTraceProtoParser` on `ProtoReader`, mirroring `OtlpLogProtoParser` | 5 428 ns/span, 7 881 B/span | ≈ 1 200–1 500 ns/span, 330 B/span (0 B after WP8) |
| TI#7 gRPC `decodeReadsFromZero` memmove | ~40 µs per 1 MB batch | gone with the span parser |
| TI#8 `CreateLogger` per request + `LogDebug(params object?[])` boxing before `IsEnabled` | ~1–2 KB/request on the busiest route | 0 |
| TI#9 `ArrayBufferWriter.Clear()` ×3/span, per-value `ArrayBufferWriter(256)`, per-escape `ArrayPool` rent | ~750 B `memset`/span, 2–3 % | `ResetWrittenCount`, thread-static scratch, `stackalloc` |

Field map, two-pass `ResourceSpans` (proto does not guarantee field order), depth cap, and the id
handling (`BinaryPrimitives.ReadUInt64BigEndian` off the slice — **no hex round trip anywhere**,
because on the trace path ids are columns, not `@tr`/`@sp` map entries) are spelled out in
`recon/traces-ingest.md` §1. `events[]`, `links[]`, `trace_state`, the dropped counts and
`status.message` are `SkipField` — the DOM materialises `OtlpSpanEvent` objects with their own
attribute lists that `OtlpTraceMapper.MapSpan` never reads.

**Tests that must pin it.** `OtlpTraceProtoParityTests` (new, modelled on `OtlpLogProtoParityTests`):
new parser vs `DecodeTraces` + `OtlpTraceMapper.Map`, every `SpanIngestItem` field and
`AttributesBytes` **byte for byte**. `OtlpTraceProtoLimitsTests` (depth cap, `MaxValueDepth = 64` —
without it a small POST of nested `array_value` is a stack overflow, i.e. process death with no
exception; the JSON parser gets this free from `Utf8JsonReader`, a hand-rolled proto reader does
not). `OtlpTraceProtoInvalidUtf8Tests` (reuse `WriteUtf8`/`WriteReplacingInvalid` verbatim — the
#79 fix applies unchanged). Extend `OtlpProtoPayloads.Traces_Realistic` with an `events[]` entry
(field 11), a `links[]` entry and an `array_value` attribute **before** trusting any number, or the
7 881 B baseline understates real SDK traffic. `OtlpGrpcMessageSegmentTests` retargeted at the new
parser. `OtlpTraceStreamingParityTests` must stay green unchanged.

**Probes.** `OtlpTraceProtoProbe` gains the parser arm (copy `OtlpLogProtoProbe`); `OtlpTraceAllocProbe`
guard tightens from `stream * 3 < dom` toward `stream * 20 < dom` **only after WP8** — leave it at
`* 3` here and say so in the commit.

**Reviewer must check.** (1) Attribute order and the map header: resource pairs first, span pairs
second, `WriteMapHeader(resCount + spanCount)`, span attrs win a collision because they are last.
(2) `service.name` is **excluded** from the pairs on the trace path — the opposite of logs — and
absent means the literal `"unknown"`. (3) Duplicate `service.name`: DOM takes the **first**, JSON
takes the **last**. Pick first-wins and **add the case to the JSON parity test too** — this is a
pre-existing divergence nobody has pinned. (4) `array_value`/`kvlist_value` are dropped to nil by
the DOM today; encoding them is a deliberate divergence in the client's favour — assert it, do not
paper over it. (5) Drop rules: trace_id ≠ 16 B or span_id ≠ 8 B drops the span; parent_span_id
parses only at exactly 8 B, else `default`. (6) `kind == 3 && IsAmetoInternalSpan` uses the byte
overload, which refuses URLs over 512 bytes while the char overload the DOM uses does not — follow
the JSON behaviour and record the divergence. (7) `end > start ? end - start : 0`, and a fixed64
past `long.MaxValue` lands as 0, matching the `long.TryParse` of the stringified value. (8) HTTP
status promotion: `http.response.status_code` beats `http.status_code` regardless of order, string
and int forms, clamped to `short`. (9) Partial-batch semantics change (a streaming parser leaves a
prefix in the ring) — that is **fine** here because `SpanRingBuffer.TryEnqueue` releases the drainer
semaphore per item; say so in the code so nobody adds a `NotifyBatchEnqueued`. (10)
`OtlpProtoDecoder.DecodeTraces` **stays in the tree** as the parity reference, exactly as
`DecodeLogs` did.

---

## WP2 — `perf/tm-traces-blob` (wave 1)

The single cleanest number in the round: `DeserializeAttributes` is **4 264 ns of the 4 557 ns
`WriteSpan` costs (94 %)** and **987 B of the 1 002 B it retains**, to inflate a 222–375 B msgpack
blob into a boxed `Dictionary<string, object?>` that the ingest path never reads — under the
engine's exclusive write lock, held **23 % of wall time at 50 k spans/s** while every query waits.
`SpanRecord.Attributes` is even documented as "lazy — null until read". It is eager on the write path.

| Item | Measured now | Target |
|---|---|---|
| TS#1 hot tier holds `ReadOnlyMemory<byte>`; `Attributes` becomes a real lazy accessor | 1 117 B/span retained; 56 MB at the 50 000 threshold, 112 MB across a flush | ≈ 400 B/span, ≈ 20 MB / 40 MB |
| TS#1 `SpanWriter.WriteAttributes` writes the blob instead of re-encoding a dictionary | decode on write **and** re-encode on flush | one copy |
| TS#5 `SpanReader` yields the blob, not a dictionary | `ReadAll` 1 740 B/span retained → a `MaxSpansPerPass` merge peaks at **199 MB**; TraceQL 913 B per span *scanned* | ≈ 375 B/span → ≈ 45 MB; TraceQL ≈ 100 B/span |
| TS#13 `GetAttr(params string[])` per row | a `string[]` per returned row | `ReadOnlySpan<string>` over a `static readonly` |

Decode on demand at the four read sites only: `TraceQLAst.cs:149-150,356-357`,
`TraceQLExecutor.cs:320-321`, `TraceStorageEngine.cs:3373-3374`, `TraceSummarySidecar.cs:127-128`.
The last three are **root spans only** — a few hundred per flush instead of 50 000. For TraceQL's
`AttrValue`/`AttrExists`, scan the blob with a `MessagePackReader` comparing UTF-8 keys and never
build the dictionary at all.

**Byte-format note.** Handing `SpanWriter` the original blob is faster *and* one fewer re-encode,
but the emitted bytes need not be identical to today's decode/re-encode round trip (int width, map
header size). The reader decodes a msgpack map either way, so this is a **byte-level, not
format-level** change. `SpanWriterAttrParityTests` (new) flushes a corpus spanning every OTLP value
type through the old `WriteAttributes(dictionary)` and the new `WriteRaw(blob)` and compares `.trc`
bytes; **if any case differs, keep `WriteAttributes` and feed it a lazily-decoded dictionary** — the
memory win is in the tier, not in the writer.

**`SpanBloom` stays untouched in this package.** At flush, decode the blob once to feed the existing
`SpanBloom.AddAttr(string, object?)`. That moves the decode off the ingest lock onto the flush
thread and keeps the bloom bits provably identical. Hashing from UTF-8 (TS#12) is a follow-up
because it can silently turn every existing `.trc` bloom into a false-negative source.

**Tests that must pin it.** `TraceQLThreeValuedTests` — a span that *cannot* answer must not answer
"no": a lazy decode that fails keeps returning null, never false. `TraceQLHttpStatusAbsenceTests`,
`TraceDetailOrderingTests`, `TraceSummarySidecarTests`, `SpanFormatV3Tests`, `SpanFormatV4Tests`,
`PlainSegmentReadbackTests`, `SpanWalTests`, `TraceFlushVisibilityTests`, `SpanSearchBoundTests` and
`SpanSearchHotTierBoundTests` (their printed per-span byte figures move — **update the figure, keep
the bound**).

**Probes.** New `TraceHotTierProbe` (µs/span, B/span allocated, **B/span retained**, at 0 and 8
attributes — the gate for the whole traces half of the round), `TraceCompactionMemoryProbe`
(`SpanReader.ReadAll` retained × `MaxSpansPerPass`), `TraceQlScanProbe` (allocated per span scanned).

**Reviewer must check.** The block buffer in `SpanReader` is `ArrayPool`-rented and returned per
block, so a lazy slice **cannot outlive the block** — take the copy (375 B vs 1 749 B is still
4.7×), do not keep the block alive. `_flushingSpans`, `RestoreSnapshotLocked` and
`UnflushedSpansLocked` hand out `SpanRecord` references that outlive the lock; with `byte[]` copies
that is safe, and it must stay safe (WP8 is where arena lifetimes arrive). `SpanWriter`'s
`default: w.Write(v.ToString())` fallback has no blob equivalent — confirm it is only reachable for
values the mapper cannot produce.

---

## WP3 — `perf/tm-metrics-hot` (wave 1)

The metrics OOM. **12 166 B per series is retained by a completely empty hot tier, forever, for
every series ever seen** — 54 % of the burst peak survives the drain. There is no `_hot.TryRemove`
anywhere in `MetricStorageEngine.cs`, and `List<T>.Clear()` does not shrink the backing array, so
each series permanently owns a `MetricDataPoint[]` sized to its largest burst. On the sandbox's own
numbers quoted at `MetricWriter.cs:52-57` (38 741 series) that is **≈ 470 MB of gen2 that nothing
can reclaim, in a container whose GC heap limit is 384 MB**.

| Item | Measured now | Target |
|---|---|---|
| M#2(a) `Drain` hands out the existing list and takes a fresh small one | a second full copy of every point per flush | one list, no copy |
| M#2(b) stale-series sweep in `FlushHotTierAsync` (`_hot.TryRemove` past `2 × MaxHotAge`) | 12 166 B/series retained forever | `retainedAfterDrain < peak / 4` |
| M#2(c) register with `MemoryShedRegistry` | only `SegmentIndexCache` is registered today | `RamPressureService` can force an early flush + sweep |
| M#6 every cap from `MemoryBudgets`, and **bytes not points** | 500 000 points = 20 MB gauge / **84 MB histogram**; WAL doubles to 32 MB; `ExemplarsPerMetric` 4 000 × ~120 B per metric **name** with no cap on the number of names | a 512 MB host gets a bounded tier; a histogram point is 4.3× a gauge point (346 vs 185 B) so a point count is the wrong unit |
| M#7 `GetMetricNames` reads `_meta.Keys`, not `_hot.Keys` | takes **all** `ConcurrentDictionary` table locks and materialises ~1.2 MB at 38 741 series, on every Explore page load | tens of entries, no ingest stall |
| M#11 `_coldLock` close gate; `PruneAsync` gated on `_disposed` | a query holding the read lock at dispose gets `ObjectDisposedException` mid-response; a waiter makes `Dispose` throw `SynchronizationLockException`, which escapes while `_disposeCompleted` still completes and the other disposers return believing teardown finished | either do not dispose `_coldLock` (a process-lifetime singleton, as `_snapshotLock` is already handled) or add a `_coldClosed` fence with every reader answering empty |
| M#13 `UpdateMeta` per label per point; the exemplar loop re-walks the batch | 4–8 concurrent-dictionary lookups per point in a steady state where nothing changes | cache `MetricMeta` on the `HotSeries`; fold the exemplar pass into the main loop behind a flag |

**This package owns `MemoryBudgets.cs` for the whole round** (see the ownership rules): it defines
`MetricHotTierFraction`, `TraceHotTierFraction` and `TraceMergeFraction` and re-balances the
existing logs fractions in one commit, so WP8 can consume the traces share in wave 3 without
reopening the file.

**Tests that must pin it.** `MetricWalTests`, `MetricFormatV3Tests`, `MetricChunkedRewriteTests`
(72 facts across the three) stay green — if the flush cadence changes, inject explicit thresholds
rather than inheriting them. New: a retained-memory fact; a dispose-during-`QueryAsync`-enumerator
fact for M#11; a fact that the derived caps on a 384 MB managed limit are what the plan says. Use
`ManualTimeProvider` while wiring options in, so the `MaxHotAge` / `FlushCheckInterval` paths become
testable without `Task.Delay`.

**Probes.** New `MetricHotTierRetentionProbe` — burst, threshold flush, `GC.GetTotalMemory(true)`
before and after; assert `afterDrain - empty < (loaded - empty) / 4`. **Feed the burst in
OTLP-sized chunks**, or the measurement reads the batch array instead of the tier (this cost the
recon a wrong number: 55 KB/series vs the true 12 KB/series). New `MetricFlushAllocProbe`
(alloc/point, ms/point, bytes-on-disk/point for a gauge and a 16-bucket histogram; assert
`flushBytes / points < 128`). Run the flush fact once under `DOTNET_GCHeapHardLimit=0x18000000`
(384 MB) at 500 k histogram points and confirm it no longer OOMs.

**Reviewer must check.** (a) `Drain`'s aliasing changes — the restore path at `:949-957` re-appends
from the snapshot and **must not see the same list**. (b) The sweep must not evict a series with
points in it, or one an open flush snapshot still names: do it inside `_snapshotLock`'s write lock,
after `Interlocked.Exchange(ref _hotPointCount, 0)`. (c) `_meta` is never pruned, so a metric that
stopped reporting stays in the names list — that is what `LastSeenMs` is for and the UI has it;
confirm the Explore list still behaves. (d) The `MetricWriteAheadLog` edit here is **the
initial-capacity parameter only** — WP6 owns that file's locking in wave 2.

---

## WP4 — `perf/tm-traces-writepath` (wave 2) — depends on WP2

Ordered so the **shutdown gate is the first commit**: every later package introduces pooled or
arena memory that a missing gate turns into a use-after-free, and WP8 is explicitly blocked on it.

| Item | Measured / observed now | Target |
|---|---|---|
| TS#8 (=TI#12) `IAsyncDisposable` with a write gate, heavy-phase count, reader drain | `grep '_disposed'` finds **three hits** in 3 500 lines; the engine is registered under six singleton interfaces so it is disposed six times and callers 2–6 **return while caller 1 is still inside a 599 ms flush**; `CompactOnePass` / `PruneAsync` / `BackfillNextSegment` / `CompactIndexOnce` check nothing and can **unlink `.trc` files after shutdown**; `SpanWriteAheadLog.Append` answers a post-dispose append with `return;` while the very next line still adds the span to the hot tier — queryable and unrecoverable at once | port `StorageEngine`'s write gate + `_heavyPhases` + `_readersDrained` + `_shutdownWaitBudget` verbatim; the second caller **awaits** the first |
| TS#4 (=TI#10) `WriteSpans(ReadOnlySpan<SpanIngestItem>)`, one engine lock + one WAL lock per drained batch | 4 interlocked round trips per span; the 293 ns/span bare figure bounds it, so ~80–150 ns is lock traffic — 25–50 % of the non-attribute cost | ~10 ns/span amortised |
| TI#6 + TS#10 span WAL **v2** | `WalVersion = 1` has **no checksum anywhere** — a torn append replays as a span with garbage name/service/attribute bytes straight into the hot tier, the same shape as the closed metrics-WAL incident, on the third signal; `FlushLocked` msyncs the **whole** 8→32 MB mapping under `_writeLock`; two `Encoding.UTF8` round trips per span on text that arrived as UTF-8 | v2 = `SpanWalEntryHeader` + `uint Crc` (CRC32C over checksummed-header + name + service + attrs, written last), `ReadAll` stops at the first mismatch; msync `[lastFlushed, writeOffset)` only, `FlushFileBuffers` outside the lock; a UTF-8 `Append` overload; growth capped against the budget |
| TS#6 aggregates off the read lock | `GetAggregateStatsAsync` 19.8 ms / 12.3 MB and `GetServiceGraphAsync` 26.7 ms / 16.1 MB, **every millisecond a read-lock hold `WriteSpan` waits out**; `BucketOf` is `new uint[19]` **per span** | accumulate into the aggregate's existing `uint[]` in place; snapshot references under the lock and do the passes outside it; a short TTL memo keyed on `(from, to, tier generation)` |
| TI#11 ingest warning storm | one formatted `LogWarning` with a `P0` format **per refused request**, written into the server's own log storage, under exactly the overload that caused the incident; and the batch-level 90 % refusal buys nothing because a batch admitted at 0.89 enqueues until `TryEnqueue` fails anyway | rate-limit to one per second or a counter on `/api/diagnostics`; drop the pre-check, keep the per-item result |

**Format change.** Span WAL v1 → v2 is the one planned format change in this round. It is gated by
the version byte, and `ReadAll` must **read v1** for one release — today `:165` re-initialises on an
unknown version, which would silently drop a v1 log. Also state explicitly, in the code, that there
is no periodic fsync between segment flushes: the mapping survives process death, not machine death.

**Tests that must pin it.** `SpanWalTests` (replay correctness) plus new `SpanWalV2Tests`: a v1 log
opens and replays; a truncated tail stops at the first CRC mismatch; a flipped byte in name /
service / attrs / header is caught; the msync range covers both the relocated tail and the header
page (the generation-0 terminator and the barrier ordering at `:334-345` are crash-safety critical).
New `TraceShutdownSeamTests`, modelled on what `tests/Ameto.Storage.Tests` got at `de6b682` —
**judged by seams, not by a 500 ms timer** — including a wedged compaction that must not hang
shutdown ("left frozen" log line verbatim). `TraceFlushVisibilityTests`, `TraceStatsEndpointTests`,
`ServiceGraphSidecarTests` green.

**Probes.** New `TraceAggregateLockProbe` — allocated bytes and read-lock hold for the four
aggregate endpoints **with a concurrent `WriteSpan` loop reporting the ingest throughput delta**.
`TraceHotTierProbe` (from WP2) re-run with a concurrent `SearchSpansAsync` loop for the contention
delta.

**Reviewer must check.** The batched lock grows the hold from ~300 ns to ~150 µs for a full
512-span batch, which **delays readers** — measure read-side latency before and after and cap the
batch under one hold (64–128) if it bites. WAL generation stamping must stay inside the same hold.
The two-phase `BeginFlush` / `CommitFlush` protocol already runs under `_writeLock` and is
unaffected — confirm it rather than assuming it. `GetAggregateStatsAsync`'s `Merge` reuses
`a.Buckets` across the tuple copy-back (`:2810-2822`): the in-place version **must not alias a
caller's array from `SpanReader.ReadStats`**. DI disposal order between the drainer singleton and
the engine singleton decides which runs first — the gate must be correct either way.

---

## WP5 — `perf/tm-traces-flush` (wave 2) — depends on WP2

Flush is **598.9 ms wall / 859.4 ms CPU / 11.98 µs per span / 20.9 MB allocated / +3.8 MB LOH** for
50 000 spans. The sidecars are ~3 % of the time and **36 % of the allocation**.

| Item | Measured now | Target |
|---|---|---|
| TS#7(a) sort an `int[]` of indices, or sort in place (`CompleteFlush` hands over a detached snapshot — it does) | `new List<SpanRecord>(spans)` = a 50 000-reference copy, 400 KB LOH, then `Sort` | no copy |
| TS#7(b) `svcBlockMap` → `Dictionary<string, List<uint>>` + a last-block guard | `SortedSet<uint>` = a red-black node per block; `new HashSet<ulong>()` per block | blocks arrive ascending, so the set is redundant |
| TS#7(c) trace index → a `(TraceId, uint)[]` sorted once | `Dictionary<TraceId, List<uint>>` with `new List<uint>(4)` per trace | what `TraceIndexFile` wants anyway |
| TS#7(d) `.svcgraph` reuses the writer's span→service pass | 6.3 ms + 1.4 MB, a second full pass, `new Dictionary<SpanId,string>(50 000)` ≈ 1.9 MB LOH | one pass |
| TS#7(e) `.tracesum` one pooled `ArrayBufferWriter`, pickle from `WrittenSpan` | 9.4 ms + 6.2 MB; two `MemoryStream`s, a `CopyTo`, a `ToArray()`, then `Pickle` — four copies | one buffer |
| TS#7(f) bloom fed from one decode of WP2's blob | today the attributes are decoded on the **ingest** lock and re-encoded here | decode once, on the flush thread |
| TS#7(g) `LZ4Level.L03` for flush, keep HC for compaction | flush is on the critical path for tier memory release; compaction is not | **measure the file-size delta first; do not adopt blind** |

**Tests that must pin it.** `SpanFormatV3Tests`, `SpanFormatV4Tests`, `PlainSegmentReadbackTests`,
`ServiceIndexCollisionTests`, `ServiceGraphSidecarTests`, `TraceSummarySidecarTests` — (a)–(f) are
**byte-identical** and must be asserted as such. (g) changes bytes on disk but not the format (LZ4
frames are self-describing and `SpanReader` calls `Unpickle`); it ships **only** with a measured
file-size number in the commit body, and is dropped if the increase is material.

**Probes.** New `TraceFlushProbe` — µs/span, B/span, LOH delta for `SpanWriter.Write`, **split into
blocks / bloom / trace index / each sidecar** so the 599 ms is attributable.

**Reviewer must check.** `SpanBloom.cs` is **not** an owner file — if a change here needs it, stop
and take it to the follow-up. Sorting the caller's snapshot in place is only safe because
`CompleteFlush` detaches it: re-verify that at the call site, not from the docstring.

---

## WP6 — `perf/tm-metrics-ingest` (wave 2) — depends on WP3

A 500-point batch materialises **8 000 label strings of which 26 are distinct** — 99.7 % are a
string already on the heap — and ingest **saturates at ~1.0 M points/s regardless of core count**
(1 thread 1 397 ns/point, 8 threads 8 236 ns/point per thread: textbook serialisation). The issue's
premise that "point ingest takes no locks" is false: it takes **two monitor acquisitions per point**,
one of them global.

| Item | Measured now | Target |
|---|---|---|
| M#1 intern label keys and values from UTF-8 | 1 199 B/point, 2 814 ns/point; ~900 B of it is label strings → **120 MB/s of gen0 churn at 100 k points/s**, promoted to gen2 because `LabelSet` is retained by `_hot`, `_wal._seriesIndex`, `_meta` and `ExemplarRing` for the process's life | `Intern(ReadOnlySpan<byte>, out string)` — `StringInternPool:123` is exactly this API and is alloc-free on a hit; `LabelSet` holds one interleaved `string[]` and compares `ReferenceEquals` first |
| M#1 **move `StringInternPool` to `Ameto.Core`** | it lives in `Ameto.Storage`, which `Ameto.Metrics` / `Ameto.Otel` do not reference; it has no storage dependency | this package performs the move for the round; WP8 consumes it in wave 3. **The move is cheap and measured: the type is already `public`, and 58 of its 61 referencing files already carry `using Ameto.Core`** — only `ServiceInternProbe.cs`, `StringInternPoolTests.cs` and `StringInternPoolSlotArrayTests.cs` need a `using` added |
| M#3(a) `RegisterSeriesLocked` lookup outside the lock | a `ConcurrentDictionary` read done *inside* the exclusive lock | `TryGetValue` first, lock only on a miss |
| M#3(b) reserve the mapped range with one `Interlocked.Add(ref _writeOffset, …)`, store the header outside the lock | `lock (_writeLock)` **per point**, plus `HotSeries.Append`'s own lock per point | the lock covers only `Grow()` and the header store |
| M#3(c) `Append(ReadOnlySpan<MetricIngestItem>)` | one acquisition per point | one per OTLP batch (~500–1 000 points) — cheapest change, most of the win, and it matches how `Ingest` is called |
| M#8 hot query touches only its own metric | `QueryAsync` and `GetLatestAsync` full-scan **every series of every metric** with a per-character `Equals`; `GetPoints` then runs `Where().OrderBy().ToList()` under the series lock — a full stable sort of an already-sorted list, per series per query | a name→series index; binary-search the range in `GetPoints` |
| M#10 perf half | `Grow()` is called from **inside** `lock (_writeLock)` and unmaps / remaps / `SetLength`s — every ingest thread in the process stalls on a file resize | bounded increment past a threshold, resize behind a short reader drain |
| M#13 exemplar ring | a `lock` per exemplar; `Snapshot()` copies the whole 4 000-entry ring on every `GET /exemplars` | |

**Tests that must pin it.** `OtlpMetricProtoParityTests` (the parity gate for M#1), `MetricWalTests`
(the `Generation == 0` "unwritten" convention at `:198` is what makes a torn reservation safe on
replay — the reserving append must keep it true), `MetricFormatV3Tests`,
`MetricChunkedRewriteTests`. New: a fact that `LabelSet` equality and hash stay **value-based for
uninterned strings**, because `MetricReader` and the WAL build label sets from disk; a fact that the
pool degrades to a plain `new string` past its cap (`PoolExhausted`) rather than dropping a point.
`MetricReader.ReadLabels:301-302` and `MetricWriteAheadLog.ReadString` must join the same pool so
replay and cold reads do not defeat it.

**Probes.** Extend `MetricIngestContentionProbe` to **sweep 1/2/4/8 threads and print per-thread
ns/point** — the single number it prints today hides the flat scaling entirely. `OtlpMetricProtoProbe`
guard tightens from `spanBytes * 3 < domBytes` to `* 8`.

**Reviewer must check.** `BeginFlush` / `CommitFlush` / `Compact` / `ReadAll` all assume
`_writeOffset` is stable under `_writeLock` — a reserving append **must make `BeginFlush` wait for
in-flight reservations** (the heavy-phase-count + reader-wait pattern from `StorageEngine`).
`GetPoints` is also the writer's path (`MetricWriter.cs:125,235`): removing the sort is only safe
with a cheap `isSorted` flag flipped on an out-of-order append, because two exporters interleaving
are not chronological. The `StringInternPool` move must leave `Ameto.Storage`'s own callers
compiling with no behaviour change — that is ten files, all mechanical, and it is the reviewer's job
to confirm none of them changed semantics.

---

## WP7 — `perf/tm-metrics-query` (wave 3) — depends on WP3, WP6

**30 MB allocated and 111 ms of CPU to answer a query whose data is 48 KB on disk: 625×.** And the
alert evaluator runs one `QueryAsync` **per enabled rule every 15 s**, unattended, forever — this is
the metrics contribution to the idle CPU sawing.

| Item | Measured now | Target |
|---|---|---|
| M#4(a) `MetricReader.ReadAsync` skips the copy when the range covers the series; better, push `fromNano` / `toNano` into `ReadPointsV3` | `series.Points.Where(...).ToList()` — a full copy of every series even when the whole series is in range | out-of-range points never materialised |
| M#4(b) `DeserializeSeries` compares UTF-8 map keys | `r.ReadString()` for the **map key**, 6 per series | `TryReadStringSpan` + a byte compare on `k` / `u` / `lbs` / `bnds` / `pts` / `cnt` |
| M#4(c) both `MatchesLabels` sites scan `Pairs` | `labels.Pairs.ToDictionary(...)` — a new `Dictionary` **per series**, twice (reader and hot tier) | the pairs are sorted by key and matcher counts are tiny: a linear/binary scan, no dictionary |
| M#4(d) `Downsample` one forward pass | `GroupBy` + `Select` + inner `OrderBy` + `Last` + `Average`/`Sum` + `OrderBy` + `ToList` — five iterators and a `List` per series | a `List` sized `(span/bucket)+1`, no LINQ |
| M#4(e) no `SortedDictionary<long, …>` | `MergeFragments`, `ReduceByTimestamp` and `AggregateQuantile` each build one — a red-black node (48+ B) **per point** | `List` + `Sort`, or a linear k-way merge (fragments are each internally sorted) |
| M#4(f) pooled `double[]` for per-step buckets | one `double[nBuckets]` per timestamp per group | rented |
| M#9 source-generated JSON, streamed rows | every endpoint is `Results.Json(<object>)` with no `JsonTypeInfo`, and each materialises the whole answer first — **120 000 `MetricPointDto` instances before a byte is written** for a 2 000-series answer | a `MetricJson` context for the seven DTOs; a `Utf8JsonWriter` row writer over `IAsyncEnumerable<MetricSeries>` |
| M#12 rollup stops re-decoding | every source file re-opened, re-inflated and every label string re-materialised **once per series chunk**, only to throw away the series the chunk does not want | cache pass 0's `SeriesKey` per (file, ordinal) and skip by position — v3 writes series back to back, so a skip is a header walk, not a decode |

**Tests that must pin it.** A **`Downsample` golden-series parity test written before the de-LINQ
touches it** — `Downsample` is shared with the **rollup**, so a behaviour change rewrites files on
disk. Counter/histogram "take last in bucket" and gauge "average" semantics must not move. Ties on
equal timestamps: `MergeFragments` is "later fragment wins", `DedupeByTimestamp` is "last wins" —
both must survive. `MetricChunkedRewriteTests` (and its explicit refusal to align chunks to files,
which M#12 must not break), `MetricFormatV3Tests`. New `MetricResponseShapeTests`: a
**byte-for-byte** response comparison before the STJ switch — the Angular client reads `ts`,
`value`, `count`, `sum`, `labels`, `bounds`, `columns` and ASP.NET Core's default is camelCase.

**Probes.** New `MetricQueryAllocProbe` — a cold-loaded engine, then raw `QueryAsync`,
`Rate` + group-by, `Quantile`, `Last` + filter; alloc and ms per query per stored series; assert
`bytes < 64 * points`. `MetricReaderStreamingProbe` stays the gate for the reader changes.

**Reviewer must check.** Every LINQ removal is a behaviour question, not a style question: confirm
the ordering, the tie-breaking and the empty-input case of each replaced chain against the golden
test, not against the old code's shape.

---

## WP8 — `perf/tm-traces-sink` (wave 3) — depends on WP1, WP2, **WP4 (hard)**, WP6

The parser in WP1 cannot get below **330 B/span** no matter how good it is, because there is no raw
span sink: every span is a `SpanIngestItem` **class** plus an attribute `byte[]`, through a
**reference** ring of 65 536 slots — a 512 KB LOH pointer array the GC scans, holding up to 65 536
live object graphs (**≈ 33 MB**) that get promoted into gen1/gen2 whenever the drainer lags. The
logs path reaches **0 B/record** because `IOtlpLogSink.TryIngestRaw` writes into a slab-backed ring.

| Item | Measured now | Target |
|---|---|---|
| TI#3 `ISpanSink.TryIngestRaw(TraceId, SpanId parent, long startNano, long durationNanos, ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8, SpanKind, SpanStatusCode, short httpStatus, ReadOnlySpan<byte> msgpackAttrs)` over a slab-backed ring | 330 B/span on the JSON path is essentially all of this | 0 B/span |
| TI#3 reuse `SpanHeader` — **it is already written for exactly this** (72 fixed bytes, pool indices for name and service, arena offset + length for the blob) and is **dead code today**: `grep -rn SpanHeader src/ tests/` returns 2 hits, both in `SpanRecord.cs` | — | the ring slot becomes 72 B |
| TI#5 intern span names and service names | **no `StringInternPool` anywhere in `Ameto.Tracing`**; every span carries a fresh `string` for a route template that repeats across essentially every span in a batch (~80 B/span, ≈ 4 MB per full tier), transcoded UTF-8→UTF-16→UTF-8 twice per span for nothing; `ServiceName` is one string per `resourceSpans` block but a **different object per request**, so a tier spanning 10 000 requests holds 10 000 copies of `"Wallet.API"` | intern the service once per `ResourceSpans` block (the log parser's `InternService` / `ServiceIdx`), the name per span, both from `ReadOnlySpan<byte>` without ever building a string |
| TS#9 ring back-pressure by bytes | `FillFraction >= 0.9` — a **count**, with no idea how big the items are | a byte budget, sized from `MemoryBudgets` |
| TS#3 (=TI#4) traces caps from the budget | `HotFlushThreshold` 50 000, `CompactionThreshold` 60 000, `MaxSpansPerPass` 120 000, `MinSegmentSpans` 500, `SpanRingBuffer.DefaultCapacity` 1<<16 and `TraceIndexCompactor.MaxEntriesPerMerge` 2 000 000 are all `const`, and `TracesOptions` has **three settings, none about memory**. The code's own comment says a span count is the wrong unit; measured, `MaxSpansPerPass` is **199 MB** and `HotFlushThreshold` is 56 MB. A 512 MB stand gets exactly what a 64 GB box gets | flush on `max(spanCount ≥ N, hotBytes ≥ budget)` with `hotBytes` accumulated from `nameLen + serviceLen + attrLen` as spans are added (free — the WAL already computes those three); `SelectCompactionBatch` plans against bytes; `TracesOptions.HotTierMaxBytes` / `MergeBudgetBytes` / `RingCapacity` overrides; `AddSingleton<SpanRingBuffer>()` currently uses the default ctor, so the capacity is **unreachable from config** — fix that |

**Hard dependency on WP4.** This is the first package that puts pooled / arena memory under readers.
It does not start until WP4's shutdown gate, heavy-phase count and reader drain are merged.

**The `SlabArena` move is not free, unlike `StringInternPool`'s.** `SlabArena` is
`internal sealed unsafe class` in `namespace Ameto.Ingestion`, and its tests
(`SlabArenaCommitTests`, `HugePageOptOutDecisionTests`, `IngestArenaCommitFailureTests`) reach it
through `Ameto.Ingestion`'s `InternalsVisibleTo`. Moving the file to `Ameto.Core` therefore needs
`Ameto.Core.csproj` to grant `InternalsVisibleTo` to `Ameto.Tracing`, `Ameto.Perf` and
`Ameto.Integration.Tests` — **or** the type becomes `public`. Decide that in the first commit, not
halfway through: it is the one part of this package that reaches into the logs module, and the logs
arena tests must stay green untouched.

**Tests that must pin it.** `OtlpTraceStreamingParityTests` compares `SpanIngestItem` fields, so the
parity gate needs a capturing sink modelled on `OtlpLogProtoParityTests.CapturingSink`.
`OtlpTraceProtoParityTests` (WP1) retargeted at the raw sink, still byte-for-byte on the attribute
blob. `SpanWalTests`, `TraceFlushVisibilityTests`, `SpanSearchHotTierBoundTests`,
`SpanTraceReadBoundTests`, `TraceDetailOrderingTests`, `CompactionThresholdTests`. New: a fact that
the derived traces caps on a 384 MB managed limit are what the plan says; a fact that a saturated
intern pool answering −1 falls back to a plain string and **never drops a span**.

**Probes.** `OtlpTraceAllocProbe` guard tightens to `stream * 20 < dom`. New `SpanRingBytesProbe` —
fill the ring, read `GC.GetTotalMemory(true)`; none exists today. `TraceHotTierProbe` (WP2) re-run.

**Reviewer must check.** `SpanIngestItem` is public **and is the WAL replay type**
(`SpanWriteAheadLog.ReadAll` → `RecoverFromWal`): keep it for replay, or drive replay through the
same raw sink — do not leave two shapes. `TryEnqueue`'s CAS-then-store leaves a published head with
a null slot that `TryDequeueMany` handles by breaking — **preserve that**. The intern pool must be
bounded and shed on flush or it becomes its own leak for high-cardinality names
(`/api/user/12345`). The arena's lifetime must be the snapshot's: `_flushingSpans`,
`RestoreSnapshotLocked` and `UnflushedSpansLocked` hand out references that outlive the lock. A
smaller tier means more, smaller segments — `CompactionThreshold` must move with it, and
`CompactionThresholdTests` must still pair the same files up on a same-size install.

**Deliberately out of scope:** the fully native `SpanHeader[]` hot tier (TS#2). WP2 already removes
987 B of the 1 117 B/span; what remains is ~130 B of `SpanRecord` + `List` overhead, and the recon
calls TS#2 "the single biggest change in the round" with the widest blast radius. It is a follow-up,
to be judged on `TraceHotTierProbe`'s post-WP8 number, not on the baseline.

---

## WP9 — `perf/tm-traces-read` (wave 3) — depends on WP2

| Item | Measured / observed now | Target |
|---|---|---|
| TS#11 trace detail and flamegraph | a 2 000-span trace builds **2 000 dictionaries, 16 000 strings and 6 000 id strings**, buffers the whole `List<SpanDto>`, and serialises it **reflectively** although `TraceStreamJson` exists for the SSE path in the same file; est. 3–4 MB and 20–30 ms per request | carry the raw msgpack into the response (the logs side already has `MsgPackJsonTranscoder`); ids via `string.Create` over a `stackalloc char[32]`; add `SpanDto` / `FlamegraphNode` to a source-generated context; stream spans as they arrive |
| TS#13 `TraceIndexStore.Lookup` | `new List<TraceIndexReader>(open.Count)` under `_gate` plus `new List<TraceIndexHit>(2)` on **every** trace lookup | pooled / `[ThreadStatic]`; a bloom-miss lookup becomes allocation-free |
| TraceQL row smalls | `BuildRow` allocates a `HashSet<string>` per returned row (bounded by `limit` — take it only if it is free) | |
| SSE hot-tier re-walk | `TraceQueryEndpointMapper.cs:440-453` already documents that **every SSE page re-walks the whole hot tier** — with WP4's numbers that is 6.8 ms and 2.8 MB per page, per connected client, forever | memo it against the tier generation, or document why not; do **not** disturb the scan-floor / `Unreadable` contract |

**Client-visible risk — the one to argue about before writing code.** Attribute values stringify via
`object?.ToString()` today; a msgpack→JSON transcode emits numbers as numbers. That is a **shape
change the Angular client can see**. Either keep stringifying inside the transcode, or coordinate
the change with the client in the same PR. Default to keeping the strings.

**Tests that must pin it.** New `TraceDetailShapeTests`: a **byte-for-byte** response comparison of
`GET /api/traces/{id}` and the flamegraph endpoint before and after. `TraceStreamEndpointTests`,
`TraceStreamPagingFloorTests`, `TraceIndexEndpointTests`, `TraceIndexLookupTests`,
`CoverageSnapshotWindowTests` green. The scan-floor / `Unreadable` contract across `SpanScanFloor`,
`TraceListPage`, `TraceQueryPage`, `VanishedRegionLog` and both SSE loops is correctness-critical and
heavily tested — **do not disturb it**.

**Probes.** New `TraceDetailAllocProbe` — allocated bytes and ms for `GET /api/traces/{id}` on a
2 000-span trace; none exists today.

**Reviewer must check.** The refcounted index readers (`TryAcquire` / `Release` / `Retire`) are the
only protection for the native bloom memory on the read path — a pooled list in `Lookup` must not
extend a reader's lifetime past its `Release`.

---

## What the 512 MB stand gets

The console.ntpayments container is 512 MB with a 384 MB GC heap limit, and logs already hold
`IndexCacheBytes` 48 MB and `HotTier.MaxSizeBytes` 16 MB. Measured at `main`, traces and metrics
can *legally* claim more than that between them.

| Structure | At `main` | After | Package |
|---|---|---|---|
| traces hot tier (50 000 spans × 1 117 B) | **56 MB** | ≈ 20 MB, then byte-budgeted | WP2, then WP8 |
| traces in-flight flush snapshot (a second tier, alive for the whole build) | **+56 MB** | ≈ +20 MB | WP2 |
| `SpanRingBuffer` (65 536 `SpanIngestItem` graphs, gen2-promoted when the drainer lags) | **≈ 33 MB** | 72 B/slot, byte back-pressure | WP8 |
| `CompactOnePass` `allSpans` (`MaxSpansPerPass` 120 000 × 1 740 B) | **199 MB** | ≈ 45 MB, then a byte budget | WP2, then WP8 |
| one TraceQL POST in flight (`limit*10` = 10 000 rows, **not admission-controlled**) | **17 MB per concurrent request** | ≈ 4 MB | WP2 |
| span WAL mapping (doubles, never shrinks, msyncs whole) | 32 MB at 49 000 spans | growth capped against the budget | WP4 |
| **traces, steady state + one compaction + one query** | **≈ 393 MB** | **≈ 110 MB** | |
| metrics hot tier retained by an **empty** tier, per series ever seen | **12 166 B × 38 741 ≈ 470 MB, unreclaimable** | swept past `2 × MaxHotAge`; `retainedAfterDrain < peak/4` | **WP3** |
| metrics tier at `HotFlushThreshold` 500 000 points | 20 MB gauge / **84 MB histogram**, unbounded by the host | a budget fraction, counted in **bytes** | WP3 |
| metrics WAL (doubles from 8 MB) | 32 MB on the histogram workload | initial capacity from the budget, bounded increment | WP3, WP6 |
| one metrics query in flight | **28–36 MB (9 % of the heap limit)**, four times a minute per alert rule | ≈ 2 MB | WP7 |

**The two sentences that matter.** The metrics OOM is **WP3** — nothing else in the round touches
470 MB of per-series arrays that an empty tier never releases. The traces OOM is **WP2** — the
attribute dictionary is 987 B of the 1 117 B/span, and it is what makes the hot tier 56 MB, the
flush snapshot another 56 MB, and a compaction pass 199 MB; **WP8** then replaces the span counts
with byte budgets so a 512 MB host stops getting a 64 GB host's caps.

## Gates

- `dotnet build Ameto.slnx -c Release` **and** `-c Debug` clean — **CI runs Debug**; a package that
  only builds in Release is not done.
- Suites for the touched projects green: Core / Indexing / Storage / Query / Integration / Perf.
- Every behavioural change has a test that **fails without it**. Parity tests pin **byte equality**
  of `.trc` segments, sidecars, `.mts` sections, attribute blobs and JSON responses. On-disk formats
  stay byte-identical except span WAL v2 (WP4), which is gated by its version byte and reads v1.
- Every item records **before/after numbers from a probe in its commit body**.
- Each package gets an **adversarial review of its commits** (`base..head`), and then a **second
  review of the fix commits that answer that review** — PR #79 taught us the fix commits carried
  6 new defects and 9 untested fixes.
- After merging each package into `perf/traces-metrics-cpu-alloc`: rebuild (Release + Debug), full
  suites, and the round's probes — `OtlpTraceProtoProbe`, `OtlpTraceAllocProbe`,
  `TraceHotTierProbe`, `TraceCompactionMemoryProbe`, `TraceQlScanProbe`, `TraceFlushProbe`,
  `TraceAggregateLockProbe`, `OtlpMetricProtoProbe`, `MetricIngestContentionProbe`,
  `MetricHotTierRetentionProbe`, `MetricFlushAllocProbe`, `MetricQueryAllocProbe`.
- Integration review of the whole branch before the PR.
- Load comparison against `main`: `tools/loadtest/k6-traces.js` and `tools/loadtest/k6-metrics.js`
  against a local server, CPU and RSS recorded at the same offered rate, **and** a run under
  `DOTNET_GCHeapHardLimit=0x18000000` (384 MB) to reproduce the stand.

## Merge order

Waves are sequential; within a wave the three branches are independent and the merge order only
decides who rebases. Merge the widest-blast-radius package of each wave first.

**Wave 1:** WP2 → WP1 → WP3.
**Wave 2:** WP4 → WP5 → WP6. (WP4 first: its shutdown gate is what WP8 is blocked on.)
**Wave 3:** WP8 → WP7 → WP9.

Each merge is followed by a review of the package's commits, a rebuild in both configurations and
the full suites.

## Follow-ups (not in this round)

- **TS#2 native hot tier** — `SpanHeader[]` over `NativeMemory` / `SlabArena`, `_traceIdx` as
  `(int First, int Count)` runs or dropped entirely. WP2 + WP8 take 1 117 B/span down to ~130 B of
  `SpanRecord` + `List` overhead; judge TS#2 on `TraceHotTierProbe`'s post-WP8 number, not on the
  baseline.
- **TS#12 `SpanBloom` UTF-8 hashing** — the one finding that can change on-disk *semantics*. The
  hash is over `lowercase(value.ToString())`; a UTF-8 path must produce the same bytes for the same
  value including `long` / `double` formatting, or every existing `.trc` bloom becomes a
  false-negative source — **silent data loss on TraceQL attribute predicates**. Needs either a fuzz
  proof over the value space or a bloom marker bump with old blooms treated as permissive.
- **M#10 durability half** — metrics WAL v2 with a per-entry CRC32C and an msync of
  `[lastFlushed, writeOffset)`. Today the file says outright that nothing msyncs it, and
  `GenerationSanityMargin` / `SeriesIndexSanityCap` exist precisely because a torn entry cannot be
  told from a good one. This is the residual from the poisoned-WAL incident; it is an on-disk format
  change (`EntryHeaderSize` moves, and every offset arithmetic in the class with it) and deserves
  its own PR.
- **Span WAL shrink** — `SetLength` + remap when `CommitFlush` leaves the tail far below capacity
  for N cycles. Needs its own crash-point test; WP4 takes the bounded growth and the msync range
  without it.
- **OTLP/HTTP `Content-Encoding: gzip`** — `grep` finds no `UseRequestDecompression` and no
  `Content-Encoding` handling anywhere in `src/`. The collector's `otlphttp` exporter **defaults to
  `compression: gzip`**, so a gzipped `POST /v1/traces` or `/v1/metrics` fails the parse and gets
  400. gRPC handles `grpc-encoding`; HTTP does not. A functional gap on all three signals, carried
  over unclosed from the logs round.
- **`MetricWriter` `LZ4Codec.Encode` into a pooled buffer** instead of `LZ4Pickler.Pickle` — touches
  the section bytes, so it needs a format note; WP7 takes the pooled `IBufferWriter` without it.
- **Deployment** — `GCHeapHardLimitPercent` and `PublishReadyToRun` on 512 MB hosts, as already
  recommended by the logs round.
