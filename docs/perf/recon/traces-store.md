# Traces — storage, query, background and memory reconnaissance (worktree `AMeto-recon-traces-store` @ main `0a7389d`)

Paths traced end to end: `SpanDrainer.DrainLoopAsync` → `TraceStorageEngine.WriteSpan` → `SpanWriteAheadLog.Append` + `AddToHotTierLocked`; flush (`FlushIfDue`/`TryStartFlushLocked` → `TakeSnapshotLocked` → `CompleteFlush` → `SpanWriter.Write` → blocks/LZ4-HC/`SpanBloom`/service index + `.stats`/`.svcgraph`/`.tracesum` + `TraceIndexFile` run + `TraceManifest`); background (`TraceCompactionWorker` → `LoadColdSegments`/`CompactSmallSegments`→`CompactOnePass`; `TraceIndexBackfillWorker` → `AdoptUnnamedSegments`/`BackfillNextSegment`/`CompactIndexOnce`; retention `PruneAsync`); read (`GetTraceAsync`, `SearchSpansAsync`, `GetTraceListAsync`, `GetTraceVolumeAsync`, `GetAggregateStatsAsync`, `GetServiceGraphAsync`, `TraceQLParser`/`TraceQLExecutor`, `TraceQueryEndpointMapper` incl. the two SSE loops).

## Headline: why traces OOM'd on the 512 MB stand

**The traces hot tier is a `List<SpanRecord>` of managed objects, and every span's attribute blob is inflated into a `Dictionary<string, object?>` on the way in and held there until flush.** `TraceStorageEngine.cs:23-24`, `:589-616`. Measured below: **1 117 B retained per span**, 1 647 B allocated per span, 5.12 µs of CPU per span.

`SpanHeader` (`SpanRecord.cs:161-206`) — the 72-byte struct whose docstring says "stored in the ring buffer and hot-tier NativeMemory array" — is **dead code**: `grep -rn SpanHeader src/ tests/` returns only its own declaration and `SizeOf`. Nothing in the traces module allocates native memory, uses `SlabArena`, `IngestBufferPool`, `StringInternPool`, `MemoryBudgets`, `PoolTrimPolicy` or `MemoryShedRegistry`. The logs module's whole memory architecture stops at the module boundary.

The 512 MB arithmetic, all measured on this machine at main:

| structure | per span | at its own cap | source |
|---|---|---|---|
| hot tier `_hotSpans` + `_traceIdx` | 1 117 B | `HotFlushThreshold` 50 000 → **56 MB** | probe 3 |
| in-flight flush snapshot `_flushingSpans` (lives for the whole build) | 1 117 B | 50 000 → **56 MB** | `:41`, `:1713-1735` |
| `SpanRingBuffer._slots` (`SpanIngestItem?[65536]`, each holding 2 strings + the msgpack `byte[]`) | ≈ 500 B | 65 536 → **≈ 33 MB** | `SpanRingBuffer.cs:13-15` |
| `CompactOnePass` `allSpans` | 1 740 B | `MaxSpansPerPass` 120 000 → **199 MB** | probe 6 |
| `TraceQLExecutor` `spans` list, POST `/api/traces/query` | 1 740 B | `limit*10` = 10 000 → **17 MB per concurrent request**, not admission-controlled | `TraceQLExecutor.cs:180-205` |
| span WAL mmap (doubling, never shrinks) | 685 B | 32 MB observed at 49 000 spans | probe 3 |
| `TraceIndexCompactor` merge | 60-88 B/entry | `MaxEntriesPerMerge` 2 000 000 → 150 MB (documented) | `TraceIndexCompactor.cs:52-59` |

A steady-state ingest with one compaction pass running and one TraceQL POST in flight is 56 + 56 + 33 + 199 + 17 + 32 = **393 MB of traces alone**, on a container where logs already hold `IndexCacheBytes` 48 MB and `HotTier.MaxSizeBytes` 16 MB. Nothing in that column is derived from available memory: `HotFlushThreshold`, `CompactionThreshold`, `MaxSpansPerPass`, `MaxEntriesPerMerge` and `SpanRingBuffer.DefaultCapacity` are `const`, and `TracesOptions` (`Options.cs:376-414`) has **three settings, none of them about memory** (`IndexBackfill`, `SegmentFormatV4`, `IndexEnabled`). A 512 MB stand gets exactly what a 64 GB box gets.

## Prior work already in place (do not re-do)

- **Bounded search**: `SearchSpansAsync` (`TraceStorageEngine.cs:1327-1650`) uses a top-K `PriorityQueue` per tier with a `present` set bounded by the heap, not by the match count; `SpanReader.SearchAsync` streams blocks. `SpanSearchBoundTests` pins it.
- **Flush off the lock**: the segment build runs outside `_lock`; `_flushingSpans` keeps the detached snapshot visible to readers (`:41-44`, `:1922-1937`).
- **Scan-floor / `Unreadable` contract** across `SpanScanFloor`, `TraceListPage`, `TraceQueryPage`, `VanishedRegionLog`, both SSE loops. Correctness, heavily tested — do not disturb.
- **Trace-id index** (`TraceIndexFile`/`TraceIndexStore`/`TraceManifest`) with refcounted readers (`TryAcquire`/`Release`/`Retire`, `TraceIndexFile.cs:731-790`) — native bloom memory is *not* use-after-free-able from the read path. `_coveredByOpen` built at publish, not per lookup.
- **`_coldSegments` sorted once at the swap**, not per reader (`:55-65`).
- **v4 segment format** (empty per-segment trace index, −38 % file size), read unconditionally, written only behind `SegmentFormatV4`.
- **`SpanWriter` block buffer reuse** (`SpanWriter.cs:170-176`), `ResetWrittenCount()` not `Clear()`, `LZ4Pickler.Pickle` straight from `WrittenSpan`, index compressed from `GetBuffer()` not `ToArray()`.
- **OTLP/JSON traces** already stream (`OtlpTraceStreamParser`) into `AttributesBytes` with no DOM.

---

## Findings (ranked by expected impact)

### 1. The hot tier deserialises every span's attributes on ingest and re-serialises them at flush — CONFIRMED (CPU + alloc + resident memory, per span)
`TraceStorageEngine.cs:589-616` (`AddToHotTierLocked`), `:2783-2792` (`DeserializeAttributes`), `SpanWriter.cs:460-479` (`WriteAttributes`).

The mapper already produced msgpack: `OtlpTraceMapper.SerializeAttributes` (`OtlpTraceMapper.cs:149-187`) / `OtlpTraceStreamParser.cs:289-310` hand `SpanIngestItem.AttributesBytes` over. `AddToHotTierLocked` then calls `MessagePackSerializer.Deserialize<Dictionary<string, object?>>(bytes)` — a `Dictionary` + a fresh `string` per key + a fresh `string`/box per value — and the tier holds that graph for up to 50 000 spans or 1 hour. At flush, `SpanWriter.WriteAttributes` walks the dictionary and writes **the same bytes back out**, type-switching each boxed `object?`.

**MEASURED** (probe `AttributeRoundTrip_Cost`, 8-attribute SqlClient span, 375 B msgpack): **3.48 µs and 1 496 B allocated per span** for the deserialise alone — **68 % of the 5.12 µs/span `WriteSpan` costs and 91 % of the 1 647 B it allocates**. At the stand's 100k spans/s that is 3.5 cores and 150 MB/s of gen0 churn, all of it to reproduce bytes the caller already had.

**Fix:** keep the blob. `SpanRecord` gains `ReadOnlyMemory<byte> AttributesBytes` and `Attributes` becomes a lazy accessor that decodes on demand (only `GetAttr`, `TraceQL` attribute predicates, `SpanBloom.AddAttr` and `SpanDto.From` need it). `SpanWriter.WriteAttributes` becomes `w.WriteRaw(span)` when the blob is present — **byte-identical output**, because `SerializeAttributes` already writes a map header + typed values in the same encoding. `SpanBloom.AddAttr` gets a `MessagePackReader` overload that hashes key/value straight from UTF-8 (it currently takes `string`, walks char-by-char through `char.ToLowerInvariant`, and calls `value.ToString()` per non-string value). Blobs should be copied into a per-tier `SlabArena` rather than kept as `byte[]` (finding 2).
**Risk:** parity of the written `attrs` map must be byte-for-byte — pin with a test that flushes the same corpus through old and new `WriteAttributes` and compares `.trc` bytes. `SpanReader` already produces a dictionary from disk, so the reader side changes only if we make it lazy too (do it: finding 5). `SpanWriter.WriteAttributes`'s `default: w.Write(v.ToString())` fallback has no blob equivalent — only reachable for values the mapper cannot produce.
**Measure:** the probe below (`HotTier_IngestCostAndRetainedBytes` + `AttributeRoundTrip_Cost`); promote it to `tests/Ameto.Perf/TraceHotTierProbe.cs`.

### 2. Hot tier is 100 % managed objects — CONFIRMED (resident memory + gen2, per span)
`TraceStorageEngine.cs:23-24`, `:589-616`; `SpanRecord.cs:208-233`.

Per span the tier holds: a `SpanRecord` (~104 B), the attribute `Dictionary` + 8 key strings + 8 value strings/boxes (the bulk), and an entry in `_traceIdx` whose value is `new List<int>(4)` **per trace** (`:611-615`, and again in `RestoreSnapshotLocked` `:1951`) — at 10 spans/trace that is 5 000 `List<int>` + 5 000 `int[4]` per tier. `Name` and `ServiceName` are fresh strings from the decoder, never interned.

**MEASURED: 1 117 B/span retained → 56 MB at the 50 000 flush threshold, doubling to 112 MB while a flush builds** (probe 3). Three gen2 collections during a single 49 000-span ingest run; everything is released at once at flush, which is the classic gen2-churn shape.

**Fix:** the logs shape. `SpanHeader` already exists and is the right 72 bytes — give the tier a `NativeMemory`/`SlabArena`-backed `SpanHeader[]` plus one arena for the msgpack blobs, and a `StringInternPool` for `Name`/`ServiceName` (`NamePoolIndex`/`ServiceNamePoolIndex` fields are already in the struct). `_traceIdx` becomes `Dictionary<TraceId, (int First, int Count)>` over a run-packed offset array, or is dropped entirely — it exists only for `GetTraceAsync`'s hot lookup, which could scan 50 000 headers in ~50 µs. Size the tier from `MemoryBudgets` (finding 3) instead of a span count.
**Risk:** `_flushingSpans`, `RestoreSnapshotLocked` and `UnflushedSpansLocked` all hand out `SpanRecord` references that outlive the lock; the arena's lifetime must be the snapshot's. This is the single biggest change in the round — stage it after 1 and 3. Tests to pin: `TraceFlushVisibilityTests`, `SpanSearchHotTierBoundTests`, `SpanTraceReadBoundTests`, `SpanWalTests`, `TraceDetailOrderingTests`.
**Measure:** `HotTier_IngestCostAndRetainedBytes` retained B/span.

### 3. No traces budget is derived from available memory — CONFIRMED (resident memory, whole module)
`TraceStorageEngine.cs:294` `HotFlushThreshold = 50_000`, `:321` `CompactionThreshold = 60_000`, `:350` `MaxSpansPerPass = 120_000`, `:359` `MinSegmentSpans = 500`, `:360` `MaxHotAge = 1 h`; `SpanRingBuffer.cs:13` `1 << 16`; `TraceIndexCompactor.cs:59` `MaxEntriesPerMerge = 2_000_000`; `SpanWriteAheadLog.cs:76` `DefaultCapacity = 8 MB`. `Options.cs:376-414` `TracesOptions` has no memory knob. `grep -rn MemoryBudgets src/Ameto.Tracing` → nothing.

The code itself says a span count is the wrong unit (`:350` docstring: "the same 120 000 is 24 MB of bare spans or 210 MB of attribute-heavy ones"). **MEASURED: 1 740 B/span for the fixture's ordinary 8-attribute OTel span** — so `MaxSpansPerPass` is 199 MB, not the 210 MB estimate, and `HotFlushThreshold` is 56 MB.

**Fix:** a `TracesOptions.HotTierBytes` / `MergeBudgetBytes` pair defaulting to fractions of `MemoryBudgets.Current()` (the logs module's `EffectiveIndexCacheBytes`/`PayloadPoolBytes` pattern, `Options.cs:195-312`), and make the flush trigger and the compaction cap **byte budgets**: the tier tracks `AttributesByteLength` + a fixed per-span estimate as it writes, and `SelectCompactionBatch` plans against bytes (it already plans against `MaxSpansPerPass`, `:2590-2650`). Ring capacity likewise. A 512 MB container then gets a ~24 MB tier and a ~48 MB merge cap instead of 56 and 199.
**Risk:** a smaller tier means more, smaller segments, which raises segment count — mitigated because `CompactionThreshold` (60 000) moves with it. `CompactionThresholdTests` pins the selection arithmetic; `SelectCompactionBatch`'s tier logic must be rewritten against bytes without changing which files pair up on a same-size install.
**Measure:** stand a `TraceStorageEngine` under a 512 MB `MemoryBudgets` and assert the derived caps; run the existing storage suite against the smaller tier.

### 4. `WriteSpan` takes the engine's exclusive lock and the WAL's lock once per span — CONFIRMED (CPU + contention, per span)
`TraceStorageEngine.cs:565-583`, `SpanWriteAheadLog.cs:212-260`.

Per span: `_lock.EnterWriteLock()`, `lock (_writeLock)` inside `Append`, two `Encoding.UTF8.GetByteCount` + two `GetBytes` into the mapping, the dictionary inflate (finding 1), a `_traceIdx` probe, `_lock.ExitWriteLock()`. The drainer is single-threaded and calls this in a loop of 512 (`SpanDrainer.cs:62-70`) — **MEASURED 5.12 µs/span uncontended → a ~195 000 spans/s ceiling on one thread**, and that is with zero readers. Every reader path takes the *read* side of the same `ReaderWriterLockSlim`, so the SSE loops (which run pages back to back, `TraceQueryEndpointMapper.cs:440-453`) and ingest fight over one lock for the whole tier.

**Fix:** batch the lock. `WriteSpans(ReadOnlySpan<SpanIngestItem>)` taking the write lock once per drained batch of 512 amortises the lock to ~10 ns/span; `_wal.Append` gets a batch overload holding `_writeLock` once. Combined with finding 1 the per-span cost should fall to well under 1 µs. Note this is the *contract* change the ring needs anyway for finding 2.
**Risk:** the flush trigger (`_hotSpans.Count >= HotFlushThreshold`) moves to the end of the batch — harmless. WAL generation stamping must stay inside the same hold (`:571-575` comment). Pinned by `SpanWalTests`, `TraceFlushVisibilityTests`.
**Measure:** `HotTier_IngestCostAndRetainedBytes` wall µs/span, plus a variant with a concurrent `SearchSpansAsync` loop to show the contention delta.

### 5. `SpanReader` materialises an attribute dictionary for every span it decodes — CONFIRMED (alloc + peak memory, per span read)
`SpanReader.cs:1815`, `:1841-1845`.

Every read path pays it: compaction (`CompactOnePass` → `SpanReader.ReadAll`), TraceQL, trace-by-id, the span search.
**MEASURED:** `SpanReader.ReadAll` of one 50 000-span segment = **203.8 ms, 83.4 MB allocated, 83.0 MB retained (1 749 B/span)** — so a `MaxSpansPerPass` merge peaks at **199 MB of spans alone** (probe 6). A TraceQL page over one 50 000-span segment allocates **43.6 MB** to return 200 rows (probe 5), i.e. 913 B per span *scanned* for a query whose predicate reads exactly one attribute key.

**Fix:** same as finding 1 on the read side — decode the span's attribute region into a `ReadOnlyMemory<byte>` slice of the already-inflated block buffer and expose `Attributes` lazily. `TraceQL`'s `AttributePredicate.Evaluate` and `SpanBloom` then probe msgpack directly. This makes compaction's peak the *block* buffer, not 120 000 dictionaries, and turns a TraceQL scan into ~100 B/span.
**Risk:** the block buffer is `ArrayPool`-rented and returned per block (`SpanReader.cs:449-470`), so a lazy slice cannot outlive the block — either copy the blob into the record (still ~375 B vs 1 749 B) or keep the block alive for the records yielded from it. Prefer the copy; it is simple and still a 4.7× cut. `SpanSearchBoundTests` measures the per-span weight and will move — update the printed figure, keep the bound.
**Measure:** `Compaction_ReadAllRetainedBytes` and `TraceQL_SearchOverColdSegment` in the probe.

### 6. Aggregate endpoints walk the whole hot tier and allocate megabytes **inside the engine read lock** — CONFIRMED (alloc + contention, per request)
`TraceStorageEngine.cs:2825-2846` (`GetAggregateStatsAsync`), `:2894-2925` (`GetServiceGraphAsync`), `:3466-3471` (`BucketOf`).

`BucketOf` is `new uint[19]` **per span**, handed to `Merge` inside a per-span `new ServiceSegmentStats { … }` (`:2833-2841`). `GetServiceGraphAsync` additionally builds `new List<SpanRecord>(unflushedCount)` + `Dictionary<SpanId, string>(unflushedCount)` under the read lock (`:2904-2912`).

**MEASURED over a 49 000-span hot tier** (probe 7): `GetAggregateStatsAsync` **19.8 ms / 12.3 MB**, `GetServiceGraphAsync` **26.7 ms / 16.1 MB**, `GetTraceListAsync(100)` 6.8 ms / 2.8 MB, `GetTraceVolumeAsync` 10.2 ms / 0.4 MB. Every one of those milliseconds is a read-lock hold that `WriteSpan` must wait out.

**Fix:** (a) `BucketOf` → accumulate into the aggregate's existing `uint[]` in place: `agg[svc].Buckets[HistogramBuckets.IndexOf(dur)]++`, no array, no DTO per span — the `Merge` local already owns a bucket array. (b) the service graph's parent→service map should be built once and cached alongside the tier (it is derivable incrementally at `AddToHotTierLocked`), or at minimum snapshot span references under the lock and do the two passes outside it. (c) both endpoints are un-cached — an SSE dashboard polling `/api/traces/stats` re-walks the tier every tick. Add a short TTL memo keyed on `(from, to, tier generation)`.
**Risk:** `GetAggregateStatsAsync`'s `Merge` reuses `a.Buckets` across the tuple copy-back (`:2810-2822`) — the in-place version must not alias a caller's array from `SpanReader.ReadStats`. `TraceStatsEndpointTests` / `ServiceGraphSidecarTests` pin the values.
**Measure:** probe 7 above.

### 7. `SpanWriter` makes three extra full passes and several LOH arrays per flush — CONFIRMED (CPU + LOH, per flush)
`SpanWriter.cs:129-131` (`new List<SpanRecord>(spans)` — a 50 000-reference copy, 400 KB LOH, then `Sort`), `:157` `traceIndex` `Dictionary<TraceId, List<uint>>` with `new List<uint>(4)` per trace (`:376-381`), `:159` `svcBlockMap` values are `SortedSet<uint>` (a red-black node per block), `:371` `new HashSet<ulong>()` per block; then `ServiceGraphSidecar.Write` (`ServiceGraphSidecar.cs:44` `new Dictionary<SpanId, string>(spans.Count)` — 50 000 entries ≈ 1.9 MB, LOH) and `TraceSummarySidecar.Write` (two `MemoryStream`s sized from the trace count, `rowsMs.CopyTo(bodyMs)`, then `TraceSummarySidecar.cs:208` `rawBody = bodyMs.ToArray()` — a third copy — then `LZ4Pickler.Pickle` allocating a fourth).

**MEASURED** (probes 1-2, 50 000 spans, 8 attributes each): flush = **598.9 ms wall / 859.4 ms CPU / 11.98 µs per span / 20.9 MB allocated (439 B/span) / +3.8 MB LOH**, of which `.svcgraph` is 6.3 ms + 1.4 MB and `.tracesum` is 9.4 ms + 6.2 MB. So the sidecars are ~3 % of the time and 36 % of the allocation; the bulk of the 599 ms is LZ4-HC (`LZ4Level.L09_HC`) plus `SpanBloom`'s char-by-char FNV over 400 000 key/value pairs.

**Fix:** (a) sort an `int[]` of indices, or sort `spans` in place when the caller owns it (`CompleteFlush` hands over a detached snapshot — it does). (b) `svcBlockMap` → `Dictionary<string, List<uint>>` with a "last block seen" guard; blocks arrive in ascending order so the set is redundant. (c) `traceIndex` → a `(TraceId, uint)[]` sorted once at the end, which is what `TraceIndexFile` wants anyway. (d) `ServiceGraphSidecar` should reuse the `spanSvc` map the writer can build during its own block pass instead of a second full pass. (e) `.tracesum`: write rows into one pooled `ArrayBufferWriter` and pickle from `WrittenSpan` — the `ToArray()` and `CopyTo` both go. (f) `SpanBloom.AddAttr` on UTF-8 spans (finding 1) removes the `ToLowerInvariant` per char and the `ToString()` per numeric value. (g) consider `LZ4Level.L03` for the flush path and keep HC for compaction — the flush is on the critical path for tier memory release, compaction is not.
**Risk:** (g) changes bytes on disk but not the format (LZ4 frames are self-describing; `SpanReader` calls `LZ4Pickler.Unpickle`) — **flag: measurable file-size increase**, measure before adopting. (a)-(f) are byte-identical. Pinned by `SpanFormatV3Tests`, `SpanFormatV4Tests`, `PlainSegmentReadbackTests`, `ServiceIndexCollisionTests`.
**Measure:** `Flush_CostPerSpan` + `Flush_SidecarShare`.

### 8. `TraceStorageEngine.Dispose` is synchronous, returns at once to a second caller, and nothing gates compaction or retention against it — CONFIRMED (correctness at shutdown; the logs engine just fixed exactly this)
`TraceStorageEngine.cs:3473-3497`. `grep -n '_disposed' TraceStorageEngine.cs` → **three hits total**: the field (`:49`), `TryStartFlushLocked` (`:1739`), and `Dispose` itself (`:3477`).

Shape, compared with `StorageEngine`'s new `DisposeAsync` (write gate `:1410`, `_heavyPhases` `:172`/`:2070`/`:2153-2156`/`:4665-4697`, `_readersDrained` `:178`/`:1335`/`:4701`):

- **Second caller returns immediately.** `Interlocked.Exchange(ref _disposed, 1) != 0 → return` (`:3477`). The engine is registered under six singleton interfaces (`TracingServiceExtensions.cs:62-77`), so the container disposes it six times; callers 2-6 return while caller 1 is still inside `FlushHotTier()`, which waits out a background flush and then builds a segment synchronously — **599 ms per 50 000 spans measured**. Nothing that observes the "disposal" can conclude the files are on disk.
- **`IDisposable` only** — there is no `DisposeAsync`, so the host never awaits it; the flush runs on the container-disposal thread.
- **No heavy-phase count.** `CompactSmallSegments` (`:2561`), `CompactOnePass` (`:2658`), `PruneAsync` (`:3503`), `LoadColdSegments` (`:1965`), `BackfillNextSegment` (`:2377`), `CompactIndexOnce` (`:2281`) check nothing. `TraceCompactionWorker`/`TraceIndexBackfillWorker` start them with `Task.Run(..., ct)`; once started, `ct` does not stop them, and `BackgroundService.StopAsync` gives up at the host shutdown timeout. A compaction can therefore be **mid-`CompactOnePass` while `Dispose` runs**: it goes on to `_lock.EnterWriteLock()` on a lock `Dispose` tried to free (`:3495` catches `SynchronizationLockException` and logs "left to the finalizer"), calls `_index.Add`/`_index.Remove` on a disposed store (which re-opens `TraceIndexReader`s — native bloom memory with nothing left to retire them), rewrites the manifest, and **unlinks the source `.trc` files** after shutdown.
- **No reader wait.** `GetTraceAsync`/`SearchSpansAsync` in flight are not waited for. They are saved only by the index readers' own refcount (`TraceIndexFile.cs:731-790`) — which is real protection for the *bloom*, and the only native memory traces owns. There is no pooled or arena memory to free under a reader **today**; findings 1-2 introduce some, so the gate must land **before** them.

**Fix:** port the logs pattern: `IAsyncDisposable` with a write gate (refuse `WriteSpan` and start no new heavy phase), an `Interlocked` heavy-phase counter incremented by `CompleteFlush`, `CompactOnePass`, `PruneAsync`, `BackfillNextSegment`, `CompactIndexOnce`, a `TaskCompletionSource` completed by the decrement to zero, a bounded wait, then a reader drain, then `_index.Dispose()` / `_lock.Dispose()` / `_wal.Dispose()`. Make the second caller **await the first** (a shared `Task`) rather than return. Register the engine's disposal through a hosted service that runs before the container teardown, as the logs side does.
**Risk:** a wedged compaction must not hang shutdown — copy `_shutdownWaitBudget` and the "left frozen" log line verbatim. Pin with seam-based tests like `tests/Ameto.Storage.Tests` got at `de6b682` ("shutdown's heavy-phase and reader waits are judged by seams, not by a 500 ms timer").
**Measure:** seam tests, not a probe.

### 9. The span ring holds 65 536 live object graphs and applies back-pressure by object count — CONFIRMED (resident memory, steady state)
`SpanRingBuffer.cs:13-31`, `:52-73`; `SpanIngestionEndpoint.cs:26-31`.

`SpanIngestItem?[65536]`, each item a class with `Name`, `ServiceName` and the `AttributesBytes` array — **≈ 33 MB of gen2-promoted objects when full**, and the threshold that rejects is `FillFraction >= 0.9`, i.e. a count, with no idea how big the items are. Slots are nulled on dequeue (`:92`) so there is no leak, but a burst parks the whole graph in gen2 for as long as the drainer is behind. Compare the logs ring: a slab arena with `PayloadPoolBytes` from `MemoryBudgets`.
**Fix:** after finding 2, the ring carries `SpanHeader` + an arena offset, so a slot is 72 B and back-pressure is a byte budget. Until then, at minimum size `DefaultCapacity` from `MemoryBudgets` and add a bytes-in-flight counter to the back-pressure test.
**Risk:** `TryEnqueue`'s CAS-then-store leaves a published head with a null slot that `TryDequeueMany` handles by breaking (`:89`) — preserve that.
**Measure:** a fill-the-ring probe reading `GC.GetTotalMemory(true)`; none exists.

### 10. `SpanWriteAheadLog` grows by doubling and never shrinks; every commit msyncs the whole mapping under the append lock — CONFIRMED (resident memory + latency, per flush)
`SpanWriteAheadLog.cs:76` (`DefaultCapacity = 8 MB`), `:543-565` (`Grow` doubles), `:204-208` (`FlushLocked` = `_accessor.Flush()` over the **whole** view + `FlushFileBuffers`), `:311-345` (`CommitFlush` holds `_writeLock` across the relocation memmove and the barrier).

**MEASURED: 32 MB WAL file after 49 000 spans (685 B/span)** — the same shape as logs finding #12, unfixed here. The file is mapped for the process's life and never truncated after a flush drains it, so a single ingest burst permanently raises the resident set. `Append` needs `_writeLock`, and `WriteSpan` holds the engine's **write** lock while calling it — so a whole-mapping `FlushViewOfFile` stalls ingest *and* every query for its duration.
**Fix:** P/Invoke `FlushViewOfFile`/`msync` over `[lastFlushed, writeOffset)` only; capture the offset under the lock and issue `FlushFileBuffers` outside it. Shrink (`SetLength` + remap) when `CommitFlush` leaves the tail far below capacity and the log has been small for N cycles. Cap `DefaultCapacity` growth against `MemoryBudgets` and refuse-with-back-pressure instead of growing without bound.
**Risk:** the generation-0 terminator and the barrier ordering (`:334-345`) are crash-safety critical — the range must cover both the relocated tail and the header page. `SpanWalTests` pins replay; add a crash-point test for a shrink.
**Measure:** WAL file size in `HotTier_IngestCostAndRetainedBytes`; a `WalFlushTickProbe` analogue for traces.

### 11. Trace-detail and flamegraph responses allocate a `Dictionary<string,string>` and four strings per span, through reflection JSON — CONFIRMED (alloc, per request)
`TraceQueryEndpointMapper.cs:1301` `Attributes = s.Attributes?.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? string.Empty) ?? []`, `:1291-1293` three `TraceId`/`SpanId.ToString()` (`SpanRecord.cs:76` / `:127` are `$"{_hi:x16}{_lo:x16}"` interpolations), `:286-289` and `:259-260` both end in `ctx.Response.WriteAsJsonAsync(...)` with **no `JsonSerializerContext`** — reflection serialisation, although `TraceStreamJson` (`:1254`) exists for the SSE path. `TraceQLExecutor.cs:339` `GetAttr(attrs, params string[] keys)` allocates the `string[]` per row.

A 2 000-span trace therefore builds 2 000 dictionaries, 16 000 strings and 6 000 id strings, buffers the whole `List<SpanDto>`, and serialises it reflectively. Estimate ~3-4 MB and 20-30 ms per trace-detail request.
**Fix:** after finding 5, `SpanDto` carries the raw msgpack and a transcoder writes it straight into the response (`MsgPackJsonTranscoder` already exists on the logs side); ids format into a `stackalloc char[32]` with `string.Create`; add the `SpanDto`/`FlamegraphNode` types to a source-generated context and pass it to `WriteAsJsonAsync`; stream spans as they arrive instead of buffering. `GetAttr` takes a `ReadOnlySpan<string>` over a `static readonly string[]`.
**Risk:** attribute values currently stringify via `object?.ToString()` — a msgpack→JSON transcode emits numbers as numbers, which is a **client-visible shape change**. Either keep stringifying during the transcode or coordinate with the Angular client.
**Measure:** an alloc probe around `GET /api/traces/{id}` for a 2 000-span trace; none exists.

### 12. `SpanBloom` hashing walks UTF-16 char-by-char with `char.ToLowerInvariant` and `value.ToString()` — CONFIRMED (CPU, per attribute, flush + compaction only)
`SpanBloom.cs:29-33` (`value.ToString()` per attribute — allocates for every numeric/bool value), `:78-116` (`Fnv1a64` → `HashUtf8`, a per-char loop with a per-char `Encoding.UTF8.GetBytes` for anything ≥ 0x80).

Two hashes per attribute × 8 attributes × 50 000 spans = **800 000 FNV walks over ~20-60 chars each per flush**, plus ~400 000 `ToString()` allocations for numeric values. Part of the 599 ms measured in finding 7.
**Fix:** hash from the msgpack UTF-8 bytes (finding 1) with an ASCII fast path (`| 0x20` for A-Z) and `SearchValues`/vectorised scan for the non-ASCII escape; numeric values hash from their wire form.
**Risk:** **the bloom bits must stay identical** or every existing `.trc` bloom becomes a false-negative source against new queries — which is silent data loss on TraceQL attribute predicates. The hash is over `lowercase(value.ToString())` today; a UTF-8 path must produce the same bytes for the same value, including `long`/`double` formatting. Either prove equality with a fuzz test over the value space, or **bump the bloom's on-disk marker and treat old blooms as permissive** (`MayContain` already returns true for an empty bitset, `:53`). Flag: this is the one finding in the round that can change on-disk semantics.
**Measure:** `Flush_SidecarShare`-style split with the bloom build isolated.

### 13. Smaller, confirmed
- `TraceIndexStore.Lookup` allocates `new List<TraceIndexReader>(open.Count)` under `_gate` plus `new List<TraceIndexHit>(2)` on every trace lookup (`TraceIndexStore.cs:222-234`). A pooled/`[ThreadStatic]` list would make a bloom-miss lookup allocation-free.
- `CompactOnePass` does `processed.Contains(s)` inside a loop over `_coldSegments` (`TraceStorageEngine.cs:2763-2766`) — O(n·m) with reference equality; a `HashSet` or a path set is one line.
- `PruneAsync` uses three LINQ passes over `_coldSegments` inside the write lock (`:3512-3515`) plus `Select(...).Where(...).ToList()` twice more (`:3525`, `:3540`).
- `DescribeIndex` (`:2163`) and `CoveredSegmentIdsForTest` use LINQ over the manifest — test-only or diagnostics-only, fine.
- `TraceQLExecutor.BuildRow` allocates a `HashSet<string>` per returned row (`TraceQLExecutor.cs:307`) — bounded by `limit`, acceptable.
- `SpanWriter.Write` calls `Guid.NewGuid().ToString("N").Substring(0, 8)` per flush (`:136`) — two strings, once per flush, ignore.

---

## Baseline numbers measured at main `0a7389d`

Machine: this Windows box, `-c Release`, server GC off (xUnit host). Temporary probe `tests/Ameto.Storage.Tests/ZzReconTraceStoreProbe.cs` (**deleted — tree is clean**; re-create it in `tests/Ameto.Perf` with a `ProjectReference` to `Ameto.Tracing` **and** an `InternalsVisibleTo("Ameto.Perf")` in `src/Ameto.Tracing/Ameto.Tracing.csproj`, which is why it lived in `Ameto.Storage.Tests` here — that project already has internals access and the `ColdSpanSegmentFixture` span shape).

Command used:
```
TMP=TEMP=<scratch>  dotnet build tests/Ameto.Storage.Tests -c Release
TMP=TEMP=<scratch>  dotnet test tests/Ameto.Storage.Tests -c Release --no-build \
  --filter 'FullyQualifiedName~ZzRecon' --logger 'console;verbosity=detailed'
```

Span shape for every figure: `ColdSpanSegmentFixture.SqlClientAttributes` — a real 8-attribute OTel SqlClient client span; the msgpack attribute blob is **375 B**.

```
DESERIALIZE 8-attr msgpack blob (375 B) x 50 000
    3,48 us/span     1 496 B/span allocated

HOT TIER  49 000 spans written through TraceStorageEngine.WriteSpan
  wall             250,9 ms      5,12 us/span
  allocated         77,0 MB     1 647 B/span
  RETAINED          52,2 MB     1 117 B/span   (managed heap held by the hot tier)
  extrapolated to the 50 000-span flush threshold:     53 MB
  g2 collections during ingest: 3
  WAL file          32,0 MB                     (685 B/span)

FLUSH  50 000 spans (8 OTel attrs each)          [SpanWriter.Write]
  wall             598,9 ms     11,98 us/span
  cpu              859,4 ms
  allocated         20,9 MB       439 B/span
  LOH delta          3,8 MB
  GC          g0=1 g1=1 g2=1
  on disk     .trc    1,0 MB (   21 B/span) + sidecars  0,09 MB
              (identical attribute values across spans — LZ4-HC crushes them;
               a production corpus with varied SQL will be several times larger)

SIDECARS over 50 000 spans
  .svcgraph       6,3 ms      1,4 MB alloc  (   30 B/span)
  .tracesum       9,4 ms      6,2 MB alloc  (  130 B/span)

COMPACTION  SpanReader.ReadAll of one 50 000-span segment
  wall             203,8 ms  ( 4,08 us/span)
  allocated         83,4 MB  ( 1 749 B/span)
  RETAINED          83,0 MB  ( 1 740 B/span)
  a MaxSpansPerPass = 120 000 merge therefore peaks at    199 MB of spans alone

TRACEQL  { .db.system = "mssql" && duration > 1s } limit 200 over a 50 000-span segment
  wall             176,1 ms   (  3,52 us/span scanned)
  allocated         43,6 MB  (   913 B/span scanned)
  LOH delta          0,1 MB
  rows        200, capped=True
  GetTraceAsync (1 trace, 10 spans, 1 segment):   11,3 ms,   2,5 MB allocated

AGGREGATES over a 49 000-span hot tier (all allocation happens inside the engine READ lock)
  GetAggregateStatsAsync      19,8 ms      12,3 MB allocated  (  264 B per hot span)
  GetServiceGraphAsync        26,7 ms      16,1 MB allocated  (  344 B per hot span)
  GetTraceVolumeAsync         10,2 ms       0,4 MB allocated  (    8 B per hot span)
  GetTraceListAsync(100)       6,8 ms       2,8 MB allocated  (   60 B per hot span)
```

## Existing probes and tests for this area, and how to run them

**There is no trace-storage probe in `tests/Ameto.Perf` at all.** The three trace files there are ingest-decode only:
- `OtlpTraceAllocProbe`, `OtlpTraceProtoProbe`, `OtlpTraceStreamingParityTests` — `dotnet test tests/Ameto.Perf --filter 'FullyQualifiedName~OtlpTrace'`. `Ameto.Perf.csproj` does **not** reference `Ameto.Tracing`.

Functional coverage that pins this lens (`dotnet test tests/Ameto.Storage.Tests -c Release --filter '<name>'`):
- memory/bounds: `SpanSearchBoundTests` (the 1 749 B/span figure lives here and is printed, not asserted), `SpanSearchHotTierBoundTests`, `SpanSearchFatBlockTests`, `SpanSearchSkippedBlockBoundTests`, `SpanTraceReadBoundTests`, `TraceIndexCountBoundTests`
- format: `SpanFormatV3Tests`, `SpanFormatV4Tests`, `PlainSegmentReadbackTests`, `NewerFormatSegmentTests`, `SegmentHeaderRangeTests`, `ServiceIndexCollisionTests`
- lifecycle/background: `TraceCatalogLifecycleTests`, `TraceIndexBackfillTests`, `TraceIndexCompactionTests`, `TraceIndexCoverageClaimTests`, `TraceIndexLookupTests`, `CoverageSnapshotWindowTests`, `CompactionThresholdTests`, `ColdSegmentFaultTests`, `ColdSegmentStartupDeleteTests`, `TraceFlushVisibilityTests`, `SpanWalTests`
- endpoints: `tests/Ameto.Integration.Tests/TraceStreamEndpointTests`, `TraceStreamPagingFloorTests`, `TraceIndexEndpointTests`, `IngestionQueryIntegrationTests`

**Probes to add** (in `tests/Ameto.Perf`, after wiring the project reference + `InternalsVisibleTo`):
1. `TraceHotTierProbe` — µs/span and **retained B/span** through `WriteSpan` (findings 1, 2, 4); the gate for the whole round.
2. `TraceFlushProbe` — µs/span, B/span, LOH for `SpanWriter.Write`, split into blocks / bloom / trace index / each sidecar (findings 7, 12).
3. `TraceCompactionMemoryProbe` — retained bytes of `SpanReader.ReadAll` × `MaxSpansPerPass` (findings 3, 5).
4. `TraceQlScanProbe` — allocated bytes per span scanned for a one-attribute predicate (finding 5).
5. `TraceAggregateLockProbe` — allocated bytes and read-lock hold for the four aggregate endpoints, with a concurrent `WriteSpan` loop reporting the ingest throughput delta (findings 4, 6).
6. `SpanWriterAttrParityTests` — byte-for-byte `.trc` equality between the current `WriteAttributes(dictionary)` and the proposed `WriteRaw(blob)` over a corpus spanning every OTLP value type (gate for finding 1).
7. `SpanBloomParityTests` — bit-for-bit bloom equality between the char path and the UTF-8 path (gate for finding 12).

## Suggested order

1. **#8 shutdown gate** first — it is independent, it is the same work the logs engine just shipped, and every later finding introduces pooled or arena memory that a missing gate turns into a use-after-free.
2. **#1 keep the blob** (hot tier) + **#5 keep the blob** (reader) — one conceptual change, the largest single win (68 % of ingest CPU, 4.7× on every read path's memory), and `.trc` bytes are unchanged.
3. **#4 batch the lock** and **#6 aggregate allocations** — cheap, self-contained, and they unblock the ingest/query contention that findings 1-2 otherwise only halve.
4. **#3 memory budgets** — needs 1/5 landed first so the byte accounting is meaningful.
5. **#7 flush passes**, **#10 WAL msync/shrink**, **#9 ring**, **#11 response DTOs**.
6. **#2 native hot tier** and **#12 bloom hashing** last: the biggest blast radius, and #12 is the only one that can change on-disk semantics.

## Out of lens but confirmed while reading

- OTLP/protobuf **traces** still decode through `OtlpProtoDecoder` over `CodedInputStream` (issue #83's own note, ~6 400 ns/span). `OtlpTraceStreamParser` handles JSON with no DOM; protobuf has no equivalent. That belongs to the ingest lens.
- `TraceQueryEndpointMapper.cs:440-453` already documents that **every SSE page re-walks the whole hot tier** — a descending cursor shortens nothing. With finding 6's numbers that is 6.8 ms and 2.8 MB per page, per connected client, forever.
