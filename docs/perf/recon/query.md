# Log query/read path — CPU & allocation reconnaissance (perf/logs-cpu-alloc @ 677b49f)

Worktree: `C:/Users/ruslan.akhmetov/Desktop/Processing/AMeto-perf-logs`. Paths below are relative to `src/`.

## Query pipeline map

| Phase | Entry point |
|---|---|
| HTTP -> SSE | `Ameto.Server/EndpointMapper.cs:64` (`/api/events`), live tail `:453`, counts `:351`, aggregate `:163`, span/trace logs `:637`/`:683` -> `WriteGuardedListAsync :727` |
| Filter compile (twice per request: validate + execute) | `EndpointMapper.cs:849 TryCompileFilter`, `Ameto.Query/QueryExecutor.cs:53` -> `Filtering/CompiledFilter.cs:40` -> `FilterParser.Parse` |
| Window/cursor/@t folding | `QueryExecutor.cs:71-95` |
| Hot tier snapshot | `Ameto.Storage/StorageEngine.cs:612 OpenHotTierReader` -> `SnapshotTiers :626` -> `HotTierScan.ReadSorted` (`HotTierScan.cs:35`) -> `HotTierSegment.MaterialiseEvent :417` |
| Catalog | `StorageEngine.cs:582 GetSegments` (LINQ Where/OrderBy/ToList over `ConcurrentDictionary.Values`) |
| Prefilter (parallel <= 8) | `QueryExecutor.cs:327 PrefilterSegmentsAsync` -> `SegmentReader.Open` (#1) -> `SegmentIndexCache.TryAcquire` / `RentBloom/Inverted/TrigramBytes` -> `SegmentIndexReader.Load` -> `TryNarrowWithIndex :610` (`SegmentTrigramIndex.Lookup :108`, `SegmentInvertedIndex.LookupIntersect :132`) |
| k-way merge | `QueryExecutor.cs:181 MergeSourcesAsync` (PriorityQueue + lazy priming) |
| Segment scan | `QueryExecutor.cs:706 ScanSegmentAsync` -> `SegmentReader.Open` (#2) -> `ReadEventsAsync :262` -> `ReadBlock :467` (LZ4 into pooled buffers) -> `DecodeColumnarBlock :507` (List<LogEvent> per block) |
| Evaluate | `ScanSegmentAsync :734-737` (InWindow, cursor, levels, `filter.Matches`) -> `FilterEvaluator.Matches :20` |
| Output | `EndpointMapper.cs:127` `LogEventDto.From :1004` -> `SseJsonWriter.WriteEventAsync(dto, JsonSerializerOptions) :73` (reflection STJ + `EventPropsConverter` -> `MsgPackJsonTranscoder.WriteMap`) -> `WriteAsync` + `FlushAsync` per event |

Iterator layers an event crosses disk -> SSE: sync `ReadBlock` iterator over a `List<LogEvent>` -> async iterator `ReadEventsAsync` -> async iterator `ScanSegmentAsync` -> async iterator `MergeSourcesAsync` (heap dequeue/enqueue with a `Comparer.Create` delegate) -> async iterator `ExecuteAsync` -> endpoint `await foreach`. Five state machines; the ValueTask machinery is allocation-free when synchronous, but every candidate event pays ~4 MoveNextAsync dispatches before reaching the evaluator.

Verified still in place (not re-reported): index LRU cache by retained bytes, bloom + inverted `@l` pruning and `levels` hints, `TimeCompareNode` + from/to folding, cursor clamp, HotTierScan two-pass exact sizing, pooled section rents + `PooledSectionRents` counter, `TryReadProperty` probe, iterative LIKE without lowercase copy, `FilterRegex` cache (NonBacktracking), trigram-OR guard, hot tier as merge entrant, `SseJsonWriter` UTF-8 buffer reuse, `QueryGuard`, live push signal.

## Findings (ranked by expected impact)

### 1. Hot tier has no predicate pushdown: every hot event in the window is materialised, then filtered — CONFIRMED
- `QueryExecutor.cs:143-163 HotEventsAsync` -> `HotTierScan.ReadSorted` filters only window/cursor/`levels` on headers; `filter.Matches` runs after `HotTierSegment.MaterialiseEvent :417` (LogEvent + `payloadSpan.ToArray()` + 1-2 `ConcurrentDictionary` pool lookups).
- Frequency: per hot event in window (not per match). For query (a) `@l = 'Error'` is in the FILTER, not the `levels` parameter, so the header-level level check never fires; with 1 % errors x 10 % "timeout", finding 500 matches costs ~500k materialisations = 500k x (LogEvent ~120 B + props copy ~200-500 B) = 150-300 MB gen0 per query. Trace-logs (`@tr = 'hex'`) is worse: every hot event additionally pays `TraceIdHelper.FormatTraceId` (`FilterEvaluator.cs:263`, 32-char interpolated string) before an OrdinalIgnoreCase compare.
- Fix: (a) add `CompiledFilter.TryMatchHeader(in LogEventHeader)` -> definite-no / maybe, covering `LevelNode`/`@l =`/`@l in`, `service.name =` (resolve pool index -> string once per query), `@tr`/`@sp =` (parse the literal once, compare `TraceIdHi/Lo`/`SpanId` numerically), `@t` nodes; call it in `HotTierScan.Matches` before `Collect`. (b) Derive the `levels` set from the filter's AND-chain (reuse `CollectHeaderShape` logic) and pass it as `levels` — zero-risk since the evaluator re-checks. (c) Add `TraceIdCompareNode`/`SpanIdCompareNode` rewrites (like `TimeCompareNode`) so cold-path candidates also stop formatting hex per event.
- Risk: hot/cold parity — pinned by `Ameto.Query.Tests/TierMergeOrderTests`, `LevelPruneTests`, `HeaderOnlyShapeTests`, `Ameto.Storage.Tests/HotTierScanTests` (oracle tests). Keyset order unchanged (pruning happens before the sort).
- Measure: extend `HotTierScanAllocProbe` with a filter (`@l = 'Error'` at 1 % density); assert bytes proportional to matches.

### 2. Live-tail poll and every windowed query walk ALL hot headers twice — CONFIRMED
- `HotTierScan.cs:60-82 CollectAll`: `CountMatches` + `Collect` over every header of current + frozen tiers (64 B each, one cache line per header). A 64 MB tier is ~300k events; the frozen backlog can push this past 1M -> ~128 MB of memory traffic per poll. `LiveTailOptions.MinInterval` = 100 ms, and on a busy server the signal fires continuously, so one tab can spend 10-20 % of a core doing nothing but header walks; page queries pay the same fixed cost regardless of `count`. Then `List<Candidate>` (32 B struct) is fully sorted with a `Comparison<>` delegate (`:44`) even when only 50-500 are consumed -> LOH list + O(n log n) delegate compares for wide windows.
- Fix: per-chunk zone map in `HotTierSegment` (min/max `TimestampUtcTicks` per 16384-slot chunk, updated in `TryWriteClaimed`) so `CollectAll` skips whole chunks outside `[from,to]`; for the tail (`from = cursor`) that reduces the walk to the last chunk. Replace the delegate sort with a struct `IComparer<Candidate>` (or sort a parallel `(long ts, ulong id)` key array), and/or heapify + pop lazily so a page of k costs O(n + k log n).
- Risk: out-of-order arrivals — a chunk's min/max must be maintained exactly (never assumed monotonic); `HotTierScanTests.MatchesOracle_*` pin results.
- Measure: new probe: 1M-header tier, `ReadSorted(from = last 1 s)`; time and bytes before/after.

### 3. Candidate rows are fully materialised before the filter runs (exception, template, service, props) — CONFIRMED
- `SegmentReader.cs:507-624 DecodeColumnarBlock`: per candidate row -> `Encoding.UTF8.GetString` for template (`:589`) and service (`:584`, no dedup although `SegmentEventCursor.Dedup` exists on the merge path), `ExceptionInfo.FromBytes` (`:594` — level-split Error segments are ~100 % exceptions; stack traces 1-5 KB), `prPayload.ToArray()` (`:605`), `new LogEvent`. All eager, whole block into a `List<LogEvent>` (`:552`), then `ScanSegmentAsync` rejects. Trigram candidates for `'timeout'` are a superset (any indexed text field), so a sizeable share is rejected after paying 2-6 KB each. Also each of the ~21 primed iterators holds a fully decoded block it may never drain (500-row page -> up to 21 blocks x 50-300 rows decoded).
- Frequency: per candidate event. Estimate (a): ~3-10k candidates decoded to return 500 rows; ~10-40 MB per query, dominated by exception decode.
- Fix (staged): (1) make `LogEvent.Exception` lazy from raw bytes (`_exceptionBytes` + `??= ExceptionInfo.FromBytes`), mirroring `RawProperties` — the evaluator only touches it for `@x` predicates, the DTO decodes only returned rows; (2) dedup template/service via a per-reader `Dictionary<string,string>` with `Utf8StringComparer` (already in `SegmentEventSource.cs:234`); (3) longer term: decode the block into a GC-owned `byte[]` (64 KB, sub-LOH) and let `LogEvent` reference slices (props/exception/template) instead of copying, evaluating the filter on a lazily populated event — rejected rows then cost one small object.
- Risk: `SegmentCandidateReadTests`, `SegmentCandidateAllocProbe` (>= 10x ratio), `LogPageJsonProbe`; `LogEvent` `required init` shape used by ingestion/tests.
- Measure: `SegmentCandidateAllocProbe` variant with exception-bearing rows and a rejecting predicate.

### 4. Aggregation always runs the full ordered event scan, even for header-only shapes — CONFIRMED
- `Ameto.Query/AggregationExecutor.cs:85-127`: `select count(*) group by service.name` requests `Count = 2_000_001`, `Direction = Backward` through `ExecuteAsync` — full k-way merge, full LogEvent materialisation, then per event `BuildKey :163` allocates `string?[]`, a LINQ `Select` iterator and a `string.Join` composite (3-4 allocs/event). Query (d) over 2M events = 2M x (~1 KB decode + 4 heap objects + heap/async overhead ~1 us) = several seconds and 1-4 GB of garbage; the header-only path (`SegmentReader.AggregateHeaders :374`, `LogVolumeAggregator`) already decodes just @t/@l/service.name but is only used by `/counts` and alerts.
- Fix: when `CompiledFilter.TryGetHeaderOnlyShape` succeeds AND keys are a subset of {`@l`, `service.name`} AND all aggregates are `count(*)`, route through an `AggregateLogVolumeAsync`-style parallel header scan (hot `AggregateInto` + cold `AggregateHeaders`) and build rows from the aggregator. For other shapes: skip global ordering (accumulation is order-independent) and scan segments in parallel with per-worker accumulators; build the group key without LINQ (`string.Create`/stackalloc, or key on a tuple for <= 2 keys).
- Risk: the 2M scan budget currently means "newest 2M"; an unordered scan changes which events are counted when partial — `AggregationHonestyTests`, `AggregationEquivalenceTests`, `HeaderCountEquivalenceTests` pin behaviour; keep the ordered path when `Partial` would be true, or document the change.
- Measure: new probe timing `count(*) group by service.name` over `QuerySegmentFixtures.ManySegmentsAsync` vs `/counts`.

### 5. Trigram lookup and candidate intersection allocate HashSets and copy arrays ~5x per group — CONFIRMED
- `Ameto.Indexing/SegmentTrigramIndex.cs:116-146`: `text.ToString().ToLowerInvariant()` (2 strings), `new HashSet<int>(int[])` for the first trigram, `IntersectWith` per further trigram (BitHelper allocs), `ToArray` + `Array.Sort` + `Array.ConvertAll`. Then `QueryExecutor.cs:625-663`: `new HashSet<uint>(offsets)`, `acc.ToArray()`, `new HashSet<uint>(invOffsets)` (for a level-split Error segment the `@l=Error` posting list is the WHOLE group — 100k entries = ~2 MB of HashSet), `List merged`, `[.. merged]`; then `candidates.AddRange` -> `ToArray` (`:556`) -> `Clone` + `Sort` in `ReadEventsAsync :287`. Per group per query, in parallel across 8 workers -> gen0/gen1 churn and CPU spent hashing already-sorted data.
- Fix: posting lists are sorted ascending (`SegmentBitmapCodec` contract) — intersect with the two-pointer merge already in `SegmentInvertedIndex.IntersectSorted :298`, rarest trigram first (sort trigrams by posting length; early exit on empty); return sorted arrays so the reader's clone + sort becomes a cheap sortedness check; lowercase the search text once at compile time (hint list). Level-split segments: skip the `@l` intersection when the posting list length == `group.EventCount` (it is the identity).
- Risk: `IndexGroupPrefilterTests`, `TrigramOrHintTests`, `IndexCacheEquivalenceTests`; the reader's sort comment says do not remove it — keep it, make the input sorted so it is O(n).
- Measure: `IndexGroupPrefilterAllocProbe` with a `like '%...%'` filter; count via `GC.GetTotalAllocatedBytes`.

### 6. `ExceptionInfo.FromBytes` copies the payload and allocates key strings — CONFIRMED
- `Ameto.Core/ExceptionInfo.cs:157-162`: `bytes.ToArray()` for the reader (pure waste — the block buffer is a `byte[]`, a `ReadOnlyMemory` slice works); `ReadAtDepth :85` calls `reader.ReadString()` for every key ("type"/"msg"/"stk"/"inner") then string-switches -> 4 short strings + UTF-16 transcode per exception, per depth.
- Frequency: per candidate event with exception (all Error rows). ~1-5 KB copy + ~150 B keys each.
- Fix: `FromBytes(ReadOnlyMemory<byte>)` overload fed from `rentedUncomp.AsMemory(...)`; classify keys with `TryReadStringSpan` + `SequenceEqual("type"u8)` like `LogEventSerializer.ClassifyKey :134`.
- Risk: none behavioural; `MergeExceptionColumnProbe`, index parity tests.

### 7. Every surviving segment is memory-mapped twice per query, never cached across queries — CONFIRMED
- Prefilter `QueryExecutor.cs:383` and scan `:721` each call `SegmentReader.Open` (`FileInfo` stat + `CreateFromFile` + `CreateViewAccessor(0, fileSize)` + block-index rent/parse `:147-167` + group directory `:220`). For 20 segments: 40 opens/query; the live tail repeats per poll for any segment overlapping the cursor.
- Fix: hand the prefilter's `SegmentReader` (or at least its parsed `_blocks/_blockOrdinals/_groups`) to `ScanSegmentAsync` via `PrefilterResult`; or a small refcounted reader cache keyed by path (segments are immutable; evict on `_segments` removal). `MemoryMappedViewAccessor.ReadArray` per block (`:483`) also copies compressed bytes before LZ4 — `LZ4Codec.Decode(ReadOnlySpan, Span)` over `AcquirePointer` avoids one memcpy per block.
- Risk: Windows delete semantics (a cached mmap blocks retention delete — see the `MergingSegmentEventSource` ownership comment); `LazySegmentPrimingTests` count opens by allocation.
- Measure: `LazySegmentPrimingProbe`; add an open counter next to `PooledSectionRents`.

### 8. Cache miss deserialises the ENTIRE inverted section (every bucket, every posting) to answer one bucket — CONFIRMED
- `SegmentInvertedIndex.Deserialise :444-493`: string per value + `int[]` per posting list + dictionaries for all properties, including high-cardinality `@tr`/`@sp`/request-id buckets (one 32 B array each). A prop-dense group is tens of MB managed and hundreds of thousands of objects, all promoted into the 256 MB cache; the first query after start or any eviction churn pays it in parallel x8.
- Fix: lazy per-property decode — keep the packed section bytes (copied once, 3-8x smaller than expanded), build a `Dictionary<string, (int off, int len)>` property directory on load, decode a property's value map on first touch; charge the cache in packed bytes. Bigger step: a v8 section layout with a bucket directory.
- Risk: `IndexCacheEquivalenceTests`, `ApproxRetainedBytes` budget semantics.
- Measure: `IndexGroupPrefilterAllocProbe` with `IndexCacheBytes = 0`.

### 9. SSE output: reflection STJ, DTO copy, per-event `ToString`s and a flush per event — CONFIRMED
- `EndpointMapper.cs:15` `_json` is reflection-based (`DynamicObjectConverter`); `LogEventDto.From :1004` allocates the DTO, `Timestamp.ToString("O")`, `Id.ToString()`, `FormatTraceId/FormatSpanId` (2 interpolated strings), an `ExceptionInfoDto` tree copy; `SseJsonWriter.cs:73-82` then `WriteAsync` + `FlushAsync` per event (a socket send per row: 500 flushes per page). Counts/aggregate already use source-gen contexts; the event stream does not (`SseJsonWriter` doc `:63-71` says so). Program.cs sets no global JSON options for this path.
- Frequency: per returned event. ~6 allocations + 2 syscalls each; for 500 rows a few ms CPU plus GC.
- Fix: write the event directly with `Utf8JsonWriter` (no DTO): timestamp via `TryFormat("O")` into stackalloc (keep the exact "O" bytes), hex ids via `stackalloc char[32]` + `TryFormat`, exception via a small `JsonConverter<ExceptionInfo>`; or source-generate `LogEventDto` with a converter for `EventProps`. Flush every N events or when `_buffer.WrittenCount > 16 KB` (and always at end/keepalive/last event of a tail poll).
- Risk: byte-exact output — `MsgPackJsonTranscoderParityTests` (props unaffected), `FullIntegrationTests`, `LiveTailPushTests` (frame boundaries; a coalesced flush must still deliver the last frame promptly for the tail).
- Measure: extend `LogPageJsonProbe` to serialise the whole DTO.

### 10. LIKE `%literal%` runs a per-char folded matcher instead of a vectorised search — CONFIRMED
- `FilterEvaluator.cs:1173-1206 LikeMatchLower` + `FoldedCharsMatch :1142`: scalar loop with `char.ToLowerInvariant` per char and backtracking; `'%timeout%'` on a 100-char template is ~100-300 ns vs ~10 ns for `IndexOf(OrdinalIgnoreCase)`. Per candidate event (plus per hot event under finding 1). Also `Matches :20` is a 50-arm type switch: `LikeNode` is arm 10 -> ~10 `isinst` checks per node per event.
- Fix: in the `LikeNode` ctor detect the `%lit%` / `lit%` / `%lit` shapes; at eval, if `Ascii.IsValid(text)` (vectorised) use `text.AsSpan().Contains/StartsWith/EndsWith(lit, OrdinalIgnoreCase)`; otherwise fall back to the folded matcher (ASCII folding is identical under both, so semantics are preserved exactly — the Kelvin/long-s cases are non-ASCII). Replace the type switch with a virtual `Matches` on `FilterNode` (or a precomputed `NodeKind` enum switch).
- Risk: `LikeAndRegexTests.Like_folds_case_the_same_way_for_literal_and_wildcard_patterns`, `Like_matches_supplementary_plane_letters_case_insensitively`, `FilterEvaluatorTests`.
- Measure: micro-benchmark in `tests/Ameto.Perf/Benchmarks.cs`.

### 11. `TryReadProperty` walks the whole map and boxes the value — CONFIRMED (medium)
- `LogEventSerializer.cs:394-452 Probe`: last-occurrence semantics force a full walk (cannot early-exit), and `ReadDynamic :300` boxes numbers / allocates a string for string values per event per predicate. Per candidate event with a user-property predicate.
- Fix: keep last-wins (the walk itself is cheap); avoid boxing by adding typed reads (`TryReadPropertyUtf8`/`AsDouble`) used by `CompareNode`/`LikeNode`/`ContainsNode` on scalars; compare strings as UTF-8 spans against a pre-encoded literal. `PropertyProbeTests` pin key semantics.

### 12. Per-poll fixed costs in the live tail — CONFIRMED (small each, up to 10x/s per tab)
- Filter re-parsed and recompiled per poll (`QueryExecutor.cs:53`); `QueryDeadline` = linked CTS + timer per poll (`QueryGuard.cs:72`); `GetSegments` LINQ + `ToList` + `covered` HashSet per poll (`StorageEngine.cs:582`, `:626`); `OrderBy...ToList` in `MergeSourcesAsync :207-209`. Fix: cache the `CompiledFilter` per tail connection (add a `CompiledFilter` field to `QueryRequest`), reuse a `CancellationTokenSource` with `CancelAfter` reset, snapshot the catalog into a sorted array on mutation instead of per query.

### 13. `Compare` string path uses `string.Compare` for `Eq` — SUSPECTED (small)
- `FilterEvaluator.cs:455-456`: `right?.ToString()` then `string.Compare(..., OrdinalIgnoreCase)` for every op, including Eq/Ne where `string.Equals(OrdinalIgnoreCase)` (length check first, vectorised) is cheaper. Per candidate event for `@l = 'Error'` (parsed as a `CompareNode`, not a `LevelNode`, because it carries an operator — `FilterParser.cs:860-866` vs `:897`).

### 14. `HotTierSegment.MaterialiseEvent` pool lookups — SUSPECTED (small)
- `HotTierSegment.cs:438`, `:452`: `pool.Get` = `ConcurrentDictionary<int,string>` lookup per event for service (and template on a miss), ~20-30 ns each; a `string?[]` indexed by pool id would do. Only matters once finding 1 stops the over-materialisation.

## Notes for the implementer
- Stale comment: `QueryExecutor.cs:455` says `ArrayPool<byte>.Shared` does not pool > 1 MB; since .NET 6 it pools up to 1 GiB. Not harmful, but do not design around it.
- `/api/events/counts` (c) is already header-only, parallel and cached 20 s (`LogVolumeCounts.cs`); nothing significant beyond finding 7 (one `SegmentReader.Open` per segment per cache miss).
- Suggested order for query (a): 1 -> 3 -> 5 -> 9 -> 10 -> 7; live tail: 2 -> 12 -> 1; aggregation (d): 4.
