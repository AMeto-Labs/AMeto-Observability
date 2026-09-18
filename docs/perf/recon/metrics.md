# Metrics — CPU / allocation / memory reconnaissance (worktree AMeto-recon-metrics @ 0a7389d, main)

Paths traced end to end: OTLP/protobuf metrics over HTTP (`OtlpEndpointMapper.cs:101-133` →
`OtlpMetricProtoParser.Parse` → `MetricStorageEngine.Ingest`) and over gRPC
(`OtlpGrpcEndpointMapper.cs:66-70`); OTLP/JSON (`JsonSerializer.Deserialize<ExportMetricsServiceRequest>` →
`OtlpMetricMapper.Map`); the hot tier (`_hot` + `HotSeries` + `MetricMeta` + `ExemplarRing`);
the WAL (`MetricWriteAheadLog.Append` / `BeginFlush` / `CommitFlush` / `ReadAll`); flush
(`FlushHotTierAsync` → `MetricWriter.Write`); read (`QueryAsync` hot + cold → `MetricReader.ReadAllSync`
→ `MetricAggregator` → `MetricQueryEndpointMapper`); and the two background loops
(`FlushLoopAsync`, `RollupLoopAsync` → `PerformRollupAsync` → `RewriteMetricInChunks`).

**Prior work verified still in place (do not re-do):** `OtlpMetricProtoParser` really is DOM-free —
`ProtoReader` spans, two-pass ResourceMetrics, no `CodedInputStream`, no number→string→number round
trip (2.8x faster / 5.6x less allocated than the DOM route, measured below). `MetricMeta.AddSeries`
is the lock-free `ConcurrentDictionary<int,byte>` + `_trackedCount` (the `Monitor.Enter_Slowpath`
fix). `MetricReader.ReadAllSync` streams one series at a time with pooled LZ4 buffers and
`FileBounds` guards on every count/length. v3 format: one LZ4-HC block per file, ms deltas,
slim 2-field points, 512-series file cap. `RewriteMetricInChunks` bounds rollup peak by series
chunk; `TakeMetricSlice` rotates 4 metrics/pass; `AggressiveGcGate` gates the maintenance collect on
`passBytes >= 8 MB`. WAL PR #59 work: `ReconcileDataEndLocked` at open, `ShrinkLocked`,
`_survivorSeriesSeed` + `SeriesIndexSanityCap`, `GenerationSanityMargin`, 24 h future guard at both
hot-tier entrances. Flush protocol: `_flushGate` serialises periodic vs threshold, `MetricWalCommit.Refused`
+ `UnwriteRefusedFlush`, `_inFlightFlushes` set, `OnFileWrittenForTest`/`duringFileWrite` seams.

**Not in place anywhere in `src/Ameto.Metrics`:** `IngestBufferPool`, `PoolTrimPolicy`,
`MemoryBudgets`, `SlabArena`, `StringInternPool`, `MemoryShedRegistry`, `ManualTimeProvider`,
a source-generated `JsonSerializerContext`, any streaming response writer. Every sizing constant is
a literal (`HotFlushThreshold = 500_000`, `MinFlushPoints = 50_000`, `DefaultCapacity = 8 MB`,
`ExemplarsPerMetric = 4_000`, `MaxTrackedSeries = 50_000`, `MaxLabelValuesPerKey = 2_000`,
`MaxSeriesPerFile = 512`, `SeriesChunk = 512`) — identical on a 512 MB container and a 64 GB host.

---

## BASELINE NUMBERS (measured on this box, Release, .NET 10.0.12, workstation GC)

Command for the two committed probes:
```
TMP=…/tt-recon-metrics TEMP=$TMP dotnet test tests/Ameto.Perf/Ameto.Perf.csproj -c Release \
  --filter "FullyQualifiedName~Metric" --logger "console;verbosity=detailed"
```
```
OtlpMetricProtoProbe.SpanParserBeatsDomPath
  payload        : 138.3 KB protobuf, 500 data points (20 instruments x 25 series, every 3rd a 15-bucket histogram)
  DOM decode+map : 3.256 ms/batch | 6511 ns/point | 6701 B/point | 154 k points/s/core
  span parser    : 1.174 ms/batch | 2348 ns/point | 1199 B/point | 426 k points/s/core
MetricIngestContentionProbe.ConcurrentIngestThroughput
  96 000 points in 61 ms | 631 ns/point | 1585 k points/s across 4 threads
```

The rest I measured with a TEMPORARY probe (`tests/Ameto.Perf/ZzReconMetricsProbe.cs`, written,
run, and deleted — the tree is clean). Shape: 2 000 series x 5 labels
(`service.name`, `http.route`, `http.request.method`, `http.response.status_code`, `server.address`),
one metric name, 15 s spacing. Reproduce by re-adding the probe (sketch in "Probes to add" below).

```
--- GAUGE: 2 000 series x 60 points = 120 000 points ---
  ingest (WAL+hot) :     1080 ns/point |       95 B/point alloc     (single thread)
  hot tier live    :     21.2 MB heap  =    185 B/point resident
  wal file         :      8.0 MB       =     70 B/point on disk + pool 372 KB (190 B/series)
  flush            :      212 ms | 35.3 MB alloc = 309 B/point
  on disk          :       20 KB in 4 .mts file(s)
--- HISTOGRAM (16 buckets): 2 000 series x 60 points = 120 000 points ---
  ingest (WAL+hot) :     1084 ns/point |      104 B/point alloc
  hot tier live    :     39.6 MB heap  =    346 B/point resident
  wal file         :     32.0 MB       =    280 B/point + pool 606 KB (310 B/series)
  flush            :      159 ms | 51.9 MB alloc = 453 B/point
  on disk          :       48 KB in 4 .mts file(s)

Ingest lock scaling (1 000 known series/thread, 8 rounds, gauge):
   1 thread(s):  716 k points/s |  1397 ns/point wall |  1397 ns/point per thread
   2 thread(s): 1051 k points/s |   952 ns/point wall |  1904 ns/point per thread
   4 thread(s):  918 k points/s |  1090 ns/point wall |  4360 ns/point per thread
   8 thread(s):  971 k points/s |  1029 ns/point wall |  8236 ns/point per thread
  → the path saturates at ~1.0 M points/s TOTAL regardless of core count. 8 cores buy 1.36x over 1.

Hot-tier retention after a drain (2 000 series, 300 points each fed in 10 k-point chunks,
one threshold flush, then 1 point/series):
  heap empty                 :   2.1 MB
  heap with burst in tier    :  45.0 MB  (+42.9)
  heap AFTER the flush drain :  25.3 MB  (+23.2 still held = 12 166 B/series by an EMPTY tier)
  heap steady state          :  26.0 MB
  → 54 % of the burst's heap survives the drain, forever, per series ever seen.

Cold query, 2 000 histogram series x 60 points (48 KB on disk, 4 .mts files):
  QueryAsync raw (no step)                 111 ms | 30.0 MB alloc | 15 753 B/stored-series
  Aggregator Rate group-by service.name    128 ms | 35.7 MB alloc | 18 727 B/stored-series
  Aggregator Quantile p95 (histogram)      187 ms | 31.0 MB alloc | 16 251 B/stored-series
  Aggregator Last + filter method=GET       41 ms | 28.5 MB alloc | 14 950 B/stored-series
  → 30 MB allocated and 111 ms of CPU to answer a query whose data is 48 KB on disk: 625x.

Parser allocation breakdown (same payload as OtlpMetricProtoProbe):
  parse: 2814 ns/point | 1199 B/point | 599 472 B/batch
  label strings materialised per batch: 8 000, DISTINCT: 26  (99.7 % are duplicates)
  MetricIngestItem + LabelSet + (string,string)[] objects per batch: 1 500 (3 per point)
```

---

## Findings (ranked by expected impact)

### 1. Every label key and value is a fresh string per data point — CONFIRMED (alloc + CPU + gen2)
`src/Ameto.Otel/OtlpMetricProtoParser.cs:391` (`key = r.ReadString()`), `:408`
(`AnyValue.Str(r.ReadString())`), `:325`, `:98`; `MetricTypes.cs:30-71` (`LabelSet` ctor);
`OtlpMetricMapper.cs:56-80` (the JSON twin).
A 500-point batch materialises **8 000 label strings of which 26 are distinct** — 99.7 % are a
string already on the heap. Per point the parser builds: 16 `string`s (~56 B each), one
`(string,string)[]` (24 + 16 B/pair), one `LabelSet` (24 B + cached hash), one
`MetricIngestItem` (96 B), plus `Array.Sort` with a **non-static comparison lambda**
(`MetricTypes.cs:58` — the delegate is cached by Roslyn, but the sort is a full
`ArraySortHelper` call with an indirect compare per element) and a `HashCode` walk. **MEASURED:
1 199 B/point, 2 814 ns/point**; ~900 B of that is label strings. At 100 k points/s that is
**120 MB/s of gen0 churn from strings that already exist**, and the surviving ones are promoted to
gen2 because `LabelSet` is retained by `SeriesKey` in `_hot`, `_wal._seriesIndex`, `_meta` and
`ExemplarRing` for the life of the process.
**Fix:** an array-backed intern pool with `Intern(ReadOnlySpan<byte> utf8, out string canonical)`
— `Ameto.Storage.StringInternPool:123` is exactly this API and is alloc-free on a hit
(alternate-key lookup). It lives in `Ameto.Storage`, which `Ameto.Metrics`/`Ameto.Otel` do not
reference; move it to `Ameto.Core` (it has no storage dependency) or add a metrics-local twin.
Then: `ReadString()` → `Intern(r.ReadLengthDelimited(), out string canonical)` at
`OtlpMetricProtoParser.cs:391/408`, and the same in `MetricReader.ReadLabels:301-302` and
`MetricWriteAheadLog.ReadString` so replay and cold reads join the same pool. Second half:
`LabelSet` should hold a `string[]` of interleaved k,v (one array, not a tuple array) and compare
by `ReferenceEquals` first once the strings are canonical.
**Risk:** the pool is capped and must degrade to a plain `new string` past the cap (`StringInternPool`
already does, via `PoolExhausted`); `LabelSet` equality/hash must stay value-based for
uninterned strings, because `MetricReader` and the WAL build label sets from disk. Pinned by
`tests/Ameto.Perf/OtlpMetricProtoParityTests.cs`, `tests/Ameto.Storage.Tests/MetricWalTests.cs`,
`MetricFormatV3Tests.cs`.
**Measure:** `OtlpMetricProtoProbe` (B/point guard is already there: `spanBytes * 3 < domBytes` —
tighten to `* 8` after the fix), plus the label-duplication line from the temp probe.

### 2. The hot tier never releases a series' point array, and never evicts a series — CONFIRMED (resident memory, gen2)
`MetricStorageEngine.cs:1817-1825` (`HotSeries.Drain` → `new List<>(_points); _points.Clear()`),
`:567` (`_hot.GetOrAdd`), `:815-816` ("`_hot` keeps its keys after a drain"). There is **no
`_hot.TryRemove` anywhere in the file**, and `List<T>.Clear()` does not shrink the backing array.
So each series permanently owns a `MetricDataPoint[]` sized to the **largest burst it ever saw**
(`MetricDataPoint` = 40 B: ts, value, count, sum, `long[]?`), plus a `HotSeries` (40 B), a `Lock`
(~48 B), a `List` (32 B), a `SeriesKey`+`LabelSet`+pair array (~180 B) and a
`ConcurrentDictionary` node (~48 B).
**MEASURED: 12 166 B per series retained by a completely empty tier**, 54 % of the burst peak.
On the sandbox's own numbers (38 741 series, 4 098 for one metric — quoted at
`MetricWriter.cs:52-57`) that is **~470 MB of gen2 that nothing can reclaim**, in a 512 MB
container whose GC heap limit is 384 MB. This is the traces-OOM shape, on the metrics side.
**Fix:** three parts, all format-neutral. (a) In `Drain`, hand the *existing* list out and give the
series a fresh `List<MetricDataPoint>(Math.Min(prevCount, 256))` — the copy at `:1821` disappears
too, so the flush stops allocating a second copy of every point. (b) A stale-series sweep in
`FlushHotTierAsync` after the drain: `_hot.TryRemove` any series whose `LastAppendTicks` is older
than, say, `2 × MaxHotAge` (add one `long` field to `HotSeries`, written with `Volatile.Write`
under its own lock) — the series is re-created for free if it comes back, and its cold data is
untouched. (c) Register the engine with `MemoryShedRegistry` (`src/Ameto.Core/MemoryShedRegistry.cs:55`,
today only `SegmentIndexCache` does) so `RamPressureService` can force an early flush + sweep
instead of watching the heap climb.
**Risk:** (a) changes `Drain`'s aliasing — the restore path at `:949-957` re-appends from the
snapshot and must not see the same list; (b) must not evict a series with points in it or one an
open flush snapshot still names — do it inside the `_snapshotLock` write lock, after
`Interlocked.Exchange(ref _hotPointCount, 0)`. Catalog `_meta` must survive the eviction
(`GetCatalog` is fed from `_meta`, not `_hot`) — but `GetMetricNames:658` reads `_hot.Keys`, so it
needs `_meta` as its hot source instead (see #7). Pinned by `MetricChunkedRewriteTests`,
`MetricWalTests`, and a new retained-memory fact.
**Measure:** the `HotTierRetentionAfterDrain` probe above (re-add it as a permanent
`MetricHotTierRetentionProbe`, assert `afterDrain - empty < (loaded - empty) / 4`).

### 3. `Ingest` serialises every point on one WAL lock — CONFIRMED (CPU, throughput ceiling)
`MetricStorageEngine.cs:425` (`_wal.Append(item, in point)` inside the per-item loop),
`MetricWriteAheadLog.cs:636` (`lock (_writeLock)` **per point**), `:671`
(`_seriesIndex.TryGetValue` — a `ConcurrentDictionary` read done *inside* the exclusive lock),
plus `HotSeries.Append:1810` (`lock (_lock)` per point) and
`_snapshotLock.EnterReadLock():397` (per batch — that one is right).
The issue's premise that "point ingest takes no locks" is false: it takes **two monitor
acquisitions per point**, one of them global. **MEASURED: 1 M points/s total, flat from 2 to 8
threads; per-thread cost rises 1 397 → 8 236 ns/point as threads are added** — textbook
serialisation. `SeriesKey.GetHashCode()` inside the lock walks nothing (the `LabelSet` hash is
cached at `MetricTypes.cs:70`) but `Equals` on a collision walks 5 string comparisons.
**Fix, in order of payoff:** (a) move `RegisterSeriesLocked`'s lookup out of the lock — the
dictionary is already concurrent, so `TryGetValue` first, and only take the lock on a miss to
assign an index and write the pool record. (b) Reserve the mapped range with one
`Interlocked.Add(ref _writeOffset, entrySize)` and store the entry header outside the lock (the
entries are fixed-stride and independent; the existing `Generation == 0` "unwritten" convention at
`:198` already makes a reserved-but-unwritten slot recoverable, exactly like the logs drainer's
pending slot). The lock then only covers `Grow()` and the header store — a reader/writer flag or a
short spin over `_capacity` is enough. (c) Batch the ingest: `Append(ReadOnlySpan<MetricIngestItem>)`
so one lock acquisition covers a whole OTLP batch (~500-1 000 points) — cheapest change, gets most
of the win, and matches how `Ingest` is actually called.
**Risk:** `BeginFlush`/`CommitFlush`/`Compact`/`ReadAll` all assume `_writeOffset` is stable under
`_writeLock`; a reserving append must make `BeginFlush` wait for in-flight reservations (the
"heavy-phase count + reader wait" pattern from `StorageEngine`). The `Generation == 0` sanity walk
in `ReadAll:1012` is what makes a torn reservation safe on replay — verify with
`MetricWalTests` (24 facts across the three storage test files, 72 in total).
**Measure:** `MetricIngestContentionProbe` (extend it to sweep 1/2/4/8 threads and print
points/s per thread count — today it only reports 4).

### 4. A cold query allocates 625x the bytes it reads — CONFIRMED (CPU + alloc + LOH, per request)
`MetricStorageEngine.cs:720` (`MetricReader.ReadAsync` per candidate file),
`MetricReader.cs:71-73` (`series.Points.Where(...).ToList()` — a full copy of every series even
when the whole series is in range), `:218` (`r.ReadString()` for the **map key**, 6 per series),
`:301-302` (fresh label strings, see #1), `:415` (`labels.Pairs.ToDictionary(...)` — a new
`Dictionary` **per series** to test the matchers), `MetricStorageEngine.cs:1440` (the same
`ToDictionary` again for the hot tier), `:1471-1505` (`Downsample` = `GroupBy` + `Select` +
inner `OrderBy` + `Last` + `Average`/`Sum` + `OrderBy` + `ToList` — five iterators and a
`List` per series), `MetricAggregator.cs:377-410` (`MergeFragments`: a
`Dictionary<LabelSet, List<MetricSeries>>` plus a `SortedDictionary<long, MetricDataPoint>` per
multi-fragment series — a red-black tree node, 48+ B, per point), `:318` and `:235`
(`SortedDictionary` again in `ReduceByTimestamp` and `AggregateQuantile`), `:1409`
(`pts.OrderBy(...).ToList()` inside the rollup transform).
**MEASURED: 111-187 ms and 28-36 MB per query over 2 000 series / 120 k points that occupy 48 KB
on disk.** On a 512 MB stand one such query is 9 % of the heap limit; the alert evaluator
(`src/Ameto.Alerts/AlertEvaluator.cs:27` `EvalInterval = 15 s`, `:551` one
`IMetricAggregator.QueryAsync` **per enabled rule**) runs this four times a minute per rule,
unattended, forever. That is the metrics contribution to "idle CPU sawing".
**Fix:** (a) `MetricReader.ReadAsync`: skip the copy when `filtered.Count == series.Points.Count`
and the range covers the series; better, pass `fromNano`/`toNano` into `ReadPointsV3` so
out-of-range points are never materialised. (b) `DeserializeSeries:218`: `TryReadStringSpan` +
a 4-way UTF-8 byte compare on `"k"/"u"/"lbs"/"bnds"/"pts"/"cnt"` instead of `ReadString()`.
(c) Both `MatchesLabels` sites: the pairs are **sorted by key** (`MetricTypes.cs:58`) and matcher
counts are tiny — a linear/binary scan over `Pairs` with no dictionary at all. (d) `Downsample`:
one forward pass over the already-sorted points writing into a `List<MetricDataPoint>` sized
`(span/bucket)+1`; no LINQ. (e) Replace every `SortedDictionary<long, …>` in the aggregator with
a `List` + `Sort` (points arrive nearly sorted; `MergeFragments` fragments are each internally
sorted, so a k-way merge is linear). (f) Give the aggregator a rented `double[]` pool for the
per-step bucket arrays at `MetricAggregator.cs:123` and `:247` — one `double[nBuckets]` per
timestamp per group today.
**Risk:** `Downsample`'s counter/histogram "take last in bucket" and gauge "average" semantics
(`:1465-1468`) must not change — it is also used by the **rollup**, so a behaviour change rewrites
files. Ties on equal timestamps: `MergeFragments:400` is "later fragment wins", `DedupeByTimestamp:1384`
is "last wins" — both must survive the de-LINQ. Pinned by `MetricChunkedRewriteTests`,
`MetricFormatV3Tests`; add a golden-series parity test over `Downsample` before touching it.
**Measure:** a new `MetricQueryAllocProbe` (the `ColdQueryCostPerSeries` fact above).

### 5. `MetricWriter` copies and re-copies the whole file payload — CONFIRMED (LOH, per flush)
`MetricWriter.cs:231` (`new ArrayBufferWriter<byte>()` — starts at 256 B and **doubles**, so a
4 MB section allocates ~14 arrays totalling ~8 MB, most of them LOH), `:240`
(`bufWriter.WrittenSpan.ToArray()` — one more full copy, always LOH above 85 KB), `:243`
(`LZ4Pickler.Pickle` allocates the compressed array), `:125` and `:235` (`hs.GetPoints(MinValue,
MaxValue)` is called **twice per series** — once for min/max in `Write`, once in `WriteFile` — and
each call is `Where(...).OrderBy(...).ToList()` under the series lock, i.e. two LINQ iterator
chains and two full point copies per series per flush), `:112` (`series.GroupBy` + `:120`
`group.ToList()`), `:399` (`SanitizeName` = `name.Select(char)` + `string.Concat(IEnumerable<char>)`).
Plus `MetricStorageEngine.cs:1821` — `Drain` already made a third copy.
**MEASURED: 35-52 MB allocated per flush of 120 k points (309-453 B/point) to produce 20-48 KB of
file.** The on-disk number is small only because the probe's series are highly compressible; the
allocation is not, it scales with points.
**Fix:** hand `WriteFile` a pooled `byte[]` from an `ArrayPool<byte>` sized off
`MemoryBudgets.Current()` (a `MetricFlushBufferPool` mirroring `IngestBufferPool` +
`PoolTrimPolicy`), wrapped in a small `IBufferWriter<byte>` so `MessagePackWriter` writes straight
into it; `LZ4Codec.Encode(ReadOnlySpan, Span)` into a second pooled buffer instead of
`LZ4Pickler.Pickle` (the 4-byte pickle header is trivial to write by hand — **flag: this touches
the section bytes, so keep `LZ4Pickler` unless a format note is accepted**); compute min/max in the
same pass that writes the points instead of the extra `GetPoints`; `SanitizeName` via
`string.Create` + a `SearchValues<char>`.
**Risk:** the `.mts` section must stay byte-identical (`MetricFormatV3Tests` reads v3 back);
`Write` is all-or-nothing on disk (`:69-94`) — a pooled buffer must be returned on every throw
path or the pool bleeds. Keep `MaxSeriesPerFile = 512` so the buffer stays bounded.
**Measure:** a new `MetricFlushAllocProbe` (alloc/point and ms/point for a 120 k-point flush,
the `IngestAndFlushCostPerPoint` fact above).

### 6. Nothing in the metrics module is sized from `MemoryBudgets` — CONFIRMED (resident memory, 512 MB stand)
`MetricStorageEngine.cs:33` (`HotFlushThreshold = 500_000`), `:38`, `:46` (`MinFlushPoints = 50_000`),
`:208` (`ExemplarsPerMetric = 4_000`), `:1757` (`MaxTrackedSeries = 50_000`), `:39`
(`MaxLabelValuesPerKey = 2_000`), `MetricWriteAheadLog.cs:174` (`DefaultCapacity = 8 MB`, grown by
**doubling** at `:1156`), `MetricsServiceExtensions.cs:18-21` (the engine is constructed with a
path and a logger — no options object at all).
Worst case at the defaults, before a flush: 500 000 points x 40 B = **20 MB of point structs**
(histograms with 16 buckets: **+64 MB of `long[]`**, since each point holds its own bucket array —
`MetricTypes.cs:166`), the WAL mapping grown to **32 MB** for the same load (MEASURED: 280 B/point
for histograms), plus #2'spermanently retained per-series arrays, plus `_exemplars` at 4 000 x ~120 B =
480 KB **per metric name** with no cap on the number of names. On the 512 MB console.ntpayments
stand (GC heap limit 384 MB, `IndexCacheBytes` 48 MB, `HotTier.MaxSizeBytes` 16 MB) the metrics
hot tier alone can legally claim more than the logs tier is allowed.
**Fix:** a `MetricsOptions` bound from config with `MemoryBudgets`-derived defaults — a new
`MemoryBudgets.MetricHotTierFraction` (~0.05 of the managed limit) turning into
`HotFlushThreshold = budget / estimatedBytesPerPoint`, `MinFlushPoints = HotFlushThreshold / 10`,
`ExemplarsPerMetric`, and `MetricWriteAheadLog.Open(initialCapacity: …)` sized to the same budget
so the doubling never overshoots. Cap the *number* of exemplar rings too. Also: track bytes, not
points — a histogram point with 16 buckets is 4.3x a gauge point (MEASURED: 346 vs 185 B/point
resident), so a point count is the wrong unit for a memory bound.
**Risk:** the flush cadence changes, so `MetricWalTests`' generation/commit assertions and
`MetricChunkedRewriteTests`' file expectations may need explicit thresholds injected rather than
inherited. Use `ManualTimeProvider` while wiring options in, so the `MaxHotAge`/`FlushCheckInterval`
paths become testable without `Task.Delay`.
**Measure:** run the `IngestAndFlushCostPerPoint` fact with `DOTNET_GCHeapHardLimit` set to
0x18000000 (384 MB) and watch for OOM at 500 k histogram points.

### 7. `GetMetricNames` takes every lock in the hot dictionary, per request — CONFIRMED (CPU + alloc, per request)
`MetricStorageEngine.cs:658` — `_hot.Keys` on a `ConcurrentDictionary` acquires **all** table locks
and materialises a `List<SeriesKey>` of every series (at 38 741 series: ~1.2 MB and every ingest
thread blocked for the duration), then `.Select(k => k.Name).Distinct()` over it, then a second
LINQ chain over `_coldSegments`, `Concat`, `Distinct`, `Where`, `OrderBy`. `GET /api/metrics/names`
is called by the Explore UI on every page load.
**Fix:** `_meta.Keys` instead — `_meta` is keyed by metric **name** (`:204`), is maintained at
ingest (`UpdateMeta:579`), and already survives hot-tier drains; it has tens of entries where
`_hot` has tens of thousands. Same substitution makes `GetCatalog:601-620` (`meta.LabelValues.Keys.OrderBy(...).ToArray()`
per metric, allocating a fresh sorted array on every catalog call) worth a cached, versioned array.
**Risk:** `_meta` is never pruned, so a metric that stopped reporting stays in the names list —
which is what `LastSeenMs` is for; the UI already has it.
**Measure:** wall time + alloc of `GetMetricNames` at 40 000 hot series; add to the query probe.

### 8. `MetricStorageEngine.QueryAsync` full-scans the hot tier per query — CONFIRMED (CPU, per request)
`MetricStorageEngine.cs:685-703` — `foreach (var (key, series) in _hot)` over **every series of
every metric**, with `key.Name.Equals(metricName, OrdinalIgnoreCase)` (a culture-free but
per-character compare) per entry; `GetLatestAsync:741` does the same. `HotSeries.GetPoints:1827-1834`
then runs `Where().OrderBy().ToList()` under the series lock — the points are appended in arrival
order and `OrderBy` is a full stable sort of an already-sorted list, per series per query.
**Fix:** a `ConcurrentDictionary<string, ConcurrentDictionary<LabelSet, HotSeries>>` keyed by name
first (or keep `_hot` and add a name→series-list index maintained in `ApplyToHotTier`), so a query
touches only its own metric; and in `GetPoints`, binary-search the sorted list for the range and
return a `List` sized to the slice — or an `ArraySegment`-shaped view, since the caller only
enumerates.
**Risk:** `FlushHotTierAsync` iterates `_hot` wholesale (`:892`) and the restore path re-adds by
`SeriesKey` (`:951`) — a two-level map changes both. `GetPoints` is also the writer's path
(`MetricWriter.cs:125,235`), so the sort removal must be justified: points are appended under the
series lock in the order `Ingest` sees them, which for a single exporter is chronological but for
two exporters interleaving is not — keep a cheap `isSorted` flag flipped on an out-of-order append.

### 9. Metric query responses go through reflection-based `System.Text.Json` and buffer whole results — CONFIRMED (CPU + alloc, per request)
`MetricQueryEndpointMapper.cs:15,32,37,42,56,76,88,114,128` — every endpoint returns
`Results.Json(<object>)` with no `JsonTypeInfo`. `Program.cs:96-101` shows the reflection resolver
registration commented out, and `Ameto.Metrics` has **no `JsonSerializerContext`** (logs have
`AggregationJsonContext`/`EventCountsJsonContext`, traces have `TraceStreamJson` at
`TraceQueryEndpointMapper.cs:1252`). On top of that every endpoint materialises the whole answer
first: `:56` `series.Select(ToDto).ToList()`, `:124-128` `List<MetricSeriesDto>`, and `ToDto:147-159`
allocates a `Dictionary<string,string>` for the labels (`Pairs.ToDictionary`) plus **one
`MetricPointDto` object per data point** — for a 2 000-series answer that is 120 000 short-lived
class instances before a byte is written.
**Fix:** a `[JsonSourceGenerationOptions(PropertyNamingPolicy = CamelCase)] partial class
MetricJson : JsonSerializerContext` covering the seven DTOs, passed to
`Results.Json(value, MetricJson.Default.X)`; and a direct `Utf8JsonWriter` row writer over
`IAsyncEnumerable<MetricSeries>` (the shape `SseJsonWriter` / the trace stream writer already use)
so `GET /api/metrics/{name}` and `POST /api/metrics/query` stream series as they are produced and
never build the DTO graph.
**Risk:** property names must stay exactly as reflection produces them today (default
`JsonSerializerOptions` in ASP.NET Core = camelCase) — the Angular client reads `ts`, `value`,
`count`, `sum`, `labels`, `bounds`, `columns`. Pin with a byte-for-byte response test before the
switch.

### 10. The metric WAL never msyncs, and a growth doubles a mapping under the ingest lock — CONFIRMED (durability + latency)
`MetricWriteAheadLog.cs:156` says it outright ("nothing msyncs this log"); there is no
`_accessor.Flush()`, no `FlushFileBuffers` anywhere in the file — only `_poolStream.Flush()`
(`:389,:433,:705,:990,:1098`), which reaches the OS, not the platter. So the whole purpose of the
log — surviving a crash without a file per metric per minute — holds for a process crash and
**not for a power loss**; `GenerationSanityMargin` (`:160`) and `SeriesIndexSanityCap` (`:171`)
exist precisely because a torn entry cannot be told from a good one (the residual "v2 per-entry
CRC" from the poisoned-WAL incident). Separately, `Grow():1156` is called from inside
`lock (_writeLock)` at `:645-646` and unmaps/remaps/`SetLength`s the file — every ingest thread in
the process stalls on a file resize, and the doubling from 8 MB reaches 32 MB in the histogram
workload above.
**Fix (perf half):** size the initial capacity from `MemoryBudgets` (#6) so growth is rare; grow by
a bounded increment rather than doubling past a threshold; take the resize outside the append lock
behind a short reader-drain (the `StorageEngine` write-gate pattern). **Fix (durability half, flag
as its own PR):** a v2 entry layout with a per-entry CRC32C (`Ameto.Storage.Crc32c` is hardware-
accelerated and ~0.1 ns/byte — free at 48 B/entry) plus an `msync` of `[lastFlushed, writeOffset)`
on the flush tick, exactly what the logs WAL got as WAL v4. **This is an on-disk format change** —
it needs a version bump and a v1-reader fallback.
**Risk:** `ReadAll:1012`'s tolerance for torn tails, `ReconcileDataEndLocked:451`, and
`Compact:935` all read the 48-byte stride directly; a CRC field changes `EntryHeaderSize` and
therefore every offset arithmetic in the class. `MetricWalTests` is the gate.

### 11. Shutdown disposes `_coldLock` while queries and retention may hold it — SUSPECTED (correctness at shutdown)
Asked directly by the brief: does `DisposeAsync`/`_disposeCompleted` really guarantee no flush
outlives dispose? **For flushes, yes.** The chain is sound: `_cts.Cancel()` → await `_flushTask`
(whose last act at `:790` is an unconditional `FlushHotTierAsync`) and `_rollupTask`; then
`DrainThresholdFlushesAsync:1664`. The threshold-flush race is genuinely closed, because
`ScheduleThresholdFlush:481-521` registers the task **before** releasing the gate that lets its
body run, and the body reads `_disposed` only after that gate — so a flush is either in the
drain's snapshot or it sees `_disposed != 0` and returns without touching a lock (`:550`). The
`_ingestClosed` fence under `_snapshotLock.EnterWriteLock():1642` then makes "no `Append` is in
flight" true rather than likely, and `_snapshotLock` is correctly left undisposed (`:1653-1658`).
**What is NOT covered:** `_coldLock.Dispose():1651` has no equivalent fence. `QueryAsync:707`,
`GetMetricNames:662`, `PerformRollupAsync:1120` and `PruneAsync:1685` all take it, and the first
two are reached from **Kestrel, which is still serving** — hosted services stop in reverse
registration order and the HTTP pipeline outlives `MetricStorageHostedService.StopAsync`
(`MetricsServiceExtensions.cs:45`). A query holding the read lock at that moment gets
`ObjectDisposedException` out of the middle of a response; a query *waiting* on it makes
`ReaderWriterLockSlim.Dispose` throw `SynchronizationLockException`, which escapes `DisposeAsync`
(nothing catches around `:1649-1651`) while `_disposeCompleted` is still completed by the
`finally` — so the other two disposers return believing the teardown finished. Second gap:
`RetentionService` holds `IRetentionTarget` (`MetricsServiceExtensions.cs:27`) and `PruneAsync`
is not gated on `_disposed` at all.
**Fix:** the same shape as `_snapshotLock` — either do not dispose `_coldLock` either (it is a
process-lifetime singleton and its handles are finalizable), or add a `_readersClosed` gate:
`_coldLock.EnterWriteLock(); Volatile.Write(ref _coldClosed, 1); ExitWriteLock();` before the
dispose, with every reader checking it and answering empty. Gate `PruneAsync` on `_disposed`.
**Risk:** none to data; pin with a test that disposes while a `QueryAsync` enumerator is mid-flight.

### 12. Rollup re-reads and re-decodes every source file once per series chunk — CONFIRMED (CPU, background)
`MetricStorageEngine.cs:1336-1342` (pass 0 over every source) then `:1355-1366` (every source
again, **per chunk**), each `ReadAllSync` reopening the file (`MetricReader.cs:461`, a fresh
64 KB `FileStream` buffer per open), re-inflating the LZ4 section and re-materialising every
label string (#1) only to throw away the series the chunk does not want. The doc comment at
`:1303-1325` argues this is cheaper than retaining the points, which is true — but the key set
from pass 0 is already in hand, so the *decode* is avoidable, not just the retention.
`CompactSegments:1266-1269` and `MergeTier:1234-1256` add `GroupBy`/`Any`/`ToList` chains per pass.
**Fix:** cache pass 0's `SeriesKey` per (file, ordinal position) so later chunks can skip a series
by its position without decoding its labels — v3 writes series back to back in one block, so a
`Skip` over the msgpack map is a header walk, not a decode. Cheaper still: order the chunks by the
key set so chunk N only needs files whose key range intersects it.
**Risk:** `RewriteMetricInChunks` explicitly refuses to align chunks to files (`:1315-1322`) and
that reasoning must not be broken. `MetricChunkedRewriteTests` is the gate.

### 13. Smaller, per-point or per-request
- `MetricStorageEngine.cs:434-458` — the exemplar loop **re-walks the whole batch** after the ingest
  loop, re-testing `item.TimestampUnixNano > futureLimit` per item; fold it into the main loop
  (the only reason it is separate is that it must not hold `_snapshotLock`, which a flag can carry).
- `:441` `_exemplars.GetOrAdd(item.Name, …)` per point with exemplars, and `ExemplarRing.Add:1726`
  takes a `lock` per exemplar; `Snapshot():1736` copies the entire 4 000-entry ring on every
  `GET /exemplars`, then `GetExemplars:643-651` filters, sorts and `GetRange`s it.
- `:586-594` `UpdateMeta` runs `_meta.GetOrAdd` + a nested `GetOrAdd` **per label per point** —
  4-8 concurrent-dictionary lookups per point in the steady state where nothing changes. Cache the
  `MetricMeta` on the `HotSeries` (the name is fixed per series) and skip `LabelValues` entirely
  once the series is known.
- `MetricTypes.cs:87-95` `LabelSet.ToString()` uses `StringBuilder` — only diagnostics, but it is
  the kind of thing a log template pulls in.
- `MetricQueryEndpointMapper.cs:163` `DateTimeOffset.TryParse(s, InvariantCulture, RoundtripKind)`
  per query parameter — `Utf8Parser`/`DateTimeOffset.TryParseExact('O')` is ~5x cheaper; minor.
- `MetricWriter.cs:141` `Guid.NewGuid().ToString("N").Substring(0, 8)` — two strings per file.
- `MetricStorageEngine.cs:1516` `Directory.EnumerateFiles(...).OrderBy(f => f)` at startup buffers
  every `.mts` path; with the 4-files-per-flush cadence a busy deployment has thousands.

---

## Existing probes and tests for this area, and how to run them

All commands from the worktree root with `TMP`/`TEMP` pointed at a scratch dir.

- `tests/Ameto.Perf/OtlpMetricProtoProbe.cs` — DOM vs span parser, ns/point + B/point; guard
  `spanBytes * 3 < domBytes`. `dotnet test tests/Ameto.Perf/Ameto.Perf.csproj -c Release --filter "FullyQualifiedName~OtlpMetricProtoProbe" --logger "console;verbosity=detailed"`
- `tests/Ameto.Perf/OtlpMetricProtoParityTests.cs` — span parser vs `OtlpProtoDecoder` + `OtlpMetricMapper`,
  field by field. **The parity gate for #1.**
- `tests/Ameto.Perf/MetricIngestContentionProbe.cs` — 4-thread `Ingest` throughput. **The gate for #3.**
- `tests/Ameto.Perf/OtlpProtoPayloads.cs` — `Metrics_Realistic()` (20 instruments x 25 series,
  every 3rd a 15-bucket histogram); the payload builder both probes share.
- `tests/Ameto.Storage.Tests/MetricReaderStreamingProbe.cs` — retained bytes at the midpoint of a
  4 098-series read, streamed vs materialised. **The gate for #4's reader changes.**
- `tests/Ameto.Storage.Tests/MetricWalTests.cs`, `MetricFormatV3Tests.cs`,
  `MetricChunkedRewriteTests.cs` — 72 facts across the three; the correctness gates for #3, #5,
  #10, #12. `dotnet test tests/Ameto.Storage.Tests -c Release --filter "FullyQualifiedName~Metric"`
- `tests/Ameto.Perf/AssemblyInfo.cs` serialises the assembly (`DisableTestParallelization`) — keep
  any new probe in it, not in a parallel collection, or the process-wide counters lie.
- `dotnet run -c Release --project tests/Ameto.Perf -- --filter *<Name>*` for BenchmarkDotNet runs
  (`Benchmarks.cs` has no metrics benchmark today).

## Probes to add

1. **`MetricFlushAllocProbe`** (#5, #2) — ingest N series x M points into a real
   `MetricStorageEngine`, measure `GC.GetTotalAllocatedBytes(precise: true)` and wall time across
   `DisposeAsync()` (the final flush), report ns/point, B/point and bytes-on-disk/point for a
   gauge and a 16-bucket histogram shape. Assert `flushBytes / points < 128`.
2. **`MetricHotTierRetentionProbe`** (#2) — burst, threshold flush, `GC.GetTotalMemory(true)`
   before and after. Assert `retainedAfterDrain < peak / 4`. Feed the burst in OTLP-sized chunks,
   or the measurement reads the batch array rather than the tier (this bit me: 55 KB/series vs the
   true 12 KB/series).
3. **`MetricQueryAllocProbe`** (#4, #7, #9) — a cold-loaded engine, then raw `QueryAsync`,
   `Rate`+group-by, `Quantile`, and `Last`+filter; alloc and ms per query per stored series.
   Assert `bytes < 64 * points`.
4. **Extend `MetricIngestContentionProbe`** (#3) to sweep 1/2/4/8 threads and print per-thread
   ns/point — the single number it prints today hides the flat scaling entirely.
5. **A `Downsample` golden-series parity test** before #4(d) touches it — it is shared with the
   rollup, so a change there rewrites files on disk.

## Note on the on-disk sizes above

The probe's 2 000 series share one label shape and a linear value ramp, so LZ4-HC plus the ms-delta
and slim-point encodings compress them to ~0.2-0.4 B/point. Real payloads will not. Read the
allocation and timing numbers as load-bearing and the bytes-on-disk numbers as a floor, not a
typical case.
