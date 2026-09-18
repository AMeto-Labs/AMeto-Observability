# Logs module — CPU / allocation / memory optimisation plan

Branch `perf/logs-cpu-alloc` (from `main` @ 677b49f, 2026-09-11). Goals, in priority order:
CPU per event, allocations per event/batch (GC pressure, LOH churn), resident memory.
On-disk formats stay byte-identical in this round; format changes are listed as follow-ups.

Recon reports (evidence, file:line, fix sketches): `docs/perf/recon/ingest.md`, `flush-merge.md`,
`query.md`, `background-memory.md`.

Baseline (Release, this branch): Core 109 / Indexing 79 / Storage 606 / Query 512 tests green.
Index build ≈ 10 µs CPU/event, ≈ 300 MB allocated per flushed group; ingest protobuf ≈ 120
allocations / 6–8 KB / 3–5 µs per record; hot-tier search materialises every hot event before
the filter runs.

## Work packages

Each package lives on its own branch/worktree off `perf/logs-cpu-alloc` and is merged back in
the order below after a review pass over its commits.

| WP | Branch | Items (recon id) | Owner files |
|----|--------|------------------|-------------|
| I1 ingest-proto | `perf/logs-ingest-proto` | ingest #1 zero-copy OTLP protobuf logs parser (HTTP+gRPC); #4 shared body-buffer pool; #5 gRPC gzip inflate sizing; #9 OTLP response writer; #10 JSON parser micro | `Ameto.Otel/*`, new pool in `Ameto.Core` |
| I2 ingest-clef | `perf/logs-ingest-clef` | ingest #2 canonical template string in hot tier; #3 streaming CLEF `/api/events` (no `LogEvent` per event); #6 service interned once per resource; #7 auth middleware bypass for ingest routes; #8 `ApiKeyCache` zero-alloc lookup; #9 `/api/events` response; #12 WAL msync range-only, file flush outside the lock | `Ameto.Ingestion/*`, `LogEventSerializer` (batch read), `StringInternPool`, `WriteAheadLog`, `Program.cs` middleware, `ApiKeyCache` |
| F1 flush-index | `perf/logs-flush-index` | flush #1+#8 trigram accumulator (direct-indexed ASCII table + posting slab arena, no locks); #2 bloom adds only on first sight; #3 streamed section serialisation (no LOH copies); #4+#5 inverted index UTF-8 term arena + span-keyed table, single transcode per value; #6 merge indexes exceptions without decoding the stack; #11 span tokenizer (optional) | `Ameto.Indexing/*`, `SegmentBloomFilter` (no format change), `ExceptionInfo.ReadIndexFields`, `SegmentWriter.SealGroup` |
| F2 flush-writer | `perf/logs-flush-writer` | flush #7 sort order + level split without delegates/LOH copies; #10 writer per-event micro-costs; #12 verify LZ4 context allocation | `SegmentWriter` (rest), `StorageEngine` flush level split, `SegmentEventSource` dedup |
| Q1 query-hot | `perf/logs-query-hot` | query #1 header-level predicate pushdown on the hot tier; #2 per-chunk ts zone map + top-k instead of full sort; #14 pool lookups by id; #12 live-tail per-poll fixed costs; ingest #11 recycle per-chunk arrays | `HotTierScan`, `HotTierSegment`, `StorageEngine` read side, `QueryExecutor` hot part, `CompiledFilter.TryMatchHeader`, `QueryGuard`, live-tail loop |
| Q2 query-cold | `perf/logs-query-cold` | query #3 lazy exception + template/service dedup on candidate rows; #6 zero-copy `ExceptionInfo` read; #5 trigram lookup + candidate intersection without HashSets/copies; #7 one `SegmentReader.Open` per segment per query, LZ4 from the mapped pointer | `SegmentReader`, `LogEvent`, `ExceptionInfo` (read), `SegmentTrigramIndex.Lookup`, `SegmentIndexReader`, `QueryExecutor` prefilter/scan |
| Q3 query-eval | `perf/logs-query-eval` | query #9 SSE output without reflection STJ/DTO copies, batched flushes; #10 LIKE `%lit%` vectorised fast path + evaluator dispatch; #11 typed `TryReadProperty`; #13 ordinal Eq; #4 aggregation via header scan / unordered per-segment accumulation | `FilterEvaluator`, `AggregationExecutor`, `LogEventSerializer` (property probe), `SseJsonWriter`, `EndpointMapper` search/aggregate/output |
| B background-memory | `perf/logs-background` | bg #1 `/api/diagnostics` cached dir walk, no thread snapshot; #12 index cache idle-age eviction + diagnostics; #8 `malloc_trim` export probed once; #9 peer prober idle without seeds; #5 flush/native/cache budgets derived from available memory; #3 alert log rules count immutable segments from the catalog, level mask on the header aggregator; #6 ingest slab arena reserve + commit on demand | `DiagnosticsEndpointMapper`, `SegmentIndexCache`, `WorkingSetTrimmer`, `RamPressureService`, `PeerProber`, `Options` (new knobs), `StorageEngine` budgets + `AggregateLogVolumeAsync`, `AlertEvaluator`, `IngestionRingBuffer` allocation |

Already covered elsewhere: bg #2 (WAL no-op tick) → I2; bg #4 (catalog snapshot, one open per query) → Q1/Q2; bg #7 (per-chunk LOH arrays) → Q1. bg #11 (GC/runtime config: `GCHeapHardLimitPercent` on 512 MB hosts, `PublishReadyToRun`, gen0 size on big boxes) is a deployment recommendation, listed under follow-ups.

## Gates

- `dotnet build Ameto.slnx -c Release` clean; suites for the touched projects green
  (Core / Indexing / Storage / Query / Integration / Perf facts).
- Every behavioural change has a test that fails without it; parity tests pin byte equality of
  segments/indexes and of JSON output.
- Every item records before/after numbers from a `tests/Ameto.Perf` probe in its commit body.
- After merging into `perf/logs-cpu-alloc`: full suites + `FlushCpuProbe`, `IndexBuildAllocProbe`,
  `HotTierScanAllocProbe`, `OtlpAllocProbe`, `SegmentCandidateAllocProbe`; then a k6/loggen run
  (`tools/loadtest`, `tools/loggen`) against a local server to compare CPU and RSS at 100k logs/s.

## Merge order

F1 → F2 → Q2 → Q1 → Q3 → I1 → I2 → B. Each merge is followed by a review of the package's
commits (`base..head`), a rebuild and the full suites.

## Follow-ups (not in this round)

- Bloom hashing: one 64-bit hash + multiply-shift (flush #9) — needs a format flag bit.
- Lazy per-property decode of the inverted section on cache miss (query #8) — after F1 lands.
- Index the emitted block on a second thread per tier (flush #13) — wall-clock only.
- OTLP/HTTP `Content-Encoding: gzip` is not handled (collector default) — functional gap.
- HTTP body parsing from `BodyReader` windows instead of a full-size rented array.
