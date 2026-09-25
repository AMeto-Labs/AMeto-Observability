# Logs module — CPU / allocation / memory round, results

Branch `perf/logs-cpu-alloc`, 167 commits off `main` @ 677b49f, 229 files, +31 235 / −1 751.
Plan and reconnaissance: [LOGS-CPU-ALLOC-PLAN.md](LOGS-CPU-ALLOC-PLAN.md), [recon/](recon/).

## What shipped

| Package | Change |
|---|---|
| F1 flush-index | Trigram and inverted accumulators on pooled arenas with a UTF-8 term table, bloom adds only on first sight, index sections streamed into the segment file, merges index an exception without decoding its stack. Sections stay byte-identical. |
| F2 flush-writer | Flush order decided in one pass over the headers with an identity short-cut, pooled level split, each row encoded once into its column, exception column written through a `MessagePackWriter`. Segment bytes pinned by a golden hash. |
| Q2 query-cold | Candidate rows decode lazily, exceptions read zero-copy, block strings deduplicated, trigram lookup by sorted merges, one `SegmentReader.Open` per segment per query. |
| Q1 query-hot | Three-valued header predicate on the hot tier (level, service, trace id), per-chunk timestamp zone map with top-k, array-backed intern pool, pooled per-chunk slot arrays, live-tail poll reuses filter, deadline and catalog. |
| Q3 query-eval | SSE rows written straight from the event, frames batched to 16 KB, the scan yields so pending rows reach the client, LIKE fast path, node dispatch by tag, typed property probes, header-road aggregation. |
| I1 ingest-proto | OTLP protobuf logs parsed straight into the ring with a nesting depth cap, dedicated request-body pool, gRPC inflate sized from the gzip trailer. |
| I2 ingest-clef | CLEF `/api/events` streams into the ring, canonical template interning, service interned once per OTLP resource, auth middleware skipped on ingest routes, API keys matched on digest bytes, WAL flush syncs only what was written. |
| B background | Diagnostics walks the data directory once a minute, index cache evicts what nothing reads, memory budgets derived from what the host can afford, alert rules count whole cold segments from the catalog, ingest arena commits on demand. |

Two follow-ups landed on the branch itself: the CLEF body buffers moved to the dedicated pool, and the live tail moved to the direct JSON writer.

## Measured, probe level

Before values come from the commit that introduced each change; after values were measured at the branch head.

| Area | Metric | Before | After |
|---|---|---|---|
| Index build | µs per event | 11.78 | 3.49 |
| Index build | bytes per event | 1 625 | 314 |
| Trigram accumulator | ns per posting | 57 | 11 |
| Segment writer | sort, 200k-event tier | 25.7 ms | 0.9 ms |
| Segment writer | exception column, bytes per event | 4 824 | 19 |
| OTLP protobuf logs | ns per record | 37 366 | 1 151 |
| OTLP protobuf logs | bytes per record | 8 015 | 0 |
| CLEF ingest | bytes per event | 710.9 | 0 |
| CLEF ingest | ns per event | 1 173 | 236 |
| Template retention | bytes per event held by a tier | 224.9 | 1.3 |
| gRPC inflate | bytes per call | 14 761 | 312 |
| WAL flush tick | idle tick | 486.4 µs | 0.03 µs |
| WAL flush tick | held under the write lock | 486.4 µs | 31.4 µs |
| API key check | bytes / ns per request | 304 B / 502 ns | 0 B / 140 ns |
| Hot tier | filtered page, 200k events | 7 109 KB / 108 ms | 9.6 KB / 1.99 ms |
| Hot tier | tail poll, 300k-event tier | 103 KB / 16.1 ms | 9.6 KB / 0.079 ms |
| Trigram lookup | bytes per exact-value call | 26 040 | 32 |
| Exception read | bytes per row left untouched | 5 349 | 1 405 |
| SSE page | bytes per row | 551 | 3 |
| Live tail | bytes per frame | 602 | 0 |
| Live tail | socket sends per 500-row poll | 500 | 84 |
| Filter eval | LIKE, bytes per event | 62.5 | 0 |
| Aggregation | count by level, 60k events | 68.2 ms / 21.9 MB | 4.4 ms / 0.7 MB |
| Diagnostics | bytes per call, 205 files | 173 491 | ~0 |
| Ingest arena | private bytes at construction | +513 MB | +5 MB |

## Measured, whole server

Both servers built from source, run one at a time on a 20-core box with the same generators and the same offered load, one run per side.

| Scenario | Metric | main | branch | Change |
|---|---|---|---|---|
| CLEF 50k/s | CPU core-seconds per million events | 10.72 | 4.27 | −60.2 % |
| CLEF 50k/s | allocated bytes per event | 2 912 | 71 | −97.6 % |
| CLEF 50k/s | gen2 collections | 135 | 1 | −99 % |
| CLEF 50k/s | batch latency p99 | 20.8 ms | 2.1 ms | −89.9 % |
| CLEF 50k/s | working set, average | 266 MB | 234 MB | −12.0 % |
| OTLP JSON 50k/s | CPU core-seconds per million events | 17.24 | 10.08 | −41.5 % |
| OTLP JSON 50k/s | allocated bytes per event | 3 060 | 227 | −92.6 % |
| Live tail 5k/s | CPU core-seconds per 100k frames | 4.21 | 0.89 | −78.9 % |
| Idle | private bytes | 561 MB | 49 MB | −91.3 % |
| Query, level filter | p95 | 447.9 ms | 383.8 ms | −14.3 % |
| Query, trace id | p95 | 216.2 ms | 180.5 ms | −16.5 % |

Neither side dropped an event, and the bytes stored on disk are unchanged.

## What the numbers do not say

- One run per side, so anything under about ten percent is noise. The large effects are far outside it.
- `RamTargetPercent` was forced to 99 on both sides because the host sat at 87–93 % memory from other applications. Everything else ran shipped defaults.
- The generators ran on the same box as the server, which inflates both sides equally.
- The derived memory budgets are a no-op on a 32 GB host: they resolve to the old constants. The 512 MB stand, which that work was aimed at, still needs its own measurement.
- The query scenario holds more resident memory than main (average 1 096 → 1 317 MB) while committing less (peak private bytes 2 099 → 2 035 MB). That is the expected shape of allocating 33.5 GB instead of 66.5 GB: half the collections, a higher resting heap.
- `@mt like '%timeout%'` looked 3–9 % slower in the load run. A dedicated harness over one corpus read by both sides says the opposite: −4.2 % median, −6.6 % best of five, with trigram lookup 6.8× faster and the scan 25.6 % cheaper. The load figure was one sample against one sample during a merge.

## Verification

At the branch head: build clean, 34 warnings, all in test projects. Core 172, Indexing 155, Storage 700 (1 skipped), Query 762, Integration 393, Perf 141 — 2 323 tests passing.

Every package was reviewed adversarially, then its fix commits were reviewed again; the integration was reviewed through five lenses with an independent skeptic per finding. Two blockers were caught before merge: a protobuf nesting bomb that crashed the process, and an intern-pool race that stored empty message templates. Four defects existed only in the combination of packages and were caught at merge time.

## Follow-ups

- The segment index cache is the biggest remaining query win: about 73 MB per entry against a 256 MB budget gave 37 hits against 2 072 misses in the load run, so most of a filtered query re-decodes index sections it decoded a query ago. A finer cache unit, or no cache at all on a small host, is worth more than anything else left on the query path.
- Re-measure on the 512 MB stand, where the budget work actually applies.
- ~~OTLP over HTTP still refuses `Content-Encoding: gzip`, which is what a default collector sends.~~ Closed by issue #82.
- Traces and metrics have not been through this treatment.
