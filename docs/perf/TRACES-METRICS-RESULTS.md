# Traces and metrics — CPU / allocation / memory round, results

Issue #83. PR #84 (`perf/traces-metrics-cpu-alloc`), merged as `7d67eeb` on 2026-09-24: 197 commits
off `main` @ `0a7389d`, nine work packages in three waves. Plan and reconnaissance:
[TRACES-METRICS-CPU-ALLOC-PLAN.md](TRACES-METRICS-CPU-ALLOC-PLAN.md), [recon/](recon/). The logs
round that came before it: [RESULTS.md](RESULTS.md).

## What shipped

| Package | Merge | Change |
|---|---|---|
| WP1 traces-proto | `4ef8c7c` | OTLP/protobuf traces parsed straight into ingest items with a nesting-depth cap, no DOM; the gRPC memmove and the per-request logger gone. |
| WP2 traces-blob | `f3e54ef` | The hot tier and the segment reader keep the msgpack attribute blob instead of a boxed dictionary; decoded only where a read needs it. |
| WP3 metrics-hot | `90478f0` | The metrics hot tier hands its lists over on drain, sweeps series that stopped reporting, and spends a byte budget from `MemoryBudgets`. |
| WP4 traces-writepath | `46c6877` | Shutdown gate, one engine lock per drained batch, span WAL v2 with CRC32C and a ranged msync, aggregates off the read lock, rate-limited ingest warnings. |
| WP5 traces-flush | `f900655` | Permutation sort, one-pass sidecars, pooled `.tracesum`, one blob decode per span per flush. |
| WP6 metrics-ingest | `830d0e7` | UTF-8 label interning, one WAL lock per batch, a name-to-series index, WAL growth outside the lock, a lock-free exemplar ring. |
| WP7 metrics-query | `49254f6` | Range-pushed cold reads, an aggregator without sorted dictionaries, source-generated JSON and streamed rows, a rollup that decodes each source once. |
| WP8 traces-sink | `2ebb0a3` | A raw span sink over a native ring and slab arena, interned span and service names, byte budgets for the hot tier, compaction and the ring. |
| WP9 traces-read | `e3d518a` | Trace detail, flame graph and compare written from the records without per-span dictionaries; allocation-free index lookups. |

Review and integration merges on the branch: `796689f` (wave-1 fixups: trace caps re-calibrated,
managed budget re-cut), `ddad460` (code-review fixes), `1af41d4` and `c1e7423` (wave-2 review
fixes), `4718411` (PR review fixes: a flush that cannot start no longer wedges the engine, budgets
and refusals reported), `c1f5197` (the span drainer's second `DisposeAsync` waits for the first).

Merged on `main` since, and so part of what is measured below as B: #96 (OTLP/HTTP gzip), #99
(flame graph at any depth, SSE bytes), #100 (a closed store reports itself), #101 (metric labels,
NaN, the label pool recovers from churn), #102 (culture-free span bloom fed from the blob), #105
(metric WAL v2, a CRC32C per entry), #106 and #107 (leftovers of #89, #90, #94), #117 (span WAL
relocation record), #119 (degraded availability), #123 (TraceQL reads a filter's keys in one walk
per span), #124 (`.tracesum` body). The logs-only merges in between (#97, #104, #109, #113, #116)
do not touch these paths.

## Measured, whole server

**A** is `main` just before #84: `0a7389d` (the first parent of `7d67eeb`). **B** is `origin/main`
at `d5ee138` (2026-10-08, after #119). Both were built in Release from their own trees and run one at
a time as a local process on the same 20-core box (i7-12700, 8 P-cores + 4 E-cores, 32 GB), each
round on a fresh data directory, with each tree's shipped `config.yml` and four environment
overrides: the port, the data directory, the admin password, and `RamTargetPercent` 99 (see below).
The load is `tools/loadtest/k6-traces.js` and `k6-metrics.js` (k6 v0.40, unchanged since A), OTLP
JSON, 1 000 spans or points per request, on the same box.

One round, as `tools/loadtest/ab-round.ps1` runs it:

- The load starts **exactly 70 s after `/health` answers**. Every background timer in the server
  starts with the process, and the trace compaction worker runs once 60 s after start, then
  hourly. The first rounds started whenever the machine went quiet, so that run landed inside the
  window in some rounds and not in others, and the rounds split into two modes 2–3 µs/span apart
  on both sides — more than A and B differ by. With the fixed start it runs on an empty store
  before the load, every round sees every timer at the same phase, and the compaction is measured
  on its own (below).
- Before the load the whole machine has to stay quiet — every 1-s sample of total CPU under 15 %,
  host RAM under 88 % — for 30 s in a row inside those 70 s; otherwise the server is stopped and
  the round starts again.
- 120 s at a constant arrival rate, then 15 s to settle: that is the window. Server CPU from
  `GetProcessTimes`; allocations and collections from `dotnet-counters` (`System.Runtime`, 1 s),
  cross-checked against the deltas of `/api/diagnostics` (`managedTotalAllocated`,
  `gen0/1/2Collections`): allocations agreed within 0.02 % and collection counts exactly in every
  round. Collections are counted once each, at the highest generation they collected (what
  `dotnet-counters` reports; `GC.CollectionCount(n)` counts n and above). Peaks from
  `GetProcessMemoryInfo` (peak working set, peak commit = private bytes); latency and the
  ingested/dropped totals from k6; refusals from `/api/diagnostics` (B) or the server log (A,
  which logged one warning per refused request and counted nothing).
- Every second, every process's CPU is read from one `SystemProcessInformation` snapshot and split
  into ours (server, k6, dotnet-counters, the script), the OS processes the load itself drives
  (System, Memory Compression, Defender), and foreign — everybody else; what no snapshot holds
  (a `git` or a compiler that starts and exits within the second) is the unattributed rest of the
  machine's total. A round whose 5-s mean of foreign plus unattributed CPU went above 15 % of the
  machine, or whose host RAM reached 90 % (where the .NET GC switches to its high-memory-load
  mode), was thrown away and run again. The same test was applied to the compaction run; one that
  failed it keeps its load figures and loses its compaction figures.
- After the window: each query five times with curl, first run and the median of the other four.
- Traces only: the server is then stopped and started again on the same data, and the compaction
  run it makes 60 s after start — merging the 120 segments the load flushed — is measured on its own.

Rounds were interleaved A, B, A, B as far as the machine allowed: a slot whose round was thrown
away was run again before the next one. Cells are the median of the kept rounds, the smallest and
largest in brackets; MB is 2^20 bytes throughout.

**What the machine allowed.** Traces at 50 000 spans/s are measured cleanly: three A rounds, two B
rounds, one clean compaction run a side. The rest is **indicative**. The two metrics pairs passed
the test in force when they ran and failed the stricter one adopted afterwards (short bursts of
other sessions' `git` and `grep` put their 5-s foreign-plus-unattributed peak at 15.3–19.2 %). The
higher rates are 30-s probes from the first protocol, one per side. Clean rounds for those, and a
third B traces round, were not taken: on 2026-10-09 another session ran the full test suite three
times within an hour (13:33, 13:47, 14:23), with Defender scanning at ~85 % of the machine in
between, and the campaign was stopped rather than measured through it. `ab-compare.ps1` finishes
them in one command on a quiet machine.

### Traces, 50 000 spans/s, 120 s

| Metric | A | B | B vs A |
|---|---|---|---|
| Spans ingested / dropped | 6 001 000 / 0 | 6 000 500 / 0 | |
| Batch latency p50, ms | 2.07 (2.06–2.11) | 2.62 (2.61–2.63) | +26.6 % |
| Batch latency p95, ms | 14.34 (13.92–14.75) | 10.79 (10.52–11.07) | −24.7 % |
| Batch latency p99, ms | 27.78 (26.29–28.30) | 22.30 (20.83–23.76) | −19.7 % |
| Server CPU, core-seconds | 73.1 (72.5–74.3) | 78.4 (76.8–80.0) | +7.3 % |
| CPU µs per span (= core-s per million) | 12.18 (12.08–12.38) | 13.06 (12.80–13.33) | +7.2 % |
| … of which kernel time, core-s | 12.6 (11.7–13.3) | 17.8 (16.9–18.6) | +41.0 % |
| Allocated bytes per span | 1 864 (1 862–1 867) | 765 (762–769) | −59.0 % |
| gen0 / gen1 / gen2 collections | 37 / 673 / 192 | 8 / 262 / 122 | gen2 −36.2 % |
| GC pause, s | 12.59 (12.11–12.81) | 4.31 (4.11–4.51) | −65.8 % |
| GC heap after a collection, max MB | 201 (195–207) | 99 (93–104) | −50.9 % |
| Peak working set, MB | 341 (331–347) | 237 (228–246) | −30.5 % |
| Peak private bytes, MB | 279 (274–279) | 171 (165–177) | −38.6 % |
| Working set, mean under load, MB | 249 (248–249) | 195 (193–197) | −21.8 % |
| Thread-pool work items per span | 0.037 (0.037–0.039) | 0.431 (0.422–0.440) | ×11.6 |
| Monitor-lock contentions | 192 (171–194) | 5 056 (4 502–5 609) | ×26 |
| Refusals | none logged | none (`/api/diagnostics`) | |
| TraceQL `{ .db.system = "mssql" && duration > 1s }` (scans every span), first / warm, ms | 1 844 / 1 559 | 1 946 / 1 588 | within spread |
| TraceQL `{ .http.status_code = 500 }` limit 100, first / warm, ms | 47.1 / 27.3 (25.6–34.0) | 53.5 / 48.6 (41.9–55.3) | warm +78 % |

### Trace compaction, on the data that load left

| Metric | A | B | B vs A |
|---|---|---|---|
| Segments in → out | 120 → 60 | 121 → 32 | |
| Spans rewritten | 6 000 000 | 11 789 261 | +96.5 % |
| Wall time, s | 39.7 | 52.9 | +33.2 % |
| CPU, core-seconds | 38.67 | 51.94 | +34.3 % |
| Allocated, MB | 9 001 | 7 368 | −18.1 % |
| gen2 collections | 131 | 146 | +11.5 % |
| Peak working set / private bytes since restart, MB | 368 / 314 | 332 / 287 | −9.8 % / −8.7 % |

One clean run a side. The segment counts and the spans rewritten were the same in every run of
either side, and the runs left out for foreign CPU kept the order: A 40.6 and 43.8 core-seconds,
B 54.3 and 62.8.

### Metrics, 50 000 points/s, 120 s — indicative

| Metric | A | B | B vs A |
|---|---|---|---|
| Points ingested / dropped | 6 000 500 / 0 | 6 000 500 / 0 | |
| Batch latency p50 / p95 / p99, ms | 2.89 / 7.48 / 11.25 | 2.72 / 7.17 / 11.96 | |
| CPU µs per point | 5.03 (4.85–5.22) | 4.73 (4.63–4.83) | −6.0 %, not claimed |
| Allocated bytes per point | 1 714 (1 714–1 714) | 1 263 (1 261–1 265) | −26.3 % |
| gen0 / gen1 / gen2 collections | 523 / 149 / 30 | 394 / 182 / 45 | gen2 +47.5 % |
| GC pause, s | 2.04 | 2.07 | |
| GC heap after a collection, max MB | 197 (197–198) | 83 (80–85) | −58.2 % |
| Peak working set, MB | 434 (413–454) | 257 (256–259) | −40.7 % |
| Peak private bytes, MB | 333 (313–353) | 149 (147–150) | −55.5 % |
| `rate` of a counter by `http.route`, first / warm, ms | 92.4 / 37.6 | 96.4 / 22.3 | warm −40.8 % |
| `quantile` 0.95 of a histogram, first / warm, ms | 94.8 / 88.9 | 133.6 / 112.7 | +40.9 % / +26.7 % |
| Metric segments on disk after the round (`/api/diagnostics`) | 480 | 760 | +58.3 % |

### Higher rates — 30-s probes, indicative

| | Traces 150 000 spans/s — A | B | Metrics 250 000 points/s — A | B |
|---|---|---|---|---|
| Requests k6 could send (of 4 500 / 7 500) | 4 241 | 4 486 | 7 476 | 7 501 |
| Dropped by the server | 966 000 (22.8 %) | 33 450 (0.75 %) | 0 | 0 |
| Refusals | 966 whole requests (90 % ring gate, logged) | 43 `RefusedNoSlot` | — | — |
| Batch latency p50 / p95 / p99, ms | 5.1 / 778 / 1 262 | 3.2 / 31 / 242 | 4.0 / 193 / 291 | 3.1 / 13 / 31 |
| CPU µs per item | 22.9 | 14.7 | 6.46 | 4.94 |
| Allocated bytes per item | 1 880 | 705 | 1 711 | 1 297 |
| Peak working set / private bytes, MB | 2 653 / 2 318 | 1 235 / 1 043 | 512 / 375 | 295 / 174 |

Where loss starts: at 150 000 spans/s A refuses whole batches at its 90 % ring gate — 23 % of the
spans — and grows to 2.6 GB; B takes 99.25 %: when its ring runs out of slots it keeps the part
of a request already in and refuses the rest.
Neither drops a metric point at 250 000 points/s; A queues instead (p95 193 ms, k6 short of VUs).

### Every round

| Round | Side | CPU µs/item | Alloc B/item | gen2 | Peak WS MB | p50 / p99 ms | Dropped | Idle before, mean (max) % | Foreign CPU, mean / 5-s max % | Host RAM max % |
|---|---|---|---|---|---|---|---|---|---|---|
| traces 10-08 11:27 | A | 12.18 | 1 862 | 186 | 347 | 2.11 / 27.78 | 0 | 5.8 (12.9) | 6.4 / 14.6 | 84 |
| traces 10-08 11:32 | B | 12.80 | 762 | 122 | 228 | 2.61 / 23.76 | 0 | 7.5 (14.1) | 6.1 / 14.2 | 85 |
| traces 10-08 11:38 | A | 12.08 | 1 867 | 192 | 331 | 2.06 / 26.29 | 0 | 7.3 (14.7) | 4.3 / 11.4 | 86 |
| traces 10-08 14:12 | A | 12.38 | 1 864 | 194 | 341 | 2.07 / 28.30 | 0 | 6.3 (14.3) | 5.3 / 13.6 | 86 |
| traces 10-08 14:33 | B | 13.33 | 769 | 123 | 246 | 2.63 / 20.83 | 0 | 5.3 (12.1) | 3.7 / 7.8 | 88.3 |
| metrics 10-08 14:50 (indicative) | A | 5.22 | 1 714 | 29 | 454 | 2.78 / 10.35 | 0 | 5.4 (11.6) | 4.7 / 15.3 | 89.3 |
| metrics 10-08 14:59 (indicative) | B | 4.63 | 1 261 | 44 | 256 | 2.69 / 11.16 | 0 | 5.8 (9.1) | 5.5 / 17.7 | 88.0 |
| metrics 10-08 15:03 (indicative) | A | 4.85 | 1 714 | 32 | 413 | 3.00 / 12.16 | 0 | 7.2 (13.1) | 6.7 / 19.2 | 89.2 |
| metrics 10-08 15:07 (indicative) | B | 4.83 | 1 265 | 46 | 259 | 2.75 / 12.75 | 0 | 6.8 (13.5) | 6.1 / 16.6 | 88.0 |

Foreign CPU is "foreign plus unattributed" in percent of all 20 logical CPUs; the idle level is
total CPU over the 30 quiet seconds the gate waited for. The first three rounds predate the
per-second RAM reading; their RAM column is the highest of the server's own 30-s `MEM` lines.

**Thrown away:** 13 rounds that ran their load were discarded for foreign CPU or host RAM (the
four indicative metrics rounds among them); 5 more never started their load because the machine
did not go quiet inside the 70 s; 4 were cut off (the host out of memory, a driver restart, the
session stopping, and one stopped in its gate to wait out a test suite). The first protocol's 13
rounds were set aside whole.
`ab-summary.ps1` lists every discarded round with its reason and the foreign processes in it.

## Where B is worse than A

Nothing below was fixed in this round; each comes with an issue text. File and line references are
to B, `d5ee138`; the code they point at is unchanged on `main` @ `981aa37`, where
`TraceStorageEngine.cs`'s lines have moved (`.WithWeight(loadedBytes)` is at `:4060` there).

### Trace ingest: CPU per span up, batch p50 up — the span drainer is woken every two or three spans

At 50 000 spans/s B spends 7 % more CPU per span than A although it allocates 59 % less and
pauses two thirds less for the GC; the CPU ranges do not overlap. User time is the same on both
sides (59–62 core-seconds a round); the difference is kernel time, 16.9–18.6 against 11.7–13.3.
It comes with a pattern A does not show: 0.43 thread-pool work items per span against 0.037 —
about 19 000 a second against 1 700 — 26 times the monitor-lock contentions, and 114–127
`SemaphoreFullException`s a round against 2–3.

`SpanRingBuffer.TryEnqueueRaw` (`src/Ameto.Tracing/Ingestion/SpanRingBuffer.cs:370`) releases the
drainer's semaphore whenever its count is 0 — per span — and `EndBatch` (`:410`) signals nothing.
A did the same, but its drainer spent ~4.5 µs per span in `WriteSpan` and rarely caught up with a
request in flight. B's `DrainOnce` → `WriteRaw` takes a batch in a fraction of that, empties the
ring while the request thread is still parsing, parks in `WaitForItemsAsync`
(`SpanDrainer.cs:159`), and the next span's `Release` schedules it again. The logs path signals
once per batch (`OtlpLogStreamParser.cs:91`, `OtlpLogProtoParser.cs:144`). This is read from the
counters and the code, not from a profile; that the same wake-ups also raise the p50 is likely,
not shown.

> **Issue: Traces: the span drainer is woken every 2–3 spans — per-span signalling against a
> drainer that now outruns the request.** At 50k OTLP/JSON spans/s (`tools/loadtest/ab-compare.ps1`,
> 3 A and 2 B rounds) `main` @ `d5ee138` runs 0.43 thread-pool work items per span against 0.037 on
> `0a7389d`, with 26× the lock contentions and +41 % kernel time; CPU per span is up ~7 % and batch
> p50 ~27 % although allocations fell 59 %. `SpanRingBuffer.TryEnqueueRaw` releases `_signal` per
> span and `EndBatch` does not signal. Signal once per request batch, the way the log parsers call
> `NotifyBatchEnqueued` — in `SpanIngestionEndpoint.EndBatch`, reached on the throwing path too, so a
> parser that fails mid-request still wakes the drainer for the prefix it enqueued (the reason the
> per-span release was kept, WP1 review note 9) — and keep a per-span signal only past a fill
> threshold. Re-measure with the A/B harness at 50k and 150k spans/s.

### Trace compaction: light spans are rewritten twice

After the 50 000/s load A's compaction run merges the 120 flushed segments in pairs, 120 → 60,
rewriting 6.0 M spans; B merges pairs of pairs, 120 → 31–32, rewriting 11.8 M, and spends a third more
CPU on the run (51.9 against 38.7 core-seconds) while allocating 18 % less.

B plans in bytes: a segment under half the 73 MB pass budget is a candidate, and a merged output
carries the weight measured as it was read (`.WithWeight(loadedBytes)`, `TraceStorageEngine.cs:4089`)
— 233 B a span (`ReadBackSpanOverheadBytes`, `:567`) plus its attribute blob. The k6 span carries
six attributes, a ~124-byte blob, so a 100 000-span output weighs ~35.7 MB, just under 36.5 MB, and
merges again into 200 000. A span with a blob over ~132 B (the calibration span's is 375 B) stops at
one rewrite, as A's span-count planner always did. `EstimatedSegmentBytes`' comment (`:586-595`)
says the byte planner admits and pairs exactly what the span planner did on a host with room for
the cap — true for a segment never weighed, not for the merged output of light spans. Half the
segments is what a trace lookup wants, so this may be the right trade, but it is a cliff at ~132 B.

> **Issue: Traces: on light spans the byte planner re-merges its own outputs — every span written
> twice by one compaction run.** After 6 M k6 spans (6 attributes, ~124-byte blobs) `main` @
> `d5ee138` compacts 120 segments into 31–32 by rewriting 11.8 M spans, where `0a7389d` made 60 by
> rewriting 6.0 M: +34 % CPU for the run (51.9 vs 38.7 core-s), 18 % less allocated. A merged
> output is weighed as read (233 B + blob a span), so 100 000 light spans weigh under the 36.5 MB
> candidate threshold and merge again; above a ~132-byte blob they do not. Decide whether a second rewrite is wanted (half the
> segments for a lookup to consult) and, if not, keep a run's own outputs out of its candidates or
> bound a candidate in spans as well as bytes. `CompactionThresholdTests`' segments carry no
> measured weight, so none of its facts sees this.

### TraceQL: a selective query is slower when warm

`{ .http.status_code = 500 }` with limit 100, over the same 6 M spans, returns the same 32 159
bytes on both sides. Its first run is alike (A 33–55 ms, B 40–67 ms); its warm median is not, A
26–34 ms against B 42–55 ms, in every round. The wide scan
(`{ .db.system = "mssql" && duration > 1s }`, which reads every span and matches none) is the
same on both sides, so this is not the per-span cost; where the warm time goes was not looked at.
Two B rounds: treat it as a lead.

> **Issue: TraceQL: a selective query answered from the newest spans is ~78 % slower warm on
> `main` than before #84.** After 6 M k6 spans, `{ .http.status_code = 500 }` limit 100 takes a
> warm median of 42–55 ms on `d5ee138` against 26–34 ms on `0a7389d` (first runs alike, the wide
> scan alike). Profile the warm path — the hot-tier walk, the promoted-status predicate, `BuildRow`
> — and re-measure on `main` after #122 and #128, which changed the hot-tier window and start index.

### Metrics (indicative): more gen2 collections and a slower histogram quantile

These come from the indicative pairs, so they are leads, not results. B's managed heap is less than
half of A's, and B collects gen2 half as often again (44–46 against 29–32 a round) for the same total
pause. The `quantile` 0.95 query over one histogram is 27–41 % slower, while the `rate` query is
faster warm. B's hot tier is budgeted in bytes (32 MB), and with this workload's histograms it
flushes more often than A's 500 000-point threshold: 760 metric segments after the round against
480, so a query over one metric opens more files until the rollup merges them (first run five
minutes after start, outside the window, not measured).

> **Issue: Metrics: with a histogram-heavy tier the byte budget flushes ~1.6× as often — more
> segments, a slower quantile query until the rollup.** On a quiet machine, repeat the 50k
> points/s pairs (`ab-compare.ps1 -Signal metrics`) and run `quantile` 0.95 over one histogram
> before and after the first rollup; if the gap closes after it, it is the segment count, else
> profile `AggregateQuantile` against `0a7389d`.

## What the numbers do not say

- **Clean: traces at 50 000 spans/s only — three A rounds, two B rounds.** The brackets are the
  whole spread; a difference inside them is not a difference. The metrics pairs and the
  higher-rate probes are indicative: the metrics rounds carry short foreign bursts, and each probe
  is one 30-s run per side from the first protocol. Their large differences (allocations, memory,
  drops, p95) are far outside anything noise did at 50 000/s; their small ones are not claimed.
- **OTLP JSON only.** Both k6 scripts post JSON. The protobuf path — what SDK exporters and the
  collector send, and what WP1 rewrote — is not exercised here; its gain is in WP1's probe numbers.
  On the metrics side the JSON request is still deserialised into an `ExportMetricsServiceRequest`
  graph on both A and B; the round changed what happens after it, not the JSON decode.
- **B is `main` as of 2026-10-08, not #84 alone.** It carries the follow-ups listed above, and some
  of them add work to the ingest path on purpose (#105: a CRC32C per metric WAL entry; #117: the
  span WAL's relocation record). What merged after it (#120, #121, #122, #126, #128, #129) is not
  measured here.
- **The window holds ingest and flush, not the hourly or five-minute work.** The fixed start puts
  the trace compaction run before the window and the metric rollup (first run 5 min after start)
  after it, on both sides. Trace compaction is measured on its own after a restart; the metric
  rollup is not measured at all. The first protocol started the load whenever the machine went
  quiet, the compaction run fell inside the window in some rounds and outside it in others, and
  the rounds of either side split into two modes (A: 12.2 against 14.2–15.1 µs/span; B: 13.0
  against 14.6). Those rounds were set aside, not averaged in.
- **`RamTargetPercent` was 99 on both sides.** The host sat at 81–89 % of its RAM from other
  applications, above the 75 % default, and without a cgroup that reading is the whole host's: at
  75 % the RAM-pressure check would have flushed and forced compacting collections on its own
  schedule — differently on the two sides, since B registers more shedders.
- **No run under a 384 MB heap limit.** The plan asks for one to reproduce the 512 MB stand. It was
  not done: this run was not to change GC settings, and on Windows without a cgroup the
  RAM-pressure check reads a heap hard limit as the host's load and flushes on every cycle, so
  the run would have measured that. What bears on the stand is the managed heap left after a
  collection: at 50 000 spans/s A's reached 195–207 MB and B's 93–104 MB; in a first-protocol
  round whose window also held the compaction run, A's reached 374 MB — 97 % of the stand's
  384 MB limit — and B's 250 MB. The stand itself still needs its own measurement.
- **The machine was shared.** ~10 Docker containers of an unrelated project (their WSL VM took
  about a fifth of a core), other agents' builds and test runs, and the user at the desk. The idle
  level before each round and the foreign CPU during it are in the tables, and the tally of rounds
  thrown away is under them. k6 ran on the same box (about half a core at 50 000/s). The
  i7-12700 mixes P- and E-cores, and where the scheduler puts the server's threads differs between
  rounds.
- **A 2-minute window.** Retention, hours of uptime, label churn and high cardinality (the metrics
  script sends 1 000 fixed series, so WP3's stale-series sweep and the label pool's resets are
  never exercised), exemplars (the script sends none) and many services (four) are not in it.

## How to reproduce

Windows, PowerShell 7, k6 v0.40 or later, `dotnet-counters` (`dotnet tool install -g dotnet-counters`),
a free port (18341 by default).

```powershell
# Both sides, each from its own tree, in Release
git worktree add --detach C:\tmp\perf\treeA 0a7389d
git worktree add --detach C:\tmp\perf\treeB d5ee138
dotnet build C:\tmp\perf\treeA\src\Ameto.Server\Ameto.Server.csproj -c Release
dotnet build C:\tmp\perf\treeB\src\Ameto.Server\Ameto.Server.csproj -c Release
$A = 'C:\tmp\perf\treeA\src\Ameto.Server\bin\Release\net10.0'
$B = 'C:\tmp\perf\treeB\src\Ameto.Server\bin\Release\net10.0'

# Interleaved A, B, A, B, A, B at the scripts' default rate, 120 s each; a round that overlapped
# foreign CPU or a host near its RAM limit is moved to <OutRoot>\discarded and run again
tools/loadtest/ab-compare.ps1 -A $A -B $B -OutRoot C:\tmp\perf\rounds -Signal traces  -Rate 50  -Pairs 3
tools/loadtest/ab-compare.ps1 -A $A -B $B -OutRoot C:\tmp\perf\rounds -Signal metrics -Rate 50  -Pairs 3
tools/loadtest/ab-compare.ps1 -A $A -B $B -OutRoot C:\tmp\perf\rounds -Signal traces  -Rate 150 -Pairs 1 -NoCompactionRun
tools/loadtest/ab-compare.ps1 -A $A -B $B -OutRoot C:\tmp\perf\rounds -Signal metrics -Rate 250 -Pairs 1

# The tables above, plus one line per round and the discarded rounds
tools/loadtest/ab-summary.ps1 -OutRoot C:\tmp\perf\rounds
```

Each round folder keeps `result.json`, the k6 summary, the per-second samples, the
`dotnet-counters` CSV, the `/api/diagnostics` snapshots, the queries' bodies and answers, and the
server's logs; the data directory itself is deleted at the end of the round.
