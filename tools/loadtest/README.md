# Ingest load tests (k6)

One script per signal, all driving the OTLP JSON endpoints at a constant offered
rate. Batch bodies are built once at init and only timestamps (and, for traces,
an iteration id baked into every trace/span id) are patched per iteration, so k6
itself stays cheap even at 100k records/s.

| Script | Endpoint | Default offered load |
|---|---|---|
| `k6-100k.js` | `/otlp/v1/logs` | 100 req/s × 1000 records = **100k logs/s** |
| `k6-traces.js` | `/otlp/v1/traces` | 50 req/s × 1000 spans = **50k spans/s** (5 spans/trace, root+children) |
| `k6-metrics.js` | `/otlp/v1/metrics` | 50 req/s × 1000 points = **50k points/s** (1000 series: 15 counters + 15 gauges + 10 histograms × 25 label combos) |

## Run

```bash
# Logs: 100k/s for 60 s
k6 run -e AMETO_API_KEY=<key> tools/loadtest/k6-100k.js

# Traces / metrics, same knobs
k6 run -e AMETO_API_KEY=<key> tools/loadtest/k6-traces.js
k6 run -e AMETO_API_KEY=<key> tools/loadtest/k6-metrics.js

# Custom target / rate / duration (any script)
k6 run -e AMETO_API_KEY=<key> -e AMETO_URL=http://host:8555 \
       -e RATE=100 -e DURATION=120s tools/loadtest/k6-traces.js
```

Create the API key under **Settings → API Keys** (needs the Logs permission).
Note: ingest auth uses the Seq-compatible **`X-Seq-ApiKey`** header.

The k6 summary reports the server's own per-batch accounting:
`ameto_ingested` / `ameto_dropped` (summed from each `{"ingested":N,"dropped":M}`
response), alongside the usual latency percentiles.

## Measured: v1.0.9, 2026-07-17

Fresh Windows install (installer defaults, 20-core dev box), k6 co-located on
the same machine, offered exactly **100 000 logs/s for 60 s**:

| Metric | Result |
|---|---|
| Ingested | **5 912 118 events = 98 463/s (98.5 %)** |
| Dropped | 87 882 (**1.46 %**, ring back-pressure during flush peaks) |
| HTTP failures | 0 / 6 000 requests |
| Batch latency (1000 records) | med 2.15 ms · p95 4.5 ms · max 122 ms |
| k6 | held the rate exactly; 64 VUs sufficed; 0 dropped iterations |

Server side during the run:

| Metric | Result |
|---|---|
| Stored | 5 869 778 events → 12 segments, 538 MB compressed (rest still in hot tier) |
| RSS | oscillated 0.8–1.7 GB with the flush cadence; settled ~0.77 GB after |
| GC | 62 gen2 collections total; managed heap ~240 MB |
| Disk burn | ≈ 0.5 GB/min compressed at this rate |

For context: v1.0.8's durable ceiling on the same box was ~76k/s — the v1.0.9
performance work (candidate-driven segment reads, header-level hot-tier scan,
pooled index sections, streaming OTLP parsers, WAL fixes) moved it.

## Measured: post-1.0.9 dev (drop hunt), 2026-07-18

Same box and methodology, chasing the remaining 1.46 % — each fix measured with
a full 100k/s × 60 s run:

| Build | Dropped |
|---|---|
| v1.0.9 baseline | 87 882 (1.46 %) |
| + SegmentWriter per-block scratch reuse (42 MB → 483 KB alloc per flushed tier) | 60 532 (1.01 %) |
| + WAL dispose moved off the swap lock, ring 16k → 64k slots | 42 486 (0.71 %) |
| + payload slab pool 128 MB → 512 MB (the root cause: 2048 slabs ≈ 20 ms of absorption) | **0 (0.00 %)** |

Final run: **6 001 000 / 6 001 000 ingested (99 947/s), zero drops**, batch
p95 5.2 ms, max 74 ms, RSS steady ~1 GB. The drop threshold was never the ring
slot count but the payload slab pool — see `Ingestion.RingCapacity` /
`Ingestion.PayloadPoolBytes` in [CONFIGURATION.md](../../docs/CONFIGURATION.md).

## Measured: v1.0.10 (installed service), 2026-07-18

Validation on the real Windows-service install (same box, same methodology):

| Run | Ingested | Dropped | Batch latency |
|---|---|---|---|
| 100k/s × 60 s, cold (right after install) | 5 995 330 (99.89 %) | 6 670 (0.11 %) | p95 5.2 ms · max 175 ms |
| 100k/s × 60 s, warm | **6 001 000 / 6 001 000** | **0 (0.00 %)** | p95 5.4 ms · max 34 ms |
| **150k/s × 60 s** (`-e RATE=150`) | **8 998 479 (149 862/s, 99.97 %)** | 2 521 (0.028 %) | p95 4.0 ms · max 23 ms |

The cold-run residue is start-up effects (JIT, WAL recovery, retention scan) —
a warm service sustains the full offered rate with zero loss, and takes 150k/s
with 0.03 % drops and a steady ~1.1 GB RSS.

### Ceiling ladder (logs, v1.0.10, 60 s per step)

| Offered | Ingested | Dropped | RSS |
|---|---|---|---|
| 100k/s | 100 % | **0 %** | ~1.0 GB |
| 150k/s | 149 862/s | 0.028 % | ~1.1 GB |
| 175k/s | 174 401/s | 0.29 % | ~1.5 GB |
| 200k/s | 198 286/s | 0.76 % | ~2.0 GB |
| 250k/s | 246 070/s | 1.48 % | ~1.9 GB |
| 500k/s | **456 570/s** | 7.97 % | ~3.5 GB (budget-capped) |

Degradation is graceful — no cliff: past ~150k/s the flush pipeline (parallel
index builds) becomes the wall and the back-pressure budget trades drops for
bounded RAM. Practical zero-drop ceiling on this box: **~150k logs/s**; peak
sustained acceptance under 5x overload: **~456k logs/s**. (k6 itself saturates
near 500k/s co-located, so higher offered rates need a separate load box.)

## Measured: traces & metrics separately (v1.0.10 installed service), 2026-07-18

Each signal driven on its own, 60 s per run, same box/methodology:

| Run | Ingested | Dropped | Batch latency | RSS |
|---|---|---|---|---|
| Traces 50k spans/s | 3 000 000 / 3 000 000 | **0** | p95 20.2 ms · max 101 ms | ~230 MB |
| **Traces 100k spans/s** | **6 001 000 / 6 001 000** | **0** | p95 21.3 ms · max 68 ms | ~240 MB |
| Metrics 50k points/s | 3 001 000 / 3 001 000 | **0** | p95 8.3 ms · max 34 ms | ~300 MB |
| **Metrics 100k points/s** | **6 001 000 / 6 001 000** | **0** | p95 9.8 ms · max 55 ms | ~280 MB |

Traces go through the streaming OTLP parser (spans carry 6 attributes each,
promoted http status, parent links); metrics cover 1000 distinct series
including 15-bucket histograms. Neither signal shows drops up to 100k/s —
the log pipeline's flush machinery remains the only path that ever needed
back-pressure tuning.

### Ceiling ladder (traces, v1.0.10, 60 s per step)

| Offered | Ingested | Dropped | p95 | RSS |
|---|---|---|---|---|
| 100k spans/s | 100 % | **0 %** | 21.3 ms | ~240 MB |
| 125k spans/s | 7 500 000 / 7 500 000 | **0 %** | 25.5 ms | ~350 MB |
| 150k spans/s | 148 363/s | 1.03 % | 28.5 ms | ~360 MB |
| 200k spans/s ² | 116 560/s | 41.6 % | 58.3 ms | ~400 MB |
| 250k spans/s ² | 112 146/s | 55.1 % | 63.5 ms | ~440 MB |

² 2000-span batches (100 / 125 req/s).

Zero-drop ceiling: **~125k spans/s**. Unlike logs, traces degrade hard past it:
under 1.6–2x overload accepted throughput *sags* to ~112–117k/s instead of
holding the peak — the span drainer + trace storage path saturates and the
ring sheds the excess. RAM stays low throughout (back-pressure is doing its
job); the wall is CPU on the span path, not memory.

### Ceiling ladder (metrics, v1.0.10, 60 s per step)

| Offered | Ingested | Dropped | p95 | RSS |
|---|---|---|---|---|
| 100k pts/s | 100 % | **0 %** | 9.8 ms | ~280 MB |
| 150k pts/s | 9 000 000 / 9 000 000 | **0 %** | 11.3 ms | ~370 MB |
| 200k pts/s | 12 001 000 / 12 001 000 | **0 %** | 15.6 ms | ~330 MB |
| 250k pts/s | 15 002 000 / 15 002 000 | **0 %** | 53.7 ms | ~500 MB |
| 300k pts/s ³ | 235 491/s delivered | **0 %** | **1.22 s** | → 4.7 GB |

³ At 300k offered the server still drops nothing — overload shows up as
request queueing instead: batch p95 explodes to ~1.2 s, queued batches balloon
RSS to ~4.7 GB (all reclaimable — the next GC returned it to ~0.4 GB), and k6
itself runs out of VUs, delivering ~235k/s. Note the queueing collapse:
250k offered cleanly ingests 249.7k/s, while 300k offered delivers *less*.

Zero-drop ceiling: **≥250k points/s** — metrics aggregate into bounded series,
so the pipeline never sheds data; past ~250k/s the cost is latency and
transient RAM, not loss. Practical guidance: keep sustained metric load
≤250k points/s per node to stay in the low-latency regime.
