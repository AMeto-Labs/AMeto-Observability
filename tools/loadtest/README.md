# Ingest load test (k6)

Drives OTLP JSON logs into `POST /otlp/v1/logs` at a constant offered rate.
The batch body is built once at init and only the timestamp is patched per
iteration, so k6 itself stays cheap even at 100k records/s.

## Run

```bash
# 100k logs/s for 60 s (defaults: RATE=100 req/s × BATCH=1000 records)
k6 run -e AMETO_API_KEY=<key> tools/loadtest/k6-100k.js

# Custom target / rate / duration
k6 run -e AMETO_API_KEY=<key> -e AMETO_URL=http://host:8555 \
       -e RATE=50 -e BATCH=1000 -e DURATION=120s tools/loadtest/k6-100k.js
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
