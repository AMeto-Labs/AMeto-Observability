# loggen — log-load generator (native MessagePack wire format)

Generates realistic structured log events and sends them to AMeto over the
server's **native ingest format**: a MessagePack array of CLEF maps POSTed to
`POST /api/events` (the same binary path `Serilog.Sinks.Ameto` uses) — no JSON
anywhere on the wire. Prints a full statistics summary at the end of the run.

Unlike the k6 scripts in [`tools/loadtest`](../loadtest/README.md) (raw
throughput ceilings, one template patched per iteration), loggen aims for
**realistic content**: 13 weighted scenarios (HTTP requests, payments, DB
commands, queue consumers, cache misses, retries, failures), a production-like
level mix, per-event properties that match the template placeholders, nested
map properties, structured exceptions (`@x` with inner + stack trace) on
errors, and `@tr`/`@sp` trace correlation on a share of events.

## Run

```bash
# 10k events/s for 60 s (defaults)
dotnet run --project tools/loggen -c Release -- --key <api-key>

# custom target / rate / duration
dotnet run --project tools/loggen -c Release -- \
    --key <api-key> --url http://host:5341 --rate 50000 --duration 120
```

| Option | Default | Meaning |
|---|---|---|
| `--key` (or env `AMETO_API_KEY`) | — | ingest API key with the Logs permission (**Settings → API Keys**) |
| `--url` (or env `AMETO_URL`) | `http://localhost:5341` | server base URL |
| `--rate` | `10000` | offered events/second |
| `--duration` | `60` | seconds |
| `--batch` | `1000` | events per request |
| `--concurrency` | `4` | max in-flight requests |
| `--seed` | random | RNG seed for reproducible runs |

## Statistics

The summary reports both sides of the pipe — what was generated and what the
server acknowledged (`{"ingested":N,"dropped":M}` per batch):

```
── loggen summary ─────────────────────────────────────────────
target       10,000 ev/s · batch 1000 · concurrency 4
sent         600,000 events in 600 batches · 87.1 MB msgpack (152 B/event)
duration     60.02 s  →  9,997 ev/s achieved
server       ingested 600,000 · dropped 0 (0.000 %)
http         600 ok · 0 failed
latency      p50 2.1 ms · p90 3.0 · p95 3.4 · p99 5.9 · max 12.4 ms
levels       V 4.0% · D 22.1% · I 58.0% · W 9.9% · E 5.0% · F 1.0%
extras       exceptions 35,821 · traced 210,004 · services 5
───────────────────────────────────────────────────────────────
```

The project references `Ameto.Core`, so the wire format (field names,
`ExceptionInfo` shape, level strings) can never drift from what the server
parses.
