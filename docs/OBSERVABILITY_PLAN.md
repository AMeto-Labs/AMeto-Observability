# Observability Platform — Design (Logs + Traces + Metrics)

> **Status: IMPLEMENTED.** This started as a plan; all phases below now ship. It is kept as
> the architecture overview. A few names/paths drifted during implementation — the real ones:
> traces live in **`Ameto.Tracing`** (not `Ameto.Traces`); OTLP is at **`/otlp/v1/{logs,traces,metrics}`**
> (not `/v1/...`); the services list is **`GET /api/events/services`**; metric time-series is
> **`GET /api/metrics/query`**. See [API.md](API.md) for the current endpoint reference.

## Principles

- **OTLP** as the single ingestion protocol (OpenTelemetry Protocol) — the de facto standard
- Each signal is stored separately, but on shared primitives (segments, indexes)
- Maximum reuse of existing code (Storage, Indexing, Query)

---

## Phase 1 — OTLP ingestion

**New project `Ameto.Otel`**

```
POST /v1/traces   → ExportTraceServiceRequest   (protobuf)
POST /v1/metrics  → ExportMetricsServiceRequest  (protobuf)
POST /v1/logs     → ExportLogsServiceRequest     (protobuf)  ← OTLP alternative to /api/events
```

- Add the `OpenTelemetry.Proto` NuGet package (contracts only, no SDK)
- Converters: `OtlpSpan` → `SpanEvent`, `OtlpMetric` → `MetricPoint`
- Separate `ISpanIngester`, `IMetricIngester` (mirroring the current ring buffer)
- The current `POST /api/events` stays — backward compatible with the Serilog sink

---

## Phase 2 — Traces

**New project `Ameto.Traces`**

### Data model

```csharp
public readonly struct SpanEvent
{
    public UInt128  TraceId;          // 16 bytes
    public ulong    SpanId;           // 8 bytes
    public ulong    ParentSpanId;     // 0 = root span
    public long     StartTimeTicks;
    public long     EndTimeTicks;     // duration = End - Start
    public byte     StatusCode;       // Ok / Error / Unset
    public int      ServiceNameOffset;
    public int      NameOffset;
    public int      AttributesOffset; // msgpack map
}
```

### Storage — `.trc` segments (analogous to `.seg`)

- The same 6-column blocks: `@l` → `StatusCode`, `@mt` → span name, etc.
- TraceId → inverted index → fast lookup `traceId = 'abc...'`
- Trigram index on span name → `contains(name, 'payment')`
- Index on duration → slow spans `duration > 500`

### API

```
GET /api/traces?filter=...&from=...&to=...
GET /api/traces/{traceId}   ← all spans of a trace (waterfall)
GET /api/services           ← list of services
```

---

## Phase 3 — Metrics

**New project `Ameto.Metrics`**

### Data model

```csharp
public readonly struct MetricPoint
{
    public long   TimestampTicks;
    public double Value;
    // for histograms: count + sum + buckets are stored in attributes
}

public sealed class MetricSeries
{
    public string Name;           // "http.server.duration"
    public MetricType Type;       // Counter | Gauge | Histogram | Summary
    public string Unit;           // "ms", "bytes", "1"
    public Dictionary<string, string> Labels; // {"service":"api","method":"GET"}
}
```

### Storage — `.met` segments

- Time-series oriented — delta encoding over time
- A separate series catalog file `series.idx` (name + labels → seriesId)
- Downsampling: raw 15s → 1min → 5min → 1h (background job)

### API

```
GET /api/metrics?name=http.server.duration&from=...&to=...&step=60s
GET /api/metrics/names          ← list of all metrics
GET /api/metrics/labels?name=.. ← available label values
GET /metrics                    ← Prometheus scrape (text/plain)
```

---

## Phase 4 — Correlation

The most valuable part — linking the three signals together:

| Link | Mechanism |
|---|---|
| Log → Trace | `traceId` / `spanId` in the log's properties (written automatically by the OpenTelemetry SDK) |
| Trace → Logs | `GET /api/events?filter=TraceId='abc'` — already works today |
| Trace → Metrics | by `service.name` + time window |
| Metrics → Logs | click an anomaly → opens logs for the same service at the same time |

`FilterEvaluator` already understands `TraceId = '...'` via properties — no changes needed.

---

## Phase 5 — UI

| Page | Status |
|---|---|
| Logs | ✅ done |
| Traces | ✅ trace list, waterfall, flamegraph, service graph, latency |
| Metrics | ✅ time-series charts, heatmap, exemplars |

---

## Final project structure

```
src/
  Ameto.Core/         ← SpanEvent, MetricPoint, MetricSeries
  Ameto.Otel/         ← OTLP HTTP endpoints + converters (+ zero-alloc streaming log parser)
  Ameto.Tracing/      ← SpanIngester, trace storage (.trc), trace query
  Ameto.Metrics/      ← MetricIngester, metric storage, metric query
  Ameto.Ingestion/    ← logs ingest (ring buffer, drainer)
  Ameto.Storage/      ← reused for traces/metrics
  Ameto.Indexing/     ← reused for .trc/.met (posting-list codec)
  Ameto.Query/        ← Seq filter parser/evaluator + query executor
  Ameto.Server/       ← Kestrel host, all endpoints
```

---

## Priorities

| Priority | Phase | Value |
|---|---|---|
| 1 | Traces | Distributed tracing, waterfall, search by traceId |
| 2 | OTLP ingestion | Any OpenTelemetry SDK can start sending data with no extra code |
| 3 | Log↔Trace correlation | Almost free — traceId is already in properties |
| 4 | Metrics | The hardest (TSDB, downsampling); lower priority when Prometheus is available |
