# Observability Platform — Plan (Logs + Traces + Metrics)

## Принципы

- **OTLP** как единый протокол приёма (OpenTelemetry Protocol) — стандарт де-факто
- Каждый сигнал хранится отдельно, но с общими примитивами (сегменты, индексы)
- Максимальное переиспользование существующего кода (Storage, Indexing, Query)

---

## Phase 1 — OTLP-приём

**Новый проект `Rd.Log.Otel`**

```
POST /v1/traces   → ExportTraceServiceRequest   (protobuf)
POST /v1/metrics  → ExportMetricsServiceRequest  (protobuf)
POST /v1/logs     → ExportLogsServiceRequest     (protobuf)  ← OTLP-альтернатива /api/events
```

- Подключить `OpenTelemetry.Proto` NuGet (только контракты, без SDK)
- Конвертеры: `OtlpSpan` → `SpanEvent`, `OtlpMetric` → `MetricPoint`
- Отдельные `ISpanIngester`, `IMetricIngester` (аналог текущего ring buffer)
- Текущий `POST /api/events` остаётся — обратная совместимость с Serilog sink

---

## Phase 2 — Traces

**Новый проект `Rd.Log.Traces`**

### Модель данных

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

### Хранение — `.trc` сегменты (аналог `.seg`)

- Те же 6-колоночных блоков: `@l` → `StatusCode`, `@mt` → span name, etc.
- TraceId → inverted index → быстрый lookup `traceId = 'abc...'`
- Trigram index на span name → `contains(name, 'payment')`
- Индекс по duration → медленные спаны `duration > 500`

### API

```
GET /api/traces?filter=...&from=...&to=...
GET /api/traces/{traceId}   ← все спаны трейса (waterfall)
GET /api/services           ← список сервисов
```

---

## Phase 3 — Metrics

**Новый проект `Rd.Log.Metrics`**

### Модель данных

```csharp
public readonly struct MetricPoint
{
    public long   TimestampTicks;
    public double Value;
    // для histogram: count + sum + buckets хранятся в attributes
}

public sealed class MetricSeries
{
    public string Name;           // "http.server.duration"
    public MetricType Type;       // Counter | Gauge | Histogram | Summary
    public string Unit;           // "ms", "bytes", "1"
    public Dictionary<string, string> Labels; // {"service":"api","method":"GET"}
}
```

### Хранение — `.met` сегменты

- Time-series ориентированные — дельта-кодирование по времени
- Отдельный файл-каталог серий `series.idx` (name + labels → seriesId)
- Downsampling: raw 15s → 1min → 5min → 1h (background job)

### API

```
GET /api/metrics?name=http.server.duration&from=...&to=...&step=60s
GET /api/metrics/names          ← список всех метрик
GET /api/metrics/labels?name=.. ← доступные label values
GET /metrics                    ← Prometheus scrape (text/plain)
```

---

## Phase 4 — Корреляция

Самая ценная часть — связать три сигнала:

| Связь | Механизм |
|---|---|
| Log → Trace | `traceId` / `spanId` в properties лога (пишет OpenTelemetry SDK автоматически) |
| Trace → Logs | `GET /api/events?filter=TraceId='abc'` — работает уже сейчас |
| Trace → Metrics | по `service.name` + временному окну |
| Metrics → Logs | клик на аномалию → открывает логи того же сервиса в то же время |

`FilterEvaluator` уже понимает `TraceId = '...'` через properties — изменений не требует.

---

## Phase 5 — UI

| Страница | Статус |
|---|---|
| Logs | ✅ есть |
| Traces | 🆕 список трейсов + waterfall view |
| Metrics | 🆕 time-series графики |

Waterfall view для трейсов — отдельный сложный Angular-компонент.

---

## Итоговая структура проектов

```
src/
  Rd.Log.Core/         ← добавить SpanEvent, MetricPoint, MetricSeries
  Rd.Log.Otel/         ← NEW: OTLP HTTP endpoints + конвертеры
  Rd.Log.Traces/       ← NEW: SpanIngester, TracesStorage, TracesQuery
  Rd.Log.Metrics/      ← NEW: MetricIngester, MetricStorage, MetricQuery
  Rd.Log.Ingestion/    ← без изменений (logs only)
  Rd.Log.Storage/      ← переиспользуется traces/metrics
  Rd.Log.Indexing/     ← переиспользуется для .trc/.met
  Rd.Log.Query/        ← минимальные расширения для traces
  Rd.Log.Server/       ← добавить новые endpoints
```

---

## Приоритеты

| Приоритет | Фаза | Ценность |
|---|---|---|
| 1 | Traces | Distributed tracing, waterfall, поиск по traceId |
| 2 | OTLP ingestion | Любой OpenTelemetry SDK начинает слать данные без доп. кода |
| 3 | Корреляция Log↔Trace | Почти бесплатна — traceId уже в properties |
| 4 | Metrics | Самое сложное (TSDB, downsampling); менее приоритетно при наличии Prometheus |
