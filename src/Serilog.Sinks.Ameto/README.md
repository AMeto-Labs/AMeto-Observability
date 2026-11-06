# Serilog.Sinks.Ameto

Serilog sink for **Ameto** — ships events to an Ameto server using its native
MessagePack CLEF endpoint (`POST /api/events`). Seq-compatible API key header.

## Install

```xml
<PackageReference Include="Serilog.Sinks.Ameto" Version="1.1.0" />
```

## Usage

`serverUrl`, `apiKey` and `serviceName` are **required**; everything else is optional
tuning via an `Action<AmetoSinkOptions>` delegate:

```csharp
using Serilog;
using Serilog.Sinks.Ameto;

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    // required: serverUrl, apiKey, serviceName
    .WriteTo.Ameto("http://ameto:5341", "your-api-key", "orders-api", o =>
    {
        o.BatchSizeLimit = 2000;   // optional tuning
    })
    .CreateLogger();

// Minimal (defaults for everything else):
.WriteTo.Ameto("http://ameto:5341", "your-api-key", "orders-api")
```

### Profiles

Ready-made tuning presets (`Action<AmetoSinkOptions>`). Pass one directly, or compose
then override your own fields:

```csharp
// A preset as-is
.WriteTo.Ameto("http://ameto:5341", "your-api-key", "orders-api", AmetoSinkProfiles.HighThroughput)

// A preset + overrides (preset first, then your tweaks)
.WriteTo.Ameto("http://ameto:5341", "your-api-key", "gateway", o =>
{
    AmetoSinkProfiles.LowLatency(o);
    o.BatchSizeLimit = 100;
})
```

| Profile             | Batch / Period / Queue  | Use case                                        |
|---------------------|-------------------------|-------------------------------------------------|
| `Balanced` (default)| 1000 / 1s / 100k        | General production.                             |
| `HighThroughput`    | 5000 / 2s / 500k        | Max throughput, higher RAM.                     |
| `LowLatency`        | 50 / 250ms / 20k        | Dev/interactive, minimal delay.                 |
| `MemoryConstrained` | 500 / 1s / 10k          | Bound memory; drops sooner under back-pressure. |
| `Resilient`         | 1000 / 1s / 250k        | Buffer through short server outages (see [Delivery](#delivery) for how long). |

## Required arguments

| Argument      | Description                                                       |
|---------------|-------------------------------------------------------------------|
| `serverUrl`   | Base URL, e.g. `http://ameto:5341`.                               |
| `apiKey`      | Sent in the `X-Seq-ApiKey` header. **Required** (throws if empty).|
| `serviceName` | Written as `service.name` on every event. **Required** (throws if empty).|

## Options (`AmetoSinkOptions`) — optional tuning

| Property                   | Default             | Description                                                    |
|----------------------------|---------------------|----------------------------------------------------------------|
| `BatchSizeLimit`           | `1000`              | Max events per batch.                                          |
| `BatchBytesLimit`          | `3 MB`              | Max serialised bytes per HTTP request. A batch past it goes as several requests, each under the limit (an event larger than the limit on its own still goes, alone). Keep it under the server's `Ingestion.MaxBatchBytes` (4 MB default). `0` = one request per batch. |
| `Period`                   | `1s`                | Flush interval (also flushes early when a batch fills).        |
| `QueueLimit`               | `100_000`           | In-memory buffer cap; excess events are dropped (back-pressure).|
| `EagerlyEmitFirstEvent`    | `true`              | Emit the first event immediately instead of waiting a period.  |
| `RestrictedToMinimumLevel` | `LevelAlias.Minimum`| Static minimum level for this sink.                            |
| `LevelSwitch`              | `null`              | Runtime-adjustable level (overrides `RestrictedToMinimumLevel`).|
| `HttpClient`               | `null` (sink-owned) | Inject a pre-configured `HttpClient` (shared pool/proxy).      |
| `PropertiesLimitBytes`     | `65_536`            | Max serialised size of one event's **properties** — the map header and its pairs, what the server measures against its `Ingestion.MaxEventPayloadBytes` (64 KB default). An event over it (a runaway `{@Object}`) is replaced by an **Error** marker (below). `0` = unlimited. Was `EventBodyLimitBytes` before 1.1, which still works and sets this value — but measured more; see [Upgrading from 1.0](#upgrading-from-10). |
| `ExceptionLimitBytes`      | `262_144`           | About the most one event's exception (`@x`) takes serialised. Over it the type is kept, the message and stack trace are cut and inner exceptions dropped — the event itself still arrives. A target rather than a hard cap: the cut is by characters, and a stack trace full of non-ASCII names can land over it. `0` = unlimited. |

## Delivery

The sink answers the server's status codes the way the server means them
(`docs/API.md`, "POST /api/events"):

| Server says | The sink does |
|-------------|---------------|
| `200` | Done. A `dropped` count in the answer is written to `SelfLog`: the server drops events over its size limit (each leaves the server's own Error marker) and, when the store is short of room, the tail of a request. The sink cannot tell the two apart, so it resends neither. |
| `400` with `failedAtElement: K` | The K events before it are stored and stay so. Event K is dropped with a `SelfLog` line; the events after it are sent again. |
| `400` without `failedAtElement` | The server could not read the request at all; it is dropped, never retried. |
| `413` | The request is cut in half and sent again, down to one event. One event the server still refuses is replaced by an **Error** marker, which takes its place in the next request; the rest of the batch goes on in full requests. |
| `429`, `503` | Nothing was stored. One retry after `Retry-After` (1 s when absent, 10 s at most), then the batch is handed back to Serilog's batching layer (below). |
| `408`, `500`, `502`, `504`, transport failure | Handed to the batching layer at once. |
| `401`, `403`, `404`, other `4xx` | Configuration, which no retry fixes: the batch is dropped with a `SelfLog` line. |

The batching layer retries a batch it was handed with back-off — one period, then 10 s doubling to
1 min — for its retry time limit of 10 minutes. The 10 minutes count from the first failure since the
last success, not per batch: in a longer outage every later batch gets one attempt and is dropped, and
after ten dropped batches in a row the queue is emptied, whatever `QueueLimit` is.

A batch the batching layer retries **resumes** past the requests the server has answered, so those are
never sent twice. A request whose answer never arrived — a timeout, or a connection dropped after the
server wrote it — is sent again and can be stored twice: the server does not de-duplicate.

Failures are reported through [`SelfLog`](https://github.com/serilog/serilog/wiki/Debugging-and-Diagnostics)
(`Serilog.Debugging.SelfLog.Enable(Console.Error)`), one line per dropped event or request, and one
per answer that counts drops.

### Markers

An event the sink cannot send as it is leaves an **Error**-level event in its place, with the
original's timestamp, trace context and `service.name`, searchable in Ameto by `DroppedBy = 'sink'`
or `OriginalTemplate`:

| Why | `@mt` | Properties |
|-----|-------|------------|
| Properties over `PropertiesLimitBytes` | `Log event properties of {EventBodyBytes} bytes exceeded the sink limit of {EventBodyLimitBytes} bytes; the event was dropped: {OriginalTemplate}` | `DroppedBy`, `EventBodyBytes`, `EventBodyLimitBytes`, `OriginalTemplate`, `OriginalLevel` |
| The server refused the event alone with `413` | `Log event of {EventBodyBytes} bytes was refused by the server as too large; the event was dropped: {OriginalTemplate}` | `DroppedBy`, `EventBodyBytes`, `OriginalTemplate`, `OriginalLevel` |
| Formatting threw (a property whose `ToString()` fails) | `Log event could not be serialised by the Ameto sink ({SinkError}); the event was dropped: {OriginalTemplate}` | `DroppedBy`, `SinkError`, `OriginalTemplate`, `OriginalLevel` |

The field names are the server's own drop marker's (`DroppedBy = 'server'`), so one filter finds both.

## Upgrading from 1.0

- **`EventBodyLimitBytes` measures less than it did.** The name still compiles (an obsolete warning,
  `AMETOSINK001`) and sets `PropertiesLimitBytes` — but 1.0 measured the whole serialised event,
  exception included, while 1.1 measures the properties map alone, as the server does, and gives the
  exception its own `ExceptionLimitBytes` (256 KB by default). A 1.0 setting meant to bound whole
  events (`EventBodyLimitBytes = 16_384`, say) no longer bounds the exception: set `ExceptionLimitBytes`
  too. A 1.0 setting of `0` ("no limit") now leaves exceptions cut at 256 KB: set `ExceptionLimitBytes = 0`
  as well.
- **A non-2xx is no longer a dropped batch.** 1.0 dropped the batch on any non-2xx with a `SelfLog`
  line; 1.1 answers each status as the [Delivery](#delivery) table says. A `413` is split, a `429`,
  `503`, `408` or `5xx` is retried through the batching layer, so a server outage now holds events in
  the queue (up to `QueueLimit`, for the retry time above) instead of dropping them at once.
- **One request is at most `BatchBytesLimit` (3 MB).** A batch that serialises past it goes as several
  requests; 1.0 sent every batch as one.

## Wire format

Each event is encoded as a MessagePack map of CLEF fields:

```
{ "@t": "<ISO-8601>", "@mt": "<template>", "@l": "Information",
  "@tr": "<traceId>", "@sp": "<spanId>", "service.name": "...",
  "@x": { "type": "...", "msg": "...", "stk": "...", "inner": { ... } },
  "<Property>": <value>, ... }
```

An array of these maps is POSTed with `Content-Type: application/x-msgpack`.
The server responds `200 OK { "ingested": N, "dropped": M }`.
