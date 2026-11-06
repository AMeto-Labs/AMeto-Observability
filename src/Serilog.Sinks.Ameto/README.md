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
| `Resilient`         | 1000 / 1s / 250k        | Buffer through transient server outages.        |

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
| `PropertiesLimitBytes`     | `65_536`            | Max serialised size of one event's **properties** — what the server measures against its `Ingestion.MaxEventPayloadBytes` (64 KB default). An event over it (a runaway `{@Object}`) is replaced by an **Error** marker (below). `0` = unlimited. Was `EventBodyLimitBytes` before 1.1, which still works and sets the same value. |
| `ExceptionLimitBytes`      | `262_144`           | Max serialised size of one event's exception (`@x`). Over it the type is kept, the message and stack trace are cut and inner exceptions dropped — the event itself still arrives. `0` = unlimited. |

## Delivery

The sink answers the server's status codes the way the server means them
(`docs/API.md`, "POST /api/events"):

| Server says | The sink does |
|-------------|---------------|
| `200` | Done. |
| `400` with `failedAtElement: K` | The K events before it are stored and stay so. Event K is dropped with a `SelfLog` line; the events after it are sent again. |
| `400` without `failedAtElement` | The server could not read the request at all; it is dropped, never retried. |
| `413` | The request is cut in half and sent again, down to one event. One event the server still refuses is replaced by an **Error** marker. |
| `429`, `503` | Nothing was stored. One retry after `Retry-After` (1 s when absent, 10 s at most), then the batch is handed back to Serilog's batching layer, which retries it with back-off for up to its retry time limit (10 minutes by default). |
| `500`, `502`, `504`, transport failure | Handed to the batching layer at once. |
| `401`, `403`, `404`, other `4xx` | Configuration, which no retry fixes: the batch is dropped with a `SelfLog` line. |

A batch the batching layer retries **resumes** past the requests that already went through, so
the server — which does not de-duplicate — never stores an event twice because of a retry.

Failures are reported through [`SelfLog`](https://github.com/serilog/serilog/wiki/Debugging-and-Diagnostics)
(`Serilog.Debugging.SelfLog.Enable(Console.Error)`), one line per dropped event or request.

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

## Wire format

Each event is encoded as a MessagePack map of CLEF fields:

```
{ "@t": "<ISO-8601>", "@mt": "<template>", "@l": "Information",
  "@x": { "type": "...", "msg": "...", "stk": "...", "inner": { ... } },
  "@tr": "<traceId>", "@sp": "<spanId>", "service.name": "...",
  "<Property>": <value>, ... }
```

An array of these maps is POSTed with `Content-Type: application/x-msgpack`.
The server responds `200 OK { "ingested": N, "dropped": M }`.
