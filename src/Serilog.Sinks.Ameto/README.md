# Serilog.Sinks.Ameto

Serilog sink for **Ameto** — ships events to an Ameto server using its native
MessagePack CLEF endpoint (`POST /api/events`). Seq-compatible API key header.

## Install

```xml
<PackageReference Include="Serilog.Sinks.Ameto" Version="1.0.0" />
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
| `BatchSizeLimit`           | `1000`              | Max events per HTTP request.                                   |
| `Period`                   | `1s`                | Flush interval (also flushes early when a batch fills).        |
| `QueueLimit`               | `100_000`           | In-memory buffer cap; excess events are dropped (back-pressure).|
| `EagerlyEmitFirstEvent`    | `true`              | Emit the first event immediately instead of waiting a period.  |
| `RestrictedToMinimumLevel` | `LevelAlias.Minimum`| Static minimum level for this sink.                            |
| `LevelSwitch`              | `null`              | Runtime-adjustable level (overrides `RestrictedToMinimumLevel`).|
| `HttpClient`               | `null` (sink-owned) | Inject a pre-configured `HttpClient` (shared pool/proxy).      |
| `EventBodyLimitBytes`      | `65_536`            | Max serialised size of one event. An oversized event (e.g. a runaway `{@Object}`) is replaced by a compact **Error** marker carrying `OriginalTemplate`, `OriginalLevel` and the measured size — visible and searchable in Ameto instead of being silently rejected by the server's payload cap. `0` = unlimited. |

> An explicit-parameter overload (`Ameto(serverUrl, apiKey:, serviceName:, …)`) is also
> available for simple cases and delegates to the same builder.

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
