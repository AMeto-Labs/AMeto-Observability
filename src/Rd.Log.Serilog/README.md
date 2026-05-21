# Rd.Log.Serilog

Serilog sink for **Rd.Log** — ships events to an Rd.Log server using its native
MessagePack CLEF endpoint (`POST /api/events`).

## Install

```xml
<PackageReference Include="Rd.Log.Serilog" Version="0.1.0" />
```

## Usage

```csharp
using Rd.Log.Serilog;
using Serilog;
using Serilog.Core;
using Serilog.Sinks.SystemConsole.Themes;

var loggerConfiguration = new LoggerConfiguration()
    .MinimumLevel.Information()
    .ReadFrom.Configuration(configuration)
    .MinimumLevel.Override("System",                       SeqLogLevel.SystemLogLevel)
    .MinimumLevel.Override("Microsoft",                    SeqLogLevel.MicrosoftLogLevel)
    .MinimumLevel.Override("Microsoft.EntityFrameworkCore", SeqLogLevel.EfCoreLogLevel)
    .MinimumLevel.Override("MassTransit",                  SeqLogLevel.MasstransitLogLevel)
    .MinimumLevel.Override("Yarp",                         new LoggingLevelSwitch { MinimumLevel = Serilog.Events.LogEventLevel.Verbose })
    .MinimumLevel.Override(LogExtensions.LogCategoryName,  SeqLogLevel.LogCategoryLevel)
    .Enrich.WithProperty("ApplicationContext", appName)
    .Enrich.WithProperty("Environment", configuration["ASPNETCORE_ENVIRONMENT"])
    .Enrich.FromLogContext()

    .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
    .WriteTo.RdLog(
        serverUrl: string.IsNullOrWhiteSpace(seqServerUrl) ? "http://rdlog:5341" : seqServerUrl,
        apiKey:    configuration["RdLog:ApiKey"],
        levelSwitch: SeqLogLevel.Default)

    .Destructure.UsingAttributes()
    .Destructure.With<IgnoreFormFileDestructuringPolicy>();
```

## Options

| Parameter                  | Default              | Description                                              |
|----------------------------|----------------------|----------------------------------------------------------|
| `serverUrl`                | (required)           | Base URL, e.g. `http://localhost:5341`.                  |
| `apiKey`                   | `null`               | Sent in `X-Seq-ApiKey` header.                           |
| `batchSizeLimit`           | `1000`               | Max events per HTTP request.                             |
| `period`                   | `2s`                 | Flush interval.                                          |
| `queueLimit`               | `100_000`            | Drop threshold for in-memory queue.                      |
| `restrictedToMinimumLevel` | `Verbose`            | Below this level, events are ignored.                    |
| `levelSwitch`              | `null`               | Runtime level switch.                                    |
| `controlLevelSwitch`       | `null`               | Reserved — Seq-style server-controlled level switch.     |
| `httpClient`               | `null` (sink-owned)  | Inject a pre-configured `HttpClient` (e.g. with proxy).  |

## Wire format

The sink encodes each event as a MessagePack map of CLEF fields:

```
{ "@t": "...", "@mt": "...", "@l": "Information",
  "@x": { "type": "...", "message": "...", "stack": "...", "inner": {...} },
  "<Property>": <value>, ... }
```

and POSTs an array of these maps with `Content-Type: application/x-msgpack`.
The server responds with `200 OK { "ingested": N, "dropped": M }`.
