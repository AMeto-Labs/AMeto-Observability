using OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Text.Json;

Directory.CreateDirectory("logs");

var builder = WebApplication.CreateSlimBuilder(args);

string otlpBase = builder.Configuration["Otlp:Endpoint"] ?? "http://localhost:4318";

// Shared resource — same service identity across logs, traces, metrics
var resource = ResourceBuilder.CreateDefault()
    .AddService("Rd.Log.OtlpDemo", serviceVersion: "1.0.0");

// ── Logging: console + plain-text file + OTLP logs ──────────────────────────
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddProvider(new FileLoggerProvider("logs/demo.log"));
builder.Logging.AddOpenTelemetry(otlp =>
{
    otlp.SetResourceBuilder(resource);
    otlp.IncludeScopes           = true;
    otlp.IncludeFormattedMessage = true;
    // OTel SDK appends /v1/logs automatically for HTTP/Protobuf
    otlp.AddOtlpExporter(o =>
    {
        o.Endpoint = new Uri(otlpBase);
        o.Protocol = OtlpExportProtocol.HttpProtobuf;
    });
});

// ── OTLP traces + metrics ────────────────────────────────────────────────────
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("Rd.Log.OtlpDemo", serviceVersion: "1.0.0"))
    .WithTracing(t => t
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter(o =>
        {
            o.Endpoint = new Uri(otlpBase);
            o.Protocol = OtlpExportProtocol.HttpProtobuf;
        }))
    .WithMetrics(m => m
        .AddRuntimeInstrumentation()           // dotnet.*, gc.*, thread.*
        .AddAspNetCoreInstrumentation()        // http.server.request.duration etc.
        .AddMeter(LogGeneratorService.MeterName) // demo.logs.generated / demo.logs.errors
        .AddOtlpExporter(o =>
        {
            o.Endpoint = new Uri(otlpBase);
            o.Protocol = OtlpExportProtocol.HttpProtobuf;
        }));

// Use source-gen JSON for all minimal-API responses
builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonCtx.Default));

// IHttpClientFactory (used by SeqSignalSender) — avoids socket exhaustion
builder.Services.AddHttpClient();
builder.Services.AddSingleton<SeqSignalSender>();
builder.Services.AddSingleton<LogStore>();
builder.Services.AddHostedService<LogGeneratorService>();

// ── Seq event replay ─────────────────────────────────────────────────────────
var seqDataPath = builder.Configuration["SeqReplay:DataPath"]
    ?? Path.Combine("data", "response.json");
builder.Services.AddSingleton(_ => new SeqEventStore(seqDataPath));
builder.Services.AddHostedService<SeqReplayService>();

var app = builder.Build();

// GET /logs — returns all in-memory entries
app.MapGet("/logs", (LogStore store) =>
    Results.Ok(store.GetAll()));

// GET /logs/count
app.MapGet("/logs/count", (LogStore store) =>
    Results.Ok(new { count = store.Count }));

// POST /signal — manually trigger a random entry → Seq signal
app.MapPost("/signal", async (LogStore store, SeqSignalSender sender) =>
{
    var all = store.GetAll();
    if (all.Count == 0)
        return Results.NotFound(new { error = "No log entries yet — wait a moment." });

    var entry = all[Random.Shared.Next(all.Count)];
    await sender.SendAsync(entry);
    return Results.Accepted(value: entry);
});

// POST /logs/ingest — receive a log entry from curl, re-emit through OTel → OTLP
app.MapPost("/logs/ingest", (IngestRequest req, LogStore store, ILoggerFactory logFactory) =>
{
    var level = req.Level switch
    {
        "Error"             => LogLevel.Error,
        "Warning" or "Warn" => LogLevel.Warning,
        "Debug"             => LogLevel.Debug,
        _                   => LogLevel.Information,
    };

    // Convert JsonElement values → CLR types that OTel emits as typed attributes
    Dictionary<string, object?> scope = req.Properties?
        .ToDictionary(kv => kv.Key, static kv => kv.Value.ValueKind switch
        {
            JsonValueKind.String => (object?)kv.Value.GetString(),
            JsonValueKind.Number => kv.Value.TryGetInt64(out var i64) ? i64 : (object?)kv.Value.GetDouble(),
            JsonValueKind.True   => (object?)true,
            JsonValueKind.False  => (object?)false,
            _                    => (object?)kv.Value.ToString(),
        }) ?? [];

    // Named logger = SourceContext (shows as scope.name in OTLP)
    var logger = logFactory.CreateLogger(req.SourceContext ?? "Ingest");
    using (logger.BeginScope(scope))
        logger.Log(level, "{Message}", req.Message);

    // Persist in-memory + NDJSON
    scope.TryGetValue("duration",     out var durObj);
    scope.TryGetValue("HttpMethod",   out var methodObj);
    scope.TryGetValue("Uri",          out var uriObj);
    scope.TryGetValue("X-Request-ID", out var traceObj);

    var entry = new LogEntry(
        Timestamp : DateTimeOffset.UtcNow,
        Level     : req.Level,
        Operation : $"{methodObj} {uriObj}".Trim(),
        User      : traceObj?.ToString() ?? "unknown",
        StatusCode: 0,
        DurationMs: durObj is long l ? l : durObj is double d ? d : 0,
        TraceId   : Guid.NewGuid().ToString("N"),
        Message   : req.Message);

    store.Add(entry);
    return Results.Created("/logs", entry);
});

await app.RunAsync();
