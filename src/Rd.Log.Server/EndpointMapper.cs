using System.Text.Json;
using System.Text.Json.Serialization;
using Rd.Log.Core;
using Rd.Log.Ingestion;
using Rd.Log.Query;
using Rd.Log.Server.Auth;
using Rd.Log.Storage;

namespace Rd.Log.Server;

/// <summary>Wire all Rd.Log HTTP endpoints onto the application.</summary>
public static class EndpointMapper
{
    private static readonly JsonSerializerOptions _json = new()
    {
        PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition      = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented               = false,
        Converters                  = { new DynamicObjectConverter() },
    };

    public static void MapRdLogEndpoints(this WebApplication app)
    {
        // ── Health ────────────────────────────────────────────────────────────
        app.MapGet("/health", () => Results.Ok(new { status = "ok", utc = DateTimeOffset.UtcNow }));

        // ── Stats ─────────────────────────────────────────────────────────────
        app.MapGet("/api/stats", (StorageEngine storage) =>
        {
            var segs = storage.GetSegments(null, null);
            return Results.Ok(new
            {
                segments        = segs.Count,
                totalEvents     = segs.Sum(s => (long)s.EventCount),
                compressedBytes = segs.Sum(s => s.CompressedBytes),
            });
        }).RequireAuthorization();

        // ── Ingest: POST /api/events  (CLEF msgpack batch) ───────────────────
        // Hot path: validated via in-memory ApiKeyCache (no JWT, no DB hit).
        app.MapPost("/api/events", async (HttpContext ctx, IngestionEndpoint ingestion, ApiKeyCache cache) =>
        {
            var key = ExtractApiKey(ctx.Request);
            if (key is null || !cache.Validate(key.AsSpan()))
            {
                ctx.Response.StatusCode  = StatusCodes.Status401Unauthorized;
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync("{\"error\":\"Valid API key required.\"}" );
                return;
            }
            await ingestion.HandleAsync(ctx);
        });

        // ── Query: GET /api/events  (SSE stream) ─────────────────────────────
        // Streams matching events as Server-Sent Events (one per data: line),
        // then signals completion with "event: done".
        // Query parameters:
        //   filter — Seq Filter Expression
        //   from   — ISO-8601 lower bound (inclusive)
        //   to     — ISO-8601 upper bound (inclusive)
        //   count  — max results (default 500)
        //   dir    — forward | backward (default backward)
        //   levels — comma-separated level names (omit = all levels)
        app.MapGet("/api/events", async (
            HttpContext           ctx,
            IQueryExecutor        executor,
            string?               filter   = null,
            string?               from     = null,
            string?               to       = null,
            int                   count    = 500,
            string?               dir      = null,
            string?               afterId  = null,
            long?                 afterTs  = null,
            string?               levels   = null) =>
        {
            ctx.Response.ContentType = "text/event-stream";
            ctx.Response.Headers.CacheControl = "no-cache";
            ctx.Response.Headers.Connection   = "keep-alive";
            await ctx.Response.Body.FlushAsync(ctx.RequestAborted);

            // afterId is the raw 64-bit Snowflake EventId; combined with afterTs it forms
            // the (ts, id) cursor used by the keyset pagination in QueryExecutor.
            Rd.Log.Core.EventId? cursor = null;
            if (!string.IsNullOrEmpty(afterId) && ulong.TryParse(afterId, out var raw))
                cursor = new Rd.Log.Core.EventId(raw);

            HashSet<Rd.Log.Core.LogLevel>? levelSet = null;
            if (!string.IsNullOrEmpty(levels))
            {
                levelSet = new HashSet<Rd.Log.Core.LogLevel>();
                foreach (var part in levels.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    if (Rd.Log.Core.LogLevelExtensions.TryParse(part.AsSpan(), out var lvl))
                        levelSet.Add(lvl);
                }
                if (levelSet.Count == 0 || levelSet.Count == 6) levelSet = null; // empty / all = no filter
            }

            var request = new QueryRequest
            {
                Filter              = filter,
                FromUtc             = from is null ? null : DateTimeOffset.Parse(from, null, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime(),
                ToUtc               = to   is null ? null : DateTimeOffset.Parse(to,   null, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime(),
                Count               = Math.Clamp(count, 1, 10_000),
                Direction           = "forward".Equals(dir, StringComparison.OrdinalIgnoreCase)
                                          ? QueryDirection.Forward : QueryDirection.Backward,
                AfterEventId        = cursor,
                AfterTimestampTicks = afterTs,
                Levels              = levelSet,
            };

            try
            {
                await foreach (var ev in executor.ExecuteAsync(request, ctx.RequestAborted))
                {
                    var json = JsonSerializer.Serialize(LogEventDto.From(ev), _json);
                    await ctx.Response.WriteAsync($"data: {json}\n\n", ctx.RequestAborted);
                    await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
                }
                await ctx.Response.WriteAsync("event: done\ndata: {}\n\n", ctx.RequestAborted);
                await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
            }
            catch (OperationCanceledException) { /* client disconnected */ }
        }).RequireAuthorization();

        // ── Distinct property names: GET /api/events/props ───────────────────
        // Returns sorted unique property keys from the last 24 h (up to 5 000 events sampled).
        app.MapGet("/api/events/props", async (HttpContext ctx, IQueryExecutor executor) =>
        {
            var request = new QueryRequest
            {
                FromUtc   = DateTimeOffset.UtcNow.AddDays(-1),
                Count     = 5_000,
                Direction = QueryDirection.Backward,
            };
            var props = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            await foreach (var ev in executor.ExecuteAsync(request, ctx.RequestAborted))
            {
                if (ev.Properties is null) continue;
                foreach (var key in ev.Properties.Keys)
                    props.Add(key);
            }
            return Results.Ok(props.ToArray());
        }).RequireAuthorization();

        // ── Live tail: GET /api/events/live  (SSE) ────────────────────────────
        // Streams new events as Server-Sent Events in CLEF JSON format.
        // Parameters: filter, from (default = now), count = 0 means unlimited.
        app.MapGet("/api/events/live", async (
            HttpContext    ctx,
            IQueryExecutor executor,
            string?        filter  = null,
            string?        from    = null) =>
        {
            ctx.Response.ContentType = "text/event-stream";
            ctx.Response.Headers.CacheControl = "no-cache";
            ctx.Response.Headers.Connection    = "keep-alive";
            await ctx.Response.Body.FlushAsync(ctx.RequestAborted);

            // Tail starts from 'from' or now, forward direction, unlimited.
            var fromDt = from is not null
                ? DateTimeOffset.Parse(from, null, System.Globalization.DateTimeStyles.RoundtripKind)
                : DateTimeOffset.UtcNow;

            Rd.Log.Core.EventId? cursor = null;
            long?                cursorTs = null;

            while (!ctx.RequestAborted.IsCancellationRequested)
            {
                var request = new QueryRequest
                {
                    Filter              = filter,
                    FromUtc             = fromDt,
                    Count               = 500,
                    Direction           = QueryDirection.Forward,
                    AfterEventId        = cursor,
                    AfterTimestampTicks = cursorTs,
                };

                int newCount = 0;
                await foreach (var ev in executor.ExecuteAsync(request, ctx.RequestAborted))
                {
                    var dto   = LogEventDto.From(ev);
                    var json  = JsonSerializer.Serialize(dto, _json);
                    var line  = $"data: {json}\n\n";
                    await ctx.Response.WriteAsync(line, ctx.RequestAborted);
                    await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
                    cursor   = (Rd.Log.Core.EventId?)ev.Id;
                    cursorTs = ev.Timestamp.UtcTicks;
                    newCount++;
                }

                if (newCount == 0)
                {
                    // Send keepalive comment and wait before next poll
                    await ctx.Response.WriteAsync(": keepalive\n\n", ctx.RequestAborted);
                    await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
                    try { await Task.Delay(250, ctx.RequestAborted); } catch (OperationCanceledException) { break; }
                }
            }
        }).RequireAuthorization();
    }

    // ── API-key extraction (ingest path only) ─────────────────────────────────
    private static string? ExtractApiKey(HttpRequest req)
    {
        if (req.Headers.TryGetValue("X-Seq-ApiKey", out var v) && v.Count > 0)
            return v[0];

        var header = req.Headers.Authorization.ToString();
        if (header.StartsWith("apikey ", StringComparison.OrdinalIgnoreCase))
            return header["apikey ".Length..].Trim();

        if (req.Query.TryGetValue("apiKey", out var qs) && qs.Count > 0)
            return qs[0];

        return null;
    }
}

// ── Dynamic object converter ──────────────────────────────────────────────────

/// <summary>
/// Serialises <c>object?</c> values stored in property dictionaries.
/// Handles the concrete types produced by <see cref="LogEventSerializer"/>:
/// nested dicts, arrays, primitives. Avoids the default ToString() fallback.
/// </summary>
internal sealed class DynamicObjectConverter : JsonConverter<object>
{
    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => throw new NotSupportedException();

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case Dictionary<string, object?> d:
                writer.WriteStartObject();
                foreach (var (k, v) in d)
                {
                    writer.WritePropertyName(k);
                    if (v is null) writer.WriteNullValue();
                    else Write(writer, v, options);
                }
                writer.WriteEndObject();
                break;
            case object[] arr:
                writer.WriteStartArray();
                foreach (var item in arr)
                {
                    if (item is null) writer.WriteNullValue();
                    else Write(writer, item, options);
                }
                writer.WriteEndArray();
                break;
            case string s:  writer.WriteStringValue(s);     break;
            case bool b:    writer.WriteBooleanValue(b);    break;
            case long l:    writer.WriteNumberValue(l);     break;
            case int i:     writer.WriteNumberValue(i);     break;
            case double d:  writer.WriteNumberValue(d);     break;
            case float f:   writer.WriteNumberValue(f);     break;
            case ulong u:   writer.WriteNumberValue(u);     break;
            default:        writer.WriteStringValue(value.ToString()); break;
        }
    }
}

// ── DTO ───────────────────────────────────────────────────────────────────────

/// <summary>JSON-serialisable view of a <see cref="LogEvent"/>.</summary>
internal sealed class LogEventDto
{
    [JsonPropertyName("@t")]            public string Timestamp       { get; init; } = "";
    [JsonPropertyName("@mt")]           public string MessageTemplate { get; init; } = "";
    [JsonPropertyName("@l")]            public string Level           { get; init; } = "";
    [JsonPropertyName("@x")]            public ExceptionInfoDto? Exception { get; init; }
    [JsonPropertyName("id")]            public string Id              { get; init; } = "";
    [JsonPropertyName("@tr")]           public string? TraceId        { get; init; }
    [JsonPropertyName("@sp")]           public string? SpanId         { get; init; }
    [JsonPropertyName("service.name")]  public string? ServiceName    { get; init; }
    [JsonPropertyName("props")]         public Dictionary<string, object?>? Properties { get; init; }

    public static LogEventDto From(LogEvent ev) => new()
    {
        Timestamp       = ev.Timestamp.ToString("O"),
        MessageTemplate = ev.MessageTemplate,
        Level           = ev.Level.ToSeqString(),
        Exception       = ExceptionInfoDto.From(ev.Exception),
        Id              = ev.Id.RawValue.ToString(),
        TraceId         = TraceIdHelper.FormatTraceId(ev.TraceIdHi, ev.TraceIdLo),
        SpanId          = TraceIdHelper.FormatSpanId(ev.SpanId),
        ServiceName     = ev.ServiceName,
        Properties      = ev.Properties,
    };
}

/// <summary>JSON-serialisable view of an <see cref="ExceptionInfo"/> tree.</summary>
internal sealed class ExceptionInfoDto
{
    [JsonPropertyName("type")]    public string  Type       { get; init; } = "";
    [JsonPropertyName("message")] public string? Message    { get; init; }
    [JsonPropertyName("stack")]   public string? StackTrace { get; init; }
    [JsonPropertyName("inner")]   public ExceptionInfoDto? Inner { get; init; }

    public static ExceptionInfoDto? From(ExceptionInfo? src)
    {
        if (src is null) return null;
        return new ExceptionInfoDto
        {
            Type       = src.Type,
            Message    = src.Message,
            StackTrace = src.StackTrace,
            Inner      = From(src.Inner),
        };
    }
}
