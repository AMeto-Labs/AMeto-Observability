using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Query;
using Ameto.Server.Auth;
using Ameto.Storage;

namespace Ameto.Server;

/// <summary>Wire all Ameto HTTP endpoints onto the application.</summary>
public static class EndpointMapper
{
    private static readonly JsonSerializerOptions _json = new()
    {
        PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition      = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented               = false,
        Converters                  = { new DynamicObjectConverter() },
    };

    public static void MapAmetoEndpoints(this WebApplication app)
    {        // ── Health ────────────────────────────────────────────────────────────
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
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewStats);

        // ── Ingest: POST /api/events  (CLEF msgpack batch) ───────────────────
        // Hot path: validated via in-memory ApiKeyCache (no JWT, no DB hit).
        app.MapPost("/api/events", async (HttpContext ctx, IngestionEndpoint ingestion, ApiKeyCache cache) =>
        {
            var key = Ameto.Ingestion.ApiKeyHeader.Extract(ctx.Request);
            if (key is null || !cache.Validate(key.AsSpan(), Ameto.Ingestion.ApiKeyPermissions.Logs))
            {
                ctx.Response.StatusCode  = StatusCodes.Status401Unauthorized;
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync("{\"error\":\"Valid API key with Logs permission required.\"}" );
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
            Ameto.Core.EventId? cursor = null;
            if (!string.IsNullOrEmpty(afterId) && ulong.TryParse(afterId, out var raw))
                cursor = new Ameto.Core.EventId(raw);

            HashSet<Ameto.Core.LogLevel>? levelSet = null;
            if (!string.IsNullOrEmpty(levels))
            {
                levelSet = new HashSet<Ameto.Core.LogLevel>();
                foreach (var part in levels.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    if (Ameto.Core.LogLevelExtensions.TryParse(part.AsSpan(), out var lvl))
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
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

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
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Distinct services: GET /api/events/services ───────────────────────
        // Returns sorted unique values of ApplicationContext / service.name properties
        // from the last 7 days (up to 10 000 events sampled) — fast index-friendly scan.
        app.MapGet("/api/events/services", async (HttpContext ctx, IQueryExecutor executor,
            int days = 7) =>
        {
            var request = new QueryRequest
            {
                FromUtc   = DateTimeOffset.UtcNow.AddDays(-Math.Clamp(days, 1, 90)),
                Count     = 10_000,
                Direction = QueryDirection.Backward,
            };
            var services = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            await foreach (var ev in executor.ExecuteAsync(request, ctx.RequestAborted))
            {
                // Prefer service.name (OTLP), fall back to ApplicationContext (Serilog)
                var svc = ev.ServiceName
                    ?? (ev.Properties?.TryGetValue("ApplicationContext", out var v) == true
                        ? v?.ToString() : null);
                if (!string.IsNullOrWhiteSpace(svc))
                    services.Add(svc);
            }
            return Results.Ok(services.ToArray());
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Event counts by service + level over time: GET /api/events/counts ─
        // Powers the "Log events" chart / stats. Buckets per-service AND per-level
        // event counts over [from, to] by scanning event HEADERS only — no LogEvent,
        // Properties, message or exception is ever materialised (see
        // StorageEngine.AggregateLogVolumeAsync). Results are dense, chart-ready arrays.
        //   from    — ISO-8601 lower bound (default: now - 24h)
        //   to      — ISO-8601 upper bound (default: now)
        //   bucket  — bucket size in seconds (default: auto from the range)
        //   limit   — accepted for backward compatibility; the header scan is cheap
        //             enough to always cover the full window, so it no longer bounds it.
        //   service — restrict to a single service (case-insensitive)
        app.MapGet("/api/events/counts", async (
            HttpContext           ctx,
            StorageEngine         storage,
            LogVolumeCountsCache  cache,
            string?               from    = null,
            string?               to      = null,
            int?                  bucket  = null,
            int                   limit   = 50_000,
            string?               service = null) =>
        {
            _ = limit; // retained for API compatibility; no longer caps the scan.

            var now   = DateTimeOffset.UtcNow;
            var toUtc = to is null ? now
                                   : DateTimeOffset.Parse(to, null, DateTimeStyles.RoundtripKind).ToUniversalTime();
            var fromUtc = from is null ? toUtc.AddDays(-1)
                                       : DateTimeOffset.Parse(from, null, DateTimeStyles.RoundtripKind).ToUniversalTime();
            if (fromUtc > toUtc) (fromUtc, toUtc) = (toUtc, fromUtc);

            double rangeSec = Math.Max(1, (toUtc - fromUtc).TotalSeconds);
            int bucketSeconds = bucket is > 0 ? bucket.Value : AutoBucketSeconds(rangeSec);

            // Keep the bucket axis manageable; widen the bucket if the requested
            // size would produce more than 2 000 columns.
            long minB = fromUtc.ToUnixTimeSeconds() / bucketSeconds;
            long maxB = toUtc.ToUnixTimeSeconds()   / bucketSeconds;
            if (maxB - minB + 1 > 2_000)
            {
                bucketSeconds = AutoBucketSeconds(rangeSec);
                minB = fromUtc.ToUnixTimeSeconds() / bucketSeconds;
                maxB = toUtc.ToUnixTimeSeconds()   / bucketSeconds;
            }
            int nBuckets = (int)(maxB - minB + 1);

            var svcFilter = string.IsNullOrEmpty(service) ? null : service;

            // Cache keyed on the bucket grid so drifting "now" bounds still hit within a TTL.
            var cacheKey = new CountsCacheKey(minB, maxB, bucketSeconds, svcFilter);
            if (cache.TryGet(cacheKey, out var cached))
                return Results.Json(cached, EventCountsJsonContext.Default.EventCountsResponse);

            var result = await storage.AggregateLogVolumeAsync(
                fromUtc, toUtc, minB, bucketSeconds, nBuckets, svcFilter, ctx.RequestAborted);

            var buckets = new long[nBuckets];
            for (int i = 0; i < nBuckets; i++)
                buckets[i] = (minB + i) * bucketSeconds * 1000L; // bucket start, unix ms

            // Top services by count (cap to 25 to bound payload / chart noise). Services
            // are already sorted descending by the aggregator.
            const int maxServices = 25;
            int svcTake = Math.Min(maxServices, result.Services.Count);
            var servicesOut = new CountSeriesDto[svcTake];
            for (int i = 0; i < svcTake; i++)
            {
                var s = result.Services[i];
                servicesOut[i] = new CountSeriesDto { Service = s.Name, Count = s.Count, Points = s.Points };
            }

            var levelsOut = new CountSeriesDto[result.Levels.Count];
            for (int i = 0; i < result.Levels.Count; i++)
            {
                var l = result.Levels[i];
                levelsOut[i] = new CountSeriesDto { Level = l.Name, Count = l.Count, Points = l.Points };
            }

            var response = new EventCountsResponse
            {
                From          = fromUtc.ToString("O"),
                To            = toUtc.ToString("O"),
                BucketSeconds = bucketSeconds,
                Total         = result.Total,
                Sampled       = result.Scanned,
                Truncated     = false, // full-window header scan — never sampled/truncated
                Buckets       = buckets,
                Services      = servicesOut,
                Levels        = levelsOut,
            };

            cache.Set(cacheKey, response);
            return Results.Json(response, EventCountsJsonContext.Default.EventCountsResponse);
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

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

            Ameto.Core.EventId? cursor = null;
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
                    cursor   = (Ameto.Core.EventId?)ev.Id;
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
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Span logs: GET /api/spans/{spanId}/logs ───────────────────────────
        // Returns up to 500 log events that were emitted within the given span.
        // spanId must be a 16-char lowercase hex string (W3C 64-bit span id).
        app.MapGet("/api/spans/{spanId}/logs", async (
            HttpContext    ctx,
            IQueryExecutor executor,
            string         spanId,
            string?        from  = null,
            string?        to    = null,
            int            count = 500) =>
        {
            if (!Ameto.Core.TraceIdHelper.TryParseSpanId(spanId, out _))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("spanId must be a 16-char hex string");
                return;
            }

            // Build filter that hits the inverted index on @sp
            string spanFilter = $"@sp = '{spanId}'";

            var request = new QueryRequest
            {
                Filter    = spanFilter,
                FromUtc   = from is null ? null
                            : DateTimeOffset.Parse(from, null,
                                System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime(),
                ToUtc     = to is null ? null
                            : DateTimeOffset.Parse(to, null,
                                System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime(),
                Count     = Math.Clamp(count, 1, 5_000),
                Direction = QueryDirection.Forward,
            };

            var results = new List<LogEventDto>();
            await foreach (var ev in executor.ExecuteAsync(request, ctx.RequestAborted))
                results.Add(LogEventDto.From(ev));

            await ctx.Response.WriteAsJsonAsync(results, _json, ctx.RequestAborted);
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Trace logs: GET /api/traces/{traceId}/logs ────────────────────────
        // Returns every log event correlated to the trace (filtered on @tr). This is
        // the primary trace↔logs correlation: logs are written under child spans, so
        // a trace-wide query is what actually surfaces them. The client narrows to a
        // single span by matching @sp on its side.
        // traceId must be a 32-char lowercase hex string (W3C 128-bit trace id).
        app.MapGet("/api/traces/{traceId}/logs", async (
            HttpContext    ctx,
            IQueryExecutor executor,
            string         traceId,
            string?        from  = null,
            string?        to    = null,
            int            count = 2000) =>
        {
            if (!Ameto.Core.TraceIdHelper.TryParseTraceId(traceId, out _, out _))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("traceId must be a 32-char hex string");
                return;
            }

            // Build filter that hits the inverted index on @tr
            string traceFilter = $"@tr = '{traceId}'";

            var request = new QueryRequest
            {
                Filter    = traceFilter,
                FromUtc   = from is null ? null
                            : DateTimeOffset.Parse(from, null,
                                System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime(),
                ToUtc     = to is null ? null
                            : DateTimeOffset.Parse(to, null,
                                System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime(),
                Count     = Math.Clamp(count, 1, 5_000),
                Direction = QueryDirection.Forward,
            };

            var results = new List<LogEventDto>();
            await foreach (var ev in executor.ExecuteAsync(request, ctx.RequestAborted))
                results.Add(LogEventDto.From(ev));

            await ctx.Response.WriteAsJsonAsync(results, _json, ctx.RequestAborted);
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);
    }

    // ── API-key extraction (ingest path only) ─────────────────────────────────

    /// <summary>
    /// Picks a "nice" time-bucket size (in seconds) for a given range so the
    /// resulting chart has ~120 columns. Used by GET /api/events/counts when no
    /// explicit <c>bucket</c> is supplied.
    /// </summary>
    private static int AutoBucketSeconds(double rangeSeconds)
    {
        const int target = 120;
        double raw = rangeSeconds / target;
        int[] steps = { 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14_400, 21_600, 43_200, 86_400, 172_800, 604_800 };
        foreach (var s in steps)
            if (s >= raw) return s;
        return (int)Math.Ceiling(raw / 604_800.0) * 604_800;
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
