using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Tracing;

public static class TraceQueryEndpointMapper
{
    public static void MapTraceEndpoints(this WebApplication app)
    {
        // GET /api/traces/stats?from=&to=
        app.MapGet("/api/traces/stats", async (HttpContext ctx) =>
        {
            var provider  = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var (from, to) = ParseFromTo(ctx);

            var allSpans = new List<SpanRecord>();
            await foreach (var s in provider.SearchSpansAsync(from, to, limit: 10_000, ct: ctx.RequestAborted))
                allSpans.Add(s);

            var groups = allSpans.GroupBy(s => s.TraceId).ToList();
            int totalTraces = groups.Count;
            int errorTraces = groups.Count(g => g.Any(s => s.Status == SpanStatusCode.Error));

            var durations = groups
                .Select(g => (GetRootSpan(g) ?? g.OrderBy(s => s.StartTimeUnixNano).First()).DurationNanos / 1_000_000.0)
                .Where(d => d > 0)
                .OrderBy(d => d)
                .ToList();

            double windowSeconds = Math.Max(1, (to - from).TotalSeconds);

            const int BUCKETS = 20;
            long fromNano  = from.ToUnixTimeMilliseconds() * 1_000_000L;
            long rangeNano = Math.Max(1L, to.ToUnixTimeMilliseconds() * 1_000_000L - fromNano);

            var totalSparkline = new double[BUCKETS];
            var errorSparkline = new double[BUCKETS];

            foreach (var g in groups)
            {
                var root   = GetRootSpan(g) ?? g.OrderBy(s => s.StartTimeUnixNano).First();
                int bucket = (int)Math.Clamp((root.StartTimeUnixNano - fromNano) * (double)BUCKETS / rangeNano, 0, BUCKETS - 1);
                totalSparkline[bucket]++;
                if (g.Any(s => s.Status == SpanStatusCode.Error)) errorSparkline[bucket]++;
            }

            var stats = new TraceStatsDto
            {
                TotalTraces    = totalTraces,
                ErrorRate      = totalTraces > 0 ? (double)errorTraces / totalTraces * 100.0 : 0,
                P50LatencyMs   = Percentile(durations, 0.50),
                P95LatencyMs   = Percentile(durations, 0.95),
                ThroughputRps  = totalTraces / windowSeconds,
                TotalSparkline = totalSparkline,
                ErrorSparkline = errorSparkline,
            };

            await ctx.Response.WriteAsJsonAsync(stats);
        });

        // GET /api/traces?from=&to=&service=&name=&status=&limit=
        // Returns one TraceRowDto per distinct trace (using the root span).
        app.MapGet("/api/traces", async (HttpContext ctx) =>
        {
            var provider  = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var (from, to) = ParseFromTo(ctx);

            var service = NullIfEmpty(ctx.Request.Query["service"]);
            var name    = NullIfEmpty(ctx.Request.Query["name"]);
            SpanStatusCode? status = null;
            if (ctx.Request.Query.TryGetValue("status", out var sv)
                && Enum.TryParse<SpanStatusCode>(sv, ignoreCase: true, out var sParsed))
                status = sParsed;
            int    limit          = ParseInt(ctx.Request.Query["limit"], 200, 1, 1000);
            long?  minDurNanos    = ParseLong(ctx.Request.Query["minDurationMs"]) is long minMs ? minMs * 1_000_000L : null;
            long?  maxDurNanos    = ParseLong(ctx.Request.Query["maxDurationMs"]) is long maxMs ? maxMs * 1_000_000L : null;
            string httpStatusRaw  = ctx.Request.Query["httpStatus"].ToString();

            var allSpans = new List<SpanRecord>();
            await foreach (var s in provider.SearchSpansAsync(from, to, service, name, status, minDurNanos, maxDurNanos, limit * 5, ctx.RequestAborted))
                allSpans.Add(s);

            var traces = allSpans
                .GroupBy(s => s.TraceId)
                .Select(g =>
                {
                    var spans   = g.ToList();
                    var root    = GetRootSpan(g) ?? spans.OrderBy(s => s.StartTimeUnixNano).First();
                    bool hasErr = spans.Any(s => s.Status == SpanStatusCode.Error);
                    return new TraceRowDto
                    {
                        TraceId           = root.TraceId.ToString(),
                        SpanId            = root.SpanId.ToString(),
                        Name              = root.Name,
                        ServiceName       = root.ServiceName,
                        Status            = hasErr ? "Error" : root.Status.ToString(),
                        HttpMethod        = GetAttr(root.Attributes, "http.request.method", "http.method"),
                        HttpPath          = GetAttr(root.Attributes, "url.path", "http.target", "http.route", "url.full", "http.url"),
                        HttpStatusCode    = GetIntAttr(root.Attributes, "http.response.status_code", "http.status_code"),
                        StartTimeUnixNano = root.StartTimeUnixNano,
                        DurationNanos     = root.DurationNanos,
                        SpanCount         = spans.Count,
                    };
                })
                .Where(t => MatchHttpStatus(t.HttpStatusCode, httpStatusRaw))
                .OrderByDescending(t => t.StartTimeUnixNano)
                .Take(limit)
                .ToList();

            await ctx.Response.WriteAsJsonAsync(traces);
        });

        // GET /api/traces/{traceId}
        app.MapGet("/api/traces/{traceId}", async (HttpContext ctx, string traceId) =>
        {
            if (!TraceId.TryParseHex(traceId, out var tid))
            {
                ctx.Response.StatusCode = 400;
                return;
            }
            var provider = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var spans    = new List<SpanDto>();
            await foreach (var s in provider.GetTraceAsync(tid, ctx.RequestAborted))
                spans.Add(SpanDto.From(s));

            await ctx.Response.WriteAsJsonAsync(spans);
        });
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (DateTimeOffset from, DateTimeOffset to) ParseFromTo(HttpContext ctx, double defaultHours = 1)
    {
        var to   = DateTimeOffset.UtcNow;
        var from = to.AddHours(-defaultHours);
        if (ctx.Request.Query.TryGetValue("from", out var fv) && DateTimeOffset.TryParse(fv, out var fp)) from = fp;
        if (ctx.Request.Query.TryGetValue("to",   out var tv) && DateTimeOffset.TryParse(tv, out var tp)) to   = tp;
        return (from, to);
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrWhiteSpace(s) ? null : s;

    private static int ParseInt(string? s, int def, int min, int max) =>
        int.TryParse(s, out var v) ? Math.Clamp(v, min, max) : def;

    private static long? ParseLong(string? s) =>
        long.TryParse(s, out var v) && v > 0 ? v : null;

    /// <summary>
    /// Matches HTTP status code against a filter string.
    /// Supports: empty (pass all), "4xx", "5xx", exact integer ("404", "500").
    /// </summary>
    private static bool MatchHttpStatus(int? code, string filter)
    {
        if (string.IsNullOrEmpty(filter)) return true;
        if (code is null) return false;
        if (filter.Equals("4xx", StringComparison.OrdinalIgnoreCase)) return code >= 400 && code < 500;
        if (filter.Equals("5xx", StringComparison.OrdinalIgnoreCase)) return code >= 500 && code < 600;
        if (filter.Equals("2xx", StringComparison.OrdinalIgnoreCase)) return code >= 200 && code < 300;
        if (filter.Equals("3xx", StringComparison.OrdinalIgnoreCase)) return code >= 300 && code < 400;
        return int.TryParse(filter, out var exact) && code == exact;
    }

    private static SpanRecord? GetRootSpan(IEnumerable<SpanRecord> spans)
    {
        foreach (var s in spans)
        {
            var p = s.ParentSpanId.ToString();
            if (string.IsNullOrEmpty(p) || p.All(c => c == '0'))
                return s;
        }
        return null;
    }

    private static string GetAttr(IReadOnlyDictionary<string, object?>? attrs, params string[] keys)
    {
        if (attrs is null) return string.Empty;
        foreach (var k in keys)
            if (attrs.TryGetValue(k, out var v) && v is not null)
                return v.ToString() ?? string.Empty;
        return string.Empty;
    }

    private static int? GetIntAttr(IReadOnlyDictionary<string, object?>? attrs, params string[] keys)
    {
        var s = GetAttr(attrs, keys);
        return int.TryParse(s, out var v) ? v : null;
    }

    private static double Percentile(List<double> sorted, double p)
    {
        if (sorted.Count == 0) return 0;
        return sorted[Math.Clamp((int)(p * (sorted.Count - 1)), 0, sorted.Count - 1)];
    }
}

/// <summary>One row per trace (root span) for the trace list view.</summary>
public sealed class TraceRowDto
{
    public string TraceId           { get; init; } = string.Empty;
    public string SpanId            { get; init; } = string.Empty;
    public string Name              { get; init; } = string.Empty;
    public string ServiceName       { get; init; } = string.Empty;
    public string Status            { get; init; } = string.Empty;
    public string HttpMethod        { get; init; } = string.Empty;
    public string HttpPath          { get; init; } = string.Empty;
    public int?   HttpStatusCode    { get; init; }
    public long   StartTimeUnixNano { get; init; }
    public long   DurationNanos     { get; init; }
    public int    SpanCount         { get; init; }
}

/// <summary>JSON DTO for a single span, returned to the Angular client.</summary>
public sealed class SpanDto
{
    public string                    TraceId           { get; init; } = string.Empty;
    public string                    SpanId            { get; init; } = string.Empty;
    public string                    ParentSpanId      { get; init; } = string.Empty;
    public long                      StartTimeUnixNano { get; init; }
    public long                      DurationNanos     { get; init; }
    public string                    Name              { get; init; } = string.Empty;
    public string                    ServiceName       { get; init; } = string.Empty;
    public string                    Kind              { get; init; } = string.Empty;
    public string                    Status            { get; init; } = string.Empty;
    public Dictionary<string,string> Attributes        { get; init; } = [];

    public static SpanDto From(SpanRecord s) => new()
    {
        TraceId           = s.TraceId.ToString(),
        SpanId            = s.SpanId.ToString(),
        ParentSpanId      = s.ParentSpanId.ToString(),
        StartTimeUnixNano = s.StartTimeUnixNano,
        DurationNanos     = s.DurationNanos,
        Name              = s.Name,
        ServiceName       = s.ServiceName,
        Kind              = s.Kind.ToString(),
        Status            = s.Status.ToString(),
        Attributes        = s.Attributes?.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? string.Empty) ?? [],
    };
}

/// <summary>Aggregate stats for the trace stats cards.</summary>
public sealed class TraceStatsDto
{
    public int      TotalTraces    { get; init; }
    public double   ErrorRate      { get; init; }
    public double   P50LatencyMs   { get; init; }
    public double   P95LatencyMs   { get; init; }
    public double   ThroughputRps  { get; init; }
    public double[] TotalSparkline { get; init; } = [];
    public double[] ErrorSparkline { get; init; } = [];
}
