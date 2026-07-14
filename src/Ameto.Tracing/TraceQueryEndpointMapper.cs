using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Tracing.Storage;
using Ameto.Tracing.TraceQL;
using HistogramBuckets = Ameto.Tracing.Storage.HistogramBuckets;

namespace Ameto.Tracing;

public static class TraceQueryEndpointMapper
{
    public static void MapTraceEndpoints(this WebApplication app)
    {
        // All trace read endpoints require the Traces view scope (admin bypasses).
        var group = app.MapGroup("").RequireAuthorization(ViewPolicies.Traces);

        // GET /api/traces/stats?from=&to=
        // Fully sidecar-based: percentiles from .stats, volume/sparkline from the .tracesum
        // volume headers. No span deserialization — sub-millisecond for any dataset/window.
        group.MapGet("/api/traces/stats", async (HttpContext ctx) =>
        {
            var statsProvider   = ctx.RequestServices.GetRequiredService<ITraceStatsProvider>();
            var summaryProvider = ctx.RequestServices.GetRequiredService<ITraceSummaryProvider>();
            var (from, to)      = ParseFromTo(ctx);

            var perService    = await statsProvider.GetAggregateStatsAsync(from, to, ctx.RequestAborted);
            var mergedBuckets = new uint[HistogramBuckets.Count];
            foreach (var svc in perService)
                for (int i = 0; i < HistogramBuckets.Count; i++)
                    mergedBuckets[i] += svc.Buckets[i];

            const int Buckets = 20;
            var volume = await summaryProvider.GetTraceVolumeAsync(from, to, Buckets, ctx.RequestAborted);

            double windowSeconds = Math.Max(1, (to - from).TotalSeconds);

            var stats = new TraceStatsDto
            {
                TotalTraces    = volume.TotalTraces,
                ErrorRate      = volume.TotalTraces > 0 ? (double)volume.ErrorTraces / volume.TotalTraces * 100.0 : 0,
                P50LatencyMs   = HistogramBuckets.Percentile(mergedBuckets, 0.50),
                P95LatencyMs   = HistogramBuckets.Percentile(mergedBuckets, 0.95),
                P99LatencyMs   = HistogramBuckets.Percentile(mergedBuckets, 0.99),
                ThroughputRps  = volume.TotalTraces / windowSeconds,
                TotalSparkline = volume.TotalSparkline,
                ErrorSparkline = volume.ErrorSparkline,
            };

            await ctx.Response.WriteAsJsonAsync(stats);
        });

        // GET /api/traces?from=&to=&service=&name=&status=&limit=&minDurationMs=&maxDurationMs=&httpStatus=
        // Served from .tracesum bodies (pre-aggregated per-trace rows) — no span deserialization.
        group.MapGet("/api/traces", async (HttpContext ctx) =>
        {
            var summaryProvider = ctx.RequestServices.GetRequiredService<ITraceSummaryProvider>();
            var (from, to) = ParseFromTo(ctx);

            var service = NullIfEmpty(ctx.Request.Query["service"]);
            var name    = NullIfEmpty(ctx.Request.Query["name"]);
            SpanStatusCode? status = null;
            if (ctx.Request.Query.TryGetValue("status", out var sv)
                && Enum.TryParse<SpanStatusCode>(sv, ignoreCase: true, out var sParsed))
                status = sParsed;

            int    limit         = ParseInt(ctx.Request.Query["limit"], 200, 1, 1000);
            long?  minDurNanos   = ParseLong(ctx.Request.Query["minDurationMs"]) is long minMs ? minMs * 1_000_000L : null;
            long?  maxDurNanos   = ParseLong(ctx.Request.Query["maxDurationMs"]) is long maxMs ? maxMs * 1_000_000L : null;
            string httpStatusRaw = ctx.Request.Query["httpStatus"].ToString();

            // Over-fetch when a post-filter (httpStatus) is active so the page still fills.
            int fetch = string.IsNullOrEmpty(httpStatusRaw) ? limit : Math.Min(1000, limit * 3);

            var summaries = await summaryProvider.GetTraceListAsync(
                from, to, service, name, status, minDurNanos, maxDurNanos, fetch, ctx.RequestAborted);

            var traces = new List<TraceRowDto>(Math.Min(summaries.Count, limit));
            foreach (var s in summaries)
            {
                int? httpSc = s.HttpStatusCode != 0 ? s.HttpStatusCode : null;
                if (!MatchHttpStatus(httpSc, httpStatusRaw)) continue;

                traces.Add(new TraceRowDto
                {
                    TraceId           = s.TraceId.ToString(),
                    SpanId            = s.RootSpanId.ToString(),
                    Name              = s.Name,
                    ServiceName       = s.ServiceName,
                    Services          = s.Services,
                    Status            = (s.HasError ? SpanStatusCode.Error : s.RootStatus).ToString(),
                    HttpMethod        = s.HttpMethod,
                    HttpPath          = s.HttpPath,
                    HttpStatusCode    = httpSc,
                    StartTimeUnixNano = s.RootStartNano,
                    DurationNanos     = s.DurationNanos,
                    SpanCount         = (int)s.SpanCount,
                });
                if (traces.Count >= limit) break;
            }

            await ctx.Response.WriteAsJsonAsync(traces);
        });

        // GET /api/traces/latency?from=&to=&service=
        // Returns per-service duration histograms + p50/p95/p99/p999 from .stats sidecars.
        group.MapGet("/api/traces/latency", async (HttpContext ctx) =>
        {
            var statsProvider = ctx.RequestServices.GetRequiredService<ITraceStatsProvider>();
            var (from, to)    = ParseFromTo(ctx);
            string? service   = NullIfEmpty(ctx.Request.Query["service"]);

            var allStats = await statsProvider.GetAggregateStatsAsync(from, to, ctx.RequestAborted);

            var result = allStats
                .Where(s => service is null || s.ServiceName.Equals(service, StringComparison.OrdinalIgnoreCase))
                .Select(s =>
                {
                    var buckets = s.Buckets;
                    var bounds  = HistogramBuckets.Bounds;
                    var dto = new
                    {
                        service    = s.ServiceName,
                        spanCount  = s.SpanCount,
                        errorCount = s.ErrorCount,
                        p50Ms      = HistogramBuckets.Percentile(buckets, 0.50),
                        p95Ms      = HistogramBuckets.Percentile(buckets, 0.95),
                        p99Ms      = HistogramBuckets.Percentile(buckets, 0.99),
                        p999Ms     = HistogramBuckets.Percentile(buckets, 0.999),
                        buckets    = BuildBucketList(s.Buckets),
                    };
                    return dto;
                })
                .ToList();

            await ctx.Response.WriteAsJsonAsync(result);
        });

        // GET /api/traces/compare?a={traceId}&b={traceId}
        group.MapGet("/api/traces/compare", async (HttpContext ctx) =>
        {
            var provider = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            string? aHex = ctx.Request.Query["a"];
            string? bHex = ctx.Request.Query["b"];

            if (!TraceId.TryParseHex(aHex, out var tidA) || !TraceId.TryParseHex(bHex, out var tidB))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("'a' and 'b' must be valid 32-char hex trace IDs");
                return;
            }

            var taskA = CollectSpansAsync(provider, tidA, ctx.RequestAborted);
            var taskB = CollectSpansAsync(provider, tidB, ctx.RequestAborted);
            await Task.WhenAll(taskA, taskB);

            await ctx.Response.WriteAsJsonAsync(new { traceA = taskA.Result, traceB = taskB.Result });
        });

        // GET /api/traces/service-graph?from=&to=
        group.MapGet("/api/traces/service-graph", async (HttpContext ctx) =>
        {
            var graphProvider = ctx.RequestServices.GetRequiredService<IServiceGraphProvider>();
            var (from, to)    = ParseFromTo(ctx);
            var graph = await graphProvider.GetServiceGraphAsync(from, to, ctx.RequestAborted);
            await ctx.Response.WriteAsJsonAsync(graph);
        });

        // POST /api/traces/query  — TraceQL
        // Body: { "query": "{ .http.status_code = 500 }", "from": "...", "to": "...", "limit": 100 }
        group.MapPost("/api/traces/query", async (HttpContext ctx) =>
        {
            TraceQueryRequest? req = null;
            try { req = await ctx.Request.ReadFromJsonAsync<TraceQueryRequest>(ctx.RequestAborted); }
            catch { ctx.Response.StatusCode = 400; await ctx.Response.WriteAsync("Invalid JSON"); return; }

            if (req is null || string.IsNullOrWhiteSpace(req.Query))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("'query' field is required");
                return;
            }

            SpanPredicate predicate;
            try   { predicate = TraceQLParser.Parse(req.Query); }
            catch (TraceQLException ex)
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync($"TraceQL parse error: {ex.Message}");
                return;
            }

            var from  = ParseDate(req.From) ?? DateTimeOffset.UtcNow.AddHours(-1);
            var to    = ParseDate(req.To)   ?? DateTimeOffset.UtcNow;
            int limit = Math.Clamp(req.Limit, 1, 1000);

            var provider = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var results  = await TraceQLExecutor.ExecuteAsync(provider, predicate, from, to, limit, ctx.RequestAborted);
            await ctx.Response.WriteAsJsonAsync(results);
        });

        // GET /api/traces/{traceId}/flamegraph
        group.MapGet("/api/traces/{traceId}/flamegraph", async (HttpContext ctx, string traceId) =>
        {
            if (!TraceId.TryParseHex(traceId, out var tid))
            {
                ctx.Response.StatusCode = 400;
                return;
            }
            var provider = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var spans    = await CollectSpansRawAsync(provider, tid, ctx.RequestAborted);

            if (spans.Count == 0) { ctx.Response.StatusCode = 404; return; }

            var flame = BuildFlamegraph(spans);
            await ctx.Response.WriteAsJsonAsync(flame);
        });

        // GET /api/traces/{traceId}
        group.MapGet("/api/traces/{traceId}", async (HttpContext ctx, string traceId) =>
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

    // ── Flamegraph builder ────────────────────────────────────────────────────

    private static FlamegraphNode? BuildFlamegraph(List<SpanRecord> spans)
    {
        // Index for O(1) lookup
        var byId       = new Dictionary<SpanId, SpanRecord>(spans.Count);
        var children   = new Dictionary<SpanId, List<SpanRecord>>(spans.Count);

        foreach (var s in spans)
        {
            byId[s.SpanId] = s;
            if (!children.ContainsKey(s.SpanId)) children[s.SpanId] = [];
        }

        SpanRecord? root = null;
        foreach (var s in spans)
        {
            if (s.ParentSpanId.IsEmpty || !byId.ContainsKey(s.ParentSpanId))
            { root = s; continue; }
            children[s.ParentSpanId].Add(s);
        }

        return root is null ? null : BuildNode(root, children);
    }

    private static FlamegraphNode BuildNode(
        SpanRecord span, Dictionary<SpanId, List<SpanRecord>> childMap)
    {
        var kids    = childMap.TryGetValue(span.SpanId, out var c) ? c : [];
        var kidNodes = kids.Select(k => BuildNode(k, childMap)).ToArray();

        double totalMs = span.DurationNanos / 1_000_000.0;
        double childMs = kidNodes.Sum(n => n.TotalMs);
        double selfMs  = Math.Max(0, totalMs - childMs);

        return new FlamegraphNode
        {
            SpanId   = span.SpanId.ToString(),
            Name     = span.Name,
            Service  = span.ServiceName,
            Kind     = span.Kind.ToString(),
            Status   = span.Status.ToString(),
            TotalMs  = Math.Round(totalMs, 3),
            SelfMs   = Math.Round(selfMs,  3),
            Children = kidNodes,
        };
    }

    // ── Misc helpers ──────────────────────────────────────────────────────────

    private static async Task<List<SpanDto>> CollectSpansAsync(
        ITraceProvider provider, TraceId tid, CancellationToken ct)
    {
        var list = new List<SpanDto>();
        await foreach (var s in provider.GetTraceAsync(tid, ct))
            list.Add(SpanDto.From(s));
        return list;
    }

    private static async Task<List<SpanRecord>> CollectSpansRawAsync(
        ITraceProvider provider, TraceId tid, CancellationToken ct)
    {
        var list = new List<SpanRecord>();
        await foreach (var s in provider.GetTraceAsync(tid, ct))
            list.Add(s);
        return list;
    }

    private static object[] BuildBucketList(uint[] buckets)
    {
        var bounds = Ameto.Tracing.Storage.HistogramBuckets.Bounds;
        var result = new object[buckets.Length];
        for (int i = 0; i < buckets.Length; i++)
        {
            double upperMs = i < bounds.Length ? bounds[i] / 1_000_000.0 : double.MaxValue;
            result[i] = new { upperMs, count = buckets[i] };
        }
        return result;
    }

    private static DateTimeOffset? ParseDate(string? s) =>
        DateTimeOffset.TryParse(s, out var v) ? v : null;

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

}

/// <summary>One row per trace (root span) for the trace list view.</summary>
public sealed class TraceRowDto
{
    public string   TraceId           { get; init; } = string.Empty;
    public string   SpanId            { get; init; } = string.Empty;
    public string   Name              { get; init; } = string.Empty;
    public string   ServiceName       { get; init; } = string.Empty;
    /// <summary>All unique service names across all spans in this trace.</summary>
    public string[] Services          { get; init; } = [];
    public string   Status            { get; init; } = string.Empty;
    public string   HttpMethod        { get; init; } = string.Empty;
    public string   HttpPath          { get; init; } = string.Empty;
    public int?     HttpStatusCode    { get; init; }
    public long     StartTimeUnixNano { get; init; }
    public long     DurationNanos     { get; init; }
    public int      SpanCount         { get; init; }
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
    public int                       HttpStatusCode    { get; init; }
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
        HttpStatusCode    = s.HttpStatusCode,
        Attributes        = s.Attributes?.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? string.Empty) ?? [],
    };
}

/// <summary>Single node in a trace flamegraph tree.</summary>
public sealed class FlamegraphNode
{
    public string          SpanId   { get; init; } = string.Empty;
    public string          Name     { get; init; } = string.Empty;
    public string          Service  { get; init; } = string.Empty;
    public string          Kind     { get; init; } = string.Empty;
    public string          Status   { get; init; } = string.Empty;
    public double          TotalMs  { get; init; }
    public double          SelfMs   { get; init; }
    public FlamegraphNode[] Children { get; init; } = [];
}

/// <summary>Request body for POST /api/traces/query.</summary>
public sealed class TraceQueryRequest
{
    public string  Query { get; init; } = string.Empty;
    public string? From  { get; init; }
    public string? To    { get; init; }
    public int     Limit { get; init; } = 100;
}

/// <summary>Aggregate stats for the trace stats cards.</summary>
public sealed class TraceStatsDto
{
    public int      TotalTraces    { get; init; }
    public double   ErrorRate      { get; init; }
    public double   P50LatencyMs   { get; init; }
    public double   P95LatencyMs   { get; init; }
    public double   P99LatencyMs   { get; init; }
    public double   ThroughputRps  { get; init; }
    public double[] TotalSparkline { get; init; } = [];
    public double[] ErrorSparkline { get; init; } = [];
}
