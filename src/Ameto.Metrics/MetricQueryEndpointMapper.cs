using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using System.Globalization;
using Ameto.Core;

namespace Ameto.Metrics;

public static class MetricQueryEndpointMapper
{
    public static void MapMetricEndpoints(this WebApplication app)
    {
        // GET /api/metrics/names?prefix=
        app.MapGet("/api/metrics/names", (IMetricQuery query, string? prefix) =>
            Results.Json(query.GetMetricNames(string.IsNullOrEmpty(prefix) ? null : prefix).ToList())
        ).RequireAuthorization(ViewPolicies.Metrics);

        // GET /api/metrics/catalog?search=
        app.MapGet("/api/metrics/catalog", (IMetricCatalog catalog, string? search) =>
        {
            var entries = catalog.GetCatalog(string.IsNullOrWhiteSpace(search) ? null : search)
                .Select(e => new MetricCatalogDto
                {
                    Name        = e.Name,
                    Type        = e.Kind.ToString(),
                    Unit        = e.Unit,
                    LabelKeys   = e.LabelKeys,
                    Cardinality = e.Cardinality,
                    LastSeenMs  = e.LastSeenMs,
                })
                .ToList();
            return Results.Json(entries);
        }).RequireAuthorization(ViewPolicies.Metrics);

        // GET /api/metrics/{name}/labels
        app.MapGet("/api/metrics/{name}/labels", (IMetricCatalog catalog, string name) =>
            Results.Json(catalog.GetLabelKeys(name))
        ).RequireAuthorization(ViewPolicies.Metrics);

        // GET /api/metrics/{name}/labels/{key}/values
        app.MapGet("/api/metrics/{name}/labels/{key}/values", (IMetricCatalog catalog, string name, string key) =>
            Results.Json(catalog.GetLabelValues(name, key))
        ).RequireAuthorization(ViewPolicies.Metrics);

        // POST /api/metrics/query  — server-side typed aggregation
        app.MapPost("/api/metrics/query", async (HttpContext ctx, IMetricAggregator agg) =>
        {
            MetricQueryDto? dto;
            try { dto = await ctx.Request.ReadFromJsonAsync<MetricQueryDto>(ctx.RequestAborted); }
            catch { return Results.BadRequest("Invalid JSON"); }

            if (dto is null || string.IsNullOrWhiteSpace(dto.Metric))
                return Results.BadRequest("'metric' is required");

            var series = await agg.QueryAsync(ToRequest(dto), ctx.RequestAborted);
            return Results.Json(series.Select(ToDto).ToList());
        }).RequireAuthorization(ViewPolicies.Metrics);

        // POST /api/metrics/expr  — binary metric expression (A op B)
        app.MapPost("/api/metrics/expr", async (HttpContext ctx, IMetricAggregator agg) =>
        {
            MetricExprDto? dto;
            try { dto = await ctx.Request.ReadFromJsonAsync<MetricExprDto>(ctx.RequestAborted); }
            catch { return Results.BadRequest("Invalid JSON"); }
            if (dto?.Left is null || dto.Right is null) return Results.BadRequest("'left' and 'right' are required");

            var req = new MetricExprRequest
            {
                Left  = ToRequest(dto.Left),
                Right = ToRequest(dto.Right),
                Op    = Enum.TryParse<MetricExprOp>(dto.Op, true, out var op) ? op : MetricExprOp.Div,
                Scale = dto.Scale is > 0 ? dto.Scale.Value : 1,
                Name  = dto.Name,
            };
            var series = await agg.EvalExprAsync(req, ctx.RequestAborted);
            return Results.Json(ToDto(series));
        }).RequireAuthorization(ViewPolicies.Metrics);

        // GET /api/metrics/{name}/heatmap?from=&to=&step=&filters=k:v,k2:v2
        app.MapGet("/api/metrics/{name}/heatmap", async (HttpContext ctx, IMetricAggregator agg, string name) =>
        {
            var from = ParseDate(ctx.Request.Query["from"]);
            var to   = ParseDate(ctx.Request.Query["to"]);
            var step = ParseStep(ctx.Request.Query["step"]);
            var filters = ParseFilters(ctx.Request.Query["filters"]);

            var hm = await agg.HeatmapAsync(name, from, to, step, filters, ctx.RequestAborted);
            return Results.Json(new HeatmapDto
            {
                Bounds  = hm.Bounds,
                Unit    = hm.Unit,
                Columns = hm.Columns.Select(c => new HeatmapColumnDto { Ts = c.Ts, Counts = c.Counts }).ToArray(),
            });
        }).RequireAuthorization(ViewPolicies.Metrics);

        // GET /api/metrics/{name}/exemplars?from=&to=&filters=k:v&limit=
        app.MapGet("/api/metrics/{name}/exemplars", (HttpContext ctx, IMetricExemplars store, string name) =>
        {
            var from    = ParseDate(ctx.Request.Query["from"]);
            var to      = ParseDate(ctx.Request.Query["to"]);
            var filters = ParseFilters(ctx.Request.Query["filters"]);
            int limit   = int.TryParse(ctx.Request.Query["limit"], out var l) ? Math.Clamp(l, 1, 1000) : 200;

            var result = store.GetExemplars(name, from, to, filters, limit)
                .Select(e => new ExemplarDto
                {
                    Ts      = e.TimestampUnixNano,
                    Value   = e.Value,
                    TraceId = e.TraceId,
                    SpanId  = e.SpanId,
                    Labels  = e.Labels.Pairs.ToDictionary(t => t.Key, t => t.Value),
                })
                .ToList();
            return Results.Json(result);
        }).RequireAuthorization(ViewPolicies.Metrics);

        // GET /api/metrics/{name}?from=&to=&step=  — raw series (no aggregation)
        app.MapGet("/api/metrics/{name}", async (HttpContext ctx, IMetricQuery query, string name) =>
        {
            var from = ParseDate(ctx.Request.Query["from"]);
            var to   = ParseDate(ctx.Request.Query["to"]);
            var step = ParseStep(ctx.Request.Query["step"]);

            var result = new List<MetricSeriesDto>();
            await foreach (var s in query.QueryAsync(name, from, to, step))
                result.Add(ToDto(s));

            return Results.Json(result);
        }).RequireAuthorization(ViewPolicies.Metrics);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static MetricQueryRequest ToRequest(MetricQueryDto dto) => new()
    {
        Metric      = dto.Metric,
        From        = ParseDate(dto.From),
        To          = ParseDate(dto.To),
        Step        = ParseStep(dto.Step),
        Aggregation = Enum.TryParse<MetricAggregation>(dto.Aggregation, true, out var a) ? a : MetricAggregation.None,
        Quantile    = dto.Quantile,
        GroupBy     = dto.GroupBy,
        Filters     = dto.Filters,
        TopK        = dto.Topk,
    };

    private static MetricSeriesDto ToDto(MetricSeries s) => new()
    {
        Name   = s.Name,
        Kind   = s.Kind.ToString(),
        Unit   = s.Unit,
        Labels = s.Labels.Pairs.ToDictionary(t => t.Key, t => t.Value),
        Points = s.Points.Select(p => new MetricPointDto
        {
            Ts    = p.TimestampUnixNano,
            Value = p.Value,
            Count = p.Count,
            Sum   = p.Sum,
        }).ToList(),
    };

    private static DateTimeOffset? ParseDate(string? s) =>
        DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var v) ? v : null;

    /// <summary>Accepts "15", "15s", "5m", "1h", or a TimeSpan string ("00:00:15").</summary>
    private static TimeSpan? ParseStep(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return null;
        s = s.Trim();
        if (int.TryParse(s, out var secs)) return TimeSpan.FromSeconds(secs);

        char suffix = s[^1];
        if ("smhd".IndexOf(suffix) >= 0 && double.TryParse(s[..^1], CultureInfo.InvariantCulture, out var n))
            return suffix switch
            {
                's' => TimeSpan.FromSeconds(n),
                'm' => TimeSpan.FromMinutes(n),
                'h' => TimeSpan.FromHours(n),
                'd' => TimeSpan.FromDays(n),
                _   => null,
            };

        return TimeSpan.TryParse(s, out var ts) ? ts : null;
    }

    private static Dictionary<string, string>? ParseFilters(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in raw.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            int eq = pair.IndexOf(':');
            if (eq <= 0) continue;
            dict[pair[..eq].Trim()] = pair[(eq + 1)..].Trim();
        }
        return dict.Count == 0 ? null : dict;
    }
}

// ── DTOs ────────────────────────────────────────────────────────────────────────

/// <summary>Request body for POST /api/metrics/query.</summary>
public sealed class MetricQueryDto
{
    public string                      Metric      { get; init; } = string.Empty;
    public string?                     From        { get; init; }
    public string?                     To          { get; init; }
    public string?                     Step        { get; init; }
    public string                      Aggregation { get; init; } = "none";
    public double?                     Quantile    { get; init; }
    public string[]?                   GroupBy     { get; init; }
    public Dictionary<string, string>? Filters     { get; init; }
    public int?                        Topk        { get; init; }
}

/// <summary>Request body for POST /api/metrics/expr.</summary>
public sealed class MetricExprDto
{
    public MetricQueryDto? Left  { get; init; }
    public MetricQueryDto? Right { get; init; }
    public string?         Op    { get; init; }
    public double?         Scale { get; init; }
    public string?         Name  { get; init; }
}

/// <summary>Catalog entry for the Explore UI.</summary>
public sealed class MetricCatalogDto
{
    public string   Name        { get; init; } = string.Empty;
    public string   Type        { get; init; } = string.Empty;
    public string   Unit        { get; init; } = string.Empty;
    public string[] LabelKeys   { get; init; } = [];
    public int      Cardinality { get; init; }
    public long     LastSeenMs  { get; init; }
}

/// <summary>JSON DTO for a metric time series.</summary>
public sealed class MetricSeriesDto
{
    public string                     Name   { get; init; } = string.Empty;
    public string                     Kind   { get; init; } = string.Empty;
    public string                     Unit   { get; init; } = string.Empty;
    public Dictionary<string, string> Labels { get; init; } = [];
    public List<MetricPointDto>       Points { get; init; } = [];
}

/// <summary>An exemplar: sampled measurement linked to a trace.</summary>
public sealed class ExemplarDto
{
    public long                       Ts      { get; init; }
    public double                     Value   { get; init; }
    public string                     TraceId { get; init; } = string.Empty;
    public string                     SpanId  { get; init; } = string.Empty;
    public Dictionary<string, string> Labels  { get; init; } = [];
}

/// <summary>A single data point in a metric time series.</summary>
public sealed class MetricPointDto
{
    public long   Ts    { get; init; }
    public double Value { get; init; }
    public long   Count { get; init; }
    public double Sum   { get; init; }
}

/// <summary>Histogram heatmap payload.</summary>
public sealed class HeatmapDto
{
    public double[]            Bounds  { get; init; } = [];
    public string              Unit    { get; init; } = string.Empty;
    public HeatmapColumnDto[]  Columns { get; init; } = [];
}

public sealed class HeatmapColumnDto
{
    public long     Ts     { get; init; }
    public double[] Counts { get; init; } = [];
}
