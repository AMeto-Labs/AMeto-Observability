using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Rd.Log.Metrics;

public static class MetricQueryEndpointMapper
{
    public static void MapMetricEndpoints(this WebApplication app)
    {
        // GET /api/metrics/names?prefix=
        app.MapGet("/api/metrics/names", (HttpContext ctx) =>
        {
            var query  = ctx.RequestServices.GetRequiredService<IMetricQuery>();
            var prefix = ctx.Request.Query["prefix"].ToString();
            var names  = query.GetMetricNames(string.IsNullOrEmpty(prefix) ? null : prefix);
            return Results.Json(names.ToList());
        });

        // GET /api/metrics/{name}?from=&to=&step=
        app.MapGet("/api/metrics/{name}", async (HttpContext ctx, string name) =>
        {
            var query = ctx.RequestServices.GetRequiredService<IMetricQuery>();

            DateTimeOffset? from = null, to = null;
            if (ctx.Request.Query.TryGetValue("from", out var fv)
                && DateTimeOffset.TryParse(fv, out var fParsed)) from = fParsed;
            if (ctx.Request.Query.TryGetValue("to", out var tv)
                && DateTimeOffset.TryParse(tv, out var tParsed)) to = tParsed;

            TimeSpan? step = null;
            if (ctx.Request.Query.TryGetValue("step", out var sv)
                && TimeSpan.TryParse(sv, out var sParsed)) step = sParsed;

            var result = new List<MetricSeriesDto>();
            await foreach (var s in query.QueryAsync(name, from, to, step))
            {
                result.Add(new MetricSeriesDto
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
                });
            }

            await ctx.Response.WriteAsJsonAsync(result);
        });
    }
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

/// <summary>A single data point in a metric time series.</summary>
public sealed class MetricPointDto
{
    public long   Ts    { get; init; }
    public double Value { get; init; }
    public long   Count { get; init; }
    public double Sum   { get; init; }
}
