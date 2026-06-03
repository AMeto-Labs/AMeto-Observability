using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using System.Buffers;
using System.Text.Json;
using Rd.Log.Ingestion;
using Rd.Log.Metrics;
using Rd.Log.Otel.Models;
using Rd.Log.Tracing;

namespace Rd.Log.Otel;

/// <summary>
/// Maps OTLP/HTTP endpoints onto the ASP.NET Core application.
///
/// Endpoints:
///   POST /otlp/v1/traces   — accepts ExportTraceServiceRequest JSON
///   POST /otlp/v1/metrics  — accepts ExportMetricsServiceRequest JSON
///   POST /otlp/v1/logs     — accepts ExportLogsServiceRequest JSON
///
/// Content-Type: application/json (OTLP/HTTP JSON encoding).
/// Protobuf (application/x-protobuf) is not yet supported — clients must
/// configure the exporter to use HTTP/JSON.
/// </summary>
public static class OtlpEndpointMapper
{
    private const int    MaxBodyBytes        = 8 * 1024 * 1024; // 8 MB
    private const string JsonContentType     = "application/json";
    private const string ProtobufContentType = "application/x-protobuf";

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        AllowTrailingCommas  = true,
    };

    public static void MapOtlpEndpoints(this WebApplication app)
    {
        // ── Traces ────────────────────────────────────────────────────────────
        app.MapPost("/otlp/v1/traces", async (HttpContext ctx) =>
        {
            var ingester = ctx.RequestServices.GetRequiredService<ISpanIngester>();

            var (body, bodyLen) = await ReadBodyAsync(ctx);
            if (body is null) return;

            ExportTraceServiceRequest? request;
            try
            {
                bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;
                request = isProto
                    ? OtlpProtoDecoder.DecodeTraces(body, bodyLen)
                    : JsonSerializer.Deserialize<ExportTraceServiceRequest>(body.AsSpan(0, bodyLen), _jsonOptions);
            }
            catch { ctx.Response.StatusCode = 400; return; }
            finally { ArrayPool<byte>.Shared.Return(body); }

            if (request is null) { ctx.Response.StatusCode = 400; return; }

            var spans = OtlpTraceMapper.Map(request);
            if (spans.Count > 0)
            {
                ingester.TryIngest(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(spans), out int accepted);
                await WriteJsonOk(ctx, accepted, spans.Count - accepted);
            }
            else
            {
                await WriteJsonOk(ctx, 0, 0);
            }
        });

        // ── Metrics ───────────────────────────────────────────────────────────
        app.MapPost("/otlp/v1/metrics", async (HttpContext ctx) =>
        {
            var ingester = ctx.RequestServices.GetRequiredService<IMetricIngester>();

            var (body, bodyLen) = await ReadBodyAsync(ctx);
            if (body is null) return;

            ExportMetricsServiceRequest? request;
            try
            {
                bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;
                request = isProto
                    ? OtlpProtoDecoder.DecodeMetrics(body, bodyLen)
                    : JsonSerializer.Deserialize<ExportMetricsServiceRequest>(body.AsSpan(0, bodyLen), _jsonOptions);
            }
            catch { ctx.Response.StatusCode = 400; return; }
            finally { ArrayPool<byte>.Shared.Return(body); }

            if (request is null) { ctx.Response.StatusCode = 400; return; }

            var points = OtlpMetricMapper.Map(request);
            ingester.Ingest(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(points));
            await WriteJsonOk(ctx, points.Count, 0);
        });

        // ── Logs ──────────────────────────────────────────────────────────────
        app.MapPost("/otlp/v1/logs", async (HttpContext ctx) =>
        {
            var endpoint = ctx.RequestServices.GetRequiredService<IngestionEndpoint>();

            var (body, bodyLen) = await ReadBodyAsync(ctx);
            if (body is null) return;

            ExportLogsServiceRequest? request;
            try
            {
                bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;
                request = isProto
                    ? OtlpProtoDecoder.DecodeLogs(body, bodyLen)
                    : JsonSerializer.Deserialize<ExportLogsServiceRequest>(body.AsSpan(0, bodyLen), _jsonOptions);
            }
            catch { ctx.Response.StatusCode = 400; return; }
            finally { ArrayPool<byte>.Shared.Return(body); }

            if (request is null) { ctx.Response.StatusCode = 400; return; }

            // NodeId.Local is the same used by the existing ingestion path
            var events = OtlpLogMapper.Map(request, Rd.Log.Core.NodeId.Local.Value);
            var (ingested, dropped) = endpoint.IngestEvents(events);
            await WriteJsonOk(ctx, ingested, dropped);
        });

        // ── Query: metrics ────────────────────────────────────────────────────
        app.MapGet("/api/metrics/names", (IMetricQuery query, string? prefix) =>
            Results.Ok(query.GetMetricNames(prefix))
        ).RequireAuthorization();

        app.MapGet("/api/metrics", async (
            HttpContext   ctx,
            IMetricQuery  query,
            string        metric,
            string?       from   = null,
            string?       to     = null,
            int?          step   = null,
            string?       labels = null) =>
        {
            DateTimeOffset? fromDt = from is not null ? DateTimeOffset.Parse(from) : null;
            DateTimeOffset? toDt   = to   is not null ? DateTimeOffset.Parse(to)   : null;
            TimeSpan? stepTs       = step.HasValue ? TimeSpan.FromSeconds(step.Value) : null;

            // Parse label matchers: "key=value,key2=value2"
            Dictionary<string, string>? labelMatchers = null;
            if (!string.IsNullOrEmpty(labels))
            {
                labelMatchers = labels.Split(',')
                    .Select(s => s.Split('=', 2))
                    .Where(p => p.Length == 2)
                    .ToDictionary(p => p[0], p => p[1]);
            }

            var series = new List<object>();
            await foreach (var s in query.QueryAsync(metric, fromDt, toDt, stepTs, labelMatchers))
                series.Add(MapSeriesToDto(s));

            return Results.Ok(series);
        }).RequireAuthorization();
    }

    // ── DTO mappers ───────────────────────────────────────────────────────────

    private static object MapSpanToDto(SpanRecord s) => new
    {
        traceId      = s.TraceId.ToString(),
        spanId       = s.SpanId.ToString(),
        parentSpanId = s.ParentSpanId.IsEmpty ? null : s.ParentSpanId.ToString(),
        name         = s.Name,
        service      = s.ServiceName,
        kind         = s.Kind.ToString(),
        status       = s.Status.ToString(),
        startTime    = s.StartTime,
        durationMs   = s.Duration.TotalMilliseconds,
    };

    private static object MapSeriesToDto(MetricSeries s) => new
    {
        name   = s.Name,
        kind   = s.Kind.ToString(),
        unit   = s.Unit,
        labels = s.Labels.Pairs.ToDictionary(t => t.Key, t => t.Value),
        points = s.Points.Select(p => new
        {
            ts    = p.TimestampUnixNano,
            value = p.Value,
            count = p.Count > 0 ? p.Count : (long?)null,
        }),
    };

    // ── Body reading ──────────────────────────────────────────────────────────

    /// <summary>
    /// Rents a buffer from <see cref="ArrayPool{T}.Shared"/> and reads the full request body.
    /// Returns (null, 0) on error (status code already set). The caller MUST return the buffer
    /// to the pool via <c>ArrayPool&lt;byte&gt;.Shared.Return(buffer)</c> — use a finally block.
    /// </summary>
    private static async ValueTask<(byte[]? Buffer, int Length)> ReadBodyAsync(HttpContext ctx)
    {
        long? declared = ctx.Request.ContentLength;
        if (declared > MaxBodyBytes)
        {
            ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
            return (null, 0);
        }

        // Rent instead of allocating MemoryStream — Content-Length known → exact size
        int initialCapacity = declared.HasValue ? (int)declared.Value : 65_536;
        byte[] buf = ArrayPool<byte>.Shared.Rent(Math.Max(initialCapacity, 256));
        int totalRead = 0;
        try
        {
            while (true)
            {
                if (totalRead == buf.Length)
                {
                    // Grow: double the rented buffer
                    byte[] larger = ArrayPool<byte>.Shared.Rent(buf.Length * 2);
                    buf.AsSpan(0, totalRead).CopyTo(larger);
                    ArrayPool<byte>.Shared.Return(buf);
                    buf = larger;
                }

                int read = await ctx.Request.Body.ReadAsync(buf.AsMemory(totalRead), ctx.RequestAborted);
                if (read == 0) break;
                totalRead += read;

                if (totalRead > MaxBodyBytes)
                {
                    ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
                    ArrayPool<byte>.Shared.Return(buf);
                    return (null, 0);
                }
            }

            return (buf, totalRead);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buf);
            throw;
        }
    }

    /// <summary>
    /// Writes the JSON response directly to the response <see cref="System.IO.Pipelines.PipeWriter"/>.
    /// Uses <see cref="Utf8JsonWriter"/> with UTF-8 string literals to avoid string interpolation allocs.
    /// </summary>
    private static async ValueTask WriteJsonOk(HttpContext ctx, int ingested, int dropped)
    {
        ctx.Response.StatusCode  = 200;
        ctx.Response.ContentType = JsonContentType;
        // Write directly to the response pipe — zero intermediate string allocation
        var jw = new Utf8JsonWriter(ctx.Response.BodyWriter);
        jw.WriteStartObject();
        jw.WriteNumber("ingested"u8, ingested);
        jw.WriteNumber("dropped"u8,  dropped);
        jw.WriteEndObject();
        jw.Flush(); // advance PipeWriter cursor
        await ctx.Response.BodyWriter.FlushAsync(ctx.RequestAborted);
    }
}
