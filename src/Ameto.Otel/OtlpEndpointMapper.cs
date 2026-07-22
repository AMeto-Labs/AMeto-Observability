using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Text.Json;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Metrics;
using Ameto.Otel.Models;
using Ameto.Tracing;

namespace Ameto.Otel;

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
    private const string JsonContentType     = "application/json";
    private const string ProtobufContentType = "application/x-protobuf";

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        AllowTrailingCommas  = true,
    };

    public static void MapOtlpEndpoints(this WebApplication app, bool enableTraces = true, bool enableMetrics = true)
    {
        // ── Traces ────────────────────────────────────────────────────────────
        if (enableTraces)
        app.MapPost("/otlp/v1/traces", async (HttpContext ctx, ISpanIngester ingester, ILoggerFactory logFactory) =>
        {
            if (!Authorized(ctx, ApiKeyPermissions.Traces)) return;
            var logger = logFactory.CreateLogger("Ameto.Otel.Traces");

            var (body, bodyLen) = await ReadBodyAsync(ctx);
            if (body is null) return;

            List<Ameto.Tracing.SpanIngestItem> spans;
            try
            {
                bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;
                if (isProto)
                {
                    // Protobuf still decodes into the object model, then maps to items.
                    var request = OtlpProtoDecoder.DecodeTraces(body, bodyLen);
                    if (request is null) { ctx.Response.StatusCode = 400; return; }
                    spans = OtlpTraceMapper.Map(request);
                }
                else
                {
                    // JSON: streaming parse straight to SpanIngestItems — no OTLP object
                    // graph, no per-field hex/nano strings (see OtlpTraceStreamParser).
                    spans = OtlpTraceStreamParser.Parse(body.AsSpan(0, bodyLen));
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "OTLP /v1/traces: failed to decode body ({Bytes} bytes, Content-Type: {Ct})",
                    bodyLen, ctx.Request.ContentType);
                ctx.Response.StatusCode = 400;
                return;
            }
            finally { ArrayPool<byte>.Shared.Return(body); }

            logger.LogDebug("OTLP /v1/traces: decoded {SpanCount} spans", spans.Count);

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
        if (enableMetrics)
        app.MapPost("/otlp/v1/metrics", async (HttpContext ctx) =>
        {
            if (!Authorized(ctx, ApiKeyPermissions.Metrics)) return;
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

            var resourceLabels = ctx.RequestServices.GetRequiredService<ServerOptions>()
                                    .Ingestion.MetricResourceLabels;
            var points = OtlpMetricMapper.Map(request, resourceLabels);
            ingester.Ingest(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(points));
            await WriteJsonOk(ctx, points.Count, 0);
        });

        // ── Logs ──────────────────────────────────────────────────────────────
        app.MapPost("/otlp/v1/logs", async (HttpContext ctx) =>
        {
            if (!Authorized(ctx, ApiKeyPermissions.Logs)) return;
            var endpoint = ctx.RequestServices.GetRequiredService<IngestionEndpoint>();

            var (body, bodyLen) = await ReadBodyAsync(ctx);
            if (body is null) return;

            int ingested = 0, dropped = 0;
            try
            {
                bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;
                if (isProto)
                {
                    // Protobuf still decodes into the object model, then maps to events.
                    var request = OtlpProtoDecoder.DecodeLogs(body, bodyLen);
                    if (request is null) { ctx.Response.StatusCode = 400; return; }
                    var events = OtlpLogMapper.Map(request, Ameto.Core.NodeId.Local.Value);
                    (ingested, dropped) = endpoint.IngestEvents(events);
                }
                else
                {
                    // JSON: zero-alloc streaming parse straight into the ring — no object
                    // graph, no per-record LogEvent, no per-attribute strings.
                    (ingested, dropped) = OtlpLogStreamParser.Parse(body.AsSpan(0, bodyLen), endpoint);
                }
            }
            catch { ctx.Response.StatusCode = 400; return; }
            finally { ArrayPool<byte>.Shared.Return(body); }

            await WriteJsonOk(ctx, ingested, dropped);
        });

        // Metric query endpoints live in Ameto.Metrics.MetricQueryEndpointMapper
        // (mapped via app.MapMetricEndpoints()).
    }

    // ── API-key authorization ───────────────────────────────────────────────────

    /// <summary>
    /// Enforces the ingest API key (cache-backed, no DB hit) for the given permission.
    /// Writes 401 and returns false when the key is missing or lacks the permission.
    /// </summary>
    private static bool Authorized(HttpContext ctx, ApiKeyPermissions required)
    {
        var validator = ctx.RequestServices.GetRequiredService<IApiKeyValidator>();
        var key = ApiKeyHeader.Extract(ctx.Request);
        if (key is not null && validator.Validate(key.AsSpan(), required)) return true;
        ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return false;
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

    // ── Body reading ──────────────────────────────────────────────────────────

    /// <summary>
    /// Rents a buffer from <see cref="ArrayPool{T}.Shared"/> and reads the full request body.
    /// Returns (null, 0) on error (status code already set). The caller MUST return the buffer
    /// to the pool via <c>ArrayPool&lt;byte&gt;.Shared.Return(buffer)</c> — use a finally block.
    /// </summary>
    private static async ValueTask<(byte[]? Buffer, int Length)> ReadBodyAsync(HttpContext ctx)
    {
        int maxBytes = ctx.RequestServices.GetRequiredService<Ameto.Core.ServerOptions>().Ingestion.MaxOtlpBatchBytes;

        long? declared = ctx.Request.ContentLength;
        if (declared > maxBytes)
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

                if (totalRead > maxBytes)
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
