using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Buffers.Text;
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
/// Both encodings are accepted: application/json and application/x-protobuf. Anything else
/// is read as JSON.
///
/// OTLP over gRPC lives in OtlpGrpcEndpointMapper.
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

    /// <param name="basePath">
    /// The deployment prefix, leading slash and no trailing one ("/ameto"), or empty at the root.
    /// Passed on to <see cref="AmetoIngestEndpoints"/>: with a prefix configured an exporter is
    /// pointed at <c>https://host/ameto/otlp/v1/traces</c>, and the self-ingest guard has to
    /// recognise that as its own receiver or the feedback loop it exists to break comes back.
    /// </param>
    public static void MapOtlpEndpoints(this WebApplication app, bool enableTraces = true, bool enableMetrics = true,
                                        string basePath = "")
    {
        AmetoIngestEndpoints.BasePath = basePath;

        // ── Traces ────────────────────────────────────────────────────────────
        var traces = async (HttpContext ctx, ISpanIngester ingester, ILoggerFactory logFactory) =>
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
            finally { IngestBufferPool.Return(body); }

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
        };

        // ── Metrics ───────────────────────────────────────────────────────────
        var metrics = async (HttpContext ctx) =>
        {
            if (!Authorized(ctx, ApiKeyPermissions.Metrics)) return;
            var ingester = ctx.RequestServices.GetRequiredService<IMetricIngester>();

            var (body, bodyLen) = await ReadBodyAsync(ctx);
            if (body is null) return;

            List<Ameto.Metrics.MetricIngestItem> points;
            try
            {
                bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;
                if (isProto)
                {
                    // Protobuf: parse straight to ingest items — no OTLP object graph, no
                    // parser object per nested message, no wire-int→string→int round trip
                    // (see OtlpMetricProtoParser). This is the encoding SDK exporters use.
                    points = OtlpMetricProtoParser.Parse(body.AsSpan(0, bodyLen));
                }
                else
                {
                    var request = JsonSerializer.Deserialize<ExportMetricsServiceRequest>(
                        body.AsSpan(0, bodyLen), _jsonOptions);
                    if (request is null) { ctx.Response.StatusCode = 400; return; }
                    points = OtlpMetricMapper.Map(request);
                }
            }
            catch { ctx.Response.StatusCode = 400; return; }
            finally { IngestBufferPool.Return(body); }

            int refused = ingester.Ingest(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(points));
            await WriteJsonOk(ctx, points.Count - refused, refused);
        };

        // ── Logs ──────────────────────────────────────────────────────────────
        var logs = async (HttpContext ctx) =>
        {
            if (!Authorized(ctx, ApiKeyPermissions.Logs)) return;
            var endpoint = ctx.RequestServices.GetRequiredService<IngestionEndpoint>();

            var (body, bodyLen) = await ReadBodyAsync(ctx);
            if (body is null) return;

            int ingested = 0, dropped = 0;
            try
            {
                bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;

                // Both encodings stream straight into the ring — no OTLP object graph, no
                // per-record LogEvent, no per-attribute strings. Protobuf is what SDK
                // exporters and the collector send, so it is the one that had to stop
                // decoding to a DOM first (see OtlpLogProtoParser).
                (ingested, dropped) = isProto
                    ? OtlpLogProtoParser.Parse(body.AsSpan(0, bodyLen), endpoint)
                    : OtlpLogStreamParser.Parse(body.AsSpan(0, bodyLen), endpoint);
            }
            catch { ctx.Response.StatusCode = 400; return; }
            finally { IngestBufferPool.Return(body); }

            await WriteJsonOk(ctx, ingested, dropped);
        };

        // ── Routes ────────────────────────────────────────────────────────────
        // Each handler is mapped at BOTH spellings. `/v1/…` is what the specification names and
        // what every exporter builds by appending to its configured endpoint, so a collector
        // pointed at this server with default settings asked for `/v1/logs` and got a 404 — the
        // receiver was reachable only by someone who had read this file. The `/otlp/v1/…` paths
        // stay because deployments are already configured with them; one handler serves both, so
        // the two cannot drift.
        app.MapPost("/otlp/v1/logs", logs);
        app.MapPost("/v1/logs",      logs);

        if (enableTraces)
        {
            app.MapPost("/otlp/v1/traces", traces);
            app.MapPost("/v1/traces",      traces);
        }

        if (enableMetrics)
        {
            app.MapPost("/otlp/v1/metrics", metrics);
            app.MapPost("/v1/metrics",      metrics);
        }

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
    /// Reads the full request body into a buffer from <see cref="IngestBufferPool"/>. The read
    /// itself is <see cref="OtlpBodyReader"/>, which the gRPC receiver shares — a body over
    /// <c>Ingestion.MaxOtlpBatchBytes</c> is refused there without ever renting past the ceiling.
    ///
    /// <para>Returns (null, 0) with the 413 already written, and nothing left rented, for a body
    /// over that limit. On success the caller MUST return the buffer via
    /// <see cref="IngestBufferPool.Return"/> — use a finally block.</para>
    /// </summary>
    private static async ValueTask<(byte[]? Buffer, int Length)> ReadBodyAsync(HttpContext ctx)
    {
        var body = await OtlpBodyReader.ReadAsync(
            ctx, ctx.RequestServices.GetRequiredService<Ameto.Core.ServerOptions>().Ingestion.MaxOtlpBatchBytes);

        // The one thing the two receivers do differently with a refusal: this one has an HTTP
        // status to say it in. The gRPC receiver says it in trailers, as RESOURCE_EXHAUSTED.
        if (body.Buffer is null) ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
        return body;
    }

    /// <summary>
    /// Writes <c>{"ingested":N,"dropped":M}</c> straight into the response pipe's own buffer.
    ///
    /// <para>It used to go through a <see cref="Utf8JsonWriter"/> constructed per request — an
    /// object, its rented state and its bookkeeping, for two integers in a fixed shape, on the
    /// reply to every ingest call the server answers. Two literals and
    /// <see cref="Utf8Formatter"/> produce the same bytes with nothing allocated at all.</para>
    /// </summary>
    private static async ValueTask WriteJsonOk(HttpContext ctx, int ingested, int dropped)
    {
        ctx.Response.StatusCode  = 200;
        ctx.Response.ContentType = JsonContentType;

        var writer = ctx.Response.BodyWriter;
        writer.Advance(FormatJsonOk(writer.GetSpan(JsonOkMaxBytes), ingested, dropped));
        await writer.FlushAsync(ctx.RequestAborted);
    }

    /// <summary>Longest the reply can be: both literals, two int32s and the closing brace.</summary>
    internal const int JsonOkMaxBytes = 12 + 11 + 1 + (11 * 2);

    /// <summary>Formats the reply into <paramref name="dest"/>; returns the byte count written.</summary>
    internal static int FormatJsonOk(Span<byte> dest, int ingested, int dropped)
    {
        int n = 0;
        "{\"ingested\":"u8.CopyTo(dest);                                    n += 12;
        Utf8Formatter.TryFormat(ingested, dest[n..], out int written);      n += written;
        ",\"dropped\":"u8.CopyTo(dest[n..]);                               n += 11;
        Utf8Formatter.TryFormat(dropped, dest[n..], out written);           n += written;
        dest[n++] = (byte)'}';
        return n;
    }
}
