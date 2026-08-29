using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Buffers;
using Ameto.Ingestion;
using Ameto.Metrics;
using Ameto.Tracing;

namespace Ameto.Otel;

/// <summary>
/// OTLP/gRPC receivers, hand-rolled onto ordinary POST routes.
///
/// <para>A collector's gRPC exporter calls three unary methods whose paths are simply
/// <c>/{package}.{Service}/{Method}</c>. The body of each is the same <c>Export…ServiceRequest</c>
/// protobuf the HTTP receivers already decode, behind a five-byte frame header — so these routes
/// reuse those decoders untouched rather than introducing a second, generated object model on the
/// ingest path.</para>
///
/// <para>gRPC reports failure in TRAILERS, not in the status line: everything below answers HTTP
/// 200 and puts <c>grpc-status</c> at the end of the response. A client reading an HTTP status
/// here would see success whatever went wrong, so the one thing this must never do is report an
/// outcome through <c>ctx.Response.StatusCode</c>.</para>
/// </summary>
public static class OtlpGrpcEndpointMapper
{
    /// <summary>One string for the one reason logs and traces refuse: TryIngest's bounded ring was full.</summary>
    private const string BufferFullReason = "the ingest buffer was full";

    private const string GrpcContentType = "application/grpc";

    // The few canonical gRPC status codes this receiver can produce.
    private const int StatusOk                = 0;
    private const int StatusInvalidArgument   = 3;
    private const int StatusResourceExhausted = 8;
    private const int StatusUnimplemented     = 12;
    private const int StatusUnauthenticated   = 16;

    public static void MapOtlpGrpcEndpoints(this WebApplication app, bool enableTraces = true, bool enableMetrics = true)
    {
        app.MapPost("/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            (HttpContext ctx) => HandleAsync(ctx, ApiKeyPermissions.Logs, static (c, msg) =>
            {
                var request = OtlpProtoDecoder.DecodeLogs(msg.Array!, msg.Offset + msg.Count);
                if (request is null) return (false, 0, null);
                var events = OtlpLogMapper.Map(request, Ameto.Core.NodeId.Local.Value);
                var (_, dropped) = c.RequestServices.GetRequiredService<IngestionEndpoint>().IngestEvents(events);
                return (true, dropped, BufferFullReason);
            }));

        if (enableTraces)
            app.MapPost("/opentelemetry.proto.collector.trace.v1.TraceService/Export",
                (HttpContext ctx) => HandleAsync(ctx, ApiKeyPermissions.Traces, static (c, msg) =>
                {
                    var request = OtlpProtoDecoder.DecodeTraces(msg.Array!, msg.Offset + msg.Count);
                    if (request is null) return (false, 0, null);
                    var spans = OtlpTraceMapper.Map(request);
                    if (spans.Count == 0) return (true, 0, null);
                    c.RequestServices.GetRequiredService<ISpanIngester>()
                     .TryIngest(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(spans), out int accepted);
                    return (true, spans.Count - accepted, BufferFullReason);
                }));

        if (enableMetrics)
            app.MapPost("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
                (HttpContext ctx) => HandleAsync(ctx, ApiKeyPermissions.Metrics, static (c, msg) =>
                {
                    var points  = OtlpMetricProtoParser.Parse(msg.AsSpan());
                    int refused = c.RequestServices.GetRequiredService<IMetricIngester>()
                     .Ingest(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(points));
                    return (true, refused, "points stamped more than 24 h in the future were refused");
                }));
    }

    /// <summary>
    /// The shape every Export call shares: check the content type, check the key, unframe, hand
    /// the protobuf to that signal's own decoder, and answer in trailers.
    /// </summary>
    private static async Task HandleAsync(
        HttpContext ctx,
        ApiKeyPermissions required,
        Func<HttpContext, ArraySegment<byte>, (bool Ok, int Rejected, string? Why)> decode)
    {
        // Committed up front: gRPC needs the headers out before trailers can be written, and a
        // client that never sees 200 + application/grpc treats the call as a transport failure
        // rather than reading the status this method is about to send.
        ctx.Response.StatusCode  = StatusCodes.Status200OK;
        ctx.Response.ContentType = GrpcContentType;

        // gRPC IS HTTP/2. Serving these paths over HTTP/1.1 looked harmless and was not: once a
        // response body has gone out on HTTP/1.1 there are no trailers to carry the status and no
        // headers left to write it into, so a SUCCESSFUL export answered 200 with a valid
        // response frame and no grpc-status anywhere — the one field a client reads. Refusing the
        // protocol outright is the only answer that can be delivered.
        if (!ctx.Request.Protocol.Equals("HTTP/2", StringComparison.OrdinalIgnoreCase))
        {
            await FinishAsync(ctx, StatusInvalidArgument, "OTLP/gRPC requires HTTP/2");
            return;
        }

        string? contentType = ctx.Request.ContentType;
        if (contentType is null || !contentType.StartsWith(GrpcContentType, StringComparison.OrdinalIgnoreCase))
        {
            await FinishAsync(ctx, StatusInvalidArgument, "expected content-type application/grpc");
            return;
        }

        if (!Authorized(ctx, required))
        {
            await FinishAsync(ctx, StatusUnauthenticated, "missing or insufficient API key");
            return;
        }

        var (body, bodyLen) = await ReadBodyAsync(ctx);
        if (body is null)
        {
            // A batch past Ingestion.MaxOtlpBatchBytes. RESOURCE_EXHAUSTED is the code a
            // collector backs off on; INVALID_ARGUMENT would make it retry the same oversized
            // batch for ever.
            await FinishAsync(ctx, StatusResourceExhausted, "batch exceeds the configured OTLP limit");
            return;
        }

        byte[]? inflated = null;
        try
        {
            int maxBytes = ctx.RequestServices.GetRequiredService<Ameto.Core.ServerOptions>().Ingestion.MaxOtlpBatchBytes;
            string? encoding = ctx.Request.Headers["grpc-encoding"];
            var unframed = OtlpGrpcFraming.TryUnframe(body.AsSpan(0, bodyLen), encoding, maxBytes,
                                                      out var message, out inflated, out int inflatedLen);
            if (unframed != UnframeResult.Ok)
            {
                switch (unframed)
                {
                    case UnframeResult.UnsupportedEncoding:
                        // Naming what we DO accept is what makes a client retry uncompressed
                        // instead of failing the batch outright.
                        ctx.Response.Headers["grpc-accept-encoding"] = "identity,gzip";
                        await FinishAsync(ctx, StatusUnimplemented, "compression '" + encoding + "' is not supported");
                        break;
                    case UnframeResult.TooLarge:
                        // Logged, not swallowed: a compressed batch that inflates past the limit
                        // is either a misconfigured exporter or someone probing, and both are
                        // worth being able to see afterwards.
                        ctx.RequestServices.GetRequiredService<ILoggerFactory>()
                           .CreateLogger("Ameto.Otel.Grpc")
                           .LogWarning("OTLP/gRPC: a compressed batch inflated past {Limit} bytes and was refused", maxBytes);
                        await FinishAsync(ctx, StatusResourceExhausted, "batch exceeds the configured OTLP limit");
                        break;
                    default:
                        await FinishAsync(ctx, StatusInvalidArgument, "malformed gRPC frame");
                        break;
                }
                return;
            }

            // The decoders take (buffer, length) and read from index 0, so an uncompressed
            // message — which sits five bytes into the request buffer — is copied down rather
            // than handed over at an offset they would misread.
            ArraySegment<byte> segment;
            if (inflated is not null)
            {
                segment = new ArraySegment<byte>(inflated, 0, inflatedLen);
            }
            else
            {
                body.AsSpan(OtlpGrpcFraming.HeaderBytes, message.Length).CopyTo(body);
                segment = new ArraySegment<byte>(body, 0, message.Length);
            }

            bool ok;
            int rejected;
            string? why;
            try
            {
                (ok, rejected, why) = decode(ctx, segment);
            }
            catch (Exception ex)
            {
                ctx.RequestServices.GetRequiredService<ILoggerFactory>()
                   .CreateLogger("Ameto.Otel.Grpc")
                   .LogWarning(ex, "OTLP/gRPC: failed to decode {Bytes} bytes", segment.Count);
                await FinishAsync(ctx, StatusInvalidArgument, "could not decode the payload");
                return;
            }

            if (!ok)
            {
                await FinishAsync(ctx, StatusInvalidArgument, "could not decode the payload");
                return;
            }

            // Accepted — and it says how much was dropped, and why: each signal's decode lambda
            // supplies its own reason (traces/logs: the ingest buffer was full; metrics: a
            // far-future timestamp), so this no longer reports a clean success — or the wrong
            // reason — while points were silently refused.
            await WriteMessageAsync(ctx, OtlpGrpcFraming.ExportResponse(
                rejected, rejected > 0 ? why : null));
            await FinishAsync(ctx, StatusOk, null);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(body);
            if (inflated is not null) ArrayPool<byte>.Shared.Return(inflated);
        }
    }

    private static async Task WriteMessageAsync(HttpContext ctx, byte[] message)
    {
        await ctx.Response.Body.WriteAsync(OtlpGrpcFraming.Frame(message), ctx.RequestAborted);
    }

    /// <summary>
    /// Writes the gRPC status.
    ///
    /// <para>Headers when nothing has been written yet — that is not a fallback, it is the
    /// protocol's own "Trailers-Only" response, which is what every failure here is. Trailers
    /// once a body has gone out. Choosing in that order matters: the previous version asked for
    /// the trailers feature first and fell back to headers only if the response had not started,
    /// so on a connection with no trailers a SUCCESSFUL export — body already written — landed in
    /// neither branch and answered with no grpc-status at all.</para>
    /// </summary>
    private static async Task FinishAsync(HttpContext ctx, int status, string? message)
    {
        if (!ctx.Response.HasStarted)
        {
            ctx.Response.Headers["grpc-status"] = status.ToString();
            if (message is not null) ctx.Response.Headers["grpc-message"] = message;
        }
        else if (ctx.Features.Get<IHttpResponseTrailersFeature>()?.Trailers is { IsReadOnly: false } trailers)
        {
            trailers["grpc-status"] = status.ToString();
            if (message is not null) trailers["grpc-message"] = message;
        }
        await ctx.Response.CompleteAsync();
    }

    // ── Shared with the HTTP receivers ────────────────────────────────────────

    private static bool Authorized(HttpContext ctx, ApiKeyPermissions required)
    {
        // gRPC metadata IS HTTP/2 headers, so the Seq-compatible extractor the HTTP receivers
        // use works unchanged — one definition of "a valid ingest key", not two that drift.
        var validator = ctx.RequestServices.GetRequiredService<IApiKeyValidator>();
        var key = ApiKeyHeader.Extract(ctx.Request);
        return key is not null && validator.Validate(key.AsSpan(), required);
    }

    private static async ValueTask<(byte[]? Buffer, int Length)> ReadBodyAsync(HttpContext ctx)
    {
        int maxBytes = ctx.RequestServices.GetRequiredService<Ameto.Core.ServerOptions>().Ingestion.MaxOtlpBatchBytes;

        long? declared = ctx.Request.ContentLength;
        if (declared > maxBytes) return (null, 0);

        // HTTP/2 rarely declares a length, so the usual path here is grow-by-doubling from
        // 64 KiB rather than the exact-size rent the HTTP receivers normally get.
        int initial = declared.HasValue ? (int)declared.Value : 65_536;
        byte[] buf = ArrayPool<byte>.Shared.Rent(Math.Max(initial, 256));
        int total = 0;
        try
        {
            while (true)
            {
                if (total == buf.Length)
                {
                    var bigger = ArrayPool<byte>.Shared.Rent(buf.Length * 2);
                    buf.AsSpan(0, total).CopyTo(bigger);
                    ArrayPool<byte>.Shared.Return(buf);
                    buf = bigger;
                }
                int read = await ctx.Request.Body.ReadAsync(buf.AsMemory(total), ctx.RequestAborted);
                if (read == 0) break;
                total += read;
                if (total > maxBytes) { ArrayPool<byte>.Shared.Return(buf); return (null, 0); }
            }
        }
        catch
        {
            // A reset stream, a client deadline, a dropped connection. Without this the rented
            // array is simply dropped: not a leak, but a permanent withdrawal from the pool the
            // CLEF path, the HTTP OTLP path and storage all share — and a collector timing out
            // mid-upload is an everyday event, not an exceptional one.
            ArrayPool<byte>.Shared.Return(buf);
            throw;
        }
        return (buf, total);
    }
}
