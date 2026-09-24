using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
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
/// Both content codings OTLP/HTTP names are accepted too: none (or identity), and gzip — the
/// collector's otlphttp exporter compresses by default, so without it a collector left on its
/// defaults could not deliver a single batch. Any other coding is 415.
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

    /// <summary>
    /// What the OTLP receivers — HTTP and gRPC — share across requests: the gate that bounds how
    /// many gzip bodies are held inflated at once (<see cref="OtlpInflateGate"/>). TryAdd, so a
    /// host that already registered one — a test with a smaller gate — keeps it.
    /// </summary>
    public static IServiceCollection AddOtlpReceivers(this IServiceCollection services)
    {
        services.TryAddSingleton(static sp => OtlpInflateGate.For(
            sp.GetRequiredService<Ameto.Core.ServerOptions>().Ingestion.MaxOtlpBatchBytes));
        return services;
    }

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

        // One logger for the life of the process, captured into the handler below. Taking
        // ILoggerFactory as a handler parameter had Minimal APIs resolve it and the handler call
        // CreateLogger on EVERY request — a lock and a walk of the provider list to hand back the
        // same logger — on the busiest route in the server.
        ILogger tracesLogger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Ameto.Otel.Traces");

        // The one gate both receivers inflate under (see OtlpInflateGate), resolved once here —
        // and REQUIRED, so a host that maps the receivers without AddOtlpReceivers fails at
        // startup rather than on its first compressed batch.
        OtlpInflateGate inflateGate = app.Services.GetRequiredService<OtlpInflateGate>();

        // ── Traces ────────────────────────────────────────────────────────────
        var traces = async (HttpContext ctx, ISpanSink sink) =>
        {
            if (!Authorized(ctx, ApiKeyPermissions.Traces)) return;

            var (body, bodyLen, slot) = await ReadBodyAsync(ctx, inflateGate);
            if (body is null) return;

            int ingested, refused;
            try
            {
                bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;

                // Both encodings STREAM straight into the span ring (TI#3) — no OTLP object graph,
                // no SpanIngestItem, no name string, no attribute array per span: the parser hands
                // each span over as slices of this body and the sink copies them into the ring's
                // arena. Protobuf is what SDK exporters and the collector send (see
                // OtlpTraceProtoParser). A malformed tail leaves its prefix ingested and answers
                // 400, which OTLP defines as not retryable — the log route's behaviour.
                (ingested, refused) = isProto
                    ? OtlpTraceProtoParser.Parse(body.AsSpan(0, bodyLen), sink)
                    : OtlpTraceStreamParser.Parse(body.AsSpan(0, bodyLen), sink);
            }
            catch (Exception ex)
            {
                LogTracesDecodeFailed(tracesLogger, bodyLen, ctx.Request.ContentType, ex);
                ctx.Response.StatusCode = 400;
                return;
            }
            finally { IngestBufferPool.Return(body); slot?.Exit(); }

            LogTracesDecoded(tracesLogger, ingested + refused);
            await WriteJsonOk(ctx, ingested, refused);
        };

        // ── Metrics ───────────────────────────────────────────────────────────
        var metrics = async (HttpContext ctx) =>
        {
            if (!Authorized(ctx, ApiKeyPermissions.Metrics)) return;
            var ingester = ctx.RequestServices.GetRequiredService<IMetricIngester>();

            var (body, bodyLen, slot) = await ReadBodyAsync(ctx, inflateGate);
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
            finally { IngestBufferPool.Return(body); slot?.Exit(); }

            int refused = ingester.Ingest(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(points));
            await WriteJsonOk(ctx, points.Count - refused, refused);
        };

        // ── Logs ──────────────────────────────────────────────────────────────
        var logs = async (HttpContext ctx) =>
        {
            if (!Authorized(ctx, ApiKeyPermissions.Logs)) return;
            var endpoint = ctx.RequestServices.GetRequiredService<IngestionEndpoint>();

            var (body, bodyLen, slot) = await ReadBodyAsync(ctx, inflateGate);
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
            finally { IngestBufferPool.Return(body); slot?.Exit(); }

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

    // ── Logging ───────────────────────────────────────────────────────────────

    /// <summary>
    /// The decoded-span-count line, pre-compiled.
    ///
    /// <para><c>logger.LogDebug("… {SpanCount} spans", spans.Count)</c> binds to
    /// <c>LoggerExtensions.LogDebug(ILogger, string, params object?[])</c>, which allocates the
    /// <c>object[1]</c> and boxes the int at the CALL SITE — before <c>IsEnabled</c> is ever
    /// consulted. At production log levels that is pure cost on every request to the busiest
    /// route in the process, for a line nobody will read.</para>
    ///
    /// <para><see cref="LoggerMessage.Define{T}"/> hands back a strongly typed delegate that
    /// asks <c>IsEnabled</c> first and formats nothing when the answer is no: 0 bytes, one
    /// virtual call. The same shape <c>SegmentIndexBuilder</c> already uses.</para>
    /// </summary>
    private static readonly Action<ILogger, int, Exception?> _tracesDecoded =
        LoggerMessage.Define<int>(
            Microsoft.Extensions.Logging.LogLevel.Debug,
            new Microsoft.Extensions.Logging.EventId(1, "OtlpTracesDecoded"),
            "OTLP /v1/traces: decoded {SpanCount} spans");

    private static readonly Action<ILogger, int, string?, Exception?> _tracesDecodeFailed =
        LoggerMessage.Define<int, string?>(
            Microsoft.Extensions.Logging.LogLevel.Warning,
            new Microsoft.Extensions.Logging.EventId(2, "OtlpTracesDecodeFailed"),
            "OTLP /v1/traces: failed to decode body ({Bytes} bytes, Content-Type: {Ct})");

    /// <summary>Internal so <c>OtlpTraceProtoProbe</c> can measure what it costs when Debug is off.</summary>
    internal static void LogTracesDecoded(ILogger logger, int spanCount)
        => _tracesDecoded(logger, spanCount, null);

    internal static void LogTracesDecodeFailed(ILogger logger, int bytes, string? contentType, Exception ex)
        => _tracesDecodeFailed(logger, bytes, contentType, ex);

    /// <summary>
    /// A gzip body that inflated past the limit — logged, not swallowed, as the gRPC receiver
    /// does: it is either a misconfigured exporter or someone probing, and both are worth being
    /// able to see afterwards. The 413 alone would tell the client and nobody else.
    /// </summary>
    private static readonly Action<ILogger, int, Exception?> _gzipTooLarge =
        LoggerMessage.Define<int>(
            Microsoft.Extensions.Logging.LogLevel.Warning,
            new Microsoft.Extensions.Logging.EventId(3, "OtlpHttpGzipTooLarge"),
            "OTLP/HTTP: a gzip body inflated past {Limit} bytes and was refused");

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
    /// Reads the full request body into a buffer from <see cref="IngestBufferPool"/> and undoes
    /// its <c>Content-Encoding</c>, so every handler below gets the protobuf or JSON message
    /// itself whichever way it travelled. The read is <see cref="OtlpBodyReader"/>, which the
    /// gRPC receiver shares — a body over <c>Ingestion.MaxOtlpBatchBytes</c> is refused there
    /// without ever renting past the ceiling — and the inflate is <see cref="OtlpGzip"/>, which
    /// it shares too, under the SAME ceiling applied to the inflated size.
    ///
    /// <para>Returns <c>default</c> — no buffer — with the refusal already written, and nothing
    /// left rented or held: 415 for a coding other than gzip or identity (before a byte of the
    /// body is read), 413 for a body over the limit on the wire or once inflated, 400 for gzip
    /// that does not inflate, 503 with <c>Retry-After</c> when every inflate slot stayed taken.
    /// On success the caller MUST return the buffer via <see cref="IngestBufferPool.Return"/>
    /// and then, when <c>Slot</c> is not null, <see cref="OtlpInflateGate.Exit"/> it — in a
    /// finally. The buffer is the inflated one when the body was compressed (the compressed one
    /// has already gone back), and <c>Slot</c> is the gate slot it holds: see
    /// <see cref="OtlpInflateGate"/> for why the slot lives as long as the buffer.</para>
    /// </summary>
    private static async ValueTask<(byte[]? Buffer, int Length, OtlpInflateGate? Slot)> ReadBodyAsync(
        HttpContext ctx, OtlpInflateGate gate)
    {
        // Decided before the body is read: bytes in a coding this receiver cannot undo could
        // only be refused after a buffer had been spent on them.
        ContentCoding coding = ClassifyContentEncoding(ctx.Request.Headers.ContentEncoding);
        if (coding == ContentCoding.Unsupported)
        {
            WriteUnsupportedEncoding(ctx);
            await ctx.Response.BodyWriter.FlushAsync(ctx.RequestAborted);
            return default;
        }

        int maxBytes = ctx.RequestServices.GetRequiredService<Ameto.Core.ServerOptions>().Ingestion.MaxOtlpBatchBytes;
        var (buffer, length) = await OtlpBodyReader.ReadAsync(ctx, maxBytes);

        // The one thing the two receivers do differently with a refusal: this one has an HTTP
        // status to say it in. The gRPC receiver says it in trailers, as RESOURCE_EXHAUSTED.
        if (buffer is null)
        {
            ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
            return default;
        }

        // gzip over NOTHING is an empty message, exactly as an uncompressed empty body is: there
        // is no member to inflate, and GZipStream would read the same zero bytes after allocating
        // itself to find that out. What an empty message answers is the parser's business.
        // Neither road inflates, so neither takes a slot.
        if (coding == ContentCoding.Identity || length == 0) return (buffer, length, null);

        // A slot before the inflate buffer exists — and the compressed buffer back on every way
        // out that does not reach InflateBody, which returns it itself.
        bool entered;
        try { entered = await gate.TryEnterAsync(ctx.RequestAborted); }
        catch { IngestBufferPool.Return(buffer); throw; }
        if (!entered)
        {
            IngestBufferPool.Return(buffer);
            WriteRetryLater(ctx, GateFullMessage);
            return default;
        }

        return InflateBody(ctx, buffer, length, maxBytes, gate);
    }

    /// <summary>
    /// The gzip road: inflates into a second pooled buffer under the same
    /// <c>MaxOtlpBatchBytes</c> ceiling the wire read used, then gives the compressed one back —
    /// on every outcome, since nothing downstream reads compressed bytes. Entered holding a slot
    /// of <paramref name="gate"/>: the slot leaves with the inflated buffer, or goes back here.
    ///
    /// <para>What the ceiling means here is what #57 established for gRPC and what this issue
    /// (#82) had to keep: it bounds the INFLATED size and is decided on bytes already written,
    /// so a body of a few hundred KB that would inflate at ~1032:1 is stopped after one limit
    /// of output — never the gigabytes it describes. No rent passes the limit; held at once,
    /// the request is the compressed body plus one limit of inflate buffer when the trailer is
    /// honest, one and a half when it understates and the buffer doubles into the limit
    /// (about 20 MiB at the 8 MiB default) — and the gate bounds how many requests do that at
    /// once. Both buffers are from <see cref="IngestBufferPool"/>, so at the steady state an
    /// accepted compressed batch allocates nothing but the inflater itself.</para>
    /// </summary>
    private static (byte[]? Buffer, int Length, OtlpInflateGate? Slot) InflateBody(
        HttpContext ctx, byte[] compressed, int compressedLength, int maxBytes, OtlpInflateGate gate)
    {
        InflateResult result;
        byte[]? inflated;
        int inflatedLength;
        try
        {
            result = OtlpGzip.Inflate(compressed.AsMemory(0, compressedLength), maxBytes, out inflated, out inflatedLength);
        }
        catch
        {
            gate.Exit();
            throw;
        }
        finally
        {
            IngestBufferPool.Return(compressed);
        }

        if (result == InflateResult.Ok) return (inflated, inflatedLength, gate);   // the slot goes with the buffer
        gate.Exit();

        if (result == InflateResult.TooLarge)
        {
            // The same answer as a body that was too big on the wire — to the client they are
            // one condition, "this batch is over the limit", and one remedy: split it.
            _gzipTooLarge(ctx.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("Ameto.Otel.Http"),
                          maxBytes, null);
            ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
        }
        else
        {
            // Not gzip, truncated, or a trailer that disagrees with what it inflated to: the
            // client sent bytes nobody can read, which is a 400 — never a 500, and nothing was
            // parsed, so unlike a malformed message there is no ingested prefix to warn about.
            ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        }
        return default;
    }

    // ── 503: retry later ──────────────────────────────────────────────────────

    /// <summary>What a 503 says when every inflate slot stayed taken.</summary>
    internal static ReadOnlySpan<byte> GateFullMessage => "the server is inflating as many gzip batches as it can hold; retry"u8;

    /// <summary>
    /// How long a 503 asks the exporter to wait. One second: a slot is held for one inflate and
    /// one parse, milliseconds for an ordinary batch, so the gate is rarely full for longer — and
    /// an exporter backs off on its own schedule past this anyway.
    /// </summary>
    internal const string RetryAfterSeconds = "1";

    /// <summary>
    /// The retryable refusal: 503, <c>Retry-After</c>, and the OTLP failure shape carrying
    /// <paramref name="message"/>. 503 is one of the statuses the OTLP/HTTP specification tells
    /// an exporter to retry — with backoff, honouring <c>Retry-After</c> — so the batch is
    /// delayed, not lost. Written without an await; the body goes out as the request completes.
    /// </summary>
    private static void WriteRetryLater(HttpContext ctx, ReadOnlySpan<byte> message)
    {
        var response = ctx.Response;
        bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;

        response.StatusCode         = StatusCodes.Status503ServiceUnavailable;
        response.Headers.RetryAfter = RetryAfterSeconds;
        response.ContentType        = isProto ? ProtobufContentType : JsonContentType;

        var writer = response.BodyWriter;
        writer.Advance(FormatStatus(writer.GetSpan(StatusBodyMaxBytes), message, isProto));
    }

    // ── Content-Encoding ──────────────────────────────────────────────────────

    /// <summary>What a request's Content-Encoding asks this receiver to undo.</summary>
    internal enum ContentCoding
    {
        /// <summary>No header, an empty one, or only <c>identity</c>: the body is the message.</summary>
        Identity,
        /// <summary>Exactly one gzip (<c>x-gzip</c> is its registered alias).</summary>
        Gzip,
        /// <summary>Anything else — deflate, br, zstd, a typo, or gzip applied twice.</summary>
        Unsupported,
    }

    /// <summary>
    /// Reads the header as the list RFC 9110 says it is — comma-separated, in any case, over one
    /// or several header lines — without allocating: the values are Kestrel's strings, walked as
    /// spans. <c>identity</c> and empty members change nothing and are skipped.
    ///
    /// <para>Gzip applied TWICE is refused rather than inflated twice: no exporter does it, and
    /// each pass would need its own limit-sized buffer. Refusing is the answer that keeps the
    /// memory bound a single buffer.</para>
    /// </summary>
    internal static ContentCoding ClassifyContentEncoding(StringValues header)
    {
        int gzip = 0;
        foreach (string? value in header)
        {
            ReadOnlySpan<char> list = value;
            foreach (Range member in list.Split(','))
            {
                ReadOnlySpan<char> coding = list[member].Trim();
                if (coding.IsEmpty || coding.Equals("identity", StringComparison.OrdinalIgnoreCase)) continue;
                if (coding.Equals("gzip", StringComparison.OrdinalIgnoreCase)
                 || coding.Equals("x-gzip", StringComparison.OrdinalIgnoreCase))
                {
                    gzip++;
                    continue;
                }
                return ContentCoding.Unsupported;
            }
        }
        return gzip switch
        {
            0 => ContentCoding.Identity,
            1 => ContentCoding.Gzip,
            _ => ContentCoding.Unsupported,
        };
    }

    /// <summary>
    /// The codings a 415 names as acceptable, in <c>Accept-Encoding</c> — which is how RFC 9110
    /// (§15.5.16) says a server refusing a content coding should tell the client what would
    /// have worked. One interned literal, so setting it allocates nothing.
    /// </summary>
    internal const string AcceptedContentEncodings = "gzip, identity";

    /// <summary>
    /// The 415, written without an await so the formatting can use spans: status,
    /// <c>Accept-Encoding</c>, and a body that is the OTLP failure shape — a
    /// <c>google.rpc.Status</c> whose <c>message</c> says in words what was refused, encoded
    /// like the request (protobuf for protobuf, JSON otherwise), as the OTLP/HTTP specification
    /// asks of every 4xx. The collector's exporter decodes exactly that and prints the message
    /// in its own log, which is where an operator who left compression on something other than
    /// gzip will be looking.
    /// </summary>
    private static void WriteUnsupportedEncoding(HttpContext ctx)
    {
        var response = ctx.Response;
        bool isProto = ctx.Request.ContentType?.StartsWith(ProtobufContentType, StringComparison.OrdinalIgnoreCase) ?? false;

        response.StatusCode             = StatusCodes.Status415UnsupportedMediaType;
        response.Headers.AcceptEncoding = AcceptedContentEncodings;
        response.ContentType            = isProto ? ProtobufContentType : JsonContentType;

        var writer = response.BodyWriter;
        writer.Advance(FormatUnsupportedEncoding(
            writer.GetSpan(UnsupportedEncodingMaxBytes), ctx.Request.Headers.ContentEncoding, isProto));
    }

    /// <summary>Longest stretch of the client's header echoed back — enough for any real coding list.</summary>
    internal const int EchoMaxChars = 64;

    /// <summary>The message, at its longest: both literals and a full echo. Under 128, so its protobuf length is one byte.</summary>
    private const int UnsupportedMessageMaxBytes = 18 + EchoMaxChars + 41;

    /// <summary>Longest message any refusal here carries — under 128, so its protobuf length is one byte.</summary>
    internal const int StatusMessageMaxBytes = 127;

    /// <summary>A refusal body at its longest: the JSON framing (<c>{"message":"</c> and <c>"}</c>) is the larger of the two.</summary>
    internal const int StatusBodyMaxBytes = 12 + StatusMessageMaxBytes + 2;

    /// <summary>The 415 body at its longest.</summary>
    internal const int UnsupportedEncodingMaxBytes = StatusBodyMaxBytes;

    /// <summary>
    /// Formats the 415 body into <paramref name="dest"/>; returns the byte count written.
    ///
    /// <para>The header is echoed so the text names what was actually sent — "deflate" and
    /// "gzip, br" are different mistakes — but only as printable ASCII, capped at
    /// <see cref="EchoMaxChars"/>, and with the quote and backslash that would end or escape
    /// a JSON string replaced: it is the client's own input, and it goes into a body.</para>
    /// </summary>
    internal static int FormatUnsupportedEncoding(Span<byte> dest, StringValues contentEncoding, bool protobuf)
    {
        ReadOnlySpan<byte> head = "Content-Encoding '"u8;
        ReadOnlySpan<byte> tail = "' is not supported; send gzip or identity"u8;

        Span<byte> message = stackalloc byte[UnsupportedMessageMaxBytes];
        head.CopyTo(message);
        int n = head.Length;
        n += Echo(message.Slice(n, EchoMaxChars), contentEncoding);
        tail.CopyTo(message[n..]);
        n += tail.Length;

        return FormatStatus(dest, message[..n], protobuf);
    }

    /// <summary>
    /// A refusal body in the OTLP failure shape: a <c>google.rpc.Status</c> with only its
    /// <c>message</c>, encoded like the request — protobuf for protobuf, JSON otherwise. The
    /// message must be printable ASCII without quote or backslash (every caller's is: literals,
    /// or the sanitised echo) and at most <see cref="StatusMessageMaxBytes"/>, so its protobuf
    /// length is one byte and its JSON needs no escaping.
    /// </summary>
    internal static int FormatStatus(Span<byte> dest, ReadOnlySpan<byte> message, bool protobuf)
    {
        int o = 0;
        if (protobuf)
        {
            dest[o++] = 0x12;                                                  // Status.message = 2, length-delimited
            dest[o++] = (byte)message.Length;                                  // < 128: a one-byte varint
            message.CopyTo(dest[o..]);
            return o + message.Length;
        }

        ReadOnlySpan<byte> open = "{\"message\":\""u8;
        open.CopyTo(dest);
        o = open.Length;
        message.CopyTo(dest[o..]);
        o += message.Length;
        "\"}"u8.CopyTo(dest[o..]);
        return o + 2;
    }

    /// <summary>The header's values joined by ", " into <paramref name="dest"/>, sanitised and cut at its length.</summary>
    private static int Echo(Span<byte> dest, StringValues values)
    {
        int n = 0;
        for (int v = 0; v < values.Count; v++)
        {
            ReadOnlySpan<char> text = values[v];
            if (v > 0)
            {
                if (n < dest.Length) dest[n++] = (byte)',';
                if (n < dest.Length) dest[n++] = (byte)' ';
            }
            for (int i = 0; i < text.Length && n < dest.Length; i++)
            {
                char c = text[i];
                dest[n++] = c is >= ' ' and <= '~' and not '"' and not '\\' and not '\'' ? (byte)c : (byte)'?';
            }
        }
        return n;
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
