using System.Buffers;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Storage;

namespace Ameto.Ingestion;

/// <summary>
/// Sink for the zero-alloc OTLP streaming parser: one already-msgpack-encoded record at a
/// time, straight into the ring. Implemented by <see cref="IngestionEndpoint"/>; abstracted
/// so the parser can be unit-tested against a capturing fake.
/// </summary>
public interface IOtlpLogSink
{
    bool TryIngestRaw(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8);

    /// <summary>
    /// Interns a <c>service.name</c> ONCE for a whole resourceLogs block and returns its
    /// pool index, so the parser does not re-hash the same name for every record under it
    /// (the name is a property of the resource, and a block carries hundreds of records).
    /// Returns -1 when there is nothing to intern or the sink has no pool.
    /// </summary>
    int InternService(ReadOnlySpan<byte> serviceUtf8) => -1;

    /// <summary>
    /// As <see cref="TryIngestRaw(long, byte, ReadOnlySpan{byte}, ReadOnlySpan{byte}, ulong, ulong, ulong, ReadOnlySpan{byte})"/>,
    /// with the service already interned by <see cref="InternService"/>. A negative
    /// <paramref name="serviceIdx"/> means "not interned yet — do it from the span".
    /// </summary>
    bool TryIngestRaw(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx)
        => TryIngestRaw(tsTicks, level, templateUtf8, msgpackProps, traceHi, traceLo, spanId, serviceUtf8);

    void NotifyBatchEnqueued();
}

/// <summary>
/// Handles POST /api/events
///
/// Wire format: MessagePack array of CLEF maps.
///   [ { "@t": "...", "@mt": "...", "@l": "...", "Prop": value, ... }, ... ]
///
/// Processing:
///   1. Read body into a pooled buffer.
///   2. Deserialise each CLEF event using <see cref="LogEventSerializer"/>.
///   3. Intern the message template via <see cref="StringInternPool"/>.
///   4. Re-serialise the properties-only map and push to <see cref="IngestionRingBuffer"/>.
///
/// Returns (every 200 and 400 is application/json with the counts):
///   200 OK          { "ingested": N, "dropped": M }
///   400 Bad Request { "ingested": N, "dropped": M, "failedAtElement": K }
///          The body stopped being a CLEF array at element K. The N events before it are
///          ALREADY INGESTED and stay so. failedAtElement is omitted when the body failed
///          before any element, at the array header (not an array, or empty).
///   400 Bad Request { "ingested": 0, "dropped": 0 }
///          The body ended short of its Content-Length on a stream that ends rather than
///          throws (see HandleAsync 1b). Kestrel fails that read itself.
///   413 Payload Too Large, no body, above <see cref="IngestionOptions.MaxBatchBytes"/>
///   500 A fault in the server underneath (ring, intern pool, logger, shutdown
///          mid-batch). It leaves the handler and hosting answers it; see IsMalformedPayload.
/// </summary>
public sealed class IngestionEndpoint : IOtlpLogSink, LogEventSerializer.IClefBatchSink
{
    private readonly IngestionRingBuffer     _ring;
    private readonly StringInternPool        _pool;
    private readonly IngestionDrainer        _drainer;
    private readonly ILogger<IngestionEndpoint> _logger;

    /// <summary>Max HTTP body bytes for one CLEF batch — 413 above this. From config.</summary>
    private readonly int _maxBatchBytes;

    /// <summary>Max properties bytes per event — matches the ring slab size. From config.</summary>
    private readonly int _maxEventPayloadBytes;

    public IngestionEndpoint(
        IngestionRingBuffer ring,
        StringInternPool pool,
        IngestionDrainer drainer,
        ServerOptions options,
        ILogger<IngestionEndpoint> logger)
    {
        _ring    = ring;
        _pool    = pool;
        _drainer = drainer;
        _logger  = logger;
        _maxBatchBytes        = options.Ingestion.MaxBatchBytes;
        _maxEventPayloadBytes = options.Ingestion.MaxEventPayloadBytes;
    }

    public async Task HandleAsync(HttpContext ctx)
    {
        // ── 1. Read body into a pooled buffer ─────────────────────────────────
        long? contentLength = ctx.Request.ContentLength;
        if (contentLength > _maxBatchBytes)
        {
            ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
            return;
        }

        // Rented, and straight into the try: a read that THROWS must still give the buffer back.
        // Kestrel raises BadHttpRequestException for a body that ends short of its
        // Content-Length, and RequestAborted cancels a read. Both used to fire before the try
        // opened, so the rented array was never returned. Every exit below, the 413 included,
        // returns it exactly once: in the finally.
        byte[] bodyBuf = ArrayPool<byte>.Shared.Rent(
            contentLength.HasValue ? Math.Max((int)contentLength.Value, 1) : 64 * 1024);
        int    bodyLen = 0;
        try
        {
            if (contentLength.HasValue)
            {
                int expected = (int)contentLength.Value;
                while (bodyLen < expected)
                {
                    int n = await ctx.Request.Body.ReadAsync(
                        bodyBuf.AsMemory(bodyLen, expected - bodyLen), ctx.RequestAborted);
                    if (n == 0) break;
                    bodyLen += n;
                }
            }
            else
            {
                while (true)
                {
                    int n = await ctx.Request.Body.ReadAsync(
                        bodyBuf.AsMemory(bodyLen), ctx.RequestAborted);
                    if (n == 0) break;
                    bodyLen += n;

                    if (bodyLen > _maxBatchBytes)
                    {
                        ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
                        return;   // the finally returns the buffer
                    }

                    if (bodyLen == bodyBuf.Length)
                    {
                        var bigger = ArrayPool<byte>.Shared.Rent(bodyBuf.Length * 2);
                        Buffer.BlockCopy(bodyBuf, 0, bigger, 0, bodyLen);
                        ArrayPool<byte>.Shared.Return(bodyBuf);
                        bodyBuf = bigger;   // nothing between the Return and here can throw
                    }
                }
            }

            // ── 1b. A body that never arrived in full is refused whole ────────
            // Defense in depth, NOT the Kestrel path. On Kestrel a body short of its
            // Content-Length never gets here: ReadAsync throws BadHttpRequestException
            // ("Unexpected end of request content") above, whether the client sends FIN or
            // RST, and hosting answers it. This guard is for a body stream that just ends
            // early instead of throwing — TestServer, or another server. On such a stream the
            // prefix parses perfectly up to the cut, so streaming it would ingest the prefix
            // and then answer 400. Seq clients retry a non-2xx (Serilog.Sinks.Seq throws, and
            // its batching sink retries), and the prefix would land again on every retry.
            // A body that arrives IN FULL and is malformed in the middle is the residual case:
            // it still ingests the prefix, see StreamBatch.
            if (contentLength.HasValue && bodyLen != (int)contentLength.Value)
            {
                _logger.LogDebug(
                    "Truncated ingestion body: {Received} of {Expected} bytes — batch refused whole",
                    bodyLen, contentLength.Value);
                // The same counts shape every other /api/events reply has: nothing landed.
                ctx.Response.StatusCode  = StatusCodes.Status400BadRequest;
                ctx.Response.ContentType = "application/json";
                WriteCountsJson(ctx.Response.BodyWriter, ingested: 0, dropped: 0);
                await ctx.Response.BodyWriter.FlushAsync(ctx.RequestAborted);
                return;
            }

            // ── 2+3. Stream the MessagePack array straight into the ring ──────
            // No LogEvent per event: the batch reader hands each event over as spans
            // into bodyBuf, and TryIngestClef copies the property bytes into the ring
            // slot. A body that is not a CLEF array is rejected before any event is
            // seen; one that turns malformed part way through answers 400 with the
            // intact prefix already ingested (see StreamBatch).
            var progress = default(LogEventSerializer.ClefBatchProgress);
            try
            {
                LogEventSerializer.StreamBatch(bodyBuf.AsMemory(0, bodyLen), this, ref progress);
            }
            // Classified by WHERE it was thrown, not by its type. This used to be
            // catch(Exception), which also swallowed failures of the sink underneath — a ring or
            // intern-pool fault, or the ObjectDisposedException a shutdown mid-batch raises —
            // and reported them to the client as "malformed payload"; those surface as 500. A
            // list of reader exception types was tried and was short on day one: a str32/bin32/
            // ext32 length prefix of 2^31 or more throws OverflowException, and went out as 500.
            catch (Exception ex) when (IsMalformedPayload(ex, progress.InSink))
            {
                // The prefix is already in the ring. Wake the drainer for it, or it sits
                // there until the drain loop's 1 s missed-signal timeout.
                if (progress.Ingested > 0)
                    _drainer.NotifyEnqueued();

                // Warning, not Debug, and with the counts: "some of that batch landed and
                // some of it did not" is an operator's problem, and the element index is
                // what makes it findable in the sender. ElementIndex -1 is the array header itself
                // (the reply then omits failedAtElement).
                _logger.LogWarning(ex,
                    "Malformed ingestion payload at element {ElementIndex} (-1 = the array header) of {ElementCount}: "
                  + "{Ingested} event(s) already ingested, {Dropped} dropped — batch refused",
                    progress.ElementIndex, progress.ElementCount, progress.Ingested, progress.Dropped);

                ctx.Response.StatusCode  = StatusCodes.Status400BadRequest;
                ctx.Response.ContentType = "application/json";
                WriteCountsJson(ctx.Response.BodyWriter, progress.Ingested, progress.Dropped,
                                failedAtElement: progress.ElementIndex);
                await ctx.Response.BodyWriter.FlushAsync(ctx.RequestAborted);
                return;
            }

            if (progress.Ingested > 0)
                _drainer.NotifyEnqueued();

            // ── 4. Response ───────────────────────────────────────────────────
            ctx.Response.StatusCode  = StatusCodes.Status200OK;
            ctx.Response.ContentType = "application/json";
            WriteCountsJson(ctx.Response.BodyWriter, progress.Ingested, progress.Dropped);
            await ctx.Response.BodyWriter.FlushAsync(ctx.RequestAborted);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bodyBuf);
        }
    }

    /// <summary>
    /// Whether a throw out of StreamBatch is the client's (400) or the server's (500), decided
    /// by where it happened. <paramref name="inSink"/> is
    /// <see cref="LogEventSerializer.ClefBatchProgress.InSink"/> as the throw left it.
    ///
    /// <para>Set: the fault came out of <see cref="TryIngestClef"/> — the ring, the intern
    /// pool, the logger, a shutdown mid-batch — and is the server's, whatever its type.</para>
    ///
    /// <para>Clear: the reader threw while walking the body, and the body is at fault whatever
    /// the TYPE. MessagePackReader throws MessagePackSerializationException for a code it cannot
    /// read, EndOfStreamException for a body that ends inside an element, and OverflowException
    /// for a str32/bin32/ext32 length prefix of 2^31 or more. A list of those types is one
    /// reader upgrade away from being short again. The one exception is
    /// <see cref="OutOfMemoryException"/>, which says nothing about the body.</para>
    /// </summary>
    internal static bool IsMalformedPayload(Exception ex, bool inSink)
        => !inSink && ex is not OutOfMemoryException;

    /// <summary>
    /// Writes <c>{"ingested":N,"dropped":M}</c> into the response buffer with no string in
    /// between — plus <c>,"failedAtElement":K</c> when the batch was refused part way, so a
    /// sender reading the 400 can tell how much of it landed and where it stopped. The
    /// interpolated form allocated the formatted string, a char[] from the handler, and then
    /// a UTF-8 transcode of it — ~300 B per request, for 30-odd bytes of constant-shaped
    /// JSON. Every count is a non-negative int, so the longest possible body is well under
    /// the requested span.
    /// </summary>
    private static void WriteCountsJson(
        System.IO.Pipelines.PipeWriter writer, int ingested, int dropped, int failedAtElement = -1)
    {
        const int MaxLen = 24 + 19 + 11 + 11 + 11; // literals + three int32s at their widest
        Span<byte> span = writer.GetSpan(MaxLen);
        int pos = 0;

        "{\"ingested\":"u8.CopyTo(span);
        pos += 12;
        System.Buffers.Text.Utf8Formatter.TryFormat(ingested, span[pos..], out int written);
        pos += written;

        ",\"dropped\":"u8.CopyTo(span[pos..]);
        pos += 11;
        System.Buffers.Text.Utf8Formatter.TryFormat(dropped, span[pos..], out written);
        pos += written;

        if (failedAtElement >= 0)
        {
            ",\"failedAtElement\":"u8.CopyTo(span[pos..]);
            pos += 19;
            System.Buffers.Text.Utf8Formatter.TryFormat(failedAtElement, span[pos..], out written);
            pos += written;
        }

        span[pos++] = (byte)'}';
        writer.Advance(pos);
    }

    /// <summary>
    /// Directly enqueue pre-decoded <see cref="LogEvent"/> objects into the ring buffer.
    /// Used by the OTLP adapter to bypass HTTP parsing while reusing the same storage path.
    /// Returns (ingested, dropped) counts.
    /// </summary>
    public (int Ingested, int Dropped) IngestEvents(IReadOnlyList<LogEvent> events)
    {
        int ingested = 0, dropped = 0;
        for (int i = 0; i < events.Count; i++)
            TryIngest(events[i], ref ingested, ref dropped);

        if (ingested > 0)
            _drainer.NotifyEnqueued();

        return (ingested, dropped);
    }

    /// <summary>
    /// Zero-alloc streaming ingest of one already-msgpack-encoded log record straight into
    /// the ring — no <see cref="LogEvent"/> object. Interns template/service directly from
    /// UTF-8 spans (no string allocation on a cache hit). Drives the OTLP JSON streaming
    /// parser. Returns true if ingested, false if dropped (oversized or back-pressure).
    /// Call <see cref="NotifyBatchEnqueued"/> once after a batch.
    /// </summary>
    public bool TryIngestRaw(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8)
        => TryIngestRaw(tsTicks, level, templateUtf8, msgpackProps, traceHi, traceLo, spanId, serviceUtf8, serviceIdx: -1);

    /// <inheritdoc/>
    public int InternService(ReadOnlySpan<byte> serviceUtf8) => _pool.Intern(serviceUtf8);

    /// <inheritdoc/>
    public bool TryIngestRaw(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx)
    {
        // The service name belongs to the resourceLogs block, not the record: when the
        // parser has already interned it, every record under that block skips the
        // UTF-8 decode + hash that used to run once per record.
        if (serviceIdx < 0) serviceIdx = _pool.Intern(serviceUtf8);   // -1 when empty

        if (msgpackProps.Length > _maxEventPayloadBytes)
        {
            // Drop the oversized original, but leave a compact Error breadcrumb in
            // the stream so the loss is visible on the Events page — not only in
            // the server's own log. Marked DroppedBy=server to distinguish it from
            // the client sink's own oversized marker.
            string origTmpl = templateUtf8.IsEmpty ? string.Empty : System.Text.Encoding.UTF8.GetString(templateUtf8);
            EnqueueServerDropMarker(tsTicks, level, origTmpl, msgpackProps.Length, traceHi, traceLo, spanId, serviceIdx);
            _ring.CountOversizedDrop();   // the drop happens HERE, before the ring sees it
            return false; // original counted as dropped by the caller
        }

        // Intern hands back the pool's OWN instance, so the tier stores the shared string
        // rather than a per-event duplicate — and one dictionary probe does the work of two.
        int     tmplIdx = _pool.Intern(templateUtf8, out string canonicalTmpl); // -1 when empty
        // Attach on CONTENT, not on the index. A full pool answers -1 with the template
        // materialised in canonicalTmpl, and past that point the attached string is the only
        // copy of it anywhere: the header says -1 and pool.Get(-1) is "". Keying this on
        // tmplIdx >= 0 dropped @mt from every event ingested after saturation. Only an empty
        // template attaches null — never "", which the tier would prefer over the pool.
        string? tmpl    = canonicalTmpl.Length != 0 ? canonicalTmpl : null;

        return _ring.TryEnqueue(
            tsTicks, level, tmplIdx, tmpl, exception: null,
            msgpackProps, traceHi, traceLo, spanId, serviceIdx);
    }

    /// <summary>Wakes the drainer once after a streaming batch (see <see cref="TryIngestRaw"/>).</summary>
    public void NotifyBatchEnqueued() => _drainer.NotifyEnqueued();

    /// <summary>
    /// Streaming CLEF ingest — one event straight from the request body into the ring, with
    /// no <see cref="LogEvent"/> in between. Same duties as <see cref="TryIngest"/>
    /// (oversized warning + server drop marker, template/service interning, ring enqueue),
    /// but everything arrives as spans over the pooled body buffer.
    /// </summary>
    public bool TryIngestClef(
        long tsTicks,
        byte level,
        ReadOnlySpan<byte> templateUtf8,
        ExceptionInfo? exception,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8)
    {
        if (msgpackProps.Length > _maxEventPayloadBytes)
        {
            // Cold path: the strings here are worth their cost — this is one log line and
            // one marker event per dropped event, not per event.
            string  tmplStr = templateUtf8.IsEmpty ? string.Empty : System.Text.Encoding.UTF8.GetString(templateUtf8);
            string? svcStr  = serviceUtf8.IsEmpty  ? null         : System.Text.Encoding.UTF8.GetString(serviceUtf8);

            _ring.CountOversizedDrop();   // the drop happens HERE, before the ring sees it
            _logger.LogWarning(
                "Dropped oversized log event: properties {PayloadBytes} B exceed limit {LimitBytes} B (service={Service}, template=\"{Template}\")",
                msgpackProps.Length, _maxEventPayloadBytes, svcStr ?? "(none)", Truncate(tmplStr, 120));
            EnqueueServerDropMarker(
                tsTicks, level, tmplStr, msgpackProps.Length, traceHi, traceLo, spanId,
                svcStr is not null ? _pool.Intern(svcStr) : -1);
            return false;
        }

        // Intern returns the pool's own instance, so the hot tier shares one string per
        // template instead of retaining this event's copy (see TryIngest).
        int     tmplIdx  = _pool.Intern(templateUtf8, out string canonical); // -1 when empty
        string? tmpl     = canonical.Length != 0 ? canonical : null;   // see TryIngestRaw: kept past saturation, never ""
        int svcIdx  = _pool.Intern(serviceUtf8);                     // -1 when empty

        return _ring.TryEnqueue(
            tsTicks, level, tmplIdx, tmpl, exception,
            msgpackProps, traceHi, traceLo, spanId, svcIdx);
    }

    /// <summary>
    /// Interns strings and pushes one event onto the ring, tallying ingested/dropped.
    /// An event whose properties blob exceeds <see cref="_maxEventPayloadBytes"/> is
    /// rejected up-front WITH a warning (size + reason) — the ring would otherwise drop
    /// it silently. Ring-full / pool-exhausted back-pressure is still counted as dropped
    /// but not logged per event, since that path is high-volume under overload.
    /// </summary>
    private void TryIngest(LogEvent ev, ref int ingested, ref int dropped)
    {
        int payloadLen = ev.RawProperties.Length;
        if (payloadLen > _maxEventPayloadBytes)
        {
            dropped++;
            _ring.CountOversizedDrop();   // the drop happens HERE, before the ring sees it
            _logger.LogWarning(
                "Dropped oversized log event: properties {PayloadBytes} B exceed limit {LimitBytes} B (service={Service}, template=\"{Template}\")",
                payloadLen, _maxEventPayloadBytes, ev.ServiceName ?? "(none)", Truncate(ev.MessageTemplate, 120));
            // Also record it in the stream as an Error marker so it surfaces on the
            // Events page, not just in the server log.
            EnqueueServerDropMarker(
                ev.Timestamp.UtcTicks, (byte)ev.Level, ev.MessageTemplate ?? string.Empty,
                payloadLen, ev.TraceIdHi, ev.TraceIdLo, ev.SpanId,
                ev.ServiceName is not null ? _pool.Intern(ev.ServiceName) : -1);
            return;
        }

        // The template that reaches the ring must be the POOL's instance, not this event's
        // fresh one: the hot tier keeps it alive for the whole life of the tier, so a
        // per-event duplicate is ~100 B/event of gen2-bound garbage (~60 MB on a 500k-event
        // tier). Intern returns the canonical string, so the tier shares one per template.
        int     tmplIdx = -1;
        string? tmpl    = null;
        if (!string.IsNullOrEmpty(ev.MessageTemplate))
        {
            tmplIdx = _pool.Intern(ev.MessageTemplate, out string canonical);
            tmpl    = tmplIdx >= 0 ? canonical : ev.MessageTemplate;   // see TryIngestRaw: never ""
        }
        int svcIdx  = ev.ServiceName is not null ? _pool.Intern(ev.ServiceName) : -1;

        bool ok = _ring.TryEnqueue(
            ev.Timestamp.UtcTicks,
            (byte)ev.Level,
            tmplIdx,
            tmpl,
            ev.Exception,
            ev.RawProperties.Span,
            ev.TraceIdHi, ev.TraceIdLo, ev.SpanId, svcIdx);

        if (ok) ingested++;
        else    dropped++;
    }

    /// <summary>Clamps a template for safe logging (cold path only — the substring alloc is fine).</summary>
    /// <summary>
    /// Serialised template of the server-side oversized-drop marker. The
    /// placeholders match the property keys below so the message renders with the
    /// numbers filled in. Interned once (low cardinality).
    /// </summary>
    private const string DropMarkerTemplate =
        "Ameto server dropped an oversized log event: properties {EventBodyBytes} B exceed limit {EventBodyLimitBytes} B ({OriginalTemplate})";

    /// <summary>
    /// Enqueues a compact Error event standing in for a dropped oversized one,
    /// preserving its timestamp / trace-span / service. The marker is tiny (the
    /// original template is truncated) so it can never itself be oversized, and it
    /// carries <c>DroppedBy=server</c> to set it apart from the client sink's marker.
    /// </summary>
    private void EnqueueServerDropMarker(
        long tsTicks, byte originalLevel, string originalTemplate,
        int payloadBytes, ulong traceHi, ulong traceLo, ulong spanId, int serviceIdx)
    {
        const int MaxTemplateChars = 256;
        string origTmpl  = originalTemplate.Length <= MaxTemplateChars ? originalTemplate : originalTemplate[..MaxTemplateChars];
        string origLevel = ((Ameto.Core.LogLevel)originalLevel).ToString();

        var buf = new ArrayBufferWriter<byte>(384);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(5);
        w.Write("DroppedBy");           w.Write("server");
        w.Write("EventBodyBytes");      w.Write(payloadBytes);
        w.Write("EventBodyLimitBytes"); w.Write(_maxEventPayloadBytes);
        w.Write("OriginalTemplate");    w.Write(origTmpl);
        w.Write("OriginalLevel");       w.Write(origLevel);
        w.Flush();

        int mtIdx = _pool.Intern(DropMarkerTemplate);
        bool ok = _ring.TryEnqueue(
            tsTicks, (byte)Ameto.Core.LogLevel.Error, mtIdx, DropMarkerTemplate,
            exception: null, buf.WrittenSpan, traceHi, traceLo, spanId, serviceIdx);
        if (ok) _drainer.NotifyEnqueued();
    }

    private static string Truncate(string? s, int max) =>
        string.IsNullOrEmpty(s) ? "(none)" : s.Length <= max ? s : string.Concat(s.AsSpan(0, max), "…");
}
