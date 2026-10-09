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
/// time. Implemented by <see cref="LogIngestBatch"/>; abstracted so the parser can be
/// unit-tested against a capturing fake.
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

    /// <summary>Called once after a batch has been parsed. A sink that writes as it goes may ignore it.</summary>
    void NotifyBatchEnqueued();
}

/// <summary>
/// The log ingest path: decodes a request into a <see cref="LogIngestBatch"/>, writes the batch
/// into the store with <see cref="StorageEngine.WriteBatchAsync"/>, and answers once it is there —
/// in the hot tier and its WAL, or in a spill file. A 200 therefore means the events survive the
/// death of this process (see <see cref="StorageEngine"/>, "The write path").
///
/// <para>Handles POST /api/events itself (<see cref="HandleAsync"/>); the OTLP receivers decode
/// into a batch from <see cref="BeginBatch"/> and commit it the same way.</para>
///
/// <para>Wire format of /api/events: MessagePack array of CLEF maps.
///   [ { "@t": "...", "@mt": "...", "@l": "...", "Prop": value, ... }, ... ]</para>
///
/// Returns (every 200, 400 and 503 is application/json with the counts):
///   200 OK          { "ingested": N, "dropped": M }
///   400 Bad Request { "ingested": N, "dropped": M, "failedAtElement": K }
///          The body stopped being a CLEF array at element K. The N events before it are
///          INGESTED and stay so. failedAtElement is omitted when the body failed
///          before any element, at the array header (not an array, or empty).
///   400 Bad Request { "ingested": 0, "dropped": 0 }
///          The body ended short of its Content-Length on a stream that ends rather than
///          throws (see HandleAsync 1b). Kestrel fails that read itself.
///   413 Payload Too Large, no body, above <see cref="IngestionOptions.MaxBatchBytes"/>
///   503 Service Unavailable, Retry-After { "ingested": 0, "dropped": M }
///          The store had no room for any of the batch within
///          <see cref="IngestionOptions.BackPressureWait"/>, or is shutting down. Nothing was
///          written, so a retry duplicates nothing.
///   500 A fault in the server underneath (intern pool, logger). It leaves the handler and
///          hosting answers it; see IsMalformedPayload.
/// </summary>
public sealed class IngestionEndpoint
{
    private readonly StorageEngine              _storage;
    private readonly StringInternPool           _pool;
    private readonly ILogger<IngestionEndpoint> _logger;

    /// <summary>Max HTTP body bytes for one CLEF batch — 413 above this. From config.</summary>
    private readonly int _maxBatchBytes;

    /// <summary>
    /// Max properties bytes per event: the configured limit, never above one hot-tier chunk — an
    /// event larger than that no tier can hold, and it is better refused here, with its marker,
    /// than counted as a store error.
    /// </summary>
    private readonly int _maxEventPayloadBytes;

    /// <summary>How long a batch waits for room in the store. From config.</summary>
    private readonly TimeSpan _backPressureWait;

    /// <summary>The seconds a 503 asks a client to wait before retrying.</summary>
    internal const string RetryAfterSeconds = "1";

    public IngestionEndpoint(
        StorageEngine storage,
        StringInternPool pool,
        ServerOptions options,
        ILogger<IngestionEndpoint> logger)
    {
        _storage  = storage;
        _pool     = pool;
        _logger   = logger;
        _maxBatchBytes        = options.Ingestion.MaxBatchBytes;
        _maxEventPayloadBytes = (int)Math.Min(options.Ingestion.MaxEventPayloadBytes, HotTierSegment.ChunkPayloadBytes);
        _backPressureWait     = options.Ingestion.BackPressureWait;

        if (options.Ingestion.RingCapacity is not null || options.Ingestion.PayloadPoolBytes is not null)
            _logger.LogWarning(
                "Ingestion.RingCapacity and Ingestion.PayloadPoolBytes no longer have any effect: logs are written " +
                "straight into the store by the request that carries them, and the ingest ring they sized is gone. " +
                "Ingestion.BackPressureWait and HotTier.SpillEnabled are what decide what happens when the flush is behind.");
        if (options.Ingestion.MaxEventPayloadBytes > HotTierSegment.ChunkPayloadBytes)
            _logger.LogWarning(
                "Ingestion.MaxEventPayloadBytes {Configured} B is more than one hot-tier chunk holds; events are refused " +
                "above {Effective} B", options.Ingestion.MaxEventPayloadBytes, _maxEventPayloadBytes);
    }

    /// <summary>The intern pool templates and services are interned into — the store's.</summary>
    internal StringInternPool Pool => _pool;

    /// <summary>A fresh batch for one request. Dispose it once the request is done with it.</summary>
    /// <param name="payloadSizeHint">
    /// The payload bytes the batch is likely to hold — the body's length is a good one — so its
    /// buffer is rented once at that size rather than doubled up to it. 0: start small and grow.
    /// </param>
    public LogIngestBatch BeginBatch(int payloadSizeHint = 0) => new(this, payloadSizeHint);

    // ── Counters (/api/diagnostics) ───────────────────────────────────────────

    private long _acceptedTotal;
    private long _droppedOversized;

    /// <summary>Events written into the store since start, drop markers included.</summary>
    public long AcceptedTotal => Interlocked.Read(ref _acceptedTotal);

    /// <summary>Events refused at the door for a properties payload over the per-event limit.</summary>
    public long DroppedOversized => Interlocked.Read(ref _droppedOversized);

    internal void CountOversizedDrop() => Interlocked.Increment(ref _droppedOversized);

    // ── Commit ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Writes a decoded batch into the store and says what became of it. Called by
    /// <see cref="LogIngestBatch.CommitAsync"/>.
    /// </summary>
    internal async ValueTask<LogIngestResult> CommitAsync(LogIngestBatch batch, CancellationToken ct)
    {
        var staged = batch.Staged;
        if (staged.Count == 0)
            return new LogIngestResult(0, batch.DroppedAtDoor, Busy: false);

        var r = await _storage.WriteBatchAsync(staged, _backPressureWait, ct).ConfigureAwait(false);
        Interlocked.Add(ref _acceptedTotal, r.Written);

        // Drop markers stand in for events refused at the door: they are written like any other
        // event, but they are not what the client sent, and the client's counts are of its own.
        // The batch was processed up to `processed`; what follows was not written.
        int processed      = r.Written + r.Refused;
        int markersWritten = batch.MarkersBefore(processed);
        int markersUnsent  = batch.MarkerCount - markersWritten;
        int ingested       = r.Written - markersWritten;
        int dropped        = batch.DroppedAtDoor + r.Refused + (r.NotWritten - markersUnsent);

        if (r.NotWritten > 0)
            _logger.LogDebug("Log batch: {NotWritten} of {Staged} event(s) not written — the store had no room within {Wait}",
                r.NotWritten, staged.Count, _backPressureWait);

        // Nothing written for want of room: the caller answers "retry later", which duplicates nothing.
        bool busy = r.Written == 0 && r.NotWritten > 0;
        return new LogIngestResult(ingested, dropped, busy);
    }

    // ── POST /api/events (CLEF) ───────────────────────────────────────────────

    public async Task HandleAsync(HttpContext ctx)
    {
        // ── 1. Read body into a pooled buffer ─────────────────────────────────
        long? contentLength = ctx.Request.ContentLength;
        if (contentLength > _maxBatchBytes)
        {
            ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
            return;
        }

        // From IngestBufferPool, the pool the OTLP receivers read into, not ArrayPool.Shared: a
        // 1.4 MB batch rounds up to the 2 MB bucket, and past Shared's shallow per-core depth
        // every concurrent request got a fresh array on the large object heap. Every buffer
        // below goes back to IngestBufferPool and nowhere else — one handed to Shared is not a
        // crash, it is this pool emptying one request at a time.
        //
        // Rented, and straight into the try: a read that THROWS must still give the buffer back.
        // Kestrel raises BadHttpRequestException for a body that ends short of its
        // Content-Length, and RequestAborted cancels a read. Both used to fire before the try
        // opened, so the rented array was never returned. Every exit below, the 413 included,
        // returns it exactly once: in the finally.
        byte[]? bodyBuf = IngestBufferPool.Rent(
            contentLength.HasValue ? Math.Max((int)contentLength.Value, 1) : 64 * 1024);
        int     bodyLen = 0;
        LogIngestBatch? batch = null;
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
                        byte[] bigger = IngestBufferPool.Rent(bodyBuf.Length * 2);
                        Buffer.BlockCopy(bodyBuf, 0, bigger, 0, bodyLen);
                        // Swap first, return second: from the swap on, the finally owns `bigger`,
                        // so even a Return that threw could not send `smaller` back twice.
                        byte[] smaller = bodyBuf;
                        bodyBuf = bigger;
                        IngestBufferPool.Return(smaller);
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
                await WriteCountsAsync(ctx, StatusCodes.Status400BadRequest, ingested: 0, dropped: 0);
                return;
            }

            // ── 2+3. Decode the MessagePack array into the batch ──────────────
            // No LogEvent per event: the batch reader hands each event over as spans into
            // bodyBuf, and the batch copies the property bytes it keeps. A body that is not a
            // CLEF array is rejected before any event is seen; one that turns malformed part way
            // through answers 400 with the intact prefix written (see StreamBatch).
            // Sized from the body: a CLEF event's properties are a part of its map, so the decoded
            // payloads never outgrow the body that carried them, and the batch is one rent.
            batch = BeginBatch(payloadSizeHint: bodyLen);
            var progress = default(LogEventSerializer.ClefBatchProgress);
            Exception? malformed = null;
            try
            {
                LogEventSerializer.StreamBatch(bodyBuf.AsMemory(0, bodyLen), batch, ref progress);
            }
            // Classified by WHERE it was thrown, not by its type. This used to be
            // catch(Exception), which also swallowed failures of the sink underneath — an
            // intern-pool fault, or the logger — and reported them to the client as "malformed
            // payload"; those surface as 500. A list of reader exception types was tried and was
            // short on day one: a str32/bin32/ext32 length prefix of 2^31 or more throws
            // OverflowException, and went out as 500.
            catch (Exception ex) when (IsMalformedPayload(ex, progress.InSink))
            {
                malformed = ex;
            }

            // The body is decoded into the batch: give it back before the write, which may wait.
            IngestBufferPool.Return(bodyBuf);
            bodyBuf = null;

            // ── 4. Write, then answer ─────────────────────────────────────────
            var result = await batch.CommitAsync(ctx.RequestAborted);

            if (malformed is not null)
            {
                // Warning, not Debug, and with the counts: "some of that batch landed and some of
                // it did not" is an operator's problem, and the element index is what makes it
                // findable in the sender. ElementIndex -1 is the array header itself (the reply
                // then omits failedAtElement).
                _logger.LogWarning(malformed,
                    "Malformed ingestion payload at element {ElementIndex} (-1 = the array header) of {ElementCount}: "
                  + "{Ingested} event(s) already ingested, {Dropped} dropped — batch refused",
                    progress.ElementIndex, progress.ElementCount, result.Ingested, result.Dropped);

                await WriteCountsAsync(ctx, StatusCodes.Status400BadRequest, result.Ingested, result.Dropped,
                                       failedAtElement: progress.ElementIndex);
                return;
            }

            if (result.Busy)
            {
                ctx.Response.Headers.RetryAfter = RetryAfterSeconds;
                await WriteCountsAsync(ctx, StatusCodes.Status503ServiceUnavailable, result.Ingested, result.Dropped);
                return;
            }

            await WriteCountsAsync(ctx, StatusCodes.Status200OK, result.Ingested, result.Dropped);
        }
        finally
        {
            if (bodyBuf is not null) IngestBufferPool.Return(bodyBuf);
            batch?.Dispose();
        }
    }

    private static async Task WriteCountsAsync(HttpContext ctx, int status, int ingested, int dropped, int failedAtElement = -1)
    {
        ctx.Response.StatusCode  = status;
        ctx.Response.ContentType = "application/json";
        WriteCountsJson(ctx.Response.BodyWriter, ingested, dropped, failedAtElement);
        await ctx.Response.BodyWriter.FlushAsync(ctx.RequestAborted);
    }

    /// <summary>
    /// Whether a throw out of StreamBatch is the client's (400) or the server's (500), decided
    /// by where it happened. <paramref name="inSink"/> is
    /// <see cref="LogEventSerializer.ClefBatchProgress.InSink"/> as the throw left it.
    ///
    /// <para>Set: the fault came out of the sink — the intern pool, the logger — and is the
    /// server's, whatever its type.</para>
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

    // ── Staging (called by LogIngestBatch) ────────────────────────────────────

    /// <summary>
    /// Stages one already-msgpack-encoded record — the OTLP parsers' road. Interns template and
    /// service straight from UTF-8 (no string allocated on a cache hit). False when the record was
    /// refused at the door (oversized: logged, and a marker staged in its place).
    /// </summary>
    internal bool StageRaw(
        LogIngestBatch batch,
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
            //
            // Cold path, one per dropped event: the strings are worth their cost. The log line
            // is the one that names the producer, as TryIngest and TryIngestClef do. The service
            // comes from the span when the caller passed one, else from the pool: a parser that
            // interned the block's service.name once may hand over only the index.
            string origTmpl = templateUtf8.IsEmpty ? string.Empty : System.Text.Encoding.UTF8.GetString(templateUtf8);
            string svcStr   = !serviceUtf8.IsEmpty ? System.Text.Encoding.UTF8.GetString(serviceUtf8)
                            : serviceIdx >= 0      ? _pool.Get(serviceIdx)
                            : string.Empty;
            CountOversizedDrop();
            _logger.LogWarning(
                "Dropped oversized log event: properties {PayloadBytes} B exceed limit {LimitBytes} B (service={Service}, template=\"{Template}\")",
                msgpackProps.Length, _maxEventPayloadBytes, svcStr.Length != 0 ? svcStr : "(none)", Truncate(origTmpl, 120));
            StageServerDropMarker(batch, tsTicks, level, origTmpl, msgpackProps.Length, traceHi, traceLo, spanId, serviceIdx);
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

        batch.Add(tsTicks, level, tmplIdx, tmpl, exception: null, msgpackProps, traceHi, traceLo, spanId, serviceIdx);
        return true;
    }

    /// <summary>
    /// Stages one CLEF event straight from the request body, with no <see cref="LogEvent"/> in
    /// between: the same duties as <see cref="StageRaw"/> (oversized warning + server drop marker,
    /// template/service interning), with everything arriving as spans over the pooled body buffer.
    /// </summary>
    internal bool StageClef(
        LogIngestBatch batch,
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

            CountOversizedDrop();
            _logger.LogWarning(
                "Dropped oversized log event: properties {PayloadBytes} B exceed limit {LimitBytes} B (service={Service}, template=\"{Template}\")",
                msgpackProps.Length, _maxEventPayloadBytes, svcStr ?? "(none)", Truncate(tmplStr, 120));
            StageServerDropMarker(
                batch, tsTicks, level, tmplStr, msgpackProps.Length, traceHi, traceLo, spanId,
                svcStr is not null ? _pool.Intern(svcStr) : -1);
            return false;
        }

        // Intern returns the pool's own instance, so the hot tier shares one string per
        // template instead of retaining this event's copy (see StageRaw).
        int     tmplIdx  = _pool.Intern(templateUtf8, out string canonical); // -1 when empty
        string? tmpl     = canonical.Length != 0 ? canonical : null;   // see StageRaw: kept past saturation, never ""
        int svcIdx  = _pool.Intern(serviceUtf8);                     // -1 when empty

        batch.Add(tsTicks, level, tmplIdx, tmpl, exception, msgpackProps, traceHi, traceLo, spanId, svcIdx);
        return true;
    }

    /// <summary>
    /// Serialised template of the server-side oversized-drop marker. The
    /// placeholders match the property keys below so the message renders with the
    /// numbers filled in. Interned once (low cardinality).
    /// </summary>
    private const string DropMarkerTemplate =
        "Ameto server dropped an oversized log event: properties {EventBodyBytes} B exceed limit {EventBodyLimitBytes} B ({OriginalTemplate})";

    /// <summary>
    /// Stages a compact Error event standing in for a dropped oversized one,
    /// preserving its timestamp / trace-span / service. The marker is tiny (the
    /// original template is truncated) so it can never itself be oversized, and it
    /// carries <c>DroppedBy=server</c> to set it apart from the client sink's marker.
    /// </summary>
    private void StageServerDropMarker(
        LogIngestBatch batch,
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
        batch.AddMarker(tsTicks, (byte)Ameto.Core.LogLevel.Error, mtIdx, DropMarkerTemplate,
                        buf.WrittenSpan, traceHi, traceLo, spanId, serviceIdx);
    }

    /// <summary>
    /// The template as the drop Warning names it: at most <paramref name="max"/> UTF-16 units and
    /// an ellipsis, or "(none)".
    ///
    /// <para>Never cut between the halves of a surrogate pair. A cut after a high surrogate left
    /// half an emoji or CJK extension character at the end of the logged template: an unpaired
    /// surrogate, which turns into U+FFFD (or an encoder error) once a sink writes the line as
    /// UTF-8. Backing off one unit leaves the pair out whole; it is the same one concat, over a
    /// span one shorter, so nothing more is allocated.</para>
    /// </summary>
    internal static string Truncate(string? s, int max)
    {
        if (string.IsNullOrEmpty(s)) return "(none)";
        if (s.Length <= max) return s;
        int cut = max > 0 && char.IsHighSurrogate(s[max - 1]) ? max - 1 : max;
        return string.Concat(s.AsSpan(0, cut), "…");
    }
}
