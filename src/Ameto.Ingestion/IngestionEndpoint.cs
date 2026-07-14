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
/// Returns:
///   200 OK + JSON { "ingested": N, "dropped": M }
///   400 Bad Request if body is not a valid MessagePack array
///   413 Payload Too Large if body exceeds the configured batch limit
///          (<see cref="IngestionOptions.MaxBatchBytes"/>)
/// </summary>
public sealed class IngestionEndpoint : IOtlpLogSink
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

        byte[] bodyBuf;
        int    bodyLen;

        if (contentLength.HasValue)
        {
            int expected = (int)contentLength.Value;
            bodyBuf = ArrayPool<byte>.Shared.Rent(Math.Max(expected, 1));
            int total = 0;
            while (total < expected)
            {
                int n = await ctx.Request.Body.ReadAsync(
                    bodyBuf.AsMemory(total, expected - total), ctx.RequestAborted);
                if (n == 0) break;
                total += n;
            }
            bodyLen = total;
        }
        else
        {
            bodyBuf = ArrayPool<byte>.Shared.Rent(64 * 1024);
            bodyLen = 0;
            while (true)
            {
                int n = await ctx.Request.Body.ReadAsync(
                    bodyBuf.AsMemory(bodyLen), ctx.RequestAborted);
                if (n == 0) break;
                bodyLen += n;

                if (bodyLen > _maxBatchBytes)
                {
                    ArrayPool<byte>.Shared.Return(bodyBuf);
                    ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
                    return;
                }

                if (bodyLen == bodyBuf.Length)
                {
                    var bigger = ArrayPool<byte>.Shared.Rent(bodyBuf.Length * 2);
                    Buffer.BlockCopy(bodyBuf, 0, bigger, 0, bodyLen);
                    ArrayPool<byte>.Shared.Return(bodyBuf);
                    bodyBuf = bigger;
                }
            }
        }

        try
        {
            // ── 2. Parse MessagePack array ────────────────────────────────────
            var events = new List<LogEvent>(64);
            try
            {
                var seq      = new ReadOnlySequence<byte>(bodyBuf, 0, bodyLen);
                uint nextSeq = 0;
                LogEventSerializer.DeserializeBatch(seq, NodeId.Local.Value, ref nextSeq, events);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Malformed ingestion payload");
                ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            // ── 3. Push to ring buffer ────────────────────────────────────────
            // Events arrive only via this HTTP API, so RawProperties is the
            // single source of truth — the msgpack slice points back into
            // bodyBuf and is copied into the ring slot by TryEnqueue.
            int ingested = 0, dropped = 0;

            foreach (var ev in events)
                TryIngest(ev, ref ingested, ref dropped);

            if (ingested > 0)
                _drainer.NotifyEnqueued();

            // ── 4. Response ───────────────────────────────────────────────────
            ctx.Response.StatusCode  = StatusCodes.Status200OK;
            ctx.Response.ContentType = "application/json";
            await ctx.Response.WriteAsync(
                $"{{\"ingested\":{ingested},\"dropped\":{dropped}}}",
                ctx.RequestAborted);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bodyBuf);
        }
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
    {
        if (msgpackProps.Length > _maxEventPayloadBytes)
            return false; // oversized — drop (hot path: no per-event log, unlike TryIngest)

        int    tmplIdx = _pool.Intern(templateUtf8);           // -1 when empty
        string tmpl    = tmplIdx >= 0 ? _pool.Get(tmplIdx) : string.Empty;
        int    svcIdx  = _pool.Intern(serviceUtf8);            // -1 when empty

        return _ring.TryEnqueue(
            tsTicks, level, tmplIdx, tmpl, exception: null,
            msgpackProps, traceHi, traceLo, spanId, svcIdx);
    }

    /// <summary>Wakes the drainer once after a streaming batch (see <see cref="TryIngestRaw"/>).</summary>
    public void NotifyBatchEnqueued() => _drainer.NotifyEnqueued();

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
            _logger.LogWarning(
                "Dropped oversized log event: properties {PayloadBytes} B exceed limit {LimitBytes} B (service={Service}, template=\"{Template}\")",
                payloadLen, _maxEventPayloadBytes, ev.ServiceName ?? "(none)", Truncate(ev.MessageTemplate, 120));
            return;
        }

        int tmplIdx = string.IsNullOrEmpty(ev.MessageTemplate) ? -1 : _pool.Intern(ev.MessageTemplate);
        int svcIdx  = ev.ServiceName is not null ? _pool.Intern(ev.ServiceName) : -1;

        bool ok = _ring.TryEnqueue(
            ev.Timestamp.UtcTicks,
            (byte)ev.Level,
            tmplIdx,
            ev.MessageTemplate,
            ev.Exception,
            ev.RawProperties.Span,
            ev.TraceIdHi, ev.TraceIdLo, ev.SpanId, svcIdx);

        if (ok) ingested++;
        else    dropped++;
    }

    /// <summary>Clamps a template for safe logging (cold path only — the substring alloc is fine).</summary>
    private static string Truncate(string? s, int max) =>
        string.IsNullOrEmpty(s) ? "(none)" : s.Length <= max ? s : string.Concat(s.AsSpan(0, max), "…");
}
