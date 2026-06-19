using System.Buffers;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Storage;

namespace Ameto.Ingestion;

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
///   413 Payload Too Large if body exceeds 4 MB
/// </summary>
public sealed class IngestionEndpoint
{
    private const int MaxBodyBytes = 4 * 1024 * 1024; // 4 MB

    private readonly IngestionRingBuffer     _ring;
    private readonly StringInternPool        _pool;
    private readonly IngestionDrainer        _drainer;
    private readonly ILogger<IngestionEndpoint> _logger;

    public IngestionEndpoint(
        IngestionRingBuffer ring,
        StringInternPool pool,
        IngestionDrainer drainer,
        ILogger<IngestionEndpoint> logger)
    {
        _ring    = ring;
        _pool    = pool;
        _drainer = drainer;
        _logger  = logger;
    }

    public async Task HandleAsync(HttpContext ctx)
    {
        // ── 1. Read body into a pooled buffer ─────────────────────────────────
        long? contentLength = ctx.Request.ContentLength;
        if (contentLength > MaxBodyBytes)
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

                if (bodyLen > MaxBodyBytes)
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
            {
                int tmplIdx = string.IsNullOrEmpty(ev.MessageTemplate)
                    ? -1
                    : _pool.Intern(ev.MessageTemplate);

                int svcIdx = ev.ServiceName is not null ? _pool.Intern(ev.ServiceName) : -1;

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
        foreach (var ev in events)
        {
            int tmplIdx = string.IsNullOrEmpty(ev.MessageTemplate)
                ? -1
                : _pool.Intern(ev.MessageTemplate);

            int svcIdx = ev.ServiceName is not null ? _pool.Intern(ev.ServiceName) : -1;

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

        if (ingested > 0)
            _drainer.NotifyEnqueued();

        return (ingested, dropped);
    }
}
