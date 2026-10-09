using System.Buffers;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// One request's log events, decoded and waiting for <see cref="StorageEngine.WriteBatchAsync"/> —
/// the staging that replaced the ingest ring.
///
/// <para><b>Why a request stages at all.</b> The ingest path answers only once its events are in
/// the hot tier AND its WAL, so a 200 means "durable against a process crash", not "parked in
/// process memory". The parsers are synchronous and stream event by event, and waiting out a full
/// tier has to be asynchronous; so the request decodes its whole body into this, gives the body
/// back, and then awaits the write. The ring it replaced held each pending event in a 64 KB slab
/// whatever its size; this holds the bytes the event actually has.</para>
///
/// <para>Memory: the payloads live in one array from <see cref="IngestBufferPool"/> (doubled as it
/// fills) and the per-event records in one from <see cref="ArrayPool{T}.Shared"/>, both handed back
/// on <see cref="Dispose"/>. A request holds about one decoded body while it waits; nothing here
/// outlives the request.</para>
///
/// <para>Not thread-safe: one request fills it, then one writer reads it.</para>
/// </summary>
public sealed class LogWriteBatch : IDisposable
{
    private struct Item
    {
        /// <summary>The event's header; <see cref="LogEventHeader.PropertiesArenaOffset"/> is its offset in <see cref="_payload"/>.</summary>
        public LogEventHeader Header;
        public string?        Template;
        public ExceptionInfo? Exception;
    }

    private const int InitialItems        = 256;
    private const int InitialPayloadBytes = 64 * 1024;

    private Item[] _items   = [];
    private byte[] _payload = [];
    private int    _count;
    private int    _payloadLength;

    /// <summary>The size the payload buffer is first rented at (see the constructor).</summary>
    private readonly int _payloadSizeHint;

    /// <param name="payloadSizeHint">
    /// The payload bytes the batch is expected to hold, so its buffer is rented once at that size
    /// instead of doubled up to it; 0 starts small. A request knows its body's length, and the
    /// payloads it decodes to are usually no larger.
    /// </param>
    public LogWriteBatch(int payloadSizeHint = 0) => _payloadSizeHint = Math.Max(0, payloadSizeHint);

    /// <summary>Events staged.</summary>
    public int Count => _count;

    /// <summary>Payload bytes staged.</summary>
    public long PayloadBytes => _payloadLength;

    /// <summary>
    /// Stages one event. <paramref name="header"/>'s id and payload offset are ignored — the writer
    /// assigns the one and this batch the other; everything else is kept as given.
    /// </summary>
    public void Add(in LogEventHeader header, ReadOnlySpan<byte> payload, string? template, ExceptionInfo? exception)
    {
        if (_count == _items.Length) GrowItems();
        if (_payload.Length - _payloadLength < payload.Length) GrowPayload(payload.Length);

        ref var item = ref _items[_count];
        item.Header                       = header;
        item.Header.Id                    = 0;
        item.Header.PropertiesArenaOffset = _payloadLength;
        item.Header.PropertiesByteLength  = payload.Length;
        item.Header.HasException          = exception is not null;
        item.Template                     = template;
        item.Exception                    = exception;

        payload.CopyTo(_payload.AsSpan(_payloadLength));
        _payloadLength += payload.Length;
        _count++;
    }

    /// <summary>The header of event <paramref name="index"/>, as staged.</summary>
    internal ref readonly LogEventHeader HeaderAt(int index) => ref _items[index].Header;

    /// <summary>The properties payload of event <paramref name="index"/>.</summary>
    internal ReadOnlySpan<byte> PayloadAt(int index)
    {
        ref readonly var h = ref _items[index].Header;
        return _payload.AsSpan(h.PropertiesArenaOffset, h.PropertiesByteLength);
    }

    internal string?        TemplateAt(int index)  => _items[index].Template;
    internal ExceptionInfo? ExceptionAt(int index) => _items[index].Exception;

    /// <summary>Gives both arrays back. The batch is empty, and usable again, afterwards.</summary>
    public void Dispose()
    {
        if (_items.Length != 0)
        {
            // Cleared: the records hold template and exception references, and a pooled array that
            // kept them would keep an old request's exceptions alive for as long as it sat there.
            ArrayPool<Item>.Shared.Return(_items, clearArray: true);
            _items = [];
        }
        if (_payload.Length != 0)
        {
            IngestBufferPool.Return(_payload);
            _payload = [];
        }
        _count         = 0;
        _payloadLength = 0;
    }

    private void GrowItems()
    {
        var bigger = ArrayPool<Item>.Shared.Rent(Math.Max(InitialItems, _items.Length * 2));
        if (_items.Length != 0)
        {
            _items.AsSpan(0, _count).CopyTo(bigger);
            ArrayPool<Item>.Shared.Return(_items, clearArray: true);
        }
        _items = bigger;
    }

    private void GrowPayload(int need)
    {
        long first  = _payload.Length == 0 ? Math.Max(InitialPayloadBytes, _payloadSizeHint) : 0;
        long wanted = Math.Max((long)_payloadLength + need, Math.Max(first, (long)_payload.Length * 2));
        var bigger  = IngestBufferPool.Rent((int)Math.Min(wanted, Array.MaxLength));
        if (_payload.Length != 0)
        {
            _payload.AsSpan(0, _payloadLength).CopyTo(bigger);
            IngestBufferPool.Return(_payload);
        }
        _payload = bigger;
    }
}

/// <summary>
/// What <see cref="StorageEngine.WriteBatchAsync"/> did with a batch. It is processed in order up to
/// the first event there was no room for: the first <c>Written + Refused</c> events were written or
/// refused, and the last <c>NotWritten</c> were not touched.
/// </summary>
/// <param name="Written">Events now in the hot tier, or a spilled block, and in its WAL.</param>
/// <param name="Refused">
/// Events left out because no tier can ever hold them (a payload larger than one hot-tier chunk).
/// Counted apart: retrying them cannot help.
/// </param>
/// <param name="NotWritten">
/// Events after the prefix that were not written: the engine had no room for them within the wait,
/// or it is shutting down. Retrying them later can succeed.
/// </param>
public readonly record struct LogBatchWriteResult(int Written, int Refused, int NotWritten);
