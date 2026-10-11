using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// What a <see cref="LogWriteBatch"/> may hold. The batch asks it before every array it grows, and
/// the ingest path (Ameto.Ingestion's <c>LogIngestBatch</c>) charges the strings it stages to it too:
/// the one place a request's decoded memory is counted, as it is taken.
/// </summary>
public interface ILogBatchBudget
{
    /// <summary>Bytes the batch may still take on before it is past its limit.</summary>
    long Headroom { get; }

    /// <summary>Counts <paramref name="bytes"/> more as held by the batch, or throws — and then nothing was grown.</summary>
    void Charge(long bytes);

    /// <summary>The batch's arrays went back: what it held is free. Called once, by the dispose that returned them.</summary>
    void Released();
}

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
/// <para>Memory: the payloads live in one array from <see cref="IngestBufferPool"/> and the
/// per-event records in one from <see cref="ArrayPool{T}.Shared"/>, both handed back on
/// <see cref="Dispose"/>. A request holds it while it waits, and nothing here outlives the
/// request. With a <see cref="ILogBatchBudget"/>, every growth is charged to it BEFORE the array is
/// taken, and the payload array is sized from what the budget has left rather than doubled past
/// it: a batch never holds more than its budget allows. (Without one it grows by doubling, as it
/// always did.)</para>
///
/// <para><b>Room kept at the end.</b> <see cref="ReserveTail"/> keeps capacity for events to be
/// appended later (a request's drop markers, put after its own events when it is committed), taken
/// and charged when the room is reserved; <see cref="AddReserved"/> appends into it and never grows,
/// so the commit cannot run out of memory or past the budget.</para>
///
/// <para><b>A write holds it.</b> <see cref="StorageEngine.WriteBatchAsync"/> leases the batch for as
/// long as it runs, and a <see cref="Dispose"/> meanwhile is deferred to the write's end: a request
/// whose own await threw (its state machine's box, out of memory) disposed the batch under a write
/// still suspended on it, and a room signal then let the write read arrays already back in the
/// pool — a zeroed event, or another request's.</para>
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
        /// <summary>The <c>@service</c> of an event whose service has no pool index (the pool was full); null otherwise.</summary>
        public string?        Service;
    }

    /// <summary>What one staged record's slot weighs.</summary>
    public static int ItemBytes => Unsafe.SizeOf<Item>();

    private const int InitialItems        = 256;
    private const int InitialPayloadBytes = 64 * 1024;

    private Item[] _items   = [];
    private byte[] _payload = [];
    /// <summary>False for a payload array allocated to an exact length past the pool's rounding: never handed to the pool.</summary>
    private bool   _payloadPooled = true;
    private int    _count;
    private int    _payloadLength;

    /// <summary>Slots and payload bytes kept free at the end for <see cref="AddReserved"/>.</summary>
    private int _tailItems, _tailPayload;

    /// <summary>The size the payload buffer is first rented at (see the constructor).</summary>
    private readonly int _payloadSizeHint;

    private readonly ILogBatchBudget? _budget;

    /// <summary>Writes running on this batch (<see cref="Lease"/>); a dispose waits for the last.</summary>
    private int _leases;

    /// <summary>1 when <see cref="Dispose"/> was asked while a write held the batch.</summary>
    private int _disposeDeferred;

    /// <param name="payloadSizeHint">
    /// The payload bytes the batch is expected to hold, so its buffer is rented once at that size
    /// instead of doubled up to it; 0 starts small. A request knows its body's length, and the
    /// payloads it decodes to are usually no larger.
    /// </param>
    /// <param name="budget">What the batch may hold, asked before each growth; null for no bound.</param>
    public LogWriteBatch(int payloadSizeHint = 0, ILogBatchBudget? budget = null)
    {
        _payloadSizeHint = Math.Max(0, payloadSizeHint);
        _budget          = budget;
    }

    /// <summary>Events staged.</summary>
    public int Count => _count;

    /// <summary>Payload bytes staged.</summary>
    public long PayloadBytes => _payloadLength;

    /// <summary>
    /// Stages one event. <paramref name="header"/>'s id and payload offset are ignored — the writer
    /// assigns the one and this batch the other; everything else is kept as given.
    /// </summary>
    /// <param name="service">
    /// The event's <c>@service</c> when it has no pool index (<see cref="LogEventHeader.ServiceNamePoolIndex"/>
    /// -1: the pool was full) — the only copy of the name, kept by the tier and the WAL as a template is.
    /// </param>
    public void Add(in LogEventHeader header, ReadOnlySpan<byte> payload, string? template, ExceptionInfo? exception,
                    string? service = null)
    {
        if (_count + _tailItems >= _items.Length) GrowItems(_count + _tailItems + 1);
        if (_payload.Length - _payloadLength - _tailPayload < payload.Length) GrowPayload(payload.Length + _tailPayload);
        Store(header, payload, template, exception, service);
    }

    /// <summary>
    /// Keeps room at the end for one event of <paramref name="payloadBytes"/>, to be appended with
    /// <see cref="AddReserved"/> after every <see cref="Add"/> to come. Grows now, if it must — this
    /// is where running out of memory or budget happens — never when the room is used.
    /// </summary>
    public void ReserveTail(int payloadBytes)
    {
        if (_count + _tailItems >= _items.Length) GrowItems(_count + _tailItems + 1);
        if (_payload.Length - _payloadLength - _tailPayload < payloadBytes) GrowPayload(payloadBytes + _tailPayload);
        _tailItems++;
        _tailPayload += payloadBytes;
    }

    /// <summary>
    /// Appends an event into the room <see cref="ReserveTail"/> kept: never grows an array, so it
    /// cannot run out of memory. The payload must be no longer than the room reserved for it.
    /// </summary>
    public void AddReserved(in LogEventHeader header, ReadOnlySpan<byte> payload, string? template, ExceptionInfo? exception,
                            string? service = null)
    {
        if (_tailItems == 0 || payload.Length > _tailPayload)
            throw new InvalidOperationException("an event appended past the room reserved for it");
        _tailItems--;
        _tailPayload -= payload.Length;
        Store(header, payload, template, exception, service);
    }

    private void Store(in LogEventHeader header, ReadOnlySpan<byte> payload, string? template, ExceptionInfo? exception,
                       string? service)
    {
        ref var item = ref _items[_count];
        item.Header                       = header;
        item.Header.Id                    = 0;
        item.Header.PropertiesArenaOffset = _payloadLength;
        item.Header.PropertiesByteLength  = payload.Length;
        item.Header.HasException          = exception is not null;
        item.Template                     = template;
        item.Exception                    = exception;
        item.Service                      = header.ServiceNamePoolIndex < 0 && !string.IsNullOrEmpty(service) ? service : null;

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

    /// <summary>Records the batch's record array has room for: what it holds of them is this times <see cref="ItemBytes"/>.</summary>
    internal int ItemCapacity => _items.Length;

    /// <summary>The length of the batch's payload array: what it holds of payloads.</summary>
    internal int PayloadCapacity => _payload.Length;

    internal string?        TemplateAt(int index)  => _items[index].Template;
    internal ExceptionInfo? ExceptionAt(int index) => _items[index].Exception;
    internal string?        ServiceAt(int index)   => _items[index].Service;

    /// <summary>A write begins on this batch: a <see cref="Dispose"/> from now waits for <see cref="EndLease"/>.</summary>
    internal void Lease() => Interlocked.Increment(ref _leases);

    /// <summary>The write ended; a dispose asked meanwhile happens now.</summary>
    internal void EndLease()
    {
        if (Interlocked.Decrement(ref _leases) == 0 && Interlocked.Exchange(ref _disposeDeferred, 0) == 1)
            DisposeCore();
    }

    /// <summary>
    /// Gives both arrays back — at once, or, while a write holds the batch, when that write ends.
    /// The batch is empty, and usable again, afterwards.
    /// </summary>
    public void Dispose()
    {
        // A full fence, not a volatile write: the read of the leases below must not pass the store,
        // or this and an EndLease racing it each read the other's old value — this a lease still held,
        // that no dispose asked — and neither gives the arrays and the reservation back, for good.
        Interlocked.Exchange(ref _disposeDeferred, 1);
        if (Volatile.Read(ref _leases) == 0 && Interlocked.Exchange(ref _disposeDeferred, 0) == 1)
            DisposeCore();
    }

    private void DisposeCore()
    {
        try
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
                if (_payloadPooled) IngestBufferPool.Return(_payload);
                _payload       = [];
                _payloadPooled = true;
            }
            _count         = 0;
            _payloadLength = 0;
            _tailItems     = 0;
            _tailPayload   = 0;
        }
        finally { _budget?.Released(); }
    }

    /// <summary>Test seam: runs before the batch grows either array. Null in production.</summary>
    internal static readonly AsyncLocal<Action?> BeforeGrowForTest = new();

    private void GrowItems(int need)
    {
        BeforeGrowForTest.Value?.Invoke();
        // What ArrayPool.Shared hands back for the length asked: a power of two, 16 at least.
        int length = (int)Math.Min(BitOperations.RoundUpToPowerOf2((uint)Math.Max(Math.Max(InitialItems, need), _items.Length * 2)),
                                   Array.MaxLength);
        _budget?.Charge((long)(length - _items.Length) * ItemBytes);
        Item[] bigger;
        try { bigger = ArrayPool<Item>.Shared.Rent(length); }
        catch
        {
            _budget?.Charge(-(long)(length - _items.Length) * ItemBytes);
            throw;
        }
        if (_items.Length != 0)
        {
            _items.AsSpan(0, _count).CopyTo(bigger);
            ArrayPool<Item>.Shared.Return(_items, clearArray: true);
        }
        if (bigger.Length != length) _budget?.Charge((long)(bigger.Length - length) * ItemBytes);   // a pool that rounds differently
        _items = bigger;
    }

    private void GrowPayload(int need)
    {
        BeforeGrowForTest.Value?.Invoke();
        long required = (long)_payloadLength + need;
        if (required > Array.MaxLength)
        {
            // Past what one array holds — and so past the batch's limit, which is below it: refused as
            // too heavy (413) by the charge, where an out-of-memory thrown first was answered 503 —
            // retried for ever — or 500. Without a budget, out of memory as before.
            _budget?.Charge(required - _payload.Length);
            throw new OutOfMemoryException("a log batch's payloads cannot exceed one array");
        }
        long first  = _payload.Length == 0 ? Math.Max(InitialPayloadBytes, _payloadSizeHint) : 0;
        long wanted = Math.Max(required, Math.Max(first, (long)_payload.Length * 2));

        // From what the budget has left, not by doubling past it: a doubling of 64 MB when 65 were
        // needed took 128 MB for a batch its limit stopped at the next record anyway.
        long length = IngestBufferPool.RentLength((int)Math.Min(wanted, Array.MaxLength));
        bool exact  = false;
        if (_budget is not null)
        {
            long most = _payload.Length + Math.Max(0, _budget.Headroom);
            if (length > most)
            {
                // The pool's rounding (or the doubling) would pass the budget: exactly what fits, or
                // what is needed if even that does not — the charge below refuses it then.
                length = Math.Max(required, Math.Min(wanted, most));
                exact  = IngestBufferPool.RentLength((int)length) != length;
            }
            _budget.Charge(length - _payload.Length);
        }

        byte[] bigger;
        try { bigger = exact ? new byte[length] : IngestBufferPool.Rent((int)length); }
        catch
        {
            _budget?.Charge(-(length - _payload.Length));
            throw;
        }
        if (_payload.Length != 0)
        {
            _payload.AsSpan(0, _payloadLength).CopyTo(bigger);
            if (_payloadPooled) IngestBufferPool.Return(_payload);
        }
        _payload       = bigger;
        _payloadPooled = !exact;
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
