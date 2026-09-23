using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Ameto.Tracing.Ingestion;

/// <summary>
/// MPMC lock-free ring buffer for span ingestion.
/// Mirrors the pattern of <c>IngestionRingBuffer</c> in Ameto.Ingestion.
///
/// <para><b>BACK-PRESSURE BY BYTES AS WELL AS SLOTS</b> (TS#9). A full ring is the burst the drainer
/// has not caught up with, and what it holds is what a small host has no room for — so the ring
/// refuses a span when taking it would put more than <see cref="MaxBytes"/> in flight, whatever
/// slots are free. The count alone had no idea how big the items were: 65 536 ordinary spans held
/// 35 MB, and 8 192 spans carrying 10 KB of attributes held 80 MB (<c>SpanRingBytesProbe</c>). By
/// default the budget is one hot tier's (<c>TracesOptions.RingMaxBytes</c>): what the ring holds is
/// about to join the tier, and a backlog heavier than a tier is one the tier cannot take anyway.</para>
/// </summary>
internal sealed class SpanRingBuffer : IDisposable
{
    private const int DefaultCapacity = 1 << 16; // 65 536 slots

    private readonly SpanIngestItem?[] _slots;
    private readonly int               _mask;
    private readonly long              _maxBytes;
    private long                       _head; // next write position
    private long                       _tail; // next read position

    /// <summary>What the spans in flight weigh by <see cref="RingBytes"/>: added before a slot is claimed, removed as the drainer takes them.</summary>
    private long _bytesInFlight;

    /// <summary>Spans refused because the byte budget was spent — as opposed to every slot being taken.</summary>
    private long _refusedForBytes;

    // Signal so the consumer can block while idle instead of polling every 5 ms.
    private readonly SemaphoreSlim _signal = new(0, 1);

    /// <param name="capacity">Slots; a power of two.</param>
    /// <param name="maxBytes">
    /// The most the spans in flight may weigh (<see cref="RingBytes"/>). Unbounded when omitted —
    /// the container passes <c>TracesOptions.EffectiveRingMaxBytes</c>.
    /// </param>
    public SpanRingBuffer(int capacity = DefaultCapacity, long maxBytes = long.MaxValue)
    {
        if (BitOperations.IsPow2(capacity) is false)
            throw new ArgumentException("Capacity must be a power of two.", nameof(capacity));
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBytes, 1);

        _slots    = new SpanIngestItem?[capacity];
        _mask     = capacity - 1;
        _maxBytes = maxBytes;
    }

    /// <summary>
    /// Blocks until a producer signals new items or <paramref name="timeoutMs"/> elapses.
    /// Lets the single consumer park while idle instead of busy-polling.
    /// </summary>
    public Task WaitForItemsAsync(int timeoutMs, CancellationToken ct) =>
        _signal.WaitAsync(timeoutMs, ct);

    /// <summary>Slots — <c>Traces:RingCapacity</c>, rounded up to a power of two.</summary>
    public int Capacity => _slots.Length;

    /// <summary>The byte budget — <c>Traces:RingMaxBytes</c>.</summary>
    public long MaxBytes => _maxBytes;

    /// <summary>What the spans in flight weigh right now.</summary>
    public long BytesInFlight => Interlocked.Read(ref _bytesInFlight);

    /// <summary>Spans refused because the byte budget was spent, since the ring was built.</summary>
    public long RefusedForBytes => Interlocked.Read(ref _refusedForBytes);

    /// <summary>
    /// What one span keeps alive while it waits in the ring: the item, its name string, its
    /// attribute array and the slot. <c>SpanRingBytesProbe</c> reads 560 B for the ordinary span
    /// (375-byte blob, 15-character name) and this says 550. The service string is one per resource
    /// block, shared by every span under it, and is not charged.
    /// </summary>
    internal static long RingBytes(SpanIngestItem item) =>
        RingItemOverheadBytes + (item.AttributesBytes?.Length ?? 0) + 2L * (item.Name?.Length ?? 0);

    /// <summary>The item (~96 B), the name string's header, the blob array's header and the slot.</summary>
    private const int RingItemOverheadBytes = 145;

    /// <summary>
    /// Returns a value in [0, 1] representing how full the buffer is — by slots or by bytes,
    /// whichever is nearer its limit.
    /// </summary>
    public double FillFraction
    {
        get
        {
            var h = Volatile.Read(ref _head);
            var t = Volatile.Read(ref _tail);
            double bySlots = (double)(h - t) / _slots.Length;
            double byBytes = (double)Interlocked.Read(ref _bytesInFlight) / _maxBytes;
            return Math.Max(bySlots, byBytes);
        }
    }

    /// <summary>
    /// Enqueue a single item. Returns false when the buffer is full — every slot taken, or the
    /// byte budget spent. Thread-safe for multiple producers.
    /// </summary>
    public bool TryEnqueue(SpanIngestItem item)
    {
        // The BYTES are reserved first and given back if no slot is free, so the budget is a
        // strict bound under any number of producers: an add-then-check can overshoot only by what
        // the losers of the check give straight back, never by a span that stays.
        long weight = RingBytes(item);
        if (Interlocked.Add(ref _bytesInFlight, weight) > _maxBytes)
        {
            Interlocked.Add(ref _bytesInFlight, -weight);
            Interlocked.Increment(ref _refusedForBytes);
            return false;
        }

        while (true)
        {
            var head = Volatile.Read(ref _head);
            var tail = Volatile.Read(ref _tail);

            if (head - tail >= _slots.Length)
            {
                Interlocked.Add(ref _bytesInFlight, -weight);
                return false; // full
            }

            if (Interlocked.CompareExchange(ref _head, head + 1, head) == head)
            {
                _slots[head & _mask] = item;
                // Wake the drainer if it is parked. Cheap when already signaled.
                if (_signal.CurrentCount == 0)
                {
                    try { _signal.Release(); }
                    catch (SemaphoreFullException) { }
                }
                return true;
            }
        }
    }

    /// <summary>
    /// Dequeue up to <paramref name="maxItems"/> items into <paramref name="dest"/>.
    /// Returns the number of items actually dequeued.
    /// Single-consumer path.
    /// </summary>
    public int TryDequeueMany(SpanIngestItem?[] dest, int maxItems)
    {
        int  count = 0;
        long freed = 0;
        while (count < maxItems)
        {
            var tail = Volatile.Read(ref _tail);
            var head = Volatile.Read(ref _head);
            if (tail >= head) break;

            var item = Volatile.Read(ref _slots[tail & _mask]);
            if (item is null) break; // producer hasn't written yet (rare race)

            Volatile.Write(ref _slots[tail & _mask], null); // release slot
            Interlocked.Increment(ref _tail);
            dest[count++] = item;
            freed += RingBytes(item);
        }
        // Once per batch: the drainer holds these now, and the tier's own budget takes over.
        if (freed != 0) Interlocked.Add(ref _bytesInFlight, -freed);
        return count;
    }

    public void Dispose() => _signal.Dispose();
}

// Alias for System.Numerics.BitOperations available in net6+
file static class BitOperations
{
    public static bool IsPow2(int v) => v > 0 && (v & (v - 1)) == 0;
}
