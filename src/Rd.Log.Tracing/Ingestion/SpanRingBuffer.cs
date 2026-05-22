using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Rd.Log.Tracing.Ingestion;

/// <summary>
/// MPMC lock-free ring buffer for span ingestion.
/// Mirrors the pattern of <c>IngestionRingBuffer</c> in Rd.Log.Ingestion.
/// </summary>
internal sealed class SpanRingBuffer : IDisposable
{
    private const int DefaultCapacity = 1 << 16; // 65 536 slots

    private readonly SpanIngestItem?[] _slots;
    private readonly int               _mask;
    private long                       _head; // next write position
    private long                       _tail; // next read position

    public SpanRingBuffer(int capacity = DefaultCapacity)
    {
        if (BitOperations.IsPow2(capacity) is false)
            throw new ArgumentException("Capacity must be a power of two.", nameof(capacity));

        _slots = new SpanIngestItem?[capacity];
        _mask  = capacity - 1;
    }

    /// <summary>Returns a value in [0, 1] representing how full the buffer is.</summary>
    public double FillFraction
    {
        get
        {
            var h = Volatile.Read(ref _head);
            var t = Volatile.Read(ref _tail);
            return (double)(h - t) / _slots.Length;
        }
    }

    /// <summary>
    /// Enqueue a single item. Returns false when the buffer is full.
    /// Thread-safe for multiple producers.
    /// </summary>
    public bool TryEnqueue(SpanIngestItem item)
    {
        while (true)
        {
            var head = Volatile.Read(ref _head);
            var tail = Volatile.Read(ref _tail);

            if (head - tail >= _slots.Length)
                return false; // full

            if (Interlocked.CompareExchange(ref _head, head + 1, head) == head)
            {
                _slots[head & _mask] = item;
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
        int count = 0;
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
        }
        return count;
    }

    public void Dispose() { /* slots array is managed memory */ }
}

// Alias for System.Numerics.BitOperations available in net6+
file static class BitOperations
{
    public static bool IsPow2(int v) => v > 0 && (v & (v - 1)) == 0;
}
