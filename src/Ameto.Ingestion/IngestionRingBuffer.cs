using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Ameto.Core;

namespace Ameto.Ingestion;

/// <summary>
/// MPMC (Multi-Producer Multi-Consumer) bounded ring buffer backed by NativeMemory.
///
/// The ring stores fixed-size <see cref="Slot"/> descriptors and sequences events with a
/// classic sequence-number algorithm (Dmitry Vyukov's MPMC queue).
///
/// Payloads do NOT live one-per-slot. Instead they are stored in a single shared,
/// bounded <em>slab pool</em> (<see cref="_payloadArena"/>) and handed out via a
/// lock-free free-list (an ABA-safe Treiber stack over slab indices). This decouples
/// payload memory from <see cref="Capacity"/>: instead of <c>capacity × maxPayload</c>
/// (e.g. 16384 × 64 KB = 1 GB) reserved up front, only <see cref="_slabCount"/> slabs
/// exist, and pages become resident on demand — when the drainer keeps up, only a
/// handful of slabs are ever touched, so RSS stays at a few MB regardless of capacity.
/// When the pool is exhausted, enqueue applies back-pressure (returns false), exactly
/// like a full ring.
///
/// Thread safety: fully concurrent producers and consumers with no locks.
/// </summary>
public sealed unsafe class IngestionRingBuffer : IDisposable
{
    /// <summary>Total bytes the shared payload pool may commit at most (back-pressure ceiling).</summary>
    private const long DefaultPayloadPoolBytes = 512L * 1024 * 1024; // virtual worst case; pages fault in on demand

    // ── Slot layout (64 bytes = one cache line) ────────────────────────────────
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct Slot
    {
        [FieldOffset( 0)] public long    Sequence;
        [FieldOffset( 8)] public long    TimestampTicks;
        [FieldOffset(16)] public byte    Level;
        [FieldOffset(17)] public byte    _pad1;
        [FieldOffset(18)] public ushort  _pad2;
        [FieldOffset(20)] public int     PayloadLen;
        [FieldOffset(24)] public int     TemplateIdx;
        [FieldOffset(28)] public int     ServiceNameIdx;   // intern-pool index (-1 if absent)
        [FieldOffset(32)] public int     PayloadSlab;      // index into the shared slab pool (-1 = none)
        [FieldOffset(40)] public ulong   TraceIdHi;        // hi 64 bits of 128-bit TraceId
        [FieldOffset(48)] public ulong   TraceIdLo;        // lo 64 bits of 128-bit TraceId
        [FieldOffset(56)] public ulong   SpanId;           // 64-bit SpanId
    }

    private readonly int    _capacity;   // power of two
    private readonly long   _mask;       // capacity - 1
    private readonly Slot*  _slots;      // NativeMemory array
    private readonly nuint  _slotsBytes;

    // ── Shared payload slab pool ────────────────────────────────────────────────
    // One contiguous arena of _slabCount × _slabBytes. Slabs are handed out by index
    // through a lock-free free-list; the arena is sized to PayloadPoolBytes, NOT to
    // capacity, so payload memory no longer scales with the ring size.
    private readonly byte*       _payloadArena;
    private readonly nuint       _payloadArenaBytes;
    private readonly int         _slabBytes;     // max payload bytes per event
    private readonly int         _slabCount;
    private readonly int*        _slabNext;      // free-list chain: _slabNext[i] = next free slab, or -1
    private          PaddedLong* _freeHead;      // packed (version:hi32 | index:lo32); index -1 ⇒ empty

    // Per-slot message-template strings + structured exceptions (managed, parallel to slots).
    private readonly string?[]        _slotTemplates;
    private readonly ExceptionInfo?[] _slotExceptions;

    // Cursors — each on its own cache line to avoid false sharing
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct PaddedLong { [FieldOffset(0)] public long Value; }

    private PaddedLong* _enqueuePos;
    private PaddedLong* _dequeuePos;

    private bool _disposed;

    // ── Construction ─────────────────────────────────────────────────────────

    /// <param name="capacity">Ring sequencing slots. Must be a power of two. Recommended: 1 << 16 = 65536.</param>
    /// <param name="maxPayloadBytesPerSlot">Max msgpack bytes per event (slab size). Default 64 KB.</param>
    /// <param name="payloadPoolBytes">
    /// Slab arena budget. slabCount = min(capacity, budget / slabSize) — this, not the ring
    /// capacity, is the true absorption window when the drainer stalls: once slabs run out,
    /// events with payloads are dropped even with free ring slots. The arena is reserved
    /// virtual memory; only pages actually written become resident (~real payload bytes,
    /// not slabCount × slabSize), so a generous budget costs little RSS.
    /// </param>
    public IngestionRingBuffer(int capacity = 1 << 16, int maxPayloadBytesPerSlot = 64 * 1024, long payloadPoolBytes = DefaultPayloadPoolBytes)
    {
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0)
            throw new ArgumentException("capacity must be a positive power of two", nameof(capacity));

        _capacity = capacity;
        _mask     = capacity - 1L;

        // Allocate slot array
        _slotsBytes = (nuint)(capacity * sizeof(Slot));
        _slots      = (Slot*)NativeMemory.AllocZeroed(_slotsBytes);
        for (int i = 0; i < capacity; i++)
            _slots[i].Sequence = i;

        // ── Shared payload slab pool ────────────────────────────────────────────
        // Cap the pool at payloadPoolBytes (and never more slabs than ring slots).
        _slabBytes = maxPayloadBytesPerSlot;
        _slabCount = (int)Math.Min(capacity, Math.Max(1, payloadPoolBytes / _slabBytes));

        _payloadArenaBytes = (nuint)((long)_slabCount * _slabBytes);
        _payloadArena      = (byte*)NativeMemory.Alloc(_payloadArenaBytes); // reserve only; pages fault in on demand

        // Free-list: chain every slab, head = slab 0 (version 0).
        _slabNext = (int*)NativeMemory.Alloc((nuint)(_slabCount * sizeof(int)));
        for (int i = 0; i < _slabCount - 1; i++) _slabNext[i] = i + 1;
        _slabNext[_slabCount - 1] = -1;

        _freeHead = (PaddedLong*)NativeMemory.AllocZeroed((nuint)sizeof(PaddedLong)); // value 0 ⇒ idx 0, ver 0

        _slotTemplates  = new string?[capacity];
        _slotExceptions = new ExceptionInfo?[capacity];

        // Allocate cursors
        _enqueuePos = (PaddedLong*)NativeMemory.AllocZeroed((nuint)sizeof(PaddedLong));
        _dequeuePos = (PaddedLong*)NativeMemory.AllocZeroed((nuint)sizeof(PaddedLong));
    }

    // ── Properties ────────────────────────────────────────────────────────────

    public int Capacity => _capacity;

    /// <summary>Approximate pending count (not exact due to concurrent access).</summary>
    public int ApproximateCount
    {
        get
        {
            long e = Volatile.Read(ref _enqueuePos->Value);
            long d = Volatile.Read(ref _dequeuePos->Value);
            long diff = e - d;
            return diff < 0 ? 0 : diff > _capacity ? _capacity : (int)diff;
        }
    }

    // ── Slab pool (lock-free Treiber stack, ABA-safe via versioned head) ────────

    /// <summary>Pops a free slab index, or -1 when the payload pool is exhausted.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int AcquireSlab()
    {
        while (true)
        {
            long head = Volatile.Read(ref _freeHead->Value);
            int  idx  = unchecked((int)head);          // low 32 bits; -1 ⇒ empty
            if (idx < 0) return -1;
            int  next = _slabNext[idx];
            long newHead = unchecked((((head >> 32) + 1) << 32) | (uint)next); // bump version, swing to next
            if (Interlocked.CompareExchange(ref _freeHead->Value, newHead, head) == head)
                return idx;
        }
    }

    /// <summary>Returns a slab index to the free-list.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReleaseSlab(int idx)
    {
        while (true)
        {
            long head = Volatile.Read(ref _freeHead->Value);
            _slabNext[idx] = unchecked((int)head);     // our slab points at the old head
            long newHead = unchecked((((head >> 32) + 1) << 32) | (uint)idx);
            if (Interlocked.CompareExchange(ref _freeHead->Value, newHead, head) == head)
                return;
        }
    }

    // ── Enqueue ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Non-blocking enqueue. Returns false if the ring is full OR the shared payload
    /// pool is exhausted (back-pressure). Copies <paramref name="payload"/> into a slab.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(
        long    timestampTicks,
        byte    level,
        int     templateIdx,
        string? template,
        ExceptionInfo? exception,
        ReadOnlySpan<byte> payload,
        ulong   traceIdHi      = 0,
        ulong   traceIdLo      = 0,
        ulong   spanId         = 0,
        int     serviceNameIdx = -1)
    {
        if (payload.Length > _slabBytes)
            return false; // drop oversized event

        // Reserve payload storage first: if the pool is full we apply back-pressure
        // without ever claiming a ring slot we couldn't fill.
        int slab = AcquireSlab();
        if (slab < 0) return false;

        long pos;
        Slot* slot;

        while (true)
        {
            pos  = Volatile.Read(ref _enqueuePos->Value);
            slot = _slots + (pos & _mask);
            long seq  = Volatile.Read(ref slot->Sequence);
            long diff = seq - pos;

            if (diff == 0)
            {
                if (Interlocked.CompareExchange(ref _enqueuePos->Value, pos + 1, pos) == pos)
                    break;
            }
            else if (diff < 0)
            {
                ReleaseSlab(slab); // ring full — give the slab back
                return false;
            }
            // diff > 0: another producer just published here, spin
        }

        // We own the slot and a private slab — write payload.
        byte* buf = _payloadArena + (long)slab * _slabBytes;
        if (payload.Length > 0)
            payload.CopyTo(new Span<byte>(buf, payload.Length));

        int slotIdx = (int)(pos & _mask);
        _slotTemplates [slotIdx] = template;
        _slotExceptions[slotIdx] = exception;

        slot->TimestampTicks = timestampTicks;
        slot->Level          = level;
        slot->TemplateIdx    = templateIdx;
        slot->PayloadLen     = payload.Length;
        slot->PayloadSlab    = slab;
        slot->TraceIdHi      = traceIdHi;
        slot->TraceIdLo      = traceIdLo;
        slot->SpanId         = spanId;
        slot->ServiceNameIdx = serviceNameIdx;

        // Publish: advance sequence to pos+1
        Volatile.Write(ref slot->Sequence, pos + 1);
        return true;
    }

    // ── Dequeue ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Non-blocking dequeue. Returns false if the buffer is empty.
    /// Copies payload into <paramref name="payloadBuffer"/> and sets <paramref name="payloadLength"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(
        out long    timestampTicks,
        out byte    level,
        out int     templateIdx,
        out string? template,
        out ExceptionInfo? exception,
        Span<byte> payloadBuffer,
        out int    payloadLength,
        out ulong  traceIdHi,
        out ulong  traceIdLo,
        out ulong  spanId,
        out int    serviceNameIdx)
    {
        timestampTicks = 0;
        level          = 0;
        templateIdx    = 0;
        template       = null;
        exception      = null;
        payloadLength  = 0;
        traceIdHi      = 0;
        traceIdLo      = 0;
        spanId         = 0;
        serviceNameIdx = -1;

        long pos;
        Slot* slot;

        while (true)
        {
            pos  = Volatile.Read(ref _dequeuePos->Value);
            slot = _slots + (pos & _mask);
            long seq  = Volatile.Read(ref slot->Sequence);
            long diff = seq - (pos + 1);

            if (diff == 0)
            {
                if (Interlocked.CompareExchange(ref _dequeuePos->Value, pos + 1, pos) == pos)
                    break;
            }
            else if (diff < 0)
            {
                return false; // empty
            }
            // diff > 0: slot is ahead (wrap-around in progress), spin
        }

        // Read payload out of its slab, then free the slab back to the pool.
        int len  = slot->PayloadLen;
        int slab = slot->PayloadSlab;
        if (len > 0 && slab >= 0 && payloadBuffer.Length >= len)
        {
            byte* src = _payloadArena + (long)slab * _slabBytes;
            new ReadOnlySpan<byte>(src, len).CopyTo(payloadBuffer);
        }

        timestampTicks = slot->TimestampTicks;
        level          = slot->Level;
        templateIdx    = slot->TemplateIdx;
        payloadLength  = len;
        traceIdHi      = slot->TraceIdHi;
        traceIdLo      = slot->TraceIdLo;
        spanId         = slot->SpanId;
        serviceNameIdx = slot->ServiceNameIdx;

        int slotIdx = (int)(pos & _mask);
        template                 = _slotTemplates [slotIdx];
        exception                = _slotExceptions[slotIdx];
        _slotTemplates [slotIdx] = null;
        _slotExceptions[slotIdx] = null;

        // Return the slab once its bytes have been copied out. The consumer already
        // holds the data, so a producer may safely reuse this slab immediately.
        if (slab >= 0)
        {
            slot->PayloadSlab = -1;
            ReleaseSlab(slab);
        }

        // Release slot: advance sequence by capacity so the next wrap-around can reuse it
        Volatile.Write(ref slot->Sequence, pos + _capacity);
        return true;
    }

    // ── IDisposable ──────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        NativeMemory.Free(_payloadArena);
        NativeMemory.Free(_slabNext);
        NativeMemory.Free(_freeHead);
        NativeMemory.Free(_slots);
        NativeMemory.Free(_enqueuePos);
        NativeMemory.Free(_dequeuePos);
    }
}
