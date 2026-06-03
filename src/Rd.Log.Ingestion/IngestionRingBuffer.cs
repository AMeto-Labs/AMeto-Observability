using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Rd.Log.Core;

namespace Rd.Log.Ingestion;

/// <summary>
/// MPMC (Multi-Producer Multi-Consumer) bounded ring buffer backed by NativeMemory.
///
/// Each slot stores a variable-length byte payload (serialised log event properties).
/// The ring stores fixed-size <see cref="Slot"/> descriptors; payloads live in a
/// separate heap-allocated byte array per slot (minimises unsafe surface while keeping
/// the hot-path descriptor array off-heap for cache efficiency).
///
/// Capacity must be a power of two. The implementation uses a classic sequence-number
/// algorithm (Dmitry Vyukov's MPMC queue) with Interlocked operations on slot sequences.
///
/// Thread safety: fully concurrent producers and consumers with no locks.
/// </summary>
public sealed unsafe class IngestionRingBuffer : IDisposable
{
    // ── Slot layout (64 bytes = one cache line) ────────────────────────────────
    // sequence : long  (8)
    // timestamp: long  (8)
    // level    : byte  (1)
    // _pad1    : byte  (1)
    // _pad2    : ushort(2)
    // payloadLen: int  (4)
    // templateIdx: int (4)
    // _pad3    : int   (4)
    // payload  : IntPtr(8) — pointer to GC-heap byte[] pinned per slot
    // _pad     : 24 bytes
    // Total    : 64 bytes
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
        [FieldOffset(32)] public IntPtr  PayloadPtr;       // byte* into PayloadBuf
        [FieldOffset(40)] public ulong   TraceIdHi;        // hi 64 bits of 128-bit TraceId
        [FieldOffset(48)] public ulong   TraceIdLo;        // lo 64 bits of 128-bit TraceId
        [FieldOffset(56)] public ulong   SpanId;           // 64-bit SpanId
    }

    private readonly int    _capacity;   // power of two
    private readonly long   _mask;       // capacity - 1
    private readonly Slot*  _slots;      // NativeMemory array
    private readonly nuint  _slotsBytes;

    // Per-slot payload buffers — avoid GC pinning by allocating per-slot on NativeMemory too
    // _slotPayloads[i] points to a NativeMemory block of _maxPayloadBytes
    private readonly byte** _payloadBufs;
    private readonly int    _maxPayloadBytes;

    // Per-slot message-template strings (managed). Parallel to the native slot
    // array. Lets the drainer hand the original template to StorageEngine even
    // when the StringInternPool entry is missing or has been evicted — so cold
    // segments preserve the template text instead of silently writing empty.
    private readonly string?[] _slotTemplates;

    // Per-slot structured exception payloads (managed, parallel to _slotTemplates).
    // Stored separately from the msgpack properties payload so the drainer can
    // hand the typed object straight to the storage engine and indexer without
    // re-deserialising.
    private readonly ExceptionInfo?[] _slotExceptions;

    // Cursors — each on its own cache line to avoid false sharing
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct PaddedLong { [FieldOffset(0)] public long Value; }

    private PaddedLong* _enqueuePos;
    private PaddedLong* _dequeuePos;

    private bool _disposed;

    // ── Construction ─────────────────────────────────────────────────────────

    /// <param name="capacity">Must be a power of two. Recommended: 1 << 14 = 16384.</param>
    /// <param name="maxPayloadBytesPerSlot">Max msgpack bytes per event. Default 64 KB.</param>
    public IngestionRingBuffer(int capacity = 1 << 14, int maxPayloadBytesPerSlot = 64 * 1024)
    {
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0)
            throw new ArgumentException("capacity must be a positive power of two", nameof(capacity));

        _capacity        = capacity;
        _mask            = capacity - 1L;
        _maxPayloadBytes = maxPayloadBytesPerSlot;

        // Allocate slot array
        _slotsBytes = (nuint)(capacity * sizeof(Slot));
        _slots      = (Slot*)NativeMemory.AllocZeroed(_slotsBytes);

        // Initialise sequences
        for (int i = 0; i < capacity; i++)
            _slots[i].Sequence = i;

        // Allocate per-slot payload buffers
        _payloadBufs = (byte**)NativeMemory.AllocZeroed((nuint)(capacity * sizeof(IntPtr)));
        for (int i = 0; i < capacity; i++)
            _payloadBufs[i] = (byte*)NativeMemory.Alloc((nuint)maxPayloadBytesPerSlot);

        _slotTemplates = new string?[capacity];
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

    // ── Enqueue ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Non-blocking enqueue. Returns false if the buffer is full (back-pressure).
    /// Copies <paramref name="payload"/> into the slot's NativeMemory buffer.
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
        if (payload.Length > _maxPayloadBytes)
            return false; // drop oversized event

        long pos;
        Slot* slot;

        while (true)
        {
            pos  = Volatile.Read(ref _enqueuePos->Value);
            slot = _slots + (pos & _mask);
            long seq = Volatile.Read(ref slot->Sequence);
            long diff = seq - pos;

            if (diff == 0)
            {
                // Slot is available — try to claim it
                if (Interlocked.CompareExchange(ref _enqueuePos->Value, pos + 1, pos) == pos)
                    break;
            }
            else if (diff < 0)
            {
                // Buffer full
                return false;
            }
            // diff > 0: another producer just published here, spin
        }

        // We own the slot — write payload
        byte* buf = _payloadBufs[pos & _mask];
        if (payload.Length > 0)
            payload.CopyTo(new Span<byte>(buf, payload.Length));

        _slotTemplates [pos & _mask] = template;
        _slotExceptions[pos & _mask] = exception;

        slot->TimestampTicks  = timestampTicks;
        slot->Level            = level;
        slot->TemplateIdx      = templateIdx;
        slot->PayloadLen       = payload.Length;
        slot->PayloadPtr       = (IntPtr)buf;
        slot->TraceIdHi        = traceIdHi;
        slot->TraceIdLo        = traceIdLo;
        slot->SpanId           = spanId;
        slot->ServiceNameIdx   = serviceNameIdx;

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
                // Slot is ready — try to claim it
                if (Interlocked.CompareExchange(ref _dequeuePos->Value, pos + 1, pos) == pos)
                    break;
            }
            else if (diff < 0)
            {
                // Buffer empty
                return false;
            }
            // diff > 0: slot is ahead (wrap-around in progress), spin
        }

        // Read payload
        int len = slot->PayloadLen;
        if (len > 0 && payloadBuffer.Length >= len)
        {
            byte* src = (byte*)slot->PayloadPtr;
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

        // Hand off the template / exception refs and clear the slot so the GC can
        // collect them when no longer referenced.
        int slotIdx = (int)(pos & _mask);
        template                 = _slotTemplates [slotIdx];
        exception                = _slotExceptions[slotIdx];
        _slotTemplates [slotIdx] = null;
        _slotExceptions[slotIdx] = null;

        // Release slot: advance sequence by capacity so the next wrap-around can reuse it
        Volatile.Write(ref slot->Sequence, pos + _capacity);
        return true;
    }

    // ── IDisposable ──────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        for (int i = 0; i < _capacity; i++)
            NativeMemory.Free(_payloadBufs[i]);

        NativeMemory.Free(_payloadBufs);
        NativeMemory.Free(_slots);
        NativeMemory.Free(_enqueuePos);
        NativeMemory.Free(_dequeuePos);
    }
}
