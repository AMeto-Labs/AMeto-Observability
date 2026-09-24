using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Ameto.Core;
using Ameto.Tracing.Storage;

namespace Ameto.Tracing.Ingestion;

/// <summary>
/// THE RAW SPAN RING (TI#3): many producers (request threads, through <see cref="ISpanSink"/>),
/// one consumer (<see cref="SpanDrainer"/>). A slot is a 72-byte <see cref="SpanHeader"/> plus its
/// sequence, in native memory; a span's name, service and attribute blob are copied into a slab
/// arena. Nothing in the ring is a managed object, so a backlog is not a heap the GC must scan
/// and promote — the old ring was a <c>SpanIngestItem?[65 536]</c> on the large object heap,
/// holding up to 65 536 live graphs (35 MB of ordinary spans, <c>SpanRingBytesProbe</c>).
///
/// <para><b>Sequencing</b> is Dmitry Vyukov's bounded MPMC queue, the log ring's: a producer
/// claims a position with one CAS on the enqueue cursor and PUBLISHES the slot by advancing its
/// sequence after writing it. The sequence is stored RELATIVE to the slot's index
/// (<c>sequence − index</c>), so zeroed memory is the initial state and creating a ring touches
/// none of its pages. A claimed-but-unwritten slot is exactly the old ring's "published head with
/// a null slot": the consumer STOPS there — it never skips it and never spins on it — and
/// returns what it has; the slot is taken, in order, on the next pass.</para>
///
/// <para><b>The arena</b> is a <see cref="SlabArena"/> — reserved, committed as it is reached, so
/// residency is the deepest the backlog has ever been — cut into <see cref="ChunkBytes"/> chunks
/// handed out LIFO by a versioned (ABA-safe) Treiber stack. A producer thread packs consecutive
/// spans of its batch into ITS chunk with a plain bump pointer (no CAS per span), and each chunk
/// is reference-counted: +1 while a producer holds it, +1 per span in it; the consumer drops a
/// span's reference when it releases the drained batch, and the last reference returns the chunk.
/// A drainer that keeps up therefore touches one or two chunks, reused over and over. A payload
/// larger than a chunk is PARKED apart in a pre-sized managed array — rare, and never dropped for its
/// size.</para>
///
/// <para><b>Back-pressure</b> is by slots AND by bytes (TS#9): a span's payload bytes are reserved
/// against <see cref="MaxBytes"/> before anything else, so a burst of heavy spans is refused at the
/// budget with slots to spare.</para>
///
/// <para><b>The native footprint, resting and at peak</b> (none of it is under a MemoryBudgets
/// share — the physical shares were not re-cut this round): the slot array, <see cref="Capacity"/>
/// x 80 B, is fixed and resident once the ring has cycled — 5.0 MB at the default 65 536 slots,
/// sized by <c>Traces:RingCapacity</c>; the arena is reserved at <see cref="MaxBytes"/> plus 64
/// slack chunks and committed only as deep as a backlog reaches — at most the budget plus the
/// slack while a burst lasts (SpanRingBytesProbe on the 512 MB stand's budget: 33.5 MB with the
/// slots) — and given back above <see cref="LowWaterChunks"/> (1 MB) by the drainer's idle trim
/// (<see cref="TrimIdleArena"/>), so at rest the ring holds the slots and 1 MB: 6.0 MB.</para>
///
/// <para><b>What the arena is NOT</b>: the storage behind anything a reader holds. The drainer
/// copies each blob out and turns each name and service into a pool string before it releases the
/// batch (<c>TraceStorageEngine.WriteRaw</c>), so a chunk can be reused the instant its last span
/// is drained while every lock-free reader keeps walking managed records.</para>
/// </summary>
internal sealed unsafe class SpanRingBuffer : IDisposable
{
    private const int DefaultCapacity = 1 << 16; // 65 536 slots

    /// <summary>One arena chunk — and the largest payload kept in the arena.</summary>
    internal const int ChunkBytes = 64 * 1024;

    /// <summary>The arena is committed in these steps as the backlog reaches deeper (Windows).</summary>
    private const long CommitChunkBytes = 1L * 1024 * 1024;

    /// <summary>
    /// Chunks beyond the byte budget: the part-filled ones producers are holding, and the tail
    /// a chunk loses when the next span does not fit. Address space only until reached.
    /// </summary>
    private const int SlackChunks = 64;

    /// <summary>1 GB of chunks: the most one ring may reserve, whatever the budget says.</summary>
    private const int MaxChunks = 16_384;

    [StructLayout(LayoutKind.Sequential)]
    private struct Slot
    {
        public long       Sequence;   // RELATIVE: the Vyukov sequence minus this slot's index
        public SpanHeader Header;     // 72 B
    }

    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct PaddedLong { [FieldOffset(0)] public long Value; }

    private readonly int         _capacity;
    private readonly long        _mask;
    private readonly Slot*       _slots;
    private readonly PaddedLong* _cursors;       // [0] enqueue, [1] dequeue, [2] free-chunk head (version:hi32 | index:lo32)

    private readonly SlabArena   _arena;
    private readonly byte*       _base;
    private readonly int         _chunkCount;
    private readonly int*        _chunkNext;     // free-list chain
    private readonly int*        _chunkRefs;     // producer hold + one per span in the chunk
    private long                 _chunkHighWater;

    private readonly long        _maxBytes;
    private long                 _bytesInFlight;

    /// <summary>
    /// Payloads larger than a chunk, PARKED here by index — a slot's header names its parking place
    /// as <see cref="SpanHeader.PayloadArenaOffset"/> = <c>-2 - index</c>. Pre-sized (one place per
    /// arena chunk, so the byte budget always binds before the parking lot does) and claimed, and
    /// filled, BEFORE the ring slot is: nothing between a slot's claim and its publish may allocate.
    /// It used to be a <c>ConcurrentDictionary</c> keyed by ring position and written AFTER the
    /// claim — a node allocation in the one window where a throw wedges the ring (review F2).
    /// </summary>
    private readonly byte[]?[] _parked;
    private readonly int[]     _parkFree;
    private int                _parkFreeCount;
    private readonly Lock      _parkLock = new();

    /// <summary>
    /// <see cref="SpanHeader.PayloadArenaOffset"/> of a TOMBSTONE: a slot whose producer faulted
    /// after claiming it. Published anyway, so the consumer — which stops at a claimed-but-unwritten
    /// slot and waits for it — steps over it instead of waiting forever.
    /// </summary>
    private const int TombstoneOffset = int.MinValue;

    private readonly SemaphoreSlim _signal = new(0, 1);

    private long _refusedForBytes;
    private long _refusedNoSlot;
    private long _refusedNoArena;

    private int  _disposed;
    private int  _producersInside;
    private bool _freed;

    /// <summary>
    /// The calling thread's open batch: which ring it is producing into, the chunk it is packing
    /// spans into, and how far. One per thread — a request thread parses one batch at a time.
    /// </summary>
    private struct ProducerState
    {
        public SpanRingBuffer? Owner;
        public int             Chunk;   // -1 = none
        public int             Used;
    }

    [ThreadStatic] private static ProducerState t_producer;

    /// <param name="capacity">Slots; a power of two.</param>
    /// <param name="maxBytes">
    /// The most the payloads in flight may weigh. 0 or less: <paramref name="capacity"/> x
    /// <see cref="TracesOptions.OrdinaryRingSpanBytes"/>, the old full ring's worth — the container
    /// passes <c>TracesOptions.EffectiveRingMaxBytes</c>.
    /// </param>
    /// <param name="pools">The service intern pool producers intern into. Null: the ring's own.</param>
    public SpanRingBuffer(int capacity = DefaultCapacity, long maxBytes = 0, SpanStringPools? pools = null)
    {
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0)
            throw new ArgumentException("Capacity must be a power of two.", nameof(capacity));

        _capacity = capacity;
        _mask     = capacity - 1L;
        Pools     = pools ?? new SpanStringPools();

        if (maxBytes <= 0) maxBytes = (long)capacity * TracesOptions.OrdinaryRingSpanBytes;
        long chunks = Math.Min(MaxChunks, (maxBytes + ChunkBytes - 1) / ChunkBytes + SlackChunks);
        _chunkCount = (int)chunks;
        _maxBytes   = Math.Min(maxBytes, (long)_chunkCount * ChunkBytes);

        // Zeroed IS the initial state (relative sequences), so neither array is walked here and no
        // page of either is touched until a span reaches it.
        _slots   = (Slot*)NativeMemory.AllocZeroed((nuint)capacity, (nuint)sizeof(Slot));
        _cursors = (PaddedLong*)NativeMemory.AllocZeroed(3, (nuint)sizeof(PaddedLong));

        _arena = SlabArena.Create((nuint)((long)_chunkCount * ChunkBytes), (nuint)CommitChunkBytes);
        _base  = _arena.Base;

        _chunkNext = (int*)NativeMemory.Alloc((nuint)_chunkCount, sizeof(int));
        for (int i = 0; i < _chunkCount - 1; i++) _chunkNext[i] = i + 1;
        _chunkNext[_chunkCount - 1] = -1;
        _chunkRefs = (int*)NativeMemory.AllocZeroed((nuint)_chunkCount, sizeof(int));
        // Free head: chunk 0, version 0 — the zeroed value.

        _parked        = new byte[]?[_chunkCount];
        _parkFree      = new int[_chunkCount];
        for (int i = 0; i < _chunkCount; i++) _parkFree[i] = _chunkCount - 1 - i;
        _parkFreeCount = _chunkCount;
        _trimTaken     = new byte[_chunkCount];
    }

    /// <summary>The span-name and service pools; producers intern the service here once per resource block.</summary>
    public SpanStringPools Pools { get; }

    /// <summary>Slots — <c>Traces:RingCapacity</c>, rounded up to a power of two.</summary>
    public int Capacity => _capacity;

    /// <summary>The byte budget — <c>Traces:RingMaxBytes</c> (capped at what the arena can reserve).</summary>
    public long MaxBytes => _maxBytes;

    /// <summary>What the payloads in flight weigh right now.</summary>
    public long BytesInFlight => Interlocked.Read(ref _bytesInFlight);

    /// <summary>Spans refused because the byte budget was spent, since the ring was built.</summary>
    public long RefusedForBytes => Interlocked.Read(ref _refusedForBytes);

    /// <summary>Spans refused because every slot was still unread.</summary>
    public long RefusedNoSlot => Interlocked.Read(ref _refusedNoSlot);

    /// <summary>Spans refused because no arena chunk could be had (exhausted, or its pages could not be committed).</summary>
    public long RefusedNoArena => Interlocked.Read(ref _refusedNoArena);

    /// <summary>Bytes of the arena the ring has ever reached — its residency (nothing below it is given back).</summary>
    public long ArenaHighWaterBytes => Volatile.Read(ref _chunkHighWater) * ChunkBytes;

    /// <summary>
    /// What an ingest item weighs against <see cref="MaxBytes"/> once it is in the ring: its payload —
    /// the name and service as UTF-8 and the attribute blob. The slot itself is fixed memory and is
    /// not charged.
    /// </summary>
    internal static long PayloadBytes(SpanIngestItem item) =>
        Encoding.UTF8.GetByteCount(item.Name ?? string.Empty)
      + Encoding.UTF8.GetByteCount(item.ServiceName ?? string.Empty)
      + (item.AttributesBytes?.Length ?? 0);

    /// <summary>Bytes of the slot array — fixed, <see cref="Capacity"/> x 80.</summary>
    public long SlotBytes => (long)_capacity * sizeof(Slot);

    /// <summary>Approximate pending count (not exact under concurrent access).</summary>
    public int ApproximateCount
    {
        get
        {
            if (Volatile.Read(ref _disposed) != 0) return 0;
            long diff = Volatile.Read(ref _cursors[0].Value) - Volatile.Read(ref _cursors[1].Value);
            return diff < 0 ? 0 : diff > _capacity ? _capacity : (int)diff;
        }
    }

    /// <summary>
    /// Returns a value in [0, 1] representing how full the buffer is — by slots or by bytes,
    /// whichever is nearer its limit.
    /// </summary>
    public double FillFraction
    {
        get
        {
            double bySlots = (double)ApproximateCount / _capacity;
            double byBytes = (double)Interlocked.Read(ref _bytesInFlight) / _maxBytes;
            return Math.Max(bySlots, byBytes);
        }
    }

    /// <summary>
    /// Blocks until a producer signals new items or <paramref name="timeoutMs"/> elapses.
    /// Lets the single consumer park while idle instead of busy-polling.
    /// </summary>
    public Task WaitForItemsAsync(int timeoutMs, CancellationToken ct) =>
        _signal.WaitAsync(timeoutMs, ct);

    /// <summary>Test seam: a producer has CLAIMED the slot at this position and not yet written it.</summary>
    internal Action<long>? _afterSlotClaimedForTest;

    // ── Produce ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Takes one span. <paramref name="fields"/> supplies ids, times, kind, status and HTTP status;
    /// the payload fields are the ring's own. False when refused — by bytes, by slots, or for want
    /// of an arena chunk. The calling thread must end its batch with <see cref="EndBatch"/>.
    /// </summary>
    public bool TryEnqueueRaw(
        in SpanHeader fields,
        ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8,
        ReadOnlySpan<byte> attributes)
    {
        ref var st = ref Bind();
        if (st.Owner != this) return false;                              // disposed

        int payload = nameUtf8.Length + serviceUtf8.Length + attributes.Length;

        // 1. BYTES FIRST, as a strict bound: reserve, then check, and give back on refusal.
        if (Interlocked.Add(ref _bytesInFlight, payload) > _maxBytes)
        {
            Interlocked.Add(ref _bytesInFlight, -payload);
            Interlocked.Increment(ref _refusedForBytes);
            return false;
        }

        // 2. Where the payload will live — decided, and for a parked payload ALLOCATED, before any
        //    slot is claimed: from the claim to the publish nothing may allocate (see step 4).
        int offset = -1;
        int park   = -1;
        if (payload > ChunkBytes)
        {
            if (!TryPark(payload, out park))
            {
                Interlocked.Add(ref _bytesInFlight, -payload);
                Interlocked.Increment(ref _refusedNoArena);
                return false;
            }
            offset = -2 - park;
        }
        else if (payload > 0 && !TryReserve(ref st, payload, out offset))
        {
            Interlocked.Add(ref _bytesInFlight, -payload);
            Interlocked.Increment(ref _refusedNoArena);
            return false;
        }

        // 3. A slot.
        long  pos;
        Slot* slot;
        while (true)
        {
            pos  = Volatile.Read(ref _cursors[0].Value);
            slot = _slots + (pos & _mask);
            long diff = Volatile.Read(ref slot->Sequence) + (pos & _mask) - pos;
            if (diff == 0)
            {
                if (Interlocked.CompareExchange(ref _cursors[0].Value, pos + 1, pos) == pos) break;
            }
            else if (diff < 0)
            {
                GiveBack(ref st, payload, offset, park);
                Interlocked.Increment(ref _refusedNoSlot);
                return false;                                            // every slot unread
            }
            // diff > 0: another producer published past us; look again
        }

        // 4. Write the payload, then the header, then PUBLISH. THE SLOT IS CLAIMED: from here to the
        //    publish the consumer is stopped at it, waiting — so this window allocates nothing, and
        //    if anything in it throws all the same, the slot is published as a TOMBSTONE the
        //    consumer steps over, the reservations are given back, and the fault goes to the caller.
        //    Without that, one throw here wedged the ring until a restart (review F2).
        try
        {
            _afterSlotClaimedForTest?.Invoke(pos);

            if (payload > 0)
            {
                var dst = park >= 0 ? _parked[park].AsSpan() : new Span<byte>(_base + offset, payload);
                nameUtf8.CopyTo(dst);
                serviceUtf8.CopyTo(dst[nameUtf8.Length..]);
                attributes.CopyTo(dst[(nameUtf8.Length + serviceUtf8.Length)..]);
            }

            ref var h = ref slot->Header;
            h                      = fields;
            h.NameByteLength       = nameUtf8.Length;
            h.ServiceByteLength    = serviceUtf8.Length;
            h.AttributesByteLength = attributes.Length;
            h.ServiceNamePoolIndex = serviceIdx;
            h.PayloadArenaOffset   = offset;
        }
        catch
        {
            slot->Header = default;
            slot->Header.PayloadArenaOffset = TombstoneOffset;
            Volatile.Write(ref slot->Sequence, pos + 1 - (pos & _mask));
            GiveBack(ref st, payload, offset, park);
            throw;
        }

        Volatile.Write(ref slot->Sequence, pos + 1 - (pos & _mask));

        // Wake the drainer if it is parked. Cheap when already signaled.
        if (_signal.CurrentCount == 0)
        {
            try { _signal.Release(); }
            catch (SemaphoreFullException) { }
            catch (ObjectDisposedException) { }
        }
        return true;
    }

    /// <summary>
    /// An ingest ITEM into the ring: the name and service transcoded to UTF-8 on the stack (pooled
    /// past 1 KB), the service interned. The shape the item APIs (gRPC, the tests) come in through;
    /// the ring still holds only the raw form.
    /// </summary>
    public bool TryEnqueue(SpanIngestItem item)
    {
        string name    = item.Name        ?? string.Empty;
        string service = item.ServiceName ?? string.Empty;
        int    max     = (name.Length + service.Length) * 3;
        byte[]? rented = null;
        Span<byte> buf = max <= 1024 ? stackalloc byte[1024] : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(max));
        try
        {
            int n = Encoding.UTF8.GetBytes(name, buf);
            int s = Encoding.UTF8.GetBytes(service, buf[n..]);
            var svc = buf.Slice(n, s);
            int idx = Pools.ServiceIndex(svc);
            var h   = TraceStorageEngine.HeaderOf(item);
            return TryEnqueueRaw(in h, buf[..n], idx, svc, item.AttributesBytes ?? []);
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// The calling thread's batch into this ring is over: its chunk goes back (or to the spans
    /// still in it, whose release will return it), and the thread stops counting as a producer.
    /// </summary>
    public void EndBatch()
    {
        ref var st = ref t_producer;
        if (st.Owner != this) return;
        CloseBatch(ref st);
    }

    private void CloseBatch(ref ProducerState st)
    {
        if (st.Chunk >= 0) ReleaseChunk(st.Chunk);
        st = new ProducerState { Chunk = -1 };
        Interlocked.Decrement(ref _producersInside);
    }

    /// <summary>
    /// The calling thread's batch state, bound to this ring — opening a batch (counting the thread
    /// as a producer, which is what <see cref="Dispose"/> waits out) on the first span of one.
    /// Owner is left unset when the ring is disposed.
    /// </summary>
    private ref ProducerState Bind()
    {
        ref var st = ref t_producer;
        if (st.Owner == this) return ref st;
        if (st.Owner is { } other) other.CloseBatch(ref st);             // a batch left open on another ring
        st = new ProducerState { Chunk = -1 };

        Interlocked.Increment(ref _producersInside);
        if (Volatile.Read(ref _disposed) != 0) { Interlocked.Decrement(ref _producersInside); return ref st; }
        st.Owner = this;
        return ref st;
    }

    private bool TryReserve(ref ProducerState st, int length, out int offset)
    {
        if (st.Chunk >= 0 && st.Used + length <= ChunkBytes)
        {
            offset   = st.Chunk * ChunkBytes + st.Used;
            st.Used += length;
            Interlocked.Increment(ref _chunkRefs[st.Chunk]);             // the span's reference
            return true;
        }

        if (st.Chunk >= 0) { ReleaseChunk(st.Chunk); st.Chunk = -1; }    // the producer's hold on the full one

        int c = AcquireChunk();
        if (c < 0) { offset = -1; return false; }
        Volatile.Write(ref _chunkRefs[c], 2);                            // the producer's hold + this span
        st.Chunk = c;
        st.Used  = length;
        offset   = c * ChunkBytes;
        return true;
    }

    /// <summary>A reservation that found no slot: it was the last one in the producer's chunk, so it is simply rolled back.</summary>
    private void Unreserve(ref ProducerState st, int length)
    {
        st.Used -= length;
        ReleaseChunk(st.Chunk);                                          // the span's reference; the hold keeps it
    }

    /// <summary>Everything a span reserved on its way in, given back: its arena bytes or its parking place, and its budget.</summary>
    private void GiveBack(ref ProducerState st, int payload, int offset, int park)
    {
        if (park >= 0)        Unpark(park);
        else if (offset >= 0) Unreserve(ref st, payload);
        Interlocked.Add(ref _bytesInFlight, -payload);
    }

    /// <summary>
    /// A parking place for a payload larger than a chunk, with the array for it allocated and stored
    /// there — all before any slot is claimed. False when every place is taken, or when the
    /// allocation itself fails: either is a refusal (back-pressure), never a wedge.
    /// </summary>
    private bool TryPark(int payload, out int park)
    {
        park = -1;
        byte[] bytes;
        try { bytes = new byte[payload]; }
        catch (OutOfMemoryException) { return false; }

        lock (_parkLock)
        {
            if (_parkFreeCount == 0) return false;
            park = _parkFree[--_parkFreeCount];
        }
        _parked[park] = bytes;
        return true;
    }

    /// <summary>Frees a parking place, and lets go of what was parked there.</summary>
    private void Unpark(int park)
    {
        _parked[park] = null;
        lock (_parkLock) _parkFree[_parkFreeCount++] = park;
    }

    // ── Chunks (lock-free Treiber stack, ABA-safe via a versioned head) ─────────

    /// <summary>
    /// Pops a free chunk whose pages are writable, or -1. The commit happens BEFORE the pop, so a
    /// chunk that cannot be committed never leaves the free list (the log ring's rule, for the
    /// reason given there): LIFO reuse keeps every committed free chunk above every uncommitted one.
    /// </summary>
    private int AcquireChunk()
    {
        ref long headRef = ref _cursors[2].Value;
        var spin = new SpinWait();
        while (true)
        {
            long head = Volatile.Read(ref headRef);
            int  idx  = unchecked((int)head);
            if (idx < 0)
            {
                // The trim holds the whole free list for a moment (microseconds, plus one
                // decommit call): an empty list then means "wait", not "full". Wait, never refuse — and
                // with SpinWait's DEFAULT escalation (spin, then yield, then Sleep(1)), not a pure spin:
                // if the drainer is descheduled mid-trim, a pure spin burns every waiting producer's CPU
                // quota in a CPU-limited container for as long as that lasts (review L3).
                if (Volatile.Read(ref _trimming) == 0) return -1;
                _onWaitingForTrimForTest?.Invoke();
                spin.SpinOnce();
                continue;
            }

            if (!_arena.TryEnsureCommitted((nuint)(((long)idx + 1) * ChunkBytes)))
            {
                if (Volatile.Read(ref headRef) != head) continue;
                return -1;
            }

            int  next    = _chunkNext[idx];
            long newHead = unchecked((((head >> 32) + 1) << 32) | (uint)next);
            if (Interlocked.CompareExchange(ref headRef, newHead, head) == head)
            {
                if (idx >= Volatile.Read(ref _chunkHighWater)) RaiseHighWater(idx);
                return idx;
            }
        }
    }

    // ── Idle trim (review F3) ────────────────────────────────────────────────────
    //
    // THE ARENA'S HIGH-WATER MARK WAS A RESTING LEVEL. SlabArena never gave committed memory back —
    // right for the log ring, whose argument is that a LIFO free list asks for the same slabs again
    // on the next burst — so one burst to the byte budget left the span ring holding it for the
    // life of the process: on the 512 MB stand ~27 MB of arena plus up to 64 slack chunks (4 MB),
    // outside every MemoryBudgets share. The drainer now calls TrimIdleArena from the wake it
    // already takes when it finds the ring empty (at most once per TrimInterval), and everything
    // above a low-water mark that no span and no open batch is using goes back to the OS.

    /// <summary>What the trim leaves committed: one 1 MB commit step — a drainer that keeps up works in one or two chunks.</summary>
    internal const int LowWaterChunks = 16;

    private int             _trimming;
    private readonly byte[] _trimTaken;   // one mark per chunk: "in the free list the trim holds"

    /// <summary>Test seam: the trim holds the whole free list, before it has decommitted anything.</summary>
    internal Action? _whileTrimmingForTest;

    /// <summary>Test seam: a producer found the free list empty because a trim holds it, and is waiting.</summary>
    internal Action? _onWaitingForTrimForTest;

    /// <summary>
    /// Gives back the arena above the highest chunk still in use (and above <see cref="LowWaterChunks"/>)
    /// — every chunk it can PROVE is free. Returns the bytes given back. Single caller: the drainer.
    ///
    /// <para><b>Safe against producers without stopping them.</b> The trim first takes the WHOLE free
    /// list with one CAS on the versioned head, so no chunk it examines can be popped under it — a
    /// pop already in flight fails its own CAS and retries, and a producer that finds the list empty
    /// while <c>_trimming</c> is set waits (spin, yield, then sleep) instead of refusing. Only chunks in the list it took are
    /// candidates, and only a contiguous run of them reaching the top of the committed range is given
    /// back, so a chunk a producer holds, or one a drained span still references, stops the run below
    /// it. The chunks go back on the list afterwards — the ones kept first, lowest index on top — and
    /// a chunk above the new committed mark is committed again by <c>AcquireChunk</c>'s
    /// commit-before-pop, exactly as a never-used one always was.</para>
    /// </summary>
    public long TrimIdleArena()
    {
        if (Volatile.Read(ref _disposed) != 0) return 0;
        ref long headRef = ref _cursors[2].Value;

        Volatile.Write(ref _trimming, 1);
        long taken;
        while (true)
        {
            taken = Volatile.Read(ref headRef);
            long empty = unchecked((((taken >> 32) + 1) << 32) | 0xFFFF_FFFFL);
            if (Interlocked.CompareExchange(ref headRef, empty, taken) == taken) break;
        }

        long given = 0;
        int  first = -1, last = -1;                                      // the chain we will push back
        try
        {
            _whileTrimmingForTest?.Invoke();

            Array.Clear(_trimTaken);
            for (int c = unchecked((int)taken); c >= 0; c = _chunkNext[c]) _trimTaken[c] = 1;

            // The top of what can be resident: the committed mark where pages are committed on
            // demand, else the whole arena (Linux, where DONTNEED on untouched pages is a no-op).
            long committed = _arena.CommittedBytes;
            int  top = committed < 0 ? _chunkCount : (int)Math.Min(_chunkCount, (committed + ChunkBytes - 1) / ChunkBytes);
            int  cut = top;
            while (cut > LowWaterChunks && _trimTaken[cut - 1] != 0) cut--;

            if (cut < top)
            {
                given = _arena.TryDecommitTail((nuint)((long)cut * ChunkBytes));
                if (given > 0) LowerHighWater(cut);
            }

            // Push back: kept (committed) chunks on top in index order, the given-back ones below.
            for (int pass = 0; pass < 2; pass++)
                for (int c = 0; c < _chunkCount; c++)
                {
                    if (_trimTaken[c] == 0 || (pass == 0) != (c < cut)) continue;
                    if (first < 0) first = c; else _chunkNext[last] = c;
                    last = c;
                }
        }
        finally
        {
            if (first >= 0)
            {
                while (true)
                {
                    long head = Volatile.Read(ref headRef);                // what was released meanwhile
                    _chunkNext[last] = unchecked((int)head);
                    long newHead = unchecked((((head >> 32) + 1) << 32) | (uint)first);
                    if (Interlocked.CompareExchange(ref headRef, newHead, head) == head) break;
                }
            }
            else if (unchecked((int)taken) >= 0)
            {
                // Faulted before the chain was built: put the list back as it was taken.
                int tail = unchecked((int)taken);
                while (_chunkNext[tail] >= 0) tail = _chunkNext[tail];
                while (true)
                {
                    long head = Volatile.Read(ref headRef);
                    _chunkNext[tail] = unchecked((int)head);
                    long newHead = unchecked((((head >> 32) + 1) << 32) | (uint)unchecked((int)taken));
                    if (Interlocked.CompareExchange(ref headRef, newHead, head) == head) break;
                }
            }
            Volatile.Write(ref _trimming, 0);
        }
        return given;
    }

    private void LowerHighWater(int mark)
    {
        long seen = Volatile.Read(ref _chunkHighWater);
        while (seen > mark)
        {
            long prev = Interlocked.CompareExchange(ref _chunkHighWater, mark, seen);
            if (prev == seen) return;
            seen = prev;
        }
    }

    private void RaiseHighWater(int idx)
    {
        long mark = idx + 1L;
        long seen = Volatile.Read(ref _chunkHighWater);
        while (mark > seen)
        {
            long prev = Interlocked.CompareExchange(ref _chunkHighWater, mark, seen);
            if (prev == seen) return;
            seen = prev;
        }
    }

    /// <summary>Drops one reference; the last one returns the chunk to the free list.</summary>
    private void ReleaseChunk(int idx)
    {
        if (Interlocked.Decrement(ref _chunkRefs[idx]) != 0) return;
        ref long headRef = ref _cursors[2].Value;
        while (true)
        {
            long head = Volatile.Read(ref headRef);
            _chunkNext[idx] = unchecked((int)head);
            long newHead = unchecked((((head >> 32) + 1) << 32) | (uint)idx);
            if (Interlocked.CompareExchange(ref headRef, newHead, head) == head) return;
        }
    }

    // ── Consume (single consumer) ───────────────────────────────────────────────

    /// <summary>
    /// Takes up to <paramref name="headers"/>.Length published spans, in order: their headers are
    /// copied out and their slots freed at once, but their PAYLOADS stay reserved — readable through
    /// <see cref="Drained"/> — until <see cref="Release"/>. Stops at the first slot not yet
    /// published (claimed by a producer that has not written it): never skipped, never waited on.
    /// </summary>
    public int TryDequeueMany(Span<SpanHeader> headers, Span<byte[]?> apart)
    {
        int  count = 0;
        long pos   = Volatile.Read(ref _cursors[1].Value);
        int  max   = Math.Min(headers.Length, apart.Length);
        while (count < max)
        {
            long  index = pos & _mask;
            Slot* slot  = _slots + index;
            if (Volatile.Read(ref slot->Sequence) + index != pos + 1) break;   // empty, or claimed and unwritten

            int offset = slot->Header.PayloadArenaOffset;
            if (offset != TombstoneOffset)                                      // a faulted producer's slot carries nothing
            {
                headers[count] = slot->Header;
                if (offset <= -2)
                {
                    int park = -2 - offset;
                    apart[count] = _parked[park];                               // the drainer holds it now
                    Unpark(park);
                }
                else apart[count] = null;
                count++;
            }

            Volatile.Write(ref slot->Sequence, pos + _capacity - index);        // the slot is free again
            pos++;
        }
        Volatile.Write(ref _cursors[1].Value, pos);
        return count;
    }

    /// <summary>
    /// The payloads of a drained run go back: each span's chunk reference is dropped (the last one
    /// returns the chunk) and its bytes leave the budget. Call once the batch's bytes have been
    /// copied wherever they are going — after this, the arena under them is reused.
    /// </summary>
    public void Release(ReadOnlySpan<SpanHeader> headers)
    {
        long bytes = 0;
        foreach (ref readonly var h in headers)
        {
            bytes += h.PayloadByteLength;
            if (h.PayloadArenaOffset >= 0) ReleaseChunk(h.PayloadArenaOffset / ChunkBytes);
        }
        if (bytes != 0) Interlocked.Add(ref _bytesInFlight, -bytes);
    }

    /// <summary>A drained span's payload: name, service and attributes, in that order.</summary>
    internal ReadOnlySpan<byte> Payload(in SpanHeader h, byte[]? apart) =>
        apart is not null              ? apart
      : h.PayloadArenaOffset >= 0      ? new ReadOnlySpan<byte>(_base + h.PayloadArenaOffset, h.PayloadByteLength)
      : default;

    /// <summary>A drained run as the engine's write path takes it. Valid until <see cref="Release"/>.</summary>
    public DrainedSpans Drained(ReadOnlySpan<SpanHeader> headers, ReadOnlySpan<byte[]?> apart, ServiceIndexCache services) =>
        new(this, headers, apart, services);

    // ── Dispose ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Frees the slots and the arena — once no producer is inside a batch. The consumer is stopped
    /// before this (the drainer is disposed first). A producer that never ends its batch leaves the
    /// memory allocated rather than freed under it: a bounded wait, then a leak, never a
    /// use-after-free.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        EndBatch();   // this thread's own open batch, if any

        var  until     = Environment.TickCount64 + (long)_producerWaitBudget.TotalMilliseconds;
        var  spin      = new SpinWait();
        bool announced = false;
        while (Volatile.Read(ref _producersInside) > 0)
        {
            if (!announced) { announced = true; _onWaitingForProducersForTest?.Invoke(); }
            if (Environment.TickCount64 > until) return;                  // left allocated, not freed under a producer
            spin.SpinOnce();
        }

        _freed = true;
        _arena.Dispose();
        NativeMemory.Free(_chunkRefs);
        NativeMemory.Free(_chunkNext);
        NativeMemory.Free(_cursors);
        NativeMemory.Free(_slots);
        _signal.Dispose();
    }

    /// <summary>Test hook: true once the native memory was actually freed.</summary>
    internal bool FreedForTest => _freed;

    /// <summary>Test seam: <see cref="Dispose"/> is about to wait for producers still inside a batch. Only fires when there is one.</summary>
    internal Action? _onWaitingForProducersForTest;

    /// <summary>How long <see cref="Dispose"/> waits for open batches before leaving the memory allocated. A test may lengthen it into a hang guard.</summary>
    internal TimeSpan _producerWaitBudget = TimeSpan.FromSeconds(5);
}

/// <summary>
/// The drainer's memo of service strings by the ring's pool index. The service pool is never
/// shed, so an index names one service for the life of the process and the lookup is an array
/// load; on a miss the name is resolved from the span's own bytes through the ENGINE's pools and
/// kept. Single consumer: no locking.
/// </summary>
internal sealed class ServiceIndexCache
{
    private string?[]        _byIndex = new string?[64];
    private SpanStringPools? _resolvedIn;

    public string Resolve(int index, ReadOnlySpan<byte> serviceUtf8, SpanStringPools pools, out bool pooled)
    {
        if (!ReferenceEquals(pools, _resolvedIn)) { Array.Clear(_byIndex); _resolvedIn = pools; _lastUnpooled = null; }
        if ((uint)index < (uint)_byIndex.Length && _byIndex[index] is { } hit) { pooled = true; return hit; }

        // A FULL SERVICE POOL, ONE STRING PER BLOCK, NOT PER SPAN (review F4). Spans of one resource
        // block arrive together and carry the same service bytes, so the string built for the first
        // is handed to the rest of the run — shared, and charged to the tier once.
        if (index < 0 && _lastUnpooled is { } last && serviceUtf8.SequenceEqual(_lastUnpooledUtf8))
        {
            pooled = true;
            return last;
        }

        string s = pools.Service(serviceUtf8, out pooled);
        if (index >= 0 && pooled)
        {
            if (index >= _byIndex.Length)
                Array.Resize(ref _byIndex, Math.Max(index + 1, _byIndex.Length * 2));
            _byIndex[index] = s;
        }
        else if (!pooled)
        {
            _lastUnpooledUtf8 = serviceUtf8.ToArray();
            _lastUnpooled     = s;
        }
        return s;
    }

    private string? _lastUnpooled;
    private byte[]  _lastUnpooledUtf8 = [];
}

/// <summary>
/// A drained run of the raw ring as the engine's <see cref="ISpanBatch"/>: the name, service and
/// attribute bytes are windows onto the ring's arena, valid until the drainer releases the run —
/// which is why <see cref="AttributesForTier"/> COPIES.
/// </summary>
internal readonly ref struct DrainedSpans : ISpanBatch
{
    private readonly SpanRingBuffer            _ring;
    private readonly ReadOnlySpan<SpanHeader>  _headers;
    private readonly ReadOnlySpan<byte[]?>     _apart;
    private readonly ServiceIndexCache         _services;

    public DrainedSpans(SpanRingBuffer ring, ReadOnlySpan<SpanHeader> headers, ReadOnlySpan<byte[]?> apart, ServiceIndexCache services)
    {
        _ring     = ring;
        _headers  = headers;
        _apart    = apart;
        _services = services;
    }

    public int Count => _headers.Length;

    public SpanHeader Header(int i) => _headers[i];

    public ReadOnlySpan<byte> NameUtf8(int i) => _ring.Payload(in _headers[i], _apart[i])[.._headers[i].NameByteLength];

    public ReadOnlySpan<byte> ServiceUtf8(int i)
    {
        ref readonly var h = ref _headers[i];
        return _ring.Payload(in h, _apart[i]).Slice(h.NameByteLength, h.ServiceByteLength);
    }

    public ReadOnlySpan<byte> Attributes(int i)
    {
        ref readonly var h = ref _headers[i];
        return _ring.Payload(in h, _apart[i]).Slice(h.NameByteLength + h.ServiceByteLength, h.AttributesByteLength);
    }

    public string Name(int i, SpanStringPools pools, out bool pooled) => pools.Name(NameUtf8(i), out pooled);

    public string Service(int i, SpanStringPools pools, out bool pooled) =>
        _services.Resolve(_headers[i].ServiceNamePoolIndex, ServiceUtf8(i), pools, out pooled);

    /// <summary>
    /// A COPY, never a view: the arena under these bytes is reused the moment the drainer releases
    /// the run, and the record that keeps them is read long after by lock-free aggregate passes.
    /// </summary>
    public ReadOnlyMemory<byte> AttributesForTier(int i)
    {
        var a = Attributes(i);
        return a.IsEmpty ? ReadOnlyMemory<byte>.Empty : a.ToArray();
    }
}
