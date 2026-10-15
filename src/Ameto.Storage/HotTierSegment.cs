using System.Buffers;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using Ameto.Core;
using Ameto.Core.Serialization;

namespace Ameto.Storage;

/// <summary>
/// Hot-tier: a dynamically-growing segmented arena in unmanaged memory.
///
/// Memory is allocated in fixed-size chunks on demand. The first chunk (~10 MB)
/// is allocated eagerly at construction; subsequent chunks are added as events arrive.
/// This avoids pre-reserving the full 256 MB upfront when the server is idle.
///
/// Chunk layout:
///   [LogEventHeader × ChunkEventCapacity]  — header array at offset 0
///   [payload bytes …]                      — msgpack property maps
///
/// Each chunk holds at most <see cref="ChunkEventCapacity"/> events and
/// <see cref="ChunkPayloadBytes"/> bytes of payload. Both limits must be respected
/// because <see cref="LogEventHeader.PropertiesArenaOffset"/> is chunk-local.
///
/// Thread safety: single writer + multiple readers, plus the flush thread's
/// <see cref="Freeze"/>.
///   Readers snapshot <c>_count</c> via a volatile read; writes to headers/payload
///   are always completed before <see cref="Interlocked.Increment"/> publishes them.
///   <see cref="Freeze"/> runs on the flush thread and hands over from the writer:
///   when it returns, <c>Count</c> is final. See the handshake in both methods.
/// </summary>
public sealed unsafe class HotTierSegment : IDisposable, IHotTierReader
{
    // ── Chunk geometry (fixed constants) ──────────────────────────────────────

    // GEOMETRY INVARIANT — read before tuning either constant:
    //   A chunk holds exactly ChunkEventCapacity header slots, addressed by index
    //   division (ci = idx / ChunkEventCapacity). The writer only advances to the next
    //   chunk when the *slot* count rolls over. Therefore the payload area MUST be able
    //   to hold ChunkEventCapacity events' worth of payload — otherwise the payload area
    //   fills first, TryWrite returns false, the slot counter never reaches the roll-over
    //   point, and the segment is permanently wedged below capacity. StorageEngine reacts
    //   to that false by flushing, so an undersized payload area turns into a flush storm
    //   (tiny cold segments), which is the ingest-drop root cause.
    //   Keep: ChunkEventCapacity * avgPayloadBytes  <=  ChunkPayloadBytes.
    //   At 16384 * 512B = 8 MB this covers structured logs up to ~512 B average.

    // This geometry is now the INGEST path's alone. Compaction used to re-pack its batch into a
    // tier and therefore had to reason about chunk fitting from slot 0, which is what produced
    // the co-fit gate that excluded dense segments from merging; it streams straight into
    // SegmentWriter now and never sees a chunk.

    /// <summary>Events per chunk. Power-of-2 for fast modulo/division by JIT.</summary>
    public const int  ChunkEventCapacity = 16_384;

    /// <summary>Payload bytes per chunk. 8 MB / 16384 slots = 512 B/event headroom (see invariant above).</summary>
    public const long ChunkPayloadBytes  = 8 * 1024 * 1024; // 8 MB

    /// <summary>Bytes occupied by the header array at the start of every chunk.</summary>
    private static readonly long ChunkHeaderBytes = (long)ChunkEventCapacity * LogEventHeader.SizeOf;

    /// <summary>Total NativeMemory bytes allocated per chunk.</summary>
    private static readonly long ChunkTotalBytes  = ChunkHeaderBytes + ChunkPayloadBytes;

    // ── Capacity planning (used by StorageEngine to size its RAM budgets) ──────
    //
    // A chunk is allocated WHOLE — ChunkHeaderBytes of header slots plus the full
    // ChunkPayloadBytes arena — however little of its payload area ends up used.
    // A tier bounded only by payload bytes therefore has no native ceiling: at a
    // 64 B average payload, 64 MB of payload needs 1,048,576 slots = 64 chunks =
    // 576 MB resident, and the old 2,000,000-event cap allowed 123 chunks = 1.1 GB
    // for a tier configured as "64 MB". StorageEngine's flat 1.4x footprint estimate
    // was consequently off by up to 17x, and the frozen-tier backlog it sized against
    // that estimate could not hold its 1 GB budget. Deriving the event cap from the
    // chunk count instead makes MaxSizeBytes a real bound: native stays within
    // ChunksFor() * 9 MB no matter how small the events are.

    /// <summary>Chunks needed to hold <paramref name="maxPayloadBytes"/> of payload.</summary>
    public static int ChunksFor(long maxPayloadBytes) =>
        (int)Math.Max(1, (maxPayloadBytes + ChunkPayloadBytes - 1) / ChunkPayloadBytes);

    /// <summary>
    /// Event capacity that keeps a tier within <see cref="ChunksFor"/> chunks. Pair it
    /// with the same <paramref name="maxPayloadBytes"/> when constructing the tier so
    /// neither limit can be exceeded without the other stopping the writer first.
    /// </summary>
    public static int EventCapacityFor(long maxPayloadBytes) =>
        ChunksFor(maxPayloadBytes) * ChunkEventCapacity;

    /// <summary>
    /// Worst-case native bytes a tier built with <see cref="EventCapacityFor"/> can hold —
    /// payload arenas plus header arrays. This is the number to budget RSS against.
    /// </summary>
    public static long NativeBytesFor(long maxPayloadBytes) =>
        (long)ChunksFor(maxPayloadBytes) * ChunkTotalBytes;

    /// <summary>A string's size in a 64-bit heap: header, method table, length, its UTF-16 and a terminator, 8-aligned — as an ingest batch charges it.</summary>
    public static long ManagedBytesOf(string? s) => s is null ? 0 : (22L + 2L * s.Length + 7) & ~7L;

    /// <summary>An exception tree's size in the heap: each node, and its type, message and stack — as an ingest batch charges it.</summary>
    public static long ManagedBytesOf(ExceptionInfo? exception)
    {
        long bytes = 0;
        for (var e = exception; e is not null; e = e.Inner)
            bytes += 48 + ManagedBytesOf(e.Type) + ManagedBytesOf(e.Message) + ManagedBytesOf(e.StackTrace);
        return bytes;
    }

    // ── Per-chunk state ───────────────────────────────────────────────────────

    // Store arena base pointers as nuint (avoids pointer-in-class-field restriction).
    private readonly nuint[]    _chunkArenas;
    private readonly long[]     _chunkPayloadTails; // bytes written into each chunk's payload area
    // Per-event message-template strings (managed). Parallel to the header array of
    // each chunk; allocated lazily on first write into the chunk. Holding the string
    // here makes the hot tier the single source of truth for the template — the
    // segment writer no longer has to round-trip through StringInternPool to
    // resolve it (which silently degraded to string.Empty on cache miss).
    private readonly string?[]?[] _chunkTemplates;
    // Per-event structured exceptions (managed, parallel to _chunkTemplates).
    // Lazily allocated per chunk on first event that carries an exception.
    private readonly ExceptionInfo?[]?[] _chunkExceptions;
    // Per-event @service strings for the events whose service has no pool index — the intern
    // pool was full when it arrived, so ServiceNamePoolIndex is -1 and this is the only copy of
    // the name, as the attached template is for a template. Parallel to _chunkTemplates, and
    // allocated per chunk only once such an event arrives: a pool with room never needs it.
    private readonly string?[]?[] _chunkServices;
    // Zone map: the smallest and largest TimestampUtcTicks written into each chunk. Updated
    // by the writer for every event BEFORE the count increment publishes it, so a reader
    // that snapshotted Count sees bounds covering every event below its snapshot — and
    // possibly events above it, which only widens the range. Each bound is a single
    // aligned 64-bit store, monotone in one direction, so there is no torn state to read:
    // any (min, max) pair a reader observes is a superset of the events it may touch.
    // Exact under out-of-order arrival — it is maintained with min/max, never assumed
    // monotone — so a query window that misses a chunk's [min, max] can skip all 16 384
    // headers of it without looking.
    private readonly long[]     _chunkMinTicks;
    private readonly long[]     _chunkMaxTicks;
    private          int        _chunksAllocated;
    /// <summary>Running sum of <see cref="_chunkPayloadTails"/> — total payload bytes
    /// actually written across all chunks. Kept incrementally so the hot-path
    /// <see cref="IsFull"/> check is O(1) instead of re-summing the tails array.</summary>
    private          long       _payloadBytes;

    /// <summary>
    /// Managed bytes the writers said their events brought that no other event shares — a template or a
    /// service the intern pool did not take, an exception (see <see cref="TryWrite"/>; round-5 review H-D).
    /// Past the pool's budget, or past its longest text, a template is a string per event, and the tier
    /// kept every one of them until it flushed: <see cref="IsFull"/> counted events and payload bytes only,
    /// and the batch that had been charged for the string let go of it at its commit. 41 requests of a
    /// distinct 4 M-character template ran a 384 MiB heap out with the tier's native memory flat at 9 MiB.
    /// Counted against the payload limit: a tier holds at most that much of either.
    /// </summary>
    private          long       _managedBytes;

    // ── Limits ────────────────────────────────────────────────────────────────

    private readonly int  _maxEvents;
    private readonly long _maxPayloadBytes;
    private readonly int  _maxChunks;

    // ── Write-path state ──────────────────────────────────────────────────────

    private volatile int  _count;
    private volatile bool _frozen;

    /// <summary>
    /// 1 while a <see cref="TryWrite"/> call has passed the frozen check and has not yet
    /// published its event. <see cref="Freeze"/> waits for it to reach 0, which is what
    /// makes "frozen" mean <i>no further event can appear</i> rather than the weaker
    /// <i>no further write may start</i>.
    /// </summary>
    private          int  _writeInProgress;

    // ── Public properties ─────────────────────────────────────────────────────

    public int  Count    => _count;
    public bool IsFrozen => _frozen;

    /// <summary>
    /// Total native (off-GC-heap) bytes currently committed by this segment:
    /// every allocated chunk reserves <see cref="ChunkTotalBytes"/> via
    /// <c>NativeMemory.AllocZeroed</c>. Lets live diagnostics attribute process
    /// RSS to the hot tier vs. the managed heap instead of guessing.
    /// </summary>
    public long AllocatedBytes => (long)_chunksAllocated * ChunkTotalBytes;

    /// <summary>
    /// True when the segment is near its event or payload limit — or holds as much managed text of its
    /// events' own as its payload limit (<see cref="ManagedBytes"/>).
    /// Used by <see cref="StorageEngine"/> as a hint to trigger an async flush.
    /// </summary>
    public bool IsFull =>
        _count >= _maxEvents ||
        _payloadBytes >= _maxPayloadBytes ||
        _managedBytes >= _maxPayloadBytes;

    /// <summary>The managed bytes this tier holds that are its events' own (see <see cref="_managedBytes"/>).</summary>
    internal long ManagedBytes => Volatile.Read(ref _managedBytes);

    /// <summary>
    /// What of <see cref="ManagedBytes"/> the engine counted against its frozen tiers' text when it froze this
    /// tier (round-5 H-D) — kept here, on the tier, so whichever owner gives the tier's flush slot back gives
    /// the text back with it, once (<see cref="TakeFrozenTextCharge"/>).
    /// </summary>
    private long _frozenTextCharge;

    /// <summary>Records the frozen-text charge the engine took for this tier. Allocates nothing.</summary>
    internal void ChargeFrozenText(long bytes) => Volatile.Write(ref _frozenTextCharge, bytes);

    /// <summary>The frozen-text charge, given back: what it was the first time, 0 after. Allocates nothing.</summary>
    internal long TakeFrozenTextCharge() => Interlocked.Exchange(ref _frozenTextCharge, 0);

    // ── Construction ─────────────────────────────────────────────────────────

    /// <param name="maxEvents">Maximum number of events before flush is triggered.</param>
    /// <param name="maxPayloadBytes">Maximum total payload bytes (headers not counted).</param>
    public HotTierSegment(int maxEvents, long maxPayloadBytes)
    {
        _maxEvents       = maxEvents;
        _maxPayloadBytes = maxPayloadBytes;

        // How many chunks can we ever allocate? Add 1 for safety margin.
        _maxChunks = (int)Math.Ceiling((double)maxEvents / ChunkEventCapacity) + 1;

        _chunkArenas       = new nuint[_maxChunks];
        _chunkPayloadTails = new long[_maxChunks];
        _chunkTemplates    = new string?[]?[_maxChunks];
        _chunkExceptions   = new ExceptionInfo?[]?[_maxChunks];
        _chunkServices     = new string?[]?[_maxChunks];
        _chunkMinTicks     = new long[_maxChunks];
        _chunkMaxTicks     = new long[_maxChunks];

        // Eagerly allocate first chunk only (~10 MB instead of 256 MB).
        AllocChunk(0);
    }

    // ── Write path ────────────────────────────────────────────────────────────

    /// <summary>
    /// Writes one event into the arena. Returns false if capacity is exhausted.
    /// <paramref name="propertiesPayload"/>: raw msgpack bytes of the properties map.
    /// <paramref name="template"/>: optional message-template string. When provided
    /// it is stored alongside the header so cold-tier flush can persist it even if
    /// the <see cref="StringInternPool"/> no longer contains the entry.
    /// <paramref name="service"/>: the event's <c>@service</c> when it has no pool index
    /// (<see cref="LogEventHeader.ServiceNamePoolIndex"/> -1: the pool was full) — kept beside the
    /// header the same way; ignored for an event whose service is pooled.
    /// <paramref name="ownManagedBytes"/>: the managed bytes of what the event brings that no other event
    /// shares (<see cref="ManagedBytesOf(string?)"/>, <see cref="ManagedBytesOf(ExceptionInfo?)"/>): counted
    /// against the payload limit, and an event that would take the tier past it is refused unless the tier
    /// is empty (round-5 H-D). The live write path says it; a replay or a despill says 0, its log bounding
    /// what it holds — and its strings are one per distinct text, shared.
    /// </summary>
    public bool TryWrite(in LogEventHeader header, ReadOnlySpan<byte> propertiesPayload, string? template = null,
                         ExceptionInfo? exception = null, string? service = null, long ownManagedBytes = 0)
    {
        // Unsynchronised fast reject. Purely an optimisation — a tier that is already
        // frozen or full says so without paying for the claim below, which matters because
        // every rotation ends with writers retrying against a full tier. The
        // authoritative check is the one inside the claim.
        if (_frozen || _count >= _maxEvents)
            return false;

        // CLAIM, then re-read _frozen — one half of a handshake with Freeze(), and the
        // reason the claim is an interlocked exchange rather than a plain store: a release
        // store may sink below the load that follows it, and then both sides could miss
        // each other. Without this the flush could freeze the tier and read Count while
        // this call — already past the check above — went on to publish one more event into
        // a tier that is about to be written out, unlisted and freed. TryWrite had returned
        // true, the writer had counted the event as stored (then: dropped it from the ring),
        // and queries served it for the length of the flush before it disappeared for good;
        // the WAL copy went with it, since that WAL is deleted when the flush publishes.
        Interlocked.Exchange(ref _writeInProgress, 1);
        try
        {
            // A pooled service needs no copy: the index names it.
            return TryWriteClaimed(in header, propertiesPayload, template, exception,
                                   header.ServiceNamePoolIndex < 0 && !string.IsNullOrEmpty(service) ? service : null,
                                   ownManagedBytes);
        }
        finally
        {
            // Release: everything above — the count publish included — is visible to a
            // Freeze() that then observes the 0. Load-bearing on the throwing path too:
            // AllocChunk raises OutOfMemoryException, and a claim left standing would park
            // the flush thread in Freeze() for good.
            Volatile.Write(ref _writeInProgress, 0);
        }
    }

    private bool TryWriteClaimed(in LogEventHeader header, ReadOnlySpan<byte> propertiesPayload, string? template,
                                 ExceptionInfo? exception, string? service, long ownManagedBytes)
    {
        if (_frozen || _count >= _maxEvents)
            return false;   // Freeze() won the race — the caller retries into the successor tier

        // The managed text of its events' own, as the payload: past the limit the event waits for the next
        // tier. An empty tier takes it whatever its size, or an event larger than a tier could never be written.
        if (ownManagedBytes > 0 && _count > 0 && _managedBytes + ownManagedBytes > _maxPayloadBytes)
            return false;

        int payloadLen = propertiesPayload.Length;
        int eventIdx   = _count;                          // this event's global index
        int ci         = eventIdx / ChunkEventCapacity;   // chunk index
        int si         = eventIdx % ChunkEventCapacity;   // slot within chunk

        // Allocate the chunk lazily on first use.
        if (_chunkArenas[ci] == 0)
        {
            // Would allocating exceed the payload budget? Measured against bytes
            // actually written, not reserved chunk capacity.
            if (_payloadBytes >= _maxPayloadBytes)
                return false;
            if (ci >= _maxChunks)
                return false;

            AllocChunk(ci);
        }

        // Verify this event's payload fits in the chunk's payload area.
        if (_chunkPayloadTails[ci] + payloadLen > ChunkPayloadBytes)
            return false;

        // ── The managed side arrays, rented BEFORE anything is stored. A rent (a 128 KB array) is
        //    the step below that can throw, and a write that throws must have stored nothing: its
        //    caller takes it for "nothing written" and tries the event again. The template used to
        //    be stored ahead of the exception's rent, and a rent that ran out of memory left it in
        //    the slot — where the next event written there, with no template of its own, read it
        //    as its own.
        string?[]?        templates  = template  is not null ? _chunkTemplates[ci]  ??= RentSlotsFor<string?>()        : _chunkTemplates[ci];
        ExceptionInfo?[]? exceptions = exception is not null ? _chunkExceptions[ci] ??= RentSlotsFor<ExceptionInfo?>() : _chunkExceptions[ci];
        string?[]?        services   = service   is not null ? _chunkServices[ci]   ??= RentSlotsFor<string?>()        : _chunkServices[ci];

        // ── Write payload ────────────────────────────────────────────────────
        byte* dest = ChunkPayloadPtr(ci) + _chunkPayloadTails[ci];
        if (payloadLen > 0)
            propertiesPayload.CopyTo(new Span<byte>(dest, payloadLen));

        // ── Write header ─────────────────────────────────────────────────────
        var h = header;
        h.PropertiesArenaOffset = (int)_chunkPayloadTails[ci]; // chunk-local offset
        h.PropertiesByteLength  = payloadLen;
        h.HasException          = exception is not null;
        ChunkHeadersPtr(ci)[si] = h;

        // ── Store template and exception (managed, parallel to header) ──────
        //    Every slot of a rented array written, set or not: nothing left in it by a write that
        //    never published can be read as this event's.
        if (templates  is not null) templates[si]  = template;
        if (exceptions is not null) exceptions[si] = exception;
        if (services   is not null) services[si]   = service;

        _chunkPayloadTails[ci] += payloadLen;
        _payloadBytes          += payloadLen;
        if (ownManagedBytes > 0) Volatile.Write(ref _managedBytes, _managedBytes + ownManagedBytes);   // the writer's alone

        // Zone map, before the publish below — see the field comment for why that order
        // is the whole guarantee.
        long ts = h.TimestampUtcTicks;
        if (ts < _chunkMinTicks[ci]) _chunkMinTicks[ci] = ts;
        if (ts > _chunkMaxTicks[ci]) _chunkMaxTicks[ci] = ts;

        // Publish: Interlocked.Increment acts as full memory barrier —
        // header + payload writes are visible to readers before _count increases.
        Interlocked.Increment(ref _count);
        return true;
    }

    // ── Chunk-wise read access (query scan) ───────────────────────────────────

    /// <summary>
    /// False when no event of chunk <paramref name="ci"/> can lie in
    /// [<paramref name="fromTicks"/>, <paramref name="toTicks"/>] — the scan then skips the
    /// chunk's headers entirely. Only meaningful for a chunk that holds at least one
    /// published event (<c>ci * ChunkEventCapacity &lt; Count</c> as snapshotted by the caller).
    /// True is "maybe": the bounds may also reflect events published after the snapshot.
    /// </summary>
    public bool ChunkMayOverlap(int ci, long fromTicks, long toTicks)
    {
        long min = Volatile.Read(ref _chunkMinTicks[ci]);
        long max = Volatile.Read(ref _chunkMaxTicks[ci]);
        return max >= fromTicks && min <= toTicks;
    }

    /// <summary>
    /// The first <paramref name="count"/> header slots of chunk <paramref name="ci"/> as a
    /// span over the native array. The caller derives <paramref name="count"/> from its
    /// own <see cref="Count"/> snapshot, which is what makes every slot in the span a
    /// published one.
    /// </summary>
    public ReadOnlySpan<LogEventHeader> ChunkHeaders(int ci, int count)
        => new(ChunkHeadersPtr(ci), count);

    /// <summary>
    /// Returns the message-template string stored for <paramref name="eventIndex"/>,
    /// or <c>null</c> if none was supplied at write time.
    /// Callers that need a non-null result should fall back to
    /// <see cref="StringInternPool.Get"/> using the header's pool index.
    /// </summary>
    public string? GetTemplate(int eventIndex)
    {
        int ci  = eventIndex / ChunkEventCapacity;
        int si  = eventIndex % ChunkEventCapacity;
        var arr = _chunkTemplates[ci];
        return arr is null ? null : arr[si];
    }

    /// <summary>
    /// The <c>@service</c> kept beside the event at <paramref name="eventIndex"/> because it has no
    /// pool index (the pool was full when it arrived), or null: the event's service, if any, is its
    /// <see cref="LogEventHeader.ServiceNamePoolIndex"/>'s. See <see cref="ServiceOf"/>.
    /// </summary>
    public string? GetService(int eventIndex)
    {
        var arr = _chunkServices[eventIndex / ChunkEventCapacity];
        return arr is null ? null : arr[eventIndex % ChunkEventCapacity];
    }

    /// <summary>The event's <c>@service</c>, wherever it is kept: its pool index's text, or the string beside it.</summary>
    public string? ServiceOf(int eventIndex, in LogEventHeader header, StringInternPool? pool) =>
        header.ServiceNamePoolIndex >= 0
            ? pool?.Get(header.ServiceNamePoolIndex)
            : GetService(eventIndex);

    /// <summary>
    /// Returns the structured <see cref="ExceptionInfo"/> attached to the event at
    /// <paramref name="eventIndex"/>, or <c>null</c> if the event has no exception.
    /// </summary>
    public ExceptionInfo? GetException(int eventIndex)
    {
        int ci  = eventIndex / ChunkEventCapacity;
        int si  = eventIndex % ChunkEventCapacity;
        var arr = _chunkExceptions[ci];
        return arr is null ? null : arr[si];
    }

    // ── IHotTierReader ────────────────────────────────────────────────────────

    /// <summary>
    /// A bare tier covers no cold segment: the reservation belongs to the flush that freezes a
    /// tier, and it is <c>StorageEngine</c>'s reader snapshot that holds it. Spelled out rather
    /// than inherited from a default, so that the next change to this member's shape reaches this
    /// class through the compiler instead of through a query returning everything twice.
    /// </summary>
    public IReadOnlySet<SegmentKey> CoveredSegmentKeys => EmptyCoveredSet.Instance;

    /// <inheritdoc/>
    public IEnumerable<LogEvent> ReadAll() => ReadAll(null);

    /// <summary>Iterates a snapshot of all events with optional template resolution.</summary>
    public IEnumerable<LogEvent> ReadAll(StringInternPool? pool)
    {
        int snapshot = _count;
        for (int i = 0; i < snapshot; i++)
            yield return MaterialiseEvent(i, pool);
    }

    // ── Header-only aggregation ─────────────────────────────────────────────────

    /// <summary>
    /// Feeds every in-window event's <b>header</b> (timestamp, level, service pool index) into
    /// <paramref name="agg"/> without touching the payload arena — no properties decode, no
    /// message-template or exception materialisation. Powers the near-zero-allocation counts path.
    /// </summary>
    public void AggregateInto(LogVolumeAggregator agg, long fromTicks, long toTicks)
    {
        int snapshot = _count;                       // volatile read: publishes prior writes
        for (int i = 0; i < snapshot; i++)
        {
            ref var h = ref GetHeader(i);
            long ts = h.TimestampUtcTicks;
            if (ts < fromTicks || ts > toTicks) continue;
            // A service with no pool index (the pool was full) is the string kept beside the event.
            if (h.ServiceNamePoolIndex < 0 && GetService(i) is { } kept) agg.AddByServiceName(ts, h.Level, kept);
            else                                                           agg.AddByPoolIndex(ts, h.Level, h.ServiceNamePoolIndex);
        }
    }

    // ── Flush helpers ─────────────────────────────────────────────────────────

    /// <summary>
    /// Prevents further writes and waits for the one that may already be in flight, so that
    /// the <see cref="Count"/> its caller reads next is final. Must be called before reading
    /// for flush: everything the flush does afterwards — sort order, segment contents, the
    /// decision to delete this tier's WAL — is derived from that one count, so an event that
    /// arrived after it was read would be persisted nowhere.
    ///
    /// <para>The wait is bounded by a single <see cref="TryWrite"/> call, which cannot block:
    /// worst case it is copying a payload or allocating one chunk.</para>
    /// </summary>
    public void Freeze()
    {
        _frozen = true;
        // The store above must not sink below the load below it, or a writer that claimed
        // just now and a freezer that published just now could each miss the other. Neither
        // volatile write nor volatile read orders that pair — only a full fence does.
        Interlocked.MemoryBarrier();

        var spin = new SpinWait();
        while (Volatile.Read(ref _writeInProgress) != 0)
            spin.SpinOnce();
    }

    /// <summary>Returns the header of event at <paramref name="eventIndex"/> by ref.</summary>
    public ref LogEventHeader GetHeader(int eventIndex)
    {
        int ci = eventIndex / ChunkEventCapacity;
        int si = eventIndex % ChunkEventCapacity;
        return ref ChunkHeadersPtr(ci)[si];
    }

    /// <summary>Returns the raw msgpack payload span for event at <paramref name="eventIndex"/>.</summary>
    public ReadOnlySpan<byte> GetPropertiesPayload(int eventIndex)
    {
        int ci    = eventIndex / ChunkEventCapacity;
        ref var h = ref GetHeader(eventIndex);
        byte* src = ChunkPayloadPtr(ci) + h.PropertiesArenaOffset;
        return new ReadOnlySpan<byte>(src, h.PropertiesByteLength);
    }

    /// <summary>Deserialises and returns the properties map for a single event.</summary>
    public Dictionary<string, object?>? ReadPropertiesPayload(int eventIndex, StringInternPool pool)
    {
        var span = GetPropertiesPayload(eventIndex);
        return span.IsEmpty ? null : Ameto.Core.Serialization.LogEventSerializer.DeserializePropertiesMap(span);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void AllocChunk(int ci)
    {
        // Alloc (not AllocZeroed): zeroing touches every page of the chunk, making the
        // whole 8 MB payload area resident even when small events fill only a fraction of
        // it — a ~3× RSS blow-up per hot tier. Uninitialised memory is safe here because
        // a header/payload slot is fully written before Interlocked.Increment(_count)
        // publishes it, and readers never touch a slot at index >= _count. Only pages we
        // actually write become resident.
        _chunkArenas[ci]       = (nuint)NativeMemory.Alloc((nuint)ChunkTotalBytes);
        _chunkPayloadTails[ci] = 0;
        // Empty zone: the first write narrows both bounds to its timestamp. Set here, before
        // the chunk's first event is published, so a reader never sees a chunk without them.
        _chunkMinTicks[ci]     = long.MaxValue;
        _chunkMaxTicks[ci]     = long.MinValue;
        _chunksAllocated++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private LogEventHeader* ChunkHeadersPtr(int ci) => (LogEventHeader*)_chunkArenas[ci];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* ChunkPayloadPtr(int ci) => (byte*)_chunkArenas[ci] + ChunkHeaderBytes;

    /// <summary>Materialises the event at <paramref name="index"/> (query-path sorted scan).</summary>
    public LogEvent Materialise(int index, StringInternPool? pool) => MaterialiseEvent(index, pool);

    private LogEvent MaterialiseEvent(int index, StringInternPool? pool)
    {
        int ci    = index / ChunkEventCapacity;
        int si    = index % ChunkEventCapacity;
        ref var h = ref ChunkHeadersPtr(ci)[si];

        byte* payloadPtr  = ChunkPayloadPtr(ci) + h.PropertiesArenaOffset;
        var   payloadSpan = new ReadOnlySpan<byte>(payloadPtr, h.PropertiesByteLength);

        // Copy the msgpack out instead of decoding it. The arena is native memory owned by
        // this tier, so a reference could outlive it — but the copy replaces the dictionary
        // rather than adding to it, which is what made the old note reject copying here.
        // Delivery re-serialises these bytes straight to JSON; a filter still materialises
        // a dictionary on demand through LogEvent.Properties.
        ReadOnlyMemory<byte> rawProps = h.PropertiesByteLength > 0
            ? payloadSpan.ToArray()
            : default;

        // Prefer the template carried alongside the event; this is robust to pool
        // misses (e.g. after WAL recovery if the pool wasn't reloaded). Fall back
        // to the pool only when no template was attached on write.
        string template = GetTemplate(index)
                          ?? (pool is not null ? pool.Get(h.MessageTemplatePoolIndex) : string.Empty);

        return new LogEvent
        {
            Id              = new EventId(h.Id),
            Timestamp       = new DateTimeOffset(h.TimestampUtcTicks, TimeSpan.Zero),
            Level           = h.Level,
            MessageTemplate = template,
            Exception       = GetException(index),
            RawProperties   = rawProps,
            TraceIdHi       = h.TraceIdHi,
            TraceIdLo       = h.TraceIdLo,
            SpanId          = h.SpanId,
            ServiceName     = ServiceOf(index, in h, pool),
        };
    }



    // ── IDisposable ──────────────────────────────────────────────────────────

    private bool _disposed;

    /// <summary>Test hook: true once the native chunks have been freed.</summary>
    internal bool IsDisposed => _disposed;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        for (int i = 0; i < _chunksAllocated; i++)
        {
            if (_chunkArenas[i] != 0)
            {
                NativeMemory.Free((void*)_chunkArenas[i]);
                _chunkArenas[i] = 0;
            }
            // Hand the managed slot arrays back. Nobody can still be reading them: the
            // engine disposes a tier only once every reader snapshot that captured it is
            // gone (_activeReaders == 0) and the flush that read it has published.
            // Cleared on return as well as on rent — deliberately, not redundantly: a
            // returned array sits in the pool for an unbounded time, and uncleared it would
            // keep up to 16 384 exception objects (stack traces, KB each) of a tier that
            // has already flushed reachable for exactly that long.
            if (_chunkTemplates[i] is { } t)
            {
                _chunkTemplates[i] = null;
                ArrayPool<string?>.Shared.Return(t, clearArray: true);
            }
            if (_chunkExceptions[i] is { } x)
            {
                _chunkExceptions[i] = null;
                ArrayPool<ExceptionInfo?>.Shared.Return(x, clearArray: true);
            }
            if (_chunkServices[i] is { } s)
            {
                _chunkServices[i] = null;
                ArrayPool<string?>.Shared.Return(s, clearArray: true);
            }
        }
    }

    /// <summary>
    /// A chunk's per-slot managed array (templates or exceptions) from the shared pool.
    /// These are 128 KB each — LOH allocations, eight or more per tier, garbage the moment
    /// the tier flushed — so they are recycled through <see cref="ArrayPool{T}.Shared"/>
    /// instead. Cleared on rent as well as on return: a slot the writer never fills is
    /// read as "no template / no exception" by <see cref="GetTemplate"/> and
    /// <see cref="GetException"/>, and that must not depend on every past returner having
    /// cleared. One memset per 16 384 events.
    /// </summary>
    private static T[] RentSlots<T>()
    {
        var arr = ArrayPool<T>.Shared.Rent(ChunkEventCapacity);
        Array.Clear(arr);
        return arr;
    }

    /// <summary><see cref="RentSlots{T}"/> for the write path, behind its test seam.</summary>
    private T[] RentSlotsFor<T>()
    {
        _beforeSlotRentForTest?.Invoke(typeof(T));
        return RentSlots<T>();
    }

    /// <summary>
    /// Test seam: called with the element type as the write path is about to rent a chunk's
    /// template or exception array — throwing from it is a rent that ran out of memory. Reached
    /// once per chunk and kind, not per event. Null in production.
    /// </summary>
    internal Action<Type>? _beforeSlotRentForTest;
}

/// <summary>
/// <see cref="HotTierSegment"/>'s admission rules, replayed on payload sizes alone: which events a
/// fresh tier of the same limits would take, in this order, before it refused one.
///
/// <para>What a spilled block needs (see <c>StorageEngine</c>, "Spill"). Its events go to a WAL and
/// to no tier, and the WAL is replayed into ONE tier later, flushed into the one level block the
/// WAL reserved. A WAL holding more than a tier takes would replay into a tier that refuses its
/// tail, and recovery used to count a refused event as simply not replayed. So the spill admits an
/// event only when this says a tier would, and a refusal here is where the WAL is closed and the
/// next one opened.</para>
///
/// <para>EXACT, not an estimate, and a copy rather than a shared helper so the tier's write path
/// stays as it is: <c>HotTierAdmissionTests</c> drives a real tier and this side by side and holds
/// every answer equal. Monotone in the limits: a sequence a tier of these limits takes whole, a
/// tier of larger ones takes whole too — the chunk geometry is a constant, and the limits only ever
/// refuse — which is what lets a replay size its tier from the WAL rather than from today's
/// configuration.</para>
/// </summary>
internal struct HotTierAdmission
{
    private readonly int  _maxEvents;
    private readonly long _maxPayloadBytes;
    private readonly int  _maxChunks;

    private int  _count;
    private long _payloadBytes;
    /// <summary>The managed text the admitted events brought of their own, as the tier counts it (round-5 H-D).</summary>
    private long _managedBytes;
    /// <summary>The highest chunk a tier would have allocated by now; chunk 0 is allocated by its constructor.</summary>
    private int  _lastChunk;
    /// <summary>Payload bytes in chunk <see cref="_lastChunk"/>, the only one still taking events.</summary>
    private long _chunkTail;

    /// <param name="maxEvents">The tier's event limit, as its constructor takes it.</param>
    /// <param name="maxPayloadBytes">The tier's payload limit, as its constructor takes it.</param>
    public HotTierAdmission(int maxEvents, long maxPayloadBytes)
    {
        _maxEvents       = maxEvents;
        _maxPayloadBytes = maxPayloadBytes;
        _maxChunks       = (int)Math.Ceiling((double)maxEvents / HotTierSegment.ChunkEventCapacity) + 1;
    }

    /// <summary>Events admitted so far.</summary>
    public readonly int Count => _count;

    /// <summary>Payload bytes admitted so far.</summary>
    public readonly long PayloadBytes => _payloadBytes;

    /// <summary>Managed text of their own the admitted events brought (see <see cref="HotTierSegment.ManagedBytes"/>).</summary>
    public readonly long ManagedBytes => _managedBytes;

    /// <summary>
    /// True, and counted, when a tier would take an event of <paramref name="payloadLength"/>
    /// bytes, bringing <paramref name="ownManagedBytes"/> of text of its own
    /// (<see cref="HotTierSegment.TryWrite"/>), next; false, and nothing changes, when it would refuse it.
    ///
    /// <para>The text is what bounds a spilled block's to a tier's (round-5 H-D): the despill replays the
    /// block into one tier that counts none of it — its log bounds it — and a block admitted on payload
    /// alone held up to twice its file in strings (an 8 MB tier's spill file took 28 MB of UTF-16
    /// templates).</para>
    /// </summary>
    public bool TryAdmit(int payloadLength, long ownManagedBytes = 0)
    {
        if (_count >= _maxEvents) return false;
        if (ownManagedBytes > 0 && _count > 0 && _managedBytes + ownManagedBytes > _maxPayloadBytes) return false;

        int ci = _count / HotTierSegment.ChunkEventCapacity;
        if (ci > _lastChunk)
        {
            // The tier's lazy allocation: refused on the bytes written so far, or past its chunks.
            if (_payloadBytes >= _maxPayloadBytes) return false;
            if (ci >= _maxChunks) return false;
            _lastChunk = ci;
            _chunkTail = 0;
        }

        if (_chunkTail + payloadLength > HotTierSegment.ChunkPayloadBytes) return false;

        _chunkTail    += payloadLength;
        _payloadBytes += payloadLength;
        _managedBytes += ownManagedBytes;
        _count++;
        return true;
    }
}
