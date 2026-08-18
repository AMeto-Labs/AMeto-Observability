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
/// Thread safety: single writer + multiple readers.
///   Readers snapshot <c>_count</c> via a volatile read; writes to headers/payload
///   are always completed before <see cref="Interlocked.Increment"/> publishes them.
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
    private          int        _chunksAllocated;
    /// <summary>Running sum of <see cref="_chunkPayloadTails"/> — total payload bytes
    /// actually written across all chunks. Kept incrementally so the hot-path
    /// <see cref="IsFull"/> check is O(1) instead of re-summing the tails array.</summary>
    private          long       _payloadBytes;

    // ── Limits ────────────────────────────────────────────────────────────────

    private readonly int  _maxEvents;
    private readonly long _maxPayloadBytes;
    private readonly int  _maxChunks;

    // ── Write-path state ──────────────────────────────────────────────────────

    private volatile int  _count;
    private volatile bool _frozen;

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
    /// True when the segment is near its event or payload limit.
    /// Used by <see cref="StorageEngine"/> as a hint to trigger an async flush.
    /// </summary>
    public bool IsFull =>
        _count >= _maxEvents ||
        _payloadBytes >= _maxPayloadBytes;

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
    /// </summary>
    public bool TryWrite(in LogEventHeader header, ReadOnlySpan<byte> propertiesPayload, string? template = null, ExceptionInfo? exception = null)
    {
        if (_frozen || _count >= _maxEvents)
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

        // ── Store template (managed, parallel to header) ────────────────────
        if (template is not null)
        {
            var arr = _chunkTemplates[ci] ??= new string?[ChunkEventCapacity];
            arr[si] = template;
        }
        if (exception is not null)
        {
            var arr = _chunkExceptions[ci] ??= new ExceptionInfo?[ChunkEventCapacity];
            arr[si] = exception;
        }

        _chunkPayloadTails[ci] += payloadLen;
        _payloadBytes          += payloadLen;

        // Publish: Interlocked.Increment acts as full memory barrier —
        // header + payload writes are visible to readers before _count increases.
        Interlocked.Increment(ref _count);
        return true;
    }

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
            agg.AddByPoolIndex(ts, h.Level, h.ServiceNamePoolIndex);
        }
    }

    // ── Flush helpers ─────────────────────────────────────────────────────────

    /// <summary>Prevents further writes. Must be called before reading for flush.</summary>
    public void Freeze() => _frozen = true;

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
            ServiceName     = (h.ServiceNamePoolIndex >= 0 && pool is not null)
                                  ? pool.Get(h.ServiceNamePoolIndex)
                                  : null,
        };
    }



    // ── IDisposable ──────────────────────────────────────────────────────────

    private bool _disposed;

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
        }
    }
}
