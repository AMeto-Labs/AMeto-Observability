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

    /// <summary>Events per chunk. Power-of-2 for fast modulo/division by JIT.</summary>
    private const int  ChunkEventCapacity = 65_536;

    /// <summary>Payload bytes per chunk. 8 MB gives ~100-byte average events comfortable headroom.</summary>
    private const long ChunkPayloadBytes  = 8 * 1024 * 1024; // 8 MB

    /// <summary>Bytes occupied by the header array at the start of every chunk.</summary>
    private static readonly long ChunkHeaderBytes = (long)ChunkEventCapacity * LogEventHeader.SizeOf;

    /// <summary>Total NativeMemory bytes allocated per chunk.</summary>
    private static readonly long ChunkTotalBytes  = ChunkHeaderBytes + ChunkPayloadBytes;

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
    /// True when the segment is near its event or payload limit.
    /// Used by <see cref="StorageEngine"/> as a hint to trigger an async flush.
    /// </summary>
    public bool IsFull =>
        _count >= _maxEvents ||
        (long)_chunksAllocated * ChunkPayloadBytes >= _maxPayloadBytes;

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
            // Would allocating exceed the payload budget?
            if ((long)_chunksAllocated * ChunkPayloadBytes >= _maxPayloadBytes)
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

    /// <inheritdoc/>
    public IEnumerable<LogEvent> ReadAll() => ReadAll(null);

    /// <summary>Iterates a snapshot of all events with optional template resolution.</summary>
    public IEnumerable<LogEvent> ReadAll(StringInternPool? pool)
    {
        int snapshot = _count;
        for (int i = 0; i < snapshot; i++)
            yield return MaterialiseEvent(i, pool);
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
        _chunkArenas[ci]       = (nuint)NativeMemory.AllocZeroed((nuint)ChunkTotalBytes);
        _chunkPayloadTails[ci] = 0;
        _chunksAllocated++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private LogEventHeader* ChunkHeadersPtr(int ci) => (LogEventHeader*)_chunkArenas[ci];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* ChunkPayloadPtr(int ci) => (byte*)_chunkArenas[ci] + ChunkHeaderBytes;

    private LogEvent MaterialiseEvent(int index, StringInternPool? pool)
    {
        int ci    = index / ChunkEventCapacity;
        int si    = index % ChunkEventCapacity;
        ref var h = ref ChunkHeadersPtr(ci)[si];

        byte* payloadPtr  = ChunkPayloadPtr(ci) + h.PropertiesArenaOffset;
        var   payloadSpan = new ReadOnlySpan<byte>(payloadPtr, h.PropertiesByteLength);

        Dictionary<string, object?>? props = null;
        if (h.PropertiesByteLength > 0)
            props = Ameto.Core.Serialization.LogEventSerializer.DeserializePropertiesMap(payloadSpan);

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
            Properties      = props,
            RawProperties   = payloadSpan.ToArray(),
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
