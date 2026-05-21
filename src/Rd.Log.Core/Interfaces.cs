namespace Rd.Log.Core;

/// <summary>
/// Accepts raw batches of events from the ingestion layer.
/// Implementations must be non-blocking on the hot path (ring buffer enqueue).
/// </summary>
public interface IEventIngester
{
    /// <summary>
    /// Enqueue a batch of raw msgpack event bytes.
    /// Returns false if the ring buffer is full (back-pressure).
    /// </summary>
    bool TryIngest(ReadOnlySpan<byte> msgpackBatch, out int eventsAccepted);
}

/// <summary>
/// Provides access to segments for query execution.
/// </summary>
public interface ISegmentProvider
{
    /// <summary>Returns segments whose time ranges overlap the requested window, newest first.</summary>
    IReadOnlyList<SegmentInfo> GetSegments(DateTimeOffset? from, DateTimeOffset? to);

    /// <summary>Opens the hot-tier for reading (snapshot of current live events).</summary>
    IHotTierReader OpenHotTierReader();
}

/// <summary>
/// Read-only view of hot-tier events. Must be disposed after use.
/// </summary>
public interface IHotTierReader : IDisposable
{
    /// <summary>Iterates events in insertion order (oldest first).</summary>
    IEnumerable<LogEvent> ReadAll();

    /// <summary>
    /// IDs of cold-tier segments whose events are already returned by this hot reader.
    /// When a hot-tier segment is frozen and being flushed to cold storage, its
    /// future segment id is reserved here so the query layer can skip the cold
    /// segment (which may be partially written or just-registered) to avoid
    /// either duplicates or missing-events races during flush.
    /// Default implementation returns an empty set for backwards compatibility.
    /// </summary>
    IReadOnlySet<ulong> CoveredSegmentIds => EmptyCoveredSet.Instance;
}

internal static class EmptyCoveredSet
{
    public static readonly IReadOnlySet<ulong> Instance = new HashSet<ulong>();
}

/// <summary>
/// Reads events from a single cold-tier segment.
/// </summary>
public interface ISegmentReader : IDisposable
{
    SegmentInfo Info { get; }

    /// <summary>
    /// Returns events matching the optional filter expression and time range.
    /// candidateBitmap: if non-null, only events at these offsets are decoded (index result).
    /// </summary>
    IAsyncEnumerable<LogEvent> ReadEventsAsync(
        uint[]? candidateOffsets,
        DateTimeOffset? from,
        DateTimeOffset? to,
        bool reversed = false,
        CancellationToken ct = default);
}

/// <summary>
/// Executes a <see cref="QueryRequest"/> across hot and cold tiers.
/// </summary>
public interface IQueryExecutor
{
    IAsyncEnumerable<LogEvent> ExecuteAsync(QueryRequest request, CancellationToken ct = default);
}

/// <summary>
/// Manages lifecycle of cold-tier segments: flush, delete, list.
/// </summary>
public interface ISegmentManager
{
    Task FlushHotTierAsync(CancellationToken ct = default);
    Task DeleteSegmentAsync(SegmentId segmentId, CancellationToken ct = default);
    IReadOnlyList<SegmentInfo> ListSegments();
}

/// <summary>
/// Per-segment index: inverted index, bloom filter, trigram.
/// Used during flush to build the index, and during query to consult it.
/// </summary>
public interface ISegmentIndex
{
    /// <summary>Returns local event offsets matching a property equality predicate.</summary>
    uint[]? Lookup(string propertyName, object? value);

    /// <summary>Quick check: returns false if the segment definitely doesn't contain the value.</summary>
    bool MightContain(string propertyName, object? value);

    /// <summary>Returns local event offsets whose message template or rendered message contains the trigram.</summary>
    uint[]? LookupTrigram(ReadOnlySpan<char> text);
}
