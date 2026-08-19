namespace Ameto.Core;

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
/// (timestamp, eventId) pagination cursor shared by the hot-tier and cold-tier scans:
/// an event qualifies when its (ts, id) is strictly past the cursor in the query
/// direction. <c>afterTs</c> is the primary key; <c>afterId</c> breaks exact-@t ties.
/// </summary>
public static class QueryCursor
{
    public static bool After(long evTs, ulong evId, long? afterTs, ulong? afterId, bool forward)
    {
        if (!afterTs.HasValue && !afterId.HasValue) return true;
        long ts = afterTs ?? (forward ? long.MinValue : long.MaxValue);
        if (evTs != ts) return forward ? evTs > ts : evTs < ts;
        if (!afterId.HasValue) return true;
        return forward ? evId > afterId.Value : evId < afterId.Value;
    }
}

/// <summary>
/// Read-only view of hot-tier events. Must be disposed after use.
/// </summary>
public interface IHotTierReader : IDisposable
{
    /// <summary>Iterates events in insertion order (oldest first).</summary>
    IEnumerable<LogEvent> ReadAll();

    /// <summary>
    /// Window/cursor/level-filtered events sorted by (@t, id) in the requested direction.
    /// The default implementation materialises everything via <see cref="ReadAll"/> and
    /// sorts — real hot-tier readers override it with a header-level scan that only
    /// materialises events actually emitted (queries touch a page, not the whole tier).
    /// </summary>
    IEnumerable<LogEvent> ReadSorted(
        long fromTicks, long toTicks,
        long? afterTsTicks, ulong? afterIdRaw, bool forward,
        IReadOnlySet<LogLevel>? levels)
    {
        var filtered = ReadAll().Where(e =>
        {
            long ts = e.Timestamp.UtcTicks;
            if (ts < fromTicks || ts > toTicks) return false;
            if (levels is not null && !levels.Contains(e.Level)) return false;
            return QueryCursor.After(ts, e.Id.RawValue, afterTsTicks, afterIdRaw, forward);
        });
        return forward
            ? filtered.OrderBy(e => e.Timestamp).ThenBy(e => e.Id)
            : filtered.OrderByDescending(e => e.Timestamp).ThenByDescending(e => e.Id);
    }

    /// <summary>
    /// Cold-tier segments whose events are already returned by this hot reader.
    /// When a hot-tier segment is frozen and being flushed to cold storage, its
    /// future segment key is reserved here so the query layer can skip the cold
    /// segment (which may be partially written or just-registered) to avoid
    /// either duplicates or missing-events races during flush.
    ///
    /// <para>Keyed by <see cref="SegmentKey"/>, not by id: the reserved block belongs to the
    /// LOCAL node, and while it was matched on the id alone a replicated peer segment whose
    /// own counter happened to land inside that block was skipped by every query for the
    /// duration of a flush — events silently missing from results, on a file that was
    /// perfectly readable.</para>
    ///
    /// <para>Required, with no default implementation, and named for keys rather than ids
    /// because BOTH halves of that are load-bearing. It carried a do-nothing default while it
    /// returned <c>IReadOnlySet&lt;ulong&gt;</c>, which was truthful for a NEW member: an
    /// implementer that had never heard of it covered nothing. Changing the element type turned
    /// the same default into a trap — an implementation outside this repo that returned a set of
    /// ids no longer implements anything, so the class compiles with no error and no warning
    /// while the interface quietly dispatches to the empty default, and the reader's cold
    /// segments are served twice for the duration of every flush. Its two sibling changes are
    /// CS0535 and CS0030; this one was the member whose break a compiler could not show, which is
    /// exactly why it may not have one. A reader that genuinely covers nothing still says so, and
    /// says it out loud, with <see cref="EmptyCoveredSet.Instance"/>.</para>
    /// </summary>
    IReadOnlySet<SegmentKey> CoveredSegmentKeys { get; }
}

/// <summary>
/// What a hot reader returns when it covers no cold segment at all. Public because
/// <see cref="IHotTierReader.CoveredSegmentKeys"/> has no default implementation: an implementer
/// that covers nothing has to write it down, and writing it down is what makes the compiler
/// speak the next time the member changes shape.
/// </summary>
public static class EmptyCoveredSet
{
    public static readonly IReadOnlySet<SegmentKey> Instance = new HashSet<SegmentKey>();
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

    /// <summary>
    /// Deletes a segment by its cluster-wide key, whichever node produced it.
    ///
    /// <para>There is deliberately no <see cref="SegmentId"/> overload beside this one. An id on
    /// its own does not name a segment — every node's counter starts at 1, so the series overlap
    /// the moment a peer replicates anything — and a method taking one could only ever mean "the
    /// local node's", a decision its own signature does not show. That is the trap the catalog key
    /// was introduced to remove, and keeping it as a convenience overload would reintroduce it one
    /// call site at a time. A caller holding a bare id builds the pair, and so has to say which
    /// node it means.</para>
    ///
    /// <para>Also deliberately without a default implementation, and no longer the only member of
    /// this change that lacks one. Any default here is either a silent no-op or a guess at the
    /// node, and a signature that REPLACES a required member is meant to be heard about: an
    /// implementer outside this repo changes a parameter type and the compiler says CS0535.
    /// <see cref="IHotTierReader.CoveredSegmentKeys"/> kept a default while its element type
    /// changed under it, which is the same break with the compiler switched off — the class goes
    /// on compiling and the interface silently dispatches to an empty set — so it lost the
    /// default too.</para>
    /// </summary>
    Task DeleteSegmentAsync(SegmentKey key, CancellationToken ct = default);
    IReadOnlyList<SegmentInfo> ListSegments();
}

/// <summary>
/// Implemented by metric / trace storage engines to support time-based data pruning.
/// Called by <c>RetentionBackgroundService</c> every hour and by the manual /run endpoint.
/// </summary>
public interface IRetentionTarget
{
    /// <summary>Logical name used to look up the TTL in <c>RetentionDto</c>: "metrics" or "traces".</summary>
    string RetentionKey { get; }

    /// <summary>Deletes cold segments whose max timestamp is older than <paramref name="ttl"/>.</summary>
    Task<int> PruneAsync(TimeSpan ttl, CancellationToken ct = default);
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

    /// <summary>
    /// AND-intersects inverted-index posting lists for multiple equality predicates.
    /// Returns null if not supported; empty array if no events match all predicates.
    /// </summary>
    uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates) => null;
}
