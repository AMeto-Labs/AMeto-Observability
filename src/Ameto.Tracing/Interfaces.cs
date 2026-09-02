namespace Ameto.Tracing;

/// <summary>
/// Accepts pre-decoded spans for storage. Non-blocking on the hot path.
/// </summary>
public interface ISpanIngester
{
    /// <summary>
    /// Enqueue a batch of decoded spans.
    /// Returns false when the ring buffer is full (back-pressure).
    /// </summary>
    bool TryIngest(ReadOnlySpan<SpanIngestItem> spans, out int accepted);
}

/// <summary>
/// A single decoded span ready for ingestion.
/// Heap-allocated to carry variable-length fields (name, service, attributes bytes).
/// </summary>
public sealed class SpanIngestItem
{
    public TraceId        TraceId             { get; init; }
    public SpanId         SpanId              { get; init; }
    public SpanId         ParentSpanId        { get; init; }
    public long           StartTimeUnixNano   { get; init; }
    public long           DurationNanos       { get; init; }
    public string         Name                { get; init; } = string.Empty;
    public string         ServiceName         { get; init; } = string.Empty;
    public SpanKind       Kind                { get; init; }
    public SpanStatusCode Status              { get; init; }

    /// <summary>Pre-serialised msgpack attributes blob. May be empty.</summary>
    public byte[]         AttributesBytes     { get; init; } = [];

    /// <summary>Promoted HTTP response status code (0 = absent). Extracted before msgpack serialisation.</summary>
    public short          HttpStatusCode      { get; init; }
}

/// <summary>
/// Returns a service dependency graph for a time window.
/// Built from .svcgraph sidecar files — no span deserialisation.
/// </summary>
public interface IServiceGraphProvider
{
    Task<ServiceGraphDto> GetServiceGraphAsync(
        DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default);
}

/// <summary>Service-level aggregate for one node in the graph.</summary>
public sealed class ServiceNodeDto
{
    public string ServiceName { get; init; } = string.Empty;
    public uint   SpanCount   { get; init; }
    public double ErrorRate   { get; init; }  // 0–1
    public double P95Ms       { get; init; }
}

/// <summary>Directed call edge between two services.</summary>
public sealed class ServiceEdgeDto
{
    public string From       { get; init; } = string.Empty;
    public string To         { get; init; } = string.Empty;
    public uint   CallCount  { get; init; }
    public uint   ErrorCount { get; init; }
    public double ErrorRate  { get; init; }  // 0–1
    public double P95Ms      { get; init; }
}

/// <summary>Full service dependency graph response.</summary>
public sealed class ServiceGraphDto
{
    public ServiceNodeDto[] Nodes { get; init; } = [];
    public ServiceEdgeDto[] Edges { get; init; } = [];
}

/// <summary>
/// Returns pre-aggregated per-service stats (from .stats sidecar files — no span scan).
/// </summary>
public interface ITraceStatsProvider
{
    /// <summary>
    /// Merges per-service histograms for all segments in [from, to].
    /// Returns one entry per service name across all matching segments + hot tier.
    /// </summary>
    Task<IReadOnlyList<Ameto.Tracing.Storage.ServiceSegmentStats>> GetAggregateStatsAsync(
        DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default);
}

/// <summary>
/// Pre-aggregated trace-level views built from <c>.tracesum</c> sidecars — the list rows
/// and the volume sparkline are served without deserialising any spans.
/// </summary>
public interface ITraceSummaryProvider
{
    /// <summary>
    /// Newest-first, filtered trace summaries for the list view. Merges the hot tier with
    /// cold <c>.tracesum</c> bodies (deduped by trace id), applies the cheap filters, and
    /// returns at most <paramref name="limit"/> rows.
    ///
    /// <para>The result also says whether the scan was CAPPED — see
    /// <see cref="TraceListPage.Capped"/>. A caller paging backwards through a window cannot
    /// infer that from the row count, because the filters are applied AFTER the scan cap
    /// bites: a page of twelve rows can mean "twelve matched in the whole window" or "twelve
    /// matched among the first few thousand traces I was allowed to look at", and those two
    /// answers end a stream very differently.</para>
    /// </summary>
    Task<TraceListPage> GetTraceListAsync(
        DateTimeOffset   from,
        DateTimeOffset   to,
        string?          serviceName,
        string?          spanName,
        SpanStatusCode?  status,
        long?            minDurationNanos,
        long?            maxDurationNanos,
        int              limit,
        CancellationToken ct = default);

    /// <summary>
    /// Trace volume (total/error counts + time-bucketed sparkline) over [from, to],
    /// read from the tiny <c>.tracesum</c> volume headers + hot tier.
    /// </summary>
    Task<TraceVolume> GetTraceVolumeAsync(
        DateTimeOffset from, DateTimeOffset to, int buckets, CancellationToken ct = default);
}

/// <summary>
/// One page of the trace list, plus the bit its caller cannot work out for itself.
/// </summary>
/// <param name="Rows">Newest-first, at most the requested limit.</param>
/// <param name="Capped">
/// True when the scan stopped because it ran out of ROOM rather than out of DATA — it hit the
/// internal scan cap, met a segment it could not read, or more traces survived the filters than
/// the limit allowed through. False is the strong statement: every trace in <c>[from, to]</c>
/// that STILL EXISTS was examined and these are all the ones that matched.
///
/// <para>Read that qualifier with <see cref="Unreadable"/> beside it, because the two CAN both be
/// set and the interesting case is when only the second is. A page whose window overlaps a segment
/// that vanished on some EARLIER request opened every file that exists and finished all of them,
/// so it is not capped by anything: there is no height a narrower page could settle that this one
/// did not. It is still missing rows, permanently, and <see cref="Unreadable"/> is the field that
/// says so. Capped means "ask me again, lower down"; Unreadable means "there is nothing left to
/// ask".</para>
/// </param>
/// <param name="ScanFloorNano">
/// THE HEIGHT ABOVE WHICH THIS PAGE SETTLED ITS WINDOW, as an EXCLUSIVE lower bound: every trace
/// in <c>(ScanFloorNano, to]</c> was examined AND every one of them that matched is in
/// <see cref="Rows"/>. Below it, this page claims nothing at all. <c>long.MinValue</c> means the
/// settled band is the whole of <c>[from, to]</c> — which is exactly when <see cref="Capped"/>
/// is false.
///
/// <para>NOT a minimum over the rows that were merged, and NOT the cursor. The hot tier is
/// merged over the whole window with no cap, so ONE late-arriving span drags such a minimum an
/// hour below anything the cold walk reached; and cold segments overlap, so a WIDE segment
/// tripping the cap drags it below a NARROWER segment nested inside its range that was never
/// opened.</para>
///
/// <para>IT IS NOT THE CURSOR EITHER, and that is the harder half. A pager that moves its
/// ceiling to this height jumps over rows it has not sent yet whenever the floor sits ABOVE the
/// oldest row of the page — which emits them on a later page, after older rows, in a list the
/// client renders in arrival order. The cursor is the oldest row the page returned; this floor
/// has exactly two jobs beside it:</para>
/// <list type="number">
///   <item>THE PAGE WITH NO USABLE ROW. Filters run after the merge, so a page can come back
///   empty (or holding only rows an earlier page already sent) while a thousand traces were read
///   and rejected. There is no row to page from, and this is the only cursor that case has —
///   sound precisely because everything above it was settled, so moving the ceiling down to it
///   skips nothing.</item>
///   <item>HONESTY. When the cursor DOES land below this height, the band between them was never
///   examined and never will be: every later ceiling is lower still. The stream reports that as
///   truncation instead of claiming the window was read out.</item>
/// </list>
/// </param>
/// <param name="Unreadable">
/// True when this page met a segment inside its window that it COULD NOT READ — a file that
/// vanished, or one a power cut left truncated — as opposed to one it merely ran out of budget
/// to open. <see cref="ScanFloorNano"/> covers both, and for a pager that is the same fact; for
/// the STREAM above it, it is not.
///
/// <para>WHY IT HAS TO BE ITS OWN BIT. A budget skip is self-correcting: the segment stays in the
/// snapshot, the next page's narrower window reaches it, and the floor stops being reported.
/// A FAULT is not, because the engine HEALS the snapshot on the very page that discovers it —
/// <c>RemoveColdSegment</c> drops a vanished file so later reads do not keep failing on it — and
/// after that no later page can find the segment, meet the fault, or record the floor. If the
/// cursor never had to descend past that one floor, every later page then reported a window it
/// had read out, and the stream ended <c>done {"complete":true}</c> having delivered half of it.
/// Measured: two segments of 40 traces each, the OLDER one unlinked, 40 of 80 rows and a positive
/// claim of completeness.</para>
///
/// <para>So the removal keeps its job and this bit carries the memory of it. It is deliberately
/// COARSE — a bool for the whole window, not a height — because the one thing a consumer may
/// never do with it is decide the fault has been made good by a later page. It has not; nothing
/// re-reads a file that is gone.</para>
///
/// <para>WHICH IS ALSO WHAT KEEPS IT OFF THE FAULTS A LATER PAGE DOES MAKE GOOD, and that boundary
/// had to be drawn explicitly because three different events arrive through one catch block.
/// <c>TraceStorageEngine.ColdReadFault</c> names them, and only two of the four set this bit: a
/// segment genuinely LOST, and one that is present and CORRUPT — both permanent, both invisible to
/// every later page. The other two set <see cref="ScanFloorNano"/> and nothing else. A compaction
/// HANDOVER means this walk held the pre-swap snapshot and the rows are in the replacement, which
/// the next page reads; a TRANSIENT fault means the data directory could not be reached at all,
/// which the next request retries. Measured before the split: 1 in 60 requests racing a compaction
/// pass on an undamaged server came back with this bit set and nothing recorded in
/// <c>VanishedRegionLog</c> — the memory right, the request wrong — and a single mount blip set it
/// permanently over the whole cold tier. Both produced the red "deleted or damaged" banner and a
/// frozen list on a server with nothing wrong with it.</para>
///
/// <para>AND THE MEMORY IS THE ENGINE'S, NOT THE REQUEST'S. Scoping it to one stream is the same
/// forbidden decision made one level up: the removal is PROCESS-WIDE and PERMANENT, so a bit that
/// dies with the stream leaves the fault recordable exactly once, by whichever request happened to
/// discover it. Measured through <c>/api/traces/stream</c> with the SAME request issued twice —
/// <c>query-error</c> and 50 rows, then <c>done {"complete":true}</c> and the same 50 of 100. In
/// the product that second request is a REFRESH, the control beside the banner, so one click
/// converted "half your traces are gone" into a list labelled complete. <c>VanishedRegionLog</c>
/// keeps the ranges instead, bounded in size and pruned by retention, and this bit is set for any
/// window overlapping one — which is why a page can carry it with <see cref="Capped"/> false.</para>
/// </param>
public readonly record struct TraceListPage(
    IReadOnlyList<Ameto.Tracing.Storage.TraceSummary> Rows,
    bool Capped,
    long ScanFloorNano,
    bool Unreadable = false);

/// <summary>
/// Where a bounded span scan actually stopped, filled in by
/// <see cref="ITraceProvider.SearchSpansAsync"/> for a caller that has to page past it.
///
/// <para>An async iterator cannot return a second value, and the caller cannot work this one
/// out for itself: only the scan knows which tier evicted, which segment it broke inside, and
/// which segment it never opened. Allocated by the caller and passed in, because the alternative
/// — inferring the floor from the spans that came back — is precisely the unsound rule this type
/// exists to replace.</para>
/// </summary>
public sealed class SpanScanFloor
{
    /// <summary>
    /// EXCLUSIVE lower bound of the region the scan speaks for: every MATCHING span whose start
    /// is STRICTLY GREATER than this was yielded. <c>long.MinValue</c> — the window was read out.
    /// </summary>
    public long FloorNano { get; private set; } = long.MinValue;

    /// <summary>True once the scan has admitted it did not read the whole window.</summary>
    public bool Truncated => FloorNano != long.MinValue;

    /// <summary>
    /// True when the scan met a segment it COULD NOT READ, as opposed to one it had no room
    /// left to open. See <see cref="TraceListPage.Unreadable"/> for why the two cannot share
    /// one signal: a fault heals itself out of the segment snapshot, so no later page can
    /// rediscover it, and a stream that forgets it ends by claiming a window it never read.
    /// </summary>
    public bool Unreadable { get; private set; }

    /// <summary>
    /// Records one place the scan stopped short. Always the MAXIMUM of what it is told: each
    /// call names a height above which everything was handed over, so the highest is the only
    /// one every other sits below — and a floor that is too LOW is a floor a pager jumps past.
    /// </summary>
    public void StoppedAbove(long nano)
    {
        if (nano > FloorNano) FloorNano = nano;
    }

    /// <summary>
    /// The same, for a segment that could not be READ. Sets <see cref="Unreadable"/> as well —
    /// and sets it even when the height it names is below one already recorded, because the
    /// fault is not a height at all.
    ///
    /// <para>CALL IT ONLY FOR A PERMANENT FAULT. Because it sets both at once it is the wrong
    /// call for a segment the NEXT request will read perfectly well — a compaction handover or a
    /// data directory that blipped — and being the unconditional call for every abandoned segment
    /// is exactly how those came to be reported as data loss. Those get plain
    /// <see cref="StoppedAbove"/>: they owe the caller a height and nothing more. See
    /// <see cref="TraceListPage.Unreadable"/> for the full split.</para>
    /// </summary>
    public void StoppedAboveUnreadable(long nano)
    {
        Unreadable = true;
        StoppedAbove(nano);
    }

    /// <summary>
    /// THE FAULT WITHOUT A HEIGHT: this window overlaps a range the storage has already lost —
    /// a segment that vanished on some earlier request and was dropped from the snapshot then, so
    /// this scan cannot meet it, cannot fail on it, and would otherwise report a window it read
    /// out in full.
    ///
    /// <para>Deliberately does NOT move <see cref="FloorNano"/>, and that is the difference from
    /// <see cref="StoppedAboveUnreadable"/> rather than an oversight. A floor is an invitation to
    /// come back with a narrower window and get the rest; there is no rest. This scan abandoned
    /// nothing part-read — it opened every file that still exists — so no height names work left
    /// undone, and a floor here would page a stream down through empty bands to a "could not be
    /// advanced" ending whose advice (narrow the window) is advice about the wrong thing.</para>
    /// </summary>
    public void MetUnreadableRegion() => Unreadable = true;
}

/// <summary>Trace-volume result: totals + per-bucket sparklines.</summary>
public sealed class TraceVolume
{
    public int      TotalTraces    { get; init; }
    public int      ErrorTraces    { get; init; }
    public double[] TotalSparkline { get; init; } = [];
    public double[] ErrorSparkline { get; init; } = [];
}

/// <summary>
/// A necessary attribute condition extracted from a TraceQL AND-chain, used to
/// skip storage blocks via their attribute blooms. <see cref="LowerValue"/> is the
/// lowercased string value for equality predicates, or null for key-presence-only
/// (any other operator still requires the key to exist on the span).
/// </summary>
public readonly record struct AttrHint(string Key, string? LowerValue);

/// <summary>
/// Provides access to stored trace/span data.
/// </summary>
public interface ITraceProvider
{
    /// <summary>
    /// Returns all spans belonging to the given trace, ordered by StartTimeUnixNano.
    /// Returns empty if the trace is not found.
    /// </summary>
    IAsyncEnumerable<SpanRecord> GetTraceAsync(TraceId traceId, CancellationToken ct = default);

    /// <summary>
    /// Returns spans whose start time falls within [from, to], optionally filtered
    /// by service name and/or span name substring. <paramref name="attrHints"/> are
    /// necessary attribute conditions — storage may use them to skip data that
    /// cannot match, and callers must still post-filter.
    /// </summary>
    /// <param name="scanFloor">
    /// Optional, and the ONLY way to learn that the scan stopped short of the window. The result
    /// is bounded at <paramref name="limit"/> spans and each tier keeps only its own newest
    /// <paramref name="limit"/>, so a caller paging backwards cannot read the returned spans as
    /// "everything down to the oldest of them" — see <see cref="SpanScanFloor"/>. An
    /// implementation that leaves it untouched is asserting it read the window out.
    /// </param>
    IAsyncEnumerable<SpanRecord> SearchSpansAsync(
        DateTimeOffset?  from             = null,
        DateTimeOffset?  to               = null,
        string?          serviceName      = null,
        string?          spanName         = null,
        SpanStatusCode?  status           = null,
        long?            minDurationNanos = null,
        long?            maxDurationNanos = null,
        short?           httpStatusCode   = null,
        int              limit            = 200,
        IReadOnlyList<AttrHint>? attrHints = null,
        SpanScanFloor?   scanFloor        = null,
        CancellationToken ct              = default);
}
