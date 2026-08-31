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
/// One page of the trace list, plus the two bits its caller cannot work out for itself.
/// </summary>
/// <param name="Rows">Newest-first, at most the requested limit.</param>
/// <param name="Capped">
/// True when the scan stopped because it ran out of ROOM rather than out of DATA — it hit the
/// internal scan cap, or more traces survived the filters than the limit allowed through. False
/// is the strong statement: every trace in <c>[from, to]</c> was examined and these are all the
/// ones that matched.
/// </param>
/// <param name="ScanFloorNano">
/// Where the scan STOPPED, as an EXCLUSIVE lower bound: every trace whose start time is
/// STRICTLY GREATER than this was examined and either returned in <see cref="Rows"/> or
/// deliberately filtered out. <c>long.MinValue</c> means the whole of <c>[from, to]</c> was
/// examined and there is nothing below to ask for — which is exactly when
/// <see cref="Capped"/> is false.
///
/// <para>NOT a minimum over the rows that were merged, and the distinction is the entire
/// reason this field is spelled the way it is. The hot tier is merged over the whole window
/// with no cap at all, so ONE late-arriving span drags such a minimum an hour below anything
/// the cold walk reached; and cold segments overlap, so a WIDE segment tripping the cap drags
/// it below a NARROWER segment nested inside its range that was never opened. A caller paging
/// to either lands under unread data, and the segment-level range check then skips it on every
/// later page — permanently, and while reporting the window exhausted.</para>
///
/// <para>What it IS for is the empty page: the filters run after the merge, so a page can come
/// back with no rows at all while a thousand traces were read and rejected, and then there is
/// no row to derive a cursor from. This is the only cursor that case has.</para>
/// </param>
public readonly record struct TraceListPage(
    IReadOnlyList<Ameto.Tracing.Storage.TraceSummary> Rows,
    bool Capped,
    long ScanFloorNano);

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
    /// Records one place the scan stopped short. Always the MAXIMUM of what it is told: each
    /// call names a height above which everything was handed over, so the highest is the only
    /// one every other sits below — and a floor that is too LOW is a floor a pager jumps past.
    /// </summary>
    public void StoppedAbove(long nano)
    {
        if (nano > FloorNano) FloorNano = nano;
    }
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
