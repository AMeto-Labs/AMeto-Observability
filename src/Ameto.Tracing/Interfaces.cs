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
    /// </summary>
    Task<IReadOnlyList<Ameto.Tracing.Storage.TraceSummary>> GetTraceListAsync(
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
        CancellationToken ct              = default);
}
