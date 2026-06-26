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
    /// by service name and/or span name substring.
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
        CancellationToken ct              = default);
}
