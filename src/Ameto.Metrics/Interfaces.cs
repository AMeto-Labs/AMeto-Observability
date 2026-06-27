namespace Ameto.Metrics;

/// <summary>
/// Accepts pre-decoded metric data points for storage.
/// </summary>
public interface IMetricIngester
{
    /// <summary>
    /// Enqueue a batch of metric data points.
    /// Always succeeds — metrics use a bounded channel with drop-oldest policy.
    /// </summary>
    void Ingest(ReadOnlySpan<MetricIngestItem> points);
}

/// <summary>
/// Queries stored metric time series.
/// </summary>
public interface IMetricQuery
{
    /// <summary>
    /// Returns distinct metric names known to the server,
    /// optionally filtered by a prefix.
    /// </summary>
    IEnumerable<string> GetMetricNames(string? prefix = null);

    /// <summary>
    /// Returns all time series for the given metric name,
    /// optionally filtered by label matchers,
    /// aggregated to the requested step granularity.
    /// </summary>
    IAsyncEnumerable<MetricSeries> QueryAsync(
        string             metricName,
        DateTimeOffset?    from        = null,
        DateTimeOffset?    to          = null,
        TimeSpan?          step        = null,
        IReadOnlyDictionary<string, string>? labelMatchers = null,
        CancellationToken  ct          = default);

    /// <summary>
    /// Returns the latest value for every time series of the given metric
    /// (useful for dashboards and gauges).
    /// </summary>
    IAsyncEnumerable<MetricSeries> GetLatestAsync(
        string            metricName,
        IReadOnlyDictionary<string, string>? labelMatchers = null,
        CancellationToken ct = default);
}

/// <summary>
/// Metric metadata catalog — powers the Explore UI (metric list, label filters).
/// Backed by in-memory metadata maintained at ingestion (no file scan).
/// </summary>
public interface IMetricCatalog
{
    /// <summary>All known metrics with type, unit, label keys, cardinality, last-seen.</summary>
    IReadOnlyList<MetricCatalogEntry> GetCatalog(string? search = null);

    /// <summary>Distinct label keys observed for a metric.</summary>
    IReadOnlyList<string> GetLabelKeys(string metricName);

    /// <summary>Distinct values observed for a label key on a metric (capped).</summary>
    IReadOnlyList<string> GetLabelValues(string metricName, string labelKey);
}

/// <summary>
/// Server-side aggregation engine: rate/increase/avg/min/max/last/sum/quantile,
/// group-by, top-K, and histogram heatmaps. Operates on raw series from
/// <see cref="IMetricQuery"/> so it is independent of the storage format.
/// </summary>
public interface IMetricAggregator
{
    /// <summary>Runs a typed aggregation and returns the resulting series.</summary>
    Task<IReadOnlyList<MetricSeries>> QueryAsync(
        MetricQueryRequest request,
        CancellationToken  ct = default);

    /// <summary>
    /// Builds a histogram heatmap: per-step bucket-count deltas over the window.
    /// Returns empty bounds if the metric is not a histogram.
    /// </summary>
    Task<HeatmapResult> HeatmapAsync(
        string            metricName,
        DateTimeOffset?   from,
        DateTimeOffset?   to,
        TimeSpan?         step,
        IReadOnlyDictionary<string, string>? filters,
        CancellationToken ct = default);
}
