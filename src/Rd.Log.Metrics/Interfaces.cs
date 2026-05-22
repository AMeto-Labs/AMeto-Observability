namespace Rd.Log.Metrics;

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
