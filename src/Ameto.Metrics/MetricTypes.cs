using System.Text;

namespace Ameto.Metrics;

/// <summary>
/// OTLP metric signal types.
/// </summary>
public enum MetricKind : byte
{
    /// <summary>Monotonically increasing counter (OTLP Sum isMonotonic=true).</summary>
    Counter   = 0,
    /// <summary>Instantaneous value that can go up or down (OTLP Gauge or non-monotonic Sum).</summary>
    Gauge     = 1,
    /// <summary>Distribution: count + sum + explicit buckets (OTLP Histogram).</summary>
    Histogram = 2,
}

/// <summary>
/// Immutable, comparable set of label key-value pairs.
/// Stored sorted by key so two identical label sets have the same hash.
/// </summary>
public sealed class LabelSet : IEquatable<LabelSet>
{
    public static readonly LabelSet Empty = new([]);

    private readonly (string Key, string Value)[] _labels;
    private readonly int _hash;

    public LabelSet(IEnumerable<KeyValuePair<string, string>> labels)
    {
        _labels = labels
            .Select(kv => (kv.Key, kv.Value))
            .OrderBy(t => t.Key, StringComparer.Ordinal)
            .ToArray();

        var h = new HashCode();
        foreach (var (k, v) in _labels)
        {
            h.Add(k, StringComparer.Ordinal);
            h.Add(v, StringComparer.Ordinal);
        }
        _hash = h.ToHashCode();
    }

    public IReadOnlyList<(string Key, string Value)> Pairs => _labels;

    public bool Equals(LabelSet? other)
    {
        if (other is null || _labels.Length != other._labels.Length) return false;
        for (int i = 0; i < _labels.Length; i++)
            if (_labels[i].Key != other._labels[i].Key || _labels[i].Value != other._labels[i].Value)
                return false;
        return true;
    }

    public override bool Equals(object? obj) => obj is LabelSet l && Equals(l);
    public override int  GetHashCode() => _hash;

    public override string ToString()
    {
        if (_labels.Length == 0) return "{}";
        var sb = new StringBuilder("{");
        foreach (var (k, v) in _labels)
            sb.Append(k).Append('=').Append('"').Append(v).Append('"').Append(',');
        sb[^1] = '}';
        return sb.ToString();
    }
}

/// <summary>
/// A single observed metric data point ready for ingestion.
/// </summary>
public sealed class MetricIngestItem
{
    /// <summary>Metric name (e.g. "http.server.request.duration").</summary>
    public string     Name              { get; init; } = string.Empty;

    /// <summary>Instrument type.</summary>
    public MetricKind Kind              { get; init; }

    /// <summary>Optional unit string (e.g. "ms", "By", "1").</summary>
    public string     Unit              { get; init; } = string.Empty;

    /// <summary>Label set identifying this time series.</summary>
    public LabelSet   Labels            { get; init; } = LabelSet.Empty;

    /// <summary>Data point timestamp, Unix nanoseconds.</summary>
    public long       TimestampUnixNano { get; init; }

    // ── Scalar (Counter / Gauge) ───────────────────────────────────────────────
    /// <summary>Scalar value for Counter or Gauge data points.</summary>
    public double     ScalarValue       { get; init; }

    // ── Histogram ─────────────────────────────────────────────────────────────
    public long       HistogramCount    { get; init; }
    public double     HistogramSum      { get; init; }
    /// <summary>Upper bounds of explicit histogram buckets (null for scalar metrics).</summary>
    public double[]?  BucketBounds      { get; init; }
    /// <summary>Counts per bucket, length == BucketBounds.Length + 1 (overflow bucket).</summary>
    public long[]?    BucketCounts      { get; init; }

    /// <summary>Sampled exemplars linking individual measurements to traces (may be null).</summary>
    public MetricExemplar[]? Exemplars   { get; init; }
}

/// <summary>
/// An exemplar: a sampled measurement linked to the trace/span that produced it.
/// Enables jumping from a metric (e.g. a latency spike) straight to the exact trace.
/// </summary>
public sealed class MetricExemplar
{
    public long   TimestampUnixNano { get; init; }
    public double Value             { get; init; }
    /// <summary>32-char lowercase hex trace id (empty if absent).</summary>
    public string TraceId           { get; init; } = string.Empty;
    /// <summary>16-char lowercase hex span id (empty if absent).</summary>
    public string SpanId            { get; init; } = string.Empty;
}

/// <summary>
/// A stored data point in a time series.
/// </summary>
public struct MetricDataPoint
{
    /// <summary>Unix nanoseconds.</summary>
    public long   TimestampUnixNano;
    /// <summary>Scalar value (Counter / Gauge); for Histogram this is the mean (sum/count).</summary>
    public double Value;
    /// <summary>Histogram count; 0 for scalar metrics.</summary>
    public long   Count;
    /// <summary>Histogram sum; 0 for scalar metrics.</summary>
    public double Sum;
    /// <summary>
    /// Per-bucket counts for Histogram points (length == series BucketBounds.Length + 1,
    /// last entry is the +Inf overflow bucket). Null for scalar metrics. The bucket
    /// upper bounds themselves are stored once per series on <see cref="MetricSeries.BucketBounds"/>.
    /// </summary>
    public long[]? BucketCounts;
}

/// <summary>
/// Query result — one time series.
/// </summary>
public sealed class MetricSeries
{
    public string       Name   { get; init; } = string.Empty;
    public MetricKind   Kind   { get; init; }
    public string       Unit   { get; init; } = string.Empty;
    public LabelSet     Labels { get; init; } = LabelSet.Empty;

    /// <summary>
    /// Histogram bucket upper bounds (explicit-bucket boundaries), shared by every
    /// point in the series. Null/empty for scalar metrics. A point's
    /// <see cref="MetricDataPoint.BucketCounts"/> has length <c>BucketBounds.Length + 1</c>.
    /// </summary>
    public double[]?    BucketBounds { get; init; }

    /// <summary>Data points ordered by timestamp ascending.</summary>
    public IReadOnlyList<MetricDataPoint> Points { get; init; } = [];
}

/// <summary>
/// Catalog entry describing one metric stream (name) — fed to the Explore UI.
/// </summary>
public sealed class MetricCatalogEntry
{
    public string     Name        { get; init; } = string.Empty;
    public MetricKind Kind        { get; init; }
    public string     Unit        { get; init; } = string.Empty;
    /// <summary>Distinct label keys observed across the metric's series.</summary>
    public string[]   LabelKeys   { get; init; } = [];
    /// <summary>Approximate number of distinct time series (label-set cardinality).</summary>
    public int        Cardinality { get; init; }
    /// <summary>Most recent data-point timestamp (Unix ms), 0 if unknown.</summary>
    public long       LastSeenMs  { get; init; }
}

/// <summary>Server-side aggregation operator applied to a metric query.</summary>
public enum MetricAggregation : byte
{
    /// <summary>Raw values, no aggregation across time (downsample only).</summary>
    None     = 0,
    /// <summary>Per-second rate of a cumulative counter (reset-aware).</summary>
    Rate     = 1,
    /// <summary>Total increase of a cumulative counter over each step (reset-aware).</summary>
    Increase = 2,
    Avg      = 3,
    Min      = 4,
    Max      = 5,
    Last     = 6,
    Sum      = 7,
    /// <summary>Histogram percentile via histogram_quantile over bucket deltas.</summary>
    Quantile = 8,
}

/// <summary>A server-side aggregated metric query.</summary>
public sealed class MetricQueryRequest
{
    public string             Metric      { get; init; } = string.Empty;
    public DateTimeOffset?    From        { get; init; }
    public DateTimeOffset?    To          { get; init; }
    public TimeSpan?          Step        { get; init; }
    public MetricAggregation  Aggregation { get; init; } = MetricAggregation.None;
    /// <summary>Quantile in [0,1] when <see cref="Aggregation"/> is Quantile (e.g. 0.95).</summary>
    public double?            Quantile    { get; init; }
    /// <summary>Label keys to group by; series sharing these labels are combined.</summary>
    public string[]?          GroupBy     { get; init; }
    /// <summary>Exact label matchers (key → value) applied before aggregation.</summary>
    public IReadOnlyDictionary<string, string>? Filters { get; init; }
    /// <summary>Keep only the top-K resulting series by their latest value.</summary>
    public int?               TopK        { get; init; }
}

/// <summary>A stored exemplar returned from a query (sample + its series labels).</summary>
public sealed class ExemplarSample
{
    public long     TimestampUnixNano { get; init; }
    public double   Value             { get; init; }
    public string   TraceId           { get; init; } = string.Empty;
    public string   SpanId            { get; init; } = string.Empty;
    public LabelSet Labels            { get; init; } = LabelSet.Empty;
}

/// <summary>One column of a latency/distribution heatmap (one time step).</summary>
public sealed class HeatmapColumn
{
    /// <summary>Bucket start timestamp, Unix ms.</summary>
    public long   Ts     { get; init; }
    /// <summary>Per-bucket counts within this time step (reset-aware delta).</summary>
    public double[] Counts { get; init; } = [];
}

/// <summary>Histogram heatmap result: shared bucket bounds + per-step columns.</summary>
public sealed class HeatmapResult
{
    public double[]        Bounds  { get; init; } = [];
    public HeatmapColumn[] Columns { get; init; } = [];
    public string          Unit    { get; init; } = string.Empty;
}
