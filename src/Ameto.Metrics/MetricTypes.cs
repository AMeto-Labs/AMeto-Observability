using System.Runtime.InteropServices;
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
}

/// <summary>
/// A stored data point in a time series.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
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

    /// <summary>Data points ordered by timestamp ascending.</summary>
    public IReadOnlyList<MetricDataPoint> Points { get; init; } = [];
}
