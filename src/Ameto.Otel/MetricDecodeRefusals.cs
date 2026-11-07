namespace Ameto.Otel;

/// <summary>
/// The data points a metric decode refused and never built, by reason — what the receiver reports to
/// the sender as rejected beside the store's own refusals (OTLP partial success, HTTP <c>dropped</c>):
/// histogram points of more than <see cref="Ameto.Metrics.MetricIngestItem.MaxBucketCounts"/> buckets
/// (#126 review NEW-0) and points of more than <see cref="MetricLabelSetBuilder.MaxLabels"/> labels (#132).
/// </summary>
public readonly record struct MetricDecodeRefusals(int TooManyBuckets, int TooManyLabels)
{
    /// <summary>Every point the decode refused.</summary>
    public int Total => TooManyBuckets + TooManyLabels;
}
