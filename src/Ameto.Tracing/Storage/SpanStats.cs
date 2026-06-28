using System.Runtime.InteropServices;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Histogram boundaries (upper-exclusive) in nanoseconds — 19 edges = 20 buckets.
/// Bucket 19 catches everything ≥ 1800s.
/// </summary>
public static class HistogramBuckets
{
    // Upper-exclusive bounds in nanoseconds
    internal static ReadOnlySpan<long> Bounds => new long[]
    {
              1_000_000L, //  1 ms
              5_000_000L, //  5 ms
             10_000_000L, // 10 ms
             25_000_000L, // 25 ms
             50_000_000L, // 50 ms
            100_000_000L, // 100 ms
            250_000_000L, // 250 ms
            500_000_000L, // 500 ms
          1_000_000_000L, //   1 s
          2_500_000_000L, // 2.5 s
          5_000_000_000L, //   5 s
         10_000_000_000L, //  10 s
         30_000_000_000L, //  30 s
         60_000_000_000L, //   1 min
        120_000_000_000L, //   2 min
        300_000_000_000L, //   5 min
        600_000_000_000L, //  10 min
      1_800_000_000_000L, //  30 min
    };

    public const int Count = 19; // buckets = Bounds.Length + 1

    public static int IndexOf(long durationNanos)
    {
        var bounds = Bounds;
        for (int i = 0; i < bounds.Length; i++)
            if (durationNanos < bounds[i]) return i;
        return bounds.Length; // last bucket
    }

    /// <summary>Compute a percentile (0–1) from merged histogram buckets.</summary>
    public static double Percentile(ReadOnlySpan<uint> buckets, double p)
    {
        long total = 0;
        foreach (var b in buckets) total += b;
        if (total == 0) return 0;

        long target = (long)Math.Ceiling(p * total);
        long cum = 0;
        var bounds = Bounds;

        for (int i = 0; i < buckets.Length; i++)
        {
            cum += buckets[i];
            if (cum >= target)
            {
                // Interpolate within bucket [lower, upper)
                double lower = i == 0              ? 0 : bounds[i - 1] / 1_000_000.0;
                double upper = i >= bounds.Length  ? bounds[^1] / 1_000_000.0 * 2
                                                   : bounds[i]  / 1_000_000.0;
                double prevCum = cum - buckets[i];
                double frac = buckets[i] == 0 ? 0 : (target - prevCum) / (double)buckets[i];
                return lower + (upper - lower) * frac;
            }
        }
        return bounds[^1] / 1_000_000.0;
    }
}

/// <summary>Per-service aggregate stats for a single segment, used by the stats endpoint.</summary>
public sealed class ServiceSegmentStats
{
    public string ServiceName       { get; init; } = string.Empty;
    public uint   SpanCount         { get; init; }
    public uint   ErrorCount        { get; init; }
    public long   MinDurationNanos  { get; init; }
    public long   MaxDurationNanos  { get; init; }

    /// <summary>20-bucket duration histogram (see <see cref="HistogramBuckets"/>).</summary>
    public uint[] Buckets           { get; init; } = new uint[HistogramBuckets.Count];
}
