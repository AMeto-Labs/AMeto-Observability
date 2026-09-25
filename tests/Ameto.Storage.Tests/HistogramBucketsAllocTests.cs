using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// <c>HistogramBuckets.IndexOf</c> and <c>Percentile</c> run once per span in the stats passes and
/// once per service per stats response. Their bounds were a <c>ReadOnlySpan&lt;long&gt;</c> property
/// over <c>new long[] { ... }</c>, which only an optimizing JIT turns into a pointer at the constant
/// data: unoptimized code — the Debug build CI runs these tests in — allocated 72 B per access.
/// This pins the span over a static array. Reverting it fails here in Debug and passes in Release,
/// which is exactly the discrepancy the change removes.
/// </summary>
public sealed class HistogramBucketsAllocTests
{
    [Fact]
    public void Bucket_lookup_and_percentile_allocate_nothing()
    {
        var buckets = new uint[HistogramBuckets.Count];
        buckets[3] = 5; buckets[10] = 7;

        // Warm: type initialiser, JIT.
        int acc = HistogramBuckets.IndexOf(1);
        double pct = HistogramBuckets.Percentile(buckets, 0.5);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
        {
            acc += HistogramBuckets.IndexOf(i * 7_000_000L);
            pct += HistogramBuckets.Percentile(buckets, 0.95);
        }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(acc > 0 && pct > 0);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void The_bounds_are_the_documented_edges()
    {
        var b = HistogramBuckets.Bounds;
        Assert.Equal(HistogramBuckets.Count - 1, b.Length);
        Assert.Equal(1_000_000L, b[0]);
        Assert.Equal(1_800_000_000_000L, b[^1]);
        for (int i = 1; i < b.Length; i++) Assert.True(b[i] > b[i - 1]);
        Assert.Equal(0, HistogramBuckets.IndexOf(999_999));
        Assert.Equal(1, HistogramBuckets.IndexOf(1_000_000));
        Assert.Equal(b.Length, HistogramBuckets.IndexOf(long.MaxValue));
    }
}
