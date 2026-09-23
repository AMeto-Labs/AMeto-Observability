using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// A cold read applies its time range WHILE it decodes (issue #83 WP7, M#4(a)): an out-of-range
/// point is walked but never stored, and its bucket array is not built unless a later in-range
/// slim point inherits it. What the read answers is pinned by <c>MetricQueryGoldenTests</c>; these
/// pin the one piece of state that crosses the range edge, and what a narrow read costs.
/// </summary>
public sealed class MetricReaderRangeTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrange-" + Guid.NewGuid().ToString("N"));

    public MetricReaderRangeTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long S  = 1_000_000_000L;
    private const long T0 = 1_784_800_020_000_000_000L;

    private static readonly double[] Bounds = [0.1, 1, 10];

    private static MetricDataPoint H(long ts, long count, double sum, long[]? buckets) =>
        new() { TimestampUnixNano = ts, Value = count > 0 ? sum / count : 0, Count = count, Sum = sum, BucketCounts = buckets };

    private string Write(params (LabelSet Labels, List<MetricDataPoint> Points)[] series)
    {
        var items = new List<(SeriesKey, HotSeries)>();
        foreach (var (labels, pts) in series)
            items.Add((new SeriesKey("range.h", MetricKind.Histogram, "s", labels), new HotSeries(pts, Bounds)));
        return Assert.Single(MetricWriter.Write(_dir, items, MetricGranularity.Raw)).FilePath;
    }

    private static async Task<MetricSeries> ReadOne(string file, long from, long to)
    {
        var got = new List<MetricSeries>();
        await foreach (var s in MetricReader.ReadAsync(file, "range.h", from, to, null, CancellationToken.None)) got.Add(s);
        return Assert.Single(got);
    }

    [Fact]
    public async Task A_slim_point_in_range_carries_the_buckets_of_the_full_point_before_the_range()
    {
        // On disk: idle, idle, FULL (buckets 1,2,3,4), slim, slim, FULL (2,2,3,5), slim. The window
        // opens between the first full point and the slim ones that inherit its state.
        long[] first = [1, 2, 3, 4], second = [2, 2, 3, 5];
        string file = Write((new LabelSet([new("k", "v")]), new List<MetricDataPoint>
        {
            H(T0,          0,  0,   null),
            H(T0 + 15 * S, 0,  0,   null),
            H(T0 + 30 * S, 10, 2.5, first),
            H(T0 + 45 * S, 10, 2.5, first),     // slim on disk
            H(T0 + 60 * S, 10, 2.5, first),     // slim on disk
            H(T0 + 75 * S, 12, 3.0, second),
            H(T0 + 90 * S, 12, 3.0, second),    // slim on disk
        }));

        var s = await ReadOne(file, T0 + 40 * S, long.MaxValue);

        Assert.Equal(new long[] { T0 + 45 * S, T0 + 60 * S, T0 + 75 * S, T0 + 90 * S }, s.Points.Select(p => p.TimestampUnixNano));
        Assert.Equal(first,  s.Points[0].BucketCounts);
        Assert.Equal(10,     s.Points[0].Count);
        Assert.Same(s.Points[0].BucketCounts, s.Points[1].BucketCounts);   // one array per full point, as ever
        Assert.Equal(second, s.Points[2].BucketCounts);
        Assert.Same(s.Points[2].BucketCounts, s.Points[3].BucketCounts);

        // The idle run at the start: slim points before any full one read back with the series'
        // all-zero array — including when the window starts inside the run.
        var idle = await ReadOne(file, T0 + 10 * S, T0 + 20 * S);
        Assert.Equal(new long[] { 0, 0, 0, 0 }, Assert.Single(idle.Points).BucketCounts);
    }

    [Fact]
    public async Task A_narrow_read_does_not_build_the_points_it_leaves_out()
    {
        // 500 busy histogram series x 60 points: every point is a full one with its own array.
        var series = new (LabelSet, List<MetricDataPoint>)[500];
        for (int s = 0; s < series.Length; s++)
        {
            var pts = new List<MetricDataPoint>(60);
            for (int p = 0; p < 60; p++)
                pts.Add(H(T0 + p * 15 * S, p + 1, p * 0.5, [p, s % 7, 1, p + s]));
            series[s] = (new LabelSet([new("replica", "r" + s.ToString(System.Globalization.CultureInfo.InvariantCulture))]), pts);
        }
        string file = Write(series);

        long from = T0 + 59 * 15 * S, to = long.MaxValue;   // the last point of each series
        int warm = 0;
        await foreach (var _ in MetricReader.ReadAsync(file, "range.h", from, to, null, CancellationToken.None)) warm++;
        Assert.Equal(500, warm);

        long before = GC.GetAllocatedBytesForCurrentThread();
        int n = DrainOnThisThread(MetricReader.ReadAsync(file, "range.h", from, to, null, CancellationToken.None));
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(500, n);
        // Decoded whole and then filtered, the 59 dropped points of each series were 40 B in a list
        // plus a 56 B bucket array each, twice over with the copy: ~10 KB a series, 5 MB here.
        Assert.True(bytes < 1_000_000, $"a one-point-per-series read of 500 series allocated {bytes:N0} B");
    }

    /// <summary>Points in an async read that never awaits, walked synchronously so every byte it allocates is on this thread.</summary>
    private static int DrainOnThisThread(IAsyncEnumerable<MetricSeries> read)
    {
        int n = 0;
        var e = read.GetAsyncEnumerator();
        try
        {
            while (true)
            {
                var next = e.MoveNextAsync();
                Assert.True(next.IsCompleted);            // no await inside: every byte lands on this thread
                if (!next.Result) break;
                n += e.Current.Points.Count;
            }
        }
        finally { Assert.True(e.DisposeAsync().IsCompleted); }
        return n;
    }
}
