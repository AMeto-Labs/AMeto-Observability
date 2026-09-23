using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// The chunked rewrite reads each source once for its identities and goes back only for a later
/// chunk's own series (issue #83 WP7, M#12). Two rules of the old re-read-everything loop that
/// the byte goldens do not exercise — their sources agree on every series' bounds and every
/// series has points — are stated here, for the first chunk and a later one alike: a series'
/// bounds are the LAST non-null ones any source gave it, and a series a source holds with no
/// points is still written.
/// </summary>
public sealed class MetricRewriteChunkTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrwchunk-" + Guid.NewGuid().ToString("N"));
    private MetricStorageEngine _engine = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(Path.Combine(_dir, "engine"));
        _engine = new MetricStorageEngine(Path.Combine(_dir, "engine"), NullLogger<MetricStorageEngine>.Instance);
        await _engine.ColdLoadCompleted;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private const long T0 = 1_784_800_020_000_000_000L;
    private const long S  = 1_000_000_000L;

    private static LabelSet Labels(int s) =>
        new([new("replica", "r" + s.ToString("D4", CultureInfo.InvariantCulture))]);

    private List<MetricSegmentInfo> Source(int series, int file, Func<int, double[]?> bounds, Func<int, bool> empty)
    {
        var items = new List<(SeriesKey, HotSeries)>();
        for (int s = 0; s < series; s++)
        {
            var pts = new List<MetricDataPoint>();
            if (!empty(s))
                for (int p = 0; p < 3; p++)
                    pts.Add(new MetricDataPoint
                    {
                        TimestampUnixNano = T0 + (file * 3 + p) * 60 * S, Count = p + 1, Sum = p,
                        BucketCounts = [p, 1, 0, 1],
                    });
            items.Add((new SeriesKey("rw.h", MetricKind.Histogram, "s", Labels(s)), new HotSeries(pts, bounds(s))));
        }
        return MetricWriter.Write(_dir, items, MetricGranularity.Raw);
    }

    [Theory]
    [InlineData(300)]    // one chunk
    [InlineData(1_100)]  // three chunks
    public void The_last_bounds_seen_win_and_a_pointless_series_is_still_written(int series)
    {
        double[] first = [1, 2, 3], second = [5, 6, 7];
        var sources = new List<MetricSegmentInfo>();
        sources.AddRange(Source(series, 0, _ => first, s => s % 11 == 3));
        // The second source re-buckets the even series and records no bounds for the odd ones.
        sources.AddRange(Source(series, 1, s => s % 2 == 0 ? second : null, s => s % 11 == 3));

        var outputs = _engine.RewriteMetricInChunks(sources, MetricGranularity.Raw,
                                                    static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts));

        var seen = new Dictionary<string, MetricSeries>();
        foreach (var o in outputs)
            foreach (var s in MetricReader.ReadAllSync(o.FilePath))
                seen.Add(s.Labels.ValueAt(0), s);

        Assert.Equal(series, seen.Count);
        for (int s = 0; s < series; s++)
        {
            var got = seen["r" + s.ToString("D4", CultureInfo.InvariantCulture)];
            Assert.Equal(s % 2 == 0 ? second : first, got.BucketBounds);
            Assert.Equal(s % 11 == 3 ? 0 : 6, got.Points.Count);
        }
    }
}
