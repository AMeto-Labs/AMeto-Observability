using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// Metric compaction and rollup rewrite a metric in bounded series chunks so peak
/// memory does not scale with cardinality (a 38k-series deployment drove ~180 MB/s
/// of allocation and a >1 GB working set on every 5-minute rollup). Chunking
/// re-reads the sources once per chunk, which is exactly where points could be
/// dropped or duplicated — these tests pin that down.
/// </summary>
public sealed class MetricChunkedRewriteTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mchunk-" + Guid.NewGuid().ToString("N"));
    private MetricStorageEngine _engine = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private const long BaseNanos = 1_784_800_000_000_000_000L;
    private const long HourNanos = 3_600L * 1_000_000_000L;

    /// <summary>
    /// One source file: <paramref name="seriesCount"/> series, each carrying
    /// <paramref name="pointsPerFile"/> points at deterministic hourly timestamps
    /// offset by <paramref name="fileIndex"/> — so the union across files is exact.
    /// </summary>
    private List<MetricSegmentInfo> WriteSourceFile(int seriesCount, int pointsPerFile, int fileIndex, bool histogram) =>
        WriteSourceFile(Enumerable.Range(0, seriesCount).ToList(), pointsPerFile, fileIndex, histogram);

    /// <summary>
    /// As above, with the series listed in <paramref name="order"/> (series ids; one missing is a
    /// series this source does not hold). Every point says whose it is: value / count
    /// <c>s x 1000 + fileIndex x 10 + p</c>.
    /// </summary>
    private List<MetricSegmentInfo> WriteSourceFile(IReadOnlyList<int> order, int pointsPerFile, int fileIndex, bool histogram)
    {
        var bounds = histogram ? new double[] { 0.01, 0.1, 1, 10 } : null;
        var items  = new List<(SeriesKey, HotSeries)>(order.Count);

        foreach (int s in order)
        {
            var labels = new LabelSet(new Dictionary<string, string>
            {
                ["service.name"] = "Chunky.API",
                ["route"]        = "/api/r" + (s % 37),
                ["replica"]      = s.ToString(),      // makes every series distinct
            });

            var pts = new List<MetricDataPoint>(pointsPerFile);
            for (int p = 0; p < pointsPerFile; p++)
            {
                long ts = BaseNanos + (fileIndex * pointsPerFile + p) * HourNanos;
                pts.Add(histogram
                    ? new MetricDataPoint
                      {
                          TimestampUnixNano = ts,
                          Count             = s * 1000 + fileIndex * 10 + p + 1,
                          Sum               = (s + p + 1) * 0.5,
                          BucketCounts      = [s % 3, p % 5, 1, 0, 2],
                      }
                    : new MetricDataPoint { TimestampUnixNano = ts, Value = s * 1000 + fileIndex * 10 + p });
            }

            items.Add((
                new SeriesKey(histogram ? "chunk.hist" : "chunk.gauge",
                              histogram ? MetricKind.Histogram : MetricKind.Gauge,
                              histogram ? "s" : "By", labels),
                new HotSeries(pts, bounds)));
        }
        return MetricWriter.Write(_dir, items, MetricGranularity.OneHour);
    }

    /// <summary>Sort + drop duplicate timestamps — mirrors the compaction transform.</summary>
    private static List<MetricDataPoint> SortDedupe(List<MetricDataPoint> pts, MetricKind _)
    {
        pts.Sort((a, b) => a.TimestampUnixNano.CompareTo(b.TimestampUnixNano));
        var outp = new List<MetricDataPoint>(pts.Count);
        for (int i = 0; i < pts.Count; i++)
            if (i + 1 >= pts.Count || pts[i + 1].TimestampUnixNano != pts[i].TimestampUnixNano)
                outp.Add(pts[i]);
        return outp;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(1300, false)]   // > 2 chunks, scalar
    [InlineData(1300, true)]    // > 2 chunks, histogram (bucket arrays per point)
    [InlineData(300,  false)]   // single chunk — the fast, single-pass path
    public void Rewrite_IsLossless_RegardlessOfCardinality(int seriesCount, bool histogram)
    {
        const int filesCount = 3, pointsPerFile = 4;

        var sources = new List<MetricSegmentInfo>();
        for (int f = 0; f < filesCount; f++)
            sources.AddRange(WriteSourceFile(seriesCount, pointsPerFile, f, histogram));

        var outputs = _engine.RewriteMetricInChunks(sources, MetricGranularity.OneHour, SortDedupe);
        Assert.NotEmpty(outputs);

        // Every series appears exactly once across the outputs, with every point.
        var seen = new Dictionary<LabelSet, List<long>>();
        foreach (var info in outputs)
            foreach (var s in MetricReader.ReadAllSync(info.FilePath))
            {
                Assert.False(seen.ContainsKey(s.Labels), "series duplicated across output files");
                seen[s.Labels] = s.Points.Select(p => p.TimestampUnixNano).ToList();
                if (histogram)
                {
                    Assert.Equal(MetricKind.Histogram, s.Kind);
                    Assert.NotNull(s.BucketBounds);          // bounds survive chunking
                    Assert.All(s.Points, p => Assert.NotNull(p.BucketCounts));
                }
            }

        Assert.Equal(seriesCount, seen.Count);

        // Points: the union of all files, in order, no gaps and no duplicates.
        var expected = Enumerable.Range(0, filesCount * pointsPerFile)
            .Select(i => BaseNanos + i * HourNanos).ToList();
        foreach (var (_, timestamps) in seen)
            Assert.Equal(expected, timestamps);
    }

    /// <summary>Chunking must honour the per-file series cap — that is the memory bound.</summary>
    [Fact]
    public void Rewrite_HonoursSeriesCapPerFile()
    {
        var sources = WriteSourceFile(1300, 2, fileIndex: 0, histogram: false);
        var outputs = _engine.RewriteMetricInChunks(sources, MetricGranularity.OneHour, SortDedupe);

        Assert.All(outputs, o => Assert.True(
            MetricReader.ReadAllSync(o.FilePath).Count() <= 512,
            "an output file exceeded the 512-series cap"));
        Assert.Equal(1300, outputs.Sum(o => MetricReader.ReadAllSync(o.FilePath).Count()));
    }

    /// <summary>
    /// POSITIONING (#94). The test above cannot see a point landing on the wrong series: its sources
    /// list the series in one order, so a later chunk read "by position in the source" and "by key"
    /// are the same thing, and every series carries the same timestamps. Here the three sources
    /// list them ascending, descending and shuffled; the shuffled one lacks every fourth series and
    /// holds one no other source has; and every point's value names its series and its source. Each
    /// output series must carry exactly its own points — from the sources that hold it, in order.
    /// (The chunked-rewrite byte goldens in <c>MetricDownsampleGoldenTests</c> pin the same thing as
    /// bytes; this says which point went where when it breaks.)
    /// </summary>
    [Theory]
    [InlineData(1300, false)]   // three chunks: the later two are read back by position
    [InlineData(1300, true)]
    [InlineData(300,  false)]   // one chunk: gathered by key in the first pass
    public void Rewrite_puts_every_point_on_its_own_series_whatever_order_each_source_lists_them(int seriesCount, bool histogram)
    {
        const int pointsPerFile = 3;
        int onlyInLast = seriesCount + 5;

        var ascending  = Enumerable.Range(0, seriesCount).ToList();
        var descending = Enumerable.Range(0, seriesCount).Reverse().ToList();
        var shuffled   = Enumerable.Range(0, seriesCount).Where(s => s % 4 != 1).Append(onlyInLast).ToList();
        new GoldenRng(0x5EED_0C4AUL).Shuffle(shuffled);
        List<int>[] orders = [ascending, descending, shuffled];

        var sources = new List<MetricSegmentInfo>();
        for (int f = 0; f < orders.Length; f++)
            sources.AddRange(WriteSourceFile(orders[f], pointsPerFile, f, histogram));

        var outputs = _engine.RewriteMetricInChunks(sources, MetricGranularity.OneHour, SortDedupe);

        var seen = new HashSet<int>();
        foreach (var info in outputs)
            foreach (var series in MetricReader.ReadAllSync(info.FilePath))
            {
                int s = int.Parse(series.Labels.Pairs.Single(p => p.Key == "replica").Value, System.Globalization.CultureInfo.InvariantCulture);
                Assert.True(seen.Add(s), $"series {s} written twice");

                var expected = new List<(long Ts, double Tag)>();
                for (int f = 0; f < orders.Length; f++)
                {
                    if (!orders[f].Contains(s)) continue;
                    for (int p = 0; p < pointsPerFile; p++)
                        expected.Add((BaseNanos + (f * pointsPerFile + p) * HourNanos,
                                      s * 1000 + f * 10 + p + (histogram ? 1 : 0)));
                }
                var actual = series.Points.Select(p => (p.TimestampUnixNano, histogram ? (double)p.Count : p.Value)).ToList();
                Assert.Equal(expected, actual);
            }

        Assert.Equal(seriesCount + 1, seen.Count);
        Assert.Contains(onlyInLast, seen);
    }
}
