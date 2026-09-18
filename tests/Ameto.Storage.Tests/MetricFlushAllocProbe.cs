using System.Diagnostics;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A FLUSH COSTS, PER POINT, FOR THE TWO SHAPES THAT WEIGH DIFFERENT AMOUNTS.
///
/// <para>The reconnaissance measured 35-52 MB allocated to flush 120 000 points into 20-48 KB of
/// file (309-453 B a point), and a hot tier that at the flat 500 000-point threshold held 20 MB
/// of gauge points or <b>84 MB of histogram points</b> — the same threshold, four times the
/// memory, on a host that was never asked how much it had.</para>
///
/// <para>The gauge and histogram arms are the point of the probe: they make the difference
/// between the two shapes a number rather than an argument, and the last fact drives 500 000
/// histogram points through the real threshold — the load that, at the flat point count, put
/// 84 MB of live bucket arrays into a 384 MB heap limit.</para>
/// </summary>
public sealed class MetricFlushAllocProbe
{
    private readonly ITestOutputHelper _out;
    public MetricFlushAllocProbe(ITestOutputHelper o) => _out = o;

    private const int SeriesCount     = 2_000;
    private const int PointsPerSeries = 50;      // 100 000 points: under the byte threshold for both shapes
    private const int Buckets         = 16;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FlushCostPerPoint(bool histogram)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mflush-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            Feed(engine, baseNano, SeriesCount, PointsPerSeries, histogram);

            int points = SeriesCount * PointsPerSeries;
            Assert.Equal(points, engine.HotPointCount);   // the whole burst must still be in the tier

            long tierBytes = engine.HotByteCount;
            long before    = GC.GetTotalAllocatedBytes(precise: true);
            var  sw        = Stopwatch.StartNew();
            await engine.ScheduleThresholdFlushForTest();
            sw.Stop();
            long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

            long diskBytes = 0;
            int  files     = 0;
            foreach (string f in Directory.EnumerateFiles(dir, "*.mts")) { diskBytes += new FileInfo(f).Length; files++; }

            _out.WriteLine($"{(histogram ? $"HISTOGRAM ({Buckets} buckets)" : "GAUGE")}: {SeriesCount:N0} series x {PointsPerSeries} points = {points:N0} points");
            _out.WriteLine($"  tier charged  : {tierBytes / 1048576.0,8:N2} MB = {tierBytes / (double)points,6:N0} B/point");
            _out.WriteLine($"  flush wall    : {sw.Elapsed.TotalMilliseconds,8:N1} ms = {sw.Elapsed.TotalMilliseconds * 1_000_000 / points,6:N0} ns/point");
            _out.WriteLine($"  flush alloc   : {allocated / 1048576.0,8:N2} MB = {allocated / (double)points,6:N0} B/point");
            _out.WriteLine($"  on disk       : {diskBytes / 1024.0,8:N1} KB in {files} file(s) = {diskBytes / (double)points,6:N2} B/point");

            Assert.Equal(0, engine.HotPointCount);
            Assert.Equal(0, engine.HotByteCount);
            Assert.True(diskBytes / points < 128,
                $"the flush wrote {diskBytes / (double)points:N1} B per point to disk");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// THE OOM LOAD, DRIVEN THROUGH THE REAL THRESHOLD. Half a million 16-bucket histogram
    /// points: 84 MB of live bucket arrays at the flat 500 000-POINT threshold, which is what a
    /// 384 MB heap limit could not carry beside a 48 MB index cache and a 16 MB log tier. With
    /// the threshold spent in bytes the tier never holds more than its budget, whatever shape the
    /// points are — so this is a memory fact, asserted as one, and not a smoke test.
    ///
    /// <para>Run once under <c>DOTNET_GCHeapHardLimit=0x18000000</c> to reproduce the stand; the
    /// budget derives to 19.2 MB there and the tier flushes about six times instead of once.</para>
    /// </summary>
    [Fact]
    public async Task Half_a_million_histogram_points_never_put_more_than_the_budget_in_the_tier()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mflushoom-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
            long budget   = new MetricsOptions().EffectiveHotTierBytes;

            const int series = 200, perSeries = 2_500;      // 500 000 points
            long peak = 0;
            Feed(engine, baseNano, series, perSeries, histogram: true,
                 afterChunk: e => peak = Math.Max(peak, e.HotByteCount));

            _out.WriteLine($"500 000 16-bucket histogram points: budget {budget / 1048576.0:N1} MB, tier peak {peak / 1048576.0:N1} MB");
            _out.WriteLine($"  at the old flat 500 000-point threshold the same load held {500_000 * 216 / 1048576.0:N0} MB");

            // THE THRESHOLD IS A TRIGGER, NOT A CEILING, and the margin says which. An ingest
            // call cannot wait on a flush — it schedules one and returns — so points keep
            // arriving until that flush reaches its snapshot, and the peak is the budget plus
            // whatever landed in between (measured: 8 % over, two OTLP chunks' worth). What the
            // byte accounting buys is that the overshoot is a flush's latency and not the shape
            // of the points: at the flat 500 000-POINT threshold this same load held 103 MB,
            // 3.4x the budget, because a 16-bucket histogram point was counted as one point.
            Assert.True(peak < budget * 2,
                $"the tier reached {peak / 1048576.0:N1} MB against a budget of {budget / 1048576.0:N1} MB");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    private const int ChunkPoints = 10_000;   // one OTLP export's worth

    private static void Feed(MetricStorageEngine engine, long baseNano, int seriesCount, int pointsPerSeries,
                             bool histogram, Action<MetricStorageEngine>? afterChunk = null)
    {
        var bounds = new double[Buckets - 1];
        for (int i = 0; i < bounds.Length; i++) bounds[i] = i + 1;

        var chunk = new List<MetricIngestItem>(ChunkPoints);
        for (int p = 0; p < pointsPerSeries; p++)
        for (int s = 0; s < seriesCount; s++)
        {
            long ts = baseNano + p * 15_000_000_000L;
            chunk.Add(histogram
                ? new MetricIngestItem
                {
                    Name              = "http.server.request.duration",
                    Kind              = MetricKind.Histogram,
                    Unit              = "ms",
                    Labels            = Labels(s),
                    TimestampUnixNano = ts,
                    HistogramCount    = 10 + p,
                    HistogramSum      = 12.5 * (p + 1),
                    BucketBounds      = bounds,
                    BucketCounts      = NewCounts(p),
                }
                : new MetricIngestItem
                {
                    Name              = "http.server.request.duration",
                    Kind              = MetricKind.Gauge,
                    Unit              = "ms",
                    Labels            = Labels(s),
                    TimestampUnixNano = ts,
                    ScalarValue       = p,
                });

            if (chunk.Count < ChunkPoints) continue;
            Send(engine, chunk, afterChunk);
        }
        if (chunk.Count > 0) Send(engine, chunk, afterChunk);

        static void Send(MetricStorageEngine engine, List<MetricIngestItem> chunk, Action<MetricStorageEngine>? afterChunk)
        {
            var arr = chunk.ToArray();
            chunk.Clear();
            engine.Ingest(arr);
            afterChunk?.Invoke(engine);
        }

        // A fresh array per point, as the OTLP parser produces: the bucket counts are what makes
        // a histogram point 3.4x a scalar one, and sharing one array would hide exactly that.
        static long[] NewCounts(int p)
        {
            var counts = new long[Buckets];
            for (int i = 0; i < counts.Length; i++) counts[i] = (p + i) & 7;
            return counts;
        }
    }

    private static LabelSet Labels(int series) => new(new Dictionary<string, string>
    {
        ["service.name"]              = "checkout",
        ["http.route"]                = "/api/v1/resource/" + series,
        ["http.request.method"]       = (series & 1) == 0 ? "GET" : "POST",
        ["http.response.status_code"] = (series % 5) == 0 ? "500" : "200",
        ["server.address"]            = "host-" + (series & 7),
    });
}
