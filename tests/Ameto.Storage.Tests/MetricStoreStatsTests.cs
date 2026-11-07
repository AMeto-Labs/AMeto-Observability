using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// The arithmetic behind <c>GET /api/metrics/stats</c> (<see cref="MetricStoreStats"/>): a segment's
/// bytes land in the buckets its time range covers, in proportion to the overlap; the store totals
/// count every segment while the window counts only what it overlaps; the top metrics get a column
/// each and the rest fold into <c>other</c>. Built over synthetic catalogue entries — the function
/// never opens a file, so a file is never needed to test it.
/// </summary>
public sealed class MetricStoreStatsTests
{
    private const long Nano = 1_000_000_000L;
    private static readonly DateTimeOffset Epoch = DateTimeOffset.FromUnixTimeSeconds(0);

    /// <summary>A catalogued segment of <paramref name="metric"/> covering <c>[fromSec, toSec]</c> with <paramref name="bytes"/> on disk.</summary>
    private static MetricSegmentInfo Seg(string metric, long fromSec, long toSec, long bytes, MetricGranularity g = MetricGranularity.Raw) => new()
    {
        FilePath = $"{metric}-{fromSec}.mts", MetricName = metric,
        MinNano = fromSec * Nano, MaxNano = toSec * Nano, SizeBytes = bytes, Granularity = g,
    };

    private static MetricCatalogEntry Entry(string name, MetricKind kind, int series) => new()
    {
        Name = name, Kind = kind, Cardinality = series, Unit = "1",
    };

    private static MetricStatsDto Build(IReadOnlyList<MetricSegmentInfo> segs, long fromSec, long toSec, int bucket,
                                        IReadOnlyList<MetricCatalogEntry>? catalog = null, IReadOnlyList<string>? hot = null,
                                        DateTimeOffset? now = null) =>
        MetricStoreStats.Build(
            new MetricStoreSnapshot { ColdSegments = segs, HotMetricNames = hot ?? [], HotSeries = hot?.Count ?? 0 },
            catalog ?? [], Epoch.AddSeconds(fromSec), Epoch.AddSeconds(toSec), bucket,
            now ?? Epoch.AddSeconds(toSec));

    [Fact]
    public void A_segment_spanning_two_buckets_is_split_between_them_by_time()
    {
        var dto = Build([Seg("cpu", 0, 1200, 1000)], 0, 3600, 600);

        Assert.Equal(600, dto.BucketSeconds);
        Assert.Equal(7, dto.Buckets.Length);                       // whole buckets 0 … 3600, the last one holding `to`
        Assert.Equal(0, dto.Buckets[0]);
        Assert.Equal(600_000, dto.Buckets[1]);
        var cpu = Assert.Single(dto.ByMetric);
        Assert.Equal([500, 500, 0, 0, 0, 0, 0], cpu.Points);
        Assert.Equal(1000, cpu.Bytes);
        Assert.Equal(1000, dto.Window.BytesWritten);
        Assert.Equal(500, dto.Window.PeakBucketBytes);
        Assert.Equal(1000, dto.Window.RawBytes);
        Assert.Equal(0, dto.Window.RollupBytes);
    }

    [Fact]
    public void A_single_instant_segment_lands_whole_in_its_bucket()
    {
        var dto = Build([Seg("cpu", 1500, 1500, 300)], 0, 3600, 600);
        Assert.Equal([0, 0, 300, 0, 0, 0, 0], Assert.Single(dto.ByMetric).Points);
    }

    [Fact]
    public void The_store_counts_every_segment_while_the_window_counts_only_what_it_overlaps()
    {
        MetricSegmentInfo[] segs =
        [
            Seg("old", 0, 100, 700, MetricGranularity.OneHour),     // before the window: store only
            Seg("cpu", 7200, 7800, 400),                            // half of it inside the window's last 300 s
            Seg("mem", 9000, 9600, 200, MetricGranularity.FiveMin), // inside
        ];
        var dto = Build(segs, 7500, 10_800, 300);

        Assert.Equal(3, dto.Store.Segments);
        Assert.Equal(1300, dto.Store.Bytes);
        Assert.Equal(1, dto.Store.SegmentsRaw);
        Assert.Equal(1, dto.Store.Segments5m);
        Assert.Equal(1, dto.Store.Segments1h);
        Assert.Equal(400, dto.Store.BytesRaw);
        Assert.Equal(200, dto.Store.Bytes5m);
        Assert.Equal(700, dto.Store.Bytes1h);
        Assert.Equal(0, dto.Store.OldestMs);
        Assert.Equal(9_600_000, dto.Store.NewestMs);

        Assert.Equal(2, dto.Window.Metrics);
        Assert.Equal(400, dto.Window.BytesWritten);                 // cpu's inside half (200) + mem (200)
        Assert.Equal(200, dto.Window.RawBytes);
        Assert.Equal(200, dto.Window.RollupBytes);
        Assert.Equal(["cpu", "mem"], dto.ByMetric.Select(m => m.Name).Order().ToArray());
        Assert.Equal(200, dto.ByMetric.Single(m => m.Name == "cpu").Bytes);
    }

    [Fact]
    public void Only_the_top_ten_metrics_keep_a_column_and_the_rest_fold_into_other()
    {
        var segs = new List<MetricSegmentInfo>();
        for (int i = 0; i < 12; i++) segs.Add(Seg($"m{i:00}", 0, 600, 100 + i));   // m11 is the largest
        var dto = Build(segs, 0, 600, 600);

        Assert.Equal(10, dto.ByMetric.Count);
        Assert.Equal("m11", dto.ByMetric[0].Name);
        Assert.Equal("m02", dto.ByMetric[9].Name);
        Assert.Equal(100 + 101, dto.OtherBytes);                    // m00 and m01
        Assert.Equal([201, 0], dto.OtherPoints);
        Assert.Equal(dto.Window.BytesWritten, dto.ByMetric.Sum(m => m.Bytes) + dto.OtherBytes);
    }

    [Fact]
    public void The_catalog_gives_the_kinds_the_series_and_each_top_metric_its_kind()
    {
        MetricCatalogEntry[] catalog =
        [
            Entry("cpu", MetricKind.Gauge, 4), Entry("req", MetricKind.Counter, 20), Entry("lat", MetricKind.Histogram, 3),
        ];
        var dto = Build([Seg("req", 0, 600, 50)], 0, 600, 600, catalog);

        Assert.Equal(3, dto.Store.Metrics);
        Assert.Equal(27, dto.Store.Series);
        Assert.Equal(["Counter", "Gauge", "Histogram"], dto.Kinds.Select(k => k.Kind).ToArray());
        Assert.Equal(20, dto.Kinds.Single(k => k.Kind == "Counter").Series);
        var req = Assert.Single(dto.ByMetric);
        Assert.Equal("Counter", req.Kind);
        Assert.Equal(20, req.Series);
    }

    [Fact]
    public void Hot_metric_names_count_as_data_only_when_the_window_reaches_now()
    {
        var now = Epoch.AddSeconds(10_000);
        var reachesNow = Build([], 6400, 10_000, 600, hot: ["live"], now: now);
        var inThePast  = Build([], 0, 3600, 600, hot: ["live"], now: now);

        Assert.Equal(1, reachesNow.Window.Metrics);
        Assert.Equal(0, inThePast.Window.Metrics);
        Assert.Equal(1, reachesNow.Store.HotSeries);
    }

    [Fact]
    public void A_bucket_too_fine_for_the_window_is_widened_to_the_auto_step()
    {
        // 3 600 s at 1 s would be 3 601 columns; the auto step for an hour is 60 s.
        var dto = Build([], 0, 3600, 1);
        Assert.Equal(60, dto.BucketSeconds);
        Assert.Equal(61, dto.Buckets.Length);
    }

    [Theory]
    [InlineData(86_400, 900)]            // 1 day  → 15 min
    [InlineData(3 * 86_400, 3600)]       // 3 days → 1 h
    [InlineData(7 * 86_400, 7200)]       // 7 days → 2 h
    [InlineData(14 * 86_400, 14_400)]    // 14 days → 4 h
    [InlineData(30 * 86_400, 43_200)]    // a month → 12 h
    [InlineData(92 * 86_400, 86_400)]    // 3 months → 1 day
    public void The_auto_bucket_follows_the_pages_step_table(double rangeSeconds, int expected) =>
        Assert.Equal(expected, MetricStoreStats.AutoBucketSeconds(rangeSeconds));
}
