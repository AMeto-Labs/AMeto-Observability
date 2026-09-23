using System.Diagnostics;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A HOT QUERY TOUCHES ITS OWN METRIC, AND A RANGE IS FOUND, NOT FILTERED. M#8 of issue #83.
///
/// <para><c>QueryAsync</c> and <c>GetLatestAsync</c> walked EVERY series of EVERY metric in the
/// hot tier with a per-character <c>OrdinalIgnoreCase</c> compare per entry, and
/// <c>HotSeries.GetPoints</c> then ran <c>Where().OrderBy().ToList()</c> under the series lock — a
/// full stable sort of a list that is already in order for every exporter that is alone on its
/// series. The alert evaluator runs one <c>QueryAsync</c> per rule every 15 s, forever.</para>
/// </summary>
public sealed class MetricHotQueryIndexTests
{
    private readonly ITestOutputHelper _out;
    public MetricHotQueryIndexTests(ITestOutputHelper output) => _out = output;

    private const int Metrics        = 100;
    private const int SeriesPerName  = 100;
    private const int PointsPerSeries = 30;
    private const int Queries        = 200;

    /// <summary>A tier that never reaches its threshold here, so every point stays hot.</summary>
    private static readonly Ameto.Core.MetricsOptions NoFlush = new() { HotTierBytes = 1L << 30 };

    /// <summary>
    /// Probe, printed, its asserts pinning only the answer: ns and bytes for one full-range and one narrow-range
    /// <c>QueryAsync</c> of ONE metric out of a hundred, and one <c>GetLatestAsync</c>.
    /// </summary>
    [Fact]
    public async Task Probe_one_metric_out_of_a_hundred()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotq-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, NoFlush);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            var batch = new MetricIngestItem[Metrics * SeriesPerName];
            for (int p = 0; p < PointsPerSeries; p++)
            {
                int n = 0;
                for (int m = 0; m < Metrics; m++)
                    for (int s = 0; s < SeriesPerName; s++)
                        batch[n++] = Point("probe.metric." + m, "s" + s, baseNano + p * 15_000_000_000L, p);
                engine.Ingest(batch);
            }

            var from   = DateTimeOffset.FromUnixTimeMilliseconds(baseNano / 1_000_000L);
            var narrow = (From: from.AddSeconds(15 * 10), To: from.AddSeconds(15 * 12));

            // Warm every path once.
            _ = await Count(engine.QueryAsync("probe.metric.42"));
            _ = await Count(engine.QueryAsync("probe.metric.42", narrow.From, narrow.To));
            _ = await Count(engine.GetLatestAsync("probe.metric.42"));

            var (fullNs, fullB, fullPts)       = await Measure(() => engine.QueryAsync("probe.metric.42"));
            var (narrowNs, narrowB, narrowPts) = await Measure(() => engine.QueryAsync("probe.metric.42", narrow.From, narrow.To));
            var (latestNs, latestB, latestPts) = await Measure(() => engine.GetLatestAsync("probe.metric.42"));

            _out.WriteLine($"HOT QUERY  {Metrics} metrics x {SeriesPerName} series x {PointsPerSeries} points, {Queries} queries");
            _out.WriteLine($"  QueryAsync full range   : {fullNs,12:N0} ns/query {fullB,12:N0} B/query  ({fullPts} points)");
            _out.WriteLine($"  QueryAsync 3 of 30 pts  : {narrowNs,12:N0} ns/query {narrowB,12:N0} B/query  ({narrowPts} points)");
            _out.WriteLine($"  GetLatestAsync          : {latestNs,12:N0} ns/query {latestB,12:N0} B/query  ({latestPts} points)");

            Assert.Equal(SeriesPerName * PointsPerSeries, fullPts);
            Assert.Equal(SeriesPerName * 3, narrowPts);
            Assert.Equal(SeriesPerName, latestPts);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }

        static async Task<(double Ns, double Bytes, int Points)> Measure(Func<IAsyncEnumerable<MetricSeries>> query)
        {
            int points = 0;
            long b0 = GC.GetAllocatedBytesForCurrentThread();
            var sw  = Stopwatch.StartNew();
            for (int i = 0; i < Queries; i++) points = await Count(query());
            sw.Stop();
            long b = GC.GetAllocatedBytesForCurrentThread() - b0;
            return (sw.Elapsed.TotalNanoseconds / Queries, b / (double)Queries, points);
        }
    }

    /// <summary>
    /// Two exporters interleaving on one series are not chronological, so the binary search is
    /// only allowed while no append has gone backwards. Revert the flag in <c>HotSeries.Append</c>
    /// (never set it) and the full-range query comes back in arrival order and the ranged one
    /// misses points.
    /// </summary>
    [Fact]
    public async Task An_out_of_order_series_answers_sorted_with_ties_in_arrival_order()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotooo-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, NoFlush);
            long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Arrival: 5, 1, 3 (value 30), 3 (value 31), 2.
            engine.Ingest([
                Point("ooo", "a", Ms(t0 + 5), 50), Point("ooo", "a", Ms(t0 + 1), 10),
                Point("ooo", "a", Ms(t0 + 3), 30), Point("ooo", "a", Ms(t0 + 3), 31),
                Point("ooo", "a", Ms(t0 + 2), 20),
            ]);

            Assert.Equal([10.0, 20, 30, 31, 50], await Values(engine.QueryAsync("ooo")));
            Assert.Equal([20.0, 30, 31], await Values(engine.QueryAsync("ooo", At(t0 + 2), At(t0 + 3))));
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// The in-order path: both ends of the range inclusive, an empty and an inverted range empty.
    /// </summary>
    [Fact]
    public async Task An_in_order_series_is_ranged_inclusively_by_binary_search()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotord-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, NoFlush);
            long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var batch = new MetricIngestItem[10];
            for (int i = 0; i < batch.Length; i++) batch[i] = Point("ord", "a", Ms(t0 + i * 10), i);
            engine.Ingest(batch);

            Assert.Equal([3.0, 4, 5], await Values(engine.QueryAsync("ord", At(t0 + 30), At(t0 + 50))));
            Assert.Equal([3.0, 4],    await Values(engine.QueryAsync("ord", At(t0 + 25), At(t0 + 45))));
            Assert.Equal([0.0],       await Values(engine.QueryAsync("ord", null, At(t0))));
            Assert.Equal([9.0],       await Values(engine.QueryAsync("ord", At(t0 + 90), null)));
            Assert.Empty(await Values(engine.QueryAsync("ord", At(t0 + 91), At(t0 + 99))));
            Assert.Empty(await Values(engine.QueryAsync("ord", At(t0 + 50), At(t0 + 30))));
            Assert.Equal(10, (await Values(engine.QueryAsync("ord"))).Count);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// The drain hands the writer a list in ARRIVAL order, wrapped in a new <c>HotSeries</c>, and
    /// the writer reads it through <c>GetPoints</c>. Revert the constructor's order scan and the
    /// file is written unsorted: the cold answer comes back 5, 1, 3, 2.
    /// </summary>
    [Fact]
    public async Task A_drained_out_of_order_series_is_written_in_order()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotooow-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            engine.Ingest([
                Point("ooow", "a", Ms(t0 + 5), 50), Point("ooow", "a", Ms(t0 + 1), 10),
                Point("ooow", "a", Ms(t0 + 3), 30), Point("ooow", "a", Ms(t0 + 2), 20),
            ]);
            await engine.ScheduleThresholdFlushForTest();
            Assert.Equal(0, engine.HotPointCount);

            Assert.Equal([10.0, 20, 30, 50], await Values(engine.QueryAsync("ooow")));
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// The name index follows the tier: a query finds its metric whatever the case, finds no other
    /// metric's series, loses an evicted series and finds its re-creation. Revert the index removal
    /// in <c>TryEvictLocked</c> and the evicted series stays filed (count 1 after the sweep).
    /// </summary>
    [Fact]
    public async Task The_name_index_follows_eviction_and_re_creation()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotidx-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var clock = new MetricTestClock();
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, timeProvider: clock);
            long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            engine.Ingest([Point("idx.a", "x", Ms(t0), 1), Point("idx.b", "y", Ms(t0), 2), Point("idx.b", "z", Ms(t0), 3)]);
            Assert.Equal(1, engine.IndexedSeriesCount("idx.a"));
            Assert.Equal(2, engine.IndexedSeriesCount("IDX.B"));

            Assert.Equal([1.0], await Values(engine.QueryAsync("IDX.A")));
            Assert.Equal([1.0], await Values(engine.GetLatestAsync("Idx.A")));
            Assert.Empty(await Values(engine.QueryAsync("idx.c")));
            Assert.Empty(await Values(engine.GetLatestAsync("idx.c")));

            // Drain, then idle past twice MaxHotAge: the tick's sweep evicts all three.
            await engine.ScheduleThresholdFlushForTest();
            clock.Advance(engine.ConfiguredOptions.MaxHotAge * 2 + TimeSpan.FromMinutes(1));
            await engine.FlushPeriodicForTest();
            Assert.Equal(3, engine.StaleSeriesEvicted);
            Assert.Equal(0, engine.IndexedSeriesCount("idx.a"));
            Assert.Equal(0, engine.IndexedSeriesCount("idx.b"));

            // A series that comes back is a new object, filed again and found again.
            engine.Ingest([Point("idx.a", "x", Ms(t0 + 1), 7)]);
            Assert.Equal(1, engine.IndexedSeriesCount("idx.a"));
            Assert.Equal([7.0], await Values(engine.GetLatestAsync("idx.a")));
            Assert.Contains(7.0, await Values(engine.QueryAsync("idx.a")));
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    private static long Ms(long unixMs) => unixMs * 1_000_000L;
    private static DateTimeOffset At(long unixMs) => DateTimeOffset.FromUnixTimeMilliseconds(unixMs);

    private static async Task<List<double>> Values(IAsyncEnumerable<MetricSeries> series)
    {
        var values = new List<double>();
        await foreach (var s in series)
            foreach (var p in s.Points) values.Add(p.Value);
        return values;
    }

    private static async Task<int> Count(IAsyncEnumerable<MetricSeries> series)
    {
        int n = 0;
        await foreach (var s in series) n += s.Points.Count;
        return n;
    }

    private static MetricIngestItem Point(string name, string series, long nano, double value = 1.0) => new()
    {
        Name              = name,
        Kind              = MetricKind.Gauge,
        Labels            = new LabelSet(new Dictionary<string, string> { ["series"] = series }),
        TimestampUnixNano = nano,
        ScalarValue       = value,
    };
}
