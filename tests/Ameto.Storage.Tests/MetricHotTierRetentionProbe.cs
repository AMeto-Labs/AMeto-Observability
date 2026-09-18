using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT AN <b>EMPTY</b> HOT TIER STILL HOLDS.
///
/// <para><c>HotSeries.Drain</c> used to copy every point into a fresh list and then call
/// <c>List.Clear()</c> on the original — and <c>Clear</c> does not shrink the backing array. So
/// each series permanently owned a <c>MetricDataPoint[]</c> sized to the largest burst it had
/// ever seen, for the life of the process, whether or not it ever reported again; and there was
/// no <c>_hot.TryRemove</c> anywhere in the engine, so a series that stopped reporting never
/// left either. The reconnaissance measured <b>12 166 B retained per series by a tier holding
/// zero points</b> — 54 % of the burst's heap surviving the drain. On the sandbox's own 38 741
/// series that is ~470 MB of gen2 nothing can reclaim, inside a container whose GC heap hard
/// limit is 384 MB.</para>
///
/// <para>The burst is fed in OTLP-sized chunks on purpose. Handing the engine one 400 000-item
/// array makes the array itself the largest thing on the heap, and the measurement then reports
/// the caller's batch instead of the tier (this cost the reconnaissance a wrong number: 55 KB
/// per series against a true 12 KB).</para>
/// </summary>
public sealed class MetricHotTierRetentionProbe
{
    private readonly ITestOutputHelper _out;
    public MetricHotTierRetentionProbe(ITestOutputHelper o) => _out = o;

    private const int SeriesCount = 2_000;
    private const int ChunkPoints = 10_000;   // one OTLP export's worth

    /// <summary>
    /// Three quarters of whatever the tier's byte budget is on THIS host, so the burst is the
    /// largest one that provably does not trip the engine's own threshold. A fixed count cannot
    /// do that: the budget is a share of the managed-heap limit, so the same 400 000 points sit
    /// comfortably inside it on a developer box and flush themselves half way through under
    /// <c>DOTNET_GCHeapHardLimit=0x18000000</c> — and a burst that flushed itself is measured as
    /// whatever was left when the drain got there.
    /// </summary>
    private static readonly int PointsPerSeries = (int)Math.Clamp(
        new MetricsOptions().EffectiveHotTierBytes * 3 / 4 / (SeriesCount * (long)MetricStorageEngine.HotPointBytes),
        20, 400);

    [Fact]
    public async Task RetainedByAnEmptyTier_IsAFractionOfThePeak()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotret-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        long loaded, drained, steady;
        long floorBefore = Live();
        try
        {
            var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            try
            {
                long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
                Feed(engine, baseNano);

                // THE BURST MUST STILL BE IN THE TIER. Above the automatic threshold Ingest
                // schedules its own flush, which drains an unpredictable share of the burst
                // before this line runs — and the probe then reports the remainder as the cost
                // of the whole burst. Seen as 7 MB for the burst on one run and 28 MB on
                // the next, entirely according to how far that flush had got.
                Assert.Equal(SeriesCount * PointsPerSeries, engine.HotPointCount);
                loaded = Live();

                // The seam takes the same path the byte threshold does, with the burst this
                // probe can afford to build.
                await engine.ScheduleThresholdFlushForTest();
                Assert.Equal(0, engine.HotPointCount);

                drained = Live();

                // One more point per series: the steady state a live deployment is actually in,
                // where every series is still named but holds almost nothing.
                Feed(engine, baseNano + 10_000_000_000L, pointsPerSeries: 1);
                steady = Live();
            }
            finally { await engine.DisposeAsync(); }
        }
        finally { try { Directory.Delete(dir, true); } catch { } }

        // THE FLOOR IS MEASURED ON BOTH SIDES AND THE LOWER READING WINS. This suite runs its
        // classes back to back, and the classes before this one leave megabytes that are dead
        // but not yet collected — a floor taken only before the burst reads those as this
        // engine's baseline and then watches them vanish mid-experiment, which turned a 28 MB
        // tier into a 7 MB one and the figure into nonsense. Nothing ELSE in the process is
        // allocating while this runs, so the contamination can only shrink: the smaller of two
        // readings around the experiment is the true floor, and taking the second one after the
        // engine is disposed is what makes it comparable.
        long floorAfter = Live();
        long empty      = Math.Min(floorBefore, floorAfter);

        int points = SeriesCount * PointsPerSeries;
        _out.WriteLine($"{SeriesCount:N0} series x {PointsPerSeries} points = {points:N0} points, fed in {ChunkPoints:N0}-point chunks");
        _out.WriteLine($"  heap floor (before / after): {floorBefore / 1048576.0,7:N1} / {floorAfter / 1048576.0:N1} MB");
        _out.WriteLine($"  heap with the burst in tier: {loaded  / 1048576.0,7:N1} MB  (+{(loaded - empty) / 1048576.0:N1})  = {(loaded - empty) / (double)points,6:N0} B/point");
        _out.WriteLine($"  heap AFTER the flush drain : {drained / 1048576.0,7:N1} MB  (+{(drained - empty) / 1048576.0:N1} still held) = {(drained - empty) / (double)SeriesCount,6:N0} B/series");
        _out.WriteLine($"  heap steady state          : {steady  / 1048576.0,7:N1} MB");
        _out.WriteLine($"  survived the drain         : {100.0 * (drained - empty) / (loaded - empty),6:N1} %");
        _out.WriteLine($"  released by the drain      : {(loaded - drained) / 1048576.0,7:N1} MB against {points * 40L / 1048576.0:N1} MB of point structs");

        // THE FLOOR-FREE HALF, and the sharper of the two. A drain must hand back at least the
        // points it drained — 40 B a MetricDataPoint, before the list slack that is the actual
        // subject here. It needs no baseline at all, so nothing another test class left behind
        // can move it: with the arrays retained the heap after the drain was HIGHER than with the
        // burst in it (measured: -1.4 MB "released"), because the snapshot's copy and the
        // series' own array were live at once and only the copy went away.
        Assert.True(loaded - drained > points * 32L,
            $"the drain released {(loaded - drained) / 1048576.0:N1} MB of a {points * 40L / 1048576.0:N1} MB "
          + "burst — the series are still holding their point arrays");

        // The round's bound: what an empty tier still holds must be under a quarter of the peak.
        //
        // Read against a floor, so it carries what a floor carries. A flush leaves rented buffers
        // in ArrayPool.Shared, LZ4 and msgpack scratch, and newly JIT'd code — measured at 1.4 to
        // 3.8 MB, none of it tier memory, all of it inside this figure. That is a FIXED cost, so
        // it is allowed for as one: without the allowance the same healthy engine reads 12 % with
        // a 21 MB budget and 27 % with the 11 MB budget a 384 MB heap limit derives, purely
        // because the burst it is compared against got smaller. Additive and named, so it cannot
        // absorb a proportional regression: before the change this figure was 22.4 MB against an
        // allowance-inclusive bound of 9.3 MB.
        const long poolAndJitAllowance = 4L * 1024 * 1024;
        Assert.True(drained - empty < (loaded - empty) / 4 + poolAndJitAllowance,
            $"an empty hot tier still holds {(drained - empty) / (double)SeriesCount:N0} B per series — "
          + $"{100.0 * (drained - empty) / (loaded - empty):N1} % of the burst's heap survived the drain");
    }

    /// <summary>
    /// The second half of the leak: a series that stops reporting is never unnamed. The sweep
    /// runs inside the drain's own write lock, past twice <c>MaxHotAge</c>, and only over series
    /// the drain left empty — so a series still reporting at any cadence the tier is built for
    /// can never be a candidate.
    /// </summary>
    [Fact]
    public async Task A_series_that_stopped_reporting_leaves_the_tier_at_the_next_flush()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotsweep-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var clock = new MetricTestClock();
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, timeProvider: clock);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            engine.Ingest([Point("quiet", baseNano), Point("chatty", baseNano)]);
            Assert.Equal(2, engine.HotSeriesCount);

            await engine.ScheduleThresholdFlushForTest();          // both drained, neither stale yet
            Assert.Equal(2, engine.HotSeriesCount);
            Assert.Equal(0, engine.StaleSeriesEvicted);

            // Three hours on: "chatty" is still reporting, "quiet" has said nothing since.
            clock.Advance(TimeSpan.FromHours(3));
            engine.Ingest([Point("chatty", baseNano + 1_000_000L)]);
            await engine.ScheduleThresholdFlushForTest();

            Assert.Equal(1, engine.StaleSeriesEvicted);
            Assert.Equal(1, engine.HotSeriesCount);

            // The catalog is fed from _meta, which the sweep does not touch: an evicted series'
            // metric is still discoverable, and its cold data is untouched on disk.
            Assert.Contains("hot.sweep.metric", engine.GetMetricNames());
            Assert.Contains(engine.GetLabelValues("hot.sweep.metric", "series"), v => v == "quiet");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// <c>Drain</c> hands the flush its list instead of copying it, so the restore path on a
    /// failed write appends into the series' NEW list while reading the drained one. Two
    /// different lists: if they were ever the same object this either duplicates every point or
    /// never terminates.
    /// </summary>
    [Fact]
    public async Task A_failed_write_restores_every_point_exactly_once()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotrestore-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            var batch = new MetricIngestItem[1_000];
            for (int i = 0; i < batch.Length; i++)
                batch[i] = Point("s" + (i % 10), baseNano + i * 1_000_000L, i);
            engine.Ingest(batch);

            long bytesBefore = engine.HotByteCount;
            engine.OnSnapshotTakenForTest = static () => throw new IOException("the disk filled");
            await engine.ScheduleThresholdFlushForTest();
            engine.OnSnapshotTakenForTest = null;

            Assert.Equal(1_000, engine.HotPointCount);
            Assert.Equal(bytesBefore, engine.HotByteCount);

            var seen = new List<double>(1_000);
            await foreach (var s in engine.QueryAsync("hot.sweep.metric"))
                foreach (var p in s.Points) seen.Add(p.Value);

            seen.Sort();
            Assert.Equal(1_000, seen.Count);
            for (int i = 0; i < seen.Count; i++) Assert.Equal(i, seen[i]);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    private static MetricIngestItem Point(string series, long nano, double value = 1.0) => new()
    {
        Name              = "hot.sweep.metric",
        Kind              = MetricKind.Gauge,
        Labels            = new LabelSet(new Dictionary<string, string> { ["series"] = series }),
        TimestampUnixNano = nano,
        ScalarValue       = value,
    };

    private static void Feed(MetricStorageEngine engine, long baseNano, int pointsPerSeries = 0)
    {
        if (pointsPerSeries <= 0) pointsPerSeries = PointsPerSeries;

        var chunk = new List<MetricIngestItem>(ChunkPoints);
        for (int p = 0; p < pointsPerSeries; p++)
        {
            for (int s = 0; s < SeriesCount; s++)
            {
                chunk.Add(new MetricIngestItem
                {
                    Name              = "http.server.request.duration",
                    Kind              = MetricKind.Gauge,
                    Unit              = "ms",
                    Labels            = Labels(s),
                    TimestampUnixNano = baseNano + p * 15_000_000_000L,
                    ScalarValue       = p,
                });
                if (chunk.Count < ChunkPoints) continue;
                Flush(engine, chunk);
            }
        }
        if (chunk.Count > 0) Flush(engine, chunk);

        static void Flush(MetricStorageEngine engine, List<MetricIngestItem> chunk)
        {
            var arr = chunk.ToArray();
            chunk.Clear();
            engine.Ingest(arr);
        }
    }

    /// <summary>Five labels, the shape a real HTTP server metric carries.</summary>
    private static LabelSet Labels(int series) => new(new Dictionary<string, string>
    {
        ["service.name"]                = "checkout",
        ["http.route"]                  = "/api/v1/resource/" + series,
        ["http.request.method"]         = (series & 1) == 0 ? "GET" : "POST",
        ["http.response.status_code"]   = (series % 5) == 0 ? "500" : "200",
        ["server.address"]              = "host-" + (series & 7),
    });

    /// <summary>
    /// The live heap, settled.
    ///
    /// <para>Three rounds and not one: this suite runs its classes one after another (see
    /// <c>AssemblyInfo.cs</c>) and the metric classes before this one leave finalizable state
    /// behind — mapped logs, file streams, lock objects. A single collect QUEUES those
    /// finalizers; the memory they hold is only released by the collect AFTER they have run. A
    /// baseline taken before that second collect reads ~17 MB of other tests' corpses as this
    /// engine's floor, and then watches them disappear mid-measurement — which is not noise
    /// around the figure, it is a figure of the wrong sign.</para>
    /// </summary>
    private static long Live()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
        }
        return GC.GetTotalMemory(forceFullCollection: true);
    }
}
