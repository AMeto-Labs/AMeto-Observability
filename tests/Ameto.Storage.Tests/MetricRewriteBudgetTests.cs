using System.Globalization;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A METRIC REWRITE IS BOUNDED BY BYTES, CLIPPED AT THE RETENTION CUTOFF, AND TAKES ITS TURN WITH
/// THE TRACE COMPACTION PASS (#125).
///
/// <para>A chunk of a compaction, merge or rollup used to end at 512 series and at nothing else, so
/// it weighed whatever those series' histories weighed: on the 512 MB stand one FiveMin chunk of a
/// histogram reached ~200 MB at peak and every pass ended in an OutOfMemoryException. These tests take
/// the rule a piece at a time — the plan, the time slices for one series too heavy alone, the cutoff,
/// and the gate — each failing if its part is taken out. What the rule buys under the stand's real
/// limits is <see cref="MetricRewriteUnderStandLimitsTests"/>.</para>
/// </summary>
public sealed class MetricRewriteBudgetTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrwbudget-" + Guid.NewGuid().ToString("N"));

    public MetricRewriteBudgetTests() => Directory.CreateDirectory(_dir);

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long T0   = 1_784_800_020_000_000_000L;
    private const long S    = 1_000_000_000L;
    private const long Min5 = 300 * S;
    private const long Hour = 3_600 * S;
    private const long Day  = 24 * Hour;

    private static long Now() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

    private string Sub(string name)
    {
        string d = Path.Combine(_dir, name);
        Directory.CreateDirectory(d);
        return d;
    }

    private static async Task<MetricStorageEngine> Engine(string dir, MetricsOptions? options = null, BackgroundRewriteGate? gate = null)
    {
        var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, options, rewriteGate: gate);
        await engine.ColdLoadCompleted;
        return engine;
    }

    /// <summary>Gauge series, one file per entry of <paramref name="pointsPerFile"/>, a point every <paramref name="step"/>.</summary>
    private static List<MetricSegmentInfo> Gauges(string dir, string metric, MetricGranularity granularity, int series,
                                                  long start, long step, int[] pointsPerFile)
    {
        var files = new List<MetricSegmentInfo>();
        long g = 0;
        foreach (int pf in pointsPerFile)
        {
            var items = new List<(SeriesKey, HotSeries)>(series);
            for (int s = 0; s < series; s++)
            {
                var pts = new List<MetricDataPoint>(pf);
                for (int p = 0; p < pf; p++)
                    pts.Add(new MetricDataPoint { TimestampUnixNano = start + (g + p) * step, Value = s * 10_000 + g + p });
                items.Add((new SeriesKey(metric, MetricKind.Gauge, "1",
                                         new LabelSet([new("replica", s.ToString(CultureInfo.InvariantCulture))])),
                           new HotSeries(pts)));
            }
            files.AddRange(MetricWriter.Write(dir, items, granularity));
            g += pf;
        }
        return files;
    }

    // ── The plan ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_metric_heavier_than_the_budget_is_rewritten_in_chunks_each_planned_within_it()
    {
        const long budget = 2_000_000;
        var written = new Dictionary<string, MetricHistogramShapes.SeriesDigest>(StringComparer.Ordinal);
        var sources = MetricHistogramShapes.Write(Sub("src"), MetricGranularity.FiveMin, series: 512, T0, Min5,
                                                  [60, 60], activeFraction: 1.0, seed: 11, written);

        await using var engine = await Engine(Sub("eng"), new MetricsOptions { RewriteBudgetBytes = budget });
        var chunks = new List<(int Off, int End, long Weight)>();
        engine.OnRewriteChunkPlannedForTest = (off, end, weight) => chunks.Add((off, end, weight));

        var outputs = engine.RewriteMetricInChunks(sources, MetricGranularity.FiveMin,
                                                   static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts));

        Assert.True(chunks.Count > 1, $"512 series of ~50 KB each in {chunks.Count} chunk(s) of a 2 MB budget");
        Assert.Equal(0, chunks[0].Off);
        for (int i = 1; i < chunks.Count; i++) Assert.Equal(chunks[i - 1].End, chunks[i].Off);
        Assert.Equal(512, chunks[^1].End);
        Assert.All(chunks, c => Assert.True(c.End - c.Off == 1 || c.Weight <= budget,
                                            $"keys [{c.Off}, {c.End}) planned at {c.Weight:N0} B against {budget:N0}"));

        // More, smaller files — the same points.
        Assert.Equal(chunks.Count, outputs.Count);
        MetricHistogramShapes.AssertSame(written, MetricHistogramShapes.Digests(outputs.Select(o => o.FilePath)));
    }

    // ── One series heavier than a chunk ───────────────────────────────────────

    [Fact]
    public async Task A_series_heavier_than_the_budget_is_rewritten_in_hour_aligned_time_slices()
    {
        var written = new Dictionary<string, MetricHistogramShapes.SeriesDigest>(StringComparer.Ordinal);
        var sources = MetricHistogramShapes.Write(Sub("src"), MetricGranularity.OneHour, series: 1, T0 / Hour * Hour, Hour,
                                                  [1000, 1000], activeFraction: 1.0, seed: 12, written);

        await using var engine = await Engine(Sub("eng"), new MetricsOptions { RewriteBudgetBytes = 64 * 1024 });
        int slices = 0;
        engine.OnRewriteChunkWrittenForTest = _ => slices++;

        var outputs = engine.RewriteMetricInChunks(sources, MetricGranularity.OneHour,
                                                   static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts));

        Assert.True(outputs.Count > 1, "a series ~13x the budget was rewritten in one piece");
        Assert.Equal(outputs.Count, slices);

        // The slices are the series in time order, hour-aligned, disjoint; together, every point once.
        var ordered = outputs.OrderBy(o => o.MinNano).ToList();
        for (int i = 1; i < ordered.Count; i++)
        {
            Assert.True(ordered[i].MinNano > ordered[i - 1].MaxNano, "two slices overlap in time");
            Assert.True(ordered[i].MinNano / Hour > ordered[i - 1].MaxNano / Hour, "two slices share an hour");
        }
        MetricHistogramShapes.AssertSame(written, MetricHistogramShapes.Digests(outputs.Select(o => o.FilePath)));
    }

    // ── The retention cutoff ──────────────────────────────────────────────────

    [Theory]
    [InlineData(1)]     // one pass: the headers show it fits one chunk
    [InlineData(600)]   // planned: more series than one chunk holds
    public async Task A_rewrite_leaves_out_what_retention_has_expired_once_a_retention_run_has_named_the_TTL(int series)
    {
        const string metric = "clip.raw";
        long now  = Now() / Hour * Hour;
        int  hrs  = 9 * 24 - 2;   // hourly points from 9 days ago to 2 hours ago: a Raw file the rollup takes

        // Without a retention run nothing is known to be expired, and nothing is left out.
        string plain = Sub("plain");
        Gauges(plain, metric, MetricGranularity.Raw, series, now - 9 * Day, Hour, [hrs]);
        await using (var engine = await Engine(plain))
        {
            await engine.PerformRollupForTest();
            var rolled = engine.ColdSegmentsForTest.Where(s => s.Granularity == MetricGranularity.FiveMin).ToList();
            Assert.NotEmpty(rolled);
            Assert.True(rolled.Min(s => s.MinNano) < now - 8 * Day, "the rollup lost points no retention run had expired");
        }

        // After one, a rewrite leaves out what is older than its TTL.
        string clipped = Sub("clipped");
        Gauges(clipped, metric, MetricGranularity.Raw, series, now - 9 * Day, Hour, [hrs]);
        await using (var engine = await Engine(clipped))
        {
            Assert.Equal(0, await engine.PruneAsync(TimeSpan.FromDays(7)));   // the file itself is fresh: nothing pruned
            await engine.PerformRollupForTest();

            var rolled = engine.ColdSegmentsForTest.Where(s => s.Granularity == MetricGranularity.FiveMin).ToList();
            Assert.NotEmpty(rolled);
            Assert.True(rolled.Min(s => s.MinNano) >= now - 7 * Day - Hour, "a point past the 7-day TTL was rewritten");

            // Every series is still there, with every point the TTL keeps.
            var points = rolled.SelectMany(s => MetricReader.ReadAllSync(s.FilePath)).GroupBy(s => s.Labels).ToList();
            Assert.Equal(series, points.Count);
            Assert.All(points, g => Assert.InRange(g.Sum(s => s.Points.Count), 7 * 24 - 3, 7 * 24));
        }
    }

    /// <summary>
    /// The cutoff is now − TTL, and nothing at all once the TTL reaches back past the Unix epoch
    /// (#125 review F2): computed in DateTimeOffset and multiplied out, a TTL past year 1 threw and a
    /// TTL of some centuries wrapped into a cutoff in the future.
    /// </summary>
    [Fact]
    public void The_cutoff_is_now_less_the_TTL_and_nothing_past_the_epoch()
    {
        var now = DateTimeOffset.UtcNow;
        Assert.Equal((now.ToUnixTimeMilliseconds() - 30L * 86_400_000) * 1_000_000L,
                     MetricStorageEngine.RetentionCutoffNano(now, TimeSpan.FromDays(30)));
        Assert.Equal(long.MinValue, MetricStorageEngine.RetentionCutoffNano(now, TimeSpan.FromDays(200_000)));
        Assert.Equal(long.MinValue, MetricStorageEngine.RetentionCutoffNano(now, TimeSpan.FromDays(800_000)));
        Assert.Equal(long.MinValue, MetricStorageEngine.RetentionCutoffNano(now, TimeSpan.MaxValue));
    }

    /// <summary>
    /// A retention of centuries — "200000 days" typed for "forever" — keeps everything: the prune
    /// deletes nothing and a rewrite clips nothing. It used to wrap into a cutoff in the future and
    /// delete every metric file (200 000 days), or throw in the prune and then in every rewrite, which
    /// all failed and backed off to 6 hours, so nothing was ever rolled up again (800 000 days).
    /// </summary>
    [Theory]
    [InlineData(200_000)]
    [InlineData(800_000)]
    public async Task A_retention_reaching_back_past_the_epoch_deletes_nothing_and_clips_nothing(int days)
    {
        const string metric = "forever.raw";
        long now = Now() / Hour * Hour;
        string dir = Sub("forever");
        Gauges(dir, metric, MetricGranularity.Raw, 3, now - 9 * Day, Hour, [9 * 24 - 2]);
        await using var engine = await Engine(dir);

        Assert.Equal(0, await engine.PruneAsync(TimeSpan.FromDays(days)));
        Assert.Single(engine.ColdSegmentsForTest.Where(s => s.Granularity == MetricGranularity.Raw));

        await engine.PerformRollupForTest();
        var rolled = engine.ColdSegmentsForTest.Where(s => s.Granularity == MetricGranularity.FiveMin).ToList();
        Assert.NotEmpty(rolled);
        Assert.True(rolled.Min(s => s.MinNano) < now - 8 * Day, "the rollup clipped points a retention of centuries keeps");
    }

    // ── The gate ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Holds a gate on a thread of its own until released: <see cref="Lock"/> is thread-affine, so an
    /// async test cannot hold it across an await.
    /// </summary>
    private sealed class GateHolder : IDisposable
    {
        private readonly ManualResetEventSlim _release = new();
        private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Thread _thread;

        public GateHolder(BackgroundRewriteGate gate)
        {
            _thread = new Thread(() =>
            {
                using (gate.Enter())
                {
                    _held.SetResult();
                    _release.Wait();
                }
            }) { IsBackground = true };
            _thread.Start();
        }

        public Task Held => _held.Task;

        public void Release() => _release.Set();

        public void Dispose()
        {
            _release.Set();
            _thread.Join();
            _release.Dispose();
        }
    }

    /// <summary>
    /// Neither a chunk nor the planning walk before it runs while the trace side holds the gate: the
    /// walk inflates every source's whole block (#125 review F4), so a plan cut while the gate was
    /// held means a source was read beside a trace pass.
    /// </summary>
    [Theory]
    [InlineData("one pass")]       // 10 series: the headers show it fits one chunk
    [InlineData("planned")]        // 600 series: more than one chunk holds
    [InlineData("time slices")]    // one series ~13x a 64 KB budget
    public async Task A_metric_rewrite_waits_its_turn_while_another_rewrite_holds_the_gate(string shape)
    {
        var gate    = new BackgroundRewriteGate();
        var sources = shape switch
        {
            "one pass" => Gauges(Sub("src"), "gate.gauge", MetricGranularity.Raw, 10, T0, 15 * S, [4, 4]),
            "planned"  => Gauges(Sub("src"), "gate.gauge", MetricGranularity.Raw, 600, T0, 15 * S, [4, 4]),
            _          => MetricHistogramShapes.Write(Sub("src"), MetricGranularity.Raw, series: 1, T0 / Hour * Hour, Hour,
                                                      [1000, 1000], activeFraction: 1.0, seed: 13),
        };
        var options = shape == "time slices" ? new MetricsOptions { RewriteBudgetBytes = 64 * 1024 } : null;
        await using var engine = await Engine(Sub("eng"), options, gate);
        int chunks = 0, planned = 0;
        engine.OnRewriteChunkWrittenForTest = _ => Interlocked.Increment(ref chunks);
        engine.OnRewriteChunkPlannedForTest = (_, _, _) => Interlocked.Increment(ref planned);

        Task<List<MetricSegmentInfo>> rewrite;
        using (var holder = new GateHolder(gate))
        {
            await holder.Held;
            rewrite = Task.Run(() => engine.RewriteMetricInChunks(sources, MetricGranularity.Raw,
                                                                 static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts)));
            await Task.WhenAny(rewrite, Task.Delay(500));
            Assert.False(rewrite.IsCompleted, "a rewrite chunk ran while another rewrite held the gate");
            Assert.Equal(0, Volatile.Read(ref chunks));
            Assert.Equal(0, Volatile.Read(ref planned));                    // and no source was walked to plan one
        }

        var outputs = await rewrite.WaitAsync(TimeSpan.FromSeconds(60));
        Assert.NotEmpty(outputs);
        Assert.True(Volatile.Read(ref chunks) >= 1);
        Assert.Equal(shape != "one pass", Volatile.Read(ref planned) >= 1);
    }

    [Fact]
    public async Task A_trace_compaction_pass_waits_its_turn_while_a_metric_chunk_holds_the_gate()
    {
        var gate = new BackgroundRewriteGate();
        using var traces = new TraceStorageEngine(Sub("traces"), NullLogger<TraceStorageEngine>.Instance,
                                                  writeSegmentFormatV4: false, indexEnabled: true, options: null, pools: null,
                                                  rewriteGate: gate);
        ulong span = 1;
        for (int s = 0; s < 3; s++)
        {
            for (int t = 0; t < 500; t++)
                traces.WriteSpan(new SpanIngestItem
                {
                    TraceId = new TraceId((ulong)(s * 100_000 + t), (ulong)t),
                    SpanId = new SpanId(span++), ParentSpanId = default,
                    StartTimeUnixNano = T0 + (s * 100_000 + t) * 1_000_000L, DurationNanos = 2_000_000,
                    Name = "GET /orders", ServiceName = "billing",
                    Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
                });
            traces.FlushHotTier();
        }
        Assert.Equal(3, traces.ColdSegmentCountForTest);

        Task compaction;
        using (var holder = new GateHolder(gate))
        {
            await holder.Held;
            compaction = Task.Run(traces.CompactSmallSegments);
            await Task.WhenAny(compaction, Task.Delay(500));
            Assert.False(compaction.IsCompleted, "a trace compaction pass ran while a metric chunk held the gate");
            Assert.Equal(3, traces.ColdSegmentCountForTest);
        }

        await compaction.WaitAsync(TimeSpan.FromSeconds(60));
        Assert.Equal(1, traces.ColdSegmentCountForTest);
    }
}
