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
        var w = await WeighAsync("ameto-mhotret-", BurstDrainAndDisposeAsync);

        long loaded   = w.Alive[0];
        long drained  = w.Alive[1];
        long held     = w.HeldMin(1);          // what a drained tier and its engine still hold
        long released = loaded - drained;      // what the drain gave back
        long peak     = held + released;       // what the burst weighed, with no floor in it
        int  points   = SeriesCount * PointsPerSeries;

        _out.WriteLine($"{SeriesCount:N0} series x {PointsPerSeries} points = {points:N0} points, fed in {ChunkPoints:N0}-point chunks");
        _out.WriteLine($"  heap with the burst in tier: {loaded  / 1048576.0,7:N1} MB");
        _out.WriteLine($"  heap AFTER the flush drain : {drained / 1048576.0,7:N1} MB");
        _out.WriteLine($"  baselines (before / gone)  : {w.Before / 1048576.0,7:N1} / {w.Gone / 1048576.0:N1} MB");
        _out.WriteLine($"  the burst weighed          : {peak    / 1048576.0,7:N1} MB  = {peak / (double)points,6:N0} B/point");
        _out.WriteLine($"  still held by an empty tier: {held    / 1048576.0,7:N1} MB  = {held / (double)SeriesCount,6:N0} B/series "
                     + $"(bracket {w.HeldMin(1) / 1048576.0:N1} .. {w.HeldMax(1) / 1048576.0:N1} MB)");
        _out.WriteLine($"  survived the drain         : {100.0 * held / peak,6:N1} %");
        _out.WriteLine($"  released by the drain      : {released / 1048576.0,7:N1} MB against {points * 40L / 1048576.0:N1} MB of point structs");

        // FLOOR-FREE OUTRIGHT, AND THE SHARPER OF THE TWO. Both readings are of the same process
        // seconds apart with the same engine in them, so no baseline appears in this figure at
        // all. A drain must hand back at least the points it drained — 40 B a MetricDataPoint,
        // before the list slack that is the actual subject here. With the arrays retained the
        // heap after the drain was HIGHER than with the burst in it (measured on the revert:
        // -0.8 MB "released"), because the snapshot's copy and the series' own array were live at
        // once and only the copy went away.
        Assert.True(released > points * 32L,
            $"the drain released {released / 1048576.0:N1} MB of a {points * 40L / 1048576.0:N1} MB "
          + "burst — the series are still holding their point arrays");

        // The round's bound: what an empty tier still holds must be under a quarter of the peak.
        //
        // NO FLOOR AND NO ALLOWANCE, WHICH IS THE WHOLE POINT. `peak` is not read against a
        // baseline taken outside the experiment: it is `held + released`. A flush leaves rented
        // buffers in ArrayPool.Shared, LZ4 and msgpack scratch and newly JIT'd code — 1.4 to
        // 3.8 MB, none of it tier memory — and every byte of it is in `drained` and in the
        // baselines alike, so it cancels instead of needing the 4 MB allowance this bound used to
        // carry. `held` is the LOW end of the bracket, so a host that moves under the measurement
        // can only make this easier; on the revert it reads 20.7 MB against a limit of -0.3 MB.
        //
        // held < peak / 4  is  3 x held < released, with peak eliminated.
        Assert.True(held * 3 < released,
            $"an empty hot tier still holds {held / (double)SeriesCount:N0} B per series — "
          + $"{100.0 * held / peak:N1} % of the burst's heap survived the drain");
    }

    /// <summary>
    /// THE BURST, THE DRAIN AND THE DISPOSAL, IN A FRAME OF THEIR OWN.
    ///
    /// <para><b>Why the engine is weighed instead of a floor.</b> This fact used to be read
    /// against <c>min(floor before, floor after)</c>, on the argument that nothing else in the
    /// process allocates while it runs and that the contamination left by earlier classes can
    /// therefore only shrink. The first half is true; the second is the trap. The contamination
    /// does shrink — on a <b>wall clock</b>, and not on collections. Measured in this suite with
    /// two cores: five back-to-back <c>Live()</c> calls all read 27.2 MB, then 12.4 MB twelve
    /// seconds later and 11.0 MB at thirty-two, with no allocation in between and nothing that
    /// renting <c>ArrayPool&lt;byte|int|long|double&gt;.Shared</c> dry could bring forward. So
    /// <c>floorAfter</c> could land on the far side of a ~14 MB step that <c>loaded</c> and
    /// <c>drained</c> were both read on the near side of, and <c>min</c> then subtracted 14 MB
    /// too little from each. A common error of C inflates the left of
    /// <c>held &lt; peak/4 + 4 MB</c> by C and the right by only C/4, which is exactly how the
    /// CI runner read 8 977 B/series and 49.5 % where the same healthy engine is 1 677 B and
    /// 13 %: its own printout has the step in it (floor 30.5 before, 16.5 after; steady state
    /// 18.3 MB, BELOW the floor it was measured against).</para>
    ///
    /// <para>So there is no floor any more. Everything is a difference between two readings
    /// seconds apart inside one run, and the baseline is the engine's own disappearance — which
    /// is why the engine is created, drained and disposed HERE: a Debug build keeps every local
    /// rooted to the end of the method that declares it, so an engine named by the frame that
    /// weighs it cannot be weighed at all. <see cref="WeighAsync"/> is what proves the two
    /// readings are comparable.</para>
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static async Task<WeighedRun> BurstDrainAndDisposeAsync(string dir)
    {
        MetricStorageEngine? engine = new(dir, NullLogger<MetricStorageEngine>.Instance);
        var  tracker = new WeakReference(engine);
        long loaded, drained;
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
            QuiesceForWeighing(engine);
            loaded = Live();

            // The seam takes the same path the byte threshold does, with the burst this
            // probe can afford to build, and the task it hands back completes only once
            // FlushHotTierAsync has returned — past the write, the commit and the release of
            // the flush gate.
            await engine.ScheduleThresholdFlushForTest();

            // AND THE STATE THE NEXT READING DEPENDS ON IS ASSERTED, NOT ASSUMED. A snapshot
            // still in flight holds every drained point list and the writer's buffers, and a
            // reading taken beside one measures the flush rather than the tier.
            QuiesceForWeighing(engine);
            Assert.Equal(0, engine.HotPointCount);
            Assert.Equal(SeriesCount, engine.HotSeriesCount);

            drained = Live();
        }
        finally { await engine.DisposeAsync(); }

        // The state machine's own field, cleared before this method's task completes: the caller
        // weighs the heap the instant it gets control back, and in Debug that field is the one
        // reference that would keep the engine alive across the reading.
        engine = null;
        return new WeighedRun([loaded, drained], tracker);
    }

    /// <summary>
    /// No flush is inside <c>FlushHotTierAsync</c>, so nothing but the tier itself is on the
    /// scale. Only the THRESHOLD path can be there during these facts: the periodic loop's first
    /// tick is a real 60-second <c>FlushCheckInterval</c> away on the engines that run on the
    /// system clock, and the ones that run on <see cref="MetricTestClock"/> are only ticked by
    /// the test that owns them.
    /// </summary>
    private static void QuiesceForWeighing(MetricStorageEngine engine)
        => Assert.Equal(0, engine.RunningThresholdFlushes);

    /// <summary>
    /// What one run of a weighing hands back: the readings it took with the engine alive, and a
    /// weak handle on the engine itself so the caller can establish that it really went.
    /// </summary>
    private readonly record struct WeighedRun(long[] Alive, WeakReference Engine);

    /// <summary>
    /// The readings one weighing is made of: the heap before the engine existed, whatever the run
    /// itself measured with the engine alive, and the heap once the engine is gone.
    ///
    /// <para><b>Two baselines, and the answer is a bracket rather than a number.</b> What the
    /// engine held at reading <c>i</c> is <c>Alive[i] - H</c>, where H is what this process would
    /// have weighed at that instant with no engine in it — and H is not observable, only
    /// straddled: <see cref="Before"/> is H a few seconds early and <see cref="Gone"/> is H a few
    /// seconds late. Neither is H, because the rest of the process does not hold still. It drops
    /// memory on a <b>wall clock</b> and not on a collection (five consecutive <see cref="Live"/>
    /// calls in this suite all read 27.2 MB and the sixth, twelve seconds later, read 12.4 MB),
    /// and a run that flushes ADDS to it by leaving rented buffers in <c>ArrayPool.Shared</c>. So
    /// H can move either way and the honest statement is
    /// <c>Min &lt;= held &lt;= Max</c>.</para>
    ///
    /// <para><b>Every bound is then asserted on the end of the bracket that cannot invent a
    /// failure.</b> An upper bound ("the tier holds less than this") is asserted on
    /// <see cref="HeldMin"/>, a lower bound on <see cref="HeldMax"/>. A host that moves under the
    /// measurement can then only make a bound easier, never harder, so the fact cannot fail for a
    /// reason it did not measure — which is exactly how the old <c>min(floor before, floor
    /// after)</c> read 8 977 B/series on the CI runner against a true 1 677 B: that formula used
    /// the LATE baseline for everything, so a 14 MB step landed whole in the figure, inflating
    /// the left of <c>held &lt; peak/4 + 4 MB</c> by 14 MB and the right by 3.5 MB. The spread is
    /// printed with each figure, so a wide bracket is visible rather than silent.</para>
    /// </summary>
    private readonly record struct Weighing(long Before, long[] Alive, long Gone)
    {
        /// <summary>How far the heap outside the engine moved while the run was in progress.</summary>
        public long Drift => Before - Gone;

        /// <summary>The least the engine can have held at reading <paramref name="i"/>.</summary>
        public long HeldMin(int i = 0) => Alive[i] - Math.Max(Before, Gone);

        /// <summary>The most it can have held.</summary>
        public long HeldMax(int i = 0) => Alive[i] - Math.Min(Before, Gone);
    }

    /// <summary>
    /// TAKES THE TWO BASELINES AROUND A RUN, AND ESTABLISHES THAT THE ENGINE REALLY WENT.
    ///
    /// <para>The bracket <see cref="Weighing"/> describes is only a bracket on what the ENGINE
    /// held if the engine was actually collected by the second reading, and that is not a thing
    /// to take on trust: the run is a Debug build's worth of hoisted locals, spilled arguments
    /// and async state machines around an object graph of tens of megabytes, and one surviving
    /// reference turns <c>Gone</c> into a second reading of <c>Alive</c> — seen once in ten runs
    /// as 51.4 MB of exemplar rings weighing 0 B, which the ring facts' lower bound caught and no
    /// arithmetic could have. A <see cref="WeakReference"/> settles it outright: after
    /// <see cref="Live"/>'s collections an unreachable engine is a dead handle.</para>
    ///
    /// <para><b>There is deliberately no retry.</b> A second weighing inside this same frame
    /// cannot produce one: measured, every attempt after the first found the engine still
    /// reachable, four times out of four, on two separate runs — the loop's own hoisted state is
    /// enough to keep it alive. So the run happens once, its bracket is used as it is, and the
    /// bounds are asserted on the end of it that a moving host can only loosen.</para>
    /// </summary>
    private static async Task<Weighing> WeighAsync(string dirPrefix, Func<string, Task<WeighedRun>> runOnce)
    {
        string dir = Path.Combine(Path.GetTempPath(), dirPrefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);

        Weighing w;
        bool     stillHeld;
        try
        {
            long before = Live();
            var  run    = await runOnce(dir);

            // Taken before the directory is removed: deleting a hundred .mts files and a mapped
            // log is a second of disk on a two-core host, and every one of those seconds widens
            // the bracket for no reason.
            //
            // Rounds, not one: an engine that has just been weighed is in gen2 with a graph of
            // finalizable handles under it, and the machinery that ran the await above lets go of
            // it a collection later than the collection that disposed it. Measured: the 256-ring
            // engine survives the first round and is gone by the second. The loop ends on the
            // reading that first sees a dead handle, so the baseline is always the EARLIEST
            // engine-free reading — the one with the least of the process's own decay in it.
            long gone = Live();
            for (int round = 0; round < 8 && run.Engine.IsAlive; round++) gone = Live();

            w         = new Weighing(before, run.Alive, gone);
            stillHeld = run.Engine.IsAlive;
        }
        finally { try { Directory.Delete(dir, true); } catch { } }

        Assert.False(stillHeld,
            "the disposed engine was still reachable when the baseline was read, so that reading is a "
          + "second reading of the engine rather than a baseline for it — nothing weighed here is its own");
        return w;
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
    /// THE TIER THAT CAN NEVER FLUSH AGAIN IS THE ONE THAT MOST NEEDS SWEEPING.
    ///
    /// <para>The sweep used to ride along with the drain and nowhere else, and both doors out of
    /// the flush stand before it: <c>FlushIfDueAsync</c> returns on <c>points == 0</c> and
    /// <c>FlushHotTierAsync</c> returns on <c>snapshot.Count == 0</c>. So a tier whose series
    /// have ALL stopped reporting — an exporter removed, a fleet scaled to zero — never flushed
    /// again, therefore never swept again, and kept every <c>HotSeries</c>, <c>SeriesKey</c>,
    /// <c>LabelSet</c> and dictionary node for the life of the process. Nothing but
    /// <c>Shed()</c> under RAM pressure could take them back, and pressure is the state the
    /// sweep exists to keep the process out of.</para>
    ///
    /// <para>Revert <c>FlushIfDueAsync</c>'s <c>SweepStaleSeriesIfIdle()</c> to a bare
    /// <c>return</c> and this fails with all four series still named after three hours of ticks.</para>
    /// </summary>
    [Fact]
    public async Task An_idle_tier_sheds_its_series_on_a_tick_that_has_nothing_to_flush()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotidle-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var clock = new MetricTestClock();
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, timeProvider: clock);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            engine.Ingest([Point("a", baseNano), Point("b", baseNano), Point("c", baseNano), Point("d", baseNano)]);
            await engine.ScheduleThresholdFlushForTest();
            Assert.Equal(0, engine.HotPointCount);      // the tier is drained and EMPTY from here on
            Assert.Equal(4, engine.HotSeriesCount);

            // A tick before the bar: nothing has been idle long enough, so nothing goes. This is
            // the half that says the tick sweeps on the AGE and not merely on being empty.
            clock.Advance(engine.ConfiguredOptions.MaxHotAge);
            await engine.FlushPeriodicForTest();
            Assert.Equal(4, engine.HotSeriesCount);
            Assert.Equal(0, engine.StaleSeriesEvicted);

            // Past twice MaxHotAge, with not one point ingested since the flush — the case that
            // never came back. No threshold flush is scheduled and none could be: there is
            // nothing to flush.
            clock.Advance(engine.ConfiguredOptions.MaxHotAge + TimeSpan.FromMinutes(1));
            await engine.FlushPeriodicForTest();

            Assert.Equal(4, engine.StaleSeriesEvicted);
            Assert.Equal(0, engine.HotSeriesCount);

            // The names survive the eviction: _meta feeds the catalog and the sweep does not
            // touch it, so an evicted series is still discoverable and its cold data untouched.
            Assert.Contains("hot.sweep.metric", engine.GetMetricNames());
            Assert.Contains(engine.GetLabelValues("hot.sweep.metric", "series"), v => v == "a");

            // And a series that comes back is re-created for free, at full strength.
            engine.Ingest([Point("a", baseNano + 1_000_000L)]);
            Assert.Equal(1, engine.HotSeriesCount);
            Assert.Equal(1, engine.HotPointCount);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// PRESSURE LOWERS THE BAR; IT DOES NOT REMOVE IT.
    ///
    /// <para><c>Shed()</c> evicted every series holding zero points, and the state a tier is in
    /// for most of its life is the state immediately after a flush — where that is ALL of them.
    /// One pressure tick landing there wiped a busy 30 000-series table, every live series then
    /// paid a fresh <c>RegisterMeta</c> walk on its next point, the log line called them "idle
    /// for over 2 h" and the <c>released</c> figure the pressure loop reads counted bytes that
    /// came back within the second.</para>
    ///
    /// <para>The three facts here are the rule: a series that reported inside one <c>MaxHotAge</c>
    /// survives a shed however empty it is; one past that bar goes, EARLIER than the ordinary
    /// sweep's twice-<c>MaxHotAge</c>, which is what makes a shed worth calling at all; and
    /// <c>released</c> counts what actually left.</para>
    ///
    /// <para>Revert <c>Shed</c> to <c>if (v.PointCount == 0)</c> and the first assertion fails
    /// with both series gone the moment the flush emptied them.</para>
    /// </summary>
    [Fact]
    public async Task Shedding_under_pressure_keeps_a_series_that_reported_inside_the_hot_age()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotshed-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var clock = new MetricTestClock();
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, timeProvider: clock);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
            var  hotAge   = engine.ConfiguredOptions.MaxHotAge;

            engine.Ingest([Point("quiet", baseNano), Point("chatty", baseNano)]);
            await engine.ScheduleThresholdFlushForTest();
            Assert.Equal(0, engine.HotPointCount);      // every series in the table now holds nothing
            Assert.Equal(2, engine.HotSeriesCount);

            // THE PRESSURE TICK THAT USED TO TAKE THE WHOLE TABLE. Both series are empty and both
            // reported seconds ago.
            Assert.Equal(0L, engine.Shed());
            Assert.Equal(2, engine.HotSeriesCount);
            Assert.Equal(0, engine.StaleSeriesEvicted);

            // An hour and a minute on, "chatty" reports again and "quiet" has not. Pressure sheds
            // "quiet" at MaxHotAge — sooner than the drain's own sweep would have, which is the
            // point of asking under pressure — and leaves the series that is still live.
            clock.Advance(hotAge + TimeSpan.FromMinutes(1));
            engine.Ingest([Point("chatty", baseNano + 1_000_000L)]);
            await engine.ScheduleThresholdFlushForTest();

            // The drain's own sweep does NOT take "quiet": at 1 h 1 min it is nowhere near the
            // ordinary twice-MaxHotAge bar. Everything below is pressure's doing and nothing else's.
            Assert.Equal(0, engine.StaleSeriesEvicted);
            Assert.Equal(2, engine.HotSeriesCount);

            long released = engine.Shed();

            Assert.Equal(1, engine.StaleSeriesEvicted);
            Assert.Equal(1, engine.HotSeriesCount);
            Assert.True(released > 0 && released <= 2 * 384,
                $"Shed() reported {released} B for one evicted series");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// AND THE ADVERTISEMENT IS THE SAME RULE, NOT THE OLD ONE.
    ///
    /// <para><c>ShedableBytes</c> kept offering <c>_hot.Count x 384 B</c> — the whole table —
    /// after <c>Shed</c> had stopped taking the whole table. The state that makes the difference
    /// is the one a busy tier is in for most of its life: every series drained by the last flush,
    /// every series reporting inside one <c>MaxHotAge</c>. Thirty thousand of them advertise
    /// 11 MB there and release nothing, and that figure is what
    /// <c>MemoryShedRegistry.ShedableBytes</c> sums for the pressure loop's "is it worth asking
    /// anybody" gate.</para>
    ///
    /// <para>Three readings of the same table, so the figure cannot be a constant either way:
    /// nothing past the bar advertises nothing, everything past it advertises all of it, and one
    /// series reporting again takes itself back out of the offer. The last is checked against
    /// <c>Shed</c>'s own return, which is what "what it would actually release" means.</para>
    ///
    /// <para>Revert to <c>_hot.Count * EmptySeriesBytes</c> and the first reading is 19 200 B.</para>
    /// </summary>
    [Fact]
    public async Task A_tier_whose_series_all_reported_recently_advertises_nothing_to_shed()
    {
        const int Series = 50;

        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotadv-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var clock = new MetricTestClock();
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, timeProvider: clock);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
            var  hotAge   = engine.ConfiguredOptions.MaxHotAge;

            var batch = new MetricIngestItem[Series];
            for (int i = 0; i < Series; i++) batch[i] = Point("s" + i, baseNano + i * 1_000_000L);
            engine.Ingest(batch);

            await engine.ScheduleThresholdFlushForTest();
            Assert.Equal(0, engine.HotPointCount);         // drained: no points left to offer
            Assert.Equal(Series, engine.HotSeriesCount);   // and every one of them still named

            // THE STEADY STATE. Nothing has been idle for a MaxHotAge, so a shed takes nothing —
            // and the offer says so.
            Assert.Equal(0L, engine.ShedableBytes);
            Assert.Equal(0L, engine.Shed());
            Assert.Equal(Series, engine.HotSeriesCount);

            // Past the bar, the same table IS the offer.
            clock.Advance(hotAge + TimeSpan.FromMinutes(1));
            Assert.Equal(Series * 384L, engine.ShedableBytes);

            // One series reports again and leaves the offer on its own — the points it brings are
            // shedable through the flush, which is the other half of the figure.
            engine.Ingest([Point("s0", baseNano + 1_000_000_000L)]);
            Assert.Equal((Series - 1) * 384L + engine.HotByteCount, engine.ShedableBytes);

            // And that is what a shed at this moment actually releases.
            Assert.Equal((Series - 1) * 384L, engine.Shed());
            Assert.Equal(1, engine.HotSeriesCount);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// A SWEEP THAT EVICTS COSTS NOTHING MORE THAN A SWEEP THAT DOES NOT, WITH DEBUG OFF.
    ///
    /// <para>The sweep's one log line was
    /// <c>_logger.LogDebug("… {Count} … {Hours} … {Named} …", evicted, idleFor.TotalHours, _hot.Count)</c>,
    /// which binds to <c>LogDebug(ILogger, string, params object?[])</c>: the <c>object[3]</c>
    /// and the three boxes are built at the CALL SITE, before <c>IsEnabled</c> is consulted, and
    /// Debug is off in every deployment this round exists for. The third argument was the worse
    /// half — <c>ConcurrentDictionary.Count</c> takes EVERY lock in the table, and it was asked
    /// from inside <c>_snapshotLock</c>'s write lock, the one that excludes all ingest.</para>
    ///
    /// <para>MEASURED AS A DIFFERENCE, which is what makes it a measurement of the LOGGING. Both
    /// phases run the same number of sweeps over the same table through the same enumerator; the
    /// only thing the second does that the first does not is evict a series and therefore reach
    /// the log line. Everything else — the enumerator, the clock, the counters — cancels.</para>
    ///
    /// <para>Restore the <c>LogDebug</c> call and this fails at ~120 B a sweep: an
    /// <c>object[3]</c> (48 B) and boxes for an <c>int</c>, a <c>double</c> and an <c>int</c>
    /// (24 B each).</para>
    /// </summary>
    [Fact]
    public async Task The_stale_sweep_allocates_nothing_for_its_log_line_with_debug_off()
    {
        const int Series = 200;   // one eviction per measured sweep

        string dir = Path.Combine(Path.GetTempPath(), "ameto-mhotswalloc-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var clock = new MetricTestClock();
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, timeProvider: clock);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
            var  start    = clock.GetUtcNow();

            // One series a second, so the stale bar crosses them one at a time and each measured
            // sweep evicts exactly one.
            for (int i = 0; i < Series; i++)
            {
                engine.Ingest([Point("s" + i, baseNano + i * 1_000_000L)]);
                clock.Advance(TimeSpan.FromSeconds(1));
            }
            await engine.ScheduleThresholdFlushForTest();
            Assert.Equal(Series, engine.HotSeriesCount);
            Assert.Equal(0, engine.StaleSeriesEvicted);   // none is anywhere near the bar yet

            _ = engine.SweepStaleSeriesForTest();         // JIT the whole path before either phase

            // PHASE A — the same walk, nothing past the bar, so the log line is never reached.
            long a0 = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < Series; i++)
            {
                _ = engine.SweepStaleSeriesForTest();
                clock.Advance(TimeSpan.FromTicks(1));
            }
            long quiet = GC.GetAllocatedBytesForCurrentThread() - a0;
            Assert.Equal(0, engine.StaleSeriesEvicted);

            // PHASE B — the bar is put just past the oldest series and walks forward one second a
            // sweep, so every sweep evicts exactly one and reaches the line.
            var bar = start + engine.ConfiguredOptions.MaxHotAge * 2 + TimeSpan.FromMilliseconds(500);
            clock.Advance(bar - clock.GetUtcNow());

            long a1 = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < Series; i++)
            {
                _ = engine.SweepStaleSeriesForTest();
                clock.Advance(TimeSpan.FromSeconds(1));
            }
            long evicting = GC.GetAllocatedBytesForCurrentThread() - a1;

            _out.WriteLine($"STALE SWEEP  {Series} sweeps, Debug off");
            _out.WriteLine($"  evicting nothing : {quiet,8:N0} B  ({quiet / (double)Series,6:N1} B/sweep)");
            _out.WriteLine($"  evicting one     : {evicting,8:N0} B  ({evicting / (double)Series,6:N1} B/sweep)");
            _out.WriteLine($"  the log line     : {(evicting - quiet) / (double)Series,6:N1} B/sweep");

            Assert.Equal(Series, engine.StaleSeriesEvicted);
            Assert.Equal(0, engine.HotSeriesCount);
            Assert.True(evicting - quiet < Series * 8,
                $"a sweep that evicts allocates {(evicting - quiet) / (double)Series:N1} B more than one that "
              + "does not, with Debug off — the log line is still boxing its arguments at the call site");
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

    /// <summary>
    /// WHAT AN EXEMPLAR ACTUALLY WEIGHS, WEIGHED.
    ///
    /// <para><see cref="MetricsOptions.ExemplarBytes"/> is the divisor the ring budget is spent
    /// with, so a figure that is 1.7x low makes every ring 1.7x deeper than the budget it was
    /// sized against — and a ring is the one piece of metric memory nothing prunes, ages out,
    /// accounts for or sheds. It was 120: the ring slot and the two id strings, with the
    /// <c>ExemplarSample</c> that holds them forgotten.</para>
    ///
    /// <para>The rings are filled from a handful of POINTS carrying many exemplars each, so the
    /// hot tier holds nothing worth measuring and no flush has to be provoked to get it out of
    /// the way. The ids are freshly built per exemplar, exactly as <c>OtlpMetricProtoParser</c>
    /// hex-encodes them. One label set for the whole fill, which is what a ring retains now that
    /// <c>AddExemplars</c> files the series' canonical set — the case where the INPUT carries a
    /// distinct set per point is
    /// <see cref="An_exemplar_does_not_retain_its_points_label_set"/>, and that is the fact that
    /// makes this one's divisor honest.</para>
    /// </summary>
    [Fact]
    public async Task An_exemplar_costs_what_the_ring_budget_is_divided_by()
    {
        const int rings = 256, depth = 1_000;
        const long entries = rings * (long)depth;

        var w = await WeighAsync("ameto-mexemplar-",
            static dir => FillRingsAndDisposeAsync(dir, rings, depth, flushFirst: false, distinctLabelSets: false));

        double perEntry = w.HeldMin() / (double)entries;

        _out.WriteLine($"{rings} rings x {depth} exemplars = {entries:N0} exemplars");
        _out.WriteLine($"  heap held  : {w.HeldMin() / 1048576.0,7:N1} .. {w.HeldMax() / 1048576.0:N1} MB = {perEntry,6:N0} B/exemplar");
        _out.WriteLine($"  constant   : {MetricsOptions.ExemplarBytes} B/exemplar "
                     + $"=> {entries * MetricsOptions.ExemplarBytes / 1048576.0:N1} MB budgeted");

        // WHAT THE ALLOWANCE IS NOW, AND WHAT IT IS NOT. It used to cover a floor: pool residue
        // and JIT'd code left over from whatever ran before this class, read as part of the
        // figure because the figure was a difference against a reading taken outside the
        // experiment. That floor moves on a wall clock — see
        // <see cref="BurstDrainAndDisposeAsync"/> for the measurement — and 6 MB of it was
        // allowed for on the belief that it was fixed. Nothing of the sort is in this figure any
        // more: `held` is what the ENGINE was holding, so every shared-pool byte is on both sides
        // of the subtraction. What is left to allow for is the engine's own furniture — the
        // catalog's 256 metric names, one hot series apiece, the log's managed state — which is
        // small, additive and genuinely fixed.
        const long engineBaseAllowance = 4L * 1024 * 1024;

        // Each bound on the end of the bracket that cannot invent a failure — the upper bound on
        // the low end, the lower bound on the high end. See Weighing.
        Assert.True(w.HeldMin() <= entries * MetricsOptions.ExemplarBytes + engineBaseAllowance,
            $"an exemplar retains {perEntry:N0} B against a budget divisor of "
          + $"{MetricsOptions.ExemplarBytes} B — every derived ring is deeper than its budget");

        // And not wildly pessimistic either: a divisor far above the truth wastes the ring
        // depth the Explore panel actually reads.
        Assert.True(w.HeldMax() >= entries * MetricsOptions.ExemplarBytes * 3 / 4,
            $"an exemplar retains {w.HeldMax() / (double)entries:N0} B against a divisor of {MetricsOptions.ExemplarBytes} B");
    }

    /// <summary>
    /// WHAT A RING KEEPS OF THE POINT THAT FILED IT.
    ///
    /// <para><c>OtlpMetricProtoParser.BuildLabels</c> builds a <b>fresh</b> <c>LabelSet</c> for
    /// every data point — a 32 B object, a 104 B pair array and ten strings decoded out of the
    /// protobuf, ~500 B for the five-label HTTP shape — and <c>AddExemplars</c> used to hand that
    /// instance straight to the ring. Nothing else keeps it: <c>_hot</c> keeps only the first
    /// batch's key, <c>_meta</c> keeps only the first instance of each string, and
    /// <c>MetricDataPoint</c> carries no labels. So from the second batch on the ring was the
    /// sole owner of one distinct label set per exemplar, and
    /// <see cref="MetricsOptions.ExemplarBytes"/> — the divisor the ring budget is spent with —
    /// was low by 4.1x, in the one piece of metric memory nothing prunes, ages out, sheds or
    /// counts.</para>
    ///
    /// <para>The input is deliberately the WORST honest case and the one the parser actually
    /// produces: a distinct <c>LabelSet</c> instance, with freshly allocated key AND value
    /// strings, for every single point — all of them equal in content, so all of them are the
    /// same series and one canonical set stands for the lot. The tier is drained before the
    /// weighing, so what is on the scale is the rings.</para>
    ///
    /// <para>ON REVERT (<c>Labels = labels</c> back to <c>Labels = item.Labels</c> in
    /// <c>AddExemplars</c>): 64 000 entries retain ~52 MB (860 B each) instead of 13.1 MB
    /// (215 B each), against a bound of 16.7 MB.</para>
    /// </summary>
    [Fact]
    public async Task An_exemplar_does_not_retain_its_points_label_set()
    {
        const int rings = 64, depth = 1_000;
        const long entries = rings * (long)depth;

        var w = await WeighAsync("ameto-mexlabels-",
            static dir => FillRingsAndDisposeAsync(dir, rings, depth, flushFirst: true, distinctLabelSets: true));

        double perEntry = w.HeldMin() / (double)entries;

        _out.WriteLine($"{rings} rings x {depth} exemplars = {entries:N0} exemplars, one point and one distinct LabelSet each");
        _out.WriteLine($"  heap held  : {w.HeldMin() / 1048576.0,7:N1} .. {w.HeldMax() / 1048576.0:N1} MB = {perEntry,6:N0} B/exemplar");
        _out.WriteLine($"  constant   : {MetricsOptions.ExemplarBytes} B/exemplar "
                     + $"=> {entries * MetricsOptions.ExemplarBytes / 1048576.0:N1} MB budgeted");

        // The engine's own furniture, as above — the catalog's 64 names, the 64 cold segments
        // this one's flush published, the log's managed state. The pool residue the old 6 MB
        // allowance was mostly made of is gone from the figure entirely: the flush's rented
        // buffers sit in ArrayPool.Shared, which the engine's disposal does not touch, so they
        // are in both readings. It cannot absorb the regression it is guarding either: one
        // retained label set per entry is 652 B against a 208 B divisor, so the revert reads
        // ~860 B an entry against a bound of 208 B + 4 MB / 64 000.
        const long engineBaseAllowance = 4L * 1024 * 1024;

        Assert.True(w.HeldMin() <= entries * MetricsOptions.ExemplarBytes + engineBaseAllowance,
            $"an exemplar retains {perEntry:N0} B against a budget divisor of "
          + $"{MetricsOptions.ExemplarBytes} B — the ring is still keeping its point's LabelSet");

        // The rings must really be there: 4.1x is the regression this guards and the other end of
        // the bracket is what makes a ring that dropped its entries fail rather than pass.
        Assert.True(w.HeldMax() >= entries * MetricsOptions.ExemplarBytes / 2,
            $"64 000 ring entries weigh {w.HeldMax() / (double)entries:N0} B each — the rings are not holding them");
    }

    /// <summary>
    /// Fills <paramref name="rings"/> exemplar rings and hands back the heap reading taken with
    /// the engine alive — its other half is the caller's <see cref="Live"/> once this frame, and
    /// with it the engine, is gone. Same construction and same reason as
    /// <see cref="BurstDrainAndDisposeAsync"/>: no reading here is taken against a floor, so
    /// nothing an earlier test class parked in a shared pool can move the figure.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static async Task<WeighedRun> FillRingsAndDisposeAsync(
        string dir, int rings, int depth, bool flushFirst, bool distinctLabelSets)
    {
        var options = new MetricsOptions
        {
            // Pinned: these facts are about what one entry weighs, not about cadence.
            // MetricsOptions.MaxExemplarMetricsFor sizes the ring COUNT to what half the tier
            // budget can afford at this depth, and 256 x 1 000 x 208 B = 50.8 MB needs 101.6 MB
            // of tier to be affordable.
            HotTierBytes       = distinctLabelSets ? 32_000_000 : 128_000_000,
            ExemplarsPerMetric = depth,
            MaxExemplarMetrics = rings,
        };
        Assert.Equal(rings, options.MaxExemplarMetricsFor(MemoryBudgets.Current()));

        MetricStorageEngine? engine = new(dir, NullLogger<MetricStorageEngine>.Instance, options);
        var  tracker = new WeakReference(engine);
        long alive;
        try
        {
            if (distinctLabelSets) FillRingsOnePointEach(engine, rings, depth);
            else                   FillRings(engine, rings, depth);

            if (flushFirst)
            {
                // The points go, the rings stay. 64 000 scalar points is 4 MB of tier — a third
                // of the figure being measured — and leaving them in would put the tier on the
                // scale next to the rings.
                await engine.ScheduleThresholdFlushForTest();
                QuiesceForWeighing(engine);
                Assert.Equal(0, engine.HotPointCount);

                CheckOneRingReadsBack(engine);
            }
            else QuiesceForWeighing(engine);

            alive = Live();
        }
        finally { await engine.DisposeAsync(); }

        engine = null;   // see BurstDrainAndDisposeAsync
        return new WeighedRun([alive], tracker);
    }

    /// <summary>
    /// The exemplars are really there, with the right labels: a ring that dropped them would pass
    /// the weighing trivially. Its own frame, and never inlined, for the reason
    /// <see cref="FillRings"/> gives — the snapshot this takes is a thousand ring entries, and in
    /// Debug it would otherwise still be rooted at the reading it is meant to make honest.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void CheckOneRingReadsBack(MetricStorageEngine engine)
    {
        var got = engine.GetExemplars("exemplar.labels.metric.0", null, null, null);
        Assert.NotEmpty(got);
        Assert.Equal("checkout", got[0].Labels.Pairs.Single(p => p.Key == "service.name").Value);
    }

    /// <summary>
    /// One POINT per exemplar, each point carrying its own freshly built label set — the shape
    /// the OTLP parser produces and the one the ring used to retain a copy of. Fed in chunks for
    /// the reason the class header gives.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void FillRingsOnePointEach(MetricStorageEngine engine, int rings, int depth)
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        const int chunk = 500;

        for (int r = 0; r < rings; r++)
        {
            string name = "exemplar.labels.metric." + r;
            for (int off = 0; off < depth; off += chunk)
            {
                int take  = Math.Min(chunk, depth - off);
                var items = new MetricIngestItem[take];
                for (int i = 0; i < take; i++)
                {
                    long id = r * (long)depth + off + i;
                    items[i] = new MetricIngestItem
                    {
                        Name              = name,
                        Kind              = MetricKind.Gauge,
                        Labels            = FreshHttpLabels(),
                        TimestampUnixNano = baseNano + id * 1_000L,
                        ScalarValue       = id,
                        Exemplars         =
                        [
                            new MetricExemplar
                            {
                                TimestampUnixNano = baseNano + id * 1_000L,
                                Value             = id,
                                TraceId           = id.ToString("x32"),
                                SpanId            = id.ToString("x16"),
                            },
                        ],
                    };
                }
                engine.Ingest(items);
            }
        }
    }

    /// <summary>
    /// The five-label HTTP shape, a NEW <see cref="LabelSet"/> with newly allocated key and value
    /// strings on every call and identical content every time — which is exactly what
    /// <c>OtlpMetricProtoParser.BuildLabels</c> hands each data point of one series, since every
    /// string comes off the wire through <c>ProtoReader.ReadString()</c> with no pooling or
    /// interning. <c>new string(span)</c> rather than a literal: literals are interned, and one
    /// shared instance is the very thing this must not hand the engine.
    /// </summary>
    private static LabelSet FreshHttpLabels() => new(new Dictionary<string, string>
    {
        [Fresh("service.name")]              = Fresh("checkout"),
        [Fresh("http.route")]                = Fresh("/api/v1/resource/7"),
        [Fresh("http.request.method")]       = Fresh("GET"),
        [Fresh("http.response.status_code")] = Fresh("200"),
        [Fresh("server.address")]            = Fresh("host-3"),
    });

    private static string Fresh(string s) => new(s.AsSpan());

    /// <summary>
    /// Its own frame, and never inlined: in Debug a local stays rooted to the end of the method
    /// that declares it, so a batch built in the caller would still be alive at the measurement
    /// and the exemplars' input objects would be weighed alongside the rings they were copied
    /// into.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void FillRings(MetricStorageEngine engine, int rings, int depth)
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var  labels   = new LabelSet(new Dictionary<string, string> { ["service.name"] = "checkout" });

        for (int r = 0; r < rings; r++)
        {
            var exemplars = new MetricExemplar[depth];
            for (int e = 0; e < depth; e++)
            {
                long id = r * (long)depth + e;
                exemplars[e] = new MetricExemplar
                {
                    TimestampUnixNano = baseNano + e * 1_000_000L,
                    Value             = e,
                    TraceId           = id.ToString("x32"),   // 32 hex chars, as Hex() produces
                    SpanId            = id.ToString("x16"),   // 16 hex chars
                };
            }

            engine.Ingest([new MetricIngestItem
            {
                Name              = "exemplar.metric." + r,
                Kind              = MetricKind.Gauge,
                Labels            = labels,
                TimestampUnixNano = baseNano,
                ScalarValue       = r,
                Exemplars         = exemplars,
            }]);
        }
    }

    private static MetricIngestItem Point(string series, long nano, double value = 1.0) => new()
    {
        Name              = "hot.sweep.metric",
        Kind              = MetricKind.Gauge,
        Labels            = new LabelSet(new Dictionary<string, string> { ["series"] = series }),
        TimestampUnixNano = nano,
        ScalarValue       = value,
    };

    private static void Feed(MetricStorageEngine engine, long baseNano)
    {
        var chunk = new List<MetricIngestItem>(ChunkPoints);
        for (int p = 0; p < PointsPerSeries; p++)
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
    /// finalizers; the memory they hold is only released by the collect AFTER they have run.</para>
    ///
    /// <para><b>Settled is not the same as clean, and no number of rounds makes it so.</b> Some
    /// of what an earlier class leaves behind comes back on a wall clock rather than on a
    /// collection: measured in this suite, five consecutive calls to this method all read
    /// 27.2 MB and the sixth, twelve seconds later, read 12.4 MB — with nothing allocating in
    /// between and with <c>ArrayPool&lt;byte|int|long|double&gt;.Shared</c> rented dry beforehand
    /// to no effect. That is why every figure in this class is now a difference between two
    /// readings of the same run with the same residue in both, and why none of them is read
    /// against a floor. See <see cref="BurstDrainAndDisposeAsync"/>.</para>
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
