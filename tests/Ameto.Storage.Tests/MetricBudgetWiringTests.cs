using System.Reflection;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE METRIC CEILINGS, AT THE SIZES THEY ARE ACTUALLY DEPLOYED AT.
///
/// <para>Every sizing constant in <c>Ameto.Metrics</c> was a literal — 500 000 points before a
/// forced flush, 50 000 before a periodic one, 4 000 exemplars per metric NAME with no cap on the
/// number of names, 50 000 tracked series, 2 000 label values per key, an 8 MB log — the same
/// number on a 512 MB container and a 64 GB host. Worst case at those defaults is 20 MB of gauge
/// points or <b>84 MB of histogram points</b> in the tier, inside a GC heap hard limit of 384 MB,
/// next to a 48 MB index cache and a 16 MB log tier.</para>
///
/// <para>Two properties are asserted here and they pull in opposite directions, which is why both
/// are needed: a host large enough for the caps must behave <b>exactly</b> as it always did — or
/// every existing flush test's point count silently stops meaning what it says — and the 512 MB
/// stand must get ceilings it can honour.</para>
/// </summary>
public sealed class MetricBudgetWiringTests
{
    private const long MB = 1024 * 1024;

    private readonly ITestOutputHelper _out;
    public MetricBudgetWiringTests(ITestOutputHelper o) => _out = o;

    /// <summary>The stand as a real 512 MB container presents itself: 384 MB heap limit, 512 MB physical.</summary>
    private static MemoryBudgets Stand => MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

    /// <summary>A CI runner or a developer box: the caps bind, not the fractions.</summary>
    private static MemoryBudgets Large => MemoryBudgets.Derive(16 * 1024 * MB, 16 * 1024 * MB);

    [Fact]
    public void On_a_large_host_every_metric_default_is_what_it_has_always_been()
    {
        var o = new MetricsOptions();

        // 500 000 scalar points at the 64 B a point costs in the tier: the old HotFlushThreshold,
        // restated in the only unit that can bound memory.
        Assert.Equal(MemoryBudgets.MetricHotTierCapBytes, o.HotTierBytesFor(Large));
        Assert.Equal(500_000, o.HotTierBytesFor(Large) / MetricStorageEngine.HotPointBytes);

        // MinFlushPoints was 50 000 — a tenth of the threshold, and still is.
        Assert.Equal(50_000, o.MinFlushBytesFor(Large) / MetricStorageEngine.HotPointBytes);

        // The old 8 MB log capacity, unchanged.
        Assert.Equal(8 * MB, o.WalInitialBytesFor(Large));

        // ExemplarsPerMetric is the ONE default that does not survive on a large host, and it is
        // the cap that kills it, not the host: 4 000 slots x 256 rings x 208 B is 213 MB retained
        // for the life of the process, and the old literal was only ever tenable because nothing
        // bounded the number of rings. 300 slots is what half a 32 MB tier buys across 256 names,
        // and GetExemplars answers at most 200 at a time anyway.
        Assert.Equal(300, o.ExemplarsPerMetricFor(Large));
    }

    /// <summary>
    /// THE BOUND THE DERIVATION CLAIMS IS THE BOUND THE ENGINE ENFORCES.
    ///
    /// <para>The rings are the one piece of metric memory nothing can take back: no ring is ever
    /// pruned or aged out, <c>ShedableBytes</c> cannot see one and <c>Shed()</c> cannot release
    /// one, so whatever the two exemplar knobs multiply out to is resident for the life of the
    /// process. The derivation used to divide by a private assumption of 32 "active" names while
    /// <c>MaxExemplarMetrics</c> admitted 256, so the stand's real ceiling was 140 MB of rings
    /// sized against a 10 MB budget — inside the 384 MB heap this package exists to fit.</para>
    ///
    /// <para><b>The product, not the depth.</b> This fact used to allow
    /// <c>Math.Max(half, MaxExemplarMetrics * 64 * ExemplarBytes)</c>, which is the same product
    /// as <c>worst</c> the moment the depth hits its 64-slot floor — so on exactly the hosts and
    /// settings where the bound was needed it compared a figure to itself. The floor binds at
    /// about 756 rings on the stand, and the cap is operator-settable: 5 000 rings x 64 slots x
    /// 208 B is 66 MB, uncatchable by a tautology. The allowance is gone and the rows that raise
    /// the cap are the ones that would have been silent.</para>
    /// </summary>
    [Fact]
    public void Every_ring_the_cap_admits_fits_the_budget_the_derivation_names()
    {
        foreach (var (label, o, b) in new (string, MetricsOptions, MemoryBudgets)[]
                 {
                     ("16 GB host",   new MetricsOptions(), Large),
                     ("512 MB stand", new MetricsOptions(), Stand),
                     ("128 MB heap",  new MetricsOptions(), MemoryBudgets.Derive(128 * MB, 160 * MB)),
                     ("16 MB heap",   new MetricsOptions(), MemoryBudgets.Derive(16 * MB, 16 * MB)),

                     // The cases the floor rules, which is where the old form went blind.
                     ("stand, cap 5k",  new MetricsOptions { MaxExemplarMetrics = 5_000 },  Stand),
                     ("stand, cap 50k", new MetricsOptions { MaxExemplarMetrics = 50_000 }, Stand),
                     ("16 MB, cap 5k",  new MetricsOptions { MaxExemplarMetrics = 5_000 },
                                        MemoryBudgets.Derive(16 * MB, 16 * MB)),

                     // An explicit depth is bought out of the ring count, not out of the budget.
                     ("stand, 4k deep", new MetricsOptions { ExemplarsPerMetric = 4_000 }, Stand),

                     // And where there is no ring count left to buy it out of, the DEPTH gives
                     // way: the count clamps at 1, so an explicit 50 000 on a 4 MB tier was one
                     // ring of 10.4 MB against a 2 MB budget — the last escape from the product.
                     ("4 MB, 50k deep", new MetricsOptions { HotTierBytes = 4_000_000, ExemplarsPerMetric = 50_000 }, Stand),
                 })
        {
            long perRing = o.ExemplarsPerMetricFor(b);
            long rings   = o.MaxExemplarMetricsFor(b);
            long worst   = rings * perRing * MetricsOptions.ExemplarBytes;
            long half    = o.HotTierBytesFor(b) / 2;

            _out.WriteLine($"{label,-14}: tier {o.HotTierBytesFor(b) / 1048576.0,6:N1} MB, "
                         + $"{perRing,5:N0} slots x {rings,6:N0} rings (cap {o.MaxExemplarMetrics:N0}) "
                         + $"= {worst / 1048576.0,6:N1} MB retained against {half / 1048576.0,5:N1} MB");

            // No allowance and no floor escape: half the tier budget is the whole of it.
            Assert.True(worst <= half,
                $"{label}: {rings:N0} rings of {perRing:N0} exemplars retain "
              + $"{worst / 1048576.0:N1} MB against a budget of {half / 1048576.0:N1} MB");

            // And the clamp may only ever take rings AWAY — it is a ceiling on the operator's
            // ceiling, never a way to exceed it.
            Assert.InRange(rings, 1, o.MaxExemplarMetrics);
        }
    }

    /// <summary>
    /// THE FIGURES <c>MaxExemplarMetricsFor</c>'s OWN DOC QUOTES, so the sentence an operator
    /// reads before setting a depth is one this file has to keep true. The doc used to say the
    /// count moves only "below the floor point", which holds for the DERIVED depth and not at
    /// all for an explicit one: nothing about 1 000 slots is near the 64-slot floor, and the
    /// stand still loses 208 of its 256 rings for asking.
    /// </summary>
    [Fact]
    public void An_explicit_depth_moves_the_ring_count_at_any_tier_size()
    {
        var deep = new MetricsOptions { ExemplarsPerMetric = 1_000 };

        // The depth is affordable for one ring at both sizes, so it is honoured whole — and the
        // count is what pays for it, nowhere near the floor.
        Assert.Equal(1_000, deep.ExemplarsPerMetricFor(Stand));
        Assert.Equal(1_000, deep.ExemplarsPerMetricFor(Large));
        Assert.Equal(48, deep.MaxExemplarMetricsFor(Stand));   // 19.2 MB tier, not the cap's 256
        Assert.Equal(76, deep.MaxExemplarMetricsFor(Large));   // 32 MB tier, not the cap's 256

        // The derived depth, by contrast, leaves the count exactly where the cap put it: the
        // derivation divided the budget by that count, so it is affordable by construction.
        Assert.Equal(256, new MetricsOptions().MaxExemplarMetricsFor(Stand));
        Assert.Equal(256, new MetricsOptions().MaxExemplarMetricsFor(Large));
    }

    /// <summary>
    /// THE CLAMP IS WHAT THE ENGINE ENFORCES, not just what the options compute. A ring is
    /// allocated at full depth the first time a metric name carries an exemplar, so "admits" has
    /// to mean "creates", and the refusal counter is the engine's own record of having said no.
    /// </summary>
    [Fact]
    public async Task An_engine_refuses_the_rings_its_budget_cannot_afford()
    {
        // A tier small enough that the 64-slot floor binds hard: half of 4 MB, at 208 B a slot,
        // is 150 rings of 64 — against a cap asking for 1 000.
        var options = new MetricsOptions { HotTierBytes = 4_000_000, MaxExemplarMetrics = 1_000 };
        var budgets = MemoryBudgets.Current();

        int perRing   = options.ExemplarsPerMetricFor(budgets);
        int affordable = options.MaxExemplarMetricsFor(budgets);
        Assert.Equal(64, perRing);
        Assert.InRange(affordable, 1, 999);
        _out.WriteLine($"cap 1 000 rings x {perRing} slots would be "
                     + $"{1_000L * perRing * MetricsOptions.ExemplarBytes / 1048576.0:N1} MB; "
                     + $"the budget affords {affordable} rings = "
                     + $"{affordable * (long)perRing * MetricsOptions.ExemplarBytes / 1048576.0:N1} MB");

        string dir = Path.Combine(Path.GetTempPath(), "ameto-mringcap-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, options);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            int names = affordable + 50;
            var items = new MetricIngestItem[names];
            for (int i = 0; i < names; i++)
                items[i] = new MetricIngestItem
                {
                    Name              = "ringcap.metric." + i,
                    Kind              = MetricKind.Gauge,
                    Labels            = LabelSet.Empty,
                    TimestampUnixNano = baseNano,
                    ScalarValue       = i,
                    Exemplars         = [new MetricExemplar { TimestampUnixNano = baseNano, Value = i }],
                };
            engine.Ingest(items);

            // The names past the affordable count were refused — under the raw cap of 1 000 all
            // 200-odd of them would have taken a ring.
            Assert.True(engine.ExemplarMetricsRefused > 0,
                $"{names} exemplar-carrying names against an affordable {affordable} rings and the "
              + "engine refused none of them — it is still sizing itself by the raw cap");
            Assert.NotEmpty(engine.GetExemplars("ringcap.metric.0", null, null, null));
            Assert.Empty(engine.GetExemplars("ringcap.metric." + (names - 1), null, null, null));
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    [Fact]
    public void On_the_512_mb_stand_the_tier_is_a_share_of_the_heap_limit_and_not_a_point_count()
    {
        var o = new MetricsOptions();
        long tier = o.HotTierBytesFor(Stand);

        Assert.Equal((long)(384 * MB * MemoryBudgets.MetricHotTierFraction), tier);   // 19.2 MB
        Assert.True(tier < MemoryBudgets.MetricHotTierCapBytes, "the fraction has to bind here, not the cap");

        // What that is in points, which is the figure the plan is spending: a scalar point is
        // 64 B and a 16-bucket histogram point is 64 + 24 + 128 = 216 B, so the same budget is
        // 3.4x as many gauges as histograms. The flat 500 000-POINT threshold was 20 MB of the
        // first and 84 MB of the second, on a heap limited to 384 MB.
        long gauges     = tier / MetricStorageEngine.HotPointBytes;
        long histograms = tier / 216;
        _out.WriteLine($"stand tier budget {tier / 1048576.0:N1} MB = {gauges:N0} gauge points or {histograms:N0} 16-bucket histogram points");
        Assert.InRange(gauges,     250_000, 350_000);
        Assert.InRange(histograms,  80_000, 100_000);

        // And the log no longer opens larger than the tier it backs.
        Assert.True(o.WalInitialBytesFor(Stand) <= 8 * MB);
    }

    [Fact]
    public void A_tiny_host_gets_floors_rather_than_a_tier_that_cannot_hold_a_minute()
    {
        // 128 MB of managed heap: 5 % is 6.4 MB, which is above the 4 MB floor and well below
        // every cap. Nothing may derive to zero — a zero threshold flushes on every point and
        // writes one .mts per metric name while doing it.
        var b = MemoryBudgets.Derive(128 * MB, 160 * MB);
        var o = new MetricsOptions();

        Assert.True(o.HotTierBytesFor(b)  > 0);
        Assert.True(o.MinFlushBytesFor(b) > 0);
        Assert.True(o.MinFlushBytesFor(b) <= o.HotTierBytesFor(b));
        Assert.True(o.ExemplarsPerMetricFor(b) >= 64);
        Assert.True(o.WalInitialBytesFor(b) >= 1 * MB);

        // The floor, at a size no real host has: the tier must still be worth a file.
        var tiny = MemoryBudgets.Derive(16 * MB, 16 * MB);
        Assert.Equal(4L * 1000 * 1000, new MetricsOptions().HotTierBytesFor(tiny));
    }

    [Fact]
    public void An_explicit_setting_always_wins()
    {
        var o = new MetricsOptions
        {
            HotTierBytes       = 3 * MB,
            MinFlushBytes      = 1 * MB,
            WalInitialBytes    = 2 * MB,
            ExemplarsPerMetric = 17,
        };

        Assert.Equal(3 * MB, o.HotTierBytesFor(Large));
        Assert.Equal(1 * MB, o.MinFlushBytesFor(Large));
        Assert.Equal(2 * MB, o.WalInitialBytesFor(Large));
        Assert.Equal(17,     o.ExemplarsPerMetricFor(Large));
    }

    /// <summary>
    /// THE SUM, AFTER THE RE-CUT. These ceilings went in as an APPEND beside logs shares of
    /// 0.30 + 0.15 + 0.10, for 0.71 of the managed limit, and this bound was 0.75 — a holding
    /// figure, so the debt could not quietly grow while it was owed. The re-cut has been made:
    /// logs are 0.25 + 0.12 + 0.05, the six total 0.58, and the bound is that total.
    ///
    /// <para>Which is the point of holding it THERE and not at a round number with slack in it.
    /// Every one of the six is a ceiling and they are not all reached at once, so the sum is a
    /// worst case rather than a forecast — but a worst case is exactly what a heap hard limit
    /// enforces, and the stand's 384 MB has to keep 40 % of itself for queries, ASP.NET, the
    /// drainer and the room the GC collects in. At 0.58 there is 42 %. A seventh share, or a
    /// raise to one of these six, therefore has to be paid for out of another one here.</para>
    /// </summary>
    [Fact]
    public void The_managed_shares_still_leave_the_heap_room_to_work_in()
    {
        double managed = MemoryBudgets.ManagedBuildFraction
                       + MemoryBudgets.IndexCacheFraction
                       + MemoryBudgets.IngestBufferFraction
                       + MemoryBudgets.MetricHotTierFraction
                       + MemoryBudgets.TraceHotTierFraction
                       + MemoryBudgets.TraceMergeFraction;

        _out.WriteLine($"managed shares total {managed:P0} of the heap limit, leaving {1 - managed:P0}");

        // Rounded, because these are decimal fractions summed in binary and need not land on 0.58.
        Assert.True(Math.Round(managed, 4) <= 0.58,
            $"the managed ceilings claim {managed:P0} of the heap limit — the re-cut left them at "
          + "58 %, so a new share comes out of one of the six (MemoryBudgetTests pins them), not "
          + "out of the heap's slack");

        // The invariant the figure was chosen to satisfy, stated on its own so a later cut that
        // moves the bound above has to face it.
        Assert.True(1 - managed >= 0.40,
            $"a {1 - managed:P0} remainder of the heap limit is not enough for queries, ASP.NET, "
          + "the drainer and the slack the GC needs to collect at all");
    }

    /// <summary>
    /// The traces fractions exist here, and only here, because this file gets one owner per
    /// round. WP8 consumes them; if they are lost in a rebase that package has nothing to spend.
    /// </summary>
    [Fact]
    public void The_traces_budgets_are_derived_for_the_package_that_will_spend_them()
    {
        Assert.Equal((long)(384 * MB * MemoryBudgets.TraceHotTierFraction), Stand.TraceHotTierBytes);
        Assert.Equal((long)(384 * MB * MemoryBudgets.TraceMergeFraction),   Stand.TraceMergeBytes);
        Assert.Equal(MemoryBudgets.TraceHotTierCapBytes, Large.TraceHotTierBytes);
        Assert.Equal(MemoryBudgets.TraceMergeCapBytes,   Large.TraceMergeBytes);
    }

    /// <summary>
    /// A TRACE CEILING IS A SPAN COUNT TIMES A SPAN'S WEIGHT, AND THE WEIGHT MOVED UNDER IT.
    ///
    /// <para>64 MB and 128 MB were cut against 1 117 B a span in the hot tier and 1 740 B a span
    /// read back out of a segment — both of which were almost entirely the attribute
    /// <c>Dictionary</c>. WP2 removed it in this same wave and neither ceiling followed, so a
    /// large host would have flushed at 124 000 spans where every trace test says 50 000, and
    /// admitted a merge pass 1.8x the one <c>MaxSpansPerPass</c> builds. A cap that no longer
    /// buys the cadence it was written for is not a cap, it is a number.</para>
    ///
    /// <para>The weights are <c>TraceHotTierProbe</c>'s and <c>TraceCompactionMemoryProbe</c>'s,
    /// which print them and gate them at 700 B/span. Restated here as literals so this fact is
    /// pure arithmetic over constants — it cannot fail because of the machine it runs on, only
    /// because someone changed a weight or a ceiling without changing the other.</para>
    /// </summary>
    [Fact]
    public void The_trace_ceilings_still_buy_the_span_counts_they_were_cut_for()
    {
        // TraceHotTierProbe: an eight-attribute span, RETAINED in the tier.
        const int HotTierBytesPerSpan = 540;
        // TraceCompactionMemoryProbe: a span SpanReader.ReadAll materialises, RETAINED.
        const int MergeBytesPerSpan = 607;
        // TraceStorageEngine.HotFlushThreshold and MaxSpansPerPass, the cadences being bought.
        const int HotFlushThreshold = 50_000;
        const int MaxSpansPerPass   = 120_000;

        double tierSpans  = MemoryBudgets.TraceHotTierCapBytes / (double)HotTierBytesPerSpan;
        double mergeSpans = MemoryBudgets.TraceMergeCapBytes   / (double)MergeBytesPerSpan;

        _out.WriteLine($"hot tier cap {MemoryBudgets.TraceHotTierCapBytes / 1048576.0:N1} MB = "
                     + $"{tierSpans:N0} spans at {HotTierBytesPerSpan} B; merge cap "
                     + $"{MemoryBudgets.TraceMergeCapBytes / 1048576.0:N1} MB = {mergeSpans:N0} spans "
                     + $"at {MergeBytesPerSpan} B");

        Assert.InRange(tierSpans,  HotFlushThreshold * 0.9, HotFlushThreshold * 1.1);
        Assert.InRange(mergeSpans, MaxSpansPerPass   * 0.9, MaxSpansPerPass   * 1.1);

        // Rounded UP and never down: a host large enough for the cap must afford the WHOLE
        // cadence, not 98 % of it and a flush that arrives early for no stated reason.
        Assert.True(MemoryBudgets.TraceHotTierCapBytes >= (long)HotFlushThreshold * HotTierBytesPerSpan);
        Assert.True(MemoryBudgets.TraceMergeCapBytes   >= (long)MaxSpansPerPass   * MergeBytesPerSpan);
    }

    /// <summary>
    /// AN OPTION AN OPERATOR CANNOT FIND IS AN OPTION THEY DO NOT HAVE. <c>Ameto:Metrics</c>
    /// arrived as nine settable knobs with no <c>Metrics:</c> block in the shipped
    /// <c>config.yml</c> and not one row in <c>docs/CONFIGURATION.md</c> — which README points at
    /// and nothing else — so the whole group was undiscoverable, including the two that decide
    /// how much of a 384 MB heap the exemplar rings pin for the life of the process.
    ///
    /// <para><c>Ameto.Core.Tests.ConfigurationDocsTests</c> is the same drift guard for
    /// <c>QueryOptions</c> and <c>IngestionOptions</c>; it does not enumerate this group, so this
    /// is where the metrics half of it lives. It asks only that every settable option appears BY
    /// NAME in the reference and that the file an operator copies from carries the block.
    /// Computed <c>…For</c> methods and <c>Effective…</c> properties are not settings.</para>
    /// </summary>
    [Fact]
    public void Every_settable_metrics_option_is_documented_and_shipped_in_config_yml()
    {
        string root = RepoRoot();
        string doc  = File.ReadAllText(Path.Combine(root, "docs", "CONFIGURATION.md"));
        string yml  = File.ReadAllText(Path.Combine(root, "src", "Ameto.Server", "config.yml"));

        var missingFromDoc = new List<string>();
        var missingFromYml = new List<string>();
        foreach (var p in typeof(MetricsOptions).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!p.CanWrite) continue;                    // computed, not a setting
            if (!doc.Contains(p.Name, StringComparison.Ordinal)) missingFromDoc.Add(p.Name);
            if (!yml.Contains(p.Name, StringComparison.Ordinal)) missingFromYml.Add(p.Name);
        }

        Assert.True(missingFromDoc.Count == 0, "docs/CONFIGURATION.md does not mention " + string.Join(", ", missingFromDoc));
        Assert.True(missingFromYml.Count == 0, "src/Ameto.Server/config.yml does not mention " + string.Join(", ", missingFromYml));

        // The section header and the block, so the rows cannot be smuggled in under another
        // group's heading or left as a bare list of keys nobody can paste anywhere.
        Assert.Contains("## Metrics options (`Ameto:Metrics`)", doc, StringComparison.Ordinal);
        Assert.Contains("\n  Metrics:", yml, StringComparison.Ordinal);
    }

    /// <summary>Walks up from the test binary to the repo root, the way ConfigurationDocsTests does.</summary>
    private static string RepoRoot()
    {
        var d = new DirectoryInfo(AppContext.BaseDirectory);
        while (d is not null && !File.Exists(Path.Combine(d.FullName, "docs", "CONFIGURATION.md")))
            d = d.Parent;

        Assert.NotNull(d);
        return d!.FullName;
    }

    /// <summary>
    /// The engine spends the budget it was given, in bytes, and a histogram point costs what it
    /// weighs. This drives the REAL trigger — points go in until the engine's own threshold
    /// flush drains the tier — rather than reading the counter the change added, because a
    /// threshold that still counted points would keep that counter perfectly and flush at the
    /// wrong time anyway.
    /// </summary>
    [Fact]
    public async Task A_histogram_reaches_the_configured_budget_in_far_fewer_points_than_a_gauge()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mbudget-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            // 1 MB of tier: ~16 400 gauge points or ~4 800 histogram points, so the handful that
            // arrive while the scheduled flush reaches its snapshot cannot blur the ratio.
            var options = new MetricsOptions { HotTierBytes = 1 * MB, WalInitialBytes = 1 * MB };

            int gaugePoints     = await PointsUntilTheEngineFlushes(dir, options, histogram: false);
            int histogramPoints = await PointsUntilTheEngineFlushes(dir, options, histogram: true);

            _out.WriteLine($"1 MB tier, flushed by the engine itself after {gaugePoints:N0} gauge points "
                         + $"or {histogramPoints:N0} 16-bucket histogram points ({gaugePoints / (double)histogramPoints:N1}x)");
            Assert.True(gaugePoints > histogramPoints * 2.5,
                $"a histogram point weighs 3.4x a gauge point, but the tier took {gaugePoints} of one "
              + $"and {histogramPoints} of the other — the threshold is still counting points");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// Points in, one at a time, until the engine's own threshold flush has drained the tier.
    /// The drop is the signal: nothing else empties it, and it needs no timer.
    /// </summary>
    private static async Task<int> PointsUntilTheEngineFlushes(string root, MetricsOptions options, bool histogram)
    {
        string dir = Path.Combine(root, histogram ? "h" : "g");
        await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, options);

        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var  bounds   = new double[15];
        for (int i = 0; i < bounds.Length; i++) bounds[i] = i + 1;

        var one = new MetricIngestItem[1];
        int peak = 0;
        for (int n = 1; n <= 200_000; n++)
        {
            one[0] = histogram
                ? new MetricIngestItem
                {
                    Name              = "budget.metric",
                    Kind              = MetricKind.Histogram,
                    Labels            = LabelSet.Empty,
                    TimestampUnixNano = baseNano + n * 1_000_000L,
                    HistogramCount    = 1,
                    HistogramSum      = n,
                    BucketBounds      = bounds,
                    BucketCounts      = new long[16],
                }
                : new MetricIngestItem
                {
                    Name              = "budget.metric",
                    Kind              = MetricKind.Gauge,
                    Labels            = LabelSet.Empty,
                    TimestampUnixNano = baseNano + n * 1_000_000L,
                    ScalarValue       = n,
                };
            engine.Ingest(one);

            int live = engine.HotPointCount;
            if (live < peak) return peak;      // the flush this crossing scheduled has drained it
            peak = live;
        }

        Assert.Fail("200 000 points did not make a 1 MB tier flush — the threshold is not being reached");
        return 0;
    }
}
