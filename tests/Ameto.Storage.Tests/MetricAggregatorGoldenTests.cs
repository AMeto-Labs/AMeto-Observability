using System.Runtime.CompilerServices;
using Ameto.Metrics;
using static Ameto.Storage.Tests.MetricGolden;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT THE AGGREGATOR ANSWERS, PINNED BEFORE ITS SORTED DICTIONARIES GO (issue #83 WP7, M#4(e)(f)).
/// Captured on the unchanged code (<c>db5cdd1</c>).
///
/// <para><c>MergeFragments</c>, <c>ReduceByTimestamp</c>, <c>AggregateQuantile</c>, the heatmap and
/// the expression's <c>SumToSingle</c> each keep a <c>SortedDictionary&lt;long, …&gt;</c> — a
/// red-black node per distinct timestamp — and each has an ordering rule the replacement must
/// reproduce, not resemble: fragments merge with the LATER fragment winning a shared timestamp
/// (and, inside one fragment, the later point); reductions add their members in member order,
/// which decides the last bit of a floating-point sum; groups come out in first-seen order; top-K
/// is a stable descending sort. The facts state those rules in words; the seeded golden hashes
/// every bit of every answer over fragments that are unsorted, overlapping, reset, NaN-carrying
/// and ragged in their bucket arrays.</para>
/// </summary>
public sealed class MetricAggregatorGoldenTests
{
    private const long S  = 1_000_000_000L;
    private const long T0 = 1_784_800_020_000_000_000L;

    /// <summary>Hands the aggregator exactly these fragments, in this order, for any metric.</summary>
    private sealed class Fragments(IReadOnlyList<MetricSeries> fragments) : IMetricQuery
    {
        public IEnumerable<string> GetMetricNames(string? prefix = null) => [];

        public async IAsyncEnumerable<MetricSeries> QueryAsync(
            string metricName, DateTimeOffset? from = null, DateTimeOffset? to = null, TimeSpan? step = null,
            IReadOnlyDictionary<string, string>? labelMatchers = null, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.CompletedTask;
            foreach (var f in fragments)
                if (string.Equals(f.Name, metricName, StringComparison.Ordinal)) yield return f;
        }

        public async IAsyncEnumerable<MetricSeries> GetLatestAsync(
            string metricName, IReadOnlyDictionary<string, string>? labelMatchers = null, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private static LabelSet L(params string[] kv)
    {
        var pairs = new List<KeyValuePair<string, string>>();
        for (int i = 0; i < kv.Length; i += 2) pairs.Add(new(kv[i], kv[i + 1]));
        return new LabelSet(pairs);
    }

    private static MetricSeries F(string name, MetricKind kind, LabelSet labels, double[]? bounds, params MetricDataPoint[] pts) =>
        new() { Name = name, Kind = kind, Unit = "u", Labels = labels, BucketBounds = bounds, Points = pts };

    private static void Same(double expected, double actual) =>
        Assert.Equal(BitConverter.DoubleToInt64Bits(expected), BitConverter.DoubleToInt64Bits(actual));

    // ── The rules, in words ───────────────────────────────────────────────────

    [Fact]
    public async Task Fragments_merge_in_time_order_and_the_later_fragment_wins_a_shared_timestamp()
    {
        var a = L("s", "a");
        var agg = new MetricAggregator(new Fragments(
        [
            F("m", MetricKind.Gauge, a, null, P(T0 + 30 * S, 1), P(T0 + 10 * S, 2), P(T0 + 30 * S, 3)),   // unsorted, a tie inside
            F("m", MetricKind.Gauge, L("s", "b"), [9], P(T0, 100)),
            F("m", MetricKind.Gauge, a, [1, 2], P(T0 + 20 * S, 4), P(T0 + 10 * S, 5)),                      // wins 10 s
            F("m", MetricKind.Gauge, a, [3], P(T0 + 30 * S, 6)),                                              // wins 30 s
        ]));

        var got = await agg.QueryAsync(new MetricQueryRequest { Metric = "m" });

        Assert.Equal(2, got.Count);
        Assert.Equal(a, got[0].Labels);                                  // first-seen order
        Assert.Equal(new long[] { T0 + 10 * S, T0 + 20 * S, T0 + 30 * S }, got[0].Points.Select(p => p.TimestampUnixNano));
        Assert.Equal(new double[] { 5, 4, 6 }, got[0].Points.Select(p => p.Value));
        Assert.Equal(new double[] { 1, 2 }, got[0].BucketBounds!);         // the first fragment that HAS bounds
        Assert.Equal(L("s", "b"), got[1].Labels);
        Assert.Equal(new double[] { 9 }, got[1].BucketBounds!);            // a lone fragment passes through
    }

    [Fact]
    public async Task A_group_sums_its_members_in_member_order()
    {
        // 1e16 + 1 + 1 - 1e16: member order gives 0 (each 1 is absorbed), any other order may not.
        var agg = new MetricAggregator(new Fragments(
        [
            F("m", MetricKind.Gauge, L("g", "x", "i", "1"), null, P(T0, 1e16)),
            F("m", MetricKind.Gauge, L("g", "x", "i", "2"), null, P(T0, 1)),
            F("m", MetricKind.Gauge, L("g", "x", "i", "3"), null, P(T0, 1)),
            F("m", MetricKind.Gauge, L("g", "x", "i", "4"), null, P(T0, -1e16)),
        ]));

        var sum = Assert.Single(await agg.QueryAsync(new MetricQueryRequest
        {
            Metric = "m", Aggregation = MetricAggregation.Sum, GroupBy = ["g"],
        }));
        Same(0, Assert.Single(sum.Points).Value);

        var avg = Assert.Single(await agg.QueryAsync(new MetricQueryRequest
        {
            Metric = "m", Aggregation = MetricAggregation.Avg, GroupBy = ["g"],
        }));
        Same(0, Assert.Single(avg.Points).Value);
    }

    /// <summary>
    /// A NaN or ±Infinity is no measurement, in EVERY reduction (#92). One series of a group carrying
    /// them used to make the group's value NaN at every timestamp it touched — sum, average, min, max,
    /// the instant "last" (the alert rules' default) — so a fleet panel blanked for one pod and an
    /// alert over it was never evaluated again; a NaN counter point read as a RESET, and the next
    /// point's whole cumulative value became one step's rate. Now the finite members answer; a
    /// timestamp with no finite member is NaN (null on the wire, a gap), never 0.
    /// </summary>
    [Fact]
    public async Task A_non_finite_member_value_is_skipped_by_every_reduction()
    {
        var agg = new MetricAggregator(new Fragments(
        [
            F("g", MetricKind.Gauge, L("svc", "x", "pod", "1"), null, P(T0, 1),          P(T0 + 15 * S, double.NaN), P(T0 + 30 * S, 3)),
            F("g", MetricKind.Gauge, L("svc", "x", "pod", "2"), null, P(T0, double.NaN), P(T0 + 15 * S, double.PositiveInfinity), P(T0 + 30 * S, 5)),
            F("g", MetricKind.Gauge, L("svc", "y", "pod", "3"), null, P(T0, double.NaN), P(T0 + 15 * S, double.NaN)),
            F("c", MetricKind.Counter, L("svc", "x", "pod", "1"), null, P(T0, 10), P(T0 + 10 * S, double.NaN), P(T0 + 20 * S, 30)),
            F("c", MetricKind.Counter, L("svc", "x", "pod", "2"), null, P(T0, 100), P(T0 + 20 * S, 140)),
        ]));

        async Task<MetricSeries> One(string metric, MetricAggregation a, string group = "x")
        {
            var got = await agg.QueryAsync(new MetricQueryRequest { Metric = metric, Aggregation = a, GroupBy = ["svc"] });
            return got.Single(s => s.Labels.ValueAt(0) == group);
        }
        static double[] Values(MetricSeries s) => s.Points.Select(p => p.Value).ToArray();

        Assert.Equal(new[] { 1, double.NaN, 8 },   Values(await One("g", MetricAggregation.Sum)));
        Assert.Equal(new[] { 1, double.NaN, 4 },   Values(await One("g", MetricAggregation.Avg)));
        Assert.Equal(new[] { 1, double.NaN, 3 },   Values(await One("g", MetricAggregation.Min)));
        Assert.Equal(new[] { 1, double.NaN, 5 },   Values(await One("g", MetricAggregation.Max)));
        Assert.Equal(new[] { T0, T0 + 15 * S, T0 + 30 * S }, (await One("g", MetricAggregation.Sum)).Points.Select(p => p.TimestampUnixNano));

        // Last: each member's latest point; a group with points and no finite latest one is NaN, not 0.
        var last = Assert.Single((await One("g", MetricAggregation.Last)).Points);
        Assert.Equal((T0 + 30 * S, 8.0), (last.TimestampUnixNano, last.Value));
        var none = Assert.Single((await One("g", MetricAggregation.Last, "y")).Points);
        Assert.Equal((T0 + 15 * S, double.NaN), (none.TimestampUnixNano, none.Value));

        // Rate: one point per point after the first, as always; the NaN's own timestamp has no rate
        // (NaN, a gap), and the next runs from the last finite point — never a "reset" at the NaN: 20
        // over 20 s for pod 1 (not 30 over 10 s), plus pod 2's 40 over 20 s.
        var rate = await One("c", MetricAggregation.Rate);
        Assert.Equal(new[] { T0 + 10 * S, T0 + 20 * S }, rate.Points.Select(p => p.TimestampUnixNano));
        Assert.Equal(new[] { double.NaN, 1.0 + 2.0 }, Values(rate));
        var ungrouped = await agg.QueryAsync(new MetricQueryRequest { Metric = "c", Aggregation = MetricAggregation.Increase });
        Assert.Equal(new[] { double.NaN, 20.0 }, Values(ungrouped[0]));

        // The expression sums series per timestamp the same way; top-K ranks by the last finite value.
        var expr = await agg.EvalExprAsync(new MetricExprRequest
        {
            Left  = new MetricQueryRequest { Metric = "g" },
            Right = new MetricQueryRequest { Metric = "g" },
            Op    = MetricExprOp.Add,
        });
        Assert.Equal(new[] { 2, double.NaN, 16 }, Values(expr));
        var top = Assert.Single(await agg.QueryAsync(new MetricQueryRequest { Metric = "g", TopK = 1 }));
        Assert.Equal("2", top.Labels.ValueAt(0));                                 // pod 2: latest 5
    }

    /// <summary>
    /// "last" reads each series' LATEST point, never an older one (#92): a series whose latest point is
    /// NaN has no current value and contributes nothing — not the finite value it reported minutes
    /// ago, on which an alert rule (this is its default aggregation) would go pending and fire. A
    /// group with no finite latest point is NaN, which the evaluator treats as "no value".
    /// </summary>
    [Fact]
    public async Task Last_reads_the_latest_point_and_never_a_stale_one()
    {
        var agg = new MetricAggregator(new Fragments(
        [
            F("h", MetricKind.Gauge, L("svc", "x", "pod", "1"), null, P(T0, 1), P(T0 + 15 * S, double.NaN)),
            F("h", MetricKind.Gauge, L("svc", "x", "pod", "2"), null, P(T0, 2), P(T0 + 15 * S, 4)),
            F("h", MetricKind.Gauge, L("svc", "z", "pod", "3"), null, P(T0, 7), P(T0 + 15 * S, double.NaN)),
        ]));
        var got = await agg.QueryAsync(new MetricQueryRequest { Metric = "h", Aggregation = MetricAggregation.Last, GroupBy = ["svc"] });

        var x = Assert.Single(got.Single(s => s.Labels.ValueAt(0) == "x").Points);
        Assert.Equal((T0 + 15 * S, 4.0), (x.TimestampUnixNano, x.Value));     // pod 1 adds nothing, not its stale 1
        var z = Assert.Single(got.Single(s => s.Labels.ValueAt(0) == "z").Points);
        Assert.Equal((T0 + 15 * S, double.NaN), (z.TimestampUnixNano, z.Value)); // not its stale 7
    }

    [Fact]
    public async Task Empty_answers_stay_empty()
    {
        var agg = new MetricAggregator(new Fragments([]));
        foreach (MetricAggregation a in Enum.GetValues<MetricAggregation>())
            Assert.Empty(await agg.QueryAsync(new MetricQueryRequest { Metric = "m", Aggregation = a, GroupBy = ["g"] }));

        var hm = await agg.HeatmapAsync("m", null, null, null, null);
        Assert.Empty(hm.Bounds);
        Assert.Empty(hm.Columns);

        var expr = await agg.EvalExprAsync(new MetricExprRequest
        {
            Left = new MetricQueryRequest { Metric = "m" }, Right = new MetricQueryRequest { Metric = "m" },
        });
        Assert.Empty(expr.Points);
        Assert.Equal("expr", expr.Name);
    }

    // ── The seeded golden ─────────────────────────────────────────────────────

    private static readonly double[] BoundsA = [0.01, 0.1, 1, 10];
    private static readonly double[] BoundsB = [0.5, 5];

    /// <summary>
    /// Fragments for three metrics — a counter with resets, a gauge with NaN and -0.0, a histogram
    /// with ragged, null and mismatched bucket arrays — split across fragments that overlap in
    /// time, collide on timestamps, and arrive unsorted. Deterministic from the seed.
    /// </summary>
    /// <param name="finite">The NaN draws replaced by a finite value, the random stream untouched — the
    /// corpus whose every answer must be what it was before non-finite values were skipped (#92).</param>
    private static List<MetricSeries> Corpus(ulong seed, bool finite = false)
    {
        var rng = new GoldenRng(seed);
        var all = new List<MetricSeries>();
        string[] services = ["a", "b", "c"];
        string[] routes   = ["/x", "/y"];

        foreach (var (name, kind) in new[] { ("g.counter", MetricKind.Counter), ("g.gauge", MetricKind.Gauge), ("g.hist", MetricKind.Histogram) })
        {
            for (int s = 0; s < 18; s++)
            {
                var labels = L("service.name", services[s % 3], "route", routes[s % 2], "i", (s % 6).ToString(System.Globalization.CultureInfo.InvariantCulture));
                double[]? bounds = kind != MetricKind.Histogram ? null : s % 7 == 3 ? BoundsB : s % 11 == 5 ? null : BoundsA;
                int fragments = 1 + rng.Next(3);
                double counter = rng.Next(100);
                for (int f = 0; f < fragments; f++)
                {
                    var pts = new List<MetricDataPoint>();
                    long ts = T0 + rng.Next(8) * 15 * S + (rng.Chance(20) ? rng.Next(1000) * 1_000_000L : 0);
                    int n = rng.Next(4) == 0 ? rng.Next(2) : 3 + rng.Next(14);
                    for (int i = 0; i < n; i++)
                    {
                        ts += rng.Chance(12) ? 0 : 15 * S;
                        counter = rng.Chance(8) ? rng.Next(5) : counter + rng.Next(50) + (rng.Chance(20) ? 0.5 : 0);
                        double v = kind switch
                        {
                            MetricKind.Counter => counter,
                            MetricKind.Gauge   => rng.Next(15) switch { 0 => finite ? 42.5 : double.NaN, 1 => -0.0, 2 => 1e16, _ => (rng.NextDouble() - 0.4) * 100 },
                            _                  => rng.NextDouble(),
                        };
                        long[]? buckets = null;
                        if (kind == MetricKind.Histogram && !rng.Chance(10))
                        {
                            int len = rng.Chance(8) ? rng.Next(7) : (bounds?.Length ?? 3) + 1;
                            buckets = new long[len];
                            for (int j = 0; j < len; j++) buckets[j] = (long)(counter / (j + 1)) + rng.Next(3);
                        }
                        pts.Add(P(ts, v, count: (long)counter, sum: counter * 0.25, buckets: buckets));
                    }
                    if (rng.Chance(15)) rng.Shuffle(pts);
                    all.Add(new MetricSeries { Name = name, Kind = kind, Unit = "u", Labels = labels, BucketBounds = bounds, Points = pts });
                }
            }
        }
        rng.Shuffle(all);
        return all;
    }

    /// <summary>
    /// The seeded corpus WITH its NaN points. Re-captured for #92: a NaN or ±Infinity is now skipped by
    /// every reduction instead of poisoning it (see <c>Accumulate</c>), which moves exactly the answers
    /// a NaN touched — <see cref="Every_aggregation_answers_what_it_answered_on_finite_fragments"/>
    /// pins that nothing else did.
    /// </summary>
    [Fact]
    public async Task Every_aggregation_answers_what_it_answered_on_seeded_fragments() =>
        Assert.Equal("1F60FE1C800450E6", await HashOfEveryAnswer(Corpus(0xA66_2E6A7E)));   // E828FB2EEAC92097 before #92

    /// <summary>
    /// The same corpus with every NaN draw made finite, captured on the aggregator BEFORE #92 changed
    /// how a non-finite value is reduced: on finite data every answer, to the bit, is what it was.
    /// </summary>
    [Fact]
    public async Task Every_aggregation_answers_what_it_answered_on_finite_fragments() =>
        Assert.Equal("F35689A3C259ACDC", await HashOfEveryAnswer(Corpus(0xA66_2E6A7E, finite: true)));

    private static async Task<string> HashOfEveryAnswer(List<MetricSeries> corpus)
    {
        var agg = new MetricAggregator(new Fragments(corpus));
        string[]?[] groupings = [null, [], ["service.name"], ["route", "service.name"], ["nope"], ["i", "nope"]];

        using var h = NewHash();
        foreach (var metric in new[] { "g.counter", "g.gauge", "g.hist" })
            foreach (MetricAggregation a in Enum.GetValues<MetricAggregation>())
                foreach (var g in groupings)
                    foreach (int? topk in new int?[] { null, 0, 2 })
                    {
                        var req = new MetricQueryRequest
                        {
                            Metric = metric, Aggregation = a, GroupBy = g, TopK = topk,
                            Quantile = a == MetricAggregation.Quantile ? (topk is null ? null : 0.5) : null,
                        };
                        Add(h, metric); Add(h, (long)a); Add(h, g is null ? -1 : g.Length); Add(h, topk ?? -1);
                        foreach (var s in await agg.QueryAsync(req)) Add(h, s);
                    }

        foreach (var metric in new[] { "g.hist", "g.gauge" })
        {
            var hm = await agg.HeatmapAsync(metric, null, null, null, null);
            Add(h, hm.Unit);
            Add(h, (long)hm.Bounds.Length);
            foreach (var b in hm.Bounds) Add(h, b);
            Add(h, (long)hm.Columns.Length);
            foreach (var c in hm.Columns)
            {
                Add(h, c.Ts);
                Add(h, (long)c.Counts.Length);
                foreach (var v in c.Counts) Add(h, v);
            }
        }

        foreach (MetricExprOp op in Enum.GetValues<MetricExprOp>())
        {
            var e = await agg.EvalExprAsync(new MetricExprRequest
            {
                Left  = new MetricQueryRequest { Metric = "g.counter", Aggregation = MetricAggregation.Rate },
                Right = new MetricQueryRequest { Metric = "g.gauge", GroupBy = ["service.name"], Aggregation = MetricAggregation.Sum },
                Op = op, Scale = 100,
            });
            Add(h, e);
        }

        return Finish(h);
    }
}
