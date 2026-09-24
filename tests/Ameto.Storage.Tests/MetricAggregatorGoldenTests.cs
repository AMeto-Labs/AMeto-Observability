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
    private static List<MetricSeries> Corpus(ulong seed)
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
                            MetricKind.Gauge   => rng.Next(15) switch { 0 => double.NaN, 1 => -0.0, 2 => 1e16, _ => (rng.NextDouble() - 0.4) * 100 },
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

    [Fact]
    public async Task Every_aggregation_answers_what_it_answered_on_seeded_fragments()
    {
        var agg = new MetricAggregator(new Fragments(Corpus(0xA66_2E6A7E)));
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

        Assert.Equal("E828FB2EEAC92097", Finish(h));
    }
}
