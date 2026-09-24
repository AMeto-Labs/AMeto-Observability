using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using static Ameto.Storage.Tests.MetricGolden;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE DOWNSAMPLE AND ROLLUP ANSWERS, PINNED BEFORE ANYTHING TOUCHES THEM (issue #83 WP7, M#4(d)).
///
/// <para><c>Downsample</c> is not only a query helper: the ROLLUP calls it to write the 5-minute
/// and 1-hour tiers, so a change in what it answers is a change in the bytes written to disk, and
/// it rewrites history the next time a rollup pass runs. It was a <c>GroupBy</c> + <c>Select</c> +
/// inner <c>OrderBy</c> + <c>Last</c> / <c>Average</c> / <c>Sum</c> + <c>OrderBy</c> +
/// <c>ToList</c> chain, and every link of that chain has a behaviour a loop has to reproduce on
/// purpose rather than by resemblance:</para>
/// <list type="bullet">
/// <item>the bucket key is <c>ts / b * b</c> — truncation toward zero, so a negative timestamp's
/// bucket is ABOVE it, and a step under a millisecond divides by zero (but only once a point
/// reaches the key selector: empty input never throws);</item>
/// <item>counter / histogram: the point with the greatest timestamp in the bucket, and among equal
/// greatest timestamps the one that came LAST in the input (a stable sort, then <c>Last</c>);</item>
/// <item>gauge: <c>Average</c> sums in INPUT order starting from the first element (so a lone -0.0
/// stays -0.0), while <c>Sum</c> starts from +0.0 (so it does not), and <c>Count</c> is a
/// <c>checked</c> long sum;</item>
/// <item>the answer is ordered by bucket, whatever order the input came in.</item>
/// </list>
///
/// <para>The rollup additionally sorts a series' gathered points first — STABLY — and the
/// compaction's <c>DedupeByTimestamp</c> keeps the last of equal timestamps. Both are pinned here,
/// with the <c>.mts</c> bytes a chunked rewrite produces from them. Every expected value below was
/// captured on the unchanged code (<c>db5cdd1</c>).</para>
/// </summary>
public sealed class MetricDownsampleGoldenTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mgold-" + Guid.NewGuid().ToString("N"));

    public MetricDownsampleGoldenTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long S   = 1_000_000_000L;
    private const long T0  = 1_784_800_020_000_000_000L;   // a whole minute
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private static List<MetricDataPoint> Down(IReadOnlyList<MetricDataPoint> pts, TimeSpan step, MetricKind kind) =>
        MetricStorageEngine.Downsample(pts, step, kind).ToList();

    private static void Same(double expected, double actual) =>
        Assert.Equal(BitConverter.DoubleToInt64Bits(expected), BitConverter.DoubleToInt64Bits(actual));

    // ── Counter / histogram: the last point of each bucket ────────────────────

    [Fact]
    public void A_counter_keeps_the_last_point_of_each_bucket_stamped_with_the_bucket_start()
    {
        long[] b3 = [3, 0], b5 = [5, 1], b6 = [6, 2];
        var pts = new List<MetricDataPoint>
        {
            P(T0,           1, count: 10, sum: 1.5),
            P(T0 + 10 * S,  2, count: 11, sum: 2.5),
            P(T0 + 59 * S,  3, count: 12, sum: 3.5, buckets: b3),
            P(T0 + 60 * S,  4, count: 13, sum: 4.5),
            P(T0 + 61 * S,  5, count: 14, sum: 5.5, buckets: b5),
            P(T0 + 125 * S, 6, count: 15, sum: 6.5, buckets: b6),
        };

        var got = Down(pts, Minute, MetricKind.Counter);

        Assert.Equal(new long[] { T0, T0 + 60 * S, T0 + 120 * S }, got.Select(p => p.TimestampUnixNano));
        Same(3, got[0].Value); Assert.Equal(12, got[0].Count); Same(3.5, got[0].Sum); Assert.Same(b3, got[0].BucketCounts);
        Same(5, got[1].Value); Assert.Equal(14, got[1].Count); Same(5.5, got[1].Sum); Assert.Same(b5, got[1].BucketCounts);
        Same(6, got[2].Value); Assert.Equal(15, got[2].Count); Same(6.5, got[2].Sum); Assert.Same(b6, got[2].BucketCounts);
    }

    [Fact]
    public void A_histogram_keeps_the_last_points_bucket_array_itself()
    {
        long[] first = [1, 1, 0], last = [2, 3, 1];
        var pts = new List<MetricDataPoint>
        {
            P(T0 + 5 * S,  0.25, count: 2, sum: 0.5, buckets: first),
            P(T0 + 50 * S, 0.5,  count: 6, sum: 3.0, buckets: last),
        };

        var got = Down(pts, Minute, MetricKind.Histogram);

        var only = Assert.Single(got);
        Assert.Equal(T0, only.TimestampUnixNano);
        Same(0.5, only.Value); Assert.Equal(6, only.Count); Same(3.0, only.Sum);
        Assert.Same(last, only.BucketCounts);
    }

    [Fact]
    public void Among_equal_greatest_timestamps_the_later_input_point_wins()
    {
        // Out of order on purpose: the winner is the greatest timestamp (30 s), and of the two
        // points carrying it, the one that came later in the INPUT — not the last point overall.
        var pts = new List<MetricDataPoint>
        {
            P(T0 + 30 * S, 1),
            P(T0 + 10 * S, 2),
            P(T0 + 30 * S, 3),
            P(T0 + 20 * S, 4),
        };

        var got = Down(pts, Minute, MetricKind.Counter);

        Same(3, Assert.Single(got).Value);
    }

    // ── Gauge: the average, in input order ────────────────────────────────────

    [Fact]
    public void A_gauge_averages_its_bucket_and_sums_count_and_sum()
    {
        var pts = new List<MetricDataPoint>
        {
            P(T0,          1, count: 1, sum: 0.1, buckets: [9]),
            P(T0 + 20 * S, 2, count: 2, sum: 0.2),
            P(T0 + 40 * S, 4, count: 4, sum: 0.4),
        };

        var only = Assert.Single(Down(pts, Minute, MetricKind.Gauge));

        Assert.Equal(T0, only.TimestampUnixNano);
        Same((1.0 + 2.0 + 4.0) / 3, only.Value);
        Assert.Equal(7, only.Count);
        Same(0.0 + 0.1 + 0.2 + 0.4, only.Sum);
        Assert.Null(only.BucketCounts);                // an average has no bucket snapshot
    }

    [Fact]
    public void A_gauge_sums_in_input_order_not_timestamp_order()
    {
        // 1e16 + 1 rounds back to 1e16, so the order of the additions decides the answer:
        //   input order     1e16, 1, 1, -1e16  ->  0 / 4 = 0
        //   timestamp order 1, 1, 1e16, -1e16  ->  2 / 4 = 0.5
        var pts = new List<MetricDataPoint>
        {
            P(T0 + 2 * S, 1e16),
            P(T0,         1),
            P(T0 + 1 * S, 1),
            P(T0 + 3 * S, -1e16),
        };

        Same(0.0, Assert.Single(Down(pts, Minute, MetricKind.Gauge)).Value);
    }

    [Fact]
    public void An_average_starts_from_its_first_value_and_a_sum_from_zero()
    {
        // Average seeds its accumulator with the first element; Sum seeds it with +0.0. A lone
        // -0.0 therefore averages to -0.0 and sums to +0.0 — and JSON writes the two differently.
        var only = Assert.Single(Down([P(T0, -0.0, sum: -0.0)], Minute, MetricKind.Gauge));

        Same(-0.0, only.Value);
        Same(0.0,  only.Sum);
    }

    [Fact]
    public void NaN_survives_both_rules()
    {
        Same(double.NaN, Assert.Single(Down([P(T0, 1), P(T0 + S, double.NaN)], Minute, MetricKind.Gauge)).Value);
        Same(double.NaN, Assert.Single(Down([P(T0, 1), P(T0 + S, double.NaN)], Minute, MetricKind.Counter)).Value);
        Same(1.0,        Assert.Single(Down([P(T0, double.NaN), P(T0 + S, 1)], Minute, MetricKind.Counter)).Value);
    }

    // ── Shape: empty, single, order, boundaries ───────────────────────────────

    [Fact]
    public void Empty_in_empty_out_whatever_the_step()
    {
        Assert.Empty(Down([], Minute, MetricKind.Gauge));
        Assert.Empty(Down([], Minute, MetricKind.Counter));
        // A zero-width bucket divides by zero — but only once a point reaches the key.
        Assert.Empty(Down([], TimeSpan.Zero, MetricKind.Gauge));
        Assert.Empty(Down([], TimeSpan.FromTicks(1), MetricKind.Counter));
    }

    [Fact]
    public void A_step_under_a_millisecond_divides_by_zero()
    {
        Assert.Throws<DivideByZeroException>(() => Down([P(T0, 1)], TimeSpan.FromTicks(9_999), MetricKind.Gauge));
        Assert.Throws<DivideByZeroException>(() => Down([P(T0, 1)], TimeSpan.Zero, MetricKind.Counter));
    }

    [Fact]
    public void A_single_point_lands_on_its_bucket_start()
    {
        var only = Assert.Single(Down([P(T0 + 42 * S + 7, 5, count: 3, sum: 1.25)], Minute, MetricKind.Gauge));
        Assert.Equal(T0, only.TimestampUnixNano);
        Same(5, only.Value);
        Assert.Equal(3, only.Count);
        Same(1.25, only.Sum);
    }

    [Fact]
    public void Buckets_come_out_in_bucket_order_whatever_order_they_went_in()
    {
        var pts = new List<MetricDataPoint> { P(T0 + 130 * S, 3), P(T0 + 10 * S, 1), P(T0 + 70 * S, 2) };

        var got = Down(pts, Minute, MetricKind.Gauge);

        Assert.Equal(new long[] { T0, T0 + 60 * S, T0 + 120 * S }, got.Select(p => p.TimestampUnixNano));
        Assert.Equal(new double[] { 1.0, 2.0, 3.0 }, got.Select(p => p.Value));
    }

    [Fact]
    public void A_bucket_is_closed_at_its_start_and_open_at_its_end_and_truncates_toward_zero()
    {
        long b = 60 * S;
        var pts = new List<MetricDataPoint>
        {
            P(T0 - 1, 1),        // the previous bucket
            P(T0,     2),        // exactly on a boundary: this bucket
            P(T0 + b - 1, 3),    // last nanosecond of this bucket
            P(T0 + b, 4),        // next bucket
        };
        var got = Down(pts, Minute, MetricKind.Counter);
        Assert.Equal(new long[] { T0 - b, T0, T0 + b }, got.Select(p => p.TimestampUnixNano));
        Assert.Equal(new double[] { 1.0, 3.0, 4.0 }, got.Select(p => p.Value));

        // Negative timestamps: ts / b * b truncates toward zero, so (-b, b) is ONE bucket keyed 0.
        var neg = Down([P(-b - 1, 1), P(-b, 2), P(-1, 3), P(0, 4), P(b - 1, 5)], Minute, MetricKind.Counter);
        Assert.Equal(new long[] { -b, 0 }, neg.Select(p => p.TimestampUnixNano));
        Assert.Equal(new double[] { 2.0, 5.0 }, neg.Select(p => p.Value));

        // A negative step keys the same buckets for positive timestamps (-(ts / b) * -b).
        var negStep = Down(pts, -Minute, MetricKind.Counter);
        Assert.Equal(got.Select(p => p.TimestampUnixNano), negStep.Select(p => p.TimestampUnixNano));
    }

    // ── Extreme spans (WP7 review, F2) ────────────────────────────────────────

    /// <summary>
    /// The LINQ chain <c>Downsample</c> was at <c>db5cdd1</c>, verbatim — the oracle for inputs the
    /// seeded golden does not reach.
    /// </summary>
    private static List<MetricDataPoint> LinqDownsample(IReadOnlyList<MetricDataPoint> points, TimeSpan step, MetricKind kind)
    {
        long bucketNanos = (long)step.TotalMilliseconds * 1_000_000L;
        bool takeLast = kind is MetricKind.Counter or MetricKind.Histogram;

        return points
            .GroupBy(p => p.TimestampUnixNano / bucketNanos * bucketNanos)
            .Select(g =>
            {
                if (takeLast)
                {
                    var last = g.OrderBy(p => p.TimestampUnixNano).Last();
                    return new MetricDataPoint
                    {
                        TimestampUnixNano = g.Key,
                        Value             = last.Value,
                        Count             = last.Count,
                        Sum               = last.Sum,
                        BucketCounts      = last.BucketCounts,
                    };
                }
                return new MetricDataPoint
                {
                    TimestampUnixNano = g.Key,
                    Value             = g.Average(p => p.Value),
                    Count             = g.Sum(p => p.Count),
                    Sum               = g.Sum(p => p.Sum),
                };
            })
            .OrderBy(p => p.TimestampUnixNano)
            .ToList();
    }

    /// <summary>
    /// A series whose timestamps span more than half the long range. OTLP carries time_unix_nano
    /// as an unsigned 64-bit field; one with its top bit set arrives as a long at or below
    /// -7.5e18, and ingest refuses only FUTURE timestamps — so such a point sits in a series beside
    /// ordinary ones, and every stepped query over the series and every rollup of its metric
    /// downsamples it. The bucket-count arithmetic that sizes the answer overflowed there
    /// (<c>lastKey - firstKey</c> wraps negative) and a negative capacity threw where GroupBy had
    /// answered.
    /// </summary>
    [Fact]
    public void A_series_spanning_most_of_the_long_range_downsamples_as_the_LINQ_chain_did()
    {
        long[][] shapes =
        [
            [-7_500_000_000_000_000_000L, T0],
            [-7_500_000_000_000_000_000L, T0, T0 + 30 * S, T0 + 90 * S],
            [long.MinValue, 0, long.MaxValue],
            [long.MinValue + 1, long.MaxValue - 1],
            [-1, 0, 1, long.MaxValue],
            [long.MinValue, long.MinValue + 1_000_000, long.MinValue + 60 * S],
        ];
        TimeSpan[] steps = [TimeSpan.FromMilliseconds(1), Minute, TimeSpan.FromHours(1), -Minute, TimeSpan.FromDays(3650)];

        int compared = 0;
        foreach (var shape in shapes)
            foreach (var step in steps)
                foreach (var kind in new[] { MetricKind.Counter, MetricKind.Gauge, MetricKind.Histogram })
                {
                    var pts = new List<MetricDataPoint>();
                    for (int i = 0; i < shape.Length; i++) pts.Add(P(shape[i], i + 0.5, count: i, sum: i * 0.25, buckets: [i]));
                    Assert.Equal(Hash(LinqDownsample(pts, step, kind)), Hash(MetricStorageEngine.Downsample(pts, step, kind)));
                    compared++;
                }
        Assert.Equal(6 * 5 * 3, compared);

        // And seeded: points drawn from the whole long range, sorted, against the same oracle.
        var rng = new GoldenRng(0xE7_7E_E3_E5);
        for (int c = 0; c < 300; c++)
        {
            var pts = new List<MetricDataPoint>();
            int n = 1 + rng.Next(20);
            for (int i = 0; i < n; i++)
                pts.Add(P(rng.Chance(30) ? T0 + rng.Next(1_000) * S : (long)rng.NextU64(), rng.NextDouble(), count: rng.Next(100), sum: rng.NextDouble()));
            if (!rng.Chance(20)) pts.Sort(static (a, b) => a.TimestampUnixNano.CompareTo(b.TimestampUnixNano));
            var kind = (MetricKind)(c % 3);
            var step = steps[rng.Next(steps.Length)];
            Assert.Equal(Hash(LinqDownsample(pts, step, kind)), Hash(MetricStorageEngine.Downsample(pts, step, kind)));
        }
    }

    // ── Seeded goldens ────────────────────────────────────────────────────────

    private static readonly TimeSpan[] Steps =
    [
        TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(15),
        TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5), TimeSpan.FromHours(1), TimeSpan.FromMinutes(-1),
        TimeSpan.FromMilliseconds(1500),
    ];

    /// <summary>
    /// Point lists shaped to find the corners: ties (a zero increment), unsorted runs, NaN, -0.0,
    /// magnitudes that make summation order visible, shared and null bucket arrays, negative
    /// timestamps. Deterministic from the seed.
    /// </summary>
    internal static List<MetricDataPoint> RandomPoints(GoldenRng rng, MetricKind kind)
    {
        int  n    = rng.Next(5) == 0 ? rng.Next(4) : rng.Next(240);
        long ts   = rng.Chance(10) ? -rng.Next(1_000_000) * 1_000_000L : T0 + rng.Next(3_600_000) * 1_000_000L;
        var  pts  = new List<MetricDataPoint>(n);
        long[]? shared = null;
        for (int i = 0; i < n; i++)
        {
            ts += rng.Chance(15) ? 0 : rng.Next(40_000) * 1_000_000L + (rng.Chance(10) ? rng.Next(999_999) : 0);
            double v = rng.Next(12) switch
            {
                0 => double.NaN,
                1 => -0.0,
                2 => 1e16 * (rng.Chance(50) ? 1 : -1),
                3 => rng.Next(1000),
                _ => (rng.NextDouble() - 0.3) * 1e3,
            };
            long[]? buckets = null;
            if (kind == MetricKind.Histogram && !rng.Chance(10))
            {
                if (shared is null || rng.Chance(60))
                {
                    shared = new long[rng.Next(6)];
                    for (int j = 0; j < shared.Length; j++) shared[j] = rng.Next(100);
                }
                buckets = shared;
            }
            pts.Add(P(ts, v, count: rng.Next(10_000), sum: rng.Chance(5) ? -0.0 : rng.NextDouble() * 50, buckets: buckets));
        }
        if (rng.Chance(30)) rng.Shuffle(pts);
        return pts;
    }

    [Fact]
    public void Downsample_answers_what_it_answered_on_seeded_input()
    {
        var rng = new GoldenRng(0xD0_5A_3F_1E);
        using var h = NewHash();
        for (int c = 0; c < 600; c++)
        {
            var kind = (MetricKind)(c % 3);
            var pts  = RandomPoints(rng, kind);
            var step = Steps[rng.Next(Steps.Length)];
            Add(h, (long)c);
            Add(h, MetricStorageEngine.Downsample(pts, step, kind));
        }
        Assert.Equal("C5BCB1C237CBCC06", Finish(h));
    }

    // ── The rollup's transform: a stable sort, then Downsample ────────────────

    [Fact]
    public void The_rollup_sorts_stably_before_it_downsamples()
    {
        // Sorted by timestamp first, so this bucket sums 1, 1, 1e16, -1e16 = 2 -> 0.5 (where
        // Downsample alone, in input order, answers 0 — see the fact above).
        var shuffled = new List<MetricDataPoint> { P(T0 + 2 * S, 1e16), P(T0, 1), P(T0 + 1 * S, 1), P(T0 + 3 * S, -1e16) };
        Same(0.5, Assert.Single(MetricStorageEngine.RollupPoints(shuffled, Minute, MetricKind.Gauge)).Value);

        // Equal timestamps keep their input order through the sort: the sum is the input-order one…
        var tied = new List<MetricDataPoint> { P(T0, 1e16), P(T0, 1), P(T0, 1), P(T0, -1e16) };
        Same(0.0, Assert.Single(MetricStorageEngine.RollupPoints(tied, Minute, MetricKind.Gauge)).Value);

        // …and the last of the greatest is the one that came last.
        var last = new List<MetricDataPoint> { P(T0 + 5 * S, 1), P(T0 + 9 * S, 2), P(T0 + 9 * S, 3), P(T0 + 1 * S, 4) };
        Same(3.0, Assert.Single(MetricStorageEngine.RollupPoints(last, Minute, MetricKind.Counter)).Value);

        Assert.Empty(MetricStorageEngine.RollupPoints([], TimeSpan.FromMinutes(5), MetricKind.Counter));
    }

    [Fact]
    public void The_rollup_transform_answers_what_it_answered_on_seeded_input()
    {
        var rng = new GoldenRng(0x0B_17_CA_FE);
        using var h = NewHash();
        for (int c = 0; c < 600; c++)
        {
            var kind = (MetricKind)(c % 3);
            var pts  = RandomPoints(rng, kind);
            var bucket = rng.Chance(50) ? TimeSpan.FromMinutes(5) : TimeSpan.FromHours(1);
            var input  = new List<MetricDataPoint>(pts);
            Add(h, (long)c);
            Add(h, MetricStorageEngine.RollupPoints(input, bucket, kind));
            Add(h, input);                    // what the transform leaves of its argument
        }
        Assert.Equal("ECA15E4E548D3D11", Finish(h));
    }

    // ── The compaction's transform: last wins ─────────────────────────────────

    [Fact]
    public void The_compaction_keeps_the_last_of_equal_timestamps()
    {
        var pts = new List<MetricDataPoint> { P(T0 + S, 1), P(T0, 2), P(T0 + S, 3), P(T0, 4), P(T0 + 2 * S, 5) };

        var got = MetricStorageEngine.DedupeByTimestamp(pts);

        Assert.Equal(new long[] { T0, T0 + S, T0 + 2 * S }, got.Select(p => p.TimestampUnixNano));
        Assert.Equal(new double[] { 4.0, 3.0, 5.0 }, got.Select(p => p.Value));
        Assert.Empty(MetricStorageEngine.DedupeByTimestamp([]));
    }

    [Fact]
    public void The_compaction_transform_answers_what_it_answered_on_seeded_input()
    {
        var rng = new GoldenRng(0xDE_D0_0B_ED);
        using var h = NewHash();
        for (int c = 0; c < 600; c++)
        {
            var pts = RandomPoints(rng, (MetricKind)(c % 3));
            Add(h, (long)c);
            Add(h, MetricStorageEngine.DedupeByTimestamp(new List<MetricDataPoint>(pts)));
        }
        Assert.Equal("79022D9A94D2B1D1", Finish(h));
    }

    // ── What a chunked rewrite puts on disk ───────────────────────────────────

    /// <summary>
    /// Sources for one metric, written the way a deployment accumulates them: three files whose
    /// series come in DIFFERENT orders (ascending, descending, shuffled — so a chunk can never be
    /// read off one file's slots), overlapping in time with colliding timestamps, a series only
    /// the last file has, and a legacy v2 file among them.
    /// </summary>
    private List<MetricSegmentInfo> WriteSources(string metric, MetricKind kind, int seriesCount, ulong seed)
    {
        var rng    = new GoldenRng(seed);
        double[]? bounds = kind == MetricKind.Histogram ? [0.005, 0.01, 0.05, 0.1, 0.5, 1, 5] : null;
        string unit = kind == MetricKind.Gauge ? "By" : "s";

        List<MetricDataPoint> Series(int s, int file)
        {
            var pts = new List<MetricDataPoint>();
            long ts = T0 + file * 20 * 60 * S + (s % 7) * S;
            int  n  = 3 + rng.Next(9);
            for (int i = 0; i < n; i++)
            {
                ts += rng.Chance(10) ? 0 : (15 + rng.Next(60)) * S;
                long[]? b = kind == MetricKind.Histogram
                    ? [rng.Next(9), rng.Next(9), rng.Next(9), 0, rng.Next(3), 0, 0, rng.Next(2)]
                    : null;
                pts.Add(P(ts, kind == MetricKind.Gauge ? (rng.NextDouble() - 0.5) * 100 : s * 10 + file * 100 + i,
                          count: kind == MetricKind.Histogram ? 10 + i : 0,
                          sum:   kind == MetricKind.Histogram ? i * 0.25 : 0,
                          buckets: b));
            }
            return pts;
        }

        LabelSet Labels(int s) => new(new Dictionary<string, string>
        {
            ["service.name"] = "Golden.API",
            ["route"]        = "/api/r" + (s % 13),
            ["replica"]      = s.ToString(System.Globalization.CultureInfo.InvariantCulture),
        });

        var sources = new List<MetricSegmentInfo>();
        for (int file = 0; file < 3; file++)
        {
            var order = Enumerable.Range(0, seriesCount).ToList();
            if (file == 1) order.Reverse();
            if (file == 2) { rng.Shuffle(order); order.RemoveAll(s => s % 5 == 0); order.Add(seriesCount + 7); }

            var items = new List<(SeriesKey, HotSeries)>();
            foreach (int s in order)
                items.Add((new SeriesKey(metric, kind, unit, Labels(s)), new HotSeries(Series(s, file), bounds)));
            sources.AddRange(MetricWriter.Write(_dir, items, MetricGranularity.Raw));
        }

        // The legacy v2 file: every third series, in a shuffled order of its own.
        var legacy = Enumerable.Range(0, seriesCount).Where(s => s % 3 == 0).ToList();
        rng.Shuffle(legacy);
        var v2Items = new List<(SeriesKey, List<MetricDataPoint>, double[]?)>();
        foreach (int s in legacy)
            v2Items.Add((new SeriesKey(metric, kind, unit, Labels(s)), Series(s, 3), bounds));
        sources.Add(WriteV2File(Path.Combine(_dir, $"legacy-{metric}.mts"), metric, MetricGranularity.Raw, v2Items));
        return sources;
    }

    private static string HashOutputs(List<MetricSegmentInfo> outputs)
    {
        using var h = NewHash();
        Add(h, (long)outputs.Count);
        foreach (var o in outputs) AddFile(h, o);
        return Finish(h);
    }

    [Theory]
    [InlineData(MetricKind.Histogram, 700,  "rollup",  "51E5E50B4186141C")]
    [InlineData(MetricKind.Gauge,     700,  "rollup",  "13C2A70FFF45D9B8")]
    [InlineData(MetricKind.Counter,   300,  "rollup",  "FA77B2C5BE7AD4D0")]
    [InlineData(MetricKind.Histogram, 1100, "compact", "CFB956B62291192F")]
    [InlineData(MetricKind.Counter,   300,  "compact", "4CA3BB20AFD9400D")]
    public async Task A_chunked_rewrite_writes_the_bytes_it_wrote(MetricKind kind, int seriesCount, string transform, string expected)
    {
        var sources = WriteSources("golden." + kind.ToString().ToLowerInvariant(), kind, seriesCount,
                                   seed: (ulong)(seriesCount * 31 + (int)kind));

        // Its own directory, and its background catalog scan finished before anything is written
        // into it: the rewrite's output lands in the engine's data directory.
        string engineDir = Path.Combine(_dir, "engine");
        Directory.CreateDirectory(engineDir);
        await using var engine = new MetricStorageEngine(engineDir, NullLogger<MetricStorageEngine>.Instance);
        await engine.ColdLoadCompleted;
        {
            var outputs = transform == "rollup"
                ? engine.RewriteMetricInChunks(sources, MetricGranularity.FiveMin,
                      static (pts, k) => MetricStorageEngine.RollupPoints(pts, TimeSpan.FromMinutes(5), k))
                : engine.RewriteMetricInChunks(sources, MetricGranularity.Raw,
                      static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts));

            Assert.Equal(expected, HashOutputs(outputs));
        }
    }
}
