using System.Buffers;
using System.Numerics;

namespace Ameto.Metrics;

/// <summary>
/// Server-side metric aggregation: reset-aware counter rate/increase, gauge
/// reductions, histogram percentiles (histogram_quantile), group-by, top-K, and
/// latency heatmaps. Built on <see cref="IMetricQuery"/> so it stays independent of
/// the on-disk format.
///
/// <para><b>No <c>SortedDictionary&lt;long, …&gt;</c>, and no array per step.</b> Every reduction
/// here — merging a series' fragments, reducing a group per timestamp, a quantile's and a
/// heatmap's per-step bucket deltas, an expression's per-timestamp sum — used to key a
/// <c>SortedDictionary</c> on the timestamp: a red-black node per distinct timestamp (per POINT
/// when fragments are merged), and for the quantile and the heatmap a fresh <c>double[]</c> per
/// timestamp per group. They now number the distinct timestamps in the order the points are
/// offered (<see cref="TimestampSlots"/>, rented), keep the per-slot state in rented arrays, and
/// sort only the distinct timestamps, once, at the end. The accumulation ORDER — member order,
/// then point order — is the order the dictionaries saw, which is what decides the last bit of a
/// floating-point sum and which of two points at one timestamp a merge keeps;
/// <c>MetricAggregatorGoldenTests</c> pins every answer.</para>
/// </summary>
public sealed class MetricAggregator : IMetricAggregator
{
    private readonly IMetricQuery _query;

    public MetricAggregator(IMetricQuery query) => _query = query;

    public Task<IReadOnlyList<MetricSeries>> QueryAsync(
        MetricQueryRequest request,
        CancellationToken  ct = default) =>
        QueryAsync(request, valuesOnly: false, ct);

    /// <summary>
    /// The point fields an aggregation's work and ANSWER can touch, so storage builds no bucket
    /// arrays for one that never reads them (<see cref="MetricPointFields.NoBuckets"/>): a rate
    /// reads the cumulative Count, a last value and every grouped reduction build fresh points
    /// from Value, and none of those answers carries a bucket array. A quantile needs them; so do
    /// the answers that ARE the stored points — no aggregation, and a reduction without a
    /// group-by, which hands a lone series back as it came (see <see cref="ReduceByTimestamp"/>) —
    /// unless the caller will only read values (<paramref name="valuesOnly"/>, the expression).
    /// </summary>
    private static MetricPointFields FieldsFor(MetricQueryRequest request, bool valuesOnly) => request.Aggregation switch
    {
        MetricAggregation.Quantile => MetricPointFields.All,
        MetricAggregation.Rate or MetricAggregation.Increase or MetricAggregation.Last => MetricPointFields.NoBuckets,
        MetricAggregation.None => valuesOnly ? MetricPointFields.NoBuckets : MetricPointFields.All,
        _ => valuesOnly || request.GroupBy is { Length: > 0 } ? MetricPointFields.NoBuckets : MetricPointFields.All,
    };

    private async Task<IReadOnlyList<MetricSeries>> QueryAsync(
        MetricQueryRequest request,
        bool               valuesOnly,
        CancellationToken  ct)
    {
        var fragments = new List<MetricSeries>();
        await foreach (var s in _query.QueryAsync(request.Metric, request.From, request.To, request.Step, request.Filters,
                                                  FieldsFor(request, valuesOnly), ct))
            fragments.Add(s);
        if (fragments.Count == 0) return [];

        // Merge per-segment fragments of the same series into continuous series first.
        var raw = MergeFragments(fragments);

        IReadOnlyList<MetricSeries> result = request.Aggregation switch
        {
            MetricAggregation.Rate or MetricAggregation.Increase
                => AggregateRate(raw, request),
            MetricAggregation.Quantile
                => AggregateQuantile(raw, request),
            MetricAggregation.None
                => raw,
            _   => AggregateScalar(raw, request),
        };

        if (request.TopK is int k && k > 0 && result.Count > k)
            result = result
                .OrderByDescending(LastValue)
                .Take(k)
                .ToList();

        return result;
    }

    public async Task<MetricSeries> EvalExprAsync(MetricExprRequest req, CancellationToken ct = default)
    {
        // Values only: SumToSingle reads nothing else of either side.
        var left  = SumToSingle(await QueryAsync(req.Left, valuesOnly: true, ct));
        var right = SumToSingle(await QueryAsync(req.Right, valuesOnly: true, ct));

        // Align by timestamp (left drives the grid; right value looked up, else carried).
        var rightByTs = new Dictionary<long, double>(right.Count);
        foreach (var (ts, v) in right) rightByTs[ts] = v;

        var pts = new List<MetricDataPoint>(left.Count);
        foreach (var (ts, l) in left)
        {
            if (!rightByTs.TryGetValue(ts, out var r)) continue;
            double v = req.Op switch
            {
                MetricExprOp.Div => r != 0 ? l / r : 0,
                MetricExprOp.Mul => l * r,
                MetricExprOp.Add => l + r,
                MetricExprOp.Sub => l - r,
                _                => 0,
            } * req.Scale;
            pts.Add(new MetricDataPoint { TimestampUnixNano = ts, Value = v });
        }

        return new MetricSeries
        {
            Name = req.Name ?? "expr", Kind = MetricKind.Gauge, Unit = "", Labels = LabelSet.Empty, Points = pts,
        };
    }

    /// <summary>
    /// Reduces a multi-series result to (ts → summed value) pairs ordered by time. Each timestamp's
    /// sum starts at 0.0 and adds its values in series order, then point order, as
    /// <c>GetValueOrDefault(ts) + value</c> into a sorted dictionary did — its FINITE values (#92):
    /// a NaN or ±Infinity is skipped, as <c>Accumulate</c> skips it, and a timestamp with no finite
    /// value sums to NaN (a gap), not 0. NaN marks "nothing added yet": finite additions cannot make
    /// one (an overflow is an infinity, and nothing finite adds the opposite infinity to it).
    /// </summary>
    private static List<(long Ts, double Value)> SumToSingle(IReadOnlyList<MetricSeries> series)
    {
        using var slots = new TimestampSlots();
        double[] sums = [];
        try
        {
            foreach (var s in series)
            {
                var points = s.Points;
                for (int i = 0; i < points.Count; i++)
                {
                    int slot = slots.SlotOf(points[i].TimestampUnixNano, out bool added);
                    if (added) { Grow(ref sums, slots.Count, clear: false); sums[slot] = double.NaN; }
                    double v = points[i].Value;
                    if (!double.IsFinite(v)) continue;
                    sums[slot] = double.IsNaN(sums[slot]) ? 0.0 + v : sums[slot] + v;
                }
            }

            var sorted = slots.Sorted();
            var result = new List<(long, double)>(sorted.Length);
            foreach (int slot in sorted) result.Add((slots.TimestampOf(slot), sums[slot]));
            return result;
        }
        finally { Return(sums, clear: false); }
    }

    public async Task<HeatmapResult> HeatmapAsync(
        string            metricName,
        DateTimeOffset?   from,
        DateTimeOffset?   to,
        TimeSpan?         step,
        IReadOnlyDictionary<string, string>? filters,
        CancellationToken ct = default)
    {
        var fragments = new List<MetricSeries>();
        await foreach (var s in _query.QueryAsync(metricName, from, to, step, filters, ct))
            if (s.Kind == MetricKind.Histogram && s.BucketBounds is { Length: > 0 })
                fragments.Add(s);

        if (fragments.Count == 0) return new HeatmapResult();

        // Merge per-segment fragments so the heatmap is continuous across the window.
        var raw = MergeFragments(fragments);

        var bounds = raw[0].BucketBounds!;
        int nBuckets = bounds.Length + 1;
        string unit = raw[0].Unit;

        // Accumulate per-step bucket-count deltas, summed across all matching series — per
        // millisecond step, into one rented block of nBuckets doubles per distinct step.
        using var slots = new TimestampSlots();
        double[] steps = [];
        try
        {
            foreach (var series in raw)
            {
                if (series.BucketBounds is null || series.BucketBounds.Length + 1 != nBuckets) continue;
                AccumulateBucketDeltas(series.Points, slots, ref steps, nBuckets, tsDivisor: 1_000_000L);
            }

            var sorted = slots.Sorted();
            var cols   = new HeatmapColumn[sorted.Length];
            for (int i = 0; i < sorted.Length; i++)
            {
                int slot = sorted[i];
                cols[i] = new HeatmapColumn
                {
                    Ts     = slots.TimestampOf(slot),
                    Counts = steps.AsSpan(slot * nBuckets, nBuckets).ToArray(),
                };
            }
            return new HeatmapResult { Bounds = bounds, Columns = cols, Unit = unit };
        }
        finally { Return(steps, clear: false); }
    }

    // ── Counter rate / increase ───────────────────────────────────────────────

    private static IReadOnlyList<MetricSeries> AggregateRate(
        IReadOnlyList<MetricSeries> raw, MetricQueryRequest req)
    {
        bool perSecond = req.Aggregation == MetricAggregation.Rate;

        // GROUPED: each series' rates go straight into its group's per-timestamp sum. They used to
        // be built as a series of their own first — a list of every rate point of every series —
        // only for the reduction to read them once and drop them. Same members (series with two
        // or more points), same group order (first seen among those), same per-timestamp order of
        // addition (member order, then point order), so the same sums.
        if (req.GroupBy is { Length: > 0 } groupBy)
        {
            var eligible = new List<MetricSeries>(raw.Count);
            foreach (var s in raw)
                if (s.Points.Count >= 2) eligible.Add(s);
            return ReduceByTimestamp(eligible, groupBy, ScalarReduce.Sum, perSecond ? RateMode.PerSecond : RateMode.Increase);
        }

        // UNGROUPED: every series is its own group and the reduction hands each one back as it is
        // — so the rate series ARE the answer, built once, as before.
        // For histograms, "rate" means rate of the sample count (requests/sec), not the
        // mean — so select the cumulative Count field instead of Value.
        var rateSeries = new List<MetricSeries>(raw.Count);
        foreach (var s in raw)
        {
            var pts = s.Points;
            if (pts.Count < 2) continue;
            bool useCount = s.Kind == MetricKind.Histogram;
            var outPts = new List<MetricDataPoint>(pts.Count - 1);
            int prev = HasRateInput(pts[0], useCount) ? 0 : -1;
            for (int i = 1; i < pts.Count; i++)
                outPts.Add(new MetricDataPoint
                {
                    TimestampUnixNano = pts[i].TimestampUnixNano,
                    Value             = RateAt(pts, i, ref prev, useCount, perSecond),
                });
            rateSeries.Add(new MetricSeries { Name = s.Name, Kind = s.Kind, Unit = s.Unit, Labels = s.Labels, Points = outPts });
        }
        return rateSeries;
    }

    /// <summary>
    /// The rate (or increase) at point <paramref name="i"/> — one per point after the first, as
    /// always — from the latest point before it that has a value (<paramref name="prev"/>, advanced
    /// here). A NaN or ±Infinity counter value is no measurement (#92): its own timestamp answers NaN
    /// (null on the wire, a gap), and the next finite point's rate runs from the last finite one
    /// before it. Taken as a value, a NaN read as a counter RESET on the point after it
    /// (x &gt;= NaN is false), and that point's whole cumulative value became one step's increase —
    /// a spike of the counter's lifetime total in every rate panel and every rate alert. With no
    /// finite point before it, a point's rate is NaN too. On finite points <paramref name="prev"/> is
    /// always i - 1: the answer is the one it always was.
    /// </summary>
    private static double RateAt(IReadOnlyList<MetricDataPoint> pts, int i, ref int prev, bool useCount, bool perSecond)
    {
        if (!HasRateInput(pts[i], useCount)) return double.NaN;
        double v = prev >= 0 ? RateBetween(pts[prev], pts[i], useCount, perSecond) : double.NaN;
        prev = i;
        return v;
    }

    /// <summary>Whether a point has a value a rate can use: always for a histogram (its cumulative
    /// count is an integer), and for a counter when its value is finite.</summary>
    private static bool HasRateInput(in MetricDataPoint p, bool useCount) => useCount || double.IsFinite(p.Value);

    /// <summary>The rate (or increase) from point <paramref name="prev"/> to point <paramref name="curr"/>.</summary>
    private static double RateBetween(in MetricDataPoint prev, in MetricDataPoint curr, bool useCount, bool perSecond)
    {
        double p = useCount ? prev.Count : prev.Value;
        double c = useCount ? curr.Count : curr.Value;
        double delta = ResetAwareDelta(p, c);
        double v = delta;
        if (perSecond)
        {
            double dtSec = (curr.TimestampUnixNano - prev.TimestampUnixNano) / 1e9;
            v = dtSec > 0 ? delta / dtSec : 0;
        }
        return v;
    }

    // ── Gauge / scalar reductions ─────────────────────────────────────────────

    private static IReadOnlyList<MetricSeries> AggregateScalar(
        IReadOnlyList<MetricSeries> raw, MetricQueryRequest req)
    {
        var op = req.Aggregation switch
        {
            MetricAggregation.Sum => ScalarReduce.Sum,
            MetricAggregation.Min => ScalarReduce.Min,
            MetricAggregation.Max => ScalarReduce.Max,
            MetricAggregation.Last => ScalarReduce.Last,
            _ => ScalarReduce.Avg,
        };

        if (op == ScalarReduce.Last)
        {
            // Instant: one point per group = sum of each series' LATEST point (#92). A series whose
            // latest point is NaN or ±Infinity has no current value, and contributes nothing — NOT an
            // older finite point, which may be minutes stale: an alert rule (this is its default
            // aggregation) would go pending and fire on a value the series no longer reports. A group
            // none of whose latest points is finite is NaN — null on the wire, and to the evaluator
            // "no value", which leaves the rule's state alone and says so once. Summed as it was, one
            // such series made the whole group NaN while the others had values.
            var groups  = Grouping.Of(raw, req.GroupBy);
            var outList = new List<MetricSeries>(groups.Count);
            for (int g = 0; g < groups.Count; g++)
            {
                var members = groups.MembersOf(g);
                long ts = 0; double sum = 0;
                bool counted = false, anyPoint = false;
                foreach (int m in members)
                {
                    var points = raw[m].Points;
                    if (points.Count == 0) continue;
                    anyPoint = true;
                    var last = points[^1];
                    if (last.TimestampUnixNano > ts) ts = last.TimestampUnixNano;
                    if (!double.IsFinite(last.Value)) continue;
                    sum += last.Value;
                    counted = true;
                }
                // Points, and no finite latest one: NaN, never the 0 the empty sum reads as. A group
                // with no points at all keeps its 0, as it always did.
                if (anyPoint && !counted) sum = double.NaN;
                var first = raw[members[0]];
                outList.Add(new MetricSeries
                {
                    Name = req.Metric, Kind = first.Kind, Unit = first.Unit, Labels = groups.Labels[g],
                    Points = [new MetricDataPoint { TimestampUnixNano = ts, Value = sum }],
                });
            }
            return outList;
        }

        return ReduceByTimestamp(raw, req.GroupBy, op, RateMode.None);
    }

    // ── Histogram percentiles ─────────────────────────────────────────────────

    private static IReadOnlyList<MetricSeries> AggregateQuantile(
        IReadOnlyList<MetricSeries> raw, MetricQueryRequest req)
    {
        double q = Math.Clamp(req.Quantile ?? 0.95, 0, 1);

        var withBounds = new List<MetricSeries>(raw.Count);
        foreach (var s in raw)
            if (s.BucketBounds is { Length: > 0 }) withBounds.Add(s);
        var groups = Grouping.Of(withBounds, req.GroupBy);

        var outList = new List<MetricSeries>(groups.Count);
        using var slots = new TimestampSlots();
        double[] steps = [];
        try
        {
            for (int g = 0; g < groups.Count; g++)
            {
                var members = groups.MembersOf(g);
                var first   = withBounds[members[0]];
                var bounds  = first.BucketBounds!;
                int nBuckets = bounds.Length + 1;

                // per-step summed bucket deltas across members — one rented block of nBuckets
                // doubles per distinct step, where each step used to be its own new double[].
                slots.Reset();
                foreach (int m in members)
                {
                    var s = withBounds[m];
                    if (s.BucketBounds is null || s.BucketBounds.Length + 1 != nBuckets) continue;
                    AccumulateBucketDeltas(s.Points, slots, ref steps, nBuckets, tsDivisor: 1);
                }

                var sorted = slots.Sorted();
                var pts    = new List<MetricDataPoint>(sorted.Length);
                foreach (int slot in sorted)
                    pts.Add(new MetricDataPoint
                    {
                        TimestampUnixNano = slots.TimestampOf(slot),
                        Value             = HistogramQuantile(q, bounds, steps.AsSpan(slot * nBuckets, nBuckets)),
                    });

                outList.Add(new MetricSeries
                {
                    Name = req.Metric, Kind = MetricKind.Histogram, Unit = first.Unit, Labels = groups.Labels[g], Points = pts,
                });
            }
        }
        finally { Return(steps, clear: false); }
        return outList;
    }

    /// <summary>
    /// Adds a series' step-to-step bucket deltas into the step blocks of <paramref name="steps"/>,
    /// keyed by <c>ts / tsDivisor</c>. A point without buckets breaks the chain, as it always did;
    /// a step's block is zeroed the moment the step is first seen, which is the fresh
    /// <c>new double[nBuckets]</c> it used to be.
    /// </summary>
    private static void AccumulateBucketDeltas(
        IReadOnlyList<MetricDataPoint> points, TimestampSlots slots, ref double[] steps, int nBuckets, long tsDivisor)
    {
        long[]? prev = null;
        for (int i = 0; i < points.Count; i++)
        {
            if (points[i].BucketCounts is not { } cur) { prev = null; continue; }
            if (prev is not null)
            {
                int slot = slots.SlotOf(points[i].TimestampUnixNano / tsDivisor, out bool added);
                if (added)
                {
                    Grow(ref steps, slots.Count * nBuckets, clear: false);
                    steps.AsSpan(slot * nBuckets, nBuckets).Clear();
                }
                AddBucketDelta(steps.AsSpan(slot * nBuckets, nBuckets), prev, cur);
            }
            prev = cur;
        }
    }

    /// <summary>
    /// Prometheus-style histogram_quantile over per-bucket (non-cumulative) counts.
    /// Linearly interpolates within the bucket that crosses the q·total rank.
    /// </summary>
    public static double HistogramQuantile(double q, double[] bounds, double[] bucketCounts) =>
        HistogramQuantile(q, bounds, (ReadOnlySpan<double>)bucketCounts);

    /// <summary>As the array overload, over counts that live in a slice of a larger buffer.</summary>
    internal static double HistogramQuantile(double q, double[] bounds, ReadOnlySpan<double> bucketCounts)
    {
        int n = bucketCounts.Length;
        if (n == 0) return 0;

        // cumulative across buckets
        double total = 0;
        for (int i = 0; i < n; i++) total += bucketCounts[i];
        if (total <= 0) return 0;

        double rank = q * total;
        double cum  = 0;
        for (int i = 0; i < n; i++)
        {
            double prevCum = cum;
            cum += bucketCounts[i];
            if (cum < rank) continue;

            // bucket i spans (lower, upper]
            double lower = i == 0 ? 0 : bounds[i - 1];
            double upper = i < bounds.Length ? bounds[i] : (bounds.Length > 0 ? bounds[^1] : lower);
            if (i >= bounds.Length) return upper; // +Inf overflow → clamp to last finite bound
            double within = bucketCounts[i] > 0 ? (rank - prevCum) / bucketCounts[i] : 0;
            return lower + (upper - lower) * within;
        }
        return bounds.Length > 0 ? bounds[^1] : 0;
    }

    // ── Reduction helpers ─────────────────────────────────────────────────────

    private enum ScalarReduce { Sum, Avg, Min, Max, Last }

    /// <summary>What a reduction reads of each member point: its value, or its rate from the previous one.</summary>
    private enum RateMode { None, Increase, PerSecond }

    private struct Accumulator
    {
        public double Sum, Min, Max;
        public int    N;
    }

    /// <summary>
    /// Groups <paramref name="series"/> (by <paramref name="groupBy"/>, or each alone) and reduces
    /// each group per timestamp. Without a group-by a lone series is handed back as it came. With
    /// <paramref name="rate"/>, a member contributes the rate between each point and the one before
    /// it (<see cref="RateAt"/>) instead of its values — the grouped rate, computed as it is summed.
    /// </summary>
    private static IReadOnlyList<MetricSeries> ReduceByTimestamp(
        IReadOnlyList<MetricSeries> series, string[]? groupBy, ScalarReduce op, RateMode rate)
    {
        bool grouped = groupBy is { Length: > 0 };
        var groups   = Grouping.Of(series, groupBy);
        var outList  = new List<MetricSeries>(groups.Count);

        TimestampSlots? slots = null;
        Accumulator[]   acc   = [];
        try
        {
            for (int g = 0; g < groups.Count; g++)
            {
                var members = groups.MembersOf(g);
                if (members.Length == 1 && !grouped)
                {
                    outList.Add(series[members[0]]);
                    continue;
                }

                slots ??= new TimestampSlots();
                slots.Reset();
                foreach (int m in members)
                {
                    var s      = series[m];
                    var points = s.Points;
                    if (rate == RateMode.None)
                    {
                        for (int i = 0; i < points.Count; i++)
                            Accumulate(slots, ref acc, points[i].TimestampUnixNano, points[i].Value);
                    }
                    else
                    {
                        bool useCount = s.Kind == MetricKind.Histogram;
                        int  prev     = points.Count > 0 && HasRateInput(points[0], useCount) ? 0 : -1;
                        for (int i = 1; i < points.Count; i++)
                            Accumulate(slots, ref acc, points[i].TimestampUnixNano,
                                       RateAt(points, i, ref prev, useCount, rate == RateMode.PerSecond));
                    }
                }

                var sorted = slots.Sorted();
                var pts    = new List<MetricDataPoint>(sorted.Length);
                foreach (int slot in sorted)
                {
                    ref readonly var a = ref acc[slot];
                    double v = a.N == 0 ? double.NaN : op switch   // no member finite here: a gap
                    {
                        ScalarReduce.Sum => a.Sum,
                        ScalarReduce.Min => a.Min,
                        ScalarReduce.Max => a.Max,
                        _                => a.Sum / a.N, // Avg
                    };
                    pts.Add(new MetricDataPoint { TimestampUnixNano = slots.TimestampOf(slot), Value = v });
                }

                var first = series[members[0]];
                outList.Add(new MetricSeries
                {
                    Name = first.Name, Kind = first.Kind, Unit = first.Unit, Labels = groups.Labels[g], Points = pts,
                });
            }
        }
        finally
        {
            slots?.Dispose();
            Return(acc, clear: false);
        }
        return outList;
    }

    /// <summary>
    /// One value into its timestamp's accumulator: the first FINITE one seeds sum, min and max with
    /// itself (not 0 + v — a lone -0.0 sums to -0.0), every later one adds, in the order offered.
    ///
    /// <para><b>A NaN or ±Infinity is no measurement and is not accumulated</b> (#92) — the rule the
    /// response writer (<c>null</c>) and the alert evaluator (skipped) already apply to a point. Folded
    /// in, one exporter's NaN made the whole group's sum, average, min and max NaN at every timestamp
    /// it touched: a fleet panel went blank for one pod, and an alert rule over it saw no value at
    /// all. Its timestamp still gets a slot: a timestamp where no member has a finite value is
    /// answered as NaN (<c>null</c>, a gap), not dropped and not 0.</para>
    /// </summary>
    private static void Accumulate(TimestampSlots slots, ref Accumulator[] acc, long ts, double v)
    {
        int slot = slots.SlotOf(ts, out bool added);
        if (added)
        {
            Grow(ref acc, slots.Count, clear: false);
            acc[slot] = default;                          // N = 0: no finite value yet
        }
        if (!double.IsFinite(v)) return;
        ref var a = ref acc[slot];
        if (a.N == 0)
        {
            a = new Accumulator { Sum = v, Min = v, Max = v, N = 1 };
            return;
        }
        a.Sum += v;
        a.Min  = Math.Min(a.Min, v);
        a.Max  = Math.Max(a.Max, v);
        a.N   += 1;
    }

    /// <summary>
    /// Merges series that share the same full label set into one continuous series.
    /// Storage returns one <see cref="MetricSeries"/> per segment (hot + each cold file),
    /// so a single logical series arrives as several time-disjoint fragments; without this
    /// they would render as separate lines. Points are concatenated and sorted by time
    /// (later fragment wins on exact-timestamp collisions — and inside one fragment, the later
    /// point: each point OVERWRITES its timestamp's slot, in fragment order then point order).
    /// </summary>
    private static List<MetricSeries> MergeFragments(IReadOnlyList<MetricSeries> raw)
    {
        var groups = Grouping.ByFullLabels(raw);
        var result = new List<MetricSeries>(groups.Count);

        TimestampSlots?   slots   = null;
        MetricDataPoint[] winners = [];
        try
        {
            for (int g = 0; g < groups.Count; g++)
            {
                var members = groups.MembersOf(g);
                if (members.Length == 1) { result.Add(raw[members[0]]); continue; }

                slots ??= new TimestampSlots();
                slots.Reset();
                double[]? bounds = null;
                foreach (int m in members)
                {
                    var fragment = raw[m];
                    bounds ??= fragment.BucketBounds;
                    var points = fragment.Points;
                    for (int i = 0; i < points.Count; i++)
                    {
                        int slot = slots.SlotOf(points[i].TimestampUnixNano, out bool added);
                        if (added) Grow(ref winners, slots.Count, clear: true);
                        winners[slot] = points[i];   // later wins
                    }
                }

                var sorted = slots.Sorted();
                var merged = new List<MetricDataPoint>(sorted.Length);
                foreach (int slot in sorted) merged.Add(winners[slot]);

                var first = raw[members[0]];
                result.Add(new MetricSeries
                {
                    Name = first.Name, Kind = first.Kind, Unit = first.Unit,
                    Labels = groups.Labels[g], BucketBounds = bounds, Points = merged,
                });
            }
        }
        finally
        {
            slots?.Dispose();
            Return(winners, clear: true);    // the points hold bucket arrays
        }
        return result;
    }

    private static LabelSet ReduceLabels(LabelSet labels, string[] keep, string[] scratch)
    {
        // The pairs are in canonical order and a subset of them keeps it, so the reduced set is
        // built straight from the kept strings — no pair list, no sort. A key repeated in a set
        // stored before ingest collapsed repeats (#92) keeps the last of its run — the ordinal-greatest
        // value, the one the answer writes and a filter matches — so such a series groups with the
        // series it reads as.
        var kv = labels.Interleaved;
        int n  = 0;
        for (int i = 0; i < kv.Length; i += 2)
            if (Array.IndexOf(keep, kv[i]) >= 0 && !(i + 2 < kv.Length && string.Equals(kv[i], kv[i + 2])))
            {
                scratch[n++] = kv[i];
                scratch[n++] = kv[i + 1];
            }
        return n == 0 ? LabelSet.Empty : LabelSet.FromSorted(scratch.AsSpan(0, n));
    }

    private static double ResetAwareDelta(double prev, double curr)
        => curr >= prev ? curr - prev : curr; // counter reset → treat curr as the increase

    private static void AddBucketDelta(Span<double> acc, long[] prev, long[] cur)
    {
        int n = Math.Min(acc.Length, Math.Min(prev.Length, cur.Length));
        // reset detection on the total
        long prevTotal = 0, curTotal = 0;
        for (int i = 0; i < n; i++) { prevTotal += prev[i]; curTotal += cur[i]; }
        bool reset = curTotal < prevTotal;
        for (int i = 0; i < n; i++)
        {
            long d = reset ? cur[i] : cur[i] - prev[i];
            if (d > 0) acc[i] += d;
        }
    }

    /// <summary>
    /// The value top-K ranks a series by: its latest point's value — as "last" reads it, never an
    /// older one (#92). A non-finite latest value is no value and ranks below every value (NaN, which
    /// the default comparer orders first, so a descending sort puts it last — where a NaN always
    /// ranked; an infinity used to rank at the top). An empty series ranks as 0, as it always did.
    /// </summary>
    private static double LastValue(MetricSeries s)
    {
        if (s.Points.Count == 0) return 0;
        double v = s.Points[^1].Value;
        return double.IsFinite(v) ? v : double.NaN;
    }

    // ── Rented per-slot storage ───────────────────────────────────────────────

    /// <summary>Makes <paramref name="array"/> hold at least <paramref name="needed"/> items, keeping what it holds.</summary>
    private static void Grow<T>(ref T[] array, int needed, bool clear)
    {
        if (array.Length >= needed) return;
        var bigger = ArrayPool<T>.Shared.Rent(Math.Max(needed, Math.Max(16, array.Length * 2)));
        array.AsSpan().CopyTo(bigger);
        Return(array, clear);
        array = bigger;
    }

    private static void Return<T>(T[] array, bool clear)
    {
        if (array.Length > 0) ArrayPool<T>.Shared.Return(array, clearArray: clear);
    }

    /// <summary>
    /// Series grouped by label set — each alone, by their full labels (the fragment merge), or by
    /// the labels a group-by keeps — with the groups in FIRST-SEEN order and each group's members
    /// in input order, which is what the dictionaries of lists this replaces enumerated. A group's
    /// label set is the first instance seen for it.
    /// </summary>
    private sealed class Grouping
    {
        public readonly List<LabelSet> Labels;
        private readonly int[] _start;     // group g's members are _members[_start[g] .. _start[g + 1])
        private readonly int[] _members;

        private Grouping(List<LabelSet> labels, int[] groupOf)
        {
            Labels   = labels;
            _start   = new int[labels.Count + 1];
            _members = new int[groupOf.Length];
            foreach (int g in groupOf) _start[g + 1]++;
            for (int g = 0; g < labels.Count; g++) _start[g + 1] += _start[g];
            var fill = new int[labels.Count];
            for (int i = 0; i < groupOf.Length; i++)
            {
                int g = groupOf[i];
                _members[_start[g] + fill[g]++] = i;
            }
        }

        public int Count => Labels.Count;

        public ReadOnlySpan<int> MembersOf(int g) => _members.AsSpan(_start[g], _start[g + 1] - _start[g]);

        /// <summary>By the labels <paramref name="groupBy"/> keeps; each series alone when there is no group-by.</summary>
        public static Grouping Of(IReadOnlyList<MetricSeries> series, string[]? groupBy)
        {
            if (groupBy is null || groupBy.Length == 0)
            {
                var labels  = new List<LabelSet>(series.Count);
                var groupOf = new int[series.Count];
                for (int i = 0; i < series.Count; i++) { labels.Add(series[i].Labels); groupOf[i] = i; }
                return new Grouping(labels, groupOf);
            }

            var scratch = ArrayPool<string>.Shared.Rent(2 * MaxReducedPairs);
            try
            {
                return Build(series, groupBy, scratch);
            }
            finally
            {
                Array.Clear(scratch);
                ArrayPool<string>.Shared.Return(scratch);
            }
        }

        /// <summary>By full label set: the fragments of one series together.</summary>
        public static Grouping ByFullLabels(IReadOnlyList<MetricSeries> series) => Build(series, null, null);

        private static Grouping Build(IReadOnlyList<MetricSeries> series, string[]? groupBy, string[]? scratch)
        {
            var labels  = new List<LabelSet>();
            var groupOf = new int[series.Count];
            var map     = new Dictionary<LabelSet, int>();
            for (int i = 0; i < series.Count; i++)
            {
                var own = series[i].Labels;
                var key = groupBy is null ? own
                        : own.Count <= MaxReducedPairs ? ReduceLabels(own, groupBy, scratch!)
                        : ReduceLabelsLarge(own, groupBy);
                if (!map.TryGetValue(key, out int g))
                {
                    g = labels.Count;
                    map[key] = g;
                    labels.Add(key);
                }
                groupOf[i] = g;
            }
            return new Grouping(labels, groupOf);
        }

        /// <summary>Pairs a label set may hold for its reduction to use the rented scratch.</summary>
        private const int MaxReducedPairs = 64;

        private static LabelSet ReduceLabelsLarge(LabelSet labels, string[] keep)
        {
            var scratch = new string[labels.Interleaved.Length];
            return ReduceLabels(labels, keep, scratch);
        }
    }

    /// <summary>
    /// Distinct timestamps numbered 0, 1, 2 … in the order they are first offered, on rented
    /// arrays — what the <c>SortedDictionary&lt;long, …&gt;</c> of each reduction was for, without
    /// a red-black node per timestamp. Callers keep their per-slot state in their own rented
    /// arrays, indexed by slot, and walk <see cref="Sorted"/> once at the end; the timestamps are
    /// distinct, so that sort has no tie to break. Open addressing, linear probing, at most half
    /// full.
    /// </summary>
    private sealed class TimestampSlots : IDisposable
    {
        private long[] _keys   = [];   // table: the timestamp in a used cell
        private int[]  _cells  = [];   // table: slot + 1 in a used cell, 0 when empty
        private long[] _bySlot = [];   // slot → timestamp
        private int[]  _cellOf = [];   // slot → the table cell it occupies, so a reset clears only those
        private int[]  _sorted = [];   // Sorted()'s answer
        private long[] _sortKeys = [];
        private int    _mask   = -1;
        private int    _count;

        public int Count => _count;

        public long TimestampOf(int slot) => _bySlot[slot];

        /// <summary>
        /// Empties the table for the next group, clearing only the cells this group used. The table
        /// keeps the size its largest group needed, so clearing all of it made every later group pay
        /// for that one: a 200 000-point series followed by 2 000 small groups was a 4 MB memset
        /// per group.
        /// </summary>
        public void Reset()
        {
            for (int s = 0; s < _count; s++) _cells[_cellOf[s]] = 0;
            _count = 0;
        }

        /// <summary>The slot of <paramref name="ts"/>, numbering it next when it is new.</summary>
        public int SlotOf(long ts, out bool added)
        {
            if ((_count + 1) * 2 > _mask + 1) Resize();

            int i = Hash(ts) & _mask;
            while (true)
            {
                int cell = _cells[i];
                if (cell == 0)
                {
                    int slot = _count++;
                    _keys[i]  = ts;
                    _cells[i] = slot + 1;
                    Grow(ref _bySlot, _count, clear: false);
                    Grow(ref _cellOf, _count, clear: false);
                    _bySlot[slot] = ts;
                    _cellOf[slot] = i;
                    added = true;
                    return slot;
                }
                if (_keys[i] == ts) { added = false; return cell - 1; }
                i = (i + 1) & _mask;
            }
        }

        /// <summary>The slots in ascending timestamp order. Valid until the next change.</summary>
        public ReadOnlySpan<int> Sorted()
        {
            Grow(ref _sorted, _count, clear: false);
            Grow(ref _sortKeys, _count, clear: false);
            for (int s = 0; s < _count; s++) { _sorted[s] = s; _sortKeys[s] = _bySlot[s]; }
            Array.Sort(_sortKeys, _sorted, 0, _count);
            return _sorted.AsSpan(0, _count);
        }

        private static int Hash(long ts)
        {
            ulong h = unchecked((ulong)ts * 0x9E3779B97F4A7C15UL);
            return (int)(h >> 32) ^ (int)h;
        }

        private void Resize()
        {
            int size = Math.Max(64, (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, (_count + 1) * 4)));
            Return(_keys, clear: false);
            Return(_cells, clear: false);
            _keys  = ArrayPool<long>.Shared.Rent(size);
            _cells = ArrayPool<int>.Shared.Rent(size);
            _cells.AsSpan(0, size).Clear();
            _mask  = size - 1;

            for (int slot = 0; slot < _count; slot++)
            {
                long ts = _bySlot[slot];
                int i = Hash(ts) & _mask;
                while (_cells[i] != 0) i = (i + 1) & _mask;
                _keys[i]  = ts;
                _cells[i] = slot + 1;
                _cellOf[slot] = i;
            }
        }

        public void Dispose()
        {
            Return(_keys, clear: false);
            Return(_cells, clear: false);
            Return(_bySlot, clear: false);
            Return(_cellOf, clear: false);
            Return(_sorted, clear: false);
            Return(_sortKeys, clear: false);
            _keys = []; _cells = []; _bySlot = []; _cellOf = []; _sorted = []; _sortKeys = [];
            _mask = -1; _count = 0;
        }
    }
}
