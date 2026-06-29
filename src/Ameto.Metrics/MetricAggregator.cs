namespace Ameto.Metrics;

/// <summary>
/// Server-side metric aggregation: reset-aware counter rate/increase, gauge
/// reductions, histogram percentiles (histogram_quantile), group-by, top-K, and
/// latency heatmaps. Built on <see cref="IMetricQuery"/> so it stays independent of
/// the on-disk format.
/// </summary>
public sealed class MetricAggregator : IMetricAggregator
{
    private readonly IMetricQuery _query;

    public MetricAggregator(IMetricQuery query) => _query = query;

    public async Task<IReadOnlyList<MetricSeries>> QueryAsync(
        MetricQueryRequest request,
        CancellationToken  ct = default)
    {
        var fragments = new List<MetricSeries>();
        await foreach (var s in _query.QueryAsync(request.Metric, request.From, request.To, request.Step, request.Filters, ct))
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
        var left  = SumToSingle(await QueryAsync(req.Left, ct));
        var right = SumToSingle(await QueryAsync(req.Right, ct));

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

    /// <summary>Reduces a multi-series result to (ts → summed value) pairs ordered by time.</summary>
    private static List<(long Ts, double Value)> SumToSingle(IReadOnlyList<MetricSeries> series)
    {
        var byTs = new SortedDictionary<long, double>();
        foreach (var s in series)
            foreach (var p in s.Points)
                byTs[p.TimestampUnixNano] = byTs.GetValueOrDefault(p.TimestampUnixNano) + p.Value;
        return byTs.Select(kv => (kv.Key, kv.Value)).ToList();
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

        // Accumulate per-step bucket-count deltas, summed across all matching series.
        var columns = new SortedDictionary<long, double[]>();
        foreach (var series in raw)
        {
            if (series.BucketBounds is null || series.BucketBounds.Length + 1 != nBuckets) continue;
            long[]? prev = null;
            foreach (var p in series.Points)
            {
                if (p.BucketCounts is not { } cur) { prev = null; continue; }
                if (prev is not null)
                {
                    long tsMs = p.TimestampUnixNano / 1_000_000L;
                    if (!columns.TryGetValue(tsMs, out var col))
                    {
                        col = new double[nBuckets];
                        columns[tsMs] = col;
                    }
                    AddBucketDelta(col, prev, cur);
                }
                prev = cur;
            }
        }

        var cols = columns
            .Select(kv => new HeatmapColumn { Ts = kv.Key, Counts = kv.Value })
            .ToArray();

        return new HeatmapResult { Bounds = bounds, Columns = cols, Unit = unit };
    }

    // ── Counter rate / increase ───────────────────────────────────────────────

    private static IReadOnlyList<MetricSeries> AggregateRate(
        IReadOnlyList<MetricSeries> raw, MetricQueryRequest req)
    {
        bool perSecond = req.Aggregation == MetricAggregation.Rate;

        // 1. per-series rate points.
        // For histograms, "rate" means rate of the sample count (requests/sec), not the
        // mean — so select the cumulative Count field instead of Value.
        var rateSeries = new List<MetricSeries>(raw.Count);
        foreach (var s in raw)
        {
            var pts = s.Points;
            if (pts.Count < 2) continue;
            bool useCount = s.Kind == MetricKind.Histogram;
            var outPts = new List<MetricDataPoint>(pts.Count - 1);
            for (int i = 1; i < pts.Count; i++)
            {
                double prev = useCount ? pts[i - 1].Count : pts[i - 1].Value;
                double curr = useCount ? pts[i].Count     : pts[i].Value;
                double delta = ResetAwareDelta(prev, curr);
                double v = delta;
                if (perSecond)
                {
                    double dtSec = (pts[i].TimestampUnixNano - pts[i - 1].TimestampUnixNano) / 1e9;
                    v = dtSec > 0 ? delta / dtSec : 0;
                }
                outPts.Add(new MetricDataPoint { TimestampUnixNano = pts[i].TimestampUnixNano, Value = v });
            }
            rateSeries.Add(new MetricSeries { Name = s.Name, Kind = s.Kind, Unit = s.Unit, Labels = s.Labels, Points = outPts });
        }

        // 2. group-reduce by SUM (rates add)
        return ReduceByTimestamp(rateSeries, req.GroupBy, ScalarReduce.Sum);
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
            // Instant: one point per group = sum of each series' latest value.
            var groups = GroupSeries(raw, req.GroupBy);
            var outList = new List<MetricSeries>(groups.Count);
            foreach (var (labels, members) in groups)
            {
                long ts = 0; double sum = 0;
                foreach (var m in members)
                {
                    if (m.Points.Count == 0) continue;
                    var last = m.Points[^1];
                    sum += last.Value;
                    if (last.TimestampUnixNano > ts) ts = last.TimestampUnixNano;
                }
                outList.Add(new MetricSeries
                {
                    Name = req.Metric, Kind = members[0].Kind, Unit = members[0].Unit, Labels = labels,
                    Points = [new MetricDataPoint { TimestampUnixNano = ts, Value = sum }],
                });
            }
            return outList;
        }

        return ReduceByTimestamp(raw, req.GroupBy, op);
    }

    // ── Histogram percentiles ─────────────────────────────────────────────────

    private static IReadOnlyList<MetricSeries> AggregateQuantile(
        IReadOnlyList<MetricSeries> raw, MetricQueryRequest req)
    {
        double q = Math.Clamp(req.Quantile ?? 0.95, 0, 1);

        var groups = GroupSeries(
            raw.Where(s => s.BucketBounds is { Length: > 0 }).ToList(),
            req.GroupBy);

        var outList = new List<MetricSeries>(groups.Count);
        foreach (var (labels, members) in groups)
        {
            var bounds = members[0].BucketBounds!;
            int nBuckets = bounds.Length + 1;

            // per-step summed bucket deltas across members
            var perStep = new SortedDictionary<long, double[]>();
            foreach (var s in members)
            {
                if (s.BucketBounds is null || s.BucketBounds.Length + 1 != nBuckets) continue;
                long[]? prev = null;
                foreach (var p in s.Points)
                {
                    if (p.BucketCounts is not { } cur) { prev = null; continue; }
                    if (prev is not null)
                    {
                        if (!perStep.TryGetValue(p.TimestampUnixNano, out var acc))
                        {
                            acc = new double[nBuckets];
                            perStep[p.TimestampUnixNano] = acc;
                        }
                        AddBucketDelta(acc, prev, cur);
                    }
                    prev = cur;
                }
            }

            var pts = new List<MetricDataPoint>(perStep.Count);
            foreach (var (ts, counts) in perStep)
                pts.Add(new MetricDataPoint { TimestampUnixNano = ts, Value = HistogramQuantile(q, bounds, counts) });

            outList.Add(new MetricSeries
            {
                Name = req.Metric, Kind = MetricKind.Histogram, Unit = members[0].Unit, Labels = labels, Points = pts,
            });
        }
        return outList;
    }

    /// <summary>
    /// Prometheus-style histogram_quantile over per-bucket (non-cumulative) counts.
    /// Linearly interpolates within the bucket that crosses the q·total rank.
    /// </summary>
    public static double HistogramQuantile(double q, double[] bounds, double[] bucketCounts)
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

    private static IReadOnlyList<MetricSeries> ReduceByTimestamp(
        IReadOnlyList<MetricSeries> series, string[]? groupBy, ScalarReduce op)
    {
        var groups = GroupSeries(series, groupBy);
        var outList = new List<MetricSeries>(groups.Count);

        foreach (var (labels, members) in groups)
        {
            if (members.Count == 1 && (groupBy is null || groupBy.Length == 0))
            {
                outList.Add(members[0]);
                continue;
            }

            var acc = new SortedDictionary<long, (double sum, double min, double max, int n)>();
            foreach (var s in members)
                foreach (var p in s.Points)
                {
                    acc.TryGetValue(p.TimestampUnixNano, out var a);
                    a = a.n == 0
                        ? (p.Value, p.Value, p.Value, 1)
                        : (a.sum + p.Value, Math.Min(a.min, p.Value), Math.Max(a.max, p.Value), a.n + 1);
                    acc[p.TimestampUnixNano] = a;
                }

            var pts = new List<MetricDataPoint>(acc.Count);
            foreach (var (ts, a) in acc)
            {
                double v = op switch
                {
                    ScalarReduce.Sum => a.sum,
                    ScalarReduce.Min => a.min,
                    ScalarReduce.Max => a.max,
                    _                => a.sum / a.n, // Avg
                };
                pts.Add(new MetricDataPoint { TimestampUnixNano = ts, Value = v });
            }

            outList.Add(new MetricSeries
            {
                Name = members[0].Name, Kind = members[0].Kind, Unit = members[0].Unit, Labels = labels, Points = pts,
            });
        }
        return outList;
    }

    private static List<(LabelSet Labels, List<MetricSeries> Members)> GroupSeries(
        IReadOnlyList<MetricSeries> series, string[]? groupBy)
    {
        if (groupBy is null || groupBy.Length == 0)
            return series.Select(s => (s.Labels, new List<MetricSeries> { s })).ToList();

        var map = new Dictionary<LabelSet, List<MetricSeries>>();
        foreach (var s in series)
        {
            var reduced = ReduceLabels(s.Labels, groupBy);
            if (!map.TryGetValue(reduced, out var list))
            {
                list = new List<MetricSeries>();
                map[reduced] = list;
            }
            list.Add(s);
        }
        return map.Select(kv => (kv.Key, kv.Value)).ToList();
    }

    /// <summary>
    /// Merges series that share the same full label set into one continuous series.
    /// Storage returns one <see cref="MetricSeries"/> per segment (hot + each cold file),
    /// so a single logical series arrives as several time-disjoint fragments; without this
    /// they would render as separate lines. Points are concatenated and sorted by time
    /// (later fragment wins on exact-timestamp collisions).
    /// </summary>
    private static List<MetricSeries> MergeFragments(IReadOnlyList<MetricSeries> raw)
    {
        var groups = new Dictionary<LabelSet, List<MetricSeries>>();
        foreach (var s in raw)
        {
            if (!groups.TryGetValue(s.Labels, out var list))
            {
                list = new List<MetricSeries>();
                groups[s.Labels] = list;
            }
            list.Add(s);
        }

        var result = new List<MetricSeries>(groups.Count);
        foreach (var (labels, members) in groups)
        {
            if (members.Count == 1) { result.Add(members[0]); continue; }

            var byTs = new SortedDictionary<long, MetricDataPoint>();
            double[]? bounds = null;
            foreach (var m in members)
            {
                bounds ??= m.BucketBounds;
                foreach (var p in m.Points) byTs[p.TimestampUnixNano] = p; // later wins
            }

            result.Add(new MetricSeries
            {
                Name = members[0].Name, Kind = members[0].Kind, Unit = members[0].Unit,
                Labels = labels, BucketBounds = bounds, Points = byTs.Values.ToList(),
            });
        }
        return result;
    }

    private static LabelSet ReduceLabels(LabelSet labels, string[] keep)
    {
        var pairs = new List<KeyValuePair<string, string>>(keep.Length);
        foreach (var (k, v) in labels.Pairs)
            if (Array.IndexOf(keep, k) >= 0)
                pairs.Add(new KeyValuePair<string, string>(k, v));
        return pairs.Count == 0 ? LabelSet.Empty : new LabelSet(pairs);
    }

    private static double ResetAwareDelta(double prev, double curr)
        => curr >= prev ? curr - prev : curr; // counter reset → treat curr as the increase

    private static void AddBucketDelta(double[] acc, long[] prev, long[] cur)
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

    private static double LastValue(MetricSeries s)
        => s.Points.Count > 0 ? s.Points[^1].Value : 0;
}
