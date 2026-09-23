using System.Diagnostics;
using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT ONE COLD METRIC QUERY COSTS (issue #83 WP7, M#4 / M#9). The reconnaissance measured 30 MB
/// allocated and 111 ms of CPU to answer a query over 48 KB of disk — 625x — and the alert
/// evaluator runs one per enabled rule every 15 s, for the life of the process; on the 384 MB
/// stand heap one such query is 9 % of the limit.
///
/// <para>The engine is COLD-LOADED: the corpus is written to <c>.mts</c> files first and the engine
/// is opened over them, so every query below reads, inflates and decodes files — the path the
/// alert evaluator takes once the hot tier has flushed. The corpus is the reconnaissance's shape:
/// 2 000 series of a 16-bucket histogram with five HTTP labels, 60 points each, 15 s apart, and a
/// busy service — every scrape observes something, so every point carries its own bucket array
/// (the worst case for bytes; an idle histogram shares one array across its slim points).</para>
///
/// <para>Measured on the calling thread (<see cref="GC.GetAllocatedBytesForCurrentThread"/>), and
/// the query must complete synchronously to be measured at all — which it does, the reader has no
/// true await — so nothing it allocates can land on a pool thread out of view. Best of
/// <see cref="Runs"/>, with nothing written to the test output inside a measurement.</para>
/// </summary>
public sealed class MetricQueryAllocProbe
{
    private readonly ITestOutputHelper _out;
    public MetricQueryAllocProbe(ITestOutputHelper output) => _out = output;

    private const int  SeriesCount     = 2_000;
    private const int  PointsPerSeries = 60;
    private const int  Runs            = 5;
    private const long S               = 1_000_000_000L;
    private const string Metric        = "http.server.request.duration";

    private static readonly double[] Bounds =
        [0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];

    private static readonly string[] Methods = ["GET", "POST", "PUT", "DELETE"];

    private static List<MetricSegmentInfo> WriteCorpus(string dir, long t0)
    {
        var items = new List<(SeriesKey, HotSeries)>(SeriesCount);
        for (int s = 0; s < SeriesCount; s++)
        {
            var labels = new LabelSet(new Dictionary<string, string>
            {
                ["service.name"]              = "svc-" + (s % 10).ToString(CultureInfo.InvariantCulture),
                ["http.route"]                = "/api/v1/resource" + (s % 20).ToString(CultureInfo.InvariantCulture),
                ["http.request.method"]       = Methods[s % Methods.Length],
                ["http.response.status_code"] = s % 7 == 0 ? "500" : "200",
                ["server.address"]            = "host-" + (s / 20).ToString(CultureInfo.InvariantCulture),
            });

            var pts     = new List<MetricDataPoint>(PointsPerSeries);
            var cum     = new long[Bounds.Length + 1];
            long count  = 0;
            double sum  = 0;
            for (int p = 0; p < PointsPerSeries; p++)
            {
                // A busy series: every scrape adds observations to a couple of buckets.
                int b = (s + p) % cum.Length;
                cum[b] += 1 + (p % 3);
                cum[(b + 5) % cum.Length] += 1;
                count  += 2 + (p % 3);
                sum    += 0.01 * (b + 1);
                pts.Add(new MetricDataPoint
                {
                    TimestampUnixNano = t0 + p * 15 * S,
                    Value             = sum / count,
                    Count             = count,
                    Sum               = sum,
                    BucketCounts      = (long[])cum.Clone(),
                });
            }
            items.Add((new SeriesKey(Metric, MetricKind.Histogram, "s", labels), new HotSeries(pts, Bounds)));
        }
        return MetricWriter.Write(dir, items, MetricGranularity.Raw);
    }

    private readonly record struct Cost(long Bytes, double Ms, int Series, long Points);

    [Fact]
    public async Task Probe_cold_query_cost_per_stored_series()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mqprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            long t0    = DateTimeOffset.UtcNow.AddHours(-3).ToUnixTimeMilliseconds() / 60_000 * 60_000 * 1_000_000L;
            var  files = WriteCorpus(dir, t0);
            long disk  = files.Sum(f => f.SizeBytes);
            long stored = (long)SeriesCount * PointsPerSeries;

            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance,
                                                             new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });
            await engine.ColdLoadCompleted;
            var agg = new MetricAggregator(engine);

            var from = DateTimeOffset.FromUnixTimeMilliseconds(t0 / 1_000_000L).AddMinutes(-1);
            var to   = from.AddHours(1);

            var raw      = Measure(() => Drain(engine.QueryAsync(Metric, from, to)));
            var rawLean  = Measure(() => Drain(engine.QueryAsync(Metric, from, to, null, null, MetricPointFields.NoBuckets)));
            var stepped  = Measure(() => Drain(engine.QueryAsync(Metric, from, to, TimeSpan.FromMinutes(1))));
            var recent   = Measure(() => Drain(engine.QueryAsync(Metric, from.AddMinutes(11), to)));
            var rate     = Measure(() => Sync(agg.QueryAsync(new MetricQueryRequest
            {
                Metric = Metric, From = from, To = to, Aggregation = MetricAggregation.Rate, GroupBy = ["service.name"],
            })));
            var quantile = Measure(() => Sync(agg.QueryAsync(new MetricQueryRequest
            {
                Metric = Metric, From = from, To = to, Aggregation = MetricAggregation.Quantile, Quantile = 0.95,
            })));
            var last     = Measure(() => Sync(agg.QueryAsync(new MetricQueryRequest
            {
                Metric = Metric, From = from, To = to, Aggregation = MetricAggregation.Last,
                Filters = new Dictionary<string, string> { ["http.request.method"] = "GET" },
            })));

            _out.WriteLine($"COLD QUERY  {SeriesCount:N0} histogram series x {PointsPerSeries} points = {stored:N0} points, " +
                           $"{files.Count} .mts file(s), {disk / 1024.0:N1} KB on disk; best of {Runs}");
            Print("QueryAsync raw (no step)       ", raw, stored);
            Print("QueryAsync raw, no buckets     ", rawLean, stored);
            Print("QueryAsync raw, step 1m        ", stepped, stored);
            Print("QueryAsync raw, last 5 minutes ", recent, stored);
            Print("Aggregator Rate by service.name", rate, stored);
            Print("Aggregator Quantile p95        ", quantile, stored);
            Print("Aggregator Last + method=GET   ", last, stored);

            // The answers, so a cheaper query is still the same query.
            Assert.Equal((SeriesCount, stored), (raw.Series, raw.Points));
            Assert.Equal((SeriesCount, stored), (rawLean.Series, rawLean.Points));
            Assert.Equal((SeriesCount, (long)SeriesCount * 15), (stepped.Series, stepped.Points));
            Assert.Equal((SeriesCount, (long)SeriesCount * 20), (recent.Series, recent.Points));
            Assert.Equal((10, 10L * (PointsPerSeries - 1)), (rate.Series, rate.Points));
            Assert.Equal((SeriesCount, (long)SeriesCount * (PointsPerSeries - 1)), (quantile.Series, quantile.Points));
            Assert.Equal((SeriesCount / Methods.Length, (long)SeriesCount / Methods.Length), (last.Series, last.Points));
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>Hands the aggregator the same fragments on every query — the merge, measured alone.</summary>
    private sealed class FixedFragments(List<MetricSeries> fragments) : IMetricQuery
    {
        public IEnumerable<string> GetMetricNames(string? prefix = null) => [];

        public async IAsyncEnumerable<MetricSeries> QueryAsync(
            string metricName, DateTimeOffset? from = null, DateTimeOffset? to = null, TimeSpan? step = null,
            IReadOnlyDictionary<string, string>? labelMatchers = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.CompletedTask;
            foreach (var f in fragments) yield return f;
        }

        public async IAsyncEnumerable<MetricSeries> GetLatestAsync(
            string metricName, IReadOnlyDictionary<string, string>? labelMatchers = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    /// <summary>
    /// The aggregator's own share, with storage out of the picture: 2 000 series arriving as three
    /// fragments each (the hot tier and two cold files, the hot one first and newest, the cold ones
    /// overlapping at their edges) — what MergeFragments and the reductions allocate beyond the
    /// fragments they are handed.
    /// </summary>
    [Fact]
    public void Probe_fragment_merge_and_reductions()
    {
        var fragments = new List<MetricSeries>();
        long t0 = 1_784_800_020_000_000_000L;
        for (int part = 0; part < 3; part++)
            for (int s = 0; s < SeriesCount; s++)
            {
                var labels = new LabelSet(new Dictionary<string, string>
                {
                    ["service.name"] = "svc-" + (s % 10).ToString(CultureInfo.InvariantCulture),
                    ["replica"]      = s.ToString(CultureInfo.InvariantCulture),
                });
                // part 0 = the newest (hot, first); parts 1 and 2 older, overlapping by one point.
                int first = part == 0 ? 40 : part == 1 ? 0 : 19;
                var pts = new List<MetricDataPoint>(21);
                for (int p = first; p < first + 21 && p < 60; p++)
                    pts.Add(new MetricDataPoint { TimestampUnixNano = t0 + p * 15 * S, Value = p + s });
                fragments.Add(new MetricSeries { Name = Metric, Kind = MetricKind.Counter, Unit = "1", Labels = labels, Points = pts });
            }
        long handed = fragments.Sum(f => (long)f.Points.Count);
        var agg = new MetricAggregator(new FixedFragments(fragments));

        var none = Measure(() => Sync(agg.QueryAsync(new MetricQueryRequest { Metric = Metric })));
        var sum  = Measure(() => Sync(agg.QueryAsync(new MetricQueryRequest
        {
            Metric = Metric, Aggregation = MetricAggregation.Sum, GroupBy = ["service.name"],
        })));
        var rate = Measure(() => Sync(agg.QueryAsync(new MetricQueryRequest
        {
            Metric = Metric, Aggregation = MetricAggregation.Rate, GroupBy = ["service.name"],
        })));

        _out.WriteLine($"AGGREGATOR ALONE  {SeriesCount:N0} series x 3 fragments, {handed:N0} points handed in; best of {Runs}");
        Print("merge only (None)              ", none, handed);
        Print("Sum by service.name            ", sum, handed);
        Print("Rate by service.name           ", rate, handed);

        Assert.Equal((SeriesCount, (long)SeriesCount * 60), (none.Series, none.Points));
        Assert.Equal((10, 10L * 60), (sum.Series, sum.Points));
        Assert.Equal((10, 10L * 59), (rate.Series, rate.Points));
    }

    private void Print(string what, Cost c, long stored) =>
        _out.WriteLine($"  {what} {c.Ms,8:N1} ms | {c.Bytes / 1048576.0,7:N2} MB | {(double)c.Bytes / stored,7:N1} B/point | " +
                       $"{(double)c.Bytes / SeriesCount,8:N0} B/stored-series  ({c.Series} series, {c.Points:N0} points out)");

    /// <summary>One warm-up, then the best of <see cref="Runs"/> for bytes and, separately, for time.</summary>
    private static Cost Measure(Func<(int Series, long Points)> query)
    {
        var shape = query();
        long bestBytes = long.MaxValue;
        double bestMs  = double.MaxValue;
        for (int r = 0; r < Runs; r++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            long start  = Stopwatch.GetTimestamp();
            shape = query();
            double ms   = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            long bytes  = GC.GetAllocatedBytesForCurrentThread() - before;
            bestBytes = Math.Min(bestBytes, bytes);
            bestMs    = Math.Min(bestMs, ms);
        }
        return new Cost(bestBytes, bestMs, shape.Series, shape.Points);
    }

    /// <summary>Walks an engine query on THIS thread, refusing one that would leave it.</summary>
    private static (int, long) Drain(IAsyncEnumerable<MetricSeries> query)
    {
        int series = 0; long points = 0;
        var e = query.GetAsyncEnumerator();
        try
        {
            while (true)
            {
                var next = e.MoveNextAsync();
                if (!next.IsCompleted) throw new InvalidOperationException("the query went asynchronous; the probe cannot see its allocations");
                if (!next.Result) break;
                series++;
                points += e.Current.Points.Count;
            }
        }
        finally
        {
            var d = e.DisposeAsync();
            if (!d.IsCompleted) d.AsTask().GetAwaiter().GetResult();
        }
        return (series, points);
    }

    private static (int, long) Sync(Task<IReadOnlyList<MetricSeries>> task)
    {
        if (!task.IsCompleted) throw new InvalidOperationException("the aggregation went asynchronous; the probe cannot see its allocations");
        var result = task.Result;
        long points = 0;
        foreach (var s in result) points += s.Points.Count;
        return (result.Count, points);
    }
}
