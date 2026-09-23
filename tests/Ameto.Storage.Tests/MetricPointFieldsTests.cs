using System.Runtime.CompilerServices;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// <see cref="MetricPointFields.NoBuckets"/>: a query's caller that will not read the histogram
/// bucket arrays says so, and the cold read builds none (issue #83 WP7, M#4(a)). A 16-bucket array
/// is 152 B against the 40 B of the rest of the point, and only a quantile or a heatmap reads it.
/// </summary>
public sealed class MetricPointFieldsTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mfields-" + Guid.NewGuid().ToString("N"));

    public MetricPointFieldsTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long S  = 1_000_000_000L;
    private const long T0 = 1_784_800_020_000_000_000L;
    private static readonly double[] Bounds = [0.1, 1, 10];

    private void WriteBusyHistograms(int seriesCount, int points)
    {
        var items = new List<(SeriesKey, HotSeries)>();
        for (int s = 0; s < seriesCount; s++)
        {
            var pts = new List<MetricDataPoint>(points);
            for (int p = 0; p < points; p++)
                pts.Add(new MetricDataPoint
                {
                    TimestampUnixNano = T0 + p * 15 * S, Value = p, Count = p + 1, Sum = p * 0.5,
                    BucketCounts = p % 5 == 4 ? null : [p, s, 1, p + s],     // every point full, now and then bucketless
                });
            var labels = new LabelSet([new("replica", "r" + s.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                                       new("svc", s % 2 == 0 ? "a" : "b")]);
            items.Add((new SeriesKey("fields.h", MetricKind.Histogram, "s", labels), new HotSeries(pts, Bounds)));
        }
        MetricWriter.Write(_dir, items, MetricGranularity.Raw);
    }

    [Fact]
    public async Task A_no_buckets_query_is_the_same_answer_without_the_cold_bucket_arrays()
    {
        WriteBusyHistograms(40, 30);
        await using var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance,
                                                         new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });
        await engine.ColdLoadCompleted;
        // And a hot tier for the same metric, whose points keep the arrays they were ingested with.
        var hot = new List<MetricIngestItem>();
        for (int i = 0; i < 20; i++)
            hot.Add(new MetricIngestItem
            {
                Name = "fields.h", Kind = MetricKind.Histogram, Unit = "s", Labels = new LabelSet([new("replica", "hot")]),
                TimestampUnixNano = T0 + (100 + i) * 15 * S, HistogramCount = i, HistogramSum = i, BucketBounds = Bounds,
                BucketCounts = [i, 0, 0, i],
            });
        engine.Ingest(hot.ToArray());

        TimeSpan?[] steps = [null, TimeSpan.FromMinutes(1)];
        (DateTimeOffset?, DateTimeOffset?)[] ranges =
        [
            (null, null),
            (DateTimeOffset.FromUnixTimeMilliseconds((T0 + 100 * S) / 1_000_000), DateTimeOffset.FromUnixTimeMilliseconds((T0 + 1_700 * S) / 1_000_000)),
        ];
        IReadOnlyDictionary<string, string>?[] filters = [null, new Dictionary<string, string> { ["svc"] = "a" }];

        int compared = 0;
        foreach (var step in steps)
            foreach (var (from, to) in ranges)
                foreach (var f in filters)
                {
                    var all = new List<MetricSeries>();
                    await foreach (var s in engine.QueryAsync("fields.h", from, to, step, f, MetricPointFields.All)) all.Add(s);
                    var lean = new List<MetricSeries>();
                    await foreach (var s in engine.QueryAsync("fields.h", from, to, step, f, MetricPointFields.NoBuckets)) lean.Add(s);

                    Assert.Equal(all.Count, lean.Count);
                    for (int i = 0; i < all.Count; i++)
                    {
                        Assert.Equal((all[i].Name, all[i].Kind, all[i].Unit, all[i].Labels), (lean[i].Name, lean[i].Kind, lean[i].Unit, lean[i].Labels));
                        Assert.Equal(all[i].BucketBounds, lean[i].BucketBounds);
                        bool isHot = all[i].Labels.ValueAt(0) == "hot";
                        Assert.Equal(all[i].Points.Count, lean[i].Points.Count);
                        for (int p = 0; p < all[i].Points.Count; p++)
                        {
                            var a = all[i].Points[p];
                            var b = lean[i].Points[p];
                            Assert.Equal((a.TimestampUnixNano, a.Value, a.Count, a.Sum), (b.TimestampUnixNano, b.Value, b.Count, b.Sum));
                            if (isHot) Assert.Same(a.BucketCounts, b.BucketCounts);   // the hot tier's own arrays
                            else       Assert.Null(b.BucketCounts);                   // never built
                            compared++;
                        }
                    }
                }
        Assert.True(compared > 1_000, $"only {compared} points compared");
    }

    [Fact]
    public async Task A_no_buckets_cold_read_builds_no_bucket_arrays()
    {
        WriteBusyHistograms(300, 60);
        await using var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance,
                                                         new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });
        await engine.ColdLoadCompleted;

        long Measure(MetricPointFields fields)
        {
            long best = long.MaxValue;
            for (int run = 0; run < 3; run++)
            {
                long before = GC.GetAllocatedBytesForCurrentThread();
                var e = engine.QueryAsync("fields.h", null, null, null, null, fields).GetAsyncEnumerator();
                int points = 0;
                while (true)
                {
                    var next = e.MoveNextAsync();
                    Assert.True(next.IsCompleted);       // synchronous: every byte on this thread
                    if (!next.Result) break;
                    points += e.Current.Points.Count;
                }
                var d = e.DisposeAsync();
                Assert.True(d.IsCompleted);
                Assert.Equal(300 * 60, points);
                best = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - before);
            }
            return best;
        }

        long all  = Measure(MetricPointFields.All);
        long lean = Measure(MetricPointFields.NoBuckets);

        // 18 000 points, 14 400 of them with a 4-bucket array (56 B): ~0.8 MB of arrays the lean
        // read must not build — the two figures differ by at least most of that.
        Assert.True(all - lean > 600_000, $"All read {all:N0} B, NoBuckets {lean:N0} B: the bucket arrays were built anyway");
    }

    /// <summary>Answers nothing; records which fields each query asked for.</summary>
    private sealed class FieldsRecorder : IMetricQuery
    {
        public readonly List<MetricPointFields?> Asked = [];

        public IEnumerable<string> GetMetricNames(string? prefix = null) => [];

        public async IAsyncEnumerable<MetricSeries> QueryAsync(
            string metricName, DateTimeOffset? from = null, DateTimeOffset? to = null, TimeSpan? step = null,
            IReadOnlyDictionary<string, string>? labelMatchers = null, [EnumeratorCancellation] CancellationToken ct = default)
        {
            Asked.Add(null);
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<MetricSeries> QueryAsync(
            string metricName, DateTimeOffset? from, DateTimeOffset? to, TimeSpan? step,
            IReadOnlyDictionary<string, string>? labelMatchers, MetricPointFields fields, [EnumeratorCancellation] CancellationToken ct = default)
        {
            Asked.Add(fields);
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<MetricSeries> GetLatestAsync(
            string metricName, IReadOnlyDictionary<string, string>? labelMatchers = null, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    [Theory]
    [InlineData(MetricAggregation.None,     false, MetricPointFields.All)]        // the stored points ARE the answer
    [InlineData(MetricAggregation.Quantile, false, MetricPointFields.All)]
    [InlineData(MetricAggregation.Quantile, true,  MetricPointFields.All)]
    [InlineData(MetricAggregation.Rate,     false, MetricPointFields.NoBuckets)]
    [InlineData(MetricAggregation.Increase, true,  MetricPointFields.NoBuckets)]
    [InlineData(MetricAggregation.Last,     false, MetricPointFields.NoBuckets)]
    [InlineData(MetricAggregation.Avg,      false, MetricPointFields.All)]        // no group-by: a lone series passes through
    [InlineData(MetricAggregation.Sum,      true,  MetricPointFields.NoBuckets)]
    [InlineData(MetricAggregation.Max,      true,  MetricPointFields.NoBuckets)]
    public async Task An_aggregation_asks_for_bucket_arrays_only_when_its_answer_can_carry_them(
        MetricAggregation aggregation, bool groupBy, MetricPointFields expected)
    {
        var recorder = new FieldsRecorder();
        var agg = new MetricAggregator(recorder);
        await agg.QueryAsync(new MetricQueryRequest { Metric = "m", Aggregation = aggregation, GroupBy = groupBy ? ["svc"] : null });
        Assert.Equal(new MetricPointFields?[] { expected }, recorder.Asked);
    }

    [Fact]
    public async Task An_expression_reads_values_only_unless_a_side_is_a_quantile()
    {
        var recorder = new FieldsRecorder();
        var agg = new MetricAggregator(recorder);
        await agg.EvalExprAsync(new MetricExprRequest
        {
            Left  = new MetricQueryRequest { Metric = "a" },
            Right = new MetricQueryRequest { Metric = "b", Aggregation = MetricAggregation.Quantile },
        });
        Assert.Equal(new MetricPointFields?[] { MetricPointFields.NoBuckets, MetricPointFields.All }, recorder.Asked);

        var hm = new FieldsRecorder();
        await new MetricAggregator(hm).HeatmapAsync("h", null, null, null, null);
        Assert.Equal(new MetricPointFields?[] { null }, hm.Asked);      // the heatmap reads buckets: the full query
    }
}
