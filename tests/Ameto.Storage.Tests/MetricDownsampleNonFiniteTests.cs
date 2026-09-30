using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE STORAGE-SIDE DOWNSAMPLE SKIPS NaN TOO (#92, PR #101 review F1), through the real engine.
///
/// <para><c>MetricAggregator</c>'s reducers skipped non-finite values; the engine's own downsample did
/// not. It runs on every stepped read — the Metrics page always sends a step, so one bad sample
/// blanked a whole bucket of the chart — and in the 5-minute and 1-hour rollups, which then DELETE
/// the raw files: the bucket's finite samples were gone for good, replaced by one NaN.</para>
/// </summary>
public sealed class MetricDownsampleNonFiniteTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mdsnan-" + Guid.NewGuid().ToString("N"));
    private MetricStorageEngine? _engine;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_engine is not null) await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private const long S = 1_000_000_000L;
    private static readonly LabelSet Labels = new([new("service.name", "nan-svc")]);

    private static MetricDataPoint P(long ts, double v) => new() { TimestampUnixNano = ts, Value = v };

    private static async Task<List<MetricSeries>> Read(MetricStorageEngine engine, string metric, TimeSpan? step)
    {
        var got = new List<MetricSeries>();
        await foreach (var s in engine.QueryAsync(metric, step: step)) got.Add(s);
        return got;
    }

    [Fact]
    public async Task A_stepped_read_reduces_a_bucket_over_its_finite_samples()
    {
        _engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance,
                                          new MetricsOptions { HotTierBytes = 1L << 30 });
        long t = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 60_000 - 2) * 60_000 * 1_000_000L;   // a minute's start, 2 min ago
        _engine.Ingest(
        [
            new MetricIngestItem { Name = "nan.gauge",   Kind = MetricKind.Gauge,   Labels = Labels, TimestampUnixNano = t,         ScalarValue = 1 },
            new MetricIngestItem { Name = "nan.gauge",   Kind = MetricKind.Gauge,   Labels = Labels, TimestampUnixNano = t + S,     ScalarValue = double.NaN },
            new MetricIngestItem { Name = "nan.gauge",   Kind = MetricKind.Gauge,   Labels = Labels, TimestampUnixNano = t + 2 * S, ScalarValue = 3 },
            new MetricIngestItem { Name = "nan.counter", Kind = MetricKind.Counter, Labels = Labels, TimestampUnixNano = t,         ScalarValue = 1 },
            new MetricIngestItem { Name = "nan.counter", Kind = MetricKind.Counter, Labels = Labels, TimestampUnixNano = t + S,     ScalarValue = 3 },
            new MetricIngestItem { Name = "nan.counter", Kind = MetricKind.Counter, Labels = Labels, TimestampUnixNano = t + 2 * S, ScalarValue = double.NaN },
        ]);

        // Before: the gauge bucket was NaN (the average of 1, NaN, 3) and the counter bucket NaN (its latest).
        var gauge = Assert.Single(Assert.Single(await Read(_engine, "nan.gauge", TimeSpan.FromMinutes(1))).Points);
        Assert.Equal((t, 2.0), (gauge.TimestampUnixNano, gauge.Value));
        var counter = Assert.Single(Assert.Single(await Read(_engine, "nan.counter", TimeSpan.FromMinutes(1))).Points);
        Assert.Equal((t, 3.0), (counter.TimestampUnixNano, counter.Value));
    }

    [Fact]
    public async Task A_rollup_keeps_the_finite_samples_of_the_bucket_it_writes_for_good()
    {
        // Raw points two hours old — past the 5-minute rollup's cutoff — in one 5-minute bucket.
        long b = (DateTimeOffset.UtcNow.AddHours(-2).ToUnixTimeMilliseconds() / 300_000) * 300_000 * 1_000_000L;
        var items = new List<(SeriesKey, HotSeries)>
        {
            (new SeriesKey("nan.roll.gauge",   MetricKind.Gauge,   "1", Labels), new HotSeries([P(b, 1), P(b + 10 * S, double.NaN), P(b + 20 * S, 3)])),
            (new SeriesKey("nan.roll.counter", MetricKind.Counter, "1", Labels), new HotSeries([P(b, 1), P(b + 10 * S, 3), P(b + 20 * S, double.NaN)])),
        };
        var raw = MetricWriter.Write(_dir, items, MetricGranularity.Raw);

        _engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        await _engine.ColdLoadCompleted;
        await _engine.PerformRollupForTest();
        await _engine.PerformRollupForTest();       // one metric per slice: a second pass for the second

        Assert.All(raw, f => Assert.False(File.Exists(f.FilePath), "the raw file outlived its rollup"));

        // What is left on disk is the rollup. Before: one NaN per bucket, the 1 and the 3 gone.
        var gauge = Assert.Single(Assert.Single(await Read(_engine, "nan.roll.gauge", step: null)).Points);
        Assert.Equal((b, 2.0), (gauge.TimestampUnixNano, gauge.Value));
        var counter = Assert.Single(Assert.Single(await Read(_engine, "nan.roll.counter", step: null)).Points);
        Assert.Equal((b, 3.0), (counter.TimestampUnixNano, counter.Value));
    }
}
