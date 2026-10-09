using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A SAME-GRANULARITY MERGE TAKES ONLY FILES LYING WHOLLY INSIDE ITS TIER'S WINDOW (#125).
///
/// <para>The 512 MB stand failed its metric compaction with an OutOfMemoryException on every pass
/// for an hour. The FiveMin merge selected files by MaxNano alone and wrote the union of their
/// ranges, so a merged file kept its oldest point and took the newest MaxNano: it was the freshest
/// file of the tier on every pass, merged again with each new arrival, never aged into the OneHour
/// rollup and never expired — retention decides by MaxNano as well. Its history grew by ~288 points
/// a series a day until one chunk of a histogram metric no longer fit the heap. These tests take
/// each tier's selection in turn; each fails if its window is put back to what it was.</para>
/// </summary>
public sealed class MetricMergeWindowTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mwin-" + Guid.NewGuid().ToString("N"));
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

    private const long Sec  = 1_000_000_000L;
    private const long Min  = 60 * Sec;
    private const long Hour = 60 * Min;
    private const long Day  = 24 * Hour;

    private static long Now() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

    /// <summary>
    /// One <c>.mts</c> of <paramref name="granularity"/> for <paramref name="metric"/>: two gauge
    /// series, a point every <paramref name="step"/> from <paramref name="from"/> to <paramref name="to"/>.
    /// </summary>
    private MetricSegmentInfo File(string metric, MetricGranularity granularity, long from, long to, long step)
    {
        var items = new List<(SeriesKey, HotSeries)>();
        for (int s = 0; s < 2; s++)
        {
            var pts = new List<MetricDataPoint>();
            for (long ts = from; ts <= to; ts += step)
                pts.Add(new MetricDataPoint { TimestampUnixNano = ts, Value = s * 1000 + (ts - from) / step });
            items.Add((new SeriesKey(metric, MetricKind.Gauge, "1", new LabelSet([new("replica", "r" + s)])),
                       new HotSeries(pts)));
        }
        return Assert.Single(MetricWriter.Write(_dir, items, granularity));
    }

    private async Task<MetricStorageEngine> Engine()
    {
        _engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        await _engine.ColdLoadCompleted;
        return _engine;
    }

    private static List<MetricSegmentInfo> Of(MetricStorageEngine engine, string metric, MetricGranularity granularity) =>
        engine.ColdSegmentsForTest.Where(s => s.MetricName == metric && s.Granularity == granularity).ToList();

    [Fact]
    public async Task An_old_FiveMin_file_is_not_merged_with_fresh_ones_and_is_rolled_up_and_pruned_once_old()
    {
        const string metric = "win.fivemin";
        long now = Now() / (5 * Min) * (5 * Min);

        // What an earlier merge left behind: twenty days of five-minute points, the last one 2 h ago.
        var carried = File(metric, MetricGranularity.FiveMin, now - 20 * Day, now - 2 * Hour, 5 * Min);
        // Four ordinary Raw->FiveMin rollup outputs from the last day: a merge is due.
        var fresh = new List<MetricSegmentInfo>();
        for (int i = 0; i < 4; i++)
            fresh.Add(File(metric, MetricGranularity.FiveMin, now - (20 - i) * Hour, now - (20 - i) * Hour + 30 * Min, 5 * Min));
        // A FiveMin file nothing merged for a week: past the 24-hour boundary, and past a 7-day TTL.
        var aged = File(metric, MetricGranularity.FiveMin, now - 9 * Day, now - 8 * Day, 5 * Min);

        var engine = await Engine();
        await engine.PerformRollupForTest();

        // The carried file is left alone: it is not merged with the fresh ones, so no output takes its
        // twenty-day start to a fresh MaxNano.
        var fiveMin = Of(engine, metric, MetricGranularity.FiveMin);
        Assert.Contains(fiveMin, s => s.FilePath == carried.FilePath);
        Assert.True(System.IO.File.Exists(carried.FilePath));
        Assert.All(fiveMin.Where(s => s.FilePath != carried.FilePath),
                   s => Assert.True(s.MinNano >= now - 24 * Hour, "a merged FiveMin file reaches back past the tier's 24 hours"));

        // The fresh ones were merged among themselves: one file, their own range.
        Assert.All(fresh, f => Assert.False(System.IO.File.Exists(f.FilePath), "a fresh file was not merged"));
        var merged = Assert.Single(fiveMin, s => s.FilePath != carried.FilePath);
        Assert.Equal(fresh[0].MinNano, merged.MinNano);
        Assert.Equal(fresh[^1].MaxNano, merged.MaxNano);

        // The aged file went down a tier, as an unmerged file always did...
        Assert.False(System.IO.File.Exists(aged.FilePath));
        var oneHour = Assert.Single(Of(engine, metric, MetricGranularity.OneHour));
        Assert.True(oneHour.MaxNano <= aged.MaxNano);

        // ...and retention removes it on its own MaxNano. The carried file is still too fresh to.
        Assert.Equal(1, await engine.PruneAsync(TimeSpan.FromDays(7)));
        Assert.Empty(Of(engine, metric, MetricGranularity.OneHour));
        Assert.Contains(Of(engine, metric, MetricGranularity.FiveMin), s => s.FilePath == carried.FilePath);
    }

    [Fact]
    public async Task A_Raw_file_reaching_back_past_the_hour_is_not_compacted_with_fresh_ones()
    {
        const string metric = "win.raw";
        long now = Now() / Min * Min;

        // A Raw file whose MaxNano is inside the compaction window and whose MinNano is not.
        var carried = File(metric, MetricGranularity.Raw, now - 3 * Hour, now - 20 * Min, 5 * Min);
        var a = File(metric, MetricGranularity.Raw, now - 50 * Min, now - 40 * Min, Min);
        var b = File(metric, MetricGranularity.Raw, now - 35 * Min, now - 25 * Min, Min);

        var engine = await Engine();
        await engine.PerformRollupForTest();

        var raw = Of(engine, metric, MetricGranularity.Raw);
        Assert.Contains(raw, s => s.FilePath == carried.FilePath);
        Assert.False(System.IO.File.Exists(a.FilePath));
        Assert.False(System.IO.File.Exists(b.FilePath));
        var merged = Assert.Single(raw, s => s.FilePath != carried.FilePath);
        Assert.Equal((a.MinNano, b.MaxNano), (merged.MinNano, merged.MaxNano));
    }

    [Fact]
    public async Task A_OneHour_file_straddling_two_windows_is_not_merged_into_either()
    {
        const string metric = "win.onehour";
        long span = 7 * Day;   // the 1-h tier's window before a retention run has named the TTL
        long ws   = (Now() - 40 * Day) / span * span;

        var inside = new List<MetricSegmentInfo>();
        for (int k = 0; k < 4; k++)
            inside.Add(File(metric, MetricGranularity.OneHour, ws + k * Day, ws + k * Day + 5 * Hour, Hour));
        var straddler = File(metric, MetricGranularity.OneHour, ws + 6 * Day, ws + 8 * Day, Hour);

        var engine = await Engine();
        await engine.PerformRollupForTest();

        var oneHour = Of(engine, metric, MetricGranularity.OneHour);
        Assert.Contains(oneHour, s => s.FilePath == straddler.FilePath);
        Assert.All(inside, f => Assert.False(System.IO.File.Exists(f.FilePath), "a file inside the window was not merged"));
        var merged = Assert.Single(oneHour, s => s.FilePath != straddler.FilePath);
        Assert.Equal((inside[0].MinNano, inside[^1].MaxNano), (merged.MinNano, merged.MaxNano));
        Assert.Equal(merged.MinNano / span, merged.MaxNano / span);
    }
}
