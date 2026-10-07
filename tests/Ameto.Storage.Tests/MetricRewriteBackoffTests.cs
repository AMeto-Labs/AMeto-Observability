using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Testing;
using Microsoft.Extensions.Logging;

namespace Ameto.Storage.Tests;

/// <summary>
/// A REWRITE THAT KEEPS FAILING BACKS OFF, AND A MERGE OF ITS OWN OUTPUTS IS NOT A MERGE (#125).
///
/// <para>On the 512 MB stand the OneHour merge of <c>http.server.request.duration</c> failed on every
/// pass for an hour — every 5–6 minutes the same batch, the same OutOfMemoryException, each attempt
/// allocating hundreds of MiB before it failed and pushing the heap against its limit, where an OTLP
/// request then failed beside it. And a metric whose window held four or more files of its own last
/// merge — over 1 536 series, or a chunked rewrite — was rewritten whole on every pass with nothing new
/// in it. A failure now holds its metric's work in that window off for 5 minutes, doubling to 6 hours,
/// with one log line a step; and a merge waits until enough NEW files have joined its outputs.</para>
/// </summary>
public sealed class MetricRewriteBackoffTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrwback-" + Guid.NewGuid().ToString("N"));

    public MetricRewriteBackoffTests() => Directory.CreateDirectory(_dir);

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long Sec  = 1_000_000_000L;
    private const long Min  = 60 * Sec;
    private const long Hour = 60 * Min;

    private static long Now() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

    private sealed class CapturingLogger : ILogger<MetricStorageEngine>
    {
        public readonly List<(LogLevel Level, string Text)> Entries = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                Func<TState, Exception?, string> formatter)
        {
            lock (Entries) Entries.Add((logLevel, formatter(state, exception)));
        }
    }

    /// <summary>Gauge series of one metric in ONE file each call (or more, past 512 series), a point every minute.</summary>
    private static List<MetricSegmentInfo> Write(string dir, string metric, MetricGranularity granularity, int series,
                                                 long from, int points, int firstSeries = 0)
    {
        var items = new List<(SeriesKey, HotSeries)>(series);
        for (int s = firstSeries; s < firstSeries + series; s++)
        {
            var pts = new List<MetricDataPoint>(points);
            for (int p = 0; p < points; p++)
                pts.Add(new MetricDataPoint { TimestampUnixNano = from + p * Min, Value = s + p });
            items.Add((new SeriesKey(metric, MetricKind.Gauge, "1", new LabelSet([new("replica", s.ToString(CultureInfo.InvariantCulture))])),
                       new HotSeries(pts)));
        }
        return MetricWriter.Write(dir, items, granularity);
    }

    private static List<string> Paths(MetricStorageEngine e, string metric, MetricGranularity granularity) =>
        e.ColdSegmentsForTest.Where(s => s.MetricName == metric && s.Granularity == granularity)
                             .Select(s => s.FilePath).OrderBy(p => p, StringComparer.Ordinal).ToList();

    [Fact]
    public void The_backoff_is_a_pass_doubling_to_six_hours()
    {
        Assert.Equal(new[] { 5, 10, 20, 40, 80, 160, 320, 360, 360 },
                     Enumerable.Range(1, 9).Select(n => (int)MetricStorageEngine.RewriteBackoff(n).TotalMinutes));
        Assert.Equal(TimeSpan.FromHours(6), MetricStorageEngine.RewriteBackoff(1_000));
    }

    [Theory]
    [InlineData("merge")]    // four fresh FiveMin files: a FiveMin compaction
    [InlineData("rollup")]   // a Raw file two hours old: a rollup to FiveMin
    public async Task A_rewrite_that_keeps_failing_waits_out_a_doubling_backoff_and_says_so_once_a_step(string work)
    {
        const string metric = "backoff.gauge";
        long now = Now() / Min * Min;
        if (work == "merge")
            for (int i = 0; i < 4; i++) Write(_dir, metric, MetricGranularity.FiveMin, 3, now - (10 - i) * Hour, 5);
        else
            Write(_dir, metric, MetricGranularity.Raw, 3, now - 2 * Hour - 30 * Min, 20);

        var clock = new ManualTimeProvider();
        var log   = new CapturingLogger();
        await using var engine = new MetricStorageEngine(_dir, log, timeProvider: clock);
        await engine.ColdLoadCompleted;

        int attempts = 0;
        engine.OnRewriteChunkWrittenForTest = _ => { attempts++; throw new IOException("injected failure"); };

        async Task PassAfter(TimeSpan wait)
        {
            clock.Advance(wait);
            await engine.PerformRollupForTest();
        }

        await PassAfter(TimeSpan.Zero);               Assert.Equal(1, attempts);   // fails: 5 minutes off
        await PassAfter(TimeSpan.FromMinutes(4));     Assert.Equal(1, attempts);   // still off
        await PassAfter(TimeSpan.FromMinutes(1));     Assert.Equal(2, attempts);   // tried again, fails: 10 minutes
        await PassAfter(TimeSpan.FromMinutes(9));     Assert.Equal(2, attempts);
        await PassAfter(TimeSpan.FromMinutes(1));     Assert.Equal(3, attempts);   // fails: 20 minutes
        await PassAfter(TimeSpan.FromMinutes(19));    Assert.Equal(3, attempts);

        List<string> errors;
        lock (log.Entries) errors = log.Entries.Where(e => e.Level == LogLevel.Error).Select(e => e.Text).ToList();
        Assert.Equal(3, errors.Count);                                              // one line a step, none while off
        Assert.All(errors, e => Assert.Contains($"for metric '{metric}'", e));
        Assert.Contains("1 time(s) in a row; it is not tried again for 00:05:00", errors[0]);
        Assert.Contains("2 time(s) in a row; it is not tried again for 00:10:00", errors[1]);
        Assert.Contains("3 time(s) in a row; it is not tried again for 00:20:00", errors[2]);

        // It works again: done, and the streak ends in one line.
        engine.OnRewriteChunkWrittenForTest = null;
        await PassAfter(TimeSpan.FromMinutes(1));
        lock (log.Entries)
            Assert.Single(log.Entries, e => e.Level == LogLevel.Information && e.Text.Contains("after 3 failure(s) in a row"));
        if (work == "merge") Assert.Single(Paths(engine, metric, MetricGranularity.FiveMin));
        else                 Assert.Empty(Paths(engine, metric, MetricGranularity.Raw));
    }

    [Fact]
    public async Task A_FiveMin_merge_whose_inputs_are_its_own_outputs_waits_for_three_new_files()
    {
        const string metric = "own.fivemin";
        long now = Now() / Min * Min;
        // 1 600 series: the writer cuts them into four files (512 x 3 + 64), so the merge's own output
        // is four files - a window that used to merge again on every pass with nothing new in it.
        Write(_dir, metric, MetricGranularity.FiveMin, 1_600, now - 10 * Hour, 2);

        await using var engine = new MetricStorageEngine(_dir, new CapturingLogger());
        await engine.ColdLoadCompleted;

        await engine.PerformRollupForTest();
        var merged = Paths(engine, metric, MetricGranularity.FiveMin);
        Assert.Equal(4, merged.Count);

        await engine.PerformRollupForTest();
        Assert.Equal(merged, Paths(engine, metric, MetricGranularity.FiveMin));     // nothing new: not rewritten

        engine.AdoptColdSegmentsForTest(Write(_dir, metric, MetricGranularity.FiveMin, 1, now - 5 * Hour, 2));
        engine.AdoptColdSegmentsForTest(Write(_dir, metric, MetricGranularity.FiveMin, 1, now - 4 * Hour, 2, firstSeries: 1));
        await engine.PerformRollupForTest();
        Assert.Equal(6, Paths(engine, metric, MetricGranularity.FiveMin).Count);     // two new: not yet

        engine.AdoptColdSegmentsForTest(Write(_dir, metric, MetricGranularity.FiveMin, 1, now - 3 * Hour, 2, firstSeries: 2));
        await engine.PerformRollupForTest();
        var again = Paths(engine, metric, MetricGranularity.FiveMin);
        Assert.Empty(again.Intersect(merged));                                       // three new: merged again
        Assert.Equal(4, again.Count);
    }

    [Fact]
    public async Task A_Raw_compaction_whose_inputs_are_its_own_outputs_is_not_rewritten()
    {
        const string metric = "own.raw";
        long now = Now() / Min * Min;
        // 600 series -> two files a flush, and two again after a compaction.
        Write(_dir, metric, MetricGranularity.Raw, 600, now - 50 * Min, 3);
        Write(_dir, metric, MetricGranularity.Raw, 600, now - 40 * Min, 3);

        await using var engine = new MetricStorageEngine(_dir, new CapturingLogger());
        await engine.ColdLoadCompleted;

        await engine.PerformRollupForTest();
        var compacted = Paths(engine, metric, MetricGranularity.Raw);
        Assert.Equal(2, compacted.Count);

        await engine.PerformRollupForTest();
        Assert.Equal(compacted, Paths(engine, metric, MetricGranularity.Raw));      // nothing new: not rewritten

        engine.AdoptColdSegmentsForTest(Write(_dir, metric, MetricGranularity.Raw, 1, now - 30 * Min, 3));
        await engine.PerformRollupForTest();
        Assert.Empty(Paths(engine, metric, MetricGranularity.Raw).Intersect(compacted));   // one new: compacted again
    }
}
