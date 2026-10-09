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

    /// <summary>Gauge series whose values are random doubles, which do not compress: files of a known, large size.</summary>
    private static List<MetricSegmentInfo> RandomGauges(string dir, string metric, MetricGranularity granularity, int series,
                                                        long from, long step, int points, int seed)
    {
        Directory.CreateDirectory(dir);
        var rng   = new Random(seed);
        var items = new List<(SeriesKey, HotSeries)>(series);
        for (int s = 0; s < series; s++)
        {
            var pts = new List<MetricDataPoint>(points);
            for (int p = 0; p < points; p++)
                pts.Add(new MetricDataPoint { TimestampUnixNano = from + p * step, Value = rng.NextDouble() * 1e6 });
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

    /// <summary>
    /// A SOURCE THAT WILL NOT DECODE IS LEFT OUT, NOT ALLOWED TO HOLD ITS METRIC BACK (#125 review F1).
    /// A rollup rewrites all of a metric's aged files together, so one file every read refuses — a
    /// pre-#125 merge's block over the reader's 64 MiB, written by a host with more memory than the
    /// stand — failed the rollup of every other file of its metric, backing off to 6 hours, until
    /// retention took it a TTL later. Here a 5-minute file whose block size no read accepts sits
    /// among healthy ones of its metric: the first pass meets it and leaves it out, with one warning
    /// and no back-off, and the next rewrites the others — three a day old rolled up, or four fresh
    /// ones merged. It stays on disk.
    /// </summary>
    [Theory]
    [InlineData("one pass")]    // a rollup; the headers show one chunk: the single read meets it
    [InlineData("planned")]     // a rollup; a 4 KB budget: the planning walk meets it
    [InlineData("truncated")]   // a rollup; cut short after the start loaded it: the header read meets it
    [InlineData("merge")]       // a 5-minute merge of the last 24 hours
    public async Task A_source_that_will_not_decode_is_left_out_and_its_metric_is_rewritten_without_it(string where)
    {
        const string metric = "unreadable.gauge";
        long now   = Now() / Min * Min;
        bool merge = where == "merge";
        var files  = new List<MetricSegmentInfo>();
        for (int i = 0; i < (merge ? 5 : 4); i++)
            files.AddRange(Write(_dir, metric, MetricGranularity.FiveMin, 3,
                                 merge ? now - 10 * Hour + i * Hour : now - 3 * 24 * Hour + i * 6 * Hour, 12));
        string bad = files[1].FilePath;
        if (where != "truncated")
            using (var fs = new FileStream(bad, FileMode.Open, FileAccess.Write))
            {
                fs.Position = 32;                                  // the compressed block's size, after the 28-byte header and its raw size
                fs.Write([0xFF, 0xFF, 0xFF, 0x7F]);
            }

        var log = new CapturingLogger();
        await using var engine = new MetricStorageEngine(_dir, log, where == "planned" ? new Ameto.Core.MetricsOptions { RewriteBudgetBytes = 4096 } : null);
        await engine.ColdLoadCompleted;
        if (where == "truncated")
            using (var fs = new FileStream(bad, FileMode.Open, FileAccess.Write)) fs.SetLength(20);

        // (The start's catalog seed warns about the file too, when it can see the damage: that is its own line.)
        List<string> LeftOut()
        {
            lock (log.Entries)
                return log.Entries.Where(e => e.Level == LogLevel.Warning && e.Text.Contains("will not decode", StringComparison.Ordinal))
                                  .Select(e => e.Text).ToList();
        }
        List<string> Errors()
        {
            lock (log.Entries) return log.Entries.Where(e => e.Level == LogLevel.Error).Select(e => e.Text).ToList();
        }

        var before = Paths(engine, metric, MetricGranularity.FiveMin);
        await engine.PerformRollupForTest();
        Assert.Equal(before, Paths(engine, metric, MetricGranularity.FiveMin));     // met the file: nothing rewritten yet
        Assert.Contains(bad, Assert.Single(LeftOut()));
        Assert.Empty(Errors());                                                      // the file's fault, not the work's: no back-off

        await engine.PerformRollupForTest();
        var after = Paths(engine, metric, MetricGranularity.FiveMin);
        Assert.Contains(bad, after);                                                 // left where it was
        Assert.True(File.Exists(bad));
        var rewritten = merge
            ? engine.ColdSegmentsForTest.Where(s => s.MetricName == metric && s.FilePath != bad).ToList()
            : engine.ColdSegmentsForTest.Where(s => s.MetricName == metric && s.Granularity == MetricGranularity.OneHour).ToList();
        if (merge) Assert.Empty(rewritten.Select(s => s.FilePath).Intersect(before));   // the four healthy ones merged
        else       Assert.Equal([bad], after);                                          // the three healthy ones rolled up
        Assert.NotEmpty(rewritten);
        Assert.Equal(3, rewritten.SelectMany(s => MetricReader.ReadAllSync(s.FilePath)).Select(s => s.Labels).Distinct().Count());
        Assert.Single(LeftOut());                                                    // said once
        Assert.Empty(Errors());
    }

    /// <summary>
    /// A BACK-OFF OUTLIVES ITS SLOT'S STALE MERGE RECORD (#126 review N4). The record and the back-off
    /// were forgotten together, so a window that merged once and then failed was tried again early,
    /// two minutes into its five, as soon as retention took the record's outputs. Each is now
    /// forgotten for its own reason: the record when its outputs are gone, the back-off when its
    /// metric has no file left or when it ends.
    /// </summary>
    [Fact]
    public async Task A_back_off_outlives_its_slots_stale_merge_record()
    {
        const string metric = "stale.record";
        long now = Now() / Min * Min;
        for (int i = 0; i < 4; i++) Write(_dir, metric, MetricGranularity.FiveMin, 3, now - (20 - i) * Hour, 5);

        var clock = new ManualTimeProvider();
        await using var engine = new MetricStorageEngine(_dir, new CapturingLogger(), timeProvider: clock);
        await engine.ColdLoadCompleted;

        await engine.PerformRollupForTest();                                         // merged once: the record
        var merged = Assert.Single(Paths(engine, metric, MetricGranularity.FiveMin));

        for (int i = 0; i < 4; i++)
            engine.AdoptColdSegmentsForTest(Write(_dir, metric, MetricGranularity.FiveMin, 3, now - (4 - i) * Hour, 5));
        int attempts = 0;
        engine.OnRewriteChunkWrittenForTest = _ => { attempts++; throw new IOException("injected failure"); };
        await engine.PerformRollupForTest();
        Assert.Equal(1, attempts);                                                   // failed: 5 minutes off

        Assert.Equal(1, await engine.PruneAsync(TimeSpan.FromHours(10)));             // retention takes the record's output
        Assert.DoesNotContain(merged, Paths(engine, metric, MetricGranularity.FiveMin));

        clock.Advance(TimeSpan.FromMinutes(2));
        await engine.PerformRollupForTest();
        Assert.Equal(1, attempts);                                                   // still off: the back-off held
    }

    /// <summary>
    /// A STOP IS HONOURED BETWEEN UNITS OF A REWRITE, AND IS NOT A FAILURE (#126 review L2). The pass
    /// never read its token, so a shutdown waited for every remaining chunk of every metric — with
    /// byte-bounded chunks, many — and a container's stop timeout killed the process mid-rewrite,
    /// leaving outputs beside their sources. Here a Raw compaction planned in many 64 KB chunks is
    /// stopped once its first chunk is written: the pass returns with that output taken back, only
    /// the sources on disk and in the catalog, no Error line — and no back-off: the next pass, not
    /// stopped, compacts.
    /// </summary>
    [Fact]
    public async Task A_rewrite_stopped_after_its_first_chunk_leaves_only_its_sources_and_no_error()
    {
        const string metric = "stopped.raw";
        long now = Now() / Min * Min;
        Write(_dir, metric, MetricGranularity.Raw, 600, now - 50 * Min, 3);
        Write(_dir, metric, MetricGranularity.Raw, 600, now - 40 * Min, 3);
        var sources = Directory.GetFiles(_dir, "*.mts").OrderBy(p => p, StringComparer.Ordinal).ToList();

        var log = new CapturingLogger();
        await using var engine = new MetricStorageEngine(_dir, log, new Ameto.Core.MetricsOptions { RewriteBudgetBytes = 64 * 1024 });
        await engine.ColdLoadCompleted;
        Assert.Equal(sources, Paths(engine, metric, MetricGranularity.Raw));

        using var stop = new CancellationTokenSource();
        int written = 0, planned = 0;
        engine.OnRewriteChunkPlannedForTest = (_, _, _) => planned++;
        engine.OnRewriteChunkWrittenForTest = _ => { written++; stop.Cancel(); };

        await engine.PerformRollupForTest(stop.Token);

        Assert.Equal(1, written);                                                    // it began, and stopped after one chunk
        Assert.Equal(sources, Directory.GetFiles(_dir, "*.mts").OrderBy(p => p, StringComparer.Ordinal).ToList());
        Assert.Equal(sources, Paths(engine, metric, MetricGranularity.Raw));
        lock (log.Entries) Assert.DoesNotContain(log.Entries, e => e.Level == LogLevel.Error);

        // Not backing off: the next pass, not stopped, does the work — in several chunks.
        engine.OnRewriteChunkWrittenForTest = null;
        planned = 0;
        await engine.PerformRollupForTest();
        Assert.Empty(Paths(engine, metric, MetricGranularity.Raw).Intersect(sources));
        Assert.True(planned >= 5, $"setup: the compaction must take several chunks, it took {planned}");
    }

    /// <summary>
    /// A PASS ASKS FOR THE AGGRESSIVE COLLECTION ONLY WHEN IT REWROTE ENOUGH (#126 review L3). The
    /// blocking, compacting gen2 at the end of a pass was gated on the bytes of the files the pass
    /// LISTED. Since #125 a window's own outputs and carried files are no longer rewritten on every
    /// pass, so most passes list a metric's 1-hour files and rewrite nothing — and each still stopped
    /// the world on the 5-minute cadence. Here three kinds of listed work are left alone, each over
    /// the 8 MiB floor by itself, and none may count (#126 review NEW-8): a 1-hour file alone in its
    /// 7-day window 40 days back; three 1-hour files in the next window, which a merge of four does
    /// not yet want; and four 5-minute files of the last day whose merge failed on the pass before
    /// and is backing off. No collection asked. Two raw files of the last hour, over the floor
    /// between them, are compacted: asked, with exactly their bytes.
    /// </summary>
    [Fact]
    public async Task A_pass_asks_for_the_aggressive_collection_only_when_it_rewrote_enough()
    {
        long floor = Ameto.Core.AggressiveGcGate.MaintenancePassBytesFloor;

        string idleDir = Path.Combine(_dir, "idle");
        long window = 7 * 24 * Hour;
        long start  = (Now() - 40 * 24 * Hour) / window * window + Hour;
        var big = RandomGauges(idleDir, "idle.gauge", MetricGranularity.OneHour, 512, start, Min, 2_400, seed: 1);
        var notDue = new List<MetricSegmentInfo>();
        for (int i = 0; i < 3; i++)
            notDue.AddRange(RandomGauges(idleDir, "notdue.gauge", MetricGranularity.OneHour, 512,
                                         start + window + i * 24 * Hour, Min, 900, seed: 10 + i));
        var backingOff = new List<MetricSegmentInfo>();
        long recent = Now() / Min * Min - 23 * Hour;
        for (int i = 0; i < 4; i++)
            backingOff.AddRange(RandomGauges(idleDir, "backoff.gauge", MetricGranularity.FiveMin, 512,
                                             recent + i * 5 * Hour, 30 * Sec, 600, seed: 20 + i));
        Assert.True(big.Sum(s => s.SizeBytes) >= floor, "setup: the 1-hour file must be over the floor");
        Assert.True(notDue.Sum(s => s.SizeBytes) >= floor, "setup: the window not yet due must be over the floor");
        Assert.True(backingOff.Sum(s => s.SizeBytes) >= floor, "setup: the merge backing off must be over the floor");
        Assert.Equal(3, notDue.Count);
        Assert.Equal(4, backingOff.Count);

        await using (var engine = new MetricStorageEngine(idleDir, new CapturingLogger()))
        {
            await engine.ColdLoadCompleted;
            long? asked = null;
            engine.OnAggressiveCollectRequestedForTest = bytes => asked = bytes;

            // The pass before: the 5-minute merge is due, is handed its files, and fails — so it backs off.
            engine.OnRewriteChunkWrittenForTest = _ => throw new IOException("injected failure");
            await engine.PerformRollupForTest();
            Assert.Equal(backingOff.Sum(s => s.SizeBytes), asked);
            engine.OnRewriteChunkWrittenForTest = null;

            asked = null;
            await engine.PerformRollupForTest();
            Assert.Null(asked);
            Assert.Equal(big.Select(s => s.FilePath).OrderBy(p => p, StringComparer.Ordinal),
                         Paths(engine, "idle.gauge", MetricGranularity.OneHour));                 // listed, and left alone
            Assert.Equal(notDue.Select(s => s.FilePath).OrderBy(p => p, StringComparer.Ordinal),
                         Paths(engine, "notdue.gauge", MetricGranularity.OneHour));
            Assert.Equal(backingOff.Select(s => s.FilePath).OrderBy(p => p, StringComparer.Ordinal),
                         Paths(engine, "backoff.gauge", MetricGranularity.FiveMin));
        }

        string busyDir = Path.Combine(_dir, "busy");
        long now = Now() / Min * Min;
        var raw = RandomGauges(busyDir, "busy.gauge", MetricGranularity.Raw, 512, now - 50 * Min, Sec, 1_500, seed: 2);
        raw.AddRange(RandomGauges(busyDir, "busy.gauge", MetricGranularity.Raw, 512, now - 50 * Min + Sec / 2, Sec, 1_500, seed: 3));
        Assert.True(raw.Sum(s => s.SizeBytes) >= floor, "setup: the raw files must be over the floor between them");
        await using (var engine = new MetricStorageEngine(busyDir, new CapturingLogger()))
        {
            await engine.ColdLoadCompleted;
            long? asked = null;
            engine.OnAggressiveCollectRequestedForTest = bytes => asked = bytes;
            await engine.PerformRollupForTest();
            Assert.Equal(raw.Sum(s => s.SizeBytes), asked);
            Assert.Empty(Paths(engine, "busy.gauge", MetricGranularity.Raw).Intersect(raw.Select(s => s.FilePath)));
        }
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
