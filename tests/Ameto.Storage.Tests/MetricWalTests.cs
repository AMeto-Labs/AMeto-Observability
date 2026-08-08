using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// Metric write-ahead log: append/replay fidelity across scalar and histogram points, the
/// flush generation, torn and zero-filled tails, the series pool — and the behaviour the log
/// exists to enable, namely that a trickle of points no longer costs one .mts per metric name
/// every minute.
/// </summary>
public sealed class MetricWalTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mwal-" + Guid.NewGuid().ToString("N"));

    /// <summary>Set by <see cref="FreezeDataDir"/> — see there for what it holds.</summary>
    private string? _frozen;

    public MetricWalTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
        if (_frozen is not null) { try { Directory.Delete(_frozen, true); } catch { } }
    }

    private string WalPath  => Path.Combine(_dir, "metrics.wal");
    private string PoolPath => WalPath + ".pool";

    private static LabelSet Labels(params (string K, string V)[] pairs) =>
        new(pairs.Select(p => new KeyValuePair<string, string>(p.K, p.V)));

    private static MetricIngestItem Scalar(string name, long nano, double value, LabelSet? labels = null) => new()
    {
        Name              = name,
        Kind              = MetricKind.Gauge,
        Unit              = "ms",
        Labels            = labels ?? LabelSet.Empty,
        TimestampUnixNano = nano,
        ScalarValue       = value,
    };

    private static MetricDataPoint PointOf(MetricIngestItem i) => new()
    {
        TimestampUnixNano = i.TimestampUnixNano,
        Value             = i.Kind == MetricKind.Histogram
                                ? (i.HistogramCount > 0 ? i.HistogramSum / i.HistogramCount : 0)
                                : i.ScalarValue,
        Count             = i.HistogramCount,
        Sum               = i.HistogramSum,
        BucketCounts      = i.BucketCounts,
    };

    private static void Append(MetricWriteAheadLog wal, MetricIngestItem item) =>
        wal.Append(item, PointOf(item));

    private static int TrcCount(string dir, string pattern) => Directory.GetFiles(dir, pattern).Length;

    /// <summary>
    /// Waits for the engine's background cold-segment scan, then queries.
    ///
    /// <para>This polled for the first non-empty answer, which is not the same thing: the hot
    /// tier is populated from the WAL in the constructor and answers immediately, so the poll
    /// returned as soon as the recovered points showed up — before a single cold file had been
    /// registered. A test that asserts nothing was lost then measured a fraction of the data
    /// and blamed the engine for it.</para>
    /// </summary>
    private static async Task<List<MetricSeries>> QueryWhenColdLoadedAsync(MetricStorageEngine engine, string name)
    {
        await engine.ColdLoadCompleted.WaitAsync(TimeSpan.FromSeconds(30));
        return engine.QueryAsync(name).ToBlockingEnumerable().ToList();
    }

    // ── Roundtrip ─────────────────────────────────────────────────────────────

    [Fact]
    public void Replays_scalar_points_with_their_series_identity()
    {
        var labels = Labels(("service", "MintRoute.API"), ("route", "/api/pay"));

        var wal = MetricWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 20; i++)
            Append(wal, Scalar("http.server.duration", 1_700_000_000_000_000_000L + i * 1_000_000L, i * 1.5, labels));
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll(out int unresolved);
        reopened.Dispose();

        Assert.Equal(0, unresolved);
        Assert.Equal(20, replayed.Count);
        Assert.All(replayed, r =>
        {
            Assert.Equal("http.server.duration", r.Name);
            Assert.Equal(MetricKind.Gauge, r.Kind);
            Assert.Equal("ms", r.Unit);
            Assert.Equal(labels, r.Labels);
        });
        Assert.Equal(0.0,  replayed[0].Point.Value);
        Assert.Equal(28.5, replayed[19].Point.Value);
        Assert.Equal(1_700_000_000_000_000_000L + 19 * 1_000_000L, replayed[19].Point.TimestampUnixNano);
    }

    [Fact]
    public void Replays_histogram_points_with_bounds_and_bucket_counts()
    {
        var bounds  = new[] { 1.0, 5.0, 10.0, 50.0 };
        var buckets = new long[] { 3, 9, 4, 1, 0 };   // bounds.Length + 1

        var wal = MetricWriteAheadLog.Open(WalPath);
        Append(wal, new MetricIngestItem
        {
            Name              = "http.server.request.duration",
            Kind              = MetricKind.Histogram,
            Unit              = "s",
            Labels            = Labels(("service", "KioskAgent.API")),
            TimestampUnixNano = 1_700_000_000_000_000_000L,
            HistogramCount    = 17,
            HistogramSum      = 42.5,
            BucketBounds      = bounds,
            BucketCounts      = buckets,
        });
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll(out _);
        reopened.Dispose();

        var r = Assert.Single(replayed);
        Assert.Equal(MetricKind.Histogram, r.Kind);
        Assert.Equal(bounds, r.Bounds);
        Assert.Equal(buckets, r.Point.BucketCounts);
        Assert.Equal(17, r.Point.Count);
        Assert.Equal(42.5, r.Point.Sum);
        Assert.Equal(42.5 / 17, r.Point.Value, 12);
    }

    [Fact]
    public void The_series_pool_is_written_once_per_series_not_once_per_point()
    {
        // The whole point of the companion file: a label set is registered once, and the
        // 500 points that follow reference it by index.
        var labels = Labels(("service", "MintRoute.API"), ("pod", "a-very-long-pod-name-0123456789"));

        var wal = MetricWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 500; i++)
            Append(wal, Scalar("cpu.utilisation", 1_700_000_000_000_000_000L + i, i, labels));
        long walBytes = wal.WrittenBytes;
        wal.Dispose();

        long poolBytes = new FileInfo(PoolPath).Length;

        Assert.Equal(500 * 48, walBytes);      // fixed 48-byte entries, no per-point labels
        Assert.True(poolBytes < 200, $"pool should hold one record, got {poolBytes} B");

        var reopened = MetricWriteAheadLog.Open(WalPath);
        Assert.Equal(500, reopened.ReadAll(out _).Count);
        reopened.Dispose();
    }

    [Fact]
    public void Distinct_label_sets_stay_distinct_across_replay()
    {
        var a = Labels(("route", "/a"));
        var b = Labels(("route", "/b"));

        var wal = MetricWriteAheadLog.Open(WalPath);
        Append(wal, Scalar("req.count", 1_700_000_000_000_000_000L, 1, a));
        Append(wal, Scalar("req.count", 1_700_000_000_000_000_001L, 2, b));
        Append(wal, Scalar("req.count", 1_700_000_000_000_000_002L, 3, a));
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll(out _);
        reopened.Dispose();

        Assert.Equal(3, replayed.Count);
        Assert.Equal(a, replayed[0].Labels);
        Assert.Equal(b, replayed[1].Labels);
        Assert.Equal(a, replayed[2].Labels);
        Assert.Equal(2, replayed.Select(r => r.Labels).Distinct().Count());
    }

    // ── Generation ────────────────────────────────────────────────────────────

    [Fact]
    public void A_committed_flush_drops_everything_it_covered()
    {
        var wal = MetricWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 10; i++) Append(wal, Scalar("m", 1_700_000_000_000_000_000L + i, i));
        wal.CommitFlush(wal.BeginFlush());
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        Assert.Empty(reopened.ReadAll(out _));
        reopened.Dispose();

        Assert.Equal(0, new FileInfo(PoolPath).Length);   // nothing references it any more
    }

    /// <summary>
    /// The defect this two-phase protocol exists for. Points keep arriving while the files
    /// are written; those are in no file, so wiping the whole log on completion left them
    /// durable nowhere. Only the snapshot's generation may be reclaimed.
    /// </summary>
    [Fact]
    public void Points_appended_during_a_flush_survive_the_commit()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        var wal = MetricWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 5; i++) Append(wal, Scalar("m", baseNano + i, i));   // in the snapshot

        ulong flushing = wal.BeginFlush();
        for (int i = 5; i < 9; i++) Append(wal, Scalar("m", baseNano + i, i));   // arrive mid-write

        wal.CommitFlush(flushing);
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll(out int unresolved);
        reopened.Dispose();

        Assert.Equal(0, unresolved);                                  // the pool survived too
        Assert.Equal([5.0, 6.0, 7.0, 8.0], replayed.Select(r => r.Point.Value).OrderBy(v => v));
    }

    [Fact]
    public void A_flush_that_never_commits_leaves_everything_replayable()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        var wal = MetricWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 5; i++) Append(wal, Scalar("m", baseNano + i, i));
        wal.BeginFlush();                                             // files failed to write
        for (int i = 5; i < 8; i++) Append(wal, Scalar("m", baseNano + i, i));
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll(out _);
        reopened.Dispose();

        Assert.Equal(8, replayed.Count);   // snapshot AND the points that followed it
    }

    [Fact]
    public void Points_logged_after_a_flush_survive_however_early_they_are_stamped()
    {
        // A metric point's timestamp is reported by the exporter, not assigned by us, and an
        // export can arrive stamped before the flush that preceded it. Replay must key off
        // the generation, never off the data's own clock.
        const long flushed = 1_700_000_000_000_000_000L;

        var wal = MetricWriteAheadLog.Open(WalPath);
        Append(wal, Scalar("m", flushed, 1));
        ulong gen = wal.BeginFlush();
        wal.CommitFlush(gen);

        Append(wal, Scalar("m", flushed - 30_000_000_000L, 2));   // stamped 30 s earlier
        Append(wal, Scalar("m", flushed + 1_000_000_000L,  3));
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var values   = reopened.ReadAll(out _).Select(r => r.Point.Value).OrderBy(v => v).ToArray();
        reopened.Dispose();

        Assert.Equal([2.0, 3.0], values);
    }

    /// <summary>
    /// Compaction moves the survivors but cannot erase where they came from, so a crash
    /// between the move and the offset store leaves an offset covering both copies. The new
    /// tail is terminated with a generation-0 slot precisely so such a scan stops on time.
    /// </summary>
    [Fact]
    public void A_compaction_whose_offset_store_was_lost_replays_survivors_once()
    {
        long baseNano = 1_700_000_000_000_000_000L;
        long offsetBeforeCommit;

        var wal = MetricWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 6; i++) Append(wal, Scalar("m", baseNano + i, i));      // snapshot
        ulong flushing = wal.BeginFlush();
        for (int i = 6; i < 10; i++) Append(wal, Scalar("m", baseNano + i, i));     // survivors
        offsetBeforeCommit = wal.WrittenBytes;
        wal.CommitFlush(flushing);
        wal.Dispose();

        // Rewind the header to its pre-commit value: "the move landed, the store did not".
        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(8, SeekOrigin.Begin);
            fs.Write(BitConverter.GetBytes(32L + offsetBeforeCommit));
        }

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var values   = reopened.ReadAll(out _).Select(r => r.Point.Value).OrderBy(v => v).ToArray();
        reopened.Dispose();

        Assert.Equal([6.0, 7.0, 8.0, 9.0], values);   // once each, and no cold point back
    }

    [Fact]
    public void Repeated_flush_cycles_keep_the_log_and_pool_bounded()
    {
        var wal = MetricWriteAheadLog.Open(WalPath);
        for (int cycle = 0; cycle < 50; cycle++)
        {
            for (int i = 0; i < 100; i++)
                Append(wal, Scalar("m", 1_700_000_000_000_000_000L + cycle * 1000 + i, i,
                                   Labels(("cycle", cycle.ToString()))));
            wal.CommitFlush(wal.BeginFlush());
            Assert.Equal(0, wal.WrittenBytes);            // fully reclaimed each time
        }
        wal.Dispose();

        Assert.Equal(0, new FileInfo(PoolPath).Length);   // and the pool with it
    }

    [Fact]
    public void A_point_stamped_zero_does_not_truncate_the_log()
    {
        // A zero timestamp and a zero value are individually legal, so nothing about the
        // point itself can mark the end of data — only the generation can.
        var wal = MetricWriteAheadLog.Open(WalPath);
        Append(wal, Scalar("m", 1_700_000_000_000_000_000L, 1));
        Append(wal, Scalar("m", 0, 0));
        Append(wal, Scalar("m", 1_700_000_000_000_000_002L, 3));
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll(out _);
        reopened.Dispose();

        Assert.Equal(3, replayed.Count);
        Assert.Contains(replayed, r => r.Point.Value == 3);
    }

    [Theory]
    [InlineData(40)]
    [InlineData(4096)]
    public void A_write_offset_past_the_real_data_replays_only_the_real_data(int overshoot)
    {
        var wal = MetricWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 8; i++) Append(wal, Scalar("m", 1_700_000_000_000_000_000L + i, i));
        long real = wal.WrittenBytes;
        wal.Dispose();

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(8, SeekOrigin.Begin);                    // WalFileHeader.WriteOffset
            fs.Write(BitConverter.GetBytes(32 + real + overshoot));
        }

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll(out _);
        reopened.Dispose();

        Assert.Equal(8, replayed.Count);
    }

    [Fact]
    public void Grows_past_the_initial_mapping_without_losing_points()
    {
        var wal = MetricWriteAheadLog.Open(WalPath, initialCapacity: 4 * 1024);
        const int n = 2_000;
        for (int i = 0; i < n; i++) Append(wal, Scalar("m", 1_700_000_000_000_000_000L + i, i));
        wal.Dispose();

        var reopened = MetricWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll(out _);
        reopened.Dispose();

        Assert.Equal(n, replayed.Count);
        Assert.Equal(0.0, replayed[0].Point.Value);
        Assert.Equal(n - 1, replayed[^1].Point.Value);
    }

    // ── The regression this change is about ───────────────────────────────────

    private static MetricIngestItem[] Minute(long nano, int metricNames, int seriesPerName)
    {
        var items = new List<MetricIngestItem>(metricNames * seriesPerName);
        for (int m = 0; m < metricNames; m++)
        for (int s = 0; s < seriesPerName; s++)
            items.Add(Scalar($"instrument.{m}", nano + s, m * 100 + s, Labels(("series", s.ToString()))));
        return [.. items];
    }

    [Fact]
    public async Task A_trickle_of_points_writes_no_files()
    {
        // The old behaviour: the flush loop ran every 60 s regardless, and a flush writes one
        // .mts PER METRIC NAME — so 40 instruments cost 40 files a minute whatever the volume.
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);

        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int minute = 0; minute < 30; minute++)
            engine.Ingest(Minute(baseNano + minute * 60_000_000_000L, metricNames: 40, seriesPerName: 5));

        // 30 checks' worth of ticks: 6 000 points, far under the 50 000 minimum.
        Assert.Equal(0, TrcCount(_dir, "*.mts"));
        Assert.True(File.Exists(WalPath));

        // …and every point is still queryable out of the hot tier.
        var series = engine.QueryAsync("instrument.7").ToBlockingEnumerable().ToList();
        Assert.Equal(5, series.Count);

        await engine.DisposeAsync();
    }

    [Fact]
    public async Task A_real_batch_still_writes_files_and_clears_the_log()
    {
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);

        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int minute = 0; minute < 30; minute++)                  // 30 x 2 000 = 60 000 points
            engine.Ingest(Minute(baseNano + minute * 60_000_000_000L, metricNames: 40, seriesPerName: 50));

        await engine.DisposeAsync();                                  // final flush

        Assert.True(TrcCount(_dir, "*.mts") > 0);

        var wal = MetricWriteAheadLog.Open(WalPath);
        Assert.Empty(wal.ReadAll(out _));                             // gave its points to the files
        wal.Dispose();
    }

    [Fact]
    public async Task Unflushed_points_come_back_after_a_crash()
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var labels = Labels(("service", "MintRoute.API"));

        // A log left behind by a process that died before any flush.
        var crashed = MetricWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 12; i++)
            Append(crashed, Scalar("cpu.utilisation", baseNano + i * 1_000_000L, i * 2.0, labels));
        crashed.Dispose();

        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);

        var series = engine.QueryAsync("cpu.utilisation").ToBlockingEnumerable().ToList();
        Assert.Single(series);
        Assert.Equal(12, series[0].Points.Count);
        Assert.Equal(0, TrcCount(_dir, "*.mts"));

        // The catalog is rebuilt from the recovered points too, not only from cold files.
        Assert.Contains(engine.GetCatalog(), e => e.Name == "cpu.utilisation");
        Assert.Contains("service", engine.GetLabelKeys("cpu.utilisation"));

        await engine.DisposeAsync();
    }

    /// <summary>
    /// Engine-level counterpart: ingest from several threads across a threshold-triggered
    /// flush and account for every point afterwards. The flush drains the tier concurrently
    /// with those appends, so anything the snapshot boundary mishandles shows up as a point
    /// that reached neither a file nor the surviving hot tier.
    /// </summary>
    [Fact]
    public async Task No_point_is_lost_when_ingest_races_a_flush()
    {
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);

        const int threads = 8;
        const int perThread = 80_000;              // 640 000 total, past HotFlushThreshold
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        await Task.WhenAll(Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            var buf = new MetricIngestItem[1];
            for (int i = 0; i < perThread; i++)
            {
                buf[0] = Scalar("race.metric", baseNano + i * 1_000L, 1.0,
                                Labels(("thread", t.ToString())));
                engine.Ingest(buf);
            }
        })));

        await engine.DisposeAsync();              // final flush lands whatever is left

        // Every ingested point must be in a file. A logical series spans as many entries as
        // the files it was split across, so account for points and label sets, not entries.
        var reader = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        var series = await QueryWhenColdLoadedAsync(reader, "race.metric");
        long total  = series.Sum(s => (long)s.Points.Count);
        int  labels = series.Select(s => s.Labels).Distinct().Count();
        await reader.DisposeAsync();

        Assert.Equal(threads, labels);
        Assert.Equal((long)threads * perThread, total);
    }

    // ── Shutdown versus a flush that belongs to neither background loop ───────

    /// <summary>
    /// A tier whose files take long enough to write that the flush is provably still running
    /// while shutdown does its work — 300 000 points over 2 000 series, which is also
    /// comfortably under <c>HotFlushThreshold</c>, so the only flushes in these tests are the
    /// ones they schedule.
    /// </summary>
    private static MetricIngestItem[] SlowFlushBatch(
        long baseNano, string seriesPrefix = "s", int series = 2_000, int pointsPerSeries = 150)
    {
        var items = new List<MetricIngestItem>(series * pointsPerSeries);
        for (int s = 0; s < series; s++)
        for (int p = 0; p < pointsPerSeries; p++)
            items.Add(Scalar("instrument.0", baseNano + p * 1_000_000L, p,
                             Labels(("series", seriesPrefix + s))));
        return [.. items];
    }

    /// <summary>Waits until a scheduled flush has taken its snapshot and moved on to the files.</summary>
    private static async Task DrainedAsync(MetricStorageEngine engine)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (engine.HotPointCount > 0)
        {
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(30), "the scheduled flush never took its snapshot");
            await Task.Delay(1);
        }
    }

    /// <summary>
    /// Copies the data directory as it stands, which is what killing the process at this
    /// instant would leave behind. A file still being written is held with
    /// <see cref="FileShare.None"/> by the writer, so it copies as "not there" — which is
    /// exactly what it would be.
    /// </summary>
    private string FreezeDataDir()
    {
        _frozen = _dir + "-frozen";
        Directory.CreateDirectory(_frozen);
        foreach (var f in Directory.GetFiles(_dir))
        {
            try { File.Copy(f, Path.Combine(_frozen, Path.GetFileName(f))); }
            catch (IOException) { /* mid-write — a kill would not have it either */ }
        }
        return _frozen;
    }

    /// <summary>
    /// Everything durable in <paramref name="dir"/>, counted the way the next start counts it:
    /// the log replayed into the hot tier plus every cold file. Short of what was ingested
    /// means points were lost, over means they came back twice.
    /// </summary>
    private static async Task<long> DurablePointsAsync(string dir, string metric)
    {
        var reader = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
        var series = await QueryWhenColdLoadedAsync(reader, metric);
        long total = series.Sum(s => (long)s.Points.Count);
        await reader.DisposeAsync();
        return total;
    }

    private static string Verdict(long durable, long expected) =>
        durable < expected
            ? $"LOST {expected - durable} of {expected} points"
            : $"DUPLICATED {durable - expected} points ({durable} where {expected} were ingested)";

    /// <summary>
    /// Two threshold flushes, where the one that finishes FIRST is the one whose handle is
    /// published LAST: a thread descheduled between starting its flush and storing the handle
    /// stores an already-completed task over a live one. A single <c>Task</c> field then holds
    /// a task that is done, and shutdown walks straight past the flush still writing its
    /// files. Membership in a set cannot be overwritten, so there is no order to lose.
    /// </summary>
    [Fact]
    public async Task A_finished_flush_cannot_hide_one_that_is_still_writing()
    {
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        engine.Ingest(SlowFlushBatch(baseNano));
        var writing = engine.ScheduleThresholdFlushForTest();
        await DrainedAsync(engine);                          // past its snapshot, into the files

        // Empty tier, so this returns at snapshot.Count == 0 — the flush that finishes first
        // and publishes last.
        await engine.ScheduleThresholdFlushForTest();
        Assert.False(writing.IsCompleted, "setup: the first flush must still be writing its files");

        await engine.DisposeAsync();
        int running = engine.RunningThresholdFlushes;

        Assert.True(running == 0,
            $"DisposeAsync returned with {running} threshold flush(es) still inside FlushHotTierAsync, " +
            "with the WAL and both locks being disposed underneath them");
        Assert.True(writing.IsCompletedSuccessfully, "the tracked flush did not complete cleanly");
        Assert.True(TrcCount(_dir, "*.mts") > 0);
    }

    /// <summary>
    /// The other half of "shutdown cannot see it". A single handle is read once and cannot
    /// cover a flush scheduled afterwards, and ingest is deliberately NOT gated on shutdown —
    /// hosted services stop in reverse registration order, so Kestrel serves
    /// <c>/otlp/v1/metrics</c> throughout this. Scheduled here at the worst possible moment,
    /// after DisposeAsync has already returned: the flush must find the door shut rather than
    /// enter a disposed lock.
    /// </summary>
    [Fact]
    public async Task A_flush_scheduled_after_shutdown_touches_nothing()
    {
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        engine.Ingest(Minute(baseNano, metricNames: 4, seriesPerName: 50));

        await engine.DisposeAsync();

        var orphan = engine.ScheduleThresholdFlushForTest();
        await orphan;   // ObjectDisposedException out of here is the defect, not a flaky test
        Assert.Equal(0, engine.RunningThresholdFlushes);
    }

    /// <summary>
    /// The loss variant, and the one that actually bites in production: ingest is still
    /// flowing, so the loop's final flush has real data, opens generation G+1 and COMMITS it —
    /// which reclaims everything at or below G+1, the orphan's own generation G included. Its
    /// records leave the log while its file is still being written, so a kill at that instant
    /// finds them in neither place. Measured exactly that way: the directory is frozen the
    /// instant DisposeAsync returns.
    /// </summary>
    [Fact]
    public async Task A_kill_the_instant_shutdown_returns_loses_nothing_when_the_tier_was_not_empty()
    {
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var flushed = SlowFlushBatch(baseNano);
        engine.Ingest(flushed);
        var writing = engine.ScheduleThresholdFlushForTest();
        await DrainedAsync(engine);
        await engine.ScheduleThresholdFlushForTest();        // the handle a single field keeps
        Assert.False(writing.IsCompleted, "setup: the orphan must still be writing its files");

        var stillArriving = SlowFlushBatch(baseNano + 3_600_000_000_000L, "late", series: 200, pointsPerSeries: 10);
        engine.Ingest(stillArriving);                        // tier NOT empty at the final flush

        await engine.DisposeAsync();
        string frozen = FreezeDataDir();

        long expected = flushed.Length + stillArriving.Length;
        long durable  = await DurablePointsAsync(frozen, "instrument.0");
        Assert.True(durable == expected, Verdict(durable, expected));
    }

    /// <summary>
    /// The duplicate variant, which is what the tier being empty turns the same shutdown into:
    /// the final flush returns at <c>snapshot.Count == 0</c> before <c>BeginFlush</c>, so the
    /// orphan's generation is never superseded and never committed — the log still holds every
    /// point whose file the orphan did land. Measured after everything settles, because that
    /// is when it shows: on the next start, beside the file.
    /// </summary>
    [Fact]
    public async Task A_quiesced_tier_does_not_replay_the_orphans_points_beside_its_file()
    {
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var flushed = SlowFlushBatch(baseNano);
        engine.Ingest(flushed);
        var writing = engine.ScheduleThresholdFlushForTest();
        await DrainedAsync(engine);
        await engine.ScheduleThresholdFlushForTest();        // the handle a single field keeps
        Assert.False(writing.IsCompleted, "setup: the orphan must still be writing its files");

        await engine.DisposeAsync();                          // tier quiesced — no final flush data
        try { await writing; } catch (ObjectDisposedException) { /* the orphan's own end */ }

        long durable = await DurablePointsAsync(_dir, "instrument.0");
        Assert.True(durable == flushed.Length, Verdict(durable, flushed.Length));
    }

    [Fact]
    public async Task Existing_files_survive_the_upgrade_and_stay_queryable()
    {
        // The deploy case: a data directory already holding .mts files from the old build,
        // opened for the first time by an engine that now keeps a WAL beside them.
        long oldNano = DateTimeOffset.UtcNow.AddMinutes(-30).ToUnixTimeMilliseconds() * 1_000_000L;

        var first = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        first.Ingest([Scalar("legacy.metric", oldNano, 7, Labels(("host", "node0")))]);
        await first.DisposeAsync();                                   // flushes, then resets the log

        int filesBefore = TrcCount(_dir, "*.mts");
        Assert.True(filesBefore > 0);

        var second = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);

        // Cold-segment discovery is deliberately kept off the startup path so ingest works
        // from second zero — a query issued immediately after construction can legitimately
        // outrun the background scan, WAL or no WAL.
        var series = await QueryWhenColdLoadedAsync(second, "legacy.metric");

        Assert.Single(series);
        Assert.Equal(7, series[0].Points[0].Value);
        Assert.Equal(filesBefore, TrcCount(_dir, "*.mts"));           // nothing rewritten on open

        await second.DisposeAsync();
    }
}
