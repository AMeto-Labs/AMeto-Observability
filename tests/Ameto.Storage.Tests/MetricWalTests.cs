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
public sealed class MetricWalTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mwal-" + Guid.NewGuid().ToString("N"));

    /// <summary>Set by <see cref="FreezeDataDir"/> — see there for what it holds.</summary>
    private string? _frozen;

    /// <summary>
    /// Every engine this class has built, so that <see cref="DisposeAsync"/> can close the ones a
    /// test did not reach. A test that ends at a failed assertion skips whatever disposal stood
    /// below it, an undisposed engine holds <c>metrics.wal</c> mapped, and the cleanup below then
    /// cannot delete the directory and swallows saying so. One such run leaves 32 MB behind — the
    /// log grows by doubling — which is litter produced EXACTLY on the days someone is debugging,
    /// and enough of it stops runs failing on the code and starts them failing on there being
    /// nowhere left to write.
    /// </summary>
    private readonly List<MetricStorageEngine> _engines = [];

    /// <summary>
    /// The same registry for the logs a test opens DIRECTLY, which the engine registry above does
    /// not cover and which leak by exactly the same mechanism. Most tests in this class never
    /// build an engine at all — they drive <see cref="MetricWriteAheadLog"/> itself and close it
    /// with a bare <c>wal.Dispose()</c> standing below their assertions, so a red assertion walks
    /// past it and leaves the log mapped. Measured: <c>Assert.Fail</c> one line above the dispose
    /// in <see cref="An_abandoned_flush_frees_the_log_to_flush_again_and_keeps_its_records"/> and
    /// the run leaves an 8.1 MB <c>ameto-mwal-*</c> directory behind — <c>metrics.wal</c> at its
    /// 8 MiB default capacity, plus the pool — because <see cref="Directory.Delete(string,bool)"/>
    /// cannot remove a mapped file and the <c>catch { }</c> below says nothing about failing.
    /// </summary>
    private readonly List<MetricWriteAheadLog> _wals = [];

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        // The raw handles first: they map the same file an engine's final flush is about to
        // write, and a closed one cannot be in its way.
        for (int i = _wals.Count - 1; i >= 0; i--)
            try { _wals[i].Dispose(); } catch { }

        // Reverse order: a test's later engines are the ones reading what its earlier ones wrote.
        for (int i = _engines.Count - 1; i >= 0; i--)
            try { await _engines[i].DisposeAsync(); } catch { }

        try { Directory.Delete(_dir, true); } catch { }
        if (_frozen is not null) { try { Directory.Delete(_frozen, true); } catch { } }
    }

    /// <summary>
    /// The only way this class builds an engine. Double disposal is what the registry relies on
    /// being free: most tests close their own engine mid-body, because closing it is how the log
    /// gets its final flush, and none of them should have to unregister it to do that.
    /// </summary>
    private MetricStorageEngine NewEngine(string? dir = null,
                                          Microsoft.Extensions.Logging.ILogger<MetricStorageEngine>? logger = null)
    {
        var engine = new MetricStorageEngine(dir ?? _dir, logger ?? NullLogger<MetricStorageEngine>.Instance);
        _engines.Add(engine);
        return engine;
    }

    /// <summary>
    /// The only way this class opens a log, for the same reason and with the same tolerance:
    /// <see cref="MetricWriteAheadLog.Dispose"/> returns on a closed log, so a test that closes
    /// its own mid-body — which most of them must, since reopening the file is how they assert
    /// what replays — pays nothing for being closed again at the end.
    /// </summary>
    private MetricWriteAheadLog OpenWal(long? initialCapacity = null,
                                        Microsoft.Extensions.Logging.ILogger? logger = null)
    {
        var wal = initialCapacity is { } capacity
            ? MetricWriteAheadLog.Open(WalPath, capacity, logger)
            : MetricWriteAheadLog.Open(WalPath, logger: logger);
        _wals.Add(wal);
        return wal;
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

        var wal = OpenWal();
        for (int i = 0; i < 20; i++)
            Append(wal, Scalar("http.server.duration", 1_700_000_000_000_000_000L + i * 1_000_000L, i * 1.5, labels));
        wal.Dispose();

        var reopened = OpenWal();
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

        var wal = OpenWal();
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

        var reopened = OpenWal();
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

        var wal = OpenWal();
        for (int i = 0; i < 500; i++)
            Append(wal, Scalar("cpu.utilisation", 1_700_000_000_000_000_000L + i, i, labels));
        long walBytes = wal.WrittenBytes;
        wal.Dispose();

        long poolBytes = new FileInfo(PoolPath).Length;

        Assert.Equal(500 * 48, walBytes);      // fixed 48-byte entries, no per-point labels
        Assert.True(poolBytes < 200, $"pool should hold one record, got {poolBytes} B");

        var reopened = OpenWal();
        Assert.Equal(500, reopened.ReadAll(out _).Count);
        reopened.Dispose();
    }

    [Fact]
    public void Distinct_label_sets_stay_distinct_across_replay()
    {
        var a = Labels(("route", "/a"));
        var b = Labels(("route", "/b"));

        var wal = OpenWal();
        Append(wal, Scalar("req.count", 1_700_000_000_000_000_000L, 1, a));
        Append(wal, Scalar("req.count", 1_700_000_000_000_000_001L, 2, b));
        Append(wal, Scalar("req.count", 1_700_000_000_000_000_002L, 3, a));
        wal.Dispose();

        var reopened = OpenWal();
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
        var wal = OpenWal();
        for (int i = 0; i < 10; i++) Append(wal, Scalar("m", 1_700_000_000_000_000_000L + i, i));
        wal.CommitFlush(wal.BeginFlush());
        wal.Dispose();

        var reopened = OpenWal();
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

        var wal = OpenWal();
        for (int i = 0; i < 5; i++) Append(wal, Scalar("m", baseNano + i, i));   // in the snapshot

        ulong flushing = wal.BeginFlush();
        for (int i = 5; i < 9; i++) Append(wal, Scalar("m", baseNano + i, i));   // arrive mid-write

        wal.CommitFlush(flushing);
        wal.Dispose();

        var reopened = OpenWal();
        var replayed = reopened.ReadAll(out int unresolved);
        reopened.Dispose();

        Assert.Equal(0, unresolved);                                  // the pool survived too
        Assert.Equal([5.0, 6.0, 7.0, 8.0], replayed.Select(r => r.Point.Value).OrderBy(v => v));
    }

    [Fact]
    public void A_flush_that_never_commits_leaves_everything_replayable()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        var wal = OpenWal();
        for (int i = 0; i < 5; i++) Append(wal, Scalar("m", baseNano + i, i));
        wal.BeginFlush();                                             // files failed to write
        for (int i = 5; i < 8; i++) Append(wal, Scalar("m", baseNano + i, i));
        wal.Dispose();

        var reopened = OpenWal();
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

        var wal = OpenWal();
        Append(wal, Scalar("m", flushed, 1));
        ulong gen = wal.BeginFlush();
        wal.CommitFlush(gen);

        Append(wal, Scalar("m", flushed - 30_000_000_000L, 2));   // stamped 30 s earlier
        Append(wal, Scalar("m", flushed + 1_000_000_000L,  3));
        wal.Dispose();

        var reopened = OpenWal();
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

        var wal = OpenWal();
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

        var reopened = OpenWal();
        var values   = reopened.ReadAll(out _).Select(r => r.Point.Value).OrderBy(v => v).ToArray();
        reopened.Dispose();

        Assert.Equal([6.0, 7.0, 8.0, 9.0], values);   // once each, and no cold point back
    }

    [Fact]
    public void Repeated_flush_cycles_keep_the_log_and_pool_bounded()
    {
        var wal = OpenWal();
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

    // ── One flush at a time, and why the watermark needs it ──────────────────

    /// <summary>
    /// The crux of the metrics-WAL loss, driven at the log with no timing at all. A flush opens
    /// generation G and starts writing its files; a second flush opens G+1 and finishes first.
    /// Its commit sets the watermark to G+1, and the watermark frees everything AT OR BELOW it —
    /// so G's records left the log while G's files were still being written. From that instant
    /// the first flush's points were in no file and no log, and its own later commit was a
    /// no-op (<c>flushedGeneration &lt;= _committedGeneration</c>), so nothing put them back.
    ///
    /// <para>Asserted on the OUTCOME, not the mechanism: the log now refuses to open a second
    /// flush while one is still writing, so the second <c>BeginFlush</c> throws rather than
    /// returning G+1. Either way what must be true is the same — nothing a flush has not
    /// finished writing may leave the log.</para>
    /// </summary>
    [Fact]
    public void A_later_flushs_commit_cannot_reclaim_one_that_is_still_writing()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        var wal = OpenWal();
        for (int i = 0; i < 5; i++) Append(wal, Scalar("m", baseNano + i, i));
        _ = wal.BeginFlush();                                 // flush A — still writing its files
        for (int i = 5; i < 9; i++) Append(wal, Scalar("m", baseNano + i, i));

        long bytesBefore = wal.WrittenBytes;

        // Flush B, exactly as an overlapping periodic flush performed it.
        try
        {
            ulong second = wal.BeginFlush();
            wal.CommitFlush(second);
        }
        catch (InvalidOperationException) { /* the fix refuses to open it at all */ }

        Assert.Equal(bytesBefore, wal.WrittenBytes);          // not one record reclaimed
        wal.Dispose();

        var reopened = OpenWal();
        var values   = reopened.ReadAll(out _).Select(r => r.Point.Value).OrderBy(v => v).ToArray();
        reopened.Dispose();

        Assert.Equal([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], values);
    }

    /// <summary>
    /// The same rule stated where it is enforced rather than where it is caused: the watermark
    /// may only ever name the flush that is currently open. A commit for anything else moves
    /// nothing — which is what keeps the guarantee true even if a caller ever reintroduces
    /// concurrent flushes by some route <see cref="MetricWriteAheadLog.BeginFlush"/> cannot see.
    /// </summary>
    [Fact]
    public void A_commit_for_a_generation_that_is_not_the_open_flush_reclaims_nothing()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        var wal = OpenWal();
        for (int i = 0; i < 5; i++) Append(wal, Scalar("m", baseNano + i, i));
        ulong first = wal.BeginFlush();
        for (int i = 5; i < 9; i++) Append(wal, Scalar("m", baseNano + i, i));

        long bytesBefore = wal.WrittenBytes;
        wal.CommitFlush(first + 1);                            // a generation nobody opened
        Assert.Equal(bytesBefore, wal.WrittenBytes);

        // And the real commit still works, reclaiming exactly its own prefix.
        wal.CommitFlush(first);
        Assert.True(wal.WrittenBytes < bytesBefore);
        wal.Dispose();

        var reopened = OpenWal();
        var values   = reopened.ReadAll(out _).Select(r => r.Point.Value).OrderBy(v => v).ToArray();
        reopened.Dispose();

        Assert.Equal([5.0, 6.0, 7.0, 8.0], values);            // the survivors, once each
    }

    /// <summary>
    /// A second flush cannot begin while one is open, and the refusal happens before the caller
    /// has drained anything — the engine calls <c>BeginFlush</c> ahead of its snapshot precisely
    /// so a throw here costs a flush rather than a tier.
    /// </summary>
    [Fact]
    public void The_log_refuses_to_open_a_second_flush_while_one_is_writing()
    {
        var wal = OpenWal();
        Append(wal, Scalar("m", 1_700_000_000_000_000_000L, 1));

        ulong open = wal.BeginFlush();
        Assert.Throws<InvalidOperationException>(() => wal.BeginFlush());

        wal.CommitFlush(open);
        wal.BeginFlush();                                      // and the next one is free to run
        wal.Dispose();
    }

    /// <summary>
    /// The other way a flush could not be opened, which used to be answered with a generation
    /// instead of a refusal: the log is alive but has no mapping. <c>Grow</c> unmaps before it
    /// extends — Windows will not resize a mapped file — and if the extend AND the restoring
    /// re-map both fail, which is one full disk away, the object stays alive with no mapping and
    /// <c>_disposed</c> false for the rest of the process.
    ///
    /// <para>From there <c>Append</c> throws, so ingest fails honestly. <c>BeginFlush</c> did
    /// not: it returned the current generation without registering it and without bumping the
    /// counter, so the flush read it as a generation it owned, drained the tier, wrote the .mts
    /// files — and <c>CommitFlush</c> then refused the same value on the same condition. The
    /// watermark never moved, so those points were in a file AND above the watermark, and every
    /// restart replayed them beside the file that already held them. Duplicates, not loss, and
    /// once per start for as long as the state lasts.</para>
    ///
    /// <para>The state is built here by making the log file read-only: both of <c>Grow</c>'s
    /// opens ask for write access, so both fail, which is what leaves the pointer null.</para>
    /// </summary>
    [Fact]
    public void A_log_that_lost_its_mapping_refuses_to_open_a_flush()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        using var wal = OpenWal(64 * 1024);   // ~1 365 entries
        Append(wal, Scalar("m", baseNano, 1));

        // The disk that fills mid-run, injected through the seam: the ReadOnly-attribute trick
        // this used died with the lifetime handle (Windows enforces the attribute at CreateFile
        // time, and resizes no longer reopen the file).
        wal.BeforeResize = static _ => throw new IOException("disk full (test seam)");
        try
        {
            var grew = Record.Exception(() =>
            {
                for (int i = 1; i < 5_000; i++) Append(wal, Scalar("m", baseNano + i, i));
            });
            Assert.NotNull(grew);   // setup: Grow really did fail rather than extend the file

            // Alive, not disposed, and unable to log anything: this is the honest half.
            var appended = Record.Exception(() => Append(wal, Scalar("m", baseNano, 1)));
            Assert.IsType<InvalidOperationException>(appended);   // not ObjectDisposedException

            // So a flush must fail the same way rather than be handed a generation nobody
            // opened. Exact type: this is the unmapped log, not a disposed one.
            var began = Record.Exception(() => wal.BeginFlush());
            Assert.IsType<InvalidOperationException>(began);
        }
        finally { wal.BeforeResize = null; }
    }

    /// <summary>
    /// The OTHER end of the same fault, and the one a refusal to begin cannot reach: the log
    /// was healthy when the flush opened its generation and lost its mapping while the files
    /// were being written. There is no flush to refuse by then — there is a caller holding
    /// finished .mts files and asking whether the watermark now covers them.
    ///
    /// <para>The answer was NOTHING, which reads exactly like the answer a successful commit
    /// gives. So the flush published its files, logged a debug line and moved on, while the
    /// generation's records sat in the log above an unchanged watermark: the next start
    /// replayed every one of those points beside the file that already held them, and the start
    /// after that did it again. Metric points are summed, so that is not a visible artefact but
    /// a wrong number, once per start for as long as the state lasts.</para>
    ///
    /// <para>Asserted here as the two facts a caller needs and could not previously tell apart:
    /// the commit says <c>Refused</c>, and the records it refused to reclaim are still whole —
    /// which is what makes the caller's files the deletable copy rather than the only one.</para>
    /// </summary>
    [Fact]
    public void A_commit_that_cannot_move_the_watermark_refuses_and_keeps_its_records()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        using var wal = OpenWal(64 * 1024);   // ~1 365 entries
        for (int i = 0; i < 3; i++) Append(wal, Scalar("m", baseNano + i, i));

        // A flush the log was perfectly able to open. Its .mts files are written from here.
        ulong flushing = wal.BeginFlush();
        long  logged   = wal.WrittenBytes;

        // Same seam-injected disk-full as above; see A_log_that_lost_its_mapping.
        wal.BeforeResize = static _ => throw new IOException("disk full (test seam)");
        try
        {
            var grew = Record.Exception(() =>
            {
                for (int i = 1; i < 5_000; i++) Append(wal, Scalar("m", baseNano + i, i));
            });
            Assert.NotNull(grew);   // setup: the mapping is gone, mid-flush

            Assert.Equal(MetricWalCommit.Refused, wal.CommitFlush(flushing));

            // Nothing was reclaimed, so the generation is still replayable in full. This is the
            // half the argument for deleting the files rests on: the log's copy is whole, so
            // removing the flush's copy leaves the points durable exactly once rather than none.
            Assert.True(wal.WrittenBytes >= logged,
                "the refused commit reclaimed records it had not covered by a watermark");
        }
        finally { wal.BeforeResize = null; }
    }

    /// <summary>
    /// The same answer from a log that is closed rather than unmapped — the shape shutdown
    /// used to produce, where a flush outlived the teardown and committed into a disposed log.
    /// </summary>
    [Fact]
    public void A_commit_into_a_closed_log_refuses()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        var wal = OpenWal();
        for (int i = 0; i < 3; i++) Append(wal, Scalar("m", baseNano + i, i));

        ulong flushing = wal.BeginFlush();
        wal.Dispose();

        Assert.Equal(MetricWalCommit.Refused, wal.CommitFlush(flushing));

        // The records went nowhere: Dispose unmaps, it does not truncate, so a reopened log
        // still replays the generation the commit could not cover.
        using var reopened = OpenWal();
        Assert.Equal(3, reopened.ReadAll(out _).Count);
    }

    /// <summary>
    /// The positive half, without which "Refused" carries no information: a commit that moves
    /// the watermark says <c>Committed</c>, and so does one whose generation the watermark
    /// already covers — the caller's points are in files either way, and the second must not be
    /// mistaken for the failure that makes it delete them.
    /// </summary>
    [Fact]
    public void A_commit_that_covers_its_generation_says_so_even_when_it_reclaims_nothing()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        using var wal = OpenWal();
        for (int i = 0; i < 3; i++) Append(wal, Scalar("m", baseNano + i, i));

        ulong first = wal.BeginFlush();
        Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(first));
        Assert.Equal(0, wal.WrittenBytes);

        // Already at or below the watermark, so there is nothing to reclaim and nothing to
        // regret. Answering Refused here would tell a flush to delete files that ARE the
        // durable copy, which is loss rather than the duplicate the answer exists to prevent.
        Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(first));
    }

    /// <summary>
    /// The same fault end to end, and the reason a returned value was worth the change: a flush
    /// that opened its generation on a healthy log, wrote its <c>.mts</c>, and found the log
    /// unmapped by the time it came to commit. Nothing about it is a failed flush — the file is
    /// complete at its final path — so nothing puts the points back and nothing throws.
    ///
    /// <para>What that used to leave is two copies of every point in it: the file, and the
    /// generation's records sitting in the log above a watermark that never moved. The next
    /// start replays the second beside the first, and metric points are SUMMED, so the result
    /// is not a visible duplicate row but a counter and a sum that are both twice what was
    /// measured — every start, for as long as those records last. It was argued to be
    /// unavoidable. It is not: a duplicate needs two copies, the reclaim never ran, so the
    /// log's copy is whole and the flush's is the deletable one.</para>
    ///
    /// <para>Driven by filling the log to within a few thousand entries of its capacity and
    /// then, from the seam that fires once the file is in place, arming the WAL's resize seam
    /// to throw and ingesting past the end of it. <c>Grow</c> unmaps before it extends and can
    /// re-map neither, which is the production state exactly: alive, unmapped, refusing every
    /// append, with a flush's generation still open. The count is taken from a SECOND engine
    /// over the same directory, because the question is what a restart sees.</para>
    /// </summary>
    [Fact]
    public async Task A_flush_whose_commit_is_refused_leaves_its_points_in_exactly_one_place()
    {
        var logger = new RecordingLogger();
        var engine = NewEngine(logger: logger);
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        // 48 bytes an entry against an 8 MB log: this leaves ~14 000 entries of room, and none
        // of it is reclaimed before the commit that never happens.
        const int flushed = 160_000;
        var batch = new MetricIngestItem[10_000];
        for (int done = 0; done < flushed; done += batch.Length)
        {
            for (int i = 0; i < batch.Length; i++)
                batch[i] = Scalar("flushed.metric", baseNano + (done + i) * 1_000L, 1.0,
                                  Labels(("series", (i & 15).ToString())));
            engine.Ingest(batch);
        }

        try
        {
            engine.OnFileWrittenForTest = _ =>
            {
                // Seam-injected: the ReadOnly trick died with the lifetime handle (see
                // A_log_that_lost_its_mapping for the mechanics).
                engine.WalForTest.BeforeResize = static _ => throw new IOException("disk full (test seam)");

                // Past the capacity, so the append underneath has to Grow — and cannot. The
                // throw is this batch's, not the flush's: ingest fails honestly from here,
                // which is the loud half of the fault and not what is under test. Swallowed
                // whatever it is (Grow rethrows the file system's own refusal), because a
                // throw OUT of this seam is a failed write, and a failed write is the other
                // path entirely — it restores, abandons, and never reaches a commit.
                var poison = new MetricIngestItem[20_000];
                for (int i = 0; i < poison.Length; i++)
                    poison[i] = Scalar("poison.metric", baseNano + i * 1_000L, 1.0);
                try { engine.Ingest(poison); } catch { /* the log is dead; that is the setup */ }
            };

            await engine.ScheduleThresholdFlushForTest();

            // The flush's own file is gone, and it never reached the cold list either.
            Assert.Empty(Directory.GetFiles(_dir, "*.mts"));
            Assert.True(logger.Saw(Microsoft.Extensions.Logging.LogLevel.Error,
                    "Metric log refused to commit generation", out _),
                "the commit went nowhere and nothing said so");
        }
        finally
        {
            engine.OnFileWrittenForTest = null;
            engine.WalForTest.BeforeResize = null;
        }

        await engine.DisposeAsync();

        // What the restart sees. Pre-fix this is 320 000 — the file the flush published plus
        // the generation the log replays beside it.
        long durable = await DurablePointsAsync(_dir, "flushed.metric");
        Assert.True(durable == flushed, Verdict(durable, flushed));
    }

    /// <summary>
    /// The failure path's exit. A flush whose files threw puts its points back into the hot tier
    /// WITHOUT re-logging them, so its records are what keeps them durable until a later flush
    /// carries them — the generation is abandoned, never committed. Without an explicit
    /// abandon the one-open-flush rule would wedge the log after the first failed flush: no
    /// further flush could begin, the watermark would never advance again, and every restart
    /// would replay everything since the failure.
    /// </summary>
    [Fact]
    public void An_abandoned_flush_frees_the_log_to_flush_again_and_keeps_its_records()
    {
        long baseNano = 1_700_000_000_000_000_000L;

        var wal = OpenWal();
        for (int i = 0; i < 3; i++) Append(wal, Scalar("m", baseNano + i, i));
        long logged = wal.WrittenBytes;

        ulong failed = wal.BeginFlush();
        wal.AbandonFlush(failed);                              // the write threw; points went back
        Assert.Equal(logged, wal.WrittenBytes);                // and its records stayed

        // The retry: the restored points are re-appended by the next snapshot's own generation,
        // and committing THAT covers the abandoned one — which is the only thing that ever
        // reclaims it.
        for (int i = 0; i < 3; i++) Append(wal, Scalar("m", baseNano + i, i));
        ulong retry = wal.BeginFlush();
        wal.CommitFlush(retry);
        Assert.Equal(0, wal.WrittenBytes);
        wal.Dispose();
    }

    /// <summary>
    /// <see cref="Repeated_flush_cycles_keep_the_log_and_pool_bounded"/> with a second flush
    /// attempted inside every cycle. A rule that held the watermark back without a way to
    /// release it would show up here as a log that never empties.
    /// </summary>
    [Fact]
    public void Interleaved_flush_attempts_keep_the_log_bounded()
    {
        var wal = OpenWal();
        for (int cycle = 0; cycle < 50; cycle++)
        {
            for (int i = 0; i < 100; i++)
                Append(wal, Scalar("m", 1_700_000_000_000_000_000L + cycle * 1000 + i, i,
                                   Labels(("cycle", cycle.ToString()))));

            ulong open = wal.BeginFlush();
            try { wal.CommitFlush(wal.BeginFlush()); }         // the overlapping flush
            catch (InvalidOperationException) { }
            wal.CommitFlush(open);

            Assert.Equal(0, wal.WrittenBytes);                 // still fully reclaimed each time
        }
        wal.Dispose();

        Assert.Equal(0, new FileInfo(PoolPath).Length);
    }

    [Fact]
    public void A_point_stamped_zero_does_not_truncate_the_log()
    {
        // A zero timestamp and a zero value are individually legal, so nothing about the
        // point itself can mark the end of data — only the generation can.
        var wal = OpenWal();
        Append(wal, Scalar("m", 1_700_000_000_000_000_000L, 1));
        Append(wal, Scalar("m", 0, 0));
        Append(wal, Scalar("m", 1_700_000_000_000_000_002L, 3));
        wal.Dispose();

        var reopened = OpenWal();
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
        var wal = OpenWal();
        for (int i = 0; i < 8; i++) Append(wal, Scalar("m", 1_700_000_000_000_000_000L + i, i));
        long real = wal.WrittenBytes;
        wal.Dispose();

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(8, SeekOrigin.Begin);                    // WalFileHeader.WriteOffset
            fs.Write(BitConverter.GetBytes(32 + real + overshoot));
        }

        var reopened = OpenWal();
        var replayed = reopened.ReadAll(out _);
        reopened.Dispose();

        Assert.Equal(8, replayed.Count);
    }

    [Fact]
    public void Grows_past_the_initial_mapping_without_losing_points()
    {
        var wal = OpenWal(4 * 1024);
        const int n = 2_000;
        for (int i = 0; i < n; i++) Append(wal, Scalar("m", 1_700_000_000_000_000_000L + i, i));
        wal.Dispose();

        var reopened = OpenWal();
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
        var engine = NewEngine();

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
        var engine = NewEngine();

        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int minute = 0; minute < 30; minute++)                  // 30 x 2 000 = 60 000 points
            engine.Ingest(Minute(baseNano + minute * 60_000_000_000L, metricNames: 40, seriesPerName: 50));

        await engine.DisposeAsync();                                  // final flush

        Assert.True(TrcCount(_dir, "*.mts") > 0);

        var wal = OpenWal();
        Assert.Empty(wal.ReadAll(out _));                             // gave its points to the files
        wal.Dispose();
    }

    [Fact]
    public async Task Unflushed_points_come_back_after_a_crash()
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var labels = Labels(("service", "MintRoute.API"));

        // A log left behind by a process that died before any flush.
        var crashed = OpenWal();
        for (int i = 0; i < 12; i++)
            Append(crashed, Scalar("cpu.utilisation", baseNano + i * 1_000_000L, i * 2.0, labels));
        crashed.Dispose();

        var engine = NewEngine();

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
        var engine = NewEngine();

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
        var reader = NewEngine();
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
    private async Task<long> DurablePointsAsync(string dir, string metric)
    {
        var reader = NewEngine(dir);
        var series = await QueryWhenColdLoadedAsync(reader, metric);
        long total = series.Sum(s => (long)s.Points.Count);
        await reader.DisposeAsync();
        return total;
    }

    private static string Verdict(long durable, long expected) =>
        durable < expected
            ? $"LOST {expected - durable} of {expected} points"
            : $"DUPLICATED {durable - expected} points ({durable} where {expected} were ingested)";

    // ── A periodic flush overlapping a threshold flush ────────────────────────

    /// <summary>
    /// The production shape of the same loss, end to end, and with no race in it. The threshold
    /// CAS gates threshold flushes against each other only; the PERIODIC flush is behind nothing,
    /// so it could snapshot, write its (small) files and commit while a threshold flush was still
    /// writing its own. Its commit set the watermark to G+1, and the watermark frees everything
    /// at or below it — so the threshold flush's records left the log mid-write.
    ///
    /// <para>Held open at the seam rather than raced: the first flush parks between its snapshot
    /// and its files, which is exactly the window, and the directory is frozen while it is
    /// parked. Frozen there the first flush has written nothing at all, so the count is exact in
    /// both directions — no partially-written file can supply a duplicate and disguise a loss.
    /// Before the fix this reports LOST 300 000 of 350 000 on every run.</para>
    /// </summary>
    [Fact]
    public async Task A_periodic_flush_cannot_commit_away_a_threshold_flush_that_is_still_writing()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var held     = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _  = Seam.ReleasedOnExit(held);   // a red assertion below must not strand a thread
        var reached  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var overtook = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parked   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int flushes  = 0;
        engine.OnSnapshotTakenForTest = () =>
        {
            // Only the FIRST flush is held. A second one reaching here IS the defect — it has
            // snapshotted while the first is between its own snapshot and its files — so it is
            // signalled rather than merely counted: that signal is what a build without the fix
            // trips, and it arrives long before the flush it belongs to has finished writing.
            if (Interlocked.Increment(ref flushes) != 1) { overtook.TrySetResult(); return; }
            reached.TrySetResult();
            held.Task.GetAwaiter().GetResult();
        };
        engine.OnFlushGateBlockedForTest = () => parked.TrySetResult();

        var first = SlowFlushBatch(baseNano);                                  // 300 000 points
        engine.Ingest(first);
        var writing = engine.ScheduleThresholdFlushForTest();
        await reached.Task;                     // snapshot taken, generation opened, no file yet

        // MinFlushPoints exactly — the bar the periodic path itself applies, so this drives the
        // real due-check and not a private flush entry point.
        var second = SlowFlushBatch(baseNano + 3_600_000_000_000L, "late", series: 200, pointsPerSeries: 250);
        engine.Ingest(second);

        var periodic = Task.Run(() => engine.FlushPeriodicForTest());

        // Every outcome announces itself, so nothing below is a stopwatch standing in for a fact:
        //
        //   parked   — the flush found the gate held. Once it fires, the periodic flush cannot
        //              reach its snapshot until `held` is set, so the assertions that follow
        //              describe a settled state rather than one sampled early.
        //   overtook — a second flush reached its snapshot: the defect itself, caught as it
        //              happens rather than after the offending flush has finished writing.
        //   periodic — it faulted, which is what a build with the gate gone but the log's
        //              one-open-flush guard still standing produces.
        //
        // So no passing run pays the window, and the window decides nothing: it is the backstop
        // for a build where none of the three arrive, and that build fails loudly on the `parked`
        // assertion instead of going quietly green.
        //
        // What it replaces was a 500 ms window that WAS load-bearing — the test slept and then
        // judged what it found — and no figure justifies one of those, which is the only claim
        // this comment now makes about time. It carried two successive numeric defences instead,
        // each measured on one machine, each unreproducible on the next, and each read by the
        // following reader as a property of this flush. They are gone rather than corrected: a
        // margin is a guess about the slowest machine that will ever run this suite, and a
        // control sized from a guess stops discriminating on the first machine slower than it —
        // and reports that by going green, which is the one failure mode a control may not have.
        //
        // What the three signals are worth is not a measurement either, so it can be stated: each
        // of them is the outcome itself, so the assertions below read a settled state on any
        // machine at any speed. Both are checked against their own controls rather than argued.
        // Gate removed, log's one-open-flush guard left standing: red in ~1 s naming
        // `InvalidOperationException: Metric WAL flush 1 is still open` out of BeginFlush. Both
        // removed: red in ~1 s on `overtook`.
        await Task.WhenAny(parked.Task, overtook.Task, periodic,
                           Task.Delay(TimeSpan.FromSeconds(30)));

        // Order matters: a FAULTED flush is a completed one, so an IsCompleted assertion standing
        // alone fires first and blames the periodic flush for a commit it never got near. Let a
        // flush that threw name itself.
        Assert.False(periodic.IsFaulted,
            "the periodic flush did not park on the gate, it threw: " +
            periodic.Exception?.GetBaseException());
        Assert.False(overtook.Task.IsCompleted,
            "a second flush took its snapshot while the first was still between its own snapshot " +
            "and its files: its commit moves the watermark past a generation that is still being " +
            "written, which is the loss this test measures");
        Assert.True(parked.Task.IsCompleted,
            "the periodic flush never queued on the flush gate — either the gate is gone, or this " +
            "flush was not due and the test is measuring nothing");
        // (No assertion on periodic.IsCompleted here: with 'parked' proven and 'overtook'
        // clear, a completed periodic flush is unreachable except through failures those two
        // assertions name first — it could never be the one that fails, and a check that
        // cannot fail reads as coverage it does not provide.)
        Assert.False(writing.IsCompleted, "setup: the first flush must still be held at its seam");
        string frozen = FreezeDataDir();

        held.SetResult();
        await writing;
        await periodic;
        await engine.DisposeAsync();

        long expected = first.Length + second.Length;
        long durable  = await DurablePointsAsync(frozen, "instrument.0");
        Assert.True(durable == expected, Verdict(durable, expected));
    }

    /// <summary>
    /// What the gate turns that overlap into, and the state the code did not re-read after
    /// waiting for it. The "is there anything to flush" check sits OUTSIDE the gate on purpose —
    /// a tick that queued behind a long write only to find nothing would make every tick during
    /// that write a wait — but it describes the tier BEFORE the wait, and the flush ahead is
    /// draining exactly that tier. So a tick queued behind a threshold flush wakes over an empty
    /// one every time, not occasionally: it opened a generation, bumped the counter, wrote the
    /// new value into the mapped header and abandoned it one drain later, and the branch it
    /// landed in reads as a rare race.
    ///
    /// <para>Held at the seam that stands INSIDE the write lock, before the drain: the tier is
    /// still full there, so the tick passes the pre-check while the flush ahead of it has yet to
    /// take a single point. That seam also counts every generation this engine opens, which is
    /// the assertion.</para>
    /// </summary>
    [Fact]
    public async Task A_tick_that_wakes_over_a_drained_tier_opens_no_generation()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var held    = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = Seam.ReleasedOnExit(held);    // a red assertion below must not strand a thread
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parked  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int opened  = 0;
        engine.OnGenerationOpenedForTest = () =>
        {
            if (Interlocked.Increment(ref opened) != 1) return;   // only the first flush is held
            reached.TrySetResult();
            held.Task.GetAwaiter().GetResult();
        };
        engine.OnFlushGateBlockedForTest = () => parked.TrySetResult();

        // 60 000 points — over MinFlushPoints, so the periodic path is genuinely due, and well
        // under HotFlushThreshold, so the only flushes here are the two this test schedules.
        engine.Ingest(SlowFlushBatch(baseNano, "a", series: 400, pointsPerSeries: 150));
        var writing = engine.ScheduleThresholdFlushForTest();
        await reached.Task;                       // holding the gate and the write lock, pre-drain

        Assert.True(engine.HotPointCount > 0, "setup: the flush ahead must not have drained yet");

        var periodic = Task.Run(() => engine.FlushPeriodicForTest());

        // The tick queueing on the gate is the setup this test needs, and the engine says so —
        // where a 500 ms sleep only ever said "it has not finished YET", which is equally true of
        // a tick that never reached the gate at all. The window here is the backstop for a build
        // with no gate to queue on, and such a build says so through the assertion rather than by
        // passing.
        await Task.WhenAny(parked.Task, periodic, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.False(periodic.IsFaulted,
            "the tick did not park on the gate, it threw: " + periodic.Exception?.GetBaseException());
        Assert.True(parked.Task.IsCompleted, "setup: the tick must be parked on the gate, not past it");
        Assert.True(engine.HotPointCount > 0, "setup: the tick read the pre-check over a full tier");

        held.SetResult();
        await writing;                            // drains everything, writes its files, commits
        await periodic;                           // wakes over the tier that flush emptied
        await engine.DisposeAsync();

        Assert.True(Volatile.Read(ref opened) == 1,
            $"{opened} generations were opened for one flush worth of points: the tick behind the " +
            "gate opened one over an empty tier and abandoned it");
    }

    /// <summary>
    /// The second half, which the write-up does not name and which decides the shape of the fix:
    /// a flush whose files THREW restores its points to the hot tier without re-logging them, so
    /// from then on only its own uncommitted WAL records keep them durable. That is safe exactly
    /// while the next flush's snapshot is guaranteed to happen after the restore — otherwise the
    /// next flush snapshots first, commits a generation above the failed one, and reclaims the
    /// records of points that now exist only in memory.
    /// </summary>
    [Fact]
    public async Task A_flush_that_failed_stays_durable_until_a_later_one_carries_its_points()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        int attempts = 0;
        engine.OnSnapshotTakenForTest = () =>
        {
            if (Interlocked.Increment(ref attempts) == 1)
                throw new IOException("simulated disk failure while writing .mts files");
        };

        var failed = SlowFlushBatch(baseNano, series: 40, pointsPerSeries: 25);   // 1 000 points
        engine.Ingest(failed);
        await engine.ScheduleThresholdFlushForTest();     // snapshot → throw → restore → abandon

        // Wedging is the failure mode a naive "hold the watermark until every earlier flush
        // commits" rule produces: the failed generation stays open forever, no later flush can
        // begin, the log grows without bound and every restart replays everything since. So the
        // next flush must run, must commit, and must leave both batches durable EXACTLY once —
        // a duplicate here means the failed generation's records outlived the flush that
        // finally carried its points.
        var after = SlowFlushBatch(baseNano + 3_600_000_000_000L, "after", series: 40, pointsPerSeries: 25);
        engine.Ingest(after);
        await engine.DisposeAsync();                      // the loop's final flush carries both

        Assert.Equal(2, Volatile.Read(ref attempts));     // the retry really did run
        Assert.True(TrcCount(_dir, "*.mts") > 0, "no .mts file was written after the failed flush");

        long expected = failed.Length + after.Length;
        long durable  = await DurablePointsAsync(_dir, "instrument.0");
        Assert.True(durable == expected, Verdict(durable, expected));
    }

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
        var engine = NewEngine();
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
        var engine = NewEngine();
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
        var engine = NewEngine();
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
        var engine = NewEngine();
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

    /// <summary>
    /// Ingest is ungated for the whole of shutdown by design, so the answer it gives has to be
    /// true: a batch it RETURNS from is a batch the exporter was told had landed. Kestrel is
    /// still serving <c>/otlp/v1/metrics</c> while the engine is disposed — hosted services
    /// stop in reverse registration order — so batches are offered until one is refused, and
    /// every accepted point must then be findable on the next start. It used to hold the
    /// snapshot lock shared across the whole batch while the log was unmapped underneath it:
    /// <c>Append</c> returns silently once disposed, so the rest of the batch went nowhere and
    /// the caller was told otherwise.
    /// </summary>
    [Fact]
    public async Task Every_batch_ingest_returns_from_during_shutdown_is_durable()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        // Give the teardown real work to wait on, so the batches below straddle it rather than
        // arriving before or after: 2 000 series of files, with the tier emptied by the
        // snapshot so the loop's own final pass returns at snapshot.Count == 0.
        var first = SlowFlushBatch(baseNano);
        engine.Ingest(first);
        var writing = engine.ScheduleThresholdFlushForTest();
        await DrainedAsync(engine);
        Assert.False(writing.IsCompleted, "setup: the flush must still be writing its files");

        var late = new MetricIngestItem[12][];
        for (int i = 0; i < late.Length; i++)
            late[i] = SlowFlushBatch(baseNano + (i + 1) * 3_600_000_000_000L, "late" + i,
                                     series: 200, pointsPerSeries: 250);   // 50 000 each

        long accepted = 0;
        string refusal = "(never refused)";
        var disposing = Task.Run(async () => await engine.DisposeAsync());
        var ingesting = Task.Run(() =>
        {
            for (int i = 0; i < late.Length; i++)
            {
                try { engine.Ingest(late[i]); Interlocked.Add(ref accepted, late[i].Length); }
                catch (ObjectDisposedException) { refusal = "ObjectDisposedException"; return; }
            }
        });

        await disposing;
        await ingesting;

        // Refusal is the only acceptable way to stop accepting: a batch that returns normally
        // has been acknowledged. (If every batch landed the run is still valid — it just did
        // not reach the door.)
        Assert.True(refusal is "ObjectDisposedException" or "(never refused)", refusal);

        long expected = first.Length + Interlocked.Read(ref accepted);
        long durable  = await DurablePointsAsync(_dir, "instrument.0");
        Assert.True(durable >= expected,
            $"LOST {expected - durable} of {expected} ACKNOWLEDGED points ({durable} durable)");
    }

    /// <summary>
    /// The second caller of <c>DisposeAsync</c> must not return before the first has finished.
    /// Both are real at host shutdown — the hosted service disposes the engine from StopAsync
    /// and from its own DisposeAsync, and the container disposes the singleton — and the
    /// shutdown timeout makes them concurrent rather than sequential. Returning on the
    /// _disposed exchange handed the loser a completion guarantee it had never waited for: it
    /// came back in about a millisecond with the flush still inside FlushHotTierAsync, and the
    /// process is then free to exit on top of it.
    /// </summary>
    [Fact]
    public async Task A_second_dispose_waits_for_the_first_rather_than_returning()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        engine.Ingest(SlowFlushBatch(baseNano));
        var writing = engine.ScheduleThresholdFlushForTest();
        await DrainedAsync(engine);
        Assert.False(writing.IsCompleted, "setup: the flush must still be writing its files");

        // Observed AT each caller's return, not afterwards — the question is what a caller is
        // entitled to believe the moment it gets control back.
        static async Task<(bool FlushDone, int Running)> DisposeAndLook(
            MetricStorageEngine e, Task flush)
        {
            await e.DisposeAsync();
            return (flush.IsCompleted, e.RunningThresholdFlushes);
        }

        var seen = await Task.WhenAll(Task.Run(() => DisposeAndLook(engine, writing)),
                                      Task.Run(() => DisposeAndLook(engine, writing)));

        foreach (var (flushDone, running) in seen)
        {
            Assert.True(flushDone,
                "DisposeAsync returned while the threshold flush was still writing its files");
            Assert.True(running == 0,
                $"DisposeAsync returned with {running} threshold flush(es) inside FlushHotTierAsync");
        }
    }

    /// <summary>
    /// The scheduling seam, held open for the whole of shutdown and past it. One drain runs,
    /// so everything scheduled from its snapshot onwards rests on the _disposed gate alone:
    /// such a flush must return before it reaches a lock, the log or the disk, and must never
    /// fault. Ingest can schedule one at any of these instants in production, because it stays
    /// open until the last step of the teardown.
    /// </summary>
    [Fact]
    public async Task A_flush_scheduled_throughout_shutdown_never_touches_anything()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        engine.Ingest(SlowFlushBatch(baseNano));
        var writing = engine.ScheduleThresholdFlushForTest();
        await DrainedAsync(engine);

        using var stop = new CancellationTokenSource();
        var scheduled = new List<Task>();
        var hammer = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                lock (scheduled) { scheduled.Add(engine.ScheduleThresholdFlushForTest()); }
                await Task.Delay(1);
            }
        });

        await engine.DisposeAsync();
        int runningAtReturn = engine.RunningThresholdFlushes;

        await Task.Delay(50);          // and past the return, the worst moment of all
        stop.Cancel();
        await hammer;

        Task[] all;
        lock (scheduled) { all = [.. scheduled]; }
        var faulted = new List<string>();
        foreach (var t in all)
        {
            try { await t; }
            catch (Exception ex) { faulted.Add(ex.GetType().Name); }
        }

        Assert.Equal(0, runningAtReturn);
        Assert.True(faulted.Count == 0,
            $"{faulted.Count} of {all.Length} flushes scheduled during shutdown faulted: " +
            string.Join(", ", faulted.Distinct()));
    }

    [Fact]
    public async Task Existing_files_survive_the_upgrade_and_stay_queryable()
    {
        // The deploy case: a data directory already holding .mts files from the old build,
        // opened for the first time by an engine that now keeps a WAL beside them.
        long oldNano = DateTimeOffset.UtcNow.AddMinutes(-30).ToUnixTimeMilliseconds() * 1_000_000L;

        var first = NewEngine();
        first.Ingest([Scalar("legacy.metric", oldNano, 7, Labels(("host", "node0")))]);
        await first.DisposeAsync();                                   // flushes, then resets the log

        int filesBefore = TrcCount(_dir, "*.mts");
        Assert.True(filesBefore > 0);

        var second = NewEngine();

        // Cold-segment discovery is deliberately kept off the startup path so ingest works
        // from second zero — a query issued immediately after construction can legitimately
        // outrun the background scan, WAL or no WAL.
        var series = await QueryWhenColdLoadedAsync(second, "legacy.metric");

        Assert.Single(series);
        Assert.Equal(7, series[0].Points[0].Value);
        Assert.Equal(filesBefore, TrcCount(_dir, "*.mts"));           // nothing rewritten on open

        await second.DisposeAsync();
    }

    // ── The abandon contract: "the flush failed" must mean no file landed ──────

    /// <summary>
    /// What the caller commits the log generation against. The writer used to rename each file as
    /// it finished, so file 1 was visible at its final path for the whole write of files 2..N —
    /// seconds under a large flush — while the generation carrying its points was still
    /// replayable. A crash in there duplicated every point in every published file: served from
    /// the file and replayed from the log, with every rollup over the range reading double.
    ///
    /// <para>Measured where the window actually is: at the moment the FIRST file becomes visible,
    /// nothing may still be waiting to be written. The seam fires with each file's final path
    /// once it is in place, so counting temp files from inside it is counting what a crash there
    /// would strand.</para>
    /// </summary>
    [Fact]
    public void No_file_is_published_until_every_file_of_the_flush_is_written()
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var corpus = new List<(SeriesKey Key, HotSeries Series)>();
        foreach (string name in new[] { "metric.one", "metric.two", "metric.three" })
        for (int s = 0; s < 4; s++)
        {
            var pts = new List<MetricDataPoint>();
            for (int p = 0; p < 20; p++)
                pts.Add(new MetricDataPoint { TimestampUnixNano = baseNano + p * 1_000_000L, Value = p });
            corpus.Add((new SeriesKey(name, MetricKind.Gauge, "ms", Labels(("series", name + s))),
                        new HotSeries(pts)));
        }

        int  seen        = 0;
        int  filesAtFirst  = 0;
        int  pointsAtFirst = 0;
        var infos = MetricWriter.Write(
            _dir, corpus, MetricGranularity.Raw,
            afterFileWritten: _ =>
            {
                if (Interlocked.Increment(ref seen) != 1) return;

                // Read here rather than after the call: two of these are still under their temp
                // names and will not carry them for much longer.
                var onDisk = Directory.GetFiles(_dir, "*.mts*");
                filesAtFirst = onDisk.Length;
                foreach (string path in onDisk)
                    foreach (var s in MetricReader.ReadAllSync(path))
                        pointsAtFirst += s.Points.Count;
            });

        Assert.Equal(3, infos.Count);
        Assert.Equal(3, seen);

        // All three exist the moment the first one becomes visible — one at its final path, the
        // other two still under their temp names with nothing left to do but be renamed. Pre-fix
        // there was one file here and the other two had not been started.
        Assert.Equal(3, filesAtFirst);

        // Existing is not the claim: COMPLETE is. Every point of the flush reads back out of the
        // three files at that instant, so what the log is still holding replayable is points that
        // are already, wholly, written.
        Assert.Equal(240, pointsAtFirst);   // 3 names × 4 series × 20 points

        foreach (var info in infos) Assert.True(File.Exists(info.FilePath));
        Assert.Empty(Directory.GetFiles(_dir, "*.mts.tmp"));
    }

    /// <summary>
    /// The writer puts one <c>.mts</c> per metric name, one after another — so a disk that fills
    /// BETWEEN two of them left every earlier file complete and discoverable by the next start's
    /// <c>LoadColdSegments</c>. The flush reads a throw as "no file carries these points",
    /// restores the whole snapshot to the hot tier and leaves the generation replayable, so each
    /// landed file's points were then in a file AND in memory AND in the log. Not a window — a
    /// certainty, and total for every metric written before the failure.
    ///
    /// <para>Driven through the writer itself rather than by planting a file beside it: the
    /// names carry a random nonce, so a partial write is not otherwise reproducible, and a
    /// fabricated file would pass whatever the writer does. Two metric names, the failure after
    /// the first.</para>
    /// </summary>
    [Fact]
    public void A_write_that_fails_partway_leaves_none_of_its_files_behind()
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var corpus = new List<(SeriesKey Key, HotSeries Series)>();
        foreach (string name in new[] { "metric.landed", "metric.pending" })
        for (int s = 0; s < 4; s++)
        {
            var pts = new List<MetricDataPoint>();
            for (int p = 0; p < 20; p++)
                pts.Add(new MetricDataPoint { TimestampUnixNano = baseNano + p * 1_000_000L, Value = p });
            corpus.Add((new SeriesKey(name, MetricKind.Gauge, "ms", Labels(("series", name + s))),
                        new HotSeries(pts)));
        }

        int written = 0;
        var ex = Assert.Throws<IOException>(() => MetricWriter.Write(
            _dir, corpus, MetricGranularity.Raw,
            afterFileWritten: _ =>
            {
                if (Interlocked.Increment(ref written) == 1) return;
                throw new IOException("disk full while writing the second metric's file");
            }));

        Assert.Equal("disk full while writing the second metric's file", ex.Message);
        Assert.True(written >= 2, $"setup: the writer produced {written} file(s), so it never failed partway");

        // The caller is about to put every one of these points back into the hot tier. That is
        // only not a duplicate if the directory holds nothing.
        Assert.Empty(Directory.GetFiles(_dir, "*.mts"));
    }

    /// <summary>
    /// The half of that contract a cleanup pass structurally cannot keep: the disk dies INSIDE
    /// a file rather than between two of them. The writer wrote straight to the final path and
    /// a file only joined the returned list once it was finished, so the one file that failed
    /// was the one name the catch did not have — it deleted files 1..k-1 and left the k-th, a
    /// footerless <c>.mts</c> sitting where the catalog scan reads. Two outcomes, both bad: the
    /// next start deletes it with "Unreadable metric segment … (likely format v1)", or — when
    /// the failure lands after the last byte, on the <c>FileInfo.Length</c> that follows — a
    /// COMPLETE file stays behind while the caller puts its points back into the hot tier and
    /// leaves the generation replayable, which is the duplicate this PR exists to remove.
    ///
    /// <para>Driven through the writer with the file still open, since that is the only state
    /// the defect lives in: a seam that fires after a successful write leaves the file in the
    /// list, and therefore inside the cleanup that already worked. Building at <c>.mts.tmp</c>
    /// and renaming after the close is what makes the state unreachable — there is nothing to
    /// clean up after the fact, which is the point.</para>
    /// </summary>
    [Fact]
    public void A_write_that_dies_inside_a_file_leaves_nothing_where_the_scan_reads()
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var corpus = new List<(SeriesKey Key, HotSeries Series)>();
        foreach (string name in new[] { "metric.landed", "metric.torn" })
        for (int s = 0; s < 4; s++)
        {
            var pts = new List<MetricDataPoint>();
            for (int p = 0; p < 20; p++)
                pts.Add(new MetricDataPoint { TimestampUnixNano = baseNano + p * 1_000_000L, Value = p });
            corpus.Add((new SeriesKey(name, MetricKind.Gauge, "ms", Labels(("series", name + s))),
                        new HotSeries(pts)));
        }

        int reached = 0;
        var ex = Assert.Throws<IOException>(() => MetricWriter.Write(
            _dir, corpus, MetricGranularity.Raw,
            duringFileWrite: _ =>
            {
                // The first file is written whole; the second dies with its handle open,
                // its header and payload down and its footer still to come.
                if (Interlocked.Increment(ref reached) == 1) return;
                throw new IOException("disk full midway through the second metric's file");
            }));

        Assert.Equal("disk full midway through the second metric's file", ex.Message);
        Assert.True(reached >= 2, $"setup: the writer opened {reached} file(s), so it never failed inside one");

        // Nothing at a path any reader scans — neither the file that landed and was retracted,
        // nor the torn one, which is the file the old cleanup could not name.
        Assert.Empty(Directory.GetFiles(_dir, "*.mts"));
        // And the build it died in does not linger under its temp name either.
        Assert.Empty(Directory.GetFiles(_dir, "*.mts.tmp"));
    }

    /// <summary>
    /// The other end of the temp name: a process killed between the write and the rename leaves
    /// a <c>.mts.tmp</c>, and <c>"*.mts"</c> does not match it — so the cold scan cannot see it,
    /// cannot delete it as unreadable, and no later pass ever visits it. It would accumulate,
    /// one per interrupted flush or rollup, until somebody wondered about the disk usage.
    ///
    /// <para>Asserted before the engine has done anything else, because WHEN the sweep runs is
    /// the whole of its correctness. A wildcard delete over a directory that has live writers
    /// unlinks the file a concurrent flush is filling — on Linux under the open handle, so the
    /// writer only finds out when it goes to rename an inode that has no name — and the flush
    /// then reports a failed write for a disk that was fine. Sweeping from the constructor makes
    /// that unreachable rather than unlikely: the engine that owns every writer of these files is
    /// the one being constructed, so no flush, rollup or ingest exists yet to collide with.</para>
    /// </summary>
    [Fact]
    public async Task An_interrupted_build_is_swept_before_anything_can_be_writing()
    {
        string tmp = Path.Combine(_dir, "metrics-instrument_0-1-2-raw-deadbeef.mts.tmp");
        File.WriteAllBytes(tmp, [0x54, 0x4D, 0x44, 0x52, 0x03, 0x00]);   // a header and nothing else

        var engine = NewEngine();

        Assert.False(File.Exists(tmp),
            "an interrupted build survived the constructor, so the sweep runs somewhere a live " +
            "flush can be holding one of these files open");

        await engine.ColdLoadCompleted.WaitAsync(TimeSpan.FromSeconds(30));
        await engine.DisposeAsync();
    }

    /// <summary>
    /// The same failure one level up, measured the way it is actually paid: a partial write, the
    /// retry that succeeds, and then a count of everything durable. The landed metric's points
    /// must appear once — pre-fix they appeared in the abandoned attempt's surviving file and
    /// again in the retry's, and every counter and sum in a rollup over the window read double.
    /// </summary>
    [Fact]
    public async Task A_partial_write_does_not_duplicate_the_metrics_whose_file_landed()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        // Two metric names, so the writer writes two files with a seam between them.
        var landed  = SlowFlushBatch(baseNano, "a", series: 10, pointsPerSeries: 20);
        var pending = new List<MetricIngestItem>(landed.Length);
        for (int s = 0; s < 10; s++)
        for (int p = 0; p < 20; p++)
            pending.Add(Scalar("instrument.1", baseNano + p * 1_000_000L, p, Labels(("series", "b" + s))));

        int written = 0;
        engine.OnFileWrittenForTest = _ =>
        {
            if (Interlocked.Increment(ref written) == 1) return;
            throw new IOException("disk full while writing the second metric's file");
        };

        engine.Ingest(landed);
        engine.Ingest(pending.ToArray());
        await engine.ScheduleThresholdFlushForTest();     // fails after the first file
        Assert.True(written >= 2, "setup: the write never reached a second file");

        engine.OnFileWrittenForTest = null;
        await engine.ScheduleThresholdFlushForTest();     // the retry that carries them all
        await engine.DisposeAsync();

        long durable = await DurablePointsAsync(_dir, "instrument.0");
        Assert.True(durable == landed.Length, Verdict(durable, landed.Length));
    }

    /// <summary>
    /// The other half of the same contract: a failure AFTER the write returned. Everything the
    /// snapshot held is complete on disk at that point, so restoring it and leaving the
    /// generation replayable duplicates all of it — and the catch wrapped the publish and the
    /// log line too, which are not the write and cannot un-write it. Killed before any retry, so
    /// what is measured is exactly what the next start would find.
    /// </summary>
    [Fact]
    public async Task A_failure_after_the_files_landed_commits_instead_of_replaying_them()
    {
        var logger = new ThrowOnFlushedDebugLogger();
        var engine = NewEngine(logger: logger);
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var one = SlowFlushBatch(baseNano, "a", series: 50, pointsPerSeries: 20);   // 1 000 points
        engine.Ingest(one);
        await engine.ScheduleThresholdFlushForTest();

        Assert.True(logger.Threw, "setup: the post-write step never threw");
        Assert.True(TrcCount(_dir, "*.mts") > 0, "setup: the write must have landed before the throw");

        string frozen = FreezeDataDir();                 // kill here, before any retry

        logger.Armed = false;
        await engine.DisposeAsync();

        long durable = await DurablePointsAsync(frozen, "instrument.0");
        Assert.True(durable == one.Length, Verdict(durable, one.Length));
    }

    /// <summary>Throws from the flush's own post-write debug line — a step that cannot un-write a file.</summary>
    private sealed class ThrowOnFlushedDebugLogger : Microsoft.Extensions.Logging.ILogger<MetricStorageEngine>
    {
        public bool Armed = true;
        public bool Threw;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!Armed) return;
            if (logLevel == Microsoft.Extensions.Logging.LogLevel.Debug &&
                formatter(state, exception).StartsWith("Flushed ", StringComparison.Ordinal))
            {
                Threw = true;
                throw new InvalidOperationException("post-write step failed");
            }
        }
    }

    // ── The generation must not outlive the flush that opened it ──────────────

    /// <summary>
    /// <c>BeginFlush</c> refusing a second open flush is a tripwire, and a tripwire that cannot
    /// be reset is worse than the loss it guards: nothing catches its
    /// <see cref="InvalidOperationException"/> on the periodic path, so the first flush to hit
    /// it takes the loop with it and no <c>.mts</c> file is written again for the life of the
    /// process, while the log grows by doubling until <c>Grow</c> throws into ingest.
    ///
    /// <para>The window it was reachable through: the drain between <c>BeginFlush</c> and the
    /// write sat outside every handler — it copies every series' point list, so an
    /// <see cref="OutOfMemoryException"/> there is the realistic trigger — and both paths that
    /// hand the generation back are below it. Provoked here from the drain itself, through the
    /// only seam that stands inside the write lock.</para>
    /// </summary>
    [Fact]
    public async Task A_flush_that_throws_before_its_write_leaves_the_log_able_to_flush_again()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var one = SlowFlushBatch(baseNano, "a", series: 40, pointsPerSeries: 25);   // 1 000 points
        engine.Ingest(one);

        // The generation is opened, and the flush then dies before it reaches the drain — where
        // the only two calls that hand a generation back both live.
        engine.OnGenerationOpenedForTest = static () => throw new OutOfMemoryException("drain");
        var first = await Record.ExceptionAsync(() => engine.ScheduleThresholdFlushForTest());
        Assert.IsType<OutOfMemoryException>(first);

        // The tripwire must be reset. Pre-fix the next BeginFlush threw
        // InvalidOperationException — for every flush after it, forever.
        engine.OnGenerationOpenedForTest = null;
        var second = await Record.ExceptionAsync(() => engine.ScheduleThresholdFlushForTest());
        Assert.True(second is null, $"the log refused every flush after the first one threw: {second?.GetType().Name}");

        await engine.DisposeAsync();
        long durable = await DurablePointsAsync(_dir, "instrument.0");
        Assert.True(durable == one.Length, Verdict(durable, one.Length));
    }

    /// <summary>
    /// The same tripwire seen from production, where nobody holds the task. The periodic path
    /// was wrapped — "a tick that throws must cost one tick" — but the THRESHOLD path is
    /// scheduled from <c>Ingest</c> with <c>_ = ScheduleThresholdFlush()</c>, because an ingest
    /// call cannot wait on a flush. So a guard against silent loss reported itself in silence:
    /// no log line, no rethrow, nothing but a <c>TaskScheduler.UnobservedTaskException</c> at
    /// some later finalisation. And the state it guards was not transient — <c>CommitFlush</c>
    /// tested the closed or unmapped log BEFORE clearing the open flush, so a flush that
    /// reached it left the flag standing and every later flush threw into the same void for
    /// the life of the process. The clear comes first now; the reporting is what this pins.
    ///
    /// <para>Driven through the real crossing rather than the test hook, since the discarded
    /// task is <c>Ingest</c>'s: 500 000 points, which is <c>HotFlushThreshold</c> exactly.</para>
    /// </summary>
    [Fact]
    public async Task A_threshold_flush_that_throws_is_not_swallowed_by_the_task_nobody_holds()
    {
        var logger = new RecordingLogger();
        var engine = NewEngine(logger: logger);
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        // Armed before the crossing: the one seam standing where the drain's own allocation
        // would fail, which is the realistic trigger and the shape BeginFlush's throw has too.
        engine.OnGenerationOpenedForTest = static () => throw new OutOfMemoryException("drain");

        const int total = 500_000;                       // == HotFlushThreshold
        var batch = new MetricIngestItem[10_000];
        for (int done = 0; done < total; done += batch.Length)
        {
            for (int i = 0; i < batch.Length; i++)
                batch[i] = Scalar("instrument.0", baseNano + (done + i) * 1_000L, 1.0,
                                  Labels(("series", (i & 3).ToString())));
            engine.Ingest(batch);
        }

        // The flush runs off the ingest path, so give it its moment to report.
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (!logger.Saw(Microsoft.Extensions.Logging.LogLevel.Error, "Threshold metric flush failed", out _)
               && sw.Elapsed < TimeSpan.FromSeconds(20))
            await Task.Delay(10);

        engine.OnGenerationOpenedForTest = null;
        bool said = logger.Saw(Microsoft.Extensions.Logging.LogLevel.Error, "Threshold metric flush failed", out var reported);
        await engine.DisposeAsync();

        Assert.True(said,
            "the flush threw and nothing said so: the hot tier stops draining and the log grows by doubling, " +
            "with no line in the log to explain either");
        Assert.IsType<OutOfMemoryException>(reported);   // the flush's own failure, not something else
    }

    /// <summary>Records every line logged, so a test can assert on the report a path makes.</summary>
    // ── Issue #56: the poisoned log is repaired at OPEN, and the file gives space back ──

    /// <summary>
    /// The incident end to end, at the moment it matters: a torn head reconciled when the log
    /// OPENS, not when the first flush commits. The distinction is the whole bug — on a trickle
    /// deployment below the flush thresholds a commit can be arbitrarily far away, and until
    /// one runs, every point appended after a poisoned open lands PAST the unreachable region,
    /// where no replay will ever find it. Acknowledged, logged, unreplayable from birth.
    /// </summary>
    [Fact]
    public void Poisoned_head_is_reconciled_and_shrunk_at_open()
    {
        var wal = OpenWal(4 * 1024);
        for (int i = 0; i < 300; i++)                       // ~14 KB of entries: grows 4 → 16 KiB
            Append(wal, Scalar("cpu", 1_000 + i, i));
        wal.Dispose();
        Assert.Equal(32 + 16 * 1024, new FileInfo(WalPath).Length);

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(32, SeekOrigin.Begin);                  // first entry's Generation field
            fs.Write(BitConverter.GetBytes(155_000_000_000UL));
        }

        var logger   = new RecordingLogger();
        var reopened = OpenWal(4 * 1024, logger);

        Assert.True(logger.Saw(Microsoft.Extensions.Logging.LogLevel.Error,
                "Metric WAL header claims", out _),
            "corruption was repaired without being reported");
        Assert.Empty(reopened.ReadAll(out _));
        Assert.Equal(0, reopened.WrittenBytes);
        // The grown corpse gave its space back instead of surviving its own cause.
        Assert.Equal(32 + 4 * 1024, new FileInfo(WalPath).Length);

        // And the repair is real, not cosmetic: a point appended now is REACHABLE — before the
        // fix it would have landed beyond gigabytes of claimed garbage no scan could cross.
        Append(reopened, Scalar("cpu", 9_000, 42.0));
        reopened.Dispose();

        var third    = OpenWal(4 * 1024);
        var replayed = third.ReadAll(out _);
        Assert.Single(replayed);
        Assert.Equal(42.0, replayed[0].Point.Value);
    }

    /// <summary>
    /// The incident's second signature, isolated: a header claiming data far past the end
    /// marker (the stand: 8.45 GiB claimed over 128 real bytes, and the 8.4 GiB in between
    /// never mentioned by anyone). Warning, not Error — a lost data page under a persisted
    /// header, or a crash between Compact's marker and its header store, present the same way
    /// legitimately. What matters is that the claim is trimmed and SAID.
    /// </summary>
    [Fact]
    public void Data_claimed_beyond_the_end_marker_is_discarded_with_a_warning()
    {
        var wal = OpenWal();
        Append(wal, Scalar("cpu", 1_000, 1.0));
        Append(wal, Scalar("cpu", 2_000, 2.0));
        wal.Dispose();

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(8, SeekOrigin.Begin);                   // header WriteOffset
            fs.Write(BitConverter.GetBytes((long)(32 + 4096)));
        }

        var logger   = new RecordingLogger();
        var reopened = OpenWal(logger: logger);

        Assert.True(logger.Saw(Microsoft.Extensions.Logging.LogLevel.Warning,
            "Metric WAL: discarding", out _));
        var replayed = reopened.ReadAll(out _);
        Assert.Equal(2, replayed.Count);                    // the real points are untouched
        Assert.Equal(96, reopened.WrittenBytes);            // the phantom 4 000 bytes are gone
    }

    /// <summary>
    /// A log that grew under load returns the space the moment a commit empties it — not at
    /// the next restart. The incident machine ran for weeks growing 1.3 GB a day; a shrink
    /// that fires only at open would have watched all of it.
    /// </summary>
    [Fact]
    public void A_grown_log_shrinks_back_when_a_commit_empties_it()
    {
        var wal = OpenWal(4 * 1024);
        for (int i = 0; i < 120; i++)                       // ~5.8 KB: grows 4 → 8 KiB
            Append(wal, Scalar("cpu", 1_000 + i, i));
        Assert.Equal(32 + 8 * 1024, new FileInfo(WalPath).Length);

        ulong gen = wal.BeginFlush();
        Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(gen));

        Assert.Equal(0, wal.WrittenBytes);
        Assert.Equal(32 + 4 * 1024, new FileInfo(WalPath).Length);

        // Still a working log at the smaller size — the shrink remapped, it did not disable.
        Append(wal, Scalar("cpu", 9_000, 7.0));
        Assert.Equal(48, wal.WrittenBytes);
    }

    /// <summary>
    /// Reopening with survivors must not hand out pool indices the survivors already use. The
    /// in-memory registry restarts empty, so before the fix the first new series took index 0
    /// again, and LoadPool's later-records-win then attributed the SURVIVING entries to the
    /// new series on the next replay — wrong name, wrong labels, and nothing anywhere said so.
    /// </summary>
    [Fact]
    public void Pool_indices_continue_past_survivors_after_a_reopen()
    {
        var wal = OpenWal();
        Append(wal, Scalar("alpha", 1_000, 1.0));
        wal.Dispose();

        var second = OpenWal();
        Append(second, Scalar("beta", 2_000, 2.0));
        second.Dispose();

        var third    = OpenWal();
        var replayed = third.ReadAll(out int unresolved);

        Assert.Equal(0, unresolved);
        Assert.Equal(2, replayed.Count);
        Assert.Contains(replayed, r => r.Name == "alpha" && r.Point.Value == 1.0);
        Assert.Contains(replayed, r => r.Name == "beta"  && r.Point.Value == 2.0);
    }

    /// <summary>
    /// A pool that outlived its log is dead weight (the stand carried 29.5 MB of it, because
    /// truncation only ever ran on a flush that emptied the log — which the poisoning made
    /// unreachable). An empty log references nothing, so opening one resets the pool.
    /// </summary>
    [Fact]
    public void A_stale_pool_is_truncated_when_the_log_opens_empty()
    {
        var wal = OpenWal();
        Append(wal, Scalar("cpu", 1_000, 1.0));
        wal.Dispose();
        Assert.True(new FileInfo(PoolPath).Length > 0);

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(8, SeekOrigin.Begin);
            fs.Write(BitConverter.GetBytes((long)32));      // header: no data at all
        }

        OpenWal();
        Assert.Equal(0, new FileInfo(PoolPath).Length);
    }

    // ── Issue #56, the comment: one far-future point makes its cold file immortal ──

    /// <summary>
    /// A point stamped a century ahead is refused at ingest — before the WAL, before the hot
    /// tier — because once flushed it becomes its file's MaxNano, and that file is then never
    /// selected by rollup, never expired by retention, and scanned by every query until 2116.
    /// A point HALF A DAY ahead must keep flowing: that is real clock trouble (a client
    /// stamping local time as UTC), and this suite's own fixtures ingest up to +12 h.
    /// </summary>
    [Fact]
    public async Task A_far_future_point_is_refused_at_ingest()
    {
        long nowNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        long century = nowNano + 100L * 365 * 24 * 3_600_000_000_000L;
        long halfDay = nowNano + 12L * 3_600_000_000_000L;

        var logger = new RecordingLogger();
        var engine = NewEngine(logger: logger);

        engine.Ingest(new[]
        {
            Scalar("cpu", century, 666.0),
            Scalar("cpu", halfDay, 2.0),
            Scalar("cpu", nowNano, 1.0),
        });

        Assert.True(logger.Saw(Microsoft.Extensions.Logging.LogLevel.Warning,
                "Dropped 1 metric point(s) at ingest", out _),
            "the drop happened silently");

        await engine.DisposeAsync();                        // final flush writes the survivors

        var files = Directory.GetFiles(_dir, "metrics-cpu-*.mts");
        Assert.NotEmpty(files);
        // File names carry {min}-{max}: the century must appear in nothing, and the
        // skewed-but-sane half-day point must be exactly the max of the file it landed in.
        Assert.DoesNotContain(files, f => f.Contains(century.ToString()));
        Assert.Contains(files, f => f.Contains("-" + halfDay + "-"));
    }

    /// <summary>
    /// The same guard at the hot tier's other entrance. The incident's garbage point came from
    /// the LOG: every restart replayed it into the hot tier, and every flush from there minted
    /// another immortal file — ten of them on the stand. Dropping (not clamping) is what keeps
    /// a crash-restart loop deterministic: a clamp would re-stamp the same WAL entry to a
    /// different "now" each start, and exact-timestamp dedupe would see them all as distinct.
    /// </summary>
    [Fact]
    public async Task A_far_future_point_in_the_wal_is_dropped_at_recovery()
    {
        long nowNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        long century = nowNano + 100L * 365 * 24 * 3_600_000_000_000L;

        var wal = OpenWal();
        Append(wal, Scalar("cpu", century, 666.0));
        Append(wal, Scalar("cpu", nowNano, 1.0));
        wal.Dispose();

        var logger = new RecordingLogger();
        var engine = NewEngine(logger: logger);

        Assert.True(logger.Saw(Microsoft.Extensions.Logging.LogLevel.Warning,
            "Dropped 1 metric point(s) at WAL recovery", out _));
        Assert.True(logger.Saw(Microsoft.Extensions.Logging.LogLevel.Information,
            "Recovered 1 metric point", out _));

        await engine.DisposeAsync();

        var files = Directory.GetFiles(_dir, "metrics-cpu-*.mts");
        Assert.NotEmpty(files);
        Assert.DoesNotContain(files, f => f.Contains(century.ToString()));
    }

    /// <summary>
    /// Mutation-proven: deleting <c>_capacity = targetCapacity</c> in <c>ShrinkLocked</c> left
    /// the suite green, and in production that is raw-pointer writes past the mapped view —
    /// the file and the mapping shrink to the floor, but every later <see cref="MetricWriteAheadLog.Append"/>
    /// would still bound itself against the pre-shrink capacity. Grow back past a commit-time
    /// shrink and check both the field and the file, so a mismatch between the two cannot hide
    /// behind either alone.
    /// </summary>
    [Fact]
    public void Grow_after_a_commit_shrink_climbs_the_ladder_again()
    {
        var wal = OpenWal(4 * 1024);
        for (int i = 0; i < 120; i++)                       // ~5.8 KB: grows 4 → 8 KiB
            Append(wal, Scalar("cpu", 1_000 + i, i));
        Assert.Equal(32 + 8 * 1024, new FileInfo(WalPath).Length);

        ulong gen = wal.BeginFlush();
        Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(gen));
        Assert.Equal(0, wal.WrittenBytes);
        Assert.Equal(32 + 4 * 1024, new FileInfo(WalPath).Length);      // shrunk back to the floor

        for (int i = 0; i < 120; i++)                       // must re-Grow to 8 KiB, not overrun 4
            Append(wal, Scalar("cpu", 2_000 + i, i));
        Assert.Equal(120 * 48, wal.WrittenBytes);
        Assert.Equal(32 + 8 * 1024, new FileInfo(WalPath).Length);
        wal.Dispose();

        var reopened = OpenWal(4 * 1024);
        Assert.Equal(120, reopened.ReadAll(out _).Count);
        reopened.Dispose();
    }

    /// <summary>
    /// <see cref="MetricStorageEngine.ReportFutureDrops"/> exists precisely so that a client
    /// with a broken clock — which sends every batch broken — costs one warning a minute, not
    /// one per <see cref="MetricStorageEngine.Ingest"/> call. Two calls close together must
    /// therefore log exactly once between them.
    /// </summary>
    [Fact]
    public void Future_drop_warnings_are_rate_limited()
    {
        long nowNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        long century = nowNano + 100L * 365 * 24 * 3_600_000_000_000L;

        var logger = new RecordingLogger();
        var engine = NewEngine(logger: logger);

        engine.Ingest(new[] { Scalar("cpu", century, 666.0), Scalar("cpu", nowNano,     1.0) });
        engine.Ingest(new[] { Scalar("cpu", century, 667.0), Scalar("cpu", nowNano + 1, 2.0) });

        Assert.Equal(1, logger.Count(Microsoft.Extensions.Logging.LogLevel.Warning, "Dropped "));
    }

    /// <summary>
    /// The pool and the log persist independently — the pool goes out through a FileStream to
    /// the OS cache, one flushed record per series; the entries are un-msynced stores into a
    /// mapped page — so a torn pool tail with intact log entries is a real crash shape, not a
    /// contrived one. Before the fix, the next new series re-took the torn record's index and
    /// the survivor whose record was lost then replayed under the new series' name and value
    /// instead of its own: silent misattribution, not loss. Seeding the next index from the
    /// log's own entries as well as the pool is what stops the index being handed out twice.
    /// </summary>
    [Fact]
    public void A_torn_pool_tail_does_not_misattribute_survivors()
    {
        var wal = OpenWal();
        Append(wal, Scalar("alpha", 1_000, 1.0));   // pool index 0
        Append(wal, Scalar("beta",  2_000, 2.0));   // pool index 1 — its record is torn off next
        wal.Dispose();

        byte[] poolBytes        = File.ReadAllBytes(PoolPath);
        uint   firstBodyLen     = BitConverter.ToUInt32(poolBytes, 4);
        int    firstRecordTotal = 8 + (int)firstBodyLen;
        using (var fs = new FileStream(PoolPath, FileMode.Open, FileAccess.ReadWrite))
            fs.SetLength(firstRecordTotal + 4);      // alpha intact, 4 bytes of beta's head only

        var reopened = OpenWal();
        Append(reopened, Scalar("charlie", 3_000, 3.0));   // must NOT take beta's old index
        reopened.Dispose();

        var third    = OpenWal();
        var replayed = third.ReadAll(out int unresolved);
        third.Dispose();

        // beta's point is gone honestly — its pool record did not survive the tear — not
        // silently folded into whichever series next took its slot.
        Assert.Equal(1, unresolved);
        Assert.Equal(2, replayed.Count);
        Assert.All(replayed, r => Assert.False(string.IsNullOrEmpty(r.Name)));
        Assert.All(replayed, r => Assert.True(r.Name is "alpha" or "charlie",
            $"a recovered point carried an unexpected identity: {r.Name}"));
        Assert.Contains(replayed, r => r.Name == "alpha"   && r.Point.Value == 1.0);
        Assert.Contains(replayed, r => r.Name == "charlie" && r.Point.Value == 3.0);
    }

    // ── PR #59 review: the second pass of blockers ──

    /// <summary>
    /// A grown file whose Magic or Version rotted lands in the fresh-header branch, and that
    /// branch used to keep the grown capacity forever — a multi-GiB corpse with a zeroed write
    /// offset that no empty-commit would ever come along to shrink on a trickle deployment.
    /// The same "grown file stays grown" disease this PR cures for a lying WriteOffset, through
    /// the other door.
    /// </summary>
    [Fact]
    public void A_grown_file_with_a_rotted_magic_shrinks_at_reopen()
    {
        var wal = OpenWal(4 * 1024);
        for (int i = 0; i < 300; i++)                       // grows 4 → 16 KiB
            Append(wal, Scalar("cpu", 1_000 + i, i));
        wal.Dispose();
        Assert.Equal(32 + 16 * 1024, new FileInfo(WalPath).Length);

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(0, SeekOrigin.Begin);                   // the Magic field
            fs.Write(BitConverter.GetBytes(0xDEADBEEFu));
        }

        var reopened = OpenWal(4 * 1024);

        Assert.Equal(0, reopened.WrittenBytes);             // fresh header, as before
        Assert.Equal(32 + 4 * 1024, new FileInfo(WalPath).Length);   // and no longer a corpse
    }

    /// <summary>
    /// A torn entry can land a PLAUSIBLE generation and garbage in the very next field. Before
    /// this guard, a series index near uint.MaxValue flowed into the survivor seed, the clamp
    /// parked the counter at the ceiling — and the counter is an unchecked increment, so the
    /// second registration after the restart wrapped to zero and collided with index 0's
    /// legitimate owner: the misattribution bug again, through the other field. No registration
    /// has ever issued an index within orders of magnitude of the cap, so stopping the walk
    /// there is the same judgement the generation margin makes.
    /// </summary>
    [Fact]
    public void A_garbage_series_index_ends_the_walk_instead_of_poisoning_the_seed()
    {
        var wal = OpenWal();
        Append(wal, Scalar("cpu", 1_000, 1.0));
        wal.Dispose();

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(32 + 8, SeekOrigin.Begin);              // entry 0's SeriesIndex field
            fs.Write(BitConverter.GetBytes(uint.MaxValue - 3));
        }

        var logger   = new RecordingLogger();
        var reopened = OpenWal(logger: logger);

        Assert.True(logger.Saw(Microsoft.Extensions.Logging.LogLevel.Error,
            "Metric WAL header claims", out _));
        Assert.Equal(0, reopened.WrittenBytes);             // the entry is garbage, not data

        // The counter was NOT parked at the ceiling: a series registered now gets a small
        // index, and a second one the next — no wrap, no collision.
        Append(reopened, Scalar("alpha", 2_000, 1.0));
        Append(reopened, Scalar("beta",  3_000, 2.0));
        reopened.Dispose();

        var third    = OpenWal();
        var replayed = third.ReadAll(out int unresolved);
        Assert.Equal(0, unresolved);
        Assert.Contains(replayed, r => r.Name == "alpha");
        Assert.Contains(replayed, r => r.Name == "beta");
    }

    /// <summary>
    /// Corruption that is about to be truncated leaves a capped forensic copy behind.
    /// Reconciliation is judgement, not proof — the header's own counter is the margin's
    /// reference — so the head of what it discards is kept where a person can look at it,
    /// without copying gigabytes onto the very disk the growth may be filling.
    /// </summary>
    [Fact]
    public void Corrupt_truncation_leaves_a_quarantine_file()
    {
        var wal = OpenWal();
        Append(wal, Scalar("cpu", 1_000, 1.0));
        wal.Dispose();

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(32, SeekOrigin.Begin);                  // entry 0's Generation field
            fs.Write(BitConverter.GetBytes(155_000_000_000UL));
        }

        OpenWal(logger: new RecordingLogger());

        string quarantine = WalPath + ".quarantine";
        Assert.True(File.Exists(quarantine));
        // It holds the discarded head verbatim — the torn generation is its first field.
        var head = File.ReadAllBytes(quarantine);
        Assert.Equal(155_000_000_000UL, BitConverter.ToUInt64(head, 0));
    }

    /// <summary>
    /// The open-time shrink swallowing a DOUBLE failure (the resize and its restore) used to
    /// hand the engine a log with no mapping: the host came up, accepted traffic, recovered
    /// nothing, and threw from every append until someone restarted it. A log that cannot
    /// hold one entry does not get to open — one clear line at startup instead.
    /// </summary>
    [Fact]
    public void A_log_that_cannot_map_after_an_open_shrink_refuses_to_open()
    {
        var wal = OpenWal(4 * 1024);
        for (int i = 0; i < 300; i++)                       // grow it, so reopen WANTS to shrink
            Append(wal, Scalar("cpu", 1_000 + i, i));
        ulong gen = wal.BeginFlush();
        wal.CommitFlush(gen);                               // empty — but capacity stays 16 KiB…
        wal.Dispose();

        // …because the commit-time shrink is also a resize, so the seam must only arm at OPEN.
        // Grow the file back by hand to guarantee the reopen attempts a shrink.
        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
            fs.SetLength(32 + 16 * 1024);

        MetricWriteAheadLog? opened = null;
        var thrown = Record.Exception(() =>
            opened = MetricWriteAheadLog.Open(WalPath, 4 * 1024,
                beforeResize: static _ => throw new IOException("disk error (test seam)")));
        try
        {
            Assert.IsType<IOException>(thrown);
            Assert.Null(opened);
        }
        finally { opened?.Dispose(); }   // only on a red assertion — Open must not return
    }

    /// <summary>
    /// An exemplar carries its OWN time_unix_nano, parsed independently of its parent point's —
    /// so a sane point can arrive wearing a 2116-stamped exemplar, and a guard keyed on the
    /// point waves it through. Exemplar queries sort newest-first with an open upper bound, so
    /// one admitted far-future exemplar sits on top of every answer until the ring rotates it
    /// out. The guard reads the exemplar's clock.
    /// </summary>
    [Fact]
    public void An_exemplar_from_the_deep_future_is_refused_even_on_a_sane_point()
    {
        long nowNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        long century = nowNano + 100L * 365 * 24 * 3_600_000_000_000L;

        var engine = NewEngine();
        engine.Ingest(new[]
        {
            new MetricIngestItem
            {
                Name              = "http.latency",
                Kind              = MetricKind.Gauge,
                Unit              = "ms",
                Labels            = LabelSet.Empty,
                TimestampUnixNano = nowNano,                // the point itself is fine
                ScalarValue       = 12.5,
                Exemplars         =
                [
                    new MetricExemplar { TimestampUnixNano = century, Value = 666.0 },
                    new MetricExemplar { TimestampUnixNano = nowNano, Value = 12.5 },
                ],
            },
        });

        var served = engine.GetExemplars("http.latency", null, null, null);

        Assert.Single(served);
        Assert.Equal(nowNano, served[0].TimestampUnixNano);
    }

    private sealed class RecordingLogger : Microsoft.Extensions.Logging.ILogger<MetricStorageEngine>
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<
            (Microsoft.Extensions.Logging.LogLevel Level, string Text, Exception? Error)> _lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _lines.Enqueue((logLevel, formatter(state, exception), exception));

        public bool Saw(Microsoft.Extensions.Logging.LogLevel level, string prefix, out Exception? error)
        {
            foreach (var (l, text, e) in _lines)
                if (l == level && text.StartsWith(prefix, StringComparison.Ordinal))
                {
                    error = e;
                    return true;
                }
            error = null;
            return false;
        }

        /// <summary>How many lines at <paramref name="level"/> start with <paramref name="prefix"/> — for asserting a count, not just presence (e.g. rate-limiting).</summary>
        public int Count(Microsoft.Extensions.Logging.LogLevel level, string prefix)
        {
            int count = 0;
            foreach (var (l, text, _) in _lines)
                if (l == level && text.StartsWith(prefix, StringComparison.Ordinal))
                    count++;
            return count;
        }
    }

    /// <summary>
    /// Shutdown has to survive a flush that throws. The loop's last act is an unconditional
    /// final flush, and that await was bare: a throw there faulted <c>_flushTask</c>, and
    /// <c>DisposeAsync</c> caught only <see cref="OperationCanceledException"/> off it — so the
    /// fault came back out of dispose and skipped everything below the await, including the
    /// ingest fence, the log's close and the locks, while the <c>finally</c> still completed
    /// <c>_disposeCompleted</c> and told the host's other two disposers that teardown had
    /// finished. The engine then exited with its log mapped and its OTLP door open.
    ///
    /// <para>What is asserted is the door, not the absence of an exception: teardown must have
    /// run PAST the await, not merely survived it.</para>
    /// </summary>
    [Fact]
    public async Task A_final_flush_that_throws_still_shuts_the_door_behind_it()
    {
        var engine = NewEngine();
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        engine.Ingest(SlowFlushBatch(baseNano, "a", series: 20, pointsPerSeries: 10));
        engine.OnGenerationOpenedForTest = static () => throw new OutOfMemoryException("final flush");

        var teardown = await Record.ExceptionAsync(async () => await engine.DisposeAsync());
        Assert.True(teardown is null, $"DisposeAsync threw {teardown?.GetType().Name}");
        Assert.Throws<ObjectDisposedException>(() => engine.Ingest(SlowFlushBatch(baseNano, "b", 1, 1)));
    }

    /// <summary>
    /// The same crossed header seen from the log alone, and the loss the reorder above does not
    /// touch. Recovery keeps entries whose generation is strictly ABOVE the watermark, so a
    /// counter that reopens at or below it stamps every new append into the range recovery
    /// discards: the points are written, acknowledged, and then dropped by the next start.
    /// Repairing the counter at open — to one past the watermark, which is
    /// <c>FirstGeneration</c> exactly when the watermark is 0 — makes the first append after the
    /// repair replayable again, and bounds the damage of a badly crossed header to nothing
    /// instead of to one skipped compaction per flush until the counter catches up.
    /// </summary>
    [Fact]
    public void A_crossed_header_reopens_with_its_counter_above_the_watermark()
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        using (var wal = OpenWal(64 * 1024))
        {
            Append(wal, Scalar("m", baseNano, 1));
            wal.CommitFlush(wal.BeginFlush());            // watermark = 1, counter = 2
        }

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(16, SeekOrigin.Begin);               // WalFileHeader.Generation
            fs.Write(new byte[8]);                       // counter behind the standing watermark
        }

        using (var reopened = OpenWal(64 * 1024))
        {
            Assert.True(reopened.BeginFlush() > 1,
                "the counter reopened at or below the watermark, so the flush it opens is one " +
                "recovery already considers committed");
            Append(reopened, Scalar("m", baseNano + 1_000_000L, 2));
        }

        // The point appended after the repair has to come back. Below the watermark it does not.
        using var after = OpenWal(64 * 1024);
        var replayed = after.ReadAll(out _);
        Assert.Single(replayed);
        Assert.Equal(2, replayed[0].Point.Value);
    }

    /// <summary>
    /// A header whose generation counter sits at or below its committed watermark. Both fields
    /// live in the same 32-byte header — hence the same sector — so a torn write cannot separate
    /// them: this is a restored, copied or bit-rotted <c>metrics.wal</c>. It cost one skipped
    /// compaction before the one-open-flush guard; with it, <c>CommitFlush</c> returned on
    /// "already committed" WITHOUT clearing the open flush, so every later <c>BeginFlush</c>
    /// threw — the flush loop faulted, the shutdown flush never ran, and the fault came back out
    /// of <c>DisposeAsync</c>, which skipped the ingest fence and the log's close while its
    /// <c>finally</c> told the host's other two disposers that teardown had finished.
    /// </summary>
    [Fact]
    public async Task A_header_whose_generation_is_behind_its_watermark_still_flushes_and_shuts_down()
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        var seed = NewEngine();
        seed.Ingest(SlowFlushBatch(baseNano, "seed", series: 10, pointsPerSeries: 10));
        await seed.ScheduleThresholdFlushForTest();      // watermark = 1, generation = 2
        await seed.DisposeAsync();

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(16, SeekOrigin.Begin);               // WalFileHeader.Generation
            fs.Write(new byte[8]);                       // counter lost, watermark kept
        }

        var engine = NewEngine();
        var batch  = SlowFlushBatch(baseNano + 3_600_000_000_000L, "a", series: 10, pointsPerSeries: 10);
        engine.Ingest(batch);
        await engine.ScheduleThresholdFlushForTest();

        // A second flush is the whole point: it is the one that used to throw.
        engine.Ingest(SlowFlushBatch(baseNano + 7_200_000_000_000L, "b", series: 10, pointsPerSeries: 10));
        var second = await Record.ExceptionAsync(() => engine.ScheduleThresholdFlushForTest());
        Assert.True(second is null, $"the log refused a second flush: {second?.GetType().Name}");

        var teardown = await Record.ExceptionAsync(async () => await engine.DisposeAsync());
        Assert.True(teardown is null, $"DisposeAsync threw {teardown?.GetType().Name}");

        // Teardown ran to the end rather than being skipped: the door is shut behind it.
        Assert.Throws<ObjectDisposedException>(() => engine.Ingest(SlowFlushBatch(baseNano, "c", 1, 1)));
    }

    /// <summary>
    /// A torn first entry whose Generation field decodes to a huge value — the production
    /// incident verbatim (gen ≈ 155e9, 8 GiB WAL). No append can stamp a generation above
    /// the header counter, so both replay and compaction must treat such an entry as
    /// end-of-data: replay must not push its garbage fields into the hot tier, and a
    /// commit must be able to empty the log instead of keeping the entry "above the
    /// watermark" forever while the file doubles without bound.
    /// </summary>
    [Fact]
    public void Corrupt_generation_entry_ends_replay_and_compacts_away()
    {
        var wal = OpenWal();
        Append(wal, Scalar("cpu", 1_000, 1.0));
        Append(wal, Scalar("cpu", 2_000, 2.0));
        wal.Dispose();

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(32, SeekOrigin.Begin);               // first entry's Generation field
            fs.Write(BitConverter.GetBytes(155_000_000_000UL));
        }

        var reopened = OpenWal();
        var replayed = reopened.ReadAll(out int unresolved);
        Assert.Empty(replayed);                          // garbage fields must not replay
        Assert.Equal(0, unresolved);

        reopened.CommitFlush(reopened.BeginFlush());     // Compact must not strand the entry
        reopened.Dispose();

        using var check = new FileStream(WalPath, FileMode.Open, FileAccess.Read);
        Span<byte> off = stackalloc byte[8];
        check.Seek(8, SeekOrigin.Begin);                 // WalFileHeader.WriteOffset
        check.ReadExactly(off);
        Assert.Equal(32L, BitConverter.ToInt64(off));    // the log is empty again
    }
}
