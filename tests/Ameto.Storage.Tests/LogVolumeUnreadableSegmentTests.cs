using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// The header aggregation skips a cold segment it cannot read, so one torn file does not blank a
/// volume chart — and it now COUNTS the skip, because the query language presents the same totals
/// as facts and has to be able to call them a floor. The skip is also the only place a torn
/// segment met at runtime is reported, so it is logged at Warning with the segment named — once,
/// since the histogram polls and alert rules tick far more often than anyone reads the log.
/// </summary>
public sealed class LogVolumeUnreadableSegmentTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 25, 9, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddHours(1);

    private readonly string           _dir = Path.Combine(Path.GetTempPath(), "ameto-volskip-" + Guid.NewGuid().ToString("N"));
    private readonly CapturingLogger  _log = new();
    private readonly StorageEngine    _engine;

    public LogVolumeUnreadableSegmentTests()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine  = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            _log);
        _engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);

        // One level, so the flush writes exactly one cold segment; then a hot tier on top that
        // the tear cannot touch.
        for (int i = 0; i < 70; i++)
        {
            if (i == 60) _engine.FlushHotTierAsync().GetAwaiter().GetResult();
            WriteError(i);
        }
    }

    /// <summary>The <paramref name="i"/>-th Error event, <paramref name="i"/> seconds after <see cref="Base"/>.</summary>
    private void WriteError(int i)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();

        Assert.True(_engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = Base.UtcTicks + i * TimeSpan.TicksPerSecond,
            Level                    = LogLevel.Error,
            MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
            ServiceNamePoolIndex     = _engine.TemplatePool.Intern("billing"),
        }, buf.WrittenSpan.ToArray()));
    }

    public void Dispose()
    {
        try { _engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private ValueTask<LogVolumeCounts> CountAsync() =>
        _engine.AggregateLogVolumeAsync(From, To, minBucket: 0, bucketSeconds: 1, nBuckets: 1, serviceFilter: null);

    [Fact]
    public async Task An_unreadable_segment_is_counted_as_skipped_and_named_once_at_warning()
    {
        var healthy = await CountAsync();
        Assert.Equal(70, healthy.Total);
        Assert.Equal(0, healthy.SkippedSegments);

        var seg = Assert.Single(_engine.ListSegments());
        Assert.Equal(60u, seg.EventCount);

        // The first block's uncompressedSize, right after the 46-byte header, torn negative:
        // ValidateBlockFrame throws InvalidDataException before any header of it is counted.
        using (var f = File.Open(seg.FilePath, FileMode.Open, FileAccess.Write))
        {
            f.Position = 46;
            f.Write([0xF9, 0xFF, 0xFF, 0xFF]);
        }

        var first  = await CountAsync();
        var second = await CountAsync();

        foreach (var c in new[] { first, second })
        {
            Assert.Equal(10, c.Total);                 // the hot tier, and nothing of the torn file
            Assert.Equal(1,  c.SkippedSegments);
        }

        var warning = Assert.Single(_log.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Warning &&
                                                        e.Message.Contains("Header aggregation", StringComparison.Ordinal));
        Assert.Contains(seg.Id.ToString(), warning.Message);
        Assert.IsType<InvalidDataException>(warning.Error);

        // The repeat is still there for whoever turns Debug on.
        Assert.Contains(_log.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Debug &&
                                           e.Error is InvalidDataException);
    }

    private IEnumerable<(Microsoft.Extensions.Logging.LogLevel Level, string Message, Exception? Error)> HeaderWarnings() =>
        _log.Entries.Where(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Warning &&
                                e.Message.Contains("Header aggregation", StringComparison.Ordinal));

    /// <summary>Tears the first block frame, as the test above does.</summary>
    private static void Tear(SegmentInfo seg)
    {
        using var f = File.Open(seg.FilePath, FileMode.Open, FileAccess.Write);
        f.Position = 46;
        f.Write([0xF9, 0xFF, 0xFF, 0xFF]);
    }

    /// <summary>The ten hot events become a second Error segment, so there are two to tear.</summary>
    private async Task<(SegmentInfo First, SegmentInfo Second)> TwoSegmentsAsync()
    {
        var first = Assert.Single(_engine.ListSegments());
        await _engine.FlushHotTierAsync();
        var second = Assert.Single(_engine.ListSegments(), s => SegmentKey.Of(s) != SegmentKey.Of(first));
        return (first, second);
    }

    [Fact]
    public async Task A_segment_removed_from_the_catalog_while_the_scan_runs_is_neither_partial_nor_warned()
    {
        var seg = Assert.Single(_engine.ListSegments());

        // Retention deleting an expired segment while a histogram poll still walks a snapshot
        // that lists it: the entry goes, the file goes, the scan opens it.
        int removed = 0;
        _engine._beforeHeaderSegmentOpen = info =>
        {
            if (Interlocked.Exchange(ref removed, 1) == 0)
                _engine.DeleteSegmentAsync(SegmentKey.Of(info)).GetAwaiter().GetResult();
        };

        var counts = await CountAsync();

        Assert.Equal(1, removed);
        Assert.False(File.Exists(seg.FilePath), "setup: the delete should have removed the file");
        Assert.Equal(0, counts.SkippedSegments);
        Assert.Equal(0, counts.MergedAwaySegments);   // its events are gone, not moved: the count is exact
        Assert.Equal(10, counts.Total);   // the hot tier; the removed segment is simply not in the window any more
        Assert.Empty(HeaderWarnings());
        Assert.Contains(_log.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Debug &&
                                           e.Message.Contains("left the catalog", StringComparison.Ordinal));
    }

    /// <summary>
    /// A second 60-event Error segment beside the first, and no hot tier left. The planner merges
    /// a pair only in a sealed bucket and only when the smaller half adds at least half the
    /// larger; <see cref="Base"/> is weeks in the past, so its bucket is sealed.
    /// </summary>
    private async Task<IReadOnlyList<SegmentInfo>> MergeablePairAsync()
    {
        for (int i = 70; i < 120; i++) WriteError(i);
        await _engine.FlushHotTierAsync();
        var pair = _engine.ListSegments();
        Assert.Equal(2, pair.Count);
        return pair;
    }

    /// <summary>
    /// Runs one merge pass from inside the scan, before its first open: the merge publishes its
    /// output and deletes both sources while the scan walks a snapshot that lists the sources and
    /// not the output. Every other worker waits for the merge, so neither source is read.
    /// Returns whether the pass merged.
    /// </summary>
    private async Task<(LogVolumeCounts Counts, bool Merged)> CountWithMergeUnderneathAsync()
    {
        using var done = new ManualResetEventSlim();
        int  first  = 0;
        bool merged = false;
        _engine._beforeHeaderSegmentOpen = _ =>
        {
            if (Interlocked.Exchange(ref first, 1) == 0)
            {
                try   { merged = _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None).GetAwaiter().GetResult(); }
                finally { done.Set(); }
            }
            else done.Wait(TimeSpan.FromSeconds(30));
        };

        try { return (await CountAsync(), merged); }
        finally { _engine._beforeHeaderSegmentOpen = null; }
    }

    /// <summary>
    /// A MERGE UNDER THE SCAN MAKES THE COUNT A FLOOR. Its sources' events are in the merged
    /// output, which the scan's snapshot does not list, so leaving them out gives a LOW number —
    /// and at 7693f2a that low number came back with nothing to say so, the race silenced along
    /// with retention's. It is still not damage: no skip, no Warning.
    /// </summary>
    [Fact]
    public async Task A_segment_a_merge_rewrote_while_the_scan_ran_is_counted_as_merged_away_without_a_warning()
    {
        var sources = await MergeablePairAsync();

        var (counts, merged) = await CountWithMergeUnderneathAsync();

        Assert.True(merged, "setup: the merge pass did not merge the pair");
        Assert.Equal(120u, Assert.Single(_engine.ListSegments()).EventCount);
        foreach (var s in sources) Assert.False(File.Exists(s.FilePath), "setup: a source survived the merge");

        Assert.Equal(0, counts.Total);                 // neither source was read, and the output was never in the snapshot
        Assert.Equal(2, counts.MergedAwaySegments);    // …which is what makes that 0 a floor
        Assert.Equal(0, counts.SkippedSegments);       // nothing is damaged
        Assert.Empty(HeaderWarnings());

        // The next scan lists the output and is whole.
        var again = await CountAsync();
        Assert.Equal(120, again.Total);
        Assert.Equal(0, again.MergedAwaySegments);
    }

    /// <summary>
    /// A MERGE WHOSE OUTPUT THE SNAPSHOT ALREADY LISTS IS NOT A FLOOR. The merge publishes its
    /// output and then deletes its sources one by one, so a scan whose snapshot is taken in
    /// between lists the output AND a source still to be deleted. The output holds that source's
    /// events and the scan reads them there; the source it then fails to open lost nothing, and
    /// calling the count a floor made an exact total look partial.
    ///
    /// <para>The scan starts right after the merge removed its first source's entry, so its
    /// snapshot lists the output and the second source; every worker waits for the merge to
    /// finish, so the second source's file is gone when it is opened.</para>
    /// </summary>
    [Fact]
    public async Task A_merge_source_deleted_after_a_snapshot_that_lists_its_output_is_not_a_floor()
    {
        var sources = await MergeablePairAsync();

        using var firstRemoved = new ManualResetEventSlim();
        using var scanning     = new ManualResetEventSlim();
        using var mergeDone    = new ManualResetEventSlim();
        int removals = 0;
        _engine._afterSegmentEntryRemoved = () =>
        {
            if (Interlocked.Increment(ref removals) != 1) return;
            firstRemoved.Set();
            Assert.True(scanning.Wait(TimeSpan.FromSeconds(30)), "the scan never took its snapshot");
        };
        var merge = Task.Run(async () =>
        {
            try     { return await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None); }
            finally { mergeDone.Set(); }
        });
        Assert.True(firstRemoved.Wait(TimeSpan.FromSeconds(30)), "the merge never removed a source");

        IReadOnlyList<SegmentInfo>? listed = null;
        _engine._beforeHeaderSegmentOpen = _ =>
        {
            // Read before the merge is released, and kept only from the worker that got here
            // first: a later worker's read may already see the second source gone.
            Interlocked.CompareExchange(ref listed, _engine.ListSegments(), null);
            scanning.Set();
            mergeDone.Wait(TimeSpan.FromSeconds(30));
        };
        LogVolumeCounts counts;
        try     { counts = await CountAsync(); }
        finally { _engine._beforeHeaderSegmentOpen = null; _engine._afterSegmentEntryRemoved = null; }

        Assert.True(await merge, "setup: the merge pass did not merge the pair");
        var output = Assert.Single(_engine.ListSegments());
        Assert.Equal(120u, output.EventCount);
        Assert.Equal(2, removals);
        Assert.NotNull(listed);
        // The catalog the scan's workers saw: the output and one source, never both sources.
        Assert.Contains(listed!, s => SegmentKey.Of(s) == SegmentKey.Of(output));
        Assert.Single(listed!, s => sources.Any(src => SegmentKey.Of(src) == SegmentKey.Of(s)));
        foreach (var s in sources) Assert.False(File.Exists(s.FilePath), "setup: a source survived the merge");

        Assert.Equal(120, counts.Total);               // every event, read in the output
        Assert.Equal(0,   counts.MergedAwaySegments);  // …so the missed source is no floor
        Assert.Equal(0,   counts.SkippedSegments);
        Assert.Empty(HeaderWarnings());
    }

    /// <summary>
    /// The record of merged-away keys is bounded, and a scan that may have lost a record to the
    /// bound calls the removal a merge rather than guess retention. With room for ONE key, the
    /// merge's second source evicts the first while the scan runs; the first is still a floor.
    /// </summary>
    [Fact]
    public async Task A_merged_away_segment_whose_record_was_evicted_during_the_scan_is_still_a_floor()
    {
        _engine.MergedAwaySegmentCap = 1;
        await MergeablePairAsync();

        var (counts, merged) = await CountWithMergeUnderneathAsync();

        Assert.True(merged, "setup: the merge pass did not merge the pair");
        Assert.Equal(0, counts.Total);
        Assert.Equal(2, counts.MergedAwaySegments);
        Assert.Empty(HeaderWarnings());
    }

    /// <summary>
    /// The fallback is decided by how far eviction has reached, not by whether it ever happened:
    /// on a server that has merged more sources than the record holds, something always has
    /// been evicted, and a rule that looked only at that would call every retention delete racing
    /// a scan a merge. Here the record is full and has evicted — before this scan started — and
    /// retention removes the merged output under the scan: its events are gone, so the count
    /// stays exact.
    /// </summary>
    [Fact]
    public async Task A_retention_delete_is_not_called_a_merge_because_older_merge_records_were_evicted()
    {
        _engine.MergedAwaySegmentCap = 1;
        await MergeablePairAsync();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the pair did not merge");
        var output = Assert.Single(_engine.ListSegments());

        int removed = 0;
        _engine._beforeHeaderSegmentOpen = info =>
        {
            if (Interlocked.Exchange(ref removed, 1) == 0)
                _engine.DeleteSegmentAsync(SegmentKey.Of(info)).GetAwaiter().GetResult();
        };

        var counts = await CountAsync();

        Assert.Equal(1, removed);
        Assert.False(File.Exists(output.FilePath), "setup: the delete should have removed the merged file");
        Assert.Equal(0, counts.Total);
        Assert.Equal(0, counts.MergedAwaySegments);
        Assert.Equal(0, counts.SkippedSegments);
    }

    [Fact]
    public async Task A_segment_whose_import_has_published_but_not_moved_is_counted_once_the_move_lands()
    {
        // A replicated segment staged outside the segments directory, moved in by the import.
        var peer    = new NodeId(7);
        var pool    = new StringInternPool();
        string staged = Path.Combine(_dir, "staged-7-40.seg");
        string final  = Path.Combine(_dir, "segments", "7-40.seg");
        using (var hot = new HotTierSegment(16, 1L << 20))
        {
            var buf = new ArrayBufferWriter<byte>(32);
            for (int i = 0; i < 5; i++)
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(1);
                w.Write("n"); w.Write((long)i);
                w.Flush();
                Assert.True(hot.TryWrite(new LogEventHeader
                {
                    Id                       = new EventId(peer.Value, (uint)i).RawValue,
                    TimestampUtcTicks        = Base.AddMinutes(30).UtcTicks + i * TimeSpan.TicksPerSecond,
                    Level                    = LogLevel.Information,
                    MessageTemplatePoolIndex = pool.Intern("peer {n}"),
                }, buf.WrittenSpan.ToArray(), "peer {n}"));
            }
            hot.Freeze();
            using var writer = new SegmentWriter(staged);
            writer.WriteEvents(hot, pool);
            writer.Finalise(peer, new SegmentId(40));
        }

        using var published = new ManualResetEventSlim();
        using var release   = new ManualResetEventSlim();
        using var opening   = new ManualResetEventSlim();
        _engine._afterImportPublish = () =>
        {
            published.Set();
            Assert.True(release.Wait(TimeSpan.FromSeconds(30)), "the test never released the import");
        };

        var import = Task.Run(() => _engine.ImportSegment(staged, final));
        Assert.True(published.Wait(TimeSpan.FromSeconds(30)), "the import never published its entry");
        Assert.False(File.Exists(final), "setup: the file must not have landed yet");

        _engine._beforeHeaderSegmentOpen = info =>
        {
            if (info.NodeId.Value == peer.Value) opening.Set();
        };
        var count = CountAsync().AsTask();

        try
        {
            Assert.True(opening.Wait(TimeSpan.FromSeconds(30)), "the scan never reached the imported segment");
            // The open has found no file. It must wait for the import rather than call the count
            // partial over a file that is only in flight.
            await Task.Delay(300);
            Assert.False(count.IsCompleted, "the scan finished while the import was still moving the file in");
        }
        finally { release.Set(); }

        Assert.Equal(SegmentImportOutcome.Registered, await import);
        var counts = await count;

        Assert.Equal(0, counts.SkippedSegments);
        Assert.Equal(75, counts.Total);   // 60 cold + 10 hot + the 5 imported
        Assert.Empty(HeaderWarnings());
    }

    [Fact]
    public async Task Past_the_warning_cap_an_unreadable_segment_is_not_warned_about_on_every_poll()
    {
        var (first, second) = await TwoSegmentsAsync();
        Tear(first);
        Tear(second);
        _engine.WarnedUnreadableSegmentCap = 1;

        for (int poll = 0; poll < 3; poll++)
            Assert.Equal(2, (await CountAsync()).SkippedSegments);   // still a floor, cap or no cap

        // One segment is named; the other is past the cap and stays at Debug, poll after poll.
        // Clearing the set when it filled up named both again on every poll.
        Assert.Single(HeaderWarnings());
    }

    [Fact]
    public async Task A_deleted_segment_gives_back_its_place_under_the_warning_cap()
    {
        var (first, second) = await TwoSegmentsAsync();
        _engine.WarnedUnreadableSegmentCap = 1;

        Tear(first);
        Assert.Equal(1, (await CountAsync()).SkippedSegments);
        Assert.Contains(first.FilePath, Assert.Single(HeaderWarnings()).Message);

        // Retention (or an operator) removes the torn segment; a different one is torn later.
        await _engine.DeleteSegmentAsync(SegmentKey.Of(first));
        Tear(second);
        Assert.Equal(1, (await CountAsync()).SkippedSegments);

        var warnings = HeaderWarnings().ToList();
        Assert.Equal(2, warnings.Count);
        Assert.Contains(second.FilePath, warnings[1].Message);
    }

    /// <summary>
    /// The catalog check and the place under the cap were two steps, and the delete's eviction
    /// was not in step with either: a delete landing between them evicted a key not yet added,
    /// and the add then kept a key for a segment nothing would ever delete again — one capped
    /// place gone for the life of the process, and a Warning about a segment already removed.
    /// The hook deletes the torn segment exactly there.
    ///
    /// <para>And the segment is not a skip either. It was counted as one before its place was
    /// taken, so the count came back a floor whose reason pointed at a Warning the take-back never
    /// let be written — for a segment retention removed, whose events are gone, so the count
    /// without it was exact. Found gone at that point it is the same race as one found gone a
    /// step earlier, and sorted the same way.</para>
    /// </summary>
    [Fact]
    public async Task A_segment_deleted_between_the_catalog_check_and_the_warning_gives_its_place_back()
    {
        var (first, second) = await TwoSegmentsAsync();
        _engine.WarnedUnreadableSegmentCap = 1;
        Tear(first);

        int deleted = 0;
        _engine._beforeUnreadableSegmentWarned = info =>
        {
            if (Interlocked.Exchange(ref deleted, 1) == 0)
                _engine.DeleteSegmentAsync(SegmentKey.Of(info)).GetAwaiter().GetResult();
        };
        var raced = await CountAsync();
        _engine._beforeUnreadableSegmentWarned = null;
        var warnedAboutTheDeleted = HeaderWarnings().Any(w => w.Message.Contains(first.FilePath, StringComparison.Ordinal));

        Tear(second);
        var later = await CountAsync();

        Assert.Equal(1, deleted);
        // Retention removed it before its place under the cap was settled: its events are gone,
        // so the count without them is exact — no skip, no merge, nothing to call a floor.
        Assert.Equal(0,  raced.SkippedSegments);
        Assert.Equal(0,  raced.MergedAwaySegments);
        Assert.Equal(10, raced.Total);             // the second segment, whole
        Assert.Contains(_log.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Debug &&
                                           e.Message.Contains("left the catalog", StringComparison.Ordinal));
        Assert.Equal(1, later.SkippedSegments);

        // The one place under the cap is free for the next torn segment…
        Assert.Contains(second.FilePath, Assert.Single(HeaderWarnings()).Message);
        // …and nothing was said at Warning about the segment already gone.
        Assert.False(warnedAboutTheDeleted, "the deleted segment was named at Warning");
    }

    /// <summary>
    /// The same window with no place to take — the cap already full — is the same race: the
    /// segment is gone for the count whether or not a place under the cap was involved, so the
    /// check after the gate runs either way.
    /// </summary>
    [Fact]
    public async Task A_segment_deleted_in_that_window_past_the_warning_cap_is_not_a_skip_either()
    {
        var (first, _) = await TwoSegmentsAsync();
        _engine.WarnedUnreadableSegmentCap = 0;
        Tear(first);

        int deleted = 0;
        _engine._beforeUnreadableSegmentWarned = info =>
        {
            if (Interlocked.Exchange(ref deleted, 1) == 0)
                _engine.DeleteSegmentAsync(SegmentKey.Of(info)).GetAwaiter().GetResult();
        };
        var raced = await CountAsync();
        _engine._beforeUnreadableSegmentWarned = null;

        Assert.Equal(1, deleted);
        Assert.Equal(0,  raced.SkippedSegments);
        Assert.Equal(0,  raced.MergedAwaySegments);
        Assert.Equal(10, raced.Total);
        Assert.Empty(HeaderWarnings());
    }

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private readonly List<(Microsoft.Extensions.Logging.LogLevel Level, string Message, Exception? Error)> _entries = [];

        public IReadOnlyList<(Microsoft.Extensions.Logging.LogLevel Level, string Message, Exception? Error)> Entries
        {
            get { lock (_entries) return _entries.ToList(); }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel level) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel level,
                                Microsoft.Extensions.Logging.EventId eventId, TState state,
                                Exception? error, Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((level, formatter(state, error), error));
        }
    }
}
