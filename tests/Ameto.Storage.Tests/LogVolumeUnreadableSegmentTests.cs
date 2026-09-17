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
        var buf = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < 70; i++)
        {
            if (i == 60) _engine.FlushHotTierAsync().GetAwaiter().GetResult();

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
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

        // A merge publishing its output and deleting its sources while a histogram poll still
        // walks a snapshot that lists them: the entry goes, the file goes, the scan opens it.
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
        Assert.Equal(10, counts.Total);   // the hot tier; the removed segment is simply not in the window any more
        Assert.Empty(HeaderWarnings());
        Assert.Contains(_log.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Debug &&
                                           e.Message.Contains("left the catalog", StringComparison.Ordinal));
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
