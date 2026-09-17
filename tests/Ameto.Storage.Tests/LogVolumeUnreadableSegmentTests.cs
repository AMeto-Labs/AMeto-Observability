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
