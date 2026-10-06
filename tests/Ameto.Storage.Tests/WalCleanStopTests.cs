using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// What a clean stop leaves in <c>wal/</c>. It used to leave one log every time: the successor its
/// final flush opened, or the boot log of a run that took no event — empty, preallocated to 64 MB,
/// deleted unread by the next start of the same format, and kept and reported by a release that
/// could not read it (#121 review F1). It now deletes its live log when nothing was ever appended to
/// it, and only then: a log that an event reached after the final flush is that event's only copy.
/// </summary>
public sealed class WalCleanStopTests : IDisposable
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(60);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-walstop-" + Guid.NewGuid().ToString("N"));

    private string WalDir => Path.Combine(_dir, "wal");

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private StorageEngine NewEngine()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        return new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
    }

    private static bool TryWrite(StorageEngine engine, long ticks, int n)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)n);
        w.Flush();
        return engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = ticks,
            Level                    = LogLevel.Information,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
            ServiceNamePoolIndex     = engine.TemplatePool.Intern("Svc.Stop"),
        }, buf.WrittenSpan, "evt {n}");
    }

    private static List<RawSegmentEvent> Cold(StorageEngine engine)
    {
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        var all   = new List<RawSegmentEvent>();
        foreach (var seg in engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            all.AddRange(r.ReadAllRaw(dedup));
        }
        return all;
    }

    /// <summary>A stop with events (the final flush's empty successor) and one without (the boot log).</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(100)]
    public async Task A_clean_stop_leaves_no_log_behind(int events)
    {
        long t = DateTime.UtcNow.Ticks;
        var engine = NewEngine();
        for (int i = 0; i < events; i++) Assert.True(TryWrite(engine, t + i, i));
        await engine.DisposeAsync().AsTask().WaitAsync(HangGuard);

        Assert.Empty(Directory.GetFiles(WalDir, "*.wal"));
        Assert.Empty(Directory.GetFiles(WalDir, "*.pool"));

        await using var again = NewEngine();
        await again.CatalogLoaded;
        Assert.Equal(events, Cold(again).Count);              // all of it in segments, none lost with the log
    }

    /// <summary>
    /// Writes are still open while the final flush runs its heavy phase, and what lands then is in
    /// the successor tier and its log only: shutdown does not flush again, and frees that tier. Its
    /// log is the events' only copy, so the stop must keep it — and the next start replays it.
    /// </summary>
    [Fact]
    public async Task Events_written_while_the_final_flush_runs_keep_the_live_log_for_the_next_start()
    {
        long t = DateTime.UtcNow.Ticks;
        var engine = NewEngine();
        for (int i = 0; i < 100; i++) Assert.True(TryWrite(engine, t + i, i));   // the final flush's tier

        int late = -1;
        engine._afterLevelPublished = _ =>
        {
            if (late >= 0) return;
            late = 0;
            for (int i = 0; i < 25; i++)
                if (TryWrite(engine, t + 1_000 + i, 1_000 + i)) late++;
        };
        await engine.DisposeAsync().AsTask().WaitAsync(HangGuard);

        Assert.Equal(25, late);
        Assert.Single(Directory.GetFiles(WalDir, "*.wal"));   // the live log, holding the 25

        await using var again = NewEngine();
        await again.CatalogLoaded;
        var cold = Cold(again);
        Assert.Equal(125, cold.Count);
        Assert.Equal(25, cold.Count(e => e.TsTicks >= t + 1_000));
        Assert.All(cold, e => Assert.Equal("Svc.Stop", e.Service));
    }
}
