using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// A flush writes one segment per log level, so a segment holds exactly one level.
///
/// <para>This is what makes retention exact. Expiry is <c>MaxTimestamp + Ttl(MinLevel)</c>
/// and MinLevel is the lowest severity VALUE in the segment — but TTL is not monotonic in
/// that value (Debug 3 days sits below Information 90), so one Debug event in a mixed
/// segment dragged every Error beside it to a 3-day deadline. Measured on the sandbox
/// stand before this change: 279 segments / 1116 MB inside 3 days and 10 segments / ~2 MB
/// older — a clean cliff exactly at Debug's TTL.</para>
/// </summary>
public sealed class LevelSplitFlushTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-levelsplit-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance)
        { _allowIndexlessMerge = true };
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private async Task WriteMixedAsync(int perLevel)
    {
        long baseTicks = DateTime.UtcNow.Ticks;
        int n = 0;
        for (int i = 0; i < perLevel; i++)
            for (int lvl = 0; lvl < 6; lvl++)
            {
                var h = new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)n).RawValue,
                    TimestampUtcTicks        = baseTicks + n * TimeSpan.TicksPerMillisecond,
                    Level                    = (LogLevel)lvl,
                    MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
                    ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc.A"),
                };
                Assert.True(_engine.TryWrite(h, Props(n)));
                n++;
            }
        await _engine.FlushHotTierAsync();
    }

    [Fact]
    public async Task OneFlushProducesOneSegmentPerLevel_EachLevelPure()
    {
        const int PerLevel = 40;
        await WriteMixedAsync(PerLevel);

        var segs = _engine.ListSegments();
        Assert.Equal(6, segs.Count);

        // Every level present exactly once, and each segment holds only its own level.
        var levels = segs.Select(s => s.MinLevel).OrderBy(l => l).ToArray();
        Assert.Equal(new[] { LogLevel.Verbose, LogLevel.Debug, LogLevel.Information,
                             LogLevel.Warning, LogLevel.Error, LogLevel.Fatal }, levels);

        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var seg in segs)
        {
            Assert.Equal((uint)PerLevel, seg.EventCount);
            using var r = SegmentReader.Open(seg.FilePath);
            foreach (var ev in r.ReadAllRaw(dedup))
                Assert.Equal((byte)seg.MinLevel, ev.Level);
        }
    }

    [Fact]
    public async Task NoEventIsLostOrDuplicatedBySplitting()
    {
        const int PerLevel = 25;
        await WriteMixedAsync(PerLevel);

        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        var ids = new List<ulong>();
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            ids.AddRange(r.ReadAllRaw(dedup).Select(e => e.Id));
        }

        Assert.Equal(PerLevel * 6, ids.Count);
        Assert.Equal(PerLevel * 6, ids.Distinct().Count());
    }

    /// <summary>
    /// Each level's subsequence must stay sorted by (ts, id) — the query k-way merge and
    /// cursor pagination both rely on per-segment ordering.
    /// </summary>
    [Fact]
    public async Task EachLevelSegmentStaysSortedByTimestampAndId()
    {
        await WriteMixedAsync(30);

        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            var evs = r.ReadAllRaw(dedup);
            for (int i = 1; i < evs.Count; i++)
            {
                bool ordered = evs[i].TsTicks > evs[i - 1].TsTicks ||
                               (evs[i].TsTicks == evs[i - 1].TsTicks && evs[i].Id >= evs[i - 1].Id);
                Assert.True(ordered, $"segment {seg.Id} out of order at {i}");
            }
        }
    }

    /// <summary>
    /// The point of the whole change: a Debug event can no longer set the deadline for
    /// Errors, because they are not in the same file.
    /// </summary>
    [Fact]
    public async Task DebugCannotShortenTheDeadlineOfErrors()
    {
        await WriteMixedAsync(10);

        // The stand's policy shape: short-lived Debug/Verbose, long-lived Information+.
        var policy = new RetentionPolicy(new Dictionary<LogLevel, TimeSpan>
        {
            [LogLevel.Verbose]     = TimeSpan.FromDays(3),
            [LogLevel.Debug]       = TimeSpan.FromDays(3),
            [LogLevel.Information] = TimeSpan.FromDays(90),
            [LogLevel.Warning]     = TimeSpan.FromDays(90),
            [LogLevel.Error]       = TimeSpan.FromDays(90),
            [LogLevel.Fatal]       = TimeSpan.FromDays(90),
        });

        var inTenDays = DateTimeOffset.UtcNow.AddDays(10);
        foreach (var seg in _engine.ListSegments())
        {
            bool shortLived = seg.MinLevel is LogLevel.Verbose or LogLevel.Debug;
            Assert.Equal(shortLived, seg.IsExpired(policy, inTenDays));
        }
    }
}
