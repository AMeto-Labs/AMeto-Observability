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

    /// <summary>
    /// Segments one level-split flush produces, and the width of the id block it reserves.
    ///
    /// <para>Production calls this <c>StorageEngine.LevelSegmentSlots</c>, which is private and
    /// is defined as "Verbose..Fatal" — one slot per <see cref="LogLevel"/>. Derived from the
    /// enum rather than repeated as a literal 6: if a seventh level were added, a stale literal
    /// would make the cleanup loop below delete six of the seven segments the shutdown flush
    /// produced, and the survivor would fail the "flush produced nothing else" assertion with a
    /// message pointing at the segment directory instead of at the constant.</para>
    /// </summary>
    private static readonly int Levels = Enum.GetValues<LogLevel>().Length;

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
            for (int lvl = 0; lvl < Levels; lvl++)
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
        Assert.Equal(Levels, segs.Count);

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

        Assert.Equal(PerLevel * Levels, ids.Count);
        Assert.Equal(PerLevel * Levels, ids.Distinct().Count());
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

    /// <summary>
    /// WAL recovery must split by level too. It wrote the recovered tier as ONE mixed-level
    /// segment, which reopened exactly the data loss the split exists to prevent — and did
    /// so on the path taken after a crash, when losing Errors is least acceptable. The live
    /// flush path was covered by the tests above; this one was not.
    ///
    /// <para>THE CRASH HAS TO BE RECONSTRUCTED, not approximated by dropping the engine.
    /// <c>DisposeAsync</c> flushes a non-empty hot tier and the flush deletes the WAL it
    /// drained, so "write without flushing, then dispose" leaves no orphan: measured, dispose
    /// wrote 6 segments and the restarted engine logged no recovery at all. Every assertion
    /// below then described the LIVE flush path — the one the tests above already cover — and
    /// <c>ReplayOrphanedWals</c> was never executed.</para>
    ///
    /// <para>So the on-disk state a kill -9 leaves is built explicitly, the way
    /// <c>WalSegmentIdTests</c> builds it: keep the WAL bytes, let shutdown flush and remove
    /// them, then delete the segments that flush produced and put the WAL back. The reserved
    /// block is what identifies those segments — startup reads "this WAL was already flushed"
    /// off a segment carrying one of its ids, so the orphan is only believable once they are
    /// gone.</para>
    /// </summary>
    [Fact]
    public async Task WalRecoveryAlsoSplitsByLevel()
    {
        const int Rounds = 12;
        int Events = Rounds * Levels;

        long baseTicks = DateTime.UtcNow.Ticks;
        int n = 0;
        for (int i = 0; i < Rounds; i++)
            for (int lvl = 0; lvl < Levels; lvl++)
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

        // The block of segment ids this WAL's events will occupy, and the WAL bytes themselves.
        ulong walId  = _engine.LiveWalSegmentId;
        var   walDir = Path.Combine(_dir, "wal");
        var   segDir = Path.Combine(_dir, "segments");
        var   wal    = Directory.GetFiles(walDir, "*.wal").Single();
        File.Copy(wal, wal + ".crash", overwrite: true);
        if (File.Exists(wal + ".pool")) File.Copy(wal + ".pool", wal + ".pool.crash", overwrite: true);

        await _engine.DisposeAsync();   // flushes every event and removes the WAL

        // Undo the shutdown flush: drop every segment the WAL's own block produced...
        foreach (var f in Directory.GetFiles(segDir, "*.seg"))
        {
            var parts = Path.GetFileNameWithoutExtension(f).Split('-');
            if (ulong.TryParse(parts[1], out var id) && id >= walId && id < walId + (ulong)Levels)
                File.Delete(f);
        }
        Assert.Empty(Directory.GetFiles(segDir, "*.seg"));   // the flush produced nothing else

        // ...and put the WAL back, so the tree reads "never flushed".
        File.Copy(wal + ".crash", wal, overwrite: true);
        if (File.Exists(wal + ".pool.crash")) File.Copy(wal + ".pool.crash", wal + ".pool", overwrite: true);

        // A fresh engine replays it. Recovery must produce level-pure segments.
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance)
        { _allowIndexlessMerge = true };

        await _engine.CatalogLoaded;

        var segs = _engine.ListSegments();
        // Every level in, one level-pure segment per level out — recovery ran and split, rather
        // than writing the recovered tier as one mixed file.
        Assert.Equal(Levels, segs.Count);

        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        var ids = new List<ulong>();
        foreach (var seg in segs)
        {
            using var r = SegmentReader.Open(seg.FilePath);
            foreach (var ev in r.ReadAllRaw(dedup))
            {
                Assert.Equal((byte)seg.MinLevel, ev.Level);   // level-pure
                ids.Add(ev.Id);
            }
        }

        Assert.Equal(Events, ids.Count);                      // nothing lost
        Assert.Equal(Events, ids.Distinct().Count());         // nothing duplicated
    }
}
