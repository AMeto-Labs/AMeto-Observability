using System.Buffers;
using Ameto.Core;
using Ameto.Storage;
using MessagePack;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// v6 gives every block its min timestamp in the block index, so a windowed read can skip
/// blocks without decompressing them. That skip is the precondition for large segments —
/// and it is also the one change that could silently DROP events, so these tests compare
/// every window against a brute-force scan and hammer the boundaries.
/// </summary>
public sealed class BlockZoneMapTests : IDisposable
{
    private const int Events = 4_000;          // enough for many 64 KB blocks
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-zonemap-" + Guid.NewGuid().ToString("N"));
    private readonly string _path;
    private readonly long   _base;

    public BlockZoneMapTests()
    {
        Directory.CreateDirectory(_dir);
        _base = new DateTimeOffset(2026, 7, 30, 12, 0, 0, TimeSpan.Zero).UtcTicks;
        _path = BuildSegment();
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>Events are one second apart, so windows map to exact event counts.</summary>
    private long TickAt(int i) => _base + i * TimeSpan.TicksPerSecond;

    private static List<LogEvent> Read(string path, DateTimeOffset? from, DateTimeOffset? to, bool reversed = false)
    {
        using var reader = SegmentReader.Open(path);
        var list = new List<LogEvent>();
        var e = reader.ReadEventsAsync(null, from, to, reversed, default).GetAsyncEnumerator();
        try { while (e.MoveNextAsync().GetAwaiter().GetResult()) list.Add(e.Current); }
        finally { e.DisposeAsync().GetAwaiter().GetResult(); }
        return list;
    }

    /// <summary>Brute force: read everything, filter in memory. The oracle.</summary>
    private List<ulong> Expected(long fromTicks, long toTicks) =>
        Read(_path, null, null)
            .Where(ev => ev.Timestamp.UtcTicks >= fromTicks && ev.Timestamp.UtcTicks <= toTicks)
            .Select(ev => ev.Id.RawValue)
            .OrderBy(x => x)
            .ToList();

    private List<ulong> Windowed(long fromTicks, long toTicks) =>
        Read(_path,
             new DateTimeOffset(fromTicks, TimeSpan.Zero),
             new DateTimeOffset(toTicks,   TimeSpan.Zero))
            .Select(ev => ev.Id.RawValue)
            .OrderBy(x => x)
            .ToList();

    [Fact]
    public void EveryWindowMatchesABruteForceScan()
    {
        Assert.Equal(Events, Read(_path, null, null).Count);

        // Windows deliberately placed to land inside blocks, on block edges and outside.
        (int From, int To)[] windows =
        [
            (0, 0), (0, 1), (0, Events - 1),
            (1, 1), (7, 9), (100, 100), (100, 900),
            (Events - 2, Events - 1), (Events - 1, Events - 1),
            (1500, 1500), (1999, 2001), (2000, 2000),
        ];

        foreach (var (from, to) in windows)
        {
            long f = TickAt(from), t = TickAt(to);
            Assert.Equal(Expected(f, t), Windowed(f, t));
        }
    }

    /// <summary>
    /// The dangerous boundary: an event sitting exactly on `from` may live in a block whose
    /// SUCCESSOR starts at `from`. Skipping on `<=` instead of `<` would lose it.
    /// </summary>
    [Fact]
    public void EventExactlyOnTheWindowEdgeSurvives()
    {
        for (int i = 0; i < Events; i += 137)
        {
            long at = TickAt(i);
            var single = Windowed(at, at);
            Assert.True(single.Count == 1, $"event at index {i} lost by the zone map (got {single.Count})");
        }
    }

    [Fact]
    public void EmptyWindowsReturnNothing()
    {
        Assert.Empty(Windowed(_base - TimeSpan.TicksPerDay, _base - TimeSpan.TicksPerSecond));
        Assert.Empty(Windowed(TickAt(Events) + TimeSpan.TicksPerSecond, TickAt(Events) + TimeSpan.TicksPerDay));
    }

    [Fact]
    public void ReversedReadIsWindowedToo()
    {
        long f = TickAt(300), t = TickAt(700);
        var desc = Read(_path, new DateTimeOffset(f, TimeSpan.Zero), new DateTimeOffset(t, TimeSpan.Zero), reversed: true);
        Assert.Equal(401, desc.Count);
        Assert.Equal(Expected(f, t), desc.Select(e => e.Id.RawValue).OrderBy(x => x).ToList());
        // Newest-first ordering preserved.
        Assert.True(desc[0].Timestamp >= desc[^1].Timestamp);
    }

    /// <summary>The point of the exercise: a narrow window must not decompress the file.</summary>
    [Fact]
    public void NarrowWindowDecompressesFarLessThanAFullScan()
    {
        long f = TickAt(10), t = TickAt(20);
        var fromDto = new DateTimeOffset(f, TimeSpan.Zero);
        var toDto   = new DateTimeOffset(t, TimeSpan.Zero);

        Read(_path, null, null);                 // warm
        Read(_path, fromDto, toDto);

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        Read(_path, null, null);
        long full = GC.GetAllocatedBytesForCurrentThread() - b0;

        long b1 = GC.GetAllocatedBytesForCurrentThread();
        Read(_path, fromDto, toDto);
        long narrow = GC.GetAllocatedBytesForCurrentThread() - b1;

        Assert.True(narrow * 10 < full,
            $"zone map did not prune: narrow window allocated {narrow} B vs {full} B for a full scan");
    }

    private string BuildSegment()
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(Events + 1, (long)Events * 512 + (8L << 20));

        int tmplIdx = pool.Intern("event {n}");
        string tmpl = pool.Get(tmplIdx);
        int svcIdx  = pool.Intern("Svc.Zone");

        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < Events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("n");   w.Write((long)i);
            w.Write("pad"); w.Write(new string('x', 200));   // push block count up
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = TickAt(i),
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        hot.Freeze();

        string path = Path.Combine(_dir, "0-1-0-0.seg");
        using (var sw = new SegmentWriter(path))
        {
            sw.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
            sw.Finalise(new NodeId(0), new SegmentId(1UL));
        }
        return path;
    }
}
