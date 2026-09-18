using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// A level-split flush partitions the tier's sort order into POOLED arrays, and a pooled array
/// is longer than the level it holds. Its count therefore travels with it —
/// <c>BuildSegmentPath(segId, hot, subset, n)</c>, <c>FlushToColdAsync(…, subset, n)</c>,
/// <c>new HotTierEventSource(hot, pool, order, 0, count)</c> — and a consumer that drops it reads
/// the tail of the rent, which is whatever tier indices an EARLIER flush left in that array.
///
/// <para>This exercises those three call sites through <see cref="StorageEngine"/> itself rather
/// than by hand-wiring the writer, because the hazard is precisely that one of the three forgets.
/// Two things are asserted, and they fail differently: the segment's EVENT COUNT catches a tail
/// being written at all, and the segment's FILE NAME catches it more quietly — retention reads
/// the expiry deadline out of the max timestamp in that name (see <c>SegmentInfo.IsExpired</c>),
/// so a stale index from another flush silently moves when a level's events are deleted.</para>
///
/// <para>The pool is dirtied first, with index 0 — a VALID tier index belonging to the earliest
/// event of the tier, which is in a different level than most. A poison that is out of range
/// would only ever produce an exception; one that is in range produces a plausible, wrong file,
/// which is the failure worth catching. A freshly allocated array is all zeros and therefore
/// carries the same poison, so this holds whether or not the dirtied buffers are the ones the
/// flush thread is handed.</para>
/// </summary>
public sealed class LevelSplitPooledOrderTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-poolorder-" + Guid.NewGuid().ToString("N"));
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
    /// Per-level counts, chosen so that NONE is a power of two: <c>ArrayPool</c> buckets are
    /// powers of two, so a level whose count is one would be handed an exactly-full array and
    /// would pass this test while the bug was present.
    /// </summary>
    private static readonly (LogLevel Level, int Count)[] Plan =
    [
        (LogLevel.Verbose,      7),
        (LogLevel.Debug,       13),
        (LogLevel.Information, 29),
        (LogLevel.Warning,      5),
        (LogLevel.Error,       11),
        (LogLevel.Fatal,        3),
    ];

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    [Fact]
    public async Task ALevelSegmentHoldsItsOwnEventsAndIsNamedAfterThem()
    {
        foreach (var (_, count) in Plan)
            Assert.NotEqual(0, count & (count - 1));         // not a power of two — see Plan

        // Hand the shared pool arrays full of tier index 0, then give them back, so the split's
        // rents come back dirty. Sizes cover every bucket the plan's counts can land in.
        foreach (int size in new[] { 16, 32, 64, 128, 256 })
        {
            var dirty = ArrayPool<int>.Shared.Rent(size);
            Array.Fill(dirty, 0);
            ArrayPool<int>.Shared.Return(dirty);
        }

        // Interleave the levels so that no level's events are contiguous and every level's
        // timestamp range is a sub-range of the tier's — a stale index lands outside it.
        long baseTicks = new DateTimeOffset(2026, 9, 14, 6, 0, 0, TimeSpan.Zero).UtcTicks;
        var remaining  = Plan.ToDictionary(p => p.Level, p => p.Count);
        var expected   = new Dictionary<LogLevel, (int Count, long MinTs, long MaxTs)>();

        int n = 0;
        var rng = new Random(4);
        while (remaining.Values.Any(c => c > 0))
        {
            var live  = remaining.Where(kv => kv.Value > 0).Select(kv => kv.Key).ToArray();
            var level = live[rng.Next(live.Length)];
            remaining[level]--;

            long ticks = baseTicks + n * TimeSpan.TicksPerMillisecond;
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)n).RawValue,
                TimestampUtcTicks        = ticks,
                Level                    = level,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc.A"),
            };
            Assert.True(_engine.TryWrite(h, Props(n)));

            expected[level] = expected.TryGetValue(level, out var e)
                ? (e.Count + 1, e.MinTs, ticks)
                : (1, ticks, ticks);
            n++;
        }

        await _engine.FlushHotTierAsync();

        var segs = _engine.ListSegments();
        Assert.Equal(Plan.Length, segs.Count);

        foreach (var seg in segs)
        {
            var (count, minTs, maxTs) = expected[seg.MinLevel];

            // 1. The writer was told how much of the rented array was real.
            Assert.Equal((uint)count, seg.EventCount);

            // 2. So was the namer. "{node}-{segId}-{minTs}-{maxTs}.seg" — and those two ticks
            //    are what retention expires the file on, so a stale index moves a deadline.
            string[] parts = Path.GetFileNameWithoutExtension(seg.FilePath).Split('-');
            Assert.Equal(4, parts.Length);
            Assert.Equal(minTs, long.Parse(parts[2]));
            Assert.Equal(maxTs, long.Parse(parts[3]));

            // 3. And the rows really are this level's, in order — a tail of index 0 would put
            //    the tier's first event (some other level) into this file.
            var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
            using var r = SegmentReader.Open(seg.FilePath);
            var rows = r.ReadAllRaw(dedup);
            Assert.Equal(count, rows.Count);
            long prev = long.MinValue;
            foreach (var ev in rows)
            {
                Assert.Equal((byte)seg.MinLevel, ev.Level);
                Assert.InRange(ev.TsTicks, minTs, maxTs);
                Assert.True(ev.TsTicks > prev, "level subsequence is out of order");
                prev = ev.TsTicks;
            }
        }
    }
}
