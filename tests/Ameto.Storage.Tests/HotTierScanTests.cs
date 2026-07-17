using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// <see cref="HotTierScan.ReadSorted"/> must be sequence-equal to the naive oracle
/// (materialise everything, filter, LINQ-sort) for every combination of direction,
/// window, cursor and level set — including duplicate timestamps (id tiebreaker)
/// and multiple tiers (frozen + current).
/// </summary>
public sealed class HotTierScanTests : IDisposable
{
    private const int EventsPerTier = 700;

    private readonly StringInternPool _pool = new();
    private readonly HotTierSegment   _frozenA;
    private readonly HotTierSegment   _frozenB;
    private readonly HotTierSegment   _current;
    private readonly long             _baseTicks = DateTimeOffset.UtcNow.UtcTicks;

    public HotTierScanTests()
    {
        var rng = new Random(7);
        _frozenA = BuildTier(rng, tierNo: 0);
        _frozenB = BuildTier(rng, tierNo: 1);
        _current = BuildTier(rng, tierNo: 2);
    }

    public void Dispose()
    {
        _frozenA.Dispose();
        _frozenB.Dispose();
        _current.Dispose();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MatchesOracle_NoFilters(bool forward)
        => AssertMatchesOracle(long.MinValue, long.MaxValue, null, null, forward, null);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MatchesOracle_TimeWindow(bool forward)
        => AssertMatchesOracle(
            _baseTicks + 200 * TimeSpan.TicksPerMillisecond,
            _baseTicks + 900 * TimeSpan.TicksPerMillisecond,
            null, null, forward, null);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MatchesOracle_LevelSubset(bool forward)
        => AssertMatchesOracle(long.MinValue, long.MaxValue, null, null, forward,
            new HashSet<Ameto.Core.LogLevel> { Ameto.Core.LogLevel.Error, Ameto.Core.LogLevel.Warning });

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MatchesOracle_Cursor(bool forward)
    {
        // Cursor in the middle of the range, with an id tiebreaker.
        long   afterTs = _baseTicks + 500 * TimeSpan.TicksPerMillisecond;
        ulong  afterId = new EventId(0u, 350u).RawValue;
        AssertMatchesOracle(long.MinValue, long.MaxValue, afterTs, afterId, forward, null);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MatchesOracle_Everything(bool forward)
        => AssertMatchesOracle(
            _baseTicks + 100 * TimeSpan.TicksPerMillisecond,
            _baseTicks + 950 * TimeSpan.TicksPerMillisecond,
            _baseTicks + 400 * TimeSpan.TicksPerMillisecond,
            new EventId(0u, 123u).RawValue,
            forward,
            new HashSet<Ameto.Core.LogLevel> { Ameto.Core.LogLevel.Information, Ameto.Core.LogLevel.Debug });

    private void AssertMatchesOracle(
        long fromTicks, long toTicks, long? afterTs, ulong? afterId, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels)
    {
        var frozen = new[] { _frozenA, _frozenB };

        var got = HotTierScan
            .ReadSorted(_current, frozen, _pool, fromTicks, toTicks, afterTs, afterId, forward, levels)
            .Select(e => e.Id.RawValue)
            .ToList();

        var all = frozen.SelectMany(t => t.ReadAll(_pool)).Concat(_current.ReadAll(_pool));
        var filtered = all.Where(e =>
        {
            long ts = e.Timestamp.UtcTicks;
            if (ts < fromTicks || ts > toTicks) return false;
            if (levels is not null && !levels.Contains(e.Level)) return false;
            return QueryCursor.After(ts, e.Id.RawValue, afterTs, afterId, forward);
        });
        var expected = (forward
                ? filtered.OrderBy(e => e.Timestamp).ThenBy(e => e.Id)
                : filtered.OrderByDescending(e => e.Timestamp).ThenByDescending(e => e.Id))
            .Select(e => e.Id.RawValue)
            .ToList();

        Assert.NotEmpty(expected);          // guard against a vacuous test
        Assert.Equal(expected, got);
    }

    /// <summary>Random timestamps over ~1s with many exact duplicates (id must tiebreak).</summary>
    private HotTierSegment BuildTier(Random rng, int tierNo)
    {
        var tier = new HotTierSegment(EventsPerTier + 1, EventsPerTier * 512 + 1024 * 1024);

        int    tmplIdx = _pool.Intern("evt");
        string tmpl    = _pool.Get(tmplIdx);
        var    buf     = new ArrayBufferWriter<byte>(64);

        for (int i = 0; i < EventsPerTier; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("n"); w.Write((long)i);
            w.Flush();

            // 0..999 ms — duplicates guaranteed (700 events × 3 tiers over 1000 slots).
            long ts = _baseTicks + rng.Next(0, 1000) * TimeSpan.TicksPerMillisecond;
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)(tierNo * EventsPerTier + i)).RawValue,
                TimestampUtcTicks        = ts,
                Level                    = (Ameto.Core.LogLevel)(rng.Next(0, 6)),
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = -1,
            };
            Assert.True(tier.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        tier.Freeze();
        return tier;
    }
}
