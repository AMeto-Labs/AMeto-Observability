using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using MessagePack;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// v7 cuts a segment into INDEX GROUPS so index-build memory is bounded by a group rather
/// than by the file. That moves the index sections into the middle of the file, between
/// blocks — the one change that could make a segment unreadable or silently truncate it —
/// so every event is compared field by field against what was written, and the group
/// directory is checked to actually PARTITION the file rather than merely describe part of it.
/// </summary>
public sealed class SegmentIndexGroupTests : IDisposable
{
    private const int  Events      = 3_000;
    private const long GroupBudget = 256 * 1024;   // forces many groups out of a small file

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-groups-" + Guid.NewGuid().ToString("N"));
    private readonly long   _base = new DateTimeOffset(2026, 7, 30, 9, 0, 0, TimeSpan.Zero).UtcTicks;

    public SegmentIndexGroupTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private long TickAt(int i) => _base + i * TimeSpan.TicksPerSecond;

    // ── Round trip ────────────────────────────────────────────────────────────

    [Fact]
    public void EveryEventSurvivesAMultiGroupSegmentByteForByte()
    {
        var (path, expected, groupCount) = BuildSegment(GroupBudget);
        Assert.True(groupCount > 3, $"test is meaningless with {groupCount} group(s)");

        var read = ReadAll(path);
        Assert.Equal(expected.Count, read.Count);

        for (int i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = read[i];
            Assert.Equal(e.Id,       a.Id.RawValue);
            Assert.Equal(e.Ticks,    a.Timestamp.UtcTicks);
            Assert.Equal(e.Level,    a.Level);
            Assert.Equal(e.Template, a.MessageTemplate);
            Assert.Equal(e.Service,  a.ServiceName);
            Assert.True(e.Props.AsSpan().SequenceEqual(a.RawProperties.Span),
                $"properties differ at ordinal {i}");
        }
    }

    /// <summary>
    /// The same events written as one group and as many must produce the same rows in the
    /// same order — grouping is an index-layout change, not a data change.
    /// </summary>
    [Fact]
    public void GroupingDoesNotChangeWhatTheSegmentContains()
    {
        var (grouped, _, groupCount) = BuildSegment(GroupBudget, "grouped.seg");
        var (single,  _, singleCount) = BuildSegment(long.MaxValue, "single.seg");

        Assert.True(groupCount > 3);
        Assert.Equal(1, singleCount);

        var a = ReadAll(grouped);
        var b = ReadAll(single);
        Assert.Equal(b.Select(e => e.Id.RawValue).ToArray(), a.Select(e => e.Id.RawValue).ToArray());
        Assert.Equal(b.Select(e => e.Timestamp.UtcTicks).ToArray(), a.Select(e => e.Timestamp.UtcTicks).ToArray());
    }

    // ── Group directory ───────────────────────────────────────────────────────

    /// <summary>
    /// The directory must partition the file: blocks and ordinals contiguous from 0, counts
    /// summing to the header's, every group carrying its own three sections. A gap here is
    /// events that no query can ever reach.
    /// </summary>
    [Fact]
    public void TheDirectoryPartitionsTheFile()
    {
        var (path, expected, _) = BuildSegment(GroupBudget);
        using var reader = SegmentReader.Open(path);
        var groups = reader.Groups;

        int  nextBlock   = 0;
        uint nextOrdinal = 0;
        uint total       = 0;
        for (int g = 0; g < groups.Length; g++)
        {
            var grp = groups[g];
            Assert.Equal(nextBlock,   grp.FirstBlock);
            Assert.Equal(nextOrdinal, grp.FirstOrdinal);
            Assert.True(grp.BlockCount > 0, $"group {g} owns no blocks");
            Assert.True(grp.EventCount > 0, $"group {g} owns no events");

            // Sections are per group and distinct — group 0's bloom must not be group 1's.
            Assert.True(grp.InvertedOffset > 0, $"group {g} has no inverted section");
            Assert.True(grp.TrigramOffset  > grp.InvertedOffset);
            Assert.True(grp.BloomOffset    > grp.TrigramOffset);

            nextBlock   += grp.BlockCount;
            nextOrdinal += grp.EventCount;
            total       += grp.EventCount;
        }

        Assert.Equal(reader.Info.EventCount, total);
        Assert.Equal((uint)expected.Count,   total);
    }

    /// <summary>
    /// A group's MinTs/MaxTs must BOUND its rows exactly — the prefilter drops a group's
    /// index sections on those two numbers alone, so a bound that is too tight loses rows.
    /// </summary>
    [Fact]
    public void GroupTimeBoundsContainExactlyTheGroupsEvents()
    {
        var (path, expected, _) = BuildSegment(GroupBudget);
        using var reader = SegmentReader.Open(path);

        var groups = reader.Groups;
        for (int g = 0; g < groups.Length; g++)
        {
            var grp = groups[g];
            long lo = long.MaxValue, hi = long.MinValue;
            for (uint o = grp.FirstOrdinal; o < grp.FirstOrdinal + grp.EventCount; o++)
            {
                long ts = expected[(int)o].Ticks;
                if (ts < lo) lo = ts;
                if (ts > hi) hi = ts;
            }
            Assert.Equal(lo, grp.MinTs);
            Assert.Equal(hi, grp.MaxTs);
        }
    }

    // ── Windowed reads over a multi-group segment ─────────────────────────────

    [Fact]
    public void WindowedReadsMatchABruteForceScan()
    {
        var (path, _, _) = BuildSegment(GroupBudget);
        var all = ReadAll(path);

        (int From, int To)[] windows =
        [
            (0, 0), (0, 1), (0, Events - 1), (7, 9), (100, 900),
            (1499, 1501), (2000, 2000), (Events - 2, Events - 1), (Events - 1, Events - 1),
        ];

        foreach (var (from, to) in windows)
        {
            long f = TickAt(from), t = TickAt(to);
            var expect = all.Where(e => e.Timestamp.UtcTicks >= f && e.Timestamp.UtcTicks <= t)
                            .Select(e => e.Id.RawValue).OrderBy(x => x).ToList();
            var got = Read(path, new DateTimeOffset(f, TimeSpan.Zero), new DateTimeOffset(t, TimeSpan.Zero))
                            .Select(e => e.Id.RawValue).OrderBy(x => x).ToList();
            Assert.Equal(expect, got);
        }
    }

    /// <summary>Candidate ordinals are FILE-global, so they must resolve across group edges.</summary>
    [Fact]
    public void CandidateOrdinalsResolveAcrossGroupBoundaries()
    {
        var (path, expected, _) = BuildSegment(GroupBudget);
        using var reader = SegmentReader.Open(path);

        // One ordinal on each side of every group boundary, plus the file's first and last.
        var wanted = new List<uint> { 0, (uint)(expected.Count - 1) };
        var groups = reader.Groups;
        for (int g = 1; g < groups.Length; g++)
        {
            wanted.Add(groups[g].FirstOrdinal - 1);
            wanted.Add(groups[g].FirstOrdinal);
        }
        var cands = wanted.Distinct().OrderBy(x => x).ToArray();

        var got = Drain(reader.ReadEventsAsync(cands, null, null, false, default));
        Assert.Equal(cands.Length, got.Count);
        for (int i = 0; i < cands.Length; i++)
            Assert.Equal(expected[(int)cands[i]].Id, got[i].Id.RawValue);
    }

    // ── Backward compatibility ────────────────────────────────────────────────

    /// <summary>
    /// v4-v6 files must keep working — the user allowed deleting old segments, but silently
    /// making an upgrade unreadable is a different thing from choosing to wipe.
    ///
    /// <para>The v6 file is derived from a single-group v7 one: stamp version 6 into the
    /// header and put the group's three section offsets back in the footer slots v7 reuses.
    /// Every offset in the format is explicit, so the orphaned group directory sitting
    /// between the bloom section and the block index is simply never addressed — the result
    /// is byte-for-byte what a v6 writer produced apart from those unreferenced bytes.</para>
    /// </summary>
    [Fact]
    public void AV6SegmentIsStillReadableAsOneImplicitGroup()
    {
        var (v7Path, expected, groupCount) = BuildSegment(long.MaxValue, "as-v6.seg");
        Assert.Equal(1, groupCount);

        long invOff, triOff, bloomOff;
        using (var reader = SegmentReader.Open(v7Path))
        {
            var g = reader.Groups[0];
            invOff = g.InvertedOffset; triOff = g.TrigramOffset; bloomOff = g.BloomOffset;
        }

        string v6Path = Path.Combine(_dir, "0-9-0-0.seg");
        File.Copy(v7Path, v6Path);
        using (var fs = new FileStream(v6Path, FileMode.Open, FileAccess.ReadWrite))
        {
            Span<byte> ver = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(ver, 6);
            fs.Seek(4, SeekOrigin.Begin);
            fs.Write(ver);

            Span<byte> slots = stackalloc byte[24];
            BinaryPrimitives.WriteInt64LittleEndian(slots,            invOff);
            BinaryPrimitives.WriteInt64LittleEndian(slots.Slice(8),   triOff);
            BinaryPrimitives.WriteInt64LittleEndian(slots.Slice(16),  bloomOff);
            fs.Seek(-44, SeekOrigin.End);
            fs.Write(slots);
        }

        using var v6 = SegmentReader.Open(v6Path);
        Assert.Equal(1, v6.Groups.Length);
        Assert.Equal(0u, v6.Groups[0].FirstOrdinal);
        Assert.Equal((uint)expected.Count, v6.Groups[0].EventCount);
        Assert.Equal(invOff, v6.Groups[0].InvertedOffset);
        Assert.Equal(v6.Info.MinTimestampTicks, v6.Groups[0].MinTs);

        var read = ReadAll(v6Path);
        Assert.Equal(expected.Count, read.Count);
        Assert.Equal(expected[0].Id,   read[0].Id.RawValue);
        Assert.Equal(expected[^1].Id,  read[^1].Id.RawValue);
        Assert.NotEmpty(v6.ReadInvertedIndexBytes());
    }

    // ── Fixtures ──────────────────────────────────────────────────────────────

    private readonly record struct Row(ulong Id, long Ticks, LogLevel Level, string Template, string Service, byte[] Props);

    /// <summary>
    /// Writes a segment through the group path. The index sections are stand-in bytes that
    /// identify their group — this file exercises the ENVELOPE (offsets, directory, block
    /// ownership); the real index build over groups is covered in Ameto.Perf, which can
    /// reference the indexing layer.
    /// </summary>
    private (string Path, List<Row> Rows, int Groups) BuildSegment(long groupBudget, string name = "0-1-0-0.seg")
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(Events + 1, (long)Events * 640 + (8L << 20));

        int tmplIdx = pool.Intern("group event {n}");
        string tmpl = pool.Get(tmplIdx);
        int svcIdx  = pool.Intern("Svc.Group");

        var rows = new List<Row>(Events);
        var buf  = new ArrayBufferWriter<byte>(512);
        for (int i = 0; i < Events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("n");   w.Write((long)i);
            w.Write("pad"); w.Write(new string((char)('a' + i % 26), 300));
            w.Flush();
            var props = buf.WrittenSpan.ToArray();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = TickAt(i),
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(h, props, tmpl));
            rows.Add(new Row(h.Id, h.TimestampUtcTicks, h.Level, tmpl, "Svc.Group", props));
        }
        hot.Freeze();

        rows.Sort(static (a, b) => a.Ticks != b.Ticks ? a.Ticks.CompareTo(b.Ticks) : a.Id.CompareTo(b.Id));

        string path = Path.Combine(_dir, name);
        int groups  = 0;
        using (var sw = new SegmentWriter(path, groupBudget))
        {
            sw.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot), (_, _) => new StubSink(() => ++groups));
            sw.Finalise(new NodeId(0), new SegmentId(1UL));
        }
        return (path, rows, groups);
    }

    /// <summary>
    /// A group's bloom filter is allocated from a forecast made at its FIRST EVENT and can never
    /// be resized, so the forecast has to survive an unrepresentative first row. Sizing it from
    /// that row's own cost let one fat event shrink the whole file's filter by the ratio of its
    /// size to the rest — measured elsewhere at 36× on a 20,000-event segment, which saturates
    /// the filter and leaves the prefilter rejecting nothing.
    /// </summary>
    [Fact]
    public void AFatFirstRowDoesNotShrinkTheGroupsEventForecast()
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(Events + 2, (long)Events * 640 + (16L << 20));
        int    tmplIdx = pool.Intern("group event {n}");
        string tmpl    = pool.Get(tmplIdx);

        for (int i = 0; i <= Events; i++)
        {
            var buf = new ArrayBufferWriter<byte>(512);
            var w   = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("n");   w.Write((long)i);
            w.Write("pad"); w.Write(new string('x', i == 0 ? 120 * 1024 : 300));   // one fat row, first
            w.Flush();
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = TickAt(i),
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
            }, buf.WrittenSpan, tmpl));
        }
        hot.Freeze();

        var estimates = new List<int>();
        using (var sw = new SegmentWriter(Path.Combine(_dir, "fat-first.seg"),
                                          SegmentWriter.DefaultGroupPayloadBudgetBytes))
        {
            sw.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot),
                           (n, _) => { estimates.Add(n); return new StubSink(() => 1); });
            sw.Finalise(new NodeId(0), new SegmentId(1UL));
        }

        // One group at the production budget, forecast at the source's own remaining count
        // (which excludes the event already in the writer's hand, hence Events and not Events+1)
        // rather than at what one 120 KB row suggests the budget affords — that would be 546.
        Assert.Single(estimates);
        Assert.True(estimates[0] >= Events,
            $"forecast {estimates[0]} for a {Events + 1}-event group — the fat first row shrank it");
    }

    /// <summary>
    /// Stand-in index sink: records the group's ordinal range from the events actually pushed
    /// into it, and emits sections sized differently per group so a mixed-up section offset
    /// shows up as a wrong length rather than as silence.
    /// </summary>
    private sealed class StubSink(Func<int> onSeal) : ISegmentIndexSink
    {
        private uint _first = uint.MaxValue;
        private int  _count;

        public void Add(uint fileOrdinal, in SegmentEventRef ev)
        {
            if (fileOrdinal < _first) _first = fileOrdinal;
            _count++;
        }

        /// <summary>No bloom here, so nothing is measured and nothing is bounded — the writer
        /// falls back to its assumption and seals on payload alone, which is what makes the
        /// forecast and geometry tests below read the same as before.</summary>
        public long BloomTermsAdded   => 0;
        public long BloomTermCapacity => 0;

        public (byte[] Inverted, byte[] Trigram, byte[] Bloom) Serialise()
        {
            int seq = onSeal();
            return (Encoding.UTF8.GetBytes($"inv:{_first}:{_count}"),
                    Encoding.UTF8.GetBytes($"tri:{_first}:{_count}:{new string('t', seq)}"),
                    Encoding.UTF8.GetBytes($"bloom:{_first}:{_count}"));
        }

        public void Dispose() { }
    }

    private static List<LogEvent> ReadAll(string path) => Read(path, null, null);

    private static List<LogEvent> Read(string path, DateTimeOffset? from, DateTimeOffset? to)
    {
        using var reader = SegmentReader.Open(path);
        return Drain(reader.ReadEventsAsync(null, from, to, false, default));
    }

    private static List<LogEvent> Drain(IAsyncEnumerable<LogEvent> src)
    {
        var list = new List<LogEvent>();
        var e = src.GetAsyncEnumerator();
        try { while (e.MoveNextAsync().GetAwaiter().GetResult()) list.Add(e.Current); }
        finally { e.DisposeAsync().GetAwaiter().GetResult(); }
        return list;
    }
}
