using System.Buffers;
using MessagePack;
using Ameto.Core;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// The file order — <see cref="SegmentWriter.ComputeSortOrder"/> and the level split that
/// partitions it — decides the BYTES of every segment, so it is pinned here rather than left to
/// whatever an introsort happens to do.
///
/// <para>The order is ascending (timestamp, id) and must not depend on the order the events
/// reached the tier: two tiers holding the same events, filled in different arrival orders, have
/// to produce the same file down to the byte. The writer takes an identity fast path when the
/// tier is already ascending and sorts otherwise, and this is what says those two routes agree.</para>
///
/// <para>The level split hands the writer POOLED arrays, which are longer than the level they
/// hold. Its count is therefore part of its result, and a consumer that ignores one stages the
/// rent's tail — tier indices left over from an earlier flush — as events.</para>
/// </summary>
public sealed class SegmentSortOrderTests
{
    private readonly record struct Row(uint Seq, long Ticks, LogLevel Level);

    private static byte[] Props(uint seq)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("n");    w.Write((long)seq);
        w.Write("cust"); w.Write("cust-" + (seq % 37));
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>Fills a tier with <paramref name="rows"/> in exactly the order given.</summary>
    private static HotTierSegment BuildTier(IReadOnlyList<Row> rows, StringInternPool pool)
    {
        var hot = new HotTierSegment(rows.Count + 1, (long)rows.Count * 512 + 1024 * 1024);
        int tmplIdx = pool.Intern("evt {n} for {cust}");
        string tmpl = pool.Get(tmplIdx);
        int svcIdx  = pool.Intern("Wallet.API");

        foreach (var r in rows)
        {
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, r.Seq).RawValue,
                TimestampUtcTicks        = r.Ticks,
                Level                    = r.Level,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
                TraceIdHi                = r.Seq,
                TraceIdLo                = r.Seq + 1,
                SpanId                   = r.Seq + 2,
            };
            Assert.True(hot.TryWrite(h, Props(r.Seq), tmpl));
        }
        hot.Freeze();
        return hot;
    }

    private static List<Row> MakeRows(int n, int seed, bool tiedTimestamps = false)
    {
        long baseTicks = new DateTimeOffset(2026, 9, 11, 8, 0, 0, TimeSpan.Zero).UtcTicks;
        var rng  = new Random(seed);
        var rows = new List<Row>(n);
        for (int i = 0; i < n; i++)
            rows.Add(new Row(
                (uint)i,
                // Ties are the interesting case for the tie-break; otherwise every event gets
                // its own tick so the expected order is unambiguous.
                tiedTimestamps ? baseTicks + (i / 8) * 10 : baseTicks + i * 10,
                (LogLevel)rng.Next(0, 6)));
        return rows;
    }

    private static byte[] WriteSegment(IReadOnlyList<Row> rows, StringInternPool pool, string path)
    {
        using (var hot = BuildTier(rows, pool))
        using (var w = new SegmentWriter(path))
        {
            w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
            w.Finalise(new NodeId(7), new SegmentId(42UL));
        }
        return File.ReadAllBytes(path);
    }

    /// <summary>
    /// The same events, reaching the tier in a different order, must produce the same file.
    /// Ascending arrival takes the writer's identity fast path; shuffled arrival takes the sort.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ShuffledArrival_WritesByteIdenticalSegment(bool tiedTimestamps)
    {
        var ordered  = MakeRows(4_000, seed: 11, tiedTimestamps);
        var shuffled = new List<Row>(ordered);
        var rng = new Random(3);
        for (int i = shuffled.Count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (shuffled[i], shuffled[j]) = (shuffled[j], shuffled[i]);
        }
        Assert.NotEqual(ordered, shuffled);           // the shuffle really did something

        string a = Path.Combine(Path.GetTempPath(), $"Ameto-sortorder-{Guid.NewGuid():N}.seg");
        string b = Path.Combine(Path.GetTempPath(), $"Ameto-sortorder-{Guid.NewGuid():N}.seg");
        try
        {
            var pool = new StringInternPool();
            byte[] fromOrdered  = WriteSegment(ordered,  pool, a);
            byte[] fromShuffled = WriteSegment(shuffled, pool, b);
            Assert.Equal(fromOrdered.Length, fromShuffled.Length);
            Assert.True(fromOrdered.AsSpan().SequenceEqual(fromShuffled),
                        "arrival order leaked into the segment bytes");
        }
        finally { File.Delete(a); File.Delete(b); }
    }

    /// <summary>Ascending (timestamp, id), whatever order the tier was filled in.</summary>
    [Fact]
    public void SortOrder_IsAscendingByTimestampThenId()
    {
        var rows = MakeRows(2_000, seed: 5, tiedTimestamps: true);
        var rng = new Random(9);
        for (int i = rows.Count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (rows[i], rows[j]) = (rows[j], rows[i]);
        }

        var pool = new StringInternPool();
        using var hot = BuildTier(rows, pool);
        int[] order = SegmentWriter.ComputeSortOrder(hot);

        Assert.Equal(hot.Count, order.Length);
        Assert.Equal(hot.Count, order.Distinct().Count());
        for (int k = 1; k < order.Length; k++)
        {
            ref var prev = ref hot.GetHeader(order[k - 1]);
            ref var cur  = ref hot.GetHeader(order[k]);
            bool ascending = cur.TimestampUtcTicks > prev.TimestampUtcTicks ||
                             (cur.TimestampUtcTicks == prev.TimestampUtcTicks && cur.Id > prev.Id);
            Assert.True(ascending, $"out of order at {k}");
        }
    }

    /// <summary>
    /// The pooled split must partition the order exactly as the obvious per-level list does —
    /// same events, same levels, same relative order — and report each level's length, because
    /// the array it returns is longer than that.
    /// </summary>
    [Fact]
    public void SplitByLevel_PartitionsExactly_AndReportsItsOwnLength()
    {
        var rows = MakeRows(5_000, seed: 21);
        var pool = new StringInternPool();
        using var hot = BuildTier(rows, pool);
        int[] order = SegmentWriter.ComputeSortOrder(hot);

        var reference = new List<int>[6];
        for (int oi = 0; oi < order.Length; oi++)
        {
            int lvl = (int)hot.GetHeader(order[oi]).Level;
            (reference[lvl] ??= new List<int>()).Add(order[oi]);
        }

        var perLevel = new int[6][];
        var counts   = new int[6];
        StorageEngine.SplitOrderByLevel(hot, order, perLevel, counts);
        try
        {
            Assert.Equal(order.Length, counts.Sum());
            for (int lvl = 0; lvl < 6; lvl++)
            {
                var expected = reference[lvl] ?? new List<int>();
                Assert.Equal(expected.Count, counts[lvl]);
                if (expected.Count == 0) { Assert.Null(perLevel[lvl]); continue; }

                // Rented ⇒ longer than the level. Only the first counts[lvl] entries are ours.
                Assert.True(perLevel[lvl]!.Length >= counts[lvl]);
                Assert.Equal(expected, perLevel[lvl]!.Take(counts[lvl]).ToList());
                foreach (int i in perLevel[lvl]!.Take(counts[lvl]))
                    Assert.Equal(lvl, (int)hot.GetHeader(i).Level);
            }
        }
        finally
        {
            for (int lvl = 0; lvl < 6; lvl++)
                if (perLevel[lvl] is { } arr) ArrayPool<int>.Shared.Return(arr);
        }
    }

    /// <summary>
    /// A rented array carries an earlier flush's indices past its own end. Handing one to the
    /// writer without its count writes those stale rows into the segment, so this dirties the
    /// pool's arrays first and then checks that a level's file holds exactly its own events.
    /// </summary>
    [Fact]
    public void SplitByLevel_DoesNotLeakThePoolsTailIntoASegment()
    {
        // Dirty the buckets the split will rent from with a recognisable pattern.
        foreach (int size in new[] { 1024, 2048, 4096, 8192 })
        {
            var dirty = ArrayPool<int>.Shared.Rent(size);
            Array.Fill(dirty, 0x0BAD_F00D);
            ArrayPool<int>.Shared.Return(dirty);
        }

        var rows = MakeRows(600, seed: 33);
        var pool = new StringInternPool();
        using var hot = BuildTier(rows, pool);
        int[] order = SegmentWriter.ComputeSortOrder(hot);

        var perLevel = new int[6][];
        var counts   = new int[6];
        StorageEngine.SplitOrderByLevel(hot, order, perLevel, counts);
        try
        {
            for (int lvl = 0; lvl < 6; lvl++)
            {
                if (counts[lvl] == 0) continue;
                string path = Path.Combine(Path.GetTempPath(), $"Ameto-sortorder-{Guid.NewGuid():N}.seg");
                try
                {
                    using (var w = new SegmentWriter(path))
                    {
                        // Exactly how the flush drives it: the array plus how much of it is real.
                        w.WriteEvents(new HotTierEventSource(hot, pool, perLevel[lvl], 0, counts[lvl]), null);
                        var info = w.Finalise(new NodeId(1), new SegmentId((ulong)lvl));
                        Assert.Equal((uint)counts[lvl], info.EventCount);
                        Assert.Equal((LogLevel)lvl, info.MinLevel);
                    }
                }
                finally { File.Delete(path); }
            }
        }
        finally
        {
            for (int lvl = 0; lvl < 6; lvl++)
                if (perLevel[lvl] is { } arr) ArrayPool<int>.Shared.Return(arr);
        }
    }
}
