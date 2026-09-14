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
    /// <param name="Seq">Distinguishes the row: its payload, trace id, and tier position.</param>
    /// <param name="Key">What becomes the EventId. Normally = Seq; the tie tests deliberately
    /// repeat it so that two rows share a whole (timestamp, id) sort key.</param>
    private readonly record struct Row(uint Seq, uint Key, long Ticks, LogLevel Level);

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
                Id                       = new EventId(0u, r.Key).RawValue,
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
                (uint)i,                       // distinct id ⇒ the sort key is still unique
                // Equal TIMESTAMPS exercise the id half of the comparison. A whole-key tie is a
                // different case and has its own test — see TiedKeys_KeepTierOrder_OnBothRoutes.
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
    /// Two events sharing a WHOLE sort key — same timestamp AND same id — come out in tier
    /// order, whichever route produced the order.
    ///
    /// <para>THE ENGINE CANNOT PRODUCE THIS INPUT. <c>StorageEngine.TryWrite</c> stamps every
    /// event from a generator that clamps to <c>prevMs+1</c>, so ids are strictly monotonic per
    /// node and no two events of one tier can tie. <see cref="SegmentWriter.ComputeSortOrder"/>
    /// is nevertheless a public function over a caller-supplied tier, and it has two routes: an
    /// identity fast path for an already-ascending tier and an introsort for everything else.
    /// An introsort is NOT stable, so without the comparer's final tie-break on the tier index
    /// those two routes would answer differently for the same events — and the answer decides
    /// the byte order of a segment. That is what is pinned here; the claim is in the remarks on
    /// ComputeSortOrder, and this is the only thing that makes it true.</para>
    ///
    /// <para>The groups are deliberately many and the arrival order deliberately shuffled: one
    /// tie proves nothing against an unstable sort, which may leave a pair alone by luck.</para>
    /// </summary>
    [Fact]
    public void TiedKeys_KeepTierOrder_OnBothRoutes()
    {
        const int Groups = 250, PerGroup = 8;
        long baseTicks = new DateTimeOffset(2026, 9, 11, 10, 0, 0, TimeSpan.Zero).UtcTicks;

        // Every row of a group carries the SAME (ticks, id) and a DIFFERENT payload, so the
        // order within a group is observable and only the tie-break can decide it.
        var ascending = new List<Row>(Groups * PerGroup);
        for (int g = 0; g < Groups; g++)
            for (int k = 0; k < PerGroup; k++)
                ascending.Add(new Row(
                    Seq:   (uint)(g * PerGroup + k),
                    Key:   (uint)g,
                    Ticks: baseTicks + g * 10,
                    Level: (LogLevel)(g % 6)));

        var shuffled = new List<Row>(ascending);
        var rng = new Random(17);
        for (int i = shuffled.Count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (shuffled[i], shuffled[j]) = (shuffled[j], shuffled[i]);
        }

        var pool = new StringInternPool();

        // Route 1: the tier is already ascending, so the identity permutation is the answer.
        using (var hot = BuildTier(ascending, pool))
        {
            int[] order = SegmentWriter.ComputeSortOrder(hot);
            for (int k = 0; k < order.Length; k++) Assert.Equal(k, order[k]);
        }

        // Route 2: the same events shuffled, so the sort runs. Tied rows must still come out in
        // tier-index order — anywhere they do not, an unstable partition has decided the file.
        using (var hot = BuildTier(shuffled, pool))
        {
            int[] order = SegmentWriter.ComputeSortOrder(hot);
            Assert.Equal(hot.Count, order.Length);

            long  prevTs = long.MinValue;
            ulong prevId = 0;
            int   prevIdx = -1;
            for (int k = 0; k < order.Length; k++)
            {
                ref var h = ref hot.GetHeader(order[k]);
                if (k > 0)
                {
                    bool tied = h.TimestampUtcTicks == prevTs && h.Id == prevId;
                    if (tied)
                        Assert.True(order[k] > prevIdx,
                            $"tied rows came out as tier index {prevIdx} then {order[k]} at position {k} — "
                            + "the sort is not keeping tied rows in tier order");
                    else
                        Assert.True(h.TimestampUtcTicks > prevTs ||
                                    (h.TimestampUtcTicks == prevTs && h.Id > prevId),
                                    $"out of order at {k}");
                }
                prevTs  = h.TimestampUtcTicks;
                prevId  = h.Id;
                prevIdx = order[k];
            }
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
