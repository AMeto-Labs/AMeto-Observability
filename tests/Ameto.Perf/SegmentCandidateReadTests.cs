using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// v5 candidate-driven segment reads: index posting lists store FILE ordinals (the
/// sort order shared by <see cref="SegmentWriter.ComputeSortOrder"/>, the index build
/// and the block writer), and <see cref="SegmentReader.ReadEventsAsync"/> uses them to
/// skip blocks and materialise only candidate rows.
///
/// The hot tier is written with SHUFFLED timestamps so the file order differs from the
/// insertion order — any ordinal-mapping mistake (indexing in hot order, off-by-one
/// block boundaries, broken two-pointer row walk) surfaces as a mismatch here.
/// </summary>
public sealed class SegmentCandidateReadTests
{
    private const int Events = 5_000;

    [Fact]
    public async Task CandidateRead_MatchesFullScan_OnShuffledSegment()
    {
        var pool = new StringInternPool();
        using var hot = BuildShuffledHotTier(pool);

        var order   = SegmentWriter.ComputeSortOrder(hot);
        var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool, order);

        string path = Path.Combine(Path.GetTempPath(), $"cand-{Guid.NewGuid():N}.seg");
        try
        {
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, order);
                w.WriteInvertedIndex(builder.SerialisedInvertedIndex);
                w.WriteTrigramIndex(builder.SerialisedTrigramIndex);
                w.WriteBloomFilter(builder.SerialisedBloomFilter);
                w.Finalise(new NodeId(0), new SegmentId(1UL));
            }

            using var reader = SegmentReader.Open(path);
            var idx = SegmentIndexReader.Load(
                reader.ReadInvertedIndexBytes(),
                reader.ReadTrigramIndexBytes(),
                reader.ReadBloomFilterBytes());

            var candidates = idx.LookupIntersect(
                new List<(string property, object? value)> { ("bucket", "needle") });
            Assert.NotNull(candidates);

            // Oracle: full scan + manual predicate.
            var expected = new List<ulong>();
            await foreach (var ev in reader.ReadEventsAsync(null, null, null))
                if (Equals(ev.Properties?["bucket"], "needle"))
                    expected.Add(ev.Id.RawValue);
            Assert.Equal(Events / 97 + 1, expected.Count); // i % 97 == 0 within [0, Events)

            // Candidate-driven scan must return exactly the same events, same order.
            var got = new List<ulong>();
            await foreach (var ev in reader.ReadEventsAsync(candidates, null, null))
            {
                Assert.Equal("needle", ev.Properties?["bucket"]);
                got.Add(ev.Id.RawValue);
            }
            Assert.Equal(expected, got);

            // And in reverse direction.
            var expectedRev = new List<ulong>(expected);
            expectedRev.Reverse();
            var gotRev = new List<ulong>();
            await foreach (var ev in reader.ReadEventsAsync(candidates, null, null, reversed: true))
                gotRev.Add(ev.Id.RawValue);
            Assert.Equal(expectedRev, gotRev);
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task EmptyOrForeignCandidates_YieldNothingExtra()
    {
        var pool = new StringInternPool();
        using var hot = BuildShuffledHotTier(pool);

        var order   = SegmentWriter.ComputeSortOrder(hot);
        var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool, order);

        string path = Path.Combine(Path.GetTempPath(), $"cand-{Guid.NewGuid():N}.seg");
        try
        {
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, order);
                w.WriteInvertedIndex(builder.SerialisedInvertedIndex);
                w.WriteTrigramIndex(builder.SerialisedTrigramIndex);
                w.WriteBloomFilter(builder.SerialisedBloomFilter);
                w.Finalise(new NodeId(0), new SegmentId(2UL));
            }

            using var reader = SegmentReader.Open(path);

            // A single candidate: exactly one event comes back, and it is the file's
            // ordinal-#k event (the k-th in @t order).
            var all = new List<LogEvent>();
            await foreach (var ev in reader.ReadEventsAsync(null, null, null))
                all.Add(ev);

            uint k = (uint)(Events / 2);
            var single = new List<LogEvent>();
            await foreach (var ev in reader.ReadEventsAsync([k], null, null))
                single.Add(ev);

            Assert.Single(single);
            Assert.Equal(all[(int)k].Id, single[0].Id);
        }
        finally { File.Delete(path); }
    }

    /// <summary>Insertion order deliberately disagrees with @t order (shuffled timestamps).</summary>
    private static HotTierSegment BuildShuffledHotTier(StringInternPool pool)
    {
        var hot = new HotTierSegment(Events + 1, (long)Events * 512 + 1024 * 1024);

        int    tmplIdx = pool.Intern("evt");
        string tmpl    = pool.Get(tmplIdx);
        int    svcIdx  = pool.Intern("Svc.A");

        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var  rng  = new Random(42);
        var  perm = new int[Events];
        for (int i = 0; i < Events; i++) perm[i] = i;
        for (int i = Events - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (perm[i], perm[j]) = (perm[j], perm[i]);
        }

        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < Events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("orderId"); w.Write((long)i);
            w.Write("bucket");  w.Write(i % 97 == 0 ? "needle" : "hay");
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + perm[i] * TimeSpan.TicksPerMillisecond,
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        hot.Freeze();
        return hot;
    }
}
