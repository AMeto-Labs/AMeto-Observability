using System.Buffers.Binary;
using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The arena-backed trigram build must write the SAME BYTES the dictionary-of-lists build
/// wrote: same bucket order (first seen), same postings (ascending, distinct), same codec.
/// The reference here is that old build, reduced to its essentials, so the pin does not
/// depend on the class under test agreeing with itself.
/// </summary>
public sealed class SegmentTrigramBuildParityTests
{
    private const uint CodecMagicV2 = 0xFFFFFFFEu;

    /// <summary>The pre-arena build: tuple-keyed dictionary, list per bucket, tail dedup.</summary>
    private static byte[] Reference(IEnumerable<(uint offset, string text)> adds)
    {
        var sets = new Dictionary<(char, char, char), List<int>>();
        bool unsorted = false;
        foreach (var (offset, text) in adds)
        {
            if (text.Length < 3) continue;
            string lower = text.ToLowerInvariant();
            for (int i = 0; i <= lower.Length - 3; i++)
            {
                var key = (lower[i], lower[i + 1], lower[i + 2]);
                if (!sets.TryGetValue(key, out var list)) sets[key] = list = new List<int>();
                int o = (int)offset;
                if (list.Count == 0 || list[^1] < o) list.Add(o);
                else if (list[^1] > o) { list.Add(o); unsorted = true; }
            }
        }
        if (unsorted)
            foreach (var list in sets.Values)
            {
                var distinct = list.Distinct().OrderBy(x => x).ToList();
                list.Clear(); list.AddRange(distinct);
            }

        var ms = new MemoryStream();
        Span<byte> tmp = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, CodecMagicV2);   ms.Write(tmp);
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, (uint)sets.Count); ms.Write(tmp);
        foreach (var ((c0, c1, c2), list) in sets)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(tmp, c0); ms.Write(tmp[..2]);
            BinaryPrimitives.WriteUInt16LittleEndian(tmp, c1); ms.Write(tmp[..2]);
            BinaryPrimitives.WriteUInt16LittleEndian(tmp, c2); ms.Write(tmp[..2]);
            var enc = new byte[SegmentBitmapCodec.MaxEncodedSize(list.Count)];
            int n   = SegmentBitmapCodec.Encode(list.ToArray(), enc);
            BinaryPrimitives.WriteUInt32LittleEndian(tmp, (uint)n); ms.Write(tmp);
            ms.Write(enc, 0, n);
        }
        return ms.ToArray();
    }

    private static byte[] Arena(IEnumerable<(uint offset, string text)> adds)
    {
        var idx = new SegmentTrigramIndex();
        foreach (var (offset, text) in adds) idx.Add(offset, text);
        return idx.Serialise();
    }

    private static List<(uint, string)> Realistic(int events, int seed)
    {
        var rng = new Random(seed);
        string[] templates =
        {
            "HTTP {Method} {Route} responded {Status} in {Elapsed} ms",
            "Платёж {Id} проведён за {Elapsed} мс",
            "Settlement failed for {wallet}",
            "用户 {User} 已登录",
        };
        string[] values = { "GET", "POST", "/api/pay", "/api/balance", "ae-dxb", "Ünerwartete Ausnahme", "aaaaaaa", "AbAbAbAb" };
        var adds = new List<(uint, string)>();
        for (int i = 0; i < events; i++)
        {
            adds.Add(((uint)i, templates[rng.Next(templates.Length)]));
            adds.Add(((uint)i, values[rng.Next(values.Length)]));
            adds.Add(((uint)i, "cust-" + rng.Next(0, 5000)));
            adds.Add(((uint)i, rng.NextInt64().ToString("x16")));
            if (i % 5 == 0) adds.Add(((uint)i, templates[0]));          // same text twice in one event
            if (i % 97 == 0) adds.Add(((uint)i, new string('z', 2000))); // dense single trigram, pooled scratch path
        }
        return adds;
    }

    [Fact]
    public void InOrderBuild_MatchesDictionaryBuild_ByteForByte()
    {
        var adds = Realistic(3_000, seed: 7);
        Assert.Equal(Reference(adds), Arena(adds));
    }

    [Fact]
    public void OutOfOrderOffsets_AreSortedAndDeduplicated_LikeBefore()
    {
        var adds = Realistic(500, seed: 3);
        // Shuffle so postings arrive out of order and repeat across non-adjacent adds.
        var rng = new Random(1);
        var shuffled = adds.OrderBy(_ => rng.Next()).Concat(adds.Take(50)).ToList();
        Assert.Equal(Reference(shuffled), Arena(shuffled));
    }

    [Fact]
    public void DenseBucket_CrossesManyChunks_AndReadsBack()
    {
        // One trigram on every event: postings cost one byte each, so 100k of them span
        // thousands of 28-byte chunks. Round-trip through the reader must give every offset.
        var idx = new SegmentTrigramIndex();
        for (uint i = 0; i < 100_000; i++) idx.Add(i, "aaa");
        var read = SegmentTrigramIndex.Deserialise(idx.Serialise());
        var hits = read.Lookup("aaa")!;
        Assert.Equal(100_000, hits.Length);
        Assert.Equal(0u, hits[0]);
        Assert.Equal(99_999u, hits[^1]);
    }

    [Fact]
    public void BuildModeLookup_StillAnswers()
    {
        var idx = new SegmentTrigramIndex();
        idx.Add(0, "payment processed");
        idx.Add(1, "платёж проведён");
        idx.Add(2, "payment failed");
        Assert.Equal([0u, 2u], idx.Lookup("payment")!);
        Assert.Equal([1u],     idx.Lookup("проведён")!);
        Assert.Empty(idx.Lookup("zzz")!);
        Assert.Equal(idx.Lookup("payment"), SegmentTrigramIndex.Deserialise(idx.Serialise()).Lookup("payment"));
    }

    [Fact]
    public void Add_DoesNotAllocatePerPosting()
    {
        var idx = new SegmentTrigramIndex();
        const string t = "HTTP {Method} {Route} responded {Status} in {Elapsed} ms";
        for (uint i = 0; i < 2_000; i++) idx.Add(i, t);          // warm: table, buckets, first slab

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (uint i = 2_000; i < 60_000; i++) { idx.Add(i, t); idx.Add(i, "cust-12345"); }
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        // 58k events × 61 dense trigrams ≈ 3.5 M postings. The list-per-bucket build doubled an
        // int[] per bucket (4 B per posting retained, as much again in doubling garbage, all
        // LOH); the arena stores a one-byte gap per posting in 1 MB slabs, and the only managed
        // allocation is a slab the pool did not have yet — ≈1.2 B per posting here, and zero
        // once the pool is warm.
        long postings = 58_000L * 61;
        Assert.True(bytes < postings * 2, $"{bytes} B allocated for {postings:N0} postings");
    }

    [Fact]
    public void Release_ReturnsBuffers_AndGuardsLaterUse()
    {
        var idx = new SegmentTrigramIndex();
        idx.Add(0, "payment processed");
        Assert.True(idx.BuildRetainedBytes > 8 * 1024 * 1024);   // the slot table alone is 8 MB
        idx.ReleaseBuildBuffers();
        Assert.Equal(0, idx.BuildRetainedBytes);
        Assert.Throws<ObjectDisposedException>(() => idx.Add(1, "anything"));
        Assert.Throws<ObjectDisposedException>(() => idx.Serialise());
        idx.ReleaseBuildBuffers();                                 // idempotent
    }
}
