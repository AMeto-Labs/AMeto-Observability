using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The UTF-8 arena build of <see cref="SegmentInvertedIndex"/> must write the SAME BYTES the
/// dictionary-of-lists build wrote: properties in first-seen order, values in first-seen order
/// within each, same codec. The reference here is that old build reduced to its essentials,
/// independent of the class under test. The builder-level test below adds the values whose
/// UTF-8 formatting could plausibly differ from the UTF-16 one — doubles with exponents,
/// negative and boundary integers, non-ASCII and mixed-case strings, nested keys.
/// </summary>
public sealed class SegmentInvertedBuildParityTests
{
    private const uint CodecMagic = 0xFFFFFFFFu;

    private static byte[] Reference(IEnumerable<(uint offset, string prop, object? value)> adds)
    {
        var index = new Dictionary<string, Dictionary<string, List<int>>>(StringComparer.Ordinal);
        bool unsorted = false;
        foreach (var (offset, prop, value) in adds)
        {
            if (!index.TryGetValue(prop, out var values)) index[prop] = values = new(StringComparer.Ordinal);
            string s = IndexValueForms.Serialise(value);
            if (!values.TryGetValue(s, out var list)) values[s] = list = new();
            int o = (int)offset;
            if (list.Count == 0 || list[^1] < o) list.Add(o);
            else if (list[^1] > o) { list.Add(o); unsorted = true; }
        }
        if (unsorted)
            foreach (var values in index.Values)
                foreach (var list in values.Values)
                {
                    var d = list.Distinct().OrderBy(x => x).ToList();
                    list.Clear(); list.AddRange(d);
                }

        var ms = new MemoryStream();
        Span<byte> tmp = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, CodecMagic); ms.Write(tmp);
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, (uint)index.Count); ms.Write(tmp);
        foreach (var (prop, values) in index)
        {
            WriteUtf8(ms, prop);
            BinaryPrimitives.WriteUInt32LittleEndian(tmp, (uint)values.Count); ms.Write(tmp);
            foreach (var (val, list) in values)
            {
                WriteUtf8(ms, val);
                var enc = new byte[SegmentBitmapCodec.MaxEncodedSize(list.Count)];
                int n   = SegmentBitmapCodec.Encode(list.ToArray(), enc);
                BinaryPrimitives.WriteUInt32LittleEndian(tmp, (uint)n); ms.Write(tmp);
                ms.Write(enc, 0, n);
            }
        }
        return ms.ToArray();

        static void WriteUtf8(MemoryStream ms, string s)
        {
            var b = Encoding.UTF8.GetBytes(s);
            Span<byte> len = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(len, (ushort)b.Length);
            ms.Write(len); ms.Write(b);
        }
    }

    private static List<(uint, string, object?)> Realistic(int events, int seed)
    {
        var rng = new Random(seed);
        string[] methods = { "GET", "get", "Post", "DELETE" };
        string[] cities  = { "ae-dxb", "Алматы", "München", "東京", "ae-DXB" };
        var adds = new List<(uint, string, object?)>();
        for (int i = 0; i < events; i++)
        {
            uint o = (uint)i;
            adds.Add((o, "@l", "Information"));
            adds.Add((o, "@tr", rng.NextInt64().ToString("x16") + rng.NextInt64().ToString("x16")));
            adds.Add((o, "orderId", (long)rng.Next()));
            adds.Add((o, "http.method", methods[rng.Next(methods.Length)]));
            adds.Add((o, "city", cities[rng.Next(cities.Length)]));
            adds.Add((o, "ratio", rng.NextDouble() * Math.Pow(10, rng.Next(-8, 25))));
            adds.Add((o, "neg", (long)-rng.Next()));
            if (i % 3 == 0) adds.Add((o, "flag", i % 2 == 0));
            if (i % 5 == 0) adds.Add((o, "nothing", null));
            if (i % 7 == 0) adds.Add((o, "username", "alice"));
            if (i % 11 == 0) adds.Add((o, "orderId", (long)rng.Next()));   // second value, same event
        }
        return adds;
    }

    private static byte[] Arena(IEnumerable<(uint offset, string prop, object? value)> adds)
    {
        var idx = new SegmentInvertedIndex();
        foreach (var (offset, prop, value) in adds) idx.Add(offset, prop, value);
        return idx.Serialise();
    }

    [Fact]
    public void InOrderBuild_MatchesDictionaryBuild_ByteForByte()
    {
        var adds = Realistic(3_000, seed: 5);
        Assert.Equal(Reference(adds), Arena(adds));
    }

    [Fact]
    public void OutOfOrderOffsets_AreSortedAndDeduplicated_LikeBefore()
    {
        var adds = Realistic(400, seed: 9);
        var rng  = new Random(2);
        var shuffled = adds.OrderBy(_ => rng.Next()).Concat(adds.Take(40)).ToList();
        Assert.Equal(Reference(shuffled), Arena(shuffled));
    }

    [Fact]
    public void PreSizedFromHints_WritesTheSameBytes()
    {
        var adds = Realistic(1_000, seed: 4);
        var small = new SegmentInvertedIndex(expectedTerms: 16);      // forces rehashes
        var big   = new SegmentInvertedIndex(expectedTerms: 1 << 16); // none
        foreach (var (o, p, v) in adds) { small.Add(o, p, v); big.Add(o, p, v); }
        Assert.Equal(Reference(adds), small.Serialise());
        Assert.Equal(Reference(adds), big.Serialise());
        Assert.Equal(small.TermCount, big.TermCount);
    }

    [Fact]
    public void BuildModeLookup_AndMightContain_StillAnswer()
    {
        var idx = new SegmentInvertedIndex();
        idx.Add(0, "http.method", "GET");
        idx.Add(1, "http.method", "POST");
        idx.Add(2, "http.method", "GET");
        idx.Add(2, "orderId", 42L);
        Assert.Equal([0u, 2u], idx.Lookup("http.method", "GET")!);
        Assert.Equal([2u],     idx.Lookup("orderId", 42L)!);
        Assert.Null(idx.Lookup("http.method", "PATCH"));
        Assert.Null(idx.Lookup("nosuch", "x"));
        Assert.True(idx.MightContain("http.method", "GET"));
        Assert.False(idx.MightContain("http.method", "PATCH"));
        Assert.True(idx.MightContain("nosuch", "x"));                // unknown property → no information
        var read = SegmentInvertedIndex.Deserialise(idx.Serialise());
        Assert.Equal(idx.Lookup("http.method", "GET"), read.Lookup("http.method", "GET"));
    }

    [Fact]
    public void Add_DoesNotAllocatePerPosting()
    {
        var rng = new Random(1);
        Span<byte> hex = stackalloc byte[32];
        // Warm the pool with builds of the same shape, released: the arrays they rented are
        // what the measured build rents back. A cold pool would show every slab and every
        // table doubling as a fresh allocation — the cost of the FIRST group in a process.
        // Twice, because a growing table rents its next size before returning the old one and
        // the pool serves a rent from the next bucket up when the exact one is empty, so one
        // pass leaves a couple of buckets short; the second settles them, as the second group
        // of a real flush does.
        for (int pass = 0; pass < 2; pass++)
        {
            var warm = new SegmentInvertedIndex();
            for (int i = 0; i < 100_000; i++)
            {
                warm.AddUtf8((uint)i, "@tr"u8, Fill(hex, rng));
                warm.AddUtf8((uint)i, "@l"u8, "Information"u8);
            }
            warm.ReleaseBuildBuffers();
        }
        var idx = new SegmentInvertedIndex();
        for (int i = 0; i < 2_000; i++) idx.AddUtf8((uint)i, "@tr"u8, Fill(hex, rng));

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 2_000; i < 100_000; i++)
        {
            idx.AddUtf8((uint)i, "@tr"u8, Fill(hex, rng));         // unique: a new term each time
            idx.AddUtf8((uint)i, "@l"u8, "Information"u8);         // repeat: one dense bucket
        }
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        // 98k new 32-byte terms: the old build allocated a string, a list, an int[] and a
        // dictionary entry per term (~150 B each, ~15 MB) plus the dictionary doublings. The
        // arena stores them in pooled slabs and allocates nothing per term.
        Assert.True(bytes < 64 * 1024, $"{bytes} B allocated for 98k new terms");

        static ReadOnlySpan<byte> Fill(Span<byte> hex, Random rng)
        {
            ((ulong)rng.NextInt64()).TryFormat(hex, out _, "x16");
            ((ulong)rng.NextInt64()).TryFormat(hex[16..], out _, "x16");
            return hex;
        }
    }

    [Fact]
    public void Release_ReturnsBuffers_AndGuardsLaterUse()
    {
        var idx = new SegmentInvertedIndex();
        idx.Add(0, "a", "b");
        Assert.True(idx.BuildRetainedBytes > 0);
        idx.ReleaseBuildBuffers();
        Assert.Equal(0, idx.BuildRetainedBytes);
        Assert.Throws<ObjectDisposedException>(() => idx.Add(1, "a", "c"));
        Assert.Throws<ObjectDisposedException>(() => idx.Serialise());
        idx.ReleaseBuildBuffers();
    }

    // ── The builder: raw-UTF-8 walk against the string-based reference oracle ──

    [Fact]
    public void StreamingWalk_MatchesReferenceBuild_OnAwkwardValues()
    {
        var pool   = new StringInternPool();
        int svcIdx = pool.Intern("Кошелёк.API");
        int tmplIdx = pool.Intern("Платёж {Id} for {User} responded {Status}");
        string tmpl = pool.Get(tmplIdx);
        const int events = 3_000;
        using var hot = new HotTierSegment(events + 1, (long)events * 1024 + 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var rng = new Random(17);
        double[] doubles = { 0.1, -2.5, 1e300, 1e-300, 123456789.125, 1e20, 5e-324, double.MaxValue, 0.0, -0.0, 1.0 / 3 };
        long[]   longs   = { long.MinValue, long.MaxValue, -1, 0, 1, 1_000_000_000_000 };
        string[] strings = { "GET", "get", "München", "ÜNERWARTET", "東京", "ae-DXB", "", "ab", "İstanbul", "ß", "ab" };
        var buf = new ArrayBufferWriter<byte>(1024);

        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(9);
            w.Write("d");     w.Write(doubles[rng.Next(doubles.Length)]);
            w.Write("l");     w.Write(longs[rng.Next(longs.Length)]);
            w.Write("u");     w.Write(ulong.MaxValue - (ulong)rng.Next(3));      // > long.Max → plain
            w.Write("s");     w.Write(strings[rng.Next(strings.Length)]);
            w.Write("Ключ");  w.Write(strings[rng.Next(strings.Length)]);
            w.Write("f");     w.Write((float)rng.NextDouble());               // msgpack float32 → double
            w.Write("b");     w.Write(rng.Next(2) == 0);
            w.Write("n");     w.WriteNil();
            w.Write("nest");  w.WriteMapHeader(2);
                              w.Write("inner"); w.Write("Alice");
                              w.Write("arr");   w.WriteArrayHeader(2); w.Write((long)7); w.Write("Seven");
            w.Flush();

            hot.TryWrite(new LogEventHeader
            {
                Id = new EventId(0u, (uint)i).RawValue, TimestampUtcTicks = baseTicks + i,
                Level = (LogLevel)(i % 6), MessageTemplatePoolIndex = tmplIdx, ServiceNamePoolIndex = svcIdx,
                TraceIdHi = i % 4 == 0 ? (ulong)rng.NextInt64() : 0, TraceIdLo = i % 4 == 0 ? (ulong)rng.NextInt64() : 0,
                SpanId = i % 2 == 0 ? (ulong)rng.NextInt64() : 0,
            }, buf.WrittenSpan, tmpl, i % 9 == 0
                ? new ExceptionInfo { Type = "System.TimeoutException", Message = "Ожидание истекло", StackTrace = "   at X.Y()", Inner = new ExceptionInfo { Type = "Inner.Ex" } }
                : null);
        }

        using var streaming = new SegmentIndexBuilder(events);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(events);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedInvertedIndex, streaming.SerialisedInvertedIndex);
        Assert.Equal(reference.SerialisedTrigramIndex,  streaming.SerialisedTrigramIndex);
        Assert.Equal(reference.SerialisedBloomFilter,   streaming.SerialisedBloomFilter);
        Assert.Equal(reference.BloomTermsAdded,         streaming.BloomTermsAdded);
    }
}
