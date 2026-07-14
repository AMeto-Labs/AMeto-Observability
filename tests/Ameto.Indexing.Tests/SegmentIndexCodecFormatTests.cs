using Ameto.Core;
using Ameto.Indexing;
using Xunit.Abstractions;

namespace Ameto.Indexing.Tests;

/// <summary>
/// Verifies the codec posting-list format for the inverted + trigram indexes: query results
/// after a codec round-trip match the in-memory build, legacy RoaringBitmap blobs still read
/// identically (backward compatibility via the magic marker), and the codec serialise allocates
/// far less than the old RoaringBitmap path.
/// </summary>
public sealed class SegmentIndexCodecFormatTests
{
    private readonly ITestOutputHelper _out;
    public SegmentIndexCodecFormatTests(ITestOutputHelper o) => _out = o;

    private static readonly string[] Methods = { "GET", "POST", "PUT", "DELETE" };

    private static SegmentInvertedIndex BuildInverted(int events)
    {
        var idx = new SegmentInvertedIndex();
        for (int i = 0; i < events; i++)
        {
            idx.Add((uint)i, "http.method", Methods[i % Methods.Length]);
            idx.Add((uint)i, "orderId",     (long)i);              // unique / high-cardinality
            idx.Add((uint)i, "region",      "ae-dxb");             // present on every event
            if (i % 3 == 0) idx.Add((uint)i, "http.status", (long)500);
        }
        return idx;
    }

    private static SegmentTrigramIndex BuildTrigram(int events)
    {
        var idx = new SegmentTrigramIndex();
        string[] msgs = { "payment processed", "http handled", "adapter query", "balance checked" };
        for (int i = 0; i < events; i++)
            idx.Add((uint)i, msgs[i % msgs.Length]);
        return idx;
    }

    // ── Inverted: codec round-trip ≡ build-mode ────────────────────────────────
    [Fact]
    public void Inverted_CodecRoundTrip_MatchesBuild()
    {
        var built = BuildInverted(300);
        var read  = SegmentInvertedIndex.Deserialise(built.Serialise());

        foreach (var m in Methods)
            Assert.Equal(built.Lookup("http.method", m), read.Lookup("http.method", m));
        Assert.Equal(built.Lookup("orderId", (long)42),   read.Lookup("orderId", (long)42));
        Assert.Equal(built.Lookup("region", "ae-dxb"),    read.Lookup("region", "ae-dxb"));
        Assert.Null(read.Lookup("http.method", "PATCH")); // absent value

        // AND-intersection across predicates (query-mode only): GET (i%4==0) ∧ status500 (i%3==0).
        var preds    = new (string, object?)[] { ("http.method", "GET"), ("http.status", (long)500) };
        var expected = Enumerable.Range(0, 300).Where(i => i % 4 == 0 && i % 3 == 0).Select(i => (uint)i).ToArray();
        Assert.Equal(expected, read.LookupIntersect(preds));
        Assert.True(read.MightContain("region", "ae-dxb"));
        Assert.False(read.MightContain("region", "xx-yyy"));
    }

    // ── Inverted: legacy RoaringBitmap blob still reads identically ─────────────
    [Fact]
    public void Inverted_LegacyRoaringBlob_ReadsSameAsCodec()
    {
        var built = BuildInverted(300);
        var fromCodec   = SegmentInvertedIndex.Deserialise(built.Serialise());
        var fromRoaring = SegmentInvertedIndex.Deserialise(built.SerialiseRoaringV1());

        foreach (var m in Methods)
            Assert.Equal(fromCodec.Lookup("http.method", m), fromRoaring.Lookup("http.method", m));
        Assert.Equal(fromCodec.Lookup("orderId", (long)99), fromRoaring.Lookup("orderId", (long)99));
        Assert.Equal(fromCodec.Lookup("region", "ae-dxb"),  fromRoaring.Lookup("region", "ae-dxb"));
    }

    // ── Trigram: codec round-trip ≡ build-mode, and legacy reads the same ───────
    [Fact]
    public void Trigram_CodecRoundTrip_And_LegacyBlob_Match()
    {
        var built       = BuildTrigram(300);
        var fromCodec   = SegmentTrigramIndex.Deserialise(built.Serialise());
        var fromRoaring = SegmentTrigramIndex.Deserialise(built.SerialiseRoaringV1());

        foreach (var q in new[] { "pay", "processed", "handled", "adapter", "balance", "zzz" })
        {
            var expected = built.Lookup(q);
            Assert.Equal(expected, fromCodec.Lookup(q));
            Assert.Equal(expected, fromRoaring.Lookup(q));
        }
    }

    // ── Allocation: codec serialise ≪ RoaringBitmap serialise ──────────────────
    [Fact]
    public void CodecSerialise_AllocatesFarLessThanRoaring()
    {
        var inv = BuildInverted(20_000);
        var tri = BuildTrigram(20_000);

        // warm up
        _ = inv.Serialise(); _ = inv.SerialiseRoaringV1(); _ = tri.Serialise(); _ = tri.SerialiseRoaringV1();

        long r0 = GC.GetAllocatedBytesForCurrentThread();
        _ = inv.SerialiseRoaringV1(); _ = tri.SerialiseRoaringV1();
        long roaring = GC.GetAllocatedBytesForCurrentThread() - r0;

        long c0 = GC.GetAllocatedBytesForCurrentThread();
        _ = inv.Serialise(); _ = tri.Serialise();
        long codec = GC.GetAllocatedBytesForCurrentThread() - c0;

        _out.WriteLine($"serialise 20k-event segment: roaring={roaring / 1048576.0:F1} MB  codec={codec / 1048576.0:F1} MB  " +
                       $"({(codec == 0 ? 0 : (double)roaring / codec):F1}x less)");
        Assert.True(codec * 3 < roaring, $"codec ({codec} B) should allocate far less than roaring ({roaring} B)");
    }
}
