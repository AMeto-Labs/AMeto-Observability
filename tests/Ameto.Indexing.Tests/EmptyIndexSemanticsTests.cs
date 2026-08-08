using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// Empty (absent) index sections mean "no index was built" — e.g. a segment
/// flushed by WAL recovery before the index builder was wired. That is NO
/// information, so every index must answer "might match" and force a scan.
/// A match-nothing answer would make the whole segment permanently invisible
/// to filtered queries.
/// </summary>
public sealed class EmptyIndexSemanticsTests
{
    [Fact]
    public void EmptyBloom_NeverRejects()
    {
        using var bloom = SegmentBloomFilter.Deserialise([]);
        Assert.True(bloom.MightContain("anything"));
        Assert.True(bloom.MightContain("wallet:1822905:balance"));
        Assert.True(bloom.MightContain(""));
    }

    [Fact]
    public void EmptyInverted_NeverRejects()
    {
        var inv = SegmentInvertedIndex.Deserialise([]);
        Assert.True(inv.MightContain("Level", "Error"));
        Assert.True(inv.MightContain("service.name", "X"));
    }

    [Fact]
    public void EmptyTrigram_ReturnsNoInformation()
    {
        var tri = SegmentTrigramIndex.Deserialise([]);
        Assert.Null(tri.Lookup("cache")); // null = "no info, scan", not "no matches"
    }

    // ── The same rule one level down: a property the index never wrote ─────────
    // LookupIntersect's empty array makes QueryExecutor drop the segment unread, so it
    // may only be returned when the index can PROVE nothing matches. It cannot prove
    // anything about a bucket it does not have.

    [Fact]
    public void UnknownProperty_ReturnsNoInformation()
    {
        var idx = Build(("@l", "Error"), ("region", "ae-dxb"));

        Assert.Null(idx.LookupIntersect([("nosuchproperty", "whatever")]));
    }

    [Fact]
    public void UnknownProperty_DoesNotVetoAKnownOne()
    {
        var idx = Build(("@l", "Error"), ("region", "ae-dxb"));

        // The known predicate still narrows; the unknown one is simply not consulted.
        var offsets = idx.LookupIntersect([("region", "ae-dxb"), ("nosuchproperty", "whatever")]);
        Assert.Equal([0u], offsets);
    }

    [Fact]
    public void KnownPropertyAbsentValue_IsStillProofOfNoMatch()
    {
        var idx = Build(("@l", "Error"), ("region", "ae-dxb"));

        // The other side of the contract — this must keep pruning, or the index is useless.
        Assert.Equal([], idx.LookupIntersect([("region", "eu-fra")]));
    }

    // ── Value casing: the scan matches it, so the index must not veto it ──────

    [Fact]
    public void ValueCasing_DoesNotHideAPosting()
    {
        var idx = Build(("@l", "Error"));

        Assert.Equal([0u], idx.LookupIntersect([("@l", "error")]));
        Assert.True(idx.MightContain("@l", "ERROR"));
    }

    [Fact]
    public void ValueCasing_CollidingBucketsMergeInsteadOfOverwriting()
    {
        var build = new SegmentInvertedIndex();
        build.Add(0u, "region", "AE-DXB");
        build.Add(1u, "region", "ae-dxb");
        var idx = SegmentInvertedIndex.Deserialise(build.Serialise());

        // Both events are reachable through either spelling — dropping one of the two
        // buckets would trade one silent false negative for another.
        Assert.Equal([0u, 1u], idx.LookupIntersect([("region", "ae-dxb")]));
        Assert.Equal([0u, 1u], idx.LookupIntersect([("region", "AE-DXB")]));
    }

    [Fact]
    public void BloomFilter_MatchesRegardlessOfValueCasing()
    {
        using var build = SegmentBloomFilter.Create(16);
        build.Add("Error");
        build.Add("System.TimeoutException");
        using var read = SegmentBloomFilter.Deserialise(build.Serialise());

        Assert.True(read.MightContain("Error"));
        Assert.True(read.MightContain("error"));
        Assert.True(read.MightContain("ERROR"));
        Assert.True(read.MightContain("system.timeoutexception"));
    }

    // ── Bloom filters that were already on disk when folding shipped ──────────

    [Fact]
    public void PreFoldingBloom_CannotProveAbsenceOfACasedValue()
    {
        using var read = SegmentBloomFilter.Deserialise(PreFoldingBlob("Error", "AE-DXB", "Wallet.API"));

        // Its own casing still answers exactly...
        Assert.True(read.MightContain("Error"));
        Assert.True(read.MightContain("AE-DXB"));

        // ...and every other casing must answer "might", not "no": the filter holds one
        // arbitrary spelling, the scan compares OrdinalIgnoreCase, and phase 1 drops the
        // segment before the inverted index gets a say. Folding on the WRITE side does
        // nothing for bytes already written.
        Assert.True(read.MightContain("error"));
        Assert.True(read.MightContain("ERROR"));
        Assert.True(read.MightContain("ae-dxb"));
        Assert.True(read.MightContain("wallet.api"));
    }

    [Fact]
    public void PreFoldingBloom_StillPrunesValuesThatHaveOnlyOneSpelling()
    {
        // A value with no cased characters has exactly one spelling, so the miss IS proof
        // and these segments keep the cheap gate for the ids and numbers it matters most for.
        using var read = SegmentBloomFilter.Deserialise(PreFoldingBlob("12345", "8080"));

        Assert.True(read.MightContain("12345"));
        Assert.False(read.MightContain("99999"));
    }

    [Fact]
    public void PostFoldingBloom_StillPrunes()
    {
        using var build = SegmentBloomFilter.Create(16);
        build.Add("Error");
        using var read = SegmentBloomFilter.Deserialise(build.Serialise());

        Assert.True(read.MightContain("error"));
        Assert.False(read.MightContain("Warning"));
    }

    /// <summary>
    /// A blob as an older build wrote it: values stored in their ORIGINAL casing (the byte
    /// overload does not fold) and no folded marker in the capacity word.
    /// </summary>
    private static byte[] PreFoldingBlob(params string[] values)
    {
        using var build = SegmentBloomFilter.Create(16);
        foreach (var v in values)
            build.Add(System.Text.Encoding.UTF8.GetBytes(v));

        var blob = build.Serialise();
        uint capacity = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(blob.AsSpan(4));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(blob.AsSpan(4), capacity & 0x7FFF_FFFFu);
        return blob;
    }

    private static SegmentInvertedIndex Build(params (string Property, string Value)[] pairs)
    {
        var build = new SegmentInvertedIndex();
        foreach (var (property, value) in pairs)
            build.Add(0u, property, value);
        return SegmentInvertedIndex.Deserialise(build.Serialise());
    }
}
