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

    private static SegmentInvertedIndex Build(params (string Property, string Value)[] pairs)
    {
        var build = new SegmentInvertedIndex();
        foreach (var (property, value) in pairs)
            build.Add(0u, property, value);
        return SegmentInvertedIndex.Deserialise(build.Serialise());
    }
}
