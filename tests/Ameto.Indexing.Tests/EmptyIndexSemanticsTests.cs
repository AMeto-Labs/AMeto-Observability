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
}
