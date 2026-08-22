using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// Trigram hints must come from pure AND paths only. The consumer
/// (<c>TryNarrowWithIndex</c>) INTERSECTS every hint's posting set and treats an empty
/// intersection as proof the segment is empty — so hints collected across OR branches
/// turned <c>contains(a) or contains(b)</c> into "contains a AND b" on cold segments:
/// silently missing rows, and whole segments dropped when one branch's term was absent.
/// Hot-tier scans evaluated the same filter correctly, so results changed at flush time.
/// </summary>
public sealed class TrigramOrHintTests
{
    [Fact]
    public void OrOfContains_ProducesNoTrigramHints()
    {
        var filter = CompiledFilter.Compile("contains(@mt, 'alpha') or contains(@mt, 'beta')");
        Assert.Empty(filter.GetTrigramHints());
    }

    [Fact]
    public void OrMixingEqualityAndContains_ProducesNoTrigramHints()
    {
        // The worst shape: narrowing to 'foo'-containing rows dropped every Error row.
        var filter = CompiledFilter.Compile("@l = 'Error' or contains(@mt, 'foo')");
        Assert.Empty(filter.GetTrigramHints());
    }

    [Fact]
    public void OrBuriedUnderAnd_StillDisablesThatBranchsHints()
    {
        var filter = CompiledFilter.Compile(
            "contains(@mt, 'outer') and (contains(@mt, 'alpha') or contains(@mt, 'beta'))");
        var hints = filter.GetTrigramHints();
        Assert.Single(hints);
        Assert.Equal("outer", hints[0].text);
    }

    [Fact]
    public void AndChain_StillProducesAllHints()
    {
        var filter = CompiledFilter.Compile("contains(@mt, 'alpha') and contains(@mt, 'beta')");
        Assert.Equal(2, filter.GetTrigramHints().Count);
    }
}
