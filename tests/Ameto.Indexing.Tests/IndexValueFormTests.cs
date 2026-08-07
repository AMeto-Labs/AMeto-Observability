using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The VALUE axis of the index/scan disagreement, the sibling of the property-name axis.
///
/// <para>The index serialises with a type tag (<c>\0l5</c>, <c>\0d5</c>, <c>\0true</c>, bare
/// text for strings) while <c>FilterEvaluator.Compare</c> coerces — numbers across types
/// through <c>ToDouble</c>, a stored bool against the literal's text, a quoted numeral parsed
/// into a double. Every coercion the scan performs and the index does not used to be a silent
/// false negative, because "known bucket, absent value" is proof and drops the segment unread.
/// </para>
///
/// <para>Each test states the shape a user actually types (a dashboard quotes its values;
/// a query bar types <c>5</c> where msgpack stored 5.0) against the shape the flush wrote.</para>
/// </summary>
public sealed class IndexValueFormTests
{
    // ── A quoted literal against a stored scalar ──────────────────────────────

    [Fact]
    public void QuotedInteger_FindsAStoredInteger()
    {
        var idx = Build(("Count", 5L));

        Assert.Equal([0u], idx.LookupIntersect([("Count", "5")]));
        Assert.True(idx.MightContain("Count", "5"));
    }

    [Fact]
    public void QuotedBool_FindsAStoredBool()
    {
        var idx = Build(("Enabled", true));

        Assert.Equal([0u], idx.LookupIntersect([("Enabled", "true")]));
        Assert.Equal([0u], idx.LookupIntersect([("Enabled", "TRUE")]));
    }

    [Fact]
    public void QuotedDouble_FindsAStoredDouble()
    {
        var idx = Build(("Ratio", 2.5d));

        Assert.Equal([0u], idx.LookupIntersect([("Ratio", "2.5")]));
    }

    // ── Numeric literals across the stored numeric types ──────────────────────

    [Fact]
    public void IntegerLiteral_FindsAStoredDouble()
    {
        var idx = Build(("Ratio", 5.0d));

        Assert.Equal([0u], idx.LookupIntersect([("Ratio", 5L)]));
    }

    [Fact]
    public void DoubleLiteral_FindsAStoredInteger()
    {
        // `Count = 5.0` — the parser yields a double the moment the user types a decimal point.
        var idx = Build(("Count", 5L));

        Assert.Equal([0u], idx.LookupIntersect([("Count", 5.0d)]));
    }

    [Fact]
    public void IntegerLiteral_FindsAStoredInt32()
    {
        var idx = Build(("Retries", 0));

        Assert.Equal([0u], idx.LookupIntersect([("Retries", 0L)]));
        Assert.Equal([0u], idx.LookupIntersect([("Retries", "0")]));
    }

    [Fact]
    public void MixedStorageOfOneProperty_ReturnsEveryMatchingEvent()
    {
        // The partial-loss shape: two events store the same number with different msgpack
        // types. Returning only one of them is harder to notice than returning none.
        var build = new SegmentInvertedIndex();
        build.Add(0u, "Count", 5);        // int32  → "\0i5"
        build.Add(1u, "Count", 5.0d);     // double → "\0d5"
        build.Add(2u, "Count", "5");      // string → "5"
        var idx = SegmentInvertedIndex.Deserialise(build.Serialise());

        Assert.Equal([0u, 1u, 2u], idx.LookupIntersect([("Count", 5L)]));
    }

    [Fact]
    public void BoolLiteral_FindsAStoredBool()
    {
        var idx = Build(("Enabled", false));

        Assert.Equal([0u], idx.LookupIntersect([("Enabled", false)]));
        Assert.Equal([],   idx.LookupIntersect([("Enabled", true)]));
    }

    // ── Pruning must survive: widening the probe is not the same as giving up ──

    [Fact]
    public void AbsentValueInEveryForm_IsStillProofOfNoMatch()
    {
        var idx = Build(("Count", 5L), ("Region", "ae-dxb"));

        Assert.Equal([], idx.LookupIntersect([("Count", 7L)]));
        Assert.Equal([], idx.LookupIntersect([("Count", "7")]));
        Assert.Equal([], idx.LookupIntersect([("Region", "eu-fra")]));
        Assert.False(idx.MightContain("Count", 7L));
    }

    [Fact]
    public void UnknownProperty_IsStillNoInformation()
    {
        var idx = Build(("Count", 5L));

        Assert.Null(idx.LookupIntersect([("NoSuchProperty", 5L)]));
    }

    private static SegmentInvertedIndex Build(params (string Property, object? Value)[] pairs)
    {
        var build = new SegmentInvertedIndex();
        foreach (var (property, value) in pairs)
            build.Add(0u, property, value);
        return SegmentInvertedIndex.Deserialise(build.Serialise());
    }
}
