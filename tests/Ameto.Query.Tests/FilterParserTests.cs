using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

public sealed class FilterParserTests
{
    // ── Match-all / empty ─────────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_NullOrWhitespace_ReturnsMatchAll(string? expr)
    {
        var node = FilterParser.Parse(expr);
        Assert.IsType<MatchAllNode>(node);
    }

    // ── Comparisons ───────────────────────────────────────────────────────────

    [Fact]
    public void Parse_LevelEquals_ReturnsCompareNode()
    {
        var node = FilterParser.Parse("@l = 'Error'");
        var cmp  = Assert.IsType<CompareNode>(node);
        Assert.Equal("@l",    cmp.Property);
        Assert.Equal(CompareOp.Eq, cmp.Op);
        Assert.Equal("Error", cmp.Value);
    }

    [Fact]
    public void Parse_PropertyNotEqual_ReturnsCompareNode()
    {
        var node = FilterParser.Parse("StatusCode != 200");
        var cmp  = Assert.IsType<CompareNode>(node);
        Assert.Equal("StatusCode", cmp.Property);
        Assert.Equal(CompareOp.Ne, cmp.Op);
        Assert.Equal(200L, cmp.Value);
    }

    [Fact]
    public void Parse_NumericGreaterThan_ReturnsCompareNode()
    {
        var node = FilterParser.Parse("Elapsed > 500");
        var cmp  = Assert.IsType<CompareNode>(node);
        Assert.Equal("Elapsed", cmp.Property);
        Assert.Equal(CompareOp.Gt, cmp.Op);
        Assert.Equal(500L, cmp.Value!);
    }

    [Fact]
    public void Parse_NumericLessOrEqual_ReturnsCompareNode()
    {
        var node = FilterParser.Parse("Elapsed <= 100");
        var cmp  = Assert.IsType<CompareNode>(node);
        Assert.Equal(CompareOp.Le, cmp.Op);
    }

    // ── Logical connectives ───────────────────────────────────────────────────

    [Fact]
    public void Parse_AndExpression_ReturnsAndNode()
    {
        var node = FilterParser.Parse("@l = 'Error' and isDefined(UserId)");
        Assert.IsType<AndNode>(node);
    }

    [Fact]
    public void Parse_OrExpression_ReturnsOrNode()
    {
        var node = FilterParser.Parse("@l = 'Error' or @l = 'Fatal'");
        Assert.IsType<OrNode>(node);
    }

    [Fact]
    public void Parse_NotExpression_ReturnsNotNode()
    {
        var node = FilterParser.Parse("not has(UserId)");
        var not  = Assert.IsType<NotNode>(node);
        Assert.IsType<HasNode>(not.Operand);
    }

    [Fact]
    public void Parse_GroupedPrecedence_AndBindsTighterThanOr()
    {
        // A or B and C  →  A or (B and C)  → OrNode( A, AndNode(B, C) )
        var node = FilterParser.Parse("@l = 'Verbose' or @l = 'Debug' and has(X)");
        var or   = Assert.IsType<OrNode>(node);
        Assert.IsType<CompareNode>(or.Left);
        Assert.IsType<AndNode>(or.Right);
    }

    // ── Functions ─────────────────────────────────────────────────────────────

    [Fact]
    public void Parse_HasFunction_ReturnsHasNode()
    {
        var node = FilterParser.Parse("has(UserId)");
        var has  = Assert.IsType<HasNode>(node);
        Assert.Equal("UserId", has.Property);
    }

    [Fact]
    public void Parse_IsDefinedFunction_ReturnsIsDefinedNode()
    {
        var node = FilterParser.Parse("isDefined(RequestId)");
        var def  = Assert.IsType<IsDefinedNode>(node);
        Assert.Equal("RequestId", def.Property);
    }

    [Fact]
    public void Parse_StartsWithFunction_ReturnsStarsWithNode()
    {
        var node = FilterParser.Parse("startsWith(@mt, 'User')");
        var sw   = Assert.IsType<StartsWithNode>(node);
        Assert.Equal("@mt",  sw.Property);
        Assert.Equal("User", sw.Prefix);
        Assert.False(sw.CaseInsensitive);
    }

    [Fact]
    public void Parse_CiStartsWith_IsCaseInsensitive()
    {
        var node = FilterParser.Parse("ci_startsWith(@mt, 'user')");
        var sw   = Assert.IsType<StartsWithNode>(node);
        Assert.True(sw.CaseInsensitive);
    }

    [Fact]
    public void Parse_ContainsFunction_ReturnsContainsNode()
    {
        var node = FilterParser.Parse("contains(@mt, 'failed')");
        var ct   = Assert.IsType<ContainsNode>(node);
        Assert.Equal("@mt",    ct.Property);
        Assert.Equal("failed", ct.Text);
        Assert.False(ct.CaseInsensitive);
    }

    [Fact]
    public void Parse_CiContains_IsCaseInsensitive()
    {
        var node = FilterParser.Parse("ci_contains(Message, 'hello')");
        var ct   = Assert.IsType<ContainsNode>(node);
        Assert.True(ct.CaseInsensitive);
    }

    [Fact]
    public void Parse_EndsWithFunction_ReturnsEndsWithNode()
    {
        var node = FilterParser.Parse("endsWith(@mt, 'done')");
        var ew   = Assert.IsType<EndsWithNode>(node);
        Assert.Equal("@mt", ew.Property);
        Assert.Equal("done", ew.Suffix);
    }

    [Fact]
    public void Parse_LengthCompare_ReturnsLengthCompareNode()
    {
        var node = FilterParser.Parse("length(UserId) > 3");
        var ln   = Assert.IsType<LengthCompareNode>(node);
        Assert.Equal("UserId", ln.Property);
        Assert.Equal(CompareOp.Gt, ln.Op);
        Assert.Equal(3L, ln.Value);
    }

    [Fact]
    public void Parse_ToJsonCompare_ReturnsToJsonCompareNode()
    {
        var node = FilterParser.Parse("toJson(StatusCode) = '200'");
        var tj   = Assert.IsType<ToJsonCompareNode>(node);
        Assert.Equal("StatusCode", tj.Property);
        Assert.Equal(CompareOp.Eq, tj.Op);
        Assert.Equal("200", tj.Value);
    }

    [Fact]
    public void Parse_FromJsonCompare_ReturnsFromJsonCompareNode()
    {
        var node = FilterParser.Parse("fromJson(Payload) = 42");
        var fj   = Assert.IsType<FromJsonCompareNode>(node);
        Assert.Equal("Payload", fj.Property);
        Assert.Equal(CompareOp.Eq, fj.Op);
        Assert.Equal(42L, fj.Value);
    }

    [Fact]
    public void Parse_FromJsonPath_DotNotation_ReturnsFromJsonPathCompareNode()
    {
        var node = FilterParser.Parse("fromJson(Body).merchBal = 155");
        var fjp  = Assert.IsType<FromJsonPathCompareNode>(node);
        Assert.Equal("Body", fjp.Property);
        Assert.Equal(new[] { "merchBal" }, fjp.Path);
        Assert.Equal(CompareOp.Eq, fjp.Op);
        Assert.Equal(155L, fjp.Value);
    }

    [Fact]
    public void Parse_FromJsonPath_NestedDot_ReturnsMultipleSegments()
    {
        var node = FilterParser.Parse("fromJson(Body).user.balance.amount > 0");
        var fjp  = Assert.IsType<FromJsonPathCompareNode>(node);
        Assert.Equal(new[] { "user", "balance", "amount" }, fjp.Path);
    }

    [Fact]
    public void Parse_FromJsonPath_BracketNotation_Works()
    {
        var node = FilterParser.Parse("fromJson(Body)['user']['name'] = 'alice'");
        var fjp  = Assert.IsType<FromJsonPathCompareNode>(node);
        Assert.Equal(new[] { "user", "name" }, fjp.Path);
    }

    [Fact]
    public void Parse_FromJsonPath_MixedNotation_Works()
    {
        var node = FilterParser.Parse("fromJson(Body).items[0] = 'first'");
        var fjp  = Assert.IsType<FromJsonPathCompareNode>(node);
        Assert.Equal(new[] { "items", "0" }, fjp.Path);
    }

    [Fact]
    public void Parse_CoalesceCompare_ReturnsCoalesceCompareNode()
    {
        var node = FilterParser.Parse("coalesce(UserId, RequestId, 'none') = 'alice'");
        var co   = Assert.IsType<CoalesceCompareNode>(node);
        Assert.Equal(3, co.Args.Count);
        Assert.Equal(CompareOp.Eq, co.Op);
        Assert.Equal("alice", co.Value);
    }

    [Fact]
    public void Parse_ToLowerCompare_ReturnsToLowerCompareNode()
    {
        var node = FilterParser.Parse("toLower(UserName) = 'alice'");
        Assert.IsType<ToLowerCompareNode>(node);
    }

    [Fact]
    public void Parse_ToUpperCompare_ReturnsToUpperCompareNode()
    {
        var node = FilterParser.Parse("toUpper(UserName) = 'ALICE'");
        Assert.IsType<ToUpperCompareNode>(node);
    }

    [Fact]
    public void Parse_ToNumberCompare_ReturnsToNumberCompareNode()
    {
        var node = FilterParser.Parse("toNumber(StatusCode) >= 400");
        Assert.IsType<ToNumberCompareNode>(node);
    }

    [Fact]
    public void Parse_SubstringCompare_ReturnsSubstringCompareNode()
    {
        var node = FilterParser.Parse("substring(Path, 1, 3) = 'api'");
        Assert.IsType<SubstringCompareNode>(node);
    }

    [Fact]
    public void Parse_IndexOfCompare_ReturnsIndexOfCompareNode()
    {
        var node = FilterParser.Parse("indexOf(Path, '/api') = 0");
        Assert.IsType<IndexOfCompareNode>(node);
    }

    [Fact]
    public void Parse_LastIndexOfCompare_ReturnsIndexOfCompareNode()
    {
        var node = FilterParser.Parse("lastIndexOf(Path, '/') >= 0");
        var idx  = Assert.IsType<IndexOfCompareNode>(node);
        Assert.True(idx.LastIndex);
    }

    [Fact]
    public void Parse_ReplaceCompare_ReturnsReplaceCompareNode()
    {
        var node = FilterParser.Parse("replace(Path, '/api', '/v1') = '/v1/users'");
        Assert.IsType<ReplaceCompareNode>(node);
    }

    [Fact]
    public void Parse_ConcatCompare_ReturnsConcatCompareNode()
    {
        var node = FilterParser.Parse("concat(FirstName, ' ', LastName) = 'John Smith'");
        var cc   = Assert.IsType<ConcatCompareNode>(node);
        Assert.Equal(3, cc.Args.Count);
    }

    [Fact]
    public void Parse_CiEndsWith_ReturnsEndsWithNode()
    {
        var node = FilterParser.Parse("ci_endsWith(@mt, 'done')");
        var ew   = Assert.IsType<EndsWithNode>(node);
        Assert.True(ew.CaseInsensitive);
    }

    [Fact]
    public void Parse_TypeOfCompare_ReturnsTypeOfCompareNode()
    {
        var node = FilterParser.Parse("typeOf(UserId) = 'string'");
        Assert.IsType<TypeOfCompareNode>(node);
    }

    [Fact]
    public void Parse_ElementAtCompare_ReturnsElementAtCompareNode()
    {
        var node = FilterParser.Parse("elementAt(Tags, 0) = 'first'");
        Assert.IsType<ElementAtCompareNode>(node);
    }

    [Fact]
    public void Parse_KeysCompare_ReturnsKeysCompareNode()
    {
        var node = FilterParser.Parse("keys(Context) != null");
        Assert.IsType<KeysCompareNode>(node);
    }

    [Fact]
    public void Parse_ValuesCompare_ReturnsValuesCompareNode()
    {
        var node = FilterParser.Parse("values(Context) != null");
        Assert.IsType<ValuesCompareNode>(node);
    }

    [Fact]
    public void Parse_RoundCompare_ReturnsRoundCompareNode()
    {
        var node = FilterParser.Parse("round(Duration, 2) = 3.14");
        Assert.IsType<RoundCompareNode>(node);
    }

    [Fact]
    public void Parse_NowCompare_ReturnsNowCompareNode()
    {
        var node = FilterParser.Parse("now() > 0");
        Assert.IsType<NowCompareNode>(node);
    }

    [Fact]
    public void Parse_DateTimeCompare_ReturnsDateTimeCompareNode()
    {
        var node = FilterParser.Parse("dateTime(StartAt) > 0");
        Assert.IsType<DateTimeCompareNode>(node);
    }

    [Fact]
    public void Parse_ToIsoStringCompare_ReturnsToIsoStringCompareNode()
    {
        var node = FilterParser.Parse("toIsoString(StartedTicks, 0) != null");
        Assert.IsType<ToIsoStringCompareNode>(node);
    }

    [Fact]
    public void Parse_DatePartCompare_ReturnsDatePartCompareNode()
    {
        var node = FilterParser.Parse("datePart(StartedAt, 'hour', 0) >= 0");
        Assert.IsType<DatePartCompareNode>(node);
    }

    [Fact]
    public void Parse_TimeOfDayCompare_ReturnsTimeOfDayCompareNode()
    {
        var node = FilterParser.Parse("timeOfDay(StartedAt, 0) >= 0");
        Assert.IsType<TimeOfDayCompareNode>(node);
    }

    [Fact]
    public void Parse_TimeSpanCompare_ReturnsTimeSpanCompareNode()
    {
        var node = FilterParser.Parse("timeSpan(DurationText) > 0");
        Assert.IsType<TimeSpanCompareNode>(node);
    }

    [Fact]
    public void Parse_TotalMillisecondsCompare_ReturnsTotalMillisecondsCompareNode()
    {
        var node = FilterParser.Parse("totalMilliseconds(DurationTicks) >= 1000");
        Assert.IsType<TotalMillisecondsCompareNode>(node);
    }

    [Fact]
    public void Parse_ToTimeStringCompare_ReturnsToTimeStringCompareNode()
    {
        var node = FilterParser.Parse("toTimeString(DurationTicks) != null");
        Assert.IsType<ToTimeStringCompareNode>(node);
    }

    [Fact]
    public void Parse_ToHexStringCompare_ReturnsToHexStringCompareNode()
    {
        var node = FilterParser.Parse("toHexString(StatusCode) = '0xc8'");
        Assert.IsType<ToHexStringCompareNode>(node);
    }

    [Fact]
    public void Parse_BucketCompare_ReturnsBucketCompareNode()
    {
        var node = FilterParser.Parse("bucket(Duration, 0.1) > 0");
        Assert.IsType<BucketCompareNode>(node);
    }

    [Fact]
    public void Parse_OffsetInCompare_ReturnsOffsetInCompareNode()
    {
        var node = FilterParser.Parse("offsetIn('UTC', StartedAt) = 0");
        Assert.IsType<OffsetInCompareNode>(node);
    }

    [Fact]
    public void Parse_ArrivedCompare_ReturnsArrivedCompareNode()
    {
        var node = FilterParser.Parse("arrived(@id) > 0");
        Assert.IsType<ArrivedCompareNode>(node);
    }

    // ── In operator ───────────────────────────────────────────────────────────

    [Fact]
    public void Parse_InOperator_ReturnsInNode()
    {
        var node = FilterParser.Parse("@l in ['Error', 'Fatal']");
        var inN  = Assert.IsType<InNode>(node);
        Assert.Equal("@l", inN.Property);
        Assert.Equal(2,    inN.Values.Length);
    }

    // ── Parentheses ───────────────────────────────────────────────────────────

    [Fact]
    public void Parse_ParenthesisedOr_InsideAnd_RespectsGrouping()
    {
        // (A or B) and C  →  AndNode( OrNode(A,B), C )
        var node = FilterParser.Parse("(@l = 'Error' or @l = 'Fatal') and has(X)");
        var and  = Assert.IsType<AndNode>(node);
        Assert.IsType<OrNode>(and.Left);
        Assert.IsType<HasNode>(and.Right);
    }
}
