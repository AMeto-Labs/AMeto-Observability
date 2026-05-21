using Rd.Log.Core;
using Rd.Log.Query.Filtering;

namespace Rd.Log.Query.Tests;

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
