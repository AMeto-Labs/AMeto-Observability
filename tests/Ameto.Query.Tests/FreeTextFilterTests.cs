using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// Free-text search: bare words typed into the filter box become a
/// <see cref="FreeTextNode"/> that substring-matches the same text fields the
/// trigram index covers (message template, exception type/message, string property
/// values), so index-accelerated and full-scan queries agree.
/// </summary>
public sealed class FreeTextFilterTests
{
    private static LogEvent MakeEvent(
        string template                    = "Hello {Name}",
        ExceptionInfo? exception           = null,
        string? serviceName                = null,
        Dictionary<string, object?>? props = null) => new()
    {
        Id              = new EventId(0u, 1u),
        Timestamp       = DateTimeOffset.UtcNow,
        Level           = LogLevel.Information,
        MessageTemplate = template,
        Exception       = exception,
        ServiceName     = serviceName,
        Properties      = props,
    };

    private static bool Eval(string? filter, LogEvent ev) =>
        CompiledFilter.Compile(filter).Matches(ev);

    // ── Parser ──────────────────────────────────────────────────────────────────

    [Fact]
    public void Parse_BareWord_ReturnsFreeTextNode()
    {
        var ft = Assert.IsType<FreeTextNode>(FilterParser.Parse("balance"));
        Assert.Equal(new[] { "balance" }, ft.Terms);
    }

    [Fact]
    public void Parse_MultipleBareWords_CollectsAllTerms()
    {
        var ft = Assert.IsType<FreeTextNode>(FilterParser.Parse("user balance transfer"));
        Assert.Equal(new[] { "user", "balance", "transfer" }, ft.Terms);
    }

    [Fact]
    public void Parse_QuotedString_IsSingleFreeTextTerm()
    {
        var ft = Assert.IsType<FreeTextNode>(FilterParser.Parse("'db timeout'"));
        Assert.Equal(new[] { "db timeout" }, ft.Terms);
    }

    [Fact]
    public void Parse_BareLevelWord_StillReturnsLevelNode()
    {
        // Backward-compat: a lone level name keeps its level-filter meaning.
        Assert.IsType<LevelNode>(FilterParser.Parse("Error"));
    }

    [Fact]
    public void Parse_FreeTextAndStructuredClause_Mixes()
    {
        var and = Assert.IsType<AndNode>(FilterParser.Parse("balance and @l = 'Error'"));
        var ft  = Assert.IsType<FreeTextNode>(and.Left);
        Assert.Equal(new[] { "balance" }, ft.Terms);
        Assert.IsType<CompareNode>(and.Right);
    }

    [Fact]
    public void Parse_WordFollowedByOperator_IsComparisonNotFreeText()
    {
        // `balance = 5` must stay a comparison — the run stops before an operator LHS.
        Assert.IsType<CompareNode>(FilterParser.Parse("balance = 5"));
    }

    // ── Evaluator: matches ──────────────────────────────────────────────────────

    [Fact]
    public void Match_TermInMessageTemplate()
    {
        var ev = MakeEvent(template: "Insufficient balance for user {UserId}");
        Assert.True(Eval("balance", ev));
    }

    [Fact]
    public void Match_TermInStringPropertyValue()
    {
        var ev = MakeEvent(template: "Processing {Operation}",
                           props: new() { ["Operation"] = "balance transfer" });
        Assert.True(Eval("balance", ev));
    }

    [Fact]
    public void Match_TermInNestedPropertyValue()
    {
        var ev = MakeEvent(props: new()
        {
            ["User"] = new Dictionary<string, object?> { ["Role"] = "administrator" },
        });
        Assert.True(Eval("admin", ev));
    }

    [Fact]
    public void Match_StringAtFlattenDepthLimit_IsFound()
    {
        // Nested exactly at the index's flatten-depth cap (5) → still trigrammed → matched.
        Assert.True(Eval("deepvalue", MakeEvent(props: NestDict(5, "deepvalue"))));
    }

    [Fact]
    public void NoMatch_StringNestedBeyondFlattenDepth()
    {
        // One level past the cap → not trigram-indexed, so free-text must not match it
        // either (keeps cold-tier index skips consistent with the hot-tier scan).
        Assert.False(Eval("deepvalue", MakeEvent(props: NestDict(6, "deepvalue"))));
    }

    /// <summary>Builds top-level props whose leaf string sits <paramref name="levels"/> nested maps deep.</summary>
    private static Dictionary<string, object?> NestDict(int levels, string leaf)
    {
        object? cur = leaf;
        for (int i = 0; i < levels; i++)
            cur = new Dictionary<string, object?> { ["n"] = cur };
        return new Dictionary<string, object?> { ["leaf"] = cur };
    }

    [Fact]
    public void Match_TermInExceptionMessage()
    {
        var ev = MakeEvent(exception: new ExceptionInfo
        {
            Type = "System.TimeoutException", Message = "Balance service did not respond",
        });
        Assert.True(Eval("respond", ev));
    }

    [Fact]
    public void Match_IsCaseInsensitive()
    {
        var ev = MakeEvent(template: "Insufficient BALANCE");
        Assert.True(Eval("balance", ev));
        Assert.True(Eval("BaLaNcE", ev));
    }

    [Fact]
    public void Match_MultipleTerms_RequiresAll()
    {
        var ev = MakeEvent(template: "user did something",
                           props: new() { ["Operation"] = "balance" });
        Assert.True(Eval("user balance", ev));   // both present (template + prop)
        Assert.False(Eval("user missing", ev));  // "missing" is absent
    }

    // ── Evaluator: non-matches (kept consistent with trigram coverage) ──────────

    [Fact]
    public void NoMatch_TermAbsent()
    {
        Assert.False(Eval("balance", MakeEvent(template: "Hello {Name}")));
    }

    [Fact]
    public void NoMatch_NumericPropertyValue()
    {
        // Numbers aren't trigram-indexed → free-text must not match them, so the
        // cold-tier index fast-path stays consistent with the hot-tier scan.
        var ev = MakeEvent(props: new() { ["StatusCode"] = (long)404 });
        Assert.False(Eval("404", ev));
    }

    [Fact]
    public void NoMatch_ServiceName()
    {
        // service.name is inverted/bloom-indexed but NOT trigram-indexed.
        var ev = MakeEvent(template: "Hello {Name}", serviceName: "BalanceService");
        Assert.False(Eval("balance", ev));
    }

    // ── CompiledFilter trigram hints (the "fast" path) ──────────────────────────

    [Fact]
    public void TrigramHint_EmittedForTermsOfThreePlusChars()
    {
        var hints = CompiledFilter.Compile("balance").GetTrigramHints();
        Assert.Contains(hints, h => h.text == "balance");
    }

    [Fact]
    public void TrigramHint_NotEmittedForShortTerms()
    {
        // A 2-char term can't form a trigram — no hint, falls back to scan.
        Assert.Empty(CompiledFilter.Compile("ab").GetTrigramHints());
    }

    [Fact]
    public void TrigramHint_OnePerTermInMultiWord()
    {
        Assert.Equal(2, CompiledFilter.Compile("user balance").GetTrigramHints().Count);
    }
}
