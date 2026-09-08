using Ameto.Tracing;
using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A SPAN THAT CANNOT ANSWER MUST NOT ANSWER "NO" — issue #74.
///
/// <para><c>SpanPredicate.Evaluate</c> used to return <c>bool</c>, so a field that was not on the
/// span had to be folded into <c>false</c>, and <c>!</c> turned that into <c>true</c>. The two
/// spellings of one question then disagreed: <c>{ .http.status_code != 200 }</c> excluded a span
/// with no HTTP while <c>{ !(.http.status_code = 200) }</c> included it — the #66 symptom, reached
/// through the negation instead of the operator.</para>
///
/// <para>THE FIX HAD TO BE ON THE CONNECTIVES, not on the one predicate with an absent state, and
/// the composition test below is why. Teaching <c>NotPredicate</c> to ask its inner predicate
/// whether it applies gives the wrong answer for <c>NOT (unknown AND false)</c>: the conjunction is
/// decided — false — even though an operand could not answer, so its negation is <c>true</c> and
/// not unknown. Unknownness belongs to each truth table rather than being inherited.</para>
///
/// <para>THE TESTS ASSERT THE THREE-VALUED ANSWER DIRECTLY, on <c>Evaluate</c>, rather than through
/// the executor's "only true selects" collapse. Both matter and they are different claims: a
/// predicate returning <c>false</c> where it should return <c>null</c> is invisible at the top
/// level and changes everything the moment it appears under a <c>!</c>.</para>
/// </summary>
public sealed class TraceQLThreeValuedTests
{
    private readonly ITestOutputHelper _out;
    public TraceQLThreeValuedTests(ITestOutputHelper output) => _out = output;

    /// <summary>A span carrying exactly one attribute, so "absent" is a real state and not a null map.</summary>
    private static SpanRecord WithTenant(string tenant) => new()
    {
        TraceId     = new TraceId(1, 2),
        SpanId      = new SpanId(3),
        Name        = "SELECT orders",
        ServiceName = "billing",
        Kind        = SpanKind.Client,
        Status      = SpanStatusCode.Ok,
        Attributes  = new Dictionary<string, object?> { ["tenant"] = tenant },
    };

    private static bool? Eval(string query, SpanRecord span) =>
        TraceQLParser.Parse(query).Evaluate(span);

    // Building blocks, chosen so each is unambiguous against WithTenant("acme"):
    private const string True    = ".tenant = \"acme\"";      // present and equal
    private const string False   = ".tenant = \"other\"";     // present and different
    private const string Unknown = ".missing = \"anything\""; // not on the span at all

    // ── The three connectives, one row of the truth table each ────────────────

    [Theory]
    // AND: false decides, unknown only survives when nothing decides.
    [InlineData(True,    "&&", True,    true)]
    [InlineData(True,    "&&", False,   false)]
    [InlineData(False,   "&&", Unknown, false)]   // decided despite an operand that cannot answer
    [InlineData(Unknown, "&&", False,   false)]   // and in the other order, which short-circuiting could break
    [InlineData(True,    "&&", Unknown, null)]
    [InlineData(Unknown, "&&", Unknown, null)]
    // OR: true decides, mirror image.
    [InlineData(False,   "||", False,   false)]
    [InlineData(True,    "||", Unknown, true)]
    [InlineData(Unknown, "||", True,    true)]
    [InlineData(False,   "||", Unknown, null)]
    [InlineData(Unknown, "||", Unknown, null)]
    public void The_connectives_follow_the_three_valued_tables(
        string left, string op, string right, bool? expected)
    {
        string query = $"{{ {left} {op} {right} }}";
        bool?  got   = Eval(query, WithTenant("acme"));

        _out.WriteLine($"{query} → {got?.ToString() ?? "unknown"}");
        Assert.Equal(expected, got);
    }

    [Theory]
    [InlineData(True,    false)]
    [InlineData(False,   true)]
    [InlineData(Unknown, null)]    // the line the whole issue is about
    public void Negation_carries_unknown_through(string inner, bool? expected)
    {
        bool? got = Eval($"{{ !({inner}) }}", WithTenant("acme"));

        _out.WriteLine($"!({inner}) → {got?.ToString() ?? "unknown"}");
        Assert.Equal(expected, got);
    }

    /// <summary>
    /// The case that rules out the cheap fix. If <c>NotPredicate</c> decided unknownness by asking
    /// its inner predicate "do you apply to this span?", this would answer unknown — because one
    /// operand does not apply — and that is wrong: the conjunction is already false without it, so
    /// the negation is true. Only per-connective truth tables get this right.
    /// </summary>
    [Fact]
    public void Not_of_an_unknown_and_a_false_is_true_not_unknown()
    {
        var span = WithTenant("acme");

        Assert.False(Eval($"{{ {Unknown} && {False} }}", span));
        Assert.True (Eval($"{{ !({Unknown} && {False}) }}", span));
    }

    // ── The general attribute case, which is #66 outside HTTP ─────────────────

    [Fact]
    public void An_absent_attribute_answers_no_comparison_and_no_negation_of_one()
    {
        var span = WithTenant("acme");

        // Before #74 the first was false and the second true, so "spans whose region is not eu"
        // returned every span that had never heard of a region.
        Assert.Null(Eval("{ .region != \"eu\" }",    span));
        Assert.Null(Eval("{ !(.region = \"eu\") }",  span));
    }

    /// <summary>
    /// PRESENT BUT INCOMPARABLE IS STILL TWO-VALUED, and this test exists so that stays a decision
    /// rather than an oversight. A span that HAS the attribute is not a span that cannot answer, so
    /// a type mismatch keeps returning false — which does leave a narrower version of the same
    /// asymmetry standing, visible in the second pair below. Changing it is a separate semantic
    /// question from the one #74 asked; if it is ever changed, this test is where it announces
    /// itself.
    /// </summary>
    [Fact]
    public void A_type_mismatch_is_still_two_valued()
    {
        var span = WithTenant("bananas");

        Assert.False(Eval("{ .tenant > 5 }",    span));
        Assert.True (Eval("{ !(.tenant > 5) }", span));   // the residual asymmetry, pinned
    }

    // ── Presence, the question three-valued logic makes necessary ─────────────

    [Fact]
    public void Nil_asks_about_presence_rather_than_value()
    {
        var span = WithTenant("acme");

        Assert.True (Eval("{ .tenant != nil }",  span));
        Assert.False(Eval("{ .tenant = nil }",   span));
        Assert.False(Eval("{ .region != nil }",  span));
        Assert.True (Eval("{ .region = nil }",   span));
    }

    /// <summary>
    /// Presence is never unknown — that is what makes it usable to find absence. A predicate that
    /// answered null for a missing attribute here would be unable to report the very thing it was
    /// added for.
    /// </summary>
    [Fact]
    public void A_presence_test_always_answers()
    {
        var span = WithTenant("acme");

        Assert.NotNull(Eval("{ .region = nil }",  span));
        Assert.NotNull(Eval("{ .region != nil }", span));
    }

    [Fact]
    public void The_quoted_string_nil_is_still_a_value_not_a_presence_test()
    {
        // `nil` is a bare word; "nil" is text. Without that distinction this query would silently
        // become a presence test and stop matching the span it was written for.
        var span = WithTenant("nil");

        Assert.True(Eval("{ .tenant = \"nil\" }", span));
        Assert.True(Eval("{ .tenant != nil }",    span));   // it IS present — its value is the text
    }

    [Theory]
    [InlineData("{ .tenant < nil }")]
    [InlineData("{ .tenant >= nil }")]
    public void Ordering_against_nil_is_refused(string query)
    {
        var ex = Assert.Throws<TraceQLException>(() => TraceQLParser.Parse(query));
        _out.WriteLine(ex.Message);
        Assert.Contains("nil", ex.Message);
    }

    /// <summary>
    /// Intrinsics are on every span, so <c>nil</c> against one asks nothing — and the answer has to
    /// be an error rather than silence. Without the guard, <c>{ service = nil }</c> builds a
    /// <c>ServicePredicate</c> comparing the service name to the four-letter string "nil": a query
    /// that looks like a presence test, matches almost nothing, and reports no problem.
    /// </summary>
    [Theory]
    [InlineData("{ service = nil }")]
    [InlineData("{ name != nil }")]
    [InlineData("{ duration = nil }")]
    [InlineData("{ kind = nil }")]
    [InlineData("{ status != nil }")]
    public void Nil_against_an_intrinsic_is_refused(string query)
    {
        var ex = Assert.Throws<TraceQLException>(() => TraceQLParser.Parse(query));
        _out.WriteLine(ex.Message);
        Assert.Contains("every span", ex.Message);
    }

    /// <summary>
    /// A PRESENCE TEST MUST NOT BECOME A PRESENCE HINT, which is the one way this feature could
    /// have lost spans instead of finding them.
    ///
    /// <para><c>TraceQLExecutor.Collect</c> turns every <c>AttributePredicate</c> in an AND-chain
    /// into an <c>AttrHint</c>, on the stated grounds that "a missing attribute never matches,
    /// whatever the op" — which is true for comparisons and exactly inverted for <c>= nil</c>.
    /// Had presence reused <c>AttributePredicate</c>, <c>{ .region = nil }</c> would have emitted a
    /// hint requiring <c>.region</c> to exist, and the per-block bloom skip would then have dropped
    /// precisely the blocks holding the answer: a query for "spans missing a region" returning
    /// nothing, silently, on a bloom that did its job. A separate predicate type is what makes the
    /// switch fall through to no hint at all.</para>
    /// </summary>
    [Fact]
    public void A_presence_test_contributes_no_attribute_hint()
    {
        var hints = TraceQLExecutor.ExtractHints(TraceQLParser.Parse("{ .region = nil }"));
        Assert.Null(hints.AttrHints);

        // The comparison form still does — that is the behaviour being preserved, not removed.
        var cmp = TraceQLExecutor.ExtractHints(TraceQLParser.Parse("{ .region = \"eu\" }"));
        Assert.NotNull(cmp.AttrHints);

        // And in an AND-chain the presence half contributes nothing while the other half still does.
        var mixed = TraceQLExecutor.ExtractHints(
            TraceQLParser.Parse("{ .region = nil && .tenant = \"acme\" }"));
        _out.WriteLine($"hints for the mixed chain: {mixed.AttrHints?.Count ?? 0}");
        Assert.Single(mixed.AttrHints!);
    }

    // ── The promoted field reads presence from the same place it reads absence ─

    [Fact]
    public void Http_status_presence_comes_from_the_promoted_field()
    {
        var noHttp = WithTenant("acme");
        var http   = new SpanRecord
        {
            TraceId        = new TraceId(1, 4),
            SpanId         = new SpanId(5),
            Name           = "GET /orders",
            ServiceName    = "gateway",
            Kind           = SpanKind.Server,
            Status         = SpanStatusCode.Ok,
            HttpStatusCode = 503,
        };

        Assert.True (Eval("{ .http.status_code = nil }",  noHttp));
        Assert.False(Eval("{ .http.status_code != nil }", noHttp));
        Assert.False(Eval("{ .http.status_code = nil }",  http));
        Assert.True (Eval("{ .http.status_code != nil }", http));

        // The two must not contradict each other: a span the presence test calls absent cannot be
        // one the comparison matches. Answering presence out of the attribute map instead of the
        // promoted field is exactly how that contradiction would arise.
        Assert.True(Eval("{ .http.status_code = 503 }", http));
        Assert.Null(Eval("{ .http.status_code = 503 }", noHttp));
    }
}
