using Ameto.Tracing;
using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A FIELD THAT IS NOT THERE MUST NOT ANSWER COMPARISONS — issue #66.
///
/// <para><see cref="SpanRecord.HttpStatusCode"/> is a plain <c>short</c> and its own docstring says
/// "0 = not extracted from attributes". The predicate compared it straight, so every span carrying
/// no HTTP at all — a database call, a queue consumer, an internal operation — answered 0 to every
/// comparison. <c>{ .http.status_code != 200 }</c> and <c>{ .http.status_code &lt; 500 }</c> were
/// therefore true for the entire non-HTTP half of the application, silently: HTTP 200, no
/// exception, nothing in the log. A user asking for "everything that is not a 200" got the whole
/// traffic and no reason to doubt it.</para>
///
/// <para>THREE OPERATORS WERE WRONG, NOT ALL SIX, and the tests are split that way on purpose.
/// <c>=</c>, <c>&gt;</c> and <c>&gt;=</c> against a real status code were always right — <c>0 &gt;
/// 200</c> is false — so a fix that changed their answers would be a new bug, and
/// <see cref="Operators_that_were_already_correct_keep_their_answers"/> is what says so.</para>
///
/// <para>DECIDED IN THE PREDICATE, NOT IN THE RECORD, which is the part that reaches existing data.
/// The segment format persists this field, so every <c>.trc</c> ever written already stores "no
/// HTTP" and "status 0" as the same byte; a presence flag added to <see cref="SpanRecord"/> could
/// only ever separate them for spans written after it. Reading 0 as absent needs no format change
/// and is sound on its own terms — 0 is not an HTTP status, the range is 100-599, and the hint path
/// in <c>TraceQLExecutor</c> already refuses anything outside 100-999.</para>
/// </summary>
public sealed class TraceQLHttpStatusAbsenceTests
{
    private readonly ITestOutputHelper _out;
    public TraceQLHttpStatusAbsenceTests(ITestOutputHelper output) => _out = output;

    /// <summary>A span with no HTTP anywhere on it — the shape the bug was about.</summary>
    private static SpanRecord DatabaseCall() => new()
    {
        TraceId     = new TraceId(1, 2),
        SpanId      = new SpanId(3),
        Name        = "SELECT orders",
        ServiceName = "billing",
        Kind        = SpanKind.Client,
        Status      = SpanStatusCode.Ok,
        // HttpStatusCode deliberately left at its default.
    };

    private static SpanRecord HttpCall(short status) => new()
    {
        TraceId        = new TraceId(1, 4),
        SpanId         = new SpanId(5),
        Name           = "GET /orders",
        ServiceName    = "gateway",
        Kind           = SpanKind.Server,
        Status         = SpanStatusCode.Ok,
        HttpStatusCode = status,
    };

    private static bool Matches(string query, SpanRecord span) =>
        TraceQLParser.Parse(query).Evaluate(span);

    // ── The regression: three operators, one span with no HTTP ─────────────────

    [Theory]
    [InlineData("{ .http.status_code != 200 }")]   // "not the 200s" → was every non-HTTP span
    [InlineData("{ .http.status_code < 500 }")]    // "not an error"  → same
    [InlineData("{ .http.status_code <= 404 }")]
    [InlineData("{ .http.status_code != 0 }")]     // the degenerate form, wrong the other way round
    public void A_span_without_http_matches_no_comparison(string query)
    {
        var span = DatabaseCall();
        bool hit = Matches(query, span);

        _out.WriteLine($"{query} against a span with no HTTP → {hit}");
        Assert.False(hit);
    }

    [Fact]
    public void Operators_that_were_already_correct_keep_their_answers()
    {
        // These three read 0 and answered correctly by luck of the comparison's direction. The fix
        // must not move them — it only has to agree with them.
        var span = DatabaseCall();

        Assert.False(Matches("{ .http.status_code = 200 }",  span));
        Assert.False(Matches("{ .http.status_code > 200 }",  span));
        Assert.False(Matches("{ .http.status_code >= 200 }", span));
    }

    // ── The other half: a real status code still compares ──────────────────────

    [Fact]
    public void A_span_with_a_status_still_answers_every_operator()
    {
        // The point of the fix is to stop the absent field answering — not to stop the field
        // working. Each line here fails if the absence check swallowed a present value too.
        Assert.True (Matches("{ .http.status_code = 500 }",  HttpCall(500)));
        Assert.False(Matches("{ .http.status_code = 500 }",  HttpCall(200)));

        Assert.True (Matches("{ .http.status_code != 200 }", HttpCall(500)));
        Assert.False(Matches("{ .http.status_code != 200 }", HttpCall(200)));

        Assert.True (Matches("{ .http.status_code < 500 }",  HttpCall(404)));
        Assert.False(Matches("{ .http.status_code < 500 }",  HttpCall(500)));

        Assert.True (Matches("{ .http.status_code <= 404 }", HttpCall(404)));
        Assert.False(Matches("{ .http.status_code <= 404 }", HttpCall(405)));

        Assert.True (Matches("{ .http.status_code > 400 }",  HttpCall(500)));
        Assert.False(Matches("{ .http.status_code > 400 }",  HttpCall(200)));

        Assert.True (Matches("{ .http.status_code >= 400 }", HttpCall(400)));
        Assert.False(Matches("{ .http.status_code >= 400 }", HttpCall(399)));
    }

    // ── The half of the door this change does not close ───────────────────────

    /// <summary>
    /// PINNED, NOT ENDORSED — issue #74. Reading 0 as absent gives this predicate three outcomes
    /// while <c>NotPredicate</c> is a plain <c>!</c> over two, so the two spellings of one question
    /// part company: <c>{ .http.status_code != 200 }</c> excludes a span with no HTTP and
    /// <c>{ !(.http.status_code = 200) }</c> includes it — which is the #66 symptom, reached the
    /// other way round.
    ///
    /// <para>Both forms included it BEFORE this change too, so nothing regressed; what is new is
    /// that only one of them is fixed, and an asymmetry nobody wrote down is how the original
    /// defect lasted as long as it did. So it is written down here, as the answer the engine gives
    /// today rather than the answer it should give.</para>
    ///
    /// <para>THIS TEST IS MEANT TO FAIL EVENTUALLY. Making the AST three-valued — <c>Evaluate</c>
    /// returning <c>bool?</c>, three-valued tables on And/Or/Not — turns every expectation below
    /// into <c>False</c>. That failure is the point: it makes the fix announce itself here instead
    /// of passing silently, and whoever does it should delete this test rather than adjust it.</para>
    /// </summary>
    [Theory]
    [InlineData("{ !(.http.status_code = 200) }")]
    [InlineData("{ !(.http.status_code >= 400) }")]
    [InlineData("{ !(.http.status_code < 500) }")]
    public void Negation_over_an_absent_field_is_still_two_valued(string query)
    {
        var span = DatabaseCall();
        bool hit = Matches(query, span);

        _out.WriteLine($"{query} against a span with no HTTP → {hit} (today's answer, not the right one)");
        Assert.True(hit, "if this now returns false the AST became three-valued — delete this test");
    }

    [Fact]
    public void The_two_spellings_of_not_two_hundred_disagree_with_each_other()
    {
        // The finding stated as one line, so the gap is visible without reading either docstring:
        // one span, two queries a user reads as identical, opposite answers.
        var span = DatabaseCall();

        Assert.False(Matches("{ .http.status_code != 200 }",    span));
        Assert.True (Matches("{ !(.http.status_code = 200) }",  span));
    }

    [Fact]
    public void The_error_query_an_operator_actually_writes_selects_only_http_errors()
    {
        // The end-to-end shape of the bug, as a dashboard would hit it: one HTTP error, one HTTP
        // success, one span with no HTTP. Before the fix this returned two of the three.
        var spans = new[] { HttpCall(503), HttpCall(200), DatabaseCall() };
        var p     = TraceQLParser.Parse("{ .http.status_code >= 400 || .http.status_code < 200 }");

        var hits = new List<string>();
        foreach (var s in spans) if (p.Evaluate(s)) hits.Add($"{s.Name}/{s.HttpStatusCode}");

        _out.WriteLine($"matched: {string.Join(", ", hits)}");
        Assert.Equal(new[] { "GET /orders/503" }, hits);
    }
}
