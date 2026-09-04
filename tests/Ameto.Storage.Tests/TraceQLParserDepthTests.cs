using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE ONE FAILURE IN THIS SERVICE THAT NO CATCH CAN HOLD.
///
/// <para>Every other fault in this branch is a bad answer: a short page calling itself whole, a
/// rent that asks for 64 MB out of a 3.5 KB file. <c>StackOverflowException</c> is not an answer at
/// all — .NET does not let it be caught, so the runtime tears the process down, taking every other
/// in-flight request, the ingest pipeline and every unwritten WAL buffer with it.</para>
///
/// <para>MEASURED AGAINST THE PARENT COMMIT, with a probe that parses and prints:
/// <c>{((((…))))}</c> survives 1500 levels and dies at 1700 — <b>a 3406-character query</b>. The
/// stack trace is <c>ParseOr → ParseExpr → ParsePrimary → ParseUnary → ParseAnd</c> repeated 1690
/// times. Kestrel's default request line is 8192 bytes and neither <c>(</c> nor <c>)</c> is escaped
/// by <c>encodeURIComponent</c>, so the <c>GET /api/traces/query/stream?ql=</c> this branch
/// introduces carries it in one request, from a browser address bar. The <c>!</c> form needs about
/// 20 000 characters and so only fits the POST body — which the length ceiling now bounds too.</para>
///
/// <para>The tests below cannot demonstrate the crash: a stack overflow would take the test host
/// with it. They demonstrate the refusal, and the measurement above is the evidence for what they
/// prevent — with <see cref="Nesting_far_past_the_measured_crash_depth_is_refused"/> sitting at a
/// depth the parent commit is already dead at.</para>
/// </summary>
public sealed class TraceQLParserDepthTests
{
    private readonly ITestOutputHelper _out;
    public TraceQLParserDepthTests(ITestOutputHelper output) => _out = output;

    private static string Nested(int n) => "{" + new string('(', n) + ".a=1" + new string(')', n) + "}";

    [Fact]
    public void Nesting_far_past_the_measured_crash_depth_is_refused()
    {
        // 2000 — past the 1700 that killed the probe process, so a build without the counter does
        // not fail this test, it fails the RUN.
        var ex = Assert.Throws<TraceQLException>(() => TraceQLParser.Parse(Nested(2000)));
        _out.WriteLine(ex.Message);

        // A TraceQLException and not something else, because that type is the contract both entry
        // points already have: a 400 on POST /api/traces/query, a `query-error` frame on the SSE
        // stream. Anything else escapes as a 500 or, on the stream, as an anonymous EventSource
        // `error` the page cannot put words to.
        Assert.Contains("nests", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void The_not_operator_is_bounded_by_the_same_counter()
    {
        // The second recursive edge. It recurses straight back into ParseUnary rather than through
        // ParsePrimary, so a counter placed one method over would have missed it — and it is the
        // cheaper attack per character (one frame per '!' against five per paren).
        var ex = Assert.Throws<TraceQLException>(
            () => TraceQLParser.Parse("{" + new string('!', 2000) + ".a=1}"));
        _out.WriteLine(ex.Message);
    }

    [Fact]
    public void A_query_longer_than_the_ceiling_is_refused_before_it_is_lexed()
    {
        // The lexer is iterative and safe at any depth, so the depth counter says nothing about it
        // — but it materialises a Token per symbol, and POST /api/traces/query has a 30 MB body
        // limit. This is the bound that keeps that from being hundreds of megabytes of token list
        // on a 512 MB box before the parser is handed anything to refuse.
        string huge = "{" + new string(' ', TraceQLParser.MaxQueryChars) + ".a=1}";
        var ex = Assert.Throws<TraceQLException>(() => TraceQLParser.Parse(huge));
        _out.WriteLine(ex.Message);
        Assert.Contains("characters", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{ .db.system = \"mssql\" && duration > 1s }")]
    [InlineData("{ (.a = 1 || .b = 2) && !(status = error) }")]
    [InlineData("{ ((((.a = 1)))) }")]
    [InlineData("{ !!!.a = 1 }")]
    public void Real_queries_still_parse(string q)
    {
        // The controls. A bound is only worth having if it is nowhere near a query somebody wrote,
        // and the shapes above — including the ones that DO nest — are what the UI emits.
        Assert.NotNull(TraceQLParser.Parse(q));
    }

    [Fact]
    public void A_flat_chain_of_many_terms_is_not_nesting_and_is_not_refused()
    {
        // THE REASON THE DECREMENT IS IN A `finally`. `{a=1 && (b=2) && (c=3) && …}` opens and
        // closes one level at a time: it is 200 SIBLINGS, not 200 levels deep, and it recurses no
        // further than any single term. Without the restore this working query would be rejected
        // in the name of a crash it cannot cause — the failure mode a depth counter gets wrong far
        // more often than it gets the crash wrong.
        var terms = new List<string>(200);
        for (int i = 0; i < 200; i++) terms.Add($"(.k{i} = {i})");
        string q = "{" + string.Join(" && ", terms) + "}";

        _out.WriteLine($"flat chain: {terms.Count} terms, {q.Length} chars");
        Assert.True(q.Length < TraceQLParser.MaxQueryChars, "the fixture outgrew the length ceiling");
        Assert.NotNull(TraceQLParser.Parse(q));
    }
}
