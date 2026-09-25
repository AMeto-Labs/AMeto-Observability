using Ameto.Tracing;
using Ameto.Tracing.TraceQL;
using MessagePack;

namespace Ameto.Storage.Tests;

/// <summary>
/// NEGATIVE NUMERIC LITERALS (#94). The lexer had no sign: a '-' fell into "skip unknown", so
/// <c>{ .x = -3 }</c> was <c>{ .x = 3 }</c> and <c>{ .code &lt; -1 }</c> was <c>{ .code &lt; 1 }</c> —
/// not an error and not a miss, an answer to a different question. The sign is part of the
/// literal now, and so is an exponent (<c>-1e3</c>); a negative DURATION is refused by name, since
/// no span has one and the comparison would silently select all or nothing.
///
/// <para>Every positive case is asked of the same attribute twice: from a dictionary (the hot tier's
/// fixtures) and from the msgpack blob a flushed span is read back as, because the two are
/// compared by different code (<c>AttributePredicate.CompareAttr</c>'s two overloads).</para>
/// </summary>
public sealed class TraceQLNegativeLiteralTests
{
    private static SpanRecord Span(object value, bool blob) => blob
        ? new SpanRecord
        {
            TraceId = new TraceId(1, 2), SpanId = new SpanId(3), Name = "op", ServiceName = "svc",
            AttributesBytes = MessagePackSerializer.Serialize(new Dictionary<string, object?> { ["x"] = value }),
        }
        : new SpanRecord
        {
            TraceId = new TraceId(1, 2), SpanId = new SpanId(3), Name = "op", ServiceName = "svc",
            Attributes = new Dictionary<string, object?> { ["x"] = value },
        };

    [Theory]
    [InlineData("{ .x = -3 }",      -3L,     true)]
    [InlineData("{ .x = -3 }",       3L,     false)]   // the old reading: the sign was skipped
    [InlineData("{ .x=-3 }",        -3L,     true)]    // no spaces: '=' then a signed literal
    [InlineData("{ .x < -1 }",      -2L,     true)]
    [InlineData("{ .x < -1 }",       0L,     false)]   // was `.x < 1`, which 0 satisfies
    [InlineData("{ .x = -0.5 }",    -0.5,    true)]
    [InlineData("{ .x = -0.5 }",     0.5,    false)]
    [InlineData("{ .x = -1e3 }",    -1000L,  true)]
    [InlineData("{ .x = 1e3 }",      1000L,  true)]
    [InlineData("{ .x = 2.5E-1 }",   0.25,   true)]
    [InlineData("{ .x > -1e3 }",    -999.5,  true)]
    [InlineData("{ .x != -3 }",      3L,     true)]
    [InlineData("{ .x = -3 }",      "-3",    true)]    // a numeric string compares as its number
    public void A_signed_literal_compares_as_the_number_it_spells(string query, object value, bool expected)
    {
        var pred = TraceQLParser.Parse(query);
        Assert.Equal(expected, pred.Evaluate(Span(value, blob: false)));
        Assert.Equal(expected, pred.Evaluate(Span(value, blob: true)));
    }

    [Fact]
    public void The_promoted_status_code_is_built_with_the_signed_value()
    {
        // The promoted-field predicate takes the literal's value as parsed: -1, not the 1 the
        // skipped sign used to leave.
        var pred = Assert.IsType<HttpStatusCodePredicate>(TraceQLParser.Parse("{ .http.status_code > -1 }"));
        Assert.Equal((short)-1, pred.Code);
    }

    [Theory]
    [InlineData("{ duration > -1ms }")]
    [InlineData("{ duration < -1s }")]
    [InlineData("{ duration > -5 }")]          // the nanosecond spelling of the same thing
    [InlineData("{ .latency > -1ms }")]        // a negative duration literal is refused everywhere
    [InlineData("{ .x = - 3 }")]               // a sign that is not attached to a number
    [InlineData("{ .x = -abc }")]
    [InlineData("{ .x = 1.2.3 }")]             // was the number 0
    [InlineData("{ .x = 1e999 }")]             // not a finite number
    public void A_literal_that_cannot_mean_what_it_says_is_a_parse_error(string query) =>
        Assert.Throws<TraceQLException>(() => TraceQLParser.Parse(query));

    [Theory]
    [InlineData("{ duration > 1ms }")]
    [InlineData("{ duration > 0 }")]
    [InlineData("{ .route-name = \"a-b\" }")]  // '-' inside a key and inside a string is not a sign
    [InlineData("{ .x = 1e }")]                // a bare `e` is still an identifier — and an error, as before
    public void What_parsed_before_still_parses_the_same(string query)
    {
        if (query.Contains("1e }", StringComparison.Ordinal))
            Assert.Throws<TraceQLException>(() => TraceQLParser.Parse(query));
        else
            Assert.NotNull(TraceQLParser.Parse(query));
    }
}
