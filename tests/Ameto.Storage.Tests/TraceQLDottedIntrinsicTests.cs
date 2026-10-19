using Ameto.Tracing;
using Ameto.Tracing.TraceQL;

namespace Ameto.Storage.Tests;

/// <summary>
/// DOTTED INTRINSICS. The parser has always named <c>service.name</c>, <c>span.name</c> and
/// <c>span.kind</c> as spellings of the <c>service</c>, <c>name</c> and <c>kind</c> intrinsics, but
/// the lexer ended an identifier at the dot: <c>{ service.name = "api" }</c> read as the intrinsic
/// <c>service</c>, then an attribute <c>.name</c> where the operator belonged — a parse error, and
/// the one the Traces page's "search by key" on a span's service field ran into.
/// </summary>
public sealed class TraceQLDottedIntrinsicTests
{
    private static SpanRecord Span(string service = "dealer.Gateway", string name = "POST", SpanKind kind = SpanKind.Server) =>
        new()
        {
            TraceId = new TraceId(1, 2), SpanId = new SpanId(3),
            Name = name, ServiceName = service, Kind = kind,
            Attributes = new Dictionary<string, object?>(),
        };

    [Theory]
    [InlineData("{ service.name = \"dealer.Gateway\" }", true)]
    [InlineData("{ service.name = \"other\" }",          false)]
    [InlineData("{ service.name != \"other\" }",         true)]
    [InlineData("{ service = \"dealer.Gateway\" }",      true)]    // the undotted spelling, as before
    [InlineData("{ span.name = \"POST\" }",              true)]
    [InlineData("{ span.kind = server }",                true)]
    [InlineData("{ service.name = \"dealer.Gateway\" && span.name = \"POST\" }", true)]
    public void A_dotted_intrinsic_is_read_as_the_intrinsic(string query, bool expected) =>
        Assert.Equal(expected, TraceQLParser.Parse(query).Evaluate(Span()));

    [Fact]
    public void Service_name_builds_the_service_predicate()
    {
        var pred = Assert.IsType<ServicePredicate>(TraceQLParser.Parse("{ service.name = \"api\" }"));
        Assert.Equal("api", pred.Value);
    }

    [Theory]
    [InlineData("{ service. = \"x\" }")]      // a dot with no segment after it is not part of the name
    [InlineData("{ service .name = \"x\" }")] // a space ends the identifier, as it always has
    public void A_dot_that_starts_no_segment_is_still_an_error(string query) =>
        Assert.Throws<TraceQLException>(() => TraceQLParser.Parse(query));
}
