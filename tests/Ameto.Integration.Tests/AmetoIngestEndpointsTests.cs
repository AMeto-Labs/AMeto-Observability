using Ameto.Otel;

namespace Ameto.Integration.Tests;

/// <summary>
/// The list that stops an instrumented exporter from tracing its own exports — one CLIENT span
/// per batch, exported, producing another, self-amplifying and with nothing in the log to explain
/// it. Two failure directions, and both are expensive:
///
/// <list type="bullet">
/// <item>Miss one of our own spellings and the loop starts. That is how this list came to be
/// audited: the spec paths were added as routes and not here.</item>
/// <item>Match too loosely and real client spans vanish. A substring test for
/// <c>/v1/traces</c> also swallows a customer's own <c>/api/v1/traces/search</c> — and a missing
/// span is silent, where the loop at least announces itself.</item>
/// </list>
/// </summary>
public sealed class AmetoIngestEndpointsTests
{
    [Theory]
    // Every route this server actually receives on, in the shapes an instrumented HTTP client
    // records them.
    [InlineData("http://ameto:5341/v1/logs")]
    [InlineData("http://ameto:5341/v1/traces")]
    [InlineData("http://ameto:5341/v1/metrics")]
    [InlineData("http://ameto:5341/otlp/v1/logs")]
    [InlineData("http://ameto:5341/otlp/v1/traces")]
    [InlineData("http://ameto:5341/otlp/v1/metrics")]
    [InlineData("http://ameto:5341/api/events")]
    [InlineData("http://ameto:4317/opentelemetry.proto.collector.trace.v1.TraceService/Export")]
    [InlineData("https://AMETO-HOST:8555/OTLP/v1/traces")]     // case-insensitive
    [InlineData("/v1/traces")]                                  // url.path, not url.full
    [InlineData("http://ameto:5341/v1/traces?x=1")]             // query ignored
    [InlineData("http://ameto:5341/v1/traces/")]                // trailing slash
    public void Our_own_receivers_are_recognised(string url)
    {
        Assert.True(AmetoIngestEndpoints.Matches(url.AsSpan()));
        Assert.True(AmetoIngestEndpoints.Matches(System.Text.Encoding.ASCII.GetBytes(url)));
    }

    [Theory]
    // Somebody else's API that merely contains our path as a substring. A `Contains` test drops
    // every one of these, and the user never learns why their traces are missing.
    [InlineData("https://internal.example.com/api/v1/traces/search")]
    [InlineData("https://acct-svc/v1/metrics-summary")]
    [InlineData("https://shop/v1/logsearch")]
    [InlineData("https://shop/api/events/archive")]
    [InlineData("https://shop/v2/traces")]
    [InlineData("https://docs/opentelemetry.proto.collector.trace.v1.TraceService/Export/help")]
    [InlineData("https://example.com/")]
    [InlineData("https://example.com")]
    public void Somebody_elses_endpoint_is_left_alone(string url)
    {
        Assert.False(AmetoIngestEndpoints.Matches(url.AsSpan()));
        Assert.False(AmetoIngestEndpoints.Matches(System.Text.Encoding.ASCII.GetBytes(url)));
    }

    [Fact]
    public void Both_overloads_agree()
    {
        // They are separate code paths — one for the object decoder, one for the streaming
        // parser — and the whole point of the shared list is that they cannot drift.
        string[] urls =
        [
            "http://a/v1/logs", "http://a/api/v1/traces/search", "/otlp/v1/metrics",
            "https://x/v1/metrics-summary", "http://a:4317/opentelemetry.proto.collector.logs.v1.LogsService/Export",
        ];
        foreach (var u in urls)
            Assert.Equal(AmetoIngestEndpoints.Matches(u.AsSpan()),
                         AmetoIngestEndpoints.Matches(System.Text.Encoding.ASCII.GetBytes(u)));
    }

    // ── Under a deployment prefix ─────────────────────────────────────────────

    [Theory]
    [InlineData("https://host/ameto/otlp/v1/traces")]
    [InlineData("https://host/ameto/v1/traces")]
    [InlineData("http://host:5341/ameto/api/events")]
    [InlineData("/ameto/otlp/v1/logs")]
    [InlineData("https://host/AMETO/otlp/v1/traces")]   // UsePathBase matches case-insensitively
    public void Prefixed_receiver_urls_are_recognised(string url)
    {
        // With Ameto:BasePath set, an exporter is configured against the prefixed address — and
        // this guard compares whole paths, so without stripping the prefix it would not recognise
        // its own receiver. The feedback loop it exists to break would then be back, and that one
        // leaves nothing in the log to explain itself.
        WithBasePath("/ameto", () =>
        {
            Assert.True(AmetoIngestEndpoints.Matches(url.AsSpan()));
            Assert.True(AmetoIngestEndpoints.Matches(System.Text.Encoding.ASCII.GetBytes(url)));
        });
    }

    [Theory]
    [InlineData("https://host/ametoX/otlp/v1/traces")]  // not a segment boundary
    [InlineData("https://host/other/otlp/v1/traces")]   // somebody else's prefix
    [InlineData("https://host/ameto/api/v1/traces/search")]
    public void A_prefix_does_not_widen_the_match(string url)
    {
        // Dropping a customer's real client span is the more expensive mistake of the two, so the
        // prefix is stripped only on a segment boundary and only when it is ours.
        WithBasePath("/ameto", () =>
        {
            Assert.False(AmetoIngestEndpoints.Matches(url.AsSpan()));
            Assert.False(AmetoIngestEndpoints.Matches(System.Text.Encoding.ASCII.GetBytes(url)));
        });
    }

    [Fact]
    public void Unprefixed_urls_still_match_under_a_prefix()
    {
        // The prefix is additive: the receivers keep answering at the root, so an agent still
        // pointed at the bare address is still talking to us.
        WithBasePath("/ameto", () =>
            Assert.True(AmetoIngestEndpoints.Matches("http://host:5341/otlp/v1/logs".AsSpan())));
    }

    /// <summary>
    /// Runs <paramref name="body"/> with the prefix set, restoring it afterwards. The property is
    /// process-wide because the option is bound once at startup and read from hot-path parsers
    /// that have no DI to reach through; here it has to be put back or it would leak into the
    /// other tests in this assembly.
    /// </summary>
    private static void WithBasePath(string basePath, Action body)
    {
        var previous = AmetoIngestEndpoints.BasePath;
        AmetoIngestEndpoints.BasePath = basePath;
        try { body(); }
        finally { AmetoIngestEndpoints.BasePath = previous; }
    }
}
