using System.Net;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>Ameto:BasePath</c> serves the whole application under a URL prefix. Two things make it
/// worth its own suite: the prefix has to reach both halves — the server's routing and the
/// browser's idea of where it is — and the default has to stay a byte-for-byte no-op, because
/// every existing deployment is running it.
/// </summary>
public sealed class PrefixedBasePathTests : IClassFixture<PrefixedBasePathTests.PrefixedFactory>
{
    /// <summary>The same host as every other suite, moved under a prefix.</summary>
    public sealed class PrefixedFactory : AmetoWebAppFactory
    {
        protected override string ConfiguredBasePath => "/ameto";
    }

    private readonly HttpClient _client;

    public PrefixedBasePathTests(PrefixedFactory factory) => _client = factory.CreateClient();

    // ── The server answers under the prefix ───────────────────────────────────

    [Fact]
    public async Task Api_UnderThePrefix_ReachesTheEndpoint()
    {
        // The failure this pins is not a 404 — it is a 200 of the WRONG thing. Without the
        // explicit UseRouting() that sits under UsePathBase in Program.cs, matching happens
        // before the prefix is stripped, "/ameto/api/stats" matches the SPA catch-all, and the
        // caller gets index.html with a 200 and no hint that routing went wrong.
        var response = await _client.GetAsync("/ameto/api/stats");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task Health_UnderThePrefix_Answers()
    {
        var response = await _client.GetAsync("/ameto/health");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("\"status\"", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task AuthorizedEndpoint_UnderThePrefix_StillCarriesItsAuthorization()
    {
        // Same 200-of-the-wrong-thing hazard, but this one is a security property: an endpoint
        // answered by the fallback carries no authorization metadata, so RequireAuthorization
        // silently stops applying. Reaching the real endpoint is the evidence it did not.
        var response = await _client.GetAsync("/ameto/api/events/services");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);
    }

    // ── The browser is told where it is ───────────────────────────────────────

    [Theory]
    [InlineData("/ameto/")]
    [InlineData("/ameto/index.html")]
    [InlineData("/ameto/events/some/client/route")]
    public async Task EntryDocument_UnderThePrefix_CarriesTheRewrittenBaseHref(string path)
    {
        // Every route into the SPA has to produce the same document: a bookmark on a deep
        // route, a hard refresh on /index.html and the bare prefix all load the app, and all
        // three would otherwise ask the browser for /main-A1B2C3.js at the host root.
        var response = await _client.GetAsync(path);
        var html     = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/html", response.Content.Headers.ContentType?.MediaType);
        Assert.Contains("<base href=\"/ameto/\">", html);
        Assert.DoesNotContain("<base href=\"/\">", html);
    }

    [Fact]
    public async Task EntryDocument_KeepsTheTrailingSlash()
    {
        // Load-bearing and easy to get wrong: HTML resolves relative URLs against the last
        // slash, so "/ameto" without one would make the browser treat "ameto" as a file and
        // fetch every asset from the parent — the host root.
        var html = await _client.GetStringAsync("/ameto/");

        var start = html.IndexOf("<base href=\"", StringComparison.Ordinal) + "<base href=\"".Length;
        var href  = html[start..html.IndexOf('"', start)];

        Assert.EndsWith("/", href);
    }

    [Fact]
    public async Task MissingAsset_UnderThePrefix_Is404_NotTheSpa()
    {
        // A missing script answered with an HTML page is a parse error in the console and a
        // blank screen, which is a far worse way to learn the file is gone.
        var response = await _client.GetAsync("/ameto/main-DEADBEEF.js");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    // ── Ingest: reachable under the prefix AND still at the root ──────────────

    /// <summary>
    /// Invalid msgpack. Enough to prove the ingest endpoint was REACHED — the discriminator
    /// that matters is not the status code but that this is not a 200 <c>text/html</c>, which
    /// is what the SPA fallback answers when routing has gone wrong.
    /// </summary>
    private static ByteArrayContent BadClefBatch()
    {
        var content = new ByteArrayContent([0xFF, 0xFE, 0x00, 0x01]);
        content.Headers.ContentType =
            new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
        return content;
    }

    [Theory]
    [InlineData("/ameto/api/events")]
    [InlineData("/api/events")]
    public async Task ClefIngest_IsReachable_UnderThePrefixAndAtTheRoot(string path)
    {
        var response = await _client.PostAsync(path, BadClefBatch());

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Theory]
    [InlineData("/ameto/otlp/v1/logs")]
    [InlineData("/ameto/otlp/v1/traces")]
    [InlineData("/ameto/otlp/v1/metrics")]
    [InlineData("/otlp/v1/logs")]
    [InlineData("/otlp/v1/traces")]
    [InlineData("/otlp/v1/metrics")]
    public async Task OtlpIngest_IsReachable_UnderThePrefixAndAtTheRoot(string path)
    {
        // Both addresses matter, for different people. Under the prefix is what an agent behind
        // the reverse proxy must use; at the root is what every agent configured before the
        // prefix existed is still using, and it must not break on upgrade.
        var content = new ByteArrayContent([0xFF, 0xFE, 0x00, 0x01]);
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/x-protobuf");

        var response = await _client.PostAsync(path, content);

        Assert.NotEqual("text/html", response.Content.Headers.ContentType?.MediaType);
    }

    // ── What the prefix deliberately does NOT do ──────────────────────────────

    [Fact]
    public async Task StrippingProxy_StillGetsTheEntryDocumentWithThePrefix()
    {
        // The other deployment shape, and a common one: nginx `location /ameto/` with
        // `proxy_pass http://host:8555/` — the trailing slash strips the prefix, so requests
        // arrive here bare. The browser is still under /ameto/, so the base href must come from
        // configuration and not from the request that happened to arrive. It does: this asks
        // for "/" the way a stripping proxy would, and must still be told "/ameto/".
        var html = await _client.GetStringAsync("/");

        Assert.Contains("<base href=\"/ameto/\">", html);
    }

    [Fact]
    public async Task Root_StillAnswers_BecauseThePrefixIsAdditive()
    {
        // UsePathBase passes a non-matching path through untouched, so configuring a prefix
        // adds an address rather than moving one. That is what lets the container health check
        // and any agent already pointed at the bare address survive the change — and it is why
        // the prefix must not be described to operators as an access boundary. Asserted so the
        // property is a decision on the record rather than an accident nobody tested.
        var health = await _client.GetAsync("/health");
        var stats  = await _client.GetAsync("/api/stats");

        Assert.Equal(HttpStatusCode.OK, health.StatusCode);
        Assert.Equal(HttpStatusCode.OK, stats.StatusCode);
    }
}

/// <summary>
/// The default. Everything here asserts that nothing moved, because every deployment that
/// upgrades into this change is running with no prefix configured.
/// </summary>
public sealed class DefaultBasePathTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly HttpClient _client;

    public DefaultBasePathTests(AmetoWebAppFactory factory) => _client = factory.CreateClient();

    [Fact]
    public async Task Api_AtTheRoot_IsUnchanged()
    {
        var response = await _client.GetAsync("/api/stats");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task EntryDocument_KeepsTheRootBaseHref()
    {
        // The rewrite runs unconditionally rather than only when a prefix is set, so that a
        // stale `ng build --base-href` flag can never contradict the configured value. Here
        // that means it must write back exactly what the file already said.
        var html = await _client.GetStringAsync("/");

        Assert.Contains("<base href=\"/\">", html);
    }

    [Fact]
    public async Task UnprefixedPath_IsNotServedUnderAPrefixThatWasNeverConfigured()
    {
        // "/ameto/..." is just another client-side route here, so it gets the SPA — but with a
        // root base href, not a prefixed one.
        var html = await _client.GetStringAsync("/ameto/api/stats");

        Assert.Contains("<base href=\"/\">", html);
    }

    [Theory]
    [InlineData("/v1/logs")]        // the OTLP spec paths — this server maps them under /otlp/,
    [InlineData("/v1/traces")]      // so a stock exporter pointed at the bare address lands here
    [InlineData("/v1/metrics")]
    [InlineData("/api/event")]      // the CLEF endpoint, mistyped
    [InlineData("/api/stats")]      // a real route, wrong verb
    [InlineData("/health")]
    public async Task UnmatchedPost_Is405_NotTheSpaDocument(string path)
    {
        // The SPA fallback must not answer a POST. A sender that gets 200 with an HTML body reads
        // it as delivery and drops the batch — silently, and far from anything that would explain
        // it. MapFallbackToFile carried GET/HEAD metadata and so returned 405 here; a bare
        // MapFallback carries none, which is what this pins.
        var response = await _client.PostAsync(path, new StringContent(""));

        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
        Assert.NotEqual("text/html", response.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task DeepRoute_StillServesTheSpa_OnGet()
    {
        // The other half of the same constraint: restricting the fallback must not break the
        // deep-link case it exists for.
        var response = await _client.GetAsync("/events/some/client/route");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/html", response.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task EntryDocument_RevalidatesRatherThanCaching()
    {
        // It names hashed chunk files that vanish on the next deploy, and its own bytes change
        // when BasePath does without the file on disk ever changing — so a cache that trusts
        // mtime would keep serving the old prefix after a reconfiguration.
        var response = await _client.GetAsync("/");

        Assert.Equal("no-cache", response.Headers.CacheControl?.ToString());
        Assert.NotNull(response.Headers.ETag);
    }

    [Fact]
    public async Task EntryDocument_AnswersIfNoneMatchWith304()
    {
        var first = await _client.GetAsync("/");
        var etag  = first.Headers.ETag!.Tag;

        using var request = new HttpRequestMessage(HttpMethod.Get, "/");
        request.Headers.TryAddWithoutValidation("If-None-Match", etag);
        var second = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NotModified, second.StatusCode);
    }
}
