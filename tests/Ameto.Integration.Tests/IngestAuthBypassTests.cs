using System.Net;
using System.Net.Http.Headers;

namespace Ameto.Integration.Tests;

/// <summary>
/// The authentication / authorization / rate-limiter stack is skipped for the telemetry
/// receivers, because on those routes it produces nothing the endpoint uses — each receiver
/// validates its own API key against <c>ApiKeyCache</c>. Two things must stay true, and the
/// bypass is only safe while both do:
///
/// <list type="bullet">
/// <item>An ingest POST without a valid API key is still refused. The bypass skips the JWT
/// middleware, not the key check.</item>
/// <item>Everything that is NOT an ingest route still goes through authentication — including
/// <c>GET /api/events</c>, the SSE search, which shares its path with the CLEF ingest POST and
/// is separated from it only by the method.</item>
/// </list>
///
/// <para>The GET half is a real gate rather than a tautology: if the predicate wrongly claimed
/// that request, ASP.NET Core would throw on an endpoint carrying authorization metadata with
/// no authorization middleware in the pipeline — 500, not 401.</para>
/// </summary>
public sealed class IngestAuthBypassTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public IngestAuthBypassTests(AmetoWebAppFactory factory) => _factory = factory;

    /// <summary>A client with no ingest API key attached.</summary>
    private HttpClient KeylessClient()
    {
        var client = _factory.CreateClient();
        client.DefaultRequestHeaders.Remove("X-Seq-ApiKey");
        return client;
    }

    private static ByteArrayContent EmptyClefBatch()
    {
        var content = new ByteArrayContent([0x90]); // msgpack: empty array
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return content;
    }

    private static StringContent EmptyOtlpLogs()
        => new("{\"resourceLogs\":[]}", System.Text.Encoding.UTF8, "application/json");

    // ── The receivers still enforce their own key ────────────────────────────

    [Fact]
    public async Task ClefIngest_WithoutApiKey_IsRefused()
    {
        var resp = await KeylessClient().PostAsync("/api/events", EmptyClefBatch());
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [Theory]
    [InlineData("/v1/logs")]
    [InlineData("/otlp/v1/logs")]
    public async Task OtlpIngest_WithoutApiKey_IsRefused(string path)
    {
        var resp = await KeylessClient().PostAsync(path, EmptyOtlpLogs());
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [Fact]
    public async Task ClefIngest_WithApiKey_StillIngests()
    {
        var resp = await _factory.CreateClient().PostAsync("/api/events", EmptyClefBatch());
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    [Fact]
    public async Task OtlpIngest_WithApiKey_StillIngests()
    {
        var resp = await _factory.CreateClient().PostAsync("/v1/logs", EmptyOtlpLogs());
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    // ── Everything else still goes through authentication ────────────────────

    [Fact]
    public async Task SseSearch_SharesThePath_ButStillRequiresUserAuth()
    {
        // No API key either: the read policy accepts the api-key scheme as well as the user
        // one, so the key alone would satisfy it and prove nothing about the middleware.
        var client = KeylessClient();
        client.DefaultRequestHeaders.Add(TestAuthHandler.RoleHeader, TestAuthHandler.AnonymousRole);

        var resp = await client.GetAsync("/api/events?count=1",
            HttpCompletionOption.ResponseHeadersRead);

        // 401, not 500: the authorization middleware ran and challenged.
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [Fact]
    public async Task SseSearch_WithUserAuth_StillAnswers()
    {
        var resp = await _factory.CreateClient().GetAsync("/api/events?count=1",
            HttpCompletionOption.ResponseHeadersRead);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    [Fact]
    public async Task ANonIngestPost_StillRequiresUserAuth()
    {
        // /api/alerts is a POST that is NOT a receiver — the bypass must not reach it.
        var client = _factory.CreateClient();
        client.DefaultRequestHeaders.Add(TestAuthHandler.RoleHeader, TestAuthHandler.AnonymousRole);

        var resp = await client.PostAsync("/api/alerts",
            new StringContent("{\"name\":\"x\",\"channels\":[]}", System.Text.Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }
}
