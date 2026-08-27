using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Server.Auth;
using Ameto.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// End-to-end integration tests using an in-memory server.
/// Ingests events via HTTP and queries them back — no external processes required.
/// </summary>
public sealed class IngestionQueryIntegrationTests
    : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    private readonly HttpClient         _client;

    public IngestionQueryIntegrationTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
    }

    // ── Stats endpoint ────────────────────────────────────────────────────────

    [Fact]
    public async Task Get_Stats_Returns200()
    {
        var response = await _client.GetAsync("/api/stats");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var json = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(json.TryGetProperty("hotTierCount", out _) ||
                    json.TryGetProperty("hotTierEvents", out _) ||
                    json.ValueKind == JsonValueKind.Object,
            "Stats response should be a JSON object");
    }

    // ── Events query without ingestion ────────────────────────────────────────

    [Fact]
    public async Task Get_Events_EmptyStore_StreamsNoEvents()
    {
        // GET /api/events is an SSE stream; against an empty store it emits only the
        // terminal "event: done" with no data events.
        var events = await TestHelpers.StreamEventsAsync(_client, "/api/events?count=10");
        Assert.Empty(events);
    }

    // ── Signals (alerts) CRUD ─────────────────────────────────────────────────

    [Fact]
    public async Task Signals_Create_And_List_Works()
    {
        var rule = new
        {
            name      = "TestSignal",
            filter    = "@l = 'Error'",
            threshold = 5,
            windowSeconds = 60,
            cooldownSeconds = 300,
            enabled   = true,
        };

        // Create
        var postResp = await _client.PostAsJsonAsync("/api/alerts", rule);
        Assert.True(postResp.IsSuccessStatusCode,
            $"POST /api/alerts failed: {postResp.StatusCode}");

        // List
        var getResp = await _client.GetAsync("/api/alerts");
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        var signals = await getResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(JsonValueKind.Array, signals.ValueKind);
        Assert.True(signals.GetArrayLength() >= 1);
    }

    // ── Cluster nodes endpoint ────────────────────────────────────────────────

    [Fact]
    public async Task Get_Nodes_WhenClusterDisabled_FallsThroughToSpa()
    {
        // Replication/clustering is disabled in the test factory, so no node API is
        // mapped (the real endpoint is /api/replication/nodes, registered only when
        // Replication.Enabled = true). An unmapped GET path falls through to the SPA
        // fallback — index.html (text/html) — rather than returning cluster data. The
        // factory supplies a stub wwwroot so this asserts ROUTING, not whether someone
        // ran `npm run build` (see AmetoWebAppFactory).
        var response = await _client.GetAsync("/api/nodes");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/html", response.Content.Headers.ContentType?.MediaType);
    }
}

/// <summary>
/// Custom WebApplicationFactory that sets up an isolated temp data directory
/// and disables clustering so tests run standalone.
///
/// Authentication: the app protects most endpoints with JWT bearer + role
/// policies, and the ingest hot path with an API key checked against
/// <see cref="ApiKeyCache"/>. The factory satisfies both:
///   • the default authentication scheme is swapped for <see cref="TestAuthHandler"/>,
///     which authenticates every request as an admin (covers RequireAuthorization);
///   • a known API key (<see cref="TestApiKey"/>) is seeded into the auth store and
///     attached as the <c>X-Seq-ApiKey</c> header on every client (covers ingest).
/// </summary>
public class AmetoWebAppFactory : WebApplicationFactory<Program>
{
    /// <summary>API key seeded into the auth store and sent by every test client.</summary>
    public const string TestApiKey = "rdl_integration_test_key";

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "Ameto-tests-" + Guid.NewGuid().ToString("N")[..8]);
    private readonly Lock   _seedGate = new();
    private bool _apiKeySeeded;

    /// <summary>
    /// The Ameto:BasePath this host runs under. Empty — every path at the root — for every
    /// suite but the one that exists to exercise a prefix; see BasePathTests.
    /// </summary>
    protected virtual string ConfiguredBasePath => "";

    /// <summary>
    /// False leaves wwwroot empty, for the suite that checks the server copes with a UI that is
    /// not there yet. Everything else wants the stub.
    /// </summary>
    protected virtual bool SeedSpaStub => true;

    /// <summary>The per-run wwwroot, so a test can populate it after the host has started.</summary>
    public string WebRootPath { get; private set; } = "";

    /// <summary>The stub itself, so a test that starts without one can write the same bytes later.</summary>
    public const string SpaStubHtml =
        "<!doctype html><html><head><base href=\"/\"><title>Ameto test SPA stub</title>" +
        "</head><body><app-root></app-root></body></html>";

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.UseSetting("Ameto:DataDirectory", _tempDir);
        builder.UseSetting("Ameto:HttpPort", "0");       // random port
        builder.UseSetting("Ameto:Cluster:Enabled", "false");
        builder.UseSetting("Ameto:BasePath", ConfiguredBasePath);

        // Stub web root. The SPA-fallback test asserts that an unmapped /api/* GET falls
        // through to index.html, but the real wwwroot is emitted by `npm run build` and
        // is gitignored — in a plain source checkout there is nothing to fall through TO,
        // so that test reported a missing optional build step as a routing failure. A
        // one-line stub makes the assertion about routing and nothing else, and keeps the
        // suite green without the Angular toolchain. Lives under the per-run temp dir, so
        // it is disposed with it and never touches the repo.
        string webRoot = Path.Combine(_tempDir, "wwwroot");
        Directory.CreateDirectory(webRoot);
        WebRootPath = webRoot;

        // Shaped like the real client/src/index.html — a <base> tag inside <head> — because
        // the server rewrites that tag as it serves the file. Against a stub without one, a
        // rewriter that quietly did nothing would look exactly like one that worked.
        if (SeedSpaStub)
            File.WriteAllText(Path.Combine(webRoot, "index.html"), SpaStubHtml);
        builder.UseSetting(Microsoft.AspNetCore.Hosting.WebHostDefaults.WebRootKey, webRoot);

        // Override the ServerOptions with test-specific settings
        builder.ConfigureServices(services =>
        {
            // Replace the IOptions<ServerOptions> with test settings
            var opts = new ServerOptions
            {
                NodeId        = NodeId.Local,
                DataDirectory = _tempDir,
                HttpPort      = 0,
                // Kept in step with the UseSetting above. The pipeline reads the prefix from
                // IConfiguration, not from here, so nothing today notices the difference — but a
                // future DI consumer of ServerOptions would read "" under PrefixedFactory and
                // pass for the wrong reason.
                BasePath      = ConfiguredBasePath,
                HotTier = new HotTierOptions
                {
                    MaxSizeBytes = 8 * 1024 * 1024, // 8 MB — small for tests
                    MaxAge       = TimeSpan.FromMinutes(60),
                },
                Retention = new RetentionConfig(),
            };

            services.AddSingleton(opts);
            services.AddSingleton<Microsoft.Extensions.Options.IOptions<ServerOptions>>(
                _ => Microsoft.Extensions.Options.Options.Create(opts));
        });

        // Swap the default authentication scheme for the test handler. This runs
        // after the app's own registration, so overriding the default
        // authenticate/challenge schemes here wins over the JWT bearer defaults.
        builder.ConfigureTestServices(services =>
        {
            services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = TestAuthHandler.SchemeName;
                options.DefaultChallengeScheme    = TestAuthHandler.SchemeName;
                options.DefaultScheme             = TestAuthHandler.SchemeName;
            })
            .AddScheme<AuthenticationSchemeOptions, TestAuthHandler>(
                TestAuthHandler.SchemeName, static _ => { });
        });
    }

    /// <summary>Attaches the seeded API key to every client the factory hands out.</summary>
    protected override void ConfigureClient(HttpClient client)
    {
        base.ConfigureClient(client);
        EnsureApiKeySeeded();
        client.DefaultRequestHeaders.Add("X-Seq-ApiKey", TestApiKey);
    }

    /// <summary>
    /// Seeds <see cref="TestApiKey"/> into the DB-backed auth store once, then
    /// refreshes the in-memory cache so the ingest endpoint accepts it.
    /// </summary>
    private void EnsureApiKeySeeded()
    {
        if (_apiKeySeeded) return;
        lock (_seedGate)
        {
            if (_apiKeySeeded) return;

            var store = Services.GetRequiredService<AuthStore>();
            var cache = Services.GetRequiredService<ApiKeyCache>();
            if (!cache.Validate(TestApiKey, ApiKeyPermissions.All))
            {
                store.CreateApiKey(
                    name:        "integration-tests",
                    description: "Seeded by AmetoWebAppFactory",
                    permissions: ApiKeyPermissions.All,
                    createdBy:   "integration-tests",
                    manualKey:   TestApiKey);
                cache.Invalidate();
            }
            _apiKeySeeded = true;
        }
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing && Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); }
            catch { /* best-effort cleanup */ }
        }
    }
}
