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
        // fallback — index.html (text/html) — rather than returning cluster data.
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
public sealed class AmetoWebAppFactory : WebApplicationFactory<Program>
{
    /// <summary>API key seeded into the auth store and sent by every test client.</summary>
    public const string TestApiKey = "rdl_integration_test_key";

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "Ameto-tests-" + Guid.NewGuid().ToString("N")[..8]);
    private readonly Lock   _seedGate = new();
    private bool _apiKeySeeded;

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.UseSetting("Ameto:DataDirectory", _tempDir);
        builder.UseSetting("Ameto:HttpPort", "0");       // random port
        builder.UseSetting("Ameto:Cluster:Enabled", "false");

        // Override the ServerOptions with test-specific settings
        builder.ConfigureServices(services =>
        {
            // Replace the IOptions<ServerOptions> with test settings
            var opts = new ServerOptions
            {
                NodeId        = NodeId.Local,
                DataDirectory = _tempDir,
                HttpPort      = 0,
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
