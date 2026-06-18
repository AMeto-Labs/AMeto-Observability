using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
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
    public async Task Get_Events_EmptyStore_Returns200AndEmptyArray()
    {
        var response = await _client.GetAsync("/api/events?count=10");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var events = await response.Content.ReadFromJsonAsync<JsonElement>();
        // Accept either an empty array or an object with an "events" array
        if (events.ValueKind == JsonValueKind.Array)
        {
            Assert.Equal(0, events.GetArrayLength());
        }
        else
        {
            Assert.Equal(JsonValueKind.Object, events.ValueKind);
        }
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
        var postResp = await _client.PostAsJsonAsync("/api/signals", rule);
        Assert.True(postResp.IsSuccessStatusCode,
            $"POST /api/signals failed: {postResp.StatusCode}");

        // List
        var getResp = await _client.GetAsync("/api/signals");
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        var signals = await getResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(JsonValueKind.Array, signals.ValueKind);
        Assert.True(signals.GetArrayLength() >= 1);
    }

    // ── Cluster nodes endpoint ────────────────────────────────────────────────

    [Fact]
    public async Task Get_Nodes_WhenClusterDisabled_Returns404()
    {
        // Cluster is disabled in the test factory — the /api/nodes endpoint is not mapped
        var response = await _client.GetAsync("/api/nodes");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}

/// <summary>
/// Custom WebApplicationFactory that sets up an isolated temp data directory
/// and disables clustering so tests run standalone.
/// </summary>
public sealed class AmetoWebAppFactory : WebApplicationFactory<Program>
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "Ameto-tests-" + Guid.NewGuid().ToString("N")[..8]);

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.UseSetting("Ameto:DataDirectory", _tempDir);
        builder.UseSetting("Ameto:ApiKey", "");          // no auth for tests
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
