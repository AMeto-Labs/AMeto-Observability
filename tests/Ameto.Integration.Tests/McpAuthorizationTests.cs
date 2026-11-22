using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Ameto.Integration.Tests;

/// <summary>
/// Who may do what around the AI endpoint, on a host that signs people in for real: the settings
/// API by role, and the two kinds of credential each kept to its own door.
/// </summary>
public sealed class McpAuthorizationTests : IClassFixture<RealAuthWebAppFactory>
{
    private readonly RealAuthWebAppFactory _factory;

    public McpAuthorizationTests(RealAuthWebAppFactory factory) => _factory = factory;

    /// <summary>A client with no credential of its own: the factory's default ingest key is taken off.</summary>
    private HttpClient Bare()
    {
        var client = _factory.CreateBrowser();
        client.DefaultRequestHeaders.Remove("X-Seq-ApiKey");
        return client;
    }

    private async Task<string> TokenAsync(string username, string password)
    {
        using var client = Bare();
        return (await _factory.LoginAsync(client, username, password)).Token;
    }

    private static async Task<HttpStatusCode> SendAsync(HttpClient client, HttpMethod method, string path, string? bearer, object? body = null)
    {
        using var request = new HttpRequestMessage(method, path);
        if (bearer is not null) request.Headers.Authorization = new("Bearer", bearer);
        if (body is not null) request.Content = JsonContent.Create(body);
        using var response = await client.SendAsync(request);
        return response.StatusCode;
    }

    [Fact]
    public async Task TheSettingsApi_IsReadByManagers_AndChangedByAdminsOnly()
    {
        _factory.CreateLocalUser("mcp-manager", "manager-password", "manager");
        _factory.CreateLocalUser("mcp-viewer",  "viewer-password",  "viewer");
        string admin   = await TokenAsync("admin", RealAuthWebAppFactory.AdminPassword);
        string manager = await TokenAsync("mcp-manager", "manager-password");
        string viewer  = await TokenAsync("mcp-viewer", "viewer-password");
        using var client = Bare();

        foreach (string path in (string[])["/api/mcp", "/api/mcp/profiles", "/api/mcp/rules", "/api/mcp/activity"])
        {
            Assert.Equal(HttpStatusCode.Unauthorized, await SendAsync(client, HttpMethod.Get, path, bearer: null));
            Assert.Equal(HttpStatusCode.Forbidden,    await SendAsync(client, HttpMethod.Get, path, viewer));
            Assert.Equal(HttpStatusCode.OK,           await SendAsync(client, HttpMethod.Get, path, manager));
        }

        var profile = new { name = "by role" };
        var rule    = new { field = "Secret", action = "remove" };
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(client, HttpMethod.Post, "/api/mcp/profiles", manager, profile));
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(client, HttpMethod.Post, "/api/mcp/rules", manager, rule));
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(client, HttpMethod.Put, "/api/mcp/settings", manager, new { enabled = false }));
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(client, HttpMethod.Post, "/api/mcp/secret/rotate", manager));
        // The preview returns an event as stored, so it is an admin's as well.
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(client, HttpMethod.Post, "/api/mcp/preview", manager, new { }));

        Assert.Equal(HttpStatusCode.OK, await SendAsync(client, HttpMethod.Post, "/api/mcp/profiles", admin, profile));
        Assert.Equal(HttpStatusCode.OK, await SendAsync(client, HttpMethod.Post, "/api/mcp/rules", admin, rule));
        Assert.Equal(HttpStatusCode.OK, await SendAsync(client, HttpMethod.Post, "/api/mcp/preview", admin, new { }));
    }

    [Fact]
    public async Task AProfileKey_OpensTheEndpoint_AndNothingElse()
    {
        string admin = await TokenAsync("admin", RealAuthWebAppFactory.AdminPassword);
        using var client = Bare();

        using var created = await client.SendAsync(new HttpRequestMessage(HttpMethod.Post, "/api/mcp/profiles")
        {
            Headers = { Authorization = new("Bearer", admin) },
            Content = JsonContent.Create(new { name = "one door" }),
        });
        string key = (await created.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("key").GetString()!;

        await using (var mcp = await McpTesting.ConnectAsync(_factory, key))
            Assert.NotEmpty(await mcp.ListToolsAsync());

        // As a bearer token, and as an API key in each of the places one is read from.
        foreach (string path in (string[])["/api/stats", "/api/events/props", "/api/events/services", "/api/mcp", "/api/mcp/profiles", "/api/auth/keys"])
        {
            Assert.Equal(HttpStatusCode.Unauthorized, await SendAsync(client, HttpMethod.Get, path, key));

            using var asApiKey = new HttpRequestMessage(HttpMethod.Get, path + "?apiKey=" + key);
            asApiKey.Headers.Add("X-Seq-ApiKey", key);
            using var response = await client.SendAsync(asApiKey);
            Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        }
    }

    [Fact]
    public async Task AUsersToken_IsNotAProfileKey()
    {
        string admin = await TokenAsync("admin", RealAuthWebAppFactory.AdminPassword);
        using var client = Bare();

        Assert.Equal(HttpStatusCode.Unauthorized,
            await SendAsync(client, HttpMethod.Post, "/mcp", admin, new { jsonrpc = "2.0", id = 1, method = "tools/list" }));
    }
}

/// <summary>The endpoint under <c>Ameto:BasePath</c>: it moves with the prefix, switch and all.</summary>
public sealed class McpBasePathTests : IClassFixture<PrefixedBasePathTests.PrefixedFactory>
{
    private readonly PrefixedBasePathTests.PrefixedFactory _factory;

    public McpBasePathTests(PrefixedBasePathTests.PrefixedFactory factory) => _factory = factory;

    [Theory]
    [InlineData(McpTesting.Stateless)]
    [InlineData(McpTesting.Handshake)]
    public async Task UnderAPrefix_TheEndpointAndItsSettingsApi_AnswerThere(string protocol)
    {
        using var admin = _factory.CreateClient();
        using var created = await admin.PostAsJsonAsync("/ameto/api/mcp/profiles", new { name = "prefixed " + protocol });
        Assert.Equal(HttpStatusCode.OK, created.StatusCode);
        string key = (await created.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("key").GetString()!;

        await using (var client = await McpTesting.ConnectAsync(_factory, key, protocol, "/ameto/mcp"))
            Assert.Equal(5, (await client.ListToolsAsync()).Count);

        // The switch reads the path with the prefix already taken off, so it works there too.
        try
        {
            using var off = await admin.PutAsJsonAsync("/ameto/api/mcp/settings", new { enabled = false });
            using var request = new HttpRequestMessage(HttpMethod.Post, "/ameto/mcp")
            {
                Headers = { Authorization = new("Bearer", key) },
                Content = JsonContent.Create(new { jsonrpc = "2.0", id = 1, method = "tools/list" }),
            };
            using var response = await admin.SendAsync(request);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        }
        finally
        {
            using var on = await admin.PutAsJsonAsync("/ameto/api/mcp/settings", new { enabled = true });
        }
    }
}
