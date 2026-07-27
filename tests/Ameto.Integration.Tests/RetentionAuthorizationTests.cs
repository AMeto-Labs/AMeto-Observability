using System.Net;
using System.Net.Http.Json;

namespace Ameto.Integration.Tests;

/// <summary>
/// Retention writes delete stored signal data and cannot be undone: shortening a window and
/// running enforcement drops every segment past the new horizon. Reading the policy is
/// harmless and stays open to any signed-in user; changing it or running it is admin-only.
/// </summary>
public sealed class RetentionAuthorizationTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public RetentionAuthorizationTests(AmetoWebAppFactory factory) => _factory = factory;

    private HttpClient ClientAs(string role)
    {
        var client = _factory.CreateClient();
        client.DefaultRequestHeaders.Add(TestAuthHandler.RoleHeader, role);
        return client;
    }

    [Fact]
    public async Task Viewer_MayReadTheRetentionPolicy()
    {
        var resp = await ClientAs("viewer").GetAsync("/api/retention");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    [Theory]
    [InlineData("viewer")]
    [InlineData("manager")]
    public async Task NonAdmin_CannotShortenRetention(string role)
    {
        var resp = await ClientAs(role).PutAsJsonAsync("/api/retention",
            new { logsDays = 1, metricsDays = 1, tracesDays = 1 });

        Assert.Equal(HttpStatusCode.Forbidden, resp.StatusCode);
    }

    [Theory]
    [InlineData("viewer")]
    [InlineData("manager")]
    public async Task NonAdmin_CannotRunEnforcement(string role)
    {
        var resp = await ClientAs(role).PostAsync("/api/retention/run", content: null);
        Assert.Equal(HttpStatusCode.Forbidden, resp.StatusCode);
    }

    [Fact]
    public async Task Admin_MayStillChangeRetention()
    {
        var resp = await ClientAs("admin").PutAsJsonAsync("/api/retention",
            new { logsDays = 30, metricsDays = 30, tracesDays = 30 });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }
}
