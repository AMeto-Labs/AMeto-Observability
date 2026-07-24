using System.Net;
using System.Net.Http.Json;

namespace Ameto.Integration.Tests;

/// <summary>
/// The local login endpoint is rate-limited per client IP so the admin account
/// can't be brute-forced. (Fresh installs also no longer seed a well-known
/// default password — that path is covered by the store's unit behaviour.)
/// </summary>
public sealed class LoginRateLimitTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public LoginRateLimitTests(AmetoWebAppFactory factory) => _factory = factory;

    [Fact]
    public async Task Login_ThrottlesAfterTooManyAttempts()
    {
        var client = _factory.CreateClient();

        int unauthorized = 0, tooMany = 0;
        for (int i = 0; i < 20; i++)
        {
            var resp = await client.PostAsJsonAsync("/api/auth/login",
                new { username = "admin", password = "definitely-wrong" });
            if (resp.StatusCode == HttpStatusCode.TooManyRequests) tooMany++;
            else if (resp.StatusCode == HttpStatusCode.Unauthorized) unauthorized++;
        }

        // The window permits 10/min; the rest (of 20) must be turned away with 429.
        Assert.True(unauthorized > 0, "first attempts should reach the validator (401)");
        Assert.True(tooMany > 0, "excess attempts should be rate-limited (429)");
        Assert.Equal(20, unauthorized + tooMany);
    }
}
