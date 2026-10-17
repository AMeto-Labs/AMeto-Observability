using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using Ameto.Server.Auth;
using Microsoft.AspNetCore.Authentication.Google;
using Microsoft.AspNetCore.Authentication.MicrosoftAccount;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// A host that signs people in for real: the application's own JWT bearer scheme stays the
/// default (no <see cref="TestAuthHandler"/>), Google and Microsoft are configured against
/// <see cref="FakeOAuthProvider"/>, and the session code reads <see cref="Clock"/> so a test can
/// move a month forward in a line.
/// </summary>
public sealed class RealAuthWebAppFactory : AmetoWebAppFactory
{
    public const string AdminPassword = "admin-password-for-tests";

    /// <summary>Request header the host takes the client address from (see <see cref="ClientIpFromHeader"/>).</summary>
    public const string ClientIpHeader = "X-Test-Client-Ip";

    public ManualClock       Clock    { get; } = new();
    public FakeOAuthProvider Provider { get; } = new();

    private int _nextClient;

    protected override bool UseTestAuthentication => false;

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);
        builder.UseSetting("Ameto:Auth:AdminPassword",          AdminPassword);
        builder.UseSetting("Ameto:Auth:Google:ClientId",        "google-client");
        builder.UseSetting("Ameto:Auth:Google:ClientSecret",    "google-secret");
        builder.UseSetting("Ameto:Auth:Microsoft:ClientId",     "ms-client");
        builder.UseSetting("Ameto:Auth:Microsoft:ClientSecret", "ms-secret");
        builder.UseSetting("Ameto:Auth:Microsoft:TenantId",     "tenant-for-tests");

        builder.ConfigureTestServices(services =>
        {
            services.AddKeyedSingleton<TimeProvider>(AuthSessions.ClockKey, Clock);
            // Configure, not PostConfigure: the OAuth post-configure builds the backchannel
            // HttpClient from this handler, and it runs after every Configure.
            services.Configure<GoogleOptions>(GoogleDefaults.AuthenticationScheme,
                o => o.BackchannelHttpHandler = Provider);
            services.Configure<MicrosoftAccountOptions>(MicrosoftAccountDefaults.AuthenticationScheme,
                o => o.BackchannelHttpHandler = Provider);
            services.AddSingleton<IStartupFilter, ClientIpFromHeader>();
        });
    }

    /// <summary>
    /// A client with an address of its own, so the login limiter's per-address window is its own
    /// too (every TestServer request otherwise shares one). <paramref name="handleCookies"/> false
    /// leaves the session cookie to the test, which needs to send stale ones on purpose.
    /// </summary>
    public HttpClient CreateBrowser(bool handleCookies = false)
    {
        var client = CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            HandleCookies     = handleCookies,
        });
        int n = Interlocked.Increment(ref _nextClient);
        client.DefaultRequestHeaders.Add(ClientIpHeader, $"10.20.{n / 250}.{n % 250 + 1}");
        return client;
    }

    /// <summary>Signs in with a password; returns the access token and the session cookie's value.</summary>
    public async Task<(string Token, string Cookie)> LoginAsync(HttpClient client, string username, string password)
    {
        using var response = await client.PostAsJsonAsync("/api/auth/login", new { username, password });
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<TokenBody>();
        var cookie = SessionCookie.CookieIn(response);
        Assert.NotNull(cookie);
        return (body!.Token, cookie!);
    }

    /// <summary>Creates a local user straight in the store and returns its id.</summary>
    public string CreateLocalUser(string username, string password, string role = "viewer") =>
        Services.GetRequiredService<AuthStore>().CreateUser(username, password, role).Id;

    public sealed record TokenBody(string Token, int ExpiresIn, string Role, int Permissions);

    /// <summary>Lets a test client choose its address (see <see cref="CreateBrowser"/>).</summary>
    private sealed class ClientIpFromHeader : IStartupFilter
    {
        public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
        {
            app.Use((ctx, nextMiddleware) =>
            {
                if (ctx.Request.Headers.TryGetValue(ClientIpHeader, out var ip)
                    && IPAddress.TryParse(ip.ToString(), out var address))
                    ctx.Connection.RemoteIpAddress = address;
                return nextMiddleware(ctx);
            });
            next(app);
        };
    }
}

/// <summary>
/// Reading and sending the session cookie by hand. A cookie is handled as its whole
/// <c>name=value</c> pair, because the name is per server (see <see cref="SessionTokenCodec"/>).
/// </summary>
public static class SessionCookie
{
    /// <summary>The raw Set-Cookie header for the session cookie, if the response set or cleared it.</summary>
    public static string? HeaderIn(HttpResponseMessage response) =>
        response.Headers.TryGetValues("Set-Cookie", out var values)
            ? values.FirstOrDefault(v => v.StartsWith(SessionTokenCodec.CookieNamePrefix, StringComparison.Ordinal))
            : null;

    /// <summary>The <c>name=value</c> the response set the session cookie to; null when absent or cleared.</summary>
    public static string? CookieIn(HttpResponseMessage response)
    {
        var pair = HeaderIn(response)?.Split(';')[0];
        return pair is null || pair.EndsWith('=') ? null : pair;
    }

    /// <summary>True when the response deleted the session cookie.</summary>
    public static bool IsClearedBy(HttpResponseMessage response) =>
        HeaderIn(response) is { } h && h.Contains("expires=Thu, 01 Jan 1970", StringComparison.OrdinalIgnoreCase);

    /// <summary>POST /api/auth/refresh with exactly this cookie (a <c>name=value</c> pair) and no bearer token.</summary>
    public static Task<HttpResponseMessage> RefreshAsync(HttpClient client, string? cookie, string path = "/api/auth/refresh")
    {
        var request = new HttpRequestMessage(HttpMethod.Post, path);
        if (cookie is not null) request.Headers.Add("Cookie", cookie);
        return client.SendAsync(request);
    }

    /// <summary>A request authorised by <paramref name="token"/> as a bearer token.</summary>
    public static HttpRequestMessage WithBearer(HttpMethod method, string path, string token)
    {
        var request = new HttpRequestMessage(method, path);
        request.Headers.Authorization = new("Bearer", token);
        return request;
    }
}

/// <summary>A clock that moves only when told to.</summary>
public sealed class ManualClock : TimeProvider
{
    private long _utcTicks = DateTimeOffset.UtcNow.UtcTicks;

    public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _utcTicks), TimeSpan.Zero);

    public void Advance(TimeSpan by) => Interlocked.Add(ref _utcTicks, by.Ticks);
}

/// <summary>
/// Google and Microsoft as their token and user-info endpoints look from the server: the
/// authorization-code exchange, the refresh-token grant, and the profile. One handler answers
/// both providers — they differ only in field names, and the profile carries both sets.
/// </summary>
public sealed class FakeOAuthProvider : HttpMessageHandler
{
    private readonly Lock _gate = new();

    // Who signs in.
    public string  Subject { get; set; } = "subject-1";
    public string  Email   { get; set; } = "someone@corp.example";

    /// <summary>The refresh token handed out with a code exchange; null when the provider sends none.</summary>
    public string? CodeRefreshToken { get; set; } = "provider-refresh-1";

    /// <summary>The answer to a refresh-token grant.</summary>
    public HttpStatusCode RefreshStatus { get; set; } = HttpStatusCode.OK;
    public string         RefreshBody   { get; set; } = OkRefreshBody(newRefreshToken: null);

    private readonly List<(string RefreshToken, string? Scope)> _refreshGrants = [];

    /// <summary>Every refresh-token grant received: the token presented, and the scope if any.</summary>
    public IReadOnlyList<(string RefreshToken, string? Scope)> RefreshGrants
    {
        get { lock (_gate) return [.. _refreshGrants]; }
    }

    public static string OkRefreshBody(string? newRefreshToken) => newRefreshToken is null
        ? """{"access_token":"provider-access-2","token_type":"Bearer","expires_in":3600}"""
        : $$"""{"access_token":"provider-access-2","token_type":"Bearer","expires_in":3600,"refresh_token":"{{newRefreshToken}}"}""";

    public const string InvalidGrantBody =
        """{"error":"invalid_grant","error_description":"Token has been expired or revoked."}""";

    /// <summary>Back to a provider that knows nobody and answers everything; for the start of a test.</summary>
    public void Reset(string subject, string email)
    {
        lock (_gate)
        {
            Subject          = subject;
            Email            = email;
            CodeRefreshToken = "provider-refresh-1";
            RefreshStatus    = HttpStatusCode.OK;
            RefreshBody      = OkRefreshBody(newRefreshToken: null);
            _refreshGrants.Clear();
        }
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        if (request.Method == HttpMethod.Post && request.Content is not null)
        {
            var form = QueryHelpers.ParseQuery(await request.Content.ReadAsStringAsync(ct));
            var grant = form.TryGetValue("grant_type", out var g) ? g.ToString() : "";
            if (grant == "authorization_code")
            {
                string? refresh;
                lock (_gate) refresh = CodeRefreshToken;
                return Json(HttpStatusCode.OK, refresh is null
                    ? """{"access_token":"provider-access-1","token_type":"Bearer","expires_in":3600}"""
                    : $$"""{"access_token":"provider-access-1","token_type":"Bearer","expires_in":3600,"refresh_token":"{{refresh}}"}""");
            }
            if (grant == "refresh_token")
            {
                lock (_gate)
                {
                    _refreshGrants.Add((form["refresh_token"].ToString(),
                                        form.TryGetValue("scope", out var s) ? s.ToString() : null));
                    return Json(RefreshStatus, RefreshBody);
                }
            }
            return Json(HttpStatusCode.BadRequest, """{"error":"unsupported_grant_type"}""");
        }

        // The profile: Google's userinfo (sub, email, email_verified) and Graph's /me (id, mail,
        // displayName) in one body.
        lock (_gate)
            return Json(HttpStatusCode.OK,
                $$"""{"sub":"{{Subject}}","id":"{{Subject}}","email":"{{Email}}","email_verified":true,"name":"Test Person","mail":"{{Email}}","displayName":"Test Person"}""");
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
}
