using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Ameto.Server.Auth;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// Sign-ins that last, and the ways they end.
///
/// <para>The access token used to BE the sign-in: two hours, renewable only while still valid,
/// so a tab closed overnight came back to the login page every morning. Now a session cookie
/// outlives the 15-minute token by a month of inactivity, and is traded for a fresh token by
/// <c>/api/auth/refresh</c> — while signing out, ending a user's sessions, blocking or deleting
/// them stops their open tabs on the very next request.</para>
/// </summary>
public sealed class AuthSessionTests : IClassFixture<RealAuthWebAppFactory>
{
    private readonly RealAuthWebAppFactory _factory;

    public AuthSessionTests(RealAuthWebAppFactory factory) => _factory = factory;

    /// <summary>An endpoint any signed-in user may call: how a test asks "is this token still accepted?".</summary>
    private const string ProbePath = "/api/auth/sse-ticket";

    private static string Unique(string name) => $"{name}-{Guid.NewGuid():N}"[..20];

    private async Task<HttpStatusCode> ProbeAsync(HttpClient client, string token)
    {
        using var request  = SessionCookie.WithBearer(HttpMethod.Post, ProbePath, token);
        using var response = await client.SendAsync(request);
        return response.StatusCode;
    }

    private async Task<string> AdminTokenAsync(HttpClient client) =>
        (await _factory.LoginAsync(client, "admin", RealAuthWebAppFactory.AdminPassword)).Token;

    // ── Signing in ────────────────────────────────────────────────────────────

    [Fact]
    public async Task SigningInSetsAnHttpOnlyStrictCookieScopedToTheAuthEndpoints()
    {
        var client = _factory.CreateBrowser();
        var name   = Unique("anna");
        _factory.CreateLocalUser(name, "anna-password");

        using var response = await client.PostAsJsonAsync("/api/auth/login", new { username = name, password = "anna-password" });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<RealAuthWebAppFactory.TokenBody>();
        Assert.Equal(15 * 60, body!.ExpiresIn);

        var header = SessionCookie.HeaderIn(response);
        Assert.NotNull(header);
        Assert.Contains("httponly", header, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("samesite=strict", header, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("path=/api/auth", header, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("expires=", header, StringComparison.OrdinalIgnoreCase);
        // Plain HTTP here: a Secure cookie would never come back, and every refresh would fail.
        Assert.DoesNotContain("secure", header, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TheCookieAloneBuysAFreshTokenAndIsReplacedEachTime()
    {
        var client = _factory.CreateBrowser();
        var name   = Unique("boris");
        _factory.CreateLocalUser(name, "boris-password");
        var (_, cookie1) = await _factory.LoginAsync(client, name, "boris-password");

        // No bearer token at all: this is what runs after the access token has expired.
        using var refreshed = await SessionCookie.RefreshAsync(client, cookie1);

        Assert.Equal(HttpStatusCode.OK, refreshed.StatusCode);
        var body    = await refreshed.Content.ReadFromJsonAsync<RealAuthWebAppFactory.TokenBody>();
        var cookie2 = SessionCookie.CookieIn(refreshed);
        Assert.NotNull(cookie2);
        Assert.NotEqual(cookie1, cookie2);
        Assert.Equal(HttpStatusCode.OK, await ProbeAsync(client, body!.Token));
    }

    [Fact]
    public async Task ASessionUsedAtLeastMonthlyNeverExpiresAndOneLeftAMonthDoes()
    {
        var client = _factory.CreateBrowser();
        var name   = Unique("vera");
        _factory.CreateLocalUser(name, "vera-password");
        var (_, cookie) = await _factory.LoginAsync(client, name, "vera-password");

        // Each refresh restarts the month.
        for (int i = 0; i < 3; i++)
        {
            _factory.Clock.Advance(TimeSpan.FromDays(29));
            using var refreshed = await SessionCookie.RefreshAsync(client, cookie);
            Assert.Equal(HttpStatusCode.OK, refreshed.StatusCode);
            cookie = SessionCookie.CookieIn(refreshed)!;
        }

        _factory.Clock.Advance(TimeSpan.FromDays(31));
        using var lapsed = await SessionCookie.RefreshAsync(client, cookie);
        Assert.Equal(HttpStatusCode.Unauthorized, lapsed.StatusCode);
        Assert.True(SessionCookie.IsClearedBy(lapsed));
    }

    // ── Rotation ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task TwoTabsRefreshingWithTheSameCookieAreBothAnsweredWithOneSuccessor()
    {
        var client = _factory.CreateBrowser();
        var name   = Unique("gleb");
        _factory.CreateLocalUser(name, "gleb-password");
        var (_, cookie1) = await _factory.LoginAsync(client, name, "gleb-password");

        using var first  = await SessionCookie.RefreshAsync(client, cookie1);
        using var second = await SessionCookie.RefreshAsync(client, cookie1);   // the other tab, same cookie

        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        var successor = SessionCookie.CookieIn(first);
        Assert.Equal(successor, SessionCookie.CookieIn(second));

        // And the session carries on from that successor.
        using var next = await SessionCookie.RefreshAsync(client, successor);
        Assert.Equal(HttpStatusCode.OK, next.StatusCode);
    }

    [Fact]
    public async Task ACookieReplayedAfterItWasReplacedEndsTheSessionForEveryHolder()
    {
        var client = _factory.CreateBrowser();
        var name   = Unique("dina");
        _factory.CreateLocalUser(name, "dina-password");
        var (_, stolen) = await _factory.LoginAsync(client, name, "dina-password");

        using var owners = await SessionCookie.RefreshAsync(client, stolen);
        var ownersCookie = SessionCookie.CookieIn(owners)!;
        var ownersToken  = (await owners.Content.ReadFromJsonAsync<RealAuthWebAppFactory.TokenBody>())!.Token;

        // Past the grace, the old cookie coming back means a copy of it is in use elsewhere.
        _factory.Clock.Advance(AuthSessions.RotationGrace + TimeSpan.FromSeconds(1));
        using var replay = await SessionCookie.RefreshAsync(client, stolen);
        Assert.Equal(HttpStatusCode.Unauthorized, replay.StatusCode);

        // Which holder was the owner cannot be told, so the session ends for both — at once.
        using var ownersNext = await SessionCookie.RefreshAsync(client, ownersCookie);
        Assert.Equal(HttpStatusCode.Unauthorized, ownersNext.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, await ProbeAsync(client, ownersToken));
    }

    [Fact]
    public async Task ACookieThatWasNotMintedHereIsRefused()
    {
        var client = _factory.CreateBrowser();
        var name   = Unique("egor");
        _factory.CreateLocalUser(name, "egor-password");
        var (_, cookie) = await _factory.LoginAsync(client, name, "egor-password");

        // Same session id and generation, one character of the MAC changed: the FIRST one. The last
        // character of 32 base64url bytes carries two padding bits, so A→B there can leave the
        // decoded MAC as it was.
        int mac = cookie.LastIndexOf('.') + 1;
        var forged = cookie[..mac] + (cookie[mac] == 'A' ? 'B' : 'A') + cookie[(mac + 1)..];
        using var tampered = await SessionCookie.RefreshAsync(client, forged);
        using var missing  = await SessionCookie.RefreshAsync(client, cookie: null);

        Assert.Equal(HttpStatusCode.Unauthorized, tampered.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, missing.StatusCode);
        // The genuine cookie is unaffected by the forgery.
        using var genuine = await SessionCookie.RefreshAsync(client, cookie);
        Assert.Equal(HttpStatusCode.OK, genuine.StatusCode);
    }

    // ── Ending sessions ───────────────────────────────────────────────────────

    [Fact]
    public async Task SigningOutEndsTheSessionOnTheServerAtOnce()
    {
        var client = _factory.CreateBrowser();
        var name   = Unique("zoya");
        _factory.CreateLocalUser(name, "zoya-password");
        var (token, cookie) = await _factory.LoginAsync(client, name, "zoya-password");

        var request = new HttpRequestMessage(HttpMethod.Post, "/api/auth/logout");
        request.Headers.Add("Cookie", cookie);
        using var logout = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.NoContent, logout.StatusCode);
        Assert.True(SessionCookie.IsClearedBy(logout));
        // The access token handed out under the session stops working now, not in 15 minutes.
        Assert.Equal(HttpStatusCode.Unauthorized, await ProbeAsync(client, token));
        using var refresh = await SessionCookie.RefreshAsync(client, cookie);
        Assert.Equal(HttpStatusCode.Unauthorized, refresh.StatusCode);
    }

    [Fact]
    public async Task BlockingEndsEverySessionAtOnceAndRefusesSignInUntilUnblocked()
    {
        var admin = _factory.CreateBrowser();
        var adminToken = await AdminTokenAsync(admin);
        var name = Unique("ivan");
        var id   = _factory.CreateLocalUser(name, "ivan-password");
        var laptop = _factory.CreateBrowser();
        var phone  = _factory.CreateBrowser();
        var (laptopToken, laptopCookie) = await _factory.LoginAsync(laptop, name, "ivan-password");
        var (phoneToken, _)             = await _factory.LoginAsync(phone,  name, "ivan-password");

        using (var detail = await admin.SendAsync(SessionCookie.WithBearer(HttpMethod.Get, $"/api/users/{id}", adminToken)))
        {
            var json = await detail.Content.ReadFromJsonAsync<JsonElement>();
            Assert.False(json.GetProperty("blocked").GetBoolean());
            Assert.Equal(2, json.GetProperty("sessions").GetArrayLength());
        }

        var block = SessionCookie.WithBearer(HttpMethod.Patch, $"/api/users/{id}/blocked", adminToken);
        block.Content = JsonContent.Create(new { blocked = true });
        using (var blocked = await admin.SendAsync(block))
            Assert.Equal(HttpStatusCode.NoContent, blocked.StatusCode);

        Assert.Equal(HttpStatusCode.Unauthorized, await ProbeAsync(laptop, laptopToken));
        Assert.Equal(HttpStatusCode.Unauthorized, await ProbeAsync(phone,  phoneToken));
        using (var refresh = await SessionCookie.RefreshAsync(laptop, laptopCookie))
            Assert.Equal(HttpStatusCode.Unauthorized, refresh.StatusCode);
        using (var login = await laptop.PostAsJsonAsync("/api/auth/login", new { username = name, password = "ivan-password" }))
        {
            Assert.Equal(HttpStatusCode.Forbidden, login.StatusCode);
            Assert.Null(SessionCookie.CookieIn(login));
        }

        var unblock = SessionCookie.WithBearer(HttpMethod.Patch, $"/api/users/{id}/blocked", adminToken);
        unblock.Content = JsonContent.Create(new { blocked = false });
        using (var unblocked = await admin.SendAsync(unblock))
            Assert.Equal(HttpStatusCode.NoContent, unblocked.StatusCode);
        var (again, _) = await _factory.LoginAsync(laptop, name, "ivan-password");
        Assert.Equal(HttpStatusCode.OK, await ProbeAsync(laptop, again));
    }

    [Fact]
    public async Task AnAdminCannotBlockTheirOwnAccount()
    {
        var admin = _factory.CreateBrowser();
        var adminToken = await AdminTokenAsync(admin);
        var adminId = _factory.Services.GetRequiredService<AuthStore>().FindByUsernameOrEmail("admin", "")!.Id;

        var block = SessionCookie.WithBearer(HttpMethod.Patch, $"/api/users/{adminId}/blocked", adminToken);
        block.Content = JsonContent.Create(new { blocked = true });
        using var response = await admin.SendAsync(block);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(HttpStatusCode.OK, await ProbeAsync(admin, adminToken));
    }

    [Fact]
    public async Task EndingSessionsSignsTheUserOutWithoutBlockingThem()
    {
        var admin = _factory.CreateBrowser();
        var adminToken = await AdminTokenAsync(admin);
        var name = Unique("olga");
        var id   = _factory.CreateLocalUser(name, "olga-password");
        var user = _factory.CreateBrowser();
        var (token, cookie) = await _factory.LoginAsync(user, name, "olga-password");

        using var ended = await admin.SendAsync(SessionCookie.WithBearer(HttpMethod.Delete, $"/api/users/{id}/sessions", adminToken));

        Assert.Equal(HttpStatusCode.OK, ended.StatusCode);
        Assert.Equal(1, (await ended.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("ended").GetInt32());
        Assert.Equal(HttpStatusCode.Unauthorized, await ProbeAsync(user, token));
        using (var refresh = await SessionCookie.RefreshAsync(user, cookie))
            Assert.Equal(HttpStatusCode.Unauthorized, refresh.StatusCode);
        // Not blocked: signing in again works.
        await _factory.LoginAsync(user, name, "olga-password");
    }

    [Fact]
    public async Task DeletingAUserStopsTheirOpenTabsAtOnce()
    {
        var admin = _factory.CreateBrowser();
        var adminToken = await AdminTokenAsync(admin);
        var name = Unique("petr");
        var id   = _factory.CreateLocalUser(name, "petr-password");
        var user = _factory.CreateBrowser();
        var (token, _) = await _factory.LoginAsync(user, name, "petr-password");

        using var deleted = await admin.SendAsync(SessionCookie.WithBearer(HttpMethod.Delete, $"/api/users/{id}", adminToken));

        Assert.Equal(HttpStatusCode.NoContent, deleted.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, await ProbeAsync(user, token));
    }

    [Fact]
    public async Task AnOpenLiveTailIsCutWhenItsUserIsBlocked()
    {
        var admin = _factory.CreateBrowser();
        var adminToken = await AdminTokenAsync(admin);
        var name = Unique("lena");
        var id   = _factory.CreateLocalUser(name, "lena-password");
        var user = _factory.CreateBrowser();
        var (token, _) = await _factory.LoginAsync(user, name, "lena-password");

        string ticket;
        using (var ticketResponse = await user.SendAsync(SessionCookie.WithBearer(HttpMethod.Post, "/api/auth/sse-ticket", token)))
            ticket = (await ticketResponse.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("ticket").GetString()!;

        var open = new HttpRequestMessage(HttpMethod.Get, $"/api/events/live?ticket={Uri.EscapeDataString(ticket)}");
        open.Headers.Accept.ParseAdd("text/event-stream");
        using var tail = await user.SendAsync(open, HttpCompletionOption.ResponseHeadersRead);
        Assert.Equal(HttpStatusCode.OK, tail.StatusCode);
        var stream = await tail.Content.ReadAsStreamAsync();

        var block = SessionCookie.WithBearer(HttpMethod.Patch, $"/api/users/{id}/blocked", adminToken);
        block.Content = JsonContent.Create(new { blocked = true });
        using (var blocked = await admin.SendAsync(block))
            Assert.Equal(HttpStatusCode.NoContent, blocked.StatusCode);

        // The tail authenticated once, at connect; without the abort it would run on for hours.
        var ended = ReadToEndOrFailureAsync(stream);
        Assert.Same(ended, await Task.WhenAny(ended, Task.Delay(TimeSpan.FromSeconds(30))));
    }

    /// <summary>Reads until the stream ends or breaks — either way the server stopped sending.</summary>
    private static async Task ReadToEndOrFailureAsync(Stream stream)
    {
        var buffer = new byte[4096];
        try
        {
            while (await stream.ReadAsync(buffer) > 0) { }
        }
        catch (IOException) { }
        catch (OperationCanceledException) { }
        catch (HttpRequestException) { }
    }
}

/// <summary>Under a deployment prefix the cookie must be scoped to the prefixed path the browser uses.</summary>
public sealed class PrefixedSessionCookieTests : IClassFixture<PrefixedBasePathTests.PrefixedFactory>
{
    private readonly PrefixedBasePathTests.PrefixedFactory _factory;

    public PrefixedSessionCookieTests(PrefixedBasePathTests.PrefixedFactory factory) => _factory = factory;

    [Fact]
    public async Task TheSessionCookieIsScopedUnderThePrefixAndRefreshesThere()
    {
        _factory.Services.GetRequiredService<AuthStore>().CreateUser("prefixed-user", "prefixed-password", "viewer");
        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { HandleCookies = false });

        using var login = await client.PostAsJsonAsync("/ameto/api/auth/login",
            new { username = "prefixed-user", password = "prefixed-password" });
        Assert.Equal(HttpStatusCode.OK, login.StatusCode);
        var header = SessionCookie.HeaderIn(login);
        Assert.Contains("path=/ameto/api/auth", header, StringComparison.OrdinalIgnoreCase);

        using var refresh = await SessionCookie.RefreshAsync(client, SessionCookie.CookieIn(login), "/ameto/api/auth/refresh");
        Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
    }
}
