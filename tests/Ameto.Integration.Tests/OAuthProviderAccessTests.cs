using System.Net;
using System.Net.Http.Json;
using Ameto.Server.Auth;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// A sign-in through Google or Microsoft keeps working for weeks, and still ends when the
/// account is disabled at the provider.
///
/// <para>Each test runs the whole OAuth dance against <see cref="FakeOAuthProvider"/>: the
/// challenge, the callback with its state and correlation cookie, the code exchange and the
/// profile. Then it moves the session clock and lets the provider answer the hourly check.</para>
/// </summary>
public sealed class OAuthProviderAccessTests : IClassFixture<RealAuthWebAppFactory>
{
    private readonly RealAuthWebAppFactory _factory;
    private readonly string _email;

    public OAuthProviderAccessTests(RealAuthWebAppFactory factory)
    {
        _factory = factory;
        // A person of its own for every test; the provider forgets the previous one.
        _email = $"p{Guid.NewGuid():N}"[..12] + "@corp.example";
        _factory.Provider.Reset(subject: "subject-" + _email, email: _email);
    }

    private AuthStore Store => _factory.Services.GetRequiredService<AuthStore>();

    private void AllowlistedAs(string provider) => Store.CreateOAuthUser(_email, "Test Person", provider, "viewer");

    /// <summary>
    /// Starts a sign-in and comes back through the callback. Returns the callback's answer: a
    /// redirect to the SPA, to the login page with an error, or back round for consent.
    /// </summary>
    private static async Task<HttpResponseMessage> SignInAsync(HttpClient browser, string startPath)
    {
        using var challenge = await browser.GetAsync(startPath);
        Assert.Equal(HttpStatusCode.Redirect, challenge.StatusCode);
        var authorize = challenge.Headers.Location!;
        var state = QueryHelpers.ParseQuery(authorize.Query)["state"].ToString();
        var provider = startPath.Contains("microsoft", StringComparison.Ordinal) ? "microsoft" : "google";
        return await browser.GetAsync($"/api/auth/oauth/{provider}/callback?code=code-1&state={Uri.EscapeDataString(state)}");
    }

    /// <summary>A refresh from the browser's own cookie jar.</summary>
    private static Task<HttpResponseMessage> RefreshAsync(HttpClient browser) =>
        browser.PostAsync("/api/auth/refresh", content: null);

    private static async Task<string> TokenOf(HttpResponseMessage refresh) =>
        (await refresh.Content.ReadFromJsonAsync<RealAuthWebAppFactory.TokenBody>())!.Token;

    // ── The sign-in ───────────────────────────────────────────────────────────

    [Fact]
    public async Task TheCallbackSetsTheSessionCookieAndPutsNoTokenInTheUrl()
    {
        AllowlistedAs("google");
        var browser = _factory.CreateBrowser(handleCookies: true);

        using var challenge = await browser.GetAsync("/api/auth/oauth/google");
        var authorize = QueryHelpers.ParseQuery(challenge.Headers.Location!.Query);
        Assert.Equal("offline", authorize["access_type"].ToString());   // a refresh token is asked for
        Assert.False(authorize.ContainsKey("prompt"));                   // without forcing consent

        using var callback = await SignInAsync(browser, "/api/auth/oauth/google");
        Assert.Equal(HttpStatusCode.Redirect, callback.StatusCode);
        Assert.Equal("/oauth-callback", callback.Headers.Location!.OriginalString);
        Assert.NotNull(SessionCookie.CookieIn(callback));

        using var refresh = await RefreshAsync(browser);
        Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
        // Signing in was the provider's confirmation: nothing to ask it for the next hour.
        Assert.Empty(_factory.Provider.RefreshGrants);
    }

    [Fact]
    public async Task ABlockedAccountIsTurnedAwayAtTheCallback()
    {
        AllowlistedAs("google");
        Store.SetDisabled(Store.FindOAuthUser(_email, "google")!.Id, disabled: true);
        var browser = _factory.CreateBrowser(handleCookies: true);

        using var callback = await SignInAsync(browser, "/api/auth/oauth/google");

        Assert.Equal("/login?error=blocked", callback.Headers.Location!.OriginalString);
        Assert.Null(SessionCookie.CookieIn(callback));
    }

    // ── The hourly check ──────────────────────────────────────────────────────

    [Fact]
    public async Task AnAccountDisabledAtTheProviderLosesEverySessionAtTheNextCheck()
    {
        AllowlistedAs("google");
        var laptop = _factory.CreateBrowser(handleCookies: true);
        var phone  = _factory.CreateBrowser(handleCookies: true);
        using (var signIn = await SignInAsync(laptop, "/api/auth/oauth/google"))
            Assert.Equal("/oauth-callback", signIn.Headers.Location!.OriginalString);
        _factory.Provider.CodeRefreshToken = null;   // the second sign-in brings no new token
        using (var signIn = await SignInAsync(phone, "/api/auth/oauth/google"))
            Assert.Equal("/oauth-callback", signIn.Headers.Location!.OriginalString);

        // An hour on, the check asks the provider, which still vouches for the account.
        _factory.Clock.Advance(TimeSpan.FromMinutes(61));
        string phoneToken;
        using (var refresh = await RefreshAsync(phone))
        {
            Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
            phoneToken = await TokenOf(refresh);
        }
        var grants = _factory.Provider.RefreshGrants;
        Assert.Equal("provider-refresh-1", grants[^1].RefreshToken);

        // Then the account is disabled at the provider.
        _factory.Provider.RefreshStatus = HttpStatusCode.BadRequest;
        _factory.Provider.RefreshBody   = FakeOAuthProvider.InvalidGrantBody;
        _factory.Clock.Advance(TimeSpan.FromMinutes(61));

        using (var refused = await RefreshAsync(laptop))
            Assert.Equal(HttpStatusCode.Unauthorized, refused.StatusCode);
        // Every session of the account, not just the one that asked — and the open tab at once.
        using (var other = await RefreshAsync(phone))
            Assert.Equal(HttpStatusCode.Unauthorized, other.StatusCode);
        using var probe = SessionCookie.WithBearer(HttpMethod.Post, "/api/auth/sse-ticket", phoneToken);
        using var probed = await phone.SendAsync(probe);
        Assert.Equal(HttpStatusCode.Unauthorized, probed.StatusCode);
    }

    [Fact]
    public async Task TheProviderIsAskedAtMostHourly()
    {
        AllowlistedAs("google");
        var browser = _factory.CreateBrowser(handleCookies: true);
        using (await SignInAsync(browser, "/api/auth/oauth/google")) { }

        _factory.Clock.Advance(TimeSpan.FromMinutes(61));
        for (int i = 0; i < 4; i++)
        {
            using var refresh = await RefreshAsync(browser);
            Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
            _factory.Clock.Advance(TimeSpan.FromMinutes(10));
        }

        Assert.Single(_factory.Provider.RefreshGrants);
    }

    [Fact]
    public async Task AProviderThatCannotBeReachedKeepsTheSessionForADayAndNoLonger()
    {
        AllowlistedAs("google");
        var browser = _factory.CreateBrowser(handleCookies: true);
        using (await SignInAsync(browser, "/api/auth/oauth/google")) { }
        _factory.Provider.RefreshStatus = HttpStatusCode.ServiceUnavailable;
        _factory.Provider.RefreshBody   = """{"error":"temporarily_unavailable"}""";

        // An outage is not a refusal: the session goes on, asking again at every refresh...
        _factory.Clock.Advance(TimeSpan.FromHours(2));
        using (var refresh = await RefreshAsync(browser))
            Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
        _factory.Clock.Advance(TimeSpan.FromHours(21));
        using (var refresh = await RefreshAsync(browser))
            Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
        Assert.Equal(2, _factory.Provider.RefreshGrants.Count);

        // ...but not for longer than a day without a confirmation.
        _factory.Clock.Advance(TimeSpan.FromHours(2));
        using var unverified = await RefreshAsync(browser);
        Assert.Equal(HttpStatusCode.Unauthorized, unverified.StatusCode);
    }

    // ── Getting a refresh token ───────────────────────────────────────────────

    [Fact]
    public async Task WithNoRefreshTokenAnywhereTheBrowserIsSentRoundOnceForConsent()
    {
        AllowlistedAs("google");
        _factory.Provider.CodeRefreshToken = null;
        var browser = _factory.CreateBrowser(handleCookies: true);

        using var first = await SignInAsync(browser, "/api/auth/oauth/google");
        var consentPath = first.Headers.Location!.OriginalString;
        Assert.Equal("/api/auth/oauth/google?consent=1", consentPath);
        Assert.Null(SessionCookie.CookieIn(first));

        using (var challenge = await browser.GetAsync(consentPath))
            Assert.Equal("consent", QueryHelpers.ParseQuery(challenge.Headers.Location!.Query)["prompt"].ToString());

        // Consent with a token: signed in, and the token is the one the hourly check uses.
        _factory.Provider.CodeRefreshToken = "provider-refresh-after-consent";
        using var second = await SignInAsync(browser, consentPath);
        Assert.Equal("/oauth-callback", second.Headers.Location!.OriginalString);
        _factory.Clock.Advance(TimeSpan.FromMinutes(61));
        using (var refresh = await RefreshAsync(browser))
            Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
        Assert.Equal("provider-refresh-after-consent", _factory.Provider.RefreshGrants[^1].RefreshToken);
    }

    [Fact]
    public async Task ConsentThatStillYieldsNoTokenSignsInButOnlyForADay()
    {
        AllowlistedAs("google");
        _factory.Provider.CodeRefreshToken = null;
        var browser = _factory.CreateBrowser(handleCookies: true);

        using (var first = await SignInAsync(browser, "/api/auth/oauth/google"))
            Assert.Equal("/api/auth/oauth/google?consent=1", first.Headers.Location!.OriginalString);
        using var second = await SignInAsync(browser, "/api/auth/oauth/google?consent=1");

        // Not a second round: consent was asked already.
        Assert.Equal("/oauth-callback", second.Headers.Location!.OriginalString);
        _factory.Clock.Advance(TimeSpan.FromHours(23));
        using (var refresh = await RefreshAsync(browser))
            Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
        _factory.Clock.Advance(TimeSpan.FromHours(2));
        using (var refresh = await RefreshAsync(browser))
            Assert.Equal(HttpStatusCode.Unauthorized, refresh.StatusCode);
    }

    [Fact]
    public async Task AStoredTokenTheProviderNoLongerHonoursIsReplacedThroughConsent()
    {
        AllowlistedAs("google");
        var browser = _factory.CreateBrowser(handleCookies: true);
        using (await SignInAsync(browser, "/api/auth/oauth/google")) { }

        // Months later: the stored token has lapsed, and a plain sign-in brings no new one.
        _factory.Provider.CodeRefreshToken = null;
        _factory.Provider.RefreshStatus    = HttpStatusCode.BadRequest;
        _factory.Provider.RefreshBody      = FakeOAuthProvider.InvalidGrantBody;
        var later = _factory.CreateBrowser(handleCookies: true);

        using var signIn = await SignInAsync(later, "/api/auth/oauth/google");

        // Tried once at the sign-in, refused, so consent is asked for rather than a session
        // opened that the first hourly check would end.
        Assert.Equal("/api/auth/oauth/google?consent=1", signIn.Headers.Location!.OriginalString);
        Assert.Single(_factory.Provider.RefreshGrants);
    }

    // ── Microsoft ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task EntraRotatesTheRefreshTokenAndTheNextCheckUsesTheNewOne()
    {
        AllowlistedAs("microsoft");
        var browser = _factory.CreateBrowser(handleCookies: true);

        using (var challenge = await browser.GetAsync("/api/auth/oauth/microsoft"))
            Assert.Contains("offline_access",
                QueryHelpers.ParseQuery(challenge.Headers.Location!.Query)["scope"].ToString(), StringComparison.Ordinal);
        using (var signIn = await SignInAsync(browser, "/api/auth/oauth/microsoft"))
            Assert.Equal("/oauth-callback", signIn.Headers.Location!.OriginalString);

        _factory.Provider.RefreshBody = FakeOAuthProvider.OkRefreshBody(newRefreshToken: "provider-refresh-2");
        _factory.Clock.Advance(TimeSpan.FromMinutes(61));
        using (var refresh = await RefreshAsync(browser))
            Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);
        _factory.Clock.Advance(TimeSpan.FromMinutes(61));
        using (var refresh = await RefreshAsync(browser))
            Assert.Equal(HttpStatusCode.OK, refresh.StatusCode);

        var grants = _factory.Provider.RefreshGrants;
        Assert.Equal(2, grants.Count);
        Assert.Equal("provider-refresh-1", grants[0].RefreshToken);
        Assert.Equal("provider-refresh-2", grants[1].RefreshToken);
        Assert.Contains("offline_access", grants[0].Scope, StringComparison.Ordinal);
    }
}
