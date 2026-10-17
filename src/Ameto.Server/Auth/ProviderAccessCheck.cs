using System.Collections.Concurrent;
using System.Text.Json;
using Ameto.Core;
using Microsoft.AspNetCore.Authentication.Google;
using Microsoft.AspNetCore.Authentication.MicrosoftAccount;
using Microsoft.AspNetCore.Authentication.OAuth;
using Microsoft.Extensions.Options;

namespace Ameto.Server.Auth;

/// <summary>What a session refresh may do, as far as the identity provider is concerned.</summary>
internal enum ProviderVerdict
{
    /// <summary>The provider vouched for the account recently enough, or could not be asked yet.</summary>
    Continue,

    /// <summary>The provider refused: the account is disabled, deleted, or its sign-ins were revoked.</summary>
    Ended,

    /// <summary>Nothing has confirmed the account for longer than <see cref="ProviderAccessCheck.TrustAtMost"/>.</summary>
    Unverified,
}

/// <summary>What the OAuth callback does next.</summary>
internal enum SignInStep
{
    Proceed,

    /// <summary>Send the browser back to the provider with <c>prompt=consent</c> to get a refresh token.</summary>
    AskConsent,
}

/// <summary>
/// Lets a block at Google or Microsoft end an Ameto session.
///
/// <para>An OAuth sign-in proves the account is good at that moment and says nothing afterwards.
/// With 2-hour tokens that hardly mattered; with sessions that last a month, an account
/// suspended in Google Workspace or disabled in Entra ID would have kept its Ameto access for the
/// rest of it. So the sign-in now asks for offline access, the provider's refresh token is kept
/// (encrypted with the master key, as alert-channel secrets are), and at most every
/// <see cref="CheckEvery"/> a session refresh redeems it. A refusal ends every session of the
/// account; an answer that is not a refusal (an outage, a timeout, our own credentials rejected)
/// keeps the session going for up to <see cref="TrustAtMost"/> since the last confirmation, after
/// which the user has to sign in through the provider again.</para>
///
/// <para>Only <c>invalid_grant</c> and <c>interaction_required</c> count as a refusal. Both mean
/// the provider will not continue this sign-in without the person present — a disabled or deleted
/// account, revoked sessions, a changed password, a new MFA requirement — and every other error
/// says something about the request or the provider instead.</para>
/// </summary>
internal sealed class ProviderAccessCheck
{
    /// <summary>How long a confirmation from the provider holds before a refresh asks again.</summary>
    public static readonly TimeSpan CheckEvery = TimeSpan.FromHours(1);

    /// <summary>How long a session may go on when the provider cannot be asked.</summary>
    public static readonly TimeSpan TrustAtMost = TimeSpan.FromHours(24);

    /// <summary>Bound on one call to the token endpoint; the refresh waits on it.</summary>
    private static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(10);

    private readonly AuthStore                                 _store;
    private readonly ISecretProtector                          _protector;
    private readonly IOptionsMonitor<GoogleOptions>            _google;
    private readonly IOptionsMonitor<MicrosoftAccountOptions>  _microsoft;
    private readonly TimeProvider                              _time;
    private readonly ILogger<ProviderAccessCheck>              _logger;

    /// <summary>One check per account at a time: tabs refreshing together ask the provider once.</summary>
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new(StringComparer.Ordinal);

    public ProviderAccessCheck(
        AuthStore store, ISecretProtector protector,
        IOptionsMonitor<GoogleOptions> google, IOptionsMonitor<MicrosoftAccountOptions> microsoft,
        [FromKeyedServices(AuthSessions.ClockKey)] TimeProvider time, ILogger<ProviderAccessCheck> logger)
    {
        _store     = store;
        _protector = protector;
        _google    = google;
        _microsoft = microsoft;
        _time      = time;
        _logger    = logger;
    }

    /// <summary>
    /// Records an OAuth sign-in. <paramref name="freshRefreshToken"/> is what the provider returned
    /// with it, if anything; <paramref name="consentAsked"/> says this sign-in already went
    /// through <c>prompt=consent</c>.
    /// </summary>
    public async Task<SignInStep> OnSignInAsync(
        UserRecord user, string? freshRefreshToken, bool consentAsked, CancellationToken ct)
    {
        long nowMs = NowMs();
        if (!string.IsNullOrEmpty(freshRefreshToken))
        {
            _store.SetProviderAccess(user.Id, _protector.Protect(freshRefreshToken), nowMs);
            return SignInStep.Proceed;
        }

        // Google hands out a refresh token only with consent — the first sign-in, not the ones
        // after it. The token kept from then is fine unless it lapsed (Google drops one unused for
        // six months) or was revoked, and keeping a dead one would end the session an hour from
        // now, at the first check, every time the user signed in. So it is tried once, here.
        var stored = _protector.Unprotect(_store.GetProviderAccess(user.Id).ProtectedRefreshToken);
        if (stored.Length > 0)
        {
            var answer = await AskAsync(user.Provider, stored, ct).ConfigureAwait(false);
            if (answer.Outcome != Outcome.Refused)
            {
                // The sign-in itself is the provider vouching for the account, whatever this one
                // call managed to say about the stored token.
                _store.SetProviderAccess(user.Id, ProtectOrKeep(answer.NewRefreshToken), nowMs);
                return SignInStep.Proceed;
            }
            _store.ClearProviderToken(user.Id);
        }

        if (!consentAsked) return SignInStep.AskConsent;

        // Asked for consent and still given nothing: carry on without. The session then lasts
        // TrustAtMost from this sign-in, the longest anything goes unconfirmed.
        _logger.LogWarning(
            "{Provider} returned no refresh token for {User} even with consent; their session will need " +
            "a new sign-in after {Hours} h", user.Provider, user.Email, TrustAtMost.TotalHours);
        _store.SetProviderAccess(user.Id, protectedRefreshToken: null, nowMs);
        return SignInStep.Proceed;
    }

    /// <summary>Decides whether a session of <paramref name="user"/> may be refreshed.</summary>
    public async Task<ProviderVerdict> CheckAsync(UserRecord user, CancellationToken ct)
    {
        if (NowMs() - _store.GetProviderAccess(user.Id).CheckedAtMs < (long)CheckEvery.TotalMilliseconds)
            return ProviderVerdict.Continue;

        var gate = _gates.GetOrAdd(user.Id, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // Read again under the gate: the request ahead of this one may have just asked.
            long nowMs = NowMs();
            var  state = _store.GetProviderAccess(user.Id);
            if (nowMs - state.CheckedAtMs < (long)CheckEvery.TotalMilliseconds)
                return ProviderVerdict.Continue;

            var token = _protector.Unprotect(state.ProtectedRefreshToken);
            if (token.Length > 0)
            {
                var answer = await AskAsync(user.Provider, token, ct).ConfigureAwait(false);
                switch (answer.Outcome)
                {
                    case Outcome.Confirmed:
                        _store.SetProviderAccess(user.Id, ProtectOrKeep(answer.NewRefreshToken), nowMs);
                        return ProviderVerdict.Continue;
                    case Outcome.Refused:
                        _store.ClearProviderToken(user.Id);
                        return ProviderVerdict.Ended;
                }
            }

            // No answer, or nothing to ask with. Left unconfirmed, so the next refresh asks again.
            return nowMs - state.CheckedAtMs <= (long)TrustAtMost.TotalMilliseconds
                ? ProviderVerdict.Continue
                : ProviderVerdict.Unverified;
        }
        finally
        {
            gate.Release();
        }
    }

    private enum Outcome { Confirmed, Refused, Failed }

    private readonly record struct Answer(Outcome Outcome, string? NewRefreshToken = null);

    /// <summary>Redeems <paramref name="refreshToken"/> at the provider's token endpoint.</summary>
    private async Task<Answer> AskAsync(string provider, string refreshToken, CancellationToken ct)
    {
        OAuthOptions? o = provider switch
        {
            "google"    => _google.Get(GoogleDefaults.AuthenticationScheme),
            "microsoft" => _microsoft.Get(MicrosoftAccountDefaults.AuthenticationScheme),
            _           => null,
        };
        if (o?.Backchannel is null || string.IsNullOrEmpty(o.ClientId) || string.IsNullOrEmpty(o.TokenEndpoint))
        {
            // The provider was configured when the user signed in and is not any more.
            _logger.LogWarning("Cannot confirm a {Provider} account: the provider is not configured", provider);
            return new Answer(Outcome.Failed);
        }

        var form = new Dictionary<string, string>
        {
            ["grant_type"]    = "refresh_token",
            ["refresh_token"] = refreshToken,
            ["client_id"]     = o.ClientId,
            ["client_secret"] = o.ClientSecret,
        };
        // Entra expects the scopes again on a refresh; Google takes the original grant's.
        if (provider == "microsoft") form["scope"] = string.Join(' ', o.Scope);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(CallTimeout);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, o.TokenEndpoint)
            {
                Content = new FormUrlEncodedContent(form),
            };
            request.Headers.Accept.ParseAdd("application/json");
            using var response = await o.Backchannel.SendAsync(request, timeout.Token).ConfigureAwait(false);
            var body = await response.Content.ReadAsStringAsync(timeout.Token).ConfigureAwait(false);

            if (response.IsSuccessStatusCode)
                return new Answer(Outcome.Confirmed, ReadString(body, "refresh_token"));

            var error = ReadString(body, "error");
            if (error is "invalid_grant" or "interaction_required")
                return new Answer(Outcome.Refused);

            _logger.LogWarning(
                "{Provider} did not confirm an account: HTTP {Status}, error {Error}",
                provider, (int)response.StatusCode, error ?? "(none)");
            return new Answer(Outcome.Failed);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning("{Provider} did not answer within {Seconds} s", provider, CallTimeout.TotalSeconds);
            return new Answer(Outcome.Failed);
        }
        catch (HttpRequestException ex)
        {
            _logger.LogWarning(ex, "Could not reach {Provider} to confirm an account", provider);
            return new Answer(Outcome.Failed);
        }
    }

    /// <summary>A rotated refresh token (Entra sends one on every redemption) replaces the stored one.</summary>
    private string? ProtectOrKeep(string? newRefreshToken) =>
        string.IsNullOrEmpty(newRefreshToken) ? null : _protector.Protect(newRefreshToken);

    private static string? ReadString(string json, string property)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                && doc.RootElement.TryGetProperty(property, out var value)
                && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private long NowMs() => _time.GetUtcNow().ToUnixTimeMilliseconds();
}
