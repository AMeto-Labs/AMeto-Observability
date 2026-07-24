using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ameto.Ingestion;

namespace Ameto.Server.Auth;

/// <summary>
/// Authenticates read (query) requests carrying an ingest-style API key
/// (<c>X-Seq-ApiKey</c> / <c>Authorization: apikey …</c> / <c>?apiKey=</c>). The
/// key's READ permission bits are mapped to the JWT <c>perm</c> claim, so the
/// existing per-view authorization policies gate an API-key caller exactly like a
/// user — but with a non-admin <c>apikey</c> role, so it never bypasses scopes.
///
/// Ingest endpoints keep their own fast <see cref="IApiKeyValidator"/> check and
/// are unaffected; this scheme only participates in the read policies that list it.
/// </summary>
internal sealed class ApiKeyAuthenticationHandler : AuthenticationHandler<AuthenticationSchemeOptions>
{
    public const string SchemeName = "ApiKey";

    private readonly ApiKeyCache _cache;

    public ApiKeyAuthenticationHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger, UrlEncoder encoder, ApiKeyCache cache)
        : base(options, logger, encoder)
    {
        _cache = cache;
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var raw = ApiKeyHeader.Extract(Request);
        if (string.IsNullOrEmpty(raw))
            return Task.FromResult(AuthenticateResult.NoResult()); // let JWT (or nothing) handle it

        var granted = _cache.Resolve(raw);
        if (granted is null)
            return Task.FromResult(AuthenticateResult.NoResult()); // unknown key — not our failure to report

        var view = ToViewPermissions(granted.Value);

        var identity = new ClaimsIdentity(SchemeName);
        identity.AddClaim(new Claim(ClaimTypes.Name, "apikey"));
        identity.AddClaim(new Claim(ClaimTypes.Role, "apikey"));
        identity.AddClaim(new Claim(ClaimsPrincipalExtensions.PermClaim, ((int)view).ToString()));

        var principal = new ClaimsPrincipal(identity);
        return Task.FromResult(AuthenticateResult.Success(
            new AuthenticationTicket(principal, SchemeName)));
    }

    /// <summary>Maps read scopes to the equivalent view scopes (ReadLogs also grants Stats — stats are log-derived).</summary>
    private static ViewPermissions ToViewPermissions(ApiKeyPermissions p)
    {
        var v = ViewPermissions.None;
        if (p.HasFlag(ApiKeyPermissions.ReadLogs))    v |= ViewPermissions.Logs | ViewPermissions.Stats;
        if (p.HasFlag(ApiKeyPermissions.ReadMetrics)) v |= ViewPermissions.Metrics;
        if (p.HasFlag(ApiKeyPermissions.ReadTraces))  v |= ViewPermissions.Traces;
        return v;
    }
}
