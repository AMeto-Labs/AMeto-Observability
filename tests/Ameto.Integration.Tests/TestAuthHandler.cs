using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ameto.Integration.Tests;

/// <summary>
/// Test-only authentication handler that authenticates every request as an admin.
///
/// The production app protects read/query endpoints with JWT bearer + role
/// policies (see <c>AddAmetoAuth</c>). Rather than mint and refresh real tokens
/// in every test, the harness swaps the default authentication scheme for this
/// handler, so <c>RequireAuthorization()</c> endpoints are reachable while still
/// exercising the real authorization pipeline (an authenticated admin principal
/// satisfies the viewer/manager/admin policies).
///
/// The ingest hot path does not use this pipeline — it validates an API key
/// against <c>ApiKeyCache</c> directly — so the factory additionally seeds a key
/// and attaches it as a header.
/// </summary>
internal sealed class TestAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
{
    public const string SchemeName = "IntegrationTest";

    /// <summary>
    /// Request header letting a test authenticate as something other than an admin, so the
    /// role-gated endpoints can be exercised from below. Absent — the case for every test that
    /// predates it — still means admin, and the full view scope with it.
    /// </summary>
    public const string RoleHeader = "X-Test-Role";

    public TestAuthHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory                               logger,
        UrlEncoder                                   encoder)
        : base(options, logger, encoder)
    {
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var role = Request.Headers.TryGetValue(RoleHeader, out var hdr) && hdr.Count > 0
                && !string.IsNullOrWhiteSpace(hdr[0])
            ? hdr[0]!
            : "admin";

        // ClaimTypes.Role = "admin" satisfies every RequireRole policy the app
        // defines (admin / manager / viewer). Lesser roles carry the full view
        // bitmask so a refusal in a test is about the role, never a missing scope.
        Claim[] claims =
        [
            new(ClaimTypes.Name, "integration-test-" + role),
            new(ClaimTypes.Role, role),
            new("perm", ((int)Ameto.Server.Auth.ViewPermissions.All).ToString()),
        ];

        var identity  = new ClaimsIdentity(claims, SchemeName);
        var principal = new ClaimsPrincipal(identity);
        var ticket    = new AuthenticationTicket(principal, SchemeName);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}
