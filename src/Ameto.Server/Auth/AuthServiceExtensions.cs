using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Google;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authentication.MicrosoftAccount;

namespace Ameto.Server.Auth;

/// <summary>
/// Configuration model for auth providers (read from config.yml).
/// </summary>
public sealed class AuthOptions
{
    /// <summary>Allow local username/password login. Defaults to true.</summary>
    public bool LocalEnabled { get; init; } = true;

    public GoogleAuthOptions? Google   { get; init; }
    public MicrosoftAuthOptions? Microsoft { get; init; }
}

public sealed class GoogleAuthOptions
{
    public string ClientId     { get; init; } = "";
    public string ClientSecret { get; init; } = "";
}

public sealed class MicrosoftAuthOptions
{
    public string ClientId     { get; init; } = "";
    public string ClientSecret { get; init; } = "";
    /// <summary>Azure AD tenant ID or "common" for multi-tenant.</summary>
    public string TenantId     { get; init; } = "common";
}

internal static class AuthServiceExtensions
{
    // Authorization policy names
    public const string PolicyAdmin   = "RequireAdmin";
    public const string PolicyManager = "RequireManager";
    public const string PolicyViewer  = "RequireViewer";

    // Per-view read scopes (admin bypasses; see ClaimsPrincipalExtensions.HasView).
    // Names live in Ameto.Core so cross-assembly mappers gate on the same strings.
    public const string PolicyViewLogs    = Ameto.Core.ViewPolicies.Logs;
    public const string PolicyViewMetrics = Ameto.Core.ViewPolicies.Metrics;
    public const string PolicyViewTraces  = Ameto.Core.ViewPolicies.Traces;
    public const string PolicyViewStats   = Ameto.Core.ViewPolicies.Stats;

    /// <summary>
    /// Registers JWT Bearer authentication, OAuth providers (optional),
    /// authorization policies and all auth-related services into DI.
    /// </summary>
    public static IServiceCollection AddAmetoAuth(
        this IServiceCollection services,
        string                  dataDirectory,
        AuthOptions?            authOptions = null)
    {
        authOptions ??= new AuthOptions();
        Directory.CreateDirectory(dataDirectory);

        // ── JWT secret ─────────────────────────────────────────────────────────
        var secretPath = Path.Combine(dataDirectory, "jwt-secret.bin");
        string secret;
        if (File.Exists(secretPath))
        {
            secret = File.ReadAllText(secretPath, Encoding.UTF8);
        }
        else
        {
            secret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(64));
            File.WriteAllText(secretPath, secret, Encoding.UTF8);
        }

        var issuer = new JwtIssuer(secret);
        services.AddSingleton(new AuthDatabase(dataDirectory));
        services.AddSingleton<AuthStore>();
        services.AddSingleton<SearchHistoryStore>();
        services.AddSingleton(issuer);
        services.AddSingleton<ApiKeyCache>();
        // Expose the cache as the ingest-path validator (used by OTLP endpoints in Ameto.Otel).
        services.AddSingleton<Ameto.Ingestion.IApiKeyValidator>(sp => sp.GetRequiredService<ApiKeyCache>());
        services.AddSingleton(authOptions);

        // ── Authentication ─────────────────────────────────────────────────────
        // Cookie scheme is needed as the temporary sign-in scheme for the OAuth
        // callback flow. JwtBearer remains the default for API requests.
        const string ExternalCookieScheme = "ExternalOAuth";

        var authBuilder = services
            .AddAuthentication(o =>
            {
                o.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                o.DefaultChallengeScheme    = JwtBearerDefaults.AuthenticationScheme;
                // OAuth middleware uses this scheme to temporarily persist the
                // external identity between the redirect and the callback.
                o.DefaultSignInScheme       = ExternalCookieScheme;
            })
            .AddCookie(ExternalCookieScheme, o =>
            {
                o.Cookie.Name     = "rd_oauth_tmp";
                o.Cookie.HttpOnly = true;
                o.Cookie.SameSite = Microsoft.AspNetCore.Http.SameSiteMode.Lax;
                o.Cookie.SecurePolicy = Microsoft.AspNetCore.Http.CookieSecurePolicy.SameAsRequest;
                o.ExpireTimeSpan  = TimeSpan.FromMinutes(10);
            })
            .AddJwtBearer(o =>
            {
                o.TokenValidationParameters = issuer.ValidationParameters;
                // Support ?access_token= query param for SSE (EventSource API
                // cannot set Authorization headers in browsers).
                o.Events = new JwtBearerEvents
                {
                    OnMessageReceived = static ctx =>
                    {
                        if (ctx.Request.Query.TryGetValue("access_token", out var token)
                            && token.Count > 0)
                            ctx.Token = token[0];
                        return Task.CompletedTask;
                    },
                };
            });

        // ── Google OAuth ───────────────────────────────────────────────────────
        if (authOptions.Google is { ClientId.Length: > 0, ClientSecret.Length: > 0 } google)
        {
            authBuilder.AddGoogle(o =>
            {
                o.ClientId     = google.ClientId;
                o.ClientSecret = google.ClientSecret;
                o.CallbackPath = "/api/auth/oauth/google/callback";
                o.SaveTokens   = false;
                // Allow correlation cookie to work over plain HTTP (development / self-hosted)
                o.CorrelationCookie.SameSite     = Microsoft.AspNetCore.Http.SameSiteMode.Lax;
                o.CorrelationCookie.SecurePolicy = Microsoft.AspNetCore.Http.CookieSecurePolicy.SameAsRequest;
                // Handle the result directly in the event so we never redirect back
                // to CallbackPath a second time (which would lose the `state` param).
                o.Events.OnTicketReceived = ctx => HandleOAuthTicket(ctx, "google");
                o.Events.OnRemoteFailure  = ctx => HandleOAuthFailure(ctx);
            });
        }

        // ── Microsoft OAuth ────────────────────────────────────────────────────
        if (authOptions.Microsoft is { ClientId.Length: > 0, ClientSecret.Length: > 0 } ms)
        {
            authBuilder.AddMicrosoftAccount(o =>
            {
                o.ClientId     = ms.ClientId;
                o.ClientSecret = ms.ClientSecret;
                o.CallbackPath = "/api/auth/oauth/microsoft/callback";
                o.AuthorizationEndpoint =
                    $"https://login.microsoftonline.com/{ms.TenantId}/oauth2/v2.0/authorize";
                o.TokenEndpoint =
                    $"https://login.microsoftonline.com/{ms.TenantId}/oauth2/v2.0/token";
                o.SaveTokens = false;
                o.CorrelationCookie.SameSite     = Microsoft.AspNetCore.Http.SameSiteMode.Lax;
                o.CorrelationCookie.SecurePolicy = Microsoft.AspNetCore.Http.CookieSecurePolicy.SameAsRequest;
                o.Events.OnTicketReceived = ctx => HandleOAuthTicket(ctx, "microsoft");
                o.Events.OnRemoteFailure  = ctx => HandleOAuthFailure(ctx);
            });
        }

        // ── Authorization policies ─────────────────────────────────────────────
        services.AddAuthorization(o =>
        {
            // Admin: full access
            o.AddPolicy(PolicyAdmin, p =>
                p.RequireAuthenticatedUser().RequireRole("admin"));

            // Manager: API keys management + all read access
            o.AddPolicy(PolicyManager, p =>
                p.RequireAuthenticatedUser().RequireRole("admin", "manager"));

            // Viewer: read-only (default for most endpoints)
            o.AddPolicy(PolicyViewer, p =>
                p.RequireAuthenticatedUser().RequireRole("admin", "manager", "viewer"));

            // Per-view read scopes — admin bypasses, otherwise the JWT 'perm' bit is required.
            o.AddPolicy(PolicyViewLogs, p => p.RequireAuthenticatedUser()
                .RequireAssertion(static c => c.User.HasView(ViewPermissions.Logs)));
            o.AddPolicy(PolicyViewMetrics, p => p.RequireAuthenticatedUser()
                .RequireAssertion(static c => c.User.HasView(ViewPermissions.Metrics)));
            o.AddPolicy(PolicyViewTraces, p => p.RequireAuthenticatedUser()
                .RequireAssertion(static c => c.User.HasView(ViewPermissions.Traces)));
            o.AddPolicy(PolicyViewStats, p => p.RequireAuthenticatedUser()
                .RequireAssertion(static c => c.User.HasView(ViewPermissions.Stats)));

            // Default policy requires at least viewer
            o.DefaultPolicy = o.GetPolicy(PolicyViewer)!;
        });

        return services;
    }

    // ── OAuth ticket event handlers ────────────────────────────────────────────

    /// <summary>
    /// Called by the OAuth middleware after it has validated the code, obtained
    /// the access token, and built the ClaimsPrincipal.
    /// We check the email against our allowlist, issue our JWT, and redirect to
    /// the SPA — then call HandleResponse() so the middleware does nothing further.
    /// This avoids the double-redirect to CallbackPath that causes the state error.
    /// </summary>
    private static Task HandleOAuthTicket(
        Microsoft.AspNetCore.Authentication.TicketReceivedContext ctx,
        string provider)
    {
        var store  = ctx.HttpContext.RequestServices.GetRequiredService<AuthStore>();
        var issuer = ctx.HttpContext.RequestServices.GetRequiredService<JwtIssuer>();

        var email = ctx.Principal?.FindFirst(System.Security.Claims.ClaimTypes.Email)?.Value
                 ?? ctx.Principal?.FindFirst("email")?.Value;

        if (string.IsNullOrWhiteSpace(email))
        {
            ctx.Response.Redirect("/login?error=no_email");
            ctx.HandleResponse();
            return Task.CompletedTask;
        }

        var displayName = ctx.Principal?.FindFirst(System.Security.Claims.ClaimTypes.Name)?.Value ?? email;
        // Per-email allowlist first, then domain allowlist (auto-provisions on first match).
        var user = store.FindOrCreateOAuthUser(email, displayName, provider);
        if (user is null)
        {
            ctx.Response.Redirect($"/login?error=access_denied&email={Uri.EscapeDataString(email)}");
            ctx.HandleResponse();
            return Task.CompletedTask;
        }

        var token = issuer.Issue(user.Username, user.Role, email, displayName, user.Permissions);

        ctx.Response.Redirect(
            $"/oauth-callback?token={Uri.EscapeDataString(token)}&expiresIn={JwtIssuer.ExpiresInSeconds}&role={user.Role}");
        ctx.HandleResponse();
        return Task.CompletedTask;
    }

    private static Task HandleOAuthFailure(
        Microsoft.AspNetCore.Authentication.RemoteFailureContext ctx)
    {
        ctx.Response.Redirect("/login?error=oauth_failed");
        ctx.HandleResponse();
        return Task.CompletedTask;
    }
}
