using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Google;
using Microsoft.AspNetCore.Authentication.MicrosoftAccount;
using Ameto.Ingestion;

namespace Ameto.Server.Auth;

internal static class AuthEndpoints
{
    public static void MapAuthEndpoints(this WebApplication app)
    {
        // ── Local login (open) ────────────────────────────────────────────────
        app.MapPost("/api/auth/login", (LoginRequest req, AuthStore store, JwtIssuer issuer, AuthOptions opts) =>
        {
            if (!opts.LocalEnabled)
                return Results.BadRequest(new { error = "Local authentication is disabled." });

            if (string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
                return Results.BadRequest(new { error = "Username and password required." });

            if (!store.ValidateUser(req.Username.Trim(), req.Password))
                return Results.Unauthorized();

            var user  = store.FindByUsernameOrEmail(req.Username.Trim(), "");
            var role  = user?.Role ?? "viewer";
            var perms = user?.Permissions ?? ViewPermissions.All;
            var token = issuer.Issue(req.Username.Trim(), role, permissions: perms);
            return Results.Ok(new
            {
                token, expiresIn = JwtIssuer.ExpiresInSeconds, role,
                permissions = (int)(role == "admin" ? ViewPermissions.All : perms),
            });
        });

        // ── OAuth: initiate ───────────────────────────────────────────────────
        // GET /api/auth/oauth/google   → redirects to Google consent screen
        // GET /api/auth/oauth/microsoft → redirects to Microsoft login
        // The actual callback is handled by the OAuth middleware (OnTicketReceived
        // event in AuthServiceExtensions), not by a separate endpoint here.
        app.MapGet("/api/auth/oauth/{provider}", async (string provider, HttpContext ctx, AuthOptions opts) =>
        {
            var scheme = provider.ToLowerInvariant() switch
            {
                "google"    when opts.Google    is { ClientId.Length: > 0 } => GoogleDefaults.AuthenticationScheme,
                "microsoft" when opts.Microsoft is { ClientId.Length: > 0 } => MicrosoftAccountDefaults.AuthenticationScheme,
                _ => null,
            };

            if (scheme is null)
                return Results.BadRequest(new { error = $"OAuth provider '{provider}' is not configured." });

            // RedirectUri here is the ASP.NET Core post-auth destination, but
            // it is overridden by HandleResponse() in the OnTicketReceived event.
            await ctx.ChallengeAsync(scheme, new AuthenticationProperties { RedirectUri = "/" });
            return Results.Empty;
        });

        // ── OAuth providers info (open) ────────────────────────────────────────
        // SPA reads this to know which buttons to show on the login page
        app.MapGet("/api/auth/providers", (AuthOptions opts) => Results.Ok(new
        {
            local     = opts.LocalEnabled,
            google    = opts.Google    is { ClientId.Length: > 0 },
            microsoft = opts.Microsoft is { ClientId.Length: > 0 },
        }));

        // ── Refresh ───────────────────────────────────────────────────────────
        // Re-checks that the user still exists in our DB before issuing a new token.
        // This means deleting a user from Settings takes effect within 2 h (next refresh).
        app.MapPost("/api/auth/refresh", (HttpContext ctx, JwtIssuer issuer, AuthStore store) =>
        {
            var name  = ctx.User.FindFirst(ClaimTypes.Name)?.Value ?? "unknown";
            var role  = ctx.User.FindFirst(ClaimTypes.Role)?.Value ?? "viewer";
            var email = ctx.User.FindFirst(ClaimTypes.Email)?.Value ?? "";

            // Verify the user still exists; if deleted, force logout.
            var user = store.FindByUsernameOrEmail(name, email);
            if (user is null)
                return Results.Unauthorized();

            // Use current role + permissions from DB (admin may have changed them since last login)
            var token = issuer.Issue(user.Username, user.Role, user.Email, permissions: user.Permissions);
            return Results.Ok(new
            {
                token, expiresIn = JwtIssuer.ExpiresInSeconds, role = user.Role,
                permissions = (int)(user.Role == "admin" ? ViewPermissions.All : user.Permissions),
            });
        }).RequireAuthorization();

        // ── Users: list ───────────────────────────────────────────────────────
        app.MapGet("/api/users", (HttpContext ctx, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            return Results.Ok(store.ListUsers().Select(u => new
            {
                u.Id, u.Username, u.DisplayName, u.Email, u.Provider, u.Role,
                Permissions = (int)u.Permissions,
                CreatedAt = u.CreatedAt.ToString("O"),
            }));
        }).RequireAuthorization();

        // ── Users: get one (detail page) ───────────────────────────────────────
        app.MapGet("/api/users/{id}", (HttpContext ctx, string id, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            var u = store.GetUser(id);
            return u is null
                ? Results.NotFound()
                : Results.Ok(new
                {
                    u.Id, u.Username, u.DisplayName, u.Email, u.Provider, u.Role,
                    Permissions = (int)u.Permissions,
                    CreatedAt = u.CreatedAt.ToString("O"),
                });
        }).RequireAuthorization();

        // ── Users: create local ───────────────────────────────────────────────
        app.MapPost("/api/users", (HttpContext ctx, CreateUserRequest req, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            if (string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
                return Results.BadRequest(new { error = "Username and password required." });

            var role = AuthStore.NormaliseRole(req.Role);
            var perms = req.Permissions is int p ? (ViewPermissions)p & ViewPermissions.All : ViewPermissions.All;
            try
            {
                var user = store.CreateUser(req.Username.Trim(), req.Password, role, perms);
                return Results.Ok(new { user.Id, user.Username, user.DisplayName,
                                        user.Email, user.Provider, user.Role,
                                        Permissions = (int)user.Permissions,
                                        CreatedAt = user.CreatedAt.ToString("O") });
            }
            catch (Microsoft.Data.Sqlite.SqliteException ex) when (ex.SqliteErrorCode == 19)
            {
                return Results.Conflict(new { error = "Username already exists." });
            }
        }).RequireAuthorization();

        // ── Users: create OAuth allowlist entry ────────────────────────────────
        app.MapPost("/api/users/oauth", (HttpContext ctx, CreateOAuthUserRequest req, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            if (string.IsNullOrWhiteSpace(req.Email))
                return Results.BadRequest(new { error = "Email is required." });

            var providerRaw = req.Provider?.ToLowerInvariant();
            var provider = providerRaw is "google" or "microsoft" ? providerRaw : "google";
            var role     = AuthStore.NormaliseRole(req.Role ?? "viewer");
            try
            {
                var user = store.CreateOAuthUser(req.Email.Trim(), req.DisplayName?.Trim() ?? req.Email.Trim(), provider, role);
                return Results.Ok(new { user.Id, user.Username, user.DisplayName,
                                        user.Email, user.Provider, user.Role,
                                        Permissions = (int)user.Permissions,
                                        CreatedAt = user.CreatedAt.ToString("O") });
            }
            catch (Microsoft.Data.Sqlite.SqliteException ex) when (ex.SqliteErrorCode == 19)
            {
                return Results.Conflict(new { error = "User with this email and provider already exists." });
            }
        }).RequireAuthorization();

        // ── Users: update role ────────────────────────────────────────────────
        app.MapPatch("/api/users/{id}/role", (HttpContext ctx, string id, UpdateRoleRequest req, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            var role = AuthStore.NormaliseRole(req.Role);
            return store.UpdateUserRole(id, role) ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization();

        // ── Users: update display name + role (detail page) ───────────────────
        app.MapPatch("/api/users/{id}", (HttpContext ctx, string id, UpdateUserRequest req, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            var name = req.DisplayName?.Trim();
            if (string.IsNullOrEmpty(name))
                return Results.BadRequest(new { error = "Display name is required." });
            var role = AuthStore.NormaliseRole(req.Role ?? "viewer");
            // Mask to known bits; when omitted, keep the user's current scopes.
            var perms = req.Permissions is int p
                ? (ViewPermissions)p & ViewPermissions.All
                : (store.GetUser(id)?.Permissions ?? ViewPermissions.All);
            return store.UpdateUser(id, name, role, perms) ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization();

        // ── Users: reset password (admin, local users only) ───────────────────
        app.MapPatch("/api/users/{id}/password", (HttpContext ctx, string id, ChangePasswordRequest req, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            // Passwords are not trimmed (leading/trailing spaces are significant);
            // reject only all-whitespace/empty and anything under the minimum length.
            if (string.IsNullOrWhiteSpace(req.Password) || req.Password.Length < 6)
                return Results.BadRequest(new { error = "Password must be at least 6 characters." });

            var user = store.GetUser(id);
            if (user is null) return Results.NotFound();
            if (user.Provider != "local")
                return Results.BadRequest(new { error = "Only local users have a password." });

            return store.SetPassword(id, req.Password) ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization();

        // ── OAuth domain allowlist: list / create / delete ────────────────────
        app.MapGet("/api/users/oauth-domains", (HttpContext ctx, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            return Results.Ok(store.ListOAuthDomains().Select(d => new
            {
                d.Id, d.Provider, d.Domain, d.Role,
                CreatedAt = d.CreatedAt.ToString("O"),
            }));
        }).RequireAuthorization();

        app.MapPost("/api/users/oauth-domains", (HttpContext ctx, CreateOAuthDomainRequest req, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            var domain = req.Domain?.Trim().TrimStart('@').ToLowerInvariant();
            if (string.IsNullOrEmpty(domain) || domain.IndexOf('@') >= 0)
                return Results.BadRequest(new { error = "A valid domain (e.g. ameto.com) is required." });

            var provider = req.Provider?.ToLowerInvariant() is "google" or "microsoft"
                ? req.Provider.ToLowerInvariant() : "google";
            var role = AuthStore.NormaliseRole(req.Role ?? "viewer");

            try
            {
                var d = store.CreateOAuthDomain(provider, domain, role);
                return Results.Ok(new
                {
                    d.Id, d.Provider, d.Domain, d.Role,
                    CreatedAt = d.CreatedAt.ToString("O"),
                });
            }
            catch (Microsoft.Data.Sqlite.SqliteException ex) when (ex.SqliteErrorCode == 19)
            {
                return Results.Conflict(new { error = "Domain rule already exists for this provider." });
            }
        }).RequireAuthorization();

        app.MapDelete("/api/users/oauth-domains/{id}", (HttpContext ctx, string id, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            return store.DeleteOAuthDomain(id) ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization();

        // ── Users: delete ─────────────────────────────────────────────────────
        app.MapDelete("/api/users/{id}", (HttpContext ctx, string id, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            var self   = ctx.User.FindFirst(ClaimTypes.Name)?.Value;
            var target = store.ListUsers().FirstOrDefault(u => u.Id == id);
            if (target?.Username.Equals(self, StringComparison.OrdinalIgnoreCase) == true)
                return Results.BadRequest(new { error = "Cannot delete your own account." });

            return store.DeleteUser(id) ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization();

        // ── API keys: list (manager+) ─────────────────────────────────────────
        app.MapGet("/api/auth/keys", (AuthStore store) =>
            Results.Ok(store.ListApiKeys().Select(k => new
            {
                k.Id, k.Name, k.Description,
                Permissions = (int)k.Permissions,
                k.CreatedBy,
                KeyPreview = k.KeyHash[..8] + "…",
                CreatedAt  = k.CreatedAt.ToString("O"),
            })))
            .RequireAuthorization(AuthServiceExtensions.PolicyManager);

        // ── API keys: create (manager+) ───────────────────────────────────────
        app.MapPost("/api/auth/keys", (
            HttpContext ctx, CreateApiKeyRequest req, AuthStore store, ApiKeyCache cache) =>
        {
            if (string.IsNullOrWhiteSpace(req.Name))
                return Results.BadRequest(new { error = "Name is required." });

            var createdBy = ctx.User.FindFirst(ClaimTypes.Name)?.Value ?? "unknown";
            // Mask to known bits; empty/None grants everything (a key with no scope is useless).
            var permissions = (ApiKeyPermissions)(req.Permissions ?? 0) & ApiKeyPermissions.All;
            if (permissions == ApiKeyPermissions.None) permissions = ApiKeyPermissions.All;
            var rec = store.CreateApiKey(
                req.Name.Trim(), req.Description?.Trim() ?? string.Empty,
                permissions, createdBy, req.Key?.Trim());
            cache.Invalidate();
            return Results.Ok(new
            {
                rec.Id, rec.Name, rec.Description,
                Permissions = (int)rec.Permissions,
                rec.Key, rec.CreatedBy,
                CreatedAt = rec.CreatedAt.ToString("O"),
            });
        }).RequireAuthorization(AuthServiceExtensions.PolicyManager);

        // ── API keys: delete (manager+) ───────────────────────────────────────
        app.MapDelete("/api/auth/keys/{id}", (string id, AuthStore store, ApiKeyCache cache) =>
        {
            var deleted = store.DeleteApiKey(id);
            if (deleted) cache.Invalidate();
            return deleted ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization(AuthServiceExtensions.PolicyManager);
    }

    private static bool IsAdmin(HttpContext ctx) =>
        ctx.User.FindFirst(ClaimTypes.Role)?.Value == "admin";
}

internal sealed record LoginRequest(string Username, string Password);
internal sealed record CreateUserRequest(string Username, string Password, string Role = "viewer", int? Permissions = null);
internal sealed record CreateOAuthUserRequest(string Email, string? DisplayName, string? Provider, string? Role);
internal sealed record UpdateRoleRequest(string Role);
internal sealed record UpdateUserRequest(string? DisplayName, string? Role, int? Permissions = null);
internal sealed record ChangePasswordRequest(string Password);
internal sealed record CreateOAuthDomainRequest(string? Domain, string? Provider, string? Role = "viewer");
internal sealed record CreateApiKeyRequest(
    string Name,
    string? Key = null,
    string? Description = null,
    int? Permissions = null);

