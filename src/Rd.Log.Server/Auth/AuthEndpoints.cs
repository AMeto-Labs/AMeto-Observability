using System.Security.Claims;

namespace Rd.Log.Server.Auth;

internal static class AuthEndpoints
{
    public static void MapAuthEndpoints(this WebApplication app)
    {
        // ── Login (open) ──────────────────────────────────────────────────────
        app.MapPost("/api/auth/login", (LoginRequest req, AuthStore store, JwtIssuer issuer) =>
        {
            if (string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
                return Results.BadRequest(new { error = "Username and password required." });

            if (!store.ValidateUser(req.Username.Trim(), req.Password))
                return Results.Unauthorized();

            var role  = store.GetRole(req.Username.Trim()) ?? "manager";
            var token = issuer.Issue(req.Username.Trim(), role);
            return Results.Ok(new { token, expiresIn = JwtIssuer.ExpiresInSeconds });
        });

        // ── Refresh ───────────────────────────────────────────────────────────
        app.MapPost("/api/auth/refresh", (HttpContext ctx, JwtIssuer issuer) =>
        {
            var name  = ctx.User.FindFirst(ClaimTypes.Name)?.Value ?? "unknown";
            var role  = ctx.User.FindFirst(ClaimTypes.Role)?.Value ?? "manager";
            var token = issuer.Issue(name, role);
            return Results.Ok(new { token, expiresIn = JwtIssuer.ExpiresInSeconds });
        }).RequireAuthorization();

        // ── Users: list ───────────────────────────────────────────────────────
        app.MapGet("/api/users", (HttpContext ctx, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            return Results.Ok(store.ListUsers().Select(u => new
            {
                u.Id, u.Username, u.Role,
                CreatedAt = u.CreatedAt.ToString("O"),
            }));
        }).RequireAuthorization();

        // ── Users: create ─────────────────────────────────────────────────────
        app.MapPost("/api/users", (HttpContext ctx, CreateUserRequest req, AuthStore store) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            if (string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
                return Results.BadRequest(new { error = "Username and password required." });

            var role = req.Role is "admin" or "manager" ? req.Role : "manager";
            try
            {
                var user = store.CreateUser(req.Username.Trim(), req.Password, role);
                return Results.Ok(new { user.Id, user.Username, user.Role,
                                        CreatedAt = user.CreatedAt.ToString("O") });
            }
            catch (Microsoft.Data.Sqlite.SqliteException ex) when (ex.SqliteErrorCode == 19)
            {
                return Results.Conflict(new { error = "Username already exists." });
            }
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

        // ── API keys: list ────────────────────────────────────────────────────
        app.MapGet("/api/auth/keys", (AuthStore store) =>
            Results.Ok(store.ListApiKeys().Select(k => new
            {
                k.Id, k.Name, k.CreatedBy,
                KeyPreview = k.KeyHash[..8] + "…",
                CreatedAt  = k.CreatedAt.ToString("O"),
            })))
            .RequireAuthorization();

        // ── API keys: create ──────────────────────────────────────────────────
        app.MapPost("/api/auth/keys", (
            HttpContext ctx, CreateApiKeyRequest req, AuthStore store, ApiKeyCache cache) =>
        {
            if (string.IsNullOrWhiteSpace(req.Name))
                return Results.BadRequest(new { error = "Name is required." });

            var createdBy = ctx.User.FindFirst(ClaimTypes.Name)?.Value ?? "unknown";
            var rec = store.CreateApiKey(req.Name.Trim(), createdBy, req.Key?.Trim());
            // Refresh the in-memory cache so the new key is usable immediately.
            cache.Invalidate();
            // Full key returned ONLY here — never again.
            return Results.Ok(new { rec.Id, rec.Name, rec.Key, rec.CreatedBy,
                                    CreatedAt = rec.CreatedAt.ToString("O") });
        }).RequireAuthorization();

        // ── API keys: delete ──────────────────────────────────────────────────
        app.MapDelete("/api/auth/keys/{id}", (string id, AuthStore store, ApiKeyCache cache) =>
        {
            var deleted = store.DeleteApiKey(id);
            if (deleted) cache.Invalidate();
            return deleted ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization();
    }

    private static bool IsAdmin(HttpContext ctx) =>
        ctx.User.FindFirst(ClaimTypes.Role)?.Value == "admin";
}

internal sealed record LoginRequest(string Username, string Password);
internal sealed record CreateUserRequest(string Username, string Password, string Role = "manager");
internal sealed record CreateApiKeyRequest(string Name, string? Key = null);
