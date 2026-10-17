using System.Security.Claims;

namespace Ameto.Server.Auth;

/// <summary>
/// Ending sign-ins: the user's own sign-out, and the admin's two levers on someone else's —
/// ending their sessions, and blocking the account outright. See <see cref="AuthSessions"/>.
/// </summary>
internal static class AuthSessionEndpointMapper
{
    public static void MapAuthSessionEndpoints(this WebApplication app)
    {
        // ── Sign out ──────────────────────────────────────────────────────────
        // Open, like refresh: the cookie names the session, and the access token may already have
        // expired. Ends the session on the server, so the cookie stops working even where a copy
        // of it survives, and clears it from this browser.
        app.MapPost("/api/auth/logout", (HttpContext ctx, AuthSessions sessions) =>
        {
            sessions.End(ctx);
            return Results.NoContent();
        });

        // ── Users: block / unblock ────────────────────────────────────────────
        // Blocking ends every session at once — open tabs included, live tails aborted — and
        // refuses every sign-in after it, through any provider, until unblocked. The row stays,
        // which is the difference from deleting it: an OAuth domain rule cannot provision the
        // same person again behind the admin's back.
        app.MapPatch("/api/users/{id}/blocked", (
            HttpContext ctx, string id, SetBlockedRequest req, AuthStore store,
            AuthSessions sessions, AuthRevocations revocations, ILogger<AuthSessions> logger) =>
        {
            if (!AuthEndpoints.IsAdmin(ctx)) return Results.Forbid();
            var user = store.GetUser(id);
            if (user is null) return Results.NotFound();

            var admin = ctx.User.FindFirst(ClaimTypes.Name)?.Value ?? "";
            if (req.Blocked && user.Username.Equals(admin, StringComparison.OrdinalIgnoreCase))
                return Results.BadRequest(new { error = "You cannot block your own account." });

            // The database first: a refresh racing this reads the flag from there.
            store.SetDisabled(id, req.Blocked);
            if (req.Blocked)
            {
                revocations.UserBlocked(user.Username);
                int ended = sessions.RevokeAllForUser(id, "blocked");
                logger.LogInformation("{Admin} blocked {User}; ended {Count} session(s)", admin, user.Username, ended);
            }
            else
            {
                revocations.UserUnblocked(user.Username);
                logger.LogInformation("{Admin} unblocked {User}", admin, user.Username);
            }
            return Results.NoContent();
        }).RequireAuthorization();

        // ── Users: end every session ──────────────────────────────────────────
        // "Sign out everywhere" without blocking: the user can sign in again straight away.
        app.MapDelete("/api/users/{id}/sessions", (
            HttpContext ctx, string id, AuthStore store, AuthSessions sessions, ILogger<AuthSessions> logger) =>
        {
            if (!AuthEndpoints.IsAdmin(ctx)) return Results.Forbid();
            var user = store.GetUser(id);
            if (user is null) return Results.NotFound();

            int ended = sessions.RevokeAllForUser(id, "admin");
            logger.LogInformation("{Admin} ended {Count} session(s) of {User}",
                ctx.User.FindFirst(ClaimTypes.Name)?.Value, ended, user.Username);
            return Results.Ok(new { ended });
        }).RequireAuthorization();
    }
}

internal sealed record SetBlockedRequest(bool Blocked);
