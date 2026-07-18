using System.Security.Claims;

namespace Ameto.Server.Updates;

/// <summary>
/// Admin-only software-update endpoints (Settings → Updates):
///   GET  /api/system/update        — current/latest version snapshot
///   POST /api/system/update/check  — force a check right now
///   POST /api/system/update/apply  — Windows: download + launch the installer
/// </summary>
public static class UpdateEndpoints
{
    public static void MapUpdateEndpoints(this WebApplication app)
    {
        app.MapGet("/api/system/update", static (HttpContext ctx, UpdateChecker checker) =>
            !IsAdmin(ctx) ? Results.Forbid() : Results.Ok(Snapshot(checker))
        ).RequireAuthorization();

        app.MapPost("/api/system/update/check", static async (HttpContext ctx, UpdateChecker checker) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            await checker.CheckAsync(ctx.RequestAborted);
            return Results.Ok(Snapshot(checker));
        }).RequireAuthorization();

        app.MapPost("/api/system/update/apply", static async (HttpContext ctx, UpdateChecker checker) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            var (ok, message) = await checker.TryApplyAsync(ctx.RequestAborted);
            return ok ? Results.Accepted(value: new { message })
                      : Results.BadRequest(new { message });
        }).RequireAuthorization();
    }

    private static object Snapshot(UpdateChecker checker)
    {
        var latest    = checker.Latest;
        var container = UpdateChecker.IsContainer();
        var platform  = container ? "docker" : OperatingSystem.IsWindows() ? "windows" : "linux";

        return new
        {
            currentVersion  = UpdateChecker.CurrentVersion,
            latestVersion   = latest?.Tag,
            updateAvailable = checker.UpdateAvailable,
            releaseUrl      = latest?.Url,
            publishedAt     = latest?.PublishedAt,
            checkedAt       = checker.CheckedAt,
            checkError      = checker.LastError,
            platform,
            // The button can only truly update a Windows (service/portable) install;
            // Docker updates via image pull (Watchtower), Linux via install.sh.
            canSelfUpdate   = !container && OperatingSystem.IsWindows(),
            applyInProgress = checker.ApplyInProgress,
        };
    }

    private static bool IsAdmin(HttpContext ctx) =>
        ctx.User.FindFirst(ClaimTypes.Role)?.Value == "admin";
}
