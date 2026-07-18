using System.Security.Claims;

namespace Ameto.Server.Updates;

/// <summary>
/// Admin-only software-update endpoints (Settings → Updates):
///   GET  /api/system/update           — version snapshot + download phase/progress
///   POST /api/system/update/check     — force a check right now
///   POST /api/system/update/download  — phase 1: download + verify the installer
///   POST /api/system/update/apply     — phase 2 (admin approval): run it → restart
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

        app.MapPost("/api/system/update/download", static (HttpContext ctx, UpdateChecker checker) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            var (ok, message) = checker.StartDownload();
            return ok ? Results.Accepted(value: new { message })
                      : Results.BadRequest(new { message });
        }).RequireAuthorization();

        app.MapPost("/api/system/update/apply", static (HttpContext ctx, UpdateChecker checker) =>
        {
            if (!IsAdmin(ctx)) return Results.Forbid();
            var (ok, message) = checker.TryInstall();
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
            // Windows installs and Linux systemd installs self-update in place;
            // Docker updates via image pull (Watchtower), bare console runs manually.
            canSelfUpdate   = UpdateChecker.CanSelfUpdate,

            // Self-update state machine: idle → downloading (progress below) →
            // ready (awaiting the admin's install approval) → installing | failed.
            downloadPhase     = checker.Phase.ToString().ToLowerInvariant(),
            downloadedBytes   = checker.DownloadedBytes,
            downloadTotalBytes = checker.DownloadTotalBytes,
            downloadedVersion = checker.DownloadedVersion,
            downloadError     = checker.DownloadError,
        };
    }

    private static bool IsAdmin(HttpContext ctx) =>
        ctx.User.FindFirst(ClaimTypes.Role)?.Value == "admin";
}
