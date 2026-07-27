using Ameto.Core;
using Ameto.Server.Auth;
using Ameto.Storage;

namespace Ameto.Server;

/// <summary>
/// Maps retention policy endpoints:
///   GET  /api/retention       — returns current per-level retention settings (any signed-in user)
///   PUT  /api/retention       — updates and persists settings (admin)
///   POST /api/retention/run   — immediately runs enforcement and returns result (admin)
///
/// The two mutations are admin-only: both delete stored signal data irreversibly —
/// shortening a window and running enforcement drops every segment past the new
/// horizon — so they are not something the read-only <c>viewer</c> role may reach.
/// </summary>
public static class RetentionEndpointMapper
{
    public static void MapRetentionEndpoints(this WebApplication app)
    {
        app.MapGet("/api/retention", (RetentionStore store) =>
            Results.Ok(store.Get()))
            .RequireAuthorization();

        app.MapPut("/api/retention", (RetentionDto dto, RetentionStore store) =>
        {
            store.Set(dto);
            return Results.Ok(store.Get());
        }).RequireAuthorization(AuthServiceExtensions.PolicyAdmin);

        app.MapPost("/api/retention/run", async (
            StorageEngine storage,
            RetentionStore store,
            IEnumerable<IRetentionTarget> targets) =>
        {
            var logResult = await storage.EnforceRetentionAsync();
            var dto = store.Get();
            var metricFiles = 0;
            var traceFiles  = 0;
            foreach (var target in targets)
            {
                var days = target.RetentionKey switch
                {
                    "metrics" => dto.MetricsDays,
                    "traces"  => dto.TracesDays,
                    _         => 30,
                };
                var pruned = await target.PruneAsync(TimeSpan.FromDays(Math.Max(1, days)));
                if (target.RetentionKey == "metrics") metricFiles += pruned;
                else if (target.RetentionKey == "traces") traceFiles += pruned;
            }
            return Results.Ok(new RetentionRunResult(
                logResult.DeletedSegments, logResult.FreedBytes,
                metricFiles, traceFiles,
                logResult.RanAt));
        }).RequireAuthorization(AuthServiceExtensions.PolicyAdmin);
    }
}
