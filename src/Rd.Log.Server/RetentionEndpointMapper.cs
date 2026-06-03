using Rd.Log.Core;
using Rd.Log.Storage;

namespace Rd.Log.Server;

/// <summary>
/// Maps retention policy endpoints:
///   GET  /api/retention       — returns current per-level retention settings
///   PUT  /api/retention       — updates and persists settings
///   POST /api/retention/run   — immediately runs enforcement and returns result
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
        }).RequireAuthorization();

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
        }).RequireAuthorization();
    }
}
