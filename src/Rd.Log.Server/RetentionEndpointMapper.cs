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

        app.MapPost("/api/retention/run", async (StorageEngine storage) =>
        {
            var result = await storage.EnforceRetentionAsync();
            return Results.Ok(result);
        }).RequireAuthorization();
    }
}
