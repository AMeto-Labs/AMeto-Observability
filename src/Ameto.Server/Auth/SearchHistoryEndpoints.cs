using System.Security.Claims;

namespace Ameto.Server.Auth;

/// <summary>
/// Per-user saved-search endpoints. Every route is scoped to the caller's
/// identity (JWT <see cref="ClaimTypes.Name"/>) — a user only ever sees / mutates
/// their own history.
/// </summary>
internal static class SearchHistoryEndpoints
{
    private const int MaxQueryLength = 2000;

    public static void MapSearchHistoryEndpoints(this WebApplication app)
    {
        app.MapGet("/api/search-history", (HttpContext ctx, SearchHistoryStore store) =>
        {
            var snap = store.Get(CurrentUser(ctx));
            return Results.Ok(new { pinned = snap.Pinned, recent = snap.Recent });
        }).RequireAuthorization();

        app.MapPost("/api/search-history", (HttpContext ctx, RecordSearchRequest req, SearchHistoryStore store) =>
        {
            var q = Normalise(req.Query);
            if (q is null) return Results.NoContent(); // ignore blank/whitespace queries
            store.Record(CurrentUser(ctx), q);
            return Results.NoContent();
        }).RequireAuthorization();

        app.MapPut("/api/search-history/pin", (HttpContext ctx, PinSearchRequest req, SearchHistoryStore store) =>
        {
            var q = Normalise(req.Query);
            if (q is null) return Results.BadRequest(new { error = "Query is required." });
            store.SetPinned(CurrentUser(ctx), q, req.Pinned);
            return Results.NoContent();
        }).RequireAuthorization();

        app.MapDelete("/api/search-history", (HttpContext ctx, string query, SearchHistoryStore store) =>
        {
            var q = Normalise(query);
            if (q is null) return Results.BadRequest(new { error = "Query is required." });
            store.Delete(CurrentUser(ctx), q);
            return Results.NoContent();
        }).RequireAuthorization();
    }

    private static string CurrentUser(HttpContext ctx) =>
        ctx.User.FindFirst(ClaimTypes.Name)?.Value ?? "unknown";

    /// <summary>Trims and length-caps a query; null when blank.</summary>
    private static string? Normalise(string? query)
    {
        var q = query?.Trim();
        if (string.IsNullOrEmpty(q)) return null;
        return q.Length > MaxQueryLength ? q[..MaxQueryLength] : q;
    }
}

internal sealed record RecordSearchRequest(string? Query);
internal sealed record PinSearchRequest(string? Query, bool Pinned);
