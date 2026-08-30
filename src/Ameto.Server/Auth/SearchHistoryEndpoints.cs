using System.Security.Claims;

namespace Ameto.Server.Auth;

/// <summary>
/// Per-user saved-search endpoints. Every route is scoped to the caller's
/// identity (JWT <see cref="ClaimTypes.Name"/>) — a user only ever sees / mutates
/// their own history — and to a page (<c>scope</c>: logs | traces | metrics), so
/// Events, Traces and Metrics each keep their own recents and pins.
/// </summary>
internal static class SearchHistoryEndpoints
{
    private const int MaxQueryLength = 2000;

    /// <summary>The pages that own a history. Closed set: a scope column is only worth
    /// having if nothing can quietly invent a fourth bucket the UI never reads.</summary>
    private static readonly string[] KnownScopes = ["logs", "traces", "metrics"];

    /// <summary>
    /// The scope a request without one means. This is the compatibility contract: a
    /// client built before scopes existed talks only to the Events page, so an absent
    /// or empty scope is 'logs' and those clients keep working untouched.
    /// </summary>
    private const string DefaultScope = "logs";

    public static void MapSearchHistoryEndpoints(this WebApplication app)
    {
        app.MapGet("/api/search-history", (HttpContext ctx, string? scope, SearchHistoryStore store) =>
        {
            if (!TryScope(scope, out var s)) return UnknownScope(scope);
            var snap = store.Get(CurrentUser(ctx), s);
            return Results.Ok(new { pinned = snap.Pinned, recent = snap.Recent });
        }).RequireAuthorization();

        app.MapPost("/api/search-history", (HttpContext ctx, RecordSearchRequest req, SearchHistoryStore store) =>
        {
            if (!TryScope(req.Scope, out var s)) return UnknownScope(req.Scope);
            var q = Normalise(req.Query);
            if (q is null) return Results.NoContent(); // ignore blank/whitespace queries
            store.Record(CurrentUser(ctx), s, q);
            return Results.NoContent();
        }).RequireAuthorization();

        app.MapPut("/api/search-history/pin", (HttpContext ctx, PinSearchRequest req, SearchHistoryStore store) =>
        {
            if (!TryScope(req.Scope, out var s)) return UnknownScope(req.Scope);
            var q = Normalise(req.Query);
            if (q is null) return Results.BadRequest(new { error = "Query is required." });
            store.SetPinned(CurrentUser(ctx), s, q, req.Pinned);
            return Results.NoContent();
        }).RequireAuthorization();

        app.MapDelete("/api/search-history", (HttpContext ctx, string query, string? scope, SearchHistoryStore store) =>
        {
            if (!TryScope(scope, out var s)) return UnknownScope(scope);
            var q = Normalise(query);
            if (q is null) return Results.BadRequest(new { error = "Query is required." });
            store.Delete(CurrentUser(ctx), s, q);
            return Results.NoContent();
        }).RequireAuthorization();
    }

    private static string CurrentUser(HttpContext ctx) =>
        ctx.User.FindFirst(ClaimTypes.Name)?.Value ?? "unknown";

    /// <summary>
    /// Validates a requested scope, case-insensitively, and hands back its stored
    /// (lowercase) form. Absent or blank is <see cref="DefaultScope"/>; anything else
    /// unrecognised fails rather than falling back, because a typo silently filed under
    /// 'logs' is a history the caller can never see again and never gets told about.
    /// </summary>
    private static bool TryScope(string? scope, out string normalised)
    {
        var s = scope?.Trim();
        if (string.IsNullOrEmpty(s))
        {
            normalised = DefaultScope;
            return true;
        }

        foreach (var known in KnownScopes)
        {
            if (string.Equals(s, known, StringComparison.OrdinalIgnoreCase))
            {
                normalised = known;
                return true;
            }
        }

        normalised = DefaultScope;
        return false;
    }

    private static IResult UnknownScope(string? scope) =>
        Results.BadRequest(new { error = $"Unknown scope '{scope}'. Expected one of: {string.Join(", ", KnownScopes)}." });

    /// <summary>Trims and length-caps a query; null when blank.</summary>
    private static string? Normalise(string? query)
    {
        var q = query?.Trim();
        if (string.IsNullOrEmpty(q)) return null;
        return q.Length > MaxQueryLength ? q[..MaxQueryLength] : q;
    }
}

// Scope is last and nullable on both records so a body from a pre-scope client — one
// that sends only the query — still binds, and lands in 'logs'.
internal sealed record RecordSearchRequest(string? Query, string? Scope = null);
internal sealed record PinSearchRequest(string? Query, bool Pinned, string? Scope = null);
