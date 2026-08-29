namespace Ameto.Server.Auth;

/// <summary>The pinned + recent saved searches for one user.</summary>
internal sealed record SearchHistorySnapshot(
    IReadOnlyList<string> Pinned,
    IReadOnlyList<string> Recent);

/// <summary>
/// Per-user saved search history, persisted in the shared auth SQLite db
/// (<c>Ameto.db</c>). Cold path (a click / an occasional search), so plain
/// parameterised commands — no span/pool work needed.
///
/// Every row carries a <c>scope</c> (logs | traces | metrics): the three pages
/// keep separate histories, so the limits below are per page, and a busy Events
/// user can never push the Traces recents out. Callers pass an already-validated
/// scope — see <c>SearchHistoryEndpoints</c>.
/// </summary>
internal sealed class SearchHistoryStore
{
    // Recent (unpinned) rows kept per user and scope; pinned rows are never pruned here.
    private const int RecentLimit = 10;
    // Pinned rows surfaced to the UI, per user and scope.
    private const int PinnedLimit = 5;

    private readonly AuthDatabase _db;

    public SearchHistoryStore(AuthDatabase db) => _db = db;

    /// <summary>Records a used query (bumps recency) and prunes old unpinned rows past the limit.</summary>
    public void Record(string username, string scope, string query)
    {
        using var conn = _db.Open();

        using (var up = conn.CreateCommand())
        {
            // Preserve an existing row's pinned flag; only refresh recency.
            up.CommandText = """
                INSERT INTO search_history_v2 (username, scope, query, pinned, updated_at)
                VALUES (@u, @s, @q, 0, @t)
                ON CONFLICT(username, scope, query) DO UPDATE SET updated_at = excluded.updated_at
                """;
            up.Parameters.AddWithValue("@u", username);
            up.Parameters.AddWithValue("@s", scope);
            up.Parameters.AddWithValue("@q", query);
            up.Parameters.AddWithValue("@t", DateTimeOffset.UtcNow.ToString("O"));
            up.ExecuteNonQuery();
        }

        using var prune = conn.CreateCommand();
        // Both the delete and the keep-list are per (user, scope): pruning across scopes
        // would let a search on one page evict another page's recents.
        prune.CommandText = """
            DELETE FROM search_history_v2
            WHERE username = @u AND scope = @s AND pinned = 0 AND query NOT IN (
                SELECT query FROM search_history_v2
                WHERE username = @u AND scope = @s AND pinned = 0
                ORDER BY updated_at DESC
                LIMIT @lim)
            """;
        prune.Parameters.AddWithValue("@u", username);
        prune.Parameters.AddWithValue("@s", scope);
        prune.Parameters.AddWithValue("@lim", RecentLimit);
        prune.ExecuteNonQuery();
    }

    /// <summary>Pins/unpins a query (inserting it if it isn't already stored).</summary>
    public void SetPinned(string username, string scope, string query, bool pinned)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO search_history_v2 (username, scope, query, pinned, updated_at)
            VALUES (@u, @s, @q, @p, @t)
            ON CONFLICT(username, scope, query) DO UPDATE SET pinned     = excluded.pinned,
                                                              updated_at = excluded.updated_at
            """;
        cmd.Parameters.AddWithValue("@u", username);
        cmd.Parameters.AddWithValue("@s", scope);
        cmd.Parameters.AddWithValue("@q", query);
        cmd.Parameters.AddWithValue("@p", pinned ? 1 : 0);
        cmd.Parameters.AddWithValue("@t", DateTimeOffset.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();

        // The cap used to live only in Get's LIMIT, which made it a display trick: the sixth
        // pin was stored pinned=1 forever, shown in neither list (pinned is capped, recent
        // filters pinned=0), and reachable by no route — invisible and immortal. Enforce it at
        // the write instead, the way the client's optimistic view already behaves: the pin
        // that falls off the end is DEMOTED to recent, not hidden — still visible, still
        // deletable, and subject to the ordinary recent prune from then on.
        if (pinned)
        {
            using var demote = conn.CreateCommand();
            demote.CommandText = """
                UPDATE search_history_v2 SET pinned = 0
                WHERE username = @u AND scope = @s AND pinned = 1 AND query NOT IN (
                    SELECT query FROM search_history_v2
                    WHERE username = @u AND scope = @s AND pinned = 1
                    ORDER BY updated_at DESC
                    LIMIT @lim)
                """;
            demote.Parameters.AddWithValue("@u", username);
            demote.Parameters.AddWithValue("@s", scope);
            demote.Parameters.AddWithValue("@lim", PinnedLimit);
            demote.ExecuteNonQuery();
        }
    }

    public void Delete(string username, string scope, string query)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM search_history_v2 WHERE username = @u AND scope = @s AND query = @q";
        cmd.Parameters.AddWithValue("@u", username);
        cmd.Parameters.AddWithValue("@s", scope);
        cmd.Parameters.AddWithValue("@q", query);
        cmd.ExecuteNonQuery();
    }

    public SearchHistorySnapshot Get(string username, string scope)
    {
        using var conn = _db.Open();
        var pinned = Query(conn, username, scope, pinned: 1, PinnedLimit);
        var recent = Query(conn, username, scope, pinned: 0, RecentLimit);
        return new SearchHistorySnapshot(pinned, recent);
    }

    private static List<string> Query(Microsoft.Data.Sqlite.SqliteConnection conn, string username, string scope, int pinned, int limit)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT query FROM search_history_v2
            WHERE username = @u AND scope = @s AND pinned = @p
            ORDER BY updated_at DESC
            LIMIT @lim
            """;
        cmd.Parameters.AddWithValue("@u", username);
        cmd.Parameters.AddWithValue("@s", scope);
        cmd.Parameters.AddWithValue("@p", pinned);
        cmd.Parameters.AddWithValue("@lim", limit);
        using var r = cmd.ExecuteReader();
        var list = new List<string>();
        while (r.Read()) list.Add(r.GetString(0));
        return list;
    }
}
