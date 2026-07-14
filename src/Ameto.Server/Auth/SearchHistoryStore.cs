namespace Ameto.Server.Auth;

/// <summary>The pinned + recent saved searches for one user.</summary>
internal sealed record SearchHistorySnapshot(
    IReadOnlyList<string> Pinned,
    IReadOnlyList<string> Recent);

/// <summary>
/// Per-user saved search history, persisted in the shared auth SQLite db
/// (<c>Ameto.db</c>). Cold path (a click / an occasional search), so plain
/// parameterised commands — no span/pool work needed.
/// </summary>
internal sealed class SearchHistoryStore
{
    // Recent (unpinned) rows kept per user; pinned rows are never pruned here.
    private const int RecentLimit = 10;
    // Pinned rows surfaced to the UI.
    private const int PinnedLimit = 5;

    private readonly AuthDatabase _db;

    public SearchHistoryStore(AuthDatabase db) => _db = db;

    /// <summary>Records a used query (bumps recency) and prunes old unpinned rows past the limit.</summary>
    public void Record(string username, string query)
    {
        using var conn = _db.Open();

        using (var up = conn.CreateCommand())
        {
            // Preserve an existing row's pinned flag; only refresh recency.
            up.CommandText = """
                INSERT INTO search_history (username, query, pinned, updated_at)
                VALUES (@u, @q, 0, @t)
                ON CONFLICT(username, query) DO UPDATE SET updated_at = excluded.updated_at
                """;
            up.Parameters.AddWithValue("@u", username);
            up.Parameters.AddWithValue("@q", query);
            up.Parameters.AddWithValue("@t", DateTimeOffset.UtcNow.ToString("O"));
            up.ExecuteNonQuery();
        }

        using var prune = conn.CreateCommand();
        prune.CommandText = """
            DELETE FROM search_history
            WHERE username = @u AND pinned = 0 AND query NOT IN (
                SELECT query FROM search_history
                WHERE username = @u AND pinned = 0
                ORDER BY updated_at DESC
                LIMIT @lim)
            """;
        prune.Parameters.AddWithValue("@u", username);
        prune.Parameters.AddWithValue("@lim", RecentLimit);
        prune.ExecuteNonQuery();
    }

    /// <summary>Pins/unpins a query (inserting it if it isn't already stored).</summary>
    public void SetPinned(string username, string query, bool pinned)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO search_history (username, query, pinned, updated_at)
            VALUES (@u, @q, @p, @t)
            ON CONFLICT(username, query) DO UPDATE SET pinned = excluded.pinned
            """;
        cmd.Parameters.AddWithValue("@u", username);
        cmd.Parameters.AddWithValue("@q", query);
        cmd.Parameters.AddWithValue("@p", pinned ? 1 : 0);
        cmd.Parameters.AddWithValue("@t", DateTimeOffset.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public void Delete(string username, string query)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM search_history WHERE username = @u AND query = @q";
        cmd.Parameters.AddWithValue("@u", username);
        cmd.Parameters.AddWithValue("@q", query);
        cmd.ExecuteNonQuery();
    }

    public SearchHistorySnapshot Get(string username)
    {
        using var conn = _db.Open();
        var pinned = Query(conn, username, pinned: 1, PinnedLimit);
        var recent = Query(conn, username, pinned: 0, RecentLimit);
        return new SearchHistorySnapshot(pinned, recent);
    }

    private static List<string> Query(Microsoft.Data.Sqlite.SqliteConnection conn, string username, int pinned, int limit)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT query FROM search_history
            WHERE username = @u AND pinned = @p
            ORDER BY updated_at DESC
            LIMIT @lim
            """;
        cmd.Parameters.AddWithValue("@u", username);
        cmd.Parameters.AddWithValue("@p", pinned);
        cmd.Parameters.AddWithValue("@lim", limit);
        using var r = cmd.ExecuteReader();
        var list = new List<string>();
        while (r.Read()) list.Add(r.GetString(0));
        return list;
    }
}
