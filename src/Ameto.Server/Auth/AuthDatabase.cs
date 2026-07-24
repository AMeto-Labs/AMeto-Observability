using System.Security.Cryptography;
using Microsoft.Data.Sqlite;

namespace Ameto.Server.Auth;

/// <summary>
/// Manages the <c>Ameto.db</c> SQLite file.
/// Creates all schema tables (users, api_keys) on first use.
/// Shared with <c>RetentionStore</c> (Storage) which creates the retention table.
///
/// User providers:
///   local    – username + password (stored hash)
///   google   – OAuth via Google; email is the identity key
///   microsoft – OAuth via Microsoft Entra ID; email is the identity key
///
/// Roles: admin | manager | viewer
/// </summary>
internal sealed class AuthDatabase
{
    internal readonly string ConnectionString;

    public AuthDatabase(string dataDirectory)
    {
        Directory.CreateDirectory(dataDirectory);
        var dbPath = Path.Combine(dataDirectory, "Ameto.db");
        ConnectionString = $"Data Source={dbPath}";
        InitSchema();
        MigrateSchema();
    }

    public SqliteConnection Open()
    {
        var conn = new SqliteConnection(ConnectionString);
        conn.Open();
        Exec(conn, "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;");
        return conn;
    }

    private void InitSchema()
    {
        using var conn = Open();
        // Base table (compatible with old schema — no new columns here)
        Exec(conn, """
            CREATE TABLE IF NOT EXISTS users (
                id            TEXT PRIMARY KEY,
                username      TEXT NOT NULL,
                password_hash TEXT NOT NULL DEFAULT '',
                salt          TEXT NOT NULL DEFAULT '',
                role          TEXT NOT NULL DEFAULT 'viewer',
                created_at    TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_users_username
                ON users(username COLLATE NOCASE);

            CREATE TABLE IF NOT EXISTS api_keys (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                key_hash    TEXT NOT NULL,
                created_by  TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_api_keys_hash
                ON api_keys(key_hash);

            -- OAuth domain allowlist: any user whose email ends with @domain
            -- for the given provider may sign in (auto-provisioned on first login).
            CREATE TABLE IF NOT EXISTS oauth_domains (
                id         TEXT PRIMARY KEY,
                provider   TEXT NOT NULL,
                domain     TEXT NOT NULL,
                role       TEXT NOT NULL DEFAULT 'viewer',
                created_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_oauth_domains_provider_domain
                ON oauth_domains(provider, domain COLLATE NOCASE);

            -- Per-user saved search / filter history. `pinned` rows survive the
            -- recent-history prune; the UI shows top pinned then recent.
            CREATE TABLE IF NOT EXISTS search_history (
                username   TEXT    NOT NULL,
                query      TEXT    NOT NULL,
                pinned     INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT    NOT NULL,
                PRIMARY KEY (username, query)
            );
            CREATE INDEX IF NOT EXISTS ix_search_history_user
                ON search_history(username, pinned, updated_at DESC);
            """);
    }

    /// <summary>
    /// Idempotent migrations for databases created with older schemas.
    /// </summary>
    private void MigrateSchema()
    {
        using var conn = Open();

        // Add columns introduced after initial release (safe to run multiple times)
        foreach (var ddl in new[]
        {
            "ALTER TABLE users ADD COLUMN display_name TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE users ADD COLUMN email        TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE users ADD COLUMN provider     TEXT NOT NULL DEFAULT 'local'",
        })
        {
            try { Exec(conn, ddl); } catch { /* column already exists */ }
        }

        // Create the unique index on (email, provider) only after the columns exist
        try
        {
            Exec(conn, """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_users_email_provider
                    ON users(email COLLATE NOCASE, provider)
                    WHERE email != '';
                """);
        }
        catch { /* index already exists */ }

        // Per-user view scopes (Logs|Metrics|Traces|Stats). Defaults to 15 (All) so
        // users created before per-view scoping keep full read access. Admins ignore it.
        try { Exec(conn, "ALTER TABLE users ADD COLUMN permissions INTEGER NOT NULL DEFAULT 15"); }
        catch { /* column already exists */ }

        // Migrate roles that are outside the allowed set
        Exec(conn, "UPDATE users SET role = 'viewer' WHERE role NOT IN ('admin','manager','viewer')");

        // api_keys columns added after initial release. Safe to run multiple times.
        // permissions defaults to 7 (All: Logs|Traces|Metrics) so keys created before
        // per-permission scoping keep ingesting everything. minimum_level is legacy
        // (no longer read); left in place so old DBs need no destructive rebuild.
        foreach (var ddl in new[]
        {
            "ALTER TABLE api_keys ADD COLUMN description   TEXT    NOT NULL DEFAULT ''",
            "ALTER TABLE api_keys ADD COLUMN minimum_level INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE api_keys ADD COLUMN permissions   INTEGER NOT NULL DEFAULT 7",
        })
        {
            try { Exec(conn, ddl); } catch { /* column already exists */ }
        }

        // Default view scopes granted to users auto-provisioned by an OAuth domain
        // rule. Defaults to 15 (All) so existing rules keep granting full read access.
        try { Exec(conn, "ALTER TABLE oauth_domains ADD COLUMN permissions INTEGER NOT NULL DEFAULT 15"); }
        catch { /* column already exists */ }
    }

    private static void Exec(SqliteConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
