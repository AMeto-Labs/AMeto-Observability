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
        // busy_timeout: the same 5 s grace AlertRuleStore gives itself on this shared file.
        // Without it a transient writer elsewhere makes any statement throw "database is
        // locked" instantly — and the v1→v2 copy in MigrateSchema is deliberately un-caught,
        // so that instant throw would fail a boot a five-second wait would have saved.
        Exec(conn, "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;");
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

            -- One row per completed one-time migration. Emptiness of a target table is
            -- NOT a substitute (a user can legitimately empty it again); see MigrateSchema.
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name TEXT PRIMARY KEY
            );

            -- The same history, now split per page (`scope`: logs|traces|metrics).
            -- A new table rather than an ALTER because scope belongs in the primary
            -- key — the same text is a legitimate separate entry on Traces and on
            -- Metrics — and SQLite cannot alter a key in place. The v1 table above
            -- stays where it is; see MigrateSchema for the one-time copy.
            CREATE TABLE IF NOT EXISTS search_history_v2 (
                username   TEXT    NOT NULL,
                scope      TEXT    NOT NULL,
                query      TEXT    NOT NULL,
                pinned     INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT    NOT NULL,
                PRIMARY KEY (username, scope, query)
            );
            CREATE INDEX IF NOT EXISTS ix_search_history_v2_user
                ON search_history_v2(username, scope, pinned, updated_at DESC);
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

        // The provider's immutable subject id (Google `sub`, Entra `oid`) for OAuth users.
        // Empty on rows created before this column existed; those bind on next sign-in.
        // See AuthStore.FindOrCreateOAuthUser — an email alone is not an identity.
        try { Exec(conn, "ALTER TABLE users ADD COLUMN provider_subject TEXT NOT NULL DEFAULT ''"); }
        catch { /* column already exists */ }

        // Carry an old un-scoped history into search_history_v2. Every row in the v1
        // table was written by the Events page — events.store.ts is the only recorder
        // that has ever existed — so 'logs' is the true retroactive scope, not a guess.
        //
        // Copied ONCE, gated on a marker rather than on v2 being empty. Emptiness is not
        // the same fact as "already copied": v1 is frozen (the store writes only v2), so a
        // user who deleted their whole history left v2 empty — and an emptiness-gated copy
        // then resurrected every one of those deleted entries on the next restart, which is
        // precisely the outcome a privacy-motivated delete must not have. The marker is
        // written in the same connection only after the copy succeeds, so a failed copy is
        // retried at the next boot instead of being skipped forever.
        //
        // No try/catch, deliberately: the old table provably exists here (InitSchema created
        // it two statements ago, IF NOT EXISTS), so the only things a catch could swallow
        // are real failures — and this class has no logger, which made a swallowed copy a
        // permanent, unexplained loss of every pre-upgrade pin. A throw fails the boot
        // loudly, exactly like the unguarded statements above it, and is retry-safe.
        if (!MigrationDone(conn, "search_history_v2_copy"))
        {
            // One transaction: the marker must never persist without the rows it vouches for.
            // (The reverse — rows without a marker — is already harmless: OR IGNORE re-copies.)
            using var tx = conn.BeginTransaction();
            Exec(conn, """
                INSERT OR IGNORE INTO search_history_v2 (username, scope, query, pinned, updated_at)
                    SELECT username, 'logs', query, pinned, updated_at FROM search_history
                """);
            Exec(conn, "INSERT OR IGNORE INTO schema_migrations (name) VALUES ('search_history_v2_copy')");
            tx.Commit();
        }
    }

    /// <summary>True when the named one-time migration has already been recorded.</summary>
    private static bool MigrationDone(SqliteConnection conn, string name)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = @n)";
        cmd.Parameters.AddWithValue("@n", name);
        return Convert.ToInt64(cmd.ExecuteScalar()) == 1;
    }

    private static void Exec(SqliteConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
