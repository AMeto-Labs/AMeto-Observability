using System.Security.Cryptography;
using Microsoft.Data.Sqlite;

namespace Rd.Log.Server.Auth;

/// <summary>
/// Manages the <c>rdlog.db</c> SQLite file.
/// Creates all schema tables (users, api_keys) on first use.
/// Shared with <c>RetentionStore</c> (Storage) which creates the retention table.
/// </summary>
internal sealed class AuthDatabase
{
    internal readonly string ConnectionString;

    public AuthDatabase(string dataDirectory)
    {
        Directory.CreateDirectory(dataDirectory);
        var dbPath = Path.Combine(dataDirectory, "rdlog.db");
        ConnectionString = $"Data Source={dbPath}";
        InitSchema();
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
        Exec(conn, """
            CREATE TABLE IF NOT EXISTS users (
                id            TEXT PRIMARY KEY,
                username      TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                salt          TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'manager',
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
            """);
    }

    private static void Exec(SqliteConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
