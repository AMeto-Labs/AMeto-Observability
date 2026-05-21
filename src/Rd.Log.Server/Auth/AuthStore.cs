using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Rd.Log.Server.Auth;

// ── Records ────────────────────────────────────────────────────────────────────

internal sealed record UserRecord(
    string Id,
    string Username,
    string Role,
    DateTimeOffset CreatedAt);

internal sealed record ApiKeyRecord(
    string Id,
    string Name,
    string KeyHash,
    string CreatedBy,
    DateTimeOffset CreatedAt)
{
    // Full key — only populated immediately after creation; never persisted.
    public string? Key { get; init; }
}

// ── Store ──────────────────────────────────────────────────────────────────────

internal sealed class AuthStore
{
    private readonly AuthDatabase _db;

    public AuthStore(AuthDatabase db)
    {
        _db = db;
        EnsureSeedAdmin();
    }

    // ── Users ─────────────────────────────────────────────────────────────────

    public bool ValidateUser(string username, string password)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            SELECT password_hash, salt FROM users
            WHERE username = @u COLLATE NOCASE
            """;
        cmd.Parameters.AddWithValue("@u", username);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return false;
        var storedHash = r.GetString(0);
        var salt       = Convert.FromBase64String(r.GetString(1));
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(storedHash),
            Encoding.UTF8.GetBytes(HashPassword(password, salt)));
    }

    public IReadOnlyList<UserRecord> ListUsers()
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT id, username, role, created_at FROM users ORDER BY created_at";
        using var r    = cmd.ExecuteReader();
        var result = new List<UserRecord>();
        while (r.Read())
            result.Add(new(r.GetString(0), r.GetString(1), r.GetString(2),
                           DateTimeOffset.Parse(r.GetString(3))));
        return result;
    }

    public UserRecord CreateUser(string username, string password, string role)
    {
        var salt = RandomNumberGenerator.GetBytes(16);
        var hash = HashPassword(password, salt);
        var rec  = new UserRecord(
            Guid.NewGuid().ToString("N"), username, role, DateTimeOffset.UtcNow);

        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO users (id, username, password_hash, salt, role, created_at)
            VALUES (@id, @u, @h, @s, @r, @ca)
            """;
        cmd.Parameters.AddWithValue("@id", rec.Id);
        cmd.Parameters.AddWithValue("@u",  rec.Username);
        cmd.Parameters.AddWithValue("@h",  hash);
        cmd.Parameters.AddWithValue("@s",  Convert.ToBase64String(salt));
        cmd.Parameters.AddWithValue("@r",  rec.Role);
        cmd.Parameters.AddWithValue("@ca", rec.CreatedAt.ToString("O"));
        cmd.ExecuteNonQuery();
        return rec;
    }

    public bool DeleteUser(string id)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM users WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd.ExecuteNonQuery() > 0;
    }

    public string? GetRole(string username)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT role FROM users WHERE username = @u COLLATE NOCASE";
        cmd.Parameters.AddWithValue("@u", username);
        var result = cmd.ExecuteScalar();
        return result as string;
    }

    // ── API keys ──────────────────────────────────────────────────────────────

    /// <summary>Validates an incoming API key. Uses constant-time compare.</summary>
    public bool ValidateApiKey(string key)
    {
        var incoming = KeyHash(key);
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT key_hash FROM api_keys WHERE key_hash = @h";
        cmd.Parameters.AddWithValue("@h", incoming);
        var stored = cmd.ExecuteScalar() as string;
        if (stored is null) return false;
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(incoming),
            Encoding.UTF8.GetBytes(stored));
    }

    public IReadOnlyList<ApiKeyRecord> ListApiKeys()
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, key_hash, created_by, created_at FROM api_keys ORDER BY created_at";
        using var r    = cmd.ExecuteReader();
        var result = new List<ApiKeyRecord>();
        while (r.Read())
            result.Add(new(r.GetString(0), r.GetString(1), r.GetString(2),
                           r.GetString(3), DateTimeOffset.Parse(r.GetString(4))));
        return result;
    }

    /// <summary>
    /// Creates an API key. If <paramref name="manualKey"/> is provided it is used
    /// as-is; otherwise an auto-generated key is returned in <see cref="ApiKeyRecord.Key"/>.
    /// The full key is NEVER stored — only SHA-256 of it is persisted.
    /// </summary>
    public ApiKeyRecord CreateApiKey(string name, string createdBy, string? manualKey = null)
    {
        // Auto-generate: SHA-256(32 random bytes) → base64 → "rdl_" prefix
        var key = manualKey?.Trim() is { Length: > 0 } mk
            ? mk
            : "rdl_" + Convert.ToBase64String(
                  SHA256.HashData(RandomNumberGenerator.GetBytes(32)));

        var hash = KeyHash(key);
        var rec  = new ApiKeyRecord(
            Guid.NewGuid().ToString("N"), name, hash, createdBy, DateTimeOffset.UtcNow)
        {
            Key = key,
        };

        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO api_keys (id, name, key_hash, created_by, created_at)
            VALUES (@id, @n, @h, @cb, @ca)
            """;
        cmd.Parameters.AddWithValue("@id", rec.Id);
        cmd.Parameters.AddWithValue("@n",  rec.Name);
        cmd.Parameters.AddWithValue("@h",  hash);
        cmd.Parameters.AddWithValue("@cb", rec.CreatedBy);
        cmd.Parameters.AddWithValue("@ca", rec.CreatedAt.ToString("O"));
        cmd.ExecuteNonQuery();
        return rec;
    }

    public bool DeleteApiKey(string id)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM api_keys WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd.ExecuteNonQuery() > 0;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string HashPassword(string password, byte[] salt)
    {
        var hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, 200_000, HashAlgorithmName.SHA256, 32);
        return Convert.ToBase64String(hash);
    }

    /// <summary>Returns hex-encoded SHA-256 of the key string.</summary>
    private static string KeyHash(string key) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key))).ToLowerInvariant();

    private void EnsureSeedAdmin()
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM users";
        var count = (long)(cmd.ExecuteScalar() ?? 0L);
        if (count == 0)
            CreateUser("admin", "123123", "admin");
    }
}
