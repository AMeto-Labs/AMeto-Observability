using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Ameto.Ingestion;

namespace Ameto.Server.Auth;

/// <summary>
/// In-memory cache mapping each API-key SHA-256 hash → its granted
/// <see cref="ApiKeyPermissions"/>. The whole map is swapped atomically so the hot
/// ingest path never takes a lock and never touches the DB.
/// Call <see cref="Invalidate"/> after any key create / delete.
/// </summary>
internal sealed class ApiKeyCache : IApiKeyValidator
{
    private readonly AuthDatabase _db;

    // Written once on startup and on every Invalidate(); read many times per request.
    private volatile ImmutableDictionary<string, ApiKeyPermissions> _keys;

    public ApiKeyCache(AuthDatabase db)
    {
        _db   = db;
        _keys = Load(db);
    }

    /// <summary>
    /// Validates an incoming raw API key for a required permission.
    /// O(1) dictionary lookup — zero allocations beyond the SHA-256 hash computation.
    /// </summary>
    public bool Validate(ReadOnlySpan<char> rawKey, ApiKeyPermissions required)
    {
        if (rawKey.IsEmpty) return false;
        var hash = ComputeHash(rawKey);
        return _keys.TryGetValue(hash, out var granted) && (granted & required) == required;
    }

    /// <summary>Reloads hashes + permissions from the database. Call after key create / delete.</summary>
    public void Invalidate() => _keys = Load(_db);

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static ImmutableDictionary<string, ApiKeyPermissions> Load(AuthDatabase db)
    {
        using var conn = db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT key_hash, permissions FROM api_keys";
        using var r    = cmd.ExecuteReader();

        var builder = ImmutableDictionary.CreateBuilder<string, ApiKeyPermissions>(StringComparer.Ordinal);
        while (r.Read())
        {
            var hash = r.GetString(0);
            if (hash is not null)
                builder[hash] = (ApiKeyPermissions)r.GetInt32(1);
        }
        return builder.ToImmutable();
    }

    internal static string ComputeHash(ReadOnlySpan<char> rawKey)
    {
        // UTF-8 encode into a stack buffer for keys ≤ 256 bytes; heap-alloc only beyond that.
        int maxBytes = Encoding.UTF8.GetMaxByteCount(rawKey.Length);
        byte[]? rented = null;
        Span<byte> utf8 = maxBytes <= 256
            ? stackalloc byte[maxBytes]
            : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(maxBytes));

        try
        {
            int written = Encoding.UTF8.GetBytes(rawKey, utf8);
            Span<byte> hashBytes = stackalloc byte[32];
            SHA256.HashData(utf8[..written], hashBytes);
            return Convert.ToHexString(hashBytes).ToLowerInvariant();
        }
        finally
        {
            if (rented is not null)
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
