using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;

namespace Ameto.Server.Auth;

/// <summary>
/// In-memory cache of SHA-256 API-key hashes.
/// Swaps the entire set atomically so the hot ingest path never takes a lock.
/// Call <see cref="Invalidate"/> after any key create / delete.
/// </summary>
internal sealed class ApiKeyCache
{
    private readonly AuthDatabase _db;

    // Written once on startup and on every Invalidate(); read many times per request.
    private volatile ImmutableHashSet<string> _hashes;

    public ApiKeyCache(AuthDatabase db)
    {
        _db     = db;
        _hashes = Load(db);
    }

    /// <summary>
    /// Validates an incoming raw API key.
    /// O(1) hash-set lookup — zero allocations beyond the SHA-256 hash computation.
    /// </summary>
    public bool Validate(ReadOnlySpan<char> rawKey)
    {
        if (rawKey.IsEmpty) return false;
        // Compute hash without allocating an intermediate string where possible.
        var hash = ComputeHash(rawKey);
        return _hashes.Contains(hash);
    }

    /// <summary>Reloads hashes from the database. Call after key create / delete.</summary>
    public void Invalidate() => _hashes = Load(_db);

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static ImmutableHashSet<string> Load(AuthDatabase db)
    {
        using var conn = db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT key_hash FROM api_keys";
        using var r    = cmd.ExecuteReader();

        var builder = ImmutableHashSet.CreateBuilder<string>(StringComparer.Ordinal);
        while (r.Read())
        {
            var hash = r.GetString(0);
            if (hash is not null) builder.Add(hash);
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
