using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Security.Cryptography;
using System.Text;
using Ameto.Ingestion;

namespace Ameto.Server.Auth;

/// <summary>
/// The 32 bytes of an API key's SHA-256 digest, as a value the dictionary can key on
/// directly. The cache used to key on the lowercase hex STRING of those bytes, which meant
/// every request allocated two strings (hex, then lowercased) purely to look one up. Four
/// ulongs compare and hash without touching the heap.
/// </summary>
internal readonly record struct ApiKeyHash(ulong A, ulong B, ulong C, ulong D)
{
    /// <summary>Reads a 32-byte digest. Shorter input is rejected by the caller, not here.</summary>
    public static ApiKeyHash From(ReadOnlySpan<byte> digest32) => new(
        BinaryPrimitives.ReadUInt64LittleEndian(digest32),
        BinaryPrimitives.ReadUInt64LittleEndian(digest32[8..]),
        BinaryPrimitives.ReadUInt64LittleEndian(digest32[16..]),
        BinaryPrimitives.ReadUInt64LittleEndian(digest32[24..]));
}

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
    // Frozen, because it is built rarely and read on every ingest request.
    private volatile FrozenDictionary<ApiKeyHash, ApiKeyPermissions> _keys;

    public ApiKeyCache(AuthDatabase db)
    {
        _db   = db;
        _keys = Load(db);
    }

    /// <summary>
    /// Validates an incoming raw API key for a required permission.
    /// O(1) dictionary lookup, and no allocation at all: the digest is hashed into a stack
    /// buffer and looked up as a value.
    /// </summary>
    public bool Validate(ReadOnlySpan<char> rawKey, ApiKeyPermissions required)
    {
        if (rawKey.IsEmpty) return false;
        return _keys.TryGetValue(ComputeHashValue(rawKey), out var granted)
            && (granted & required) == required;
    }

    /// <summary>
    /// Returns the permissions granted by a raw key, or null when the key is
    /// unknown. Used by the read-path API-key authentication handler (the query
    /// endpoints), unlike <see cref="Validate"/> which the ingest hot path uses.
    /// </summary>
    public ApiKeyPermissions? Resolve(ReadOnlySpan<char> rawKey)
    {
        if (rawKey.IsEmpty) return null;
        return _keys.TryGetValue(ComputeHashValue(rawKey), out var granted) ? granted : null;
    }

    /// <summary>Reloads hashes + permissions from the database. Call after key create / delete.</summary>
    public void Invalidate() => _keys = Load(_db);

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static FrozenDictionary<ApiKeyHash, ApiKeyPermissions> Load(AuthDatabase db)
    {
        using var conn = db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT key_hash, permissions FROM api_keys";
        using var r    = cmd.ExecuteReader();

        var map = new Dictionary<ApiKeyHash, ApiKeyPermissions>();
        Span<byte> digest = stackalloc byte[32];
        while (r.Read())
        {
            // Stored as lowercase hex of the SHA-256 digest (AuthStore writes it that way).
            // Anything that is not exactly 32 bytes of hex cannot be a digest this cache
            // could ever be asked about, so it is skipped rather than guessed at.
            string? hex = r.GetString(0);
            if (hex is null || hex.Length != 64) continue;
            if (Convert.FromHexString(hex, digest, out _, out int written) != OperationStatus.Done
                || written != 32)
                continue;

            map[ApiKeyHash.From(digest)] = (ApiKeyPermissions)r.GetInt32(1);
        }
        return map.ToFrozenDictionary();
    }

    /// <summary>SHA-256 of the key's UTF-8 bytes, as a comparable value — no strings.</summary>
    private static ApiKeyHash ComputeHashValue(ReadOnlySpan<char> rawKey)
    {
        // UTF-8 encode into a stack buffer for keys ≤ 256 bytes; pooled only beyond that.
        int maxBytes = Encoding.UTF8.GetMaxByteCount(rawKey.Length);
        byte[]? rented = null;
        Span<byte> utf8 = maxBytes <= 256
            ? stackalloc byte[256]
            : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(maxBytes));

        try
        {
            int written = Encoding.UTF8.GetBytes(rawKey, utf8);
            Span<byte> digest = stackalloc byte[32];
            SHA256.HashData(utf8[..written], digest);
            return ApiKeyHash.From(digest);
        }
        finally
        {
            if (rented is not null)
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// The lowercase-hex spelling of a key's digest — the form stored in the database.
    /// Only for the cold paths (create / audit); the request path never needs a string.
    /// </summary>
    internal static string ComputeHash(ReadOnlySpan<char> rawKey)
    {
        int maxBytes = Encoding.UTF8.GetMaxByteCount(rawKey.Length);
        byte[]? rented = null;
        Span<byte> utf8 = maxBytes <= 256
            ? stackalloc byte[256]
            : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(maxBytes));

        try
        {
            int written = Encoding.UTF8.GetBytes(rawKey, utf8);
            Span<byte> hashBytes = stackalloc byte[32];
            SHA256.HashData(utf8[..written], hashBytes);
            return Convert.ToHexStringLower(hashBytes);
        }
        finally
        {
            if (rented is not null)
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
