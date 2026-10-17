using System.Buffers.Text;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Ameto.Server.Auth;

/// <summary>
/// The session cookie's value: <c>{sessionId}.{generation}.{mac}</c>, where the MAC is
/// HMAC-SHA256 over the first two parts under a key derived from the JWT secret.
///
/// <para>Deterministic on purpose. When two tabs refresh with the same cookie at once, the one
/// that loses the race must be handed the successor the winner minted — see
/// <see cref="AuthSessions.RefreshAsync"/> — so the server has to be able to mint it again.
/// Random tokens would need that successor stored, and a stored successor is a live credential
/// sitting in the database. Here the database holds only the generation number.</para>
///
/// <para>Deriving the key from the JWT secret keeps one secret to protect and one to rotate:
/// deleting <c>jwt-secret.bin</c> already invalidates every access token, and now ends every
/// session with them.</para>
/// </summary>
internal sealed class SessionTokenCodec
{
    private const int MacLength = 32;

    /// <summary>Longest value accepted: id (22) + generation (≤19) + MAC (43) + two dots, with room.</summary>
    private const int MaxLength = 128;

    private readonly byte[] _key;

    public SessionTokenCodec(byte[] key)
    {
        _key = key;
        // Cookies are not separated by port: two servers on one host (localhost:8555 and :8556,
        // an A/B pair) would each overwrite the other's cookie of the same name and sign the
        // other's user out at every refresh. A name derived from the key keeps them apart, and
        // stays the same across restarts because the key does.
        Span<byte> tag = stackalloc byte[MacLength];
        HMACSHA256.HashData(_key, "cookie-name"u8, tag);
        CookieName = string.Concat(CookieNamePrefix, Convert.ToHexStringLower(tag[..4]));
    }

    /// <summary>Every session cookie's name starts with this; the rest is per server (see the constructor).</summary>
    public const string CookieNamePrefix = "rd_session_";

    /// <summary>This server's session cookie name.</summary>
    public string CookieName { get; }

    public static SessionTokenCodec FromJwtSecret(string secret) => new(
        HKDF.DeriveKey(HashAlgorithmName.SHA256, Encoding.UTF8.GetBytes(secret), MacLength,
                       info: "ameto/session-cookie/v1"u8.ToArray()));

    public string Encode(string sessionId, long generation)
    {
        var payload = string.Concat(sessionId, ".", generation.ToString(CultureInfo.InvariantCulture));
        Span<byte> mac = stackalloc byte[MacLength];
        HMACSHA256.HashData(_key, Encoding.ASCII.GetBytes(payload), mac);
        return string.Concat(payload, ".", Base64Url.EncodeToString(mac));
    }

    /// <summary>
    /// Reads a cookie value back. False for anything not minted with this key — which includes a
    /// value cut short, padded, or carrying another session's MAC — before any lookup is made.
    /// </summary>
    public bool TryDecode(string? value, out string sessionId, out long generation)
    {
        sessionId  = "";
        generation = 0;
        if (string.IsNullOrEmpty(value) || value.Length > MaxLength) return false;

        int macDot = value.LastIndexOf('.');
        if (macDot <= 0) return false;
        var payload = value.AsSpan(0, macDot);
        int idDot = payload.IndexOf('.');
        if (idDot <= 0) return false;
        if (!long.TryParse(payload[(idDot + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out generation)
            || generation <= 0)
            return false;

        Span<byte> given = stackalloc byte[MacLength];
        if (!Base64Url.TryDecodeFromChars(value.AsSpan(macDot + 1), given, out int written) || written != MacLength)
            return false;

        Span<byte> payloadBytes = stackalloc byte[payload.Length];
        if (Encoding.ASCII.GetBytes(payload, payloadBytes) != payload.Length) return false;
        Span<byte> expected = stackalloc byte[MacLength];
        HMACSHA256.HashData(_key, payloadBytes, expected);
        if (!CryptographicOperations.FixedTimeEquals(expected, given)) return false;

        sessionId = payload[..idDot].ToString();
        return true;
    }

    /// <summary>A new session id: 128 random bits, base64url.</summary>
    public static string NewSessionId() => Base64Url.EncodeToString(RandomNumberGenerator.GetBytes(16));
}
