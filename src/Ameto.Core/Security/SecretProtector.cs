using System.Security.Cryptography;
using System.Text;

namespace Ameto.Core;

/// <summary>
/// Reversible encryption for secret values that must be presented back to a third party
/// (bot tokens, SMTP passwords, webhook auth headers). Unlike passwords/API keys — which
/// are one-way hashed — these need to be decryptable at send time, so they are encrypted
/// at rest with AES-256-GCM under a master key.
/// </summary>
public interface ISecretProtector
{
    /// <summary>Encrypts to <c>enc:v1:base64</c>. Empty/already-encrypted values pass through unchanged.</summary>
    string Protect(string? plaintext);

    /// <summary>Decrypts an <c>enc:v1:</c> value. Non-encrypted (legacy plaintext) values pass through unchanged.</summary>
    string Unprotect(string? value);

    /// <summary>True if the value carries the encrypted marker.</summary>
    bool IsProtected(string? value);
}

/// <summary>
/// AES-256-GCM protector. Ciphertext layout inside the base64 blob: <c>nonce(12) | tag(16) | ciphertext</c>.
/// GCM is authenticated — a tampered blob fails to decrypt rather than yielding garbage.
/// </summary>
public sealed class AesGcmSecretProtector : ISecretProtector
{
    public const string Prefix = "enc:v1:";
    private const int NonceLen = 12;
    private const int TagLen   = 16;

    private readonly byte[] _key; // 32 bytes (AES-256)

    public AesGcmSecretProtector(byte[] key)
    {
        if (key.Length != 32)
            throw new ArgumentException($"Master key must be 32 bytes (got {key.Length}).", nameof(key));
        _key = key;
    }

    public bool IsProtected(string? value) =>
        value is not null && value.StartsWith(Prefix, StringComparison.Ordinal);

    public string Protect(string? plaintext)
    {
        if (string.IsNullOrEmpty(plaintext)) return plaintext ?? string.Empty;
        if (IsProtected(plaintext)) return plaintext; // idempotent — never double-encrypt

        int ptLen = Encoding.UTF8.GetByteCount(plaintext);
        byte[] blob = new byte[NonceLen + TagLen + ptLen];
        var nonce = blob.AsSpan(0, NonceLen);
        var tag   = blob.AsSpan(NonceLen, TagLen);
        var ct    = blob.AsSpan(NonceLen + TagLen, ptLen);

        RandomNumberGenerator.Fill(nonce);

        // Rent-free: encode plaintext straight into a stack/heap span sized exactly.
        byte[] pt = ptLen <= 256 ? null! : new byte[ptLen];
        Span<byte> ptSpan = pt is null ? stackalloc byte[ptLen] : pt;
        Encoding.UTF8.GetBytes(plaintext, ptSpan);

        using var gcm = new AesGcm(_key, TagLen);
        gcm.Encrypt(nonce, ptSpan, ct, tag);

        return Prefix + Convert.ToBase64String(blob);
    }

    public string Unprotect(string? value)
    {
        if (string.IsNullOrEmpty(value) || !IsProtected(value)) return value ?? string.Empty;

        try
        {
            byte[] blob = Convert.FromBase64String(value[Prefix.Length..]);
            if (blob.Length < NonceLen + TagLen) return string.Empty;

            var nonce = blob.AsSpan(0, NonceLen);
            var tag   = blob.AsSpan(NonceLen, TagLen);
            var ct    = blob.AsSpan(NonceLen + TagLen);
            Span<byte> pt = ct.Length <= 256 ? stackalloc byte[ct.Length] : new byte[ct.Length];

            using var gcm = new AesGcm(_key, TagLen);
            gcm.Decrypt(nonce, ct, tag, pt);
            return Encoding.UTF8.GetString(pt);
        }
        catch
        {
            // Wrong key or tampered blob — never surface partial/garbage plaintext.
            return string.Empty;
        }
    }
}

/// <summary>
/// Resolves the master key and builds the protector. Precedence:
///   1. <paramref name="configuredKey"/> (from <c>Ameto:MasterKey</c> / env <c>AMETO__MasterKey</c>) —
///      base64 (32 bytes) or 64-char hex.
///   2. A generated <c>{dataDirectory}/secret.key</c> (created once, owner-only).
/// For production, set the env key and keep it OFF the data volume.
/// </summary>
public static class SecretProtectorFactory
{
    /// <param name="onGeneratedKey">Invoked with the key path when a new key file is generated (for a warning log).</param>
    public static ISecretProtector Create(string dataDirectory, string? configuredKey, Action<string>? onGeneratedKey = null)
    {
        if (!string.IsNullOrWhiteSpace(configuredKey))
            return new AesGcmSecretProtector(DecodeKey(configuredKey.Trim()));

        Directory.CreateDirectory(dataDirectory);
        var path = Path.Combine(dataDirectory, "secret.key");
        byte[] fileKey;
        if (File.Exists(path))
        {
            fileKey = DecodeKey(File.ReadAllText(path).Trim());
        }
        else
        {
            fileKey = RandomNumberGenerator.GetBytes(32);
            File.WriteAllText(path, Convert.ToBase64String(fileKey));
            TrySetOwnerOnly(path);
            onGeneratedKey?.Invoke(path);
        }
        return new AesGcmSecretProtector(fileKey);
    }

    private static byte[] DecodeKey(string s)
    {
        // 64 hex chars → 32 bytes
        if (s.Length == 64 && s.All(Uri.IsHexDigit))
            return Convert.FromHexString(s);

        byte[] bytes;
        try { bytes = Convert.FromBase64String(s); }
        catch (FormatException) { throw new InvalidOperationException("Ameto:MasterKey must be base64 or 64-char hex."); }

        if (bytes.Length != 32)
            throw new InvalidOperationException($"Ameto:MasterKey must decode to 32 bytes (got {bytes.Length}).");
        return bytes;
    }

    private static void TrySetOwnerOnly(string path)
    {
        if (OperatingSystem.IsWindows()) return; // POSIX modes not supported on Windows
        try { File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite); }
        catch { /* best effort */ }
    }
}
