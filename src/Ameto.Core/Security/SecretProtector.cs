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
        // READ FIRST, GENERATE ONLY WHEN THE FILE SYSTEM SAYS THERE IS NO FILE (#138 item 7). This asked
        // File.Exists, which answers false for a file it could not look at as well as for one that is gone —
        // a share that did not answer, an antivirus hold, an access check — and then generated a new key
        // and wrote it OVER the one that existed: every secret stored under the old key undecryptable, for
        // good. A read that fails any other way than "not found" now fails the start, which is recoverable;
        // and the new file is created with CreateNew, so a key that appeared meanwhile is never overwritten.
        try
        {
            FileRefusal.ThrowIfRefused(path, FileRefusal.Access.Open);
            fileKey = DecodeKey(File.ReadAllText(path).Trim(), $"the key file {path}");
        }
        catch (FileNotFoundException)
        {
            fileKey = RandomNumberGenerator.GetBytes(32);
            KeyFile.WriteNew(path, Encoding.ASCII.GetBytes(Convert.ToBase64String(fileKey)));
            onGeneratedKey?.Invoke(path);
        }
        return new AesGcmSecretProtector(fileKey);
    }

    /// <param name="source">What held the text, for the message: the configuration key, or the key file.</param>
    private static byte[] DecodeKey(string s, string source = "Ameto:MasterKey")
    {
        // 64 hex chars → 32 bytes
        if (s.Length == 64 && s.All(Uri.IsHexDigit))
            return Convert.FromHexString(s);

        byte[] bytes;
        try { bytes = Convert.FromBase64String(s); }
        catch (FormatException) { throw new InvalidOperationException($"{source} must be base64 or 64-char hex."); }

        if (bytes.Length != 32)
            throw new InvalidOperationException($"{source} must decode to 32 bytes (got {bytes.Length}).");
        return bytes;
    }
}

/// <summary>
/// Writes a new key file — <c>secret.key</c>, <c>jwt-secret.bin</c> — so that neither a crash nor a power
/// loss can leave it empty or missing, and no other user can read it meanwhile (#138 item 7 review). A
/// temp file is created owner-only from its creation (a mode set after the write left the file at the
/// umask's 0644 for a moment), written, flushed to the platter, and moved into place without overwrite,
/// so a key that appeared meanwhile is never replaced. Written in place and unflushed, a crash between
/// the create and the write left an empty file (every start then failed, with a message naming the
/// configuration key), and a power loss under ext4's delayed allocation could leave it missing: a new
/// key was generated, and every secret stored under the old one became undecryptable.
/// </summary>
public static class KeyFile
{
    public static void WriteNew(string path, ReadOnlySpan<byte> content)
    {
        // A temp of this writer's own: with one shared name, a second process starting on the same directory
        // (not supported, but nothing locks the directory before this runs) unlinked the first's temp
        // mid-write on POSIX, and the first's Move then published the second's key while the first kept
        // its own in memory — every secret it sealed undecryptable after its restart. With a name of its
        // own each writer moves its own bytes, and the loser's Move finds the key there and fails its start.
        string tmp = $"{path}.{Guid.NewGuid():N}.tmp";
        var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        try
        {
            using (var fs = new FileStream(tmp, options))
            {
                fs.Write(content);
                fs.Flush(flushToDisk: true);
            }
            File.Move(tmp, path, overwrite: false);   // IOException when a key appeared meanwhile: never over one that exists
        }
        catch
        {
            try { File.Delete(tmp); } catch { /* an orphan temp is read by nothing */ }
            throw;
        }
    }
}
