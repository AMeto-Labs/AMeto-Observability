using System.Security.Cryptography;
using Ameto.Core;

namespace Ameto.Core.Tests;

public sealed class SecretProtectorTests
{
    private static AesGcmSecretProtector New() => new(RandomNumberGenerator.GetBytes(32));

    [Fact]
    public void RoundTrips_Value()
    {
        var p = New();
        var enc = p.Protect("bot-token-12345:AAH");
        Assert.StartsWith(AesGcmSecretProtector.Prefix, enc);
        Assert.True(p.IsProtected(enc));
        Assert.Equal("bot-token-12345:AAH", p.Unprotect(enc));
    }

    [Fact]
    public void Protect_IsIdempotent()
    {
        var p = New();
        var once = p.Protect("secret");
        var twice = p.Protect(once);       // already encrypted → unchanged
        Assert.Equal(once, twice);
        Assert.Equal("secret", p.Unprotect(twice));
    }

    [Fact]
    public void Protect_ProducesDifferentCiphertext_PerCall()
    {
        var p = New();
        Assert.NotEqual(p.Protect("secret"), p.Protect("secret")); // random nonce
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void EmptyOrNull_PassThrough(string? input)
    {
        var p = New();
        Assert.Equal(string.Empty, p.Protect(input));
        Assert.Equal(string.Empty, p.Unprotect(input));
        Assert.False(p.IsProtected(input));
    }

    [Fact]
    public void LegacyPlaintext_PassesThroughUnprotect()
    {
        var p = New();
        Assert.Equal("plain-legacy-token", p.Unprotect("plain-legacy-token"));
        Assert.False(p.IsProtected("plain-legacy-token"));
    }

    [Fact]
    public void Tampered_Ciphertext_YieldsEmpty()
    {
        var p = New();
        var enc = p.Protect("secret");
        // flip a character in the base64 body
        var body = enc[AesGcmSecretProtector.Prefix.Length..].ToCharArray();
        body[^2] = body[^2] == 'A' ? 'B' : 'A';
        var tampered = AesGcmSecretProtector.Prefix + new string(body);
        Assert.Equal(string.Empty, p.Unprotect(tampered));
    }

    [Fact]
    public void WrongKey_CannotDecrypt()
    {
        var key = RandomNumberGenerator.GetBytes(32);
        var enc = new AesGcmSecretProtector(key).Protect("secret");
        var other = new AesGcmSecretProtector(RandomNumberGenerator.GetBytes(32));
        Assert.Equal(string.Empty, other.Unprotect(enc)); // auth tag mismatch → empty
    }

    [Fact]
    public void Ctor_RejectsWrongKeySize()
    {
        Assert.Throws<ArgumentException>(() => new AesGcmSecretProtector(new byte[16]));
    }

    [Fact]
    public void Factory_GeneratesAndReusesKeyFile()
    {
        var dir = Directory.CreateTempSubdirectory("secretprotector").FullName;
        try
        {
            bool generated = false;
            var p1 = SecretProtectorFactory.Create(dir, configuredKey: null, onGeneratedKey: _ => generated = true);
            Assert.True(generated);
            Assert.True(File.Exists(Path.Combine(dir, "secret.key")));

            var enc = p1.Protect("secret");
            // A second protector from the SAME persisted key can decrypt.
            var p2 = SecretProtectorFactory.Create(dir, configuredKey: null);
            Assert.Equal("secret", p2.Unprotect(enc));
        }
        finally { Directory.Delete(dir, recursive: true); }
    }

    [Fact]
    public void Factory_UsesConfiguredKey_Base64()
    {
        var dir = Directory.CreateTempSubdirectory("secretprotector").FullName;
        try
        {
            var keyB64 = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
            var p = SecretProtectorFactory.Create(dir, configuredKey: keyB64);
            Assert.Equal("secret", p.Unprotect(p.Protect("secret")));
            Assert.False(File.Exists(Path.Combine(dir, "secret.key"))); // no file when key is configured
        }
        finally { Directory.Delete(dir, recursive: true); }
    }
}
