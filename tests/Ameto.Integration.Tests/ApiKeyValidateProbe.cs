using System.Collections.Immutable;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Ameto.Ingestion;
using Ameto.Server.Auth;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>ApiKeyCache.Validate</c> runs once per ingest request. It used to key its map on the
/// lowercase hex spelling of the SHA-256 digest, so every request allocated two strings — the
/// hex, then its lowercased copy — for the sole purpose of a dictionary lookup. Keying a
/// frozen dictionary on the 32 digest bytes as a value removes both.
/// </summary>
public sealed class ApiKeyValidateProbe : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    private readonly ITestOutputHelper  _out;

    public ApiKeyValidateProbe(AmetoWebAppFactory factory, ITestOutputHelper o)
    {
        _factory = factory;
        _out     = o;
    }

    [Fact]
    public void ValidateIsAllocationFree()
    {
        // Force the factory to seed its key, then take the cache the server actually uses.
        _factory.CreateClient().Dispose();
        var cache = _factory.Services.GetRequiredService<ApiKeyCache>();

        string key = AmetoWebAppFactory.TestApiKey;
        Assert.True(cache.Validate(key, ApiKeyPermissions.Logs));

        // The shape Validate had before: hex string key into an ImmutableDictionary.
        var legacy = ImmutableDictionary.CreateBuilder<string, ApiKeyPermissions>(StringComparer.Ordinal);
        legacy[LegacyHash(key)] = ApiKeyPermissions.All;
        var legacyMap = legacy.ToImmutable();

        for (int i = 0; i < 1000; i++)
        {
            cache.Validate(key, ApiKeyPermissions.Logs);
            LegacyValidate(legacyMap, key, ApiKeyPermissions.Logs);
        }

        const int iters  = 100_000;
        const int rounds = 3;

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) LegacyValidate(legacyMap, key, ApiKeyPermissions.Logs);
        double legacyBytes = (GC.GetAllocatedBytesForCurrentThread() - b0) / (double)iters;

        long b1 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) cache.Validate(key, ApiKeyPermissions.Logs);
        double nowBytes = (GC.GetAllocatedBytesForCurrentThread() - b1) / (double)iters;

        double legacyNs = double.MaxValue, nowNs = double.MaxValue;
        var sw = new Stopwatch();
        for (int r = 0; r < rounds; r++)
        {
            sw.Restart();
            for (int i = 0; i < iters; i++) LegacyValidate(legacyMap, key, ApiKeyPermissions.Logs);
            sw.Stop();
            legacyNs = Math.Min(legacyNs, sw.Elapsed.TotalNanoseconds / iters);

            sw.Restart();
            for (int i = 0; i < iters; i++) cache.Validate(key, ApiKeyPermissions.Logs);
            sw.Stop();
            nowNs = Math.Min(nowNs, sw.Elapsed.TotalNanoseconds / iters);
        }

        _out.WriteLine(
            $"ApiKeyCache.Validate — one call per ingest request:\n" +
            $"  hex-string key (before): {legacyBytes,6:F1} B  {legacyNs,6:F0} ns\n" +
            $"  32-byte value  (after) : {nowBytes,6:F1} B  {nowNs,6:F0} ns");

        Assert.Equal(0, nowBytes);
        Assert.True(legacyBytes > 0, "the legacy shape should allocate — otherwise the comparison is meaningless");
    }

    [Fact]
    public void ValidateStillAcceptsAndRefusesTheSameKeys()
    {
        _factory.CreateClient().Dispose();
        var cache = _factory.Services.GetRequiredService<ApiKeyCache>();

        Assert.True(cache.Validate(AmetoWebAppFactory.TestApiKey, ApiKeyPermissions.Logs));
        Assert.True(cache.Validate(AmetoWebAppFactory.TestApiKey, ApiKeyPermissions.All));
        Assert.False(cache.Validate("not-a-real-key", ApiKeyPermissions.Logs));
        Assert.False(cache.Validate("", ApiKeyPermissions.Logs));
        Assert.False(cache.Validate(AmetoWebAppFactory.TestApiKey.ToUpperInvariant(), ApiKeyPermissions.Logs));

        Assert.Equal(ApiKeyPermissions.All, cache.Resolve(AmetoWebAppFactory.TestApiKey));
        Assert.Null(cache.Resolve("not-a-real-key"));
    }

    private static string LegacyHash(ReadOnlySpan<char> rawKey)
    {
        Span<byte> utf8 = stackalloc byte[256];
        int written = Encoding.UTF8.GetBytes(rawKey, utf8);
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(utf8[..written], hash);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private static bool LegacyValidate(
        ImmutableDictionary<string, ApiKeyPermissions> map, ReadOnlySpan<char> rawKey, ApiKeyPermissions required)
        => map.TryGetValue(LegacyHash(rawKey), out var granted) && (granted & required) == required;
}
