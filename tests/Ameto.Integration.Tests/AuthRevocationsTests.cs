using System.Security.Claims;
using Ameto.Server.Auth;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// The memory behind "revoked means revoked now": what survives a restart, and the cookie codec
/// it trusts.
/// </summary>
public sealed class AuthRevocationsTests : IDisposable
{
    private readonly string       _dir = Path.Combine(Path.GetTempPath(), "ameto-revocations-" + Guid.NewGuid().ToString("N")[..8]);
    private readonly AuthDatabase _db;
    private readonly AuthStore    _store;

    public AuthRevocationsTests()
    {
        _db    = new AuthDatabase(_dir);
        _store = new AuthStore(_db, new AuthOptions { AdminPassword = "seeded-for-tests" }, NullLogger<AuthStore>.Instance);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* SQLite handles may linger */ }
    }

    private void InsertSession(string id, string userId, DateTimeOffset? revokedAt)
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO auth_sessions (id, user_id, generation, created_at, rotated_at, last_used_at, expires_at, revoked_at)
            VALUES (@id, @u, 1, @now, @now, @now, @exp, @rev)
            """;
        cmd.Parameters.AddWithValue("@id",  id);
        cmd.Parameters.AddWithValue("@u",   userId);
        cmd.Parameters.AddWithValue("@now", now);
        cmd.Parameters.AddWithValue("@exp", now + (long)TimeSpan.FromDays(30).TotalMilliseconds);
        cmd.Parameters.AddWithValue("@rev", revokedAt is { } r ? r.ToUnixTimeMilliseconds() : DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    private static ClaimsPrincipal TokenOf(string username, string sessionId) => new(new ClaimsIdentity(
    [
        new Claim(ClaimTypes.Name, username),
        new Claim(JwtIssuer.SessionClaim, sessionId),
    ], "Bearer"));

    [Fact]
    public void ARestartStillRefusesTokensOfARecentlyRevokedSession()
    {
        // Revoked a minute before the restart: a token issued under it may have 14 minutes left.
        InsertSession("recent", "u1", DateTimeOffset.UtcNow.AddMinutes(-1));
        // Revoked an hour ago: every token under it has expired; nothing to remember.
        InsertSession("old", "u1", DateTimeOffset.UtcNow.AddHours(-1));
        InsertSession("live", "u1", revokedAt: null);

        var afterRestart = new AuthRevocations(_db, _store, TimeProvider.System);

        Assert.True(afterRestart.IsRefused(TokenOf("someone", "recent")));
        Assert.False(afterRestart.IsRefused(TokenOf("someone", "old")));
        Assert.False(afterRestart.IsRefused(TokenOf("someone", "live")));
    }

    [Fact]
    public void ARestartStillRefusesABlockedUsersTokensWhateverTheirSession()
    {
        var user = _store.CreateUser("mila", "mila-password", "viewer");
        _store.SetDisabled(user.Id, disabled: true);

        var afterRestart = new AuthRevocations(_db, _store, TimeProvider.System);

        Assert.True(afterRestart.IsRefused(TokenOf("MILA", "any-session")));   // usernames are case-insensitive
        afterRestart.UserUnblocked("mila");
        Assert.False(afterRestart.IsRefused(TokenOf("mila", "any-session")));
    }

    [Fact]
    public void TheCookieCodecReadsBackOnlyWhatItMinted()
    {
        var codec = SessionTokenCodec.FromJwtSecret("secret-one");
        var value = codec.Encode("session-id", 7);

        Assert.True(codec.TryDecode(value, out var id, out var generation));
        Assert.Equal("session-id", id);
        Assert.Equal(7, generation);

        // Another key (a rotated jwt-secret.bin), another generation under the same MAC, garbage.
        Assert.False(SessionTokenCodec.FromJwtSecret("secret-two").TryDecode(value, out _, out _));
        Assert.False(codec.TryDecode(value.Replace(".7.", ".8.", StringComparison.Ordinal), out _, out _));
        Assert.False(codec.TryDecode("session-id.7", out _, out _));
        Assert.False(codec.TryDecode("", out _, out _));
        Assert.False(codec.TryDecode(new string('a', 500), out _, out _));
    }

    [Fact]
    public void TwoServersOnOneHostNameTheirCookiesApart()
    {
        // Cookies ignore the port, so localhost:8555 and localhost:8556 share one cookie jar; a
        // shared name would let each server overwrite the other's session at every refresh.
        var one = SessionTokenCodec.FromJwtSecret("secret-one");
        var two = SessionTokenCodec.FromJwtSecret("secret-two");

        Assert.NotEqual(one.CookieName, two.CookieName);
        Assert.StartsWith(SessionTokenCodec.CookieNamePrefix, one.CookieName, StringComparison.Ordinal);
        // And stable across restarts, which keep the secret.
        Assert.Equal(one.CookieName, SessionTokenCodec.FromJwtSecret("secret-one").CookieName);
    }
}
