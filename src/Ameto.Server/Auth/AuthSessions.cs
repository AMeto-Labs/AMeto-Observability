namespace Ameto.Server.Auth;

/// <summary>A session as the admin's user page lists it.</summary>
internal sealed record SessionInfo(
    DateTimeOffset CreatedAt, DateTimeOffset LastUsedAt, DateTimeOffset ExpiresAt, string UserAgent, string Ip);

/// <summary>The session a refresh continued, and whose it is.</summary>
internal readonly record struct ContinuedSession(UserRecord User, string SessionId);

/// <summary>
/// Sign-ins that last: one row per signed-in browser in <c>auth_sessions</c>, and an HttpOnly
/// cookie that the page trades for a fresh access token whenever the old one runs out.
///
/// <para>Before this, the 2-hour access token WAS the sign-in. It could only be renewed while it
/// was still valid, so a tab closed overnight — or a laptop asleep for two hours — came back to
/// the login page. Now the token lives <see cref="JwtIssuer.ExpiresInSeconds"/> and the session
/// <see cref="Lifetime"/>, extended by every refresh: someone who opens Ameto at least once a
/// month never signs in again.</para>
///
/// <para>What still ends a session: signing out; an admin ending the user's sessions or blocking
/// the account; deleting the account; the identity provider no longer vouching for an OAuth
/// account (<see cref="ProviderAccessCheck"/>); <see cref="Lifetime"/> without a single refresh;
/// and the cookie turning up in two places at once (below).</para>
///
/// <para><b>Rotation.</b> Every refresh replaces the cookie with the next generation of the same
/// session (<see cref="SessionTokenCodec"/>). A generation older than the one just replaced means
/// the cookie was copied and both copies are in use, so the session is ended for both. The
/// generation just replaced is still accepted for <see cref="RotationGrace"/>, and answered with
/// its successor: two tabs refreshing at the same moment, or an answer lost on the way, are
/// ordinary.</para>
///
/// <para><b>The cookie.</b> HttpOnly, so script on the page cannot read it; SameSite=Strict, so
/// another site cannot make the browser send it; scoped to <c>/api/auth</c> under the deployment
/// prefix, so no other request carries it. Secure whenever the request came over HTTPS.</para>
/// </summary>
internal sealed class AuthSessions
{
    /// <summary>DI key of the clock the session code reads, so a test can move it.</summary>
    public const string ClockKey = "ameto-auth-clock";

    /// <summary>How long a session lasts without a refresh. Every refresh restarts it.</summary>
    public static readonly TimeSpan Lifetime = TimeSpan.FromDays(30);

    /// <summary>How long the generation a refresh replaced is still accepted.</summary>
    public static readonly TimeSpan RotationGrace = TimeSpan.FromMinutes(2);

    /// <summary>
    /// How long an ended session's row is kept. At least <see cref="AuthRevocations.RememberRevokedFor"/>,
    /// which reloads recent revocations from these rows at startup.
    /// </summary>
    private static readonly TimeSpan KeepEndedFor = TimeSpan.FromDays(1);

    private const int MaxUserAgentLength = 256;

    private readonly AuthDatabase          _db;
    private readonly AuthStore             _store;
    private readonly SessionTokenCodec     _codec;
    private readonly AuthRevocations       _revocations;
    private readonly ProviderAccessCheck   _provider;
    private readonly TimeProvider          _time;
    private readonly ILogger<AuthSessions> _logger;

    public AuthSessions(
        AuthDatabase db, AuthStore store, SessionTokenCodec codec, AuthRevocations revocations,
        ProviderAccessCheck provider, [FromKeyedServices(ClockKey)] TimeProvider time,
        ILogger<AuthSessions> logger)
    {
        _db          = db;
        _store       = store;
        _codec       = codec;
        _revocations = revocations;
        _provider    = provider;
        _time        = time;
        _logger      = logger;
    }

    /// <summary>
    /// Opens a session for a user who has just proved who they are, and sets its cookie on the
    /// response. Returns the session id, for the access token.
    /// </summary>
    public string Start(HttpContext ctx, UserRecord user)
    {
        var  now   = _time.GetUtcNow();
        long nowMs = now.ToUnixTimeMilliseconds();
        var  id    = SessionTokenCodec.NewSessionId();

        var userAgent = ctx.Request.Headers.UserAgent.ToString();
        if (userAgent.Length > MaxUserAgentLength) userAgent = userAgent[..MaxUserAgentLength];

        using (var conn = _db.Open())
        {
            using (var prune = conn.CreateCommand())
            {
                // Sign-ins are rare enough to carry the clean-up. A NULL revoked_at compares false.
                prune.CommandText = "DELETE FROM auth_sessions WHERE expires_at < @cutoff OR revoked_at < @cutoff";
                prune.Parameters.AddWithValue("@cutoff", (now - KeepEndedFor).ToUnixTimeMilliseconds());
                prune.ExecuteNonQuery();
            }

            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO auth_sessions
                    (id, user_id, generation, created_at, rotated_at, last_used_at, expires_at, user_agent, ip)
                VALUES (@id, @u, 1, @now, @now, @now, @exp, @ua, @ip)
                """;
            cmd.Parameters.AddWithValue("@id",  id);
            cmd.Parameters.AddWithValue("@u",   user.Id);
            cmd.Parameters.AddWithValue("@now", nowMs);
            cmd.Parameters.AddWithValue("@exp", (now + Lifetime).ToUnixTimeMilliseconds());
            cmd.Parameters.AddWithValue("@ua",  userAgent);
            cmd.Parameters.AddWithValue("@ip",  ctx.Connection.RemoteIpAddress?.ToString() ?? "");
            cmd.ExecuteNonQuery();
        }

        WriteCookie(ctx, _codec.Encode(id, 1), now + Lifetime);
        return id;
    }

    /// <summary>
    /// Continues the session the request's cookie names: checks it is still allowed to, rotates the
    /// cookie, and extends the session by <see cref="Lifetime"/>. Null — with the cookie cleared —
    /// when there is no session to continue. Throws only for failures that say nothing about the
    /// session (the database), which the client must not mistake for a sign-out.
    /// </summary>
    public async Task<ContinuedSession?> RefreshAsync(HttpContext ctx, CancellationToken ct)
    {
        if (!ctx.Request.Cookies.TryGetValue(_codec.CookieName, out var cookie)
            || !_codec.TryDecode(cookie, out var id, out long generation))
            return Refuse(ctx);

        var  row   = Read(id);
        long nowMs = _time.GetUtcNow().ToUnixTimeMilliseconds();
        if (row is null || row.RevokedAtMs is not null || row.ExpiresAtMs <= nowMs)
            return Refuse(ctx);

        // Ahead of the database: only a database restored from a backup gets here. Nothing to
        // continue from, but nothing suggests a copied cookie either.
        if (generation > row.Generation)
            return Refuse(ctx);

        bool current      = generation == row.Generation;
        bool justReplaced = generation == row.Generation - 1
                         && nowMs - row.RotatedAtMs <= (long)RotationGrace.TotalMilliseconds;
        if (!current && !justReplaced)
        {
            // Anything older than the generation just replaced was handed out earlier and taken
            // back since, so somebody still holding it kept a copy. Which of the two holders is
            // the owner cannot be told from here; ending the session stops both.
            if (Revoke(id, "reuse"))
                _logger.LogWarning(
                    "Ended a session of user {UserId}: a cookie {Behind} generation(s) behind came back " +
                    "after being replaced, so a copy of it is in use somewhere else",
                    row.UserId, row.Generation - generation);
            return Refuse(ctx);
        }

        var user = _store.GetUser(row.UserId);
        if (user is null || user.Disabled)
        {
            Revoke(id, user is null ? "deleted" : "blocked");
            return Refuse(ctx);
        }

        if (user.Provider != "local")
        {
            switch (await _provider.CheckAsync(user, ct).ConfigureAwait(false))
            {
                case ProviderVerdict.Ended:
                    int ended = RevokeAllForUser(user.Id, "provider");
                    _logger.LogInformation(
                        "{Provider} no longer accepts the sign-in of {User}: ended {Count} session(s)",
                        user.Provider, user.Email, ended);
                    return Refuse(ctx);
                case ProviderVerdict.Unverified:
                    Revoke(id, "unverified");
                    _logger.LogWarning(
                        "Ended a session of {User}: {Provider} has not confirmed the account for more than {Hours} h",
                        user.Email, user.Provider, ProviderAccessCheck.TrustAtMost.TotalHours);
                    return Refuse(ctx);
            }
        }

        long next;
        if (TryRotate(id, generation, nowMs))
        {
            next = generation + 1;
        }
        else
        {
            // Not the current generation any more: either it was the one just replaced, or another
            // tab rotated it while this request waited on the provider. Either way the successor
            // already exists, and this request gets that one rather than minting a third.
            var latest = Read(id);
            if (latest is null || latest.RevokedAtMs is not null || latest.Generation != generation + 1)
                return Refuse(ctx);
            Touch(id, nowMs);
            next = latest.Generation;
        }

        WriteCookie(ctx, _codec.Encode(id, next), _time.GetUtcNow() + Lifetime);
        return new ContinuedSession(user, id);
    }

    /// <summary>Signs the request's browser out: ends its session and clears the cookie.</summary>
    public void End(HttpContext ctx)
    {
        if (ctx.Request.Cookies.TryGetValue(_codec.CookieName, out var cookie)
            && _codec.TryDecode(cookie, out var id, out _))
            Revoke(id, "logout");
        ClearCookie(ctx);
    }

    /// <summary>Ends every live session of a user. Returns how many there were.</summary>
    public int RevokeAllForUser(string userId, string reason)
    {
        var  ended = new List<string>();
        long nowMs = _time.GetUtcNow().ToUnixTimeMilliseconds();
        using (var conn = _db.Open())
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = """
                UPDATE auth_sessions SET revoked_at = @now, revoke_reason = @r
                WHERE user_id = @u AND revoked_at IS NULL AND expires_at > @now
                RETURNING id
                """;
            cmd.Parameters.AddWithValue("@now", nowMs);
            cmd.Parameters.AddWithValue("@r",   reason);
            cmd.Parameters.AddWithValue("@u",   userId);
            using var r = cmd.ExecuteReader();
            while (r.Read()) ended.Add(r.GetString(0));
        }
        foreach (var id in ended) _revocations.SessionRevoked(id);
        return ended.Count;
    }

    /// <summary>A user's live sessions, most recently used first.</summary>
    public IReadOnlyList<SessionInfo> ListActive(string userId)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            SELECT created_at, last_used_at, expires_at, user_agent, ip FROM auth_sessions
            WHERE user_id = @u AND revoked_at IS NULL AND expires_at > @now
            ORDER BY last_used_at DESC
            """;
        cmd.Parameters.AddWithValue("@u",   userId);
        cmd.Parameters.AddWithValue("@now", _time.GetUtcNow().ToUnixTimeMilliseconds());
        using var r = cmd.ExecuteReader();
        var result = new List<SessionInfo>();
        while (r.Read())
            result.Add(new SessionInfo(
                DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(0)),
                DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(1)),
                DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(2)),
                r.GetString(3), r.GetString(4)));
        return result;
    }

    // ── Rows ──────────────────────────────────────────────────────────────────

    private sealed record SessionRow(string UserId, long Generation, long RotatedAtMs, long ExpiresAtMs, long? RevokedAtMs);

    private SessionRow? Read(string id)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT user_id, generation, rotated_at, expires_at, revoked_at FROM auth_sessions WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;
        return new SessionRow(r.GetString(0), r.GetInt64(1), r.GetInt64(2), r.GetInt64(3),
                              r.IsDBNull(4) ? null : r.GetInt64(4));
    }

    /// <summary>
    /// Moves the session from <paramref name="generation"/> to the next one. False when it is not
    /// at that generation any more (a concurrent refresh moved it) or has been revoked meanwhile.
    /// </summary>
    private bool TryRotate(string id, long generation, long nowMs)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            UPDATE auth_sessions
            SET generation = @g + 1, rotated_at = @now, last_used_at = @now, expires_at = @exp
            WHERE id = @id AND generation = @g AND revoked_at IS NULL
            """;
        cmd.Parameters.AddWithValue("@id",  id);
        cmd.Parameters.AddWithValue("@g",   generation);
        cmd.Parameters.AddWithValue("@now", nowMs);
        cmd.Parameters.AddWithValue("@exp", nowMs + (long)Lifetime.TotalMilliseconds);
        return cmd.ExecuteNonQuery() == 1;
    }

    private void Touch(string id, long nowMs)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            UPDATE auth_sessions SET last_used_at = @now, expires_at = @exp
            WHERE id = @id AND revoked_at IS NULL
            """;
        cmd.Parameters.AddWithValue("@id",  id);
        cmd.Parameters.AddWithValue("@now", nowMs);
        cmd.Parameters.AddWithValue("@exp", nowMs + (long)Lifetime.TotalMilliseconds);
        cmd.ExecuteNonQuery();
    }

    /// <summary>Ends one session. False when it had already ended.</summary>
    private bool Revoke(string id, string reason)
    {
        bool revoked;
        using (var conn = _db.Open())
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = """
                UPDATE auth_sessions SET revoked_at = @now, revoke_reason = @r
                WHERE id = @id AND revoked_at IS NULL
                """;
            cmd.Parameters.AddWithValue("@id",  id);
            cmd.Parameters.AddWithValue("@r",   reason);
            cmd.Parameters.AddWithValue("@now", _time.GetUtcNow().ToUnixTimeMilliseconds());
            revoked = cmd.ExecuteNonQuery() > 0;
        }
        // Told even when the row was already revoked: telling twice is harmless, and a revocation
        // that reached the database but not the memory (a crash in between) is healed by the next.
        _revocations.SessionRevoked(id);
        return revoked;
    }

    // ── Cookie ────────────────────────────────────────────────────────────────

    private ContinuedSession? Refuse(HttpContext ctx)
    {
        ClearCookie(ctx);
        return null;
    }

    private void WriteCookie(HttpContext ctx, string value, DateTimeOffset expires) =>
        ctx.Response.Cookies.Append(_codec.CookieName, value, CookieOptionsFor(ctx, expires));

    private void ClearCookie(HttpContext ctx) =>
        ctx.Response.Cookies.Delete(_codec.CookieName, CookieOptionsFor(ctx, expires: null));

    /// <summary>
    /// Under the prefix the request came in on: the browser sees <c>/ameto/api/auth/…</c> behind a
    /// prefixed proxy, and a cookie scoped to <c>/api/auth</c> would never be sent back there.
    /// </summary>
    internal static string CookiePath(HttpContext ctx) => ctx.Request.PathBase.Add("/api/auth").ToUriComponent();

    private static CookieOptions CookieOptionsFor(HttpContext ctx, DateTimeOffset? expires) => new()
    {
        HttpOnly    = true,
        // A Secure cookie set over plain HTTP — a self-hosted install without TLS — is never sent
        // back, which would sign everyone out at their first refresh.
        Secure      = ctx.Request.IsHttps,
        SameSite    = SameSiteMode.Strict,
        Path        = CookiePath(ctx),
        Expires     = expires,
        IsEssential = true,
    };
}
