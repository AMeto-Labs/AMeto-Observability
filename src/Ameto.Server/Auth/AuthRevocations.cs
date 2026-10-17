using System.Collections.Concurrent;
using System.Security.Claims;

namespace Ameto.Server.Auth;

/// <summary>
/// Makes an ended session or a blocked account stop working on its very next request, instead of
/// when its access token runs out.
///
/// <para>An access token is checked by signature and expiry alone, so on its own it outlives
/// whatever ended its session by up to <see cref="JwtIssuer.ExpiresInSeconds"/>. The bearer
/// handler asks <see cref="IsRefused"/> after validating each token (see AddAmetoAuth): a token
/// whose session was revoked, or whose user is blocked, fails authentication there.</para>
///
/// <para>Both sets live in memory and stay small. A revoked session is remembered only as long as
/// a token issued under it can still be unexpired; a blocked account until it is unblocked or
/// deleted. Both are reloaded from the database at startup, so a restart does not hand a
/// just-revoked token its last minutes back.</para>
///
/// <para>Open event streams are the one kind of request that outlives a revocation by more than
/// its own round trip — a live tail runs for hours. They are registered while they run
/// (<see cref="TrackStream"/>) and aborted when their session ends.</para>
/// </summary>
internal sealed class AuthRevocations
{
    /// <summary>
    /// How long a revoked session id is remembered: a token issued the instant before the
    /// revocation expires <see cref="JwtIssuer.ExpiresInSeconds"/> later and is accepted for
    /// <see cref="JwtIssuer.AllowedClockSkew"/> past that. One more minute for clocks.
    /// </summary>
    public static readonly TimeSpan RememberRevokedFor =
        TimeSpan.FromSeconds(JwtIssuer.ExpiresInSeconds) + JwtIssuer.AllowedClockSkew + TimeSpan.FromMinutes(1);

    /// <summary>Above this many remembered sessions an addition sweeps out the ones past their time.</summary>
    private const int SweepAbove = 256;

    private readonly ConcurrentDictionary<string, long> _revokedSessions = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _blockedUsers    = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<long, OpenStream> _streams     = new();
    private readonly TimeProvider _time;
    private long _nextStreamId;

    public AuthRevocations(AuthDatabase db, AuthStore store,
        [FromKeyedServices(AuthSessions.ClockKey)] TimeProvider time)
    {
        _time = time;
        foreach (var username in store.ListDisabledUsernames())
            _blockedUsers[username] = 0;

        long since = (_time.GetUtcNow() - RememberRevokedFor).ToUnixTimeMilliseconds();
        using var conn = db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT id, revoked_at FROM auth_sessions WHERE revoked_at >= @since";
        cmd.Parameters.AddWithValue("@since", since);
        using var r = cmd.ExecuteReader();
        while (r.Read()) _revokedSessions[r.GetString(0)] = r.GetInt64(1);
    }

    /// <summary>True when the token's session has ended or its user is blocked.</summary>
    public bool IsRefused(ClaimsPrincipal principal) =>
        IsRefused(principal.FindFirst(JwtIssuer.SessionClaim)?.Value, principal.FindFirst(ClaimTypes.Name)?.Value);

    // ContainsKey and not an IsEmpty short-cut first: on an EMPTY ConcurrentDictionary — the usual
    // state of both — IsEmpty takes every bucket lock to be sure, and this runs on each request.
    private bool IsRefused(string? sessionId, string? username) =>
        (sessionId is not null && _revokedSessions.ContainsKey(sessionId))
        || (username is not null && _blockedUsers.ContainsKey(username));

    /// <summary>Called once a session is revoked in the database.</summary>
    public void SessionRevoked(string sessionId)
    {
        long now = _time.GetUtcNow().ToUnixTimeMilliseconds();
        _revokedSessions[sessionId] = now;
        if (_revokedSessions.Count > SweepAbove)
        {
            long forgetBefore = now - (long)RememberRevokedFor.TotalMilliseconds;
            foreach (var (id, at) in _revokedSessions)
                if (at < forgetBefore) _revokedSessions.TryRemove(id, out _);
        }
        AbortStreams(sessionId, username: null);
    }

    /// <summary>Called once an account is blocked in the database.</summary>
    public void UserBlocked(string username)
    {
        _blockedUsers[username] = 0;
        AbortStreams(sessionId: null, username);
    }

    /// <summary>Called once an account is unblocked, or deleted (its username may be provisioned again).</summary>
    public void UserUnblocked(string username) => _blockedUsers.TryRemove(username, out _);

    /// <summary>
    /// Registers an open event stream so that ending its session aborts it. Dispose when the
    /// request ends — the registration holds the request's <see cref="HttpContext"/>, which the
    /// server reuses for another request afterwards.
    /// </summary>
    public IDisposable TrackStream(HttpContext ctx)
    {
        var stream = new OpenStream(this, Interlocked.Increment(ref _nextStreamId), ctx,
            ctx.User.FindFirst(JwtIssuer.SessionClaim)?.Value, ctx.User.FindFirst(ClaimTypes.Name)?.Value);
        _streams[stream.Id] = stream;
        // A revocation that landed between the token check and the line above found nothing to
        // abort. Checked again now that the stream can be found.
        if (IsRefused(stream.SessionId, stream.Username)) stream.Abort();
        return stream;
    }

    private void AbortStreams(string? sessionId, string? username)
    {
        foreach (var stream in _streams.Values)
            if ((sessionId is not null && stream.SessionId == sessionId)
                || (username is not null && string.Equals(stream.Username, username, StringComparison.OrdinalIgnoreCase)))
                stream.Abort();
    }

    private sealed class OpenStream : IDisposable
    {
        private readonly AuthRevocations              _owner;
        private readonly CancellationTokenSource      _abort = new();
        private readonly CancellationTokenRegistration _registration;

        public OpenStream(AuthRevocations owner, long id, HttpContext ctx, string? sessionId, string? username)
        {
            _owner    = owner;
            Id        = id;
            SessionId = sessionId;
            Username  = username;
            _registration = _abort.Token.Register(static c => ((HttpContext)c!).Abort(), ctx);
        }

        public long    Id        { get; }
        public string? SessionId { get; }
        public string? Username  { get; }

        public void Abort()
        {
            // Racing Dispose: once the registration is disposed nothing reaches the context, and
            // cancelling a disposed source throws instead — either way the request has ended.
            // An abort is best-effort; it must never fail the revocation that asked for it.
            try { _abort.Cancel(); }
            catch (ObjectDisposedException) { }
            catch (AggregateException) { }
        }

        public void Dispose()
        {
            _owner._streams.TryRemove(Id, out _);
            // Waits for an Abort callback already running, so none can reach the context after
            // this returns and the server hands it to the next request.
            _registration.Dispose();
            _abort.Dispose();
        }
    }
}
