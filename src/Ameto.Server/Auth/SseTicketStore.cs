using System.Collections.Concurrent;
using System.Security.Cryptography;

namespace Ameto.Server.Auth;

/// <summary>
/// Short-lived, single-use tickets that stand in for a JWT on the SSE query
/// string. <see cref="EventSource"/> can't send an <c>Authorization</c> header,
/// so the token would otherwise ride in the URL — where it leaks into proxy
/// access logs, browser history and <c>Referer</c>. Instead the client exchanges
/// its bearer token for a ticket (a random opaque value), passes the ticket in
/// the URL, and the JWT auth middleware redeems it back to the token. Tickets are
/// removed on first use and expire after <see cref="TtlSeconds"/>.
/// </summary>
internal sealed class SseTicketStore
{
    private const int TtlSeconds = 60;

    private readonly ConcurrentDictionary<string, (string Jwt, long ExpiryTicks)> _tickets = new(StringComparer.Ordinal);

    /// <summary>Stores a JWT behind a fresh opaque ticket and returns the ticket.</summary>
    public string Issue(string jwt)
    {
        PruneExpired();
        var ticket = Convert.ToHexString(RandomNumberGenerator.GetBytes(24)).ToLowerInvariant();
        _tickets[ticket] = (jwt, DateTime.UtcNow.AddSeconds(TtlSeconds).Ticks);
        return ticket;
    }

    /// <summary>Redeems a ticket exactly once; returns its JWT or null when unknown/expired/used.</summary>
    public string? Consume(string ticket)
    {
        if (string.IsNullOrEmpty(ticket)) return null;
        if (!_tickets.TryRemove(ticket, out var e)) return null;
        return e.ExpiryTicks > DateTime.UtcNow.Ticks ? e.Jwt : null;
    }

    public static int TicketTtlSeconds => TtlSeconds;

    private void PruneExpired()
    {
        if (_tickets.Count < 256) return; // cheap: only sweep once it grows
        long now = DateTime.UtcNow.Ticks;
        foreach (var (k, v) in _tickets)
            if (v.ExpiryTicks <= now) _tickets.TryRemove(k, out _);
    }
}
