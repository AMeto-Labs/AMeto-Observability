using Ameto.Core;

namespace Ameto.Replication;

/// <summary>Runtime state of a known replication peer.</summary>
public sealed class ReplicationNode
{
    public required NodeId        Id          { get; init; }
    /// <summary>
    /// <c>"http://host:port"</c>, optionally carrying the peer's deployment prefix
    /// (<c>"http://host:port/ameto"</c>).
    ///
    /// <para>Normalised here rather than at each use, because every consumer builds its URL by
    /// concatenating a root-absolute path onto this — and a trailing slash would produce
    /// <c>"…/ameto//api/replication/ping"</c>, which matches no route. That failure is a silent
    /// one: <c>PeerProber</c> returns on a non-2xx without logging and <c>SegmentReplicator</c>
    /// pushes each segment once with no retry, so the next reader to forget would simply stop
    /// replicating, with nothing in the log to say why.</para>
    /// </summary>
    public required string        BaseAddress { get => field; init => field = (value ?? "").Trim().TrimEnd('/'); }
    public DateTimeOffset         LastSeen    { get; set; }   = DateTimeOffset.MinValue;

    /// <summary>True if a probe succeeded within the last 30 seconds.</summary>
    public bool IsHealthy => (DateTimeOffset.UtcNow - LastSeen) < TimeSpan.FromSeconds(30);
}

/// <summary>Payload exchanged in peer-probe requests.</summary>
public sealed class PeerPayload
{
    public required uint           NodeId    { get; init; }
    public required string         Address   { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
}
