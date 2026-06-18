using Ameto.Core;

namespace Ameto.Replication;

/// <summary>Runtime state of a known replication peer.</summary>
public sealed class ReplicationNode
{
    public required NodeId        Id          { get; init; }
    public required string        BaseAddress { get; init; }  // "http://host:port"
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
