namespace Rd.Log.Replication;

/// <summary>
/// Configuration for segment replication.
/// Bind from "RdLog:Replication" configuration section.
/// When Enabled = false the node runs standalone (no replication).
/// </summary>
public sealed class ReplicationOptions
{
    /// <summary>Enable replication. Default: false (standalone).</summary>
    public bool     Enabled       { get; init; } = false;

    /// <summary>
    /// Peer addresses to contact on startup for initial discovery.
    /// Format: "http://host:port".
    /// </summary>
    public string[] SeedNodes     { get; init; } = [];

    /// <summary>Publicly reachable base URL of THIS node (sent to peers in probes).</summary>
    public string   LocalAddress  { get; init; } = "http://localhost:5341";

    /// <summary>Per-segment HTTP push timeout.</summary>
    public TimeSpan PushTimeout   { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>How often to probe known peers to refresh liveness.</summary>
    public TimeSpan ProbeInterval { get; init; } = TimeSpan.FromSeconds(10);
}
