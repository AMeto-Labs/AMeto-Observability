namespace Ameto.Replication;

/// <summary>
/// Configuration for segment replication.
/// Bind from "Ameto:Replication" configuration section.
/// When Enabled = false the node runs standalone (no replication).
/// </summary>
public sealed class ReplicationOptions
{
    /// <summary>Enable replication. Default: false (standalone).</summary>
    public bool     Enabled       { get; init; } = false;

    /// <summary>
    /// Shared secret authenticating peer-to-peer calls (ping + segment push).
    /// REQUIRED when replication is enabled: peers send it in the
    /// <c>X-Ameto-Replication</c> header and the receiver rejects mismatches.
    /// When blank, the receive endpoints refuse every request (fail-closed) so an
    /// open port can't be used to inject forged segments.
    /// </summary>
    public string   Secret        { get; init; } = "";

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
