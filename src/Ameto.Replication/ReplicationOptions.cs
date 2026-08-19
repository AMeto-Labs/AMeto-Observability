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

    /// <summary>
    /// Largest segment body this node accepts on
    /// <c>POST /api/replication/segments/{nodeId}/{segmentId}</c>. A body over it is refused
    /// with <c>413</c> — the one refusal a sender must not answer by retrying the same bytes.
    ///
    /// <para>It has to be stated because the framework's own default, 30 MB, is below what this
    /// system produces. One hot-tier flush starts from a 64 MB budget
    /// (<c>HotTier.MaxSizeBytes</c>) and the merge planner deliberately grows cold files towards
    /// half a gigabyte of payload before LZ4, so a merged segment clears 30 MB as a matter of
    /// course. Kestrel answers an over-long body by throwing out of the body read, and the
    /// receive endpoint folded that into the 500 its own published contract labels RETRYABLE:
    /// a peer holding a merged segment retried the same body until the segment expired, and
    /// neither side logged a reason.</para>
    ///
    /// <para>512 MB, which is the merge target measured BEFORE compression, so a compressed
    /// segment fits under it with room to spare. The ceiling costs no memory — the endpoint
    /// streams the body straight into a staging file — only the disk one authenticated peer
    /// can make this node write in a single request, which is why it is a number at all.</para>
    /// </summary>
    public long     MaxSegmentBytes { get; init; } = 512L * 1024 * 1024;
}
