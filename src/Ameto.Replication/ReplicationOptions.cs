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
    /// Format: <c>"http://host:port"</c>, or <c>"http://host:port/ameto"</c> for a peer served
    /// under a deployment prefix. A trailing slash is trimmed here so that no consumer has to
    /// remember to — see <see cref="ReplicationNode.BaseAddress"/> for what forgetting costs.
    /// </summary>
    public string[] SeedNodes     { get => field; init => field = (value ?? []).Select(static s => (s ?? "").Trim().TrimEnd('/')).ToArray(); } = [];

    /// <summary>
    /// Publicly reachable base URL of THIS node (sent to peers in probes). Include the
    /// deployment prefix if this node is served under one; a trailing slash is trimmed.
    /// </summary>
    public string   LocalAddress  { get => field; init => field = (value ?? "").Trim().TrimEnd('/'); } = "http://localhost:5341";

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
    /// system pushes. What a peer sends is a hot-tier LEVEL segment: <c>SegmentReplicator</c> is
    /// wired to <c>StorageEngine.SegmentFlushed</c> and nothing else, and that fires only where a
    /// flush publishes — a merged segment is never offered to a peer at all. So the bound is one
    /// flush's budget, <c>HotTier.MaxSizeBytes</c>, 64 MB by default and configurable upward,
    /// with LZ4 the only thing between it and the wire. Kestrel answers an over-long body by
    /// throwing out of the body read, and the receive endpoint folded that into the 500 its own
    /// published contract labels RETRYABLE.</para>
    ///
    /// <para>What that cost was NOT a retry storm, and the sentence that stood here described a
    /// mechanism this repository does not contain. <c>SegmentReplicator</c> pushes each segment
    /// once per healthy peer, fire-and-forget; a non-success status is one <c>LogWarning</c> and
    /// the push is dropped. No backoff, no queue, no reconciliation pass. So the segment was
    /// simply never replicated — permanently, from a single push, with the sender holding a
    /// status and no reason and the receiver holding neither.</para>
    ///
    /// <para>512 MB: the merge target measured BEFORE compression. That is not what reaches this
    /// endpoint today, it is the largest file this system builds, and the point of sizing the
    /// ceiling to it is that no legitimate body can ever be the thing it refuses — including if
    /// merged segments are replicated later. It costs the RECEIVER no memory, since the endpoint
    /// streams the body straight into a staging file; what it bounds there is the disk one
    /// authenticated peer can make this node write in a single request, which is why it is a
    /// number at all. It is not free on the SENDER: <c>SegmentReplicator.PushAsync</c> reads the
    /// whole segment through <c>File.ReadAllBytesAsync</c> into a <c>byte[]</c> and wraps it in
    /// <c>ByteArrayContent</c>, so a body at this ceiling is a half-gigabyte LOH allocation over
    /// there. That predates this setting and is not caused by it — raising the number is only
    /// what makes it reachable — which is why it is recorded beside the number.</para>
    /// </summary>
    public long     MaxSegmentBytes { get; init; } = 512L * 1024 * 1024;
}
