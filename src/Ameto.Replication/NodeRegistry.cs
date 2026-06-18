using System.Collections.Concurrent;
using Ameto.Core;

namespace Ameto.Replication;

/// <summary>
/// Thread-safe registry of all known replication peers.
/// Updated by <see cref="PeerProber"/> as probe responses arrive.
/// </summary>
public sealed class NodeRegistry
{
    private readonly ConcurrentDictionary<uint, ReplicationNode> _nodes = new();

    /// <summary>Register or update a peer from an incoming probe payload.</summary>
    public ReplicationNode Upsert(PeerPayload payload)
    {
        return _nodes.AddOrUpdate(
            payload.NodeId,
            _ => new ReplicationNode
            {
                Id          = new NodeId(payload.NodeId),
                BaseAddress = payload.Address,
                LastSeen    = payload.Timestamp,
            },
            (_, existing) =>
            {
                existing.LastSeen = payload.Timestamp;
                return existing;
            });
    }

    /// <summary>Register a peer from static config (before first probe).</summary>
    public void EnsureKnown(NodeId id, string address)
    {
        _nodes.TryAdd(id.Value, new ReplicationNode
        {
            Id          = id,
            BaseAddress = address,
        });
    }

    public ReplicationNode? Get(NodeId id) =>
        _nodes.TryGetValue(id.Value, out var n) ? n : null;

    public IReadOnlyList<ReplicationNode> GetAll() => _nodes.Values.ToList();

    /// <summary>All healthy peers except the local node.</summary>
    public IReadOnlyList<ReplicationNode> GetHealthyPeers(NodeId localId) =>
        _nodes.Values.Where(n => n.Id.Value != localId.Value && n.IsHealthy).ToList();

    public void Remove(NodeId id) => _nodes.TryRemove(id.Value, out _);
}
