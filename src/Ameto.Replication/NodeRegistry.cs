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

    /// <summary>
    /// Raised once for each peer id that appears here for the first time. Exists so
    /// <see cref="PeerProber"/> can sleep instead of polling when this node is alone:
    /// with no seeds, an inbound ping is the only way a peer can ever show up, and this
    /// is that moment.
    /// </summary>
    public event Action? PeerAdded;

    /// <summary>Register or update a peer from an incoming probe payload.</summary>
    public ReplicationNode Upsert(PeerPayload payload)
    {
        // Not AddOrUpdate: its add-factory may run and be discarded under contention, which
        // would announce a peer that was never added. TryAdd tells us who actually won.
        while (true)
        {
            if (_nodes.TryGetValue(payload.NodeId, out var existing))
            {
                existing.LastSeen = payload.Timestamp;
                return existing;
            }

            var fresh = new ReplicationNode
            {
                Id          = new NodeId(payload.NodeId),
                BaseAddress = payload.Address,
                LastSeen    = payload.Timestamp,
            };
            if (_nodes.TryAdd(payload.NodeId, fresh))
            {
                PeerAdded?.Invoke();
                return fresh;
            }
        }
    }

    /// <summary>
    /// Whether any node other than <paramref name="local"/> is known. The local node is
    /// always registered, so "is there anything to probe" is not the same as "is this empty".
    /// Allocation-free on the common answer.
    /// </summary>
    public bool HasPeerOtherThan(NodeId local)
    {
        if (_nodes.IsEmpty) return false;
        foreach (var kv in _nodes)
            if (kv.Key != local.Value) return true;
        return false;
    }

    /// <summary>
    /// Register a peer from static config (before first probe). Deliberately silent: its only
    /// caller registers THIS node, and waking the prober for ourselves would have it probe its
    /// own address — see <see cref="PeerAdded"/>, which announces discovered peers only.
    /// </summary>
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
