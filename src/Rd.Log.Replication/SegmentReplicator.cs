using System.Net.Http.Headers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Rd.Log.Core;
using Rd.Log.Storage;

namespace Rd.Log.Replication;

/// <summary>
/// Replicates completed cold-tier segments to all healthy peers.
/// Each node replicates only segments it produced itself — prevents re-replication loops.
/// Triggered via <see cref="StorageEngine.SegmentFlushed"/>, wired by <see cref="ReplicationServiceExtensions"/>.
/// </summary>
public sealed class SegmentReplicator : IDisposable
{
    private readonly ReplicationOptions          _opts;
    private readonly NodeRegistry                _registry;
    private readonly ILogger<SegmentReplicator>  _logger;
    private readonly HttpClient                  _http;
    private          NodeId                      _localId;

    public SegmentReplicator(
        IOptions<ReplicationOptions>  opts,
        NodeRegistry                  registry,
        ILogger<SegmentReplicator>    logger,
        IHttpClientFactory            httpFactory)
    {
        _opts     = opts.Value;
        _registry = registry;
        _logger   = logger;
        _http     = httpFactory.CreateClient("replication-push");
        _http.Timeout = _opts.PushTimeout;
    }

    internal void SetLocalNodeId(NodeId id) => _localId = id;

    /// <summary>
    /// Hook wired into <see cref="StorageEngine.SegmentFlushed"/>.
    /// Only replicates OWN segments — segments imported from peers are ignored.
    /// Fire-and-forget so the storage flush path is never blocked.
    /// </summary>
    public void OnSegmentFlushed(SegmentInfo segment)
    {
        if (!_opts.Enabled) return;
        if (segment.NodeId.Value != _localId.Value) return; // not ours — skip
        _ = Task.Run(() => ReplicateAsync(segment, CancellationToken.None));
    }

    private async Task ReplicateAsync(SegmentInfo segment, CancellationToken ct)
    {
        var peers = _registry.GetHealthyPeers(_localId);
        if (peers.Count == 0) return;

        _logger.LogInformation("Replicating segment {Id} to {Count} peer(s)", segment.Id, peers.Count);

        byte[] data;
        try
        {
            data = await File.ReadAllBytesAsync(segment.FilePath, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read segment {Path}", segment.FilePath);
            return;
        }

        await Task.WhenAll(peers.Select(p => PushAsync(p, segment, data, ct)));
    }

    private async Task PushAsync(
        ReplicationNode   peer,
        SegmentInfo       segment,
        byte[]            data,
        CancellationToken ct)
    {
        var url = $"{peer.BaseAddress}/api/replication/segments/{segment.NodeId.Value}/{segment.Id.Value}";
        try
        {
            using var content = new ByteArrayContent(data);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            using var resp = await _http.PostAsync(url, content, ct);

            if (!resp.IsSuccessStatusCode)
                _logger.LogWarning("Push to {Addr} returned {Status}", peer.BaseAddress, resp.StatusCode);
            else
                _logger.LogDebug("Replicated segment {Id} -> {Addr}", segment.Id, peer.BaseAddress);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Push segment {Id} to {Addr} failed", segment.Id, peer.BaseAddress);
        }
    }

    public void Dispose() => _http.Dispose();
}
