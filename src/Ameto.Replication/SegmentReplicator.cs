using System.Net.Http.Headers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Replication;

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
            using var req = new HttpRequestMessage(HttpMethod.Post, url) { Content = content };
            req.Headers.Add("X-Ameto-Replication", _opts.Secret);
            // ResponseHeadersRead: the status decides everything below, and the body is only
            // ever read -- bounded -- to be quoted in a log line. A buffered send with a size
            // cap was tried here and rejected: the cap fires inside SendAsync itself, so an
            // oversized body (a proxy's HTML error page, a ProblemDetails with a stack) took
            // down the classified response whole, and 409/413 collapsed into a generic
            // "push failed" with no status at all -- diagnostics erased exactly where they
            // were needed.
            using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);

            // 409 is the peer REFUSING the segment, and the receiver has TWO distinct reasons
            // to do it: a different segment already sits under this (NodeId, Id), or the id
            // falls inside the span its own allocator has already handed out. Both mean a
            // duplicated NodeId, but they name different evidence, and this method used to
            // compose its own single story -- confidently describing the first cause when the
            // receiver had reported the second. The receiver's body says which one it saw, so
            // it is quoted instead of paraphrased. Error, not warning: a deployment fault, not
            // a push failure.
            if (resp.StatusCode == System.Net.HttpStatusCode.Conflict)
                _logger.LogError(
                    "Peer {Addr} refused segment {Id} under node id {Node} (409): two nodes appear " +
                    "to share that NodeId, and nothing will replicate under it until one of them " +
                    "is renumbered. Receiver said: {Body}",
                    peer.BaseAddress, segment.Id, segment.NodeId.Value, await ReadBodyAsync(resp, ct));
            // 413 is terminal for this segment: the same bytes are the same size, so a retry
            // cannot fix it -- only raising Ameto:Replication:MaxSegmentBytes on the RECEIVER
            // can, and the receiver's body is the one line that says so. This branch used to
            // fall into the generic warning below, which discarded that line and made the
            // refusal look transient.
            else if (resp.StatusCode == System.Net.HttpStatusCode.RequestEntityTooLarge)
                _logger.LogWarning(
                    "Peer {Addr} refused segment {Id}: {Bytes} bytes is over the receiver's " +
                    "request-body limit, and re-sending cannot change that. Raise " +
                    "Ameto:Replication:MaxSegmentBytes on the receiver or this segment will never " +
                    "replicate there. Receiver said: {Body}",
                    peer.BaseAddress, segment.Id, data.Length, await ReadBodyAsync(resp, ct));
            else if (!resp.IsSuccessStatusCode)
                _logger.LogWarning("Push to {Addr} returned {Status}", peer.BaseAddress, resp.StatusCode);
            else
                _logger.LogDebug("Replicated segment {Id} -> {Addr}", segment.Id, peer.BaseAddress);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Push segment {Id} to {Addr} failed", segment.Id, peer.BaseAddress);
        }
    }

    /// <summary>
    /// At most 4 KB of the response body, read off the stream into a pooled buffer, flattened
    /// onto one line and capped -- a log field, not a payload. The response is never buffered
    /// (the send uses ResponseHeadersRead), so a body of any size costs at most the 4 KB
    /// actually read.
    /// </summary>
    private static async Task<string> ReadBodyAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        try
        {
            await using var stream = await resp.Content.ReadAsStreamAsync(ct);
            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(4096);
            try
            {
                int filled = 0;
                while (filled < 4096)
                {
                    int n = await stream.ReadAsync(rented.AsMemory(filled, 4096 - filled), ct);
                    if (n == 0) break;
                    filled += n;
                }
                if (filled == 0) return "(empty body)";
                var body = System.Text.Encoding.UTF8.GetString(rented, 0, filled).ReplaceLineEndings(" ").Trim();
                return body.Length == 0 ? "(empty body)" : body.Length <= 500 ? body : body[..500];
            }
            finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
        }
        catch { return "(unreadable body)"; }
    }

    public void Dispose() => _http.Dispose();
}
