using System.Net.Http.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Rd.Log.Core;

namespace Rd.Log.Replication;

/// <summary>
/// Periodically probes all seed nodes and previously discovered peers.
/// Each probe announces this node's presence (POST /api/replication/ping) and
/// expects the remote node's own <see cref="PeerPayload"/> in response.
/// Both sides update their <see cref="NodeRegistry"/> on every successful exchange,
/// keeping <see cref="ReplicationNode.LastSeen"/> fresh for liveness detection.
/// </summary>
public sealed class PeerProber : IHostedService, IDisposable
{
    private readonly ReplicationOptions   _opts;
    private readonly NodeRegistry         _registry;
    private readonly ILogger<PeerProber>  _logger;
    private readonly HttpClient           _http;
    private          NodeId               _localId;

    private CancellationTokenSource? _cts;
    private Task?                    _loop;

    public PeerProber(
        IOptions<ReplicationOptions> opts,
        NodeRegistry                 registry,
        ILogger<PeerProber>          logger,
        IHttpClientFactory           httpFactory)
    {
        _opts     = opts.Value;
        _registry = registry;
        _logger   = logger;
        _http     = httpFactory.CreateClient("replication");
    }

    internal void SetLocalNodeId(NodeId id) => _localId = id;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_opts.Enabled) return Task.CompletedTask;

        _cts  = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loop = RunAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cts is null) return;
        await _cts.CancelAsync();
        if (_loop is not null)
            await Task.WhenAny(_loop, Task.Delay(3_000, cancellationToken));
    }

    private async Task RunAsync(CancellationToken ct)
    {
        // Probe seeds immediately on startup so the registry is populated before
        // the first SegmentReplicator flush.
        await ProbeAllAsync(ct);

        using var timer = new PeriodicTimer(_opts.ProbeInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                // Re-probe seeds + any peers discovered via previous probes.
                await ProbeAllAsync(ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PeerProber loop failed");
        }
    }

    private Task ProbeAllAsync(CancellationToken ct)
    {
        var payload = BuildPayload();

        // Probe static seeds + all dynamically discovered peers.
        var addresses = _opts.SeedNodes
            .Concat(_registry.GetAll()
                .Where(n => n.Id.Value != _localId.Value)
                .Select(n => n.BaseAddress))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        return Task.WhenAll(addresses.Select(addr => ProbeOneAsync(addr, payload, ct)));
    }

    private async Task ProbeOneAsync(string baseAddress, PeerPayload payload, CancellationToken ct)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(5));

            var resp = await _http.PostAsJsonAsync(
                $"{baseAddress}/api/replication/ping", payload, cts.Token);

            if (!resp.IsSuccessStatusCode) return;

            var peer = await resp.Content.ReadFromJsonAsync<PeerPayload>(cts.Token);
            if (peer is not null)
                _registry.Upsert(peer);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug("Probe of {Addr} failed: {Msg}", baseAddress, ex.Message);
        }
    }

    private PeerPayload BuildPayload() => new()
    {
        NodeId    = _localId.Value,
        Address   = _opts.LocalAddress,
        Timestamp = DateTimeOffset.UtcNow,
    };

    public void Dispose() => _http.Dispose();
}
