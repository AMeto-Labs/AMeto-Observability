using System.Net.Http.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Replication;

/// <summary>
/// Periodically probes all seed nodes and previously discovered peers.
/// Each probe announces this node's presence (POST /api/replication/ping) and
/// expects the remote node's own <see cref="PeerPayload"/> in response.
/// Both sides update their <see cref="NodeRegistry"/> on every successful exchange,
/// keeping <see cref="ReplicationNode.LastSeen"/> fresh for liveness detection.
///
/// <para>Replication is enabled by default with an empty seed list, which is the shipped
/// single-node configuration, so this loop used to wake every 10 seconds forever to build a
/// payload and a LINQ chain over one entry — itself — and await <c>Task.WhenAll</c> of nothing.
/// It now parks on <see cref="NodeRegistry.PeerAdded"/> when there is nothing to probe: with no
/// seeds, an inbound ping is the only way a peer can appear, so nothing is missed and the timer
/// does not exist while this node is alone.</para>
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

    /// <summary>Pulsed when a peer is discovered, so a parked loop can resume immediately.</summary>
    private readonly SemaphoreSlim _wake = new(0, 1);

    /// <summary>Probe cycles run. Zero after start on a node with no seeds and no peers.</summary>
    public int ProbeCycles => Volatile.Read(ref _cycles);
    private int _cycles;

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

        _registry.PeerAdded += OnPeerAdded;
        _cts  = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loop = RunAsync(_cts.Token);
        return Task.CompletedTask;
    }

    /// <summary>
    /// A peer appeared while we were parked. One pending pulse is enough — the loop re-reads
    /// the registry when it wakes, so a burst of discoveries is one wake-up, not N.
    /// </summary>
    private void OnPeerAdded()
    {
        if (_wake.CurrentCount == 0)
        {
            try { _wake.Release(); } catch (SemaphoreFullException) { /* raced; already pulsed */ }
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _registry.PeerAdded -= OnPeerAdded;
        if (_cts is null) return;
        await _cts.CancelAsync();
        if (_loop is not null)
            await Task.WhenAny(_loop, Task.Delay(3_000, cancellationToken));
    }

    private async Task RunAsync(CancellationToken ct)
    {
        // Created only while there is something to probe, and disposed again when there is
        // not: a PeriodicTimer that exists is a wake-up every ProbeInterval whether or not
        // the loop does anything with it.
        PeriodicTimer? timer = null;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (!HasSomethingToProbe)
                {
                    timer?.Dispose();
                    timer = null;
                    // No seeds and no peers: an inbound ping (NodeRegistry.Upsert) is the only
                    // event that can change that, and it pulses us. Until then, no wakes.
                    await _wake.WaitAsync(ct);
                    continue;
                }

                // Seeds are probed on the first pass so the registry is populated before the
                // first SegmentReplicator flush, then re-probed on the interval.
                await ProbeAllAsync(ct);
                Interlocked.Increment(ref _cycles);

                timer ??= new PeriodicTimer(_opts.ProbeInterval);
                if (!await timer.WaitForNextTickAsync(ct)) break;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PeerProber loop failed");
        }
        finally
        {
            timer?.Dispose();
        }
    }

    /// <summary>
    /// Static seeds, or any peer besides ourselves. Checked before anything is built, because
    /// the answer on a single node is "no" for the life of the process.
    /// </summary>
    private bool HasSomethingToProbe =>
        _opts.SeedNodes.Length > 0 || _registry.HasPeerOtherThan(_localId);

    private Task ProbeAllAsync(CancellationToken ct)
    {
        // Second guard: a peer may have been removed between the check and here, and an empty
        // cycle must not cost a payload, four LINQ operators and a Task.WhenAll over nothing.
        if (!HasSomethingToProbe) return Task.CompletedTask;

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

            // Trimmed at the boundary: ReplicationNode.BaseAddress and ReplicationOptions.SeedNodes.
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{baseAddress}/api/replication/ping")
            {
                Content = System.Net.Http.Json.JsonContent.Create(payload),
            };
            req.Headers.Add("X-Ameto-Replication", _opts.Secret);
            var resp = await _http.SendAsync(req, cts.Token);

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

    public void Dispose()
    {
        _registry.PeerAdded -= OnPeerAdded;
        _wake.Dispose();
        _http.Dispose();
    }
}
