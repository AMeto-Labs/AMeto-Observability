using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Replication;

// ── Hosted wiring service ─────────────────────────────────────────────────────

/// <summary>
/// Sets local NodeId on <see cref="PeerProber"/> and <see cref="SegmentReplicator"/>,
/// registers the local node in the registry, and hooks <see cref="StorageEngine.SegmentFlushed"/>.
/// </summary>
internal sealed class ReplicationWiring : IHostedService
{
    private readonly ReplicationOptions  _opts;
    private readonly NodeId              _localId;
    private readonly NodeRegistry        _registry;
    private readonly PeerProber          _prober;
    private readonly SegmentReplicator   _replicator;
    private readonly StorageEngine       _storage;

    public ReplicationWiring(
        IOptions<ReplicationOptions> opts,
        IOptions<ServerOptions>      serverOpts,
        NodeRegistry                 registry,
        PeerProber                   prober,
        SegmentReplicator            replicator,
        StorageEngine                storage)
    {
        _opts       = opts.Value;
        _localId    = serverOpts.Value.NodeId;
        _registry   = registry;
        _prober     = prober;
        _replicator = replicator;
        _storage    = storage;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_opts.Enabled) return Task.CompletedTask;

        _registry.EnsureKnown(_localId, _opts.LocalAddress);
        _prober.SetLocalNodeId(_localId);
        _replicator.SetLocalNodeId(_localId);
        _storage.SegmentFlushed = _replicator.OnSegmentFlushed;
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _storage.SegmentFlushed = null;
        return Task.CompletedTask;
    }
}

// ── DI registration ───────────────────────────────────────────────────────────

public static class ReplicationServiceExtensions
{
    /// <summary>
    /// Registers all replication services.
    /// Call after <c>AddAmetoStorage()</c> and before <c>Build()</c>.
    /// </summary>
    public static IServiceCollection AddAmetoReplication(
        this IServiceCollection services,
        ReplicationOptions      opts)
    {
        services.AddSingleton(Options.Create(opts));

        if (!opts.Enabled) return services;

        services.AddHttpClient("replication");
        services.AddHttpClient("replication-push");

        services.AddSingleton<NodeRegistry>();
        services.AddSingleton<PeerProber>();
        services.AddHostedService(static sp => sp.GetRequiredService<PeerProber>());
        services.AddSingleton<SegmentReplicator>();
        services.AddHostedService<ReplicationWiring>();

        return services;
    }
}
