using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Ingestion;

public static class IngestionServiceExtensions
{
    /// <summary>
    /// Registers ingestion services: ring buffer, drainer, and endpoint.
    /// Must be called after <c>AddAmetoStorage</c>.
    /// </summary>
    public static IServiceCollection AddAmetoIngestion(this IServiceCollection services)
    {
        // Ring buffer — singleton shared between endpoint (producers) and drainer (consumer).
        // Slab size (max properties bytes per event) is taken from config; a static lambda
        // keeps this closure allocation-free.
        services.AddSingleton<IngestionRingBuffer>(static sp =>
        {
            var ing = sp.GetRequiredService<ServerOptions>().Ingestion;
            // The ring requires a power-of-two capacity — round the configured value up.
            int cap = (int)System.Numerics.BitOperations.RoundUpToPowerOf2(
                (uint)Math.Clamp(ing.RingCapacity, 1024, 1 << 24));
            var ring = new IngestionRingBuffer(cap,
                maxPayloadBytesPerSlot: ing.MaxEventPayloadBytes,
                payloadPoolBytes:       ing.EffectivePayloadPoolBytes);

            // Once, since this is a singleton. The arena works either way; what is lost is the
            // per-4 KB-page residency under transparent_hugepage=always (see SlabArena).
            int thp = ring.ArenaHugePageOptOutErrno;
            if (OperatingSystem.IsLinux() && thp != 0)
                sp.GetService<ILogger<IngestionRingBuffer>>()?.LogInformation(
                    "The ingest payload arena could not opt out of transparent huge pages (madvise result {Result}). " +
                    "Where transparent_hugepage is 'always', a burst can make whole 2 MB ranges of it resident " +
                    "rather than the 4 KB pages it writes; set Ingestion.PayloadPoolBytes for a hard ceiling.",
                    thp);
            return ring;
        });

        // Endpoint — singleton, mapped as a route handler in Program.cs
        services.AddSingleton<IngestionEndpoint>();

        // Drainer — started as a hosted service
        services.AddSingleton<IngestionDrainer>();
        services.AddHostedService<IngestionDrainerService>();

        return services;
    }
}

/// <summary>Hosted service wrapper that starts/stops <see cref="IngestionDrainer"/>.</summary>
internal sealed class IngestionDrainerService : IHostedService, IAsyncDisposable
{
    private readonly IngestionDrainer _drainer;

    public IngestionDrainerService(IngestionDrainer drainer) => _drainer = drainer;

    // Drainer starts itself in its constructor — nothing to do here.
    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken ct) =>
        await _drainer.DisposeAsync();

    public async ValueTask DisposeAsync() =>
        await _drainer.DisposeAsync();
}
