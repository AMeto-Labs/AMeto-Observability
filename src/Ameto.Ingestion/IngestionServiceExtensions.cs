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
            new IngestionRingBuffer(
                maxPayloadBytesPerSlot: sp.GetRequiredService<ServerOptions>().Ingestion.MaxEventPayloadBytes));

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
