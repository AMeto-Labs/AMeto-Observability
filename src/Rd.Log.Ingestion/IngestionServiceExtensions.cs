using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rd.Log.Core;
using Rd.Log.Storage;

namespace Rd.Log.Ingestion;

public static class IngestionServiceExtensions
{
    /// <summary>
    /// Registers ingestion services: ring buffer, drainer, and endpoint.
    /// Must be called after <c>AddRdLogStorage</c>.
    /// </summary>
    public static IServiceCollection AddRdLogIngestion(this IServiceCollection services)
    {
        // Ring buffer — singleton shared between endpoint (producers) and drainer (consumer)
        services.AddSingleton<IngestionRingBuffer>();

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
