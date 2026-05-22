using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Rd.Log.Tracing.Ingestion;
using Rd.Log.Tracing.Storage;

namespace Rd.Log.Tracing;

public static class TracingServiceExtensions
{
    /// <summary>
    /// Registers all distributed-tracing services: ring buffer, drainer, storage engine,
    /// and the <see cref="ISpanIngester"/> / <see cref="ITraceProvider"/> singletons.
    /// </summary>
    public static IServiceCollection AddRdLogTracing(
        this IServiceCollection services,
        string dataDirectory)
    {
        services.AddSingleton(sp =>
            new TraceStorageEngine(
                Path.Combine(dataDirectory, "traces"),
                sp.GetRequiredService<ILogger<TraceStorageEngine>>()));

        services.AddSingleton<SpanRingBuffer>();
        services.AddSingleton<SpanIngestionEndpoint>();
        services.AddSingleton<ISpanIngester>(sp => sp.GetRequiredService<SpanIngestionEndpoint>());
        services.AddSingleton<ITraceProvider>(sp => sp.GetRequiredService<TraceStorageEngine>());

        services.AddSingleton<SpanDrainer>();
        services.AddHostedService<SpanDrainerService>();

        return services;
    }
}

internal sealed class SpanDrainerService : IHostedService, IAsyncDisposable
{
    private readonly SpanDrainer _drainer;
    public SpanDrainerService(SpanDrainer d) => _drainer = d;
    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
    public async Task StopAsync(CancellationToken ct) => await _drainer.DisposeAsync();
    public async ValueTask DisposeAsync() => await _drainer.DisposeAsync();
}
