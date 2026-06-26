using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;

namespace Ameto.Tracing;

public static class TracingServiceExtensions
{
    /// <summary>
    /// Registers all distributed-tracing services: ring buffer, drainer, storage engine,
    /// and the <see cref="ISpanIngester"/> / <see cref="ITraceProvider"/> singletons.
    /// </summary>
    public static IServiceCollection AddAmetoTracing(
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
        services.AddSingleton<ITraceStatsProvider>(sp => sp.GetRequiredService<TraceStorageEngine>());
        services.AddSingleton<IRetentionTarget>(sp => sp.GetRequiredService<TraceStorageEngine>());

        services.AddSingleton<SpanDrainer>();
        services.AddHostedService<SpanDrainerService>();
        services.AddHostedService<TraceCompactionWorker>();

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

internal sealed class TraceCompactionWorker(TraceStorageEngine engine, ILogger<TraceCompactionWorker> logger)
    : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromHours(1);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // First run after a short delay so startup I/O settles
        await Task.Delay(TimeSpan.FromMinutes(5), ct).ConfigureAwait(false);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                engine.CompactSmallSegments();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "TraceCompactionWorker: unexpected error");
            }

            await Task.Delay(Interval, ct).ConfigureAwait(false);
        }
    }
}
