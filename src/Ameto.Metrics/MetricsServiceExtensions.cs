using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Metrics.Storage;

namespace Ameto.Metrics;

public static class MetricsServiceExtensions
{
    /// <summary>
    /// Registers metric storage, ingestion, and query services.
    /// </summary>
    public static IServiceCollection AddAmetoMetrics(
        this IServiceCollection services,
        string dataDirectory)
    {
        // RegisterForMemoryPressure HERE and not in the constructor: registration is a
        // process-wide effect, so it belongs to the composition that means it, not to every
        // engine a test builds. Until this, the only shedder in the process was the
        // segment-index cache — the RAM pressure loop could flush the LOG tier and drop cached
        // indexes while the metric tier, which is the larger of the two on a metrics-heavy
        // deployment, sat there holding everything it had.
        services.AddSingleton(sp =>
            new MetricStorageEngine(
                Path.Combine(dataDirectory, "metrics"),
                sp.GetRequiredService<ILogger<MetricStorageEngine>>())
            .RegisterForMemoryPressure());

        services.AddSingleton<IMetricIngester>(sp => sp.GetRequiredService<MetricStorageEngine>());
        services.AddSingleton<IMetricQuery>(sp => sp.GetRequiredService<MetricStorageEngine>());
        services.AddSingleton<IMetricCatalog>(sp => sp.GetRequiredService<MetricStorageEngine>());
        services.AddSingleton<IMetricExemplars>(sp => sp.GetRequiredService<MetricStorageEngine>());
        services.AddSingleton<IRetentionTarget>(sp => sp.GetRequiredService<MetricStorageEngine>());

        services.AddSingleton<IMetricAggregator, MetricAggregator>();

        services.AddHostedService<MetricStorageHostedService>();

        return services;
    }
}

internal sealed class MetricStorageHostedService : IHostedService, IAsyncDisposable
{
    private readonly MetricStorageEngine _engine;

    public MetricStorageHostedService(MetricStorageEngine engine) => _engine = engine;

    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken ct) => await _engine.DisposeAsync();

    public async ValueTask DisposeAsync() => await _engine.DisposeAsync();
}
