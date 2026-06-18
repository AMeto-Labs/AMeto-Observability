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
        services.AddSingleton(sp =>
            new MetricStorageEngine(
                Path.Combine(dataDirectory, "metrics"),
                sp.GetRequiredService<ILogger<MetricStorageEngine>>()));

        services.AddSingleton<IMetricIngester>(sp => sp.GetRequiredService<MetricStorageEngine>());
        services.AddSingleton<IMetricQuery>(sp => sp.GetRequiredService<MetricStorageEngine>());
        services.AddSingleton<IRetentionTarget>(sp => sp.GetRequiredService<MetricStorageEngine>());

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
