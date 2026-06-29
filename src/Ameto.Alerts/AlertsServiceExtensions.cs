using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Ameto.Alerts;

/// <summary>
/// Hosted wrapper that owns the periodic <see cref="AlertEvaluator"/> lifecycle
/// (its eval loop starts in the constructor and stops on disposal).
/// </summary>
internal sealed class AlertsHostedService : IHostedService, IAsyncDisposable
{
    private readonly AlertEvaluator _evaluator;
    public AlertsHostedService(AlertEvaluator evaluator) => _evaluator = evaluator;
    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
    public async Task StopAsync(CancellationToken ct) => await _evaluator.DisposeAsync();
    public async ValueTask DisposeAsync() => await _evaluator.DisposeAsync();
}

/// <summary>
/// DI extension for registering alert services. Must be called after the log/metric/trace
/// query services are registered (the evaluator consumes IQueryExecutor / IMetricAggregator /
/// ITraceStatsProvider).
/// </summary>
public static class AlertsServiceExtensions
{
    public static IServiceCollection AddAmetoAlerts(
        this IServiceCollection services,
        string dataDirectory)
    {
        services.AddSingleton<AlertRuleStore>(sp =>
            new AlertRuleStore(dataDirectory, sp.GetRequiredService<ILogger<AlertRuleStore>>()));

        services.AddSingleton<AlertPersistence>(sp =>
            new AlertPersistence(dataDirectory, sp.GetRequiredService<ILogger<AlertPersistence>>()));

        services.AddSingleton<AlertDispatcher>();
        services.AddSingleton<AlertEvaluator>();
        services.AddHostedService<AlertsHostedService>();

        return services;
    }
}
