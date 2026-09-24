using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Ameto.Alerts;

/// <summary>
/// Hosted wrapper that owns the periodic <see cref="AlertEvaluator"/> lifecycle
/// (its eval loop starts in the constructor and stops on disposal).
///
/// <para>It also stops the evaluator ACTING the moment the host begins to stop —
/// <see cref="IHostApplicationLifetime.ApplicationStopping"/> fires before the first hosted
/// service's <c>StopAsync</c>, so before any engine the evaluator reads has begun to close. The
/// registration order in Program.cs already stops this service first; this is what still holds
/// if a host composes the services in another order, or if a second stopper overtakes the first.</para>
/// </summary>
internal sealed class AlertsHostedService : IHostedService, IAsyncDisposable
{
    private readonly AlertEvaluator _evaluator;
    private readonly IHostApplicationLifetime _lifetime;
    private CancellationTokenRegistration _onStopping;

    public AlertsHostedService(AlertEvaluator evaluator, IHostApplicationLifetime lifetime)
    {
        _evaluator = evaluator;
        _lifetime  = lifetime;
    }

    public Task StartAsync(CancellationToken ct)
    {
        _onStopping = _lifetime.ApplicationStopping.Register(
            static s => ((AlertEvaluator)s!).StopEvaluating(), _evaluator);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken ct)
    {
        _onStopping.Dispose();
        await _evaluator.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _onStopping.Dispose();
        await _evaluator.DisposeAsync();
    }
}

/// <summary>
/// DI extension for registering alert services. Must be called after the log/metric/trace
/// query services are registered (the evaluator consumes IQueryExecutor / IMetricAggregator /
/// ITraceStatsProvider) — and AFTER the hosted services that tear those engines down, because
/// hosted services stop in reverse registration order and a closed engine answers the evaluator
/// with an empty result, which reads as a value of 0 and resolves every firing "&gt;" rule.
/// </summary>
public static class AlertsServiceExtensions
{
    public static IServiceCollection AddAmetoAlerts(
        this IServiceCollection services,
        string dataDirectory)
    {
        services.AddSingleton<AlertRuleStore>(sp =>
            new AlertRuleStore(
                dataDirectory,
                sp.GetRequiredService<Ameto.Core.ISecretProtector>(),
                sp.GetRequiredService<ILogger<AlertRuleStore>>()));

        services.AddSingleton<AlertPersistence>(sp =>
            new AlertPersistence(dataDirectory, sp.GetRequiredService<ILogger<AlertPersistence>>()));

        services.AddSingleton<AlertDispatcher>();
        services.AddSingleton<AlertEvaluator>();
        services.AddHostedService<AlertsHostedService>();

        return services;
    }
}
