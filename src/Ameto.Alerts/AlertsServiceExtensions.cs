using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Alerts;

/// <summary>
/// Hosted service that wires <see cref="AlertEvaluator"/> into
/// <see cref="StorageEngine.EventWritten"/> at application startup.
/// </summary>
internal sealed class AlertsWiring : IHostedService
{
    private readonly AlertEvaluator _evaluator;
    private readonly StorageEngine  _engine;

    public AlertsWiring(AlertEvaluator evaluator, StorageEngine engine)
    {
        _evaluator = evaluator;
        _engine    = engine;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _evaluator.Attach(_engine);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>
/// DI extension for registering alert services.
/// Must be called after <c>AddAmetoStorage</c>.
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
                sp.GetRequiredService<ILogger<AlertRuleStore>>()));

        services.AddSingleton<AlertDispatcher>();
        services.AddSingleton<AlertEvaluator>();
        services.AddHostedService<AlertsWiring>();

        return services;
    }
}

// ── Request DTO ────────────────────────────────────────────────

public sealed class AlertRuleUpsertRequest
{
    public string?                  Id              { get; init; }
    public string?                  Name            { get; init; }
    public string?                  Filter          { get; init; }
    public int                      Threshold       { get; init; } = 1;
    public int                      WindowSeconds   { get; init; } = 300;
    public int                      CooldownSeconds { get; init; } = 900;
    public bool                     Enabled         { get; init; } = true;
    public IReadOnlyList<AlertChannel>? Channels    { get; init; }
}
