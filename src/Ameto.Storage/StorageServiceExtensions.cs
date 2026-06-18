using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Background service that periodically enforces the retention policy.
/// Runs every hour; deletes cold-tier segments whose max timestamp is past their TTL.
/// </summary>
public sealed class RetentionBackgroundService : BackgroundService
{
    private readonly StorageEngine                        _storage;
    private readonly RetentionStore                       _retentionStore;
    private readonly IEnumerable<IRetentionTarget>        _targets;
    private readonly ILogger<RetentionBackgroundService>  _logger;
    private static readonly TimeSpan                      _interval = TimeSpan.FromHours(1);

    public RetentionBackgroundService(
        StorageEngine storage,
        RetentionStore retentionStore,
        IEnumerable<IRetentionTarget> targets,
        ILogger<RetentionBackgroundService> logger)
    {
        _storage        = storage;
        _retentionStore = retentionStore;
        _targets        = targets;
        _logger         = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Initial run shortly after startup
        await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = await _storage.EnforceRetentionAsync(stoppingToken);
                if (result.DeletedSegments > 0)
                    _logger.LogInformation(
                        "Retention run: deleted {Count} segments, freed {Bytes:N0} bytes",
                        result.DeletedSegments, result.FreedBytes);

                var dto = _retentionStore.Get();
                foreach (var target in _targets)
                {
                    var days = target.RetentionKey switch
                    {
                        "metrics" => dto.MetricsDays,
                        "traces"  => dto.TracesDays,
                        _         => 30,
                    };
                    var pruned = await target.PruneAsync(TimeSpan.FromDays(Math.Max(1, days)), stoppingToken);
                    if (pruned > 0)
                        _logger.LogInformation(
                            "Retention run: pruned {Count} {Key} file(s)", pruned, target.RetentionKey);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Retention enforcement failed");
            }

            await Task.Delay(_interval, stoppingToken);
        }
    }
}

/// <summary>
/// DI extension for registering storage services.
/// </summary>
public static class StorageServiceExtensions
{
    public static IServiceCollection AddAmetoStorage(this IServiceCollection services)
    {
        services.AddSingleton<RetentionStore>();
        services.AddSingleton<StorageEngine>(); 
        services.AddSingleton<ISegmentProvider>(sp => sp.GetRequiredService<StorageEngine>());
        services.AddSingleton<ISegmentManager>(sp => sp.GetRequiredService<StorageEngine>());
        // Expose the engine's shared intern pool so Ingestion can inject it directly
        services.AddSingleton(sp => sp.GetRequiredService<StorageEngine>().TemplatePool);
        services.AddHostedService<RetentionBackgroundService>();
        services.AddHostedService<RamPressureService>();
        return services;
    }
}
