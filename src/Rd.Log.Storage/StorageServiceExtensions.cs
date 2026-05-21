using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rd.Log.Core;

namespace Rd.Log.Storage;

/// <summary>
/// Background service that periodically enforces the retention policy.
/// Runs every hour; deletes cold-tier segments whose max timestamp is past their TTL.
/// </summary>
public sealed class RetentionBackgroundService : BackgroundService
{
    private readonly StorageEngine            _storage;
    private readonly ILogger<RetentionBackgroundService> _logger;
    private static readonly TimeSpan          _interval = TimeSpan.FromHours(1);

    public RetentionBackgroundService(StorageEngine storage, ILogger<RetentionBackgroundService> logger)
    {
        _storage = storage;
        _logger  = logger;
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
    public static IServiceCollection AddRdLogStorage(this IServiceCollection services)
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
