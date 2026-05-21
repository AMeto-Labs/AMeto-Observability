using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rd.Log.Core;

namespace Rd.Log.Storage;

/// <summary>
/// Monitors system-wide RAM utilisation and flushes the hot tier when the
/// configured <see cref="ServerOptions.RamTargetPercent"/> threshold is exceeded.
///
/// Behaviour:
/// - Polls every <see cref="_checkInterval"/> (30 s by default).
/// - Uses <see cref="GC.GetGCMemoryInfo"/> to obtain the OS-level memory load
///   without taking a P/Invoke dependency.
/// - After a flush the service backs off for <see cref="_cooldown"/> (5 min)
///   to avoid continuous thrashing when pressure is sustained.
/// </summary>
public sealed class RamPressureService : BackgroundService
{
    private readonly StorageEngine                  _storage;
    private readonly ServerOptions                  _options;
    private readonly ILogger<RamPressureService>    _logger;

    private static readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan _cooldown       = TimeSpan.FromMinutes(5);

    public RamPressureService(
        StorageEngine               storage,
        ServerOptions               options,
        ILogger<RamPressureService> logger)
    {
        _storage = storage;
        _options = options;
        _logger  = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Give the process a moment to settle before we start monitoring.
        await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);

        DateTimeOffset lastFlush = DateTimeOffset.MinValue;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                int pct = GetSystemRamPercent();

                if (pct > _options.RamTargetPercent)
                {
                    if (DateTimeOffset.UtcNow - lastFlush >= _cooldown)
                    {
                        _logger.LogWarning(
                            "RAM pressure: system memory at {Pct}% (target {Target}%). " +
                            "Flushing hot tier to release memory.",
                            pct, _options.RamTargetPercent);

                        await _storage.FlushHotTierAsync(stoppingToken);

                        // Ask the GC to reclaim any freed managed memory.
                        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
                        // Ask the OS to release resident pages we no longer need.
                        WorkingSetTrimmer.TryTrim();

                        lastFlush = DateTimeOffset.UtcNow;
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RAM pressure check failed");
            }

            await Task.Delay(_checkInterval, stoppingToken);
        }
    }

    /// <summary>
    /// Returns the current system-wide physical memory utilisation as a
    /// percentage (0–100).
    /// </summary>
    public static int GetSystemRamPercent()
    {
        var info  = GC.GetGCMemoryInfo();
        long total = info.TotalAvailableMemoryBytes;
        if (total <= 0) return 0;
        long used = info.MemoryLoadBytes;
        return (int)Math.Round(used * 100.0 / total);
    }
}
