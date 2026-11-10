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
                    var pruned = await target.PruneAsync(RetentionMath.TtlOf(days), stoppingToken);
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
/// Keeps <c>counts.db</c> whole: once the boot catalog scan has finished, after a minute's
/// settling, then every ten minutes, one backfill pass gives every cold segment its counts
/// summary — once per segment, ever, since the store is on disk — and clears the rows of segments
/// that have gone. A pass over a store that is up to date is one read of the fingerprints; the
/// first pass after an upgrade summarises the older segments at a quarter of one thread.
///
/// <para>It waits for the catalog for the reason the engine's maintenance loop does: until the
/// boot scan ends the catalog is a partial view, and a pass over it would leave most segments for
/// the next one (the engine also refuses to sweep against it). A scan that faults counts as ended,
/// as there it does.</para>
///
/// <para>Guarded like the engine's own loops (#137): a failed pass, or a log line that throws, costs
/// that pass, never the service, and never stops the host.</para>
/// </summary>
public sealed class CountsSummaryBackfillService : BackgroundService
{
    private readonly StorageEngine                         _storage;
    private readonly ILogger<CountsSummaryBackfillService> _logger;
    private readonly TimeSpan                              _firstPassDelay;
    private readonly TimeSpan                              _interval;

    public CountsSummaryBackfillService(StorageEngine storage, ILogger<CountsSummaryBackfillService> logger)
        : this(storage, logger, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10)) { }

    /// <summary>For tests: the same service on a shorter clock.</summary>
    internal CountsSummaryBackfillService(StorageEngine storage, ILogger<CountsSummaryBackfillService> logger,
                                          TimeSpan firstPassDelay, TimeSpan interval)
    {
        _storage        = storage;
        _logger         = logger;
        _firstPassDelay = firstPassDelay;
        _interval       = interval;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try { await _storage.CatalogLoaded.WaitAsync(stoppingToken); }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
        catch (Exception) { /* a faulted scan has ended; the engine says why, and sweeps nothing after it */ }

        try { await Task.Delay(_firstPassDelay, stoppingToken); }
        catch (OperationCanceledException) { return; }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var r = await _storage.BackfillCountsOnceAsync(stoppingToken);
                // Every pass copies the sections of the segments flushed and merged since the last
                // one, so that alone is Debug; a scan, a failure or a stopped pass is worth reading.
                var level = r.Scanned > 0 || r.Failed > 0 || r.StoppedOnStoreFailure
                    ? Microsoft.Extensions.Logging.LogLevel.Information
                    : Microsoft.Extensions.Logging.LogLevel.Debug;
                if (r.Written > 0 || r.Swept > 0 || r.Failed > 0 || r.StoppedOnStoreFailure)
                    Say(level,
                        "Counts summaries: {Written} segment(s) summarised ({FromFiles} from their files, {Scanned} by a scan, {None} with none possible), {Swept} stale row(s) cleared, {Failed} not readable now{Stopped}; {Stored} of {Segments} segments have theirs",
                        r.Written, r.FromFileSections, r.Scanned, r.Unsummarisable, r.Swept, r.Failed,
                        r.StoppedOnStoreFailure ? ", pass stopped: the store cannot be read or written" : "",
                        r.AlreadyStored + r.Written, r.Segments);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                try { _logger.LogError(ex, "Counts summaries backfill pass failed"); }
                catch (Exception) { /* the line is lost; the service is not */ }
            }

            try { await Task.Delay(_interval, stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    private void Say(Microsoft.Extensions.Logging.LogLevel level, string message, params object[] args)
    {
        try { _logger.Log(level, message, args); }
        catch (Exception) { /* the line is lost; the service is not */ }
    }
}

/// <summary>
/// DI extension for registering storage services.
/// </summary>
public static class StorageServiceExtensions
{
    /// <summary>How long the server's engine lets its despill wait for the index sink at startup (see <c>StorageEngine._indexSinkWait</c>).</summary>
    internal static readonly TimeSpan ServerIndexSinkWait = TimeSpan.FromSeconds(30);

    public static IServiceCollection AddAmetoStorage(this IServiceCollection services)
    {
        services.AddSingleton<RetentionStore>();
        // The server's engine: its despill waits (bounded) for the indexing layer to wire the index
        // sink, which happens only once the hosted services start — see StorageEngine._indexSinkWait.
        services.AddSingleton(static sp => new StorageEngine(
            sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ServerOptions>>(),
            sp.GetRequiredService<RetentionStore>(),
            sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<StorageEngine>>(),
            MemoryBudgets.Current(),
            indexSinkWait: ServerIndexSinkWait));
        services.AddSingleton<ISegmentProvider>(sp => sp.GetRequiredService<StorageEngine>());
        services.AddSingleton<ISegmentManager>(sp => sp.GetRequiredService<StorageEngine>());
        // Expose the engine's shared intern pool so Ingestion can inject it directly
        services.AddSingleton(sp => sp.GetRequiredService<StorageEngine>().TemplatePool);
        services.AddHostedService<RetentionBackgroundService>();
        services.AddHostedService<CountsSummaryBackfillService>();
        // Shared so the 30 s attribution line and /api/diagnostics measure the same interval
        // — a CPU percentage only exists between two reads of the process total.
        services.AddSingleton<ProcessCpuSampler>();
        services.AddHostedService<RamPressureService>();
        return services;
    }
}
