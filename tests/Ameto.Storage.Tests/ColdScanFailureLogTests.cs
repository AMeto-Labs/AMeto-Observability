using Ameto.Core;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MsLogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// A cold scan that fails as a whole leaves the store answering from its hot tier alone, and the
/// alert evaluator read that partial answer as the truth — #95 by another road (#94). The store now
/// says it is <see cref="QueryAvailability.Degraded"/>, which the evaluator skips, and the engine
/// says it in the log: ONE Error per failed scan, naming what it means for alert rules. Each fact
/// fails its engine's scan through a seam, since a real disk produces this only by failing to list
/// a directory.
/// </summary>
public sealed class ColdScanFailureLogTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-scanfail-" + Guid.NewGuid().ToString("N"));

    public ColdScanFailureLogTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public async Task A_failed_metric_cold_scan_leaves_the_store_degraded_and_says_so_once()
    {
        var log  = new Entries<MetricStorageEngine>();
        var fail = new IOException("the data directory cannot be listed");

        MetricStorageEngine engine;
        MetricStorageEngine.FailColdScanForTest.Value = fail;
        try { engine = new MetricStorageEngine(_dir, log); }
        finally { MetricStorageEngine.FailColdScanForTest.Value = null; }

        await using (engine)
        {
            await engine.ColdLoadCompleted;   // a failed scan still ends the load: nobody waits for ever
            Assert.Equal(QueryAvailability.Degraded, engine.Availability);

            var error = Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
            Assert.Same(fail, error.Error);
            Assert.Contains("cold metric segment scan", error.Message);
            Assert.Contains(_dir, error.Message);
            Assert.Contains("reports itself Degraded", error.Message);
            Assert.Contains("ALERT RULES are not evaluated on that partial data", error.Message);
        }
    }

    /// <summary>The control: a scan that succeeds leaves the store Available, with no Error.</summary>
    [Fact]
    public async Task A_metric_cold_scan_that_succeeds_leaves_the_store_available()
    {
        var log = new Entries<MetricStorageEngine>();
        await using var engine = new MetricStorageEngine(_dir, log);
        await engine.ColdLoadCompleted;

        Assert.Equal(QueryAvailability.Available, engine.Availability);
        Assert.DoesNotContain(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
    }

    [Fact]
    public async Task A_failed_log_catalog_scan_says_once_that_alert_rules_are_not_evaluated_on_partial_data()
    {
        var log  = new Entries<StorageEngine>();
        var opts = new ServerOptions { DataDirectory = _dir };
        await using var engine = new StorageEngine(Options.Create(opts), new RetentionStore(opts, NullLogger<RetentionStore>.Instance), log);
        await engine.CatalogLoaded;   // the constructor's scan, clean
        Assert.DoesNotContain(log.Snapshot(), e => e.Level >= MsLogLevel.Error);

        // A scan run the way the constructor runs it — the same method — failing as a whole.
        var fail = new IOException("the segments directory cannot be listed");
        engine._beforeCatalogScanForTest = () => throw fail;
        Assert.Same(fail, Assert.Throws<IOException>(engine.LoadSegmentCatalog));   // still faults its task

        var error = Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
        Assert.Same(fail, error.Error);
        Assert.Contains("log segment catalog scan", error.Message);
        Assert.Contains(Path.Combine(_dir, "segments"), error.Message);
        Assert.Contains("ALERT RULES are not evaluated on that partial data", error.Message);
    }

    /// <summary>
    /// THE LOG STORE'S DEGRADED IS ITS BOOT SCAN'S TASK (#94): the constructor's catalog scan, held
    /// until the seam that fails it is in place, faults — and the store that was Loading reads
    /// Degraded from then on, not Available. Before, a faulted scan ended Loading in Available.
    /// </summary>
    [Fact]
    public async Task A_log_store_whose_boot_catalog_scan_faults_is_degraded()
    {
        var log  = new Entries<StorageEngine>();
        var opts = new ServerOptions { DataDirectory = _dir };
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var engine = new StorageEngine(
            Options.Create(opts), new RetentionStore(opts, NullLogger<RetentionStore>.Instance), log,
            maintenanceStartDelay: TimeSpan.FromHours(1), bootScanHeldUntil: hold.Task);

        Assert.Equal(QueryAvailability.Loading, engine.Availability);
        var fail = new IOException("the segments directory cannot be listed");
        engine._beforeCatalogScanForTest = () => throw fail;
        hold.SetResult();

        Assert.Same(fail, await Assert.ThrowsAsync<IOException>(() => engine.CatalogLoaded));
        Assert.Equal(QueryAvailability.Degraded, engine.Availability);
        Assert.Same(fail, Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error).Error);
    }

    /// <summary>Every entry at every level, formatted. Invariant-safe: the messages carry paths and words only.</summary>
    private sealed class Entries<T> : ILogger<T>
    {
        private readonly List<(MsLogLevel Level, string Message, Exception? Error)> _entries = [];

        public List<(MsLogLevel Level, string Message, Exception? Error)> Snapshot() { lock (_entries) return [.. _entries]; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(MsLogLevel level) => true;

        public void Log<TState>(MsLogLevel level, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? error,
                                Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((level, formatter(state, error), error));
        }
    }
}
