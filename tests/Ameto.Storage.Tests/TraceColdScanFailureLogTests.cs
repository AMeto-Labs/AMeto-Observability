using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging;

namespace Ameto.Storage.Tests;

/// <summary>
/// A TRACE COLD SCAN THAT FAILS AS A WHOLE SAYS, ONCE, THAT ALERT RULES NOW RUN ON PARTIAL DATA
/// (#94) — the trace side of what the metric and log engines log for theirs. The scan used to throw
/// out to the compaction worker, which logged "cold-segment load failed": nothing about the
/// segments staying unserved until a restart, and nothing about the alert evaluator reading the
/// missing window as a quiet one.
///
/// <para>At the seam: the scan throws before the directory is listed. Reverted (no catch in the
/// engine): <c>LoadColdSegments</c> throws, and no Error of this shape is logged.</para>
/// </summary>
public sealed class TraceColdScanFailureLogTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-coldscan-" + Guid.NewGuid().ToString("N"));

    public TraceColdScanFailureLogTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public void A_failed_cold_scan_logs_one_error_naming_the_alert_rules_and_marks_the_tier_short()
    {
        var logger = new CapturingLogger();
        using var e = new TraceStorageEngine(_dir, logger);
        var fault = new IOException("injected: the data directory would not list");
        e._failColdScanForTest = fault;

        e.LoadColdSegments();                                            // does not throw

        var errors = logger.Entries.Where(static x => x.Level == LogLevel.Error).ToList();
        var line   = Assert.Single(errors);
        Assert.Same(fault, line.Error);
        Assert.Contains(_dir, line.Message, StringComparison.Ordinal);
        Assert.Contains("Trace ALERT RULES keep being evaluated on that partial data", line.Message, StringComparison.Ordinal);
        Assert.True(e.ColdTierIncompleteForTest);                        // reads now report the window short
    }

    [Fact]
    public void A_scan_that_succeeds_logs_no_error()
    {
        var logger = new CapturingLogger();
        using var e = new TraceStorageEngine(_dir, logger);
        e.LoadColdSegments();
        Assert.DoesNotContain(logger.Entries, static x => x.Level == LogLevel.Error);
        Assert.False(e.ColdTierIncompleteForTest);
    }

    private sealed class CapturingLogger : ILogger<TraceStorageEngine>
    {
        private readonly List<(LogLevel Level, string Message, Exception? Error)> _entries = [];

        public IReadOnlyList<(LogLevel Level, string Message, Exception? Error)> Entries
        {
            get { lock (_entries) return [.. _entries]; }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => true;

        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? error,
                                Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((level, formatter(state, error), error));
        }
    }
}
