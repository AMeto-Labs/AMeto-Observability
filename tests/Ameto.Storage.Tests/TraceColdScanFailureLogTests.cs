using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using QueryAvailability = Ameto.Core.QueryAvailability;

namespace Ameto.Storage.Tests;

/// <summary>
/// A TRACE COLD SCAN THAT FAILS AS A WHOLE SAYS, ONCE, WHAT IT LEFT BEHIND (#94) — the trace side of
/// what the metric and log engines log for theirs. The scan used to throw out to the compaction
/// worker, which logged "cold-segment load failed": nothing about the segments staying unserved until
/// a restart, and nothing about the alert evaluator reading the missing window as a quiet one. The
/// store now also says it is <see cref="QueryAvailability.Degraded"/>, which the evaluator skips.
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
    public void A_failed_cold_scan_logs_one_error_naming_the_alert_rules_and_leaves_the_store_degraded()
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
        Assert.Contains("trace store reports itself Degraded", line.Message, StringComparison.Ordinal);
        Assert.Contains("trace ALERT RULES are not evaluated on that partial data", line.Message, StringComparison.Ordinal);
        Assert.True(e.ColdTierIncompleteForTest);                        // reads now report the window short
        Assert.Equal(QueryAvailability.Degraded, e.Availability);        // and the evaluator skips the store
    }

    [Fact]
    public void A_scan_that_succeeds_logs_no_error()
    {
        var logger = new CapturingLogger();
        using var e = new TraceStorageEngine(_dir, logger);
        e.LoadColdSegments();
        Assert.DoesNotContain(logger.Entries, static x => x.Level == LogLevel.Error);
        Assert.False(e.ColdTierIncompleteForTest);
        Assert.Equal(QueryAvailability.Available, e.Availability);
    }

    /// <summary>
    /// A SEGMENT STILL HELD BY ANOTHER PROCESS AFTER THE SCAN'S RETRIES IS A LOAD LEFT UNFINISHED
    /// (#94): kept on disk, missing from this run's cold tier, one Error naming it — and the store is
    /// Degraded until the restart that reads it. Held here by a handle opened without sharing, which
    /// fails the scan's open on Windows (a sharing violation) and on Linux (.NET's advisory lock)
    /// alike, for as long as the test holds it; the scan's retries (~0.6 s) run out against it. The
    /// next start, with the handle gone, loads the segment and is Available.
    /// </summary>
    [Fact]
    public void A_segment_held_open_through_the_retries_is_kept_and_leaves_the_store_degraded_until_a_restart()
    {
        string trc;
        using (var writer = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance))
        {
            writer.LoadColdSegments();
            writer.WriteSpan(new SpanIngestItem
            {
                TraceId = new TraceId(0, 1), SpanId = new SpanId(1), ParentSpanId = default,
                StartTimeUnixNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L, DurationNanos = 1_000_000L,
                Name = "GET /orders", ServiceName = "billing", Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
            });
            writer.FlushHotTier();
            trc = Assert.Single(Directory.GetFiles(_dir, "*.trc"));
        }

        var logger = new CapturingLogger();
        using (new FileStream(trc, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        using (var held = new TraceStorageEngine(_dir, logger))
        {
            held.LoadColdSegments();

            Assert.True(File.Exists(trc), "a segment the scan could not open was deleted");
            Assert.Equal(0, held.ColdSegmentCountForTest);
            var error = Assert.Single(logger.Entries, static x => x.Level == LogLevel.Error);
            Assert.Contains(trc, error.Message, StringComparison.Ordinal);
            Assert.Contains("reports itself Degraded", error.Message, StringComparison.Ordinal);
            Assert.Equal(QueryAvailability.Degraded, held.Availability);
        }

        using var restarted = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        restarted.LoadColdSegments();
        Assert.Equal(1, restarted.ColdSegmentCountForTest);
        Assert.Equal(QueryAvailability.Available, restarted.Availability);
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
