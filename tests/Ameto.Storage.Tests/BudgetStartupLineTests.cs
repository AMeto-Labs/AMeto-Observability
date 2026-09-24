using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;
using EventId = Microsoft.Extensions.Logging.EventId;
using Ameto.Core;
using Ameto.Metrics.Storage;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE STARTUP LINES THE DOCS PROMISE (PR #84 review, #7): config.yml says the effective Metrics
/// figures "are printed at startup", and until now only the logs engine printed its budgets. One
/// line each for metrics and traces, carrying what the engines ENFORCE — here figures no derivation
/// produces, so a line that printed a fresh derivation would not match.
/// </summary>
public sealed class BudgetStartupLineTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-budgetline-" + Guid.NewGuid().ToString("N"));

    public BudgetStartupLineTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task The_metric_engine_prints_its_effective_budgets_once_at_startup()
    {
        var log = new Lines<MetricStorageEngine>();
        await using var engine = new MetricStorageEngine(Path.Combine(_dir, "metrics"), log,
                                                         new MetricsOptions { HotTierBytes = 12_345_678, MaxExemplarMetrics = 3 });

        string line = Assert.Single(log.Messages, m => m.StartsWith("Metric budgets:", StringComparison.Ordinal));
        Assert.Contains("hot tier 12345678 B", line);
        Assert.Contains("periodic flush above 1234567 B", line);
        Assert.Contains($"metrics.wal opened at {engine.WalInitialBytes} B", line);
        Assert.Contains($"exemplars {engine.ExemplarsPerMetric} per metric in at most {engine.MaxExemplarMetrics} rings", line);
    }

    [Fact]
    public void The_trace_line_carries_what_the_engine_and_the_ring_were_built_with()
    {
        var log = new Lines<TraceDiagnostics>();
        var pools = new SpanStringPools();
        using var engine = new TraceStorageEngine(Path.Combine(_dir, "traces"), NullLogger<TraceStorageEngine>.Instance,
                                                  false, true, new TracesOptions { HotTierMaxBytes = 9_876_543 }, pools);
        using var ring = new SpanRingBuffer(capacity: 2_048, maxBytes: 777_777, pools);

        TraceDiagnostics.LogBudgets(log, engine, ring);

        string line = Assert.Single(log.Messages);
        Assert.StartsWith("Trace budgets:", line, StringComparison.Ordinal);
        Assert.Contains("hot tier 9876543 B", line);
        Assert.Contains($"compaction pass {engine.MergeBudgetBytesForTest} B", line);
        Assert.Contains("ingest ring 2048 slots holding at most 777777 B", line);
    }

    /// <summary>Formatted messages at Information and above. Invariant formatting: the dev box is ru-KZ.</summary>
    private sealed class Lines<T> : ILogger<T>
    {
        private readonly List<string> _messages = [];
        public IReadOnlyList<string> Messages { get { lock (_messages) return [.. _messages]; } }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => level >= LogLevel.Information;

        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? error,
                                Func<TState, Exception?, string> formatter)
        {
            if (level < LogLevel.Information) return;
            lock (_messages) _messages.Add(formatter(state, error));
        }
    }
}
