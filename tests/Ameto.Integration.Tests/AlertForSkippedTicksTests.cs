using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Alerts;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>For</c> is how long a rule's condition has been SEEN to hold (#94). A tick that did not evaluate
/// the rule saw nothing — its evaluation threw, its store was loading or closed, the process was down —
/// and counting it as held time let a rule that went Pending just before a long outage fire on the
/// first tick after it, on a single observation. Each fact drives the evaluator's ticks at explicit
/// times, fifteen seconds apart as the loop spaces them, through the cycle hook; no clock is waited on.
/// </summary>
public sealed class AlertForSkippedTicksTests : IAsyncLifetime
{
    private static readonly TimeSpan Tick = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan For  = TimeSpan.FromSeconds(60);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "Ameto-alertfor-" + Guid.NewGuid().ToString("N"));
    private readonly FlakyTraces _traces = new();
    private readonly DateTimeOffset _t0 = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds());   // whole seconds: the state store keeps them exactly
    private AlertRuleStore _store = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _store = new AlertRuleStore(_dir, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
        _store.Upsert(new AlertRule
        {
            Id = "spans", Name = "spans", Source = AlertSource.Trace, TraceMetric = TraceMetricKind.SpanCount,
            Comparator = AlertComparator.GreaterThan, Threshold = 5, Window = TimeSpan.FromMinutes(5),
            For = For, Cooldown = TimeSpan.Zero,
        });
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
        return Task.CompletedTask;
    }

    private AlertEvaluator NewEvaluator() => new(
        _store,
        new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
        new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance),
        AlertHeaderCountTests.ThrowingProxy.For<IQueryExecutor>(),
        null!,   // the log engine: no rule here reads it
        AlertHeaderCountTests.ThrowingProxy.For<Ameto.Metrics.IMetricAggregator>(),
        _traces,
        NullLogger<AlertEvaluator>.Instance);

    private static AlertStateSnapshot StateOf(AlertEvaluator evaluator) =>
        evaluator.GetStates().Single(s => s.RuleId == "spans");

    private DateTimeOffset At(int ticks) => _t0 + ticks * Tick;

    /// <summary>
    /// THE ISSUE'S SCENARIO. Pending at tick 0, seen again at ticks 1 and 2 (30 s of the 60), then
    /// eighteen ticks the rule could not be evaluated — the store throwing, as a failing read does.
    /// The first tick after them has 45 s seen, not 315: Pending. The one after, 60 s: Firing.
    /// Before, the first tick after the outage fired.
    /// </summary>
    [Fact]
    public async Task Ticks_that_could_not_evaluate_the_rule_do_not_count_toward_For()
    {
        await using var evaluator = NewEvaluator();

        for (int t = 0; t <= 2; t++) await evaluator.EvaluateOnceAsync(At(t));
        Assert.Equal(AlertState.Pending, StateOf(evaluator).State);
        Assert.Equal(At(0), StateOf(evaluator).PendingSince);

        _traces.Failing = true;
        for (int t = 3; t <= 20; t++) await evaluator.EvaluateOnceAsync(At(t));
        Assert.Equal(AlertState.Pending, StateOf(evaluator).State);   // skipped ticks change nothing
        Assert.Equal(At(2), StateOf(evaluator).EvaluatedAt);

        _traces.Failing = false;
        await evaluator.EvaluateOnceAsync(At(21));
        var st = StateOf(evaluator);
        Assert.Equal(AlertState.Pending, st.State);
        Assert.Equal(At(0) + (At(20) - At(2)), st.PendingSince);   // moved past ticks 3..20: 45 s held

        await evaluator.EvaluateOnceAsync(At(22));
        Assert.Equal(AlertState.Firing, StateOf(evaluator).State);   // 60 s seen
    }

    /// <summary>
    /// Consecutive ticks keep the rule they always had: Pending at tick 0, Firing at the tick that
    /// makes 60 s — a slow gap between two ticks (40 s here) is a cycle that ran late, not a skip,
    /// and is credited whole.
    /// </summary>
    [Fact]
    public async Task Consecutive_ticks_count_in_full_however_far_apart()
    {
        await using var evaluator = NewEvaluator();

        await evaluator.EvaluateOnceAsync(At(0));
        await evaluator.EvaluateOnceAsync(At(0) + TimeSpan.FromSeconds(40));
        Assert.Equal(AlertState.Pending, StateOf(evaluator).State);
        await evaluator.EvaluateOnceAsync(At(0) + TimeSpan.FromSeconds(60));
        Assert.Equal(AlertState.Firing, StateOf(evaluator).State);
        Assert.Equal(At(0), StateOf(evaluator).PendingSince);
    }

    /// <summary>
    /// The same rule across a restart, where it RESTARTS the clock: the state comes back from disk
    /// with its PendingSince and without the time it was last evaluated, so nothing after PendingSince
    /// is known to have been seen. Before, a rule Pending when the process stopped fired on the first
    /// tick after a ten-minute restart; now it waits out For from that tick.
    /// </summary>
    [Fact]
    public async Task A_restart_does_not_count_toward_For_either()
    {
        await using (var before = NewEvaluator())
        {
            await before.EvaluateOnceAsync(At(0));
            Assert.Equal(AlertState.Pending, StateOf(before).State);
        }

        await using var after = NewEvaluator();
        Assert.Equal(AlertState.Pending, StateOf(after).State);   // restored from disk
        Assert.Equal(At(0), StateOf(after).PendingSince);

        await after.EvaluateOnceAsync(At(40));   // ten minutes later
        Assert.Equal(AlertState.Pending, StateOf(after).State);
        Assert.Equal(At(40), StateOf(after).PendingSince);

        for (int t = 41; t <= 43; t++) await after.EvaluateOnceAsync(At(t));
        Assert.Equal(AlertState.Pending, StateOf(after).State);   // 45 s seen
        await after.EvaluateOnceAsync(At(44));
        Assert.Equal(AlertState.Firing, StateOf(after).State);    // 60 s seen
    }

    /// <summary>
    /// The restart again, with the first tick after it SKIPPING the rule — a store still loading, a
    /// read that failed. The clock restarts at the first tick that sees the breach, not at the cycle
    /// before it: counting from the skipped tick credited 15 s nobody saw.
    /// </summary>
    [Fact]
    public async Task After_a_restart_the_clock_starts_at_the_first_tick_that_sees_the_breach()
    {
        await using (var before = NewEvaluator())
            await before.EvaluateOnceAsync(At(0));

        await using var after = NewEvaluator();
        _traces.Failing = true;
        await after.EvaluateOnceAsync(At(40));   // the first tick after the restart: skipped
        _traces.Failing = false;
        await after.EvaluateOnceAsync(At(41));

        Assert.Equal(AlertState.Pending, StateOf(after).State);
        Assert.Equal(At(41), StateOf(after).PendingSince);

        for (int t = 42; t <= 44; t++) await after.EvaluateOnceAsync(At(t));
        Assert.Equal(AlertState.Pending, StateOf(after).State);   // 45 s seen: was Firing, on 60 s counted from tick 40
        await after.EvaluateOnceAsync(At(45));
        Assert.Equal(AlertState.Firing, StateOf(after).State);    // 60 s seen
    }

    /// <summary>A trace store answering 20 spans, or failing the read while <see cref="Failing"/> is set.</summary>
    private sealed class FlakyTraces : ITraceStatsProvider
    {
        public volatile bool Failing;

        public Task<IReadOnlyList<ServiceSegmentStats>> GetAggregateStatsAsync(
            DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default) =>
            Failing
                ? Task.FromException<IReadOnlyList<ServiceSegmentStats>>(new IOException("the span store could not be read"))
                : Task.FromResult<IReadOnlyList<ServiceSegmentStats>>([new ServiceSegmentStats { ServiceName = "api", SpanCount = 20 }]);
    }

    private sealed class NoopProtector : ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }
}
