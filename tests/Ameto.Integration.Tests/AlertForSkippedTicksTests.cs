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

    /// <summary>
    /// An evaluator whose timed loop never runs: every tick here is one this class drives at an
    /// explicit time, and a real one fifteen seconds in (a slow runner) would evaluate the rule at the
    /// wall clock and move the cycle clock these facts reason about.
    /// </summary>
    private AlertEvaluator NewEvaluator()
    {
        AlertEvaluator.NoTimedLoopForTest.Value = true;
        try
        {
            return new(
                _store,
                new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
                new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance),
                AlertHeaderCountTests.ThrowingProxy.For<IQueryExecutor>(),
                null!,   // the log engine: no rule here reads it
                AlertHeaderCountTests.ThrowingProxy.For<Ameto.Metrics.IMetricAggregator>(),
                _traces,
                NullLogger<AlertEvaluator>.Instance);
        }
        finally { AlertEvaluator.NoTimedLoopForTest.Value = false; }
    }

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
    /// A DEGRADED STORE'S TICKS ARE SKIPS LIKE ANY OTHER (#94). Pending at tick 0, seen at ticks 1
    /// and 2; then eighteen ticks over a store whose load ended short — skipped, its partial answer
    /// never read — and the store whole again, which in production is the restart that ends the
    /// state (a restart is the same arithmetic, through the persisted evaluation time — see
    /// <see cref="A_restart_keeps_the_credit_seen_before_it_and_does_not_count_the_downtime"/>). The
    /// first tick after has 45 s seen, not 315: Pending. The one after, 60 s: Firing.
    /// </summary>
    [Fact]
    public async Task Ticks_skipped_over_a_degraded_store_do_not_count_toward_For()
    {
        await using var evaluator = NewEvaluator();

        for (int t = 0; t <= 2; t++) await evaluator.EvaluateOnceAsync(At(t));
        Assert.Equal(AlertState.Pending, StateOf(evaluator).State);

        _traces.Availability = QueryAvailability.Degraded;
        for (int t = 3; t <= 20; t++) await evaluator.EvaluateOnceAsync(At(t));
        Assert.Equal(AlertState.Pending, StateOf(evaluator).State);
        Assert.Equal(At(2), StateOf(evaluator).EvaluatedAt);   // not one of them evaluated it

        _traces.Availability = QueryAvailability.Available;
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
        // The fixture's premise: no timed tick can land between these. The loop returns at once
        // (a bounded wait for a task that is already done or about to be; a started loop sits in
        // its 15 s delay and this fails after five).
        Assert.True(SpinWait.SpinUntil(() => evaluator.LoopEndedForTest, TimeSpan.FromSeconds(5)),
            "the evaluator's timed loop is running under a test that drives its ticks by hand");

        await evaluator.EvaluateOnceAsync(At(0));
        await evaluator.EvaluateOnceAsync(At(0) + TimeSpan.FromSeconds(40));
        Assert.Equal(AlertState.Pending, StateOf(evaluator).State);
        await evaluator.EvaluateOnceAsync(At(0) + TimeSpan.FromSeconds(60));
        Assert.Equal(AlertState.Firing, StateOf(evaluator).State);
        Assert.Equal(At(0), StateOf(evaluator).PendingSince);
    }

    /// <summary>
    /// A restart is a run of skipped ticks (#94, #106 review F1): the state comes back from disk WITH
    /// the time the rule was last evaluated, so the downtime adds nothing and the 30 s seen before it
    /// are kept. Ten minutes later the first tick resumes at 30 s, the next makes 45, the one after
    /// fires. Before #94 the downtime counted and the first tick fired; between 1d8d777 and this the
    /// credit was thrown away and the count started over.
    /// </summary>
    [Fact]
    public async Task A_restart_keeps_the_credit_seen_before_it_and_does_not_count_the_downtime()
    {
        await using (var before = NewEvaluator())
        {
            for (int t = 0; t <= 2; t++) await before.EvaluateOnceAsync(At(t));
            Assert.Equal(AlertState.Pending, StateOf(before).State);
        }

        await using var after = NewEvaluator();
        Assert.Equal(AlertState.Pending, StateOf(after).State);   // restored from disk
        Assert.Equal(At(0), StateOf(after).PendingSince);
        Assert.Equal(At(2), StateOf(after).EvaluatedAt);         // and when it was last seen

        await after.EvaluateOnceAsync(At(40));   // ten minutes later
        Assert.Equal(AlertState.Pending, StateOf(after).State);
        Assert.Equal(At(0) + (At(40) - At(2)), StateOf(after).PendingSince);   // 30 s held, the downtime skipped

        await after.EvaluateOnceAsync(At(41));
        Assert.Equal(AlertState.Pending, StateOf(after).State);   // 45 s seen
        await after.EvaluateOnceAsync(At(42));
        Assert.Equal(AlertState.Firing, StateOf(after).State);    // 60 s seen
    }

    /// <summary>
    /// THE REVIEW'S SCENARIO (#106 F1): For 60 s, the breach seen on every tick, the server restarting
    /// every 45 s — three ticks a life. The second life's third tick has seen 60 s, and fires. While a
    /// restart reset the clock, every life started over at 0 and the rule was still Pending after ten
    /// lives; on main before #94 it fired on the second life's first tick, the downtime counted.
    /// </summary>
    [Fact]
    public async Task A_server_restarting_more_often_than_For_still_fires()
    {
        int tick = 0;
        for (int life = 1; life <= 4; life++)
        {
            await using var evaluator = NewEvaluator();
            for (int t = 0; t < 3; t++, tick++)
            {
                await evaluator.EvaluateOnceAsync(At(tick));
                if (StateOf(evaluator).State == AlertState.Firing)
                {
                    Assert.Equal((2, 2), (life, t));   // the second life's last tick: 30 s + 30 s seen
                    return;
                }
            }
        }
        Assert.Fail("a rule whose breach was seen on every tick never fired across four restarts");
    }

    /// <summary>
    /// A state row WITHOUT an evaluation time — written before the column existed — says nothing about
    /// what was seen after PendingSince, so the clock restarts at the first tick that sees the breach;
    /// and when the first tick after the restart SKIPS the rule (a store still loading, a failed
    /// read), not at that skipped tick: counting from it would credit 15 s nobody saw.
    /// </summary>
    [Fact]
    public async Task A_state_from_before_the_evaluation_column_restarts_the_clock_at_the_first_tick_that_sees_the_breach()
    {
        new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance)
            .SaveState("spans", AlertState.Pending, 20, pendingSince: At(0), lastFired: null);   // no evaluation time

        await using var after = NewEvaluator();
        Assert.Equal(AlertState.Pending, StateOf(after).State);
        _traces.Failing = true;
        await after.EvaluateOnceAsync(At(40));   // the first tick after the restart: skipped
        _traces.Failing = false;
        await after.EvaluateOnceAsync(At(41));

        Assert.Equal(AlertState.Pending, StateOf(after).State);
        Assert.Equal(At(41), StateOf(after).PendingSince);

        for (int t = 42; t <= 44; t++) await after.EvaluateOnceAsync(At(t));
        Assert.Equal(AlertState.Pending, StateOf(after).State);   // 45 s seen
        await after.EvaluateOnceAsync(At(45));
        Assert.Equal(AlertState.Firing, StateOf(after).State);    // 60 s seen
    }

    /// <summary>
    /// The migration: an <c>alert_state</c> table created before <c>evaluated_ticks</c> existed gains the
    /// column in place when the persistence opens, its rows read back with no evaluation time, and a
    /// write then carries one. Opening it again changes nothing.
    /// </summary>
    [Fact]
    public void An_alert_state_table_from_before_the_column_is_migrated_in_place()
    {
        using (var conn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={Path.Combine(_dir, "Ameto.db")};Pooling=False"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE alert_state (
                    rule_id TEXT PRIMARY KEY, state INTEGER NOT NULL, last_value REAL NOT NULL,
                    pending_ticks INTEGER, fired_ticks INTEGER);
                INSERT INTO alert_state VALUES ('old', 1, 7, 638000000000000000, NULL);
                """;
            cmd.ExecuteNonQuery();
        }

        var persist = new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance);
        var old = Assert.Single(persist.LoadStates());
        Assert.Equal(("old", AlertState.Pending, (DateTimeOffset?)null), (old.RuleId, old.State, old.Evaluated));

        persist.SaveState("old", AlertState.Pending, 7, old.Pending, null, evaluatedAt: At(3));
        var again = Assert.Single(new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance).LoadStates());
        Assert.Equal(At(3), again.Evaluated);
        Assert.Equal(old.Pending, again.Pending);
    }

    /// <summary>
    /// A trace store answering 20 spans, or failing the read while <see cref="Failing"/> is set, and
    /// saying it cannot answer truly while <see cref="Availability"/> is not Available.
    /// </summary>
    private sealed class FlakyTraces : ITraceStatsProvider
    {
        public volatile bool Failing;

        private volatile QueryAvailability _availability;
        public QueryAvailability Availability { get => _availability; set => _availability = value; }

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
