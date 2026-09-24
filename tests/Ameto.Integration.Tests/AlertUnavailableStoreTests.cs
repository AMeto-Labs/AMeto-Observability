using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Alerts;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;
using EventId  = Microsoft.Extensions.Logging.EventId;

namespace Ameto.Integration.Tests;

/// <summary>
/// A STORE THAT CANNOT ANSWER IS NOT A STORE THAT ANSWERED ZERO (#95).
///
/// <para>#84 made the evaluator stop acting once the HOST stops, which covers the host's own
/// shutdown and nothing else. Any other way a store stops answering truly — closed by something
/// that is not <c>ApplicationStopping</c>, or still scanning its cold tier after a start — reached
/// the evaluator as an empty answer, and an empty answer is a 0: a firing "&gt;" rule resolved,
/// with an Ok notification to every channel and an Ok state persisted, and a "&lt;" rule fired.</para>
///
/// <para>Every test here runs with NO host and NO lifetime, so <c>IsStopping</c> is never set: what
/// holds the rule is the store saying it cannot answer, and nothing else. The stores are stand-ins
/// whose availability the test sets; the real engines' side of the contract is pinned by
/// <c>TraceStoreUnavailableTests</c> and <c>MetricStoreUnavailableTests</c>.</para>
/// </summary>
public sealed class AlertUnavailableStoreTests : IAsyncLifetime
{
    private const int Spans = 20;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "Ameto-alertunavail-" + Guid.NewGuid().ToString("N"));
    private readonly FakeTraceStore  _traces  = new();
    private readonly FakeMetricStore _metrics = new();
    private readonly CapturingLogger _log     = new();
    private readonly ConcurrentQueue<AlertFiredEvent> _dispatched = new();

    private AlertRuleStore _store     = null!;
    private AlertEvaluator _evaluator = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _store = new AlertRuleStore(_dir, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
        _evaluator = new AlertEvaluator(
            _store,
            new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
            new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance),
            AlertHeaderCountTests.ThrowingProxy.For<IQueryExecutor>(),
            null!,   // the log engine: no rule here reads it
            _metrics,
            _traces,
            _log);
        _evaluator._onDispatchForTest = _dispatched.Enqueue;
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _evaluator.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    // ── The closed store ──────────────────────────────────────────────────────────────────────

    /// <summary>
    /// THE ISSUE'S SCENARIO. A trace rule fires; the trace store closes — not because the host is
    /// stopping; nothing here has a host — and answers empty. The rule stays Firing, in memory and
    /// on disk, with no Ok in its history and nothing dispatched after the firing notification.
    /// Before #95 this tick read 0 and resolved it.
    /// </summary>
    [Fact]
    public async Task A_firing_rule_stays_firing_when_its_store_closes_outside_a_host_stop()
    {
        var rule = TraceRule(AlertComparator.GreaterThan, Spans - 1);
        await _evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, StateOf(rule).State);
        Assert.Single(_dispatched);

        _traces.Availability = QueryAvailability.Closed;
        await _evaluator.EvaluateOnceAsync();

        AssertUntouched(rule, AlertState.Firing, Spans);
        Assert.Single(_dispatched);   // the firing notification, and no Ok after it
    }

    /// <summary>
    /// The other half of the same 0: a "&lt;" rule over a closed store would FIRE — "fewer than 5
    /// spans" is true of an empty answer — and page someone about a service that is fine.
    /// </summary>
    [Fact]
    public async Task A_less_than_rule_does_not_fire_on_a_closed_store()
    {
        var rule = TraceRule(AlertComparator.LessThan, 5);
        await _evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Ok, StateOf(rule).State);

        _traces.Availability = QueryAvailability.Closed;
        await _evaluator.EvaluateOnceAsync();

        AssertUntouched(rule, AlertState.Ok, Spans);
        Assert.Empty(_dispatched);
    }

    /// <summary>
    /// THE QUESTION ASKED AFTER THE READ. The store is open when the tick asks and closes INSIDE the
    /// read, answering empty — the interleaving #84 met for the host stop, here without one. A
    /// check made only before the read passes this tick and resolves the rule.
    /// </summary>
    [Fact]
    public async Task A_store_that_closes_during_the_read_is_caught_after_it()
    {
        var rule = TraceRule(AlertComparator.GreaterThan, Spans - 1);
        await _evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, StateOf(rule).State);

        _traces.BeforeDoor = () => _traces.Availability = QueryAvailability.Closed;
        await _evaluator.EvaluateOnceAsync();

        Assert.Null(_traces.BeforeDoor);   // the read ran, and the store closed inside it
        AssertUntouched(rule, AlertState.Firing, Spans);
        Assert.Single(_dispatched);
    }

    /// <summary>A metric rule is held the same way: the aggregator speaks for the store beneath it.</summary>
    [Fact]
    public async Task A_firing_metric_rule_stays_firing_when_its_store_closes()
    {
        var rule = new AlertRule
        {
            Id = "latency", Name = "latency", Source = AlertSource.Metric, Metric = "http.latency",
            Aggregation = "max", Comparator = AlertComparator.GreaterThan, Threshold = 50,
            Window = TimeSpan.FromMinutes(5), For = TimeSpan.Zero, Cooldown = TimeSpan.Zero,
        };
        _store.Upsert(rule);
        await _evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, StateOf(rule).State);

        _metrics.Availability = QueryAvailability.Closed;
        await _evaluator.EvaluateOnceAsync();

        AssertUntouched(rule, AlertState.Firing, FakeMetricStore.Peak);
        Assert.Single(_dispatched);
    }

    // ── The store still loading ───────────────────────────────────────────────────────────────

    /// <summary>
    /// A store still scanning its cold tier answers a PART: here, none of the 20 spans. A "&lt;"
    /// rule reading that part fires; it must wait until the store has loaded, and then be evaluated
    /// normally — the skip is a skip, not a latch.
    /// </summary>
    [Fact]
    public async Task A_store_still_loading_is_not_evaluated_until_it_has_loaded()
    {
        _traces.Availability = QueryAvailability.Loading;
        var rule = TraceRule(AlertComparator.LessThan, 5);

        await _evaluator.EvaluateOnceAsync();

        Assert.DoesNotContain(_evaluator.GetStates(), s => s.RuleId == rule.Id && s.EvaluatedAt != DateTimeOffset.MinValue);
        Assert.Equal(0, _traces.Reads);   // asked before the read: a loading store is not even read
        Assert.Empty(_dispatched);

        _traces.Availability = QueryAvailability.Available;
        await _evaluator.EvaluateOnceAsync();

        var st = StateOf(rule);
        Assert.Equal(AlertState.Ok, st.State);
        Assert.Equal(Spans, st.LastValue);
        Assert.NotEqual(DateTimeOffset.MinValue, st.EvaluatedAt);
    }

    /// <summary>
    /// THE QUESTION ASKED BEFORE THE READ. A read that begins while the store loads and whose load
    /// finishes INSIDE it, after its snapshot: a partial answer from a store that says Available by
    /// the time anyone asks again. A check made only after the read launders that part into a
    /// value — here, 0 spans, and "fewer than 5" fires.
    /// </summary>
    [Fact]
    public async Task A_load_that_finishes_during_the_read_does_not_launder_its_partial_answer()
    {
        _traces.Availability  = QueryAvailability.Loading;
        _traces.AfterSnapshot = () => _traces.Availability = QueryAvailability.Available;
        var rule = TraceRule(AlertComparator.LessThan, 5);

        await _evaluator.EvaluateOnceAsync();

        var st = StateOf(rule);
        Assert.Equal(AlertState.Ok, st.State);
        Assert.Equal(DateTimeOffset.MinValue, st.EvaluatedAt);   // not evaluated at all
        Assert.Empty(_dispatched);
    }

    // ── What the operator sees ────────────────────────────────────────────────────────────────

    /// <summary>
    /// ONE warning per source per minute, however many rules the closed store skips and however
    /// many ticks it stays closed: three rules and two ticks are six skips and one line.
    /// </summary>
    [Fact]
    public async Task A_closed_store_is_reported_once_not_once_per_rule_per_tick()
    {
        TraceRule(AlertComparator.GreaterThan, 1, "a");
        TraceRule(AlertComparator.GreaterThan, 2, "b");
        TraceRule(AlertComparator.LessThan,    3, "c");
        _traces.Availability = QueryAvailability.Closed;

        await _evaluator.EvaluateOnceAsync();
        await _evaluator.EvaluateOnceAsync();

        var warnings = _log.Lines.Where(l => l.Level == LogLevel.Warning && l.Message.Contains("was not evaluated")).ToList();
        Assert.Single(warnings);
        Assert.Contains("Trace store is Closed", warnings[0].Message);
    }

    /// <summary>The editor's preview says "cannot say" rather than 0 — "would not fire" about nothing.</summary>
    [Fact]
    public async Task The_preview_of_a_rule_over_a_closed_store_is_unavailable_not_zero()
    {
        var rule = TraceRule(AlertComparator.LessThan, 5);
        _traces.Availability = QueryAvailability.Closed;

        var value = await _evaluator.PreviewAsync(rule);

        Assert.False(value.IsAvailable);
        Assert.Equal(QueryAvailability.Closed, value.Availability);
        Assert.True(double.IsNaN(value.Value));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────────────

    private AlertRule TraceRule(AlertComparator cmp, double threshold, string id = "checkout-spans")
    {
        var rule = new AlertRule
        {
            Id          = id,
            Name        = id,
            Source      = AlertSource.Trace,
            Service     = "checkout",
            TraceMetric = TraceMetricKind.SpanCount,
            Comparator  = cmp,
            Threshold   = threshold,
            Window      = TimeSpan.FromHours(1),
            For         = TimeSpan.Zero,
            Cooldown    = TimeSpan.Zero,
        };
        _store.Upsert(rule);
        return rule;
    }

    private AlertStateSnapshot StateOf(AlertRule rule) =>
        _evaluator.GetStates().Single(s => s.RuleId == rule.Id);

    /// <summary>The state, the last value and the history are what the last AVAILABLE tick left, in memory and on disk.</summary>
    private void AssertUntouched(AlertRule rule, AlertState expected, double lastValue)
    {
        var st = StateOf(rule);
        Assert.Equal(expected, st.State);
        Assert.Equal(lastValue, st.LastValue);
        Assert.DoesNotContain(_evaluator.GetHistory(), h => h.RuleId == rule.Id && h.State != expected);

        var persisted = new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance).LoadStates()
                            .Where(s => s.RuleId == rule.Id).ToList();
        if (expected == AlertState.Ok) Assert.All(persisted, s => Assert.Equal(AlertState.Ok, s.State));
        else                           Assert.Equal(expected, Assert.Single(persisted).State);
    }

    /// <summary>
    /// The trace store as the evaluator sees it: <see cref="Spans"/> spans of "checkout" while
    /// Available, none otherwise — the empty answer the real engine gives once it has closed, and
    /// the hot-only part it gives while it loads (here, the spans are all cold).
    /// </summary>
    private sealed class FakeTraceStore : ITraceStatsProvider
    {
        private int _availability;
        public QueryAvailability Availability
        {
            get => (QueryAvailability)Volatile.Read(ref _availability);
            set => Volatile.Write(ref _availability, (int)value);
        }

        /// <summary>Runs inside the read BEFORE the store looks at its own door — a close here is the real engine's failed TryEnterEngine.</summary>
        public Action? BeforeDoor;

        /// <summary>Runs inside the read AFTER the store took its snapshot — a load finishing here finished too late for this read.</summary>
        public Action? AfterSnapshot;

        public int Reads;

        public Task<IReadOnlyList<ServiceSegmentStats>> GetAggregateStatsAsync(
            DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
        {
            Interlocked.Increment(ref Reads);
            Interlocked.Exchange(ref BeforeDoor, null)?.Invoke();
            var at = Availability;
            Interlocked.Exchange(ref AfterSnapshot, null)?.Invoke();
            return Task.FromResult<IReadOnlyList<ServiceSegmentStats>>(at == QueryAvailability.Available
                ? [new ServiceSegmentStats { ServiceName = "checkout", SpanCount = Spans }]
                : []);
        }
    }

    /// <summary>One gauge series peaking at <see cref="Peak"/> while Available; no series otherwise.</summary>
    private sealed class FakeMetricStore : IMetricAggregator
    {
        public const double Peak = 120;

        private int _availability;
        public QueryAvailability Availability
        {
            get => (QueryAvailability)Volatile.Read(ref _availability);
            set => Volatile.Write(ref _availability, (int)value);
        }

        public Task<IReadOnlyList<MetricSeries>> QueryAsync(MetricQueryRequest request, CancellationToken ct = default)
        {
            if (Availability != QueryAvailability.Available)
                return Task.FromResult<IReadOnlyList<MetricSeries>>([]);
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
            return Task.FromResult<IReadOnlyList<MetricSeries>>(
            [
                new MetricSeries
                {
                    Name = request.Metric, Kind = MetricKind.Gauge,
                    Points = [new MetricDataPoint { TimestampUnixNano = now - 1_000_000_000L, Value = Peak }],
                },
            ]);
        }

        public Task<MetricSeries> EvalExprAsync(MetricExprRequest request, CancellationToken ct = default) =>
            throw new InvalidOperationException("not expected");

        public Task<HeatmapResult> HeatmapAsync(string metricName, DateTimeOffset? from, DateTimeOffset? to,
            TimeSpan? step, IReadOnlyDictionary<string, string>? filters, CancellationToken ct = default) =>
            throw new InvalidOperationException("not expected");
    }

    private sealed class CapturingLogger : ILogger<AlertEvaluator>
    {
        public readonly ConcurrentQueue<(LogLevel Level, string Message)> Lines = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Lines.Enqueue((logLevel, formatter(state, exception)));
    }

    private sealed class NoopProtector : ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }
}
