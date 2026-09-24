using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Alerts;
using Ameto.Metrics;
using Ameto.Tracing;

namespace Ameto.Integration.Tests;

/// <summary>
/// A NaN OR AN INFINITY IN A METRIC WINDOW MUST NEITHER FIRE NOR RESOLVE A RULE SILENTLY (#92).
///
/// <para>The evaluator reduces a window to its peak (for "&gt;") or trough (for "&lt;"). A non-finite
/// point folded into that reduction did damage both ways: <c>Math.Max(acc, NaN)</c> is NaN, which
/// reset the reduction and dropped the peak before it; a NaN last in the window came out as the
/// "empty window" 0, which resolves a firing "&gt;" rule and fires a "&lt;" one; and an infinity was
/// compared as a value. Nothing said so in the log. Now non-finite points are skipped — the panel
/// shows them as gaps — a window with nothing BUT them leaves the rule's state alone, and the
/// evaluator says so once per rule.</para>
///
/// <para>The aggregator is a stand-in answering a fixed window, so each case is one evaluation
/// against known points; no timer decides anything.</para>
/// </summary>
public sealed class AlertNonFiniteMetricTests
{
    private const long T0 = 1_784_800_800_000_000_000L;
    private const long S  = 1_000_000_000L;

    [Fact]
    public async Task A_NaN_in_the_window_does_not_drop_the_peak_before_it()
    {
        await using var rig = new Rig(Rule(AlertComparator.GreaterThan, 50));
        rig.Metrics.Points = [100, double.NaN, 1];

        await rig.Evaluator.EvaluateOnceAsync();

        // Before: Max(100, NaN) = NaN reset the peak, the 1 after it became the value, and the rule
        // that crossed its threshold at 100 stayed Ok.
        var state = rig.State();
        Assert.Equal(AlertState.Firing, state.State);
        Assert.Equal(100, state.LastValue);
    }

    [Fact]
    public async Task An_infinity_is_not_a_value_either()
    {
        await using var rig = new Rig(Rule(AlertComparator.GreaterThan, 50));
        rig.Metrics.Points = [10, double.PositiveInfinity, 20];

        await rig.Evaluator.EvaluateOnceAsync();

        // Before: the peak was +Infinity — the rule fired, and /api/alerts/state then failed to
        // serialize the LastValue it kept.
        var state = rig.State();
        Assert.Equal(AlertState.Ok, state.State);
        Assert.Equal(20, state.LastValue);
    }

    [Fact]
    public async Task A_firing_rule_is_not_resolved_by_a_window_of_NaN()
    {
        await using var rig = new Rig(Rule(AlertComparator.GreaterThan, 50));
        rig.Metrics.Points = [100];
        await rig.Evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, rig.State().State);

        rig.Metrics.Points = [double.NaN, double.NaN];
        await rig.Evaluator.EvaluateOnceAsync();

        // Before: no finite value read as 0, and 0 > 50 is false — an Ok notification for an
        // incident nobody resolved.
        Assert.Equal(AlertState.Firing, rig.State().State);
        Assert.Equal(100, rig.State().LastValue);
        Assert.DoesNotContain(rig.Evaluator.GetHistory(), h => h.State == AlertState.Ok);
        Assert.Equal(1, rig.Warnings(undetermined: true));
    }

    [Fact]
    public async Task A_less_than_rule_is_not_fired_by_a_window_of_non_finite_points()
    {
        await using var rig = new Rig(Rule(AlertComparator.LessThan, 5));
        rig.Metrics.Points = [double.PositiveInfinity, double.NegativeInfinity, double.NaN];

        await rig.Evaluator.EvaluateOnceAsync();

        // Before: the window reduced to NaN, read as 0, and 0 < 5 fired the rule.
        Assert.NotEqual(AlertState.Firing, rig.State().State);
        Assert.DoesNotContain(rig.Evaluator.GetHistory(), h => h.State == AlertState.Firing);

        // The preview claims no value either — it answered 0 here, and "0 < 5" previewed as "would fire".
        Assert.Null(await rig.Evaluator.PreviewAsync(rig.Rule));
    }

    [Fact]
    public async Task It_is_said_once_per_rule_not_every_tick()
    {
        await using var rig = new Rig(Rule(AlertComparator.GreaterThan, 50));
        rig.Metrics.Points = [1, double.NaN];
        for (int i = 0; i < 3; i++) await rig.Evaluator.EvaluateOnceAsync();

        rig.Metrics.Points = [double.NaN];
        for (int i = 0; i < 3; i++) await rig.Evaluator.EvaluateOnceAsync();

        Assert.Equal(1, rig.Warnings(undetermined: false));
        Assert.Equal(1, rig.Warnings(undetermined: true));
    }

    [Fact]
    public async Task A_finite_window_is_evaluated_as_before()
    {
        await using var rig = new Rig(Rule(AlertComparator.GreaterThan, 50));
        rig.Metrics.Points = [10, 70, 30];
        await rig.Evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, rig.State().State);
        Assert.Equal(70, rig.State().LastValue);

        rig.Metrics.Points = [];
        await rig.Evaluator.EvaluateOnceAsync();                 // an EMPTY window is still 0
        Assert.Equal(AlertState.Ok, rig.State().State);
        Assert.Equal(0, await rig.Evaluator.PreviewAsync(rig.Rule));
        Assert.Equal(0, rig.Warnings(undetermined: false) + rig.Warnings(undetermined: true));
    }

    /// <summary>
    /// THROUGH THE REAL AGGREGATOR: two pods of one service, one exporting NaN. A rule over the
    /// service — grouped, as fleet rules are, with the default instant "last" or a grouped max —
    /// used to see the group's value as NaN at every timestamp (the aggregator folded the NaN into
    /// the sum / the max before the evaluator could skip it), so it was never evaluated while that
    /// pod lived. It is evaluated on the finite pod.
    /// </summary>
    [Theory]
    [InlineData(null,  4.0)]      // the default: last — pod a's 4 (pod b has no finite value)
    [InlineData("max", 4.0)]
    [InlineData("sum", 4.0)]
    public async Task A_rule_over_a_group_with_a_NaN_member_is_evaluated_on_the_finite_one(string? aggregation, double expected)
    {
        var fleet = new Fleet(
            new MetricSeries { Name = "queue.depth", Kind = MetricKind.Gauge, Labels = Pod("a"),
                               Points = [P(T0, 3), P(T0 + S, 4)] },
            new MetricSeries { Name = "queue.depth", Kind = MetricKind.Gauge, Labels = Pod("b"),
                               Points = [P(T0, double.NaN), P(T0 + S, double.NaN)] });
        var rule = new AlertRule
        {
            Id = "fleet-rule", Name = "fleet rule", Source = AlertSource.Metric, Metric = "queue.depth",
            Aggregation = aggregation, GroupBy = ["service.name"],
            Comparator = AlertComparator.GreaterThan, Threshold = 3.5,
            Window = TimeSpan.FromMinutes(5), For = TimeSpan.Zero, Cooldown = TimeSpan.Zero,
        };
        await using var rig = new Rig(rule, new MetricAggregator(fleet));

        await rig.Evaluator.EvaluateOnceAsync();

        var state = rig.State();
        Assert.Equal(AlertState.Firing, state.State);
        Assert.Equal(expected, state.LastValue);
        Assert.Equal(0, rig.Warnings(undetermined: true));
    }

    private static LabelSet Pod(string pod) =>
        new([new("service.name", "checkout"), new("pod", pod)]);

    private static MetricDataPoint P(long ts, double v) => new() { TimestampUnixNano = ts, Value = v };

    /// <summary>Storage as the aggregator sees it: these series, for any metric and window.</summary>
    private sealed class Fleet(params MetricSeries[] series) : IMetricQuery
    {
        public IEnumerable<string> GetMetricNames(string? prefix = null) => [];

        public async IAsyncEnumerable<MetricSeries> QueryAsync(
            string metricName, DateTimeOffset? from = null, DateTimeOffset? to = null, TimeSpan? step = null,
            IReadOnlyDictionary<string, string>? labelMatchers = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.CompletedTask;
            foreach (var s in series) yield return s;
        }

        public async IAsyncEnumerable<MetricSeries> GetLatestAsync(
            string metricName, IReadOnlyDictionary<string, string>? labelMatchers = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    // ── Rig ───────────────────────────────────────────────────────────────────

    private static AlertRule Rule(AlertComparator comparator, double threshold) => new()
    {
        Id          = "nan-rule",
        Name        = "nan rule",
        Source      = AlertSource.Metric,
        Metric      = "queue.depth",
        Aggregation = "max",
        Comparator  = comparator,
        Threshold   = threshold,
        Window      = TimeSpan.FromMinutes(5),
        For         = TimeSpan.Zero,
        Cooldown    = TimeSpan.Zero,
    };

    private sealed class Rig : IAsyncDisposable
    {
        private readonly string _dir = Path.Combine(Path.GetTempPath(), "Ameto-alertnan-" + Guid.NewGuid().ToString("N"));
        private readonly WarningLog _log = new();

        public FixedWindow    Metrics   { get; } = new();
        public AlertEvaluator Evaluator { get; }
        public AlertRule      Rule      { get; }

        /// <param name="aggregator">What the evaluator queries: <see cref="Metrics"/>' single fixed
        /// series unless a test hands it the real aggregator.</param>
        public Rig(AlertRule rule, IMetricAggregator? aggregator = null)
        {
            Directory.CreateDirectory(_dir);
            var store = new AlertRuleStore(_dir, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
            Evaluator = new AlertEvaluator(
                store,
                new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
                new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance),
                AlertHeaderCountTests.ThrowingProxy.For<Ameto.Core.IQueryExecutor>(),
                null!,   // the log engine: a metric rule never reaches it
                aggregator ?? Metrics,
                AlertHeaderCountTests.ThrowingProxy.For<ITraceStatsProvider>(),
                _log);
            Rule = rule;
            store.Upsert(rule);
        }

        public AlertStateSnapshot State() => Evaluator.GetStates().Single(s => s.RuleId == Rule.Id);

        public int Warnings(bool undetermined) => _log.Count(undetermined ? "was not evaluated" : "skipped");

        public async ValueTask DisposeAsync()
        {
            await Evaluator.DisposeAsync();
            try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
        }
    }

    /// <summary>One series answering <see cref="Points"/>, 1 s apart — whatever the rule asks.</summary>
    private sealed class FixedWindow : IMetricAggregator
    {
        public double[] Points = [];

        public Task<IReadOnlyList<MetricSeries>> QueryAsync(MetricQueryRequest request, CancellationToken ct = default)
        {
            var pts = new MetricDataPoint[Points.Length];
            for (int i = 0; i < pts.Length; i++) pts[i] = new MetricDataPoint { TimestampUnixNano = T0 + i * S, Value = Points[i] };
            return Task.FromResult<IReadOnlyList<MetricSeries>>(
                [new MetricSeries { Name = request.Metric, Kind = MetricKind.Gauge, Points = pts }]);
        }

        public Task<MetricSeries> EvalExprAsync(MetricExprRequest request, CancellationToken ct = default) =>
            throw new InvalidOperationException("not expected");

        public Task<HeatmapResult> HeatmapAsync(string metricName, DateTimeOffset? from, DateTimeOffset? to, TimeSpan? step,
            IReadOnlyDictionary<string, string>? filters, CancellationToken ct = default) =>
            throw new InvalidOperationException("not expected");
    }

    /// <summary>The evaluator's warnings, as rendered text.</summary>
    private sealed class WarningLog : ILogger<AlertEvaluator>
    {
        private readonly List<string> _lines = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                Func<TState, Exception?, string> formatter)
        {
            if (logLevel != LogLevel.Warning) return;
            lock (_lines) _lines.Add(formatter(state, exception));
        }

        public int Count(string fragment)
        {
            lock (_lines) return _lines.Count(l => l.Contains(fragment, StringComparison.Ordinal));
        }
    }

    private sealed class NoopProtector : Ameto.Core.ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }
}
