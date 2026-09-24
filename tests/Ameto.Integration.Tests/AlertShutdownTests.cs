using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Ameto.Alerts;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// A SERVER THAT IS STOPPING MUST NOT TELL ANYONE THEIR INCIDENT IS OVER.
///
/// <para>The engines the alert evaluator reads answer EMPTY once their teardown has shut the door
/// — the trace engine's <c>GetAggregateStatsAsync</c> returns no services, the metric engine's
/// closed cold tier returns no series — and that is the right answer for a query that arrives
/// late. To the evaluator it is a value of 0, and 0 resolves every firing "&gt; threshold" rule:
/// an Ok notification to every channel and an Ok state persisted, for an incident that is still
/// burning, on every restart. Hosted services stop in REVERSE registration order, and the
/// evaluator was registered before tracing and metrics, so it was the last of the three to stop
/// and ticked for the whole of both teardowns (the trace one waits up to 30 s for a compaction).</para>
/// </summary>
public sealed class AlertShutdownTests
{
    private const int Spans = 20;

    /// <summary>
    /// The order itself, read off the container the way the host reads it: <c>Host.StopAsync</c>
    /// walks <c>IEnumerable&lt;IHostedService&gt;</c> backwards, so the evaluator must come AFTER
    /// every hosted service that tears down an engine it reads.
    /// </summary>
    [Fact]
    public void The_alert_evaluator_stops_before_the_engines_it_reads()
    {
        using var factory = new AmetoWebAppFactory();
        var names = factory.Services.GetServices<IHostedService>()
                                    .Select(static s => s.GetType().Name)
                                    .ToList();

        int alerts = names.IndexOf("AlertsHostedService");
        Assert.True(alerts >= 0, $"no AlertsHostedService among {string.Join(", ", names)}");

        foreach (string engine in (string[])["TraceStorageHostedService", "MetricStorageHostedService"])
        {
            int at = names.IndexOf(engine);
            Assert.True(at >= 0, $"no {engine} among {string.Join(", ", names)}");
            Assert.True(alerts > at,
                $"AlertsHostedService is registered at {alerts}, before {engine} at {at}; hosted services "
              + $"stop in reverse, so the evaluator would still be ticking while that engine closes. "
              + $"Order: {string.Join(", ", names)}");
        }
    }

    /// <summary>
    /// The whole scenario, in the real host: a trace rule fires, the host stops, and at the instant
    /// the trace engine shuts its door — when every read starts answering empty — the evaluator's
    /// loop gets the tick it would get if it were still running. No timer decides whether that tick
    /// lands: the seam is the teardown's own, and the tick runs exactly when the loop is alive.
    /// </summary>
    [Fact]
    public async Task A_firing_trace_rule_is_not_resolved_by_the_trace_engine_closing_under_it()
    {
        var factory = new AmetoWebAppFactory();
        bool seamRan = false, loopAliveAtClose = false, okRecorded = false;
        AlertState? stateAtClose = null, persistedAtClose = null;
        Exception? seamFault = null;
        try
        {
            var traces    = factory.Services.GetRequiredService<TraceStorageEngine>();
            var evaluator = factory.Services.GetRequiredService<AlertEvaluator>();
            var store     = factory.Services.GetRequiredService<AlertRuleStore>();
            var persist   = factory.Services.GetRequiredService<AlertPersistence>();

            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
            for (int i = 0; i < Spans; i++)
                Assert.True(traces.WriteSpan(new SpanIngestItem
                {
                    TraceId           = new TraceId(0, (ulong)i + 1),
                    SpanId            = new SpanId((ulong)i + 1),
                    StartTimeUnixNano = now - (i + 1) * 1_000_000_000L,   // the last 20 s
                    DurationNanos     = 1_000_000,
                    Name              = "GET /orders",
                    ServiceName       = "checkout",
                    Kind              = SpanKind.Server,
                }));

            var rule = new AlertRule
            {
                Id          = "checkout-spans",
                Name        = "checkout spans",
                Source      = AlertSource.Trace,
                Service     = "checkout",
                TraceMetric = TraceMetricKind.SpanCount,
                Comparator  = AlertComparator.GreaterThan,
                Threshold   = Spans - 1,
                Window      = TimeSpan.FromHours(1),
                For         = TimeSpan.Zero,
                Cooldown    = TimeSpan.Zero,
            };
            store.Upsert(rule);

            await evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, StateOf(evaluator, rule.Id));

            traces._onWritesClosedForTest = () =>
            {
                try
                {
                    seamRan = true;
                    loopAliveAtClose = !evaluator.LoopEndedForTest;
                    if (loopAliveAtClose) evaluator.EvaluateOnceAsync().GetAwaiter().GetResult();

                    stateAtClose     = StateOf(evaluator, rule.Id);
                    persistedAtClose = persist.LoadStates().Single(s => s.RuleId == rule.Id).State;
                    okRecorded       = evaluator.GetHistory().Any(h => h.RuleId == rule.Id && h.State == AlertState.Ok);
                }
                catch (Exception ex) { seamFault = ex; }
            };
        }
        finally
        {
            factory.Dispose();   // stops the host: every hosted service, in reverse registration order
        }

        Assert.Null(seamFault);
        Assert.True(seamRan, "the trace engine's teardown never reached the close");
        Assert.Equal(AlertState.Firing, stateAtClose);
        Assert.Equal(AlertState.Firing, persistedAtClose);
        Assert.False(okRecorded, "an Ok transition was recorded — and dispatched — for an incident nobody resolved");
    }

    // ── The evaluator's own guard: defence in depth under the order ─────────────────────────
    //
    // The order above is Program.cs's to keep. These hold for any host: once ApplicationStopping
    // has fired, a value the evaluator computes is not applied — so an engine closing under it
    // cannot flip a state whatever order the services stop in.

    /// <summary>
    /// THE RACE THAT MATTERS: a tick that began while the host was running, and read its answer
    /// after the host had begun stopping and the engine had closed. The stats provider stands in
    /// for the engine and stops the host from INSIDE the read, so the interleaving is the seam's,
    /// not a timer's. A guard that only checked at the top of a cycle passes a tick like this.
    /// </summary>
    [Fact]
    public async Task A_value_read_after_the_host_began_stopping_is_not_applied()
    {
        await using var rig = await EvaluatorRig.StartAsync();
        rig.Stats.OnRead = () =>
        {
            rig.Lifetime.StopApplication();   // ApplicationStopping, then the engine shuts its door
            rig.Stats.Closed = true;
        };

        await rig.Evaluator.EvaluateOnceAsync();

        Assert.Null(rig.Stats.OnRead);   // the read ran, and the host stopped inside it
        rig.AssertStillFiring();
    }

    /// <summary>A tick that starts after the host began stopping changes nothing either.</summary>
    [Fact]
    public async Task A_cycle_that_starts_after_the_host_began_stopping_changes_nothing()
    {
        await using var rig = await EvaluatorRig.StartAsync();
        rig.Lifetime.StopApplication();
        rig.Stats.Closed = true;

        await rig.Evaluator.EvaluateOnceAsync();

        rig.AssertStillFiring();
    }

    private static AlertState StateOf(AlertEvaluator evaluator, string ruleId) =>
        evaluator.GetStates().Single(s => s.RuleId == ruleId).State;

    /// <summary>
    /// An evaluator behind a real <see cref="AlertsHostedService"/> and a real
    /// <see cref="Microsoft.Extensions.Hosting.Internal.ApplicationLifetime"/>, with one trace rule
    /// already FIRING against a stand-in engine that answers <see cref="Spans"/> spans until it is
    /// closed and nothing after.
    /// </summary>
    private sealed class EvaluatorRig : IAsyncDisposable
    {
        private readonly string _dir = Path.Combine(Path.GetTempPath(), "Ameto-alertstop-" + Guid.NewGuid().ToString("N"));

        public FakeTraceStats Stats { get; } = new();
        public Microsoft.Extensions.Hosting.Internal.ApplicationLifetime Lifetime { get; } =
            new(Microsoft.Extensions.Logging.Abstractions.NullLogger<Microsoft.Extensions.Hosting.Internal.ApplicationLifetime>.Instance);
        public AlertEvaluator Evaluator { get; private set; } = null!;
        public AlertRule Rule { get; private set; } = null!;
        private AlertsHostedService _hosted = null!;

        public static async Task<EvaluatorRig> StartAsync()
        {
            var rig = new EvaluatorRig();
            Directory.CreateDirectory(rig._dir);
            var store = new AlertRuleStore(rig._dir, new NoopProtector(),
                Microsoft.Extensions.Logging.Abstractions.NullLogger<AlertRuleStore>.Instance);
            rig.Evaluator = new AlertEvaluator(
                store,
                new AlertDispatcher(Microsoft.Extensions.Logging.Abstractions.NullLogger<AlertDispatcher>.Instance),
                new AlertPersistence(rig._dir, Microsoft.Extensions.Logging.Abstractions.NullLogger<AlertPersistence>.Instance),
                AlertHeaderCountTests.ThrowingProxy.For<Ameto.Core.IQueryExecutor>(),
                null!,   // the log engine: a trace rule never reaches it
                AlertHeaderCountTests.ThrowingProxy.For<Ameto.Metrics.IMetricAggregator>(),
                rig.Stats,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<AlertEvaluator>.Instance);
            rig._hosted = new AlertsHostedService(rig.Evaluator, rig.Lifetime);
            await rig._hosted.StartAsync(CancellationToken.None);

            rig.Rule = new AlertRule
            {
                Id          = "checkout-spans",
                Name        = "checkout spans",
                Source      = AlertSource.Trace,
                Service     = "checkout",
                TraceMetric = TraceMetricKind.SpanCount,
                Comparator  = AlertComparator.GreaterThan,
                Threshold   = Spans - 1,
                Window      = TimeSpan.FromHours(1),
                For         = TimeSpan.Zero,
                Cooldown    = TimeSpan.Zero,
            };
            store.Upsert(rig.Rule);

            await rig.Evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, StateOf(rig.Evaluator, rig.Rule.Id));
            return rig;
        }

        public void AssertStillFiring()
        {
            Assert.Equal(AlertState.Firing, StateOf(Evaluator, Rule.Id));
            Assert.DoesNotContain(Evaluator.GetHistory(), h => h.RuleId == Rule.Id && h.State == AlertState.Ok);
            var persisted = new AlertPersistence(_dir,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<AlertPersistence>.Instance).LoadStates();
            Assert.Equal(AlertState.Firing, persisted.Single(s => s.RuleId == Rule.Id).State);
        }

        public async ValueTask DisposeAsync()
        {
            await _hosted.StopAsync(CancellationToken.None);
            await _hosted.DisposeAsync();
            try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
        }
    }

    /// <summary>The trace engine as the evaluator sees it: <see cref="Spans"/> spans, or — closed — none.</summary>
    private sealed class FakeTraceStats : ITraceStatsProvider
    {
        public volatile bool Closed;
        public Action? OnRead;
        public int Reads;

        public Task<IReadOnlyList<ServiceSegmentStats>> GetAggregateStatsAsync(
            DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
        {
            Interlocked.Increment(ref Reads);
            Interlocked.Exchange(ref OnRead, null)?.Invoke();
            return Task.FromResult<IReadOnlyList<ServiceSegmentStats>>(Closed
                ? []
                : [new ServiceSegmentStats { ServiceName = "checkout", SpanCount = Spans }]);
        }
    }

    private sealed class NoopProtector : Ameto.Core.ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }
}
