using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Alerts;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// THE METRIC ENGINE'S HALF OF #95. Its teardown ends with the <c>_coldClosed</c> fence, after which
/// every query answers "no cold segments" — and the final flush has just moved the whole hot tier
/// to cold, so the answer is EMPTY. The aggregator computes on that, the alert evaluator reads the
/// aggregate as 0, and the API used to answer it with 200 and no series. The engine now says Closed
/// from that fence; the aggregator speaks for it; the evaluator and the API believe it.
/// </summary>
public sealed class MetricStoreUnavailableTests
{
    private const string Metric = "http.latency";
    private const double Peak   = 120;

    /// <summary>Available once the cold scan has run; Closed once the teardown has fenced the cold tier — and the aggregator says the same.</summary>
    [Fact]
    public async Task The_engine_and_its_aggregator_say_closed_once_the_cold_tier_is_fenced()
    {
        using var dir = new TempDir();
        var engine = new MetricStorageEngine(dir.Path, NullLogger<MetricStorageEngine>.Instance);
        var agg    = new MetricAggregator(engine);
        try
        {
            await engine.ColdLoadCompleted.WaitAsync(TimeSpan.FromSeconds(60));
            Assert.Equal(QueryAvailability.Available, engine.Availability);
            Assert.Equal(QueryAvailability.Available, agg.Availability);
            Ingest(engine);
            Assert.NotEmpty(await agg.QueryAsync(Request()));

            await engine.DisposeAsync();

            Assert.Equal(QueryAvailability.Closed, engine.Availability);
            Assert.Equal(QueryAvailability.Closed, agg.Availability);
            Assert.Empty(await agg.QueryAsync(Request()));   // the empty answer the flag explains
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// On the real engine, closed by its own teardown with no host stopping: a Firing "&gt;" rule
    /// stays Firing and a "&lt;" rule stays Ok, with nothing dispatched. Before the engine said
    /// Closed, that tick read 0: the first resolved (Ok to every channel) and the second fired.
    /// </summary>
    [Fact]
    public async Task A_firing_metric_rule_survives_the_engine_closing_outside_a_host_stop()
    {
        using var dir = new TempDir();
        var engine = new MetricStorageEngine(Path.Combine(dir.Path, "metrics"), NullLogger<MetricStorageEngine>.Instance);
        await engine.ColdLoadCompleted.WaitAsync(TimeSpan.FromSeconds(60));
        Ingest(engine);

        string alerts = Path.Combine(dir.Path, "alerts");
        Directory.CreateDirectory(alerts);
        var store = new AlertRuleStore(alerts, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
        var dispatched = new ConcurrentQueue<AlertFiredEvent>();
        var evaluator = new AlertEvaluator(
            store,
            new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
            new AlertPersistence(alerts, NullLogger<AlertPersistence>.Instance),
            AlertHeaderCountTests.ThrowingProxy.For<IQueryExecutor>(),
            null!,
            new MetricAggregator(engine),
            AlertHeaderCountTests.ThrowingProxy.For<Ameto.Tracing.ITraceStatsProvider>(),
            NullLogger<AlertEvaluator>.Instance);
        evaluator._onDispatchForTest = dispatched.Enqueue;
        try
        {
            store.Upsert(Rule("above", AlertComparator.GreaterThan, Peak - 1));
            store.Upsert(Rule("below", AlertComparator.LessThan, 5));

            await evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, StateOf(evaluator, "above"));
            Assert.Equal(AlertState.Ok,     StateOf(evaluator, "below"));
            Assert.Single(dispatched);

            await engine.DisposeAsync();   // closed; nothing is stopping
            await evaluator.EvaluateOnceAsync();

            Assert.Equal(AlertState.Firing, StateOf(evaluator, "above"));
            Assert.Equal(AlertState.Ok,     StateOf(evaluator, "below"));
            Assert.Single(dispatched);
            Assert.DoesNotContain(evaluator.GetHistory(), h => h.State == AlertState.Ok);
            Assert.Equal(AlertState.Firing, new AlertPersistence(alerts, NullLogger<AlertPersistence>.Instance)
                .LoadStates().Single(s => s.RuleId == "above").State);
        }
        finally
        {
            await evaluator.DisposeAsync();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// THE METRIC ENGINE'S LOADING WINDOW, as a restart produces it: the final flush put every
    /// point in a cold segment, and until the new engine's cold scan has run its queries answer from
    /// an empty hot tier. The rule was Firing at 120; read then, it is 0, and a "&gt;" rule resolves
    /// with an Ok. The engine says Loading until the scan ends, so the tick is skipped — and once the
    /// scan has run the rule reads 120 again, still Firing, with no second notification.
    /// </summary>
    [Fact]
    public async Task A_restart_does_not_resolve_a_firing_metric_rule_before_the_cold_tier_has_loaded()
    {
        using var dir = new TempDir();
        string metrics = Path.Combine(dir.Path, "metrics"), alerts = Path.Combine(dir.Path, "alerts");
        Directory.CreateDirectory(alerts);

        var before = new MetricStorageEngine(metrics, NullLogger<MetricStorageEngine>.Instance);
        await before.ColdLoadCompleted.WaitAsync(TimeSpan.FromSeconds(60));
        Ingest(before);
        var first = NewEvaluator(alerts, before, out _, out var store);
        store.Upsert(Rule("above", AlertComparator.GreaterThan, Peak - 1));
        await first.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, StateOf(first, "above"));
        await first.DisposeAsync();
        await before.DisposeAsync();   // the final flush: every point is now cold

        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        MetricStorageEngine.HoldColdLoadForTest.Value = hold.Task;
        MetricStorageEngine after;
        try { after = new MetricStorageEngine(metrics, NullLogger<MetricStorageEngine>.Instance); }
        finally { MetricStorageEngine.HoldColdLoadForTest.Value = null; }

        var evaluator = NewEvaluator(alerts, after, out var dispatched, out _);
        try
        {
            Assert.Equal(QueryAvailability.Loading, after.Availability);
            Assert.Empty(await new MetricAggregator(after).QueryAsync(Request()));   // the part a loading store gives

            await evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, StateOf(evaluator, "above"));
            Assert.Empty(dispatched);

            hold.SetResult();
            await after.ColdLoadCompleted.WaitAsync(TimeSpan.FromSeconds(60));
            await evaluator.EvaluateOnceAsync();

            Assert.Equal(AlertState.Firing, StateOf(evaluator, "above"));
            Assert.Equal(Peak, evaluator.GetStates().Single(s => s.RuleId == "above").LastValue);
            Assert.Empty(dispatched);
            Assert.DoesNotContain(evaluator.GetHistory(), h => h.State == AlertState.Ok);
        }
        finally
        {
            hold.TrySetResult();
            await evaluator.DisposeAsync();
            await after.DisposeAsync();
        }
    }

    private static AlertEvaluator NewEvaluator(
        string alerts, MetricStorageEngine engine,
        out ConcurrentQueue<AlertFiredEvent> dispatched, out AlertRuleStore store)
    {
        store = new AlertRuleStore(alerts, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
        var sent = new ConcurrentQueue<AlertFiredEvent>();
        var evaluator = new AlertEvaluator(
            store,
            new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
            new AlertPersistence(alerts, NullLogger<AlertPersistence>.Instance),
            AlertHeaderCountTests.ThrowingProxy.For<IQueryExecutor>(),
            null!,
            new MetricAggregator(engine),
            AlertHeaderCountTests.ThrowingProxy.For<Ameto.Tracing.ITraceStatsProvider>(),
            NullLogger<AlertEvaluator>.Instance);
        evaluator._onDispatchForTest = sent.Enqueue;
        dispatched = sent;
        return evaluator;
    }

    /// <summary>
    /// Every metric query the client makes answers 200 while the store is open and 503 — with a
    /// sentence — once it has closed, where it used to answer 200 with no series. The alert preview
    /// of a metric rule answers 503 too. The catalog and the name list are NOT refused: they are
    /// in-memory metadata the teardown does not close, and they still answer truly.
    /// </summary>
    [Fact]
    public async Task The_query_api_answers_503_once_the_store_has_closed_and_200_before()
    {
        using var factory = new AmetoWebAppFactory();
        var client = factory.CreateClient();
        var engine = factory.Services.GetRequiredService<MetricStorageEngine>();
        await engine.ColdLoadCompleted.WaitAsync(TimeSpan.FromSeconds(60));
        Ingest(engine);

        foreach (var req in Requests())
        {
            using var res = await SendAsync(client, req);
            Assert.True(res.StatusCode == HttpStatusCode.OK, $"{req.Method} {req.Url}: {(int)res.StatusCode} before the close");
        }

        await engine.DisposeAsync();   // the store closes; the host is not stopping
        Assert.Equal(QueryAvailability.Closed, engine.Availability);

        foreach (var req in Requests())
        {
            using var res = await SendAsync(client, req);
            string body = await res.Content.ReadAsStringAsync();
            Assert.True(res.StatusCode == HttpStatusCode.ServiceUnavailable,
                $"{req.Method} {req.Url}: {(int)res.StatusCode} after the close — {body}");
            Assert.Contains(req.Url.StartsWith("/api/alerts", StringComparison.Ordinal) ? "shut down" : "metric store has shut down", body);
            Assert.Null(res.Headers.RetryAfter);   // Closed is final: nothing to retry until a restart
        }

        // Metadata, not data: the teardown does not close it, and the whole name list is still in it.
        using var catalog = await client.GetAsync("/api/metrics/catalog");
        Assert.Equal(HttpStatusCode.OK, catalog.StatusCode);
        using var names = await client.GetAsync("/api/metrics/names");
        Assert.Equal(HttpStatusCode.OK, names.StatusCode);
        Assert.Contains(Metric, await names.Content.ReadAsStringAsync());
    }

    private static IEnumerable<(string Method, string Url, object? Body)> Requests()
    {
        var q = new { metric = Metric, aggregation = "max" };
        return
        [
            ("GET",  $"/api/metrics/{Metric}",          null),
            ("GET",  $"/api/metrics/{Metric}/heatmap",  null),
            ("POST", "/api/metrics/query",              q),
            ("POST", "/api/metrics/expr",               new { left = q, right = q, op = "div" }),
            ("POST", "/api/alerts/preview", new
            {
                name = "preview", source = "Metric", metric = Metric, aggregation = "max",
                comparator = "GreaterThan", threshold = 1, windowSeconds = 3600,
            }),
        ];
    }

    private static Task<HttpResponseMessage> SendAsync(HttpClient client, (string Method, string Url, object? Body) req) =>
        req.Method == "POST" ? client.PostAsJsonAsync(req.Url, req.Body) : client.GetAsync(req.Url);

    // ── Helpers ───────────────────────────────────────────────────────────────────────────────

    /// <summary>One gauge series peaking at <see cref="Peak"/>, in the last minute.</summary>
    private static void Ingest(IMetricIngester engine)
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        MetricIngestItem[] points =
        [
            new() { Name = Metric, Kind = MetricKind.Gauge, TimestampUnixNano = now - 30_000_000_000L, ScalarValue = 40 },
            new() { Name = Metric, Kind = MetricKind.Gauge, TimestampUnixNano = now - 20_000_000_000L, ScalarValue = Peak },
            new() { Name = Metric, Kind = MetricKind.Gauge, TimestampUnixNano = now - 10_000_000_000L, ScalarValue = 60 },
        ];
        Assert.Equal(0, engine.Ingest(points));
    }

    private static MetricQueryRequest Request() => new()
    {
        Metric = Metric, From = DateTimeOffset.UtcNow.AddHours(-1), To = DateTimeOffset.UtcNow,
        Aggregation = MetricAggregation.Max,
    };

    private static AlertRule Rule(string id, AlertComparator cmp, double threshold) => new()
    {
        Id = id, Name = id, Source = AlertSource.Metric, Metric = Metric, Aggregation = "max",
        Comparator = cmp, Threshold = threshold,
        Window = TimeSpan.FromHours(1), For = TimeSpan.Zero, Cooldown = TimeSpan.Zero,
    };

    private static AlertState StateOf(AlertEvaluator evaluator, string id) =>
        evaluator.GetStates().Single(s => s.RuleId == id).State;

    private sealed class TempDir : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "Ameto-metricstore-" + Guid.NewGuid().ToString("N"));
        public TempDir() => Directory.CreateDirectory(Path);
        public void Dispose() { try { Directory.Delete(Path, recursive: true); } catch { /* best effort */ } }
    }

    private sealed class NoopProtector : ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }
}
