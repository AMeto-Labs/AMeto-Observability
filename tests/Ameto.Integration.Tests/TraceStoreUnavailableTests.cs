using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Alerts;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// THE TRACE ENGINE'S HALF OF #95: it says when it cannot answer, and the two callers that turn an
/// answer into a statement — the alert evaluator and the read API — believe it.
///
/// <para>The engine answers every read with an EMPTY result once its teardown has shut the door,
/// and with its hot tier alone until the background cold scan has run. Both are right for a request
/// that arrives at those moments and wrong for anything that reports the answer as a fact. Nothing
/// here stops a host: the engine is closed by calling its teardown directly, which is exactly the
/// path #84's host-stop guard does not cover.</para>
/// </summary>
public sealed class TraceStoreUnavailableTests
{
    private const int Spans = 20;

    // ── The engine ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Loading until the cold scan has run, Available after, Closed from the instant the teardown
    /// shuts the door — the same instant from which every read answers empty, asserted at that
    /// instant through the teardown's own seam.
    /// </summary>
    [Fact]
    public async Task The_engine_says_loading_then_available_then_closed_at_the_door()
    {
        using var dir = new TempDir();
        var engine = new TraceStorageEngine(dir.Path, NullLogger<TraceStorageEngine>.Instance);
        QueryAvailability? atDoor = null;
        int statsAtDoor = -1;
        try
        {
            Assert.Equal(QueryAvailability.Loading, engine.Availability);
            engine.LoadColdSegments();
            Assert.Equal(QueryAvailability.Available, engine.Availability);

            WriteSpans(engine);
            Assert.Single(await engine.GetAggregateStatsAsync(DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow));

            engine._onWritesClosedForTest = () =>
            {
                atDoor      = engine.Availability;
                statsAtDoor = engine.GetAggregateStatsAsync(DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow).Result.Count;
            };
            await engine.DisposeAsync();

            Assert.Equal(QueryAvailability.Closed, atDoor);
            Assert.Equal(0, statsAtDoor);   // the empty answer the flag exists to explain
            Assert.Equal(QueryAvailability.Closed, engine.Availability);
        }
        finally { await engine.DisposeAsync(); }
    }

    // ── The evaluator over the real engine ────────────────────────────────────────────────────

    /// <summary>
    /// THE ISSUE'S TEST, on the real engine: a trace rule fires; the engine is closed by its own
    /// teardown with no host stopping; the next tick leaves the rule Firing with nothing dispatched,
    /// and a "&lt;" rule over the same service does not fire. Before the engine said Closed, that
    /// tick read the teardown's empty answer as 0, resolved the first rule and fired the second.
    /// </summary>
    [Fact]
    public async Task A_firing_rule_survives_the_engine_closing_outside_a_host_stop()
    {
        using var dir = new TempDir();
        var engine = new TraceStorageEngine(Path.Combine(dir.Path, "traces"), NullLogger<TraceStorageEngine>.Instance);
        engine.LoadColdSegments();
        WriteSpans(engine);

        await using var rig = new EvaluatorRig(Path.Combine(dir.Path, "alerts"), engine);
        var above = rig.Rule("above", AlertComparator.GreaterThan, Spans - 1);
        var below = rig.Rule("below", AlertComparator.LessThan, 5);

        await rig.Evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, rig.StateOf(above));
        Assert.Equal(AlertState.Ok,     rig.StateOf(below));
        Assert.Single(rig.Dispatched);

        await engine.DisposeAsync();   // closed; nothing is stopping
        await rig.Evaluator.EvaluateOnceAsync();

        Assert.Equal(AlertState.Firing, rig.StateOf(above));
        Assert.Equal(AlertState.Ok,     rig.StateOf(below));
        Assert.Single(rig.Dispatched);
        Assert.DoesNotContain(rig.Evaluator.GetHistory(), h => h.State == AlertState.Ok);
        Assert.Equal(AlertState.Firing, rig.PersistedStateOf(above));
    }

    /// <summary>
    /// THE NOT-YET-LOADED HALF, as a restart produces it: the final flush put every span on disk,
    /// so the restarted engine's hot tier is empty and its reads answer nothing until the cold scan
    /// runs. The rule was Firing before the restart and must still be Firing after the first tick —
    /// which lands 15 s after start, and on a large install the scan takes longer than that.
    /// </summary>
    [Fact]
    public async Task A_restart_does_not_resolve_a_firing_rule_before_the_cold_tier_has_loaded()
    {
        using var dir = new TempDir();
        string traces = Path.Combine(dir.Path, "traces"), alerts = Path.Combine(dir.Path, "alerts");

        var before = new TraceStorageEngine(traces, NullLogger<TraceStorageEngine>.Instance);
        before.LoadColdSegments();
        WriteSpans(before);
        await using (var rig = new EvaluatorRig(alerts, before))
        {
            rig.Rule("above", AlertComparator.GreaterThan, Spans - 1);
            await rig.Evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, rig.StateOf("above"));
        }
        await before.DisposeAsync();   // the final flush: every span is now cold

        var after = new TraceStorageEngine(traces, NullLogger<TraceStorageEngine>.Instance);
        try
        {
            Assert.Empty(await after.GetAggregateStatsAsync(DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow));

            await using var rig = new EvaluatorRig(alerts, after);
            await rig.Evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, rig.StateOf("above"));
            Assert.Empty(rig.Dispatched);

            after.LoadColdSegments();
            await rig.Evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, rig.StateOf("above"));
            Assert.Equal(Spans, rig.Evaluator.GetStates().Single(s => s.RuleId == "above").LastValue);
            Assert.Empty(rig.Dispatched);   // still the same incident: no second notification
            Assert.DoesNotContain(rig.Evaluator.GetHistory(), h => h.State == AlertState.Ok);
        }
        finally { await after.DisposeAsync(); }
    }

    // ── The read API ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Every trace read the client makes answers 200 while the store is open and 503 — with a
    /// sentence, and before any stream opens — once it has closed, where it used to answer 200 with
    /// nothing (and the flame graph a 404, "trace not found"). The alert preview, which reads the
    /// same store, answers 503 instead of a 0 that "would not fire". The store is closed by its own
    /// teardown while the host keeps serving: the late request Kestrel really does deliver.
    /// </summary>
    [Fact]
    public async Task The_read_api_answers_503_once_the_store_has_closed_and_200_before()
    {
        using var factory = new AmetoWebAppFactory();
        var client = factory.CreateClient();
        var engine = factory.Services.GetRequiredService<TraceStorageEngine>();
        await engine.ColdLoadCompleted.WaitAsync(TimeSpan.FromSeconds(60));
        WriteSpans(engine);
        string id = new TraceId(0, 1).ToString();

        foreach (var req in Requests(id))
        {
            using var res = await SendAsync(client, req);
            Assert.True(res.StatusCode == HttpStatusCode.OK, $"{req.Method} {req.Url}: {(int)res.StatusCode} before the close");
        }

        await engine.DisposeAsync();   // the store closes; the host is not stopping
        Assert.Equal(QueryAvailability.Closed, engine.Availability);

        foreach (var req in Requests(id))
        {
            using var res = await SendAsync(client, req);
            string body = await res.Content.ReadAsStringAsync();
            Assert.True(res.StatusCode == HttpStatusCode.ServiceUnavailable,
                $"{req.Method} {req.Url}: {(int)res.StatusCode} after the close — {body}");
            Assert.Contains("\"error\":", body);
            Assert.Contains(req.Url.StartsWith("/api/alerts", StringComparison.Ordinal) ? "shut down" : "trace store has shut down", body);
            Assert.Equal("5", res.Headers.RetryAfter?.ToString());
        }
    }

    private static IEnumerable<(string Method, string Url, object? Body)> Requests(string id) =>
    [
        ("GET",  "/api/traces/stats",                         null),
        ("GET",  "/api/traces",                               null),
        ("GET",  "/api/traces/latency",                       null),
        ("GET",  "/api/traces/service-graph",                 null),
        ("GET",  $"/api/traces/{id}",                         null),
        ("GET",  $"/api/traces/{id}/flamegraph",              null),
        ("GET",  $"/api/traces/compare?a={id}&b={id}",        null),
        ("GET",  "/api/traces/stream",                        null),
        ("GET",  "/api/traces/query/stream?ql=%7B%20duration%20%3E%200s%20%7D", null),
        ("POST", "/api/traces/query",                         new { query = "{ duration > 0s }" }),
        ("POST", "/api/alerts/preview", new
        {
            name = "preview", source = "Trace", service = "checkout", traceMetric = "SpanCount",
            comparator = "GreaterThan", threshold = 1, windowSeconds = 3600,
        }),
    ];

    /// <summary>Headers only for the streams: the status line is the whole question, and a stream never ends on its own.</summary>
    private static Task<HttpResponseMessage> SendAsync(HttpClient client, (string Method, string Url, object? Body) req) =>
        req.Method == "POST"
            ? client.PostAsJsonAsync(req.Url, req.Body)
            : client.GetAsync(req.Url, HttpCompletionOption.ResponseHeadersRead);

    // ── Helpers ───────────────────────────────────────────────────────────────────────────────

    /// <summary><see cref="Spans"/> spans of "checkout" in the last 20 s; the first is trace 0…01.</summary>
    private static void WriteSpans(TraceStorageEngine engine)
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < Spans; i++)
            Assert.True(engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0, (ulong)i + 1),
                SpanId            = new SpanId((ulong)i + 1),
                StartTimeUnixNano = now - (i + 1) * 1_000_000_000L,
                DurationNanos     = 1_000_000,
                Name              = "GET /orders",
                ServiceName       = "checkout",
                Kind              = SpanKind.Server,
            }));
    }

    /// <summary>An evaluator over <paramref name="traces"/> with no host and no lifetime; dispatches are counted, not sent.</summary>
    private sealed class EvaluatorRig : IAsyncDisposable
    {
        private readonly string _dir;
        private readonly AlertRuleStore _store;
        public AlertEvaluator Evaluator { get; }
        public ConcurrentQueue<AlertFiredEvent> Dispatched { get; } = new();

        public EvaluatorRig(string dir, ITraceStatsProvider traces)
        {
            _dir = dir;
            Directory.CreateDirectory(dir);
            _store = new AlertRuleStore(dir, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
            Evaluator = new AlertEvaluator(
                _store,
                new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
                new AlertPersistence(dir, NullLogger<AlertPersistence>.Instance),
                AlertHeaderCountTests.ThrowingProxy.For<IQueryExecutor>(),
                null!,
                AlertHeaderCountTests.ThrowingProxy.For<Ameto.Metrics.IMetricAggregator>(),
                traces,
                NullLogger<AlertEvaluator>.Instance);
            Evaluator._onDispatchForTest = Dispatched.Enqueue;
        }

        public string Rule(string id, AlertComparator cmp, double threshold)
        {
            _store.Upsert(new AlertRule
            {
                Id = id, Name = id, Source = AlertSource.Trace, Service = "checkout",
                TraceMetric = TraceMetricKind.SpanCount, Comparator = cmp, Threshold = threshold,
                Window = TimeSpan.FromHours(1), For = TimeSpan.Zero, Cooldown = TimeSpan.Zero,
            });
            return id;
        }

        public AlertState StateOf(string id) => Evaluator.GetStates().Single(s => s.RuleId == id).State;

        public AlertState PersistedStateOf(string id) =>
            new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance).LoadStates().Single(s => s.RuleId == id).State;

        public ValueTask DisposeAsync() => Evaluator.DisposeAsync();
    }

    private sealed class TempDir : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "Ameto-tracestore-" + Guid.NewGuid().ToString("N"));
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
