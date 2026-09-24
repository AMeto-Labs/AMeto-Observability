using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Alerts;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;
using EventId  = Microsoft.Extensions.Logging.EventId;

namespace Ameto.Integration.Tests;

/// <summary>
/// THE LOG ENGINE'S HALF OF #95 — the same contract as the trace and metric engines, over a store
/// that fails differently.
///
/// <para><b>Closed</b>, the log engine never answered empty: once its teardown has collected its
/// hot tiers, its reader snapshot THROWS. A firing rule was therefore never resolved by a close —
/// but every rule over the store logged "Failed to evaluate" with a stack trace on every tick for
/// as long as anything kept ticking, and the search and count endpoints answered with a dead stream
/// and a 500. <b>Loading</b> is the store's real hole: until the constructor's catalog scan ends, a
/// count covers only the segments registered so far — after a restart, whose final flush put
/// everything on disk, none — and a firing "&gt;" rule resolved on it.</para>
/// </summary>
public sealed class LogStoreUnavailableTests
{
    private const int Events = 90;   // three flushes of 30: three cold segments

    // ── Closed ────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The log engine closed by its own teardown, no host stopping: the Firing "&gt;" rule and a
    /// "&lt;" rule are left exactly as they were over two ticks, nothing is dispatched, and the
    /// operator gets ONE line saying why — not a "Failed to evaluate" per rule per tick. (The state
    /// half held before #95 too, because the closed engine throws; the log half did not.)
    /// </summary>
    [Fact]
    public async Task A_closed_log_store_leaves_its_rules_alone_and_says_so_once()
    {
        using var dir = new TempDir();
        var engine = await EngineWithEventsAsync(dir.Data);
        await using var rig = new EvaluatorRig(dir.Alerts, engine, new UnusedExecutor());
        rig.Rule("above", AlertComparator.GreaterOrEqual, Events);
        rig.Rule("below", AlertComparator.LessThan, 5);

        await rig.Evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, rig.StateOf("above"));
        Assert.Equal(AlertState.Ok,     rig.StateOf("below"));
        Assert.Single(rig.Dispatched);

        await engine.DisposeAsync();   // closed; nothing is stopping
        Assert.Equal(QueryAvailability.Closed, engine.Availability);
        await rig.Evaluator.EvaluateOnceAsync();
        await rig.Evaluator.EvaluateOnceAsync();

        Assert.Equal(AlertState.Firing, rig.StateOf("above"));
        Assert.Equal(AlertState.Ok,     rig.StateOf("below"));
        Assert.Single(rig.Dispatched);
        Assert.Equal(AlertState.Firing, rig.PersistedStateOf("above"));
        Assert.DoesNotContain(rig.Log.Lines, l => l.Message.Contains("Failed to evaluate"));
        Assert.Single(rig.Log.Lines, l => l.Level == LogLevel.Warning && l.Message.Contains("Log store is Closed"));
    }

    /// <summary>
    /// THE CLOSE INSIDE THE READ. The store is open when the tick asks, and its teardown runs inside
    /// the scan, whose next step meets the closed snapshot and throws — the real engine's own
    /// <see cref="ObjectDisposedException"/>. That is the Closed answer arriving by exception, and
    /// it is treated as one: the rule is left alone, and no failure is reported.
    /// </summary>
    [Fact]
    public async Task A_store_that_closes_inside_the_scan_is_the_closed_answer_not_a_failure()
    {
        using var dir = new TempDir();
        var engine   = await EngineWithEventsAsync(dir.Data);
        var executor = new ClosingExecutor(engine);
        await using var rig = new EvaluatorRig(dir.Alerts, engine, executor);
        rig.Rule("scan", AlertComparator.GreaterOrEqual, ClosingExecutor.Found, filter: "k = 0");

        await rig.Evaluator.EvaluateOnceAsync();
        Assert.Equal(AlertState.Firing, rig.StateOf("scan"));

        executor.CloseInside = true;
        await rig.Evaluator.EvaluateOnceAsync();

        Assert.True(executor.Threw, "the scan never met the closed engine");
        Assert.Equal(AlertState.Firing, rig.StateOf("scan"));
        Assert.Single(rig.Dispatched);
        Assert.DoesNotContain(rig.Log.Lines, l => l.Message.Contains("Failed to evaluate"));
        Assert.Single(rig.Log.Lines, l => l.Message.Contains("Log store is Closed"));
    }

    // ── Loading ───────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// THE RESTART. The rule fired over 90 events; the engine stopped (its final flush put them all
    /// in cold segments) and a new one started over the same directory with its catalog scan held
    /// open — the first ticks of a large install. Its count is 0, and before #95 that tick resolved
    /// the rule and sent Ok. Now it is skipped until the scan ends, and then the rule reads 90 again,
    /// still Firing, with no second notification.
    /// </summary>
    [Fact]
    public async Task A_restart_does_not_resolve_a_firing_log_rule_before_the_catalog_has_loaded()
    {
        using var dir = new TempDir();
        var before = await EngineWithEventsAsync(dir.Data);
        await using (var rig = new EvaluatorRig(dir.Alerts, before, new UnusedExecutor()))
        {
            rig.Rule("above", AlertComparator.GreaterOrEqual, Events);
            await rig.Evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, rig.StateOf("above"));
        }
        await before.DisposeAsync();

        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        StorageEngine.HoldCatalogScanForTest.Value = hold.Task;
        StorageEngine after;
        try { after = NewEngine(dir.Data); }
        finally { StorageEngine.HoldCatalogScanForTest.Value = null; }

        try
        {
            Assert.Equal(QueryAvailability.Loading, after.Availability);
            var partial = await after.AggregateLogVolumeAsync(
                DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow, 0, 3600, 1, null);
            Assert.Equal(0L, partial.Total);   // the answer a loading store gives: none of the 90

            await using var rig = new EvaluatorRig(dir.Alerts, after, new UnusedExecutor());
            await rig.Evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, rig.StateOf("above"));
            Assert.Empty(rig.Dispatched);
            Assert.Single(rig.Log.Lines, l => l.Message.Contains("Log store is Loading"));

            hold.SetResult();
            await after.CatalogLoaded.WaitAsync(TimeSpan.FromSeconds(60));
            Assert.Equal(QueryAvailability.Available, after.Availability);

            await rig.Evaluator.EvaluateOnceAsync();
            Assert.Equal(AlertState.Firing, rig.StateOf("above"));
            Assert.Equal(Events, rig.Evaluator.GetStates().Single(s => s.RuleId == "above").LastValue);
            Assert.Empty(rig.Dispatched);
            Assert.DoesNotContain(rig.Evaluator.GetHistory(), h => h.State == AlertState.Ok);
        }
        finally
        {
            hold.TrySetResult();
            await after.DisposeAsync();
        }
    }

    // ── The API ───────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The endpoints the evaluator's two reads stand behind — the search (<c>GET /api/events</c>)
    /// and the counts (<c>GET /api/events/counts</c>) — answer 200 while the log store is open and
    /// 503 with a sentence once it has closed, where they used to answer a stream that died and a
    /// 500 (or, for the counts, the last answer still in its cache). The preview of a log rule
    /// answers 503 as well. The store is closed by its own teardown while the host keeps serving.
    /// </summary>
    [Fact]
    public async Task The_search_and_counts_api_answer_503_once_the_store_has_closed_and_200_before()
    {
        using var factory = new AmetoWebAppFactory();
        var client  = factory.CreateClient();
        var storage = factory.Services.GetRequiredService<StorageEngine>();
        await storage.CatalogLoaded.WaitAsync(TimeSpan.FromSeconds(60));

        foreach (var req in Requests())
        {
            using var res = await SendAsync(client, req);
            Assert.True(res.StatusCode == HttpStatusCode.OK, $"{req.Method} {req.Url}: {(int)res.StatusCode} before the close");
        }

        await storage.DisposeAsync();   // the store closes; the host is not stopping
        Assert.Equal(QueryAvailability.Closed, storage.Availability);

        foreach (var req in Requests())
        {
            using var res = await SendAsync(client, req);
            string body = await res.Content.ReadAsStringAsync();
            Assert.True(res.StatusCode == HttpStatusCode.ServiceUnavailable,
                $"{req.Method} {req.Url}: {(int)res.StatusCode} after the close — {body}");
            Assert.Contains(req.Url.StartsWith("/api/alerts", StringComparison.Ordinal) ? "shut down" : "log store has shut down", body);
            Assert.Equal("5", res.Headers.RetryAfter?.ToString());
        }
    }

    private static IEnumerable<(string Method, string Url, object? Body)> Requests() =>
    [
        ("GET",  "/api/events/counts", null),
        ("GET",  "/api/events",        null),
        ("POST", "/api/alerts/preview", new
        {
            name = "preview", source = "Log", comparator = "GreaterThan", threshold = 1, windowSeconds = 3600,
        }),
    ];

    /// <summary>Headers only for the search stream: the status line is the whole question.</summary>
    private static Task<HttpResponseMessage> SendAsync(HttpClient client, (string Method, string Url, object? Body) req) =>
        req.Method == "POST"
            ? client.PostAsJsonAsync(req.Url, req.Body)
            : client.GetAsync(req.Url, HttpCompletionOption.ResponseHeadersRead);

    // ── Helpers ───────────────────────────────────────────────────────────────────────────────

    private static StorageEngine NewEngine(string dataDir)
    {
        var opts   = new ServerOptions { DataDirectory = dataDir };
        var engine = new StorageEngine(
            Options.Create(opts), new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        return engine;
    }

    /// <summary><see cref="Events"/> events of "checkout", ten minutes ago, in three cold segments; the catalog scan has ended.</summary>
    private static async Task<StorageEngine> EngineWithEventsAsync(string dataDir)
    {
        var engine = NewEngine(dataDir);
        await engine.CatalogLoaded.WaitAsync(TimeSpan.FromSeconds(60));

        long baseTicks = DateTimeOffset.UtcNow.AddMinutes(-10).UtcTicks;
        byte[] payload = [0x81, 0xA1, (byte)'k', 0x00];    // msgpack {"k": 0}
        for (int i = 0; i < Events; i++)
        {
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {k}"),
                ServiceNamePoolIndex     = engine.TemplatePool.Intern("checkout"),
            }, payload));
            if (i % 30 == 29) await engine.FlushHotTierAsync();
        }
        return engine;
    }

    /// <summary>An evaluator over a real log engine with no host and no lifetime; dispatches and log lines are captured.</summary>
    private sealed class EvaluatorRig : IAsyncDisposable
    {
        private readonly string _dir;
        private readonly AlertRuleStore _store;
        public AlertEvaluator Evaluator { get; }
        public CapturingLogger Log { get; } = new();
        public ConcurrentQueue<AlertFiredEvent> Dispatched { get; } = new();

        public EvaluatorRig(string dir, StorageEngine logs, IQueryExecutor executor)
        {
            _dir = dir;
            Directory.CreateDirectory(dir);
            _store = new AlertRuleStore(dir, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
            Evaluator = new AlertEvaluator(
                _store,
                new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
                new AlertPersistence(dir, NullLogger<AlertPersistence>.Instance),
                executor,
                logs,
                AlertHeaderCountTests.ThrowingProxy.For<Ameto.Metrics.IMetricAggregator>(),
                AlertHeaderCountTests.ThrowingProxy.For<Ameto.Tracing.ITraceStatsProvider>(),
                Log);
            Evaluator._onDispatchForTest = Dispatched.Enqueue;
        }

        /// <summary>A log rule over the last hour. No filter: counted from headers, the evaluator's cheap path.</summary>
        public void Rule(string id, AlertComparator cmp, double threshold, string? filter = null) =>
            _store.Upsert(new AlertRule
            {
                Id = id, Name = id, Source = AlertSource.Log, Filter = filter,
                Comparator = cmp, Threshold = threshold,
                Window = TimeSpan.FromHours(1), For = TimeSpan.Zero, Cooldown = TimeSpan.Zero,
            });

        public AlertState StateOf(string id) => Evaluator.GetStates().Single(s => s.RuleId == id).State;

        public AlertState PersistedStateOf(string id) =>
            new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance).LoadStates().Single(s => s.RuleId == id).State;

        public ValueTask DisposeAsync() => Evaluator.DisposeAsync();
    }

    /// <summary>
    /// The scan path, reduced to what these tests need of it: <see cref="Found"/> matches while the
    /// engine is open; with <see cref="CloseInside"/> set, the engine's teardown runs INSIDE the
    /// scan and the scan's next step is the real engine's own — opening a hot-tier reader, which a
    /// closed engine refuses by throwing.
    /// </summary>
    private sealed class ClosingExecutor(StorageEngine engine) : IQueryExecutor
    {
        public const int Found = 5;
        public volatile bool CloseInside;
        public volatile bool Threw;

        public async IAsyncEnumerable<LogEvent> ExecuteAsync(
            QueryRequest request, [EnumeratorCancellation] CancellationToken ct = default)
        {
            if (CloseInside)
            {
                await engine.DisposeAsync();
                try { using var _ = engine.OpenHotTierReader(); }
                catch (ObjectDisposedException) { Threw = true; throw; }
            }
            for (int i = 0; i < Found; i++) yield return null!;   // the evaluator only counts
        }
    }

    private sealed class UnusedExecutor : IQueryExecutor
    {
        public IAsyncEnumerable<LogEvent> ExecuteAsync(QueryRequest request, CancellationToken ct = default) =>
            throw new InvalidOperationException("a level-less log rule must be counted from headers, not scanned");
    }

    private sealed class CapturingLogger : ILogger<AlertEvaluator>
    {
        public readonly ConcurrentQueue<(LogLevel Level, string Message)> Lines = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Lines.Enqueue((logLevel, formatter(state, exception)));
    }

    private sealed class TempDir : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "Ameto-logstore-" + Guid.NewGuid().ToString("N"));
        public string Data   => System.IO.Path.Combine(Path, "data");
        public string Alerts => System.IO.Path.Combine(Path, "alerts");
        public TempDir() { Directory.CreateDirectory(Data); Directory.CreateDirectory(Alerts); }
        public void Dispose() { try { Directory.Delete(Path, recursive: true); } catch { /* best effort */ } }
    }

    private sealed class NoopProtector : ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }
}
