using System.Diagnostics;
using System.Net;
using Ameto.Alerts;
using Ameto.Core;
using Ameto.Replication;
using Ameto.Tracing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;


namespace Ameto.Integration.Tests;

/// <summary>
/// #137: the alert evaluation loop and the peer prober died when their own error line threw — an
/// <see cref="OutOfMemoryException"/> on a stand under load — and never ran again: alerts silently
/// stopped being evaluated (and <c>DisposeAsync</c> rethrew the fault), peers stopped being probed.
/// Each fact makes the error line throw and shows the loop still turning.
/// </summary>
public sealed class LoopGuardTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-loopguard-" + Guid.NewGuid().ToString("N"));

    public LoopGuardTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        // The pool keeps Ameto.db open after the stores are gone; on Windows the delete then fails and the
        // directory stays in TEMP for ever.
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    /// <summary>A logger that throws an <see cref="OutOfMemoryException"/> from the lines a predicate picks.</summary>
    private sealed class ThrowingLogger<T>(Func<string, bool> throwOn) : Microsoft.Extensions.Logging.ILogger<T>
    {
        private int _thrown;

        public int Thrown => Volatile.Read(ref _thrown);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!throwOn(formatter(state, exception))) return;
            Interlocked.Increment(ref _thrown);
            throw new OutOfMemoryException("test: this log line ran out of memory");
        }
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail($"timed out waiting for {what}");
            await Task.Delay(25);
        }
    }

    private sealed class NoopProtector : ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }

    // ── Alert evaluator ───────────────────────────────────────────────────────

    /// <summary>
    /// A trace rule against a stats provider that throws: every cycle fails. The per-rule warning
    /// throws, so the cycle throws; the cycle's error line throws too — and the loop used to end
    /// there, after its first cycle. Now the loop goes round, and the disposal does not rethrow.
    /// </summary>
    [Fact]
    public async Task The_alert_loop_goes_round_when_its_error_line_throws()
    {
        var logger = new ThrowingLogger<AlertEvaluator>(static line =>
            line.Contains("Failed to evaluate alert rule") || line.Contains("Alert evaluation cycle failed"));
        var store  = new AlertRuleStore(_dir, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
        store.Upsert(new AlertRule
        {
            Id          = "guarded-spans",
            Name        = "guarded spans",
            Source      = AlertSource.Trace,
            Service     = "checkout",
            TraceMetric = TraceMetricKind.SpanCount,
            Comparator  = AlertComparator.GreaterThan,
            Threshold   = 1,
            Window      = TimeSpan.FromHours(1),
            For         = TimeSpan.Zero,
            Cooldown    = TimeSpan.Zero,
        });

        AlertEvaluator evaluator;
        AlertEvaluator.EvalIntervalForTest.Value = TimeSpan.FromMilliseconds(50);
        try
        {
            evaluator = new AlertEvaluator(
                store,
                new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
                new AlertPersistence(_dir, NullLogger<AlertPersistence>.Instance),
                AlertHeaderCountTests.ThrowingProxy.For<IQueryExecutor>(),
                null!,   // the log engine: a trace rule never reaches it
                AlertHeaderCountTests.ThrowingProxy.For<Ameto.Metrics.IMetricAggregator>(),
                AlertHeaderCountTests.ThrowingProxy.For<ITraceStatsProvider>(),
                logger);
        }
        finally { AlertEvaluator.EvalIntervalForTest.Value = null; }

        Exception? disposal;
        try
        {
            // Two lines throw per failed cycle (the rule's, then the cycle's); a third throw is a second
            // cycle, which the loop that died after its first never ran.
            await WaitUntil(() => logger.Thrown >= 3, TimeSpan.FromSeconds(20), "a second evaluation cycle");
            Assert.False(evaluator.LoopEndedForTest, "the evaluation loop ended");
        }
        finally   // a fact that fails must not leave a 50 ms loop running in the test host
        {
            disposal = await Record.ExceptionAsync(() => evaluator.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)));
        }
        Assert.Null(disposal);
        Assert.True(evaluator.LoopEndedForTest);
    }

    // ── Peer prober ───────────────────────────────────────────────────────────

    private sealed class CountingHandler : HttpMessageHandler
    {
        public int Requests;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Interlocked.Increment(ref Requests);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
        }
    }

    private sealed class StubFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    /// <summary>
    /// Two cycles fail before the probe and their error line throws; the loop used to end at the first
    /// (the whole loop sat in one try). Now a third cycle probes the seed.
    /// </summary>
    [Fact]
    public async Task The_peer_prober_goes_round_when_its_error_line_throws()
    {
        var handler  = new CountingHandler();
        var registry = new NodeRegistry();
        registry.EnsureKnown(new NodeId(1), "http://localhost:5341");
        var logger = new ThrowingLogger<PeerProber>(static line => line.Contains("PeerProber cycle failed"));

        var prober = new PeerProber(
            Options.Create(new ReplicationOptions
            {
                Enabled       = true,
                ProbeInterval = TimeSpan.FromMilliseconds(20),
                SeedNodes     = ["http://localhost:1"],   // something to probe, so the loop cycles
            }),
            registry, logger, new StubFactory(handler));
        prober.SetLocalNodeId(new NodeId(1));

        int cycles = 0;
        prober._beforeProbeForTest = () =>
        {
            if (Interlocked.Increment(ref cycles) <= 2) throw new InvalidOperationException("probe fault (test seam)");
        };

        // The loop runs on the pool: a cycle that fails before its first await sleeps its guarded second
        // there, not on the host's startup thread (StartAsync took two seconds here when it did).
        var sw = Stopwatch.StartNew();
        await prober.StartAsync(CancellationToken.None);
        Assert.True(sw.ElapsedMilliseconds < 500, $"StartAsync took {sw.ElapsedMilliseconds} ms: the loop ran on the caller's thread");
        try
        {
            await WaitUntil(() => logger.Thrown >= 2, TimeSpan.FromSeconds(20), "two error lines to throw");
            await WaitUntil(() => prober.ProbeCycles >= 1, TimeSpan.FromSeconds(20), "a cycle that probed");
            Assert.True(Volatile.Read(ref handler.Requests) >= 1, "the seed was never probed");
        }
        finally
        {
            await prober.StopAsync(CancellationToken.None);
            prober.Dispose();
        }
    }

    /// <summary>Every probe ends as the probe's own five-second timeout ends: a cancellation that is not the prober's.</summary>
    private sealed class TimingOutHandler : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            await Task.Yield();   // asynchronous, as a real connect is
            throw new TaskCanceledException("test: the probe's own timeout");
        }
    }

    /// <summary>
    /// A peer that accepts and never answers, or an address whose packets are dropped: the probe's own
    /// timeout fires, and HttpClient throws a cancellation that is not the prober's. On <c>main</c> that
    /// escaped the probe and ended probing for good, without a line; with the loop guarded, it failed the
    /// whole cycle as an Error every ~6 s. A failed probe like any other: the cycles go on, and no cycle
    /// is said to have failed.
    /// </summary>
    [Fact]
    public async Task A_probe_that_times_out_is_a_failed_probe_not_a_failed_cycle()
    {
        var registry = new NodeRegistry();
        registry.EnsureKnown(new NodeId(1), "http://localhost:5341");
        var logger = new ThrowingLogger<PeerProber>(static line => line.Contains("PeerProber cycle failed"));
        var prober = new PeerProber(
            Options.Create(new ReplicationOptions
            {
                Enabled = true, ProbeInterval = TimeSpan.FromMilliseconds(20), SeedNodes = ["http://localhost:1"],
            }),
            registry, logger, new StubFactory(new TimingOutHandler()));
        prober.SetLocalNodeId(new NodeId(1));

        await prober.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntil(() => prober.ProbeCycles >= 3, TimeSpan.FromSeconds(15), "three cycles past a timing-out seed");
            Assert.Equal(0, logger.Thrown);   // no cycle failed
        }
        finally
        {
            await prober.StopAsync(CancellationToken.None);
            prober.Dispose();
        }
    }
}
