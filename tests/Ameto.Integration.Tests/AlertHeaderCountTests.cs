using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Alerts;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// The alert evaluator's header-count path: a log rule that constrains no level ("at least N
/// events in the last hour", optionally for one service) is counted from event headers rather
/// than scanned. Two behaviours of that path had nothing pinning them at evaluator level:
///
/// <list type="bullet">
/// <item>With no service filter, a cold segment lying entirely inside the window is counted from
/// its catalog entry without opening the file. The storage-level test calls StorageEngine
/// directly, so an evaluator that stopped asking for the shortcut stayed green.</item>
/// <item>The path has no per-rule budget. When it briefly had one, a rule whose count took longer
/// than the budget was cancelled every cycle with no partial count to fall back on, so it never
/// changed state and could never fire. The scan path keeps its budget because it has a floor.</item>
/// </list>
///
/// <para>Each test drives one evaluation cycle through the internal hook, over cold segments
/// written a few minutes ago so they lie inside a one-hour window.</para>
/// </summary>
public sealed class AlertHeaderCountTests : IAsyncLifetime
{
    private const int Events = 90;   // three flushes of 30, so three cold segments

    private readonly string _root = Path.Combine(Path.GetTempPath(), "Ameto-alertheader-" + Guid.NewGuid().ToString("N"));
    private StorageEngine  _engine    = null!;
    private AlertRuleStore _store     = null!;
    private AlertEvaluator _evaluator = null!;

    public async Task InitializeAsync()
    {
        string dataDir  = Path.Combine(_root, "data");
        string alertDir = Path.Combine(_root, "alerts");
        Directory.CreateDirectory(dataDir);
        Directory.CreateDirectory(alertDir);

        var opts = new ServerOptions { DataDirectory = dataDir };
        _engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);

        // The evaluator does not act on a log store still scanning its catalog (#95), and the
        // engine starts that scan in the background — so the ticks below wait for it, not race it.
        await _engine.CatalogLoaded.WaitAsync(TimeSpan.FromSeconds(60));

        // Minutes ago, one second apart: every segment lies wholly inside a one-hour window.
        long baseTicks = DateTimeOffset.UtcNow.AddMinutes(-10).UtcTicks;
        byte[] payload = [0x81, 0xA1, (byte)'k', 0x00];    // msgpack {"k": 0}
        for (int i = 0; i < Events; i++)
        {
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {k}"),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("checkout"),
            }, payload));
            if (i % 30 == 29) await _engine.FlushHotTierAsync();
        }

        _store = new AlertRuleStore(alertDir, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);
        _evaluator = new AlertEvaluator(
            _store,
            new AlertDispatcher(NullLogger<AlertDispatcher>.Instance),
            new AlertPersistence(alertDir, NullLogger<AlertPersistence>.Instance),
            new UnusedQueryExecutor(),
            _engine,
            ThrowingProxy.For<Ameto.Metrics.IMetricAggregator>(),
            ThrowingProxy.For<Ameto.Tracing.ITraceStatsProvider>(),
            NullLogger<AlertEvaluator>.Instance);
    }

    public async Task DisposeAsync()
    {
        await _evaluator.DisposeAsync();
        await _engine.DisposeAsync();
        try { Directory.Delete(_root, true); } catch { }
    }

    /// <summary>A level-less rule with no service filter: "at least <see cref="Events"/> events in the last hour".</summary>
    private AlertRule VolumeRule()
    {
        var rule = new AlertRule
        {
            Id         = "volume",
            Name       = "volume",
            Source     = AlertSource.Log,
            Filter     = null,
            Comparator = AlertComparator.GreaterOrEqual,
            Threshold  = Events,
            Window     = TimeSpan.FromHours(1),
            For        = TimeSpan.Zero,
            Cooldown   = TimeSpan.Zero,
        };
        _store.Upsert(rule);
        return rule;
    }

    private AlertStateSnapshot StateOf(AlertRule rule)
    {
        foreach (var s in _evaluator.GetStates())
            if (s.RuleId == rule.Id) return s;
        throw new InvalidOperationException("rule not listed");
    }

    /// <summary>
    /// Proof the evaluator really takes the catalog shortcut, not merely that the numbers agree:
    /// with the segment files renamed out from under the catalog, a decoding count loses every
    /// one of them (it skips unreadable segments) and the rule reads 0; the shortcut counts them
    /// from the catalog and the rule fires.
    /// </summary>
    [Fact]
    public async Task A_level_less_rule_without_a_service_counts_whole_segments_from_the_catalog()
    {
        var rule = VolumeRule();

        var segs = Directory.GetFiles(Path.Combine(_root, "data", "segments"), "*.seg");
        Assert.Equal(3, segs.Length);
        foreach (var p in segs) File.Move(p, p + ".hidden");
        try
        {
            await _evaluator.EvaluateOnceAsync();

            var st = StateOf(rule);
            Assert.Equal(Events, st.LastValue);
            Assert.Equal(AlertState.Firing, st.State);
        }
        finally
        {
            foreach (var p in segs) File.Move(p + ".hidden", p);
        }
    }

    /// <summary>
    /// A budget that has already run out must not stop a header-count rule from being evaluated:
    /// there is no partial count to report, so a budget here means the rule never fires.
    /// </summary>
    [Fact]
    public async Task A_level_less_rule_is_evaluated_even_when_its_budget_has_expired()
    {
        var rule = VolumeRule();
        _evaluator.RuleEvaluationBudget = TimeSpan.Zero;   // already expired before the rule starts

        await _evaluator.EvaluateOnceAsync();

        var st = StateOf(rule);
        Assert.Equal(Events, st.LastValue);
        Assert.Equal(AlertState.Firing, st.State);
        Assert.NotEqual(DateTimeOffset.MinValue, st.EvaluatedAt);
    }

    // ── Stand-ins for the sources a level-less log rule never touches ─────────────────────

    private sealed class NoopProtector : ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }

    /// <summary>The scan path. A level-less rule must never reach it.</summary>
    private sealed class UnusedQueryExecutor : IQueryExecutor
    {
        public IAsyncEnumerable<LogEvent> ExecuteAsync(QueryRequest request, CancellationToken ct = default) =>
            throw new InvalidOperationException("a level-less log rule must be counted from headers, not scanned");
    }

    /// <summary>Implements any interface by throwing — for dependencies these rules never call.</summary>
    public class ThrowingProxy : DispatchProxy
    {
        public static T For<T>() where T : class => Create<T, ThrowingProxy>();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            throw new InvalidOperationException($"{targetMethod?.Name} was not expected to be called");
    }
}
