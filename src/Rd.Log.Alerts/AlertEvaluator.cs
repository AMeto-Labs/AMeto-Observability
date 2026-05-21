using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Rd.Log.Core;
using Rd.Log.Query.Filtering;
using Rd.Log.Storage;

namespace Rd.Log.Alerts;

/// <summary>
/// Evaluates alert rules against every ingested event using a sliding-window counter.
///
/// Design:
/// - One <see cref="RuleState"/> per enabled rule, keyed by rule id.
/// - Hooks into <see cref="StorageEngine.EventWritten"/> — called on the ingestion write path.
/// - Builds a minimal <see cref="LogEvent"/> from header + template for filter evaluation.
/// - Thread-safe: multiple ingestion threads may call Evaluate concurrently.
/// - Non-blocking: firing is dispatched to the thread pool, never blocks the caller.
/// </summary>
public sealed class AlertEvaluator
{
    private readonly AlertRuleStore              _store;
    private readonly AlertDispatcher             _dispatcher;
    private readonly ILogger<AlertEvaluator>     _logger;
    private readonly ConcurrentDictionary<string, RuleState> _states = new();

    public AlertEvaluator(
        AlertRuleStore          store,
        AlertDispatcher         dispatcher,
        ILogger<AlertEvaluator> logger)
    {
        _store      = store;
        _dispatcher = dispatcher;
        _logger     = logger;

        RebuildStates();
        _store.RulesChanged += RebuildStates;
    }

    /// <summary>
    /// Wires this evaluator into <paramref name="engine"/> by setting its
    /// <see cref="StorageEngine.EventWritten"/> hook.
    /// </summary>
    public void Attach(StorageEngine engine)
    {
        engine.EventWritten = Evaluate;
    }

    private void Evaluate(LogEventHeader header, string messageTemplate)
    {
        if (_states.IsEmpty) return;

        // Build a lightweight LogEvent — no properties, just enough for filter evaluation.
        var ev = new LogEvent
        {
            Id              = new Rd.Log.Core.EventId(header.Id),
            Timestamp       = new DateTimeOffset(header.TimestampUtcTicks, TimeSpan.Zero),
            Level           = (Rd.Log.Core.LogLevel)header.Level,
            MessageTemplate = messageTemplate,
        };

        foreach (var (_, state) in _states)
            state.Push(ev);
    }

    private void RebuildStates()
    {
        var rules   = _store.GetAll();
        var newKeys = new HashSet<string>(rules.Select(r => r.Id));

        foreach (var key in _states.Keys)
            if (!newKeys.Contains(key))
                _states.TryRemove(key, out _);

        foreach (var rule in rules.Where(r => r.Enabled))
        {
            _states.AddOrUpdate(
                rule.Id,
                _  => new RuleState(rule, _dispatcher, _logger),
                (_, _) => new RuleState(rule, _dispatcher, _logger));
        }
    }

    // ── Per-rule state ────────────────────────────────────────────────────────

    private sealed class RuleState
    {
        private readonly AlertRule       _rule;
        private readonly CompiledFilter  _filter;
        private readonly AlertDispatcher _dispatcher;
        private readonly ILogger         _logger;

        private readonly Queue<(long ticks, LogEvent ev)> _window = new();
        private readonly object _lock = new();
        private long _lastFiredTicks = long.MinValue;

        public RuleState(AlertRule rule, AlertDispatcher dispatcher, ILogger logger)
        {
            _rule       = rule;
            _filter     = CompiledFilter.Compile(rule.Filter);
            _dispatcher = dispatcher;
            _logger     = logger;
        }

        public void Push(LogEvent ev)
        {
            if (!_filter.Matches(ev)) return;

            long nowTicks    = ev.Timestamp.UtcTicks;
            long windowTicks = _rule.Window.Ticks;

            lock (_lock)
            {
                _window.Enqueue((nowTicks, ev));

                // Expire old events
                while (_window.Count > 0 && nowTicks - _window.Peek().ticks > windowTicks)
                    _window.Dequeue();

                if (_window.Count < _rule.Threshold) return;

                // Check cooldown
                if (nowTicks - _lastFiredTicks < _rule.Cooldown.Ticks) return;

                _lastFiredTicks = nowTicks;

                var sample = _window.Take(5).Select(x => x.ev).ToList();
                int count  = _window.Count;
                _window.Clear();

                var fired = new AlertFiredEvent
                {
                    Rule         = _rule,
                    Count        = count,
                    FiredAt      = new DateTimeOffset(nowTicks, TimeSpan.Zero),
                    SampleEvents = sample,
                };

                _ = Task.Run(() => _dispatcher.DispatchAsync(fired));
            }
        }
    }
}
