using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Alerts;

/// <summary>
/// Periodic, unified evaluator for log / metric / trace alert rules.
///
/// Every <see cref="EvalInterval"/> it computes one numeric value per enabled rule,
/// compares it to the threshold, and drives a state machine:
/// OK → (condition true) → Pending → (held for <c>For</c>) → Firing → (condition false) → OK.
/// Firing/resolve transitions are dispatched to channels (unless silenced) and recorded
/// in the in-memory history ring.
/// </summary>
public sealed class AlertEvaluator : IAsyncDisposable
{
    private static readonly TimeSpan EvalInterval = TimeSpan.FromSeconds(15);
    private const int HistoryCapacity = 2_000;

    private readonly AlertRuleStore        _store;
    private readonly AlertDispatcher       _dispatcher;
    private readonly IQueryExecutor        _logQuery;
    private readonly IMetricAggregator     _metrics;
    private readonly ITraceStatsProvider   _traceStats;
    private readonly ILogger<AlertEvaluator> _logger;

    private readonly ConcurrentDictionary<string, MutableState> _states = new();
    private readonly ConcurrentDictionary<string, AlertSilence> _silences = new();
    private readonly AlertHistoryEntry[]   _history = new AlertHistoryEntry[HistoryCapacity];
    private readonly object                _histLock = new();
    private int _histCount, _histHead;

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    public AlertEvaluator(
        AlertRuleStore store, AlertDispatcher dispatcher,
        IQueryExecutor logQuery, IMetricAggregator metrics, ITraceStatsProvider traceStats,
        ILogger<AlertEvaluator> logger)
    {
        _store = store; _dispatcher = dispatcher;
        _logQuery = logQuery; _metrics = metrics; _traceStats = traceStats;
        _logger = logger;
        _loop = Task.Run(EvalLoopAsync);
    }

    // ── Public API (read by endpoints) ─────────────────────────────────────────

    public IReadOnlyList<AlertStateSnapshot> GetStates()
    {
        var rules = _store.GetAll();
        var list = new List<AlertStateSnapshot>(rules.Count);
        foreach (var r in rules)
        {
            var s = _states.GetValueOrDefault(r.Id);
            list.Add(new AlertStateSnapshot
            {
                RuleId = r.Id,
                State = s?.State ?? AlertState.Ok,
                LastValue = s?.LastValue ?? 0,
                PendingSince = s?.PendingSince,
                LastFiredAt = s?.LastFiredAt,
                EvaluatedAt = s?.EvaluatedAt ?? DateTimeOffset.MinValue,
            });
        }
        return list;
    }

    public IReadOnlyList<AlertHistoryEntry> GetHistory(int limit = 200)
    {
        lock (_histLock)
        {
            int n = Math.Min(limit, _histCount);
            var outArr = new AlertHistoryEntry[n];
            for (int i = 0; i < n; i++)            // newest first
                outArr[i] = _history[(_histHead - 1 - i + HistoryCapacity) % HistoryCapacity];
            return outArr;
        }
    }

    public IReadOnlyList<AlertSilence> GetSilences()
    {
        PurgeExpiredSilences();
        return _silences.Values.OrderByDescending(s => s.Until).ToList();
    }

    public AlertSilence AddSilence(AlertSilence s) { _silences[s.Id] = s; return s; }
    public bool RemoveSilence(string id) => _silences.TryRemove(id, out _);

    /// <summary>Evaluate a rule's value right now without affecting state (for the editor preview).</summary>
    public async Task<double> PreviewAsync(AlertRule rule, CancellationToken ct = default)
        => await ComputeValueAsync(rule, DateTimeOffset.UtcNow, ct);

    // ── Eval loop ───────────────────────────────────────────────────────────────

    private async Task EvalLoopAsync()
    {
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(EvalInterval, ct); }
            catch (OperationCanceledException) { break; }

            try { await EvaluateAllAsync(ct); }
            catch (Exception ex) { _logger.LogError(ex, "Alert evaluation cycle failed"); }
        }
    }

    private async Task EvaluateAllAsync(CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var rule in _store.GetAll())
        {
            if (!rule.Enabled) { _states.TryRemove(rule.Id, out _); continue; }
            try
            {
                double value = await ComputeValueAsync(rule, now, ct);
                Transition(rule, value, now);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to evaluate alert rule {Rule}", rule.Id);
            }
        }
    }

    // ── State machine ───────────────────────────────────────────────────────────

    private void Transition(AlertRule rule, double value, DateTimeOffset now)
    {
        var st = _states.GetOrAdd(rule.Id, _ => new MutableState());
        st.LastValue   = value;
        st.EvaluatedAt = now;

        bool breached = Compare(value, rule.Comparator, rule.Threshold);

        if (!breached)
        {
            // Resolve if we were firing
            if (st.State == AlertState.Firing)
            {
                st.State = AlertState.Ok;
                st.PendingSince = null;
                Record(rule, AlertState.Ok, value, now);
                Dispatch(rule, AlertState.Ok, value, now);
            }
            else
            {
                st.State = AlertState.Ok;
                st.PendingSince = null;
            }
            return;
        }

        // breached
        if (st.State is AlertState.Ok or AlertState.NoData)
        {
            st.PendingSince = now;
            st.State = rule.For <= TimeSpan.Zero ? AlertState.Firing : AlertState.Pending;
        }

        if (st.State == AlertState.Pending && st.PendingSince is { } since && now - since >= rule.For)
            st.State = AlertState.Firing;

        if (st.State == AlertState.Firing)
        {
            // Cooldown gate on repeated firing notifications
            bool cooled = st.LastFiredAt is null || now - st.LastFiredAt >= rule.Cooldown;
            if (cooled && !st.Notified)
            {
                st.LastFiredAt = now;
                st.Notified = true;
                Record(rule, AlertState.Firing, value, now);
                Dispatch(rule, AlertState.Firing, value, now);
            }
        }

        if (st.State != AlertState.Firing) st.Notified = false;
    }

    private void Dispatch(AlertRule rule, AlertState state, double value, DateTimeOffset now)
    {
        if (IsSilenced(rule.Id)) return;
        var fired = new AlertFiredEvent { Rule = rule, State = state, Value = value, At = now };
        _ = Task.Run(() => _dispatcher.DispatchAsync(fired));
    }

    private void Record(AlertRule rule, AlertState state, double value, DateTimeOffset now)
    {
        var entry = new AlertHistoryEntry
        {
            RuleId = rule.Id, RuleName = rule.Name, Severity = rule.Severity,
            State = state, Value = value, Threshold = rule.Threshold, At = now,
        };
        lock (_histLock)
        {
            _history[_histHead] = entry;
            _histHead = (_histHead + 1) % HistoryCapacity;
            if (_histCount < HistoryCapacity) _histCount++;
        }
    }

    private bool IsSilenced(string ruleId)
    {
        PurgeExpiredSilences();
        foreach (var s in _silences.Values)
            if (s.RuleId == ruleId && s.Until > DateTimeOffset.UtcNow) return true;
        return false;
    }

    private void PurgeExpiredSilences()
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var (id, s) in _silences)
            if (s.Until <= now) _silences.TryRemove(id, out _);
    }

    // ── Value computation per source ────────────────────────────────────────────

    private async Task<double> ComputeValueAsync(AlertRule rule, DateTimeOffset now, CancellationToken ct)
    {
        var from = now - rule.Window;
        return rule.Source switch
        {
            AlertSource.Metric => await MetricValueAsync(rule, from, now, ct),
            AlertSource.Trace  => await TraceValueAsync(rule, from, now, ct),
            _                  => await LogValueAsync(rule, from, now, ct),
        };
    }

    private async Task<double> LogValueAsync(AlertRule rule, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        var req = new QueryRequest
        {
            Filter = rule.Filter,
            FromUtc = from,
            ToUtc = to,
            Count = 10_000,
            Direction = QueryDirection.Backward,
        };
        int count = 0;
        await foreach (var _ in _logQuery.ExecuteAsync(req, ct))
            if (++count >= 10_000) break;
        return count;
    }

    private async Task<double> MetricValueAsync(AlertRule rule, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(rule.Metric)) return 0;
        var series = await _metrics.QueryAsync(new MetricQueryRequest
        {
            Metric = rule.Metric,
            From = from, To = to,
            Aggregation = Enum.TryParse<MetricAggregation>(rule.Aggregation, true, out var a) ? a : MetricAggregation.Last,
            Quantile = rule.Quantile,
            GroupBy = rule.GroupBy,
            Filters = rule.Labels,
        }, ct);

        // value = max latest across resulting series (worst case)
        double worst = double.NaN;
        foreach (var s in series)
        {
            if (s.Points.Count == 0) continue;
            double v = s.Points[^1].Value;
            if (double.IsNaN(worst) || v > worst) worst = v;
        }
        return double.IsNaN(worst) ? 0 : worst;
    }

    private async Task<double> TraceValueAsync(AlertRule rule, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        var stats = await _traceStats.GetAggregateStatsAsync(from, to, ct);
        var svc = stats.FirstOrDefault(s =>
            string.IsNullOrEmpty(rule.Service) ||
            s.ServiceName.Equals(rule.Service, StringComparison.OrdinalIgnoreCase));
        if (svc is null) return 0;

        return rule.TraceMetric switch
        {
            TraceMetricKind.ErrorRatePct => svc.SpanCount > 0 ? (double)svc.ErrorCount / svc.SpanCount * 100.0 : 0,
            TraceMetricKind.SpanCount    => svc.SpanCount,
            TraceMetricKind.P50Ms        => HistogramBuckets.Percentile(svc.Buckets, 0.50),
            TraceMetricKind.P95Ms        => HistogramBuckets.Percentile(svc.Buckets, 0.95),
            TraceMetricKind.P99Ms        => HistogramBuckets.Percentile(svc.Buckets, 0.99),
            _                            => HistogramBuckets.Percentile(svc.Buckets, 0.50),
        };
    }

    private static bool Compare(double value, AlertComparator cmp, double threshold) => cmp switch
    {
        AlertComparator.GreaterThan    => value >  threshold,
        AlertComparator.GreaterOrEqual => value >= threshold,
        AlertComparator.LessThan       => value <  threshold,
        AlertComparator.LessOrEqual    => value <= threshold,
        _                              => false,
    };

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _loop; } catch (OperationCanceledException) { }
        _cts.Dispose();
    }

    private sealed class MutableState
    {
        public AlertState      State;
        public double          LastValue;
        public DateTimeOffset? PendingSince;
        public DateTimeOffset? LastFiredAt;
        public DateTimeOffset  EvaluatedAt;
        public bool            Notified;
    }
}
