using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Query.Filtering;
using Ameto.Storage;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

// Two LogLevels are in scope (ours and the logging framework's); in this file the level of
// a LOG EVENT is the one that matters.
using LogLevel = Ameto.Core.LogLevel;

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
    private readonly AlertPersistence      _persist;
    private readonly IQueryExecutor        _logQuery;
    /// <summary>For the header-only count path — see <see cref="HeaderCountAsync"/>.</summary>
    private readonly StorageEngine         _storage;
    private readonly IMetricAggregator     _metrics;
    private readonly ITraceStatsProvider   _traceStats;
    private readonly ILogger<AlertEvaluator> _logger;

    /// <summary>
    /// The clock the unavailable-store warning is rate-limited on. A seam, so that "a second line
    /// after a minute" is a test that advances a clock rather than one that waits a minute.
    /// </summary>
    private readonly TimeProvider _time;

    /// <summary><see cref="AlertEvaluatorOptions.EvaluateOnDegradedStore"/>: whether a Degraded store's answers are acted on.</summary>
    private readonly bool _evaluateOnDegraded;

    private readonly ConcurrentDictionary<string, MutableState> _states = new();
    private readonly ConcurrentDictionary<string, AlertSilence> _silences = new();
    private readonly ConcurrentDictionary<string, MaintenanceWindow> _maintenance = new();
    private readonly AlertHistoryEntry[]   _history = new AlertHistoryEntry[HistoryCapacity];
    private readonly object                _histLock = new();
    private int _histCount, _histHead;

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    // 0 = live, 1 = disposed. Guards against the multiple DisposeAsync calls at
    // host shutdown (see DisposeAsync).
    private int _disposed;

    /// <summary>Completed when the first DisposeAsync has finished; every later caller awaits it.</summary>
    private readonly TaskCompletionSource _disposeCompleted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// 1 once the host has begun stopping (<see cref="StopEvaluating"/>) or this evaluator is being
    /// disposed. From then on no cycle starts and — the half that matters — no value already being
    /// computed is APPLIED: the engines behind it are about to answer empty, and an empty answer is
    /// a 0 that resolves every firing "&gt;" rule. See <see cref="EvaluateAllAsync"/>.
    /// </summary>
    private int _stopping;

    private bool IsStopping => Volatile.Read(ref _stopping) != 0;

    public AlertEvaluator(
        AlertRuleStore store, AlertDispatcher dispatcher, AlertPersistence persist,
        IQueryExecutor logQuery, StorageEngine storage,
        IMetricAggregator metrics, ITraceStatsProvider traceStats,
        ILogger<AlertEvaluator> logger, TimeProvider? time = null, AlertEvaluatorOptions? options = null)
    {
        _store = store; _dispatcher = dispatcher; _persist = persist;
        _logQuery = logQuery; _storage = storage; _metrics = metrics; _traceStats = traceStats;
        _logger = logger;
        _time   = time ?? TimeProvider.System;
        _evaluateOnDegraded = options?.EvaluateOnDegradedStore ?? false;
        LoadFromDb();
        _loop = Task.Run(EvalLoopAsync);
    }

    /// <summary>Restore silences, per-rule state, and recent history from SQLite on startup.</summary>
    private void LoadFromDb()
    {
        foreach (var s in _persist.LoadSilences())
            if (s.Until > DateTimeOffset.UtcNow) _silences[s.Id] = s;

        foreach (var m in _persist.LoadMaintenance())
            _maintenance[m.Id] = m;

        foreach (var (ruleId, state, lastValue, pending, fired, evaluated) in _persist.LoadStates())
            _states[ruleId] = new MutableState
            {
                State = state, LastValue = lastValue, PendingSince = pending,
                LastFiredAt = fired, Notified = state == AlertState.Firing,
                // When it was last evaluated, so a Pending rule's For credit survives the restart (#94).
                EvaluatedAt = evaluated ?? default, PersistedEvaluatedAt = evaluated,
            };

        var hist = _persist.LoadHistory(HistoryCapacity);
        // LoadHistory returns newest-first; fill ring oldest-first so newest-first read works.
        for (int i = hist.Count - 1; i >= 0; i--)
        {
            _history[_histHead] = hist[i];
            _histHead = (_histHead + 1) % HistoryCapacity;
            if (_histCount < HistoryCapacity) _histCount++;
        }
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
                AckedAt = s?.AckedAt,
                AckedBy = s?.AckedBy,
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

    /// <summary>Acknowledge a currently-firing rule: mutes re-notify until it resolves. No-op if not firing.</summary>
    public bool Acknowledge(string ruleId, string? by)
    {
        if (!_states.TryGetValue(ruleId, out var st) || st.State != AlertState.Firing) return false;
        st.AckedAt = DateTimeOffset.UtcNow;
        st.AckedBy = by;
        return true;
    }

    /// <summary>Clear an acknowledgement (re-notify resumes on the next repeat interval).</summary>
    public bool Unacknowledge(string ruleId)
    {
        if (!_states.TryGetValue(ruleId, out var st) || st.AckedAt is null) return false;
        st.AckedAt = null;
        st.AckedBy = null;
        return true;
    }

    public AlertSilence AddSilence(AlertSilence s) { _silences[s.Id] = s; _persist.UpsertSilence(s); return s; }
    public bool RemoveSilence(string id)
    {
        bool ok = _silences.TryRemove(id, out _);
        if (ok) _persist.DeleteSilence(id);
        return ok;
    }

    // ── Maintenance windows ─────────────────────────────────────────────────────

    public IReadOnlyList<MaintenanceWindow> GetMaintenance() => _maintenance.Values.OrderBy(m => m.Name).ToList();

    public MaintenanceWindow UpsertMaintenance(MaintenanceWindow w)
    {
        _maintenance[w.Id] = w;
        _persist.UpsertMaintenance(w);
        return w;
    }

    public bool RemoveMaintenance(string id)
    {
        bool ok = _maintenance.TryRemove(id, out _);
        if (ok) _persist.DeleteMaintenance(id);
        return ok;
    }

    private bool IsInMaintenance(AlertRule rule, DateTimeOffset now)
    {
        foreach (var w in _maintenance.Values)
            if (w.IsActiveAt(now) && w.Matches(rule.Severity)) return true;
        return false;
    }

    /// <summary>
    /// Evaluate a rule's value right now without affecting state (for the editor preview). The
    /// same answer a tick would act on — including "the store cannot say" (#95), which a tick skips
    /// and the preview endpoint turns into a 503 rather than a 0 that "would not fire"; and an
    /// available NaN, a metric window with points and not one finite (#92), which a tick also skips
    /// and the endpoint answers as <c>value: null, wouldFire: false</c> — never a verdict the
    /// evaluator would not reach. An EMPTY window is 0, as the evaluator reads it. A preview logs no
    /// non-finite warning (see <see cref="WarnNonFinite"/>).
    /// </summary>
    public ValueTask<AlertValue> PreviewAsync(AlertRule rule, CancellationToken ct = default)
        => ComputeValueAsync(rule, DateTimeOffset.UtcNow, ct, preview: true);

    /// <summary>
    /// Dispatches a one-off TEST notification through the rule's channels — bypasses the
    /// state machine, cooldown and silences so the user can verify channel delivery.
    /// </summary>
    public Task SendTestAsync(AlertRule rule, CancellationToken ct = default)
    {
        var fired = new AlertFiredEvent
        {
            Rule = rule, State = AlertState.Firing, Value = rule.Threshold,
            At = DateTimeOffset.UtcNow, IsTest = true,
        };
        return _dispatcher.DispatchAsync(fired);
    }

    // ── Eval loop ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Test seam: an evaluator constructed while this is true never runs its timed loop, so the only
    /// ticks it sees are the ones a test drives through <see cref="EvaluateOnceAsync(DateTimeOffset, CancellationToken)"/>
    /// — a real tick fifteen seconds in would move the cycle clock under a test that sets it. An
    /// <see cref="AsyncLocal{T}"/>, which the constructor's <c>Task.Run</c> carries into the loop, so it
    /// reaches only the evaluators the setting test constructs. False in production.
    /// </summary>
    internal static readonly AsyncLocal<bool> NoTimedLoopForTest = new();

    private async Task EvalLoopAsync()
    {
        if (NoTimedLoopForTest.Value) return;
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(EvalInterval, ct); }
            catch (OperationCanceledException) { break; }

            try { await EvaluateAllAsync(ct); }
            catch (Exception ex) { _logger.LogError(ex, "Alert evaluation cycle failed"); }
        }
    }

    /// <summary>
    /// Test hook: one evaluation cycle, now, instead of after <see cref="EvalInterval"/>. It is the
    /// method the loop calls, so what a test observes is what the loop does.
    /// </summary>
    internal Task EvaluateOnceAsync(CancellationToken ct = default) => EvaluateAllAsync(ct);

    /// <summary>
    /// Test hook: one evaluation cycle as if it began at <paramref name="at"/> — the tick's time, which
    /// every rule of the cycle is evaluated at and which <c>For</c> is measured on.
    /// </summary>
    internal Task EvaluateOnceAsync(DateTimeOffset at, CancellationToken ct = default) => EvaluateAllAsync(ct, at);

    /// <summary>Test hook: true once the eval loop has ended — no further tick can land.</summary>
    internal bool LoopEndedForTest => _loop.IsCompleted;

    /// <summary>
    /// The host is stopping: finish nothing, start nothing. Called from
    /// <see cref="Microsoft.Extensions.Hosting.IHostApplicationLifetime.ApplicationStopping"/>,
    /// which fires before any hosted service — so before any engine this reads — begins to stop.
    /// The loop is cancelled too, but cancelling it is not enough on its own: a cycle already past
    /// its delay runs to the end, and the trace path ignores the token.
    /// </summary>
    internal void StopEvaluating()
    {
        Interlocked.Exchange(ref _stopping, 1);
        try { _cts.Cancel(); }
        catch (ObjectDisposedException) { /* already disposed: the loop is gone */ }
    }

    private async Task EvaluateAllAsync(CancellationToken ct, DateTimeOffset? at = null)
    {
        var now = at ?? DateTimeOffset.UtcNow;
        _previousCycleAt = _cycleAt;   // see HeldPastUnobservedTicks
        _cycleAt         = now;
        foreach (var rule in _store.GetAll())
        {
            if (IsStopping) return;
            if (!rule.Enabled) { _states.TryRemove(rule.Id, out _); continue; }
            try
            {
                var value = await ComputeValueAsync(rule, now, ct);

                // CHECKED AFTER THE VALUE, NOT ONLY BEFORE THE CYCLE. An engine closes only after
                // _stopping is set (ApplicationStopping, or this evaluator's own disposal, precedes
                // every engine's StopAsync), so a value read out of a closed engine is always seen
                // here with the flag up. Checking only at the top of the cycle left the case that
                // matters: a tick that began while the host was running and read its 0 after.
                if (IsStopping) return;

                // THE SAME CASE WITHOUT A HOST STOP (#95). The flag above covers an engine closed by
                // the host's own shutdown; this covers every other way a store can stop answering
                // truly — closed by something else, still loading its cold tier, or degraded: done
                // loading without having reached everything on disk (#94) — because the store says
                // so itself. Nothing about the rule changes: not its state, not its last value, not
                // its evaluation time. A skipped tick is a tick that did not happen, so it adds
                // nothing to a Pending rule's For either (see HeldPastUnobservedTicks).
                if (!value.IsAvailable)
                {
                    WarnUnavailable(rule, value.Availability);
                    continue;
                }

                // Available, and still no value to compare (#92): a metric window whose every point
                // is NaN or infinite (MetricValueAsync has said so in the log). The rule keeps its
                // state: a comparison against NaN is false, and "not breached" would resolve it. A
                // SEPARATE test from the one above, and after it — NaN is never the availability test.
                if (double.IsNaN(value.Value)) continue;

                Transition(rule, value.Value, now);
            }
            catch (OperationCanceledException) when (IsStopping)
            {
                return;   // a scan cut short by the stop, not a failing rule
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to evaluate alert rule {Rule}", rule.Id);
            }
        }

        // After the tick's last rule, so a line counts every rule the tick skipped (a cycle cut
        // short by the host stopping returns above and says nothing).
        FlushUnavailableWarnings();
        ForgetDeletedRules();
    }

    /// <summary>
    /// Drops the once-only non-finite warnings of rules that no longer exist — a rule deleted, or
    /// replaced under a new id — so the set holds at most the saved rules' entries (two each). Once a
    /// tick, and only when the set is not empty.
    /// </summary>
    private void ForgetDeletedRules()
    {
        if (_nonFiniteWarned.IsEmpty) return;
        foreach (var entry in _nonFiniteWarned)
            if (_store.GetById(entry.Key.RuleId) is null) _nonFiniteWarned.TryRemove(entry.Key, out _);
    }

    /// <summary>Test hook: how many once-only non-finite warnings are remembered.</summary>
    internal int NonFiniteWarnedCountForTest => _nonFiniteWarned.Count;

    // ── State machine ───────────────────────────────────────────────────────────

    /// <summary>When the cycle before the current one began, and the current one; null until there was one.</summary>
    private DateTimeOffset? _previousCycleAt, _cycleAt;

    /// <summary>
    /// A Pending rule's <see cref="MutableState.PendingSince"/>, moved forward past the ticks that did not
    /// evaluate it (#94). <c>For</c> is how long the condition has been SEEN to hold, and a tick that
    /// skipped the rule saw nothing: its evaluation threw, its value was not a number, its store was
    /// loading, degraded or closed (the skips #92, #95 and #94 add), or the process was not running. Counted as
    /// time held, those ticks let a rule that went Pending just before a long outage fire on the
    /// first tick after it, on one observation.
    ///
    /// <para>So each evaluated tick adds the interval since the tick before it, as it always did, and
    /// a skipped tick adds nothing: the span from this rule's last evaluation to the cycle before
    /// this one is added to <c>PendingSince</c>. Consecutive ticks move nothing — the last evaluation
    /// WAS the previous cycle. The comparison is to the previous cycle's time, not to a fixed
    /// interval, so a slow cycle costs no rule its credit.</para>
    ///
    /// <para><b>Across a restart, the same rule.</b> A Pending state is persisted with the time the rule
    /// was last evaluated (<c>alert_state.evaluated_ticks</c>, written on every Pending tick — see
    /// <see cref="PersistEvaluationIfDue"/>), and restored with it, so a restart is a run of skipped
    /// ticks like any other: the downtime adds nothing, the credit seen before it is kept, and the
    /// first tick after it that sees the breach resumes the count. Resetting instead — the rule
    /// between 1d8d777 and this — meant a server restarting more often than <c>For</c> never fired a
    /// Pending rule at all (#106 review F1); counting the downtime, as main did before #94, fired a
    /// rule pending a minute before a ten-minute restart on the first tick after it.</para>
    ///
    /// <para>When the last evaluation is NOT known — a row written before the column existed, or one an
    /// older build rewrote without it, which then reads as older than <c>PendingSince</c> — nothing
    /// after <c>PendingSince</c> is known to have been seen, and the clock restarts at this tick:
    /// later by at most <c>For</c>, never earlier.</para>
    /// </summary>
    private DateTimeOffset HeldPastUnobservedTicks(DateTimeOffset since, DateTimeOffset lastEvaluated, DateTimeOffset now)
    {
        if (lastEvaluated == default || lastEvaluated < since) return now;
        DateTimeOffset previousTick = _previousCycleAt ?? now;   // none yet in this process: this one
        return previousTick > lastEvaluated ? since + (previousTick - lastEvaluated) : since;
    }

    /// <summary>
    /// The least time between two writes of a Pending rule's evaluation time (<see cref="PersistEvaluationIfDue"/>):
    /// ticks are fifteen seconds apart, so in practice this is one write per Pending rule per tick, and
    /// only a cycle driven twice within a second (a test, a manual re-run) is coalesced.
    /// </summary>
    private static readonly TimeSpan EvaluationPersistInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Writes a Pending rule's state with its evaluation time when the last write of it is at least
    /// <see cref="EvaluationPersistInterval"/> old — what a restart restores its <c>For</c> credit from
    /// (see <see cref="HeldPastUnobservedTicks"/>). Only Pending rules: an Ok or Firing rule's
    /// evaluation time changes nothing a restart decides, and a state change is written anyway.
    /// </summary>
    private void PersistEvaluationIfDue(string ruleId, MutableState st, DateTimeOffset now)
    {
        if (st.State != AlertState.Pending) return;
        if (st.PersistedEvaluatedAt is { } last && now - last < EvaluationPersistInterval && now >= last) return;
        PersistState(ruleId, st);
    }

    private void Transition(AlertRule rule, double value, DateTimeOffset now)
    {
        var st = _states.GetOrAdd(rule.Id, _ => new MutableState());
        var lastEvaluated = st.EvaluatedAt;   // before this tick; default: never (or not known, restored from an older row)
        st.LastValue   = value;
        st.EvaluatedAt = now;
        var prevState  = st.State;

        bool breached = Compare(value, rule.Comparator, rule.Threshold);

        if (!breached)
        {
            // Resolve if we were firing
            if (st.State == AlertState.Firing)
            {
                st.State = AlertState.Ok;
                st.PendingSince = null;
                st.AckedAt = null;   // clear ack — the incident is over
                st.AckedBy = null;
                st.FiringSince = null;
                st.Escalated = false;
                Record(rule, AlertState.Ok, value, now);
                Dispatch(rule, AlertState.Ok, value, now);
            }
            else
            {
                st.State = AlertState.Ok;
                st.PendingSince = null;
            }
            if (st.State != prevState) PersistState(rule.Id, st);
            return;
        }

        // breached
        if (st.State is AlertState.Ok or AlertState.NoData)
        {
            st.PendingSince = now;
            st.State = rule.For <= TimeSpan.Zero ? AlertState.Firing : AlertState.Pending;
        }
        else if (st.State == AlertState.Pending && st.PendingSince is { } held)
        {
            st.PendingSince = HeldPastUnobservedTicks(held, lastEvaluated, now);
        }

        if (st.State == AlertState.Pending && st.PendingSince is { } since && now - since >= rule.For)
            st.State = AlertState.Firing;

        if (st.State == AlertState.Firing)
        {
            st.FiringSince ??= now;   // mark the start of this firing incident

            // Cooldown gate on the first firing notification.
            bool cooled = st.LastFiredAt is null || now - st.LastFiredAt >= rule.Cooldown;
            if (cooled && !st.Notified)
            {
                st.LastFiredAt = now;
                st.Notified = true;
                Record(rule, AlertState.Firing, value, now);
                Dispatch(rule, AlertState.Firing, value, now);
            }
            // Re-notify: while still firing (and not acknowledged), re-send every RepeatInterval.
            else if (st.Notified && st.AckedAt is null && rule.RepeatInterval > TimeSpan.Zero
                     && st.LastFiredAt is { } last && now - last >= rule.RepeatInterval)
            {
                st.LastFiredAt = now;
                Dispatch(rule, AlertState.Firing, value, now);
            }

            // Escalation: unacknowledged past EscalateAfter → notify the escalation-only channels once.
            if (rule.EscalateAfter > TimeSpan.Zero && !st.Escalated && st.AckedAt is null
                && st.FiringSince is { } fs && now - fs >= rule.EscalateAfter)
            {
                st.Escalated = true;
                Dispatch(rule, AlertState.Firing, value, now, escalation: true);
            }
        }

        if (st.State != AlertState.Firing) { st.Notified = false; st.FiringSince = null; st.Escalated = false; }

        if (st.State != prevState) PersistState(rule.Id, st);
        else                       PersistEvaluationIfDue(rule.Id, st, now);   // the For credit a restart restores (#94)
    }

    private void PersistState(string ruleId, MutableState st)
    {
        DateTimeOffset? evaluated = st.EvaluatedAt == default ? null : st.EvaluatedAt;
        _persist.SaveState(ruleId, st.State, st.LastValue, st.PendingSince, st.LastFiredAt, evaluated);
        st.PersistedEvaluatedAt = evaluated;
    }

    private void Dispatch(AlertRule rule, AlertState state, double value, DateTimeOffset now, bool escalation = false)
    {
        if (IsSilenced(rule.Id)) return;
        if (IsInMaintenance(rule, now)) return;
        var fired = new AlertFiredEvent { Rule = rule, State = state, Value = value, At = now, IsEscalation = escalation };
        _onDispatchForTest?.Invoke(fired);
        _ = Task.Run(() => _dispatcher.DispatchAsync(fired));
    }

    /// <summary>
    /// Test seam: every notification the state machine decides to send, on the evaluating thread,
    /// BEFORE it is handed to the pool — so "nothing was dispatched" is a count a test reads when the
    /// tick returns, not a fire-and-forget it has to wait out. Null in production.
    /// </summary>
    internal Action<AlertFiredEvent>? _onDispatchForTest;

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
        _persist.AppendHistory(entry);
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

    /// <summary>
    /// The rule's value — or, when the store behind it could not answer truly, which way it could
    /// not (#95). A store that has closed answers EMPTY, and one still loading — or degraded, done
    /// loading without having reached everything on disk (#94) — answers a PART; either, taken as a
    /// number, is a 0 (or a low count) that resolves every firing "&gt;" rule and fires every "&lt;"
    /// one. The store is asked instead of the answer being guessed at. A degraded store's answer is
    /// acted on only when <see cref="AlertEvaluatorOptions.EvaluateOnDegradedStore"/> says so.
    ///
    /// <para><b>Asked twice, around the read, and each question catches one state.</b> Availability
    /// only moves forward — Loading → Available or Degraded → Closed, and a store is Degraded from the
    /// end of its load or never — so:</para>
    /// <list type="bullet">
    /// <item>BEFORE the read catches <see cref="QueryAvailability.Loading"/> and
    /// <see cref="QueryAvailability.Degraded"/>. A read that ran while the store was partial began
    /// while it was, so this question, asked earlier still, saw it too. Asked only AFTER, it would
    /// miss a load that finished during the read — the read's snapshot of the cold tier was taken
    /// before it.</item>
    /// <item>AFTER the read catches <see cref="QueryAvailability.Closed"/>. A read that met a closed
    /// door is followed by this question, which sees the same door. Asked only BEFORE, it misses the
    /// tick that began open and read after the close — the race #84 closed for the host stop.</item>
    /// </list>
    ///
    /// <para><b>The log store differs in one way: closed, it THROWS.</b> Once its teardown has
    /// collected its hot tiers, its reader snapshot raises <see cref="ObjectDisposedException"/>
    /// instead of answering. The rule was never resolved by that, but every rule over it logged a
    /// failure with a stack trace on every tick. A read the store's own close cut short is now the
    /// Closed answer it is: skipped, and said once a minute. An ObjectDisposedException from a store
    /// that is NOT closed is still a failure, and still reported as one.</para>
    ///
    /// <para>An AVAILABLE value can still be NaN (#92): a metric window with points and not one
    /// finite. The caller tests <see cref="AlertValue.IsAvailable"/> first and NaN second.</para>
    /// </summary>
    /// <param name="preview">The editor's preview, not an evaluation: says nothing in the log (see
    /// <see cref="WarnNonFinite"/>).</param>
    private async ValueTask<AlertValue> ComputeValueAsync(AlertRule rule, DateTimeOffset now, CancellationToken ct, bool preview = false)
    {
        IQueryAvailability? store = rule.Source switch
        {
            AlertSource.Metric => _metrics,
            AlertSource.Trace  => _traceStats,
            _                  => _storage,
        };

        var before = store?.Availability ?? QueryAvailability.Available;
        if (!CanActOn(before)) return AlertValue.Unavailable(before);

        var from = now - rule.Window;
        double value;
        try
        {
            value = rule.Source switch
            {
                AlertSource.Metric => await MetricValueAsync(rule, from, now, ct, preview),
                AlertSource.Trace  => await TraceValueAsync(rule, from, now, ct),
                _                  => await LogValueAsync(rule, from, now, ct),
            };
        }
        catch (ObjectDisposedException) when (store?.Availability == QueryAvailability.Closed)
        {
            return AlertValue.Unavailable(QueryAvailability.Closed);
        }

        var after = store?.Availability ?? QueryAvailability.Available;
        if (!CanActOn(after)) return AlertValue.Unavailable(after);

        return AlertValue.Of(value);
    }

    /// <summary>
    /// Whether a store's answer may be acted on in <paramref name="availability"/>: an Available
    /// store's always, a Degraded one's only when the operator has chosen rules over its partial data
    /// to rules that wait for a restart (<see cref="AlertEvaluatorOptions.EvaluateOnDegradedStore"/>).
    /// </summary>
    private bool CanActOn(QueryAvailability availability) =>
        availability == QueryAvailability.Available
        || (availability == QueryAvailability.Degraded && _evaluateOnDegraded);

    /// <summary>
    /// Records that a rule was left as it was because its store cannot answer. Nothing is logged
    /// here: the tick's skips are counted per (source, Loading, Degraded or Closed) and said once, by
    /// <see cref="FlushUnavailableWarnings"/> after the tick's last rule — so a line counts the
    /// whole tick, not the first rule of it.
    /// </summary>
    private void WarnUnavailable(AlertRule rule, QueryAvailability why)
    {
        int i = ((int)rule.Source % SourceCount) * StatesPerSource + SlotOf(why);
        Interlocked.Increment(ref _unavailableSkipped[i]);
        Volatile.Write(ref _unavailableLastRule[i], rule.Id);
    }

    /// <summary>
    /// Says, once a tick and at most once a minute per SOURCE AND STATE, that rules are being left
    /// as they were because their store cannot answer. Not per rule: a closed store skips every
    /// rule that reads it, on every tick, and a line per rule per tick would bury the log for as
    /// long as the store stays down. Loading, Degraded and Closed have a slot each, so a store that
    /// finishes loading and then closes within the minute is still reported closed at once — and a
    /// store whose load ends Degraded is reported Degraded at once, not a minute after its Loading
    /// line; and each line counts every evaluation its slot skipped since its last line — the ticks
    /// it was held back included — so the rules it does not name are accounted for.
    ///
    /// <para>A Degraded line says what Loading's need not: that it will not end by itself, and the
    /// setting that evaluates the rules anyway.</para>
    /// </summary>
    private void FlushUnavailableWarnings()
    {
        long now = _time.GetTimestamp();
        for (int i = 0; i < _unavailableSkipped.Length; i++)
        {
            if (Volatile.Read(ref _unavailableSkipped[i]) == 0) continue;

            long last = Volatile.Read(ref _unavailableWarnedAt[i]);
            if (last != 0 && _time.GetElapsedTime(last, now) < UnavailableWarnInterval) continue;   // held: it keeps counting
            if (Interlocked.CompareExchange(ref _unavailableWarnedAt[i], now, last) != last) continue;

            int skipped = Interlocked.Exchange(ref _unavailableSkipped[i], 0);
            var why     = StateOfSlot(i % StatesPerSource);
            var source  = (AlertSource)(i / StatesPerSource);
            string? rule = Volatile.Read(ref _unavailableLastRule[i]);
            if (why == QueryAvailability.Degraded)
                _logger.LogWarning(
                    "Alert rule {Rule} was not evaluated: the {Source} store is {Availability} (it could not "
                  + "load everything on disk at startup, and stays partial until a restart), so its answer "
                  + "would be {Answer}, not a value — {Skipped} rule evaluation(s) skipped for this reason "
                  + "since the last such line. Rules keep their state and nothing is sent; set "
                  + "Ameto:Alerts:EvaluateOnDegradedStore to evaluate them on what the store has instead. "
                  + "Said at most once a minute per source and state",
                    rule, source, why, "partial", skipped);
            else
                _logger.LogWarning(
                    "Alert rule {Rule} was not evaluated: the {Source} store is {Availability}, so its answer "
                  + "would be {Answer}, not a value — {Skipped} rule evaluation(s) skipped for this reason "
                  + "since the last such line. Rules keep their state and nothing is sent. Said at most once "
                  + "a minute per source and state",
                    rule, source, why, why == QueryAvailability.Loading ? "partial" : "empty", skipped);
        }
    }

    private static readonly TimeSpan UnavailableWarnInterval = TimeSpan.FromMinutes(1);

    /// <summary>The <see cref="AlertSource"/> values: Log, Metric, Trace.</summary>
    private const int SourceCount = 3;

    /// <summary>The states a rule is skipped for, a warning slot each per source: Loading, Degraded, Closed.</summary>
    private const int StatesPerSource = 3;

    private static int SlotOf(QueryAvailability why) => why switch
    {
        QueryAvailability.Loading  => 0,
        QueryAvailability.Degraded => 1,
        _                          => 2,   // Closed
    };

    private static QueryAvailability StateOfSlot(int slot) => slot switch
    {
        0 => QueryAvailability.Loading,
        1 => QueryAvailability.Degraded,
        _ => QueryAvailability.Closed,
    };

    /// <summary>
    /// Last <see cref="_time"/> timestamp a warning was logged, per (<see cref="AlertSource"/>,
    /// state) — index <c>source * StatesPerSource + SlotOf(state)</c>; 0 = never.
    /// </summary>
    private readonly long[] _unavailableWarnedAt = new long[SourceCount * StatesPerSource];

    /// <summary>Evaluations skipped per slot of <see cref="_unavailableWarnedAt"/> since its last line.</summary>
    private readonly int[] _unavailableSkipped = new int[SourceCount * StatesPerSource];

    /// <summary>The most recent rule skipped per slot — the one a line names.</summary>
    private readonly string?[] _unavailableLastRule = new string?[SourceCount * StatesPerSource];
    /// <summary>
    /// Safety bound for the scanning fallback. It replaces a hard 10 000 that was NOT a
    /// safety bound but a silent ceiling: a rule counting more than that reported exactly
    /// 10 000, so "more than 20 000 errors" could never fire, and every firing rule
    /// reported a value that was not the count. Reaching this one is logged.
    /// </summary>
    private const int MaxScannedForCount = 1_000_000;

    private async Task<double> LogValueAsync(AlertRule rule, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        // FAST PATH — and only where it is actually fast. The header aggregator reads three
        // columns instead of materialising events, but it consults NO index: it decompresses
        // every block of every segment in the window. The scan does the opposite — for a
        // named level it gets an exact index hint, and flushes write level-pure segments, so
        // it touches almost nothing. So the aggregator is used exactly when the scan has
        // nothing to narrow with: when the rule constrains no level at all ("volume over the
        // last five minutes", optionally for one service), where the scan would otherwise
        // materialise every event it counts.
        //
        // That case is now much cheaper still: with no service filter a cold segment lying
        // entirely inside the window is counted from its catalog entry alone, so a 24-hour
        // rule over 500 segments opens the two boundary segments instead of all 500, every
        // 15 seconds. See the totalsOnly parameter on AggregateLogVolumeAsync.
        if (TryHeaderShape(rule.Filter, out var levels, out var service) && levels is null)
            return await HeaderCountAsync(from, to, service, ct);

        var req = new QueryRequest
        {
            Filter    = rule.Filter,
            FromUtc   = from,
            ToUtc     = to,
            Count     = MaxScannedForCount,
            Direction = QueryDirection.Backward,
        };

        // A budget per rule. The cap alone bounds the RESULT, not the work: a filter that
        // matches nothing still walks the catalog, and the evaluation loop runs rules one
        // after another, so one slow rule delays every rule behind it — and the loop waits
        // its interval AFTER the cycle, so the delay compounds.
        using var budget = StartRuleBudget(ct);

        int count = 0;
        try
        {
            await foreach (var _ in _logQuery.ExecuteAsync(req, budget.Token))
            {
                if (++count >= MaxScannedForCount) break;
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            WarnPartialCount(rule, count, "its evaluation budget expired");
            return count;
        }

        if (count >= MaxScannedForCount) WarnPartialCount(rule, count, "the scan cap was reached");
        return count;
    }

    /// <summary>
    /// Says once a minute per rule that a value is a FLOOR rather than a count — the
    /// distinction matters for the number that reaches history and the notification, and
    /// repeating it every tick for the life of a saturating rule would bury the log.
    /// </summary>
    private void WarnPartialCount(AlertRule rule, int count, string why)
    {
        var now = DateTimeOffset.UtcNow;
        if (_partialWarned.TryGetValue(rule.Id, out var last) && now - last < TimeSpan.FromMinutes(1)) return;
        _partialWarned[rule.Id] = now;
        _logger.LogWarning(
            "Alert rule {Rule} matched at least {Count} events — {Why}, so the value is a floor, not a count",
            rule.Id, count, why);
    }

    private readonly ConcurrentDictionary<string, DateTimeOffset> _partialWarned = new();

    /// <summary>
    /// Wall-clock a single rule may spend SCANNING (the header-count path has no budget — see
    /// <see cref="HeaderCountAsync"/>). Comfortably inside <see cref="EvalInterval"/> so a cycle
    /// of several slow rules still finishes before the next one is due.
    ///
    /// <para>Internal and settable for tests only. Zero or negative means "already expired": the
    /// budget token is cancelled before the rule starts, which is the only way to make expiry
    /// deterministic — a timer of 1 ms races whatever it is meant to interrupt.</para>
    /// </summary>
    internal TimeSpan RuleEvaluationBudget { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>A token source that is cancelled when this rule's evaluation budget runs out.</summary>
    private CancellationTokenSource StartRuleBudget(CancellationToken ct)
    {
        var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (RuleEvaluationBudget <= TimeSpan.Zero) budget.Cancel();
        else                                       budget.CancelAfter(RuleEvaluationBudget);
        return budget;
    }

    /// <summary>
    /// Counts straight from event headers over the whole window. <paramref name="service"/>
    /// null means every service; no level is constrained on this path (see the caller).
    /// </summary>
    private async Task<double> HeaderCountAsync(
        DateTimeOffset from, DateTimeOffset to, string? service, CancellationToken ct)
    {
        // The aggregator's axis is (bucket, service, level) and the alert wants one number,
        // so the axis collapses to a single column spanning the window. Total already has the
        // service filter applied, and no level is constrained here, so it is the answer.
        int bucketSeconds = (int)Math.Max(1, Math.Ceiling((to - from).TotalSeconds));
        long minBucket    = from.ToUnixTimeSeconds() / bucketSeconds;

        // The catalog shortcut answers Total and nothing else, and cannot attribute a segment's
        // events to one service — so it is available exactly when no service is filtered.
        bool totalsOnly = service is null;

        // NO per-rule budget on this path, deliberately, unlike the scan below. The scan has a
        // floor to report when its budget expires — the events it counted so far — so a rule
        // that is slow to count can still fire. This aggregation has nothing: cancelled, it
        // returns no number at all, and a rule that always takes longer than the budget would
        // never be evaluated, never change state and never fire, every cycle, for as long as
        // its window is that expensive. A late alert is a smaller failure than a missing one.
        //
        // What the budget protected is the rules behind this one, and the shortcut above is
        // what protects them now: with no service filter a 24-hour window costs the two
        // boundary segments, not the day. A service-filtered rule still decodes the window,
        // exactly as it did before the budget was added here.
        var counts = await _storage.AggregateLogVolumeAsync(
            from, to, minBucket, bucketSeconds, nBuckets: 1,
            serviceFilter: service, ct, totalsOnly);

        return counts.Total;
    }

    /// <summary>
    /// Analyses a rule's filter once and remembers the answer: the filter text of a rule
    /// changes only when someone edits the rule, while this question is asked of every
    /// rule on every tick.
    /// </summary>
    private bool TryHeaderShape(string? filter, out HashSet<LogLevel>? levels, out string? service)
    {
        // Bounded: the key is caller-supplied filter text, and POST /api/alerts/preview
        // lets a client mint a new one per request. Cleared wholesale rather than evicted
        // one by one — the population that matters is the set of saved rules, which is
        // small, and re-analysing a filter costs one parse.
        if (_headerShapes.Count >= MaxCachedShapes) _headerShapes.Clear();

        var shape = _headerShapes.GetOrAdd(filter ?? string.Empty, static f =>
        {
            try
            {
                var compiled = CompiledFilter.Compile(f);
                return compiled.TryGetHeaderOnlyShape(out var lv, out var svc)
                    ? new HeaderShape(true, lv, svc)
                    : new HeaderShape(false, null, null);
            }
            catch
            {
                // A filter that will not compile is the scan path's problem to report.
                return new HeaderShape(false, null, null);
            }
        });

        // A COPY of the level set: the caller must not be able to mutate the cached shape.
        levels  = shape.Levels is null ? null : new HashSet<LogLevel>(shape.Levels);
        service = shape.Service;
        return shape.HeaderOnly;
    }

    private readonly ConcurrentDictionary<string, HeaderShape> _headerShapes = new();
    private const int MaxCachedShapes = 512;

    private readonly record struct HeaderShape(bool HeaderOnly, HashSet<LogLevel>? Levels, string? Service);

    private async Task<double> MetricValueAsync(AlertRule rule, DateTimeOffset from, DateTimeOffset to, CancellationToken ct, bool preview)
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

        // Reduce over the whole window (not just the last point — a quiet final
        // interval would read 0 and miss the spike). For ">" thresholds take the
        // peak; for "<" thresholds take the trough.
        //
        // A NaN or ±Infinity point is SKIPPED (#92): it is no measurement — an exporter's division
        // by zero, an empty histogram's mean — and the panel shows it as a gap (the JSON writes it
        // as null). Folded in, it did damage both ways: Math.Max(acc, NaN) is NaN, which reset the
        // reduction and dropped the peak before it, and a NaN last in the window came out as 0 —
        // resolving a firing ">" rule, or firing a "<" rule, with nothing in the log.
        bool wantMax = rule.Comparator is AlertComparator.GreaterThan or AlertComparator.GreaterOrEqual;
        double acc = double.NaN;
        int skipped = 0;
        foreach (var s in series)
            foreach (var p in s.Points)
            {
                if (!double.IsFinite(p.Value)) { skipped++; continue; }
                if (double.IsNaN(acc)) acc = p.Value;
                else acc = wantMax ? Math.Max(acc, p.Value) : Math.Min(acc, p.Value);
            }

        if (skipped > 0 && !preview) WarnNonFinite(rule, skipped, undetermined: double.IsNaN(acc));

        // Points, and not one of them finite: there is no value to compare, and 0 — the answer for
        // an empty window — would decide the rule on data that says nothing. NaN tells the caller
        // to leave the rule's state as it is (see EvaluateAllAsync).
        if (double.IsNaN(acc)) return skipped > 0 ? double.NaN : 0;
        return acc;
    }

    /// <summary>
    /// Says ONCE per rule — per kind: some points skipped, or no value at all — that a metric rule
    /// met non-finite values. Once, because an exporter that sends NaN sends it every interval, and a
    /// line every 15 s for the life of the rule would bury the log; the operator needs to learn it
    /// happens, and which rule it touches.
    ///
    /// <para>Evaluations only, never the editor's preview: a preview of an UNSAVED rule gets a fresh
    /// random id on every click, so each one used to add a permanent entry here, and a preview of a
    /// saved rule spent that rule's only warning on something that decided nothing — and logged "the
    /// rule was not evaluated and keeps its state" for it. Entries of deleted rules are dropped each
    /// tick (<see cref="ForgetDeletedRules"/>), and the set is bounded besides, as
    /// <see cref="_headerShapes"/> is: past <see cref="MaxNonFiniteWarned"/> it is cleared wholesale
    /// (a rule may then say it once more).</para>
    /// </summary>
    private void WarnNonFinite(AlertRule rule, int skipped, bool undetermined)
    {
        if (_nonFiniteWarned.Count >= MaxNonFiniteWarned) _nonFiniteWarned.Clear();
        if (!_nonFiniteWarned.TryAdd((rule.Id, undetermined), 0)) return;
        if (undetermined)
            _logger.LogWarning(
                "Alert rule {Rule}: every point of metric {Metric} in the window is NaN or infinite ({Skipped} point(s)), "
              + "so the rule was not evaluated and keeps its state. Said once per rule",
                rule.Id, rule.Metric, skipped);
        else
            _logger.LogWarning(
                "Alert rule {Rule}: skipped {Skipped} NaN or infinite point(s) of metric {Metric}; the rule is evaluated "
              + "on the finite ones. Said once per rule",
                rule.Id, skipped, rule.Metric);
    }

    private readonly ConcurrentDictionary<(string RuleId, bool Undetermined), byte> _nonFiniteWarned = new();
    private const int MaxNonFiniteWarned = 512;

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
        // Idempotent: AlertsHostedService disposes this from both StopAsync and
        // its own DisposeAsync, and the DI container disposes the singleton too.
        // Cancelling/disposing the CTS twice throws ObjectDisposedException.
        //
        // The later callers WAIT for the first rather than returning on the exchange. Returning
        // let a second stopper (app.Run()'s chain beside the host's own, say) walk on to the
        // engines' teardown while the first was still awaiting a cycle in flight here.
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            await _disposeCompleted.Task.ConfigureAwait(false);
            return;
        }

        try
        {
            StopEvaluating();
            try { await _loop.ConfigureAwait(false); } catch (OperationCanceledException) { }
            _cts.Dispose();
        }
        finally { _disposeCompleted.TrySetResult(); }
    }

    private sealed class MutableState
    {
        public AlertState      State;
        public double          LastValue;
        public DateTimeOffset? PendingSince;
        public DateTimeOffset? LastFiredAt;
        public DateTimeOffset  EvaluatedAt;
        /// <summary>The evaluation time last written to <c>alert_state</c>; see <c>PersistEvaluationIfDue</c>.</summary>
        public DateTimeOffset? PersistedEvaluatedAt;
        public bool            Notified;
        public DateTimeOffset? AckedAt;
        public string?         AckedBy;
        public DateTimeOffset? FiringSince;
        public bool            Escalated;
    }
}

/// <summary>
/// A rule's condition value, or the statement that the store behind it could not give one (#95).
///
/// <para>An explicit result rather than an exception: a closed store is not an error in the rule,
/// and the evaluator meets it on every tick for as long as the store stays down — a throw per rule
/// per tick is a cost and a log line nobody needs.</para>
///
/// <para><see cref="Value"/> is NaN when <see cref="IsAvailable"/> is false, so it can never pass
/// for a measured 0 in a log line or a preview. It is NOT a safe value to act on: NaN compares
/// false against every threshold, and the state machine reads "not breached" as a resolve. A caller
/// asks <see cref="IsAvailable"/>.</para>
///
/// <para><b>NaN also means "available, but no finite point"</b> (#92): a metric window whose every
/// point is NaN or infinite answers <c>Of(NaN)</c> — the store answered truly, and the answer holds
/// no value. So NaN is NEVER the availability test: a caller asks <see cref="IsAvailable"/> first
/// (a store that cannot say: skip, #95), then <c>double.IsNaN(Value)</c> (no value to compare:
/// skip, #92), and acts on <see cref="Value"/> only after both.</para>
/// </summary>
public readonly record struct AlertValue(double Value, QueryAvailability Availability)
{
    /// <summary>
    /// True when <see cref="Value"/> may be acted on: the store's whole, true answer — or a Degraded
    /// store's partial one, when <see cref="AlertEvaluatorOptions.EvaluateOnDegradedStore"/> asks for it.
    /// </summary>
    public bool IsAvailable => Availability == QueryAvailability.Available;

    internal static AlertValue Of(double value) => new(value, QueryAvailability.Available);

    internal static AlertValue Unavailable(QueryAvailability why) => new(double.NaN, why);
}
