namespace Ameto.Alerts;

// ── Enums ───────────────────────────────────────────────────────────────────────

/// <summary>What signal the rule evaluates.</summary>
public enum AlertSource { Log, Metric, Trace }

public enum AlertSeverity { Info, Warning, Critical }

/// <summary>How the evaluated value is compared to the threshold.</summary>
public enum AlertComparator { GreaterThan, GreaterOrEqual, LessThan, LessOrEqual }

/// <summary>Lifecycle state of a rule.</summary>
public enum AlertState { Ok, Pending, Firing, NoData }

/// <summary>For trace rules — which quantity to evaluate per service.</summary>
public enum TraceMetricKind { ErrorRatePct, P50Ms, P95Ms, P99Ms, AvgDurationMs, SpanCount }

// ── Alert rule model (v2 — unified log/metric/trace) ────────────────────────────

/// <summary>
/// A unified alerting rule. The evaluator periodically computes a single numeric
/// <em>value</em> from the chosen <see cref="Source"/> over <see cref="Window"/>,
/// compares it to <see cref="Threshold"/> via <see cref="Comparator"/>, and drives a
/// state machine (OK → Pending → Firing → resolved) honouring <see cref="For"/>.
/// </summary>
public sealed class AlertRule
{
    public required string        Id       { get; init; }
    public required string        Name     { get; init; }
    public bool                   Enabled  { get; init; } = true;
    public AlertSeverity          Severity { get; init; } = AlertSeverity.Warning;
    public AlertSource            Source   { get; init; } = AlertSource.Log;

    // ── Condition ───────────────────────────────────────────────────────────────
    public AlertComparator        Comparator { get; init; } = AlertComparator.GreaterOrEqual;
    public double                 Threshold  { get; init; } = 1;
    /// <summary>Rolling evaluation window.</summary>
    public TimeSpan               Window     { get; init; } = TimeSpan.FromMinutes(5);
    /// <summary>Condition must hold this long before firing (anti-flap). Zero = fire immediately.</summary>
    public TimeSpan               For        { get; init; } = TimeSpan.Zero;
    /// <summary>Minimum gap between repeated firings.</summary>
    public TimeSpan               Cooldown   { get; init; } = TimeSpan.FromMinutes(15);

    // ── Log source ────────────────────────────────────────────────────────────
    /// <summary>Seq filter expression; value = count of matching events in the window.</summary>
    public string?                Filter     { get; init; }
    /// <summary>When true, the rule fires on the ABSENCE of matching events (dead-man's switch).</summary>
    public bool                   NoData     { get; init; }

    // ── Metric source ───────────────────────────────────────────────────────────
    public string?                Metric      { get; init; }
    /// <summary>rate|increase|avg|min|max|last|sum|quantile.</summary>
    public string?                Aggregation { get; init; }
    public double?                Quantile    { get; init; }
    public string[]?              GroupBy     { get; init; }
    public Dictionary<string,string>? Labels  { get; init; }

    // ── Trace source ──────────────────────────────────────────────────────────
    public string?                Service     { get; init; }
    public TraceMetricKind        TraceMetric { get; init; } = TraceMetricKind.ErrorRatePct;

    // ── Notification ────────────────────────────────────────────────────────────
    public IReadOnlyList<AlertChannel> Channels { get; init; } = [];
    /// <summary>Optional message template; placeholders {{name}} {{value}} {{severity}} {{state}} {{threshold}}.</summary>
    public string?                Template { get; init; }
}

// ── Notification channels ─────────────────────────────────────────────────────

public abstract class AlertChannel
{
    public string Type { get; init; } = "";
}

/// <summary>POST JSON payload to an HTTP endpoint.</summary>
public sealed class WebhookChannel : AlertChannel
{
    public WebhookChannel() => Type = "webhook";
    public required string Url { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
}

/// <summary>Send an email via SMTP.</summary>
public sealed class SmtpChannel : AlertChannel
{
    public SmtpChannel() => Type = "smtp";
    public required string Host     { get; init; }
    public int             Port     { get; init; } = 587;
    public bool            UseSsl   { get; init; } = true;
    public string?         Username { get; init; }
    public string?         Password { get; init; }
    public required string From     { get; init; }
    public required string To       { get; init; }
}

/// <summary>Send a message via the Telegram Bot API.</summary>
public sealed class TelegramChannel : AlertChannel
{
    public TelegramChannel() => Type = "telegram";
    public required string BotToken { get; init; }
    public required string ChatId   { get; init; }
}

// ── Runtime state + history + silences ──────────────────────────────────────────

/// <summary>Live evaluation state of a rule (not persisted with the rule itself).</summary>
public sealed class AlertStateSnapshot
{
    public required string         RuleId      { get; init; }
    public AlertState              State       { get; init; }
    public double                  LastValue   { get; init; }
    public DateTimeOffset?         PendingSince { get; init; }
    public DateTimeOffset?         LastFiredAt  { get; init; }
    public DateTimeOffset          EvaluatedAt  { get; init; }
}

/// <summary>A persisted state-transition record for the history timeline.</summary>
public sealed class AlertHistoryEntry
{
    public required string         RuleId    { get; init; }
    public required string         RuleName  { get; init; }
    public AlertSeverity           Severity  { get; init; }
    public AlertState              State     { get; init; } // Firing or Ok (resolved)
    public double                  Value     { get; init; }
    public double                  Threshold { get; init; }
    public required DateTimeOffset At        { get; init; }
}

/// <summary>Temporarily mutes a rule's notifications until <see cref="Until"/>.</summary>
public sealed class AlertSilence
{
    public required string         Id     { get; init; }
    public required string         RuleId { get; init; }
    public string?                 Reason { get; init; }
    public required DateTimeOffset Until  { get; init; }
    public DateTimeOffset          CreatedAt { get; init; } = DateTimeOffset.UtcNow;
}

// ── Firing event ──────────────────────────────────────────────────────────────

/// <summary>Payload passed to channel dispatchers on a state transition.</summary>
public sealed class AlertFiredEvent
{
    public required AlertRule      Rule    { get; init; }
    public required AlertState     State   { get; init; } // Firing or Ok (resolved)
    public required double         Value   { get; init; }
    public required DateTimeOffset At      { get; init; }
    /// <summary>Representative sample events (log source only), up to 5.</summary>
    public IReadOnlyList<Ameto.Core.LogEvent> SampleEvents { get; init; } = [];
}
