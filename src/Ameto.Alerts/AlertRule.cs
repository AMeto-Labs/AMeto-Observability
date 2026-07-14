using System.Text.Json.Serialization;

namespace Ameto.Alerts;

// ── Enums ───────────────────────────────────────────────────────────────────────
// Serialized by name (not ordinal) so API responses match the client's string unions.

/// <summary>What signal the rule evaluates.</summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum AlertSource { Log, Metric, Trace }

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum AlertSeverity { Info, Warning, Critical }

/// <summary>How the evaluated value is compared to the threshold.</summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum AlertComparator { GreaterThan, GreaterOrEqual, LessThan, LessOrEqual }

/// <summary>Lifecycle state of a rule.</summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum AlertState { Ok, Pending, Firing, NoData }

/// <summary>For trace rules — which quantity to evaluate per service.</summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
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
    /// <summary>While a rule stays firing, re-send the notification this often. Zero = notify once until resolved.</summary>
    public TimeSpan               RepeatInterval { get; init; } = TimeSpan.Zero;
    /// <summary>If still firing and un-acknowledged after this long, notify the escalation-only channels. Zero = never.</summary>
    public TimeSpan               EscalateAfter  { get; init; } = TimeSpan.Zero;

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
    /// <summary>When true, this channel only fires on escalation (not on the initial notification).</summary>
    public bool   EscalationOnly { get; set; }
    /// <summary>When set, this channel only fires if the rule's severity is at least this. Null = any severity.</summary>
    public AlertSeverity? MinSeverity { get; set; }
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

/// <summary>Post to a Slack incoming webhook. The URL is a capability secret.</summary>
public sealed class SlackChannel : AlertChannel
{
    public SlackChannel() => Type = "slack";
    public required string WebhookUrl { get; init; }
}

/// <summary>Post to a Discord webhook. The URL is a capability secret.</summary>
public sealed class DiscordChannel : AlertChannel
{
    public DiscordChannel() => Type = "discord";
    public required string WebhookUrl { get; init; }
}

/// <summary>Post a MessageCard to a Microsoft Teams incoming webhook. The URL is a capability secret.</summary>
public sealed class TeamsChannel : AlertChannel
{
    public TeamsChannel() => Type = "teams";
    public required string WebhookUrl { get; init; }
}

/// <summary>Trigger/resolve a PagerDuty incident via the Events API v2. The routing key is a secret.</summary>
public sealed class PagerDutyChannel : AlertChannel
{
    public PagerDutyChannel() => Type = "pagerduty";
    public required string RoutingKey { get; init; }
}

/// <summary>
/// A multi-step HTTP workflow (Postman-style). Steps run sequentially; a value can be
/// extracted from a response into a variable, and variables (<c>{{name}}</c>) substitute
/// anywhere — URL, header key/value, or body. Secret values live in <see cref="Secrets"/>
/// (encrypted at rest) and are referenced as <c>{{secret.name}}</c>.
/// </summary>
public sealed class HttpFlowChannel : AlertChannel
{
    public HttpFlowChannel() => Type = "httpflow";
    public List<HttpFlowStep>          Steps   { get; init; } = [];
    /// <summary>Secret variables — encrypted at rest, referenced as <c>{{secret.name}}</c>.</summary>
    public Dictionary<string, string>  Secrets { get; init; } = [];
}

public sealed class HttpFlowStep
{
    public string            Name     { get; init; } = "";
    public string            Method   { get; init; } = "POST";      // GET/POST/PUT/PATCH/DELETE
    public string            Url      { get; init; } = "";
    public List<HttpHeader>  Headers  { get; init; } = [];
    public string            BodyType { get; init; } = "none";      // none|json|text|form|xml
    public string?           Body     { get; init; }                // raw template with {{vars}}
    /// <summary>Response values to capture into variables for later steps.</summary>
    public List<HttpExtract> Extracts { get; init; } = [];
}

public sealed class HttpHeader
{
    public string Key   { get; init; } = "";
    public string Value { get; init; } = "";
}

public sealed class HttpExtract
{
    public string Var    { get; init; } = "";       // variable name to set
    public string Source { get; init; } = "json";   // json | xml | header | regex | status
    public string Expr   { get; init; } = "";       // JSONPath / XPath / header name / regex pattern
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
    /// <summary>When the current firing incident was acknowledged (mutes re-notify). Null = not acked.</summary>
    public DateTimeOffset?         AckedAt      { get; init; }
    public string?                 AckedBy      { get; init; }
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

/// <summary>
/// A recurring maintenance window that suppresses alert notifications on a daily schedule
/// (e.g. nightly deploys). While active it mutes matching rules — like a silence, but
/// scheduled and repeating rather than one-off.
/// </summary>
public sealed class MaintenanceWindow
{
    public required string  Id      { get; init; }
    public required string  Name    { get; init; }
    public bool             Enabled { get; init; } = true;
    /// <summary>Days the window is active. Bit 0 = Sunday … bit 6 = Saturday. 127 = every day.</summary>
    public int              DaysOfWeek     { get; init; } = 127;
    /// <summary>Start time as minutes from midnight UTC (0–1439).</summary>
    public int              StartMinuteUtc { get; init; }
    /// <summary>Window length in minutes (may cross midnight).</summary>
    public int              DurationMinutes { get; init; } = 60;
    /// <summary>When set, only suppresses rules at this severity or below (null = all severities).</summary>
    public AlertSeverity?   MaxSeverity    { get; init; }

    /// <summary>True if the window covers <paramref name="nowUtc"/> (handles cross-midnight spill).</summary>
    public bool IsActiveAt(DateTimeOffset nowUtc)
    {
        if (!Enabled || DurationMinutes <= 0) return false;
        int nowMin = nowUtc.Hour * 60 + nowUtc.Minute;
        int nowDow = (int)nowUtc.DayOfWeek; // 0 = Sunday
        // A window active now may have started today (offset 0) or yesterday (offset 1, crossing midnight).
        for (int offset = 0; offset <= 1; offset++)
        {
            int startDow = (nowDow - offset + 7) % 7;
            if ((DaysOfWeek & (1 << startDow)) == 0) continue;
            int elapsed = nowMin + offset * 1440 - StartMinuteUtc;
            if (elapsed >= 0 && elapsed < DurationMinutes) return true;
        }
        return false;
    }

    public bool Matches(AlertSeverity severity) => MaxSeverity is null || severity <= MaxSeverity.Value;
}

// ── Firing event ──────────────────────────────────────────────────────────────

/// <summary>Payload passed to channel dispatchers on a state transition.</summary>
public sealed class AlertFiredEvent
{
    public required AlertRule      Rule    { get; init; }
    public required AlertState     State   { get; init; } // Firing or Ok (resolved)
    public required double         Value   { get; init; }
    public required DateTimeOffset At      { get; init; }
    /// <summary>True for a manual "send test" — rendered with a TEST marker, not tied to state.</summary>
    public bool                    IsTest  { get; init; }
    /// <summary>True when this is an escalation notification (routed to escalation-only channels).</summary>
    public bool                    IsEscalation { get; init; }
    /// <summary>Representative sample events (log source only), up to 5.</summary>
    public IReadOnlyList<Ameto.Core.LogEvent> SampleEvents { get; init; } = [];
}
