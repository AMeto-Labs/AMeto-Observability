namespace Ameto.Alerts;

// ── Alert rule model ──────────────────────────────────────────────────────────

/// <summary>
/// Defines when to fire an alert.
///
/// An alert fires when the number of events matching <see cref="Filter"/>
/// reaches <see cref="Threshold"/> within the rolling <see cref="Window"/>.
/// Cooldown prevents re-firing until the window has elapsed after the last fire.
/// </summary>
public sealed class AlertRule
{
    /// <summary>Stable unique identifier (slug). Used as storage key.</summary>
    public required string Id { get; init; }

    /// <summary>Human-readable name shown in the UI.</summary>
    public required string Name { get; init; }

    /// <summary>Seq Filter Expression. Empty / null means match all events.</summary>
    public string? Filter { get; init; }

    /// <summary>How many matching events trigger the alert.</summary>
    public int Threshold { get; init; } = 1;

    /// <summary>Sliding evaluation window.</summary>
    public TimeSpan Window { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Minimum gap between repeated firings of the same rule.</summary>
    public TimeSpan Cooldown { get; init; } = TimeSpan.FromMinutes(15);

    /// <summary>Whether this rule is currently active.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Notification channels to invoke when the rule fires.</summary>
    public IReadOnlyList<AlertChannel> Channels { get; init; } = [];
}

// ── Notification channels ─────────────────────────────────────────────────────

public abstract class AlertChannel
{
    public required string Type { get; init; }
}

/// <summary>POST JSON payload to an HTTP endpoint.</summary>
public sealed class WebhookChannel : AlertChannel
{
    public WebhookChannel() => Type = "webhook";
    public required string Url { get; init; }

    /// <summary>Optional extra headers, e.g. Authorization.</summary>
    public Dictionary<string, string>? Headers { get; init; }
}

/// <summary>Send an email via SMTP.</summary>
public sealed class SmtpChannel : AlertChannel
{
    public SmtpChannel() => Type = "smtp";
    public required string Host       { get; init; }
    public int             Port       { get; init; } = 587;
    public bool            UseSsl     { get; init; } = true;
    public string?         Username   { get; init; }
    public string?         Password   { get; init; }
    public required string From       { get; init; }
    public required string To         { get; init; }
}

// ── Firing event ──────────────────────────────────────────────────────────────

/// <summary>Payload passed to channel dispatchers when a rule fires.</summary>
public sealed class AlertFiredEvent
{
    public required AlertRule Rule       { get; init; }
    public required int       Count      { get; init; }
    public required DateTimeOffset FiredAt { get; init; }

    /// <summary>A representative sample of the matching events (up to 5).</summary>
    public IReadOnlyList<Ameto.Core.LogEvent> SampleEvents { get; init; } = [];
}
