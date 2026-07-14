using System.Text.Json;
using System.Text.Json.Serialization;
using Ameto.Alerts;

namespace Ameto.Server;

public static class AlertEndpointMapper
{
    /// <summary>
    /// Alert rules CRUD + live state, history, silences, and condition preview.
    ///   GET/POST/PUT/DELETE /api/alerts[/{id}]
    ///   GET  /api/alerts/state
    ///   GET  /api/alerts/history
    ///   GET/POST/DELETE /api/alerts/silences[/{id}]
    ///   POST /api/alerts/preview
    /// </summary>
    public static void MapAlertEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/alerts").RequireAuthorization();

        // ── Live state / history (literal segments before {id}) ─────────────────
        group.MapGet("/state", (AlertEvaluator ev) => Results.Ok(ev.GetStates()));

        group.MapGet("/history", (AlertEvaluator ev, int? limit) =>
            Results.Ok(ev.GetHistory(Math.Clamp(limit ?? 200, 1, 2000))));

        // ── Silences ────────────────────────────────────────────────────────────
        group.MapGet("/silences", (AlertEvaluator ev) => Results.Ok(ev.GetSilences()));

        group.MapPost("/silences", (SilenceRequest req, AlertEvaluator ev) =>
        {
            if (string.IsNullOrWhiteSpace(req.RuleId) || req.Minutes <= 0)
                return Results.BadRequest("ruleId and positive minutes required");
            var s = ev.AddSilence(new AlertSilence
            {
                Id     = Guid.NewGuid().ToString("N")[..8],
                RuleId = req.RuleId!,
                Reason = req.Reason,
                Until  = DateTimeOffset.UtcNow.AddMinutes(req.Minutes),
            });
            return Results.Ok(s);
        });

        group.MapDelete("/silences/{id}", (string id, AlertEvaluator ev) =>
            ev.RemoveSilence(id) ? Results.NoContent() : Results.NotFound());

        // ── Maintenance windows (scheduled recurring silences) ──────────────────
        group.MapGet("/maintenance", (AlertEvaluator ev) => Results.Ok(ev.GetMaintenance()));

        group.MapPost("/maintenance", (MaintenanceRequest req, AlertEvaluator ev) =>
        {
            if (string.IsNullOrWhiteSpace(req.Name)) return Results.BadRequest("name is required");
            var w = BuildMaintenance(null, req);
            ev.UpsertMaintenance(w);
            return Results.Created($"/api/alerts/maintenance/{w.Id}", w);
        });

        group.MapPut("/maintenance/{id}", (string id, MaintenanceRequest req, AlertEvaluator ev) =>
        {
            ev.UpsertMaintenance(BuildMaintenance(id, req));
            return Results.Ok(BuildMaintenance(id, req));
        });

        group.MapDelete("/maintenance/{id}", (string id, AlertEvaluator ev) =>
            ev.RemoveMaintenance(id) ? Results.NoContent() : Results.NotFound());

        // ── Preview: evaluate the condition value right now ─────────────────────
        group.MapPost("/preview", async (AlertRuleUpsertRequest req, AlertEvaluator ev, CancellationToken ct) =>
        {
            var rule  = BuildRule(req.Id, req, null);
            double v  = await ev.PreviewAsync(rule, ct);
            bool fires = Compare(v, rule.Comparator, rule.Threshold);
            return Results.Ok(new { value = v, threshold = rule.Threshold, wouldFire = fires });
        });

        // ── Test: send a one-off notification through the rule's channels ────────
        group.MapPost("/test", async (AlertRuleUpsertRequest req, AlertRuleStore store, AlertEvaluator ev, CancellationToken ct) =>
        {
            // Resolve masked secrets against the stored rule so the test uses real credentials.
            var existing = string.IsNullOrEmpty(req.Id) ? null : store.GetById(req.Id!);
            var rule = BuildRule(req.Id, req, existing);
            if (rule.Channels.Count == 0)
                return Results.BadRequest("No channels configured to test.");
            await ev.SendTestAsync(rule, ct);
            return Results.Ok(new { sent = rule.Channels.Count });
        });

        // ── Acknowledge / un-acknowledge a firing incident (mutes re-notify) ─────
        group.MapPost("/{id}/ack", (string id, HttpContext ctx, AlertEvaluator ev) =>
            ev.Acknowledge(id, ctx.User.Identity?.Name) ? Results.Ok() : Results.BadRequest("Rule is not firing."));

        group.MapDelete("/{id}/ack", (string id, AlertEvaluator ev) =>
            ev.Unacknowledge(id) ? Results.NoContent() : Results.NotFound());

        // ── CRUD ────────────────────────────────────────────────────────────────
        // Secrets are redacted in every response; on upsert a redacted (unchanged) secret
        // is merged back from the stored rule so it is never lost or exposed to the client.
        group.MapGet("/", (AlertRuleStore store) => Results.Ok(store.GetAll().Select(Redact)));

        group.MapGet("/{id}", (string id, AlertRuleStore store) =>
            store.GetById(id) is { } r ? Results.Ok(Redact(r)) : Results.NotFound());

        group.MapPost("/", (AlertRuleUpsertRequest req, AlertRuleStore store) =>
        {
            var rule = BuildRule(null, req, null);
            store.Upsert(rule);
            return Results.Created($"/api/alerts/{rule.Id}", Redact(rule));
        });

        group.MapPut("/{id}", (string id, AlertRuleUpsertRequest req, AlertRuleStore store) =>
        {
            var existing = store.GetById(id);
            if (existing is null) return Results.NotFound();
            var rule = BuildRule(id, req, existing);
            store.Upsert(rule);
            return Results.Ok(Redact(rule));
        });

        group.MapDelete("/{id}", (string id, AlertRuleStore store) =>
            store.Delete(id) ? Results.NoContent() : Results.NotFound());
    }

    // ── Mapping ─────────────────────────────────────────────────────────────────

    /// <summary>Sentinel returned to the client in place of a set secret; sent back verbatim = "unchanged".
    /// ASCII so it round-trips through any client body encoding.</summary>
    private const string SecretMask = "********";

    private static AlertRule BuildRule(string? id, AlertRuleUpsertRequest req, AlertRule? existing)
    {
        var prev = existing?.Channels ?? [];
        var dtos = req.Channels ?? [];
        var channels = new List<AlertChannel>(dtos.Count);
        for (int i = 0; i < dtos.Count; i++)
        {
            // Match against the same-index existing channel to resolve masked (unchanged) secrets.
            var ch = MapChannel(dtos[i], i < prev.Count ? prev[i] : null);
            if (ch is not null) channels.Add(ch);
        }

        return new AlertRule
        {
            Id          = id ?? (string.IsNullOrEmpty(req.Id) ? Guid.NewGuid().ToString("N")[..8] : req.Id!),
            Name        = string.IsNullOrWhiteSpace(req.Name) ? "Unnamed" : req.Name!,
            Enabled     = req.Enabled,
            Severity    = ParseEnum(req.Severity, AlertSeverity.Warning),
            Source      = ParseEnum(req.Source, AlertSource.Log),
            Comparator  = ParseEnum(req.Comparator, AlertComparator.GreaterOrEqual),
            Threshold   = req.Threshold,
            Window      = TimeSpan.FromSeconds(req.WindowSeconds   > 0 ? req.WindowSeconds   : 300),
            For         = TimeSpan.FromSeconds(req.ForSeconds      > 0 ? req.ForSeconds      : 0),
            Cooldown       = TimeSpan.FromSeconds(req.CooldownSeconds > 0 ? req.CooldownSeconds : 900),
            RepeatInterval = TimeSpan.FromSeconds(req.RepeatSeconds > 0 ? req.RepeatSeconds : 0),
            EscalateAfter  = TimeSpan.FromSeconds(req.EscalateSeconds > 0 ? req.EscalateSeconds : 0),
            Filter      = req.Filter,
            NoData      = req.NoData,
            Metric      = req.Metric,
            Aggregation = req.Aggregation,
            Quantile    = req.Quantile,
            GroupBy     = req.GroupBy,
            Labels      = req.Labels,
            Service     = req.Service,
            TraceMetric = ParseEnum(req.TraceMetric, TraceMetricKind.ErrorRatePct),
            Channels    = channels,
            Template    = req.Template,
        };
    }

    private static AlertChannel? MapChannel(ChannelDto d, AlertChannel? prev)
    {
        var ch = BuildChannel(d, prev);
        if (ch is not null)
        {
            ch.EscalationOnly = d.EscalationOnly;
            ch.MinSeverity    = Enum.TryParse<AlertSeverity>(d.MinSeverity, true, out var s) ? s : null;
        }
        return ch;
    }

    private static AlertChannel? BuildChannel(ChannelDto d, AlertChannel? prev)
    {
        switch (d.Type?.ToLowerInvariant())
        {
            case "webhook":
                if (string.IsNullOrEmpty(d.Url)) return null;
                return new WebhookChannel { Url = d.Url!, Headers = UnmaskHeaders(d.Headers, (prev as WebhookChannel)?.Headers) };

            case "smtp":
                if (string.IsNullOrEmpty(d.Host) || string.IsNullOrEmpty(d.From) || string.IsNullOrEmpty(d.To)) return null;
                return new SmtpChannel
                {
                    Host = d.Host!, Port = d.Port ?? 587, UseSsl = d.UseSsl ?? true, Username = d.Username,
                    Password = Unmask(d.Password, (prev as SmtpChannel)?.Password), From = d.From!, To = d.To!,
                };

            case "telegram":
                var token = Unmask(d.BotToken, (prev as TelegramChannel)?.BotToken);
                if (string.IsNullOrEmpty(d.ChatId) || string.IsNullOrEmpty(token)) return null;
                return new TelegramChannel { BotToken = token!, ChatId = d.ChatId! };

            case "slack":
                var slackUrl = Unmask(d.WebhookUrl, (prev as SlackChannel)?.WebhookUrl);
                return string.IsNullOrEmpty(slackUrl) ? null : new SlackChannel { WebhookUrl = slackUrl! };

            case "discord":
                var discordUrl = Unmask(d.WebhookUrl, (prev as DiscordChannel)?.WebhookUrl);
                return string.IsNullOrEmpty(discordUrl) ? null : new DiscordChannel { WebhookUrl = discordUrl! };

            case "teams":
                var teamsUrl = Unmask(d.WebhookUrl, (prev as TeamsChannel)?.WebhookUrl);
                return string.IsNullOrEmpty(teamsUrl) ? null : new TeamsChannel { WebhookUrl = teamsUrl! };

            case "pagerduty":
                var key = Unmask(d.RoutingKey, (prev as PagerDutyChannel)?.RoutingKey);
                return string.IsNullOrEmpty(key) ? null : new PagerDutyChannel { RoutingKey = key! };

            case "httpflow":
                var steps = d.Steps ?? [];
                if (steps.Count == 0) return null;
                return new HttpFlowChannel
                {
                    Steps   = steps,
                    Secrets = MergeSecrets(d.Secrets, (prev as HttpFlowChannel)?.Secrets),
                };

            default: return null;
        }
    }

    /// <summary>Write-only merge for the secret variables: an unchanged (masked) value keeps the stored one.</summary>
    private static Dictionary<string, string> MergeSecrets(Dictionary<string, string>? incoming, Dictionary<string, string>? existing)
    {
        var result = new Dictionary<string, string>();
        if (incoming is null) return result;
        foreach (var (k, v) in incoming)
            result[k] = v == SecretMask && existing is not null && existing.TryGetValue(k, out var e) ? e : v;
        return result;
    }

    // ── Secret redaction (responses) + unmask-merge (requests) ────────────────────

    private static AlertRule Redact(AlertRule r) => new()
    {
        Id          = r.Id, Name = r.Name, Enabled = r.Enabled, Severity = r.Severity, Source = r.Source,
        Comparator  = r.Comparator, Threshold = r.Threshold, Window = r.Window, For = r.For, Cooldown = r.Cooldown,
        RepeatInterval = r.RepeatInterval, EscalateAfter = r.EscalateAfter,
        Filter      = r.Filter, NoData = r.NoData, Metric = r.Metric, Aggregation = r.Aggregation, Quantile = r.Quantile,
        GroupBy     = r.GroupBy, Labels = r.Labels, Service = r.Service, TraceMetric = r.TraceMetric,
        Channels    = r.Channels.Select(RedactChannel).ToList(), Template = r.Template,
    };

    private static AlertChannel RedactChannel(AlertChannel ch)
    {
        AlertChannel red = ch switch
        {
            TelegramChannel t => new TelegramChannel { ChatId = t.ChatId, BotToken = Mask(t.BotToken) },
            SmtpChannel s     => new SmtpChannel { Host = s.Host, Port = s.Port, UseSsl = s.UseSsl, Username = s.Username,
                                                   Password = Mask(s.Password), From = s.From, To = s.To },
            WebhookChannel w  => new WebhookChannel { Url = w.Url, Headers = w.Headers?.ToDictionary(kv => kv.Key, _ => SecretMask) },
            SlackChannel s     => new SlackChannel     { WebhookUrl = Mask(s.WebhookUrl) },
            DiscordChannel d   => new DiscordChannel   { WebhookUrl = Mask(d.WebhookUrl) },
            TeamsChannel tm    => new TeamsChannel     { WebhookUrl = Mask(tm.WebhookUrl) },
            PagerDutyChannel p => new PagerDutyChannel { RoutingKey = Mask(p.RoutingKey) },
            HttpFlowChannel hf => new HttpFlowChannel  { Steps = hf.Steps, Secrets = hf.Secrets.ToDictionary(kv => kv.Key, _ => SecretMask) },
            _ => ch,
        };
        red.EscalationOnly = ch.EscalationOnly;
        red.MinSeverity    = ch.MinSeverity;
        return red;
    }

    private static string Mask(string? v) => string.IsNullOrEmpty(v) ? string.Empty : SecretMask;
    private static string? Unmask(string? incoming, string? existing) => incoming == SecretMask ? existing : incoming;

    private static Dictionary<string, string>? UnmaskHeaders(Dictionary<string, string>? incoming, Dictionary<string, string>? existing)
    {
        if (incoming is null || existing is null) return incoming;
        return incoming.ToDictionary(
            kv => kv.Key,
            kv => kv.Value == SecretMask && existing.TryGetValue(kv.Key, out var e) ? e : kv.Value);
    }

    private static MaintenanceWindow BuildMaintenance(string? id, MaintenanceRequest req) => new()
    {
        Id              = id ?? (string.IsNullOrEmpty(req.Id) ? Guid.NewGuid().ToString("N")[..8] : req.Id!),
        Name            = string.IsNullOrWhiteSpace(req.Name) ? "Maintenance" : req.Name!,
        Enabled         = req.Enabled,
        DaysOfWeek      = req.DaysOfWeek is > 0 and < 128 ? req.DaysOfWeek : 127,
        StartMinuteUtc  = Math.Clamp(req.StartMinuteUtc, 0, 1439),
        DurationMinutes = Math.Clamp(req.DurationMinutes, 1, 1440),
        MaxSeverity     = Enum.TryParse<AlertSeverity>(req.MaxSeverity, true, out var s) ? s : null,
    };

    private static bool Compare(double v, AlertComparator c, double t) => c switch
    {
        AlertComparator.GreaterThan => v > t, AlertComparator.GreaterOrEqual => v >= t,
        AlertComparator.LessThan => v < t, AlertComparator.LessOrEqual => v <= t, _ => false,
    };

    private static T ParseEnum<T>(string? s, T fallback) where T : struct, Enum =>
        Enum.TryParse<T>(s, ignoreCase: true, out var v) ? v : fallback;
}

// ── Request DTOs ────────────────────────────────────────────────────────────────

public sealed class AlertRuleUpsertRequest
{
    public string?  Id          { get; init; }
    public string?  Name        { get; init; }
    public bool     Enabled     { get; init; } = true;
    public string?  Severity    { get; init; }
    public string?  Source      { get; init; }
    public string?  Comparator  { get; init; }
    public double   Threshold   { get; init; } = 1;
    public int      WindowSeconds   { get; init; } = 300;
    public int      ForSeconds      { get; init; }
    public int      CooldownSeconds { get; init; } = 900;
    public int      RepeatSeconds   { get; init; }
    public int      EscalateSeconds { get; init; }
    public string?  Filter      { get; init; }
    public bool     NoData      { get; init; }
    public string?  Metric      { get; init; }
    public string?  Aggregation { get; init; }
    public double?  Quantile    { get; init; }
    public string[]? GroupBy    { get; init; }
    public Dictionary<string,string>? Labels { get; init; }
    public string?  Service     { get; init; }
    public string?  TraceMetric { get; init; }
    public List<ChannelDto>? Channels { get; init; }
    public string?  Template    { get; init; }
}

/// <summary>Flat channel payload — avoids polymorphic deserialization of abstract AlertChannel.</summary>
public sealed class ChannelDto
{
    public string? Type     { get; init; }
    // webhook
    public string? Url      { get; init; }
    public Dictionary<string,string>? Headers { get; init; }
    // smtp
    public string? Host     { get; init; }
    public int?    Port     { get; init; }
    public bool?   UseSsl   { get; init; }
    public string? Username { get; init; }
    public string? Password { get; init; }
    public string? From     { get; init; }
    public string? To       { get; init; }
    // telegram
    public string? BotToken { get; init; }
    public string? ChatId   { get; init; }
    // slack / discord / teams
    public string? WebhookUrl { get; init; }
    // pagerduty
    public string? RoutingKey { get; init; }
    // routing
    public bool    EscalationOnly { get; init; }
    public string? MinSeverity    { get; init; }  // "Info" | "Warning" | "Critical" | null (any)
    // httpflow
    public List<HttpFlowStep>?          Steps   { get; init; }
    public Dictionary<string, string>?  Secrets { get; init; }
}

public sealed class SilenceRequest
{
    public string? RuleId  { get; init; }
    public int     Minutes { get; init; }
    public string? Reason  { get; init; }
}

public sealed class MaintenanceRequest
{
    public string? Id              { get; init; }
    public string? Name            { get; init; }
    public bool    Enabled         { get; init; } = true;
    public int     DaysOfWeek      { get; init; } = 127;
    public int     StartMinuteUtc  { get; init; }
    public int     DurationMinutes { get; init; } = 60;
    public string? MaxSeverity     { get; init; }  // "Info" | "Warning" | "Critical" | null (all)
}

/// <summary>
/// Serialises <see cref="AlertChannel"/> by its runtime type so derived fields
/// (chatId, url, host, and the already-masked secrets) reach the client — the default
/// serializer would emit only the abstract base's <c>type</c>. Deserialisation goes
/// through <see cref="ChannelDto"/>, so <see cref="Read"/> is never exercised.
/// </summary>
public sealed class AlertChannelResponseConverter : JsonConverter<AlertChannel>
{
    public override AlertChannel? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => throw new NotSupportedException("AlertChannel is read via ChannelDto.");

    public override void Write(Utf8JsonWriter writer, AlertChannel value, JsonSerializerOptions options)
        => JsonSerializer.Serialize(writer, value, value.GetType(), options);
}
