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

        // ── Preview: evaluate the condition value right now ─────────────────────
        group.MapPost("/preview", async (AlertRuleUpsertRequest req, AlertEvaluator ev, CancellationToken ct) =>
        {
            var rule  = BuildRule(req.Id, req);
            double v  = await ev.PreviewAsync(rule, ct);
            bool fires = Compare(v, rule.Comparator, rule.Threshold);
            return Results.Ok(new { value = v, threshold = rule.Threshold, wouldFire = fires });
        });

        // ── CRUD ────────────────────────────────────────────────────────────────
        group.MapGet("/", (AlertRuleStore store) => Results.Ok(store.GetAll()));

        group.MapGet("/{id}", (string id, AlertRuleStore store) =>
            store.GetById(id) is { } r ? Results.Ok(r) : Results.NotFound());

        group.MapPost("/", (AlertRuleUpsertRequest req, AlertRuleStore store) =>
        {
            var rule = BuildRule(null, req);
            store.Upsert(rule);
            return Results.Created($"/api/alerts/{rule.Id}", rule);
        });

        group.MapPut("/{id}", (string id, AlertRuleUpsertRequest req, AlertRuleStore store) =>
        {
            if (store.GetById(id) is null) return Results.NotFound();
            var rule = BuildRule(id, req);
            store.Upsert(rule);
            return Results.Ok(rule);
        });

        group.MapDelete("/{id}", (string id, AlertRuleStore store) =>
            store.Delete(id) ? Results.NoContent() : Results.NotFound());
    }

    // ── Mapping ─────────────────────────────────────────────────────────────────

    private static AlertRule BuildRule(string? id, AlertRuleUpsertRequest req) => new()
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
        Cooldown    = TimeSpan.FromSeconds(req.CooldownSeconds > 0 ? req.CooldownSeconds : 900),
        Filter      = req.Filter,
        NoData      = req.NoData,
        Metric      = req.Metric,
        Aggregation = req.Aggregation,
        Quantile    = req.Quantile,
        GroupBy     = req.GroupBy,
        Labels      = req.Labels,
        Service     = req.Service,
        TraceMetric = ParseEnum(req.TraceMetric, TraceMetricKind.ErrorRatePct),
        Channels    = (req.Channels ?? []).Select(MapChannel).Where(c => c is not null).Select(c => c!).ToList(),
        Template    = req.Template,
    };

    private static AlertChannel? MapChannel(ChannelDto d) => d.Type?.ToLowerInvariant() switch
    {
        "webhook" when !string.IsNullOrEmpty(d.Url)
            => new WebhookChannel { Url = d.Url!, Headers = d.Headers },
        "smtp" when !string.IsNullOrEmpty(d.Host) && !string.IsNullOrEmpty(d.From) && !string.IsNullOrEmpty(d.To)
            => new SmtpChannel { Host = d.Host!, Port = d.Port ?? 587, UseSsl = d.UseSsl ?? true,
                                 Username = d.Username, Password = d.Password, From = d.From!, To = d.To! },
        "telegram" when !string.IsNullOrEmpty(d.BotToken) && !string.IsNullOrEmpty(d.ChatId)
            => new TelegramChannel { BotToken = d.BotToken!, ChatId = d.ChatId! },
        _ => null,
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
}

public sealed class SilenceRequest
{
    public string? RuleId  { get; init; }
    public int     Minutes { get; init; }
    public string? Reason  { get; init; }
}
