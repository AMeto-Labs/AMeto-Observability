using System.Net.Mail;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Ameto.Alerts;

/// <summary>
/// Dispatches an alert state-transition (firing or resolved) to every channel on the rule.
/// Channels run concurrently; a channel failure is logged and does not affect the others.
/// </summary>
public sealed class AlertDispatcher
{
    private readonly ILogger<AlertDispatcher> _logger;
    private static readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(15) };
    private static readonly JsonSerializerOptions _json = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public AlertDispatcher(ILogger<AlertDispatcher> logger) => _logger = logger;

    public async Task DispatchAsync(AlertFiredEvent fired)
    {
        bool resolved = fired.State == AlertState.Ok;
        _logger.Log(resolved && !fired.IsTest ? LogLevel.Information : LogLevel.Warning,
            "Alert {State}: [{Rule}] value={Value} threshold={Threshold} at {At:O}",
            fired.IsTest ? "TEST" : fired.IsEscalation ? "ESCALATED" : resolved ? "RESOLVED" : "FIRING",
            fired.Rule.Name, fired.Value, fired.Rule.Threshold, fired.At);

        await Task.WhenAll(SelectChannels(fired).Select(ch => DispatchOneAsync(ch, fired)));
    }

    /// <summary>
    /// Routes an event to the right channels: a test hits every channel; otherwise a channel
    /// must pass its severity gate, and the tier must match (escalation events → escalation-only
    /// channels; the initial firing → non-escalation channels; a resolve → all matching channels).
    /// </summary>
    private static IEnumerable<AlertChannel> SelectChannels(AlertFiredEvent f)
    {
        if (f.IsTest) return f.Rule.Channels;
        var sev = f.Rule.Severity;
        if (f.State == AlertState.Ok)
            return f.Rule.Channels.Where(c => SeverityAllows(c, sev));
        return f.Rule.Channels.Where(c => c.EscalationOnly == f.IsEscalation && SeverityAllows(c, sev));
    }

    private static bool SeverityAllows(AlertChannel c, AlertSeverity ruleSeverity) =>
        c.MinSeverity is null || ruleSeverity >= c.MinSeverity.Value;

    private Task DispatchOneAsync(AlertChannel channel, AlertFiredEvent fired) => channel switch
    {
        WebhookChannel   wh => SendWebhookAsync(wh, fired),
        SmtpChannel      sm => SendSmtpAsync(sm, fired),
        TelegramChannel  tg => SendTelegramAsync(tg, fired),
        SlackChannel     sl => SendSlackAsync(sl, fired),
        DiscordChannel   dc => SendDiscordAsync(dc, fired),
        TeamsChannel     tm => SendTeamsAsync(tm, fired),
        PagerDutyChannel pd => SendPagerDutyAsync(pd, fired),
        HttpFlowChannel  hf => SendHttpFlowAsync(hf, fired),
        _                   => Task.CompletedTask,
    };

    // ── HTTP flow (multi-step, Postman-style) ───────────────────────────────────

    private async Task SendHttpFlowAsync(HttpFlowChannel ch, AlertFiredEvent fired)
    {
        try { await HttpFlowExecutor.RunAsync(ch, BuildFlowVars(fired), _http, _logger, CancellationToken.None); }
        catch (Exception ex) { _logger.LogError(ex, "HTTP flow dispatch failed for rule {Rule}", fired.Rule.Name); }
    }

    private static Dictionary<string, string> BuildFlowVars(AlertFiredEvent fired)
    {
        var r = fired.Rule;
        bool resolved = fired.State == AlertState.Ok;
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["alert.name"]      = r.Name,
            ["alert.value"]     = Fmt(fired.Value),
            ["alert.threshold"] = Fmt(r.Threshold),
            ["alert.severity"]  = r.Severity.ToString().ToLowerInvariant(),
            ["alert.state"]     = fired.IsTest ? "test" : resolved ? "resolved" : "firing",
            ["alert.source"]    = r.Source.ToString().ToLowerInvariant(),
            ["alert.message"]   = RenderMessage(fired),
            ["alert.at"]        = fired.At.ToString("O"),
        };
    }

    // ── Message rendering ───────────────────────────────────────────────────────

    /// <summary>Renders the notification text — custom template or a sensible default.</summary>
    private static string RenderMessage(AlertFiredEvent fired)
    {
        var r = fired.Rule;
        bool resolved = fired.State == AlertState.Ok;
        string status = fired.IsTest       ? "🧪 TEST"
                      : fired.IsEscalation ? "⏫ ESCALATED"
                      : resolved           ? "✅ RESOLVED"
                                           : Icon(r.Severity) + " FIRING";

        if (!string.IsNullOrWhiteSpace(r.Template))
            return r.Template
                .Replace("{{name}}",      r.Name)
                .Replace("{{value}}",     Fmt(fired.Value))
                .Replace("{{threshold}}", Fmt(r.Threshold))
                .Replace("{{severity}}",  r.Severity.ToString().ToLowerInvariant())
                .Replace("{{state}}",     resolved ? "resolved" : "firing")
                .Replace("{{source}}",    r.Source.ToString().ToLowerInvariant());

        var sb = new StringBuilder();
        sb.AppendLine($"{status} — {r.Name}");
        sb.AppendLine($"severity: {r.Severity.ToString().ToLowerInvariant()} · source: {r.Source.ToString().ToLowerInvariant()}");
        sb.AppendLine($"value {Fmt(fired.Value)} {Op(r.Comparator)} {Fmt(r.Threshold)} (window {(int)r.Window.TotalSeconds}s)");
        if (r.Source == AlertSource.Log && !string.IsNullOrEmpty(r.Filter)) sb.AppendLine($"filter: {r.Filter}");
        if (r.Source == AlertSource.Metric && !string.IsNullOrEmpty(r.Metric)) sb.AppendLine($"metric: {r.Metric}");
        if (r.Source == AlertSource.Trace && !string.IsNullOrEmpty(r.Service)) sb.AppendLine($"service: {r.Service} · {r.TraceMetric}");
        sb.AppendLine($"at {fired.At:yyyy-MM-dd HH:mm:ss} UTC");
        return sb.ToString();
    }

    private static string Icon(AlertSeverity s) => s switch
    {
        AlertSeverity.Critical => "🔴",
        AlertSeverity.Warning  => "🟠",
        _                      => "🔵",
    };
    private static string Op(AlertComparator c) => c switch
    {
        AlertComparator.GreaterThan => ">", AlertComparator.GreaterOrEqual => "≥",
        AlertComparator.LessThan => "<", AlertComparator.LessOrEqual => "≤", _ => "?",
    };
    private static string Fmt(double v) => v == Math.Floor(v) ? ((long)v).ToString() : v.ToString("0.##");

    // ── Telegram ────────────────────────────────────────────────────────────────

    private async Task SendTelegramAsync(TelegramChannel ch, AlertFiredEvent fired)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{ch.BotToken}/sendMessage";
            var payload = new { chat_id = ch.ChatId, text = RenderMessage(fired), disable_web_page_preview = true };
            var content = new StringContent(JsonSerializer.Serialize(payload, _json), Encoding.UTF8, "application/json");
            using var resp = await _http.PostAsync(url, content);
            if (!resp.IsSuccessStatusCode)
                _logger.LogWarning("Telegram sendMessage returned {Status} for chat {Chat}", resp.StatusCode, ch.ChatId);
        }
        catch (Exception ex) { _logger.LogError(ex, "Telegram dispatch to {Chat} failed", ch.ChatId); }
    }

    // ── Slack / Discord / Teams (incoming webhooks) ─────────────────────────────

    private async Task SendSlackAsync(SlackChannel ch, AlertFiredEvent fired)
    {
        try
        {
            var payload = new { attachments = new[] { new
            {
                color    = ColorHex(fired),
                fallback = fired.Rule.Name,
                text     = RenderMessage(fired),
            } } };
            await PostJsonAsync(ch.WebhookUrl, payload, "Slack");
        }
        catch (Exception ex) { _logger.LogError(ex, "Slack dispatch failed for rule {Rule}", fired.Rule.Name); }
    }

    private async Task SendDiscordAsync(DiscordChannel ch, AlertFiredEvent fired)
    {
        try
        {
            bool resolved = fired.State == AlertState.Ok;
            var payload = new { embeds = new[] { new
            {
                title       = (resolved ? "✅ RESOLVED — " : Icon(fired.Rule.Severity) + " FIRING — ") + fired.Rule.Name,
                description = RenderMessage(fired),
                color       = ColorInt(fired),
            } } };
            await PostJsonAsync(ch.WebhookUrl, payload, "Discord");
        }
        catch (Exception ex) { _logger.LogError(ex, "Discord dispatch failed for rule {Rule}", fired.Rule.Name); }
    }

    private async Task SendTeamsAsync(TeamsChannel ch, AlertFiredEvent fired)
    {
        try
        {
            bool resolved = fired.State == AlertState.Ok;
            // MessageCard uses '@'-prefixed keys → build via dictionary.
            var payload = new Dictionary<string, object>
            {
                ["@type"]      = "MessageCard",
                ["@context"]   = "http://schema.org/extensions",
                ["themeColor"] = ColorHex(fired).TrimStart('#'),
                ["summary"]    = fired.Rule.Name,
                ["title"]      = (resolved ? "RESOLVED — " : "FIRING — ") + fired.Rule.Name,
                ["text"]       = RenderMessage(fired).Replace("\n", "  \n"), // Teams needs 2 spaces for a line break
            };
            await PostJsonAsync(ch.WebhookUrl, payload, "Teams");
        }
        catch (Exception ex) { _logger.LogError(ex, "Teams dispatch failed for rule {Rule}", fired.Rule.Name); }
    }

    // ── PagerDuty (Events API v2) ───────────────────────────────────────────────

    private async Task SendPagerDutyAsync(PagerDutyChannel ch, AlertFiredEvent fired)
    {
        try
        {
            bool resolved = fired.State == AlertState.Ok;
            // dedup_key ties trigger↔resolve to the same incident.
            var body = new Dictionary<string, object>
            {
                ["routing_key"]  = ch.RoutingKey,
                ["event_action"] = resolved ? "resolve" : "trigger",
                ["dedup_key"]    = "ameto-" + fired.Rule.Id,
            };
            if (!resolved)
                body["payload"] = new
                {
                    summary  = $"{fired.Rule.Name}: {Fmt(fired.Value)} {Op(fired.Rule.Comparator)} {Fmt(fired.Rule.Threshold)}",
                    source   = "ameto",
                    severity = fired.Rule.Severity switch
                    {
                        AlertSeverity.Critical => "critical",
                        AlertSeverity.Warning  => "warning",
                        _                      => "info",
                    },
                };
            await PostJsonAsync("https://events.pagerduty.com/v2/enqueue", body, "PagerDuty");
        }
        catch (Exception ex) { _logger.LogError(ex, "PagerDuty dispatch failed for rule {Rule}", fired.Rule.Name); }
    }

    // ── Shared helpers ──────────────────────────────────────────────────────────

    /// <summary>POSTs a JSON payload; logs status without ever logging the (secret) URL.</summary>
    private async Task PostJsonAsync(string url, object payload, string name)
    {
        var content = new StringContent(JsonSerializer.Serialize(payload, _json), Encoding.UTF8, "application/json");
        using var resp = await _http.PostAsync(url, content);
        if (!resp.IsSuccessStatusCode)
            _logger.LogWarning("{Name} webhook returned {Status}", name, resp.StatusCode);
    }

    private static string ColorHex(AlertFiredEvent f) =>
        f.State == AlertState.Ok ? "#22c55e"
        : f.Rule.Severity switch { AlertSeverity.Critical => "#ef4444", AlertSeverity.Warning => "#f59e0b", _ => "#3b82f6" };

    private static int ColorInt(AlertFiredEvent f) => Convert.ToInt32(ColorHex(f).TrimStart('#'), 16);

    // ── Webhook ───────────────────────────────────────────────────────────────

    private async Task SendWebhookAsync(WebhookChannel ch, AlertFiredEvent fired)
    {
        try
        {
            var payload = new
            {
                alert     = fired.Rule.Name,
                state     = fired.IsTest ? "test" : fired.State == AlertState.Ok ? "resolved" : "firing",
                severity  = fired.Rule.Severity.ToString().ToLowerInvariant(),
                source    = fired.Rule.Source.ToString().ToLowerInvariant(),
                value     = fired.Value,
                threshold = fired.Rule.Threshold,
                window    = (int)fired.Rule.Window.TotalSeconds,
                at        = fired.At.ToString("O"),
                message   = RenderMessage(fired),
            };
            var content = new StringContent(JsonSerializer.Serialize(payload, _json), Encoding.UTF8, "application/json");

            using var req = new HttpRequestMessage(HttpMethod.Post, ch.Url) { Content = content };
            if (ch.Headers is not null)
                foreach (var (k, v) in ch.Headers) req.Headers.TryAddWithoutValidation(k, v);

            using var resp = await _http.SendAsync(req);
            if (!resp.IsSuccessStatusCode)
                _logger.LogWarning("Webhook {Url} returned {Status}", ch.Url, resp.StatusCode);
        }
        catch (Exception ex) { _logger.LogError(ex, "Webhook dispatch to {Url} failed", ch.Url); }
    }

    // ── SMTP ──────────────────────────────────────────────────────────────────

    private async Task SendSmtpAsync(SmtpChannel ch, AlertFiredEvent fired)
    {
        try
        {
            bool resolved = fired.State == AlertState.Ok;
            var subject = $"[Ameto {(fired.IsTest ? "TEST" : resolved ? "RESOLVED" : "ALERT")}] {fired.Rule.Name}";
            using var client = new SmtpClient(ch.Host, ch.Port)
            {
                EnableSsl = ch.UseSsl, DeliveryMethod = SmtpDeliveryMethod.Network, UseDefaultCredentials = false,
            };
            if (!string.IsNullOrEmpty(ch.Username))
                client.Credentials = new System.Net.NetworkCredential(ch.Username, ch.Password);

            using var msg = new MailMessage(ch.From, ch.To, subject, RenderMessage(fired)) { IsBodyHtml = false };
            await client.SendMailAsync(msg);
        }
        catch (Exception ex) { _logger.LogError(ex, "SMTP dispatch to {To} failed", ch.To); }
    }
}
