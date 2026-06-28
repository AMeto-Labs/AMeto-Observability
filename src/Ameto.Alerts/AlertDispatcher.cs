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
        _logger.Log(resolved ? LogLevel.Information : LogLevel.Warning,
            "Alert {State}: [{Rule}] value={Value} threshold={Threshold} at {At:O}",
            resolved ? "RESOLVED" : "FIRING", fired.Rule.Name, fired.Value, fired.Rule.Threshold, fired.At);

        await Task.WhenAll(fired.Rule.Channels.Select(ch => DispatchOneAsync(ch, fired)));
    }

    private Task DispatchOneAsync(AlertChannel channel, AlertFiredEvent fired) => channel switch
    {
        WebhookChannel  wh => SendWebhookAsync(wh, fired),
        SmtpChannel     sm => SendSmtpAsync(sm, fired),
        TelegramChannel tg => SendTelegramAsync(tg, fired),
        _                  => Task.CompletedTask,
    };

    // ── Message rendering ───────────────────────────────────────────────────────

    /// <summary>Renders the notification text — custom template or a sensible default.</summary>
    private static string RenderMessage(AlertFiredEvent fired)
    {
        var r = fired.Rule;
        bool resolved = fired.State == AlertState.Ok;
        string status = resolved ? "✅ RESOLVED" : Icon(r.Severity) + " FIRING";

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

    // ── Webhook ───────────────────────────────────────────────────────────────

    private async Task SendWebhookAsync(WebhookChannel ch, AlertFiredEvent fired)
    {
        try
        {
            var payload = new
            {
                alert     = fired.Rule.Name,
                state     = fired.State == AlertState.Ok ? "resolved" : "firing",
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
            var subject = $"[Ameto {(resolved ? "RESOLVED" : "ALERT")}] {fired.Rule.Name}";
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
