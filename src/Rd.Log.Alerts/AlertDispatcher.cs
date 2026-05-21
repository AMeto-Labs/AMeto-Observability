using System.Net.Mail;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Rd.Log.Core;

namespace Rd.Log.Alerts;

/// <summary>
/// Dispatches a fired alert to all channels defined in the rule.
/// Channels execute concurrently; failures are logged but do not affect other channels.
/// </summary>
public sealed class AlertDispatcher
{
    private readonly ILogger<AlertDispatcher> _logger;
    // A single long-lived HttpClient is correct for a singleton dispatcher.
    private static readonly HttpClient _http = new();

    private static readonly JsonSerializerOptions _json = new()
    {
        PropertyNamingPolicy   = JsonNamingPolicy.CamelCase,
        WriteIndented          = false,
    };

    public AlertDispatcher(ILogger<AlertDispatcher> logger)
    {
        _logger = logger;
    }

    public async Task DispatchAsync(AlertFiredEvent fired)
    {
        _logger.LogWarning(
            "Alert fired: [{Rule}] — {Count} events in window at {FiredAt:O}",
            fired.Rule.Name, fired.Count, fired.FiredAt);

        var tasks = fired.Rule.Channels.Select(ch => DispatchOneAsync(ch, fired));
        await Task.WhenAll(tasks);
    }

    private Task DispatchOneAsync(AlertChannel channel, AlertFiredEvent fired)
    {
        return channel switch
        {
            WebhookChannel wh => SendWebhookAsync(wh, fired),
            SmtpChannel    sm => SendSmtpAsync(sm, fired),
            _                 => Task.CompletedTask,
        };
    }

    // ── Webhook ───────────────────────────────────────────────────────────────

    private async Task SendWebhookAsync(WebhookChannel ch, AlertFiredEvent fired)
    {
        try
        {
            var payload = new
            {
                alert      = fired.Rule.Name,
                filter     = fired.Rule.Filter,
                count      = fired.Count,
                threshold  = fired.Rule.Threshold,
                window     = (int)fired.Rule.Window.TotalSeconds,
                firedAt    = fired.FiredAt.ToString("O"),
                sampleEvents = fired.SampleEvents.Select(e => new
                {
                    timestamp = e.Timestamp.ToString("O"),
                    level     = LogLevelExtensions.ToSeqString(e.Level),
                    message   = e.MessageTemplate,
                    exception = e.Exception is null ? null : new
                    {
                        type    = e.Exception.Type,
                        message = e.Exception.Message,
                    },
                }).ToList(),
            };

            var json     = JsonSerializer.Serialize(payload, _json);
            var content  = new StringContent(json, Encoding.UTF8, "application/json");

            if (ch.Headers is not null)
                foreach (var (k, v) in ch.Headers)
                    _http.DefaultRequestHeaders.TryAddWithoutValidation(k, v);

            using var resp = await _http.PostAsync(ch.Url, content);
            if (!resp.IsSuccessStatusCode)
                _logger.LogWarning("Webhook {Url} returned {Status}", ch.Url, resp.StatusCode);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Webhook dispatch to {Url} failed", ch.Url);
        }
    }

    // ── SMTP ──────────────────────────────────────────────────────────────────

    private async Task SendSmtpAsync(SmtpChannel ch, AlertFiredEvent fired)
    {
        try
        {
            var subject = $"[Rd.Log Alert] {fired.Rule.Name} — {fired.Count} events";
            var body    = BuildEmailBody(fired);

            using var client = new SmtpClient(ch.Host, ch.Port)
            {
                EnableSsl             = ch.UseSsl,
                DeliveryMethod        = SmtpDeliveryMethod.Network,
                UseDefaultCredentials = false,
            };

            if (!string.IsNullOrEmpty(ch.Username))
                client.Credentials = new System.Net.NetworkCredential(ch.Username, ch.Password);

            var msg = new MailMessage(ch.From, ch.To, subject, body)
            {
                IsBodyHtml = false,
            };

            await Task.Run(() => client.Send(msg));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SMTP dispatch to {To} failed", ch.To);
        }
    }

    private static string BuildEmailBody(AlertFiredEvent fired)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Alert: {fired.Rule.Name}");
        sb.AppendLine($"Fired at: {fired.FiredAt:O}");
        sb.AppendLine($"Matching events in window: {fired.Count} (threshold: {fired.Rule.Threshold})");
        sb.AppendLine($"Window: {fired.Rule.Window.TotalSeconds}s");
        if (!string.IsNullOrEmpty(fired.Rule.Filter))
            sb.AppendLine($"Filter: {fired.Rule.Filter}");
        sb.AppendLine();
        sb.AppendLine("Sample events:");
        foreach (var ev in fired.SampleEvents)
        {
            sb.AppendLine($"  [{ev.Timestamp:O}] [{LogLevelExtensions.ToSeqString(ev.Level)}] {ev.MessageTemplate}");
            if (ev.Exception is not null)
                sb.AppendLine($"    {ev.Exception.Type}: {ev.Exception.Message}");
        }
        return sb.ToString();
    }
}
