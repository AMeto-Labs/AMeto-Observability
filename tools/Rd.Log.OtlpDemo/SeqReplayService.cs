using System.Text;
using System.Text.Json;

/// <summary>
/// Background worker that replays real Seq log events from <see cref="SeqEventStore"/>.
/// Every second it picks a random entry, renders the message from its template tokens,
/// and re-emits it through the <see cref="ILogger"/> pipeline → OTel SDK → OTLP.
/// </summary>
internal sealed class SeqReplayService(
    SeqEventStore  store,
    ILoggerFactory logFactory,
    ILogger<SeqReplayService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (store.Count == 0)
        {
            logger.LogWarning("SeqReplayService: no events loaded — check SeqReplay:DataPath in appsettings.json");
            return;
        }

        logger.LogInformation("SeqReplayService: replaying {Count} events, 1/sec", store.Count);

        // Let the host finish startup before the first emit
        await Task.Delay(500, stoppingToken).ConfigureAwait(false);

        //while (!stoppingToken.IsCancellationRequested)
        //{
        //    var ev = store.GetRandom();
        //    if (ev is not null)
        //        Emit(ev);

        //    await Task.Delay(1_000, stoppingToken).ConfigureAwait(false);
        //}
    }

    // ── emit ─────────────────────────────────────────────────────────────────

    private void Emit(SeqEvent ev)
    {
        var level = ev.Level switch
        {
            "Error"             => LogLevel.Error,
            "Warning" or "Warn" => LogLevel.Warning,
            "Debug"             => LogLevel.Debug,
            "Fatal"             => LogLevel.Critical,
            _                   => LogLevel.Information,
        };

        var props = BuildScope(ev.Properties);

        // Use SourceContext as the logger category name — shows as scope.name in OTLP
        var sourceContext = props.TryGetValue("SourceContext", out var sc)
            ? sc?.ToString() ?? "SeqReplay"
            : "SeqReplay";

        var message = RenderMessage(ev.MessageTemplateTokens, props);

        var cat = logFactory.CreateLogger(sourceContext);
        using (cat.BeginScope(props))
            cat.Log(level, "{EventType} {Message}", ev.EventType, message);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static Dictionary<string, object?> BuildScope(SeqProperty[] properties)
    {
        var dict = new Dictionary<string, object?>(properties.Length);
        foreach (var p in properties)
            dict[p.Name] = ToClr(p.Value);
        return dict;
    }

    private static object? ToClr(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.String => e.GetString(),
        JsonValueKind.Number => e.TryGetInt64(out var i64) ? i64 : (object?)e.GetDouble(),
        JsonValueKind.True   => true,
        JsonValueKind.False  => false,
        JsonValueKind.Null   => null,
        _                    => e.ToString(),   // Array / Object → JSON string
    };

    private static string RenderMessage(SeqToken[]? tokens, Dictionary<string, object?> props)
    {
        if (tokens is null or { Length: 0 }) return string.Empty;
        var sb = new StringBuilder();
        foreach (var t in tokens)
        {
            if (t.Text is not null)
                sb.Append(t.Text);
            else if (t.PropertyName is not null)
                sb.Append(props.TryGetValue(t.PropertyName, out var v) ? v?.ToString() : $"{{{t.PropertyName}}}");
        }
        return sb.ToString();
    }
}
