using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// Maps a <see cref="LogEntry"/> to the Seq "create signal" REST payload
/// and POSTs it to <c>{SeqBaseUrl}/api/signals/</c>.
/// </summary>
internal sealed class SeqSignalSender(
    IHttpClientFactory       factory,
    IConfiguration           config,
    ILogger<SeqSignalSender> logger)
{
    private readonly string _base    = config["Seq:BaseUrl"]       ?? "http://sandbox-kz02:5341";
    private readonly string _session = config["Seq:SessionCookie"] ?? string.Empty;

    private static readonly JsonSerializerOptions s_json =
        new() { TypeInfoResolver = AppJsonCtx.Default };

    /// <summary>Maps <paramref name="entry"/> → <see cref="SeqSignalPayload"/> and POSTs it.</summary>
    public async ValueTask SendAsync(LogEntry entry)
    {
        SeqSignalPayload payload = Map(entry);

        // IHttpClientFactory creates a pooled client — avoids socket exhaustion
        using var http = factory.CreateClient();
        using var req  = new HttpRequestMessage(HttpMethod.Post, new Uri($"{_base}/api/signals/"));

        req.Headers.TryAddWithoutValidation("Cookie",       $"Seq-Session={_session}");
        req.Headers.TryAddWithoutValidation("Accept",       "application/json, text/plain, */*");
        req.Headers.TryAddWithoutValidation("Cache-Control","no-cache");
        req.Headers.TryAddWithoutValidation("Origin",       _base);
        req.Headers.TryAddWithoutValidation("Referer",      $"{_base}/");
        req.Headers.TryAddWithoutValidation("User-Agent",   "Mozilla/5.0 (compatible; Rd.Log.OtlpDemo/1.0)");
        req.Content = JsonContent.Create(payload, options: s_json);

        try
        {
            using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead)
                                       .ConfigureAwait(false);
            if (resp.IsSuccessStatusCode)
                logger.LogInformation(
                    "Seq signal created: [{Level}] {Operation} traceId={TraceId}",
                    entry.Level, entry.Operation, entry.TraceId);
            else
                logger.LogWarning(
                    "Seq signal POST returned {StatusCode} for {Operation}",
                    (int)resp.StatusCode, entry.Operation);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to send Seq signal for {Operation}", entry.Operation);
        }
    }

    // ── Mapper ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Converts a <see cref="LogEntry"/> into the Seq signal creation DTO.
    /// The signal title uses the operation name and level; the filter
    /// expression targets <c>@Properties['Operation']</c>.
    /// </summary>
    internal static SeqSignalPayload Map(LogEntry entry) =>
        new(
            Title            : $"[{entry.Level}] {entry.Operation}",
            Description      : entry.Message,
            Filters          : BuildFilters(entry),
            Columns          : [],
            IsWatched        : false,
            Id               : null,
            Grouping         : "Inferred",
            ExplicitGroupName: null,
            OwnerId          : "user-admin",
            Links            : new SeqSignalLinks("api/signals/"));

    private static SeqSignalFilter[] BuildFilters(LogEntry entry)
    {
        // Use ReadOnlySpan to avoid ToUpperInvariant / extra string allocations
        ReadOnlySpan<char> op = entry.Operation.AsSpan();
        return
        [
            new SeqSignalFilter(
                Filter     : $"@Properties['Operation'] = '{EscapeSeqLiteral(op)}'",
                Description: $"Events from operation '{entry.Operation}'"),
        ];
    }

    /// <summary>
    /// Escapes single-quotes in a Seq filter string literal.
    /// Most operation names are safe (alphanumeric + dot), so the
    /// fast path returns <c>input.ToString()</c> without allocation.
    /// </summary>
    private static string EscapeSeqLiteral(ReadOnlySpan<char> input) =>
        input.IndexOf('\'') < 0
            ? input.ToString()
            : input.ToString().Replace("'", "\\'", StringComparison.Ordinal);
}

// ── Seq REST API DTOs ─────────────────────────────────────────────────────────

internal sealed record SeqSignalPayload(
    string            Title,
    string?           Description,
    SeqSignalFilter[] Filters,
    string[]          Columns,
    bool              IsWatched,
    string?           Id,
    string            Grouping,
    string?           ExplicitGroupName,
    string            OwnerId,
    SeqSignalLinks    Links);

internal sealed record SeqSignalFilter(
    string  Filter,
    string? Description);

internal sealed record SeqSignalLinks(
    [property: JsonPropertyName("Create")] string Create);
