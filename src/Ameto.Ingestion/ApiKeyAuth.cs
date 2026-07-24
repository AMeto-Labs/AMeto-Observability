using System;
using Microsoft.AspNetCore.Http;

namespace Ameto.Ingestion;

/// <summary>
/// Capabilities an API key grants. Bit flags, so one key can allow any
/// combination of ingest (write) and query (read) per signal. The low three
/// bits are the original ingest scopes; keys created before permissions existed
/// migrate to <see cref="Ingest"/> so they keep working. Read scopes are
/// separate bits so an ingest-only shipping key can never read stored data.
/// </summary>
[Flags]
public enum ApiKeyPermissions
{
    None        = 0,

    // ── Ingest (write) ──
    Logs        = 1,
    Traces      = 2,
    Metrics     = 4,

    // ── Query (read) ──
    ReadLogs    = 8,
    ReadTraces  = 16,
    ReadMetrics = 32,

    Ingest      = Logs | Traces | Metrics,             // 7 (legacy "All")
    Read        = ReadLogs | ReadTraces | ReadMetrics, // 56
    All         = Ingest | Read,                       // 63
}

/// <summary>
/// Validates a raw ingest API key against a required permission. Implemented by
/// the server's in-memory cache so the hot ingest path never touches the database.
/// </summary>
public interface IApiKeyValidator
{
    /// <summary>True when the key exists AND grants every bit in <paramref name="required"/>.</summary>
    bool Validate(ReadOnlySpan<char> rawKey, ApiKeyPermissions required);
}

/// <summary>
/// Extracts the ingest API key from a request. Seq-compatible: the
/// <c>X-Seq-ApiKey</c> header, an <c>Authorization: apikey &lt;key&gt;</c> header,
/// or an <c>?apiKey=</c> query-string parameter.
/// </summary>
public static class ApiKeyHeader
{
    public static string? Extract(HttpRequest req)
    {
        if (req.Headers.TryGetValue("X-Seq-ApiKey", out var v) && v.Count > 0)
            return v[0];

        var header = req.Headers["Authorization"].ToString();
        if (header.StartsWith("apikey ", StringComparison.OrdinalIgnoreCase))
            return header["apikey ".Length..].Trim();

        if (req.Query.TryGetValue("apiKey", out var qs) && qs.Count > 0)
            return qs[0];

        return null;
    }
}
