using System;
using Microsoft.AspNetCore.Http;

namespace Ameto.Ingestion;

/// <summary>
/// Capabilities an API key grants on the ingest endpoints. Bit flags, so one key
/// can allow any combination. Keys created before permissions existed migrate to
/// <see cref="All"/> so they keep working.
/// </summary>
[Flags]
public enum ApiKeyPermissions
{
    None    = 0,
    Logs    = 1,
    Traces  = 2,
    Metrics = 4,
    All     = Logs | Traces | Metrics,
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
