using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Ameto.Ingestion;

namespace Ameto.Otel;

/// <summary>
/// The warning for a gzip batch that inflated past <c>Ingestion.MaxOtlpBatchBytes</c> — at most
/// one a second, with a count, and saying who sent it. Shared by the HTTP and gRPC receivers.
///
/// <para><b>Why it is throttled.</b> It was one line per refused request, and a refusal costs
/// its sender ~8 KB: anyone with an ingest key could write to the server's log at request rate.
/// #84 held its refused-ingest warnings to one per second for the same reason, and this is that
/// shape (<c>SpanIngestionEndpoint.NoteRefusal</c>): every refusal is one interlocked add, and
/// the one that finds the second elapsed writes the count since the last line.</para>
///
/// <para><b>Why it names the sender.</b> The line exists to be read afterwards — a misconfigured
/// exporter or a probe — and "something happened" is not enough to act on. It carries the
/// latest sender's API key as <c>KeyPreview</c>: the first eight hex digits of the key's
/// SHA-256, exactly what <c>GET /api/auth/keys</c> lists for each key, so the line identifies the
/// key without putting any of the secret in a log. And the remote address. Both are worked out
/// only when the line is written — at most once a second, never per refusal.</para>
///
/// <para>Created once, with its logger, when the services are registered: the per-request
/// <c>CreateLogger</c> it replaces is the cost <c>OtlpEndpointMapper</c>'s own note on its traces
/// logger warns against.</para>
/// </summary>
internal sealed class OtlpGzipTooLargeLog
{
    /// <summary>The shortest gap between two lines.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromSeconds(1);

    private static readonly Action<ILogger, long, int, string, string, Exception?> _write =
        LoggerMessage.Define<long, int, string, string>(
            LogLevel.Warning,
            new EventId(3, "OtlpGzipTooLarge"),
            "OTLP: {Count} gzip batch(es) inflated past {Limit} bytes and were refused since the last such warning; "
          + "the latest came with API key {KeyPreview} from {RemoteAddress}");

    private readonly ILogger      _logger;
    private readonly TimeProvider _time;

    /// <summary>Refusals since the last line; swapped to zero by the thread that writes the next.</summary>
    private long _pending;

    /// <summary>The <see cref="TimeProvider.GetTimestamp"/> before which no line is written.</summary>
    private long _nextAt = long.MinValue;

    public OtlpGzipTooLargeLog(ILogger logger, TimeProvider time)
    {
        _logger = logger;
        _time   = time;
    }

    /// <summary>Counts one refusal; writes the line if a second has passed since the last.</summary>
    public void Note(HttpContext ctx, int limit)
    {
        Interlocked.Increment(ref _pending);

        long now  = _time.GetTimestamp();
        long next = Volatile.Read(ref _nextAt);
        if (now < next) return;
        long step = (long)(Interval.TotalSeconds * _time.TimestampFrequency);
        if (Interlocked.CompareExchange(ref _nextAt, now + step, next) != next) return;

        long count = Interlocked.Exchange(ref _pending, 0);
        _write(_logger, count, limit, KeyPreview(ctx.Request),
               ctx.Connection.RemoteIpAddress?.ToString() ?? "unknown", null);
    }

    /// <summary>
    /// The key as the key list shows it: <c>KeyHash[..8]</c>, where <c>KeyHash</c> is the
    /// lowercase hex SHA-256 of the key's UTF-8 bytes (<c>AuthStore</c>).
    /// </summary>
    internal static string KeyPreview(HttpRequest request)
    {
        string? key = ApiKeyHeader.Extract(request);
        if (string.IsNullOrEmpty(key)) return "none";

        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(key), digest);
        return Convert.ToHexStringLower(digest[..4]);
    }
}
