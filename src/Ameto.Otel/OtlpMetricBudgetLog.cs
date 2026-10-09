using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Ameto.Otel;

/// <summary>
/// The warning for a metrics batch refused for decoding past <c>Ingestion:MaxOtlpMetricPoints</c> —
/// 413 over HTTP, RESOURCE_EXHAUSTED over gRPC (<see cref="OtlpMetricPointBudget"/>) — at most one
/// a second, with the count since the last line, the latest batch's data points and decoded size
/// against the limit, and its sender. Shared by the HTTP and gRPC receivers (#126 review NEW-2).
///
/// <para><b>Why there is a line at all.</b> Exporters treat both answers as permanent: the
/// Collector's otlphttp exporter retries only 429, 502, 503 and 504, and RESOURCE_EXHAUSTED without
/// RetryInfo is not retried. The Collector does not split the batch either, so it is lost, and
/// without this line only the exporter's own log said so. The limit is a setting someone may have
/// to raise — by default it follows the heap, not a raised <c>MaxOtlpBatchBytes</c> — so the line
/// names it.</para>
///
/// <para><b>Throttled, and naming the sender,</b> for <see cref="OtlpGzipTooLargeLog"/>'s reasons: a
/// refusal costs its sender a few kilobytes, so one line per refusal would let any ingest key write
/// to the server's log at request rate; and the line exists to be acted on. Every refusal is one
/// interlocked add, and the one that finds the second elapsed writes the count since the last
/// line, with its own batch, API key preview and address.</para>
/// </summary>
internal sealed class OtlpMetricBudgetLog
{
    /// <summary>The shortest gap between two lines.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromSeconds(1);

    private static readonly Action<ILogger, long, int, long, long, string, string, Exception?> _write =
        LoggerMessage.Define<long, int, long, long, string, string>(
            LogLevel.Warning,
            new EventId(5, "OtlpMetricsOverBudget"),
            "OTLP: {Count} metrics batch(es) refused since the last such warning for decoding past "
          + "Ingestion:MaxOtlpMetricPoints = {Limit} points; the latest held {Points} data points, about {DecodedBytes} bytes "
          + "decoded, and came with API key {KeyPreview} from {RemoteAddress}. The exporter does not retry it: split its "
          + "batches, or raise the setting");

    private readonly ILogger      _logger;
    private readonly TimeProvider _time;

    /// <summary>Refusals since the last line; swapped to zero by the thread that writes the next.</summary>
    private long _pending;

    /// <summary>The <see cref="TimeProvider.GetTimestamp"/> before which no line is written.</summary>
    private long _nextAt = long.MinValue;

    public OtlpMetricBudgetLog(ILogger logger, TimeProvider time)
    {
        _logger = logger;
        _time   = time;
    }

    /// <summary>
    /// Counts one refusal; writes the line if a second has passed since the last. Never throws: it is
    /// called before the refusal is written, and a throw from a logger would turn the 413 into the
    /// 400 a malformed payload gets. The line is best-effort; the answer is not.
    /// </summary>
    public void Note(HttpContext ctx, in OtlpMetricPointBudget.Weight weight, int limit)
    {
        try
        {
            Interlocked.Increment(ref _pending);

            long now  = _time.GetTimestamp();
            long next = Volatile.Read(ref _nextAt);
            if (now < next) return;
            long step = (long)(Interval.TotalSeconds * _time.TimestampFrequency);
            if (Interlocked.CompareExchange(ref _nextAt, now + step, next) != next) return;

            long count = Interlocked.Exchange(ref _pending, 0);
            _write(_logger, count, limit, weight.Points, weight.DecodedBytes, OtlpGzipTooLargeLog.KeyPreview(ctx.Request),
                   ctx.Connection.RemoteIpAddress?.ToString() ?? "unknown", null);
        }
        catch
        {
            // Lost: the line, never the answer.
        }
    }
}
