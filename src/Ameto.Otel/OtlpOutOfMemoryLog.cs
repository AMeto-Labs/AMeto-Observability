using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Ameto.Otel;

/// <summary>
/// The error for an OTLP batch the server ran out of memory taking in — answered 503 with
/// <c>Retry-After</c> over HTTP and <c>UNAVAILABLE</c> over gRPC, both of which exporters retry —
/// at most one line a second, with the count since the last line and the latest failure's
/// exception. Shared by the HTTP and gRPC receivers (#125).
///
/// <para><b>Why there is a line at all.</b> The failure used to leave the handler: hosting
/// answered it 500 — which OTLP exporters do not retry, so the batch was dropped — and logged it
/// at Error with its stack, which is how the 512 MB stand's metric WAL append was found running
/// out. The answer is the receivers' own now, and without this line the only sign of a server
/// short of memory would be its exporters' retries.</para>
///
/// <para><b>Why it is throttled.</b> It is written when memory is short and the next request is
/// likely to fail the same way: one line, with a stack, per request is an allocation the server
/// can least afford then, and a flood that buries the cause. The shape is
/// <see cref="OtlpGzipTooLargeLog"/>'s — every failure one interlocked add, and the one that finds
/// the second elapsed writes the count since the last line.</para>
/// </summary>
internal sealed class OtlpOutOfMemoryLog
{
    /// <summary>The shortest gap between two lines.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromSeconds(1);

    private static readonly Action<ILogger, long, string, Exception?> _write =
        LoggerMessage.Define<long, string>(
            LogLevel.Error,
            new EventId(4, "OtlpOutOfMemory"),
            "OTLP: the server ran out of memory taking in {Count} batch(es) since the last such error and asked "
          + "the exporter to retry each (HTTP 503 / gRPC UNAVAILABLE); the latest was on {Path}");

    private readonly ILogger      _logger;
    private readonly TimeProvider _time;

    /// <summary>Failures since the last line; swapped to zero by the thread that writes the next.</summary>
    private long _pending;

    /// <summary>The <see cref="TimeProvider.GetTimestamp"/> before which no line is written.</summary>
    private long _nextAt = long.MinValue;

    public OtlpOutOfMemoryLog(ILogger logger, TimeProvider time)
    {
        _logger = logger;
        _time   = time;
    }

    /// <summary>
    /// Counts one failure; writes the line, carrying <paramref name="ex"/>, if a second has passed
    /// since the last. Called after the refusal is decided, and NEVER throws, whatever the logger
    /// does (#126 review F3): on HTTP the 503 is only buffered at that point, so a throw here turned
    /// it into hosting's 500 — and the logger rethrows any provider's failure, a provider that runs
    /// out of memory formatting this line's stack included, as an <see cref="AggregateException"/>,
    /// which the narrower catch this had let through. The line is best-effort; the answer is not.
    /// </summary>
    public void Note(HttpContext ctx, Exception ex)
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
            _write(_logger, count, ctx.Request.Path.Value ?? "", ex);
        }
        catch
        {
            // Lost: the line, never the answer.
        }
    }

    /// <summary>
    /// Whether <paramref name="ex"/> is the server running out of memory — the exception itself, or
    /// an <see cref="AggregateException"/> holding one (#126 review F3): Microsoft.Extensions.Logging's
    /// logger rethrows a provider's failure that way, so an OutOfMemoryException inside a log call a
    /// sink makes — the log ring's oversized-record warning, the span ring's ring-full warning —
    /// reaches the receivers wrapped, and was taken for a malformed payload.
    ///
    /// <para><b>It allocates nothing</b> (#126 review NEW-5). It runs in <c>when</c> filters, while
    /// the batch's buffers are still held, and a throw inside a filter is swallowed and reads as
    /// false — so an allocation here that itself ran out of memory turned the 503 into a 500 (HTTP)
    /// or INVALID_ARGUMENT (gRPC). A <c>foreach</c> over the inner exceptions allocated an
    /// enumerator; the collection is indexed instead.</para>
    /// </summary>
    internal static bool IsOutOfMemory(Exception ex)
    {
        if (ex is OutOfMemoryException) return true;
        if (ex is AggregateException aggregate)
        {
            var inner = aggregate.InnerExceptions;
            for (int i = 0; i < inner.Count; i++)
                if (IsOutOfMemory(inner[i])) return true;
        }
        return false;
    }
}
