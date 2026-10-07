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
    /// since the last. Called AFTER the refusal is written, and never throws: a line lost to the
    /// same shortage must not cost the exporter the answer it retries on.
    /// </summary>
    public void Note(HttpContext ctx, OutOfMemoryException ex)
    {
        Interlocked.Increment(ref _pending);

        long now  = _time.GetTimestamp();
        long next = Volatile.Read(ref _nextAt);
        if (now < next) return;
        long step = (long)(Interval.TotalSeconds * _time.TimestampFrequency);
        if (Interlocked.CompareExchange(ref _nextAt, now + step, next) != next) return;

        long count = Interlocked.Exchange(ref _pending, 0);
        try
        {
            _write(_logger, count, ctx.Request.Path.Value ?? "", ex);
        }
        catch (OutOfMemoryException)
        {
            // Still short: the line is lost, the answer is not.
        }
    }
}
