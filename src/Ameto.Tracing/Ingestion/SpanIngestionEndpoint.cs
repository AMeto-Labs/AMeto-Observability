using Microsoft.Extensions.Logging;

namespace Ameto.Tracing.Ingestion;

/// <summary>
/// Entry point for span ingestion. Enqueues items into the ring buffer.
///
/// <para><b>A FULL RING IS REPORTED ONCE A SECOND, NOT ONCE A REQUEST.</b> This used to write one
/// formatted warning per refused request into the server's own log storage — so under exactly the
/// overload that fills the ring, the server added a log event (and its formatting, and its ingest)
/// for every export it turned away: thousands a second, competing with the drainer for the same CPU.
/// Now a refusal is counted, and at most one warning per second carries the counts since the last
/// one. <see cref="RefusedSpans"/> and <see cref="RefusedRequests"/> keep the lifetime totals.</para>
///
/// <para><b>NO BATCH-LEVEL PRE-CHECK.</b> A batch used to be refused WHOLE when the ring was 90 %
/// full. That bought nothing: a batch admitted at 89 % enqueued until <c>TryEnqueue</c> failed anyway,
/// and a batch refused at 90 % dropped spans the ring had room for. The per-item result is the
/// decision now — the caller already reports <c>accepted</c> against the batch size.</para>
/// </summary>
internal sealed partial class SpanIngestionEndpoint : ISpanIngester
{
    private readonly SpanRingBuffer                 _ring;
    private readonly ILogger<SpanIngestionEndpoint> _logger;
    private readonly TimeProvider                   _time;

    // Since the last warning; swapped to zero by the thread that writes it.
    private long _pendingRequests;
    private long _pendingSpans;

    // Lifetime totals, for diagnostics.
    private long _refusedRequests;
    private long _refusedSpans;

    /// <summary>The timestamp (<see cref="TimeProvider.GetTimestamp"/>) before which no further warning is written.</summary>
    private long _nextWarningAt;

    public SpanIngestionEndpoint(
        SpanRingBuffer ring,
        ILogger<SpanIngestionEndpoint> logger,
        TimeProvider? time = null)
    {
        _ring   = ring;
        _logger = logger;
        _time   = time ?? TimeProvider.System;
        _nextWarningAt = long.MinValue;
    }

    /// <summary>Requests that could not be taken whole since the process started.</summary>
    public long RefusedRequests => Interlocked.Read(ref _refusedRequests);

    /// <summary>Spans refused since the process started.</summary>
    public long RefusedSpans => Interlocked.Read(ref _refusedSpans);

    /// <inheritdoc/>
    public bool TryIngest(ReadOnlySpan<SpanIngestItem> spans, out int accepted)
    {
        accepted = 0;
        foreach (var span in spans)
        {
            if (!_ring.TryEnqueue(span)) break;
            accepted++;
        }

        if (accepted == spans.Length) return true;
        NoteRefusal(spans.Length - accepted);
        return false;
    }

    private void NoteRefusal(int refused)
    {
        Interlocked.Increment(ref _refusedRequests);
        Interlocked.Add(ref _refusedSpans, refused);
        Interlocked.Increment(ref _pendingRequests);
        Interlocked.Add(ref _pendingSpans, refused);

        // One thread per second wins the exchange and writes the warning; every other refusal in
        // that second is two interlocked adds and a compare.
        long now  = _time.GetTimestamp();
        long next = Volatile.Read(ref _nextWarningAt);
        if (now < next) return;
        if (Interlocked.CompareExchange(ref _nextWarningAt, now + _time.TimestampFrequency, next) != next) return;

        long requests = Interlocked.Exchange(ref _pendingRequests, 0);
        long spans    = Interlocked.Exchange(ref _pendingSpans, 0);
        LogRingFull(_logger, spans, requests);
    }

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Span ring buffer full: refused {RefusedSpans} span(s) from {RefusedRequests} request(s) since the last warning — the drainer is behind")]
    private static partial void LogRingFull(ILogger logger, long refusedSpans, long refusedRequests);
}
