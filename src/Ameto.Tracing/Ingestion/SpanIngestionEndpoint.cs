using Microsoft.Extensions.Logging;

namespace Ameto.Tracing.Ingestion;

/// <summary>
/// Entry point for span ingestion. Enqueues items into the ring buffer.
/// </summary>
internal sealed class SpanIngestionEndpoint : ISpanIngester
{
    private readonly SpanRingBuffer              _ring;
    private readonly ILogger<SpanIngestionEndpoint> _logger;

    private const double BackPressureThreshold = 0.9;

    public SpanIngestionEndpoint(
        SpanRingBuffer ring,
        ILogger<SpanIngestionEndpoint> logger)
    {
        _ring   = ring;
        _logger = logger;
    }

    /// <inheritdoc/>
    public bool TryIngest(ReadOnlySpan<SpanIngestItem> spans, out int accepted)
    {
        accepted = 0;
        if (_ring.FillFraction >= BackPressureThreshold)
        {
            _logger.LogWarning("Span ring buffer is over {Threshold:P0} full — applying back-pressure", BackPressureThreshold);
            return false;
        }

        foreach (var span in spans)
        {
            if (_ring.TryEnqueue(span))
                accepted++;
            else
                break;
        }

        return accepted == spans.Length;
    }
}
