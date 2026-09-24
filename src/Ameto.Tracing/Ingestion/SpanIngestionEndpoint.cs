using Microsoft.Extensions.Logging;

namespace Ameto.Tracing.Ingestion;

/// <summary>
/// Entry point for span ingestion: the raw sink the OTLP/HTTP parsers stream into
/// (<see cref="ISpanSink"/>, TI#3), and the item door the gRPC receiver and the tests use
/// (<see cref="ISpanIngester"/>) — both into the one raw ring.
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
///
/// <para><b>A BATCH LANDS AS A PREFIX, WHICHEVER DOOR IT CAME THROUGH.</b> The item door stops at
/// the first span the ring refuses; the raw sink does the same by refusing every span of a batch
/// after its first refusal (the batch is the calling thread's, from its first span to
/// <see cref="EndBatch"/>), and counts the batch as one refused request at its end.</para>
/// </summary>
internal sealed partial class SpanIngestionEndpoint : ISpanIngester, ISpanSink
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

    /// <summary>The calling thread's raw batch into this endpoint: how many of its spans were refused.</summary>
    private struct RawBatch
    {
        public SpanIngestionEndpoint? Owner;
        public int                    Refused;
    }

    [ThreadStatic] private static RawBatch t_batch;

    public SpanIngestionEndpoint(
        SpanRingBuffer ring,
        ILogger<SpanIngestionEndpoint> logger,
        TimeProvider? time = null)
    {
        _ring   = ring;
        _logger = logger;
        _time   = time ?? TimeProvider.System;
        _nextWarningAt     = long.MinValue;
        _nextPoolWarningAt = long.MinValue;
        _ring.Pools.Saturated += OnPoolSaturated;
    }

    /// <summary>Requests that could not be taken whole since the process started.</summary>
    public long RefusedRequests => Interlocked.Read(ref _refusedRequests);

    /// <summary>Spans refused since the process started.</summary>
    public long RefusedSpans => Interlocked.Read(ref _refusedSpans);

    /// <summary>Span-name strings built outside the (full) name pool since the process started.</summary>
    public long UnpooledSpanNames => _ring.Pools.UnpooledNames;

    /// <summary>Service strings built outside the (full) service pool — once per run of a block's spans.</summary>
    public long UnpooledServiceNames => _ring.Pools.UnpooledServices;

    /// <summary>How many times an intern pool has filled up: the service pool at most once, the name pool at most once per tier.</summary>
    public long InternPoolSaturations => _ring.Pools.Saturations;

    // ── A FULL INTERN POOL IS SAID OUT LOUD (review F4) ─────────────────────────
    //
    // A full pool drops nothing — every span keeps its own string — but past that point each span
    // (or each block, for services) costs a string the tier used to share, and the pool never says
    // so: a service.name per pod or a route with an id in it filled it silently. At most one
    // warning a minute, the 6c614c5 rate limit: the service pool fills once for the life of the
    // process, the name pool at most once per tier, which under a high-cardinality burst is every
    // flush.

    /// <summary>The shortest gap between two pool warnings.</summary>
    internal static readonly TimeSpan PoolWarningInterval = TimeSpan.FromMinutes(1);

    private long _nextPoolWarningAt;
    private long _poolSaturationsSinceWarning;

    private void OnPoolSaturated(SpanPoolKind kind, int cap)
    {
        Interlocked.Increment(ref _poolSaturationsSinceWarning);
        long now  = _time.GetTimestamp();
        long next = Volatile.Read(ref _nextPoolWarningAt);
        if (now < next) return;
        long step = (long)(PoolWarningInterval.TotalSeconds * _time.TimestampFrequency);
        if (Interlocked.CompareExchange(ref _nextPoolWarningAt, now + step, next) != next) return;

        long since = Interlocked.Exchange(ref _poolSaturationsSinceWarning, 0);
        LogPoolSaturated(_logger, kind == SpanPoolKind.Services ? "service-name" : "span-name", cap, since,
                         _ring.Pools.UnpooledNames, _ring.Pools.UnpooledServices);
    }

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "The trace {Pool} intern pool is full ({Cap} distinct values; {Saturations} pool(s) filled since the last warning): "
                + "past this, spans keep their own strings — nothing is dropped, but the hot tier holds more. "
                + "Unpooled so far: {UnpooledNames} span name(s), {UnpooledServices} service string(s). "
                + "A service.name per pod, or a span name carrying an id instead of a route template, does this")]
    private static partial void LogPoolSaturated(ILogger logger, string pool, int cap, long saturations,
                                                 long unpooledNames, long unpooledServices);

    /// <inheritdoc/>
    public bool TryIngest(ReadOnlySpan<SpanIngestItem> spans, out int accepted)
    {
        accepted = 0;
        try
        {
            foreach (var span in spans)
            {
                if (!_ring.TryEnqueue(span)) break;
                accepted++;
            }
        }
        finally { _ring.EndBatch(); }

        if (accepted == spans.Length) return true;
        NoteRefusal(spans.Length - accepted);
        return false;
    }

    /// <inheritdoc/>
    public int InternService(ReadOnlySpan<byte> serviceUtf8) =>
        _ring.Pools.ServiceIndex(serviceUtf8);

    /// <inheritdoc/>
    public bool TryIngestRaw(
        TraceId traceId, SpanId spanId, SpanId parentSpanId,
        long startTimeUnixNano, long durationNanos,
        ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8,
        SpanKind kind, SpanStatusCode status, short httpStatusCode,
        ReadOnlySpan<byte> msgpackAttributes)
    {
        ref var batch = ref t_batch;
        if (batch.Owner != this) batch = new RawBatch { Owner = this };

        // A PREFIX: once one span of the batch was refused, the rest are too.
        if (batch.Refused > 0) { batch.Refused++; return false; }

        var fields = new SpanHeader
        {
            TraceId           = traceId,
            SpanId            = spanId,
            ParentSpanId      = parentSpanId,
            StartTimeUnixNano = startTimeUnixNano,
            DurationNanos     = durationNanos,
            Kind              = kind,
            Status            = status,
            HttpStatusCode    = httpStatusCode,
        };
        if (_ring.TryEnqueueRaw(in fields, nameUtf8, serviceIdx, serviceUtf8, msgpackAttributes)) return true;

        batch.Refused = 1;
        return false;
    }

    /// <inheritdoc/>
    public void EndBatch()
    {
        _ring.EndBatch();

        ref var batch = ref t_batch;
        if (batch.Owner != this) return;
        int refused = batch.Refused;
        batch = default;
        if (refused > 0) NoteRefusal(refused);
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
