namespace Rd.Log.LoadTest;

/// <summary>
/// Live-updating metrics collected during a load-test run.
/// All counters are updated with <see cref="Interlocked"/> from multiple workers.
/// </summary>
public sealed class RunMetrics
{
    // ── Counters ──────────────────────────────────────────────────────────────

    private long _eventsSent;
    private long _batchesSent;
    private long _eventsDropped;
    private long _httpErrors;
    private long _totalLatencyMs;
    private long _minLatencyMs = long.MaxValue;
    private long _maxLatencyMs;

    public long EventsSent    => Interlocked.Read(ref _eventsSent);
    public long BatchesSent   => Interlocked.Read(ref _batchesSent);
    public long EventsDropped => Interlocked.Read(ref _eventsDropped);
    public long HttpErrors    => Interlocked.Read(ref _httpErrors);

    // ── Time window ───────────────────────────────────────────────────────────

    public DateTimeOffset StartedAt  { get; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? StoppedAt { get; private set; }
    public TimeSpan Elapsed => (StoppedAt ?? DateTimeOffset.UtcNow) - StartedAt;

    // ── Derived ───────────────────────────────────────────────────────────────

    /// <summary>Events per second over the whole run.</summary>
    public double EventsPerSecond =>
        Elapsed.TotalSeconds > 0 ? EventsSent / Elapsed.TotalSeconds : 0;

    public long MinLatencyMs => _minLatencyMs == long.MaxValue ? 0 : _minLatencyMs;
    public long MaxLatencyMs => Interlocked.Read(ref _maxLatencyMs);
    public long AvgLatencyMs => BatchesSent > 0
        ? Interlocked.Read(ref _totalLatencyMs) / BatchesSent : 0;

    // ── Mutation (called from workers) ────────────────────────────────────────

    public void RecordBatch(int ingested, int dropped, long latencyMs)
    {
        Interlocked.Add(ref _eventsSent,    ingested);
        Interlocked.Add(ref _eventsDropped, dropped);
        Interlocked.Increment(ref _batchesSent);
        Interlocked.Add(ref _totalLatencyMs, latencyMs);

        // min
        long prev = _minLatencyMs;
        while (latencyMs < prev)
            prev = Interlocked.CompareExchange(ref _minLatencyMs, latencyMs, prev);

        // max
        long prevMax = _maxLatencyMs;
        while (latencyMs > prevMax)
            prevMax = Interlocked.CompareExchange(ref _maxLatencyMs, latencyMs, prevMax);
    }

    public void RecordHttpError() => Interlocked.Increment(ref _httpErrors);

    public void MarkStopped() => StoppedAt = DateTimeOffset.UtcNow;

    // ── Snapshot ──────────────────────────────────────────────────────────────

    public MetricSnapshot Snapshot() => new()
    {
        StartedAt      = StartedAt,
        StoppedAt      = StoppedAt,
        ElapsedSeconds = Elapsed.TotalSeconds,
        EventsSent     = EventsSent,
        EventsDropped  = EventsDropped,
        BatchesSent    = BatchesSent,
        HttpErrors     = HttpErrors,
        EventsPerSecond = EventsPerSecond,
        MinLatencyMs   = MinLatencyMs,
        AvgLatencyMs   = AvgLatencyMs,
        MaxLatencyMs   = MaxLatencyMs,
    };
}

public sealed class MetricSnapshot
{
    public DateTimeOffset  StartedAt       { get; init; }
    public DateTimeOffset? StoppedAt       { get; init; }
    public double          ElapsedSeconds  { get; init; }
    public long            EventsSent      { get; init; }
    public long            EventsDropped   { get; init; }
    public long            BatchesSent     { get; init; }
    public long            HttpErrors      { get; init; }
    public double          EventsPerSecond { get; init; }
    public long            MinLatencyMs    { get; init; }
    public long            AvgLatencyMs    { get; init; }
    public long            MaxLatencyMs    { get; init; }
    public bool            IsRunning       => StoppedAt is null;
}
