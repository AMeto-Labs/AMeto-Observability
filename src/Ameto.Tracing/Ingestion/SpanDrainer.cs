using MessagePack;
using Microsoft.Extensions.Logging;
using Ameto.Tracing.Storage;

namespace Ameto.Tracing.Ingestion;

/// <summary>
/// Drains the <see cref="SpanRingBuffer"/> and writes spans to
/// <see cref="TraceStorageEngine"/> in batches.
/// </summary>
internal sealed class SpanDrainer : IAsyncDisposable
{
    private const int BatchSize = 512;

    /// <summary>
    /// How often the engine is asked whether the hot tier has earned a cold segment.
    /// This is a CHECK interval, not a flush interval: durability is the write-ahead log's
    /// job now, so a tick with only a handful of spans buffered correctly does nothing
    /// rather than writing a segment (plus its .stats sidecar) for them.
    /// </summary>
    private static readonly TimeSpan FlushCheckInterval = TimeSpan.FromSeconds(30);

    private readonly SpanRingBuffer       _ring;
    private readonly TraceStorageEngine   _storage;
    private readonly ILogger<SpanDrainer> _logger;
    private readonly Task                 _drainTask;
    private readonly CancellationTokenSource _cts = new();

    // 0 = live, 1 = disposed. Guards against the multiple DisposeAsync calls at
    // host shutdown (see DisposeAsync).
    private int _disposed;

    private DateTime _lastFlush = DateTime.UtcNow;

    // Non-nullable so a slice of it is the ReadOnlySpan<SpanIngestItem> WriteSpans takes; the
    // drain fills [0, count) and clears it again after every hand-over.
    private readonly SpanIngestItem[] _batch = new SpanIngestItem[BatchSize];

    public SpanDrainer(
        SpanRingBuffer ring,
        TraceStorageEngine storage,
        ILogger<SpanDrainer> logger)
    {
        _ring    = ring;
        _storage = storage;
        _logger  = logger;
        _drainTask = Task.Run(DrainLoopAsync);
    }

    private async Task DrainLoopAsync()
    {
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            int count = _ring.TryDequeueMany(_batch, BatchSize);
            if (count == 0)
            {
                MaybeFlush();
                // Park until a producer signals new spans. The 1 s timeout is only a
                // missed-signal safety net (was 50 ms, which burned ~20 idle wake-ups/sec).
                await _ring.WaitForItemsAsync(1000, ct).ConfigureAwait(false);
                continue;
            }

            try
            {
                // ONE call per drained batch: the engine takes its write lock and the log's
                // append lock once per hold (see TraceStorageEngine.WriteSpans), not per span.
                int taken = _storage.WriteSpans(new ReadOnlySpan<SpanIngestItem>(_batch, 0, count));
                // The engine has closed its write path. Every further span would be refused
                // too, so stop draining rather than spinning the ring empty into a closed
                // engine — and say so once, with the count, instead of once per span.
                if (taken < count) { ReportRefused(count - taken); return; }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SpanDrainer: error writing batch of {Count} spans", count);
            }
            finally
            {
                // The ring's references go either way: a span the engine took is in the tier, and
                // one it failed on is reported above — neither may be kept alive by this array.
                Array.Clear(_batch, 0, count);
            }

            MaybeFlush();
        }

        // Drain remaining items before shutdown
        int remaining;
        do
        {
            remaining = _ring.TryDequeueMany(_batch, BatchSize);
            if (remaining == 0) break;
            int taken;
            try { taken = _storage.WriteSpans(new ReadOnlySpan<SpanIngestItem>(_batch, 0, remaining)); }
            finally { Array.Clear(_batch, 0, remaining); }
            if (taken < remaining) { ReportRefused(remaining - taken); return; }
        } while (remaining > 0);
    }

    /// <summary>
    /// Says, once, that the engine stopped taking spans. It is not an error: the spans were
    /// never durable, the refusal is what keeps them from being queryable-but-unrecoverable,
    /// and by this point the process is going down anyway.
    /// </summary>
    private void ReportRefused(int dropped) =>
        _logger.LogWarning(
            "SpanDrainer: the trace engine has closed its write path — {Dropped} span(s) from "
          + "this batch, and whatever is still in the ring, were not stored", dropped);

    /// <summary>Asks the engine to flush if the hot tier is due, every <see cref="FlushCheckInterval"/>.</summary>
    private void MaybeFlush()
    {
        if (DateTime.UtcNow - _lastFlush < FlushCheckInterval) return;
        _lastFlush = DateTime.UtcNow;
        try { _storage.FlushIfDue(); }
        catch (Exception ex) { _logger.LogWarning(ex, "SpanDrainer: periodic hot-tier flush failed"); }
    }

    public async ValueTask DisposeAsync()
    {
        // Idempotent: SpanDrainerService disposes this from both StopAsync and its
        // own DisposeAsync, and the DI container disposes the singleton as well.
        // Cancelling/disposing the CTS twice throws ObjectDisposedException.
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0) return;

        _cts.Cancel();
        try { await _drainTask.ConfigureAwait(false); }
        catch (OperationCanceledException) { }

        // Final flush so spans drained from the ring buffer at shutdown reach disk
        // even if the engine's own Dispose flush is cut short by the host timeout.
        try { _storage.FlushHotTier(); }
        catch (Exception ex) { _logger.LogWarning(ex, "SpanDrainer: shutdown hot-tier flush failed"); }

        _cts.Dispose();
    }
}
