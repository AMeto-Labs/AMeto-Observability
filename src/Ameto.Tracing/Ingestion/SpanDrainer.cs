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
    /// How often the in-memory hot tier is flushed to disk regardless of fill level.
    /// Bounds data loss on restart to this interval under low-traffic loads that never
    /// reach the engine's 50k-span flush threshold.
    /// </summary>
    private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(30);

    private readonly SpanRingBuffer       _ring;
    private readonly TraceStorageEngine   _storage;
    private readonly ILogger<SpanDrainer> _logger;
    private readonly Task                 _drainTask;
    private readonly CancellationTokenSource _cts = new();

    // 0 = live, 1 = disposed. Guards against the multiple DisposeAsync calls at
    // host shutdown (see DisposeAsync).
    private int _disposed;

    private DateTime _lastFlush = DateTime.UtcNow;

    private readonly SpanIngestItem?[] _batch = new SpanIngestItem?[BatchSize];

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
                for (int i = 0; i < count; i++)
                {
                    var item = _batch[i]!;
                    _storage.WriteSpan(item);
                    _batch[i] = null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SpanDrainer: error writing batch of {Count} spans", count);
            }

            MaybeFlush();
        }

        // Drain remaining items before shutdown
        int remaining;
        do
        {
            remaining = _ring.TryDequeueMany(_batch, BatchSize);
            for (int i = 0; i < remaining; i++)
            {
                _storage.WriteSpan(_batch[i]!);
                _batch[i] = null;
            }
        } while (remaining > 0);
    }

    /// <summary>Flushes the hot tier when <see cref="FlushInterval"/> has elapsed. No-op when empty.</summary>
    private void MaybeFlush()
    {
        if (DateTime.UtcNow - _lastFlush < FlushInterval) return;
        _lastFlush = DateTime.UtcNow;
        try { _storage.FlushHotTier(); }
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
