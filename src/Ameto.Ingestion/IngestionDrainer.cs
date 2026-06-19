using System.Buffers;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Ingestion;

/// <summary>
/// Drains the <see cref="IngestionRingBuffer"/> and writes events into <see cref="StorageEngine"/>.
///
/// Runs as a tight async loop on a dedicated background task.
/// Batches up to <see cref="DrainBatchSize"/> events per iteration to amortise the
/// <see cref="StorageEngine.TryWrite"/> call overhead.
///
/// Back-pressure: if <see cref="StorageEngine.TryWrite"/> returns false (hot tier full),
/// the drainer parks for one tick so the flush loop can catch up.
/// </summary>
public sealed class IngestionDrainer : IAsyncDisposable
{
    private const int DrainBatchSize = 512;

    private readonly IngestionRingBuffer     _ring;
    private readonly StorageEngine           _storage;
    private readonly ILogger<IngestionDrainer> _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task                    _loop;
    private int _disposed;

    // Reusable payload buffer per drain call (max 64 KB per event)
    private readonly byte[] _payloadBuf = new byte[64 * 1024];

    // Signal: producers release once when enqueueing into an empty buffer.
    // Drainer blocks here instead of polling, eliminating idle CPU usage.
    private readonly SemaphoreSlim _signal = new(0, 1);

    public IngestionDrainer(
        IngestionRingBuffer ring,
        StorageEngine storage,
        ILogger<IngestionDrainer> logger)
    {
        _ring    = ring;
        _storage = storage;
        _logger  = logger;
        _loop    = Task.Run(() => DrainLoopAsync(_cts.Token));
    }

    /// <summary>
    /// Called by producers after successfully enqueuing into the ring buffer.
    /// Wakes the drain loop if it is currently idle.
    /// </summary>
    internal void NotifyEnqueued()
    {
        if (_signal.CurrentCount == 0)
        {
            try { _signal.Release(); }
            catch (SemaphoreFullException) { } // already signaled — fine
        }
    }

    private async Task DrainLoopAsync(CancellationToken ct)
    {
        int storageBackoffMs = 0;

        while (!ct.IsCancellationRequested)
        {
            int drained = 0;

            try
            {
            for (int i = 0; i < DrainBatchSize; i++)
            {
                // Guard: stop before touching native storage if shutdown was requested.
                // The outer while-check runs only after a full batch; this closes the window
                // where a signal-continuation could resume after disposal.
                if (ct.IsCancellationRequested) return;

                if (!_ring.TryDequeue(
                        out long tsTicks, out byte level, out int tmplIdx,
                        out string? tmpl, out ExceptionInfo? exception,
                        _payloadBuf, out int payloadLen,
                        out ulong traceIdHi, out ulong traceIdLo,
                        out ulong spanId, out int serviceNameIdx))
                    break;

                var payload = new ReadOnlySpan<byte>(_payloadBuf, 0, payloadLen);

                var header = new LogEventHeader
                {
                    Id                       = 0,  // assigned by StorageEngine.TryWrite
                    TimestampUtcTicks        = tsTicks,
                    Level                    = (Ameto.Core.LogLevel)level,
                    MessageTemplatePoolIndex = tmplIdx,
                    PropertiesArenaOffset    = 0,  // filled by TryWrite
                    PropertiesByteLength     = payloadLen,
                    ServiceNamePoolIndex     = serviceNameIdx,
                    TraceIdHi                = traceIdHi,
                    TraceIdLo                = traceIdLo,
                    SpanId                   = spanId,
                    Flags                    = (byte)(exception is not null ? LogEventFlags.HasException : LogEventFlags.None),
                };

                bool written = _storage.TryWrite(header, payload, tmpl, exception);
                if (!written)
                {
                    // Hot tier full — re-enqueue is not possible without risking ordering issues,
                    // so we park briefly and retry from ring on next iteration.
                    storageBackoffMs = Math.Min(storageBackoffMs + 1, 5);
                    break;
                }

                drained++;
                storageBackoffMs = 0;
            }

            if (storageBackoffMs > 0 && drained == 0)
            {
                await Task.Delay(storageBackoffMs, ct).ConfigureAwait(false);
            }
            else if (drained == 0)
            {
                // Nothing in the ring — block until a producer signals or 50 ms elapse.
                // This replaces the old 1–3 ms busy-poll that kept CPU at 3-5% when idle.
                await _signal.WaitAsync(50, ct).ConfigureAwait(false);
            }
            }
            catch (ObjectDisposedException) { return; } // ring buffer disposed during shutdown
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _cts.CancelAsync();
        try { await _loop.ConfigureAwait(false); }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        _cts.Dispose();
    }
}
