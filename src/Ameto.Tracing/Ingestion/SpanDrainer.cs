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

    private readonly SpanRingBuffer       _ring;
    private readonly TraceStorageEngine   _storage;
    private readonly ILogger<SpanDrainer> _logger;
    private readonly Task                 _drainTask;
    private readonly CancellationTokenSource _cts = new();

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
                await Task.Delay(5, ct).ConfigureAwait(false);
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

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _drainTask.ConfigureAwait(false); }
        catch (OperationCanceledException) { }
        _cts.Dispose();
    }
}
