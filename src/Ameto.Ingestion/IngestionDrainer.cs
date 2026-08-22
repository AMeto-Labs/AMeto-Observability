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

    // Reusable payload buffer per drain call. MUST be at least the ring's slab size
    // (ServerOptions.Ingestion.MaxEventPayloadBytes) — dequeued payloads are copied into
    // it, and the ring drops anything larger, so sizing them from the same config value
    // keeps them in lock-step. Sized in the constructor.
    private readonly byte[] _payloadBuf;

    // Signal: producers release once when enqueueing into an empty buffer.
    // Drainer blocks here instead of polling, eliminating idle CPU usage.
    private readonly SemaphoreSlim _signal = new(0, 1);

    public IngestionDrainer(
        IngestionRingBuffer ring,
        StorageEngine storage,
        ServerOptions options,
        ILogger<IngestionDrainer> logger)
    {
        _ring       = ring;
        _storage    = storage;
        _logger     = logger;
        _payloadBuf = new byte[options.Ingestion.MaxEventPayloadBytes];
        _loop       = Task.Run(() => DrainLoopAsync(_cts.Token));
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

    /// <summary>Events abandoned after exhausting per-event write retries (see DrainLoopAsync).</summary>
    public long ErrorDrops => Interlocked.Read(ref _errorDrops);
    private long _errorDrops;

    // Retry budget for an event whose TryWrite THREW (as opposed to returning false —
    // back-pressure, retried forever). A deterministic per-event failure must not wedge
    // the whole pipeline behind one poison event.
    private const int MaxWriteAttemptsOnError = 5;

    private async Task DrainLoopAsync(CancellationToken ct)
    {
        int  storageBackoffMs = 0;

        // The PENDING slot. TryDequeue is destructive — the slot is recycled and the slab
        // freed before TryWrite runs — so an event that fails to write exists only here.
        // It used to be a local of the loop body and was silently abandoned on every
        // TryWrite==false (which happens routinely: each flush swap has a freeze window),
        // losing one acknowledged event per park, uncounted. The payload stays valid in
        // _payloadBuf because nothing dequeues over it until the pending event lands.
        bool           havePending    = false;
        LogEventHeader pendingHeader  = default;
        string?        pendingTmpl    = null;
        ExceptionInfo? pendingExc     = null;
        int            pendingLen     = 0;
        int            pendingAttempts = 0;

        while (!ct.IsCancellationRequested)
        {
            int drained = 0;
            bool parked = false;

            try
            {
            for (int i = 0; i < DrainBatchSize; i++)
            {
                if (!havePending)
                {
                    // Stop dequeuing once shutdown is requested — the final drain below
                    // owns the remainder of the ring.
                    if (ct.IsCancellationRequested) break;

                    if (!_ring.TryDequeue(
                            out long tsTicks, out byte level, out int tmplIdx,
                            out pendingTmpl, out pendingExc,
                            _payloadBuf, out pendingLen,
                            out ulong traceIdHi, out ulong traceIdLo,
                            out ulong spanId, out int serviceNameIdx))
                        break;

                    pendingHeader = new LogEventHeader
                    {
                        Id                       = 0,  // assigned by StorageEngine.TryWrite
                        TimestampUtcTicks        = tsTicks,
                        Level                    = (Ameto.Core.LogLevel)level,
                        MessageTemplatePoolIndex = tmplIdx,
                        PropertiesArenaOffset    = 0,  // filled by TryWrite
                        PropertiesByteLength     = pendingLen,
                        ServiceNamePoolIndex     = serviceNameIdx,
                        TraceIdHi                = traceIdHi,
                        TraceIdLo                = traceIdLo,
                        SpanId                   = spanId,
                        Flags                    = (byte)(pendingExc is not null ? LogEventFlags.HasException : LogEventFlags.None),
                    };
                    havePending     = true;
                    pendingAttempts = 0;
                }

                if (!TryWritePending(ref havePending, ref pendingAttempts,
                        in pendingHeader, pendingTmpl, pendingExc, pendingLen))
                {
                    // Back-pressure (or a transient write error): park and retry the SAME
                    // event next iteration.
                    storageBackoffMs = Math.Min(storageBackoffMs + 1, 5);
                    parked = true;
                    break;
                }

                drained++;
                storageBackoffMs = 0;
            }

            if (parked)
            {
                await Task.Delay(Math.Max(storageBackoffMs, 1), ct).ConfigureAwait(false);
            }
            else if (drained == 0 && !ct.IsCancellationRequested)
            {
                // Nothing in the ring — block until a producer signals. The 1 s timeout
                // is only a missed-signal safety net: NotifyEnqueued releases the semaphore
                // on every enqueue into an empty ring, so real wake-up latency stays ~0.
                // A short timeout here (was 50 ms) cost ~20 idle wake-ups/sec for nothing.
                await _signal.WaitAsync(1000, ct).ConfigureAwait(false);
            }
            }
            catch (OperationCanceledException) { break; } // shutdown — fall through to final drain
            catch (ObjectDisposedException) { return; }   // ring buffer disposed during shutdown
        }

        // ── Final drain ──────────────────────────────────────────────────────
        // Producers acknowledged these events with 200 the moment they entered the ring;
        // returning on cancellation abandoned up to the ring's whole capacity of them on
        // every clean restart. Drain until empty (the storage engine is disposed AFTER
        // this service, so writes still land and its final flush persists them), bounded
        // by a deadline so a wedged storage engine cannot hang host shutdown.
        var deadline = Environment.TickCount64 + ShutdownDrainMaxMs;
        try
        {
            while (Environment.TickCount64 < deadline)
            {
                if (!havePending)
                {
                    if (!_ring.TryDequeue(
                            out long tsTicks, out byte level, out int tmplIdx,
                            out pendingTmpl, out pendingExc,
                            _payloadBuf, out pendingLen,
                            out ulong traceIdHi, out ulong traceIdLo,
                            out ulong spanId, out int serviceNameIdx))
                        break; // ring empty — done

                    pendingHeader = new LogEventHeader
                    {
                        Id                       = 0,
                        TimestampUtcTicks        = tsTicks,
                        Level                    = (Ameto.Core.LogLevel)level,
                        MessageTemplatePoolIndex = tmplIdx,
                        PropertiesArenaOffset    = 0,
                        PropertiesByteLength     = pendingLen,
                        ServiceNamePoolIndex     = serviceNameIdx,
                        TraceIdHi                = traceIdHi,
                        TraceIdLo                = traceIdLo,
                        SpanId                   = spanId,
                        Flags                    = (byte)(pendingExc is not null ? LogEventFlags.HasException : LogEventFlags.None),
                    };
                    havePending     = true;
                    pendingAttempts = 0;
                }

                if (!TryWritePending(ref havePending, ref pendingAttempts,
                        in pendingHeader, pendingTmpl, pendingExc, pendingLen))
                    await Task.Delay(5, CancellationToken.None).ConfigureAwait(false);
            }
            if (havePending || _ring.ApproximateCount > 0)
                _logger.LogWarning("Shutdown drain deadline hit — events remaining in the ingestion ring were abandoned");
        }
        catch (ObjectDisposedException) { }
    }

    /// <summary>Shutdown budget for writing out ring residue; must fit the host's shutdown timeout.</summary>
    private const long ShutdownDrainMaxMs = 10_000;

    /// <summary>
    /// Attempts to write the pending event. Returns true when the slot is free again —
    /// written, or abandoned after repeated THROWN failures (counted + logged; a thrown
    /// error is a storage fault, and before this catch existed a single one silently
    /// killed the drain task: ingestion stopped for the life of the process while
    /// producers kept seeing ordinary back-pressure drops).
    /// </summary>
    private bool TryWritePending(
        ref bool havePending, ref int attempts,
        in LogEventHeader header, string? tmpl, ExceptionInfo? exception, int payloadLen)
    {
        // An event larger than one hot-tier chunk can NEVER be written — TryWrite returns
        // false on a fresh chunk too, so retrying it would wedge the single drain thread
        // behind one unwritable event forever (MaxEventPayloadBytes is not validated
        // against the chunk size, so the ring can accept and acknowledge such an event).
        if (payloadLen > HotTierSegment.ChunkPayloadBytes)
        {
            Interlocked.Increment(ref _errorDrops);
            _logger.LogError(
                "Event payload of {Bytes} B exceeds the hot-tier chunk capacity of {Cap} B — dropped " +
                "(lower Ingestion.MaxEventPayloadBytes below the chunk size to reject these at the door)",
                payloadLen, HotTierSegment.ChunkPayloadBytes);
            havePending = false;
            return true;
        }

        var payload = new ReadOnlySpan<byte>(_payloadBuf, 0, payloadLen);
        try
        {
            if (!_storage.TryWrite(header, payload, tmpl, exception))
                return false; // back-pressure — retry the same event after a park
        }
        catch (Exception ex) when (ex is not ObjectDisposedException)
        {
            attempts++;
            if (attempts < MaxWriteAttemptsOnError)
            {
                _logger.LogWarning(ex, "Storage write failed (attempt {Attempt}) — will retry", attempts);
                return false;
            }
            Interlocked.Increment(ref _errorDrops);
            _logger.LogError(ex, "Storage write failed {Attempts} times — dropping the event ({Drops} dropped total)",
                attempts, Interlocked.Read(ref _errorDrops));
        }
        havePending = false;
        return true;
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
