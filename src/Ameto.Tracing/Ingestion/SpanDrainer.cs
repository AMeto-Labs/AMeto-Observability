using Microsoft.Extensions.Logging;
using Ameto.Tracing.Storage;

namespace Ameto.Tracing.Ingestion;

/// <summary>
/// Drains the <see cref="SpanRingBuffer"/> and writes spans to
/// <see cref="TraceStorageEngine"/> in batches.
///
/// <para><b>A drained batch is headers and arena windows</b> (TI#3): the ring hands over each span's
/// 72-byte header and keeps its payload reserved; the engine appends the payload's UTF-8 to the
/// log as it lies, copies the blob out for the tier and resolves the name and service into pool
/// strings; and only then does this give the payloads back (<see cref="SpanRingBuffer.Release"/>),
/// whatever happened in between.</para>
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

    // Completed when the one teardown has ended; every later DisposeAsync awaits it (see DisposeAsync).
    private readonly TaskCompletionSource _disposeCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private DateTime _lastFlush = DateTime.UtcNow;

    /// <summary>
    /// How often, at most, the drainer asks the ring to give back arena memory above its low-water
    /// mark (review F3). Only when it finds the ring EMPTY — the wake it already takes then — so
    /// this adds no timer: an idle server trims once after a burst and is quiet after that.
    /// </summary>
    internal static readonly TimeSpan ArenaTrimInterval = TimeSpan.FromSeconds(30);

    private readonly long _trimIntervalMs;
    private long _lastTrimTicks = Environment.TickCount64;

    /// <summary>Test seam: the idle branch just asked the ring for a trim; the argument is what it gave back.</summary>
    private readonly Action<long>? _afterArenaTrimForTest;

    /// <summary>
    /// Test seam: the loop is about to park on the ring. Handed the loop's own token source, so a
    /// test can land the shutdown exactly there — the one moment a cancellation surfaces as a throw
    /// out of the park rather than as the loop's condition.
    /// </summary>
    private readonly Action<CancellationTokenSource>? _beforeParkForTest;

    // One drained run: the headers copied out of the ring, and the payloads kept apart from the
    // arena (larger than a chunk) — both reused batch after batch, both holding no reference to a
    // tier once the run is released.
    private readonly SpanHeader[]      _headers  = new SpanHeader[BatchSize];
    private readonly byte[]?[]         _apart    = new byte[]?[BatchSize];
    private readonly ServiceIndexCache _services = new();

    public SpanDrainer(
        SpanRingBuffer ring,
        TraceStorageEngine storage,
        ILogger<SpanDrainer> logger)
        : this(ring, storage, logger, startLoop: true)
    {
    }

    /// <param name="startLoop">False for a test that drives <see cref="DrainOnce"/> itself.</param>
    /// <param name="arenaTrimInterval">Null: <see cref="ArenaTrimInterval"/>. A test passes zero to have the
    /// first idle wake after a burst trim, instead of waiting out 30 s.</param>
    /// <param name="afterArenaTrimForTest">Test seam, called with what each trim gave back. A constructor
    /// argument, not a settable field: the loop starts here, and may reach its first trim before a
    /// field set afterwards is seen.</param>
    /// <param name="beforeParkForTest">Test seam, called with the loop's token source each time the loop is
    /// about to park. A constructor argument for the same reason as the trim seam.</param>
    internal SpanDrainer(SpanRingBuffer ring, TraceStorageEngine storage, ILogger<SpanDrainer> logger, bool startLoop,
                         TimeSpan? arenaTrimInterval = null, Action<long>? afterArenaTrimForTest = null,
                         Action<CancellationTokenSource>? beforeParkForTest = null)
    {
        _ring    = ring;
        _storage = storage;
        _logger  = logger;
        _trimIntervalMs = (long)(arenaTrimInterval ?? ArenaTrimInterval).TotalMilliseconds;
        _afterArenaTrimForTest = afterArenaTrimForTest;
        _beforeParkForTest     = beforeParkForTest;
        _drainTask = startLoop ? Task.Run(DrainLoopAsync) : Task.CompletedTask;
    }

    /// <summary>
    /// One drained run through the engine: dequeue, write, release. Returns how many spans came
    /// out of the ring and, in <paramref name="taken"/>, how many the engine took (fewer only once
    /// its write path has closed). A throw from the engine propagates AFTER the payloads are
    /// released.
    /// </summary>
    internal int DrainOnce(out int taken)
    {
        taken = 0;
        int count = _ring.TryDequeueMany(_headers, _apart);
        if (count == 0) return 0;
        try
        {
            // ONE call per drained batch: the engine takes its write lock and the log's append
            // lock once per hold (see TraceStorageEngine.WriteRaw), not per span.
            var batch = _ring.Drained(new ReadOnlySpan<SpanHeader>(_headers, 0, count),
                                      new ReadOnlySpan<byte[]?>(_apart, 0, count), _services);
            taken = _storage.WriteRaw(ref batch);
        }
        finally
        {
            // Whatever the engine did, the arena under this run goes back now: everything the tier
            // keeps was copied out of it inside WriteRaw.
            _ring.Release(new ReadOnlySpan<SpanHeader>(_headers, 0, count));
            Array.Clear(_apart, 0, count);
        }
        return count;
    }

    private async Task DrainLoopAsync()
    {
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            int count, taken;
            try
            {
                count = DrainOnce(out taken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SpanDrainer: error writing a drained batch");
                MaybeFlush();
                continue;
            }

            if (count == 0)
            {
                MaybeFlush();
                MaybeTrimArena();
                // Park until a producer signals new spans. The 1 s timeout is only a
                // missed-signal safety net (was 50 ms, which burned ~20 idle wake-ups/sec).
                //
                // A SHUTDOWN THAT LANDS WHILE PARKED IS A BREAK, NOT A THROW. The park throws
                // OperationCanceledException then — even with a span published and signalled a
                // moment before, since a cancelled token wins over a ready count — and letting it
                // out skipped the final drain below: whatever was published after the last empty
                // pass (a slot claimed before that pass and published after it included) stayed in
                // the ring, acknowledged and never written.
                _beforeParkForTest?.Invoke(_cts);
                try { await _ring.WaitForItemsAsync(1000, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
                continue;
            }

            // The engine has closed its write path. Every further span would be refused
            // too, so stop draining rather than spinning the ring empty into a closed
            // engine — and say so once, with the count, instead of once per span.
            if (taken < count) { ReportRefused(count - taken); return; }

            MaybeFlush();
        }

        // Drain remaining items before shutdown
        while (true)
        {
            int remaining = DrainOnce(out int taken);
            if (remaining == 0) break;
            if (taken < remaining) { ReportRefused(remaining - taken); return; }
        }
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

    /// <summary>
    /// Gives the ring's arena back above its low-water mark, at most once per
    /// <see cref="ArenaTrimInterval"/>, from the idle branch of the loop. Cheap when there is
    /// nothing to give: the ring's high-water mark is already at the low-water mark.
    /// </summary>
    private void MaybeTrimArena()
    {
        long now = Environment.TickCount64;
        if (now - _lastTrimTicks < _trimIntervalMs) return;
        _lastTrimTicks = now;
        if (_ring.ArenaHighWaterBytes <= (long)SpanRingBuffer.LowWaterChunks * SpanRingBuffer.ChunkBytes) return;
        try
        {
            long given = _ring.TrimIdleArena();
            _afterArenaTrimForTest?.Invoke(given);
            if (given > 0)
                _logger.LogDebug("SpanDrainer: gave back {Bytes} B of span-ring arena after a burst", given);
        }
        catch (Exception ex) { _logger.LogWarning(ex, "SpanDrainer: span-ring arena trim failed"); }
    }

    /// <summary>Asks the engine to flush if the hot tier is due, every <see cref="FlushCheckInterval"/>.</summary>
    private void MaybeFlush()
    {
        if (DateTime.UtcNow - _lastFlush < FlushCheckInterval) return;
        _lastFlush = DateTime.UtcNow;
        try { _storage.FlushIfDue(); }
        catch (Exception ex) { _logger.LogWarning(ex, "SpanDrainer: periodic hot-tier flush failed"); }
    }

    /// <summary>
    /// Stops the loop, lets its final drain finish, and flushes the tier — ONCE; every other caller
    /// waits for that one teardown to end.
    ///
    /// <para><b>A SECOND CALLER MUST NOT RETURN EARLY.</b> SpanDrainerService disposes this from both
    /// StopAsync and its own DisposeAsync, a host stops on two chains at once (the stop the caller
    /// asked for, and <c>app.Run()</c>'s once ApplicationStopping wakes it), and the DI container
    /// disposes the singleton as well — and, because this was resolved after the ring, disposes the
    /// RING right after it. Returning on the exchange let that happen while the first caller was
    /// still joining a final drain: <see cref="SpanRingBuffer.Dispose"/> freed the slots, cursors,
    /// chunk counts and arena under a loop still reading them (an AccessViolation in
    /// <c>TryDequeueMany</c>, or a silent read of whatever reused the pages), and the other chain
    /// tore the engine down under the drain it was still feeding. The engine's DisposeAsync hands
    /// its later callers the same teardown for the same reason.</para>
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            await _disposeCompleted.Task.ConfigureAwait(false);
            return;
        }

        try
        {
            _cts.Cancel();
            try { await _drainTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                // THE FINAL DRAIN IS THE ONE WRITE THE LOOP DOES NOT GUARD: a batch that throws in the
                // main loop is logged and the loop goes on, but a throw out of the "drain remaining
                // items" pass ends the task. Letting it out of here skipped the flush below — the
                // spans that drain had already put in the tier were then left to the engine's own
                // teardown, which the host timeout may cut short — and threw out of a disposal the
                // host runs on two chains at once. Logged, then the tier is flushed all the same.
                _logger.LogError(ex, "SpanDrainer: the final drain at shutdown failed; flushing what reached the hot tier");
            }

            // Final flush so spans drained from the ring buffer at shutdown reach disk
            // even if the engine's own Dispose flush is cut short by the host timeout.
            try { _storage.FlushHotTier(); }
            catch (Exception ex) { _logger.LogWarning(ex, "SpanDrainer: shutdown hot-tier flush failed"); }

            // Cancelling/disposing the CTS twice throws ObjectDisposedException — one caller only.
            _cts.Dispose();
        }
        finally { _disposeCompleted.TrySetResult(); }
    }
}
