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
/// strings; and only then does this give back the payloads of the spans the engine took
/// (<see cref="SpanRingBuffer.Release"/>). The ones it did not take stay reserved, at the front of
/// the run, for the next pass (#138).</para>
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
    /// Spans of the last run the engine did not take — a write that threw part-way — held at the front
    /// of <see cref="_headers"/>/<see cref="_apart"/> with their payloads still reserved in the ring, for
    /// the next pass to offer again (#138). 0 when the last run went through.
    /// </summary>
    private int _held;

    /// <summary>The pause before a held tail is offered again: doubles from <see cref="RetryPauseMinMs"/> to <see cref="RetryPauseMaxMs"/> while the engine keeps refusing.</summary>
    private int _retryPauseMs = RetryPauseMinMs;
    private const int RetryPauseMinMs = 100;
    private const int RetryPauseMaxMs = 1_000;

    /// <summary>
    /// The failing passes, said at most once a minute (<see cref="FailureLineIntervalMs"/>): when the
    /// current streak began (<see cref="Environment.TickCount64"/>; 0 outside a streak) and whether a line
    /// has said so, when the last Error about failures was written — kept across streaks, so a store that
    /// flaps between taking and refusing is limited too — and the passes failed since that line. A volume
    /// that stays full would otherwise be an Error with a stack trace every second for as long as it
    /// lasts, some 3 600 an hour, into log sinks short of room themselves. The first pass that goes
    /// through after a streak that was said says how long the store refused. One consumer thread: no
    /// interlocked.
    /// </summary>
    private long _refusingSinceTicks;
    private bool _streakSaid;
    private long _lastFailureLineTicks;
    private int  _failedPassesSinceLine;
    private const long FailureLineIntervalMs = 60_000;

    /// <summary>What the last pass took, as <see cref="DrainOnce"/> leaves it: the final drain's cap counts passes that took nothing.</summary>
    private int _lastPassTook;

    /// <summary>Test observer: the spans of the last run the engine did not take, waiting for the next pass.</summary>
    internal int HeldForTest => _held;

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
    /// One drained run through the engine: dequeue — or the tail of the last run the engine did not
    /// take — write, release what was taken. Returns how many spans the run held and, in
    /// <paramref name="taken"/>, how many the engine took: fewer without a throw only once its write
    /// path has closed; fewer with a throw when the write failed part-way (the span log could not
    /// grow on a full volume, an out-of-memory building a name).
    ///
    /// <para><b>THE SPANS THE ENGINE DID NOT TAKE ARE KEPT (#138).</b> They used to be released
    /// whatever happened, and the receiver had answered 200 for them the moment they entered the ring
    /// — acknowledged to the exporter, which never retries a 200, and gone: 2 165 of 3 000 spans in a
    /// probe on a 12 MB volume. Now the taken prefix goes back to the arena and the rest move to the
    /// front of the run's arrays, their payloads still reserved, and the next pass offers them again
    /// after a pause. A ring whose tail is stuck fills, and the receivers then turn a batch it has no
    /// room for away with 503 / UNAVAILABLE, which exporters retry: acknowledged means written.</para>
    ///
    /// <para>An engine that has closed its write path takes nothing and throws nothing, so its run stays
    /// held and every later call answers the same count: a caller that loops on the count must stop on
    /// <paramref name="taken"/> &lt; the count, as the loop and the final drain do.</para>
    /// </summary>
    internal int DrainOnce(out int taken)
    {
        taken = 0;
        int count = _held > 0 ? _held : _ring.TryDequeueMany(_headers, _apart);
        if (count == 0) return 0;
        _held = count;
        int written = 0;
        try
        {
            // ONE call per drained batch: the engine takes its write lock and the log's append
            // lock once per hold (see TraceStorageEngine.WriteRaw), not per span. `written` is
            // advanced hold by hold inside the engine, so it is right when the engine throws.
            var batch = _ring.Drained(new ReadOnlySpan<SpanHeader>(_headers, 0, count),
                                      new ReadOnlySpan<byte[]?>(_apart, 0, count), _services);
            _storage.WriteRaw(ref batch, ref written);
        }
        finally
        {
            // The arena under the spans the engine TOOK goes back now: everything the tier keeps was
            // copied out of it inside WriteRaw. The rest keep their payloads and wait at the front.
            _ring.Release(new ReadOnlySpan<SpanHeader>(_headers, 0, written));
            int left = count - written;
            if (left == 0)
            {
                Array.Clear(_apart, 0, count);
            }
            else if (written > 0)
            {
                Array.Copy(_headers, written, _headers, 0, left);
                Array.Copy(_apart,   written, _apart,   0, left);
                Array.Clear(_apart, left, written);
            }
            _held         = left;
            _lastPassTook = written;
        }
        taken = written;
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
                NoteFailedPass(ex);
                MaybeFlush();
                // The tail is still ours (DrainOnce). Offered again after a pause that doubles while the
                // engine keeps refusing, 100 ms to a second: a volume that is full is not hammered, and
                // meanwhile the ring fills behind the held tail until the receivers turn batches away
                // for the exporters to retry (#138). A shutdown that lands in the pause goes to the
                // final drain below, which offers the tail again.
                try { await Task.Delay(_retryPauseMs, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
                _retryPauseMs = Math.Min(_retryPauseMs * 2, RetryPauseMaxMs);
                continue;
            }
            _retryPauseMs = RetryPauseMinMs;
            if (_refusingSinceTicks != 0 && taken > 0) NoteRecovered();

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

        // Drain remaining items before shutdown.
        //
        // A BATCH THAT THROWS IS LOGGED AND THE DRAIN GOES ON, as it does in the loop above. This
        // pass used to end at the first batch that threw, and every span queued behind it — each
        // one already acknowledged to its exporter — stayed in the ring and was never written.
        // DrainOnce keeps the spans the engine did not take (#138), so each failure here offers the
        // SAME tail again, less whatever the engine took of it; a run of failures that long is an
        // engine that will not take spans at all, and the pass stops there and says how many it
        // left — the held tail and what is still in the ring.
        int consecutiveFailures = 0;
        while (true)
        {
            int remaining, taken;
            try
            {
                remaining = DrainOnce(out taken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SpanDrainer: error writing a drained batch in the final drain — {Kept} span(s) of it are kept and offered again", _held);
                // A failing pass that still took spans is the engine taking them, slowly: the eight
                // failures that end this pass are eight that took NOTHING.
                if (_lastPassTook > 0) consecutiveFailures = 0;
                if (++consecutiveFailures < MaxConsecutiveFinalDrainFailures) continue;
                _logger.LogError(
                    "SpanDrainer: the final drain stopped after {Failures} failing batches in a row that took nothing — "
                  + "{Lost} span(s) still in the ring were not stored",
                    consecutiveFailures, _ring.ApproximateCount + _held);
                return;
            }
            consecutiveFailures = 0;
            if (remaining == 0) break;
            if (taken < remaining) { ReportRefused(remaining - taken); return; }
        }
    }

    /// <summary>
    /// Failing passes in a row, none of which took a span, after which the final drain gives up: since
    /// a failed pass keeps its tail (#138), this cap is what ends a final drain into an engine that is
    /// not taking anything — without it the disposal would never return, and the host's shutdown
    /// timeout does not stop the drain task.
    /// </summary>
    private const int MaxConsecutiveFinalDrainFailures = 8;

    /// <summary>
    /// A pass failed. Said at most once a minute, whatever the streaks: a failure a minute or more after
    /// the last line is said in full with its exception when it is the first since that line, else with
    /// the count failed since it, how long the current streak has lasted, and the held tail; one within a
    /// minute of the last line is counted and said with the next (see <see cref="_refusingSinceTicks"/>).
    /// </summary>
    private void NoteFailedPass(Exception ex)
    {
        long now = Environment.TickCount64;
        if (_refusingSinceTicks == 0) { _refusingSinceTicks = now; _streakSaid = false; }
        _failedPassesSinceLine++;
        if (_lastFailureLineTicks != 0 && now - _lastFailureLineTicks < FailureLineIntervalMs) return;
        _lastFailureLineTicks = now;
        _streakSaid           = true;
        if (_failedPassesSinceLine == 1)
            _logger.LogError(ex, "SpanDrainer: error writing a drained batch — {Kept} span(s) of it are kept and offered again", _held);
        else
            _logger.LogError(ex,
                "SpanDrainer: the trace store refused {Failures} pass(es) since the last such line ({Seconds} s into this outage) — "
              + "{Kept} span(s) are kept and offered again",
                _failedPassesSinceLine, (now - _refusingSinceTicks) / 1000, _held);
        _failedPassesSinceLine = 0;
    }

    /// <summary>
    /// The first pass that took spans after a streak of failures: the one sign the outage is over, said
    /// when the streak was. The streak is forgotten BEFORE the line is written: a logger that throws here
    /// must not have every later pass say it again (and, under #137's guard, sleep a second each time).
    /// </summary>
    private void NoteRecovered()
    {
        long seconds = (Environment.TickCount64 - _refusingSinceTicks) / 1000;
        bool said    = _streakSaid;
        _refusingSinceTicks = 0;
        _streakSaid         = false;
        if (said) _logger.LogInformation("SpanDrainer: the trace store takes spans again, after refusing for {Seconds} s", seconds);
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
                // A DRAIN TASK THAT ENDED IN A FAULT. Both drains now catch per batch, so a write
                // that throws no longer gets here; what still can is a fault outside them — the park
                // throwing something other than a cancellation, a logger that throws. Letting it out
                // of here skipped the flush below — the spans already in the tier were then left to
                // the engine's own teardown, which the host timeout may cut short — and threw out of
                // a disposal the host runs on two chains at once. Logged, then the tier is flushed
                // all the same.
                _logger.LogError(ex, "SpanDrainer: the drain loop ended in a fault; flushing what reached the hot tier");
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
