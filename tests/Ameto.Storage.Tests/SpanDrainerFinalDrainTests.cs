using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// A SHUTDOWN THAT LANDS WHILE THE DRAINER IS PARKED STILL RUNS THE FINAL DRAIN (PR #84 review,
/// #2). The park is <c>WaitForItemsAsync(1000, ct)</c>, and a cancelled token makes it throw — even
/// with a span published and signalled a moment before, because a semaphore wait checks the token
/// before the count. The throw left the loop past the "drain remaining items" pass, and
/// <c>DisposeAsync</c> swallowed it: the span stayed in the ring, acknowledged to its exporter and
/// never written.
///
/// <para>At the seam, not on a clock: the loop's park seam publishes one span and cancels the
/// loop's own token, on the loop's thread, immediately before the park.</para>
/// </summary>
public sealed class SpanDrainerFinalDrainTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-finaldrain-" + Guid.NewGuid().ToString("N"));

    public SpanDrainerFinalDrainTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    /// <summary>
    /// Reverted (the park's cancellation let out of the loop): the span is still in the ring after
    /// <c>DisposeAsync</c> (ApproximateCount 1, expected 0) and the engine never saw it.
    /// </summary>
    [Fact]
    public async Task A_span_published_as_the_drainer_parks_into_a_shutdown_is_still_written()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        var trace = new TraceId(0xD2A1, 7);

        int parks = 0;
        var cancelledWhileParking = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // `await using`, declared after the ring and the engine: on EVERY exit — a timed-out wait
        // included — the drainer's loop is joined before `using` frees the ring's native memory, so
        // a failure fails this test instead of killing the test host with an AccessViolation.
        await using var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: true,
            beforeParkForTest: cts =>
            {
                if (Interlocked.Increment(ref parks) != 1) return;

                // Published — and signalled — just before the park; the shutdown lands at once.
                var h = new SpanHeader
                {
                    TraceId           = trace,
                    SpanId            = new SpanId(1),
                    StartTimeUnixNano = 1_785_000_000_000_000_000L,
                    DurationNanos     = 1_000,
                    Kind              = SpanKind.Server,
                };
                Assert.True(ring.TryEnqueueRaw(in h, "GET /late"u8, -1, "gateway"u8, []));
                ring.EndBatch();
                cts.Cancel();
                cancelledWhileParking.TrySetResult();
            });

        // The shutdown is the SEAM's, landed at the park; disposing before it would cancel the loop at its
        // condition instead, which never took the throwing road. The guard only bounds a hang.
        await cancelledWhileParking.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await drainer.DisposeAsync();                    // joins the loop, then flushes the tier

        Assert.Equal(1, Volatile.Read(ref parks));       // the loop parked once, and left
        Assert.Equal(0, ring.ApproximateCount);          // nothing left behind in the ring
        int found = 0;
        await foreach (var _ in engine.GetTraceAsync(trace)) found++;
        Assert.Equal(1, found);
    }

    /// <summary>
    /// EVERY <c>DisposeAsync</c> RETURNS AFTER THE LOOP HAS LET GO OF THE RING, NOT ONLY THE FIRST.
    /// A host stops on two chains at once (the factory's stop, and <c>app.Run()</c>'s once
    /// ApplicationStopping wakes it), and the container disposes the drainer as well; the drainer
    /// is resolved after the ring, so the container disposes the RING right after it. A second
    /// caller that returned at once — while the first was still joining a final drain — let the
    /// container free the ring's slots, cursors, chunk counts and arena under that drain: a
    /// use-after-free of native memory (an AccessViolation in <c>TryDequeueMany</c>, or a silent
    /// read of whatever reused the pages). Since the final drain runs on every stop that lands at
    /// the park, that race was open on nearly every host stop.
    ///
    /// <para>At the seams, not on a clock: the park seam publishes one span and cancels the loop,
    /// so the loop goes into its final drain; the engine's after-hold seam holds it THERE — inside
    /// <c>DrainOnce</c>, with the drained run not yet released to the ring. Both disposals are made
    /// while it is held. The waits only bound a hang.</para>
    ///
    /// <para>Reverted (a second caller returns on the exchange): the second DisposeAsync has
    /// completed while the loop is still inside the ring.</para>
    /// </summary>
    [Fact]
    public async Task A_second_dispose_waits_until_the_final_drain_has_let_go_of_the_ring()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        var trace = new TraceId(0xD2A2, 9);

        var inFinalDrain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var letGo  = new ManualResetEventSlim(false);
        engine._afterWriteHoldForTest = _ =>
        {
            // Only the final drain writes in this test: the one span is published at the park.
            inFinalDrain.TrySetResult();
            letGo.Wait(TimeSpan.FromSeconds(30));
        };

        int parks = 0;
        // `await using`, declared after the ring and the engine: on EVERY exit — a timed-out wait
        // included — the drainer's loop is joined before `using` frees the ring's native memory, so
        // a failure fails this test instead of killing the test host with an AccessViolation.
        await using var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: true,
            beforeParkForTest: cts =>
            {
                if (Interlocked.Increment(ref parks) != 1) return;
                var h = new SpanHeader
                {
                    TraceId           = trace,
                    SpanId            = new SpanId(1),
                    StartTimeUnixNano = 1_785_000_000_000_000_000L,
                    DurationNanos     = 1_000,
                    Kind              = SpanKind.Server,
                };
                Assert.True(ring.TryEnqueueRaw(in h, "GET /held"u8, -1, "gateway"u8, []));
                ring.EndBatch();
                cts.Cancel();
            });

        ValueTask first = default, second = default;
        bool secondDoneWhileHeld;
        try
        {
            await inFinalDrain.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // The loop is inside DrainOnce now, its run unreleased. The token is already cancelled,
            // so neither call runs the loop inline: the first joins it, the second is the other chain.
            first  = drainer.DisposeAsync();
            second = drainer.DisposeAsync();
            secondDoneWhileHeld = second.IsCompleted;
        }
        finally { letGo.Set(); }

        await first.AsTask().WaitAsync(TimeSpan.FromSeconds(30));
        await second.AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.False(secondDoneWhileHeld,
            "a second DisposeAsync returned while the drain loop was still inside the ring — the container "
          + "disposes the ring next, and frees its native memory under that loop");
        Assert.Equal(0, ring.ApproximateCount);
        int found = 0;
        await foreach (var _ in engine.GetTraceAsync(trace)) found++;
        Assert.Equal(1, found);
    }

    /// <summary>
    /// A FINAL DRAIN THAT THROWS STILL ENDS IN THE DRAINER'S OWN FLUSH (#94). The main loop logs a
    /// batch that throws and goes on; the "drain remaining items" pass after it does not, so its
    /// throw ends the drain task, and <c>DisposeAsync</c> caught only
    /// <c>OperationCanceledException</c> around the join: the exception left the disposal and the
    /// <c>FlushHotTier</c> after it never ran.
    ///
    /// <para>At the seams: the park seam publishes one span and cancels the loop, so the only write
    /// in the test is the final drain's; the engine's after-hold seam throws from inside that write,
    /// after the span is in the hot tier — the shape of any failure past the tier insert.</para>
    ///
    /// <para>Reverted (only the cancellation caught): <c>DisposeAsync</c> throws the seam's
    /// exception, and the span is still in the hot tier with no cold segment.</para>
    /// </summary>
    [Fact]
    public async Task A_final_drain_that_throws_is_logged_and_the_tier_is_still_flushed()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        var trace  = new TraceId(0xD2A3, 11);
        var logger = new CapturingLogger();

        int throws = 0;
        engine._afterWriteHoldForTest = _ =>
        {
            // Only the final drain writes here; its span is in the tier by now.
            if (Interlocked.Increment(ref throws) == 1)
                throw new InvalidOperationException("final-drain fault (test seam)");
        };

        int parks = 0;
        var cancelledWhileParking = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // `await using` after the ring and the engine, as above: the loop is joined before the ring is freed.
        await using var drainer = new SpanDrainer(ring, engine, logger, startLoop: true,
            beforeParkForTest: cts =>
            {
                if (Interlocked.Increment(ref parks) != 1) return;
                var h = new SpanHeader
                {
                    TraceId           = trace,
                    SpanId            = new SpanId(1),
                    StartTimeUnixNano = 1_785_000_000_000_000_000L,
                    DurationNanos     = 1_000,
                    Kind              = SpanKind.Server,
                };
                Assert.True(ring.TryEnqueueRaw(in h, "GET /faulted"u8, -1, "gateway"u8, []));
                ring.EndBatch();
                cts.Cancel();
                cancelledWhileParking.TrySetResult();
            });

        await cancelledWhileParking.Task.WaitAsync(TimeSpan.FromSeconds(30));
        var disposal = await Record.ExceptionAsync(() => drainer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Null(disposal);                                   // the fault stayed inside the disposal
        Assert.Equal(1, Volatile.Read(ref throws));              // and it did happen, in the final drain
        Assert.Equal(0, engine.HotBytesForTest);                 // the drainer's own flush ran
        Assert.Equal(1, engine.ColdSegmentCountForTest);
        Assert.Contains(logger.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Error
                                          && e.Error is InvalidOperationException);
        int found = 0;
        await foreach (var _ in engine.GetTraceAsync(trace)) found++;
        Assert.Equal(1, found);
    }

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger<SpanDrainer>
    {
        private readonly List<(Microsoft.Extensions.Logging.LogLevel Level, Exception? Error)> _entries = [];

        public IReadOnlyList<(Microsoft.Extensions.Logging.LogLevel Level, Exception? Error)> Entries
        {
            get { lock (_entries) return [.. _entries]; }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel level) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel level,
                                Microsoft.Extensions.Logging.EventId eventId, TState state,
                                Exception? error, Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((level, error));
        }
    }
}
