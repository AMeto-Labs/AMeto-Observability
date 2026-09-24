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
        var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: true,
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
}
