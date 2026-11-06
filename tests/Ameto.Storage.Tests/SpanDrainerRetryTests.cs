using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// #138: the receiver answers 200 when spans enter the ring; the drainer then handed each run to the
/// engine and released it in a <c>finally</c> whatever happened. A write that threw part-way — the span
/// log could not grow on a full volume — lost every span of the run the engine had not taken: acknowledged
/// to an exporter that never retries a 200, and gone (2 165 of 3 000 in the review's probe). The drainer
/// now releases only what the engine took and offers the rest again, after a pause, pass after pass.
/// </summary>
public sealed class SpanDrainerRetryTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-drainretry-" + Guid.NewGuid().ToString("N"));

    public SpanDrainerRetryTests() => Directory.CreateDirectory(_dir);

    public void Dispose() { try { Directory.Delete(_dir, recursive: true); } catch { } }

    private static void Publish(SpanRingBuffer ring, int count)
    {
        for (int i = 0; i < count; i++)
        {
            var h = new SpanHeader
            {
                TraceId           = new TraceId(0xD138, (ulong)(i + 1)),
                SpanId            = new SpanId(1),
                StartTimeUnixNano = 1_785_000_000_000_000_000L + i,
                DurationNanos     = 1_000,
                Kind              = SpanKind.Server,
            };
            Assert.True(ring.TryEnqueueRaw(in h, "GET /kept"u8, -1, "gateway"u8, []));
        }
        ring.EndBatch();
    }

    private static async Task AssertEachStoredOnce(TraceStorageEngine engine, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int found = 0;
            await foreach (var _ in engine.GetTraceAsync(new TraceId(0xD138, (ulong)(i + 1)))) found++;
            Assert.True(found == 1, $"span {i + 1}: stored {found} time(s), expected once");
        }
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail($"timed out waiting for {what}");
            await Task.Delay(25);
        }
    }

    /// <summary>
    /// 300 spans in one run: more than one write hold (128), fewer than one drained run (512). The
    /// first two passes throw after their first hold; the third writes what is left. Every span is
    /// stored exactly once — the 172 behind the first fault used to be released and lost.
    /// </summary>
    [Fact]
    public async Task A_write_that_fails_part_way_keeps_the_rest_of_the_run_and_writes_it_on_a_later_pass()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);

        int holds = 0;
        engine._afterWriteHoldForTest = taken =>
        {
            if (Interlocked.Increment(ref holds) <= 2) throw new InvalidOperationException($"write fault after {taken} (test seam)");
        };
        await using var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: true);

        Publish(ring, 300);

        // Pass one: 128 written, fault, 172 held. Pass two (after 100 ms): 128 written, fault, 44 held.
        // Pass three (after 200 ms): 44 written, nothing held — three holds in all.
        await WaitUntil(() => Volatile.Read(ref holds) >= 3 && drainer.HeldForTest == 0, TimeSpan.FromSeconds(20),
                        "the held tail to be written");
        Assert.Equal(0, ring.ApproximateCount);

        var disposal = await Record.ExceptionAsync(() => drainer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Null(disposal);
        await AssertEachStoredOnce(engine, 300);
    }

    /// <summary>
    /// The shutdown lands while the drainer is pausing after a fault: the final drain offers the held
    /// tail again, and every span is stored once.
    /// </summary>
    [Fact]
    public async Task A_held_tail_is_written_by_the_final_drain_at_shutdown()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);

        int holds = 0;
        var faulted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine._afterWriteHoldForTest = _ =>
        {
            if (Interlocked.Increment(ref holds) != 1) return;
            faulted.TrySetResult();
            throw new InvalidOperationException("write fault (test seam)");
        };
        var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: true);

        Publish(ring, 300);
        await faulted.Task.WaitAsync(TimeSpan.FromSeconds(20));

        // Disposed at once: the loop is in its pause, or about to be; the final drain takes the tail.
        var disposal = await Record.ExceptionAsync(() => drainer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Null(disposal);
        Assert.Equal(0, drainer.HeldForTest);
        await AssertEachStoredOnce(engine, 300);
    }
}
