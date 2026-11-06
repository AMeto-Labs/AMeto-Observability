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

    /// <summary>
    /// <paramref name="count"/> spans of 16 payload bytes each — span <paramref name="heavyAt"/> (1-based)
    /// carrying <paramref name="heavy"/> as its attributes instead, if given: a payload larger than a
    /// ring chunk, which the ring parks apart from its arena.
    /// </summary>
    private static void Publish(SpanRingBuffer ring, int count, int heavyAt = 0, byte[]? heavy = null)
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
            ReadOnlySpan<byte> attrs = i + 1 == heavyAt && heavy is not null ? heavy.AsSpan() : ReadOnlySpan<byte>.Empty;
            Assert.True(ring.TryEnqueueRaw(in h, "GET /kept"u8, -1, "gateway"u8, attrs));
        }
        ring.EndBatch();
    }

    /// <summary>A valid msgpack map holding one string of <paramref name="chars"/> characters: past a 64 KiB chunk, the ring parks it apart.</summary>
    private static byte[] HeavyBlob(int chars)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(chars + 64);
        var w   = new MessagePack.MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("db.statement");
        w.Write(new string('x', chars));
        w.Flush();
        return buf.WrittenMemory.ToArray();
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

    private static async Task<SpanRecord> StoredSpan(TraceStorageEngine engine, int i)
    {
        SpanRecord? found = null;
        await foreach (var s in engine.GetTraceAsync(new TraceId(0xD138, (ulong)i))) { Assert.Null(found); found = s; }
        Assert.NotNull(found);
        return found;
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
    /// 300 spans in one run: more than one write hold (128, pinned), fewer than one drained run (512),
    /// published before the loop starts so its first pass sees all of them. The first two passes throw
    /// after their first hold; the third writes what is left. Every span is stored exactly once — the
    /// 172 behind the first fault used to be released and lost. Span 201 carries a 70 000-byte
    /// attribute blob, which the ring parks apart from its arena: it sits in the held tail at index 72
    /// and is written by the second pass, and it comes back with its own blob — the tail's parked
    /// payloads move with their headers.
    /// </summary>
    [Fact]
    public async Task A_write_that_fails_part_way_keeps_the_rest_of_the_run_and_writes_it_on_a_later_pass()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        engine._maxSpansPerWriteHold = 128;

        int holds = 0;
        engine._afterWriteHoldForTest = taken =>
        {
            if (Interlocked.Increment(ref holds) <= 2) throw new InvalidOperationException($"write fault after {taken} (test seam)");
        };
        byte[] heavy = HeavyBlob(70_000);
        Publish(ring, 300, heavyAt: 201, heavy);
        await using var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: true);

        // Pass one: 128 written, fault, 172 held. Pass two (after 100 ms): 128 written, fault, 44 held.
        // Pass three (after 200 ms): 44 written, nothing held — three holds in all.
        await WaitUntil(() => Volatile.Read(ref holds) >= 3 && drainer.HeldForTest == 0, TimeSpan.FromSeconds(20),
                        "the held tail to be written");
        Assert.Equal(0, ring.ApproximateCount);

        await drainer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));
        await AssertEachStoredOnce(engine, 300);
        var parked = await StoredSpan(engine, 201);
        Assert.Equal("GET /kept", parked.Name);
        Assert.True(parked.AttributesBytes.Span.SequenceEqual(heavy), "the parked span came back with another span's attributes, or none");
    }

    /// <summary>
    /// The log refuses an entry in the MIDDLE of a hold — the probe's full volume, where the span WAL
    /// could not grow under the 51st append. The 50 it took are in the log and the tier, and the drainer
    /// must hear 50, which the per-hold finally mirrors. Mirrored only at a hold's end, the 50 would be
    /// offered again and stored twice.
    /// </summary>
    [Fact]
    public async Task A_log_that_refuses_an_entry_mid_hold_keeps_the_rest_and_stores_each_span_once()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        engine._maxSpansPerWriteHold = 128;
        int faults = 0;
        engine.WalForTest._beforeBatchEntryForTest = index =>
        {
            if (index == 50 && Interlocked.Increment(ref faults) == 1)
                throw new IOException("injected: the span WAL could not grow");
        };
        await using var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: false);
        Publish(ring, 300);

        Assert.Throws<IOException>(() => drainer.DrainOnce(out _));
        Assert.Equal(250, drainer.HeldForTest);                     // 50 in the log and the tier, 250 kept
        Assert.Equal(250, drainer.DrainOnce(out int taken));
        Assert.Equal(250, taken);
        Assert.Equal(0, drainer.HeldForTest);
        await AssertEachStoredOnce(engine, 300);
    }

    /// <summary>
    /// The shutdown lands while the drainer holds a tail the store keeps refusing: the final drain offers
    /// it again, and every span is stored once. The store refuses every hold at its first entry until the
    /// test lets go, so the tail is held across every pause, not only the first 100 ms.
    /// </summary>
    [Fact]
    public async Task A_held_tail_is_written_by_the_final_drain_at_shutdown()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        engine._maxSpansPerWriteHold = 128;

        int  holds    = 0;
        bool refusing = true;   // read on the drainer's thread; cleared once, right before the disposal
        engine._afterWriteHoldForTest = _ =>
        {
            if (Interlocked.Increment(ref holds) == 1) throw new InvalidOperationException("write fault after the first hold (test seam)");
        };
        engine.WalForTest._beforeBatchEntryForTest = index =>
        {
            if (index == 0 && Volatile.Read(ref holds) >= 1 && Volatile.Read(ref refusing))
                throw new IOException("injected: the volume is still full");
        };
        Publish(ring, 300);
        await using var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: true);

        await WaitUntil(() => drainer.HeldForTest == 172, TimeSpan.FromSeconds(20), "the tail behind the first hold to be held");
        Volatile.Write(ref refusing, false);

        // Disposed while the loop pauses after a refused pass: the final drain offers the tail, and the store takes it.
        await drainer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(0, drainer.HeldForTest);
        await AssertEachStoredOnce(engine, 300);
    }

    /// <summary>
    /// The final drain into a store that takes nothing: since a failed pass keeps its tail, the
    /// eight-failure cap is the only thing that ends it — without the cap the disposal never returns, and
    /// the host's shutdown timeout does not stop the drain task.
    /// </summary>
    [Fact]
    public async Task A_final_drain_into_a_store_that_takes_nothing_gives_up_after_eight_passes()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        engine._maxSpansPerWriteHold = 128;
        int calls = 0;
        engine.WalForTest._beforeBatchEntryForTest = _ =>
        {
            // The ceiling keeps a drain without the cap from hanging the run: it gets through on call 101
            // and fails the count below instead.
            if (Interlocked.Increment(ref calls) <= 100) throw new IOException("injected: the volume is full");
        };
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int parks = 0;
        await using var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: true,
            beforeParkForTest: cts =>
            {
                if (Interlocked.Increment(ref parks) != 1) return;
                Publish(ring, 300);
                cts.Cancel();
                cancelled.TrySetResult();
            });
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await drainer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(8, Volatile.Read(ref calls));     // eight passes, each refused at its first entry, then it gave up
        Assert.Equal(300, drainer.HeldForTest);       // all of it still held, and counted in the give-up line
    }
}
