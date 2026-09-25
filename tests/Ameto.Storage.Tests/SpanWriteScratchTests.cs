using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE FLUSH'S BIG SCRATCH ARRAYS ARE THE ENGINE'S, NOT THE LAST FLUSHING THREAD'S (#90).
/// <c>ArrayPool.Shared</c> keeps a returned array in the returning thread's slot first, and a flush
/// runs on whatever thread-pool thread its task lands on — so a flush on another thread allocated
/// the sort permutation, its keys and the <c>.tracesum</c> body afresh on the LOH: +1.8 MB of a
/// 50 000-span flush's 20.8 MB on a fresh thread (measured in Release, grpI probe; 19.0 MB with the
/// engine's scratch, 18.35 MB on the thread that last returned them).
///
/// <para>Asserted by IDENTITY, not by bytes: the arrays the engine holds after a flush on one thread
/// are the very ones a flush on another thread used and gave back. A byte figure on a fresh thread
/// depends on what other tests left in the shared pool's per-core stacks; an array's identity does
/// not. Reverted (the engine passes no scratch): the engine holds nothing.</para>
/// </summary>
public sealed class SpanWriteScratchTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-scratch-" + Guid.NewGuid().ToString("N"));

    public SpanWriteScratchTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static void Fill(TraceStorageEngine e, int first, int count)
    {
        for (int i = first; i < first + count; i++)
            e.WriteSpan(new SpanIngestItem
            {
                TraceId = new TraceId(0x5C4A, (ulong)(i / 10 + 1)), SpanId = new SpanId((ulong)(i + 1)),
                StartTimeUnixNano = 1_785_000_000_000_000_000L + i * 1_000L, DurationNanos = 1_000,
                Name = "op", ServiceName = "svc", Kind = SpanKind.Server,
            });
    }

    private static void OnFreshThread(Action a)
    {
        Exception? fault = null;
        var t = new Thread(() => { try { a(); } catch (Exception ex) { fault = ex; } });
        t.Start();
        Assert.True(t.Join(TimeSpan.FromSeconds(60)), "the flush thread hung");
        if (fault is not null) throw new Xunit.Sdk.XunitException("flush thread faulted: " + fault);
    }

    [Fact]
    public void A_flush_on_another_thread_reuses_the_arrays_the_last_flush_gave_back()
    {
        using var e = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);

        Fill(e, 0, 5_000);
        OnFreshThread(e.FlushHotTier);
        var first = e.WriteScratchForTest.HeldForTest;
        Assert.NotNull(first.Order);
        Assert.NotNull(first.Keys);
        Assert.NotNull(first.Body);
        Assert.NotNull(first.Pairs);                                   // the trace-index refs, released after the run (TS#7(c))

        Fill(e, 5_000, 5_000);
        OnFreshThread(e.FlushHotTier);                                 // another thread, empty pool slots
        var second = e.WriteScratchForTest.HeldForTest;

        Assert.Same(first.Order, second.Order);
        Assert.Same(first.Keys,  second.Keys);
        Assert.Same(first.Body,  second.Body);
        Assert.Same(first.Pairs, second.Pairs);
        Assert.Equal(2, e.ColdSegmentCountForTest);
    }

    [Fact]
    public void An_array_past_a_flush_size_is_not_kept()
    {
        var s = new SpanWriteScratch();

        var big = s.RentOrder(SpanWriteScratch.MaxKeptElements + 1);   // a compaction pass's
        s.Return(big);
        Assert.Null(s.HeldForTest.Order);

        var fits = s.RentOrder(SpanWriteScratch.MaxKeptElements);
        s.Return(fits);
        Assert.Same(fits, s.HeldForTest.Order);

        // Taken is taken: a second renter finds the slot empty and gets its own array.
        var a = s.RentOrder(10);
        var b = s.RentOrder(10);
        Assert.Same(fits, a);
        Assert.NotSame(a, b);
        s.Return(b);
        s.Return(a);
        Assert.Same(a, s.HeldForTest.Order);                              // the last given back is kept
    }
}
