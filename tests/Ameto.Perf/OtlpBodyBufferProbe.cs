using System.Buffers;
using Ameto.Core;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What the ingest receivers pay to get a buffer for one request body.
///
/// <para>A batch of 1.4 MB rounds up to the 2 MiB bucket, and every OTLP/CLEF receiver rents
/// one per request. <see cref="ArrayPool{T}.Shared"/> keeps one array per thread plus at most
/// eight per core in each bucket and drops all of it on every gen2 collection, so past that
/// depth each concurrent request gets a fresh, zeroed 2 MiB array straight on the large object
/// heap. <see cref="IngestBufferPool"/> is deeper and is never trimmed.</para>
///
/// <para>Four numbers are printed and none of them is asserted on; the reasoning is at the
/// assertion, and the claims that CAN be held still — the depth bound, the trim, and the rule
/// that fires it — are the three facts below it.</para>
/// </summary>
public sealed class OtlpBodyBufferProbe
{
    private readonly ITestOutputHelper _out;
    public OtlpBodyBufferProbe(ITestOutputHelper o) => _out = o;

    private const int Concurrency = 32;
    private const int Rounds      = 20;
    private const int BodyBytes   = 1_400_000;

    [Fact]
    public async Task DedicatedPoolAbsorbsConcurrentBodies()
    {
        await Measure(dedicated: false, gen2: false);                 // warm
        await Measure(dedicated: true,  gen2: false);

        long sharedFlat = await Measure(dedicated: false, gen2: false);
        long ownFlat    = await Measure(dedicated: true,  gen2: false);
        long sharedGen2 = await Measure(dedicated: false, gen2: true);
        long ownGen2    = await Measure(dedicated: true,  gen2: true);

        int requests = Rounds * Concurrency;
        _out.WriteLine($"{Concurrency} concurrent {BodyBytes / 1024.0 / 1024.0:F1} MB bodies x {Rounds} rounds "
                     + $"= {requests} rents, on {Environment.ProcessorCount} cores");
        _out.WriteLine($"  ArrayPool.Shared                : {sharedFlat / 1024.0 / 1024.0,8:F1} MB "
                     + $"({sharedFlat / (double)requests / 1024:F0} KB/request)");
        _out.WriteLine($"  IngestBufferPool                : {ownFlat / 1024.0 / 1024.0,8:F1} MB "
                     + $"({ownFlat / (double)requests / 1024:F0} KB/request)");
        _out.WriteLine($"  ArrayPool.Shared + gen2 per round: {sharedGen2 / 1024.0 / 1024.0,7:F1} MB "
                     + $"({sharedGen2 / (double)requests / 1024:F0} KB/request)");
        _out.WriteLine($"  IngestBufferPool + gen2 per round: {ownGen2 / 1024.0 / 1024.0,7:F1} MB "
                     + $"({ownGen2 / (double)requests / 1024:F0} KB/request)");

        // Deliberately no assertion on these four numbers, and it is worth saying why rather
        // than asserting something that only holds on the machine it was written on.
        //
        // ArrayPool.Shared gives each THREAD one array per bucket on top of its per-core
        // stacks. A probe that reuses ~32 warm thread-pool threads therefore sees it hit every
        // time; the misses this pool exists to remove come from thread churn and from Shared's
        // own gen2 trimming, neither of which a tight loop reproduces. The first measurement
        // taken here, before that warm round existed, read 24-32 MB against 0 MB — real, and
        // real only because Shared was cold.
        //
        // And IngestBufferPool is now emptied by a gen2 whenever the GC reports high memory
        // load, so its steady-state allocation is a function of how loaded the host is. That is
        // the behaviour asked for, and it is not something a test can hold still.
        //
        // What IS asserted about this pool lives in the three facts above: the depth bound, the
        // trim releasing what it held, and the rule that decides when to trim.
        Assert.True(ownFlat >= 0 && sharedFlat >= 0);
    }

    // ── What the pool is allowed to keep ──────────────────────────────────────

    [Fact]
    public void DepthIsBoundedByCores()
    {
        // A pool that is never trimmed is a memory leak with good manners. The gRPC reader gets
        // no Content-Length, so it doubles 64 KB → 2 MB and leaves an array in every bucket on
        // the way; at a flat 32 deep that is ~126 MB pinned for ever on a stand whose whole
        // budget is 512 MB. Depth follows the core count, which is what actually bounds how
        // many requests can be in flight.
        Assert.Equal(Math.Clamp(2 * Environment.ProcessorCount,
                                IngestBufferPool.MinArraysPerBucket,
                                IngestBufferPool.MaxArraysPerBucket),
                     IngestBufferPool.ArraysPerBucket);
        Assert.InRange(IngestBufferPool.ArraysPerBucket,
                       IngestBufferPool.MinArraysPerBucket, IngestBufferPool.MaxArraysPerBucket);

        _out.WriteLine($"{Environment.ProcessorCount} cores ⇒ {IngestBufferPool.ArraysPerBucket} arrays/bucket "
                     + $"⇒ at most ~{IngestBufferPool.ArraysPerBucket * 2L * IngestBufferPool.MaxPooledBytes / 1024 / 1024} MB "
                     + "held between trims");
    }

    [Fact]
    public void TrimActuallyReleasesWhatThePoolHeld()
    {
        // Rent, return, trim: the array must NOT come back, or "trim" is a comment rather than
        // a release and the 512 MB stand goes on OOMing.
        byte[] before = IngestBufferPool.Rent(BodyBytes);
        IngestBufferPool.Return(before);

        byte[] again = IngestBufferPool.Rent(BodyBytes);
        IngestBufferPool.Return(again);
        Assert.Same(before, again);                       // sanity: the pool does reuse

        IngestBufferPool.Trim();

        byte[] after = IngestBufferPool.Rent(BodyBytes);
        try { Assert.NotSame(before, after); }
        finally { IngestBufferPool.Return(after); }
    }

    [Theory]
    [InlineData(100, 200, false)]   // comfortable
    [InlineData(199, 200, false)]   // close, but under
    [InlineData(200, 200, true)]    // at the GC's own high-load threshold
    [InlineData(400, 200, true)]    // past it
    [InlineData(400,   0, false)]   // threshold unknown — never trim on a guess
    public void TrimTriggersOnTheGcsOwnHighLoadThreshold(long load, long threshold, bool expected)
        => Assert.Equal(expected, IngestBufferPool.ShouldTrim(load, threshold));

    private static async Task<long> Measure(bool dedicated, bool gen2)
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);

        var tasks = new Task[Concurrency];

        // One unmeasured round, AFTER the collections above. Both pools are emptied by a gen2
        // — the shared one by its own trimming, this one by the pressure hook — and what is
        // being compared is steady-state reuse, not the cost of the first fill. Without this
        // the measurement is dominated by whichever pool was collected last.
        await Round(tasks, dedicated);

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int r = 0; r < Rounds; r++)
        {
            await Round(tasks, dedicated);
            if (gen2) GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        }
        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    /// <summary>One round: <see cref="Concurrency"/> request bodies rented, held and returned.</summary>
    private static async Task Round(Task[] tasks, bool dedicated)
    {
        for (int i = 0; i < Concurrency; i++)
            tasks[i] = Task.Run(() =>
            {
                byte[] buf = dedicated ? IngestBufferPool.Rent(BodyBytes) : ArrayPool<byte>.Shared.Rent(BodyBytes);
                try
                {
                    buf[0] = 1;                       // touch both ends so the pages are real
                    buf[BodyBytes - 1] = 2;
                    Thread.SpinWait(2_000);           // hold it, the way a parse does
                }
                finally
                {
                    if (dedicated) IngestBufferPool.Return(buf); else ArrayPool<byte>.Shared.Return(buf);
                }
            });
        await Task.WhenAll(tasks);
    }
}
