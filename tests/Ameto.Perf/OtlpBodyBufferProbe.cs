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
/// <para>The assertion is on the depth alone, which is what a probe can hold still: with more
/// requests in flight than the shared pool's per-core capacity, the overflow is allocated. The
/// gen2 figures are printed and NOT asserted on — the shared pool's trimming is time-based and
/// pressure-based, so a tight loop with a forced collection does not reproduce what a server
/// sees over minutes, and the two numbers there are within one 2 MiB array of each other in
/// either direction.</para>
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

        // More requests in flight than the shared pool holds ⇒ the overflow is fresh LOH; the
        // dedicated pool is deep enough to hand back what it was given.
        Assert.True(ownFlat < sharedFlat,
            $"expected the deeper pool to allocate less, got own={ownFlat} shared={sharedFlat}");
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

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var tasks = new Task[Concurrency];
        for (int r = 0; r < Rounds; r++)
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
            if (gen2) GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        }
        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }
}
