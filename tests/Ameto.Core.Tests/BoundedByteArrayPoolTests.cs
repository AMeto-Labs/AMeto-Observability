using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// The pool behind <see cref="IngestBufferPool"/>. What it has that
/// <c>ArrayPool&lt;byte&gt;.Create</c> does not — and what the ingest road needed — is a TOTAL
/// byte cap and a way to read what is parked. Depth alone bounded nothing usable: the real
/// ceiling was depth x a full set of buckets, which for request bodies is depth x ~2 x the
/// largest array, and depth came from the core count of a host that may be far larger than the
/// container.
///
/// <para>Tested by constructing pools with small numbers, which is the only way these bounds can
/// be exercised at all: the process-wide pool's cap is a share of the real machine's heap limit,
/// so filling it in a test would mean allocating hundreds of megabytes.</para>
/// </summary>
public sealed class BoundedByteArrayPoolTests
{
    private const int  OneMB = 1 << 20;

    [Fact]
    public void AnArrayComesBackFromTheBucketItWasReturnedTo()
    {
        var pool = new BoundedByteArrayPool(maxLength: OneMB, maxArraysPerBucket: 4, maxPooledBytes: 8L * OneMB);

        byte[] first = pool.Rent(700_000);
        Assert.Equal(OneMB, first.Length);                 // rounded up to the bucket
        pool.Return(first);
        Assert.Equal(OneMB, pool.PooledBytes);

        Assert.Same(first, pool.Rent(700_000));
        Assert.Equal(0, pool.PooledBytes);                 // and it is no longer parked
    }

    /// <summary>
    /// THE BOUND THAT BINDS. Eight 1 MB arrays fit the depth (16) but not the 4 MB cap, so the
    /// last four returns are dropped and become ordinary garbage.
    /// </summary>
    [Fact]
    public void TheTotalByteCapDropsReturnsBeyondIt()
    {
        var pool = new BoundedByteArrayPool(maxLength: OneMB, maxArraysPerBucket: 16, maxPooledBytes: 4L * OneMB);

        var held = new byte[8][];
        for (int i = 0; i < held.Length; i++) held[i] = pool.Rent(OneMB);
        for (int i = 0; i < held.Length; i++) pool.Return(held[i]);

        Assert.Equal(4L * OneMB, pool.PooledBytes);
        Assert.True(pool.PooledBytes <= pool.MaxPooledBytes);
    }

    /// <summary>
    /// The cap is over the WHOLE pool, not per bucket — which is the case that used to escape,
    /// because a reader without a Content-Length doubles from 64 KB and leaves an array in every
    /// bucket on the way up.
    /// </summary>
    [Fact]
    public void TheCapCountsEveryBucketTogether()
    {
        var pool = new BoundedByteArrayPool(maxLength: OneMB, maxArraysPerBucket: 4, maxPooledBytes: 3L * OneMB);

        // One array in each bucket from 64 KB to 1 MB: 64K+128K+256K+512K+1M = just under 2 MB.
        for (int len = 65_536; len <= OneMB; len <<= 1) pool.Return(pool.Rent(len));
        long afterLadder = pool.PooledBytes;
        Assert.True(afterLadder < 3L * OneMB, $"{afterLadder} B parked by the ladder");

        // Two more 1 MB arrays would be within the bucket's depth; the cap takes the second.
        pool.Return(pool.Rent(OneMB));
        pool.Return(pool.Rent(OneMB));
        Assert.True(pool.PooledBytes <= 3L * OneMB, $"{pool.PooledBytes} B parked, cap 3 MB");
    }

    [Fact]
    public void DepthStillBoundsOneBucket()
    {
        var pool = new BoundedByteArrayPool(maxLength: OneMB, maxArraysPerBucket: 2, maxPooledBytes: 64L * OneMB);

        var held = new byte[4][];
        for (int i = 0; i < held.Length; i++) held[i] = pool.Rent(OneMB);
        for (int i = 0; i < held.Length; i++) pool.Return(held[i]);

        Assert.Equal(2L * OneMB, pool.PooledBytes);        // the cap had room; the depth did not
    }

    /// <summary>
    /// A body larger than the pool serves is still served — allocated at exactly the length
    /// asked for, and dropped on return. That is what an operator who raises a body limit past
    /// this pool gets: correct, just not pooled.
    /// </summary>
    [Fact]
    public void AnArrayLargerThanThePoolIsServedButNeverParked()
    {
        var pool = new BoundedByteArrayPool(maxLength: OneMB, maxArraysPerBucket: 4, maxPooledBytes: 64L * OneMB);

        byte[] big = pool.Rent(3 * OneMB);
        Assert.Equal(3 * OneMB, big.Length);               // exact, not rounded to 4 MB
        pool.Return(big);

        Assert.Equal(0, pool.PooledBytes);
    }

    [Fact]
    public void ClearReleasesEverythingParked()
    {
        var pool = new BoundedByteArrayPool(maxLength: OneMB, maxArraysPerBucket: 4, maxPooledBytes: 64L * OneMB);

        byte[] before = pool.Rent(OneMB);
        pool.Return(before);
        Assert.Equal(OneMB, pool.PooledBytes);

        pool.Clear();

        Assert.Equal(0, pool.PooledBytes);
        Assert.NotSame(before, pool.Rent(OneMB));          // the trim really let go of it
    }

    /// <summary>A null return is ignored: cleanup after a failed rent must not throw.</summary>
    [Fact]
    public void ANullReturnIsIgnored()
    {
        var pool = new BoundedByteArrayPool(maxLength: OneMB, maxArraysPerBucket: 4, maxPooledBytes: 64L * OneMB);
        pool.Return(null);
        Assert.Equal(0, pool.PooledBytes);
    }

    /// <summary>The process-wide pool takes its cap from the memory model, not from the core count.</summary>
    [Fact]
    public void TheIngestPoolsCapComesFromTheMemoryBudgets()
    {
        Assert.Equal(MemoryBudgets.Current().IngestBufferBytes, IngestBufferPool.MaxPooledTotalBytes);
        Assert.True(IngestBufferPool.MaxPooledTotalBytes <= MemoryBudgets.IngestBufferCapBytes);
        Assert.True(IngestBufferPool.PooledBytes <= IngestBufferPool.MaxPooledTotalBytes);
    }
}
