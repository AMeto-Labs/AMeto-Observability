using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The section writer owns exactly one pooled slab at a time, and hands it back exactly once —
/// including when growing it fails half way.
/// </summary>
public sealed class StreamSectionWriterTests
{
    [Fact]
    public void AFailedGrow_LeavesTheWriterOwningItsSlab_SoDisposeReturnsItOnce()
    {
        IndexBuildPool.TrimAll();
        var w = new StreamSectionWriter(new MemoryStream());

        // A hint no array can satisfy: the bigger rental throws, as it does under out-of-memory
        // on the hard-limited stand when a posting list outgrows the 1 MB slab.
        Assert.ThrowsAny<Exception>(() => { w.GetSpan(int.MaxValue); });
        w.Dispose();

        // Returned before the failed rent AND again by Dispose, the one slab would be parked
        // twice — and handed to two renters, who would write their sections into one array.
        Assert.Equal(IndexBuildPool.SlabBytes, IndexBuildPool.Slabs.PooledBytes);
        var a = IndexBuildPool.Slabs.Rent(IndexBuildPool.SlabBytes);
        var b = IndexBuildPool.Slabs.Rent(IndexBuildPool.SlabBytes);
        try
        {
            Assert.NotSame(a, b);
        }
        finally
        {
            IndexBuildPool.Slabs.Return(a);
            if (!ReferenceEquals(a, b)) IndexBuildPool.Slabs.Return(b);
            IndexBuildPool.TrimAll();
        }
    }

    [Fact]
    public void AGrow_ThatSucceeds_SwapsTheSlab_AndStillReturnsOnlyOne()
    {
        IndexBuildPool.TrimAll();
        var ms = new MemoryStream();
        using (var w = new StreamSectionWriter(ms))
        {
            var span = w.GetSpan(IndexBuildPool.SlabBytes + 1);   // needs the 2 MB bucket (beyond the pool)
            Assert.True(span.Length > IndexBuildPool.SlabBytes);
            span[0] = 7;
            w.Advance(1);
        }
        Assert.Equal(1, ms.Length);
        Assert.Equal(IndexBuildPool.SlabBytes, IndexBuildPool.Slabs.PooledBytes);   // the 1 MB slab came back; the oversized one is dropped
        IndexBuildPool.TrimAll();
    }
}
