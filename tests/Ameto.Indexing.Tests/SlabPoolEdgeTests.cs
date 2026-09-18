using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The build pools' edges: requests no array can satisfy fail as ordinary allocation failures,
/// and cleanup after such a failure can hand back what it holds without tripping on a null.
/// </summary>
public sealed class SlabPoolEdgeTests
{
    [Fact]
    public void ARentPastTwoToThe30_FailsAsAnAllocation_NotAnIndexOutOfRange()
    {
        // int.MaxValue is past Array.MaxLength, so the allocation fails deterministically without
        // touching memory. The old Rent rounded first: 2^31 as an int is negative, slipped under
        // the oversized test, and indexed bucket 27 inside the lock.
        Assert.Throws<OutOfMemoryException>(() => IndexBuildPool.Ints.Rent(int.MaxValue));
        Assert.Throws<OutOfMemoryException>(() => IndexBuildPool.Slabs.Rent(int.MaxValue));
    }

    [Fact]
    public void AnOversizedRent_BelowTwoToThe30_IsStillAPowerOfTwo()
    {
        // The tables use Length as their capacity (mask = Length - 1), so the beyond-the-pool
        // path must still round while a power of two fits in an int.
        var t = IndexBuildPool.Ints.Rent(IndexBuildPool.Ints.MaxLength + 1);
        Assert.Equal(IndexBuildPool.Ints.MaxLength * 2, t.Length);
    }

    [Fact]
    public void ReturningNull_IsIgnored()
    {
        IndexBuildPool.TrimAll();
        IndexBuildPool.Slabs.Return(null);
        IndexBuildPool.Ints.Return(null);
        Assert.Equal(0, IndexBuildPool.PooledBytes);
    }
}
