using Ameto.Indexing;
using Xunit;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The filter's bits live in <c>NativeMemory</c> and <c>Dispose</c> returns them, but the pointer
/// field is <c>readonly</c> and keeps pointing at the freed block. Every method that touches the
/// bits must therefore say so rather than read whatever now occupies them.
///
/// <para>The failure this pins is silent by construction — no null, no access violation, just a
/// bloom section built from an unrelated allocation's bytes, which the query prefilter then trusts
/// to drop whole segments unread. Measured before the guard: a disposed 4096-term filter
/// re-serialised to 5120 payload bytes of which 5120 differed from the pre-dispose blob.</para>
/// </summary>
public sealed class SegmentBloomFilterLifetimeTests
{
    [Fact]
    public void Serialise_after_dispose_throws_rather_than_reading_freed_bits()
    {
        var filter = SegmentBloomFilter.Create(1024);
        filter.Add("Wallet.API");
        _ = filter.Serialise();          // fine while it is alive
        filter.Dispose();

        Assert.Throws<ObjectDisposedException>(() => { _ = filter.Serialise(); });
    }

    [Fact]
    public void Add_after_dispose_throws()
    {
        var filter = SegmentBloomFilter.Create(1024);
        filter.Dispose();

        Assert.Throws<ObjectDisposedException>(() => filter.Add("Error"));
        Assert.Throws<ObjectDisposedException>(() => filter.Add("Error"u8));
    }

    [Fact]
    public void MightContain_after_dispose_throws()
    {
        var filter = SegmentBloomFilter.Create(1024);
        filter.Add("Error");
        filter.Dispose();

        Assert.Throws<ObjectDisposedException>(() => { _ = filter.MightContain("Error"); });
        Assert.Throws<ObjectDisposedException>(() => { _ = filter.MightContain("Error"u8); });
    }

    /// <summary>
    /// A LONG value takes the pooled-buffer branch of the char overloads, and those paths return
    /// their rental without a <c>finally</c> — so the guard has to sit AHEAD of the rent or the
    /// throw strands a buffer the pool never sees again.
    ///
    /// <para>Asserting only that an <see cref="ObjectDisposedException"/> comes out does not test
    /// that. The byte overload throws too, so a guard anywhere on the call chain satisfies it:
    /// with the two char-overload guards deleted, <c>Add</c> rents its fold buffer, <c>AddRaw</c>
    /// rents an encode buffer, the byte overload throws from underneath both, and the assertion is
    /// still green while two buffers per call leak. Measured under exactly that revert: 6 of 6 in
    /// this class passed.</para>
    ///
    /// <para>So the rent is observed instead. <c>ArrayPool&lt;T&gt;.Shared</c> parks the most
    /// recently returned array of a bucket in a THREAD-LOCAL slot that <c>Rent</c> drains first,
    /// which makes rent → return → rent hand back the same instance on one thread — and makes a
    /// rent that never came back visible as a different instance. The round trip is asserted first
    /// so that a runtime that stopped behaving that way fails HERE, saying so, rather than making
    /// the probe fail for a reason that is not the one it is named for.</para>
    /// </summary>
    [Fact]
    public void Disposed_char_overloads_throw_before_renting()
    {
        var filter = SegmentBloomFilter.Create(1024);
        filter.Dispose();

        string longValue = new('x', PooledLength);   // > 256 ⇒ the pooled branch, not the stackalloc one

        AssertNothingWasRented(() => filter.Add(longValue));
        AssertNothingWasRented(() => { _ = filter.MightContain(longValue); });
    }

    /// <summary>Length of the probe value: past the 256-char stackalloc threshold in both char
    /// overloads, and rented at exactly this size, so the probe below shares their bucket.</summary>
    private const int PooledLength = 4096;

    private static void AssertNothingWasRented(Action disposedCall)
    {
        var pool = System.Buffers.ArrayPool<char>.Shared;

        // Precondition, not the assertion under test: the pool must round-trip one instance on
        // this thread for the probe to mean anything.
        char[] first = pool.Rent(PooledLength);
        pool.Return(first);
        char[] second = pool.Rent(PooledLength);
        pool.Return(second);
        Assert.Same(first, second);

        Assert.Throws<ObjectDisposedException>(disposedCall);

        char[] after = pool.Rent(PooledLength);
        pool.Return(after);
        Assert.Same(second, after);   // the guarded call took nothing out of the bucket
    }

    /// <summary>
    /// The two counters are managed state and <c>ISegmentIndexSink</c> requires them to outlive
    /// disposal — the writer seals a group, disposes the sink, and sizes the next group from what
    /// this one measured. Guarding them alongside the bits would have broken that contract, so the
    /// exemption is pinned here rather than left to the comment on it.
    /// </summary>
    [Fact]
    public void Term_counters_stay_readable_after_dispose()
    {
        var filter = SegmentBloomFilter.Create(1024);
        filter.Add("Error");
        filter.Add("Wallet.API");
        long added    = filter.AddedTermCount;
        long capacity = filter.Capacity;
        filter.Dispose();

        Assert.Equal(added,    filter.AddedTermCount);
        Assert.Equal(capacity, filter.Capacity);
        Assert.Equal(2,        filter.AddedTermCount);
    }

    /// <summary>
    /// The accessor the review named. It reaches the same bits through
    /// <c>SegmentIndexBuilder</c>, whose <c>Dispose</c> is what frees them.
    /// </summary>
    [Fact]
    public void Builder_bloom_accessor_after_dispose_throws()
    {
        var builder = new SegmentIndexBuilder(expectedEventCount: 16);
        _ = builder.SerialisedBloomFilter;
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => { _ = builder.SerialisedBloomFilter; });
        Assert.Throws<ObjectDisposedException>(() => { _ = builder.Serialise(); });

        // The other two sections are pure managed state; they were never the hazard and are not
        // made to throw, so a probe that only wants those still works on a disposed builder.
        _ = builder.SerialisedInvertedIndex;
        _ = builder.SerialisedTrigramIndex;
        Assert.Equal(0, builder.BloomTermsAdded);
    }
}
