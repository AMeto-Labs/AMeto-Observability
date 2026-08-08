using Ameto.Indexing;
using Xunit;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The filter's size against what it was asked for. Sizing is the only thing about a bloom filter
/// that cannot be corrected after the fact — the bits are allocated before the first term arrives
/// and there is no resize — so the bounds on the forecast are the whole safety story.
/// </summary>
public sealed class SegmentBloomFilterSizingTests
{
    private const long MaxFilterBytes = 16L * 1024 * 1024;

    /// <summary>
    /// The design point, over four orders of magnitude: ~10 bits a term, which is ~1 % false
    /// positives. Below it the filter saturates and the query's phase-1 gate stops rejecting;
    /// above it the extra bytes are written to every segment and bought nothing.
    ///
    /// <para>The upper bound is 10.6 rather than 10 because bits round up to a whole 512-bit
    /// block — at most 511 spare bits, which on the smallest case here is half a bit a term and
    /// on the largest is nothing at all.</para>
    /// </summary>
    [Theory]
    [InlineData(1_000)]
    [InlineData(100_000)]
    [InlineData(1_000_000)]
    [InlineData(10_000_000)]
    public void AFilterIsSizedAtTenBitsPerTerm(long terms)
    {
        using var filter = SegmentBloomFilter.Create(terms);
        long bits = (filter.Serialise().Length - 8L) * 8;

        Assert.InRange(bits / (double)terms, 10.0, 10.6);
    }

    /// <summary>
    /// The ceiling, at the size the writer's own worst case asks for.
    ///
    /// <para>2 097 152 events is what <c>SegmentWriter.EnsureSink</c> forecasts for one 64 MB
    /// group when the row-cost floor (32 B) is the divisor and the source's remaining-event hint
    /// does not bind — which on the merge path it does not, because the hint there is the sum of
    /// every source's events. At the fallback 64 terms/event that is 134 M terms, and without a
    /// ceiling the filter is 160 MiB of native memory for ONE group, copied into an equally large
    /// managed array at <c>Serialise</c> and written to the file.</para>
    /// </summary>
    [Fact]
    public void TheWritersWorstCaseForecastCannotAllocateAnUnboundedFilter()
    {
        const long Events = 2_097_152;
        const long Terms  = Events * 64;

        using var filter = SegmentBloomFilter.Create(Terms);
        long bytes = filter.Serialise().Length - 8L;

        Assert.True(bytes <= MaxFilterBytes,
            $"the forecast allocated {bytes / 1048576.0:F1} MB for one group — the filter ceiling is not holding");

        // Capped, not merely clamped to something arbitrary: it should still be AT the ceiling,
        // because spending the whole allowance is the best a capped filter can do for its terms.
        Assert.True(bytes > MaxFilterBytes - 1024,
            $"the filter came back at {bytes / 1048576.0:F1} MB — well under the ceiling it was entitled to");
    }

    /// <summary>
    /// A capped filter must not go on claiming the capacity it was asked for. Capacity is what a
    /// reader divides by to say how loaded a filter is, so a filter that kept the request would
    /// make every bits-per-term reading taken off a file too optimistic — in exactly the case
    /// where the truth matters.
    /// </summary>
    [Fact]
    public void ACappedFilterReportsTheCapacityItCanActuallyHold()
    {
        var  serialised = SegmentBloomFilter.Create(MaxFilterBytes * 8).Serialise();
        uint capacity   = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(serialised.AsSpan(4))
                        & 0x7FFF_FFFFu;

        Assert.True(capacity <= MaxFilterBytes * 8 / 10,
            $"capacity {capacity:N0} exceeds the {MaxFilterBytes * 8 / 10:N0} terms the capped bits can carry");
    }

    /// <summary>
    /// Capping trades bits for terms; it must never trade away correctness. A bloom filter's one
    /// hard guarantee is no false negatives, and that has to survive the ceiling — the query path
    /// drops a whole group on a filter's "no".
    /// </summary>
    [Fact]
    public void ACappedFilterStillNeverAnswersNoToSomethingItHolds()
    {
        using var filter = SegmentBloomFilter.Create(MaxFilterBytes * 8);

        for (int i = 0; i < 20_000; i++) filter.Add($"wallet-{i:D9}");
        for (int i = 0; i < 20_000; i++)
            Assert.True(filter.MightContain($"wallet-{i:D9}"), $"false negative on wallet-{i:D9}");
    }

    /// <summary>
    /// Terms are counted on the way in, which is what lets the writer size the next group from the
    /// last one instead of assuming. Every overload funnels into the byte one, so all four forms
    /// have to land in the same counter.
    /// </summary>
    [Fact]
    public void EveryAddOverloadIsCounted()
    {
        using var filter = SegmentBloomFilter.Create(64);
        Assert.Equal(0, filter.AddedTermCount);

        filter.Add("a string");
        filter.Add((ReadOnlySpan<char>)"a char span");
        filter.Add("some bytes"u8);
        filter.Add(ReadOnlySpan<char>.Empty);

        Assert.Equal(4, filter.AddedTermCount);
    }
}
