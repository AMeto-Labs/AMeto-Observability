using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// Pins the hot-tier capacity planning that StorageEngine budgets its flush RAM against.
///
/// Chunks are allocated whole (header array + full 8 MB payload arena), so a tier bounded
/// only by payload bytes has no native ceiling: at a 64 B average payload the old
/// 2,000,000-event cap let a "64 MB" tier reach 123 chunks ≈ 1.1 GB resident, while
/// StorageEngine sized its backlog against a flat 1.4 × MaxSizeBytes — off by up to 17x.
/// </summary>
public sealed class HotTierGeometryTests
{
    private const long MB = 1024 * 1024;

    [Theory]
    [InlineData(64  * MB, 8,  72  * MB)]
    [InlineData(8   * MB, 1,  9   * MB)]
    [InlineData(128 * MB, 16, 144 * MB)]
    [InlineData(256 * MB, 32, 288 * MB)]
    public void Geometry_IsDerivedFromChunkCount(long maxPayload, int expectChunks, long expectNative)
    {
        Assert.Equal(expectChunks, HotTierSegment.ChunksFor(maxPayload));
        Assert.Equal(expectNative, HotTierSegment.NativeBytesFor(maxPayload));
        Assert.Equal(expectChunks * 16_384, HotTierSegment.EventCapacityFor(maxPayload));
    }

    /// <summary>
    /// The point of the change: native memory is capped by MaxSizeBytes regardless of how
    /// small the events are. Filling a tier built from EventCapacityFor with 16-byte
    /// payloads must not exceed NativeBytesFor.
    /// </summary>
    [Fact]
    public void TinyEvents_CannotExceedTheNativeBudget()
    {
        const long maxPayload = 64 * MB;
        long budget = HotTierSegment.NativeBytesFor(maxPayload);

        using var tier = new HotTierSegment(HotTierSegment.EventCapacityFor(maxPayload), maxPayload);
        Span<byte> tiny = stackalloc byte[16];

        int written = 0;
        while (tier.TryWrite(new Ameto.Core.LogEventHeader { TimestampUtcTicks = written }, tiny))
            written++;

        Assert.True(written > 0);
        Assert.True(tier.AllocatedBytes <= budget,
            $"tier grew to {tier.AllocatedBytes / MB} MB, budget is {budget / MB} MB ({written:N0} events)");
    }

    /// <summary>
    /// The geometry invariant documented in HotTierSegment: a chunk's payload area must
    /// hold a full chunk of average-sized events, or the writer wedges below capacity and
    /// StorageEngine turns that into a flush storm.
    /// </summary>
    [Fact]
    public void PayloadArea_CoversAFullChunkOf512ByteEvents()
    {
        const long maxPayload = 64 * MB;
        using var tier = new HotTierSegment(HotTierSegment.EventCapacityFor(maxPayload), maxPayload);
        Span<byte> ev = stackalloc byte[512];

        int written = 0;
        while (tier.TryWrite(new Ameto.Core.LogEventHeader { TimestampUtcTicks = written }, ev))
            written++;

        // 64 MB / 512 B = 131,072 = exactly the derived event capacity.
        Assert.Equal(HotTierSegment.EventCapacityFor(maxPayload), written);
    }
}
