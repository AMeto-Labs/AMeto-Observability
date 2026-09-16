using System.Diagnostics;
using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// The one rule both gen2-trimmed pools now ask — the ingest body-buffer pool and the index-build
/// pools, which used to carry a copy each, with a private gen2 callback each and two
/// unsynchronised windows.
///
/// <para>The condition these tests exist for is the third argument to the decision: WHOSE memory
/// the GC's reading describes. <c>MemoryLoadBytes</c> is measured against the container's limit
/// under a cgroup or job object, and against the whole machine without one — a bare Windows host
/// (the sandbox-kz02 install) or any container started without <c>--memory</c>. There, a
/// neighbouring process past the threshold made every gen2 empty both pools, once every thirty
/// seconds, for ever: megabytes of request buffers and multi-megabyte index tables re-allocated
/// on the large object heap because of someone else's memory, which is precisely the churn the
/// pools exist to remove. The window bounds how often that happens and cannot stop it happening
/// at all, so the reading's scope has to be part of the rule — the same test the RAM-pressure
/// loop already applies one layer up.</para>
/// </summary>
public sealed class PoolTrimPolicyTests
{
    private const long MB = 1024 * 1024;
    private const long GB = 1024 * MB;

    private static readonly long Interval =
        (long)(PoolTrimPolicy.MinTrimInterval.TotalSeconds * Stopwatch.Frequency);

    // ── The threshold half ────────────────────────────────────────────────────

    [Theory]
    [InlineData(100, 200, false)]   // comfortable
    [InlineData(199, 200, false)]   // close, but under
    [InlineData(200, 200, true)]    // at the GC's own high-load threshold
    [InlineData(400, 200, true)]    // past it
    [InlineData(400,   0, false)]   // threshold unknown — never trim on a guess
    public void ATrimNeedsTheGcsOwnHighLoadThreshold(long load, long threshold, bool expected)
        => Assert.Equal(expected, PoolTrimPolicy.ShouldTrim(
            pooledBytes: 64 * MB, memoryLoadBytes: load, highLoadThresholdBytes: threshold,
            scaleBytes: 0, readingIsOurs: true, Stopwatch.GetTimestamp(), lastTrimTimestamp: 0));

    [Fact]
    public void ATrimDoesNotFireAgainWithinItsInterval()
    {
        long first = Stopwatch.GetTimestamp();
        Assert.True(Ours(400, 200, first, lastTrim: 0));

        long oneSecondLater = first + Stopwatch.Frequency;
        Assert.False(Ours(400, 200, oneSecondLater, first));

        // Still high load, but the window has passed: trim again.
        Assert.True(Ours(400, 200, first + Interval, first));

        // The gap never turns a trim ON: below the threshold it stays false however long ago
        // the last one was.
        Assert.False(Ours(100, 200, first + Interval, first));

        static bool Ours(long load, long threshold, long now, long lastTrim) =>
            PoolTrimPolicy.ShouldTrim(64 * MB, load, threshold, scaleBytes: 0,
                                      readingIsOurs: true, now, lastTrim);
    }

    // ── Whose memory it is ────────────────────────────────────────────────────

    /// <summary>
    /// A 16 GB shared box under pressure that this process did not cause. Emptying a pool
    /// holding 2 MB cannot relieve it; all it does is throw away the working set and allocate it
    /// again — every thirty seconds, for as long as the neighbour is busy.
    /// </summary>
    [Fact]
    public void AHostWideReadingDoesNotEmptyAPoolThatCouldNotHaveRelievedIt()
    {
        long now = Stopwatch.GetTimestamp();

        Assert.False(PoolTrimPolicy.ShouldTrim(
            pooledBytes: 2 * MB, memoryLoadBytes: 15 * GB, highLoadThresholdBytes: 14 * GB,
            scaleBytes: 16 * GB, readingIsOurs: false, now, lastTrimTimestamp: 0));
    }

    /// <summary>…but a pool holding a meaningful share of the machine is worth giving back.</summary>
    [Fact]
    public void AHostWideReadingStillEmptiesAPoolBigEnoughToMatter()
    {
        long now = Stopwatch.GetTimestamp();

        Assert.True(PoolTrimPolicy.ShouldTrim(
            pooledBytes: 300 * MB, memoryLoadBytes: 15 * GB, highLoadThresholdBytes: 14 * GB,
            scaleBytes: 16 * GB, readingIsOurs: false, now, lastTrimTimestamp: 0));
    }

    /// <summary>
    /// Under a container limit the reading IS this process's business: the pressure is ours
    /// whatever we happen to be holding, and an empty pool still trims (there is nothing to
    /// lose by it).
    /// </summary>
    [Fact]
    public void AContainerScopedReadingTrimsWhateverIsHeld()
    {
        long now = Stopwatch.GetTimestamp();

        Assert.True(PoolTrimPolicy.ShouldTrim(
            pooledBytes: 0, memoryLoadBytes: 460 * MB, highLoadThresholdBytes: 460 * MB,
            scaleBytes: 512 * MB, readingIsOurs: true, now, lastTrimTimestamp: 0));
    }

    /// <summary>
    /// On a small machine one percent is a handful of megabytes, so the absolute floor is what
    /// decides — otherwise a 400 MB box would trim a pool holding 4 MB.
    /// </summary>
    [Fact]
    public void TheWorthwhileFloorHoldsOnASmallMachine()
    {
        long now = Stopwatch.GetTimestamp();

        Assert.False(PoolTrimPolicy.ShouldTrim(
            pooledBytes: PoolTrimPolicy.MinWorthwhileBytes - 1, memoryLoadBytes: 900, highLoadThresholdBytes: 900,
            scaleBytes: 400 * MB, readingIsOurs: false, now, lastTrimTimestamp: 0));

        Assert.True(PoolTrimPolicy.ShouldTrim(
            pooledBytes: PoolTrimPolicy.MinWorthwhileBytes, memoryLoadBytes: 900, highLoadThresholdBytes: 900,
            scaleBytes: 400 * MB, readingIsOurs: false, now, lastTrimTimestamp: 0));
    }

    /// <summary>An unknown scale must not be read as "nothing matters": the floor still applies.</summary>
    [Fact]
    public void AnUnknownScaleFallsBackToTheFloor()
    {
        long now = Stopwatch.GetTimestamp();

        Assert.False(PoolTrimPolicy.ShouldTrim(
            pooledBytes: 1 * MB, memoryLoadBytes: 900, highLoadThresholdBytes: 900,
            scaleBytes: 0, readingIsOurs: false, now, lastTrimTimestamp: 0));

        Assert.True(PoolTrimPolicy.ShouldTrim(
            pooledBytes: 64 * MB, memoryLoadBytes: 900, highLoadThresholdBytes: 900,
            scaleBytes: 0, readingIsOurs: false, now, lastTrimTimestamp: 0));
    }

    /// <summary>
    /// The window applies whoever the memory belongs to — a host-wide reading that IS worth
    /// acting on must still not act twice inside it.
    /// </summary>
    [Fact]
    public void TheIntervalAppliesToAHostWideTrimToo()
    {
        long first = Stopwatch.GetTimestamp();

        Assert.False(PoolTrimPolicy.ShouldTrim(300 * MB, 15 * GB, 14 * GB, 16 * GB, false, first + Interval - 1, first));
        Assert.True (PoolTrimPolicy.ShouldTrim(300 * MB, 15 * GB, 14 * GB, 16 * GB, false, first + Interval,     first));
    }

    /// <summary>
    /// The scope is detected, not configured, and this test asserts only that it answers without
    /// throwing and agrees with itself — the value depends on the host running the suite.
    /// </summary>
    [Fact]
    public void TheScopeOfTheReadingIsAnswerableOnThisHost()
        => Assert.Equal(PoolTrimPolicy.ReadingIsScopedToThisProcess, PoolTrimPolicy.ReadingIsScopedToThisProcess);
}
