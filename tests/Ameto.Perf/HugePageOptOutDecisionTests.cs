using SlabArena = Ameto.Core.SlabArena;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// What the payload arena's <c>madvise(MADV_NOHUGEPAGE)</c> result is taken to mean. Only Linux
/// produces most of these numbers, and CI's main job runs on Windows, so the decision is a pure
/// function of the number and is checked here on every platform; the Linux test in
/// <see cref="SlabArenaCommitTests"/> checks the call itself.
/// </summary>
public sealed class HugePageOptOutDecisionTests
{
    /// <summary>
    /// A kernel built without transparent huge pages answers EINVAL (22) to MADV_NOHUGEPAGE: there
    /// is nothing to opt out of, so it is not a failure — and neither is "advised" nor "not
    /// attempted". A refusal for any other reason, or a libc that could not be bound, leaves huge
    /// pages possible and is. The errnos are literals, not the class's constant, so a mistyped
    /// EINVAL fails here too.
    /// </summary>
    [Theory]
    [InlineData(0,  false)]   // advised
    [InlineData(-1, false)]   // SlabArena.NoHugePageOptOut: not attempted
    [InlineData(22, false)]   // EINVAL: no THP in this kernel
    [InlineData(-2, true)]    // SlabArena.HugePageOptOutUnbound: madvise could not be called
    [InlineData(1,  true)]    // EPERM
    [InlineData(11, true)]    // EAGAIN
    [InlineData(12, true)]    // ENOMEM: part of the range was not mapped
    [InlineData(38, true)]    // ENOSYS
    public void Only_a_refusal_that_leaves_huge_pages_possible_is_a_failure(int result, bool failure)
    {
        Assert.Equal(-1, SlabArena.NoHugePageOptOut);
        Assert.Equal(-2, SlabArena.HugePageOptOutUnbound);
        Assert.Equal(failure, SlabArena.IsHugePageOptOutFailure(result));
    }

    /// <summary>
    /// An allocation that holds no whole page gets no madvise call, and must not say it was
    /// advised: the answer is "not attempted", so <see cref="SlabArena.HugePagesDisabled"/> stays
    /// false. It used to be 0. The address is never dereferenced — no whole page means the
    /// function returns before the P/Invoke — so this runs on every platform.
    /// </summary>
    [Fact]
    public void An_allocation_holding_no_whole_page_is_not_reported_as_advised()
    {
        // 4 000 bytes starting one byte past a 4 KB boundary: the next boundary is past the end.
        Assert.Equal(SlabArena.NoHugePageOptOut, SlabArena.DisableHugePages(0x1001, 4000, 4096));
        // Aligned, but one byte short of a page.
        Assert.Equal(SlabArena.NoHugePageOptOut, SlabArena.DisableHugePages(0x10000, 4095, 4096));
        // A 16 KB page (some arm64 kernels) over 12 KB that a 4 KB page would have found.
        Assert.Equal(SlabArena.NoHugePageOptOut, SlabArena.DisableHugePages(0x4000, 12288, 16384));
        Assert.False(SlabArena.IsHugePageOptOutFailure(SlabArena.NoHugePageOptOut));

        // End to end through Create: on Linux this is the path above over a real 100-byte block;
        // elsewhere nothing is attempted. Either way the arena does not claim the advice.
        using var tiny = SlabArena.Create(100, 100, reserve: false);
        Assert.Equal(SlabArena.NoHugePageOptOut, tiny.HugePageOptOutErrno);
        Assert.False(tiny.HugePagesDisabled);
    }
}
