using Ameto.Ingestion;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// What the payload arena's <c>madvise(MADV_NOHUGEPAGE)</c> result is taken to mean. Only Linux
/// produces most of these numbers, and CI's main job runs on Windows, so what can be decided
/// without the syscall is checked here on every platform; the Linux test in
/// <see cref="SlabArenaCommitTests"/> checks the call itself.
/// </summary>
public sealed class HugePageOptOutDecisionTests
{
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

        // End to end through Create: on Linux this is the path above over a real 100-byte block;
        // elsewhere nothing is attempted. Either way the arena does not claim the advice.
        using var tiny = SlabArena.Create(100, 100, reserve: false);
        Assert.Equal(SlabArena.NoHugePageOptOut, tiny.HugePageOptOutErrno);
        Assert.False(tiny.HugePagesDisabled);
    }
}
