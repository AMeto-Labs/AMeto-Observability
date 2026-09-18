using Ameto.Ingestion;
using Microsoft.Extensions.Logging;
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
    /// The startup message follows the decision: nothing for a kernel without THP, nothing for an
    /// arena that was advised or never attempted, one Information line naming the result
    /// otherwise — and a host without a logger does not throw.
    /// </summary>
    [Fact]
    public void The_startup_message_is_logged_only_when_the_opt_out_failed()
    {
        foreach (int silent in new[] { 0, -1, 22 })
        {
            var quiet = new CapturingLogger();
            IngestionServiceExtensions.ReportHugePageOptOut(quiet, silent);
            Assert.True(quiet.Entries.Count == 0, $"madvise result {silent} was logged: {string.Join(" | ", quiet.Entries)}");
        }

        foreach (int failed in new[] { 12, -2 })
        {
            var log = new CapturingLogger();
            IngestionServiceExtensions.ReportHugePageOptOut(log, failed);
            var entry = Assert.Single(log.Entries);
            Assert.Equal(LogLevel.Information, entry.Level);
            Assert.Equal(failed, entry.Result);
        }

        IngestionServiceExtensions.ReportHugePageOptOut(null, 12);
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

    private sealed record Entry(LogLevel Level, object? Result)
    {
        public override string ToString() => $"{Level} {Result}";
    }

    private sealed class CapturingLogger : ILogger
    {
        public readonly List<Entry> Entries = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                Func<TState, Exception?, string> formatter)
        {
            object? result = null;
            if (state is IReadOnlyList<KeyValuePair<string, object?>> kvs)
                foreach (var kv in kvs)
                    if (kv.Key == "Result") result = kv.Value;
            Entries.Add(new Entry(logLevel, result));
        }
    }
}
