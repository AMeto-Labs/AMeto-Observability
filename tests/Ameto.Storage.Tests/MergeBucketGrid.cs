using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Places test data on the merge planner's BUCKET GRID instead of relative to the wall clock.
///
/// <para>The planner buckets a segment by <c>MaxTimestampTicks / width * width</c> — an ABSOLUTE
/// grid anchored at tick zero, not a window measured back from now. That is deliberate and
/// correct: it is what makes a bucket's identity stable across restarts. The consequence for
/// tests is that "five days ago" is NOT a fixed position on the grid. Information's 90-day TTL
/// gives a 7-day bucket, so whether <c>UtcNow - 5d</c> falls in the currently OPEN bucket or in
/// an already SEALED one depends on where UtcNow happens to sit inside its own bucket.</para>
///
/// <para>That distinction decides the merge threshold: an open bucket needs
/// <see cref="StorageEngine.MergeMinSources"/> (8) sources, a sealed one only
/// <see cref="StorageEngine.MergeSealedMinSources"/> (2). A test that stages 3 segments "five
/// days back" and expects them to merge therefore passes or fails BY TIME OF DAY — roughly two
/// days in every seven. Anchoring to the grid removes the clock from the test entirely without
/// changing what the test proves.</para>
/// </summary>
internal static class MergeBucketGrid
{
    /// <summary>
    /// The planner's bucket width for <paramref name="level"/>, derived from the SAME retention
    /// policy the engine will consult. Never hardcode 7 days: the width is
    /// <c>Ttl(level) / 12</c> snapped to an aligned ladder, so it moves with the policy.
    /// </summary>
    internal static long BucketWidth(LogLevel level, RetentionPolicy? policy = null) =>
        StorageEngine.MergeBucketTicks((policy ?? RetentionPolicy.Default).GetTtl(level));

    /// <summary>
    /// Start of the bucket <paramref name="level"/>'s data would land in if it were written NOW.
    /// This bucket is always OPEN — its window has not ended, so a merge inside it needs the full
    /// <see cref="StorageEngine.MergeMinSources"/> fanout.
    /// </summary>
    internal static long OpenBucketStart(LogLevel level, RetentionPolicy? policy = null)
    {
        long width = BucketWidth(level, policy);
        return DateTime.UtcNow.Ticks / width * width;
    }

    /// <summary>
    /// Start of the oldest of <paramref name="bucketsSpanned"/> CONSECUTIVE buckets, every one of
    /// which is guaranteed already SEALED — so a merge inside any of them needs only
    /// <see cref="StorageEngine.MergeSealedMinSources"/> sources.
    ///
    /// <para>Sealing is not simply "the window is in the past": the planner also waits a grace of
    /// <c>min(48 h, width)</c> past the window's end before dropping the fanout. Rather than
    /// depend on that private constant, this backs off a whole extra bucket width, which
    /// dominates any grace the planner can apply (the grace is capped AT the width). The newest
    /// bucket returned therefore ends at least one full width ago, and the margin holds at every
    /// instant of every day.</para>
    ///
    /// <para>It backs off no FURTHER than that on purpose. Burying data far in the past would
    /// make the tests pass for a second reason — distance — and stop exercising the boundary they
    /// are about; this returns the closest position to now that is provably sealed.</para>
    /// </summary>
    internal static long SealedBucketStart(LogLevel level, int bucketsSpanned = 1, RetentionPolicy? policy = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bucketsSpanned, 1);

        long width = BucketWidth(level, policy);
        long nowBucketStart = DateTime.UtcNow.Ticks / width * width;

        // The newest sealed bucket starts at nowBucketStart - 2*width: its window ends at
        // nowBucketStart - width, i.e. at least one full width before now. Older buckets stack
        // backwards from there.
        return nowBucketStart - (long)(bucketsSpanned + 1) * width;
    }
}
