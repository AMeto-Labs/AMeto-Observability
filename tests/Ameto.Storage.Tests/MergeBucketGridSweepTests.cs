using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Sweeps the merge planner's sealing predicate over a FULL BUCKET PERIOD of synthetic "now"
/// values, for every shape the anchored tests actually stage.
///
/// <para>The tests these anchors serve were clock-dependent: they placed data at
/// <c>UtcNow - 5d</c> and needed it to land in a SEALED bucket, which is true only for part of
/// each 7-day period. A single green run cannot distinguish "fixed" from "run at a lucky
/// moment" — the bug reproduced roughly two days in seven, so a suite that runs once has a
/// ~71 % chance of agreeing with a broken anchor. That is what this file exists to replace:
/// <c>now</c> is a parameter here, swept across the whole period, so the guarantee is checked
/// at every position rather than at the one the clock happened to supply.</para>
///
/// <para>There is no clock seam in <see cref="StorageEngine"/> — <c>SelectMergeBatch</c> reads
/// <c>DateTimeOffset.UtcNow</c> directly — so the sweep evaluates the predicate rather than
/// driving a real engine. <see cref="SealedFanout"/> mirrors the planner's private
/// <c>MinSourcesFor</c> (StorageEngine.cs:1177-1183) line for line, and
/// <see cref="TheSweepHasTeeth_TheOldIdiomStillFails"/> proves the sweep can tell the two apart
/// by running the OLD idiom through it and requiring it to fail.</para>
/// </summary>
public sealed class MergeBucketGridSweepTests
{
    // ── The planner's predicate, mirrored ─────────────────────────────────────

    /// <summary>StorageEngine.MergeSealGraceTicks — private, so restated here.</summary>
    private static long SealGrace(long width) => Math.Min(48L * TimeSpan.TicksPerHour, width);

    /// <summary>
    /// StorageEngine.MinSourcesFor, verbatim: a bucket whose window ended at least a grace ago
    /// merges from 2 sources, otherwise it needs the full fanout of 8.
    /// </summary>
    private static int SourcesNeeded(long bucketStart, long width, long now) =>
        now - (bucketStart + width) >= SealGrace(width)
            ? StorageEngine.MergeSealedMinSources
            : StorageEngine.MergeMinSources;

    private static long BucketOf(long ticks, long width) => ticks / width * width;

    // ── The shapes the anchored tests stage ───────────────────────────────────

    /// <summary>
    /// One staged test: which level it writes, how far back its anchor reaches, and the offset
    /// of every source segment from that anchor. Offsets are the MAXIMUM timestamp each flush
    /// produces, because that is what the planner buckets on
    /// (<c>s.MaxTimestampTicks / width * width</c>).
    /// </summary>
    private sealed record Shape(string Test, LogLevel Level, int BucketsSpanned, long[] SourceMaxOffsets);

    private static readonly long H = TimeSpan.TicksPerHour;
    private static readonly long D = TimeSpan.TicksPerDay;
    private static readonly long W = MergeBucketGrid.BucketWidth(LogLevel.Information);

    /// <summary>
    /// Every anchored test, with its real staging. Offsets include the intra-flush spread: a
    /// round of N events runs to <c>base + n·1 ms</c>, so the flush's MAX timestamp is a few
    /// seconds past its base, and it is the max that picks the bucket.
    /// </summary>
    public static TheoryData<string> ShapeNames()
    {
        var d = new TheoryData<string>();
        foreach (var s in Shapes) d.Add(s.Test);
        return d;
    }

    private static readonly Shape[] Shapes =
    [
        // SegmentMergeTests.Merge_ConsolidatesSparseSettledWindows — 3 rounds, 50 events each.
        new("Merge_ConsolidatesSparseSettledWindows", LogLevel.Information, 1,
            [0 * H + 50 * TimeSpan.TicksPerMillisecond,
             1 * H + 1050 * TimeSpan.TicksPerMillisecond,
             2 * H + 2050 * TimeSpan.TicksPerMillisecond]),

        // SegmentMergeTests.Merge_MergesDenseSegments_Losslessly — 2500 / 2500 / 9000 events.
        new("Merge_MergesDenseSegments_Losslessly", LogLevel.Information, 1,
            [0 * H + 2500 * TimeSpan.TicksPerMillisecond,
             1 * H + 3500 * TimeSpan.TicksPerMillisecond,
             2 * H + 11000 * TimeSpan.TicksPerMillisecond]),

        // WalSegmentIdTests — both tests, 4 rounds of 50.
        new("AMergeNeverTakesAnIdOutOfTheLiveWalsBlock", LogLevel.Information, 1,
            [0 * H, 1 * H, 2 * H, 3 * H]),
        new("EventsLivingOnlyInTheWalSurviveACrashAfterAMerge", LogLevel.Information, 1,
            [0 * H, 1 * H, 2 * H, 3 * H]),

        // StreamingMergeCrashSafetyTests — both tests, 4 rounds of 60.
        new("UnreadableSource_AbortsTheBatch_WithoutTouchingAnything", LogLevel.Information, 1,
            [0 * H, 1 * H, 2 * H, 3 * H]),
        new("CorruptBlockMidStream_AbortsTheMerge_AndLeavesNoManifest", LogLevel.Information, 1,
            [0 * H, 1 * H, 2 * H, 3 * H]),

        // SegmentMergeTests.Merge_NeverSpansMoreThanOneBucket — 9 daily rounds over 2 buckets.
        new("Merge_NeverSpansMoreThanOneBucket", LogLevel.Information, 2,
            [0 * D, 1 * D, 2 * D, 3 * D, 4 * D, 5 * D, 6 * D, 7 * D, 8 * D]),

        // SegmentMergeTests.Merge_RetriesNextWindow_AfterAnchorSkip — 2 buckets, 2 sources each;
        // the last two sit one full width along, in the next window on the grid.
        new("Merge_RetriesNextWindow_AfterAnchorSkip", LogLevel.Information, 2,
            [1 * H, 2 * H, W + 1 * H, W + 2 * H]),
    ];

    private static long[] OffsetsOf(Shape s) => s.SourceMaxOffsets;

    // ── The sweep ─────────────────────────────────────────────────────────────

    /// <summary>
    /// "Now" positions covering a full bucket period: both ends, the midpoint, the grace
    /// boundary either side, a fine regular comb, and a seeded pseudo-random scatter. The
    /// predicate is modular in <c>now / width</c>, so one period is the whole space — but the
    /// comb is also replayed across several periods and across a ±2-year absolute range, which
    /// is what would catch an anchor that drifted with the calendar rather than the grid.
    /// </summary>
    private static IEnumerable<long> NowSweep(long width)
    {
        long grace = SealGrace(width);
        long baseNow = DateTime.UtcNow.Ticks;

        var offsets = new List<long>
        {
            0, 1, width - 1, width / 2,
            grace, grace - 1, grace + 1,
            width - grace, width - grace - 1, width - grace + 1,
        };
        for (int i = 0; i < 1000; i++) offsets.Add(width * i / 1000);        // regular comb
        var rng = new Random(20260807);
        for (int i = 0; i < 2000; i++) offsets.Add((long)(rng.NextDouble() * width));

        // Several periods, and a wide absolute range, so nothing can pass by sitting on one
        // convenient week of the calendar.
        long[] eras =
        [
            BucketOf(baseNow, width),
            BucketOf(baseNow, width) - 104 * 7 * D,
            BucketOf(baseNow, width) + 104 * 7 * D,
            BucketOf(baseNow, width) + width,
            BucketOf(baseNow, width) - width,
        ];

        foreach (long era in eras)
            foreach (long o in offsets)
            {
                long now = era + o;
                if (now > 0) yield return now;
            }
    }

    /// <summary>
    /// THE CLAIM, machine-checked: for every staged shape and every instant, every bucket the
    /// shape puts two or more sources into is SEALED, so the 2-source threshold applies and the
    /// merge the test is waiting for actually fires.
    /// </summary>
    [Theory]
    [MemberData(nameof(ShapeNames))]
    public void EverySealedAnchorIsSealedAtEveryInstant(string testName)
    {
        var shape = Shapes.Single(s => s.Test == testName);
        long width = MergeBucketGrid.BucketWidth(shape.Level);
        var offsets = OffsetsOf(shape);

        int instants = 0;
        foreach (long now in NowSweep(width))
        {
            instants++;
            long anchor = MergeBucketGrid.SealedBucketStartAt(now, shape.Level, shape.BucketsSpanned);

            // Group the sources exactly as SelectMergeBatch does: by (level, aligned bucket).
            var perBucket = new Dictionary<long, int>();
            foreach (long o in offsets)
            {
                long key = BucketOf(anchor + o, width);
                perBucket[key] = perBucket.GetValueOrDefault(key) + 1;
            }

            // The shape must occupy exactly the buckets it claims to, and no more: a source
            // spilling into a third bucket would silently change what the test merges.
            Assert.True(perBucket.Count == shape.BucketsSpanned,
                $"{testName}: sources spread over {perBucket.Count} buckets, expected {shape.BucketsSpanned} " +
                $"(now = {new DateTime(now, DateTimeKind.Utc):O})");

            foreach ((long bucketStart, int sources) in perBucket)
            {
                Assert.True(sources >= StorageEngine.MergeSealedMinSources,
                    $"{testName}: bucket holds {sources} source(s), below the sealed floor");

                Assert.Equal(StorageEngine.MergeSealedMinSources, SourcesNeeded(bucketStart, width, now));

                // Stronger, and the reason the helper backs off a whole width rather than
                // reading the private grace: the margin dominates ANY grace the planner can
                // produce, since MergeSealGraceTicks is capped at the width.
                Assert.True(now - (bucketStart + width) >= width,
                    $"{testName}: margin {(now - (bucketStart + width)) / (double)width:F3} widths — " +
                    "no longer independent of the grace constant");
            }
        }
        Assert.True(instants > 15_000, $"sweep only visited {instants} instants");
    }

    /// <summary>
    /// The anchors must also stay INSIDE the level's TTL at every instant. A sealed bucket that
    /// has expired is a different failure with the same symptom — the retention sweep deletes
    /// the sources and the merge finds nothing — and backing off further to be "safely sealed"
    /// is exactly how a test walks into it.
    /// </summary>
    [Theory]
    [MemberData(nameof(ShapeNames))]
    public void NoAnchorIsEverOlderThanItsOwnTtl(string testName)
    {
        var shape = Shapes.Single(s => s.Test == testName);
        long width = MergeBucketGrid.BucketWidth(shape.Level);
        long ttl = RetentionPolicy.Default.GetTtl(shape.Level).Ticks;
        long newest = OffsetsOf(shape).Max();

        foreach (long now in NowSweep(width))
        {
            long anchor = MergeBucketGrid.SealedBucketStartAt(now, shape.Level, shape.BucketsSpanned);
            // Expiry is MaxTimestamp + Ttl(MinLevel); the newest source is the last to go.
            Assert.True(now - (anchor + newest) < ttl,
                $"{testName}: newest source is {(now - anchor - newest) / (double)D:F1} d old, past a {ttl / (double)D:F0} d TTL");
        }
    }

    /// <summary>
    /// The OPEN side of the line, so the two tests that assert a REFUSAL keep their meaning:
    /// data written at now is never sealed, at any instant.
    /// </summary>
    [Fact]
    public void TheOpenAnchorIsOpenAtEveryInstant()
    {
        foreach (LogLevel level in Enum.GetValues<LogLevel>())
        {
            long width = MergeBucketGrid.BucketWidth(level);
            foreach (long now in NowSweep(width))
            {
                long start = MergeBucketGrid.OpenBucketStartAt(now, level);
                Assert.Equal(StorageEngine.MergeMinSources, SourcesNeeded(start, width, now));
            }
        }
    }

    /// <summary>
    /// The helper is level-generic, so the guarantee has to hold for every level's width — not
    /// just Information's 7 days. Debug's TTL is 3 days, giving a 6 h bucket, where the grace is
    /// no longer 48 h but the width itself; that is the case where "back off one width" is
    /// exactly tight rather than comfortable, and the predicate uses >=, so it holds.
    /// </summary>
    [Theory]
    [InlineData(LogLevel.Verbose)]
    [InlineData(LogLevel.Debug)]
    [InlineData(LogLevel.Information)]
    [InlineData(LogLevel.Warning)]
    [InlineData(LogLevel.Error)]
    [InlineData(LogLevel.Fatal)]
    public void TheHelperSealsForEveryLevelsWidth(LogLevel level)
    {
        long width = MergeBucketGrid.BucketWidth(level);
        for (int spanned = 1; spanned <= 3; spanned++)
            foreach (long now in NowSweep(width))
            {
                long anchor = MergeBucketGrid.SealedBucketStartAt(now, level, spanned);
                for (int b = 0; b < spanned; b++)
                    Assert.Equal(StorageEngine.MergeSealedMinSources,
                                 SourcesNeeded(anchor + b * width, width, now));
            }
    }

    /// <summary>
    /// THE SWEEP HAS TEETH. A sweep that passes on the fixed anchors proves nothing unless it
    /// FAILS on the broken ones, so this runs the old <c>UtcNow - 5d</c> idiom through the same
    /// machinery and requires a real failure window — and pins its size, which is the ~2-days-in-7
    /// the bug was reported at.
    /// </summary>
    [Fact]
    public void TheSweepHasTeeth_TheOldIdiomStillFails()
    {
        long width = MergeBucketGrid.BucketWidth(LogLevel.Information);
        long[] offsets = [0 * H, 1 * H, 2 * H, 3 * H];   // the 4-source staging, verbatim

        // A UNIFORM comb over one period, so the fraction below is a calendar probability rather
        // than an artefact of where the sweep chose to sample.
        int total = 0, wouldFail = 0;
        long era = BucketOf(DateTime.UtcNow.Ticks, width);
        for (int i = 0; i < 100_000; i++)
        {
            long now = era + width * i / 100_000;
            total++;
            long anchor = now - 5 * D;                    // the idiom the fix removed
            bool merges = true;
            var perBucket = new Dictionary<long, int>();
            foreach (long o in offsets)
            {
                long key = BucketOf(anchor + o, width);
                perBucket[key] = perBucket.GetValueOrDefault(key) + 1;
            }
            foreach ((long start, int sources) in perBucket)
                if (sources < SourcesNeeded(start, width, now)) merges = false;
            if (!merges) wouldFail++;
        }

        double fraction = wouldFail / (double)total;
        Assert.True(wouldFail > 0, "the sweep cannot detect the original bug — it proves nothing");

        // The window is TWO disjoint regions, not one, which is why the failure rate is double
        // the "~2 days in 7" the defect was reported at. Writing now = k·7d + r:
        //   r ∈ [0, 48h)      — sources land in the PREVIOUS bucket, but it is still inside the
        //                       48 h seal grace, so the fanout is 8 and 4 sources do not merge;
        //   r ∈ [5d - 3h, 5d) — the 3 h staging straddles the boundary, so the newest sources
        //                       fall into the open current bucket;
        //   r ∈ [5d, 7d)      — UtcNow - 5d has caught up into the current bucket, still open.
        // Total 4 d 3 h of every 7 d = 0.589. The reported 29 % counted only the last region.
        Assert.InRange(fraction, 0.58, 0.60);
    }

    /// <summary>
    /// The <c>*At</c> overloads the sweep drives must be the same code the tests call, or the
    /// sweep is checking a copy of the helper rather than the helper.
    /// </summary>
    [Fact]
    public void TheSweptOverloadIsTheOneTheTestsUse()
    {
        long now = DateTime.UtcNow.Ticks;
        foreach (LogLevel level in Enum.GetValues<LogLevel>())
        {
            long width = MergeBucketGrid.BucketWidth(level);
            // Same bucket ⇒ same answer. Recomputed either side of the call to stay immune to a
            // period boundary crossing mid-test.
            if (BucketOf(now, width) != BucketOf(DateTime.UtcNow.Ticks, width)) continue;
            Assert.Equal(MergeBucketGrid.SealedBucketStartAt(now, level), MergeBucketGrid.SealedBucketStart(level));
            Assert.Equal(MergeBucketGrid.OpenBucketStartAt(now, level), MergeBucketGrid.OpenBucketStart(level));
        }
    }
}
