using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// The run planner on its own, against synthetic catalog metadata — which is all it ever reads.
/// The engine-level probes measure what a WORKLOAD converges to; these pin the three decisions
/// the planner makes, at the sizes that make each one visible, without writing a byte.
///
/// <para>The end-to-end tests cannot separate the two candidate sources any more: once the
/// contiguous planner is exhausted the tier fallback merges the same files, so a bucket compacted
/// to exhaustion reaches the same terminal state whether the contiguous run stops at an
/// unmergeable neighbour or steps over it. That difference is still real — it decides whether the
/// FIRST merge overlaps — and this is where it stays observable.</para>
/// </summary>
public sealed class MergeRunPlannerTests
{
    private const long Target  = 512L * 1024 * 1024;
    private const long Maximal = Target / 2;

    /// <summary>A catalog entry with nothing filled in but the two fields the planner reads.</summary>
    private static SegmentInfo Seg(int ordinal, long payloadBytes) => new()
    {
        Id                = new SegmentId((ulong)ordinal),
        NodeId            = new NodeId(0),
        FilePath          = $"seg-{ordinal}",
        MinTimestampTicks = ordinal * TimeSpan.TicksPerHour,
        MaxTimestampTicks = ordinal * TimeSpan.TicksPerHour + TimeSpan.TicksPerMinute,
        EventCount        = 1,
        MinLevel          = LogLevel.Information,
        CompressedBytes   = payloadBytes,
        UncompressedBytes = payloadBytes,
    };

    private static List<SegmentInfo> Bucket(params long[] payloads)
    {
        var list = new List<SegmentInfo>(payloads.Length);
        for (int i = 0; i < payloads.Length; i++) list.Add(Seg(i, payloads[i]));
        return list;
    }

    // ── Contiguity ────────────────────────────────────────────────────────────

    /// <summary>
    /// THE ORIGINAL FINDING. The span guard <c>continue</c>d past a source it could not take, so
    /// the "contiguous oldest-first run" was not contiguous and the merged file spanned right
    /// across the skipped source — every query into that window then opened both.
    ///
    /// <para>A run STOPS at the first file it cannot take. With a 20× outlier at index 2 the run
    /// from index 0 is exactly {0,1}: it may not reach {3,4} however well those would fit.</para>
    /// </summary>
    [Fact]
    public void AContiguousRunStopsAtTheFirstFileItCannotTake()
    {
        var run = StorageEngine.SelectMergeRun(
            Bucket(1000, 1000, 20_000, 1000, 1000),
            StorageEngine.MergeSealedMinSources, Target, Maximal);

        Assert.NotNull(run);
        Assert.Equal([0ul, 1ul], run!.Select(s => s.Id.Value).ToArray());
    }

    /// <summary>
    /// And the walk does not advance past the stop: a run that ends at index j says nothing about
    /// the starts inside it. Here the file at 0 is too small to sit beside either of the others,
    /// so the only legal run begins at 1 — which a planner that skipped ahead to j would miss.
    /// This is why the scan is O(n²) and why that is not a bug to optimise away.
    /// </summary>
    [Fact]
    public void EveryStartIsTriedEvenAfterARunHasBeenCutShort()
    {
        var run = StorageEngine.SelectMergeRun(
            Bucket(100, 100_000, 100_000),
            StorageEngine.MergeSealedMinSources, Target, Maximal);

        Assert.NotNull(run);
        Assert.Equal([1ul, 2ul], run!.Select(s => s.Id.Value).ToArray());
    }

    // ── The three conditions ──────────────────────────────────────────────────

    /// <summary>
    /// GROWTH. A run has to add up to real progress up the size ladder, or a bucket's collapsed
    /// file is re-absorbed for every trickle that reaches size/ratio. Three files at the ratio
    /// bound still fail it; four clear it.
    /// </summary>
    [Fact]
    public void AStragglerIsNeverALegalPartnerForAFileItCannotGrow()
    {
        Assert.Null(StorageEngine.SelectMergeRun(
            Bucket(1_000_000, 300_000), StorageEngine.MergeSealedMinSources, Target, Maximal));

        var run = StorageEngine.SelectMergeRun(
            Bucket(1_000_000, 300_000, 300_000), StorageEngine.MergeSealedMinSources, Target, Maximal);
        Assert.Equal(3, run?.Count);
    }

    /// <summary>
    /// FANOUT, and its fallback. An open bucket needs <see cref="StorageEngine.MergeMinSources"/>
    /// files — unless the run already fills the target, in which case the file it produces is
    /// maximal and leaves the candidate set for good, so it is the last rewrite those bytes will
    /// ever get. Testing the count AFTER the payload budget had truncated the batch is what once
    /// stalled an open bucket at 40 segments and 0 merges.
    /// </summary>
    [Fact]
    public void AMaximalOutputIsWorthMergingHoweverFewSourcesItTook()
    {
        long half = Maximal / 2 + 1;
        Assert.Null(StorageEngine.SelectMergeRun(
            Bucket(1000, 1000, 1000), StorageEngine.MergeMinSources, Target, Maximal));

        var run = StorageEngine.SelectMergeRun(
            Bucket(half, half), StorageEngine.MergeMinSources, Target, Maximal);
        Assert.Equal(2, run?.Count);
    }

    // ── The tier fallback ─────────────────────────────────────────────────────

    /// <summary>
    /// THE BLOCKER BOTH REVIEWS FOUND. Contiguity and the size ratio together left a file whose
    /// time-neighbours are all more than the ratio away in size with no partner at all — it could
    /// not even reach a file of its own exact size, because the run would have to step over the
    /// neighbour. Alternating flush volumes build that shape by themselves, and the catalog then
    /// grew one file per flush forever, in sealed buckets as well as open ones.
    ///
    /// <para>Grouped by size tier, the three 1 KB files are a run whatever sits between them.</para>
    /// </summary>
    [Fact]
    public void SameSizedFilesFindEachOtherAcrossATimeNeighbourNeitherCanTake()
    {
        var bucket = Bucket(1000, 400_000, 1000, 400_000, 1000);
        Assert.Null(StorageEngine.SelectMergeRun(bucket, StorageEngine.MergeSealedMinSources, Target, Maximal));

        var run = StorageEngine.SelectMergeTierRun(bucket, StorageEngine.MergeSealedMinSources, Target, Maximal);
        Assert.NotNull(run);
        Assert.Equal([0ul, 2ul, 4ul], run!.Select(s => s.Id.Value).ToArray());
    }

    /// <summary>
    /// The fallback is a fallback. It declines a bucket whose files are all one tier — there the
    /// contiguous planner has already seen the identical list and its answer is the one that does
    /// not overlap — and it takes the SMALLEST tier first, which is both the cheapest progress per
    /// file removed and what keeps a straggler ladder away from the bucket's collapsed file.
    /// </summary>
    [Fact]
    public void TheFallbackDeclinesASingleTierAndOtherwiseTakesTheSmallest()
    {
        Assert.Null(StorageEngine.SelectMergeTierRun(
            Bucket(1100, 2000, 3000, 1500), StorageEngine.MergeSealedMinSources, Target, Maximal));

        // A collapsed 4 MB file and a ladder of stragglers beside it: the stragglers coalesce,
        // the collapsed file is not touched.
        var run = StorageEngine.SelectMergeTierRun(
            Bucket(4_000_000, 600, 700, 800), StorageEngine.MergeSealedMinSources, Target, Maximal);
        Assert.NotNull(run);
        Assert.DoesNotContain(run!, s => s.UncompressedBytes == 4_000_000);
    }

    /// <summary>
    /// The tier is <c>floor(log₍ratio₎ payload)</c>, so a tier's members satisfy the size-spread
    /// rule by construction — which is why the fallback needs no separate check for it.
    /// </summary>
    [Fact]
    public void ATiersMembersAreWithinTheSizeRatioByConstruction()
    {
        for (long p = 1; p < 1L << 34; p = p * 3 / 2 + 1)
        {
            int t = StorageEngine.SizeTier(p);
            long floor = 1L << (t * 2), ceiling = floor * StorageEngine.MergeRunSizeRatio;
            Assert.True(p >= floor && p < ceiling, $"{p} B is tier {t}, outside [{floor}, {ceiling})");
        }
    }
}
