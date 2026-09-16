using Ameto.Core;
using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The accumulators' pools must not turn one large group into permanent resident memory:
/// hints are clamped so the builders after it do not pre-size to it, the pool drops buckets
/// nothing rents from between two gen2 collections, and a byte cap bounds what a burst of
/// returns can park.
/// </summary>
public sealed class IndexBuildPoolTests
{
    private static void BuildAndRelease(int terms, IndexBuildHints hints)
    {
        var idx = new SegmentInvertedIndex(hints.LastTerms);
        var tri = new SegmentTrigramIndex(hints.LastTrigrams);
        Span<byte> hex = stackalloc byte[32];
        var rng = new Random(terms);
        for (int i = 0; i < terms; i++)
        {
            ((ulong)rng.NextInt64()).TryFormat(hex, out _, "x16");
            ((ulong)rng.NextInt64()).TryFormat(hex[16..], out _, "x16");
            idx.AddUtf8((uint)i, "@tr"u8, hex);
            tri.Add((uint)i, hex);
        }
        hints.Record(idx.TermCount, tri.BucketCount);
        idx.ReleaseBuildBuffers();
        tri.ReleaseBuildBuffers();
    }

    [Fact]
    public void ALargeGroup_FollowedBySmallOnes_DoesNotKeepItsArrays()
    {
        IndexBuildPool.TrimAll();
        var hints = new IndexBuildHints();

        BuildAndRelease(600_000, hints);                 // one giant group: ~2^20 entries, 2^21-int table, 20 slabs
        long afterLarge = IndexBuildPool.PooledBytes;
        Assert.True(afterLarge > 40L << 20, $"{afterLarge >> 20} MB parked after the large group");
        Assert.Equal(600_000, hints.LastTerms);          // the first measurement is taken at face value

        for (int g = 0; g < 3; g++) BuildAndRelease(5_000, hints);
        Assert.True(hints.LastTerms <= 5_000, $"hint {hints.LastTerms} still follows the large group");

        // Two gen2 cycles of small groups: each bucket keeps only what those groups had out
        // at once — the large group's twenty slabs shrink to their three, its entry and table
        // sizes (which they never rent) go entirely; their own stay warm.
        IndexBuildPool.TrimIdle();
        for (int g = 0; g < 3; g++) BuildAndRelease(5_000, hints);
        IndexBuildPool.TrimIdle();
        long afterTrim = IndexBuildPool.PooledBytes;
        Assert.True(afterTrim < 16L << 20, $"{afterTrim >> 20} MB still parked after the small groups and two idle trims");
        Assert.True(afterTrim > 0, "the small groups' own buckets were dropped too");
        Assert.True(afterTrim * 3 < afterLarge, $"{afterTrim >> 20} MB after vs {afterLarge >> 20} MB before");

        IndexBuildPool.TrimAll();
        Assert.Equal(0, IndexBuildPool.PooledBytes);
    }

    [Fact]
    public void Hints_FollowAStepUpInTwoOrThreeGroups_AndAStepDownAtOnce()
    {
        var h = new IndexBuildHints();
        h.Record(10_000, 1_000);
        h.Record(400_000, 50_000);
        Assert.True(h.LastTerms < 400_000 && h.LastTerms >= 20_000, $"{h.LastTerms}");   // min(last, 2×avg)
        h.Record(400_000, 50_000);
        h.Record(400_000, 50_000);
        h.Record(400_000, 50_000);
        Assert.Equal(400_000, h.LastTerms);
        h.Record(10_000, 1_000);
        Assert.Equal(10_000, h.LastTerms);
        h.Record(int.MaxValue, int.MaxValue);
        Assert.True(h.LastTerms <= IndexBuildHints.MaxTerms && h.LastTrigrams <= IndexBuildHints.MaxTrigrams);
    }

    [Fact]
    public void ByteCap_DropsReturnsBeyondIt()
    {
        IndexBuildPool.TrimAll();
        var held = new byte[80][];
        for (int i = 0; i < held.Length; i++) held[i] = IndexBuildPool.Slabs.Rent(IndexBuildPool.SlabBytes);
        for (int i = 0; i < held.Length; i++) IndexBuildPool.Slabs.Return(held[i]);
        Assert.True(IndexBuildPool.Slabs.PooledBytes <= IndexBuildPool.Slabs.MaxPooledBytes);
        Assert.True(IndexBuildPool.Slabs.PooledBytes >= 32L << 20);   // it did keep a working set
        IndexBuildPool.TrimAll();
    }

    [Fact]
    public void ByteCap_BindsOnTheIntsPool_WhereTheDepthAloneWouldKeepMore()
    {
        // Four 16 MB tables fit the depth (4) but not the 48 MB cap, so the fourth return is
        // dropped. The slab pool cannot show this: 64 slabs x 1 MB IS its cap, so its depth binds first.
        IndexBuildPool.TrimAll();
        int len = IndexBuildPool.Ints.MaxLength;                        // 1 << 22 ints = 16 MB
        var held = new int[4][];
        for (int i = 0; i < held.Length; i++) held[i] = IndexBuildPool.Ints.Rent(len);
        for (int i = 0; i < held.Length; i++) IndexBuildPool.Ints.Return(held[i]);
        Assert.Equal(48L << 20, IndexBuildPool.Ints.MaxPooledBytes);
        Assert.Equal(3L * len * sizeof(int), IndexBuildPool.Ints.PooledBytes);
        IndexBuildPool.TrimAll();
    }

    [Fact]
    public void AGen2Collection_TrimsThePools_ThroughTheRegisteredCallback()
    {
        IndexBuildPool.TrimAll();
        var held = new byte[4][];
        for (int i = 0; i < held.Length; i++) held[i] = IndexBuildPool.Slabs.Rent(IndexBuildPool.SlabBytes);
        for (int i = 0; i < held.Length; i++) IndexBuildPool.Slabs.Return(held[i]);
        IndexBuildPool.TrimIdle();                                      // keeps the four (all were out at once), resets the high-water mark
        Assert.Equal(4L << 20, IndexBuildPool.Slabs.PooledBytes);

        // Nothing rents before the next gen2, so its high-water trim (or, under memory pressure,
        // its full trim) drops all four. Nothing but the registered gen2 callback runs one here.
        for (int i = 0; i < 2; i++)
        {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            GC.WaitForPendingFinalizers();
        }
        Assert.Equal(0, IndexBuildPool.Slabs.PooledBytes);
    }

    [Fact]
    public void OversizedRent_IsNotPooled()
    {
        IndexBuildPool.TrimAll();
        var big = IndexBuildPool.Ints.Rent(IndexBuildPool.Ints.MaxLength * 2);
        Assert.True(big.Length >= IndexBuildPool.Ints.MaxLength * 2);
        IndexBuildPool.Ints.Return(big);
        Assert.Equal(0, IndexBuildPool.PooledBytes);
    }

    private static readonly long Interval =
        (long)(IndexBuildPool.MinTrimInterval.TotalSeconds * System.Diagnostics.Stopwatch.Frequency);

    /// <summary>
    /// The rule these pools ask is now <see cref="PoolTrimPolicy"/>, shared with the ingest
    /// buffer pool rather than copied into it. Asked here the way a container asks it — the
    /// reading is this process's — with the held bytes standing in for a pool with something in
    /// it; the host-wide half is pinned in <c>Ameto.Core.Tests.PoolTrimPolicyTests</c>.
    /// </summary>
    [Fact]
    public void HighMemoryLoad_TrimsEverything_ButNotAgainWithinTheInterval()
    {
        const long t = 1_000_000;
        Assert.True (ShouldTrimAll(memoryLoadBytes: 900, highLoadThresholdBytes: 900, t, lastTrimTimestamp: 0));
        Assert.False(ShouldTrimAll(memoryLoadBytes: 899, highLoadThresholdBytes: 900, t, lastTrimTimestamp: 0));
        Assert.False(ShouldTrimAll(memoryLoadBytes: 900, highLoadThresholdBytes: 0,   t, lastTrimTimestamp: 0));
        // The refill a full trim causes drives the next gen2: still under pressure, it must not empty the pools again yet.
        Assert.False(ShouldTrimAll(memoryLoadBytes: 950, highLoadThresholdBytes: 900, t + Interval - 1, lastTrimTimestamp: t));
        Assert.True (ShouldTrimAll(memoryLoadBytes: 950, highLoadThresholdBytes: 900, t + Interval,     lastTrimTimestamp: t));

        static bool ShouldTrimAll(long memoryLoadBytes, long highLoadThresholdBytes, long now, long lastTrimTimestamp)
            => PoolTrimPolicy.ShouldTrim(pooledBytes: 64L << 20, memoryLoadBytes, highLoadThresholdBytes,
                                         scaleBytes: 0, readingIsOurs: true, now, lastTrimTimestamp);
    }

    private sealed class RecordingPools : IndexBuildPool.ITrimmable
    {
        public int Idle, All;
        public long PooledBytes => 0;
        public void TrimIdle() => Idle++;
        public void TrimAll()  => All++;
    }

    [Fact]
    public void Gen2_UnderPressure_EmptiesThePools_ThenIdleTrimsUntilTheIntervalHasPassed()
    {
        var pools = new RecordingPools();
        long last = 0;
        long t = System.Diagnostics.Stopwatch.GetTimestamp();

        Assert.False(OnGen2(memoryLoadBytes: 100, highLoadThresholdBytes: 900, t, ref last, pools));
        Assert.Equal((1, 0), (pools.Idle, pools.All));                  // no pressure: the high-water trim
        Assert.Equal(0, last);

        Assert.True(OnGen2(memoryLoadBytes: 900, highLoadThresholdBytes: 900, t, ref last, pools));
        Assert.Equal((1, 1), (pools.Idle, pools.All));                  // pressure: emptied, and the window opens
        Assert.Equal(t, last);

        // Sustained pressure, a gen2 every few seconds: inside the window each one idle-trims.
        for (int i = 1; i <= 5; i++)
            Assert.False(OnGen2(memoryLoadBytes: 950, highLoadThresholdBytes: 900, t + i * (Interval / 6), ref last, pools));
        Assert.Equal((6, 1), (pools.Idle, pools.All));
        Assert.Equal(t, last);

        Assert.True(OnGen2(memoryLoadBytes: 950, highLoadThresholdBytes: 900, t + Interval, ref last, pools));
        Assert.Equal((6, 2), (pools.Idle, pools.All));
        Assert.Equal(t + Interval, last);

        static bool OnGen2(long memoryLoadBytes, long highLoadThresholdBytes, long now,
                           ref long last, IndexBuildPool.ITrimmable pools)
            => IndexBuildPool.OnGen2(memoryLoadBytes, highLoadThresholdBytes, scaleBytes: 0,
                                     readingIsOurs: true, now, ref last, pools);
    }

    /// <summary>
    /// THE NEIGHBOUR. On a host-wide reading — a bare Windows box, or a container started without
    /// a memory limit — a process that is not this one can hold the machine past the GC's
    /// threshold indefinitely. Emptying pools that hold nothing worth reclaiming cannot relieve
    /// that, and doing it every thirty seconds for ever re-allocates the slabs and tables these
    /// pools exist to stop re-allocating. The gen2 still does its ordinary high-water trim.
    /// </summary>
    [Fact]
    public void Gen2_UnderSomeoneElsesPressure_DoesNotEmptyPoolsThatCannotRelieveIt()
    {
        var pools = new RecordingPools();          // holds nothing: PooledBytes is 0
        long last = 0;
        long t = System.Diagnostics.Stopwatch.GetTimestamp();

        for (int i = 0; i < 4; i++)
            Assert.False(IndexBuildPool.OnGen2(
                memoryLoadBytes: 15L << 30, highLoadThresholdBytes: 14L << 30, scaleBytes: 16L << 30,
                readingIsOurs: false, t + i * Interval, ref last, pools));

        Assert.Equal((4, 0), (pools.Idle, pools.All));                  // high-water only, never emptied
        Assert.Equal(0, last);

        // The same pressure, in a container whose limit this reading describes: ours to relieve.
        Assert.True(IndexBuildPool.OnGen2(
            memoryLoadBytes: 15L << 30, highLoadThresholdBytes: 14L << 30, scaleBytes: 16L << 30,
            readingIsOurs: true, t + 4 * Interval, ref last, pools));
        Assert.Equal((4, 1), (pools.Idle, pools.All));   // emptied — and a full trim does not also idle-trim
    }
}
