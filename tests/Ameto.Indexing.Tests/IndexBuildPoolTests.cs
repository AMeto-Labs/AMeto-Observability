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

    /// <summary>
    /// The byte cap, driven on a pool of this suite's own.
    ///
    /// <para>NOT on the process-wide pools: those are registered with the gen2 hook, which empties
    /// them from the finaliser thread whenever the GC reports high memory load — and on Windows
    /// that reading is MACHINE-wide. An exact parked-bytes assertion on them therefore reads 0
    /// whenever a build box happens to be loaded while these very tests allocate tens of megabytes
    /// on the large object heap, which is a failure that says nothing about the cap. A local pool
    /// is never registered, so what it parks depends only on the rule under test. The real pools'
    /// caps are asserted below, on numbers rather than on what they hold at an instant.</para>
    /// </summary>
    [Fact]
    public void ByteCap_DropsReturnsBeyondIt()
    {
        var slabs = new IndexBuildPool.SlabPool<byte>(
            maxLength: IndexBuildPool.SlabBytes, maxArraysPerBucket: 64, maxPooledBytes: 48L << 20);

        var held = new byte[80][];
        for (int i = 0; i < held.Length; i++) held[i] = slabs.Rent(IndexBuildPool.SlabBytes);
        for (int i = 0; i < held.Length; i++) slabs.Return(held[i]);

        // 80 returns of 1 MB against a 48 MB cap: it keeps 48 and drops the rest, rather than
        // keeping all 80 because the per-bucket depth (64) had not been reached either.
        Assert.Equal(48L << 20, slabs.PooledBytes);
        Assert.True(slabs.PooledBytes <= slabs.MaxPooledBytes);
    }

    [Fact]
    public void ByteCap_BindsOnTheIntsPool_WhereTheDepthAloneWouldKeepMore()
    {
        // Four 16 MB tables fit the depth (4) but not the 48 MB cap, so the fourth return is
        // dropped. The slab pool cannot show this: 64 slabs x 1 MB IS its cap, so its depth binds
        // first. On a local pool, for the reason given on the test above.
        const int len = 1 << 22;                                       // 1 << 22 ints = 16 MB
        var ints = new IndexBuildPool.SlabPool<int>(
            maxLength: len, maxArraysPerBucket: 4, maxPooledBytes: 48L << 20);

        var held = new int[4][];
        for (int i = 0; i < held.Length; i++) held[i] = ints.Rent(len);
        for (int i = 0; i < held.Length; i++) ints.Return(held[i]);

        Assert.Equal(3L * len * sizeof(int), ints.PooledBytes);
    }

    /// <summary>
    /// …and the pools the process actually builds through carry the caps the budget derives, at
    /// the shape the tests above drive. This is the half that has to be asserted on the real
    /// pools, and it is a wiring question: it does not depend on when a collection happened.
    /// </summary>
    [Fact]
    public void TheProcessWidePools_CarryTheCapsTheBudgetDerives()
    {
        Assert.Equal(IndexBuildPool.CapsFor(MemoryBudgets.Current().ManagedBuildBytes).Ints,
                     IndexBuildPool.Ints.MaxPooledBytes);
        Assert.Equal(IndexBuildPool.CapsFor(MemoryBudgets.Current().ManagedBuildBytes).Slabs,
                     IndexBuildPool.Slabs.MaxPooledBytes);
        Assert.Equal(1 << 22,                 IndexBuildPool.Ints.MaxLength);
        Assert.Equal(IndexBuildPool.SlabBytes, IndexBuildPool.Slabs.MaxLength);
    }

    // ── What every pool may park, against what the heap allows ───────────────

    /// <summary>
    /// THE SUM NOBODY CHECKED. MemoryBudgetTests asserts that the managed ceilings leave the heap
    /// room for queries, ASP.NET and the GC — and these pools sat entirely outside those very
    /// assertions, at three flat constants totalling 400 MB against the 384 MB managed hard limit
    /// of the 512 MB container they were shipped to. Caps above the limit can never bind: the
    /// OutOfMemoryException arrives first, and "bounded three ways" reduces to the gen2 trim.
    /// </summary>
    [Fact]
    public void AtA512MbContainersBudgets_EveryManagedCeilingFitsTheHeapLimit()
    {
        const long MB = 1024 * 1024;
        var b = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

        long pools = IndexBuildPool.TotalCapBytes(b.ManagedBuildBytes);
        long total = b.ManagedBuildBytes + b.IndexCacheBytes + b.IngestBufferBytes + pools;

        Assert.True(total <= b.ManagedLimitBytes,
            $"{total / MB} MB of managed ceilings against a {b.ManagedLimitBytes / MB} MB heap limit "
          + $"— build {b.ManagedBuildBytes / MB}, cache {b.IndexCacheBytes / MB}, "
          + $"ingest {b.IngestBufferBytes / MB}, pools {pools / MB}");
    }

    /// <summary>A host with room keeps exactly the constants these shares replace.</summary>
    [Fact]
    public void OnAHostWithRoom_ThePoolCapsAreTheConstantsTheyAlwaysWere()
    {
        var caps = IndexBuildPool.CapsFor(MemoryBudgets.ManagedBuildCapBytes);

        Assert.Equal(64L << 20, caps.Slabs);
        Assert.Equal(48L << 20, caps.Ints);
        Assert.Equal(96L << 20, caps.Entries);
        Assert.Equal(400L << 20, IndexBuildPool.TotalCapBytes(MemoryBudgets.ManagedBuildCapBytes));
    }

    /// <summary>A constrained host gets caps it can honour, and the live pools use them.</summary>
    [Fact]
    public void OnAConstrainedHost_TheCapsShrinkWithTheBuildBudget()
    {
        const long MB = 1024 * 1024;
        var stand = IndexBuildPool.CapsFor(MemoryBudgets.Derive(384 * MB, 512 * MB).ManagedBuildBytes);

        Assert.True(stand.Slabs   < 64L << 20);
        Assert.True(stand.Ints    < 48L << 20);
        Assert.True(stand.Entries < 96L << 20);

        var live = IndexBuildPool.CapsFor(MemoryBudgets.Current().ManagedBuildBytes);
        Assert.Equal(live.Slabs,   IndexBuildPool.Slabs.MaxPooledBytes);
        Assert.Equal(live.Ints,    IndexBuildPool.Ints.MaxPooledBytes);
        Assert.Equal(live.Entries, IndexBuildPool.Entries<long>().MaxPooledBytes);
    }

    [Fact]
    public void AGen2Collection_TrimsThePools_ThroughTheRegisteredCallback()
    {
        // The four parked slabs below are only there to be trimmed by the collection this test
        // forces, and the trim callback is registered process-wide AND runs on the finaliser
        // thread: any gen2 — one an earlier class's allocations set up, or one THESE RENTS
        // provoke — can run the very trim the first assertion denies (seen in a full-suite run:
        // 2 MB parked where 4 was expected). After a TrimAll each of the four rents takes a fresh
        // 1 MB array off the LOH, which is exactly what charges the gen2 budget, so a bracket
        // that starts after them leaves the likeliest cause outside it; and a collection counted
        // BEFORE the bracket can still have its callback run inside, because that callback is a
        // finaliser. So the measured window is made unable to cause a collection and drained of
        // one it could inherit:
        //   · a callback already queued is run out first (forced gen2 + WaitForPendingFinalizers);
        //   · the bucket is WARMED outside the window, so the window's four rents come from the
        //     pool and it allocates nothing at all — an attempt whose warm-up was itself
        //     disturbed is started over rather than measured;
        //   · the window is still bracketed by the gen2 count and the attempt RE-ENTERED when a
        //     foreign collection lands in it, rather than asserted through.
        const long Parked = 4L << 20;
        var  held     = new byte[4][];                                  // allocated once, outside the window
        long pooled   = -1;
        bool measured = false;

        for (int attempt = 0; attempt < 8 && !measured; attempt++)
        {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);     // run out a queued callback
            GC.WaitForPendingFinalizers();

            IndexBuildPool.TrimAll();                                   // warm-up: the LOH rents live HERE
            for (int i = 0; i < held.Length; i++) held[i] = IndexBuildPool.Slabs.Rent(IndexBuildPool.SlabBytes);
            for (int i = 0; i < held.Length; i++) IndexBuildPool.Slabs.Return(held[i]);
            IndexBuildPool.TrimIdle();                                  // four parked, the high-water mark back at zero
            if (IndexBuildPool.Slabs.PooledBytes != Parked) continue;   // a trim landed in the warm-up: start over

            int gen2 = GC.CollectionCount(2);                           // ─── window opens: nothing below allocates ───
            for (int i = 0; i < held.Length; i++) held[i] = IndexBuildPool.Slabs.Rent(IndexBuildPool.SlabBytes);
            for (int i = 0; i < held.Length; i++) IndexBuildPool.Slabs.Return(held[i]);
            IndexBuildPool.TrimIdle();                                  // keeps the four (all were out at once), resets the high-water mark
            pooled   = IndexBuildPool.Slabs.PooledBytes;
            measured = GC.CollectionCount(2) == gen2;                   // ─── window closes ───
        }

        Assert.True(measured, $"eight attempts, and a foreign gen2 disturbed every one — last reading {pooled} bytes");
        Assert.Equal(Parked, pooled);

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
