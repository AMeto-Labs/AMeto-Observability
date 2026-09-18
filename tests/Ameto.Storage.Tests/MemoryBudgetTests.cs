using System.Diagnostics;
using Ameto.Core;
using Ameto.Storage;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// The flush budgets used to be flat constants — 640 MB of concurrent index builds, 512 MB of
/// frozen tiers, a 256 MB index cache — chosen for a host with room for them and never
/// reconsidered. On the 512 MB console stand that is over a gigabyte of intent, under a runtime
/// whose own managed hard limit is 384 MB, and the failure mode is an OOM kill that reads like
/// a crash. Each ceiling is now min(constant, a share of a limit).
///
/// <para>Two limits, not one. A 512 MB container never shows the GC 512 MB: the GC's heap hard
/// limit is 75 % of it, 384 MB, and that is what TotalAvailableMemoryBytes reports. Managed
/// shares are taken of that; the native frozen-tier share is taken of the container limit,
/// because native memory is not under the GC's hard limit at all. A single base under-provisioned
/// native by a quarter in every container (96 MB instead of 128 MB at 512 MB).</para>
///
/// <para>Derive is a pure function over injected figures so the arithmetic can be checked at
/// 512 MB, 4 GB and 64 GB without three machines; <c>Current()</c> is checked end to end in child
/// processes started under a GC memory setting, because the GC reads its limits once at startup.</para>
/// </summary>
public sealed class MemoryBudgetTests
{
    private const long MB = 1024 * 1024;
    private const long GB = 1024 * MB;

    /// <summary>The stand as a real 512 MB container presents itself: a 384 MB heap limit, 512 MB physical.</summary>
    [Fact]
    public void In_a_512_mb_container_managed_shares_come_from_the_heap_limit_and_native_from_the_container()
    {
        var b = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

        Assert.Equal((long)(384 * MB * 0.25), b.ManagedBuildBytes);   //  96 MB
        Assert.Equal((long)(512 * MB * 0.25), b.NativeTierBytes);     // 128 MB
        Assert.Equal((long)(384 * MB * 0.12), b.IndexCacheBytes);     //  46 MB
        Assert.True(b.IsConstrained);

        // The index cache's NATIVE share (bloom bits) is the second budget taken of the
        // CONTAINER rather than of the heap limit, and that choice is the whole point of having
        // it: 5 % of 512 MB is 25.6 MB, where 5 % of the 384 MB heap limit would be 20.1 MB —
        // managed headroom spent on bytes the GC cannot see. Spelled out in bytes as well as in
        // the formula, so swapping the base under it fails here rather than moving with it.
        Assert.Equal(26_843_545L, b.IndexCacheNativeBytes);
        Assert.Equal((long)(512 * MB * 0.05), b.IndexCacheNativeBytes);
        Assert.NotEqual((long)(384 * MB * 0.05), b.IndexCacheNativeBytes);
        Assert.True(b.IndexCacheNativeBytes < b.IndexCacheBytes,
            "the native ceiling is a backstop on part of the cache, never larger than the whole");

        // The managed shares must leave the heap room for queries, ASP.NET and the GC itself, and
        // all three must leave the container room for the runtime, the ring and the live tier.
        Assert.True(b.ManagedBuildBytes + b.IndexCacheBytes < 384 * MB / 2);
        Assert.True(b.ManagedBuildBytes + b.NativeTierBytes + b.IndexCacheBytes < 512 * MB * 0.70);
    }

    /// <summary>
    /// The ingest body-buffer pool is the fourth ceiling, and the last one that sized itself from
    /// something other than memory: its depth came from <c>2 x ProcessorCount</c>, so a 512 MB
    /// container on a 16-core host took the 32-deep ceiling and could park more than the whole
    /// container. It is a share of the MANAGED limit, because request bodies are byte arrays on
    /// the large object heap.
    /// </summary>
    [Fact]
    public void The_ingest_buffer_pool_is_a_share_of_the_heap_limit_like_the_other_managed_ceilings()
    {
        var stand = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

        Assert.Equal((long)(384 * MB * 0.05), stand.IngestBufferBytes);            // 19 MB
        Assert.True(stand.IngestBufferBytes < MemoryBudgets.IngestBufferCapBytes);

        // Every managed ceiling together still has to leave the heap room for queries, ASP.NET
        // and the GC itself — the sum the pool used to sit outside of. The three logs ceilings
        // are 0.42 of the heap limit since the re-cut, and the three tier ceilings that share it
        // with them take it to 0.58 (MetricBudgetWiringTests holds that total).
        Assert.True(stand.ManagedBuildBytes + stand.IndexCacheBytes + stand.IngestBufferBytes < 384 * MB * 0.43);

        // A host with room keeps the absolute ceiling, and an unknown limit falls back to it
        // rather than strangling a healthy machine.
        Assert.Equal(MemoryBudgets.IngestBufferCapBytes, MemoryBudgets.Derive(64 * GB).IngestBufferBytes);
        Assert.Equal(MemoryBudgets.IngestBufferCapBytes, MemoryBudgets.Derive(0).IngestBufferBytes);
        Assert.True(MemoryBudgets.Derive(32 * MB).IngestBufferBytes >= 8 * MB);    // the floor
    }

    /// <summary>
    /// The ingest payload arena's BYTE-SHARE term: a share of the PHYSICAL limit, like the frozen
    /// tiers and for the same reason — it is native memory, not under the GC's hard limit. It is
    /// one of two terms of the arena's default, not the default itself (the slab floor is the
    /// other, tested below), so the 76 MB figure here is the share and no longer what a 512 MB
    /// container's ring gets.
    /// </summary>
    [Fact]
    public void The_ingest_arena_byte_share_is_a_share_of_the_physical_limit()
    {
        var stand = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

        Assert.Equal((long)(512 * MB * 0.15), stand.IngestArenaBytes);              // 76 MB
        Assert.True(stand.IngestArenaBytes < MemoryBudgets.IngestArenaCapBytes);

        // A host with room keeps the 512 MB cap; an unknown limit falls back to it.
        Assert.Equal(MemoryBudgets.IngestArenaCapBytes, MemoryBudgets.Derive(64 * GB).IngestArenaBytes);
        Assert.Equal(MemoryBudgets.IngestArenaCapBytes, MemoryBudgets.Derive(0).IngestArenaBytes);
        Assert.True(MemoryBudgets.Derive(32 * MB).IngestArenaBytes >= 16 * MB);     // the floor
    }

    /// <summary>
    /// The review scenario: an OpenTelemetry collector sends 8 192 records a batch by default, a
    /// pending event holds a slab whatever its size, and the parser fills the ring faster than the
    /// drainer empties it. With the byte share alone a 512 MB container's arena was ~76 MB — about
    /// 1 200 slabs at 64 KB — so one ordinary batch could hit DroppedNoSlab part way through, where
    /// main's flat 512 MB (8 192 slabs) absorbed it. The default now holds a batch's worth of slabs.
    /// </summary>
    [Fact]
    public void The_default_arena_holds_a_collector_batch_of_slabs_in_a_512_mb_container()
    {
        var stand = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);
        var ingestion = new IngestionOptions();

        long arena = IngestionOptions.DefaultPayloadPoolBytesFor(stand, ingestion.MaxEventPayloadBytes);
        // The ring's own slab arithmetic: min(ring slots, budget / slab size).
        long slabs = Math.Min(ingestion.RingCapacity, arena / ingestion.MaxEventPayloadBytes);

        Assert.True(slabs >= 8192, $"a 512 MB container's default arena holds {slabs} slabs; one collector batch is 8 192");
        Assert.True(arena <= MemoryBudgets.IngestArenaCapBytes);
    }

    /// <summary>
    /// The floor is a slab COUNT, so it scales with the slab size — and a raised
    /// MaxEventPayloadBytes must not turn it into a multi-gigabyte reservation. 8 192 slabs of
    /// 1 MB would be 8 GB; the default never goes above the 512 MB the flat default was.
    /// </summary>
    [Theory]
    [InlineData(256 * 1024)]
    [InlineData(1024 * 1024)]
    [InlineData(16 * 1024 * 1024)]
    public void A_raised_slab_size_never_pushes_the_default_arena_above_512_mb(int slabBytes)
    {
        foreach (var host in (MemoryBudgets[])[
                     MemoryBudgets.Derive(384 * MB, 512 * MB), MemoryBudgets.Derive(768 * MB, 1 * GB),
                     MemoryBudgets.Derive(64 * GB), MemoryBudgets.Derive(0)])
        {
            Assert.Equal(MemoryBudgets.IngestArenaCapBytes, IngestionOptions.DefaultPayloadPoolBytesFor(host, slabBytes));
        }
    }

    /// <summary>
    /// The byte share is still the other term: with a small slab, 8 192 slabs come to less than the
    /// share, and the larger figure wins. Big hosts are unchanged at 512 MB.
    /// </summary>
    [Fact]
    public void The_default_arena_is_the_larger_of_the_slab_floor_and_the_byte_share()
    {
        var stand = MemoryBudgets.Derive(384 * MB, 512 * MB);

        // 4 KB slabs: floor 32 MB, share 76 MB.
        Assert.Equal(stand.IngestArenaBytes, IngestionOptions.DefaultPayloadPoolBytesFor(stand, 4 * 1024));
        // 16 KB slabs at 32 GB: floor 128 MB, share capped at 512 MB.
        Assert.Equal(MemoryBudgets.IngestArenaCapBytes,
                     IngestionOptions.DefaultPayloadPoolBytesFor(MemoryBudgets.Derive(24 * GB, 32 * GB), 16 * 1024));

        foreach (var big in (MemoryBudgets[])[MemoryBudgets.Derive(24 * GB, 32 * GB), MemoryBudgets.Derive(64 * GB), MemoryBudgets.Derive(0)])
            Assert.Equal(MemoryBudgets.IngestArenaCapBytes, IngestionOptions.DefaultPayloadPoolBytesFor(big, 64 * 1024));
    }

    /// <summary>
    /// An explicit Ingestion.PayloadPoolBytes always wins — including a value BELOW the slab floor,
    /// which is how a small host that expects large events buys a hard ceiling.
    /// </summary>
    [Fact]
    public void An_explicit_payload_pool_wins_over_the_default_rule()
    {
        Assert.Equal(96 * MB, new IngestionOptions { PayloadPoolBytes = 96 * MB }.EffectivePayloadPoolBytes);
        Assert.Equal(16 * MB, new IngestionOptions { PayloadPoolBytes = 16 * MB }.EffectivePayloadPoolBytes);
        Assert.Equal(2 * GB,  new IngestionOptions { PayloadPoolBytes = 2 * GB, MaxEventPayloadBytes = 1024 * 1024 }.EffectivePayloadPoolBytes);
    }

    /// <summary>
    /// A big host that caps its managed heap with GCHeapHardLimit has said nothing about native
    /// memory, so the native budget must not shrink with the heap.
    /// </summary>
    [Fact]
    public void A_low_heap_hard_limit_on_a_big_host_does_not_shrink_the_native_budget()
    {
        var b = MemoryBudgets.Derive(managedLimitBytes: 512 * MB, physicalLimitBytes: 64 * GB);

        Assert.Equal((long)(512 * MB * 0.25), b.ManagedBuildBytes);
        Assert.Equal(MemoryBudgets.NativeTierCapBytes, b.NativeTierBytes);
        Assert.Equal((long)(512 * MB * 0.12), b.IndexCacheBytes);

        // Including the cache's own native share, which is the sharpest reading of the rule:
        // on a 64 GB host it takes the fixed ceiling, where the managed base would have given
        // 5 % of 512 MB = 26.8 MB — a quarter of it, on a host with 64 GB of room.
        Assert.Equal(MemoryBudgets.IndexCacheNativeCapBytes, b.IndexCacheNativeBytes);
        Assert.True(b.IndexCacheNativeBytes > (long)(512 * MB * 0.05),
            "a heap hard limit says nothing about native memory and must not shrink this either");
    }

    [Fact]
    public void At_4_gb_the_shares_are_above_two_ceilings_and_below_none()
    {
        var b = MemoryBudgets.Derive(4 * GB);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes, b.ManagedBuildBytes);  // 25 % = 1.0 GB > cap
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,   b.NativeTierBytes);    // 25 % = 1.0 GB > cap
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,   b.IndexCacheBytes);    // 12 % = 491 MB > cap
        Assert.Equal(MemoryBudgets.IndexCacheNativeCapBytes, b.IndexCacheNativeBytes); // 5 % = 205 MB > cap
        Assert.False(b.IsConstrained);
    }

    /// <summary>
    /// THE RE-CUT IS FREE ABOVE THE STAND, which is the claim that made it safe to make. The
    /// managed logs shares went 0.30 / 0.15 / 0.10 -> 0.25 / 0.12 / 0.05 to pay for the metric
    /// and trace tiers that WP3 appended beside them, and a fraction change is only ever a
    /// behaviour change where the fraction BINDS. Every one of the nine ceilings is
    /// <c>min(cap, share)</c>, and the first share to stop binding is the largest — 640 MB of a
    /// 16 GB limit is 3.9 %, six times below the smallest fraction here — so every derived figure
    /// on a host with room is its constant, before and after.
    ///
    /// <para>Asserted at both ends of "large": 16 GB, which is a CI runner or a developer box and
    /// the smallest host where this has to hold, and 64 GB. If a later cut takes a fraction below
    /// its cap's share of 16 GB, this fails rather than quietly re-sizing every real deployment.</para>
    /// </summary>
    [Theory]
    [InlineData(16)]
    [InlineData(64)]
    public void The_re_cut_of_the_managed_shares_moves_nothing_on_a_large_host(int hostGb)
    {
        var b = MemoryBudgets.Derive(hostGb * GB);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes,     b.ManagedBuildBytes);
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,       b.IndexCacheBytes);
        Assert.Equal(MemoryBudgets.IngestBufferCapBytes,     b.IngestBufferBytes);
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,       b.NativeTierBytes);
        Assert.Equal(MemoryBudgets.IndexCacheNativeCapBytes, b.IndexCacheNativeBytes);
        Assert.Equal(MemoryBudgets.IngestArenaCapBytes,      b.IngestArenaBytes);
        Assert.Equal(MemoryBudgets.MetricHotTierCapBytes,    b.MetricHotTierBytes);
        Assert.Equal(MemoryBudgets.TraceHotTierCapBytes,     b.TraceHotTierBytes);
        Assert.Equal(MemoryBudgets.TraceMergeCapBytes,       b.TraceMergeBytes);
        Assert.False(b.IsConstrained);

        // And the margin, so the next cut can see how much room it is spending: the largest cap
        // as a share of this host is the bar every managed fraction has to stay above.
        double bar = MemoryBudgets.ManagedBuildCapBytes / (double)(hostGb * GB);
        foreach (double f in (double[])[MemoryBudgets.ManagedBuildFraction, MemoryBudgets.IndexCacheFraction,
                                        MemoryBudgets.IngestBufferFraction, MemoryBudgets.MetricHotTierFraction,
                                        MemoryBudgets.TraceHotTierFraction, MemoryBudgets.TraceMergeFraction])
            Assert.True(f > bar, $"a fraction of {f:P0} is below {bar:P1}, so a {hostGb} GB host no longer takes the caps");
    }

    [Fact]
    public void At_64_gb_nothing_grows_past_the_fixed_ceilings()
    {
        var b = MemoryBudgets.Derive(64 * GB);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes, b.ManagedBuildBytes);
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,   b.NativeTierBytes);
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,   b.IndexCacheBytes);
        Assert.Equal(MemoryBudgets.IndexCacheNativeCapBytes, b.IndexCacheNativeBytes);
        Assert.False(b.IsConstrained);
    }

    /// <summary>
    /// IS-CONSTRAINED IS A DISJUNCTION OVER EVERY CEILING A HOST CAN CUT, and it listed three of
    /// the six that <c>Derive</c> can cut: the metric tier, the trace tier and the trace merge
    /// pass went into the struct without going into it. The word is what <c>StorageEngine</c>
    /// prints beside the budgets at startup — "host-constrained" or "fixed ceilings" — and its
    /// reader is an operator asking why this install behaves unlike the last one.
    ///
    /// <para><b>The three-term form was not giving a wrong answer, and that was a coincidence.</b>
    /// Every managed share comes off the same base, so the budget cut first and capped last is
    /// whichever cap is the largest multiple of its own fraction: index builds, at
    /// 640 MB / 0.25 = 2 560 MB of managed limit, against 610 MB for the metric tier, 515 MB for
    /// the trace tier and 1 160 MB for the merge pass. Every host small enough to have a tier cut
    /// had its build budget cut too. One fraction change breaks that, which is why the second row
    /// here is a 2 GB host — where the build budget is still a share at 512 of 640 MB while all
    /// three tier budgets are already at their caps, the narrowest gap the ordering leaves — and
    /// why the sweep below compares the property against the ceilings themselves rather than
    /// restating its terms.</para>
    /// </summary>
    [Theory]
    [InlineData( 384,  512, true)]    // the stand: every managed ceiling and the native one are shares
    [InlineData(2048, 2048, true)]    // builds still a share (512 of 640 MB) with all three tiers capped
    [InlineData(4096, 4096, false)]   // nothing is a share
    [InlineData(65536, 65536, false)] // 64 GB
    [InlineData(   0,    0, false)]   // a runtime that could not say falls back to the constants
    public void Is_constrained_answers_for_every_ceiling_a_host_can_cut(long managedMb, long physicalMb, bool expected)
    {
        var b = MemoryBudgets.Derive(managedMb * MB, physicalMb * MB);
        Assert.Equal(expected, b.IsConstrained);
    }

    /// <summary>
    /// ...and the guard that outlives the ordering above: across four decades of limit, the
    /// property agrees with the ceilings. Recomputed from the budgets and their constants, not
    /// restated from the property's terms, so dropping one of them is caught the moment some
    /// host can tell the two forms apart.
    /// </summary>
    [Fact]
    public void Is_constrained_agrees_with_the_ceilings_at_every_size()
    {
        for (long limit = 16 * MB; limit <= 64 * GB; limit = limit * 3 / 2)
        {
            var b = MemoryBudgets.Derive(limit, limit);

            bool cut = b.ManagedBuildBytes  < MemoryBudgets.ManagedBuildCapBytes
                    || b.NativeTierBytes    < MemoryBudgets.NativeTierCapBytes
                    || b.IndexCacheBytes    < MemoryBudgets.IndexCacheCapBytes
                    || b.MetricHotTierBytes < MemoryBudgets.MetricHotTierCapBytes
                    || b.TraceHotTierBytes  < MemoryBudgets.TraceHotTierCapBytes
                    || b.TraceMergeBytes    < MemoryBudgets.TraceMergeCapBytes;

            Assert.True(cut == b.IsConstrained,
                $"at a {limit / MB} MB limit some ceiling is {(cut ? "cut" : "at its constant")} "
              + $"and IsConstrained says {b.IsConstrained}");
        }
    }

    /// <summary>
    /// A figure the runtime could not produce must not be read as "almost no memory" — that
    /// would silently strangle a healthy host. Fall back to the constants, which is what the
    /// engine did before any of this existed.
    /// </summary>
    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    public void Unknown_available_memory_falls_back_to_the_constants(long available)
    {
        var b = MemoryBudgets.Derive(available);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes, b.ManagedBuildBytes);
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,   b.NativeTierBytes);
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,   b.IndexCacheBytes);
        Assert.Equal(MemoryBudgets.IndexCacheNativeCapBytes, b.IndexCacheNativeBytes);
        Assert.False(b.IsConstrained);
    }

    /// <summary>An unknown physical limit borrows the managed one rather than the constant.</summary>
    [Fact]
    public void Unknown_physical_limit_borrows_the_managed_limit()
    {
        var b = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 0);
        Assert.Equal((long)(384 * MB * 0.25), b.NativeTierBytes);
        Assert.Equal((long)(384 * MB * 0.05), b.IndexCacheNativeBytes);
        Assert.Equal(384 * MB, b.PhysicalLimitBytes);
    }

    /// <summary>An absurd limit still has to yield an engine that can flush at all.</summary>
    [Fact]
    public void Floors_keep_a_tiny_limit_workable()
    {
        var b = MemoryBudgets.Derive(32 * MB);

        Assert.True(b.ManagedBuildBytes >= 16 * MB);
        Assert.True(b.NativeTierBytes   >= 16 * MB);
        Assert.True(b.IndexCacheBytes   >=  8 * MB);

        // 5 % of 32 MB is 1.6 MB, which would not hold one production group's bloom section:
        // the native ceiling has a floor of its own, and it is the floor that applies here.
        Assert.Equal(4 * MB, b.IndexCacheNativeBytes);
    }

    [Fact]
    public void Budgets_rise_monotonically_with_available_memory()
    {
        long prevManaged = 0, prevNative = 0, prevCache = 0, prevCacheNative = 0;
        for (long available = 64 * MB; available <= 64 * GB; available *= 2)
        {
            var b = MemoryBudgets.Derive(available * 3 / 4, available);
            Assert.True(b.ManagedBuildBytes     >= prevManaged);
            Assert.True(b.NativeTierBytes       >= prevNative);
            Assert.True(b.IndexCacheBytes       >= prevCache);
            Assert.True(b.IndexCacheNativeBytes >= prevCacheNative);
            (prevManaged, prevNative, prevCache, prevCacheNative) =
                (b.ManagedBuildBytes, b.NativeTierBytes, b.IndexCacheBytes, b.IndexCacheNativeBytes);
        }
    }

    /// <summary>
    /// The GC computes its high-memory-load threshold as GCHighMemPercent of the physical limit,
    /// truncating; dividing back out has to land on the limit, not a few bytes under it.
    /// </summary>
    [Theory]
    [InlineData(512L,     90)]
    [InlineData(32_541L,  90)]
    [InlineData(512L,     50)]
    [InlineData(100_000L, 97)]
    public void The_physical_limit_is_recovered_from_the_high_memory_load_threshold(long physicalMb, int percent)
    {
        long physical  = physicalMb * MB;
        long threshold = (long)((double)percent / 100 * physical);   // the GC's own arithmetic

        Assert.Equal(physical, MemoryBudgets.PhysicalLimitFrom(threshold, percent, managedLimitBytes: 1));
    }

    [Theory]
    [InlineData(0L,    90)]
    [InlineData(1000L, 0)]
    [InlineData(1000L, 101)]
    public void An_unusable_threshold_falls_back_to_the_managed_limit(long threshold, int percent) =>
        Assert.Equal(384 * MB, MemoryBudgets.PhysicalLimitFrom(threshold, percent, 384 * MB));

    /// <summary>
    /// The reason the native budget had to scale too: the frozen-tier slot count is the native
    /// budget divided by one tier's real footprint, so the stand's 16 MB tier used to clamp at
    /// 512 MB / 18 MB = 28 slots — half a gigabyte of frozen tiers allowed on a 512 MB host.
    /// (StorageEngineBudgetWiringTests checks the engine actually uses these numbers.)
    /// </summary>
    [Fact]
    public void Slot_count_on_the_512_mb_stand_no_longer_allows_half_a_gigabyte_of_tiers()
    {
        long tierFootprint = HotTierSegment.NativeBytesFor(16 * MB);

        int before = (int)Math.Clamp(MemoryBudgets.NativeTierCapBytes / tierFootprint, 1, 64);
        int after  = (int)Math.Clamp(MemoryBudgets.Derive(384 * MB, 512 * MB).NativeTierBytes / tierFootprint, 1, 64);

        Assert.True(after < before, $"slots should shrink on a small host: {before} -> {after}");
        Assert.True(after >= 1,     "at least one flush must always be able to proceed");
        Assert.True((long)after * tierFootprint <= 512 * MB / 4,
            "frozen tiers must stay inside a quarter of the host");
    }

    /// <summary>An explicit configuration value wins over anything derived, in both directions.</summary>
    [Fact]
    public void Configured_index_cache_budget_wins()
    {
        Assert.Equal(48 * MB, new QueryOptions { IndexCacheBytes = 48 * MB }.EffectiveIndexCacheBytes);
        Assert.Equal(0,       new QueryOptions { IndexCacheBytes = 0 }.EffectiveIndexCacheBytes);
        Assert.Equal(4 * GB,  new QueryOptions { IndexCacheBytes = 4 * GB }.EffectiveIndexCacheBytes);

        long derived = new QueryOptions().EffectiveIndexCacheBytes;
        Assert.Equal(MemoryBudgets.Current().IndexCacheBytes, derived);
        Assert.True(derived > 0 && derived <= MemoryBudgets.IndexCacheCapBytes);
    }

    /// <summary>
    /// The native ceiling is a backstop, not a cap on a cache an operator asked for. Fixed at
    /// min(96 MB, 5 % of physical) whatever the budget said, it silently capped anyone who raised
    /// IndexCacheBytes: at the worst measured native share of an entry it binds at roughly 1.2 GB
    /// of configured cache, and past that every insert evicts the LRU tail while indexCacheBytes
    /// rests far below indexCacheBudgetBytes and the hit rate never improves.
    /// </summary>
    [Fact]
    public void An_explicitly_configured_cache_budget_carries_its_native_ceiling_up_with_it()
    {
        // A big host: 64 GB, no container. The backstop is the 96 MB constant, and 20 % of a 4 GB
        // budget — well past where the fixed ceiling used to start binding — is far under what the
        // host clamp allows there (10 % of 64 GB), so the scaling is what decides.
        var host = MemoryBudgets.Derive(64 * GB);
        long derived = host.IndexCacheNativeBytes;
        Assert.Equal(MemoryBudgets.IndexCacheNativeCapBytes, derived);

        long big = new QueryOptions { IndexCacheBytes = 4 * GB }.IndexCacheNativeBytesFor(host);
        Assert.Equal((long)(4 * GB * MemoryBudgets.IndexCacheNativeEntryShare), big);   // 819 MB
        Assert.True(big > derived, "a budget an operator set must not be capped by the backstop");
        Assert.True(big < 4 * GB, "and the native share is still a part of the total, never all of it");

        // It never follows the budget DOWN: a small cache keeps the derived backstop, which is
        // what bounds where these bytes actually live.
        Assert.Equal(derived, new QueryOptions { IndexCacheBytes = 1 * MB }.IndexCacheNativeBytesFor(host));
        // A disabled cache has no native ceiling question to answer.
        Assert.Equal(derived, new QueryOptions { IndexCacheBytes = 0 }.IndexCacheNativeBytesFor(host));

        // And the property the server actually reads is that same rule against THIS process.
        var here = MemoryBudgets.Current();
        Assert.Equal(here.IndexCacheNativeBytes, new QueryOptions().EffectiveIndexCacheNativeBytes);
        Assert.Equal(new QueryOptions { IndexCacheBytes = 4 * GB }.IndexCacheNativeBytesFor(here),
                     new QueryOptions { IndexCacheBytes = 4 * GB }.EffectiveIndexCacheNativeBytes);
    }

    /// <summary>
    /// ...and the scaling stays anchored to what the HOST can afford. These bytes are the ones the
    /// GC cannot see and RAM pressure cannot reclaim, so a knob that raises the MANAGED budget must
    /// not raise the native pin without bound: on the 512 MB stand this whole round was sized for,
    /// 20 % of a 1 GB budget is 204 MB of bloom bits — 40 % of the container, outside the heap
    /// limit — which is the class of defect the backstop exists to prevent.
    /// </summary>
    [Fact]
    public void A_configured_budget_cannot_raise_the_native_ceiling_past_the_hosts_share()
    {
        var stand = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);
        Assert.Equal(26_843_545L, stand.IndexCacheNativeBytes);                   // 5 % of 512 MB

        long hostCeiling = (long)(512 * MB * MemoryBudgets.IndexCacheNativeMaxFraction);   // 51.2 MB
        Assert.True(hostCeiling < (long)(1 * GB * MemoryBudgets.IndexCacheNativeEntryShare),
            "the case only means anything if the scaling would otherwise have gone higher");

        Assert.Equal(hostCeiling, new QueryOptions { IndexCacheBytes = 1 * GB }.IndexCacheNativeBytesFor(stand));
        // However large the budget gets. The managed knob cannot move this figure past the host.
        Assert.Equal(hostCeiling, new QueryOptions { IndexCacheBytes = 64 * GB }.IndexCacheNativeBytesFor(stand));

        // A clamp, never a floor: it only ever lowers a SCALED ceiling. On a host small enough
        // that its own 10 % is under the 4 MB floor, a configured cache still keeps the backstop.
        var tiny = MemoryBudgets.Derive(managedLimitBytes: 24 * MB, physicalLimitBytes: 32 * MB);
        Assert.True(tiny.IndexCacheNativeBytes > (long)(32 * MB * MemoryBudgets.IndexCacheNativeMaxFraction),
            "the case only means anything where the clamp is below the floor the backstop sits on");
        Assert.Equal(tiny.IndexCacheNativeBytes, new QueryOptions { IndexCacheBytes = 8 * MB }.IndexCacheNativeBytesFor(tiny));
        Assert.True(new QueryOptions { IndexCacheBytes = 4 * GB }.IndexCacheNativeBytesFor(tiny)
                        >= tiny.IndexCacheNativeBytes,
            "the clamp must never cut a configured host below the backstop");

        // A runtime that cannot report a physical limit has nothing to clamp against, so it gets
        // no scaling at all rather than a scaling anchored to a figure nobody can vouch for.
        var unknown = MemoryBudgets.Derive(0, 0);
        Assert.Equal(unknown.IndexCacheNativeBytes,
                     new QueryOptions { IndexCacheBytes = 4 * GB }.IndexCacheNativeBytesFor(unknown));
    }

    // ── Current(), end to end, in a process started under a GC memory setting ─────────────

    /// <summary>
    /// The stand's shape without a container: DOTNET_GCTotalPhysicalMemory tells the GC the physical
    /// limit, and the GC treats it exactly as a detected cgroup or job-object limit — a 75 % heap
    /// hard limit follows (measured: 384 MB of 512 MB, the same as a 512 MB job object). Works on
    /// every OS. With one base, this child reported native 96 MB.
    /// </summary>
    [Fact]
    public void Current_in_a_512_mb_container_takes_native_from_the_container_not_the_heap_limit()
    {
        var child = RunChild(("DOTNET_GCTotalPhysicalMemory", "0x20000000"));   // 512 MB

        Assert.Equal(384 * MB, child.ManagedLimit);
        Assert.Equal(512 * MB, child.PhysicalLimit);

        var expected = MemoryBudgets.Derive(384 * MB, 512 * MB);
        Assert.Equal(expected.ManagedBuildBytes, child.ManagedBuild);   //  96 MB
        Assert.Equal(expected.NativeTierBytes,   child.NativeTier);     // 128 MB
        Assert.Equal(expected.IndexCacheBytes,   child.IndexCache);     //  46 MB

        // The index cache's native share is the other budget this test exists for: bloom bits are
        // NativeMemory, so they are bounded by the CONTAINER, not by the heap limit the rest of
        // the cache is a share of. 25.6 MB here; taken of the managed limit it would be 20.1 MB,
        // and the difference is managed headroom spent on bytes the GC never sees.
        Assert.Equal(expected.IndexCacheNativeBytes, child.IndexCacheNative);
        Assert.Equal(26_843_545L, child.IndexCacheNative);

        // The ring's default arena, as the options the server binds compute it in this container:
        // a collector batch of 64 KB slabs (512 MB), not the 76 MB byte share alone.
        Assert.Equal(512 * MB, child.IngestArenaDefault);
        Assert.True(child.IngestArenaDefault / (64 * 1024) >= 8192);
    }

    /// <summary>
    /// A 512 MB GCHeapHardLimit on this host caps the managed shares and leaves the native one on
    /// the host's physical memory, which this (unlimited) test process reads the same way.
    /// </summary>
    [Fact]
    public void Current_under_a_low_heap_hard_limit_keeps_native_on_the_host()
    {
        long hostPhysical = MemoryBudgets.Current().PhysicalLimitBytes;
        Assert.True(hostPhysical > 1 * GB, $"this test needs a host with more than 1 GB, saw {hostPhysical / MB} MB");

        var child = RunChild(("DOTNET_GCHeapHardLimit", "0x20000000"));        // 512 MB

        Assert.Equal(512 * MB,     child.ManagedLimit);
        Assert.Equal(hostPhysical, child.PhysicalLimit);

        var expected = MemoryBudgets.Derive(512 * MB, hostPhysical);
        Assert.Equal(expected.ManagedBuildBytes, child.ManagedBuild);
        Assert.Equal(expected.NativeTierBytes,   child.NativeTier);
        Assert.Equal(expected.IndexCacheNativeBytes, child.IndexCacheNative);
        Assert.True(child.NativeTier > MemoryBudgets.Derive(512 * MB).NativeTierBytes,
            "a heap hard limit must not shrink the native budget");
        Assert.True(child.IndexCacheNative > MemoryBudgets.Derive(512 * MB).IndexCacheNativeBytes,
            "nor the cache's native ceiling, which is native memory for exactly the same reason");
    }

    /// <summary>
    /// The physical limit is the threshold divided by GCHighMemPercent, so that has to be the
    /// percentage the GC actually used. A 512 MB container that sets GCHighMemPercent=70 has a
    /// 358 MB threshold. Divided by the default 90 %, that reads as a 398 MB container and a 99 MB
    /// native budget instead of 128 MB. The two tests above run at the default, so they cannot
    /// tell the lookup from the constant.
    /// </summary>
    [Fact]
    public void Current_divides_by_a_configured_high_memory_percent_not_the_default()
    {
        var child = RunChild(("DOTNET_GCTotalPhysicalMemory", "0x20000000"),    // 512 MB
                             ("DOTNET_GCHighMemPercent",      "46"));           // hex: 70 %

        Assert.Equal(384 * MB, child.ManagedLimit);
        Assert.Equal(512 * MB, child.PhysicalLimit);
        Assert.Equal(128 * MB, child.NativeTier);
    }

    private readonly record struct ChildBudgets(
        long ManagedLimit, long PhysicalLimit, long ManagedBuild, long NativeTier, long IndexCache,
        long IndexCacheNative, long IngestArenaDefault);

    /// <summary>Runs this test assembly's own entry point (<see cref="ChildProcessEntry"/>) under the given GC settings.</summary>
    private static ChildBudgets RunChild(params ReadOnlySpan<(string Name, string Value)> gcSettings)
    {
        string? hostPath = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH");
        var psi = new ProcessStartInfo(hostPath is { Length: > 0 } && File.Exists(hostPath) ? hostPath : "dotnet")
        {
            UseShellExecute        = false,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
        };
        psi.ArgumentList.Add("exec");
        psi.ArgumentList.Add(typeof(MemoryBudgetTests).Assembly.Location);
        psi.ArgumentList.Add(ChildProcessEntry.MemoryBudgetsCommand);

        // Nothing inherited may pre-empt the settings under test.
        foreach (string prefix in (string[])["DOTNET_", "COMPlus_"])
            foreach (string name in (string[])["GCHeapHardLimit", "GCHeapHardLimitPercent", "GCTotalPhysicalMemory",
                                               "GCHeapHardLimitSOH", "GCHeapHardLimitLOH", "GCHeapHardLimitPOH",
                                               "GCHeapHardLimitSOHPercent", "GCHeapHardLimitLOHPercent", "GCHeapHardLimitPOHPercent",
                                               "GCHighMemPercent"])
                psi.Environment.Remove(prefix + name);
        foreach (var (name, value) in gcSettings)
            psi.Environment[name] = value;

        using var proc = Process.Start(psi)!;
        var stderr = proc.StandardError.ReadToEndAsync();
        string stdout = proc.StandardOutput.ReadToEnd();
        Assert.True(proc.WaitForExit(60_000), "child process did not exit");

        // stderr is read only on the road that reports it. Interpolated into the assert message it
        // was evaluated on EVERY call, including the ones that pass: a blocking wait on the happy
        // path, which does not return until the handle is closed — and a grandchild that inherited
        // it keeps it open past the child's own exit, so the bounded WaitForExit above would be
        // followed by an unbounded wait here.
        if (proc.ExitCode != 0)
            Assert.Fail($"child exited {proc.ExitCode}: {stderr.GetAwaiter().GetResult()}");

        var values = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (string line in stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            int eq = line.IndexOf('=');
            if (eq > 0 && long.TryParse(line.AsSpan(eq + 1), out long v)) values[line[..eq]] = v;
        }

        return new ChildBudgets(
            values["managedLimit"], values["physicalLimit"],
            values["managedBuild"], values["nativeTier"], values["indexCache"],
            values["indexCacheNative"], values["ingestArenaDefault"]);
    }
}
