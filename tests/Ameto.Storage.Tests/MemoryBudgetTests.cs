using Ameto.Core;
using Ameto.Storage;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// The flush budgets used to be flat constants — 640 MB of concurrent index builds, 512 MB of
/// frozen tiers, a 256 MB index cache — chosen for a host with room for them and never
/// reconsidered. On the 512 MB console stand that is over a gigabyte of intent, under a runtime
/// whose own managed hard limit is 384 MB, and the failure mode is an OOM kill that reads like
/// a crash. Each ceiling is now min(constant, a share of available memory).
///
/// <para>Derive is a pure function over an injected figure precisely so these can be checked at
/// 512 MB, 4 GB and 64 GB without three machines.</para>
/// </summary>
public sealed class MemoryBudgetTests
{
    private const long MB = 1024 * 1024;
    private const long GB = 1024 * MB;

    [Fact]
    public void At_512_mb_every_ceiling_is_a_share_of_the_host()
    {
        var b = MemoryBudgets.Derive(512 * MB);

        Assert.Equal((long)(512 * MB * 0.30), b.ManagedBuildBytes);   // 153 MB
        Assert.Equal((long)(512 * MB * 0.25), b.NativeTierBytes);     // 128 MB
        Assert.Equal((long)(512 * MB * 0.15), b.IndexCacheBytes);     //  76 MB
        Assert.True(b.IsConstrained);

        // The three together must leave the runtime, the ring, the live tier and the GC room
        // to work in — the whole point of the exercise.
        Assert.True(b.ManagedBuildBytes + b.NativeTierBytes + b.IndexCacheBytes < 512 * MB * 0.75);
    }

    [Fact]
    public void At_4_gb_the_shares_are_above_two_ceilings_and_below_none()
    {
        var b = MemoryBudgets.Derive(4 * GB);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes, b.ManagedBuildBytes);  // 30 % = 1.2 GB > cap
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,   b.NativeTierBytes);    // 25 % = 1.0 GB > cap
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,   b.IndexCacheBytes);    // 15 % = 614 MB > cap
        Assert.False(b.IsConstrained);
    }

    [Fact]
    public void At_64_gb_nothing_grows_past_the_fixed_ceilings()
    {
        var b = MemoryBudgets.Derive(64 * GB);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes, b.ManagedBuildBytes);
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,   b.NativeTierBytes);
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,   b.IndexCacheBytes);
        Assert.False(b.IsConstrained);
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
        Assert.False(b.IsConstrained);
    }

    /// <summary>An absurd limit still has to yield an engine that can flush at all.</summary>
    [Fact]
    public void Floors_keep_a_tiny_limit_workable()
    {
        var b = MemoryBudgets.Derive(32 * MB);

        Assert.True(b.ManagedBuildBytes >= 16 * MB);
        Assert.True(b.NativeTierBytes   >= 16 * MB);
        Assert.True(b.IndexCacheBytes   >=  8 * MB);
    }

    [Fact]
    public void Budgets_rise_monotonically_with_available_memory()
    {
        long prevManaged = 0, prevNative = 0, prevCache = 0;
        for (long available = 64 * MB; available <= 64 * GB; available *= 2)
        {
            var b = MemoryBudgets.Derive(available);
            Assert.True(b.ManagedBuildBytes >= prevManaged);
            Assert.True(b.NativeTierBytes   >= prevNative);
            Assert.True(b.IndexCacheBytes   >= prevCache);
            (prevManaged, prevNative, prevCache) = (b.ManagedBuildBytes, b.NativeTierBytes, b.IndexCacheBytes);
        }
    }

    /// <summary>
    /// The reason the native budget had to scale too: the frozen-tier slot count is the native
    /// budget divided by one tier's real footprint, so the stand's 16 MB tier used to clamp at
    /// 512 MB / 18 MB = 28 slots — half a gigabyte of frozen tiers allowed on a 512 MB host.
    /// </summary>
    [Fact]
    public void Slot_count_on_the_512_mb_stand_no_longer_allows_half_a_gigabyte_of_tiers()
    {
        long tierFootprint = HotTierSegment.NativeBytesFor(16 * MB);

        int before = (int)Math.Clamp(MemoryBudgets.NativeTierCapBytes / tierFootprint, 1, 64);
        int after  = (int)Math.Clamp(MemoryBudgets.Derive(512 * MB).NativeTierBytes / tierFootprint, 1, 64);

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
}
