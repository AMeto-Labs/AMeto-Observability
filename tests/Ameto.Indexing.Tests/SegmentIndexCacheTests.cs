using Ameto.Indexing;
using Xunit;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The cache's ownership contract: a leased reader stays alive whatever evicts it, an
/// evicted unleased reader is freed, a trigram-less entry upgrades in place, and the
/// budget is honoured through the LRU tail. Readers here are built over empty sections —
/// the cache never looks inside them, only at sizes and keys.
/// </summary>
public sealed class SegmentIndexCacheTests
{
    private static SegmentIndexReader NewReader() => SegmentIndexReader.Load([], [], []);

    [Fact]
    public void Hit_returns_the_same_reader_and_counts()
    {
        var cache = new SegmentIndexCache(1024);
        var r = NewReader();
        using (var inserted = cache.Insert("a.seg", 0, hasTrigram: true, r, 10))
            Assert.Same(r, inserted.Index);

        var hit = cache.TryAcquire("a.seg", 0, needTrigram: true);
        Assert.NotNull(hit);
        using (hit.Value)
            Assert.Same(r, hit.Value.Index);

        Assert.Equal(1, cache.HitCount);
        Assert.Equal(0, cache.MissCount);
        Assert.Equal(1, cache.EntryCount);
    }

    [Fact]
    public void Trigramless_entry_misses_a_trigram_query_and_upgrades()
    {
        var cache = new SegmentIndexCache(1024);
        using (cache.Insert("a.seg", 0, hasTrigram: false, NewReader(), 10)) { }

        // Satisfies a no-trigram query...
        var plainHit = cache.TryAcquire("a.seg", 0, needTrigram: false);
        Assert.NotNull(plainHit);
        plainHit.Value.Dispose();
        // ...but not a trigram one.
        Assert.Null(cache.TryAcquire("a.seg", 0, needTrigram: true));

        // The full reader replaces the entry; both query shapes now hit it.
        var full = NewReader();
        using (var lease = cache.Insert("a.seg", 0, hasTrigram: true, full, 12))
            Assert.Same(full, lease.Index);
        var hit = cache.TryAcquire("a.seg", 0, needTrigram: true);
        Assert.NotNull(hit);
        using (hit.Value) Assert.Same(full, hit.Value.Index);
        Assert.Equal(1, cache.EntryCount);
        Assert.Equal(12, cache.TotalBytes);
    }

    [Fact]
    public void Racing_insert_of_an_equal_entry_serves_the_winner_and_disposes_the_loser()
    {
        var cache = new SegmentIndexCache(1024);
        var winner = NewReader();
        using (cache.Insert("a.seg", 0, hasTrigram: true, winner, 10)) { }

        var loser = NewReader();
        using (var lease = cache.Insert("a.seg", 0, hasTrigram: true, loser, 10))
            Assert.Same(winner, lease.Index);   // the earlier entry won; ours was dropped

        Assert.Equal(1, cache.EntryCount);
        Assert.Equal(10, cache.TotalBytes);
    }

    [Fact]
    public void Budget_evicts_from_the_lru_tail()
    {
        var cache = new SegmentIndexCache(25);
        using (cache.Insert("a.seg", 0, true, NewReader(), 10)) { }
        using (cache.Insert("b.seg", 0, true, NewReader(), 10)) { }
        // Touch a — b becomes the tail.
        using (var l = cache.TryAcquire("a.seg", 0, false)!.Value) { }

        using (cache.Insert("c.seg", 0, true, NewReader(), 10)) { } // 30 > 25 → evict b

        Assert.Null(cache.TryAcquire("b.seg", 0, false));
        var aHit = cache.TryAcquire("a.seg", 0, false);
        Assert.NotNull(aHit);
        aHit.Value.Dispose();
        var cHit = cache.TryAcquire("c.seg", 0, false);
        Assert.NotNull(cHit);
        cHit.Value.Dispose();
        Assert.Equal(20, cache.TotalBytes);
    }

    [Fact]
    public void Evicted_entry_under_lease_stays_usable_until_released()
    {
        var cache = new SegmentIndexCache(15);
        var r = NewReader();
        var lease = cache.Insert("a.seg", 0, true, r, 10);

        // Push it out while leased: unlisted, bytes stop counting, reader stays alive.
        using (cache.Insert("b.seg", 0, true, NewReader(), 10)) { }
        Assert.Null(cache.TryAcquire("a.seg", 0, false));
        Assert.Equal(10, cache.TotalBytes);

        // Still usable through the lease — a disposed reader's bloom throws on the probe.
        // (An EMPTY index answers "no information" → true; liveness is what's asserted.)
        Assert.True(lease.Index.MightContain("p", "v"));
        lease.Dispose(); // last reference — freed here, must not throw
    }

    [Fact]
    public void Oversized_entry_does_not_wedge_the_budget()
    {
        var cache = new SegmentIndexCache(5);
        using (var lease = cache.Insert("big.seg", 0, true, NewReader(), 100))
            Assert.True(lease.Index.MightContain("p", "v")); // usable while leased (empty = no info)
        Assert.Equal(0, cache.EntryCount);                    // but never retained
        Assert.Equal(0, cache.TotalBytes);
    }

    [Fact]
    public void Disabled_cache_misses_and_leases_own_the_reader()
    {
        var cache = new SegmentIndexCache(0);
        Assert.False(cache.Enabled);
        Assert.Null(cache.TryAcquire("a.seg", 0, false));

        var r = NewReader();
        using (var lease = cache.Insert("a.seg", 0, true, r, 10))
            Assert.Same(r, lease.Index);
        Assert.Equal(0, cache.EntryCount);
        Assert.Null(cache.TryAcquire("a.seg", 0, false));
    }
}
