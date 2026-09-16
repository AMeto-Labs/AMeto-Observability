using Ameto.Core;
using Ameto.Indexing;
using Xunit;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The native half of a cached index, and giving it back.
///
/// <para>An entry is not one kind of memory. Its postings are managed; its bloom bits are
/// <c>NativeMemory</c> — 15.6-26.6 % of an entry by the repo's own <c>BloomSizingProbe</c>. The
/// cache charged the sum against one budget derived from the GC's HARD LIMIT, so the native part
/// spent managed headroom on bytes the GC never sees; and nothing in the RAM-pressure path could
/// drop the cache, because it lives in an assembly <c>RamPressureService</c> cannot reference and
/// holds memory no collection reclaims. On a 512 MB stand that is the one component under
/// pressure that pressure could not reach.</para>
///
/// <para>These pin the three things that had to become true: the native share is counted and
/// bounded separately, pressure reaching the cache through <see cref="MemoryShedRegistry"/>
/// actually frees those bytes, and a shed obeys the same ownership rule as an eviction — the last
/// lease frees the reader, never the shed.</para>
/// </summary>
public sealed class SegmentIndexCacheShedTests
{
    /// <summary>
    /// A reader whose bloom really owns native memory. <c>Create(1024)</c> asks for 10 bits per
    /// term, which rounds to one 512-bit block multiple: 10240 bits = 20 blocks = 1280 bytes.
    /// </summary>
    private const long NativeBytesPerReader = 1280;

    private static SegmentIndexReader NewNativeReader()
    {
        using var bloom = SegmentBloomFilter.Create(1024);
        bloom.Add("service");
        return SegmentIndexReader.Load([], [], bloom.Serialise());
    }

    private static bool BloomIsFreed(SegmentIndexReader r)
    {
        try { r.Bloom.MightContain("service"); return false; }
        catch (ObjectDisposedException) { return true; }
    }

    [Fact]
    public void A_reader_reports_the_part_of_itself_the_gc_cannot_see()
    {
        var r = NewNativeReader();

        Assert.Equal(NativeBytesPerReader, r.ApproxNativeBytes);
        Assert.Equal(r.ApproxRetainedBytes - r.ApproxNativeBytes, r.ApproxManagedBytes);
        Assert.True(r.ApproxRetainedBytes >= r.ApproxNativeBytes);
        r.Dispose();
    }

    /// <summary>
    /// The whole point: memory pressure reported to the registry reaches this cache, and the
    /// NATIVE bytes — the ones no <c>GC.Collect</c> could ever have returned — are actually gone.
    /// </summary>
    [Fact]
    public void Pressure_sheds_the_cache_and_native_bytes_fall()
    {
        using var cache = new SegmentIndexCache(1 << 20);
        cache.RegisterForMemoryPressure();

        var a = NewNativeReader();
        var b = NewNativeReader();
        using (cache.Insert("a.seg", 0, true, a, 5000)) { }
        using (cache.Insert("b.seg", 0, true, b, 5000)) { }

        Assert.Equal(2, cache.EntryCount);
        Assert.Equal(2 * NativeBytesPerReader, cache.NativeBytes);
        Assert.Equal(cache.NativeBytes, cache.ShedableNativeBytes);
        Assert.Equal(cache.TotalBytes,  cache.ShedableBytes);

        // Through the registry, which is the road RamPressureService takes — it cannot call the
        // cache directly, and that was half the defect.
        long shed = MemoryShedRegistry.Shed();

        Assert.Equal(10_000L, shed);
        Assert.Equal(0, cache.EntryCount);
        Assert.Equal(0L, cache.TotalBytes);
        Assert.Equal(0L, cache.NativeBytes);
        Assert.Equal(2L, cache.ShedEvictedCount);
        Assert.Null(cache.TryAcquire("a.seg", 0, false));
        Assert.True(BloomIsFreed(a), "shed entries' native bloom bits should be freed");
        Assert.True(BloomIsFreed(b), "shed entries' native bloom bits should be freed");
    }

    /// <summary>
    /// Eviction's ownership rule, applied to the shed: pressure may unlist an entry out from
    /// under a running query, but must never free the bits that query is reading.
    /// </summary>
    [Fact]
    public void A_leased_entry_is_not_freed_while_leased()
    {
        using var cache = new SegmentIndexCache(1 << 20);
        var r = NewNativeReader();
        var lease = cache.Insert("a.seg", 0, true, r, 5000);

        Assert.Equal(5000L, cache.Shed());
        Assert.Equal(0, cache.EntryCount);          // unlisted...
        Assert.Equal(0L, cache.NativeBytes);        // ...and off both budgets
        Assert.Null(cache.TryAcquire("a.seg", 0, false));

        Assert.False(BloomIsFreed(lease.Index));    // but alive for its holder
        lease.Dispose();
        Assert.True(BloomIsFreed(r));               // freed exactly once, here
    }

    /// <summary>
    /// The native ceiling has to bite on its own. A cache of thin-event groups is a quarter bloom
    /// by weight, so it can sit far inside a total budget derived from the managed-heap limit
    /// while holding native bytes that limit says nothing about.
    /// </summary>
    [Fact]
    public void The_native_ceiling_evicts_while_the_total_budget_still_has_room()
    {
        // Native room for two readers; the total budget has room for thousands.
        using var cache = new SegmentIndexCache(1 << 20, 3000, TimeSpan.Zero);
        Assert.Equal(3000L, cache.NativeBudgetBytes);

        using (cache.Insert("a.seg", 0, true, NewNativeReader(), 10)) { }
        using (cache.Insert("b.seg", 0, true, NewNativeReader(), 10)) { }
        Assert.Equal(2, cache.EntryCount);
        Assert.Equal(2 * NativeBytesPerReader, cache.NativeBytes);   // 2560 ≤ 3000

        using (cache.Insert("c.seg", 0, true, NewNativeReader(), 10)) { }   // 3840 > 3000

        Assert.Equal(2, cache.EntryCount);
        Assert.Equal(2 * NativeBytesPerReader, cache.NativeBytes);
        Assert.Equal(20L, cache.TotalBytes);                        // the total never came close
        Assert.Null(cache.TryAcquire("a.seg", 0, false));           // the LRU tail is what went
    }

    /// <summary>
    /// A caller with only one number gets the pre-existing behaviour: no native ceiling, so the
    /// same three entries all stay. This is what every existing construction site does.
    /// </summary>
    [Fact]
    public void Without_a_native_ceiling_the_total_budget_alone_governs()
    {
        using var cache = new SegmentIndexCache(1 << 20);
        Assert.Equal(0L, cache.NativeBudgetBytes);

        using (cache.Insert("a.seg", 0, true, NewNativeReader(), 10)) { }
        using (cache.Insert("b.seg", 0, true, NewNativeReader(), 10)) { }
        using (cache.Insert("c.seg", 0, true, NewNativeReader(), 10)) { }

        Assert.Equal(3, cache.EntryCount);
        Assert.Equal(3 * NativeBytesPerReader, cache.NativeBytes);
    }

    /// <summary>
    /// A disposed cache stops being shed. Every integration test disposes a host inside a
    /// still-running process, and a registry holding those would shed caches on behalf of servers
    /// that no longer exist.
    /// </summary>
    [Fact]
    public void A_disposed_cache_is_no_longer_reachable_from_the_pressure_path()
    {
        var cache = new SegmentIndexCache(1 << 20);
        cache.RegisterForMemoryPressure();
        using (cache.Insert("a.seg", 0, true, NewNativeReader(), 5000)) { }

        int registered = MemoryShedRegistry.RegisteredCount;
        cache.Dispose();

        Assert.Equal(registered - 1, MemoryShedRegistry.RegisteredCount);
        Assert.Equal(0L, MemoryShedRegistry.Shed());   // nothing left registered to shed
        Assert.Equal(1, cache.EntryCount);             // and this cache was not touched
    }

    /// <summary>Registering twice is one registration, so a shed counts its bytes once.</summary>
    [Fact]
    public void Registering_twice_registers_once()
    {
        using var cache = new SegmentIndexCache(1 << 20);
        int before = MemoryShedRegistry.RegisteredCount;

        cache.RegisterForMemoryPressure();
        cache.RegisterForMemoryPressure();

        Assert.Equal(before + 1, MemoryShedRegistry.RegisteredCount);
        using (cache.Insert("a.seg", 0, true, NewNativeReader(), 5000)) { }
        Assert.Equal(5000L, MemoryShedRegistry.Shed());
    }
}
