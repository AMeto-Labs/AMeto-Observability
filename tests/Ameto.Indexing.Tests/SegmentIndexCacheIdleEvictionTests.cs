using Ameto.Core;
using Ameto.Indexing;
using Ameto.Testing;
using Xunit;

namespace Ameto.Indexing.Tests;

/// <summary>
/// Idle-age eviction. Budget pressure was the only thing that ever removed an entry, so a
/// server that answered one wide query and then went quiet kept every decoded posting list
/// AND the native bloom bits behind them resident for the rest of its life. These pin the
/// three things that had to stay true while fixing that: an untouched entry goes, a touched
/// one stays, and native memory is still freed by the LAST lease and never under a live query.
/// </summary>
public sealed class SegmentIndexCacheIdleEvictionTests
{
    /// <summary>
    /// A reader whose bloom really owns native memory — an empty one allocates nothing, so it
    /// could not show that the bits are freed at the right moment.
    /// </summary>
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

    /// <summary>
    /// An operator who means "practically never" writes a long idle age. The sweep period was a
    /// quarter of it, and System.Threading.Timer rejects periods above 0xFFFFFFFE ms (~49.7 days),
    /// so anything from ~199 days up threw ArgumentOutOfRangeException out of the DI factory — and
    /// every endpoint that resolves the cache (every query, /api/diagnostics) returned 500.
    /// </summary>
    [Theory]
    [InlineData(200)]
    [InlineData(365)]
    [InlineData(36_500)]   // a century: still a real idle age, and still within the tick range
    public void A_long_idle_age_constructs_and_still_keeps_young_entries(int days)
    {
        using var cache = new SegmentIndexCache(1 << 20, TimeSpan.FromDays(days));
        Assert.Equal(TimeSpan.FromDays(days), cache.IdleEvict);

        using (cache.Insert("a.seg", 0, true, NewNativeReader(), 100)) { }
        Assert.Equal(0, cache.Sweep());               // a second-old entry is young for months
        Assert.Equal(1, cache.EntryCount);
    }

    /// <summary>
    /// TimeSpan.MaxValue is "never". Converted to Stopwatch ticks it does not fit a long on Linux
    /// (Stopwatch.Frequency = 1e9), so it is treated as off: no timer, nothing evicted.
    /// </summary>
    [Fact]
    public void An_idle_age_of_TimeSpan_MaxValue_turns_idle_eviction_off()
    {
        using var cache = new SegmentIndexCache(1 << 20, TimeSpan.MaxValue);
        Assert.Equal(TimeSpan.Zero, cache.IdleEvict);

        using (cache.Insert("a.seg", 0, true, NewNativeReader(), 100)) { }
        Assert.Equal(0, cache.Sweep());
        Assert.Equal(1, cache.EntryCount);
    }

    /// <summary>
    /// A cache on a clock that moves only when the test advances it. Its sweep timer fires inside
    /// <see cref="ManualTimeProvider.Advance"/>, on the test's thread, so no sweep ever runs beside
    /// an assertion, and a stamp cannot age because the runner descheduled the test for a while.
    /// </summary>
    private static SegmentIndexCache NewCache(TimeSpan idleEvict, ManualTimeProvider clock) =>
        new(1 << 20, 0, idleEvict, clock);

    /// <summary>
    /// The clock tests run at both <c>Stopwatch.Frequency</c> values the cache meets: 1e7 per second
    /// (Windows, where a timestamp is a TimeSpan tick) and 1e9 (Linux). The idle age is converted into
    /// timestamps once, and a conversion made in ticks instead is exact at 1e7; only at 1e9 does it
    /// age every entry out a hundred times too soon.
    /// </summary>
    public static TheoryData<long> Frequencies => new()
    {
        ManualTimeProvider.TickFrequency,
        ManualTimeProvider.NanosecondFrequency,
    };

    [Theory]
    [MemberData(nameof(Frequencies))]
    public void Untouched_entry_is_evicted_and_its_native_memory_released(long frequency)
    {
        var clock = new ManualTimeProvider(frequency);
        using var cache = NewCache(TimeSpan.FromMilliseconds(30), clock);
        var r = NewNativeReader();
        Assert.True(r.ApproxRetainedBytes > 0);      // there really are native bits to free
        using (cache.Insert("a.seg", 0, true, r, 100)) { }

        clock.Advance(TimeSpan.FromMilliseconds(29));
        Assert.Equal(0, cache.Sweep());               // still young: nothing goes
        Assert.Equal(1, cache.EntryCount);

        // The sweep timer fires inside the advance and may get there first — assert the outcome,
        // not who did it. Everything it does has finished by the time Advance returns.
        clock.Advance(TimeSpan.FromMilliseconds(51));
        cache.Sweep();
        Assert.Equal(0, cache.EntryCount);
        Assert.Equal(0, cache.TotalBytes);
        Assert.Equal(1, cache.IdleEvictedCount);
        Assert.Null(cache.TryAcquire("a.seg", 0, false));
        Assert.True(BloomIsFreed(r), "unleased entry's bloom bits should be freed by the sweep");
    }

    /// <summary>
    /// The ownership rule budget eviction follows, applied to the sweep: unlist now, free on
    /// the last release. A query holding a lease must never have its bloom pulled away.
    /// </summary>
    [Fact]
    public void Leased_entry_is_unlisted_but_freed_only_after_the_last_lease()
    {
        var cache = new SegmentIndexCache(1 << 20, TimeSpan.FromMilliseconds(30));
        var r = NewNativeReader();
        var lease = cache.Insert("a.seg", 0, true, r, 100);

        Thread.Sleep(80);
        cache.Sweep();
        Assert.Equal(0, cache.EntryCount);            // gone from the map...
        Assert.Equal(0, cache.TotalBytes);            // ...and off the budget
        Assert.Null(cache.TryAcquire("a.seg", 0, false));

        Assert.False(BloomIsFreed(lease.Index));      // but alive for the holder
        lease.Dispose();
        Assert.True(BloomIsFreed(r));                 // freed exactly once, here
    }

    /// <summary>
    /// The sweep walks the LRU tail and stops at the first young entry, so a hot entry
    /// survives however long the cache has been up.
    /// </summary>
    [Theory]
    [MemberData(nameof(Frequencies))]
    public void Touched_entry_survives_while_its_neighbours_age_out(long frequency)
    {
        var clock = new ManualTimeProvider(frequency);
        using var cache = NewCache(TimeSpan.FromMilliseconds(50), clock);
        using (cache.Insert("cold.seg", 0, true, NewNativeReader(), 100)) { }
        using (cache.Insert("hot.seg",  0, true, NewNativeReader(), 100)) { }

        // 120 ms in all, well past cold's idle age; hot is never more than 20 ms old, and the
        // sweep timer runs at every 12.5 ms crossed on the way.
        for (int i = 0; i < 6; i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(20));
            cache.TryAcquire("hot.seg", 0, false)?.Dispose();   // keeps refreshing its stamp
        }

        cache.Sweep();
        Assert.Equal(1, cache.EntryCount);
        Assert.Null(cache.TryAcquire("cold.seg", 0, false));
        var hit = cache.TryAcquire("hot.seg", 0, false);
        Assert.NotNull(hit);
        hit.Value.Dispose();
    }

    [Fact]
    public void Idle_eviction_off_by_default_and_when_zero()
    {
        var off = new SegmentIndexCache(1 << 20);
        Assert.Equal(TimeSpan.Zero, off.IdleEvict);
        using (off.Insert("a.seg", 0, true, NewNativeReader(), 100)) { }
        Thread.Sleep(30);
        Assert.Equal(0, off.Sweep());
        Assert.Equal(1, off.EntryCount);

        var zero = new SegmentIndexCache(1 << 20, TimeSpan.Zero);
        Assert.Equal(TimeSpan.Zero, zero.IdleEvict);
    }

    /// <summary>
    /// The sweep must happen without anyone calling the cache: a server that goes idle after
    /// one query is precisely the case this exists for, and nothing would touch the cache
    /// again to trigger a lazy sweep.
    /// </summary>
    [Theory]
    [MemberData(nameof(Frequencies))]
    public void Timer_sweeps_an_idle_cache_with_no_caller(long frequency)
    {
        var clock = new ManualTimeProvider(frequency);
        using var cache = NewCache(TimeSpan.FromMilliseconds(100), clock);
        using (cache.Insert("a.seg", 0, true, NewNativeReader(), 100)) { }
        Assert.Equal(1, clock.ActiveTimers);          // the cache armed its own sweep

        // The idle age plus one sweep period (a quarter of it). Nothing here calls Sweep: only the
        // cache's timer, which fires inside the advance, can have removed the entry.
        clock.Advance(TimeSpan.FromMilliseconds(100 + 25));

        Assert.Equal(0, cache.EntryCount);
        Assert.Equal(1, cache.IdleEvictedCount);
    }

    /// <summary>A disabled cache retains nothing, so it must not start a timer either.</summary>
    [Fact]
    public void Disabled_cache_never_sweeps()
    {
        using var cache = new SegmentIndexCache(0, TimeSpan.FromMilliseconds(10));
        Assert.False(cache.Enabled);
        Thread.Sleep(50);
        Assert.Equal(0, cache.IdleEvictedCount);
    }
}
