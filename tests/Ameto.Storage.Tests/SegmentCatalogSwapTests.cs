using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// <see cref="SegmentCatalog.Swap"/> is a merge's commit: the output appears and the sources go
/// as ONE catalog generation. Issue #85: the merge published its output and then removed its
/// sources one at a time, so every reader that listed the catalog in between counted the batch
/// twice — the query path, the live tail and <c>/api/events/counts</c> all read it.
/// </summary>
public sealed class SegmentCatalogSwapTests
{
    private static SegmentInfo Seg(ulong id, uint events, long min, long max) => new()
    {
        Id                = new SegmentId(id),
        NodeId            = new NodeId(1),
        FilePath          = $"seg-{id}.seg",
        MinTimestampTicks = min,
        MaxTimestampTicks = max,
        EventCount        = events,
        MinLevel          = LogLevel.Information,
        CompressedBytes   = 100,
        UncompressedBytes = 200,
    };

    private static long Events(IEnumerable<SegmentInfo> segs)
    {
        long n = 0;
        foreach (var s in segs) n += s.EventCount;
        return n;
    }

    /// <summary>
    /// A read that lands INSIDE the swap waits for it and then sees the whole of it. Two reads are
    /// started from the instant after the output went in and before the sources come out: a
    /// snapshot build and <see cref="SegmentCatalog.Values"/>. Neither may finish there, and both
    /// must list exactly the output — 12 events, never 24.
    ///
    /// <para>No snapshot exists when the swap starts, so the snapshot read has to BUILD one, which
    /// is the read the lock is there for: a build that did not wait would read the version the
    /// swap has not bumped yet, walk the map with the output and both sources in it, and serve
    /// that until the bump. The wait inside the swap is bounded, and it is the negative case
    /// that costs it: a read that is not held back finishes a three-entry walk long before.</para>
    /// </summary>
    [Fact]
    public async Task A_read_inside_a_swap_waits_for_it_and_sees_it_whole()
    {
        var catalog = new SegmentCatalog();
        var a       = Seg(1, 5, 10, 20);
        var b       = Seg(2, 7, 21, 30);
        var merged  = Seg(3, 12, 10, 30);
        Assert.True(catalog.TryAdd(SegmentKey.Of(a), a));
        Assert.True(catalog.TryAdd(SegmentKey.Of(b), b));

        Task<IReadOnlyList<SegmentInfo>>? snapshot = null;
        Task<List<SegmentInfo>>?          values   = null;
        bool snapshotFinishedInside = true, valuesFinishedInside = true;
        catalog._afterSwapAdd = () =>
        {
            snapshot = Task.Run(() => catalog.GetOverlapping(long.MinValue, long.MaxValue));
            values   = Task.Run(() => catalog.Values.ToList());
            snapshotFinishedInside = snapshot.Wait(TimeSpan.FromMilliseconds(500));
            valuesFinishedInside   = values.Wait(TimeSpan.FromMilliseconds(500));
        };

        var removed = new SegmentInfo?[2];
        catalog.Swap(merged, [SegmentKey.Of(a), SegmentKey.Of(b)], removed);
        catalog._afterSwapAdd = null;

        Assert.NotNull(snapshot);
        Assert.NotNull(values);
        var listed = await snapshot!.WaitAsync(TimeSpan.FromSeconds(30));
        var copied = await values!.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(12, Events(listed));   // 24 = the output AND its sources, counted twice
        Assert.Equal(12, Events(copied));
        Assert.False(snapshotFinishedInside, "a snapshot build finished inside the swap instead of waiting for it");
        Assert.False(valuesFinishedInside,   "Values finished inside the swap instead of waiting for it");
    }

    /// <summary>
    /// The swap retires the snapshot readers were being served. It is built before the swap and
    /// read after it: the version bump is all that tells the next reader to build again, and
    /// without it the catalog goes on serving both sources and no output.
    /// </summary>
    [Fact]
    public void A_swap_retires_the_snapshot_built_before_it()
    {
        var catalog = new SegmentCatalog();
        var a       = Seg(1, 5, 10, 20);
        var b       = Seg(2, 7, 21, 30);
        var merged  = Seg(3, 12, 10, 30);
        catalog.TryAdd(SegmentKey.Of(a), a);
        catalog.TryAdd(SegmentKey.Of(b), b);
        Assert.Equal(12, Events(catalog.GetOverlapping(long.MinValue, long.MaxValue)));   // built and cached

        catalog.Swap(merged, [SegmentKey.Of(a), SegmentKey.Of(b)], new SegmentInfo?[2]);

        var after = catalog.GetOverlapping(long.MinValue, long.MaxValue);
        Assert.Same(merged, Assert.Single(after));
        Assert.Same(merged, Assert.Single(catalog.GetOverlapping(15, 25)));   // the windowed path reads the same snapshot
    }

    /// <summary>
    /// What the swap reports back is what the merge acts on: the entry each source key held — its
    /// file is the one to unlink — or null where the key held nothing any more (retention got
    /// there first, and the file is not the merge's to touch); and an entry the output displaced
    /// under its own key, for the engine to judge and log.
    /// </summary>
    [Fact]
    public void A_swap_reports_what_it_removed_and_what_it_displaced()
    {
        var catalog  = new SegmentCatalog();
        var a        = Seg(1, 5, 10, 20);
        var gone     = Seg(2, 7, 21, 30);
        var squatter = Seg(3, 1, 10, 11);
        var merged   = Seg(3, 12, 10, 30);
        catalog.TryAdd(SegmentKey.Of(a), a);
        catalog.TryAdd(SegmentKey.Of(squatter), squatter);

        var removed   = new SegmentInfo?[2];
        var displaced = catalog.Swap(merged, [SegmentKey.Of(a), SegmentKey.Of(gone)], removed);

        Assert.Same(a, removed[0]);
        Assert.Null(removed[1]);
        Assert.Same(squatter, displaced);
        Assert.Same(merged, Assert.Single(catalog.Values));

        var fresh = Seg(4, 3, 40, 50);
        Assert.Null(catalog.Swap(fresh, [], []));   // a free key displaces nothing
        Assert.Equal(2, catalog.Count);
    }
}
