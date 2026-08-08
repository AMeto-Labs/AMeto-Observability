using Ameto.Core;
using Ameto.Query;
using Ameto.Storage;
using Xunit;

namespace Ameto.Query.Tests;

/// <summary>
/// A page query must open only the segments that can actually reach the page.
///
/// <para>It used to open all of them: <c>GetSegments(null, null)</c> returns every segment
/// in the catalog, and the merge primed an iterator for each — memory-mapping the file and
/// decompressing a block — before serving 50 events off the top of the heap. On the sandbox
/// stand that is 291 opens for one page, and it is why paging cost grew with the catalog
/// (10 ms → 379 ms per page as scrolling went deeper).</para>
///
/// <para>What lazy priming must NOT change is the answer, and that is what is asserted here.
/// The allocation ratio that shows it is actually lazy stayed in <c>Ameto.Perf</c>
/// (<c>LazySegmentPrimingProbe</c>) over the same fixture.</para>
/// </summary>
public sealed class LazySegmentPrimingTests : IAsyncLifetime
{
    private const int Segments     = QuerySegmentFixtures.ManySegments;
    private const int EventsPerSeg = QuerySegmentFixtures.EventsPerSegment;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-lazyprime-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;
    private QueryExecutor _query  = null!;

    public async Task InitializeAsync() =>
        (_engine, _query) = await QuerySegmentFixtures.ManySegmentsAsync(_dir);

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private Task<List<LogEvent>> PageAsync(int count, bool forward = false) =>
        QuerySegmentFixtures.RunAsync(_query, null, count, forward: forward);

    /// <summary>
    /// A short page must equal the head of a full read — the oracle is the query itself
    /// reading everything, so the assertion does not depend on how ids are encoded.
    /// </summary>
    [Fact]
    public async Task NewestPageIsCorrectAcrossManySegments()
    {
        var all  = await PageAsync(Segments * EventsPerSeg);
        var page = await PageAsync(5);

        Assert.Equal(5, page.Count);
        Assert.Equal(all.Take(5).Select(e => e.Id.RawValue).ToArray(),
                     page.Select(e => e.Id.RawValue).ToArray());

        for (int i = 1; i < page.Count; i++)
            Assert.True(page[i].Timestamp <= page[i - 1].Timestamp, $"order broken at {i}");
    }

    [Fact]
    public async Task OldestPageIsCorrectWhenReadingForward()
    {
        var all  = await PageAsync(Segments * EventsPerSeg, forward: true);
        var page = await PageAsync(5, forward: true);

        Assert.Equal(5, page.Count);
        Assert.Equal(all.Take(5).Select(e => e.Id.RawValue).ToArray(),
                     page.Select(e => e.Id.RawValue).ToArray());

        for (int i = 1; i < page.Count; i++)
            Assert.True(page[i].Timestamp >= page[i - 1].Timestamp, $"order broken at {i}");
    }

    /// <summary>Forward and backward must be exact reverses of one another.</summary>
    [Fact]
    public async Task ForwardAndBackwardAgreeOnTheWholeSet()
    {
        var desc = await PageAsync(Segments * EventsPerSeg);
        var asc  = await PageAsync(Segments * EventsPerSeg, forward: true);

        Assert.Equal(desc.Count, asc.Count);
        Assert.Equal(desc.Select(e => e.Id.RawValue).ToArray(),
                     asc.Select(e => e.Id.RawValue).Reverse().ToArray());
    }

    [Fact]
    public async Task AFullReadStillReturnsEverything()
    {
        var all = await PageAsync(Segments * EventsPerSeg);
        Assert.Equal(Segments * EventsPerSeg, all.Count);
        Assert.Equal(all.Count, all.Select(e => e.Id.RawValue).Distinct().Count());
    }

    /// <summary>
    /// Breaking out of a page early must still release every mmap. On Windows an open
    /// mapping blocks File.Delete, so deleting the segment files is a definitive check —
    /// and a leak here would silently block merge and retention in production.
    /// </summary>
    [Fact]
    public async Task EarlyBreakReleasesEveryMappedSegment()
    {
        await PageAsync(3);

        var paths = _engine.ListSegments().Select(s => s.FilePath).ToList();
        await _engine.DisposeAsync();

        foreach (var p in paths)
        {
            File.Delete(p);                        // throws if a mapping is still open
            Assert.False(File.Exists(p));
        }

        // Re-create so DisposeAsync in the fixture stays valid.
        (_engine, _query) = await QuerySegmentFixtures.ManySegmentsAsync(_dir);
    }
}
