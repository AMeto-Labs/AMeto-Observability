using Ameto.Core;
using Ameto.Query;
using Ameto.Query.Tests;   // QuerySegmentFixtures, compiled in from the query suite (see .csproj)
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What lazy priming SAVES, over the same 40 segments
/// <c>Ameto.Query.Tests.LazySegmentPrimingTests</c> asserts correctness on. Priming a segment
/// memory-maps it and decompresses a block, so allocation tracks segments opened.
///
/// <para>A ratio between two readings of a per-thread allocation counter is not a correctness
/// claim and does not belong beside one — it is what to read when paging feels slow, and it is
/// allowed to be sensitive to the machine it runs on.</para>
/// </summary>
public sealed class LazySegmentPrimingProbe : IAsyncLifetime
{
    private const int Segments     = QuerySegmentFixtures.ManySegments;
    private const int EventsPerSeg = QuerySegmentFixtures.EventsPerSegment;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-lazyalloc-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine = null!;
    private QueryExecutor _query  = null!;

    public LazySegmentPrimingProbe(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync() =>
        (_engine, _query) = await QuerySegmentFixtures.ManySegmentsAsync(_dir);

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>The point: a small page must not pay for the whole catalog.</summary>
    [Fact]
    public async Task SmallPageCostsFarLessThanReadingEverything()
    {
        await PageAsync(5);                       // warm
        await PageAsync(Segments * EventsPerSeg);

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        await PageAsync(5);
        long small = GC.GetAllocatedBytesForCurrentThread() - b0;

        long b1 = GC.GetAllocatedBytesForCurrentThread();
        await PageAsync(Segments * EventsPerSeg);
        long full = GC.GetAllocatedBytesForCurrentThread() - b1;

        _out.WriteLine($"{Segments} segments x {EventsPerSeg} events");
        _out.WriteLine($"page of 5 : {small / 1024.0:F0} KB");
        _out.WriteLine($"full read : {full / 1024.0:F0} KB   ({(double)full / small:F1}x)");

        Assert.True(small * 5 < full,
            $"a 5-event page still costs like a full read: {small} B vs {full} B — lazy priming is not working");
    }

    /// <summary>
    /// OPENS PER QUERY, over the same 40 segments. A filter that survives every segment's
    /// prefilter used to map each survivor TWICE — once to read its index sections, once to
    /// read its blocks — and the second mapping is the one lazy priming was supposed to have
    /// avoided. The prefilter's reader is handed to the scan now.
    ///
    /// <para>Reported per query and per SURVIVING segment; the count is what the assertion is
    /// about, so unlike the allocation ratio above it is exact.</para>
    /// </summary>
    [Fact]
    public async Task AFilteredPageMapsEachSurvivingSegmentOnce()
    {
        const string Filter = "@mt like '%evt%'";   // every segment holds it: all 40 survive

        await FilteredPageAsync(5);                 // warm

        long b0 = SegmentReader.Opens;
        var page = await FilteredPageAsync(5);
        long pageOpens = SegmentReader.Opens - b0;

        long b1 = SegmentReader.Opens;
        var all = await FilteredPageAsync(Segments * EventsPerSeg);
        long fullOpens = SegmentReader.Opens - b1;

        Assert.Equal(5, page.Count);
        Assert.Equal(Segments * EventsPerSeg, all.Count);

        _out.WriteLine($"{Segments} segments, filter {Filter}");
        _out.WriteLine($"page of 5 : {pageOpens} opens");
        _out.WriteLine($"full read : {fullOpens} opens");

        // The prefilter maps every segment once; the scan borrows. Nothing is mapped twice,
        // however much of the catalog the query ends up draining.
        Assert.Equal(Segments, fullOpens);
        Assert.Equal(Segments, pageOpens);

        // …AND EVERY ONE OF THEM IS CLOSED. This is the half a single-segment test cannot
        // reach: a page of 5 primes one or two of the 40, so ~38 readers were opened by the
        // prefilter, handed to a scan that never ran, and have no iterator to close them —
        // only the merge's finally does. On Windows a mapping keeps the file undeletable, and
        // retention and the merge delete segments while queries run, so an escaped reader here
        // is not a leak that shows up as memory, it is a file that never goes away.
        //
        // COUNTED, not renamed. A rename is the honest end-to-end question ("can retention
        // delete this?") but it answers it for the wrong reason as soon as a collection runs:
        // a leaked reader is unreachable, so the finaliser behind MemoryMappedFile releases
        // the handle and the rename succeeds over a bug that is really there. Closes against
        // Opens is the same claim without the GC in it.
        long o0 = SegmentReader.Opens, c0 = SegmentReader.Closes;
        await FilteredPageAsync(5);
        long opened = SegmentReader.Opens - o0, closed = SegmentReader.Closes - c0;

        _out.WriteLine($"page of 5 : {opened} opened, {closed} closed");
        Assert.Equal(Segments, opened);
        Assert.Equal(opened, closed);

        // Belt and braces, and the operation the merge's source cleanup actually needs.
        var files = _engine.ListSegments().Select(s => s.FilePath).ToArray();
        Assert.Equal(Segments, files.Length);
        foreach (var path in files)
        {
            string moved = path + ".moved";
            File.Move(path, moved);      // throws IOException if anything still holds it
            File.Move(moved, path);
        }
        _out.WriteLine($"all {files.Length} segment files renameable — no reader outlived the query");

        Task<List<LogEvent>> FilteredPageAsync(int count) =>
            QuerySegmentFixtures.RunAsync(_query, Filter, count);
    }

    private Task<List<LogEvent>> PageAsync(int count) =>
        QuerySegmentFixtures.RunAsync(_query, null, count);
}
