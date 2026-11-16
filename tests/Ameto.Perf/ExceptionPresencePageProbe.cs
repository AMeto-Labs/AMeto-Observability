using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Query.Tests;   // QuerySegmentFixtures, compiled in from the query suite (see .csproj)
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What a search for exceptions costs over the Error level of a level-split store, where nearly
/// every row carries one — by the PAGE as well as by the full read, because the two disagree.
///
/// <para>The index's exception marker lists the rows that have an exception. Over an Information
/// segment that list is empty and the group is dropped unread
/// (<c>IndexGroupPrefilterAllocProbe.ASearchForExceptionsDoesNotReadTheGroupsThatHoldNone</c>).
/// Over an Error segment it is the opposite: the list names nearly the whole group, and building
/// it narrows next to nothing. A full read hides what that costs, since the rows it returns
/// dwarf the list. A page does not: the prefilter works a whole batch of segments before the
/// first row comes back, so fifty rows used to pay for a decoded list, a copy of it and two more
/// on the way to the reader, in every group of the batch.</para>
///
/// <para>So the narrowing counts the marker before it names it (<c>ISegmentIndex.CountAtMost</c>),
/// and a list that would name fifteen rows in sixteen or more is carried as "every row"
/// (<c>QueryExecutor.NamesNearlyAllOf</c>). <see cref="WhatNamingTheRowsCosts"/> prints both
/// sides of that line — 94 rows in a hundred is above it, 93 below — next to the same question
/// spelled so that the hint does not recognise it (<c>not not has(@x)</c>), which is the plain
/// scan a search for exceptions was before the marker.</para>
///
/// <para>MEASURED (Release; allocation in KB, the least of the runs):</para>
/// <code>
///   rows with an  |  page of 50                        |  full read of all 80 000
///   exception     |  plain scan   has(@x)    cached    |  plain scan   has(@x), cached
///      100 %      |     134         186        170     |   120 256       120 214
///       94 %      |      89         146        124     |   115 121       114 769
///       93 %      |     149        1368       1055     |   112 795       112 669
///       50 %      |     147         793        621     |    67 518        61 138
/// </code>
/// <para>With the list always built, the page at 95 rows in a hundred cost 1.0 MB with the cache
/// and 1.3 MB without. The full read is indifferent at the line — 93 and 94 differ from the plain
/// scan by less than the runs differ from each other — and by one row in two the list is worth
/// a tenth of the read. A full read's uncached column is left out of this table, though the
/// probe prints it: an executor with no cache reads each group's inverted section again for
/// every query (186 KB a group here, into a 256 KB buffer: up to 2 MB a query), whatever the
/// marker then says, and on a full read that shows as a difference from the plain scan that
/// is not the list's.</para>
///
/// <para>WHAT IS ASSERTED: that a page over groups above the line pays for no list, with or
/// without the index cache. Time is printed, never asserted. What is NOT claimed: below the line
/// the list is built, and a first page pays for it in every group of the batch, exactly as a
/// page of any other indexed equality does — the 93 % and 50 % rows are printed to show that
/// cost, not to bound it.</para>
/// </summary>
public sealed class ExceptionPresencePageProbe(ITestOutputHelper output) : IAsyncLifetime
{
    private const int    RowsPerSegment = 10_000;
    private const int    Rows           = QuerySegmentFixtures.ErrorSegments * RowsPerSegment;
    private const int    Page           = 50;
    private const int    Runs           = 5;
    private const int    FullReadRuns   = 2;
    private const string Asked          = "has(@x)";
    private const string Scanned        = "not not has(@x)";

    /// <summary>
    /// What naming every row of the store costs a query at its cheapest: twelve bytes a row —
    /// the copy the index hands out, the list the groups are gathered in and the array the
    /// reader walks, four bytes each. A reader without the cache decodes the list first, which
    /// makes it sixteen.
    /// </summary>
    private const long CheapestListBytes = (long)Rows * 12;

    private readonly string              _root    = Path.Combine(Path.GetTempPath(), "ameto-xpage-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) await engine.DisposeAsync();
        QuerySegmentFixtures.DeleteDataDirectory(_root);
    }

    /// <summary>
    /// Above the line the marker's list is never built, so the page costs what the plain scan's
    /// page costs, plus opening the batch's segments and one count per group — read off the
    /// group's section, or off the memo with the cache.
    ///
    /// <para>The bound is HALF of what the lists would cost at their cheapest
    /// (<see cref="CheapestListBytes"/>), over the plain scan's page. Measured, the difference is
    /// 30-80 KB; with the lists built it was 0.9-1.3 MB. A ratio to the scanned page would be the
    /// natural way to say "about a page", and is not used because the uncached number carries
    /// the pool's noise (see <see cref="MeasureAsync"/>), which a page of ~100 KB cannot absorb
    /// inside a ratio and a bound of ~450 KB can.</para>
    /// </summary>
    [Theory]
    [InlineData(100)]
    [InlineData(94)]
    public async Task APageOverGroupsThatNearlyAllHaveAnExceptionPaysForNoList(int percent)
    {
        var (uncached, cached) = await StoreAsync(percent);

        var scanned    = await MeasureAsync(uncached, Scanned, Page);
        var asked      = await MeasureAsync(uncached, Asked,   Page);
        var askedCache = await MeasureAsync(cached,   Asked,   Page);

        output.WriteLine($"{percent} % of {Rows:N0} Error rows carry an exception; a page of {Page}");
        output.WriteLine($"  plain scan        : {Kb(scanned.Bytes),7} KB  {scanned.Ms,6:F2} ms");
        output.WriteLine($"  has(@x), no cache : {Kb(asked.Bytes),7} KB  {asked.Ms,6:F2} ms");
        output.WriteLine($"  has(@x), cached   : {Kb(askedCache.Bytes),7} KB  {askedCache.Ms,6:F2} ms");
        output.WriteLine($"  the lists, built  : {Kb(CheapestListBytes * percent / 100),7} KB at least");

        long bound = CheapestListBytes * percent / 100 / 2;
        Assert.True(asked.Bytes - scanned.Bytes < bound,
            $"a page of has(@x) is paying for the marker's list: {asked.Bytes} B against {scanned.Bytes} B scanned");
        Assert.True(askedCache.Bytes - scanned.Bytes < bound,
            $"a page of has(@x) is paying for the marker's list out of the cache: {askedCache.Bytes} B against {scanned.Bytes} B scanned");
    }

    /// <summary>
    /// The table the threshold was read from: the page and the full read, asked and scanned, at
    /// densities on both sides of the line. Printed only — but each cell's query is checked to
    /// have returned the same rows as the plain scan's.
    /// </summary>
    [Fact]
    public async Task WhatNamingTheRowsCosts()
    {
        output.WriteLine($"{QuerySegmentFixtures.ErrorSegments} segments x {RowsPerSegment:N0} Error rows; a page is {Page} rows");
        output.WriteLine("with an    |        page, KB (ms)                         |        full read, KB (ms)");
        output.WriteLine("exception  |  plain scan     has(@x)        cached        |  plain scan       has(@x)          cached");

        foreach (int percent in (int[])[100, 94, 93, 50])
        {
            var (uncached, cached) = await StoreAsync(percent);

            var pageScanned = await MeasureAsync(uncached, Scanned, Page);
            var pageAsked   = await MeasureAsync(uncached, Asked,   Page);
            var pageCached  = await MeasureAsync(cached,   Asked,   Page);
            var fullScanned = await MeasureAsync(uncached, Scanned, Rows + 10, FullReadRuns);
            var fullAsked   = await MeasureAsync(uncached, Asked,   Rows + 10, FullReadRuns);
            var fullCached  = await MeasureAsync(cached,   Asked,   Rows + 10, FullReadRuns);

            // The premise of every number above: each spelling and each executor found the same rows.
            Assert.Equal(Rows / 100 * percent, fullScanned.Rows);
            Assert.Equal(fullScanned.Rows, fullAsked.Rows);
            Assert.Equal(fullScanned.Rows, fullCached.Rows);

            output.WriteLine(
                $"{percent,7} %  |{Cell(pageScanned, 5)}{Cell(pageAsked, 5)}{Cell(pageCached, 5)} |" +
                $"{Cell(fullScanned, 7)}{Cell(fullAsked, 7)}{Cell(fullCached, 7)}");
        }

        // What the uncached executor reads again for every query, whatever the marker says.
        using var segment = SegmentReader.Open(_engines[^1].ListSegments()[0].FilePath);
        using var section = segment.RentInvertedIndexBytes(0);
        output.WriteLine($"an inverted section: {Kb(section.Span.Length)} KB a group, {QuerySegmentFixtures.ErrorSegments} groups a query");
    }

    private static string Cell((long Bytes, double Ms, int Rows) m, int width) =>
        $"  {Kb(m.Bytes).PadLeft(width)} ({m.Ms,5:F1})";

    private static string Kb(long bytes) => (bytes / 1024.0).ToString("F0", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>One store at the given density, and two executors over it: one that answers every
    /// query from the sections, and one with the index cache production runs with.</summary>
    private async Task<(QueryExecutor Uncached, QueryExecutor Cached)> StoreAsync(int percent)
    {
        var (engine, uncached) = await QuerySegmentFixtures.ErrorSegmentsAsync(
            Path.Combine(_root, "x" + percent), RowsPerSegment, percent);
        _engines.Add(engine);

        var cached = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance,
                                       new SegmentIndexCache(64L * 1024 * 1024));
        return (uncached, cached);
    }

    /// <summary>
    /// What one run of a query allocates and how long it takes, after a run that is not counted
    /// (the JIT, the segment mappings and — for the cached executor — what the memo learns once).
    ///
    /// <para>Allocation is the LEAST of the runs, time their mean. The counter is process-wide,
    /// because the prefilter runs under <c>Parallel.ForEachAsync</c>, and the assembly disables
    /// test parallelisation so that it is attributable — but the uncached executor rents a
    /// buffer for each group's inverted section (186 KB here, so a 256 KB one) from the shared
    /// array pool, and whether the pool has one to give is not the query's doing: after a
    /// collection has trimmed it, a run allocates them. Over five pages that came to a mean of
    /// 373 KB beside a least of 150, so a mean reports the pool and the least reports the query.
    /// A full read provokes the collection itself, and there an uncached run pays for most of
    /// the eight every time.</para>
    /// </summary>
    private static async Task<(long Bytes, double Ms, int Rows)> MeasureAsync(
        QueryExecutor query, string filter, int count, int runs = Runs)
    {
        int expected = (await QuerySegmentFixtures.RunAsync(query, filter, count)).Count;

        long least = long.MaxValue;
        var  clock = new Stopwatch();
        for (int i = 0; i < runs; i++)
        {
            long before = GC.GetTotalAllocatedBytes(precise: true);
            clock.Start();
            int got = (await QuerySegmentFixtures.RunAsync(query, filter, count)).Count;
            clock.Stop();
            least = Math.Min(least, GC.GetTotalAllocatedBytes(precise: true) - before);
            Assert.Equal(expected, got);
        }
        return (least, clock.Elapsed.TotalMilliseconds / runs, expected);
    }
}
