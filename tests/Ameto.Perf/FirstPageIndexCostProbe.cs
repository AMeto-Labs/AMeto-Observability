using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Query.Tests;   // QuerySegmentFixtures, compiled in from the query suite (see .csproj)
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What the FIRST PAGE of an indexed filter costs, next to the same page read by a plain scan —
/// which is what a page ought to cost: about what it reads.
///
/// <para>It did not. The prefilter works a batch of segments before the first row is returned
/// (<c>QueryExecutor.PrefilterParallelism</c>, eight at its cap), and for every index group of
/// every segment in the batch it built the candidate list of the filter's equalities: the
/// posting list decoded, copied out of the index, gathered into a list and copied out of that —
/// twelve to sixteen bytes for each row named, in groups the page never read. The lists that
/// named the most rows cost the most and narrowed the least: <c>@l = 'Error'</c> over the Error
/// level of a level-split store, typed or picked in the level menu, is every row of every group,
/// and was built to be recognised as that.</para>
///
/// <para>Now each list is COUNTED first, which the index answers without building it. A list
/// that names nearly all of its group is never built; a short one is named on the spot; and a
/// long one is built when the scan reaches its group, by which time a page has usually ended
/// (<c>QueryExecutor.Narrow</c>, <c>SegmentPlan</c>). What is built is lent to the reader as the
/// index keeps it, uncopied.</para>
///
/// <para>MEASURED (Release, a page of 50, KB allocated — the least of five runs; "no cache" is an
/// executor that answers every query from the sections, "cached" the one production runs, asked
/// once before it is measured). "Before" is the commit this was branched from (ab7d381c), the
/// two run back to back. Eight one-group segments of Error rows, half of them with an
/// exception:</para>
/// <code>
///                                   10 000 rows a group              40 000 rows a group
///                                plain   no cache    cached       plain   no cache    cached
///   has(@x)           before      163       811       638          130      2708      2059
///                     after       163       206       169          130       286       185
///   @l = 'Error'      before       23      1003       673           58      3905      2632
///                     after        23        68        49           58       215       132
///   levels=Error      before       23       689       359           58      2648      1381
///                     after        23        67        48           58       150       131
///   Lane = 'even'     before      167       753       580          133      2665      2023
///                     after       167       151       112          134       245       148
///   @mt like '%settle%'  before    23      2242       985           58      8888      3882
///                     after        23      1617       361           58      6389      1382
/// </code>
/// <para>"Before" grew with the group — four times the rows, 3.2-3.9 times the page — and
/// production Error groups reach a hundred thousand rows. "After" does not name the batch. What
/// is left over the plain scan is opening the batch's segments, which a plain scan opens one of
/// (about 4 KB each at 10 000 rows and 11 KB at 40 000: the block index, read whole at every
/// open), and, with no cache to keep it, decoding the list of the one group the page reads.
/// <c>Lane = 'even'</c> comes out under its plain scan because the scan decodes the rows it
/// then rejects.</para>
///
/// <para>The same 80 000 rows as ONE segment of nine index groups — the shape compaction leaves
/// a day in — where the unit that matters is the group and not the file
/// (<see cref="APageOffASegmentOfManyGroupsNamesOneOfThem"/>):</para>
/// <code>
///                                plain   no cache    cached
///   has(@x)           before      179      1256      1081
///                     after       179       202       163
///   @l = 'Error'      before       38      1000       668
///                     after        38        64        42
///   levels=Error      before       38       686       354
///                     after        38        63        41
///   Lane = 'even'     before      182      1198      1022
///                     after       182       147       106
///   @mt like '%settle%'  before    38      3132      1875
///                     after        38      1613       355
/// </code>
/// <para>What a page left behind in the index cache fell with it: the lists of all eight groups
/// before (176 KB for a half-group list at 10 000 rows, 333 KB for a whole-group one), the list
/// of the group it read after (39-41 KB, and 21 KB where the count settles it).</para>
///
/// <para>TIME is printed and never asserted, and on a machine doing anything else it says
/// little: a page is one to three milliseconds either way with a cache, and without one it is
/// 2-20 ms of reading eight groups' sections, before and after — the lists were never most of
/// that. It is the least of the runs, like the bytes.</para>
///
/// <para>WHAT IS ASSERTED, loosely: that a page costs less, over its plain scan, than ONE
/// four-byte copy of the lists its batch would name — it used to cost three or four of them
/// (two or three where the list is the group), and now costs a fraction of one. And the rows:
/// each asking returns the page the plain scan does.</para>
///
/// <para>WHAT IS NOT COVERED: a filter with a substring predicate (<c>like</c>, free text) is
/// still narrowed whole by the prefilter — a trigram list has no count to ask for, and a group
/// it proves empty has to be dropped in the batch — so its page still pays for the trigram
/// lists of every group in reach. The last row of each table prints that, to show the cost and
/// not to bound it. (It fell all the same: each group's candidates are handed to the reader as
/// they are, where they used to be gathered into one list for the segment and copied out.)</para>
/// </summary>
public sealed class FirstPageIndexCostProbe(ITestOutputHelper output) : IAsyncLifetime
{
    private const int Page = 50;
    private const int Runs = 5;

    private readonly string              _root    = Path.Combine(Path.GetTempPath(), "ameto-firstpage-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) await engine.DisposeAsync();
        QuerySegmentFixtures.DeleteDataDirectory(_root);
    }

    /// <summary>
    /// One question put to the store two ways: as the index hears it, and spelled so that no
    /// hint recognises it — the plain scan the page is compared with. <paramref name="Named"/>
    /// is the share of each group the question's posting lists name; <paramref name="Bounded"/>
    /// is false for the one printed to show what is not covered.
    /// </summary>
    private sealed record Question(string Name, string? Filter, LogLevel? Level, string? Scanned, double Named, bool Bounded = true);

    private static readonly Question[] Questions =
    [
        new("has(@x), half the rows",       "has(@x)",               null,           "not not has(@x)",       0.5),
        new("@l = 'Error', every row",      "@l = 'Error'",          null,           null,                    1.0),
        new("levels=Error, every row",      null,                    LogLevel.Error, null,                    1.0),
        new("Lane = 'even', half the rows", "Lane = 'even'",         null,           "not not Lane = 'even'", 0.5),
        new("@mt like '%settle%', not bounded", "@mt like '%settle%'", null,         null,                    1.0, Bounded: false),
    ];

    /// <summary>
    /// Eight one-group segments: one prefilter batch, of which a page reads the newest. At two
    /// group sizes, because what a page paid grew with the group and what it pays must not.
    /// </summary>
    [Theory]
    [InlineData(10_000)]
    [InlineData(40_000)]
    public async Task AFirstPageCostsWhatItReads_NotWhatItsBatchCouldName(int rowsPerSegment)
    {
        var (engine, uncached) = await QuerySegmentFixtures.ErrorSegmentsAsync(
            Path.Combine(_root, "r" + rowsPerSegment), rowsPerSegment, percentWithException: 50);
        _engines.Add(engine);

        output.WriteLine($"{QuerySegmentFixtures.ErrorSegments} segments x {rowsPerSegment:N0} Error rows, half of them with an exception; a page of {Page}");
        await MeasureAsync(engine, uncached, QuerySegmentFixtures.ErrorSegments * rowsPerSegment);
    }

    /// <summary>
    /// The same rows as ONE segment of several index groups. The whole segment is in the batch,
    /// so every group of it used to be named — and gathered into one list for the file — for a
    /// page that reads the last. A group is named when the scan reaches it.
    /// </summary>
    [Fact]
    public async Task APageOffASegmentOfManyGroupsNamesOneOfThem()
    {
        const int Rows = 80_000;
        var (engine, uncached) = await QuerySegmentFixtures.ErrorSegmentsAsync(
            Path.Combine(_root, "groups"), Rows, percentWithException: 50, segments: 1,
            groupPayloadBudgetBytes: 7_500_000);
        _engines.Add(engine);

        int groups;
        using (var reader = SegmentReader.Open(engine.ListSegments()[0].FilePath))
            groups = reader.Groups.Length;
        Assert.True(groups >= 4, $"setup: the segment has {groups} index group(s)");

        output.WriteLine($"1 segment of {groups} index groups, {Rows:N0} Error rows, half of them with an exception; a page of {Page}");
        await MeasureAsync(engine, uncached, Rows);
    }

    private async Task MeasureAsync(StorageEngine engine, QueryExecutor uncached, int rows)
    {
        output.WriteLine("                                 |  plain scan      |  no cache        |  cached          | kept by the cache");
        output.WriteLine("                                 |     KB      ms   |     KB      ms   |     KB      ms   |     KB");

        foreach (var q in Questions)
        {
            using var cache = new SegmentIndexCache(64L * 1024 * 1024);
            var cached = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);

            var scanned = await MeasureAsync(uncached, q.Scanned, null);
            var asked   = await MeasureAsync(uncached, q.Filter, q.Level);
            var hit     = await MeasureAsync(cached,   q.Filter, q.Level);

            // The premise of the comparison: every asking found the page the plain scan finds.
            Assert.Equal(Page, scanned.Rows.Length);
            Assert.Equal(scanned.Rows, asked.Rows);
            Assert.Equal(scanned.Rows, hit.Rows);

            output.WriteLine($"{q.Name,-32} |{Cell(scanned)} |{Cell(asked)} |{Cell(hit)} | {Kb(cache.TotalBytes),6}");
            if (!q.Bounded) continue;

            // One four-byte copy of what the lists of every group in reach would name. Before,
            // a page paid three or four of these (two or three for a list that is the group).
            long bound = (long)(rows * q.Named * 4);
            Assert.True(asked.Bytes - scanned.Bytes < bound,
                $"a page of {q.Name} is paying for lists it does not read: {asked.Bytes} B against {scanned.Bytes} B scanned (bound {bound} B over)");
            Assert.True(hit.Bytes - scanned.Bytes < bound,
                $"a page of {q.Name} is paying for lists it does not read, out of the cache: {hit.Bytes} B against {scanned.Bytes} B scanned (bound {bound} B over)");
        }
    }

    private static string Cell((long Bytes, double Ms, string[] Rows) m) => $" {Kb(m.Bytes),6}  {m.Ms,6:F2}  ";

    private static string Kb(long bytes) => (bytes / 1024.0).ToString("F0", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// What one page allocates and how long it takes, after a run that is not counted (the JIT,
    /// the segment mappings and — for the cached executor — what the memos learn once).
    /// Both are the LEAST of the runs. Allocation, for the reason
    /// <c>ExceptionPresencePageProbe.MeasureAsync</c> gives: an executor with no cache rents a
    /// buffer per group for the inverted section, and whether the pool has one to give is not
    /// the query's doing. Time, because the prefilter runs on the thread pool and a page is a
    /// millisecond or two: on a machine doing anything else the mean reports the machine.
    /// </summary>
    private static async Task<(long Bytes, double Ms, string[] Rows)> MeasureAsync(
        QueryExecutor query, string? filter, LogLevel? level)
    {
        string[] expected = RowsOf(await PageAsync(query, filter, level));

        long   least   = long.MaxValue;
        double fastest = double.MaxValue;
        for (int i = 0; i < Runs; i++)
        {
            long before = GC.GetTotalAllocatedBytes(precise: true);
            long start  = Stopwatch.GetTimestamp();
            var got = await PageAsync(query, filter, level);
            fastest = Math.Min(fastest, Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            least   = Math.Min(least, GC.GetTotalAllocatedBytes(precise: true) - before);
            Assert.Equal(expected, RowsOf(got));
        }
        return (least, fastest, expected);
    }

    private static async Task<List<LogEvent>> PageAsync(QueryExecutor query, string? filter, LogLevel? level)
    {
        var rows = new List<LogEvent>(Page);
        await foreach (var ev in query.ExecuteAsync(new QueryRequest
        {
            Filter = filter,
            Count  = Page,
            Levels = level is { } l ? [l] : null,
        }))
        {
            rows.Add(ev);
        }
        return rows;
    }

    /// <summary>The page as its rows' names, read outside the measured window: properties decode on first touch.</summary>
    private static string[] RowsOf(List<LogEvent> page) => [.. page.Select(static ev => (string)ev.Properties!["Row"]!)];
}
