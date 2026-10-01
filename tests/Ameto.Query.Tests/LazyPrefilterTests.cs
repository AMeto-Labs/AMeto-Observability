using Ameto.Core;
using Ameto.Query;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Query.Tests;

/// <summary>
/// An indexed filter's page must check only the segments that page can reach, and return
/// exactly what reading everything and filtering it would.
///
/// <para>The index prefilter used to run over every segment of the window before the first row:
/// open the file, read each group's bloom, copy its inverted section. Priming was already lazy, so
/// a 50-row page went on to scan only the newest handful of those segments, and the rest of the
/// prefilter bought nothing. A filtered page cost the window, not the page: on a copy of the
/// sandbox stand <c>['service.name'] = 'Axiom.API'</c> took 0.10 s over a day and 0.95 s over 90
/// days for the same 50 rows. The prefilter now walks the merge's priming order, a doubling batch
/// at a time, only when the priming queue runs dry and the next segment could still beat the
/// front.</para>
///
/// <para>Two kinds of claim, kept apart. COUNTS, off <see cref="SegmentReader.Opens"/>: a page
/// the newest segment fills opens one prefilter batch at most, a page that needs the oldest
/// segment still checks them all, and every reader opened is closed again. ANSWERS: whatever the
/// value, direction, page size or cursor, the rows are the head of a full unfiltered read filtered
/// in memory, an oracle that never touches an index. The counters are process-wide, so this
/// depends on the assembly's <c>DisableTestParallelization</c> (see AssemblyInfo.cs).</para>
/// </summary>
public sealed class LazyPrefilterTests : IAsyncLifetime
{
    private const int Segments = QuerySegmentFixtures.ManySegments;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-lazyprefilter-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine = null!;
    private QueryExecutor _query  = null!;

    public LazyPrefilterTests(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync() =>
        (_engine, _query) = await QuerySegmentFixtures.ManyIndexedSegmentsAsync(_dir);

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    private static ulong Id(LogEvent ev) => ev.Id.RawValue;

    private async Task<(long Opens, long Closes, List<LogEvent> Rows)> CountAsync(
        string filter, int count, bool forward = false, long? afterTs = null, EventId? afterId = null)
    {
        long opens = SegmentReader.Opens, closes = SegmentReader.Closes;
        var rows = new List<LogEvent>();
        await foreach (var ev in _query.ExecuteAsync(new QueryRequest
        {
            Filter              = filter,
            Count               = count,
            Direction           = forward ? QueryDirection.Forward : QueryDirection.Backward,
            AfterTimestampTicks = afterTs,
            AfterEventId        = afterId,
        }))
        {
            rows.Add(ev);
            if (rows.Count >= count) break;
        }
        return (SegmentReader.Opens - opens, SegmentReader.Closes - closes, rows);
    }

    /// <summary>Every event in the requested order, kept where <paramref name="property"/> reads <paramref name="value"/>.</summary>
    private async Task<List<LogEvent>> OracleAsync(string property, string value, bool forward)
    {
        var all = await QuerySegmentFixtures.RunAsync(_query, null, int.MaxValue, forward: forward);
        Assert.Equal(QuerySegmentFixtures.ManyIndexedEvents, all.Count);
        return all.Where(e => e.Properties?[property] as string == value).ToList();
    }

    /// <summary>
    /// The point. The newest segment alone holds a page of five <c>Customer = 'cust-1'</c> rows,
    /// so every segment checked past the first prefilter batch is waste, and the eager prefilter
    /// opened all forty. The bound is the batch and not 1: the first batch is checked as one
    /// parallel wave, the price of not knowing in advance which segments a page will need.
    /// </summary>
    [Fact]
    public async Task APageTheNewestSegmentFillsOpensOneBatchAtMost()
    {
        var oracle = await OracleAsync("Customer", "cust-1", forward: false);

        var (opens, closes, rows) = await CountAsync("Customer = 'cust-1'", 5);
        _out.WriteLine($"page of 5 over {Segments} segments: {opens} open(s), {closes} close(s); batch {QueryExecutor.PrefilterParallelism}");

        Assert.Equal(oracle.Take(5).Select(Id), rows.Select(Id));
        Assert.True(opens <= QueryExecutor.PrefilterParallelism,
            $"a page the newest segment fills opened {opens} segments, more than one prefilter batch ({QueryExecutor.PrefilterParallelism})");
        Assert.Equal(opens, closes);
    }

    /// <summary>A level-only filter takes the prefilter too, and stops as early.</summary>
    [Fact]
    public async Task ALevelFilterPageOpensOneBatchAtMost()
    {
        var (opens, closes, rows) = await CountAsync("@l = 'Information'", 5);

        Assert.Equal(5, rows.Count);
        Assert.True(opens <= QueryExecutor.PrefilterParallelism,
            $"a level-filtered page opened {opens} segments, more than one prefilter batch ({QueryExecutor.PrefilterParallelism})");
        Assert.Equal(opens, closes);
    }

    /// <summary>
    /// The other end. <c>Batch = 'first'</c> holds in the oldest segment only, so a newest-first
    /// page has to check every segment of the window and must still find its rows there: a stop
    /// rule that gave up early would come back empty. Each segment is opened exactly once, the
    /// rejected ones closed at once by the prefilter, the survivor by the merge.
    /// </summary>
    [Fact]
    public async Task APageThatNeedsTheOldestSegmentStillChecksTheWholeWindow()
    {
        var oracle = await OracleAsync("Batch", "first", forward: false);

        var (opens, closes, rows) = await CountAsync("Batch = 'first'", 5);
        _out.WriteLine($"rare value, page of 5: {opens} open(s), {closes} close(s)");

        Assert.Equal(oracle.Take(5).Select(Id), rows.Select(Id));
        Assert.Equal(Segments, opens);
        Assert.Equal(opens, closes);
    }

    /// <summary>Read forward, the same value sits in the first segment the merge reaches.</summary>
    [Fact]
    public async Task ReadingForwardTheOldestSegmentIsInTheFirstBatch()
    {
        var oracle = await OracleAsync("Batch", "first", forward: true);

        var (opens, closes, rows) = await CountAsync("Batch = 'first'", 5, forward: true);

        Assert.Equal(oracle.Take(5).Select(Id), rows.Select(Id));
        Assert.True(opens <= QueryExecutor.PrefilterParallelism,
            $"a forward page the oldest segment fills opened {opens} segments, more than one prefilter batch ({QueryExecutor.PrefilterParallelism})");
        Assert.Equal(opens, closes);
    }

    /// <summary>
    /// Whatever the page size and direction, the answer is the head of everything, filtered. The
    /// sizes straddle the batch edges: one row, a few, more than the newest segment holds, more
    /// than one batch holds, more than exist.
    /// </summary>
    [Theory]
    [InlineData("Customer", "cust-1")]
    [InlineData("Customer", "cust-3")]
    [InlineData("Batch", "first")]
    [InlineData("Batch", "later")]
    [InlineData("Customer", "nobody")]
    public async Task EveryPageIsTheHeadOfTheFilteredWhole(string property, string value)
    {
        foreach (bool forward in new[] { false, true })
        {
            var oracle = await OracleAsync(property, value, forward);
            foreach (int count in new[] { 1, 5, 7, 30, 260, 2_000 })
            {
                var (opens, closes, rows) = await CountAsync($"{property} = '{value}'", count, forward);

                Assert.Equal(oracle.Take(count).Select(Id).ToArray(), rows.Select(Id).ToArray());
                Assert.Equal(opens, closes);
            }
        }
    }

    /// <summary>
    /// Paging with the (ts, id) cursor restarts the prefilter at the cursor on every page, and
    /// the pages must still join up into the whole, without a gap or a repeat, across segments
    /// whose time ranges overlap.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CursorPagesJoinUpIntoTheFilteredWhole(bool forward)
    {
        const int Page = 7;
        var oracle = await OracleAsync("Customer", "cust-2", forward);

        var walked = new List<LogEvent>();
        long? afterTs = null;
        EventId? afterId = null;
        for (int page = 0; page <= oracle.Count / Page; page++)
        {
            var (opens, closes, rows) = await CountAsync("Customer = 'cust-2'", Page, forward, afterTs, afterId);
            Assert.Equal(opens, closes);

            walked.AddRange(rows);
            if (rows.Count < Page) break;
            afterTs = rows[^1].Timestamp.UtcTicks;
            afterId = rows[^1].Id;
        }

        Assert.Equal(oracle.Select(Id).ToArray(), walked.Select(Id).ToArray());
    }
}
