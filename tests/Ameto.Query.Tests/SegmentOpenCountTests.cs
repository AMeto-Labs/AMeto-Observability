using Ameto.Core;
using Ameto.Query;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Query.Tests;

/// <summary>
/// A SEGMENT IS MAPPED ONCE PER QUERY, not once per phase.
///
/// <para>The prefilter opened every segment to read its index sections and closed it again;
/// the scan then opened the survivors a second time to read their blocks. Opening is a
/// <c>FileInfo</c> stat, a <c>CreateFromFile</c>, a <c>CreateViewAccessor</c> over the whole
/// file and a block-index read and parse — cheap next to a decode, 40 mappings for a
/// 20-segment query, and every one of them a handle that blocks deletion on Windows. The
/// prefilter's reader is carried to the scan now.</para>
///
/// <para>Counted, not weighed: an exact expected number is the only way to state "once". The
/// counter is process-wide, so this depends on the assembly's <c>DisableTestParallelization</c>
/// — see AssemblyInfo.cs.</para>
/// </summary>
public sealed class SegmentOpenCountTests : IAsyncLifetime
{
    private const int Events = QuerySegmentFixtures.GroupedEvents;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-opencount-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine  = null!;
    private QueryExecutor _query   = null!;
    private string        _segPath = null!;

    public SegmentOpenCountTests(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync() =>
        (_engine, _query, _segPath, _) = await QuerySegmentFixtures.GroupedSegmentAsync(_dir);

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private async Task<(long Opens, List<LogEvent> Rows)> CountOpensAsync(string? filter, int count = Events + 10)
    {
        long before = SegmentReader.Opens;
        var rows = await QuerySegmentFixtures.RunAsync(_query, filter, count);
        return (SegmentReader.Opens - before, rows);
    }

    /// <summary>
    /// A filtered query prefilters the segment and then scans it. That is one mapping, not two.
    /// </summary>
    [Fact]
    public async Task AFilteredQueryOpensEachSegmentOnce()
    {
        await CountOpensAsync("Customer = 'cust-7'");            // warm

        var (opens, rows) = await CountOpensAsync("Customer = 'cust-7'");
        _out.WriteLine($"filtered query over 1 segment: {opens} open(s), {rows.Count} rows");

        Assert.NotEmpty(rows);
        Assert.Equal(1, opens);
    }

    /// <summary>A substring predicate takes the trigram path; still one mapping.</summary>
    [Fact]
    public async Task ASubstringQueryOpensEachSegmentOnce()
    {
        await CountOpensAsync("@mt like '%processed%'");          // warm

        var (opens, rows) = await CountOpensAsync("@mt like '%processed%'");
        _out.WriteLine($"substring query over 1 segment: {opens} open(s), {rows.Count} rows");

        Assert.NotEmpty(rows);
        Assert.Equal(1, opens);
    }

    /// <summary>
    /// A query the prefilter cannot narrow takes the passthrough path and opens nothing before
    /// the scan — so the scan's own open is still the only one.
    /// </summary>
    [Fact]
    public async Task AnUnfilteredQueryStillOpensEachSegmentOnce()
    {
        await CountOpensAsync(null);                              // warm

        var (opens, rows) = await CountOpensAsync(null);
        _out.WriteLine($"unfiltered query over 1 segment: {opens} open(s), {rows.Count} rows");

        Assert.Equal(Events, rows.Count);
        Assert.Equal(1, opens);
    }

    /// <summary>
    /// A segment the prefilter REJECTS is never scanned, so it is opened once and closed at
    /// once — the carried reader must not keep a rejected segment mapped.
    /// </summary>
    [Fact]
    public async Task ARejectedSegmentIsOpenedOnceAndReturnsNothing()
    {
        await CountOpensAsync("OrderId = 'order-nowhere'");       // warm

        var (opens, rows) = await CountOpensAsync("OrderId = 'order-nowhere'");
        _out.WriteLine($"rejected query over 1 segment: {opens} open(s), {rows.Count} rows");

        Assert.Empty(rows);
        Assert.Equal(1, opens);
    }

    /// <summary>
    /// The lifetime claim, and the one that matters beyond speed: when the query is finished,
    /// nothing is holding the file. On Windows a mapped file cannot be deleted, and retention
    /// and the merge delete segments while queries run, so a reader carried from the prefilter
    /// to the scan must be closed by the end of the query.
    ///
    /// <para>SCOPE, because the fixture is one segment and one segment always primes: what
    /// this covers is the reader a DRAINED iterator borrowed, and the early-stop page. The
    /// other half — a survivor the prefilter opened whose scan never ran, which only the
    /// merge's own finally can close — needs a catalog the page cannot exhaust, and is
    /// asserted over 40 segments by
    /// <c>Ameto.Perf.LazySegmentPrimingProbe.AFilteredPageMapsEachSurvivingSegmentOnce</c>.</para>
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("Customer = 'cust-7'")]
    [InlineData("@mt like '%processed%'")]
    public async Task NoReaderOutlivesTheQuery(string? filter)
    {
        // A SHORT page: the merge stops priming as soon as the heap can serve it, which is
        // exactly the case where a carried reader has no iterator to close it.
        var page = await QuerySegmentFixtures.RunAsync(_query, filter, 5);
        Assert.NotEmpty(page);

        // …and a full read, which primes and drains everything.
        var all = await QuerySegmentFixtures.RunAsync(_query, filter, Events + 10);
        Assert.NotEmpty(all);

        // Renaming is the honest test of "no handle left" — it is what Windows refuses while
        // any mapping is open, and it is the operation retention and the merge need.
        string moved = _segPath + ".moved";
        File.Move(_segPath, moved);
        File.Move(moved, _segPath);
    }
}
