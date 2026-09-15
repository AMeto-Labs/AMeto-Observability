using Ameto.Core;
using Ameto.Query;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Query.Tests;

/// <summary>
/// A SEGMENT IS MAPPED ONCE PER QUERY, not once per phase — and every mapping is released.
///
/// <para>The prefilter opened every segment to read its index sections and closed it again;
/// the scan then opened the survivors a second time to read their blocks. Opening is a
/// <c>FileInfo</c> stat, a <c>CreateFromFile</c>, a <c>CreateViewAccessor</c> over the whole
/// file and a block-index read and parse — cheap next to a decode, 40 mappings for a
/// 20-segment query, and every one of them a handle that blocks deletion on Windows. The
/// prefilter's reader is carried to the scan now.</para>
///
/// <para>Carrying it made the release path three paths: the scan's iterator, the merge's
/// finally for survivors that never primed, and the prefilter's own finally for a segment it
/// rejects. Every case below therefore asserts Closes against Opens as well as the open
/// count, so a path that stops disposing fails here and not only in a Perf probe whose filter
/// every segment survives.</para>
///
/// <para>Counted, not weighed: an exact expected number is the only way to state "once". The
/// counters are process-wide, so this depends on the assembly's
/// <c>DisableTestParallelization</c> — see AssemblyInfo.cs.</para>
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

    private async Task<(long Opens, long Closes, List<LogEvent> Rows)> CountAsync(string? filter, int count = Events + 10)
    {
        long opens = SegmentReader.Opens, closes = SegmentReader.Closes;
        var rows = await QuerySegmentFixtures.RunAsync(_query, filter, count);
        return (SegmentReader.Opens - opens, SegmentReader.Closes - closes, rows);
    }

    /// <summary>
    /// A filtered query prefilters the segment and then scans it. That is one mapping, not two.
    /// </summary>
    [Fact]
    public async Task AFilteredQueryOpensEachSegmentOnce()
    {
        await CountAsync("Customer = 'cust-7'");                 // warm

        var (opens, closes, rows) = await CountAsync("Customer = 'cust-7'");
        _out.WriteLine($"filtered query over 1 segment: {opens} open(s), {closes} close(s), {rows.Count} rows");

        Assert.NotEmpty(rows);
        Assert.Equal(1, opens);
        Assert.Equal(opens, closes);
    }

    /// <summary>A substring predicate takes the trigram path; still one mapping.</summary>
    [Fact]
    public async Task ASubstringQueryOpensEachSegmentOnce()
    {
        await CountAsync("@mt like '%processed%'");               // warm

        var (opens, closes, rows) = await CountAsync("@mt like '%processed%'");
        _out.WriteLine($"substring query over 1 segment: {opens} open(s), {closes} close(s), {rows.Count} rows");

        Assert.NotEmpty(rows);
        Assert.Equal(1, opens);
        Assert.Equal(opens, closes);
    }

    /// <summary>
    /// A query the prefilter cannot narrow takes the passthrough path and opens nothing before
    /// the scan — so the scan's own open is still the only one.
    /// </summary>
    [Fact]
    public async Task AnUnfilteredQueryStillOpensEachSegmentOnce()
    {
        await CountAsync(null);                                   // warm

        var (opens, closes, rows) = await CountAsync(null);
        _out.WriteLine($"unfiltered query over 1 segment: {opens} open(s), {closes} close(s), {rows.Count} rows");

        Assert.Equal(Events, rows.Count);
        Assert.Equal(1, opens);
        Assert.Equal(opens, closes);
    }

    /// <summary>
    /// A segment the prefilter REJECTS is never scanned, so it is opened once and closed at
    /// once — the carried reader must not keep a rejected segment mapped.
    ///
    /// <para>Nothing downstream ever sees a rejected segment, so the prefilter body's own
    /// <c>finally</c> is the only thing that closes it, and the Perf probe cannot reach that
    /// path: its filter is one all 40 segments survive. Two ways in: an <c>OrderId</c> that
    /// exists nowhere (the bloom, or failing that the inverted index, drops every group), and
    /// a level the all-Information segment does not hold (the <c>@l</c> bloom drops every
    /// group before a big section is read).</para>
    /// </summary>
    [Theory]
    [InlineData("OrderId = 'order-nowhere'")]
    [InlineData("@l = 'Error'")]
    public async Task ARejectedSegmentIsOpenedOnceClosedOnceAndReturnsNothing(string filter)
    {
        await CountAsync(filter);                                 // warm

        var (opens, closes, rows) = await CountAsync(filter);
        _out.WriteLine($"rejected query `{filter}` over 1 segment: {opens} open(s), {closes} close(s), {rows.Count} rows");

        Assert.Empty(rows);
        Assert.Equal(1, opens);
        Assert.Equal(opens, closes);
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
        var (pageOpens, pageCloses, page) = await CountAsync(filter, 5);
        Assert.NotEmpty(page);
        Assert.Equal(pageOpens, pageCloses);

        // …and a full read, which primes and drains everything.
        var (allOpens, allCloses, all) = await CountAsync(filter);
        Assert.NotEmpty(all);
        Assert.Equal(allOpens, allCloses);

        // Renaming is the honest test of "no handle left" — it is what Windows refuses while
        // any mapping is open, and it is the operation retention and the merge need.
        string moved = _segPath + ".moved";
        File.Move(_segPath, moved);
        File.Move(moved, _segPath);
    }

    /// <summary>
    /// A CLOSE IS COUNTED ONCE PER READER, however many times it is disposed. Every assertion
    /// above compares Closes with Opens, so a reader that counts a second Dispose would push
    /// Closes past Opens and fail them for a reason that has nothing to do with a leak.
    /// <c>DisposeReaders</c> is documented as idempotent per slot and every release path in the
    /// executor is best-effort, so a second dispose is a thing the code allows.
    ///
    /// <para>SCOPE: this pins the sequential case. The guard is also atomic
    /// (<c>Interlocked.Exchange</c>) so that two threads disposing at once count once, but that
    /// race is a two-instruction window no test here can hit on demand; it is argued in the
    /// field's doc, not measured.</para>
    /// </summary>
    [Fact]
    public void AReaderDisposedTwiceIsCountedClosedOnce()
    {
        long opens = SegmentReader.Opens, closes = SegmentReader.Closes;

        var reader = SegmentReader.Open(_segPath);
        reader.Dispose();
        reader.Dispose();

        Assert.Equal(1, SegmentReader.Opens - opens);
        Assert.Equal(1, SegmentReader.Closes - closes);
    }
}
