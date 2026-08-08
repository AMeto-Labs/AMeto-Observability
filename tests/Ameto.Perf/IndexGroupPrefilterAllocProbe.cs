using Ameto.Core;
using Ameto.Query;
using Ameto.Query.Tests;   // QuerySegmentFixtures, compiled in from the query suite (see .csproj)
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What the per-group prefilter SAVES, over the same segment
/// <c>Ameto.Query.Tests.IndexGroupPrefilterTests</c> asserts correctness on.
///
/// <para>These stayed out of the functional suite deliberately. Both compare two readings of
/// <c>GC.GetTotalAllocatedBytes</c> — a process-wide counter — against a hard ratio, so they
/// fail for reasons that have nothing to do with the prefilter: a background collection, a
/// machine under load, another class allocating in the window. That is the right trade for a
/// number you go and read, and the wrong one for a gate on every commit.</para>
/// </summary>
public sealed class IndexGroupPrefilterAllocProbe : IAsyncLifetime
{
    private const int Events = QuerySegmentFixtures.GroupedEvents;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-groupalloc-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine  = null!;
    private QueryExecutor _query   = null!;
    private string        _segPath = null!;

    public IndexGroupPrefilterAllocProbe(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync() =>
        (_engine, _query, _segPath, _) = await QuerySegmentFixtures.GroupedSegmentAsync(_dir);

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>
    /// The selectivity claim. A per-file prefilter over a multi-group segment would have to
    /// read the whole file's inverted+trigram sections; per group, a unique value survives in
    /// one group and the rest are dropped on their bloom alone. Allocation is the proxy: the
    /// sections are what get deserialised into dictionaries and int[].
    ///
    /// <para>"Their bloom alone" is the whole saving, and it is a factor of four to six rather
    /// than the orders of magnitude it used to be described as — MEASURED by
    /// <c>BloomSizingProbe</c>, bloom is 15.6 % of a prop-dense group's three index sections and
    /// 26.6 % of a thin one's. This fixture shrinks the group budget to a test value, so its own
    /// sections are far smaller than a production file's and the ratio it prints is not that
    /// number.</para>
    /// </summary>
    [Fact]
    public async Task AUniqueValueCostsFarLessThanReadingTheSegment()
    {
        await RunAsync("OrderId = 'order-4321'");   // warm
        await RunAsync(null);

        // Process-wide, not per-thread: the prefilter runs under Parallel.ForEachAsync, so
        // GetAllocatedBytesForCurrentThread misses exactly the work being measured. The
        // assembly disables test parallelisation, so this counter is attributable.
        long b0 = GC.GetTotalAllocatedBytes(precise: true);
        await RunAsync("OrderId = 'order-4321'");
        long hinted = GC.GetTotalAllocatedBytes(precise: true) - b0;

        long b1 = GC.GetTotalAllocatedBytes(precise: true);
        await RunAsync(null);
        long full = GC.GetTotalAllocatedBytes(precise: true) - b1;

        _out.WriteLine($"groups            : {QuerySegmentFixtures.GroupCountOf(_segPath)}");
        _out.WriteLine($"unique-value query: {hinted / 1024.0:F0} KB");
        _out.WriteLine($"unfiltered read   : {full / 1024.0:F0} KB   ({(double)full / hinted:F1}x)");

        Assert.True(hinted * 3 < full,
            $"per-group prefilter is not narrowing: {hinted} B for one row vs {full} B for {Events}");
    }

    /// <summary>
    /// An unfiltered query must not pay for grouping. It takes the passthrough path — no
    /// segment is even opened during prefiltering — so a small page must still cost a small
    /// fraction of a full read.
    /// </summary>
    [Fact]
    public async Task AnUnfilteredPageDoesNotPayForTheGroups()
    {
        await RunAsync(null, count: 5);
        await RunAsync(null);

        long b0 = GC.GetTotalAllocatedBytes(precise: true);
        await RunAsync(null, count: 5);
        long page = GC.GetTotalAllocatedBytes(precise: true) - b0;

        long b1 = GC.GetTotalAllocatedBytes(precise: true);
        await RunAsync(null);
        long full = GC.GetTotalAllocatedBytes(precise: true) - b1;

        _out.WriteLine($"page of 5      : {page / 1024.0:F0} KB");
        _out.WriteLine($"full read      : {full / 1024.0:F0} KB   ({(double)full / page:F1}x)");
        Assert.True(page * 10 < full, $"an unfiltered page got expensive: {page} B vs {full} B");
    }

    private Task<List<LogEvent>> RunAsync(string? filter, int count = Events + 10) =>
        QuerySegmentFixtures.RunAsync(_query, filter, count);
}
