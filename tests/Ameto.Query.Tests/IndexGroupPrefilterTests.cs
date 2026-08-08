using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Query.Tests;

/// <summary>
/// The query side of index groups: the prefilter now runs PER GROUP, and it must find
/// everything a per-file prefilter found while touching less of the file.
///
/// <para>Why per group at all: one bloom sized for ~10 bits/term over a whole day answers
/// "maybe" to every query, so a day-scale segment would survive every fast-skip and the
/// prefilter would stop being a filter. Per group the filter keeps the selectivity it is
/// sized for — but only if candidate ordinals from different groups still resolve to the
/// right rows, which is what these tests pin down.</para>
///
/// <para>The allocation-ratio probes over this same fixture stayed in <c>Ameto.Perf</c>
/// (<c>IndexGroupPrefilterAllocProbe</c>): they assert a ratio between two process-wide
/// counters, which is a different kind of claim from "this query returns these rows" and
/// fails for different reasons. What is here asserts behaviour, including the two claims
/// that happen to be counted rather than compared — a section read exactly twice per group,
/// and exactly one group's bloom saying maybe.</para>
/// </summary>
public sealed class IndexGroupPrefilterTests : IAsyncLifetime
{
    private const int Events = QuerySegmentFixtures.GroupedEvents;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-groupquery-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine  = null!;
    private QueryExecutor _query   = null!;
    private string        _segPath = null!;
    private long          _baseTicks;

    public IndexGroupPrefilterTests(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync() =>
        (_engine, _query, _segPath, _baseTicks) = await QuerySegmentFixtures.GroupedSegmentAsync(_dir);

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private int GroupCount() => QuerySegmentFixtures.GroupCountOf(_segPath);

    [Fact]
    public void TheFlushProducedAMultiGroupSegment()
    {
        int groups = GroupCount();
        _out.WriteLine($"{Events} events → {groups} index groups at a {QuerySegmentFixtures.GroupBudget / 1024} KB budget");
        Assert.True(groups >= 4, $"only {groups} group(s) — the rest of this fixture proves nothing");
    }

    /// <summary>
    /// A value that exists in exactly one group must be found — and the groups that cannot
    /// hold it must be rejected on their own bloom, before their multi-MB sections are read.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2_999)]
    [InlineData(Events - 1)]
    public async Task AnEqualityHitIsFoundWhicheverGroupHoldsIt(int n)
    {
        var got = await RunAsync($"OrderId = 'order-{n}'");
        Assert.Single(got);
        Assert.Equal($"order-{n}", QuerySegmentFixtures.OrderIdOf(got[0]));
    }

    /// <summary>A predicate that spans every group must return every matching row.</summary>
    [Fact]
    public async Task AnEqualityHitSpreadOverAllGroupsReturnsEveryRow()
    {
        var got = await RunAsync("Customer = 'cust-7'");
        var expected = Enumerable.Range(0, Events).Where(i => i % 40 == 7).Select(i => $"order-{i}").OrderBy(s => s).ToList();
        Assert.Equal(expected.Count, got.Count);
        Assert.Equal(expected, got.Select(QuerySegmentFixtures.OrderIdOf).OrderBy(s => s).ToList());
    }

    /// <summary>Substring search resolves through the per-group trigram sections.</summary>
    [Fact]
    public async Task ASubstringHitIsFoundAcrossGroups()
    {
        var got = await RunAsync("OrderId like '%order-4321%'");
        Assert.Single(got);
        Assert.Equal("order-4321", QuerySegmentFixtures.OrderIdOf(got[0]));
    }

    /// <summary>An equality predicate combined with a window must still hit the right rows.</summary>
    [Fact]
    public async Task AWindowedEqualityQueryAgreesWithAnUnwindowedOne()
    {
        var all = await RunAsync("Customer = 'cust-7'");

        var from = new DateTimeOffset(_baseTicks + 1_000 * TimeSpan.TicksPerSecond, TimeSpan.Zero);
        var to   = new DateTimeOffset(_baseTicks + 3_000 * TimeSpan.TicksPerSecond, TimeSpan.Zero);
        var windowed = await RunAsync("Customer = 'cust-7'", from, to);

        var expected = all.Where(e => e.Timestamp >= from && e.Timestamp <= to)
                          .Select(QuerySegmentFixtures.OrderIdOf).OrderBy(s => s).ToList();
        Assert.NotEmpty(expected);
        Assert.Equal(expected, windowed.Select(QuerySegmentFixtures.OrderIdOf).OrderBy(s => s).ToList());
    }

    /// <summary>A miss must stay a miss — grouping must not turn a rejection into a scan.</summary>
    [Fact]
    public async Task AValueInNoGroupReturnsNothing()
    {
        Assert.Empty(await RunAsync("OrderId = 'order-does-not-exist'"));
        Assert.Empty(await RunAsync("Customer = 'cust-nobody'"));
    }

    [Fact]
    public async Task AnUnfilteredQueryStillReturnsEverything()
    {
        var got = await RunAsync(null);
        Assert.Equal(Events, got.Count);
        Assert.Equal(Events, got.Select(QuerySegmentFixtures.OrderIdOf).Distinct().Count());
    }

    /// <summary>
    /// The selectivity claim, measured on the blooms themselves rather than through the
    /// query: a value that lives in one group must read as "maybe" in that group and be
    /// REJECTED by the others. One filter over the whole file cannot do this by construction —
    /// which is why a day-scale segment used to survive every fast-skip.
    /// </summary>
    [Fact]
    public void OnlyTheGroupHoldingAValueSaysMaybe()
    {
        using var reader = SegmentReader.Open(_segPath);
        var groups = reader.Groups;
        Assert.True(groups.Length >= 4);

        int surviving = 0, owner = -1;
        for (int g = 0; g < groups.Length; g++)
        {
            using var sec   = reader.RentBloomFilterBytes(g);
            using var bloom = SegmentBloomFilter.Deserialise(sec.Span);
            if (bloom.MightContain("order-4321")) { surviving++; owner = g; }
        }

        _out.WriteLine($"'order-4321': {surviving} of {groups.Length} group blooms said maybe (group {owner})");
        Assert.Equal(1, surviving);

        // …and the surviving group is the one that actually owns that row.
        var grp = groups[owner];
        var ordinals = OrdinalsOf(reader, "order-4321");
        Assert.Single(ordinals);
        Assert.InRange(ordinals[0], grp.FirstOrdinal, grp.FirstOrdinal + grp.EventCount - 1);
    }

    private static uint[] OrdinalsOf(SegmentReader reader, string orderId)
    {
        for (int g = 0; g < reader.Groups.Length; g++)
        {
            using var sec = reader.RentInvertedIndexBytes(g);
            using var idx = SegmentIndexReader.Load(sec.Span, default, default);
            var hit = idx.Lookup("OrderId", orderId);
            if (hit is { Length: > 0 }) return hit;
        }
        return [];
    }

    /// <summary>
    /// The BLOOM SECTION IS RENTED ONCE PER GROUP, not once per phase. Both phases need the same
    /// bytes, and renting them twice is invisible to every other instrument here: under 1 MB the
    /// pool absorbs the second rent, which is exactly the size these tests run at. Over 1 MB —
    /// where a production 64 MB-group file's sections live — <c>ArrayPool&lt;byte&gt;.Shared</c>
    /// stops pooling and serves each rent from a fresh LOH allocation that Return drops, so the
    /// redundant call becomes the most expensive thing on the prefilter path, once per group, per
    /// segment, in parallel across the catalog.
    ///
    /// <para>Counted rather than weighed, for that reason — an exact expected number, not a
    /// ratio, which is why this stayed with the functional tests when the allocation probes
    /// moved out. The predicate is an equality on a low-cardinality value that every group holds
    /// and no group can reject, so every group runs both phases: bloom once, then inverted.
    /// Trigram is not read at all — the filter has no substring predicate — which is the other
    /// thing this pins.</para>
    ///
    /// <para><c>PooledSectionRents</c> is process-wide, so this needs the assembly's
    /// <c>DisableTestParallelization</c> to be attributable — see AssemblyInfo.cs.</para>
    /// </summary>
    [Fact]
    public async Task EveryGroupReadsItsBloomSectionOnce()
    {
        await RunAsync("Customer = 'cust-7'");   // warm
        int groups = GroupCount();

        long before = SegmentReader.PooledSectionRents;
        await RunAsync("Customer = 'cust-7'");
        long rents = SegmentReader.PooledSectionRents - before;

        _out.WriteLine($"{groups} groups → {rents} section rents ({rents / (double)groups:F1} per group)");
        Assert.Equal(2L * groups, rents);
    }

    private Task<List<LogEvent>> RunAsync(
        string? filter, DateTimeOffset? from = null, DateTimeOffset? to = null, int count = Events + 10) =>
        QuerySegmentFixtures.RunAsync(_query, filter, count, from, to);
}
