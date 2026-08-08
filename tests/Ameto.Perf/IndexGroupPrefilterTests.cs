using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// The query side of index groups: the prefilter now runs PER GROUP, and it must find
/// everything a per-file prefilter found while touching less of the file.
///
/// <para>Why per group at all: one bloom sized for ~10 bits/term over a whole day answers
/// "maybe" to every query, so a day-scale segment would survive every fast-skip and the
/// prefilter would stop being a filter. Per group the filter keeps the selectivity it is
/// sized for — but only if candidate ordinals from different groups still resolve to the
/// right rows, which is what these tests pin down.</para>
/// </summary>
public sealed class IndexGroupPrefilterTests : IAsyncLifetime
{
    private const int  Events      = 6_000;
    private const long GroupBudget = 512 * 1024;   // several groups out of one small segment

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-groupquery-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine = null!;
    private QueryExecutor _query  = null!;
    private string        _segPath = null!;
    private long          _baseTicks;

    public IndexGroupPrefilterTests(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engine._groupPayloadBudgetBytes = GroupBudget;
        // The same wiring IndexingWiring installs in production: a fresh builder per group,
        // posting offsets based at the group's first FILE ordinal.
        _engine.IndexSinkFactory = (estimatedEventCount, termsPerEvent) =>
            new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);
        _query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        _baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        int tmplIdx = _engine.TemplatePool.Intern("order {OrderId} processed for {Customer}");
        int svcIdx  = _engine.TemplatePool.Intern("Svc.Orders");

        var buf = new ArrayBufferWriter<byte>(512);
        for (int i = 0; i < Events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(3);
            w.Write("OrderId");  w.Write("order-" + i);                 // unique per event
            w.Write("Customer"); w.Write("cust-" + (i % 40));           // low cardinality
            w.Write("pad");      w.Write(new string((char)('a' + i % 26), 220));
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = _baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(_engine.TryWrite(h, buf.WrittenSpan.ToArray()));
        }
        await _engine.FlushHotTierAsync();

        var segs = _engine.ListSegments();
        Assert.Single(segs);
        _segPath = segs[0].FilePath;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private int GroupCount()
    {
        using var reader = SegmentReader.Open(_segPath);
        return reader.Groups.Length;
    }

    [Fact]
    public void TheFlushProducedAMultiGroupSegment()
    {
        int groups = GroupCount();
        _out.WriteLine($"{Events} events → {groups} index groups at a {GroupBudget / 1024} KB budget");
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
        Assert.Equal($"order-{n}", OrderIdOf(got[0]));
    }

    /// <summary>A predicate that spans every group must return every matching row.</summary>
    [Fact]
    public async Task AnEqualityHitSpreadOverAllGroupsReturnsEveryRow()
    {
        var got = await RunAsync("Customer = 'cust-7'");
        var expected = Enumerable.Range(0, Events).Where(i => i % 40 == 7).Select(i => $"order-{i}").OrderBy(s => s).ToList();
        Assert.Equal(expected.Count, got.Count);
        Assert.Equal(expected, got.Select(OrderIdOf).OrderBy(s => s).ToList());
    }

    /// <summary>Substring search resolves through the per-group trigram sections.</summary>
    [Fact]
    public async Task ASubstringHitIsFoundAcrossGroups()
    {
        var got = await RunAsync("OrderId like '%order-4321%'");
        Assert.Single(got);
        Assert.Equal("order-4321", OrderIdOf(got[0]));
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
                          .Select(OrderIdOf).OrderBy(s => s).ToList();
        Assert.NotEmpty(expected);
        Assert.Equal(expected, windowed.Select(OrderIdOf).OrderBy(s => s).ToList());
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
        Assert.Equal(Events, got.Select(OrderIdOf).Distinct().Count());
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
    /// The selectivity claim. A per-file prefilter over a multi-group segment would have to
    /// read the whole file's inverted+trigram sections; per group, a unique value survives in
    /// one group and the rest are dropped on a few KB of bloom. Allocation is the proxy: the
    /// sections are what get deserialised into dictionaries and int[].
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

        _out.WriteLine($"groups            : {GroupCount()}");
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

    /// <summary>Identity by payload, not by EventId — the engine assigns ids itself.</summary>
    private static string OrderIdOf(LogEvent ev) => ev.Properties?["OrderId"] as string ?? "<none>";

    private async Task<List<LogEvent>> RunAsync(
        string? filter, DateTimeOffset? from = null, DateTimeOffset? to = null, int count = Events + 10)
    {
        var res = new List<LogEvent>();
        await foreach (var ev in _query.ExecuteAsync(new QueryRequest
        {
            Filter    = filter,
            Count     = count,
            FromUtc   = from,
            ToUtc     = to,
            Direction = QueryDirection.Backward,
        }))
        {
            res.Add(ev);
            if (res.Count >= count) break;
        }
        return res;
    }
}
