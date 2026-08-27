using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// An aggregation is only worth having if its numbers are the ones you would get by counting
/// the events yourself. So every case here computes both: the aggregation, and the same figure
/// derived from streaming the matching events out of the executor. The corpus spans both tiers,
/// several services and levels, a numeric property that is absent from some events, and one
/// service that only ever logs at one level — so at least one group is legitimately empty.
/// </summary>
public sealed class AggregationEquivalenceTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 24, 9, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddHours(2);

    private static readonly string[]   Services = ["checkout", "billing", "gateway"];
    private static readonly LogLevel[] Levels   =
        [LogLevel.Verbose, LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error, LogLevel.Fatal];

    private readonly string        _dir = Path.Combine(Path.GetTempPath(), "ameto-agg-" + Guid.NewGuid().ToString("N"));
    private readonly StorageEngine _engine;
    private readonly QueryExecutor _query;
    private readonly AggregationExecutor _agg;

    public AggregationEquivalenceTests()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine  = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        _query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        _agg   = new AggregationExecutor(_query);

        var buf = new ArrayBufferWriter<byte>(128);
        void Write(int i)
        {
            string service = Services[i % 3];
            var    level   = service == "gateway" ? LogLevel.Information : Levels[(i / 3) % Levels.Length];

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            // Every third event has no Elapsed at all — an absent value must contribute to no
            // sum, no average and no minimum, rather than counting as a zero.
            bool hasElapsed = i % 3 != 0;
            w.WriteMapHeader(hasElapsed ? 2 : 1);
            w.Write("n");       w.Write((long)i);
            if (hasElapsed) { w.Write("Elapsed"); w.Write((double)(i % 50)); }
            w.Flush();

            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = Base.UtcTicks + i * TimeSpan.TicksPerSecond,
                Level                    = level,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern(service),
            }, buf.WrittenSpan.ToArray()));
        }

        for (int i = 0; i < 200; i++) Write(i);
        _engine.FlushHotTierAsync().GetAwaiter().GetResult();   // cold tier
        for (int i = 200; i < 320; i++) Write(i);               // …and hot tier
    }

    public void Dispose()
    {
        try { _engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<AggregationResult> RunAsync(string text)
    {
        Assert.True(AggregationParser.TryParse(text, out var q));
        return await _agg.ExecuteAsync(q!, From, To);
    }

    /// <summary>Every event the same where-clause matches, materialised — the thing to agree with.</summary>
    private async Task<List<LogEvent>> ScanAsync(string? filter)
    {
        var events = new List<LogEvent>();
        await foreach (var ev in _query.ExecuteAsync(new QueryRequest
        {
            Filter    = filter,
            FromUtc   = From,
            ToUtc     = To,
            Count     = 1_000_000,
            Direction = QueryDirection.Backward,
        }))
        {
            events.Add(ev);
        }
        return events;
    }

    private static double? Elapsed(LogEvent ev)
        => FilterEvaluator.ReadProperty(ev, "Elapsed") is { } v && v is double d ? d : null;

    // ── Scalar ────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("select count(*)",                             null)]
    [InlineData("select count(*) where @l = 'Error'",          "@l = 'Error'")]
    [InlineData("select count(*) where ['service.name'] = 'checkout'", "['service.name'] = 'checkout'")]
    [InlineData("select count(*) where @l = 'Fatal' and ['service.name'] = 'gateway'",
                "@l = 'Fatal' and ['service.name'] = 'gateway'")]     // deliberately empty
    public async Task A_count_equals_the_number_of_events_the_same_filter_returns(string text, string? filter)
    {
        var result = await RunAsync(text);
        var scan   = await ScanAsync(filter);

        Assert.False(result.Partial);
        var row = Assert.Single(result.Rows);
        Assert.Equal((double)scan.Count, row.Values[0]);
    }

    [Fact]
    public async Task Sum_min_max_and_average_equal_the_same_figures_over_the_scan()
    {
        var result = await RunAsync("select sum(Elapsed), min(Elapsed), max(Elapsed), avg(Elapsed), count(Elapsed)");
        var values = Assert.Single(result.Rows).Values;

        var numbers = (await ScanAsync(null)).Select(Elapsed).Where(v => v is not null).Select(v => v!.Value).ToArray();
        Assert.NotEmpty(numbers);

        Assert.Equal(numbers.Sum(),     values[0]!.Value, 6);
        Assert.Equal(numbers.Min(),     values[1]!.Value, 6);
        Assert.Equal(numbers.Max(),     values[2]!.Value, 6);
        Assert.Equal(numbers.Average(), values[3]!.Value, 6);
        // count(P) counts events that HAVE the property, which is not every event scanned.
        Assert.Equal((double)numbers.Length, values[4]);
        Assert.True(numbers.Length < (await ScanAsync(null)).Count, "the corpus should exercise absent values");
    }

    [Fact]
    public async Task An_aggregate_over_no_numbers_is_null_rather_than_zero()
    {
        // A group with nothing to average has no average. Reporting 0 would be a number the
        // data does not contain, and it would sort and chart as if it were real.
        var result = await RunAsync("select avg(Elapsed), min(Elapsed), sum(Elapsed) where Nonexistent = 'x'");

        var row = Assert.Single(result.Rows);
        Assert.All(row.Values, v => Assert.Null(v));
    }

    // ── Grouped ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task Grouping_by_level_partitions_the_same_events()
    {
        var result = await RunAsync("select count(*) group by @l limit 100");
        var scan   = await ScanAsync(null);

        var expected = scan.GroupBy(e => e.Level.ToSeqString())
                           .ToDictionary(g => g.Key, g => (double)g.Count());

        Assert.Equal(expected.Count, result.Rows.Count);
        foreach (var row in result.Rows)
            Assert.Equal(expected[row.Key[0]!], row.Values[0]);

        // Nothing is lost or double-counted by the partition.
        Assert.Equal((double)scan.Count, result.Rows.Sum(r => r.Values[0]!.Value));
    }

    [Fact]
    public async Task Grouping_by_two_keys_matches_the_pairs_in_the_data()
    {
        var result = await RunAsync("select count(*) group by ['service.name'], @l limit 100");
        var scan   = await ScanAsync(null);

        var expected = scan.GroupBy(e => (e.ServiceName ?? "", e.Level.ToSeqString()))
                           .ToDictionary(g => g.Key, g => (double)g.Count());

        Assert.Equal(expected.Count, result.Rows.Count);
        foreach (var row in result.Rows)
            Assert.Equal(expected[(row.Key[0]!, row.Key[1]!)], row.Values[0]);
    }

    [Fact]
    public async Task A_where_clause_narrows_the_groups_the_same_way_it_narrows_a_search()
    {
        var result = await RunAsync("select count(*) where @l = 'Error' group by ['service.name']");
        var scan   = await ScanAsync("@l = 'Error'");

        var expected = scan.GroupBy(e => e.ServiceName ?? "").ToDictionary(g => g.Key, g => (double)g.Count());

        Assert.Equal(expected.Count, result.Rows.Count);
        foreach (var row in result.Rows)
            Assert.Equal(expected[row.Key[0]!], row.Values[0]);
    }

    [Fact]
    public async Task An_absent_key_is_its_own_group_and_is_reported_as_absent()
    {
        // Grouping by something two thirds of the events do not carry: those events must land
        // in one group with a null key, not be dropped and not be merged into "".
        var result = await RunAsync("select count(*) group by Elapsed limit 100");
        var scan   = await ScanAsync(null);

        int missing = scan.Count(e => Elapsed(e) is null);
        Assert.True(missing > 0);

        var nullRow = Assert.Single(result.Rows.Where(r => r.Key[0] is null));
        Assert.Equal((double)missing, nullRow.Values[0]);
    }

    // ── Ordering and limits ───────────────────────────────────────────────────

    [Fact]
    public async Task Rows_come_back_largest_first_and_the_limit_takes_from_the_top()
    {
        var all = await RunAsync("select count(*) group by @l limit 100");
        var top = await RunAsync("select count(*) group by @l limit 2");

        var ordered = all.Rows.Select(r => r.Values[0]!.Value).ToArray();
        Assert.Equal(ordered.OrderByDescending(v => v).ToArray(), ordered);

        Assert.Equal(2, top.Rows.Count);
        Assert.Equal(ordered.Take(2), top.Rows.Select(r => r.Values[0]!.Value));
        // The count of distinct groups is what was FOUND, not what was returned — otherwise a
        // limited answer would look complete.
        Assert.Equal(all.Rows.Count, top.GroupsFound);
    }
}
