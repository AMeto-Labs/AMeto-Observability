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
/// A <c>count(*)</c> whose grouping and where-clause live entirely in the event header is
/// answered by the parallel header scan instead of the ordered event scan. Two roads to one
/// number is a standing invitation for them to drift, so every query below is run BOTH ways —
/// with the header scan wired in and with it absent — and the tables must be identical.
///
/// <para>The corpus spans both tiers, every level, a user property the header knows nothing
/// about, and six producers chosen for the ways a header aggregator and an ordinal group-by can
/// disagree: two differing only in CASING, one named with the EMPTY STRING, one with no service
/// at all. So the tests cover what the shortcut answers and — just as load-bearing — what it
/// has to refuse.</para>
/// </summary>
public sealed class AggregationHeaderPathTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 24, 9, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddHours(4);

    /// <summary>
    /// Six producers, chosen for the disagreements they expose: two that differ only in CASING,
    /// one whose name is the EMPTY STRING, and one with no service at all. The header
    /// aggregator folds the last three together and the first two into one; the scan road keeps
    /// all six apart. That is why grouping by service stays on the scan road.
    /// </summary>
    private static readonly string?[]  Services = ["checkout", "billing", "Billing", "", "gateway", null];
    private static readonly LogLevel[] Levels   =
        [LogLevel.Verbose, LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error, LogLevel.Fatal];

    private readonly string        _dir = Path.Combine(Path.GetTempPath(), "ameto-agghdr-" + Guid.NewGuid().ToString("N"));
    private readonly StorageEngine _engine;
    private readonly QueryExecutor _query;

    /// <summary>With the shortcut available…</summary>
    private readonly AggregationExecutor _withHeader;

    /// <summary>…and without it, which is the road every existing aggregation test takes.</summary>
    private readonly AggregationExecutor _scanOnly;

    public AggregationHeaderPathTests()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine  = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        _query      = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        _withHeader = new AggregationExecutor(_query, headerScan: _engine);
        _scanOnly   = new AggregationExecutor(_query);

        var buf = new ArrayBufferWriter<byte>(128);
        void Write(int i)
        {
            string? service = Services[i % Services.Length];
            var     level   = Levels[(i / Services.Length) % Levels.Length];

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("n"); w.Write((long)i);
            w.Flush();

            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = Base.UtcTicks + i * TimeSpan.TicksPerSecond,
                Level                    = level,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
                ServiceNamePoolIndex     = service is null ? -1 : _engine.TemplatePool.Intern(service),
            }, buf.WrittenSpan.ToArray()));
        }

        for (int i = 0; i < 240; i++) Write(i);
        _engine.FlushHotTierAsync().GetAwaiter().GetResult();   // cold tier
        for (int i = 240; i < 400; i++) Write(i);               // …and hot tier
    }

    public void Dispose()
    {
        try { _engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // ── Running out of time on the header road ────────────────────────────────

    /// <summary>
    /// The header road owns the whole query budget, so when it runs out of time the counts it
    /// had already merged are gone. What must NOT happen then is the answer becoming zero.
    ///
    /// <para>It did. The cancelled header road returned null, which falls through to the scan
    /// road — with the SAME token, already cancelled. That road yields nothing, and for a
    /// `count(*)` with no `group by` it seeds one group with a count of 0 before the first
    /// event and reports it. The client was shown 0 events, flagged partial, after the server
    /// had spent sixty seconds counting a real number; before the header road existed, the
    /// scan reported what it had counted when the budget expired, which is a floor but a true
    /// one. An already-cancelled token reproduces the timeout deterministically: the cold leg
    /// runs its segments under Parallel.ForEach with the token in ParallelOptions, which
    /// throws, and this fixture writes a cold tier.</para>
    /// </summary>
    [Fact]
    public async Task A_header_count_that_runs_out_of_time_answers_no_rows_rather_than_zero()
    {
        Assert.True(AggregationParser.TryParse("select count(*)", out var q));

        using var spent = new CancellationTokenSource();
        spent.Cancel();

        var result = await _withHeader.ExecuteAsync(q!, From, To, spent.Token);

        Assert.True(result.Partial);
        Assert.NotNull(result.PartialReason);
        Assert.Empty(result.Rows);
    }

    // ── The two roads answer the same table ───────────────────────────────────

    [Theory]
    // Answered by the header scan.
    [InlineData("select count(*)")]
    [InlineData("select count(*) group by @l")]
    [InlineData("select count(*) where ['service.name'] = 'billing'")]
    [InlineData("select count(*) where ['service.name'] = 'billing' group by @l")]
    // Declined by the header scan, and therefore identical for a duller reason.
    [InlineData("select count(*) where @l = 'Error'")]                           // a level the scan's index narrows by
    [InlineData("select count(*) where @l = 'Error' group by @l")]
    [InlineData("select count(*) where @l in ['Error','Fatal'] group by @l")]
    [InlineData("select count(*) where @l = 'Fatal' and ['service.name'] = 'gateway'")]
    [InlineData("select count(*) group by ['service.name']")]                    // casing and empty names
    [InlineData("select count(*) group by ['service.name'] limit 2")]
    [InlineData("select count(*) where @l = 'Error' group by ['service.name']")]
    [InlineData("select count(*) where n > 100")]                                // not a header field
    [InlineData("select count(*) group by n")]                                   // not a header key
    [InlineData("select count(*), count(n) group by ['service.name']")]          // not every column is count(*)
    [InlineData("select sum(n) group by ['service.name']")]
    [InlineData("select count(*) group by ['service.name'], @l")]                // two keys
    public async Task Both_roads_answer_the_same_table(string text)
    {
        Assert.True(AggregationParser.TryParse(text, out var q));

        var viaHeader = await _withHeader.ExecuteAsync(q!, From, To);
        var viaScan   = await _scanOnly.ExecuteAsync(q!, From, To);

        Assert.Equal(viaScan.KeyColumns,   viaHeader.KeyColumns);
        Assert.Equal(viaScan.ValueColumns, viaHeader.ValueColumns);
        Assert.Equal(viaScan.GroupsFound,  viaHeader.GroupsFound);
        Assert.Equal(viaScan.Partial,      viaHeader.Partial);
        Assert.Equal(viaScan.Rows.Count,   viaHeader.Rows.Count);

        for (int i = 0; i < viaScan.Rows.Count; i++)
        {
            Assert.Equal(viaScan.Rows[i].Key,    viaHeader.Rows[i].Key);
            Assert.Equal(viaScan.Rows[i].Values, viaHeader.Rows[i].Values);
        }
    }

    /// <summary>
    /// GROUPING BY SERVICE IS DECLINED, and these are the reasons why — the cases the header
    /// aggregator cannot reproduce, each asserted against the scan road's answer.
    ///
    /// <para>The corpus carries <c>Billing</c> and <c>billing</c> as separate producers, and an
    /// event whose service is the EMPTY STRING. The scan road keys ordinally, so that is three
    /// distinct groups plus the absent one. The aggregator keys case-insensitively and folds
    /// the empty name into its "(unknown)" placeholder, so it would answer with fewer groups,
    /// one of them labelled with whichever casing a parallel worker happened to merge first —
    /// which its own contract admits can flap between refreshes. Counts on a volume chart do
    /// not care; a table of counts by service does.</para>
    ///
    /// <para>So the assertion is the scan road's, and that the shortcut did not take the query:
    /// if it ever does, the group count drops and this fails.</para>
    /// </summary>
    [Fact]
    public async Task Grouping_by_service_keeps_the_scan_road_and_its_ordinal_groups()
    {
        Assert.True(AggregationParser.TryParse("select count(*) group by ['service.name']", out var q));
        var result = await _withHeader.ExecuteAsync(q!, From, To);

        // checkout, billing, Billing, "", gateway, and the absent one.
        Assert.Equal(6, result.GroupsFound);

        Assert.Equal(2, result.Rows.Count(r => string.Equals(r.Key[0], "billing", StringComparison.OrdinalIgnoreCase)));
        Assert.Contains(result.Rows, r => r.Key[0] == "billing");
        Assert.Contains(result.Rows, r => r.Key[0] == "Billing");
        Assert.Contains(result.Rows, r => r.Key[0] == "");        // the empty name is its own group
        Assert.Contains(result.Rows, r => r.Key[0] is null);      // …and is not the absent one

        // The header road, had it been taken, would have merged three of those into two.
        var viaScan = await _scanOnly.ExecuteAsync(q!, From, To);
        Assert.Equal(viaScan.GroupsFound, result.GroupsFound);
        for (int i = 0; i < viaScan.Rows.Count; i++)
            Assert.Equal(viaScan.Rows[i].Key[0], result.Rows[i].Key[0]);
    }

    /// <summary>
    /// The header scan reads the WHOLE window, so it is not subject to the event scan's budget —
    /// and it says so. A budget small enough to truncate the scan leaves the header answer
    /// complete AND larger, which is the point: the same query stops being partial.
    /// </summary>
    [Fact]
    public async Task The_header_road_is_complete_where_the_scan_road_would_be_partial()
    {
        Assert.True(AggregationParser.TryParse("select count(*) group by @l", out var q));

        var starved   = new AggregationExecutor(_query, scanBudget: 50);
        var viaScan   = await starved.ExecuteAsync(q!, From, To);
        var viaHeader = await new AggregationExecutor(_query, scanBudget: 50, headerScan: _engine)
                                  .ExecuteAsync(q!, From, To);

        Assert.True(viaScan.Partial);
        Assert.Equal(50, viaScan.Scanned);

        Assert.False(viaHeader.Partial);
        Assert.Null(viaHeader.PartialReason);
        Assert.Equal(400d, viaHeader.Rows.Sum(r => r.Values[0] ?? 0));
    }

    // ── Which road a shape takes ──────────────────────────────────────────────

    /// <summary>
    /// A LEVEL IN THE WHERE-CLAUSE KEEPS THE SCAN ROAD. The header aggregator consults no index:
    /// it decompresses every block of every segment in the window and would drop the other
    /// levels only afterwards, while the scan gets an exact level hint and level-pure flushes
    /// leave it a handful of segments. On a busy store an Error count over a day decoded the
    /// Information and Debug volume too, ran out of time, and came back partial with no rows
    /// where the scan returns the number.
    ///
    /// <para>The two roads report different <see cref="AggregationResult.Scanned"/> for the same
    /// question, and that is the observable: the scan counts only the events its filter
    /// yielded, the header road every in-window header it walked — all 400 of them. Equal to the
    /// scan-only executor's figure, and below the corpus size, means the scan answered.</para>
    /// </summary>
    [Theory]
    [InlineData("select count(*) where @l = 'Error'")]
    [InlineData("select count(*) where @l = 'Error' group by @l")]
    [InlineData("select count(*) where @l in ['Error','Fatal'] group by @l")]
    [InlineData("select count(*) where @l = 'Fatal' and ['service.name'] = 'gateway'")]
    public async Task A_level_in_the_filter_keeps_the_scan_road(string text)
    {
        Assert.True(AggregationParser.TryParse(text, out var q));

        var viaHeader = await _withHeader.ExecuteAsync(q!, From, To);
        var viaScan   = await _scanOnly.ExecuteAsync(q!, From, To);

        Assert.Equal(viaScan.Scanned, viaHeader.Scanned);
        Assert.True(viaHeader.Scanned < 400, $"read {viaHeader.Scanned} of 400 — the header road walked the whole window");
        Assert.Equal(viaScan.Rows.Sum(r => r.Values[0] ?? 0), viaHeader.Rows.Sum(r => r.Values[0] ?? 0));
    }

    /// <summary>
    /// …and a filter that constrains NO level still takes the header road, where the scan has
    /// nothing to narrow with. A budget too small for the scan tells them apart: the scan road
    /// comes back partial, the header road complete with the scan-only executor's full count.
    /// </summary>
    [Theory]
    [InlineData("select count(*)")]
    [InlineData("select count(*) group by @l")]
    [InlineData("select count(*) where ['service.name'] = 'billing'")]
    [InlineData("select count(*) where ['service.name'] = 'billing' group by @l")]
    public async Task A_filter_with_no_level_keeps_the_header_road(string text)
    {
        Assert.True(AggregationParser.TryParse(text, out var q));

        var complete = await _scanOnly.ExecuteAsync(q!, From, To);
        var starved  = await new AggregationExecutor(_query, scanBudget: 50).ExecuteAsync(q!, From, To);
        var viaHeader = await new AggregationExecutor(_query, scanBudget: 50, headerScan: _engine)
                                  .ExecuteAsync(q!, From, To);

        Assert.True(starved.Partial, "the corpus no longer starves the scan road — the test proves nothing");
        Assert.False(viaHeader.Partial);
        Assert.Equal(complete.Rows.Sum(r => r.Values[0] ?? 0), viaHeader.Rows.Sum(r => r.Values[0] ?? 0));
    }

    /// <summary>
    /// A <c>@t</c> bound in the where-clause compiles to a TimeCompareNode, which the
    /// header-only shape rejects — so the query falls back rather than silently counting events
    /// outside the bound. The assertion is on the NUMBER, which is what would be wrong.
    /// </summary>
    [Fact]
    public async Task A_time_bound_in_the_filter_is_not_silently_dropped()
    {
        string cut  = Base.AddSeconds(100).ToString("O");
        string text = $"select count(*) where @t < '{cut}'";
        Assert.True(AggregationParser.TryParse(text, out var q));

        var viaHeader = await _withHeader.ExecuteAsync(q!, From, To);
        var viaScan   = await _scanOnly.ExecuteAsync(q!, From, To);

        Assert.Equal(viaScan.Rows[0].Values[0], viaHeader.Rows[0].Values[0]);
        Assert.True(viaHeader.Rows[0].Values[0] < 400d, "the time bound must have narrowed the count");
    }
}
