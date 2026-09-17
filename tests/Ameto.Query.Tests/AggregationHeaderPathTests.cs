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

    // ── A segment the header road cannot read ─────────────────────────────────

    /// <summary>
    /// A COUNT OVER AN UNREADABLE SEGMENT IS A FLOOR, AND SAYS SO. The header aggregator skips a
    /// cold segment that throws — right for a volume chart, which must not go blank over one bad
    /// file — and the header road used to present what was left as the complete answer. The
    /// event scan fails the query over the same file; a total that is quietly low reads as a
    /// fact and is worse than that error.
    ///
    /// <para>Two segments of different levels are torn, so the skips come from different
    /// parallel workers and the count in the reason proves they were merged, not raced. The
    /// fixture flushes one segment per level, and tearing a segment's FIRST block frame means
    /// nothing of it is counted — so the rows must be exactly the readable data: the
    /// untouched totals minus each torn segment's catalog event count, in its own level.</para>
    /// </summary>
    [Fact]
    public async Task A_segment_the_header_road_cannot_read_makes_the_count_partial()
    {
        Assert.True(AggregationParser.TryParse("select count(*) group by @l", out var byLevel));
        Assert.True(AggregationParser.TryParse("select count(*)", out var total));

        var healthy = await _withHeader.ExecuteAsync(byLevel!, From, To);
        Assert.False(healthy.Partial);

        var segments = _engine.ListSegments();
        var tornA = Assert.Single(segments, s => s.MinLevel == LogLevel.Error);
        var tornB = Assert.Single(segments, s => s.MinLevel == LogLevel.Debug);
        TearFirstBlockFrame(tornA.FilePath);
        TearFirstBlockFrame(tornB.FilePath);

        var grouped = await _withHeader.ExecuteAsync(byLevel!, From, To);
        Assert.True(grouped.Partial, "a count missing two unreadable segments was reported as complete");
        Assert.NotNull(grouped.PartialReason);
        Assert.Contains("2 storage segment(s) in the window could not be read", grouped.PartialReason);

        var expected = healthy.Rows.ToDictionary(r => r.Key[0]!, r => r.Values[0]!.Value);
        expected["Error"] -= tornA.EventCount;
        expected["Debug"] -= tornB.EventCount;
        Assert.Equal(expected, grouped.Rows.ToDictionary(r => r.Key[0]!, r => r.Values[0]!.Value));

        var single = await _withHeader.ExecuteAsync(total!, From, To);
        Assert.True(single.Partial);
        Assert.Equal(400d - tornA.EventCount - tornB.EventCount, Assert.Single(single.Rows).Values[0]);
    }

    /// <summary>
    /// A MERGE UNDER THE HEADER ROAD MAKES THE COUNT PARTIAL, IN ITS OWN WORDS. A merge that lands
    /// while the header scan walks its snapshot deletes sources the snapshot lists, and moves
    /// their events into an output it does not — so the total comes back low, and it used to come
    /// back low with <c>Partial = false</c>. Nothing is damaged, so the reason must not send the
    /// user to the server log for a fault: it says the storage changed and that a rerun helps.
    ///
    /// <para>The merge runs inside the scan, before its first segment open, and every other
    /// worker waits for it, so neither source is read. The fixture's hot tier is flushed first,
    /// which gives every level a second segment the planner can pair with its first.</para>
    /// </summary>
    [Fact]
    public async Task A_merge_under_the_header_road_makes_the_count_partial_with_its_own_reason()
    {
        Assert.True(AggregationParser.TryParse("select count(*)", out var total));
        await _engine.FlushHotTierAsync();
        var before = _engine.ListSegments();

        using var done = new ManualResetEventSlim();
        int  first  = 0;
        bool merged = false;
        _engine._beforeHeaderSegmentOpen = _ =>
        {
            if (Interlocked.Exchange(ref first, 1) == 0)
            {
                try   { merged = _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None).GetAwaiter().GetResult(); }
                finally { done.Set(); }
            }
            else done.Wait(TimeSpan.FromSeconds(30));
        };

        AggregationResult raced;
        try { raced = await _withHeader.ExecuteAsync(total!, From, To); }
        finally { _engine._beforeHeaderSegmentOpen = null; }

        Assert.True(merged, "setup: the merge pass merged nothing — the test proves nothing");
        var after   = _engine.ListSegments().Select(SegmentKey.Of).ToHashSet();
        var sources = before.Where(s => !after.Contains(SegmentKey.Of(s))).ToList();
        Assert.Equal(2, sources.Count);

        Assert.True(raced.Partial, "a count missing a merge's sources was reported as complete");
        Assert.NotNull(raced.PartialReason);
        Assert.StartsWith("storage changed during the scan", raced.PartialReason);
        Assert.Contains("2 segment(s)", raced.PartialReason);
        Assert.DoesNotContain("server log", raced.PartialReason);
        Assert.Equal(400d - sources.Sum(s => (double)s.EventCount), Assert.Single(raced.Rows).Values[0]);

        // Run again: the snapshot now lists the merged output, and the count is whole.
        var rerun = await _withHeader.ExecuteAsync(total!, From, To);
        Assert.False(rerun.Partial);
        Assert.Equal(400d, Assert.Single(rerun.Rows).Values[0]);
    }

    /// <summary>
    /// The first block's <c>uncompressedSize</c> sits right after the 46-byte segment header;
    /// torn to a negative it fails <c>ValidateBlockFrame</c> with InvalidDataException before a
    /// single header of the segment is counted (the same tear SegmentCatalogKeyTests uses).
    /// </summary>
    private static void TearFirstBlockFrame(string path)
    {
        using var f = File.Open(path, FileMode.Open, FileAccess.Write);
        f.Position = 46;
        f.Write([0xF9, 0xFF, 0xFF, 0xFF]);
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
