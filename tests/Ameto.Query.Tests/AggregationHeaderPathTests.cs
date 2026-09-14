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
/// <para>The corpus spans both tiers, three named services and one that is deliberately
/// NAMELESS, every level, and a user property the header knows nothing about, so the tests
/// cover what the shortcut answers, what it must decline, and the one input on which the two
/// roads genuinely disagree.</para>
/// </summary>
public sealed class AggregationHeaderPathTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 24, 9, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddHours(4);

    private static readonly string?[]  Services = ["checkout", "billing", "gateway", null];
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
            string? service = Services[i % 4];
            var     level   = Levels[(i / 4) % Levels.Length];

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
        try { Directory.Delete(_dir, true); } catch { }
    }

    // ── The two roads answer the same table ───────────────────────────────────

    [Theory]
    // Answered by the header scan.
    [InlineData("select count(*)")]
    [InlineData("select count(*) group by ['service.name']")]
    [InlineData("select count(*) group by @l")]
    [InlineData("select count(*) where @l = 'Error'")]
    [InlineData("select count(*) where @l = 'Error' group by @l")]
    [InlineData("select count(*) where @l in ['Error','Fatal'] group by @l")]
    [InlineData("select count(*) where ['service.name'] = 'billing'")]
    [InlineData("select count(*) where ['service.name'] = 'billing' group by ['service.name']")]
    [InlineData("select count(*) where ['service.name'] = 'billing' group by @l")]
    [InlineData("select count(*) group by ['service.name'] limit 2")]
    [InlineData("select count(*) where @l = 'Fatal' and ['service.name'] = 'gateway'")]   // legitimately empty
    // Declined by the header scan, and therefore identical for a duller reason.
    [InlineData("select count(*) where @l = 'Error' group by ['service.name']")]          // level x service
    [InlineData("select count(*) where n > 100")]                                          // not a header field
    [InlineData("select count(*) group by n")]                                             // not a header key
    [InlineData("select count(*), count(n) group by ['service.name']")]                     // not every column is count(*)
    [InlineData("select sum(n) group by ['service.name']")]
    [InlineData("select count(*) group by ['service.name'], @l")]                           // two keys
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
    /// The absent service is reported as absent, not as a group named after a placeholder —
    /// the header aggregator calls it "(unknown)" for the counts chart, and this path has to
    /// translate that back.
    /// </summary>
    [Fact]
    public async Task An_event_with_no_service_is_its_own_absent_group()
    {
        Assert.True(AggregationParser.TryParse("select count(*) group by ['service.name']", out var q));
        var result = await _withHeader.ExecuteAsync(q!, From, To);

        Assert.Equal(4, result.GroupsFound);
        var absent = Assert.Single(result.Rows.Where(r => r.Key[0] is null));
        Assert.Equal(100d, absent.Values[0]);                      // every fourth event of 400
        Assert.DoesNotContain(result.Rows, r => r.Key[0] == "(unknown)");
    }

    /// <summary>
    /// The header scan reads the WHOLE window, so it is not subject to the event scan's budget —
    /// and it says so. A budget small enough to truncate the scan leaves the header answer
    /// complete AND larger, which is the point: the same query stops being partial.
    /// </summary>
    [Fact]
    public async Task The_header_road_is_complete_where_the_scan_road_would_be_partial()
    {
        Assert.True(AggregationParser.TryParse("select count(*) group by ['service.name']", out var q));

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
