using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;
using Xunit;

namespace Ameto.Query.Tests;

/// <summary>
/// A LEVEL-ONLY FILTER, which is a query shape no single work package could see arriving here.
/// Q1 made the compiled filter derive a level set from the filter's own AND-chain, so
/// <c>@l != 'Error'</c> with no <c>levels=</c> parameter now builds level hints; Q2 wrote the
/// candidate road those hints then take. Against a level-split store the level union IS the
/// group, so a group that contributes every one of its rows answered with a group-sized uint[]
/// naming all of them — unioned out of the posting lists, copied into a List, copied out again,
/// and then resolved by the reader with two binary searches per block and a cursor step per row,
/// to select 100 % of them.
///
/// <para>The narrowing now has a third state for that: "every row", carried as a flag rather
/// than as ordinals, and a segment whose every group is in it hands the scan no candidate list —
/// the plain block walk, over the same rows. What these tests pin is that the state is reached,
/// that it names nothing, and — the part that matters most — that the rows do not change,
/// including in the mixed shape where one group contributes everything and another genuinely
/// narrows, which is where an out-of-order candidate list would silently lose rows.</para>
/// </summary>
public sealed class LevelOnlyNarrowingTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-levelonly-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _extra = [];

    private StorageEngine _engine  = null!;
    private QueryExecutor _query   = null!;
    private string        _segPath = null!;

    public async Task InitializeAsync() =>
        (_engine, _query, _segPath, _) = await QuerySegmentFixtures.GroupedSegmentAsync(Path.Combine(_dir, "pure"));

    public async Task DisposeAsync()
    {
        foreach (var e in _extra) { try { await e.DisposeAsync(); } catch { } }
        await _engine.DisposeAsync();
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    /// <summary>The level hints QueryExecutor builds from a derived level set.</summary>
    private static (string, object?)[][] HintsFor(HashSet<LogLevel> levels)
    {
        var hints = new (string, object?)[levels.Count][];
        int i = 0;
        foreach (var l in levels) hints[i++] = [(ClefFields.Level, l.ToSeqString())];
        return hints;
    }

    // ── The third state ───────────────────────────────────────────────────────

    /// <summary>
    /// The fixture writes Information events only, so every group is level-pure and the union of
    /// the five levels <c>@l != 'Error'</c> admits is exactly the group. That must come back as
    /// "every row" with nothing materialised — not as an array naming all of them, and not as
    /// null, which means "no information" and would send the whole segment to a full scan.
    /// </summary>
    [Fact]
    public void ALevelPureGroupContributesEveryRow_WithoutNamingItsOrdinals()
    {
        using var reader = SegmentReader.Open(_segPath);
        Assert.True(reader.Groups.Length >= 2, $"fixture produced {reader.Groups.Length} group(s)");
        uint groupEvents = reader.Groups[0].EventCount;

        using var invSec = reader.RentInvertedIndexBytes(0);
        using var triSec = reader.RentTrigramIndexBytes(0);
        using var bloSec = reader.RentBloomFilterBytes(0);
        using var idx    = SegmentIndexReader.Load(invSec.Span, triSec.Span, bloSec.Span);

        var filter = CompiledFilter.Compile("@l != 'Error'");
        Assert.NotNull(filter.DerivedLevels);          // the step that routes this query here at all
        var hints = HintsFor(filter.DerivedLevels!);

        Assert.True(QueryExecutor.TryNarrowWithIndex(filter, idx, hints, groupEvents, out var candidates));
        Assert.Null(candidates);

        // With the group's size unknown the shortcut cannot fire, and the SAME rows arrive as an
        // explicit union — the state is about how the answer is carried, not which rows it names.
        Assert.True(QueryExecutor.TryNarrowWithIndex(filter, idx, hints, 0, out var named));
        Assert.NotNull(named);
        Assert.Equal(groupEvents, (uint)named!.Length);
    }

    // ── The rows do not change ────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]                              // no filter at all — the baseline
    [InlineData("@l != 'Error'")]                   // derives five levels, no other hint
    [InlineData("@l in ['Information','Warning']")] // derives two
    [InlineData("@l = 'Information'")]              // derives one (this shape already hinted before)
    public async Task ALevelOnlyFilterOverALevelPureStoreReturnsEveryRow(string? filter)
    {
        var got = await QuerySegmentFixtures.RunAsync(_query, filter, QuerySegmentFixtures.GroupedEvents + 10);
        Assert.Equal(QuerySegmentFixtures.GroupedEvents, got.Count);
    }

    /// <summary>A filter that derives levels AND narrows must still get exactly its own row.</summary>
    [Fact]
    public async Task AFilterThatNarrowsAndDerivesLevelsStillGetsExactlyItsRows()
    {
        var got = await QuerySegmentFixtures.RunAsync(_query, "@l != 'Error' and OrderId = 'order-4321'", 10);

        Assert.Single(got);
        Assert.Equal("order-4321", QuerySegmentFixtures.OrderIdOf(got[0]));
    }

    /// <summary>
    /// THE MIXED SHAPE, and the reason the whole-group ranges are generated where they are. One
    /// segment, one group level-pure (every row contributes) and later groups carrying both
    /// Information and Error (the level union is a genuine subset, so they name ordinals). The
    /// segment therefore has to name ordinals after all, and the whole-group range has to be
    /// spliced in at its own position — groups are visited in ordinal order, so an appended or
    /// out-of-order range would leave the candidate list unsorted and the reader would silently
    /// return the wrong rows.
    /// </summary>
    [Fact]
    public async Task AMixedLevelSegmentReturnsExactlyTheEventsTheFilterAdmits()
    {
        const int Pure = 1_500, Mixed = 1_500;

        string dir = Path.Combine(_dir, "mixed");
        Directory.CreateDirectory(dir);
        var opts   = new ServerOptions { DataDirectory = dir };
        var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _extra.Add(engine);
        engine._groupPayloadBudgetBytes = QuerySegmentFixtures.GroupBudget;   // several groups, one small file
        engine.IndexSinkFactory = static (events, termsPerEvent) => new SegmentIndexBuilder(events, 5, termsPerEvent);

        var query     = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        long baseTicks = new DateTimeOffset(2026, 8, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        int  tmplIdx   = engine.TemplatePool.Intern("order {OrderId} processed");

        // The first run is one level, so its groups are pure; the rest alternate, so theirs are
        // not. Both kinds end up in the same file.
        var buf      = new ArrayBufferWriter<byte>(512);
        int expected = 0;
        for (int i = 0; i < Pure + Mixed; i++)
        {
            var level = i < Pure || i % 2 == 0 ? LogLevel.Information : LogLevel.Error;
            if (level != LogLevel.Error) expected++;

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("OrderId"); w.Write("order-" + i);
            w.Write("pad");     w.Write(new string((char)('a' + i % 26), 220));
            w.Flush();

            Assert.True(engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = level,
                MessageTemplatePoolIndex = tmplIdx,
            }, buf.WrittenSpan.ToArray()));
        }
        await engine.FlushHotTierAsync();

        // The flush splits by level, so the two levels are separate files; what this exercises is
        // a query whose surviving groups are a mix of whole-group and genuinely narrowed ones.
        var got = await QuerySegmentFixtures.RunAsync(query, "@l != 'Error'", Pure + Mixed + 10, forward: true);

        Assert.Equal(expected, got.Count);
        Assert.All(got, ev => Assert.NotEqual(LogLevel.Error, ev.Level));

        // …and the ordering the candidate list has to preserve.
        for (int i = 1; i < got.Count; i++)
            Assert.True(got[i].Timestamp >= got[i - 1].Timestamp, "results left ascending order");
    }
}
