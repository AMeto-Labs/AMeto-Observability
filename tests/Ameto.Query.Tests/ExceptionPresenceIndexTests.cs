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
/// A search for exceptions — <c>has(@x)</c>, <c>@x.exists = true</c> — answered from the segment
/// index instead of by reading every row of the window. It used to scan everything: the
/// predicate gave the index nothing to look up, so a week of Information segments holding no
/// exception at all was decoded row by row to say so.
///
/// <para>What is pinned here, over real flushed segments and through the real executor:</para>
/// <list type="bullet">
///   <item>THE ROWS DO NOT CHANGE — for the predicates that ask the marker and for the ones that
///   still scan (<c>not has(@x)</c>, <c>@x.exists = false</c>), including the row whose exception
///   map the index could not read and the row whose exception column holds nil.</item>
///   <item>WHAT THE INDEX SAYS PER GROUP: a group with no exception is dropped unread, a group
///   where every row has one contributes every row without naming them, a mixed one names its
///   rows.</item>
///   <item>A STORE WRITTEN BEFORE THIS loses nothing. Its groups carry no word that their marker
///   is complete, so they answer "no information" and are scanned, exactly as before — and once
///   a merge has rewritten them they answer from the marker like any other.</item>
/// </list>
/// </summary>
public sealed class ExceptionPresenceIndexTests : IAsyncLifetime
{
    /// <summary>Rows of each level in each of the two flushes.</summary>
    private const int PerLevel = 60;

    private static readonly LogLevel[] Levels = [LogLevel.Information, LogLevel.Warning, LogLevel.Error];

    private static readonly string[] AskForAnException =
    [
        "has(@x)", "isDefined(@x)", "has(Exception)", "has(@x.type)",
        "@x.exists = true", "@x.exists = 'TRUE'", "@x.exists != false", "@x.exists in [true]",
        "['@x.exists'] = true", "Exception.Exists = true",
    ];

    private static readonly string[] AskForNone =
    [
        "not has(@x)", "@x.exists = false", "@x.exists != true", "@x.exists in [false]",
    ];

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-xpresence-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // ── A store this build wrote ──────────────────────────────────────────────

    [Fact]
    public async Task AStoreWrittenByThisBuild_AnswersFromTheMarker_AndReturnsTheSameRows()
    {
        var store = await StoreAsync("vouched", static (events, terms) => new SegmentIndexBuilder(events, 5, terms));

        await AssertAnswersAsync(store);

        // Six segments of one group each: an Information, a Warning and an Error one per flush.
        // Information holds no exception; every Error row has one; a third of Warning does,
        // plus the row whose map cannot be read.
        string   mixed    = $"{WarningRowsWithAnException} of {PerLevel}";
        string[] expected = [mixed, mixed, "dropped", "dropped", "every row", "every row"];
        foreach (string filter in AskForAnException)
            Assert.Equal(expected, GroupVerdicts(store.Engine, filter));

        // The predicates that are not that question still ask the index nothing.
        foreach (string filter in AskForNone)
            Assert.All(GroupVerdicts(store.Engine, filter), static verdict => Assert.Equal("no information", verdict));
    }

    // ── A store written before the marker was complete ────────────────────────

    [Fact]
    public async Task AStoreWrittenBeforeThis_IsScanned_LosesNothing_AndAnswersFromTheMarkerOnceMerged()
    {
        var store = await StoreAsync("older", static (events, terms) => new PreVouchingIndexSink(events, terms));

        // Its groups hold a marker — without the unreadable row — and no word. Narrowing by that
        // list would lose the row; dropping a group for lacking one would lose a Warning segment.
        foreach (string filter in AskForAnException)
            Assert.All(GroupVerdicts(store.Engine, filter), static verdict => Assert.Equal("no information", verdict));
        await AssertAnswersAsync(store);

        // The next build's compaction rewrites them, index and all.
        store.Engine.IndexSinkFactory = static (events, terms) => new SegmentIndexBuilder(events, 5, terms);
        int before = store.Engine.ListSegments().Count;
        while (await store.Engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None)) { }
        Assert.True(store.Engine.ListSegments().Count < before, "setup: nothing was merged — the test proves nothing");

        foreach (string filter in AskForAnException)
            Assert.DoesNotContain("no information", GroupVerdicts(store.Engine, filter));
        await AssertAnswersAsync(store);
    }

    // ── Through the index cache, which is how production asks ─────────────────

    /// <summary>
    /// Everything above runs an executor with no index cache, which answers each query from the
    /// sections. Production has one: the first query teaches each group's memo, and the next is
    /// answered from what it learned — the marker's count, its list where one was worth
    /// building, the proof that a group holds none. The rows must be the same from either, and
    /// the second asking must not go back to a section.
    /// </summary>
    [Fact]
    public async Task ThroughTheIndexCache_TheRowsAreTheSame_FromTheSectionsAndThenFromTheMemos()
    {
        var store = await StoreAsync("cached", static (events, terms) => new SegmentIndexBuilder(events, 5, terms));
        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        var cached = store with { Query = WithCache(store.Engine, cache) };

        await AssertAnswersAsync(cached);
        Assert.True(cache.MissCount > 0, "setup: nothing was read through the cache");
        long misses = cache.MissCount, hits = cache.HitCount;

        await AssertAnswersAsync(cached);
        Assert.Equal(misses, cache.MissCount);
        Assert.True(cache.HitCount > hits);
    }

    // ── A group the marker nearly fills ───────────────────────────────────────

    /// <summary>
    /// The Error level of a level-split store: nearly every row carries an exception, so the
    /// marker's list names nearly the whole group and narrows next to nothing. From fifteen rows
    /// in sixteen up it is not built — the group is carried as "every row" — and that must
    /// change what a query costs and nothing about what it returns: the rows the list would
    /// have left out are still in the group, and the scan has to reject them.
    ///
    /// <para>200 rows a group, so the line is at 188: 94 in a hundred is on it, 93 is one row
    /// past and named. A page is asked for as well as the whole window, newest first as the UI
    /// does, and each executor twice — the cached one answers the second time from its memos.</para>
    /// </summary>
    [Theory]
    [InlineData(100, "every row")]
    [InlineData(97,  "every row")]
    [InlineData(94,  "every row")]
    [InlineData(93,  "186 of 200")]
    [InlineData(50,  "100 of 200")]
    [InlineData(0,   "dropped")]
    public async Task AGroupTheMarkerNearlyFills_IsCarriedWhole_AndReturnsOnlyTheRowsThatMatch(int percent, string verdict)
    {
        const int PerSegment = 200, Rows = QuerySegmentFixtures.ErrorSegments * PerSegment, Page = 50;

        var (engine, uncached) = await QuerySegmentFixtures.ErrorSegmentsAsync(
            Path.Combine(_dir, "dense-" + percent), PerSegment, percent);
        _engines.Add(engine);
        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        var cached = WithCache(engine, cache);

        Assert.All(GroupVerdicts(engine, "has(@x)"), v => Assert.Equal(verdict, v));
        // Beside the level every row of the group has: that list is the whole group and is
        // recognised as such once built, so the verdict is the marker's either way.
        Assert.All(GroupVerdicts(engine, "@x.exists = true and @l = 'Error'"), v => Assert.Equal(verdict, v));

        string[] with    = [.. Enumerable.Range(0, Rows).Where(row => row % 100 <  percent).Select(RowName)];
        string[] without = [.. Enumerable.Range(0, Rows).Where(row => row % 100 >= percent).Select(RowName)];
        string[] newest  = [.. with.Reverse().Take(Page)];

        foreach (var query in (QueryExecutor[])[uncached, uncached, cached, cached])
        {
            foreach (string filter in (string[])["has(@x)", "@x.exists = true", "has(@x) and @l = 'Error'"])
            {
                Assert.Equal(with, await RowsAsync(query, filter));
                Assert.Equal(newest, RowsOf(await QuerySegmentFixtures.RunAsync(query, filter, Page)));
            }
            Assert.Equal(without, await RowsAsync(query, "not has(@x)"));

            // Beside a hint that does narrow, the marker's list is left out and the hint narrows alone.
            string one = RowName(Rows / 2 + 7);
            Assert.Equal(7 < percent ? [one] : [], await RowsAsync(query, $"has(@x) and Row = '{one}'"));
        }

        static string RowName(int row) => row.ToString("D7", System.Globalization.CultureInfo.InvariantCulture);
    }

    // ── One segment, groups of every kind ─────────────────────────────────────

    /// <summary>
    /// The verdicts are per index GROUP, and a segment's candidate list is assembled from them:
    /// a dropped group contributes nothing, a narrowed one its ordinals, and an "every row" group
    /// a contiguous range that is generated only if the segment has to name rows at all — before
    /// a narrowed group's ordinals, or after the last one. Every other test here has one group
    /// to a segment and so exercises none of that.
    ///
    /// <para>One segment, cut into a group per block, whose rows run through six stretches: all
    /// with an exception, none, every third, all but one in thirty-two, none, all. So "every
    /// row" groups sit on both sides of narrowed ones with dropped ones between, and one kind of
    /// "every row" group holds rows WITHOUT an exception that only the scan can reject. Read in
    /// both directions, by the page from either end, through two time windows, with and without
    /// the cache.</para>
    /// </summary>
    [Fact]
    public async Task OneSegmentWhoseGroupsAnswerDifferently_ReturnsTheSameRows_InEitherDirection()
    {
        var (engine, uncached, hasException, baseTicks) = await ManyGroupsSegmentAsync();
        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        var cached = WithCache(engine, cache);

        // The premise: the segment really does hold every kind of group, in an order that makes
        // the assembly do each thing it can do.
        var groups = GroupAnswers(engine, "has(@x)");
        bool Whole(int g)    => groups[g].Verdict == "every row";
        bool Narrowed(int g) => groups[g].Verdict.Contains(" of ", StringComparison.Ordinal);
        bool HoldsARowWithout(int g) =>
            Enumerable.Range((int)groups[g].First, (int)groups[g].Count).Any(row => !hasException[row]);
        int[] all = [.. Enumerable.Range(0, groups.Count)];

        Assert.True(groups.Count >= 12, $"setup: only {groups.Count} groups");
        Assert.DoesNotContain(groups, static g => g.Verdict == "no information");
        Assert.Contains(groups, static g => g.Verdict == "dropped");
        Assert.Contains(all, g => Whole(g) && !HoldsARowWithout(g));
        Assert.Contains(all, g => Whole(g) && HoldsARowWithout(g));
        int firstNarrowed = Array.FindIndex(all, Narrowed), lastNarrowed = Array.FindLastIndex(all, Narrowed);
        Assert.True(firstNarrowed >= 0, "setup: no group was narrowed");
        Assert.Contains(all, g => Whole(g) && g < firstNarrowed);
        Assert.Contains(all, g => Whole(g) && g > lastNarrowed);

        int      rows    = hasException.Length;
        string[] with    = [.. Enumerable.Range(0, rows).Where(row => hasException[row]).Select(RowName)];
        string[] without = [.. Enumerable.Range(0, rows).Where(row => !hasException[row]).Select(RowName)];

        // A window that opens and closes inside a stretch with no exception, so every group
        // between is asked whole; and one that opens half a second before a row deep in the last
        // stretch, where every group it reaches says "every row" and none is narrowed.
        int opens = Stretches[0] + Stretches[1] / 2, closes = rows - Stretches[5] - Stretches[4] / 2;
        int late  = rows - Stretches[5] / 2;
        DateTimeOffset At(int row, long nudge = 0) => new(baseTicks + row * TimeSpan.TicksPerSecond + nudge, TimeSpan.Zero);
        string[] between = [.. Enumerable.Range(opens, closes - opens + 1).Where(row => hasException[row]).Select(RowName)];
        string[] tail    = [.. Enumerable.Range(late, rows - late).Select(RowName)];
        Assert.All(Enumerable.Range(late, rows - late), row => Assert.True(hasException[row]));

        foreach (var query in (QueryExecutor[])[uncached, cached, cached])
        {
            foreach (string filter in (string[])["has(@x)", "@x.exists = true"])
            {
                Assert.Equal(with, RowsOf(await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true)));
                Assert.Equal(with.Reverse(), RowsOf(await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue)));
                Assert.Equal(with.Take(40), RowsOf(await QuerySegmentFixtures.RunAsync(query, filter, 40, forward: true)));
                Assert.Equal(with.Reverse().Take(40), RowsOf(await QuerySegmentFixtures.RunAsync(query, filter, 40)));

                Assert.Equal(between, RowsOf(await QuerySegmentFixtures.RunAsync(
                    query, filter, int.MaxValue, At(opens), At(closes), forward: true)));
                Assert.Equal(between.Reverse(), RowsOf(await QuerySegmentFixtures.RunAsync(
                    query, filter, int.MaxValue, At(opens), At(closes))));
                Assert.Equal(tail, RowsOf(await QuerySegmentFixtures.RunAsync(
                    query, filter, int.MaxValue, At(late, -TimeSpan.TicksPerSecond / 2), At(rows + 60), forward: true)));
            }
            Assert.Equal(without, RowsOf(await QuerySegmentFixtures.RunAsync(query, "not has(@x)", int.MaxValue, forward: true)));
        }

        static string RowName(int row) => row.ToString("D5", System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Rows in each stretch of <see cref="ManyGroupsSegmentAsync"/>, in the order written.</summary>
    private static readonly int[] Stretches = [700, 3_000, 1_500, 1_000, 3_000, 700];

    /// <summary>Whether row <paramref name="i"/> of stretch <paramref name="stretch"/> carries an exception.</summary>
    private static bool StretchHasException(int stretch, int i) => stretch switch
    {
        0 or 5 => true,
        1 or 4 => false,
        2      => i % 3 == 0,
        _      => i % 32 != 5,          // one row in thirty-two without: inside the one in sixteen
    };

    /// <summary>
    /// One Warning segment whose index is cut into a group per block (a group owns whole blocks,
    /// and a budget of one byte fills it with the first), written through the production index
    /// wiring. Returns, per file ordinal, whether that row carries an exception.
    /// </summary>
    private async Task<(StorageEngine Engine, QueryExecutor Query, bool[] HasException, long BaseTicks)> ManyGroupsSegmentAsync()
    {
        string dir = Path.Combine(_dir, "many-groups");
        Directory.CreateDirectory(dir);
        var options = new ServerOptions { DataDirectory = dir };
        var engine  = new StorageEngine(
            Options.Create(options),
            new RetentionStore(options, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engines.Add(engine);
        engine._groupPayloadBudgetBytes = 1;
        engine.IndexSinkFactory = static (events, terms) => new SegmentIndexBuilder(events, 5, terms);
        var query = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        long baseTicks = new DateTimeOffset(2026, 8, 30, 9, 0, 0, TimeSpan.Zero).UtcTicks;
        int  template  = engine.TemplatePool.Intern("settle {Row}");
        int  service   = engine.TemplatePool.Intern("Billing.API");
        var  buf       = new ArrayBufferWriter<byte>(64);
        var  has       = new List<bool>(Stretches.Sum());

        for (int stretch = 0; stretch < Stretches.Length; stretch++)
        for (int i = 0; i < Stretches[stretch]; i++)
        {
            int row = has.Count;
            has.Add(StretchHasException(stretch, i));

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("Row"); w.Write(row.ToString("D5", System.Globalization.CultureInfo.InvariantCulture));
            w.Flush();

            Assert.True(engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)row).RawValue,
                TimestampUtcTicks        = baseTicks + row * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = template,
                ServiceNamePoolIndex     = service,
            }, buf.WrittenSpan, "settle {Row}", has[row] ? Readable : default));
        }
        await engine.FlushHotTierAsync();

        Assert.Single(engine.ListSegments());
        return (engine, query, [.. has], baseTicks);
    }

    // ── Assertions ────────────────────────────────────────────────────────────

    private sealed record Store(StorageEngine Engine, QueryExecutor Query, string[] WithAnException, string[] Without);

    /// <summary>An executor over <paramref name="engine"/> with the index cache production runs with.</summary>
    private static QueryExecutor WithCache(StorageEngine engine, SegmentIndexCache cache) =>
        new(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);

    private static string[] RowsOf(List<LogEvent> events) =>
        [.. events.Select(static ev => (string)ev.Properties!["Row"]!)];

    private static async Task AssertAnswersAsync(Store store)
    {
        foreach (string filter in AskForAnException)
            Assert.Equal(store.WithAnException, await RowsAsync(store.Query, filter));

        foreach (string filter in AskForNone)
            Assert.Equal(store.Without, await RowsAsync(store.Query, filter));

        // Beside a conjunct of its own, in either order, and beside the level the UI adds.
        string[] warnings = [.. store.WithAnException.Where(static row => row.StartsWith("W", StringComparison.Ordinal))];
        Assert.Equal(warnings, await RowsAsync(store.Query, "has(@x) and @l = 'Warning'"));
        Assert.Equal(warnings, await RowsAsync(store.Query, "@l = 'Warning' and @x.exists = true"));
        string[] first = [warnings[0]];
        Assert.Equal(first, await RowsAsync(store.Query, $"@x.exists = true and Row = '{warnings[0]}'"));

        // And the whole store is still there for a query that asks nothing.
        Assert.Equal(store.WithAnException.Length + store.Without.Length, (await RowsAsync(store.Query, null)).Length);
    }

    /// <summary>The <c>Row</c> of every event a filter returns, oldest first — the order written.</summary>
    private static async Task<string[]> RowsAsync(QueryExecutor query, string? filter)
    {
        var events = await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true);
        return [.. events.Select(static ev => (string)ev.Properties!["Row"]!)];
    }

    /// <summary>
    /// What the production narrowing says about <paramref name="filter"/> for every index group
    /// of every segment, sorted: "dropped" (proven to hold no match), "every row", "n of m" (the
    /// rows it names) or "no information" (scan).
    /// </summary>
    private static string[] GroupVerdicts(StorageEngine engine, string filter)
    {
        var verdicts = GroupAnswers(engine, filter).ConvertAll(static g => g.Verdict);
        verdicts.Sort(StringComparer.Ordinal);
        return [.. verdicts];
    }

    /// <summary>
    /// The same verdicts in file order, each with the ordinals its group covers — for a test
    /// that needs to know which group said what.
    /// </summary>
    private static List<(string Verdict, uint First, uint Count)> GroupAnswers(StorageEngine engine, string filter)
    {
        var compiled = CompiledFilter.Compile(filter);
        var answers  = new List<(string, uint, uint)>();
        foreach (var segment in engine.ListSegments())
        {
            using var reader = SegmentReader.Open(segment.FilePath);
            for (int g = 0; g < reader.Groups.Length; g++)
            {
                using var inverted = reader.RentInvertedIndexBytes(g);
                using var trigram  = reader.RentTrigramIndexBytes(g);
                using var bloom    = reader.RentBloomFilterBytes(g);
                using var index    = SegmentIndexReader.Load(inverted.Span, trigram.Span, bloom.Span);
                uint rows = reader.Groups[g].EventCount;

                string verdict =
                    !QueryExecutor.TryNarrowWithIndex(compiled, index, null, rows, out uint[]? candidates, out bool everyRow) ? "dropped"
                    : everyRow           ? "every row"
                    : candidates is null ? "no information"
                    : $"{candidates.Length} of {rows}";
                answers.Add((verdict, reader.Groups[g].FirstOrdinal, rows));
            }
        }
        return answers;
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

    /// <summary>A third of the Warning rows carry a readable exception, and row 1 an unreadable one.</summary>
    private const int WarningRowsWithAnException = PerLevel / 3 + 1;

    private static readonly byte[] Readable =
        new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "gateway timeout on settle",
            StackTrace = "   at Billing.Settle()",
            Inner      = new ExceptionInfo { Type = "System.IO.IOException", Message = "disk" },
        }.ToBytes();

    /// <summary>
    /// A map whose <c>type</c> is not a string. <c>LogEvent.HasException</c> answers from the lead
    /// byte and says yes; the index cannot read its fields.
    /// </summary>
    private static readonly byte[] Unreadable = Pack(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(1);
        w.Write("type");
        w.Write(7);
    });

    /// <summary>msgpack nil: an exception column that is not empty and holds no exception.</summary>
    private static readonly byte[] Nil = [0xC0];

    /// <summary>What row <paramref name="i"/> of a level carries in its exception column.</summary>
    private static byte[] ExceptionOf(LogLevel level, int i) => level switch
    {
        LogLevel.Error                  => Readable,
        LogLevel.Warning when i == 1    => Unreadable,
        LogLevel.Warning when i == 2    => Nil,
        LogLevel.Warning when i % 3 == 0 => Readable,
        _                               => [],
    };

    /// <summary>
    /// Two flushes of <see cref="PerLevel"/> rows per level. A flush splits by level, so each
    /// makes three segments — one that holds no exception, one where every row has one, and a
    /// mixed one — and two of each is what a merge needs to have something to do.
    /// </summary>
    private async Task<Store> StoreAsync(string name, SegmentIndexSinkFactory indexSink)
    {
        string dir = Path.Combine(_dir, name);
        Directory.CreateDirectory(dir);
        var options = new ServerOptions { DataDirectory = dir };
        var engine  = new StorageEngine(
            Options.Create(options),
            new RetentionStore(options, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engines.Add(engine);
        engine.IndexSinkFactory = indexSink;
        var query = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        // Well in the past, so the merge treats each level's bucket as sealed and takes a pair.
        long baseTicks = new DateTimeOffset(2026, 8, 30, 9, 0, 0, TimeSpan.Zero).UtcTicks;
        int  template  = engine.TemplatePool.Intern("settle {Row}");
        int  service   = engine.TemplatePool.Intern("Billing.API");
        var  buf       = new ArrayBufferWriter<byte>(64);
        var  with      = new List<string>();
        var  without   = new List<string>();
        int  written   = 0;

        for (int flush = 0; flush < 2; flush++)
        {
            foreach (var level in Levels)
            for (int i = 0; i < PerLevel; i++)
            {
                // Named so that ordinal order is write order, and the level reads off the name.
                string row       = $"{level.ToString()[0]}-{flush}-{i:D3}";
                byte[] exception = ExceptionOf(level, i);
                (ExceptionInfo.IsPresent(exception) ? with : without).Add(row);

                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(1);
                w.Write("Row"); w.Write(row);
                w.Flush();

                Assert.True(engine.TryWrite(new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)written).RawValue,
                    TimestampUtcTicks        = baseTicks + written * TimeSpan.TicksPerSecond,
                    Level                    = level,
                    MessageTemplatePoolIndex = template,
                    ServiceNamePoolIndex     = service,
                }, buf.WrittenSpan, "settle {Row}", exception));
                written++;
            }
            await engine.FlushHotTierAsync();
        }

        Assert.Equal(2 * Levels.Length, engine.ListSegments().Count);
        return new Store(engine, query, [.. with], [.. without]);
    }

    private delegate void PackAction(ref MessagePackWriter writer);

    private static byte[] Pack(PackAction write)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
