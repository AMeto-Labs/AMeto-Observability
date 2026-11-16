using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// A group's candidate rows are named when the scan reaches the group, and not by the prefilter
/// for every group of its batch — through the real executor, over real flushed segments.
///
/// <para>The prefilter used to build each group's candidate list before the first row was
/// returned, for a whole batch of segments: a page of fifty rows that the newest segment fills
/// paid for the posting lists of all eight. It now settles what the lists' counts settle —
/// dropped, every row, a short list named on the spot — and leaves a long list unbuilt
/// (<c>QueryExecutor.Narrow</c>, <c>SegmentPlan</c>); the scan asks for it per index group, as
/// its walk gets there (<c>SegmentReader.ReadGroupsAsync</c>).</para>
///
/// <para>Only a query's FIRST prefilter batch leaves lists for the scan. A page that needs a
/// second batch is a search whose rows are thin on the ground; from there each batch names as it
/// goes, in parallel, as every batch used to.</para>
///
/// <para>Two kinds of claim, kept apart, as in <c>LazyPrefilterTests</c>. ANSWERS: whatever the
/// filter, direction, page, window or cursor, the rows are the ones a read of everything,
/// filtered in memory, gives — an oracle that never touches an index. The groups here are large
/// enough that their lists ARE left for later (the fixtures of the older suites are not: two
/// hundred rows a group is a list short enough to name at once), and each case says so by the
/// counters before it trusts its rows. COUNTS, off
/// <see cref="QueryExecutor.GroupsLeftForLater"/>, <see cref="QueryExecutor.GroupsNamedLate"/>,
/// <see cref="SegmentReader.PooledSliceRents"/> and <see cref="SegmentReader.BlocksDecoded"/>: a
/// page names the rows of the groups it reads and of no other, and a group the index ruled out
/// is not read. Process-wide counters, so this depends on the assembly's
/// <c>DisableTestParallelization</c> (see AssemblyInfo.cs).</para>
/// </summary>
public sealed class LateNamingTests : IAsyncLifetime
{
    /// <summary>More segments than the largest prefilter batch (eight), so that on any machine
    /// a read of everything goes on to a second batch.</summary>
    private const int Segments       = 12;
    private const int RowsPerSegment = 1_200;
    private const int Rows           = Segments * RowsPerSegment;

    /// <summary>A query's first prefilter batch: one wave of the prefilter's parallelism, which
    /// depends on the machine — computed, never written down.</summary>
    private static readonly int FirstBatch = Math.Min(Segments, QueryExecutor.PrefilterParallelism);

    private readonly string              _dir     = Path.Combine(Path.GetTempPath(), "ameto-latenaming-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    private StorageEngine _engine   = null!;
    private QueryExecutor _uncached = null!;
    private long          _baseTicks;

    public async Task InitializeAsync()
    {
        (_engine, _uncached) = await QuerySegmentFixtures.ErrorSegmentsAsync(
            Path.Combine(_dir, "errors"), RowsPerSegment, percentWithException: 50, segments: Segments);
        _engines.Add(_engine);
        _baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
    }

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // What the fixture writes, by row number: a hundred rows in every two hundred carry an
    // exception, and the lanes alternate.
    private static bool HasException(int row) => row % 100 < 50;
    private static bool Even(int row)         => row % 2 == 0;

    /// <summary>A filter, the level set beside it, and the rows it is owed — by row number.</summary>
    public sealed record Case(string Name, string? Filter, LogLevel[]? Levels, Func<int, bool> Matches, bool LeftForLater)
    {
        public override string ToString() => Name;
    }

    public static readonly Case[] Cases =
    [
        // One long list.
        new("has(@x)",                         "has(@x)",                                   null, HasException, true),
        new("Lane = 'even'",                   "Lane = 'even'",                             null, Even, true),
        // Two, intersected when they are built.
        new("has(@x) and Lane",                "has(@x) and Lane = 'even'",                 null, static row => HasException(row) && Even(row), true),
        // Two that share no row: every group survives the prefilter and names nothing.
        new("Lane even and odd",               "Lane = 'even' and Lane = 'odd'",            null, static _ => false, true),
        // A long list beside a short one: left for later, and one row at the end of it. The
        // row is in the oldest segment, which a forward read's first batch holds on any machine.
        new("Lane and one row",                "Lane = 'odd' and Row = '0000321'",          null, static row => row == 321, true),
        new("Lane and a row of the other",     "Lane = 'even' and Row = '0000321'",         null, static _ => false, true),
        // Beside the level every row has, typed and as the parameter.
        new("Lane and @l",                     "Lane = 'even' and @l = 'Error'",            null, Even, true),
        new("Lane, levels=Error",              "Lane = 'even'",                             [LogLevel.Error], Even, true),
        new("has(@x), levels=Warning,Error",   "has(@x)",                                   [LogLevel.Warning, LogLevel.Error], HasException, true),
        // What the counts settle: nothing is left for later, and nothing named.
        new("@l = 'Error'",                    "@l = 'Error'",                              null, static _ => true, false),
        new("levels=Error",                    null,                                        [LogLevel.Error], static _ => true, false),
        new("levels=Warning",                  null,                                        [LogLevel.Warning], static _ => false, false),
        new("Lane = 'nobody'",                 "Lane = 'nobody'",                           null, static _ => false, false),
        new("one row",                         "Row = '0004321'",                           null, static row => row == 4321, false),
        // A substring predicate is narrowed whole by the prefilter, as it always was.
        new("Lane and a substring",            "Lane = 'even' and @mt like '%settle%'",     null, Even, false),
    ];

    public static TheoryData<Case> EveryCase()
    {
        var data = new TheoryData<Case>();
        foreach (var c in Cases) data.Add(c);
        return data;
    }

    // ── Answers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Every shape a list can be left in, read every way a query reads: whole in both
    /// directions, by pages that end inside the newest segment, at its edge and several
    /// segments in, through a window that opens and closes inside a group, and by the cursor.
    /// Without a cache, then with one twice — the second time the memos answer.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryCase))]
    public async Task TheRowsAreTheOnesAScanGives(Case c)
    {
        int[] owed = [.. Enumerable.Range(0, Rows).Where(c.Matches)];

        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        var cached = WithCache(cache);

        foreach (var query in (QueryExecutor[])[_uncached, cached, cached])
        {
            long left = QueryExecutor.GroupsLeftForLater;
            Assert.Equal(owed, await RowsAsync(query, c, int.MaxValue, forward: true));
            // The premise: this case is, or is not, one whose lists wait for the scan.
            Assert.Equal(c.LeftForLater, QueryExecutor.GroupsLeftForLater > left);

            Assert.Equal(owed.Reverse(), await RowsAsync(query, c, int.MaxValue, forward: false));

            foreach (int page in (int[])[1, 50, RowsPerSegment / 2 + 1, 2_500])
            {
                Assert.Equal(owed.Take(page), await RowsAsync(query, c, page, forward: true));
                Assert.Equal(owed.Reverse().Take(page), await RowsAsync(query, c, page, forward: false));
            }

            // A window from the middle of the second segment to the middle of the eleventh.
            int opens = RowsPerSegment + 333, closes = 10 * RowsPerSegment + 777;
            int[] between = [.. owed.Where(row => row >= opens && row <= closes)];
            Assert.Equal(between, await RowsAsync(query, c, int.MaxValue, forward: true, At(opens), At(closes)));
            Assert.Equal(between.Reverse().Take(70), await RowsAsync(query, c, 70, forward: false, At(opens), At(closes)));
        }
    }

    /// <summary>
    /// Paging by the (timestamp, id) cursor takes the plan again on every page, and each page
    /// names its groups afresh. The pages still join up into the whole, with no gap and no repeat.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CursorPagesJoinUpIntoTheWhole(bool forward)
    {
        const int Page = 173;
        var c = Cases[2];                                   // two long lists, intersected
        int[] owed = [.. Enumerable.Range(0, Rows).Where(c.Matches)];
        if (!forward) Array.Reverse(owed);

        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        foreach (var query in (QueryExecutor[])[_uncached, WithCache(cache)])
        {
            var      walked  = new List<int>();
            long?    afterTs = null;
            EventId? afterId = null;
            while (true)
            {
                var page = await RunAsync(query, c, Page, forward, afterTs: afterTs, afterId: afterId);
                walked.AddRange(page.Select(RowOf));
                if (page.Count < Page) break;
                afterTs = page[^1].Timestamp.UtcTicks;
                afterId = page[^1].Id;
            }
            Assert.Equal(owed, walked);
        }
    }

    // ── Counts ────────────────────────────────────────────────────────────────

    /// <summary>
    /// THE POINT. A page the newest segment fills names the rows of ONE group, however many the
    /// prefilter batch held; read the other way it names the oldest one's. Before, every group
    /// of the batch had its list built to be thrown away.
    ///
    /// <para>What is left for later is the whole first batch, whatever the page then reads of it.</para>
    /// </summary>
    [Theory]
    [InlineData("has(@x)")]
    [InlineData("Lane = 'even'")]
    [InlineData("has(@x) and Lane = 'even'")]
    public async Task APageNamesTheRowsOfTheGroupItReads_AndOfNoOther(string filter)
    {
        var c = new Case(filter, filter, null, static _ => true, true);

        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        foreach (var query in (QueryExecutor[])[_uncached, WithCache(cache)])
        foreach (bool forward in (bool[])[false, true])
        {
            var (left, named, _, page) = await CountAsync(query, c, 50, forward);
            Assert.Equal(50, page.Count);
            Assert.Equal(FirstBatch, left);
            Assert.Equal(1, named);
        }
    }

    /// <summary>
    /// ONLY THE FIRST BATCH WAITS. A read that goes on past it — a page whose rows are thin on
    /// the ground, a search for something that is not there, a read of everything — has its
    /// later batches name their groups as they are checked, in parallel, as every batch used
    /// to: nothing more is left for the scan, which would name those groups one at a time on
    /// the consumer's thread. So however much is read, what was left for later is the first
    /// batch, and each of its groups is named once.
    /// </summary>
    [Theory]
    [InlineData("Lane = 'even'")]
    [InlineData("has(@x) and Lane = 'even'")]
    [InlineData("Lane = 'even' and Lane = 'odd'")]        // every group survives the prefilter and names no row
    public async Task APageThatNeedsASecondBatch_LeavesOnlyTheFirstForTheScan(string filter)
    {
        var c = new Case(filter, filter, null, static _ => true, true);
        Assert.True(FirstBatch < Segments, "setup: the first batch is the whole store");

        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        foreach (var query in (QueryExecutor[])[_uncached, WithCache(cache), WithCache(cache)])
        foreach (bool forward in (bool[])[false, true])
        {
            var (left, named, _, _) = await CountAsync(query, c, int.MaxValue, forward);
            Assert.Equal(FirstBatch, left);
            Assert.Equal(FirstBatch, named);
        }
    }

    /// <summary>
    /// The list a group is named by is read by itself. The prefilter's walk of the group's
    /// section counted the list and noted where it lies; the scan's asking decodes those bytes
    /// and no others — one piece of one section for the group the page reads, where the whole
    /// section of every group of the batch used to be read for its list. And once a cache holds
    /// the list, the next page reads nothing of the file's index at all.
    /// </summary>
    [Fact]
    public async Task TheListOfAGroupNamedLate_IsReadByItself_AndThenFromTheCache()
    {
        var c     = Cases[1];                               // Lane = 'even'
        int batch = FirstBatch;

        // No cache: nothing remembers the list between queries, so every page fetches it again.
        // What it reads of whole sections is the prefilter's: a bloom and an inverted section
        // for each group of the batch, once — the second asking of the group the page reads
        // brings back what the first learned, and reads the list alone.
        for (int pass = 0; pass < 2; pass++)
        {
            long sectionsBefore = SegmentReader.PooledSectionRents;
            var (_, named, slices, _) = await CountAsync(_uncached, c, 50, forward: false);
            Assert.Equal(1, named);
            Assert.Equal(1, slices);
            Assert.Equal(2L * batch, SegmentReader.PooledSectionRents - sectionsBefore);
        }

        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        var cached = WithCache(cache);

        var first = await CountAsync(cached, c, 50, forward: false);
        Assert.Equal(1, first.Named);
        Assert.Equal(1, first.Slices);
        long misses = cache.MissCount;
        Assert.True(misses > 0, "setup: nothing was read through the cache");

        long sections = SegmentReader.PooledSectionRents;
        var again = await CountAsync(cached, c, 50, forward: false);
        Assert.Equal(1, again.Named);                       // asked again, as every query asks
        Assert.Equal(0, again.Slices);                      // …and answered from the memo
        Assert.Equal(0, SegmentReader.PooledSectionRents - sections);
        Assert.Equal(misses, cache.MissCount);
        Assert.Equal(first.Page.Select(RowOf), again.Page.Select(RowOf));
    }

    /// <summary>
    /// What the counts settle leaves nothing to name: the level every row has — typed, or from
    /// the level menu — costs a page no list in any group, and no piece of any section.
    /// </summary>
    [Theory]
    [InlineData("@l = 'Error'", null)]
    [InlineData(null, LogLevel.Error)]
    [InlineData("has(@x) and @l = 'Fatal'", null)]
    public async Task WhatTheCountsSettle_NamesNothing(string? filter, LogLevel? level)
    {
        var c = new Case("counted", filter, level is { } l ? [l] : null, static _ => true, false);

        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        foreach (var query in (QueryExecutor[])[_uncached, WithCache(cache)])
        {
            var (left, named, slices, _) = await CountAsync(query, c, 50, forward: false);
            Assert.Equal(0, left);
            Assert.Equal(0, named);
            Assert.Equal(0, slices);
        }
    }

    // ── One segment of many groups ────────────────────────────────────────────

    /// <summary>
    /// The plan is per index GROUP, and a segment of many groups is read through as many
    /// answers: a dropped group is not read, a group that contributes every row is read without
    /// a list, one with a short list by that list, and one with a long list is named when the
    /// walk reaches it — and only then, so a page off either end of a large segment names one
    /// group of it. Every other test here has one group to a segment and exercises none of that.
    ///
    /// <para>One segment cut into a group per block, whose rows run through five stretches of
    /// <c>Lane</c>: three rows in four 'even', none, all, one in ninety-seven, three in four
    /// again. Read in both directions, by pages from either end, and through a window.</para>
    /// </summary>
    [Fact]
    public async Task OneSegmentWhoseGroupsAnswerDifferently_IsNamedGroupByGroup_AsItIsRead()
    {
        var (engine, uncached, even, baseTicks) = await ManyGroupsSegmentAsync();
        using var cache = new SegmentIndexCache(64L * 1024 * 1024);
        var cached = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);
        var c      = new Case("Lane = 'even'", "Lane = 'even'", null, static _ => true, true);

        int[] owed = [.. Enumerable.Range(0, even.Length).Where(row => even[row])];
        DateTimeOffset at(int row) => new(baseTicks + row * TimeSpan.TicksPerSecond, TimeSpan.Zero);

        // The premise: the segment holds every kind of group.
        var verdicts = GroupVerdicts(engine, "Lane = 'even'");
        Assert.True(verdicts.Count >= 10, $"setup: only {verdicts.Count} groups");
        foreach (var kind in Enum.GetValues<QueryExecutor.GroupVerdict>())
        {
            if (kind == QueryExecutor.GroupVerdict.NoInformation) continue;
            Assert.Contains(kind, verdicts);
        }
        int late = verdicts.Count(static v => v == QueryExecutor.GroupVerdict.Deferred);
        Assert.True(late >= 4, $"setup: only {late} groups are left for later");
        Assert.Equal(QueryExecutor.GroupVerdict.Deferred, verdicts[0]);

        // A group here is one block. What the index rules out is not read: a read of everything
        // decodes a block for each group that could hold a row, and none for a group that could
        // not — which the rows alone would not show, since a scan of those groups rejects them all.
        int couldHold = verdicts.Count(static v => v != QueryExecutor.GroupVerdict.Dropped);
        Assert.True(couldHold < verdicts.Count);

        foreach (var query in (QueryExecutor[])[uncached, cached, cached])
        {
            long blocks = SegmentReader.BlocksDecoded;
            var whole = await CountAsync(query, c, int.MaxValue, forward: true);
            Assert.Equal(owed, whole.Page.Select(RowOf));
            Assert.Equal(couldHold, SegmentReader.BlocksDecoded - blocks);
            Assert.Equal(late, whole.Left);
            Assert.Equal(late, whole.Named);                // every group left for later was reached

            var back = await CountAsync(query, c, int.MaxValue, forward: false);
            Assert.Equal(owed.Reverse(), back.Page.Select(RowOf));

            // A page off either end reads into one group, and names that one — of the `late`
            // the prefilter left. (The file's last block may be a short one whose list the
            // prefilter named itself; then the newest page names one group or none.)
            var oldest = await CountAsync(query, c, 20, forward: true);
            Assert.Equal(owed.Take(20), oldest.Page.Select(RowOf));
            Assert.Equal(late, oldest.Left);
            Assert.Equal(1, oldest.Named);

            var newest = await CountAsync(query, c, 20, forward: false);
            Assert.Equal(owed.Reverse().Take(20), newest.Page.Select(RowOf));
            Assert.Equal(late, newest.Left);
            Assert.InRange(newest.Named, 0, 1);

            // A window inside the stretch that holds no 'even' row at all, to past the end of
            // the next: groups dropped, groups whole, and the edges of both.
            int opens = Stretches[0] + 200, closes = Stretches[0] + Stretches[1] + Stretches[2] + 300;
            int[] between = [.. owed.Where(row => row >= opens && row <= closes)];
            Assert.Equal(between, (await RunAsync(query, c, int.MaxValue, forward: true, at(opens), at(closes))).Select(RowOf));
            Assert.Equal(between.Reverse(), (await RunAsync(query, c, int.MaxValue, forward: false, at(opens), at(closes))).Select(RowOf));
        }
    }

    /// <summary>Rows in each stretch of <see cref="ManyGroupsSegmentAsync"/>, in the order written.</summary>
    private static readonly int[] Stretches = [6_000, 3_000, 3_000, 4_000, 6_000];

    /// <summary>Whether row <paramref name="i"/> of stretch <paramref name="stretch"/> is in the 'even' lane.</summary>
    private static bool StretchIsEven(int stretch, int i) => stretch switch
    {
        0 or 4 => i % 4 != 3,           // three rows in four: a long list, and not "nearly all"
        1      => false,                // none: the group is dropped
        2      => true,                 // all: every row, by the count
        _      => i % 97 == 0,          // a few: a short list, named by the prefilter
    };

    /// <summary>
    /// One Warning segment whose index is cut into a group per block (a group owns whole blocks,
    /// and a budget of one byte fills it with the first), written through the production index
    /// wiring. Returns, per file ordinal, whether that row is in the 'even' lane.
    /// </summary>
    private async Task<(StorageEngine Engine, QueryExecutor Query, bool[] Even, long BaseTicks)> ManyGroupsSegmentAsync()
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
        var  even      = new List<bool>(Stretches.Sum());

        for (int stretch = 0; stretch < Stretches.Length; stretch++)
        for (int i = 0; i < Stretches[stretch]; i++)
        {
            int row = even.Count;
            even.Add(StretchIsEven(stretch, i));

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("Row");  w.Write(row.ToString("D7", System.Globalization.CultureInfo.InvariantCulture));
            w.Write("Lane"); w.Write(even[row] ? QuerySegmentFixtures.EvenLane : QuerySegmentFixtures.OddLane);
            w.Flush();

            Assert.True(engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)row).RawValue,
                TimestampUtcTicks        = baseTicks + row * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = template,
                ServiceNamePoolIndex     = service,
            }, buf.WrittenSpan.ToArray()));
        }
        await engine.FlushHotTierAsync();

        Assert.Single(engine.ListSegments());
        return (engine, query, [.. even], baseTicks);
    }

    /// <summary>
    /// What the prefilter's asking says about <paramref name="filter"/> for every index group of
    /// the store's one segment, in file order — the production narrowing, at the prefilter's limit.
    /// </summary>
    private static List<QueryExecutor.GroupVerdict> GroupVerdicts(StorageEngine engine, string filter)
    {
        var compiled = Filtering.CompiledFilter.Compile(filter);
        var verdicts = new List<QueryExecutor.GroupVerdict>();

        using var reader = SegmentReader.Open(Assert.Single(engine.ListSegments()).FilePath);
        for (int g = 0; g < reader.Groups.Length; g++)
        {
            using var index = SegmentIndexView.Open(null, reader.Info.FilePath, g, reader);
            verdicts.Add(QueryExecutor.PassesBloomGate(compiled, index)
                ? QueryExecutor.Narrow(compiled, index, null, reader.Groups[g].EventCount, QueryExecutor.NameUpFront, out _)
                : QueryExecutor.GroupVerdict.Dropped);
        }
        return verdicts;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private QueryExecutor WithCache(SegmentIndexCache cache) =>
        new(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);

    private DateTimeOffset At(int row) => new(_baseTicks + row * TimeSpan.TicksPerSecond, TimeSpan.Zero);

    private static int RowOf(LogEvent ev) =>
        int.Parse((string)ev.Properties!["Row"]!, System.Globalization.CultureInfo.InvariantCulture);

    private static async Task<int[]> RowsAsync(
        QueryExecutor query, Case c, int count, bool forward, DateTimeOffset? from = null, DateTimeOffset? to = null) =>
        [.. (await RunAsync(query, c, count, forward, from, to)).Select(RowOf)];

    private static async Task<List<LogEvent>> RunAsync(
        QueryExecutor query, Case c, int count, bool forward,
        DateTimeOffset? from = null, DateTimeOffset? to = null, long? afterTs = null, EventId? afterId = null)
    {
        var rows = new List<LogEvent>();
        await foreach (var ev in query.ExecuteAsync(new QueryRequest
        {
            Filter              = c.Filter,
            Levels              = c.Levels is null ? null : [.. c.Levels],
            Count               = count,
            FromUtc             = from,
            ToUtc               = to,
            Direction           = forward ? QueryDirection.Forward : QueryDirection.Backward,
            AfterTimestampTicks = afterTs,
            AfterEventId        = afterId,
        }))
        {
            rows.Add(ev);
        }
        return rows;
    }

    /// <summary>
    /// One query, and what it did to the counters: groups the prefilter left for later, groups
    /// the scan named, and pieces of a section read for a list.
    /// </summary>
    private static async Task<(long Left, long Named, long Slices, List<LogEvent> Page)> CountAsync(
        QueryExecutor query, Case c, int count, bool forward)
    {
        long left = QueryExecutor.GroupsLeftForLater, named = QueryExecutor.GroupsNamedLate, slices = SegmentReader.PooledSliceRents;
        var  page = await RunAsync(query, c, count, forward);
        return (QueryExecutor.GroupsLeftForLater - left, QueryExecutor.GroupsNamedLate - named,
                SegmentReader.PooledSliceRents - slices, page);
    }
}
