using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Storage;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using EventId  = Ameto.Core.EventId;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Query.Tests;

/// <summary>
/// A MERGE THAT COMMITS WHILE A QUERY RUNS MUST NOT COST THE QUERY A ROW (#114).
///
/// <para>The query snapshots the catalog and then opens the window's segments LAZILY, a batch at
/// a time as its merge front reaches them. A merge commit in between swaps a batch of them out for
/// its output and unlinks them; opened by path afterwards, a source was gone — the prefilter fell
/// back, the scan's open failed and skipped it without a word — and the output holding its rows
/// was not in the query's snapshot. Measured at b6df5e9 (main, after the lazy prefilter of #109)
/// with a merge after the first row over these 40 segments: a filtered query returned 50 of 252
/// rows backward and 51 of 252 forward, an unfiltered one 25 of 1008 and 26 of 1008, and every one
/// of them ended as a normal, complete-looking stream.</para>
///
/// <para>The query now pins every segment of its window with a file handle right after the
/// snapshot, and maps from the pin; these tests hold it to the whole answer, in order, across a
/// merge, and to releasing every pin and every reader on every exit. A segment that vanishes
/// BEFORE its pin is sorted the way the header aggregation sorts one that vanishes under its scan
/// (LogVolumeUnreadableSegmentTests): a race with a merge or retention is neither warned nor short;
/// a file gone behind the catalog's back is named at Warning, once.</para>
///
/// <para>The counters are process-wide, so this depends on the assembly's
/// <c>DisableTestParallelization</c> (see AssemblyInfo.cs).</para>
/// </summary>
public sealed class QueryWindowPinTests : IAsyncLifetime
{
    private const int    Segments = QuerySegmentFixtures.ManySegments;
    private const string Filtered = "Customer = 'cust-1'";   // ~6 rows in every segment, 252 in all

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-windowpin-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper               _out;
    private readonly CapturingLogger<QueryExecutor>  _queryLog  = new();
    private readonly CapturingLogger<StorageEngine>  _engineLog = new();
    private StorageEngine _engine = null!;
    private QueryExecutor _query  = null!;

    public QueryWindowPinTests(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync()
    {
        (_engine, _) = await QuerySegmentFixtures.ManyIndexedSegmentsAsync(_dir, _engineLog);
        _query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), _queryLog);
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ulong Id(LogEvent ev) => ev.Id.RawValue;

    private static QueryRequest Request(string? filter, int count, bool forward, long? afterTs = null, EventId? afterId = null) => new()
    {
        Filter              = filter,
        Count               = count,
        Direction           = forward ? QueryDirection.Forward : QueryDirection.Backward,
        AfterTimestampTicks = afterTs,
        AfterEventId        = afterId,
    };

    /// <summary>Every row the store holds for <paramref name="filter"/> right now, in query order.</summary>
    private Task<List<LogEvent>> FullReadAsync(string? filter, bool forward = false) =>
        QuerySegmentFixtures.RunAsync(_query, filter, int.MaxValue, forward: forward);

    private string[] SegmentPaths() => _engine.ListSegments().Select(s => s.FilePath).ToArray();

    private static List<CapturingLogger<T>.Entry> Warnings<T>(CapturingLogger<T> log) =>
        log.Entries.Where(e => e.Level >= LogLevel.Warning).ToList();

    private static bool LoggedLeftTheCatalog(CapturingLogger<QueryExecutor> log) =>
        log.Entries.Any(e => e.Level == LogLevel.Debug && e.Message.Contains("left the catalog", StringComparison.Ordinal));

    // ── A merge under a running query ─────────────────────────────────────────

    /// <summary>
    /// THE BUG, as the issue reproduced it: a merge of all 40 segments into one, run after the
    /// query's first row. Red at b6df5e9: 50 / 252, 51 / 252, 25 / 1008 and 26 / 1008 rows (filtered
    /// backward, filtered forward, unfiltered backward, unfiltered forward).
    ///
    /// <para>And the pins are what held the sources, not a leak: once the query is over, every
    /// parked delete goes through and every source file is gone. On Windows the merge could not
    /// unlink a pinned source and parked it; on Linux the unlink went through at once and the pin
    /// kept the inode.</para>
    /// </summary>
    [Theory]
    [InlineData(Filtered, false)]
    [InlineData(Filtered, true)]
    [InlineData(null, false)]
    [InlineData(null, true)]
    public async Task AMergeAfterTheFirstRowCostsTheQueryNoRow(string? filter, bool forward)
    {
        var expected = (await FullReadAsync(filter, forward)).Select(Id).ToArray();
        var sources  = SegmentPaths();

        long opens = SegmentReader.Opens, closes = SegmentReader.Closes;
        long pins  = QueryExecutor.Pins,  unpins = QueryExecutor.Unpins;

        var  rows   = new List<LogEvent>();
        bool merged = false;
        await foreach (var ev in _query.ExecuteAsync(Request(filter, int.MaxValue, forward)))
        {
            rows.Add(ev);
            if (rows.Count == 1) merged = await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None);
        }

        _out.WriteLine($"filter={filter ?? "<none>"} forward={forward}: {rows.Count} / {expected.Length} rows");
        Assert.True(merged, "setup: the merge did not run");
        Assert.Single(_engine.ListSegments());

        Assert.Equal(expected, rows.Select(Id).ToArray());

        Assert.Equal(SegmentReader.Opens - opens, SegmentReader.Closes - closes);
        Assert.Equal(Segments, QueryExecutor.Pins - pins);                    // every segment of the window, once
        Assert.Equal(QueryExecutor.Pins - pins, QueryExecutor.Unpins - unpins);
        Assert.Empty(Warnings(_queryLog));

        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        foreach (var path in sources) Assert.False(File.Exists(path), $"source {Path.GetFileName(path)} outlived the query that held it");
    }

    /// <summary>
    /// The same merge under a pager: pages of 100 with the (ts, id) cursor, the merge after the
    /// first row of the first page. The first page reads the sources through its pins, every later
    /// page reads the merged output, and the pages join up into the whole without a gap or a
    /// repeat. At b6df5e9 the first page came back short — the rows its pins now keep were gone —
    /// so the walk took it for the last page and ended there.
    /// </summary>
    [Theory]
    [InlineData(Filtered, false)]
    [InlineData(Filtered, true)]
    [InlineData(null, false)]
    [InlineData(null, true)]
    public async Task ACursorWalkAcrossAMergeJoinsUpIntoTheWhole(string? filter, bool forward)
    {
        const int Page = 100;
        var expected = (await FullReadAsync(filter, forward)).Select(Id).ToArray();

        var      walked  = new List<LogEvent>();
        long?    afterTs = null;
        EventId? afterId = null;
        bool     tried   = false, merged = false;
        for (int page = 0; page <= expected.Length / Page + 1; page++)
        {
            var rows = new List<LogEvent>();
            await foreach (var ev in _query.ExecuteAsync(Request(filter, Page, forward, afterTs, afterId)))
            {
                rows.Add(ev);
                if (!tried && rows.Count == 1)
                {
                    tried  = true;
                    merged = await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None);
                }
            }

            walked.AddRange(rows);
            if (rows.Count < Page) break;
            afterTs = rows[^1].Timestamp.UtcTicks;
            afterId = rows[^1].Id;
        }

        _out.WriteLine($"filter={filter ?? "<none>"} forward={forward}: walked {walked.Count} / {expected.Length} rows");
        Assert.True(merged, "setup: the merge did not run");
        Assert.Equal(expected, walked.Select(Id).ToArray());
    }

    /// <summary>
    /// What a merge under a running query costs the STORAGE side, which is Windows': it cannot
    /// unlink the sources the query pinned, so it parks them for the retry and keeps its manifest
    /// for the recovery sweep. Expected, and quiet: the query holds them for as long as it runs, so
    /// every maintenance pass in that time meets them still there. The sweep used to try each one
    /// itself and warn per source per pass — measured over this fixture before it left the parked
    /// ones to their retry, 80 Warnings for two passes under one query, plus the merge's own
    /// "still held open" Warning. Once the query lets go, the parked deletes go through and the
    /// next pass drops the manifest. On Linux the unlinks never fail and this is trivially quiet.
    /// </summary>
    [Fact]
    public async Task AMergeUnderARunningQueryParksItsSourcesWithoutAWarningStorm()
    {
        var sources = SegmentPaths();

        var it = _query.ExecuteAsync(Request(null, int.MaxValue, forward: false)).GetAsyncEnumerator();
        int rows;
        try
        {
            Assert.True(await it.MoveNextAsync());                 // the plan is taken: every segment pinned
            Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge did not run");

            _out.WriteLine($"{sources.Count(File.Exists)} of {sources.Length} sources still on disk under the query, " +
                           $"{_engine.PendingSegmentDeleteCount} parked");
            for (int pass = 0; pass < 2; pass++)
                await _engine.RunColdMaintenancePassAsync(CancellationToken.None);

            rows = 1;
            while (await it.MoveNextAsync()) rows++;
        }
        finally { await it.DisposeAsync(); }

        Assert.Equal(QuerySegmentFixtures.ManyIndexedEvents, rows);

        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        foreach (var path in sources) Assert.False(File.Exists(path), $"source {Path.GetFileName(path)} outlived the query");
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(_dir, "segments"), "*.mergemanifest"));

        _out.WriteLine($"storage warnings: {Warnings(_engineLog).Count}");
        Assert.Empty(Warnings(_engineLog));
        Assert.Empty(Warnings(_queryLog));
    }

    // ── Every pin released, on every exit ─────────────────────────────────────

    public enum Exit { PageFull, ConsumerStops, Cancelled, ConsumerThrows }

    /// <summary>
    /// A pin is a handle with no mapping behind it, so the reader counters cannot see a leaked one,
    /// and on Windows a leaked one is a segment file nothing can delete while the process lives.
    /// Counted against the pins taken, and then the operation retention and the merge need — a
    /// rename — tried on every segment file.
    /// </summary>
    [Theory]
    [InlineData(Exit.PageFull)]
    [InlineData(Exit.ConsumerStops)]
    [InlineData(Exit.Cancelled)]
    [InlineData(Exit.ConsumerThrows)]
    public async Task EveryPinAndReaderIsReleasedOnEveryExit(Exit exit)
    {
        long opens = SegmentReader.Opens, closes = SegmentReader.Closes;
        long pins  = QueryExecutor.Pins,  unpins = QueryExecutor.Unpins;

        using var cts = new CancellationTokenSource();
        int n = 0;
        try
        {
            await foreach (var _ in _query.ExecuteAsync(Request(Filtered, exit == Exit.PageFull ? 5 : 200, forward: false), cts.Token))
            {
                n++;
                if (n < 3) continue;
                if (exit == Exit.ConsumerStops)  break;
                if (exit == Exit.Cancelled)      cts.Cancel();
                if (exit == Exit.ConsumerThrows) throw new InvalidOperationException("the consumer gave up");
            }
        }
        catch (OperationCanceledException) when (exit == Exit.Cancelled) { }
        catch (InvalidOperationException)   when (exit == Exit.ConsumerThrows) { }

        Assert.True(n >= 3, $"setup: only {n} rows arrived");
        Assert.Equal(Segments, QueryExecutor.Pins - pins);
        Assert.Equal(QueryExecutor.Pins - pins, QueryExecutor.Unpins - unpins);
        Assert.Equal(SegmentReader.Opens - opens, SegmentReader.Closes - closes);

        foreach (var path in SegmentPaths())
        {
            File.Move(path, path + ".moved");    // throws on Windows if anything still holds it
            File.Move(path + ".moved", path);
        }
    }

    // ── A segment that vanishes before its pin ────────────────────────────────

    /// <summary>
    /// A MERGE COMMITTING BETWEEN THE SNAPSHOT AND THE PIN. Nothing holds the sources yet, so the
    /// merge unlinks every one of them and the pins find no file. The header aggregation calls such
    /// a count a floor, because by then it has read the rest of its window; the query has read
    /// nothing yet, so it takes its plan again, and the new snapshot lists the merged output. The
    /// whole answer, no Warning — nothing is damaged — and the race at Debug.
    ///
    /// <para>Seen red with the retake taken out (the vanished sources left in the plan unpinned):
    /// 0 of 252 filtered rows and 0 of 1008 unfiltered, every source warned as unopenable.</para>
    /// </summary>
    [Theory]
    [InlineData(Filtered)]
    [InlineData(null)]
    public async Task AMergeBetweenTheSnapshotAndThePinIsReadFromItsOutput(string? filter)
    {
        var expected = (await FullReadAsync(filter)).Select(Id).ToArray();
        var sources  = SegmentPaths();
        long pins    = QueryExecutor.Pins, unpins = QueryExecutor.Unpins;

        int  calls  = 0;
        bool merged = false;
        _query.BeforePinForTest = async _ =>
        {
            if (++calls == 1) merged = await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None);
        };
        List<LogEvent> rows;
        try     { rows = await FullReadAsync(filter); }
        finally { _query.BeforePinForTest = null; }

        _out.WriteLine($"filter={filter ?? "<none>"}: {rows.Count} / {expected.Length} rows over {calls} plan(s)");
        Assert.True(merged, "setup: the merge did not run");
        foreach (var path in sources) Assert.False(File.Exists(path), "setup: nothing held the sources, so the merge should have unlinked them");

        Assert.Equal(expected, rows.Select(Id).ToArray());
        Assert.Equal(2, calls);                                              // the plan was taken twice
        Assert.Equal(1, QueryExecutor.Pins - pins);                          // the second plan pinned the output alone
        Assert.Equal(QueryExecutor.Pins - pins, QueryExecutor.Unpins - unpins);
        Assert.Empty(Warnings(_queryLog));
        Assert.True(LoggedLeftTheCatalog(_queryLog), "the race should be on record at Debug");
    }

    /// <summary>
    /// RETENTION DELETING A SEGMENT BETWEEN THE SNAPSHOT AND THE PIN: its events have left the
    /// store, so a query that leaves them out is exact — not short, and not warned. The mirror of
    /// the header aggregation's "a segment removed from the catalog while the scan runs is neither
    /// partial nor warned". Seen red with the retake taken out: the deleted segment stayed in the
    /// plan unpinned, and its open by path was warned at both sites as a lost file.
    /// </summary>
    [Fact]
    public async Task ARetentionDeleteBetweenTheSnapshotAndThePinIsNeitherShortNorWarned()
    {
        int before = (await FullReadAsync(Filtered)).Count;

        SegmentInfo? deleted = null;
        int calls = 0;
        _query.BeforePinForTest = async window =>
        {
            if (++calls != 1) return;
            deleted = window[0];                                             // the newest: the first a backward page needs
            await _engine.DeleteSegmentAsync(SegmentKey.Of(deleted));
        };
        List<LogEvent> rows;
        try     { rows = await FullReadAsync(Filtered); }
        finally { _query.BeforePinForTest = null; }

        Assert.NotNull(deleted);
        Assert.False(File.Exists(deleted!.FilePath), "setup: the delete should have removed the file");

        var now = await FullReadAsync(Filtered);                             // the store as it is now
        Assert.True(now.Count < before, "setup: the deleted segment held no matching rows");
        Assert.Equal(now.Select(Id).ToArray(), rows.Select(Id).ToArray());
        Assert.Empty(Warnings(_queryLog));
        Assert.True(LoggedLeftTheCatalog(_queryLog), "the race should be on record at Debug");
        Assert.Equal(2, calls);                                              // the plan was taken twice
    }

    /// <summary>
    /// A FILE GONE BEHIND THE CATALOG'S BACK: the catalog still serves the segment and nothing will
    /// bring its rows back, so the query cannot be whole — and must say so where it can, by name, at
    /// Warning. Once per file and site across queries, like the header aggregation's "named once at
    /// Warning": the next query logs it at Debug. Red at b6df5e9: the prefilter logged Debug and the
    /// scan nothing at all, so both queries came back short and the log was silent.
    /// </summary>
    [Fact]
    public async Task ASegmentFileGoneBehindTheCatalogsBackIsNamedOnceAtWarning()
    {
        var lost = _engine.ListSegments().MaxBy(s => s.MaxTimestampTicks)!;
        File.Delete(lost.FilePath);                                          // the entry stays

        long pins = QueryExecutor.Pins, unpins = QueryExecutor.Unpins;
        var first  = await FullReadAsync(Filtered);
        var second = await FullReadAsync(Filtered);
        Assert.Equal(2 * (Segments - 1), QueryExecutor.Pins - pins);          // everything else was pinned
        Assert.Equal(QueryExecutor.Pins - pins, QueryExecutor.Unpins - unpins);

        // Short by exactly the lost segment: what a query returns once the catalog agrees.
        await _engine.DeleteSegmentAsync(SegmentKey.Of(lost));
        var expected = (await FullReadAsync(Filtered)).Select(Id).ToArray();
        Assert.Equal(expected, first.Select(Id).ToArray());
        Assert.Equal(expected, second.Select(Id).ToArray());

        var warnings = Warnings(_queryLog);
        var scan = Assert.Single(warnings, e => e.Message.Contains("its rows are missing", StringComparison.Ordinal));
        Assert.Contains(lost.FilePath, scan.Message, StringComparison.Ordinal);
        Assert.IsType<FileNotFoundException>(scan.Error);
        var prefilter = Assert.Single(warnings, e => e.Message.Contains("Index prefilter could not open", StringComparison.Ordinal));
        Assert.Contains(lost.FilePath, prefilter.Message, StringComparison.Ordinal);
        Assert.Equal(2, warnings.Count);

        // The repeat is still there for whoever turns Debug on.
        Assert.Contains(_queryLog.Entries, e => e.Level == LogLevel.Debug
                                                && e.Message.Contains("its rows are missing", StringComparison.Ordinal)
                                                && e.Error is FileNotFoundException);
    }

    // ── Logger ────────────────────────────────────────────────────────────────

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public readonly record struct Entry(LogLevel Level, string Message, Exception? Error);

        private readonly List<Entry> _entries = [];

        public IReadOnlyList<Entry> Entries
        {
            get { lock (_entries) return _entries.ToList(); }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => true;

        public void Log<TState>(LogLevel level, Microsoft.Extensions.Logging.EventId eventId, TState state,
                                Exception? error, Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add(new Entry(level, formatter(state, error), error));
        }
    }
}
