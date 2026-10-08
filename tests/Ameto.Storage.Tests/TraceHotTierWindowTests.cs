using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// A PAGE OVER THE UNFLUSHED SPANS RETURNS WHAT IT RETURNED BEFORE THE HOT TIER WAS INDEXED (#94):
/// both stream fetchers, paged down their window the way <c>StreamTracePagesAsync</c> pages them,
/// against an oracle that does not share the engine's code.
///
/// <para><b>The trace list</b> is checked against <see cref="ListModel"/>, a first-principles
/// restatement of <c>GetTraceListAsync</c>: every in-window unflushed span merged in tier order
/// (the detached flush snapshot, then the live tier), the cold segments' summaries merged after
/// them under the same scan cap, the filters, newest-first, the <c>limit</c> cut and the floor it
/// leaves. Every field of every row is compared, the service set IN ORDER, plus Capped, the floor
/// and the fault bit.</para>
///
/// <para><b>The TraceQL fetch</b> is checked against <see cref="ReferenceHotPass"/> — the bounded
/// top-K heap <c>SearchSpansAsync</c> ran over the tier in tier order, verbatim — on states with no
/// cold segment in the window, where the hot pass is the whole answer: the spans, their order, and
/// the floor the scan reports.</para>
///
/// <para>THE DATA IS HOSTILE ON PURPOSE (<see cref="Corpus"/>): spans arrive out of order within
/// their trace, five percent of them hundreds to thousands of positions late (a long span reported
/// when it ends), traces whose root is missing, doubled, or starts after a child, a few traces
/// stamped thirty seconds in the future, exact duplicates re-sent later (an exporter's retry, a WAL
/// replay), spans with no span id, long traces that straddle many page ceilings, and a service
/// spelled two ways that the filter treats as one and the service set as two. Start times are
/// unique per span, so the order every oracle produces is total and a page has one right answer.
/// </para>
///
/// <para>THE STATES are the ones an index over the tier can get wrong: the live tier alone; a flush
/// parked mid-build, so the detached snapshot and a new tier are read side by side; the same after
/// the segment is published, with its rows coming back out of a cold sidecar; a flush that FAILED,
/// whose snapshot is put back in front of the new tier as one rebuilt list; and a stream that pages
/// across a flush, so consecutive pages see different tier generations.</para>
/// </summary>
public sealed class TraceHotTierWindowTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddMinutes(10);
    private static readonly TimeSpan HangGuard  = TimeSpan.FromSeconds(30);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-hotwin-" + Guid.NewGuid().ToString("N"));
    private readonly Xunit.Abstractions.ITestOutputHelper _out;

    public TraceHotTierWindowTests(Xunit.Abstractions.ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    private TraceStorageEngine NewEngine(string? sub = null)
    {
        string dir = sub is null ? _dir : Path.Combine(_dir, sub);
        Directory.CreateDirectory(dir);
        return new(dir, NullLogger<TraceStorageEngine>.Instance);
    }

    // ── The filters each state is paged under ───────────────────────────────────

    private sealed record ListFilter(string? Service, string? Name, SpanStatusCode? Status, long? MinDur, long? MaxDur, int Limit)
    {
        public override string ToString() =>
            $"service={Service ?? "-"} name={Name ?? "-"} status={Status?.ToString() ?? "-"} " +
            $"min={MinDur?.ToString() ?? "-"} max={MaxDur?.ToString() ?? "-"} limit={Limit}";
    }

    private static readonly ListFilter[] ListFilters =
    [
        new(null,      null,   null,                 null,       null,          500),
        new(null,      null,   null,                 null,       null,          25),
        new(null,      null,   null,                 null,       null,          7),
        new("billing", null,   null,                 null,       null,          20),
        new(null,      "GET",  null,                 null,       null,          30),
        new(null,      null,   SpanStatusCode.Error, null,       null,          15),
        new(null,      null,   null,                 50_000_000, null,          40),
        new(null,      null,   null,                 null,       900_000_000,   60),
    ];

    private sealed record QlFilter(string? Service, string? Name, SpanStatusCode? Status, short? Http, long? MinDur, long? MaxDur, int Limit)
    {
        public override string ToString() =>
            $"service={Service ?? "-"} name={Name ?? "-"} status={Status?.ToString() ?? "-"} http={Http?.ToString() ?? "-"} " +
            $"min={MinDur?.ToString() ?? "-"} max={MaxDur?.ToString() ?? "-"} limit={Limit}";
    }

    private static readonly QlFilter[] QlFilters =
    [
        new(null,      null,       null,                 null, null,       null,        2_000),
        new(null,      null,       null,                 null, null,       null,        70),
        new(null,      null,       null,                 null, null,       null,        1),
        new("billing", null,       null,                 null, null,       null,        90),
        new(null,      "redis",    null,                 null, null,       null,        25),
        new(null,      null,       SpanStatusCode.Error, null, null,       null,        40),
        new(null,      null,       null,                 500,  null,       null,        5),
        new(null,      null,       null,                 null, 10_000_000, 400_000_000, 120),
    ];

    // ── The states ──────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Pages_over_the_live_tier_match_the_oracles(int seed)
    {
        var corpus = Corpus(seed, traces: 420);
        using var engine = NewEngine();
        Write(engine, corpus);

        await AssertListPagesAsync(engine, flushing: [], "live tier");
        AssertQlPages(engine, flushing: [], "live tier");
    }

    /// <summary>
    /// A flush parked inside its segment build: its snapshot has left the live tier and its segment
    /// is not registered, so every page reads the snapshot and the NEW tier as two runs. Then the
    /// flush is let go and the same pages are read again, the snapshot's rows now coming out of the
    /// cold sidecar.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(5)]
    public async Task Pages_across_an_in_flight_flush_and_its_publication_match_the_oracles(int seed)
    {
        var corpus = Corpus(seed, traces: 420);
        int half   = corpus.Count / 2;
        using var engine = NewEngine();
        Write(engine, corpus.GetRange(0, half));

        using var parked  = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        engine._beforeSegmentWrite = () => { parked.Set(); release.Wait(HangGuard); };

        var snapshot = engine.HotSpansForTest;     // what the flush is about to detach, in tier order
        engine.FlushIfDue();
        Assert.True(parked.Wait(HangGuard), "the flush never reached its segment build");
        Write(engine, corpus.GetRange(half, corpus.Count - half));

        try
        {
            await AssertListPagesAsync(engine, snapshot, "in-flight flush");
            AssertQlPages(engine, snapshot, "in-flight flush");
        }
        finally
        {
            release.Set();
            engine.WaitForFlushForTest();
            engine._beforeSegmentWrite = null;
        }

        Assert.Equal(1, engine.ColdSegmentCountForTest);
        await AssertListPagesAsync(engine, flushing: [], "published");
    }

    /// <summary>
    /// A flush whose segment build FAILED puts its snapshot back in front of whatever arrived since,
    /// as one new list — the one generation change that rebuilds the tier rather than swapping it.
    /// </summary>
    [Theory]
    [InlineData(6)]
    [InlineData(7)]
    public async Task Pages_after_a_failed_flush_restored_its_snapshot_match_the_oracles(int seed)
    {
        var corpus = Corpus(seed, traces: 420);
        int half   = corpus.Count / 2;
        using var engine = NewEngine();
        Write(engine, corpus.GetRange(0, half));

        using var parked  = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        engine._beforeSegmentWrite = () =>
        {
            parked.Set();
            release.Wait(HangGuard);
            throw new IOException("simulated: the segment could not be written");
        };
        engine.FlushIfDue();
        Assert.True(parked.Wait(HangGuard), "the flush never reached its segment build");
        Write(engine, corpus.GetRange(half, corpus.Count - half));   // into the new tier, behind the snapshot
        release.Set();
        engine.WaitForFlushForTest();
        engine._beforeSegmentWrite = null;

        Assert.Equal(0, engine.ColdSegmentCountForTest);
        Assert.Equal(corpus.Count, engine.HotSpansForTest.Count);

        await AssertListPagesAsync(engine, flushing: [], "restored");
        AssertQlPages(engine, flushing: [], "restored");

        // And the restored tier keeps taking spans: an index rebuilt for it must keep up.
        Write(engine, Corpus(seed + 100, traces: 60, firstTrace: 10_000));
        await AssertListPagesAsync(engine, flushing: [], "restored, then appended to");
        AssertQlPages(engine, flushing: [], "restored, then appended to");
    }

    /// <summary>
    /// ONE STREAM ACROSS TWO TIER GENERATIONS: the first pages read the live tier, the tier is then
    /// flushed and refilled, and the pages that follow — same cursor, carried over — read a cold
    /// segment and a new tier. Each page must still be the model's page for the state it read.
    /// </summary>
    [Theory]
    [InlineData(8)]
    [InlineData(9)]
    public async Task A_stream_that_pages_across_a_flush_matches_the_model_page_by_page(int seed)
    {
        var corpus = Corpus(seed, traces: 420);
        int half   = corpus.Count / 2;
        int crossed = 0;

        for (int fi = 0; fi < ListFilters.Length; fi++)
        {
            var f = ListFilters[fi];
            using var engine = NewEngine($"filter-{fi}");
            Write(engine, corpus.GetRange(0, half));

            long fromNano = Nano(From);
            long cursor   = Nano(To);
            for (int page = 0; page < 40; page++)
            {
                if (page == 1)
                {
                    engine.FlushHotTier();                                         // the generation changes here
                    Write(engine, corpus.GetRange(half, corpus.Count - half));     // and the new tier fills
                    crossed++;
                }

                Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
                var actual   = await List(engine, f, pageTo);
                var expected = ListModel(engine.HotSpansForTest, [], engine.ColdSegmentsForTest, f, pageTo);
                AssertSamePage(expected, actual, $"across a flush, {f}, page {page}");

                if (!NextCursor(actual, cursor, fromNano, out cursor)) break;
            }
        }

        // Most filters page more than once over half the corpus; the ones that end on their first
        // page never reach the flush, and that is fine as long as the rest do.
        Assert.True(crossed >= ListFilters.Length / 2, $"only {crossed} of {ListFilters.Length} streams crossed the flush");
    }

    /// <summary>
    /// A tier rebuilt from the write-ahead log after a crash goes through the same insert as live
    /// ingest, so it is indexed the same way — and pages over it are the model's pages.
    /// </summary>
    [Fact]
    public async Task Pages_over_a_tier_replayed_from_the_log_match_the_oracles()
    {
        var corpus = Corpus(10, traces: 420);
        var crashed = SpanWriteAheadLog.Open(Path.Combine(_dir, "spans.wal"));
        foreach (var item in corpus) crashed.Append(item);
        crashed.Dispose();

        using var engine = NewEngine();
        Assert.Equal(corpus.Count, engine.HotSpansForTest.Count);

        await AssertListPagesAsync(engine, flushing: [], "replayed");
        AssertQlPages(engine, flushing: [], "replayed");
    }

    // ── What the bounded reads promise beyond their answers ─────────────────────

    /// <summary>
    /// THE TRACEQL HOT PASS WALKS WITH THE LOCK FREE, and answers for exactly what it captured. It is
    /// parked after its capture, before its walk; from there another thread must take the engine's
    /// write lock without waiting (TryEnterWriteLock(0): no timer decides it), and 500 newer spans and
    /// a whole flush go in underneath it — appends past the captured prefix, a list growth, a
    /// widening of the captured partial block, a detach and a publish. Let go, the walk must return
    /// the newest of the spans it captured — not one of the newer ones. Back inside the read lock,
    /// the writer cannot get in.
    ///
    /// <para>This does not race a walk: nothing runs while the walk itself is reading.
    /// <see cref="Readers_racing_a_writer_get_exactly_the_page_of_what_they_captured"/> does.</para>
    /// </summary>
    [Fact]
    public async Task The_TraceQL_hot_pass_walks_with_the_lock_free_and_answers_for_what_it_captured()
    {
        using var engine = NewEngine();
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 2_000)]);
        var captured = engine.HotSpansForTest;

        using var parked  = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        bool readLockHeld = true;
        engine._hotSearchPassForTest = () =>
        {
            readLockHeld = engine.LockForTest.IsReadLockHeld;
            parked.Set();
            release.Wait(HangGuard);
        };

        var call = Task.Run(() => engine.SearchSpansAsync(From, To, limit: 300).ToBlockingEnumerable().ToList());
        Assert.True(parked.Wait(HangGuard), "hang guard: the hot pass never started");

        bool writerGotIn = false;
        var writer = new Thread(() =>
        {
            if (engine.LockForTest.TryEnterWriteLock(0)) { writerGotIn = true; engine.LockForTest.ExitWriteLock(); }
        });
        writer.Start();
        writer.Join();
        if (writerGotIn)
        {
            Write(engine, [.. TraceAggregateLockProbe.Corpus(2_000, 500)]);   // newer than anything captured
            engine.FlushHotTier();
        }
        release.Set();
        var got = await call.WaitAsync(HangGuard);

        Assert.False(readLockHeld, "the TraceQL hot pass walked holding the read lock");
        Assert.True(writerGotIn, "a writer could not take the lock while the hot pass was parked after its capture");
        var (kept, _) = ReferenceHotPass(captured, static _ => true, 300);
        Assert.Equal(kept.Select(Describe), got.Select(Describe));
    }

    // What the engine captured for the read in progress on this thread — handed over by its seams.
    [ThreadStatic] private static SpanRecord[]?      t_flushing;
    [ThreadStatic] private static SpanRecord[]?      t_hot;
    [ThreadStatic] private static SpanSegmentInfo[]? t_cold;

    /// <summary>
    /// READERS RACING A WRITER GET EXACTLY THE PAGE OF WHAT THEY CAPTURED (#122 review N2). Three
    /// readers page both fetchers — the TraceQL hot pass and the trace list — while one thread
    /// appends spans in drainer-sized holds and another flushes the tier every few tens of
    /// milliseconds, alternately publishing a segment and failing to (so the tier is detached,
    /// published, or put back as a rebuilt list underneath the walks). Every page is checked against
    /// the oracle over EXACTLY the runs it captured, copied in the same read-lock hold through the
    /// engine's seams: <see cref="ReferenceHotPass"/> for the hot pass, <see cref="ListModel"/> for the
    /// list. A walk that read anything but its captured prefix — an element overwritten, a block
    /// skipped on a bound narrower than its spans — returns a page the oracle does not.
    ///
    /// <para>NEVER RED BY CHANCE: nothing here is timed, every start is unique (ties would let two
    /// right answers differ), and a reader's verdict depends only on what it captured. Only the
    /// amount of racing varies with the machine; the readers each finish at least
    /// <c>MinPages</c> pages, and the writer does not start until every reader is reading.</para>
    /// </summary>
    [Fact]
    public void Readers_racing_a_writer_get_exactly_the_page_of_what_they_captured()
    {
        const int Readers = 3, MinPages = 15, Spans = 16_000, Hold = 256;
        using var engine = NewEngine();
        var corpus = RaceCorpus(Spans);
        engine._hotSearchCapturedForTest = static (f, h) => { t_flushing = f; t_hot = h; };
        engine._listCapturedForTest      = static (c, f, h) => { t_cold = c; t_flushing = f; t_hot = h; };

        var  failures   = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var  pages      = new int[Readers];
        int  writerDone = 0;
        using var reading = new CountdownEvent(Readers);
        long newest = corpus.Max(static s => s.StartTimeUnixNano);

        var readers = Enumerable.Range(0, Readers).Select(r => new Thread(() =>
        {
            var rnd = new Random(1_000 + r);
            try
            {
                reading.Signal();
                for (int page = 0; Volatile.Read(ref writerDone) == 0 || page < MinPages; page++)
                {
                    var to = DateTimeOffset.FromUnixTimeMilliseconds(
                        rnd.Next(4) == 0 ? To.ToUnixTimeMilliseconds()
                                         : (Nano(Base) + rnd.NextInt64(0, newest - Nano(Base))) / 1_000_000L);
                    if (page % 2 == 0) CheckListPage(engine, rnd, to, failures, r, page);
                    else               CheckHotPass(engine, rnd, to, failures, r, page);
                    pages[r]++;
                }
            }
            catch (Exception ex) { failures.Enqueue($"reader {r} threw: {ex}"); }
        }) { IsBackground = true, Name = $"race-reader-{r}" }).ToList();
        readers.ForEach(static t => t.Start());
        Assert.True(reading.Wait(HangGuard), "hang guard: the readers never started");

        var flusher = new Thread(() =>
        {
            try
            {
                for (int n = 0; Volatile.Read(ref writerDone) == 0; n++)
                {
                    Thread.Sleep(25);
                    engine._beforeSegmentWrite = n % 2 == 1 ? (Action)(static () => throw new IOException("simulated")) : null;
                    engine.FlushHotTier();   // detach, build, then publish — or restore the tier
                }
            }
            catch (Exception ex) { failures.Enqueue($"flusher threw: {ex}"); }
            finally { engine._beforeSegmentWrite = null; }
        }) { IsBackground = true, Name = "race-flusher" };
        flusher.Start();

        for (int i = 0; i < corpus.Count; i += Hold)
        {
            var call = corpus.GetRange(i, Math.Min(Hold, corpus.Count - i)).ToArray();
            if (engine.WriteSpans(call) != call.Length) failures.Enqueue("a hold was refused");
            Thread.Sleep(2);
        }
        Volatile.Write(ref writerDone, 1);

        Assert.True(flusher.Join(HangGuard), "hang guard: the flusher never finished");
        foreach (var t in readers) Assert.True(t.Join(HangGuard * 4), "hang guard: a reader never finished");

        _out.WriteLine($"pages checked per reader: {string.Join(", ", pages)}; cold segments at the end: "
                     + $"{engine.ColdSegmentCountForTest}; failures: {failures.Count}");
        Assert.True(failures.IsEmpty, string.Join(Environment.NewLine, failures.Take(5)));
        Assert.All(pages, static p => Assert.True(p >= MinPages));
        Assert.Equal(0, engine.UnindexedCapturesForTest);
    }

    private static void CheckListPage(TraceStorageEngine engine, Random rnd, DateTimeOffset to,
                                      System.Collections.Concurrent.ConcurrentQueue<string> failures, int reader, int page)
    {
        var f    = ListFilters[rnd.Next(ListFilters.Length)];
        var task = engine.GetTraceListAsync(From, to, f.Service, f.Name, f.Status, f.MinDur, f.MaxDur, f.Limit);
        if (!task.IsCompleted) { failures.Enqueue($"reader {reader} page {page}: a list page yielded"); return; }
        var actual   = task.GetAwaiter().GetResult();
        var expected = ListModel([.. t_hot!], [.. t_flushing!], t_cold!, f, to);
        try { AssertSamePage(expected, actual, $"reader {reader} page {page} (list, {f}, to {to:O})"); }
        catch (Exception ex) { failures.Enqueue(ex.Message); }
    }

    private static void CheckHotPass(TraceStorageEngine engine, Random rnd, DateTimeOffset to,
                                     System.Collections.Concurrent.ConcurrentQueue<string> failures, int reader, int page)
    {
        SpanStatusCode? status = rnd.Next(3) == 0 ? SpanStatusCode.Error : null;
        int  limit = rnd.Next(3) switch { 0 => 10, 1 => 200, _ => 2_000 };
        var  got   = engine.HotMatchesForTest(From, to, status, limit, out bool evicted);
        long fromNano = Nano(From), toNano = Nano(to);
        var (kept, expectEvicted) = ReferenceHotPass(
            [.. t_flushing!, .. t_hot!],
            s => s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano && (status is null || s.Status == status.Value),
            limit);
        if (evicted != expectEvicted || !kept.Select(Describe).SequenceEqual(got.Select(Describe)))
            failures.Enqueue($"reader {reader} page {page} (hot pass, status {status?.ToString() ?? "-"}, limit {limit}, "
                           + $"to {to:O}): {got.Count} spans / evicted {evicted}, the reference has "
                           + $"{kept.Count} / {expectEvicted}");
    }

    /// <summary>
    /// The race's spans: three services, one span a millisecond, and the disorder that loosens the
    /// start index's bounds — one in a hundred 30 s ahead, one in fifty reported 10 s late. Moved by
    /// a fraction of a millisecond as well, so no two starts are equal.
    /// </summary>
    private static List<SpanIngestItem> RaceCorpus(int count)
    {
        var items = TraceAggregateLockProbe.Corpus(0, count, services: 3);
        for (int i = 0; i < items.Length; i++)
        {
            long shift = i % 100 == 37 ? 30_000_000_000L + 250_000L
                       : i % 50  == 11 ? -10_000_000_000L - 500_000L
                       :                 0;
            if (shift != 0) items[i] = At(items[i], items[i].StartTimeUnixNano + shift);
        }
        return [.. items];
    }

    /// <summary>
    /// A TRACEQL PAGE READS THE TOP OF ITS WINDOW, NOT THE TIER. 20 000 spans in arrival order, a
    /// page of 200: the walk reads the blocks holding the newest 200 and one block more, where it
    /// read all 20 000 before. A page deep in the window does not open the blocks above its ceiling
    /// at all.
    /// </summary>
    [Fact]
    public void A_TraceQL_page_reads_the_top_of_its_window_not_the_tier()
    {
        using var engine = NewEngine();
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 20_000)]);   // one span a millisecond from Base
        int visited = -1;
        engine._hotSearchVisitedForTest = n => visited = n;

        var top = engine.SearchSpansAsync(From, To, limit: 200).ToBlockingEnumerable().ToList();
        Assert.Equal(200, top.Count);
        Assert.InRange(visited, 200, 200 + 2 * SpanStartIndex.BlockSize);

        var deep = engine.SearchSpansAsync(From, Base.AddMilliseconds(5_000), limit: 200).ToBlockingEnumerable().ToList();
        Assert.Equal(200, deep.Count);
        Assert.Equal(Nano(Base) + 5_000_000_000L, deep[0].StartTimeUnixNano);
        Assert.InRange(visited, 200, 200 + 2 * SpanStartIndex.BlockSize);
    }

    /// <summary>
    /// STOPPING EARLY MUST NOT HIDE A MATCH IT TURNED AWAY. Two blocks, every span a match, and a page
    /// exactly one block deep: the newest block fills the heap to its last span without evicting
    /// anything, and the next block lies wholly below what was kept, so the walk stops there — before
    /// meeting the match the full walk would have turned away. "Turned away" is what makes the page
    /// CAPPED; without it the stream would call the window read out over a block it never read.
    /// </summary>
    [Fact]
    public void A_pass_that_stops_early_still_reports_the_match_it_turned_away()
    {
        using var engine = NewEngine();
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 2 * SpanStartIndex.BlockSize)]);
        int visited = -1;
        engine._hotSearchVisitedForTest = n => visited = n;

        var floor = new SpanScanFloor();
        var got   = engine.SearchSpansAsync(From, To, limit: SpanStartIndex.BlockSize, scanFloor: floor)
                          .ToBlockingEnumerable().ToList();
        var (kept, evicted) = ReferenceHotPass(engine.HotSpansForTest, static _ => true, SpanStartIndex.BlockSize);

        Assert.True(evicted);
        Assert.Equal(kept.Select(Describe), got.Select(Describe));
        Assert.Equal(kept[^1].StartTimeUnixNano, floor.FloorNano);
        Assert.Equal(SpanStartIndex.BlockSize + 1, visited);   // the block, and the one match below it that settles the floor
    }

    /// <summary>
    /// A SPAN ID REUSED BELOW THE CUT IS A COPY, NOT A MATCH TURNED AWAY — the dedupe's rule, which the
    /// early stop's search for "one more match" has to keep. The id is reused with an OLDER start (a
    /// malformed producer; a true re-send carries its original's start and can never lie below the
    /// cut). Which copy a page shows is where the bounded walk and the tier-order heap part: the heap
    /// showed whichever ARRIVED first, the walk shows the one it meets first — the copy in the block
    /// with the larger largest start, usually but not always the newer. Here it is the newer: the two
    /// blocks arrive in start order and no span runs ahead.
    /// </summary>
    [Fact]
    public void A_span_id_reused_below_the_cut_is_a_copy_not_a_match_turned_away()
    {
        using var engine = NewEngine();
        var newest = TraceAggregateLockProbe.Corpus(SpanStartIndex.BlockSize, SpanStartIndex.BlockSize);
        var older  = TraceAggregateLockProbe.Corpus(0, SpanStartIndex.BlockSize);
        for (int i = 0; i < newest.Length; i++) newest[i] = Status(newest[i], SpanStatusCode.Error);
        older[5] = new SpanIngestItem   // the reused id: the same trace and span as newest[17], older, and an error too
        {
            TraceId = newest[17].TraceId, SpanId = newest[17].SpanId, ParentSpanId = newest[17].ParentSpanId,
            StartTimeUnixNano = older[5].StartTimeUnixNano, DurationNanos = 1_000_000L, Name = "reused",
            ServiceName = "billing", Kind = SpanKind.Client, Status = SpanStatusCode.Error, AttributesBytes = [],
        };
        Write(engine, [.. older, .. newest]);

        var floor = new SpanScanFloor();
        var got   = engine.SearchSpansAsync(From, To, status: SpanStatusCode.Error, limit: SpanStartIndex.BlockSize,
                                            scanFloor: floor).ToBlockingEnumerable().ToList();

        Assert.Equal(newest.Select(s => s.StartTimeUnixNano).OrderByDescending(s => s), got.Select(s => s.StartTimeUnixNano));
        Assert.False(floor.Truncated, $"the reused id was counted as a match turned away (floor {floor.FloorNano})");
    }

    /// <summary>
    /// A TIE AT THE CUT IS DECIDED BY ARRIVAL. Three hundred spans with one start, a page of a
    /// hundred: the first hundred to arrive, in the order they arrived — every time, whatever the
    /// tier's block layout. The heap over the tier left both the choice and the order to its own
    /// internal arrangement.
    /// </summary>
    [Fact]
    public void A_tie_at_the_cut_is_decided_by_arrival()
    {
        using var engine = NewEngine();
        var items = TraceAggregateLockProbe.Corpus(0, 300);
        long start = items[150].StartTimeUnixNano;
        for (int i = 0; i < items.Length; i++) items[i] = At(items[i], start);
        Write(engine, [.. items]);

        var floor = new SpanScanFloor();
        var got   = engine.SearchSpansAsync(From, To, limit: 100, scanFloor: floor).ToBlockingEnumerable().ToList();

        Assert.Equal(items.Take(100).Select(s => s.SpanId), got.Select(s => s.SpanId));
        Assert.Equal(start, floor.FloorNano);
    }

    /// <summary>
    /// A SPAN FROM A CLOCK RUNNING AHEAD IS THE NEWEST, WHEREVER IT ARRIVED. It arrives first — in the
    /// oldest block by arrival — and starts an hour after everything else. The walk orders blocks by
    /// their newest start, so it reads that block first, keeps the span, and stops; a walk in arrival
    /// order from the end would have read the whole tier to find it.
    /// </summary>
    [Fact]
    public void A_span_from_a_clock_running_ahead_is_found_first_wherever_it_arrived()
    {
        using var engine = NewEngine();
        var items  = TraceAggregateLockProbe.Corpus(0, 5_000);
        var future = At(items[0], items[^1].StartTimeUnixNano + 3_600_000_000_000L);
        items[0]   = future;
        Write(engine, [.. items]);
        int visited = -1;
        engine._hotSearchVisitedForTest = n => visited = n;

        var got = engine.SearchSpansAsync(From, Base.AddHours(2), limit: 1).ToBlockingEnumerable().ToList();

        Assert.Equal(future.StartTimeUnixNano, Assert.Single(got).StartTimeUnixNano);
        Assert.InRange(visited, 1, 2 * SpanStartIndex.BlockSize);
    }

    /// <summary>
    /// A SELECTIVE TRACEQL PAGE PAYS FOR WHAT IT FINDS, NOT FOR ITS LIMIT (#122 review L2). The page
    /// asks for 2 000 spans; how many it finds is the query's business. The heap and its id set used
    /// to jump, at 256 entries, to the page's LIMIT — capped only by the spans in the window, not by
    /// its matches — so a page ending near 300 matches paid for 2 000: 185 KB against main's 76.
    ///
    /// <para>The shapes are the review's: a 20 000-span hot tier whose durations cycle through
    /// 1..2 000 ms, so a duration floor of (2 001 − k) ms matches k spans in every 2 000, spread
    /// evenly through the window. Each is the smallest of the last five of six pages, on this
    /// thread, over a page that completes synchronously (asserted). The bounds are main's
    /// allocation for the same page (Debug: 31.6, 75.6, 158.7, 297.7 and 622.3 KB) rounded up, but
    /// the all-match page's, which is this change's own 268 KB plus margin: that is the page every
    /// attribute query is in the hot pass, where nothing narrows the walk but the window.</para>
    ///
    /// <para>AND THE SHAPE THE SIZING GETS WRONG, printed and held to its one promise: a burst of
    /// matches at the top of the window (errors in the newest 300 or 600 spans) looks, 256 matches
    /// in, like a page about to fill its limit, and is sized for it — never for more than a full
    /// page. See <c>NextHeapCapacity</c> for why no rule decided at that point can tell the two
    /// apart, and what the alternative costs.</para>
    ///
    /// <para>AND THE LAST PAGE OF A SELECTIVE STREAM (#122 review, round 3), in order and down the
    /// probe's disordered tier, each held to main's figure for its match count rounded up like the
    /// spread shapes. Disordered, it used to be sized for the limit: see <c>NextHeapCapacity</c>.</para>
    /// </summary>
    [Fact]
    public void A_selective_TraceQL_page_pays_for_what_it_finds_not_for_its_limit()
    {
        using var engine = NewEngine("spread");
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 20_000)]);

        (string Shape, long? MinDuration, int Matches, long BoundBytes)[] shapes =
        [
            ("~100",   1_991_000_000L,    100,  40 * 1024),
            ("~300",   1_971_000_000L,    300,  90 * 1024),
            ("~600",   1_941_000_000L,    600, 160 * 1024),
            ("~1 000", 1_901_000_000L,  1_000, 300 * 1024),
            ("all",    null,            2_000, 300 * 1024),
        ];

        var failures = new List<string>();
        foreach (var (shape, minDuration, matches, bound) in shapes)
            Gate(engine, shape, minDuration, status: null, matches, bound, failures);

        // THE LAST PAGE OF A SELECTIVE STREAM (round 3): matches in runs of 230 and 260 in every
        // 2 000 spans, so page 0 fills and page 1 keeps the other 301 and 601. In order, and
        // DISORDERED — where page 1's window overlaps every block a late span reaches back from, most
        // of whose spans lie above its ceiling, and an estimate over all of them sized the heap for
        // the limit: 176 and 189 KB, against main's 76 and 159.
        Gate(engine, "last ~300", 1_771_000_000L, null, 301, 90 * 1024, failures, SecondPageCeiling(engine, 1_771_000_000L));
        using (var disordered = NewEngine("disordered"))
        {
            Write(disordered, [.. TraceStreamPageProbe.Disordered(TraceAggregateLockProbe.Corpus(0, 20_000))]);
            foreach (var (shape, minDuration, matches, bound) in new (string, long, int, long)[]
                     { ("~300", 1_771_000_000L, 301, 90 * 1024), ("~600", 1_741_000_000L, 601, 160 * 1024) })
                Gate(disordered, $"last {shape}, disordered", minDuration, null, matches, bound, failures,
                     SecondPageCeiling(disordered, minDuration));
        }

        foreach (int burst in (int[])[300, 600])
        {
            using var bursty = NewEngine($"burst-{burst}");
            var items = TraceAggregateLockProbe.Corpus(0, 20_000);
            for (int i = items.Length - burst; i < items.Length; i++) items[i] = Status(items[i], SpanStatusCode.Error);
            Write(bursty, [.. items]);
            Gate(bursty, $"burst {burst}", null, SpanStatusCode.Error, burst, 300 * 1024, failures);
        }

        Assert.True(failures.Count == 0,
            "a TraceQL page allocated more than it should for the matches it found — "
            + string.Join("; ", failures));
    }

    /// <summary>The smallest allocation of the last five of six identical pages, against <paramref name="bound"/>.</summary>
    private void Gate(TraceStorageEngine engine, string shape, long? minDuration, SpanStatusCode? status,
                      int matches, long bound, List<string> failures, DateTimeOffset? to = null)
    {
        long best = long.MaxValue;
        for (int round = 0; round < 6; round++)
        {
            long a0    = GC.GetAllocatedBytesForCurrentThread();
            var (n, sync, _) = SearchSynchronously(engine, minDuration, status, limit: 2_000, to: to);
            long bytes = GC.GetAllocatedBytesForCurrentThread() - a0;
            Assert.True(sync, $"{shape}: the hot-only page yielded, so this thread saw only part of it");
            Assert.Equal(matches, n);
            if (round > 0) best = Math.Min(best, bytes);   // the first round is the warm-up
        }
        _out.WriteLine($"{shape,-9} {matches,5:N0} matches  {best / 1024.0,7:N1} KB  (bound {bound / 1024:N0} KB)");
        if (best > bound) failures.Add($"{shape}: {best / 1024.0:N1} KB > {bound / 1024:N0} KB");
    }

    /// <summary>The ceiling of a stream's second page: its full first page's oldest row, rounded up to its millisecond.</summary>
    private static DateTimeOffset SecondPageCeiling(TraceStorageEngine engine, long minDuration)
    {
        var (n, _, oldest) = SearchSynchronously(engine, minDuration, null, limit: 2_000);
        Assert.Equal(2_000, n);
        Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(oldest, out var ceiling));
        return ceiling;
    }

    /// <summary>
    /// A FULL PAGE OF A DISORDERED TIER IS SIZED ONCE (#122 review L2). The tier is the probe's
    /// disordered one — 1 span in 100 from a clock 30 s ahead, 1 in 50 reported 10 s late — and a
    /// stream pages down it, 2 000 spans a page. The walk then reads whole blocks that are mostly
    /// outside a deep page's window, each kept in by one late span; counted against everything it
    /// read, the match rate came out low and the heap grew in steps, so a full page cost about half
    /// as much again as a full page of an ordered tier. Every page here must cost what the ordered
    /// tier's full page is held to.
    /// </summary>
    [Fact]
    public void A_full_page_of_a_disordered_tier_is_sized_once()
    {
        using var engine = NewEngine();
        Write(engine, [.. TraceStreamPageProbe.Disordered(TraceAggregateLockProbe.Corpus(0, 20_000))]);

        long cursor = Nano(To);
        var  pages  = new List<string>();
        for (int page = 0; page < 6; page++)
        {
            Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
            long best = long.MaxValue, oldest = long.MaxValue;
            for (int round = 0; round < 6; round++)
            {
                long a0 = GC.GetAllocatedBytesForCurrentThread();
                var (n, sync, low) = SearchSynchronously(engine, null, null, limit: 2_000, to: pageTo);
                long bytes = GC.GetAllocatedBytesForCurrentThread() - a0;
                Assert.True(sync, $"page {page}: the hot-only page yielded, so this thread saw only part of it");
                Assert.Equal(2_000, n);
                if (round > 0) best = Math.Min(best, bytes);
                oldest = low;
            }
            pages.Add($"{best / 1024.0:N1}");
            _out.WriteLine($"disordered page {page}: {best / 1024.0,7:N1} KB");
            Assert.True(best <= 300 * 1024,
                $"page {page} of a disordered tier allocated {best / 1024.0:N1} KB for a full page "
                + $"(pages so far: {string.Join(", ", pages)} KB) — the heap is growing in steps again");
            Assert.True(oldest < cursor);
            cursor = oldest;
        }
    }

    /// <summary>
    /// A HEAP NEVER GROWS PAST ITS PAGE'S LIMIT (#122 review, round 3). <c>PriorityQueue.EnsureCapacity</c>
    /// grows to at least twice the heap's length, so a last step cut down to the limit overshot it:
    /// from 1 900 nodes to a limit of 2 000 it allocates 3 800, on the large-object heap. The stream
    /// is one whose matches come in runs — 1 000 of every 2 000 spans, newest run first — so a deep
    /// page's top reads a whole run of non-matches, its estimate comes out low, and the heap needs a
    /// second step to the limit: page 4 ended at 2 604 nodes. Every page of the stream must stay at
    /// or under 2 000; the seam reports the heap's array, which no allocation total separates from
    /// the id set's.
    /// </summary>
    [Fact]
    public void A_heap_never_grows_past_its_pages_limit()
    {
        using var engine = NewEngine();
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 20_000)]);
        int capacity = -1;
        engine._hotHeapCapacityForTest = c => capacity = c;

        long minDuration = 1_001_000_000L;   // i % 2 000 >= 1 000: runs of 1 000 matches
        long cursor      = Nano(To);
        var  pages       = new List<string>();
        for (int page = 0; page < 5; page++)
        {
            Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
            var (n, sync, oldest) = SearchSynchronously(engine, minDuration, null, limit: 2_000, to: pageTo);
            Assert.True(sync);
            Assert.Equal(2_000, n);
            pages.Add($"{capacity:N0}");
            _out.WriteLine($"page {page}: heap {capacity:N0} nodes");
            Assert.True(capacity <= 2_000,
                $"page {page}'s heap grew to {capacity:N0} nodes for a limit of 2 000 (pages so far: "
                + $"{string.Join(", ", pages)}) — a last step was doubled past the limit");
            cursor = oldest;
        }
    }

    /// <summary>
    /// A FULL PAGE WHOSE ESTIMATE LANDS JUST SHORT OF ITS LIMIT IS STILL SIZED ONCE (#122 review,
    /// round 3). Matches come in runs of 400 in every 2 000 spans; page 1's window holds 2 001 of
    /// them, and 256 matches in, its estimate is 1 998 — two short of the limit. A heap sized to it
    /// fills, and the last step to 2 000 pays a second array of the limit for two slots: 306 KB for
    /// the page against page 0's 259. Within a sixteenth of the limit, the limit — so page 1 costs
    /// what page 0 does.
    /// </summary>
    [Fact]
    public void A_full_page_whose_estimate_lands_just_short_of_its_limit_is_sized_once()
    {
        using var engine = NewEngine();
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 20_000)]);

        long minDuration = 1_601_000_000L;   // i % 2 000 >= 1 600: runs of 400 matches
        long cursor      = Nano(To);
        var  cost        = new long[2];
        for (int page = 0; page < 2; page++)
        {
            Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
            long best = long.MaxValue, oldest = long.MaxValue;
            for (int round = 0; round < 6; round++)
            {
                long a0 = GC.GetAllocatedBytesForCurrentThread();
                var (n, sync, low) = SearchSynchronously(engine, minDuration, null, limit: 2_000, to: pageTo);
                long bytes = GC.GetAllocatedBytesForCurrentThread() - a0;
                Assert.True(sync, $"page {page}: the hot-only page yielded, so this thread saw only part of it");
                Assert.Equal(2_000, n);
                if (round > 0) best = Math.Min(best, bytes);
                oldest = low;
            }
            cost[page] = best;
            _out.WriteLine($"page {page}: {best / 1024.0,7:N1} KB");
            cursor = oldest;
        }
        Assert.True(cost[1] <= cost[0] + 4 * 1024,
            $"page 1 allocated {cost[1] / 1024.0:N1} KB against page 0's {cost[0] / 1024.0:N1} KB for the same "
            + "2 000 spans — its heap was sized just short of the limit and grew a second time");
    }

    /// <summary>
    /// SearchSpansAsync driven by hand: how many spans came back, the oldest start among them, and
    /// whether every step completed synchronously.
    /// </summary>
    private static (int Spans, bool Sync, long Oldest) SearchSynchronously(
        TraceStorageEngine engine, long? minDuration, SpanStatusCode? status, int limit, DateTimeOffset? to = null)
    {
        int  n      = 0;
        long oldest = long.MaxValue;
        bool sync   = true;
        var  e      = engine.SearchSpansAsync(From, to ?? To, status: status, minDurationNanos: minDuration, limit: limit)
                            .GetAsyncEnumerator();
        while (true)
        {
            var step = e.MoveNextAsync();
            sync &= step.IsCompleted;
            if (!(step.IsCompleted ? step.Result : step.AsTask().GetAwaiter().GetResult())) break;
            oldest = Math.Min(oldest, e.Current.StartTimeUnixNano);
            n++;
        }
        var done = e.DisposeAsync();
        sync &= done.IsCompleted;
        if (!done.IsCompleted) done.AsTask().GetAwaiter().GetResult();
        return (n, sync, oldest);
    }

    /// <summary>
    /// A LIST PAGE DEEP IN ITS WINDOW READS ONLY THE BLOCKS THAT REACH IT. 20 000 spans, one a
    /// millisecond: a page whose ceiling is five seconds in reads the five thousand spans below it and
    /// the one block that straddles it — the blocks above are never opened. (It may not stop any
    /// earlier: see MergeRunInto.)
    /// </summary>
    [Fact]
    public async Task A_list_page_deep_in_its_window_reads_only_the_blocks_that_reach_it()
    {
        using var engine = NewEngine();
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 20_000)]);
        int visited = -1;
        engine._listHotVisitedForTest = n => visited = n;

        var all = await engine.GetTraceListAsync(From, To, null, null, null, null, null, 500);
        Assert.Equal(20_000, visited);
        Assert.Equal(500, all.Rows.Count);

        var deep = await engine.GetTraceListAsync(From, Base.AddMilliseconds(5_000), null, null, null, null, null, 500);
        Assert.InRange(visited, 5_001, 5_001 + SpanStartIndex.BlockSize);
        Assert.Equal(500, deep.Rows.Count);
    }

    /// <summary>
    /// A LIST PAGE MAKES ROWS ONLY FOR WHAT IT RETURNS. 2 000 traces in three services, a page of 100:
    /// every trace is merged — the filters and the cold walk's cap need all of them — but the row (a
    /// TraceSummary, its services array) and the root's HTTP method and path (a walk of its attribute
    /// blob and a string) are made for the 100 the page keeps, and a merged trace no longer carries a
    /// HashSet for one to three services.
    ///
    /// <para>Counted on this thread over a hot-only page, which completes synchronously (asserted).
    /// Measured (Debug, the smallest of three pages): 639 B per merged trace before #94, 253 after;
    /// making rows for every trace again reads 509, and giving every trace a HashSet again 429.</para>
    /// </summary>
    [Fact]
    public async Task A_list_page_makes_rows_only_for_the_traces_it_returns()
    {
        using var engine = NewEngine();
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 20_000, services: 3)]);
        const int Traces = 2_000;

        await engine.GetTraceListAsync(From, To, null, null, null, null, null, 100);   // warm

        long best = long.MaxValue;
        for (int r = 0; r < 3; r++)
        {
            long a0   = GC.GetAllocatedBytesForCurrentThread();
            var  task = engine.GetTraceListAsync(From, To, null, null, null, null, null, 100);
            bool sync = task.IsCompleted;
            var  page = await task;
            best = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - a0);
            Assert.True(sync, "a hot-only list page yielded, so this thread saw only part of it");
            Assert.Equal(100, page.Rows.Count);
            Assert.Equal("/api/v1/tenants/{tenantId}/payments", page.Rows[0].HttpPath);   // resolved for the rows kept
        }

        _out.WriteLine($"list page of 100 over {Traces:N0} hot traces: {best:N0} B, {best / Traces:N0} B per merged trace");
        Assert.True(best / Traces < 330,
            $"a list page allocated {best / Traces:N0} B per merged trace — rows or service sets are being "
            + "made for traces the page does not return");
    }

    /// <summary>
    /// A STREAM OF LIST PAGES LEAVES NO DECODED ATTRIBUTE MAP ON THE TIER. The root's HTTP method and
    /// path are read straight out of its blob; reaching them through <c>SpanRecord.Attributes</c>
    /// instead would decode the map and MEMOISE it on a record the tier keeps until it flushes.
    /// <c>TraceHotTierProbe</c>'s per-page allocation gate caught that while every root of the tier
    /// was asked on every page; now that only the rows a page returns are asked, a decode put back
    /// there would cost one page a twentieth of what it did — and still inflate the whole tier, one
    /// page at a time, over a stream. So the guard is the memo flag itself, read off every record
    /// after a stream has returned every trace in the tier.
    /// </summary>
    [Fact]
    public async Task A_stream_of_list_pages_leaves_no_decoded_attributes_on_the_tier()
    {
        // Private by design; found by name and asserted found, so a rename fails here, loudly.
        var decodedFlag = typeof(SpanRecord).GetField("_decoded",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(decodedFlag);

        using var engine = NewEngine();
        Write(engine, [.. TraceAggregateLockProbe.Corpus(0, 5_000, services: 3)]);

        long fromNano = Nano(From), cursor = Nano(To);
        var  traces   = new HashSet<TraceId>();   // the stream's dedupe: the ceiling millisecond overlaps
        int  withPath = 0;
        for (int page = 0; page < 100; page++)
        {
            Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
            var p = await engine.GetTraceListAsync(From, pageTo, null, null, null, null, null, 100);
            foreach (var r in p.Rows)
                if (traces.Add(r.TraceId) && r.HttpPath.Length > 0) withPath++;
            if (!NextCursor(p, cursor, fromNano, out cursor)) break;
        }

        Assert.Equal(500, traces.Count);   // every trace of the tier came back
        Assert.Equal(500, withPath);       // and each row read its path out of its root's blob
        int decoded = engine.HotSpansForTest.Count(s => !s.AttributesBytes.IsEmpty && (bool)decodedFlag!.GetValue(s)!);
        Assert.Equal(0, decoded);
    }

    private static SpanIngestItem At(SpanIngestItem s, long start) => new()
    {
        TraceId = s.TraceId, SpanId = s.SpanId, ParentSpanId = s.ParentSpanId, StartTimeUnixNano = start,
        DurationNanos = s.DurationNanos, Name = s.Name, ServiceName = s.ServiceName, Kind = s.Kind,
        Status = s.Status, HttpStatusCode = s.HttpStatusCode, AttributesBytes = s.AttributesBytes,
    };

    private static SpanIngestItem Status(SpanIngestItem s, SpanStatusCode status) => new()
    {
        TraceId = s.TraceId, SpanId = s.SpanId, ParentSpanId = s.ParentSpanId, StartTimeUnixNano = s.StartTimeUnixNano,
        DurationNanos = s.DurationNanos, Name = s.Name, ServiceName = s.ServiceName, Kind = s.Kind,
        Status = status, HttpStatusCode = s.HttpStatusCode, AttributesBytes = s.AttributesBytes,
    };

    // ── The trace list against its model ───────────────────────────────────────

    private async Task AssertListPagesAsync(TraceStorageEngine engine, List<SpanRecord> flushing, string state)
    {
        var hot  = engine.HotSpansForTest;
        var cold = engine.ColdSegmentsForTest;
        long fromNano = Nano(From);
        int  pages = 0, rows = 0;

        foreach (var f in ListFilters)
        {
            long cursor = Nano(To);
            for (int page = 0; page < 60; page++)
            {
                Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
                var actual   = await List(engine, f, pageTo);
                var expected = ListModel(hot, flushing, cold, f, pageTo);
                AssertSamePage(expected, actual, $"{state}, {f}, page {page}");
                pages++;
                rows += actual.Rows.Count;

                if (!NextCursor(actual, cursor, fromNano, out cursor)) break;
            }
        }

        // Not vacuous: the small limits page the window many times over, and the pages carry rows.
        Assert.True(pages >= 40 && rows >= 1_000, $"{state}: only {pages} pages and {rows} rows were compared");

        // And every run those pages read was read through its own start index — the pairing the
        // engine maintains by hand at every swap held in every state these tests build.
        Assert.Equal(0, engine.UnindexedCapturesForTest);
    }

    private static Task<TraceListPage> List(TraceStorageEngine engine, ListFilter f, DateTimeOffset pageTo) =>
        engine.GetTraceListAsync(From, pageTo, f.Service, f.Name, f.Status, f.MinDur, f.MaxDur, f.Limit);

    /// <summary>
    /// The stream's cursor rule, minus its bookkeeping: the oldest row returned, or the floor when no
    /// row can carry it. False when the stream would end here.
    /// </summary>
    private static bool NextCursor(TraceListPage page, long cursor, long fromNano, out long next)
    {
        next = cursor;
        if (!page.Capped) return false;
        long oldest = long.MaxValue;
        foreach (var r in page.Rows) oldest = Math.Min(oldest, r.RootStartNano);
        long candidate = page.Rows.Count > 0 && oldest < cursor ? oldest : page.ScanFloorNano;
        if (candidate >= cursor || candidate <= fromNano) return false;
        next = candidate;
        return true;
    }

    private static void AssertSamePage(TraceListPage expected, TraceListPage actual, string where)
    {
        Assert.False(actual.Unreadable, $"{where}: a healthy tier reported an unreadable region");
        Assert.True(expected.Rows.Count == actual.Rows.Count,
            $"{where}: {actual.Rows.Count} rows, the model has {expected.Rows.Count}");
        for (int i = 0; i < expected.Rows.Count; i++)
        {
            var e = expected.Rows[i];
            var a = actual.Rows[i];
            string at = $"{where}, row {i} (trace {e.TraceId})";
            Assert.True(e.TraceId.Equals(a.TraceId), $"{at}: trace {a.TraceId} where the model has {e.TraceId}");
            Assert.True(e.RootSpanId.Equals(a.RootSpanId),    $"{at}: RootSpanId");
            Assert.True(e.RootStartNano == a.RootStartNano,   $"{at}: RootStartNano {a.RootStartNano} vs {e.RootStartNano}");
            Assert.True(e.DurationNanos == a.DurationNanos,   $"{at}: DurationNanos");
            Assert.True(e.SpanCount == a.SpanCount,           $"{at}: SpanCount {a.SpanCount} vs {e.SpanCount}");
            Assert.True(e.HasRoot == a.HasRoot,               $"{at}: HasRoot");
            Assert.True(e.HasError == a.HasError,             $"{at}: HasError");
            Assert.True(e.RootStatus == a.RootStatus,         $"{at}: RootStatus");
            Assert.True(e.HttpStatusCode == a.HttpStatusCode, $"{at}: HttpStatusCode");
            Assert.True(e.Name == a.Name,                     $"{at}: Name '{a.Name}' vs '{e.Name}'");
            Assert.True(e.ServiceName == a.ServiceName,       $"{at}: ServiceName '{a.ServiceName}' vs '{e.ServiceName}'");
            Assert.True(e.HttpMethod == a.HttpMethod,         $"{at}: HttpMethod '{a.HttpMethod}' vs '{e.HttpMethod}'");
            Assert.True(e.HttpPath == a.HttpPath,             $"{at}: HttpPath '{a.HttpPath}' vs '{e.HttpPath}'");
            Assert.True(e.Services.SequenceEqual(a.Services),
                $"{at}: services [{string.Join(",", a.Services)}] vs [{string.Join(",", e.Services)}]");
        }
        Assert.True(expected.Capped == actual.Capped, $"{where}: Capped {actual.Capped} vs {expected.Capped}");
        Assert.True(expected.ScanFloorNano == actual.ScanFloorNano,
            $"{where}: floor {actual.ScanFloorNano} vs {expected.ScanFloorNano}");
    }

    /// <summary>One trace being merged, by the rules <c>GetTraceListAsync</c> documents.</summary>
    private sealed class ModelTrace
    {
        public TraceId        TraceId;
        public uint           SpanCount;
        public bool           HasError;
        public long           EarliestNano = long.MaxValue;
        public string         EarliestService = string.Empty;
        public bool           HasRoot;
        public SpanId         RootSpanId;
        public long           RootStartNano;
        public long           DurationNanos;
        public SpanStatusCode RootStatus;
        public short          HttpStatusCode;
        public string         Name        = string.Empty;
        public string         ServiceName = string.Empty;
        public string         HttpMethod  = string.Empty;
        public string         HttpPath    = string.Empty;
        public readonly List<string> Services = [];   // distinct by ordinal, in the order first met

        public long Key => HasRoot ? RootStartNano : EarliestNano;

        public void AddService(string s)
        {
            foreach (var x in Services) if (string.Equals(x, s, StringComparison.Ordinal)) return;
            Services.Add(s);
        }

        public void Merge(SpanRecord s)
        {
            SpanCount++;
            if (s.Status == SpanStatusCode.Error) HasError = true;
            AddService(s.ServiceName);
            if (s.StartTimeUnixNano < EarliestNano) { EarliestNano = s.StartTimeUnixNano; EarliestService = s.ServiceName; }
            if (s.ParentSpanId.IsEmpty && !HasRoot)
            {
                HasRoot        = true;
                RootSpanId     = s.SpanId;
                RootStartNano  = s.StartTimeUnixNano;
                DurationNanos  = s.DurationNanos;
                RootStatus     = s.Status;
                HttpStatusCode = s.HttpStatusCode;
                Name           = s.Name;
                ServiceName    = s.ServiceName;
                HttpSemconvKeys.Resolve(s, out HttpMethod, out HttpPath);
            }
        }

        public void Merge(TraceSummary r)
        {
            SpanCount += r.SpanCount;
            if (r.HasError) HasError = true;
            foreach (var sv in r.Services) AddService(sv);
            if (r.RootStartNano < EarliestNano) { EarliestNano = r.RootStartNano; EarliestService = r.ServiceName; }
            if (r.HasRoot && !HasRoot)
            {
                HasRoot        = true;
                RootSpanId     = r.RootSpanId;
                RootStartNano  = r.RootStartNano;
                DurationNanos  = r.DurationNanos;
                RootStatus     = r.RootStatus;
                HttpStatusCode = r.HttpStatusCode;
                Name           = r.Name;
                ServiceName    = r.ServiceName;
                HttpMethod     = r.HttpMethod;
                HttpPath       = r.HttpPath;
            }
        }

        public bool Passes(ListFilter f)
        {
            var status = HasError ? SpanStatusCode.Error : RootStatus;
            if (f.Status is not null && status != f.Status.Value) return false;
            if (f.Service is not null
                && !ServiceName.Equals(f.Service, StringComparison.OrdinalIgnoreCase)
                && !Services.Exists(x => x.Equals(f.Service, StringComparison.OrdinalIgnoreCase))) return false;
            if (f.Name is not null && !Name.Contains(f.Name, StringComparison.OrdinalIgnoreCase)) return false;
            if (f.MinDur is not null && DurationNanos < f.MinDur.Value) return false;
            if (f.MaxDur is not null && DurationNanos > f.MaxDur.Value) return false;
            return true;
        }

        public TraceSummary ToSummary() => new()
        {
            TraceId        = TraceId,
            RootSpanId     = RootSpanId,
            RootStartNano  = Key,
            DurationNanos  = DurationNanos,
            SpanCount      = SpanCount,
            HasRoot        = HasRoot,
            HasError       = HasError,
            RootStatus     = RootStatus,
            HttpStatusCode = HttpStatusCode,
            Name           = Name,
            ServiceName    = HasRoot ? ServiceName : EarliestService,
            HttpMethod     = HttpMethod,
            HttpPath       = HttpPath,
            Services       = [.. Services],
        };
    }

    /// <summary>
    /// THE TRACE LIST, RESTATED. The unflushed spans in [from, to] merged in tier order — the
    /// detached snapshot first, then the live tier — then the cold segments newest-first, each one
    /// read whole unless the merge already holds <c>max(limit*5, 500)</c> traces and another segment
    /// has been read; the filters on the merged traces; newest-first; the <c>limit</c> cut. The floor
    /// is the highest of what stopped short: the cap's next segment and the cut's last kept row.
    /// </summary>
    private static TraceListPage ListModel(
        List<SpanRecord> hot, List<SpanRecord> flushing, SpanSegmentInfo[] cold, ListFilter f, DateTimeOffset to)
    {
        long fromNano = Nano(From);
        long toNano   = Nano(to);
        int  scanCap  = Math.Max(f.Limit * 5, 500);

        var order  = new List<ModelTrace>();
        var byId   = new Dictionary<TraceId, ModelTrace>();
        ModelTrace For(TraceId id)
        {
            if (!byId.TryGetValue(id, out var m)) { m = new ModelTrace { TraceId = id }; byId[id] = m; order.Add(m); }
            return m;
        }

        foreach (var s in flushing) if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano) For(s.TraceId).Merge(s);
        foreach (var s in hot)      if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano) For(s.TraceId).Merge(s);

        long floor      = long.MinValue;
        bool visitedAny = false;
        foreach (var seg in cold)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            if (f.Service is not null && seg.Services.Length > 0 &&
                !Array.Exists(seg.Services, x => x.Equals(f.Service, StringComparison.OrdinalIgnoreCase))) continue;
            if (visitedAny && byId.Count >= scanCap) { floor = Math.Max(floor, Math.Min(seg.MaxStartNano, toNano)); break; }
            visitedAny = true;
            Assert.True(TraceSummarySidecar.TryReadSummaries(seg.FilePath, fromNano, toNano, out var rows), "a sidecar would not read");
            foreach (var r in rows) For(r.TraceId).Merge(r);
        }

        var kept = order.Where(m => m.Passes(f)).OrderByDescending(m => m.Key).ToList();
        if (kept.Count > f.Limit)
        {
            floor = Math.Max(floor, kept[Math.Max(0, f.Limit - 1)].Key);
            kept.RemoveRange(f.Limit, kept.Count - f.Limit);
        }
        return new TraceListPage([.. kept.Select(m => m.ToSummary())], floor != long.MinValue, floor);
    }

    // ── The TraceQL fetch against the heap it used to run ───────────────────────

    /// <summary>
    /// Pages <c>SearchSpansAsync</c> down the window under every <see cref="QlFilters"/> entry and
    /// compares each page with <see cref="ReferenceHotPass"/> over the same unflushed spans. Only for
    /// states with no cold segment, where the hot pass is the whole answer.
    /// </summary>
    private static void AssertQlPages(TraceStorageEngine engine, List<SpanRecord> flushing, string state)
    {
        Assert.Equal(0, engine.ColdSegmentCountForTest);
        var unflushed = new List<SpanRecord>(flushing);
        unflushed.AddRange(engine.HotSpansForTest);
        long fromNano = Nano(From);
        int  pages = 0, evictions = 0;

        foreach (var f in QlFilters)
        {
            long cursor = Nano(To);
            for (int page = 0; page < 40; page++)
            {
                Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
                long toNano = Nano(pageTo);

                var floor  = new SpanScanFloor();
                var actual = engine.SearchSpansAsync(From, pageTo, f.Service, f.Name, f.Status, f.MinDur, f.MaxDur,
                                                     f.Http, f.Limit, null, floor).ToBlockingEnumerable().ToList();
                var (kept, evicted) = ReferenceHotPass(unflushed, s => QlMatch(s, f, fromNano, toNano), f.Limit);

                string where = $"{state}, {f}, page {page}";
                Assert.True(kept.Count == actual.Count, $"{where}: {actual.Count} spans, the reference has {kept.Count}");
                for (int i = 0; i < kept.Count; i++)
                    Assert.True(SameSpan(kept[i], actual[i]),
                        $"{where}, span {i}: {Describe(actual[i])} where the reference has {Describe(kept[i])}");
                long expectedFloor = evicted && kept.Count > 0 ? kept[^1].StartTimeUnixNano : long.MinValue;
                Assert.True(expectedFloor == floor.FloorNano, $"{where}: floor {floor.FloorNano} vs {expectedFloor}");
                Assert.False(floor.Unreadable, $"{where}: a healthy tier reported an unreadable region");
                pages++;
                if (evicted) evictions++;

                if (actual.Count == 0 || !floor.Truncated) break;
                long oldest = actual.Min(s => s.StartTimeUnixNano);
                if (oldest >= cursor || oldest <= fromNano) break;
                cursor = oldest;
            }
        }

        // Not vacuous: pages that turned matches away (the floor's whole subject) and pages that did not.
        Assert.True(pages >= 30 && evictions >= 10 && evictions < pages,
            $"{state}: {pages} pages compared, {evictions} of them evicting");
        Assert.Equal(0, engine.UnindexedCapturesForTest);
    }

    /// <summary>
    /// THE CORPUS IS AS HOSTILE AS THE TYPE SUMMARY SAYS. Every shape the oracles are meant to be
    /// exercised by is counted in every seed the states use, so a change to the generator cannot
    /// quietly turn these tests into a check of in-order, well-formed traces.
    /// </summary>
    [Theory]
    [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)] [InlineData(5)]
    [InlineData(6)] [InlineData(7)] [InlineData(8)] [InlineData(9)]
    public void The_corpus_has_every_hostile_shape(int seed)
    {
        var corpus = Corpus(seed, traces: 420);

        int late = 0, dupes = 0, noId = 0, future = 0;
        long highest = long.MinValue;
        var  seen    = new HashSet<(TraceId, ulong)>();
        foreach (var s in corpus)
        {
            if (s.StartTimeUnixNano < highest - 100_000_000L) late++;            // 100 ms behind what already arrived
            highest = Math.Max(highest, s.StartTimeUnixNano);
            if (s.SpanId.IsEmpty) noId++;
            else if (!seen.Add((s.TraceId, s.SpanId.RawValue))) dupes++;
            if (s.StartTimeUnixNano > Nano(Base) + 420 * 2_000_000L + 25_000_000_000L) future++;
        }

        var byTrace = corpus.GroupBy(s => s.TraceId)
                            .Select(g => g.DistinctBy(s => (s.SpanId.RawValue, s.StartTimeUnixNano)).ToList()).ToList();
        int orphans   = byTrace.Count(g => !g.Any(s => s.ParentSpanId.IsEmpty));
        int twoRoots  = byTrace.Count(g => g.Count(s => s.ParentSpanId.IsEmpty) > 1);
        int rootLater = byTrace.Count(g => g.FirstOrDefault(s => s.ParentSpanId.IsEmpty) is { } r
                                            && g.Any(s => s.StartTimeUnixNano < r.StartTimeUnixNano));
        int straddle  = byTrace.Count(g => g.Max(s => s.StartTimeUnixNano) - g.Min(s => s.StartTimeUnixNano) > 1_000_000_000L);
        int billings  = corpus.Select(s => s.ServiceName).Distinct(StringComparer.Ordinal)
                              .Count(n => n.Equals("billing", StringComparison.OrdinalIgnoreCase));

        Assert.True(late > 50 && dupes > 5 && noId > 5 && future > 0 && orphans > 5 && twoRoots > 5
                    && rootLater > 5 && straddle > 5 && billings == 2,
            $"seed {seed}: late {late}, dupes {dupes}, no-id {noId}, future {future}, orphans {orphans}, "
            + $"two roots {twoRoots}, root after a child {rootLater}, straddling {straddle}, billing spellings {billings}");
        // TWO DIFFERENT SPANS NEVER SHARE A START, so every oracle's order is total. (A re-sent span
        // shares its original's start, and a re-sent span with no id is two records the engine must
        // not fold together — but either way the two are the same span.)
        Assert.All(corpus.GroupBy(s => s.StartTimeUnixNano),
                   g => Assert.Single(g.Select(s => (s.TraceId, s.SpanId.RawValue)).Distinct()));
    }

    private static bool QlMatch(SpanRecord s, QlFilter f, long fromNano, long toNano) =>
        s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano &&
        (f.Service is null || s.ServiceName.Equals(f.Service, StringComparison.OrdinalIgnoreCase)) &&
        (f.Name    is null || s.Name.Contains(f.Name, StringComparison.OrdinalIgnoreCase)) &&
        (f.Status  is null || s.Status == f.Status.Value) &&
        (f.Http    is null || s.HttpStatusCode == f.Http.Value) &&
        (f.MinDur  is null || s.DurationNanos >= f.MinDur.Value) &&
        (f.MaxDur  is null || s.DurationNanos <= f.MaxDur.Value);

    /// <summary>
    /// THE HOT PASS AS <c>SearchSpansAsync</c> RAN IT before the tier was indexed, verbatim: every
    /// unflushed span in tier order offered to a min-heap of <paramref name="limit"/> on start time,
    /// one copy per (trace, span) id in the heap at a time, the oldest evicted once it is full.
    /// Returns what the heap kept, newest first, and whether a match was ever turned away.
    /// </summary>
    internal static (List<SpanRecord> Kept, bool Evicted) ReferenceHotPass(
        IEnumerable<SpanRecord> unflushedInTierOrder, Func<SpanRecord, bool> match, int limit)
    {
        var  top     = new PriorityQueue<SpanRecord, long>();
        var  present = new HashSet<(TraceId Trace, ulong Span)>();
        bool evicted = false;
        foreach (var r in unflushedInTierOrder)
        {
            if (!match(r)) continue;
            var  id         = (r.TraceId, r.SpanId.RawValue);
            bool identified = !r.SpanId.IsEmpty;
            if (identified && !present.Add(id)) continue;
            if (top.Count < limit) { top.Enqueue(r, r.StartTimeUnixNano); continue; }
            if (!top.TryPeek(out _, out long oldestKept) || r.StartTimeUnixNano <= oldestKept)
            {
                if (identified) present.Remove(id);
                evicted = true;
                continue;
            }
            var out_ = top.EnqueueDequeue(r, r.StartTimeUnixNano);
            if (!out_.SpanId.IsEmpty) present.Remove((out_.TraceId, out_.SpanId.RawValue));
            evicted = true;
        }
        var kept = new List<SpanRecord>(top.Count);
        while (top.TryDequeue(out var k, out _)) kept.Add(k);
        kept.Reverse();
        return (kept, evicted);
    }

    /// <summary>The same span: a duplicate is an exact copy, so which copy came back is not the question.</summary>
    private static bool SameSpan(SpanRecord a, SpanRecord b) =>
        a.TraceId.Equals(b.TraceId) && a.SpanId.Equals(b.SpanId) && a.StartTimeUnixNano == b.StartTimeUnixNano;

    private static string Describe(SpanRecord s) => $"{s.TraceId}/{s.SpanId}@{s.StartTimeUnixNano}";

    // ── The corpus ──────────────────────────────────────────────────────────────

    private static long Nano(DateTimeOffset t) => t.ToUnixTimeMilliseconds() * 1_000_000L;

    private static void Write(TraceStorageEngine engine, List<SpanIngestItem> items)
    {
        // In drainer-sized calls, so the write path takes them hold by hold as it does in production.
        for (int i = 0; i < items.Count; i += 512)
        {
            var call = items.GetRange(i, Math.Min(512, items.Count - i)).ToArray();
            Assert.Equal(call.Length, engine.WriteSpans(call));
        }
    }

    private static readonly string[] Services = ["gateway", "billing", "ledger", "auth", "search", "BILLING"];
    private static readonly string[] Roots    = ["GET /api/orders", "POST /api/pay", "GET /health"];
    private static readonly string[] Children = ["SELECT payments", "redis.GET", "HTTP POST", "Kafka.Produce"];

    /// <summary>
    /// <paramref name="traces"/> traces of 1-12 spans, in ARRIVAL order, seeded. See the type's
    /// summary for the shapes; every span's start time is unique.
    /// </summary>
    private static List<SpanIngestItem> Corpus(int seed, int traces, int firstTrace = 0)
    {
        var rnd    = new Random(seed);
        var starts = new HashSet<long>();
        long baseNano = Nano(Base);
        ulong spanSerial = (ulong)seed << 40;

        long UniqueStart(long wanted)
        {
            while (!starts.Add(wanted)) wanted++;
            return wanted;
        }

        var arrival = new List<SpanIngestItem>();
        for (int t = firstTrace; t < firstTrace + traces; t++)
        {
            var  traceId   = new TraceId(0xC0FFEE00UL + (ulong)seed, (ulong)t + 1);
            long traceBase = baseNano + (long)t * 2_000_000L + rnd.Next(0, 1_500_000);
            if (rnd.Next(100) == 0) traceBase += 30_000_000_000L;                 // a clock thirty seconds ahead
            bool longTrace = rnd.Next(20) == 0;                                    // straddles many page ceilings

            int  spans     = 1 + rnd.Next(12);
            int  shape     = rnd.Next(20);                                         // 0 orphan, 1 two roots, 2 root after a child
            var  rootId    = new SpanId(++spanSerial);
            var  group     = new List<SpanIngestItem>(spans + 1);
            for (int j = 0; j < spans; j++)
            {
                bool root  = j == 0 && shape != 0 || j == 1 && shape == 1;
                long start = j == 0 ? traceBase
                           : traceBase + (longTrace ? rnd.NextInt64(0, 20_000_000_000L) : rnd.Next(0, 30_000_000));
                if (j == 0 && shape == 2) start += 40_000_000;                     // the root starts after its children
                string svc = Services[rnd.Next(Services.Length)];
                bool   noId = rnd.Next(60) == 0;                                   // a producer that omits the span id
                group.Add(new SpanIngestItem
                {
                    TraceId           = traceId,
                    SpanId            = j == 0 ? rootId : noId ? default : new SpanId(++spanSerial),
                    ParentSpanId      = root ? default : rootId,
                    StartTimeUnixNano = UniqueStart(start),
                    DurationNanos     = 1_000L * rnd.Next(1, 1_500_000),
                    Name              = root ? Roots[rnd.Next(Roots.Length)] : Children[rnd.Next(Children.Length)],
                    ServiceName       = svc,
                    Kind              = root ? SpanKind.Server : SpanKind.Client,
                    Status            = rnd.Next(10) == 0 ? SpanStatusCode.Error : rnd.Next(2) == 0 ? SpanStatusCode.Ok : SpanStatusCode.Unset,
                    HttpStatusCode    = root ? (short)(rnd.Next(4) switch { 0 => 500, 1 => 404, _ => 200 }) : (short)0,
                    AttributesBytes   = root && rnd.Next(4) != 0 ? HttpBlob(rnd, t) : rnd.Next(3) == 0 ? [] : TraceHotTierProbe.SqlClientBlob(t),
                });
            }
            // Within a trace, spans arrive in no particular order — children usually end, and are
            // exported, before their parent.
            for (int j = group.Count - 1; j > 0; j--)
            {
                int k = rnd.Next(j + 1);
                (group[j], group[k]) = (group[k], group[j]);
            }
            arrival.AddRange(group);
        }

        // LATE ARRIVALS: one span in twenty is exported hundreds to thousands of positions after its
        // neighbours — a long span reported when it ends, or a slow exporter.
        int late = arrival.Count / 20;
        for (int n = 0; n < late; n++)
        {
            int from = rnd.Next(arrival.Count);
            var item = arrival[from];
            arrival.RemoveAt(from);
            arrival.Insert(Math.Min(arrival.Count, from + rnd.Next(100, 3_000)), item);
        }

        // DUPLICATES: one span in a hundred sent again, later — an exporter's retry, a WAL replay.
        int dups = arrival.Count / 100;
        for (int n = 0; n < dups; n++)
        {
            int from = rnd.Next(arrival.Count);
            arrival.Insert(Math.Min(arrival.Count, from + rnd.Next(1, 2_000)), arrival[from]);
        }
        return arrival;
    }

    private static byte[] HttpBlob(Random rnd, int t)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("http.request.method"); w.Write(rnd.Next(3) == 0 ? "POST" : "GET");
        w.Write("url.path");            w.Write("/api/v1/orders/" + t % 97);
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }
}
