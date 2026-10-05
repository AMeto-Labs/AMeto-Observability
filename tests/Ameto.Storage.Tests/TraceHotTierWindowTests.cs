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

    public TraceHotTierWindowTests() => Directory.CreateDirectory(_dir);

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
