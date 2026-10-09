using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT ONE PAGE OF A TRACE STREAM COSTS OVER THE HOT TIER — the two fetchers behind the SSE
/// routes, driven down the window the way <c>StreamTracePagesAsync</c> drives them: each page asks
/// for <c>[from, ceil_ms(cursor)]</c> and the cursor moves to the oldest row it returned (issue #94).
///
/// <para>The list path is <c>GetTraceListAsync</c> at the filter stream's 500-row page; the TraceQL
/// path is <c>TraceQLExecutor.ExecuteAsync</c> at the TraceQL stream's 200-row page, which asks the
/// engine for ten spans a row. Both over a hot tier only — nothing flushed — so every page is the
/// hot-tier pass and nothing else, and it completes synchronously: the per-thread allocation
/// counter sees the whole page (asserted, so a page that starts yielding cannot quietly halve the
/// figure).</para>
///
/// <para>PRINTED, NOT ASSERTED. Per page: the spans in the page's window, the unflushed spans the
/// engine read for it (its start index lets it skip the rest), the rows, the bytes this thread
/// allocated (the SMALLEST of <see cref="Repeats"/> calls — a GC inside the window can only add)
/// and the wall time (their median). The numbers in the commit bodies are Release's, over the
/// 49 000-span tier; the suite runs this in Debug on a tenth of it.</para>
///
/// <para>TIME IT WITH TIERED COMPILATION OFF (<c>DOTNET_TieredCompilation=0</c>). The warm-up is
/// one page an arm, so with tiering on a measured call can still run tier-0 code: the ordered
/// tier's engine call then reads 1.2-2.1 ms instead of 0.35, and the disordered tier, measured
/// second, can look no slower than the ordered one while reading fifteen times the spans. The
/// bytes do not depend on it, except up to 14 KB on the disordered tier's whole TraceQL page.</para>
///
/// <para>TWO TIERS, because the index prunes by ARRIVAL order (#122 review L1). The first arrives in
/// start order; the second is the same spans arriving as an imperfect fleet sends them — one in a
/// hundred from a clock 30 s ahead, one in fifty a long span reported 10 s after it started. A
/// block's bounds used to cover all of its spans, so those few kept most blocks in every page's
/// walk; since #127 the index lists them apart and the readers take each on its own, so the "read"
/// column of the disordered tier is about the ordered one's, plus the listed spans in the
/// window.</para>
/// </summary>
public sealed class TraceStreamPageProbe : IDisposable
{
#if DEBUG
    private const int Spans   = 5_000;
#else
    private const int Spans   = 49_000;   // just under the 50 000-span flush threshold
#endif
    private const int Pages   = 6;
    private const int Repeats = 5;

    /// <summary>What the SSE routes page at: TraceQueryEndpointMapper.FilterStreamPageSize and QlStreamPageSize.</summary>
    private const int ListPageRows = 500;
    private const int QlPageRows   = 200;

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddDays(1);

    private readonly List<string>      _dirs = [];
    private readonly ITestOutputHelper _out;

    public TraceStreamPageProbe(ITestOutputHelper output) => _out = output;

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    private readonly record struct PageCost(int Page, int WindowSpans, int Read, int Rows, long Bytes, double Ms);

    [Fact]
    public void Stream_pages_over_the_hot_tier()
    {
        // Three services, every seventh span an error: the list rows carry a real service set and
        // `{ status = error }` has something to find.
        var corpus = TraceAggregateLockProbe.Corpus(0, Spans, services: 3);

        MeasureTier(corpus, "ARRIVING IN START ORDER");
        MeasureTier(Disordered(corpus),
            "ARRIVING DISORDERED (1 span in 100 from a clock 30 s ahead, 1 in 50 reported 10 s late)");
    }

    private void MeasureTier(SpanIngestItem[] corpus, string arrival)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-pageprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        Assert.Equal(Spans, engine.WriteSpans(corpus));

        // What the engine read for the page just fetched: every fetch below makes one hot pass.
        int read = -1;
        engine._hotSearchVisitedForTest = n => read = n;
        engine._listHotVisitedForTest   = n => read = n;
        Func<int> lastRead = () => read;

        long[] starts = new long[corpus.Length];
        for (int i = 0; i < corpus.Length; i++) starts[i] = corpus[i].StartTimeUnixNano;

        var everySpan = TraceQLParser.Parse("{ .db.system = \"mssql\" }");   // no scalar hint: every span matches
        var errors    = TraceQLParser.Parse("{ status = error }");           // a status hint: one span in seven

        // Warm every path on the first page, so no measured call is jitting.
        Walk(starts, lastRead, (to, n) => ListPage(engine, to, n), ListPageRows, measure: false);
        Walk(starts, lastRead, (to, n) => QlPage(engine, everySpan, to, n), QlPageRows, measure: false);
        Walk(starts, lastRead, (to, n) => QlPage(engine, errors, to, n), QlPageRows, measure: false);
        Walk(starts, lastRead, (to, n) => SearchPage(engine, to, n, null), QlPageRows * 10, measure: false);

        var list   = Walk(starts, lastRead, (to, n) => ListPage(engine, to, n), ListPageRows, measure: true);
        var all    = Walk(starts, lastRead, (to, n) => QlPage(engine, everySpan, to, n), QlPageRows, measure: true);
        var err    = Walk(starts, lastRead, (to, n) => QlPage(engine, errors, to, n), QlPageRows, measure: true);
        var search = Walk(starts, lastRead, (to, n) => SearchPage(engine, to, n, null), QlPageRows * 10, measure: true);
        var errHot = Walk(starts, lastRead, (to, n) => SearchPage(engine, to, n, SpanStatusCode.Error), QlPageRows * 10, measure: true);

        _out.WriteLine("");
        _out.WriteLine($"STREAM PAGES over a {Spans:N0}-span hot tier {arrival}");
        _out.WriteLine($"  (10 spans/trace, 3 services, 8 attributes); bytes = this thread, min of {Repeats}; "
                     + $"ms = median of {Repeats}; read = unflushed spans the engine read for the page");
        Print($"list   GetTraceListAsync({ListPageRows})", list);
        Print($"traceql {{ .db.system = \"mssql\" }} ({QlPageRows} rows)", all);
        Print($"traceql {{ status = error }} ({QlPageRows} rows)", err);
        Print($"engine SearchSpansAsync({QlPageRows * 10}), no hint — the TraceQL page's own engine call", search);
        Print($"engine SearchSpansAsync({QlPageRows * 10}, status = Error)", errHot);

        Assert.All(list, static p => Assert.True(p.Rows > 0));
    }

    /// <summary>
    /// The same spans, arriving as an imperfect fleet sends them: one in a hundred from a clock 30 s
    /// ahead, one in fifty a long span reported 10 s after it started. Same order of arrival, same
    /// ids, the starts moved.
    /// </summary>
    internal static SpanIngestItem[] Disordered(SpanIngestItem[] corpus)
    {
        var items = (SpanIngestItem[])corpus.Clone();
        for (int i = 0; i < items.Length; i++)
        {
            long shift = i % 100 == 37 ? 30_000_000_000L
                       : i % 50  == 11 ? -10_000_000_000L
                       :                 0;
            if (shift == 0) continue;
            var s = items[i];
            items[i] = new SpanIngestItem
            {
                TraceId = s.TraceId, SpanId = s.SpanId, ParentSpanId = s.ParentSpanId,
                StartTimeUnixNano = s.StartTimeUnixNano + shift, DurationNanos = s.DurationNanos,
                Name = s.Name, ServiceName = s.ServiceName, Kind = s.Kind, Status = s.Status,
                HttpStatusCode = s.HttpStatusCode, AttributesBytes = s.AttributesBytes,
            };
        }
        return items;
    }

    private void Print(string title, List<PageCost> pages)
    {
        _out.WriteLine("");
        _out.WriteLine($"  {title}");
        _out.WriteLine($"    {"page",4} {"window spans",12} {"read",7} {"rows",5} {"KB",9} {"ms",7}");
        foreach (var p in pages)
            _out.WriteLine($"    {p.Page,4} {p.WindowSpans,12:N0} {p.Read,7:N0} {p.Rows,5} {p.Bytes / 1024.0,9:N0} {p.Ms,7:N2}");
    }

    /// <summary>One page: the rows' start times, and whether the call completed on this thread.</summary>
    private delegate (List<long> Starts, bool Sync) Fetch(DateTimeOffset pageTo, int rows);

    private static (List<long>, bool) ListPage(TraceStorageEngine engine, DateTimeOffset to, int rows)
    {
        var task = engine.GetTraceListAsync(From, to, null, null, null, null, null, rows);
        bool sync = task.IsCompleted;
        var page = task.GetAwaiter().GetResult();
        var starts = new List<long>(page.Rows.Count);
        foreach (var r in page.Rows) starts.Add(r.RootStartNano);
        return (starts, sync);
    }

    private static (List<long>, bool) QlPage(TraceStorageEngine engine, SpanPredicate pred, DateTimeOffset to, int rows)
    {
        var task = TraceQLExecutor.ExecuteAsync(engine, pred, From, to, rows, CancellationToken.None);
        bool sync = task.IsCompleted;
        var page = task.GetAwaiter().GetResult();
        var starts = new List<long>(page.Rows.Count);
        foreach (var r in page.Rows) starts.Add(r.StartTimeUnixNano);
        return (starts, sync);
    }

    /// <summary>
    /// The engine call alone: the newest <paramref name="spans"/> unflushed spans, the way the
    /// TraceQL page asks for them (ten a row). Driven by hand so "completed synchronously" can be
    /// checked step by step.
    /// </summary>
    private static (List<long>, bool) SearchPage(TraceStorageEngine engine, DateTimeOffset to, int spans, SpanStatusCode? status)
    {
        var  starts = new List<long>(spans);
        bool sync   = true;
        var  e      = engine.SearchSpansAsync(From, to, status: status, limit: spans).GetAsyncEnumerator();
        while (true)
        {
            var  step = e.MoveNextAsync();
            sync &= step.IsCompleted;
            bool more = step.IsCompleted ? step.Result : step.AsTask().GetAwaiter().GetResult();
            if (!more) break;
            starts.Add(e.Current.StartTimeUnixNano);
        }
        var done = e.DisposeAsync();
        sync &= done.IsCompleted;
        if (!done.IsCompleted) done.AsTask().GetAwaiter().GetResult();
        return (starts, sync);
    }

    /// <summary>
    /// Pages down the window as the stream does: the ask is the cursor rounded UP to its
    /// millisecond, the next cursor the oldest row returned.
    /// </summary>
    private static List<PageCost> Walk(long[] starts, Func<int> lastRead, Fetch fetch, int rows, bool measure)
    {
        var  result = new List<PageCost>(Pages);
        long cursor = To.ToUnixTimeMilliseconds() * 1_000_000L;
        long from   = From.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int p = 0; p < (measure ? Pages : 1); p++)
        {
            Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
            long toNano = pageTo.ToUnixTimeMilliseconds() * 1_000_000L;

            List<long>? rowStarts = null;
            long   bytes = long.MaxValue;
            var    ms    = new double[Repeats];
            for (int r = 0; r < Repeats; r++)
            {
                long a0 = GC.GetAllocatedBytesForCurrentThread();
                long t0 = Stopwatch.GetTimestamp();
                var (got, sync) = fetch(pageTo, rows);
                ms[r]  = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
                bytes  = Math.Min(bytes, GC.GetAllocatedBytesForCurrentThread() - a0);
                Assert.True(sync, "a hot-only page yielded, so this thread's counter saw only part of it");
                rowStarts = got;
            }
            Array.Sort(ms);

            int inWindow = 0;
            foreach (long s in starts) if (s >= from && s <= toNano) inWindow++;
            result.Add(new PageCost(p, inWindow, lastRead(), rowStarts!.Count, bytes, ms[Repeats / 2]));

            if (rowStarts.Count == 0) break;
            long oldest = long.MaxValue;
            foreach (long s in rowStarts) oldest = Math.Min(oldest, s);
            if (oldest >= cursor) break;
            cursor = oldest;
        }
        return result;
    }
}
