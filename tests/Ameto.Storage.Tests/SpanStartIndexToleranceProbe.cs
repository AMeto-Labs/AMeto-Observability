using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT THE START INDEX'S TOLERANCE AND CAP COST AND BUY (#127): for arrival orders a hot tier sees,
/// how many spans each tolerance lists as outliers, and how many spans a TraceQL page and a list page
/// then read. The tolerance is how far outside its block's range a start may lie and still widen
/// it; a span past it is listed instead, and a reader reads it on its own.
///
/// <para>PRINTED, NOT ASSERTED. The arrival orders:</para>
/// <list type="bullet">
///   <item>in order: one span a millisecond (<c>TraceAggregateLockProbe.Corpus</c>);</item>
///   <item>the stream probe's disordered tier: one span in a hundred 30 s ahead, one in fifty
///   reported 10 s late;</item>
///   <item>one span in N 30 s ahead, for N = 1 000, 100, 20 and 10 (the last past the cap);</item>
///   <item>one span in a hundred 1, 3 and 10 s ahead: skews around the tolerance;</item>
///   <item>exporters: twenty producers, a thousand spans a second between them, each exporting
///   what ended in the last five seconds every five seconds (the OpenTelemetry SDK's batch
///   default) — a few long spans, and the start level jumping back and forth by up to five seconds
///   between batches;</item>
///   <item>a slow producer: a span every three seconds.</item>
/// </list>
/// <para>For each: the outliers listed; the spans a TraceQL stream (2 000 a page, no filter) reads,
/// on page 0 and the most on pages 1-5; and the spans a list page ten seconds deep reads.</para>
/// </summary>
public sealed class SpanStartIndexToleranceProbe : IDisposable
{
#if DEBUG
    private const int Spans = 20_000;
#else
    private const int Spans = 49_000;
#endif
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddDays(1);
    private static readonly long[] Tolerances = [500_000_000L, 1_000_000_000L, 2_000_000_000L, 5_000_000_000L, 10_000_000_000L, long.MaxValue];   // the last: every span widens its block, as before #127

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-tolerance-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;

    public SpanStartIndexToleranceProbe(ITestOutputHelper output) { _out = output; Directory.CreateDirectory(_dir); }

    public void Dispose()
    {
        SpanStartIndex.ShapeForTest = null;
        try { Directory.Delete(_dir, true); } catch { }
    }

    [Fact]
    public void Outliers_and_reads_by_tolerance()
    {
        var corpus = TraceAggregateLockProbe.Corpus(0, Spans);
        var shapes = new (string Name, SpanIngestItem[] Items)[]
        {
            ("in order",                   corpus),
            ("disordered (the probe's)",   TraceStreamPageProbe.Disordered(corpus)),
            ("1 in 1 000, 30 s ahead",     Skewed(corpus, 1_000, 30)),
            ("1 in 100, 30 s ahead",       Skewed(corpus, 100, 30)),
            ("1 in 20, 30 s ahead",        Skewed(corpus, 20, 30)),
            ("1 in 10, 30 s ahead",        Skewed(corpus, 10, 30)),
            ("1 in 100, 1 s ahead",        Skewed(corpus, 100, 1)),
            ("1 in 100, 3 s ahead",        Skewed(corpus, 100, 3)),
            ("1 in 100, 10 s ahead",       Skewed(corpus, 100, 10)),
            ("exporters (5 s batches)",    Exporters(corpus)),
            ("a span every 3 s",           Slow(corpus, Math.Min(Spans, 1_200))),
        };

        _out.WriteLine($"START INDEX OUTLIERS AND READS by tolerance, {Spans:N0} spans; cap {SpanStartIndex.MaxOutliers:N0}");
        _out.WriteLine("  outliers = spans listed; traceql = spans a TraceQL page (2 000, no filter) read on page 0 / the most on pages 1-5;");
        _out.WriteLine("  list = spans a list page ten seconds below the newest on-time span read / the spans in its window;");
        _out.WriteLine("  KB / us = TraceQL page 0's engine call: this thread's bytes, the least of 5, and its median time; tolerance - = none (before #127)");
        _out.WriteLine("");
        _out.WriteLine($"  {"arrival order",-26} {"tolerance",9} {"outliers",9} {"traceql p0",11} {"p1-5 max",9} {"list",7} {"window",7} {"p0 KB",7} {"p0 us",7}");
        foreach (var (name, items) in shapes)
        {
            foreach (long tolerance in Tolerances)
            {
                SpanStartIndex.ShapeForTest = (tolerance, SpanStartIndex.MaxOutliers, SpanStartIndex.ShiftAfter);
                var (outliers, page0, deeper, list, window, kb, us) = Measure(items);
                string tol = tolerance == long.MaxValue ? "-" : $"{tolerance / 1e9:0.#}s";
                _out.WriteLine($"  {name,-26} {tol,9} {outliers,9:N0} {page0,11:N0} {deeper,9:N0} {list,7:N0} {window,7:N0} {kb,7:N0} {us,7:N0}");
            }
            SpanStartIndex.ShapeForTest = null;
            _out.WriteLine("");
        }
    }

    /// <summary>
    /// THE RUN THAT MAKES A NEW LEVEL. Outliers in a row, each within the tolerance of the one before,
    /// re-anchor the range at the last of them. Too short a run lets a producer's small skewed batch
    /// poison its block; too long a one lists every span of a level change — a gap in the traffic, the
    /// next exporter's batch — until it ends.
    /// </summary>
    [Fact]
    public void Outliers_and_reads_by_level_shift_run()
    {
        var corpus = TraceAggregateLockProbe.Corpus(0, Spans);
        var shapes = new (string Name, SpanIngestItem[] Items)[]
        {
            ("skewed runs of 2, 2 %",     SkewedRuns(corpus, 2)),
            ("skewed runs of 4, 2 %",     SkewedRuns(corpus, 4)),
            ("skewed runs of 8, 2 %",     SkewedRuns(corpus, 8)),
            ("skewed runs of 64, 2 %",    SkewedRuns(corpus, 64)),
            ("a 60 s gap every 10 000",   Gaps(corpus)),
            ("exporters (5 s batches)",   Exporters(corpus)),
        };
        _out.WriteLine($"START INDEX OUTLIERS AND READS by the run that makes a new level, {Spans:N0} spans; tolerance {SpanStartIndex.ToleranceNanos / 1e9:0.#} s");
        _out.WriteLine($"  {"arrival order",-26} {"run",5} {"outliers",9} {"traceql p0",11} {"p1-5 max",9} {"list",7} {"window",7}");
        foreach (var (name, items) in shapes)
        {
            foreach (int run in (int[])[2, 3, 4, 8, 16, int.MaxValue])
            {
                SpanStartIndex.ShapeForTest = (SpanStartIndex.ToleranceNanos, SpanStartIndex.MaxOutliers, run);
                var (outliers, page0, deeper, list, window, _, _) = Measure(items);
                string r = run == int.MaxValue ? "-" : run.ToString();
                _out.WriteLine($"  {name,-26} {r,5} {outliers,9:N0} {page0,11:N0} {deeper,9:N0} {list,7:N0} {window,7:N0}");
            }
            SpanStartIndex.ShapeForTest = null;
            _out.WriteLine("");
        }
    }

    /// <summary>Two spans in a hundred 30 s ahead, in runs of <paramref name="run"/> — a skewed producer's batches.</summary>
    private static SpanIngestItem[] SkewedRuns(SpanIngestItem[] corpus, int run)
    {
        var items = (SpanIngestItem[])corpus.Clone();
        for (int at = run * 25; at + run <= items.Length; at += run * 50)
            for (int i = at; i < at + run; i++)
                items[i] = At(items[i], items[i].StartTimeUnixNano + 30_000_000_000L);
        return items;
    }

    /// <summary>In order, with the traffic pausing for a minute after every 10 000 spans.</summary>
    private static SpanIngestItem[] Gaps(SpanIngestItem[] corpus) =>
        [.. corpus.Select((s, i) => At(s, s.StartTimeUnixNano + i / 10_000 * 60_000_000_000L))];

    private (int Outliers, int Page0, int Deeper, int List, int Window, double Kb, double Us) Measure(SpanIngestItem[] items)
    {
        string dir = Path.Combine(_dir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int i = 0; i < items.Length; i += 512)
            Assert.Equal(Math.Min(512, items.Length - i), engine.WriteSpans(items.AsSpan(i, Math.Min(512, items.Length - i))));

        int visited = 0;
        engine._hotSearchVisitedForTest = n => visited = n;
        engine._listHotVisitedForTest   = n => visited = n;

        int page0 = 0, deeper = 0;
        long cursor = To.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int page = 0; page < 6; page++)
        {
            Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(cursor, out var pageTo));
            var got = engine.SearchSpansAsync(From, pageTo, limit: 2_000).ToBlockingEnumerable().ToList();
            if (page == 0) page0 = visited; else deeper = Math.Max(deeper, visited);
            if (got.Count < 2_000) break;
            cursor = got.Min(static s => s.StartTimeUnixNano);
        }

        // Ten seconds below the newest on-time start, whatever runs ahead of it.
        long ceiling = Base.ToUnixTimeMilliseconds() * 1_000_000L + (Spans - 1) * 1_000_000L - 10_000_000_000L;
        engine.GetTraceListAsync(From, DateTimeOffset.FromUnixTimeMilliseconds(ceiling / 1_000_000L), null, null, null, null, null, 500)
              .GetAwaiter().GetResult();
        int window = items.Count(s => s.StartTimeUnixNano <= ceiling);
        int list   = visited;

        // Page 0's engine call alone, warm: the least bytes and the median time of five.
        long best = long.MaxValue; var us = new double[5];
        for (int r = -1; r < 5; r++)
        {
            long a0 = GC.GetAllocatedBytesForCurrentThread(), t0 = System.Diagnostics.Stopwatch.GetTimestamp();
            int n = 0;
            var e = engine.SearchSpansAsync(From, To, limit: 2_000).GetAsyncEnumerator();
            while (e.MoveNextAsync().AsTask().GetAwaiter().GetResult()) n++;
            e.DisposeAsync().AsTask().GetAwaiter().GetResult();
            if (r < 0) continue;
            us[r] = System.Diagnostics.Stopwatch.GetElapsedTime(t0).TotalMicroseconds;
            best  = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - a0);
        }
        Array.Sort(us);
        return (engine.HotOutliersForTest, page0, deeper, list, window, best / 1024.0, us[2]);
    }

    private static SpanIngestItem At(SpanIngestItem s, long start) => new()
    {
        TraceId = s.TraceId, SpanId = s.SpanId, ParentSpanId = s.ParentSpanId, StartTimeUnixNano = start,
        DurationNanos = s.DurationNanos, Name = s.Name, ServiceName = s.ServiceName, Kind = s.Kind,
        Status = s.Status, HttpStatusCode = s.HttpStatusCode, AttributesBytes = s.AttributesBytes,
    };

    private static SpanIngestItem[] Skewed(SpanIngestItem[] corpus, int oneIn, int seconds)
    {
        var items = (SpanIngestItem[])corpus.Clone();
        for (int i = oneIn / 2; i < items.Length; i += oneIn)
            items[i] = At(items[i], items[i].StartTimeUnixNano + seconds * 1_000_000_000L);
        return items;
    }

    /// <summary>
    /// Twenty producers, a thousand spans a second between them; each exports, every five seconds
    /// at its own phase, the spans that ENDED since its last export, in the order they ended.
    /// Durations: most under 50 ms, one in ten up to a second, one in a hundred up to twenty.
    /// </summary>
    private static SpanIngestItem[] Exporters(SpanIngestItem[] corpus)
    {
        const int  producers = 20;
        const long period    = 5_000_000_000L;
        var rnd     = new Random(127);
        long t0     = corpus[0].StartTimeUnixNano;
        var  phase  = Enumerable.Range(0, producers).Select(_ => rnd.NextInt64(0, period)).ToArray();
        var  spans  = new List<(long Export, int Producer, long End, SpanIngestItem Item)>(corpus.Length);
        for (int i = 0; i < corpus.Length; i++)
        {
            int  p     = rnd.Next(producers);
            long end   = t0 + i * 1_000_000L;                                // one span ends a millisecond
            long dur   = rnd.Next(100) switch
            {
                0     => rnd.NextInt64(1_000_000_000L, 20_000_000_000L),
                < 10  => rnd.NextInt64(50_000_000L, 1_000_000_000L),
                _     => rnd.NextInt64(1_000_000L, 50_000_000L),
            };
            long export = phase[p] + ((end - t0 - phase[p]) / period + 1) * period;
            spans.Add((export, p, end, At(corpus[i], end - dur)));
        }
        return [.. spans.OrderBy(static s => s.Export).ThenBy(static s => s.Producer).ThenBy(static s => s.End).Select(static s => s.Item)];
    }

    private static SpanIngestItem[] Slow(SpanIngestItem[] corpus, int count) =>
        [.. corpus.Take(count).Select((s, i) => At(s, corpus[0].StartTimeUnixNano + i * 3_000_000_000L))];
}
