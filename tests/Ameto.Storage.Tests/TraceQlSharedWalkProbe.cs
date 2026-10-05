using System.Buffers;
using System.Diagnostics;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A TRACEQL FILTER'S ATTRIBUTE PREDICATES COST PER SPAN: one walk of the span's map each,
/// against one walk for all of them (#94).
///
/// <para>Filters of one, three and six attribute predicates over the fixture's eight-attribute
/// SqlClient span, every predicate true for every span — so the conjunction never short-circuits
/// and the per-predicate evaluation pays a whole walk per predicate — and the case the shared walk
/// is NOT for: three predicates whose first rejects every span, where both make one walk and the
/// shared one compares each map key with three keys instead of one.</para>
///
/// <para>THE SAME PAGE BOTH WAYS, in one process, interleaved round by round, so the before and the
/// after are one machine's at one moment — on a shared box that is worth more than any absolute
/// figure. The per-predicate side is <see cref="SpanPredicateEvaluator.PerPredicateForTest"/>: the
/// AST, exactly as every page ran it before. Two readings each: the post-filter alone, over the very
/// spans the page's scan hands back, and the whole page through <see cref="TraceQLExecutor"/>, which
/// says what share of a page the walks were.</para>
///
/// <para>TIMINGS ARE PRINTED, NOT ASSERTED — they move with the machine and the configuration (the
/// suite runs in Debug; the figures worth quoting are Release's). What is asserted holds anywhere:
/// one walk per span for every filter, nothing allocated per span by the post-filter, and the same
/// rows from both.</para>
///
/// <para>MEASURED when it was written, Release, post-filter ns per span, per-predicate against
/// shared, on a development box shared with six other build-and-test jobs: cold 173 → 140 (one
/// predicate), 523 → 207 (three), 1 093 → 329 (six), 157 → 146 (three, the first rejecting); hot
/// 178 → 144, 518 → 209, 1 086 → 315, 161 → 150. Nothing allocated per span either way. Over three
/// runs the absolute figures moved by up to 2x with the box's load and the ratios held: 1.0-1.4x,
/// 2.4-2.6x, 2.8-4.6x and 1.1-1.3x.</para>
/// </summary>
public sealed class TraceQlSharedWalkProbe : IClassFixture<ColdSpanSegmentFixture>, IDisposable
{
#if DEBUG
    private const int Limit      = 200;     // 2 000 spans post-filtered a page: QlStreamPageSize
    private const int EvalRounds = 3;
    private const int PageRounds = 1;
#else
    private const int Limit      = 1_000;   // 10 000 spans post-filtered a page: the POST clamp
    private const int EvalRounds = 15;
    private const int PageRounds = 7;
#endif

    /// <summary>The hot tier holds exactly one page's span limit, so both tiers post-filter the same count.</summary>
    private const int HotSpans = Limit * 10;

    private const string Route = "/api/v1/tenants/{tenantId}/payments";

    /// <summary>
    /// The filters, and how many walks of a span's map the per-predicate evaluation makes for each —
    /// one per predicate it reaches.
    /// </summary>
    private static readonly (string Label, int PerPredicateWalks, string Query)[] Filters =
    [
        ("1 predicate",    1, "{ .db.system = \"mssql\" }"),
        ("3 predicates",   3, $"{{ .db.system = \"mssql\" && .http.route = \"{Route}\" && .net.peer.port = 1433 }}"),
        ("6 predicates",   6, $"{{ .db.system = \"mssql\" && .db.name = \"payments\" && .net.peer.name = \"sql-prod-03.svc.cluster.local\" "
                            + $"&& .net.peer.port = 1433 && .http.route = \"{Route}\" "
                            + "&& .otel.library.name = \"OpenTelemetry.Instrumentation.SqlClient\" }"),
        ("3, 1st rejects", 1, $"{{ .net.peer.port < 1000 && .db.system = \"mssql\" && .http.route = \"{Route}\" }}"),
    ];

    private readonly ColdSpanSegmentFixture _fx;
    private readonly ITestOutputHelper      _out;
    private readonly List<string>           _dirs = [];

    public TraceQlSharedWalkProbe(ColdSpanSegmentFixture fx, ITestOutputHelper output)
    {
        _fx  = fx;
        _out = output;
    }

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    private readonly record struct Reading(
        string Tier, string Filter, int Rows,
        double WalksPerSpan, int PerPredicateWalks,
        double AloneNs, double SharedNs, double AloneBytes, double SharedBytes,
        double AlonePageMs, double SharedPageMs, double AlonePageKb, double SharedPageKb);

    [Fact]
    public async Task One_walk_per_span_for_all_the_attribute_predicates_of_a_filter()
    {
        string coldDir = _fx.CopySegmentToPrivateDir();
        _dirs.Add(coldDir);
        using var cold = new TraceStorageEngine(coldDir, NullLogger<TraceStorageEngine>.Instance);
        cold.LoadColdSegments();   // the constructor does not rescan; the background worker would

        string hotDir = Path.Combine(Path.GetTempPath(), "ameto-qlshared-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(hotDir);
        _dirs.Add(hotDir);
        using var hot = new TraceStorageEngine(hotDir, NullLogger<TraceStorageEngine>.Instance);
        FillHotTier(hot);

        // TWICE, AND THE FIRST PASS IS NOT READ. Tiered compilation promotes the walk, the comparer
        // and the page path off this thread while whichever row is measured first is running, and
        // that row carries the promotion: on the development box the first row (cold, one
        // predicate) read 1 244 -> 1 110 ns per span on the unread pass and 410 -> 320 on the read
        // one.
        var readings = new List<Reading>();
        for (int pass = 0; pass < 2; pass++)
        {
            readings.Clear();
            foreach (var (tier, engine) in new[] { ("cold", cold), ("hot", hot) })
                foreach (var (label, perPredicateWalks, query) in Filters)
                    readings.Add(await MeasureAsync(tier, engine, label, perPredicateWalks, TraceQLParser.Parse(query)));
        }

        // Nothing flushed under the hot measurement: its spans were the tier's all along.
        Assert.Empty(Directory.GetFiles(hotDir, "*.trc", SearchOption.AllDirectories));

        _out.WriteLine($"TRACEQL FILTER WALKS  limit {Limit:N0}: the page post-filters {HotSpans:N0} spans; "
                     + $"post-filter median of {EvalRounds}, page median of {PageRounds} round(s), "
                     + "per-predicate and shared interleaved");
        _out.WriteLine($"  {"",-4} {"filter",-15} {"walks/span",13}   {"post-filter ns/span",29} {"B/span",9}   "
                     + $"{"page ms",17} {"page KB",17}   rows");
        foreach (var r in readings)
            _out.WriteLine($"  {r.Tier,-4} {r.Filter,-15} {r.PerPredicateWalks,5} -> {r.WalksPerSpan,4:N0}   "
                         + $"{r.AloneNs,9:N0} -> {r.SharedNs,9:N0} ({r.AloneNs / r.SharedNs,4:N1}x) "
                         + $"{r.AloneBytes,3:N0} -> {r.SharedBytes,1:N0}   "
                         + $"{r.AlonePageMs,7:N1} -> {r.SharedPageMs,7:N1} {r.AlonePageKb,7:N0} -> {r.SharedPageKb,7:N0}   {r.Rows}");
    }

    private async Task<Reading> MeasureAsync(
        string tier, TraceStorageEngine engine, string label, int perPredicateWalks, SpanPredicate pred)
    {
        var from = ColdSpanSegmentFixture.Base.AddMinutes(-1);
        var to   = ColdSpanSegmentFixture.Base.AddDays(1);

        // ── The post-filter alone, over exactly what the page's scan hands it.
        var hints = TraceQLExecutor.ExtractHints(pred);
        var spans = new List<SpanRecord>(Limit * 10);
        await foreach (var s in engine.SearchSpansAsync(
            from, to,
            serviceName      : hints.ServiceName,
            status           : hints.Status,
            minDurationNanos : hints.MinDurationNanos,
            maxDurationNanos : hints.MaxDurationNanos,
            httpStatusCode   : hints.HttpStatusCode,
            limit            : Limit * 10,
            attrHints        : hints.AttrHints))
            spans.Add(s);
        Assert.Equal(Limit * 10, spans.Count);

        var aloneNs  = new double[EvalRounds];
        var sharedNs = new double[EvalRounds];
        double aloneBytes = double.MaxValue, sharedBytes = double.MaxValue, walksPerSpan = double.NaN;
        int selectedAlone = -1, selectedShared = -1;
        for (int round = -1; round < EvalRounds; round++)   // round -1 jits both and is not read
        {
            bool sharedFirst = (round & 1) == 1;
            for (int k = 0; k < 2; k++)
            {
                bool shared = sharedFirst == (k == 0);
                var  eval   = shared ? new SpanPredicateEvaluator(pred) : SpanPredicateEvaluator.PerPredicateForTest(pred);
                var (ns, bytes, selected) = PostFilter(eval, spans);
                if (round < 0) continue;

                if (shared)
                {
                    sharedNs[round] = ns;
                    sharedBytes     = Math.Min(sharedBytes, bytes);
                    selectedShared  = selected;
                    walksPerSpan    = eval.WalksForTest / (double)spans.Count;
                    // THE POINT, COUNTED: one walk per span, however many predicates the filter has.
                    Assert.Equal(spans.Count, eval.WalksForTest);
                }
                else
                {
                    aloneNs[round] = ns;
                    aloneBytes     = Math.Min(aloneBytes, bytes);
                    selectedAlone  = selected;
                }
            }
        }
        Assert.Equal(selectedAlone, selectedShared);

        // A GC landing inside a window adds this thread's unused allocation context to the reading
        // and can only add, so the smallest of the rounds is the clean one. The bound is 8 B and not
        // 1 because that context is up to 8 KB — 4 B a span over Debug's 2 000 — while anything the
        // walk allocated per span would be an object, 24 B at the least.
        Assert.True(sharedBytes < 8,
            $"{tier} {label}: the shared post-filter allocated {sharedBytes:N1} B per span — it allocates nothing per span");

        // ── The whole page, both ways.
        var alonePageMs  = new double[PageRounds];
        var sharedPageMs = new double[PageRounds];
        var alonePageKb  = new double[PageRounds];
        var sharedPageKb = new double[PageRounds];
        int rows = -1;
        for (int round = -1; round < PageRounds; round++)
        {
            bool sharedFirst = (round & 1) == 1;
            for (int k = 0; k < 2; k++)
            {
                bool shared = sharedFirst == (k == 0);
                var  eval   = shared ? new SpanPredicateEvaluator(pred) : SpanPredicateEvaluator.PerPredicateForTest(pred);

                // PROCESS-WIDE: a cold page awaits, and part of it runs on the pool. Nothing is
                // printed until the end, so xUnit's output drain has nothing to bill in here.
                long a0 = GC.GetTotalAllocatedBytes(precise: true);
                long t0 = Stopwatch.GetTimestamp();
                var page = await TraceQLExecutor.ExecuteAsync(engine, eval, from, to, Limit, CancellationToken.None);
                long t1 = Stopwatch.GetTimestamp();
                long a1 = GC.GetTotalAllocatedBytes(precise: true);

                rows = rows < 0 ? page.Rows.Count : rows;
                Assert.Equal(rows, page.Rows.Count);   // both evaluations select the same traces
                if (round < 0) continue;

                double ms = Stopwatch.GetElapsedTime(t0, t1).TotalMilliseconds;
                double kb = (a1 - a0) / 1024.0;
                if (shared) { sharedPageMs[round] = ms; sharedPageKb[round] = kb; }
                else        { alonePageMs[round]  = ms; alonePageKb[round]  = kb; }
            }
        }

        return new Reading(
            tier, label, rows, walksPerSpan, perPredicateWalks,
            Median(aloneNs), Median(sharedNs), aloneBytes, sharedBytes,
            Median(alonePageMs), Median(sharedPageMs), Median(alonePageKb), Median(sharedPageKb));
    }

    /// <summary>The post-filter as the page runs it: ns and bytes per span, and how many it selected.</summary>
    private static (double Ns, double Bytes, int Selected) PostFilter(SpanPredicateEvaluator eval, List<SpanRecord> spans)
    {
        int  selected = 0;
        long a0       = GC.GetAllocatedBytesForCurrentThread();
        long t0       = Stopwatch.GetTimestamp();
        foreach (var s in spans)
            if (eval.Evaluate(s) == true) selected++;
        long t1 = Stopwatch.GetTimestamp();
        long a1 = GC.GetAllocatedBytesForCurrentThread();
        return (Stopwatch.GetElapsedTime(t0, t1).TotalNanoseconds / spans.Count, (a1 - a0) / (double)spans.Count, selected);
    }

    private static double Median(double[] v)
    {
        var sorted = (double[])v.Clone();
        Array.Sort(sorted);
        return sorted[sorted.Length / 2];
    }

    /// <summary>
    /// The cold fixture's spans, again, in a hot tier: the same eight SqlClient attributes in the same
    /// order, the same start times and durations — so a hot page and a cold page post-filter the
    /// same maps and differ only in where the spans were read from.
    ///
    /// <para>A MAP OF ITS OWN PER SPAN, as ingest gives every span: sixty-four shared arrays would
    /// keep every map the post-filter walks in the cache, and the walk the shared evaluation saves
    /// is cheapest exactly when its bytes are already there.</para>
    /// </summary>
    private static void FillHotTier(TraceStorageEngine engine)
    {
        var templates = new byte[64][];   // thread.id is i % 64; everything else is one value
        for (int t = 0; t < templates.Length; t++)
            templates[t] = MapOf(ColdSpanSegmentFixture.SqlClientAttributes(t));

        for (int i = 0; i < HotSpans; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0x5A4ED, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = ColdSpanSegmentFixture.StartNano(i),
                DurationNanos     = ColdSpanSegmentFixture.DurationNanos(i),
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                AttributesBytes   = (byte[])templates[i % templates.Length].Clone(),
            });
    }

    private static byte[] MapOf(IReadOnlyDictionary<string, object?> attrs)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(attrs.Count);
        foreach (var (key, value) in attrs)
        {
            w.Write(key);
            switch (value)
            {
                case string s: w.Write(s); break;
                case long l:   w.Write(l); break;
                default: throw new ArgumentException($"the fixture has no {value?.GetType()} attribute", nameof(attrs));
            }
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
