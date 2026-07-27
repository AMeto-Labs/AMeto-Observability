using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// DIAGNOSTIC PROBE (temporary). Splits the flush-path index-build retention between the
/// inverted index and the trigram index, and attributes the inverted share per property,
/// so a fix targets whichever structure actually holds the memory.
///
/// Feeds the two public accumulators exactly what <c>SegmentIndexBuilder</c> feeds them
/// (see IndexHeaderFields / AddScalar): every event contributes @l, the message template
/// (trigram), TraceId, SpanId, service name and each flattened property; every string
/// value of length >= 3 also goes to the trigram index.
/// </summary>
public sealed class IndexBuildBreakdownProbe
{
    private readonly ITestOutputHelper _out;
    public IndexBuildBreakdownProbe(ITestOutputHelper o) => _out = o;

    private const int Events = 130_000;
    private const double MB  = 1048576.0;

    [Fact]
    public void Breakdown_InvertedVsTrigram()
    {
        var ev = Generate(Events);

        _out.WriteLine($"{Events:N0} events, one 64 MB hot tier's worth\n");

        long inverted = MeasureInverted(ev, includeTrace: true);
        long invertedNoTrace = MeasureInverted(ev, includeTrace: false);
        long trigram = MeasureTrigram(ev);

        _out.WriteLine($"  inverted index  (with @tr/@sp)   {inverted / MB,8:F1} MB");
        _out.WriteLine($"  inverted index  (without @tr/@sp){invertedNoTrace / MB,8:F1} MB");
        _out.WriteLine($"  trigram index                    {trigram / MB,8:F1} MB");
        _out.WriteLine($"  ---------------------------------------------");
        _out.WriteLine($"  total                            {(inverted + trigram) / MB,8:F1} MB");
        _out.WriteLine("");

        // Per-property attribution of the inverted index: which fields cost what.
        _out.WriteLine("  inverted index, per property (distinct values -> retained):");
        foreach (var name in new[] { "@l", "@tr", "@sp", "@svc", "orderId", "customerId",
                                     "http.method", "http.route", "http.status_code",
                                     "duration_ms", "region", "RequestId" })
        {
            var (bytes, distinct) = MeasureSingleProperty(ev, name);
            _out.WriteLine($"    {name,-18} {distinct,8:N0} distinct  {bytes / MB,7:F1} MB  {(double)bytes / Events,6:F0} B/event");
        }

        _out.WriteLine("");
        _out.WriteLine("  trigram index, source attribution:");
        _out.WriteLine($"    template only              {MeasureTrigramSubset(ev, tmpl: true, values: false) / MB,7:F1} MB");
        _out.WriteLine($"    string property values only{MeasureTrigramSubset(ev, tmpl: false, values: true) / MB,7:F1} MB");
    }

    // ── measurement helpers ───────────────────────────────────────────────────

    private static long MeasureInverted(Ev[] ev, bool includeTrace)
    {
        long before = GC.GetTotalMemory(true);
        var idx = new SegmentInvertedIndex();
        for (uint i = 0; i < ev.Length; i++)
        {
            ref readonly var e = ref ev[i];
            idx.Add(i, "@l", "Information");
            if (includeTrace)
            {
                idx.Add(i, "@tr", e.TraceId);
                idx.Add(i, "@sp", e.SpanId);
            }
            idx.Add(i, "@svc", "Etisalat.API");
            idx.Add(i, "orderId",          e.OrderId);
            idx.Add(i, "customerId",       e.CustomerId);
            idx.Add(i, "http.method",      e.Method);
            idx.Add(i, "http.route",       e.Route);
            idx.Add(i, "http.status_code", e.Status);
            idx.Add(i, "duration_ms",      e.Duration);
            idx.Add(i, "region",           "ae-dxb");
            idx.Add(i, "RequestId",        e.RequestId);
        }
        long after = GC.GetTotalMemory(true);
        GC.KeepAlive(idx);
        return after - before;
    }

    private static (long Bytes, int Distinct) MeasureSingleProperty(Ev[] ev, string name)
    {
        var distinct = new HashSet<string>(StringComparer.Ordinal);
        long before = GC.GetTotalMemory(true);
        var idx = new SegmentInvertedIndex();
        for (uint i = 0; i < ev.Length; i++)
        {
            ref readonly var e = ref ev[i];
            object? v = name switch
            {
                "@l"               => "Information",
                "@tr"              => e.TraceId,
                "@sp"              => e.SpanId,
                "@svc"             => "Etisalat.API",
                "orderId"          => e.OrderId,
                "customerId"       => e.CustomerId,
                "http.method"      => e.Method,
                "http.route"       => e.Route,
                "http.status_code" => e.Status,
                "duration_ms"      => e.Duration,
                "region"           => "ae-dxb",
                _                  => e.RequestId,
            };
            idx.Add(i, name, v);
            distinct.Add(v?.ToString() ?? "");
        }
        long after = GC.GetTotalMemory(true);
        GC.KeepAlive(idx);
        // Subtract the distinct-set's own cost so we report only the index.
        return (after - before, distinct.Count);
    }

    private static long MeasureTrigram(Ev[] ev) => MeasureTrigramSubset(ev, tmpl: true, values: true);

    private static long MeasureTrigramSubset(Ev[] ev, bool tmpl, bool values)
    {
        const string template = "HTTP {Method} {Route} responded {Status} in {Elapsed} ms";
        long before = GC.GetTotalMemory(true);
        var idx = new SegmentTrigramIndex();
        for (uint i = 0; i < ev.Length; i++)
        {
            ref readonly var e = ref ev[i];
            if (tmpl) idx.Add(i, template);
            if (values)
            {
                // SegmentIndexBuilder.AddScalar feeds every string value of length >= 3.
                idx.Add(i, e.TraceId);
                idx.Add(i, e.SpanId);
                idx.Add(i, e.CustomerId);
                idx.Add(i, e.Method);
                idx.Add(i, e.Route);
                idx.Add(i, "ae-dxb");
                idx.Add(i, e.RequestId);
            }
        }
        long after = GC.GetTotalMemory(true);
        GC.KeepAlive(idx);
        return after - before;
    }

    // ── data ──────────────────────────────────────────────────────────────────

    private readonly struct Ev
    {
        public readonly string TraceId, SpanId, CustomerId, Method, Route, RequestId;
        public readonly int    OrderId, Status;
        public readonly double Duration;
        public Ev(string tr, string sp, string cust, string m, string r, string rq, int o, int st, double d)
        { TraceId = tr; SpanId = sp; CustomerId = cust; Method = m; Route = r; RequestId = rq; OrderId = o; Status = st; Duration = d; }
    }

    private static Ev[] Generate(int n)
    {
        var rng = new Random(5);
        string[] methods = { "GET", "POST", "PUT", "DELETE" };
        string[] routes  = { "/api/pay", "/api/topup", "/api/status", "/api/balance" };
        int[]    codes   = { 200, 201, 400, 404, 500 };
        var arr = new Ev[n];
        for (int i = 0; i < n; i++)
            arr[i] = new Ev(
                TraceIdHelper.FormatTraceId((ulong)rng.NextInt64(), (ulong)rng.NextInt64())!,
                TraceIdHelper.FormatSpanId((ulong)rng.NextInt64())!,
                "cust-" + rng.Next(0, 100_000),
                methods[rng.Next(methods.Length)],
                routes[rng.Next(routes.Length)],
                "0HN" + rng.Next().ToString("x"),
                rng.Next(0, 10_000_000),
                codes[rng.Next(codes.Length)],
                Math.Round(rng.NextDouble() * 500, 2));
        return arr;
    }
}
