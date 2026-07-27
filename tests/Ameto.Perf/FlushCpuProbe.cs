using System.Diagnostics;
using Ameto.Core;
using Ameto.Indexing;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// DIAGNOSTIC PROBE (temporary). Measures the CPU cost and GC pressure of ONE
/// flush-path index build, so the observed "45 % CPU then back to 0" burst can be
/// attributed. StorageEngine runs FlushConcurrency = clamp(ProcessorCount/2, 1, 8)
/// of these at once, so multiply the wall time by nothing and the core count by
/// FlushConcurrency to get the expected utilisation plateau.
/// </summary>
public sealed class FlushCpuProbe
{
    private readonly ITestOutputHelper _out;
    public FlushCpuProbe(ITestOutputHelper o) => _out = o;

    private const int Events = 130_000;
    private const double MB  = 1048576.0;

    [Fact]
    public void IndexBuild_CpuAndGcCost()
    {
        var ev = Generate(Events);

        // Mirror StorageEngine's derivation so the extrapolation below matches the engine.
        const long tierPayload   = 64L * 1024 * 1024;
        const long bytesPerEvent = 1_400;                 // IndexBuildBytesPerEvent
        const long managedBudget = 640L * 1024 * 1024;    // FlushManagedBudgetBytes
        const long nativeBudget  = 512L * 1024 * 1024;    // FlushNativeBudgetBytes

        int  capacity      = Ameto.Storage.HotTierSegment.EventCapacityFor(tierPayload);
        long tierNative    = Ameto.Storage.HotTierSegment.NativeBytesFor(tierPayload);
        long perFlush      = capacity * bytesPerEvent;
        int  widthByMemory = (int)Math.Clamp(managedBudget / perFlush, 1, 64);
        int  width         = Math.Clamp(Math.Min(Environment.ProcessorCount / 2, widthByMemory), 1, 8);
        int  slots         = Math.Clamp((int)(nativeBudget / tierNative), width, 64);

        _out.WriteLine($"cores = {Environment.ProcessorCount}, GC = {(System.Runtime.GCSettings.IsServerGC ? "Server" : "Workstation")}");
        _out.WriteLine($"tier  = {capacity:N0} events / {tierNative / (long)MB} MB native");
        _out.WriteLine($"width = {width} (core-based {Environment.ProcessorCount / 2}, memory-based {widthByMemory}), slots = {slots}\n");

        // Warm the JIT so the measurement is steady-state, not first-call.
        RunOne(ev, 5_000);

        int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);
        long allocBefore = GC.GetTotalAllocatedBytes(precise: false);
        var cpuBefore = Process.GetCurrentProcess().TotalProcessorTime;
        var sw = Stopwatch.StartNew();

        RunOne(ev, Events);

        sw.Stop();
        var cpu = Process.GetCurrentProcess().TotalProcessorTime - cpuBefore;
        long alloc = GC.GetTotalAllocatedBytes(precise: false) - allocBefore;

        _out.WriteLine($"  one 64 MB tier ({Events:N0} events)");
        _out.WriteLine($"    wall time            {sw.Elapsed.TotalMilliseconds,8:F0} ms");
        _out.WriteLine($"    CPU time             {cpu.TotalMilliseconds,8:F0} ms");
        _out.WriteLine($"    allocated            {alloc / MB,8:F1} MB");
        _out.WriteLine($"    gen0 / gen1 / gen2   {GC.CollectionCount(0) - g0,4} /{GC.CollectionCount(1) - g1,4} /{GC.CollectionCount(2) - g2,4}");
        _out.WriteLine("");
        _out.WriteLine($"  extrapolated burst with {width} concurrent flushes:");
        _out.WriteLine($"    duration            ~{sw.Elapsed.TotalMilliseconds,8:F0} ms");
        _out.WriteLine($"    utilisation         ~{100.0 * width / Environment.ProcessorCount,8:F0} % of {Environment.ProcessorCount} cores");
        _out.WriteLine($"    managed ceiling     ~{width * perFlush / (long)MB,8:N0} MB  (budget {managedBudget / (long)MB} MB)");
        _out.WriteLine($"    native  ceiling     ~{slots * tierNative / (long)MB,8:N0} MB  (budget {nativeBudget / (long)MB} MB)");

        // Cost of the maintenance GC that StorageEngine.ReleaseMaintenanceMemory forces.
        var swGc = Stopwatch.StartNew();
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        swGc.Stop();
        _out.WriteLine("");
        _out.WriteLine($"  ReleaseMaintenanceMemory() blocking compacting gen2: {swGc.Elapsed.TotalMilliseconds:F0} ms");
    }

    private static void RunOne(Ev[] ev, int count)
    {
        const string template = "HTTP {Method} {Route} responded {Status} in {Elapsed} ms";
        var inverted = new SegmentInvertedIndex();
        var trigram  = new SegmentTrigramIndex();
        var bloom    = SegmentBloomFilter.Create(count);

        for (uint i = 0; i < count; i++)
        {
            ref readonly var e = ref ev[i];
            inverted.Add(i, "@l", "Information");
            inverted.Add(i, "@tr", e.TraceId);
            inverted.Add(i, "@sp", e.SpanId);
            inverted.Add(i, "@svc", "Etisalat.API");
            inverted.Add(i, "orderId", e.OrderId);
            inverted.Add(i, "customerId", e.CustomerId);
            inverted.Add(i, "http.method", e.Method);
            inverted.Add(i, "http.route", e.Route);
            inverted.Add(i, "http.status_code", e.Status);
            inverted.Add(i, "duration_ms", e.Duration);
            inverted.Add(i, "RequestId", e.RequestId);

            trigram.Add(i, template);
            trigram.Add(i, e.TraceId);
            trigram.Add(i, e.SpanId);
            trigram.Add(i, e.CustomerId);
            trigram.Add(i, e.Method);
            trigram.Add(i, e.Route);
            trigram.Add(i, e.RequestId);

            bloom.Add(e.TraceId);
            bloom.Add(e.CustomerId);
            bloom.Add(e.RequestId);
        }

        // Serialisation is part of the flush burst too (MemoryStream + ToArray → LOH).
        GC.KeepAlive(inverted.Serialise());
        GC.KeepAlive(trigram.Serialise());
        GC.KeepAlive(bloom.Serialise());
    }

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
