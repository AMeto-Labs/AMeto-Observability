using System.Diagnostics;

using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Concurrent metric ingest, which is how it actually happens: every Kestrel thread
/// handling an OTLP POST calls Ingest at once. A CPU profile of the sandbox stand put
/// Monitor.Enter_Slowpath under MetricStorageEngine.Ingest in the top stacks — the
/// per-point cardinality bookkeeping was serialising all of them on one lock per metric
/// name. This measures the path under that contention.
///
/// <para><b>It sweeps 1/2/4/8 threads and reports PER-THREAD ns/point.</b> The single
/// 4-thread figure it used to print hid the only thing worth knowing: aggregate throughput
/// is flat from 1 to 8 threads, so the per-thread cost rises linearly with the thread count
/// and every core past the first buys nothing. One number cannot show that; a sweep of
/// per-thread numbers is the whole measurement. Each thread times ITS OWN work and counts
/// ITS OWN points — a wall clock over a <c>Parallel.For</c> divided by the total says only
/// what the slowest thread did.</para>
/// </summary>
public sealed class MetricIngestContentionProbe
{
    private readonly ITestOutputHelper _out;
    public MetricIngestContentionProbe(ITestOutputHelper o) => _out = o;

    // The steady state that matters: a small set of instruments, each with many series, all
    // re-sent every export interval — so every point hits an ALREADY KNOWN series and pays
    // only the bookkeeping, not the insert.
    private const int Names = 8, SeriesPerName = 250, Rounds = 12;
    private static readonly int[] ThreadCounts = [1, 2, 4, 8];

    [Fact]
    public void ConcurrentIngestThroughput()
    {
        _out.WriteLine($"{Names} instruments x {SeriesPerName} known series, {Rounds} rounds per thread");
        _out.WriteLine("threads | per-thread ns/point | total k points/s | scaling vs 1 thread");

        double oneThreadRate = 0;

        foreach (int threads in ThreadCounts)
        {
            var (nsPerPointPerThread, totalRate) = RunSweepPoint(threads);
            if (threads == 1) oneThreadRate = totalRate;

            _out.WriteLine($"{threads,7} | {nsPerPointPerThread,19:F0} | {totalRate / 1000.0,16:F0} "
                         + $"| {(oneThreadRate > 0 ? totalRate / oneThreadRate : 1),8:F2}x");
        }
    }

    /// <summary>
    /// One point of the sweep, on its own engine and its own directory: a shared engine would
    /// carry the previous point's WAL capacity, series registry and hot-tier fill into the next
    /// one and the numbers would drift with the sweep order, not with the thread count.
    /// </summary>
    private static (double NsPerPointPerThread, double TotalPointsPerSecond) RunSweepPoint(int threads)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mcontention-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var engine  = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            var batches = BuildBatches(threads);

            foreach (var b in batches) engine.Ingest(b);          // warm: register every series

            // PER-THREAD counters. Each worker stops its own clock; the aggregate rate comes
            // from the wall clock around the whole set, which is what a caller would see.
            var elapsedTicks = new long[threads];
            var workers      = new Thread[threads];
            using var start  = new Barrier(threads + 1);

            for (int t = 0; t < threads; t++)
            {
                int me = t;
                workers[t] = new Thread(() =>
                {
                    start.SignalAndWait();
                    var mine = Stopwatch.StartNew();
                    for (int r = 0; r < Rounds; r++) engine.Ingest(batches[me]);
                    mine.Stop();
                    elapsedTicks[me] = mine.ElapsedTicks;
                }) { IsBackground = true };
                workers[t].Start();
            }

            var wall = Stopwatch.StartNew();
            start.SignalAndWait();
            for (int t = 0; t < threads; t++) workers[t].Join();
            wall.Stop();

            long pointsPerThread = (long)Rounds * Names * SeriesPerName;
            long totalPoints     = pointsPerThread * threads;

            double sumNsPerPoint = 0;
            for (int t = 0; t < threads; t++)
            {
                double ms = elapsedTicks[t] * 1000.0 / Stopwatch.Frequency;
                sumNsPerPoint += ms * 1_000_000.0 / pointsPerThread;
            }

            engine.DisposeAsync().AsTask().GetAwaiter().GetResult();

            return (sumNsPerPoint / threads, totalPoints / wall.Elapsed.TotalSeconds);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    private static MetricIngestItem[][] BuildBatches(int threads)
    {
        var batches = new MetricIngestItem[threads][];
        for (int t = 0; t < threads; t++)
        {
            var items = new MetricIngestItem[Names * SeriesPerName];
            int i = 0;
            for (int n = 0; n < Names; n++)
            for (int s = 0; s < SeriesPerName; s++)
                items[i++] = new MetricIngestItem
                {
                    Name   = $"http.server.request.duration.{n}",
                    Unit   = "ms",
                    Kind   = MetricKind.Gauge,
                    Labels = new LabelSet(
                    [
                        new("service.name", "Etisalat.API"),
                        new("http.route",   $"/api/v1/resource/{s % 25}"),
                        new("http.request.method", s % 2 == 0 ? "GET" : "POST"),
                        new("server.address", $"node-{s % 3}"),
                    ]),
                    TimestampUnixNano = 1_785_300_000_000_000_000L,
                    ScalarValue       = s,
                };
            batches[t] = items;
        }
        return batches;
    }
}
