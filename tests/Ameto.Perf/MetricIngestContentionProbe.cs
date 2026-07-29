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
/// </summary>
public sealed class MetricIngestContentionProbe
{
    private readonly ITestOutputHelper _out;
    public MetricIngestContentionProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void ConcurrentIngestThroughput()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mcontention-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);

            // The steady state that matters: a small set of instruments, each with many
            // series, all re-sent every export interval — so every point hits an ALREADY
            // KNOWN series and pays only the bookkeeping, not the insert.
            const int names = 8, seriesPerName = 250, threads = 4, rounds = 12;
            var batches = new MetricIngestItem[threads][];
            for (int t = 0; t < threads; t++)
            {
                var items = new MetricIngestItem[names * seriesPerName];
                int i = 0;
                for (int n = 0; n < names; n++)
                for (int s = 0; s < seriesPerName; s++)
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

            foreach (var b in batches) engine.Ingest(b);          // warm: register every series

            var sw = Stopwatch.StartNew();
            Parallel.For(0, threads, t =>
            {
                for (int r = 0; r < rounds; r++) engine.Ingest(batches[t]);
            });
            sw.Stop();

            long points = (long)threads * rounds * names * seriesPerName;
            double nsPerPoint = sw.Elapsed.TotalMilliseconds * 1_000_000.0 / points;

            _out.WriteLine($"{threads} threads x {rounds} rounds x {names * seriesPerName} known series");
            _out.WriteLine($"{points:N0} points in {sw.Elapsed.TotalMilliseconds:F0} ms");
            _out.WriteLine($"{nsPerPoint:F0} ns/point | {points / sw.Elapsed.TotalSeconds / 1000.0:F0} k points/s across {threads} threads");

            engine.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }
}
