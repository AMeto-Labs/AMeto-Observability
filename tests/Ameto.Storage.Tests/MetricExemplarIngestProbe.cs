using System.Diagnostics;

using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT THE EXEMPLAR PASS COSTS THE INGEST PATH, PER ITEM — the
/// <c>MetricIngestContentionProbe</c> workload (an OTLP export re-sending a small set of
/// instruments with many series each, so every point lands on an ALREADY KNOWN series and pays
/// only bookkeeping) with an exemplar arm beside the bare one.
///
/// <para>The pass runs outside <c>_snapshotLock</c> and so cannot be folded into the ingest
/// loop; it re-walks the batch, which is cheap. What is not obviously cheap is that it used to
/// re-RESOLVE THE SERIES: the ring files the series' canonical <c>LabelSet</c>, and
/// <c>AddExemplars</c> got it from a second <c>_hot.TryGetValue</c> on a freshly built
/// <c>SeriesKey</c> — an uncached record-struct hash (a string hash of the name, a string hash
/// of the unit, the label set's cached one), a bucket probe and a <c>LabelSet.Equals</c>, all of
/// which <c>ApplyToHotTier</c> had paid for the SAME item one pass earlier.</para>
///
/// <para><b>Single-threaded and min-of-N on purpose.</b> The first shape of this probe ran the
/// contention probe's four threads, where the lock the two arms share swamps the difference: the
/// same build measured the pass at +179, -129, -182 and -45 ns an item on four consecutive runs.
/// One thread, nine alternating passes and the minimum of each arm — the pass least interrupted
/// by the scheduler and the GC — put the run-to-run spread under 5 ns.</para>
///
/// <para>One exemplar an item, which is the ratio that isolates the per-ITEM lookup: ten
/// exemplars an item would bury it under ten ring locks. The arms differ by the WHOLE pass —
/// the walk, the lookup, the ring lock and the sample copy — so the lookup's share shows as a
/// change in that difference between two builds of the engine.</para>
///
/// <para><b>What it found</b>, Release, three runs each: with the second lookup the pass cost
/// 118.9 / 126.3 / 117.8 ns an item over a bare path of 487.7 / 508.4 / 505.6; with the series
/// handed over from the ingest loop it costs 34.2 / 54.6 / 37.4 over 531.2 / 474.9 / 522.5. The
/// lookup was about 80 ns of the ~121 ns pass — two thirds of it, and 16 % of the whole ingest
/// path for an exemplar-carrying item — so it was worth not paying twice.</para>
/// </summary>
public sealed class MetricExemplarIngestProbe
{
    private const long MB = 1024 * 1024;

    private const int Names = 8, SeriesPerName = 250, RoundsPerPass = 5, Passes = 9;
    private const int ItemsPerRound = Names * SeriesPerName;

    private readonly ITestOutputHelper _out;
    public MetricExemplarIngestProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public async Task The_exemplar_pass_costs_what_it_costs_per_item()
    {
        string root = Path.Combine(Path.GetTempPath(), "ameto-mexprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            await using var bare     = Open(root, "bare");
            await using var exemplar = Open(root, "exemplar");

            var bareItems = Batch(withExemplars: false);
            var exItems   = Batch(withExemplars: true);

            // Register every series, every ring and every JIT stub before anything is timed.
            for (int i = 0; i < 3; i++) { bare.Ingest(bareItems); exemplar.Ingest(exItems); }

            double bareNs = double.MaxValue, exNs = double.MaxValue;
            for (int p = 0; p < Passes; p++)
            {
                // Alternated, so a machine that drifts over the run drifts under both arms.
                bareNs = Math.Min(bareNs, TimeOnePass(bare,     bareItems));
                exNs   = Math.Min(exNs,   TimeOnePass(exemplar, exItems));
            }

            _out.WriteLine($"1 thread, {Passes} passes x {RoundsPerPass} rounds x {ItemsPerRound:N0} known series, "
                         + "min of the passes");
            _out.WriteLine($"no exemplars      : {bareNs,6:F1} ns/item");
            _out.WriteLine($"one exemplar each : {exNs,6:F1} ns/item");
            _out.WriteLine($"the exemplar pass : {exNs - bareNs,6:F1} ns/item "
                         + $"({(exNs - bareNs) / bareNs:P0} of the bare path)");

            // Not a threshold — a measurement. The only thing worth asserting is that both arms
            // really ran, so the numbers above are of an engine that did the work.
            Assert.True(bareNs > 0 && exNs > 0 && exNs < double.MaxValue);
        }
        finally { try { Directory.Delete(root, true); } catch { } }
    }

    private static double TimeOnePass(MetricStorageEngine engine, MetricIngestItem[] items)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();

        var sw = Stopwatch.StartNew();
        for (int r = 0; r < RoundsPerPass; r++) engine.Ingest(items);
        sw.Stop();

        return sw.Elapsed.TotalMilliseconds * 1_000_000.0 / (RoundsPerPass * (long)ItemsPerRound);
    }

    /// <summary>
    /// A tier far larger than the run needs and a log that never has to double: a threshold
    /// flush or a remap inside a timed pass is the one thing that would make the arms
    /// incomparable.
    /// </summary>
    private static MetricStorageEngine Open(string root, string name) =>
        new(Path.Combine(root, name), NullLogger<MetricStorageEngine>.Instance, new MetricsOptions
        {
            HotTierBytes       = 256 * MB,
            WalInitialBytes    =  64 * MB,
            ExemplarsPerMetric = 256,
        });

    private static MetricIngestItem[] Batch(bool withExemplars)
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L - 60_000_000_000L;

        var items = new MetricIngestItem[ItemsPerRound];
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
                    new("service.name",        "Etisalat.API"),
                    new("http.route",          $"/api/v1/resource/{s % 25}"),
                    new("http.request.method", s % 2 == 0 ? "GET" : "POST"),
                    new("server.address",      $"node-{s % 3}"),
                ]),
                TimestampUnixNano = baseNano,
                ScalarValue       = s,
                Exemplars         = withExemplars
                    ? [new MetricExemplar
                       {
                           TimestampUnixNano = baseNano,
                           Value             = s,
                           TraceId           = "4bf92f3577b34da6a3ce929d0e0e4736",
                           SpanId            = "00f067aa0ba902b7",
                       }]
                    : null,
            };
        return items;
    }
}
