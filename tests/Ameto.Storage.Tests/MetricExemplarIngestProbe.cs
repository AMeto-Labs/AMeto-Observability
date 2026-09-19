using System.Buffers;
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

    /// <summary>
    /// WHAT THE EXEMPLAR HANDOVER ARRAY COSTS TO GIVE BACK.
    ///
    /// <para>The ingest loop rents a <c>HotSeries?[]</c> of <c>items.Length</c> to hand the
    /// exemplar pass the series it already resolved, and gave it back with
    /// <c>Return(resolved, clearArray: true)</c>. <see cref="ArrayPool{T}"/> rounds a rent up to
    /// a power of two, so a 10 000-point batch is handed 16 384 slots and that return memset
    /// ALL of them — 128 KB of writes per exemplar-carrying batch, to null a handful of
    /// references and then re-null 16 000 slots that were already null.</para>
    ///
    /// <para>The guarantee the memset was there for is narrower than the memset: nothing THIS
    /// BATCH wrote may outlive it, because a <c>HotSeries</c> left in a pooled slot keeps a
    /// series — its points, its label set, its catalog entry — alive for as long as the pool
    /// holds the array, long after a stale sweep evicted it. Nothing is said about slots this
    /// batch never touched; they are their previous owner's business and were dirty when it
    /// arrived, which is what a pool means.</para>
    ///
    /// <para>BOTH HALVES ARE ASSERTED, and the pool is seeded with a sentinel so that they can
    /// be told apart: every ordinal the batch wrote comes back null, and an ordinal it never
    /// touched still holds the sentinel. Restore <c>clearArray: true</c> and the second half
    /// fails — the sentinel is gone, which is the 16 384-slot memset showing itself.</para>
    /// </summary>
    [Fact]
    public async Task The_exemplar_handover_array_is_returned_clear_of_its_own_writes_only()
    {
        const int Items = 10;   // rents a 16-slot array, so there are untouched slots to see

        string root = Path.Combine(Path.GetTempPath(), "ameto-mexret-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            await using var engine = Open(root, "handover");

            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L - 60_000_000_000L;
            var  items    = new MetricIngestItem[Items];
            for (int i = 0; i < Items; i++)
                items[i] = new MetricIngestItem
                {
                    Name              = "handover.metric",
                    Unit              = "ms",
                    Kind              = MetricKind.Gauge,
                    Labels            = new LabelSet([new("series", "s" + i)]),
                    TimestampUnixNano = baseNano + i,
                    ScalarValue       = i,
                    // Ordinals 0 and 2 only: the written set has to be a strict subset of the
                    // batch, and the batch a strict subset of the rented array.
                    Exemplars         = i is 0 or 2
                        ? [new MetricExemplar
                           {
                               TimestampUnixNano = baseNano + i,
                               Value             = i,
                               TraceId           = "4bf92f3577b34da6a3ce929d0e0e4736",
                               SpanId            = "00f067aa0ba902b7",
                           }]
                        : null,
                };

            // SEED THE POOL, so what the return did is legible. Everything below runs on this
            // thread with no await in between, which is what makes the shared pool hand the same
            // array back: Return parks it in this thread's slot for the bucket, and the next
            // Rent of the same bucket on this thread takes it from there.
            var sentinel = new HotSeries(new LabelSet([new("sentinel", "yes")]));
            var seeded   = ArrayPool<HotSeries?>.Shared.Rent(Items);
            seeded.AsSpan().Fill(sentinel);
            ArrayPool<HotSeries?>.Shared.Return(seeded, clearArray: false);

            engine.Ingest(items);

            var back = ArrayPool<HotSeries?>.Shared.Rent(Items);
            try
            {
                Assert.Same(seeded, back);   // otherwise the two halves below are about two arrays

                _out.WriteLine($"EXEMPLAR HANDOVER  batch of {Items} items -> a {back.Length}-slot rented array, "
                             + "2 ordinals written");

                // Half one: nothing this batch wrote survived it.
                Assert.Null(back[0]);
                Assert.Null(back[2]);

                // Half two: and nothing else was touched. The slots past the batch never belonged
                // to it, and the memset that reached them is what this item removed.
                for (int i = Items; i < back.Length; i++)
                    Assert.True(ReferenceEquals(sentinel, back[i]),
                        $"slot {i} of a {back.Length}-slot array was cleared by a {Items}-item batch — "
                      + "the return is still memsetting the whole rented array");
            }
            finally { ArrayPool<HotSeries?>.Shared.Return(back, clearArray: true); }
        }
        finally { try { Directory.Delete(root, true); } catch { } }
    }

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
