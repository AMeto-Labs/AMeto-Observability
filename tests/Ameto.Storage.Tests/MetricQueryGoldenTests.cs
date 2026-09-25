using System.Globalization;
using System.Security.Cryptography;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using static Ameto.Storage.Tests.MetricGolden;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A COLD READ AND AN ENGINE QUERY ANSWER, PINNED BEFORE THE READER IS REWRITTEN (issue #83
/// WP7, M#4(a)(b)(c) and the WP6 interning follow-up). Captured on the unchanged code
/// (<c>db5cdd1</c>).
///
/// <para>The reader is about to stop copying every series (range pushdown), stop decoding map
/// keys into strings, resolve its labels through the shared interner, and match label filters
/// with a scan instead of a dictionary per series. Each of those is a place an answer can
/// quietly move: an inclusive range bound, a histogram's reconstructed zero buckets past a
/// pushed-down range, a '|' option list with an empty option, a key that differs only in case,
/// a v2 file. The goldens hash every bit of every answer; the facts below them state the edge
/// behaviours in words. One was a latent bug kept on purpose by the rewrite — a repeated label key
/// threw, exactly as <c>ToDictionary</c> did — and is fixed since (#92): such a key is matched on
/// its ordinal-greatest value, the last of its sorted run.</para>
/// </summary>
public sealed class MetricQueryGoldenTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mqgold-" + Guid.NewGuid().ToString("N"));

    public MetricQueryGoldenTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long S  = 1_000_000_000L;
    private const long Ms = 1_000_000L;
    private const long T0 = 1_784_800_020_000_000_000L;

    private static readonly double[] Bounds = [0.005, 0.01, 0.05, 0.1, 0.5, 1];

    private static LabelSet Labels(int s) => new(new Dictionary<string, string>
    {
        ["service.name"] = s % 4 == 0 ? "Golden.API" : "Other.API",
        ["route"]        = s % 9 == 0 ? "" : "/api/r" + (s % 5),
        ["replica"]      = s.ToString(CultureInfo.InvariantCulture),
    });

    /// <summary>
    /// A cumulative histogram with idle stretches (slim points on disk — the reader rebuilds their
    /// zero bucket arrays), a gauge, and the same shapes in a legacy v2 file. Point timestamps
    /// are whole milliseconds so v3's ms precision loses nothing a range test could trip on.
    /// </summary>
    private (List<MetricSegmentInfo> Files, string Hist, string Gauge) WriteCorpus(string dir, long t0, ulong seed)
    {
        var rng = new GoldenRng(seed);
        const string hist = "golden.q.hist", gauge = "golden.q.gauge";

        List<MetricDataPoint> Hist(int s)
        {
            var pts = new List<MetricDataPoint>();
            var cum = new long[Bounds.Length + 1];
            long count = 0; double sum = 0;
            long ts = t0 + (s % 3) * Ms;
            int n = 4 + rng.Next(20);
            for (int i = 0; i < n; i++)
            {
                ts += (10 + rng.Next(50)) * S;
                if (i > 0 && rng.Chance(35))
                {
                    int b = rng.Next(cum.Length);
                    cum[b] += 1 + rng.Next(3);
                    count  += 2;
                    sum    += 0.125 * (b + 1);
                }
                // Idle points (count 0, sum 0, all-zero buckets) are what v3 stores slim and the
                // reader re-expands; a leading run of them is the case a range cut can split.
                pts.Add(P(ts, count > 0 ? sum / count : 0, count: count, sum: sum,
                          buckets: count == 0 && rng.Chance(50) ? null : (long[])cum.Clone()));
            }
            return pts;
        }

        List<MetricDataPoint> Gauge(int s)
        {
            var pts = new List<MetricDataPoint>();
            long ts = t0 + (s % 5) * Ms;
            int n = 1 + rng.Next(25);
            for (int i = 0; i < n; i++)
            {
                ts += (rng.Chance(10) ? 0 : 15) * S;
                pts.Add(P(ts, rng.Chance(5) ? -0.0 : (rng.NextDouble() - 0.5) * 1e4));
            }
            return pts;
        }

        var files = new List<MetricSegmentInfo>();
        var hItems = new List<(SeriesKey, HotSeries)>();
        var gItems = new List<(SeriesKey, HotSeries)>();
        for (int s = 0; s < 60; s++)
        {
            hItems.Add((new SeriesKey(hist, MetricKind.Histogram, "s", Labels(s)), new HotSeries(Hist(s), Bounds)));
            gItems.Add((new SeriesKey(gauge, MetricKind.Gauge, "By", Labels(s)), new HotSeries(Gauge(s))));
        }
        files.AddRange(MetricWriter.Write(dir, hItems, MetricGranularity.Raw));
        files.AddRange(MetricWriter.Write(dir, gItems, MetricGranularity.Raw));

        var v2 = new List<(SeriesKey, List<MetricDataPoint>, double[]?)>();
        for (int s = 100; s < 130; s++)
            v2.Add((new SeriesKey(hist, MetricKind.Histogram, "s", Labels(s)), Hist(s), Bounds));
        files.Add(WriteV2File(Path.Combine(dir, $"metrics-v2-{t0}.mts"), hist, MetricGranularity.Raw, v2));
        return (files, hist, gauge);
    }

    private static readonly IReadOnlyDictionary<string, string>?[] Matchers =
    [
        null,
        new Dictionary<string, string>(),
        new Dictionary<string, string> { ["route"] = "/api/r1" },
        new Dictionary<string, string> { ["route"] = "/api/r1|/api/r3" },
        new Dictionary<string, string> { ["route"] = "|/api/r2" },            // an empty option matches the empty value
        new Dictionary<string, string> { ["route"] = "/api/r1|" },
        new Dictionary<string, string> { ["route"] = "" },
        new Dictionary<string, string> { ["Route"] = "/api/r1" },            // keys are ordinal: no match
        new Dictionary<string, string> { ["missing"] = "x" },
        new Dictionary<string, string> { ["service.name"] = "Golden.API", ["route"] = "/api/r0|/api/r4|" },
        new Dictionary<string, string> { ["replica"] = "7" },
    ];

    private static (long From, long To)[] Ranges(long t0) =>
    [
        (long.MinValue, long.MaxValue),
        (t0, t0 + 120 * S),
        (t0 + 60 * S + 1 * Ms, t0 + 300 * S + 2 * Ms),     // the corpus' ms offsets land on these edges
        (t0 + 200 * S, t0 + 200 * S),
        (t0 + 500 * S, t0 + 100 * S),                      // inverted: empty
        (t0 + 10_000 * S, long.MaxValue),                  // after everything
    ];

    [Fact]
    public async Task A_cold_read_answers_what_it_answered()
    {
        var (files, hist, gauge) = WriteCorpus(_dir, T0, 0xC01D_5EED);

        using var h = NewHash();
        foreach (var file in files)
        {
            // The full decode, as the rollup and the catalog seed see it.
            foreach (var s in MetricReader.ReadAllSync(file.FilePath)) Add(h, s);

            foreach (var name in new[] { hist, gauge, hist.ToUpperInvariant(), "golden.q.none" })
                foreach (var (from, to) in Ranges(T0))
                    foreach (var m in Matchers)
                    {
                        Add(h, from); Add(h, to);
                        await foreach (var s in MetricReader.ReadAsync(file.FilePath, name, from, to, m, CancellationToken.None))
                            Add(h, s);
                    }
        }
        Assert.Equal("E87A9649D1806AFF", Finish(h));
    }

    private static readonly TimeSpan?[] Steps =
        [null, TimeSpan.FromSeconds(15), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5), TimeSpan.FromHours(1)];

    /// <summary>Every (name, range, step, matchers) query, each answer handed to <paramref name="add"/>.</summary>
    private static async Task QueryAll(MetricStorageEngine engine, IncrementalHash h,
                                       Func<IAsyncEnumerable<MetricSeries>, Task> add)
    {
        foreach (var name in new[] { "golden.q.hist", "GOLDEN.Q.GAUGE", "golden.q.none" })
            foreach (var (from, to) in Ranges(T0))
                foreach (var step in Steps)
                    foreach (var m in Matchers)
                    {
                        DateTimeOffset? f = from == long.MinValue ? null : DateTimeOffset.FromUnixTimeMilliseconds(from / Ms);
                        DateTimeOffset? t = to   == long.MaxValue ? null : DateTimeOffset.FromUnixTimeMilliseconds(to / Ms);
                        Add(h, from); Add(h, to); Add(h, step?.Ticks ?? -1);
                        await add(engine.QueryAsync(name, f, t, step, m));
                    }

        foreach (var m in Matchers)
            await add(engine.GetLatestAsync("golden.q.gauge", m));
    }

    [Fact]
    public async Task An_engine_query_answers_what_it_answered_from_cold_files()
    {
        // Each corpus has its own time base, so the catalog's file order — and so the ORDER the
        // fragments come back in, which the aggregator's tie-break depends on — is the same on
        // every run. Ordered hash.
        WriteCorpus(_dir, T0, 0x0001);
        WriteCorpus(_dir, T0 + 3_600 * S, 0x0002);

        await using var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance,
                                                         new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });
        await engine.ColdLoadCompleted;

        using var h = NewHash();
        await QueryAll(engine, h, async q => { await foreach (var s in q) Add(h, s); });
        Assert.Equal("A3F9774CA790F32F", Finish(h));
    }

    [Fact]
    public async Task An_engine_query_answers_what_it_answered_from_the_hot_tier()
    {
        await using var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance,
                                                         new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });

        var rng   = new GoldenRng(0xB0_7B_07);
        var batch = new List<MetricIngestItem>();
        for (int s = 0; s < 40; s++)
        {
            long ts = T0 + 30 * S + (s % 7) * Ms;
            for (int i = 0; i < 12; i++)
            {
                ts += rng.Chance(10) ? -20 * S : 15 * S;          // an out-of-order append now and then
                batch.Add(new MetricIngestItem
                {
                    Name = s % 2 == 0 ? "golden.q.gauge" : "golden.q.hist",
                    Kind = s % 2 == 0 ? MetricKind.Gauge : MetricKind.Histogram,
                    Unit = s % 2 == 0 ? "By" : "s",
                    Labels = Labels(s),
                    TimestampUnixNano = ts,
                    ScalarValue    = (rng.NextDouble() - 0.5) * 100,
                    HistogramCount = s % 2 == 0 ? 0 : i * 3,
                    HistogramSum   = s % 2 == 0 ? 0 : i * 0.75,
                    BucketBounds   = s % 2 == 0 ? null : Bounds,
                    BucketCounts   = s % 2 == 0 ? null : [i, 0, i, 0, 0, i, 0],
                });
            }
        }
        engine.Ingest(batch.ToArray());

        // The hot tier answers in its concurrent dictionary's order, which follows the label sets'
        // hashes — randomly seeded per process. So each answer is hashed as a SET of series: what
        // is in it is pinned, the order it came in cannot be.
        using var h = NewHash();
        await QueryAll(engine, h, async q =>
        {
            var each = new List<string>();
            await foreach (var s in q)
            {
                using var one = NewHash();
                Add(one, s);
                each.Add(Finish(one));
            }
            each.Sort(StringComparer.Ordinal);
            foreach (var x in each) Add(h, x);
        });
        Assert.Equal("577180AA7A46C4CF", Finish(h));
    }

    // ── A repeated label key: matched on its ordinal-greatest value (#92) ─────
    //
    // The latent bug this class used to pin — kept by the rewrite, not fixed by it — is fixed now:
    // a set with a key twice (stored before ingest collapsed repeats) failed ANY filtered read with
    // the ArgumentException ToDictionary threw. It is matched instead, on the value the answer
    // writes for the key: the last of its run, which is its ORDINAL-GREATEST value ("v2") — not
    // necessarily the one sent last, whose order was never stored.

    private static LabelSet Duplicated() =>
        new([new("k", "v1"), new("k", "v2"), new("z", "1")]);

    private static IEnumerable<(Dictionary<string, string> Matchers, bool Matches)> DupFilters() =>
    [
        (new Dictionary<string, string>(),                              true),
        (new Dictionary<string, string> { ["z"] = "1" },                true),
        (new Dictionary<string, string> { ["k"] = "v2" },               true),
        (new Dictionary<string, string> { ["k"] = "v1" },               false),   // not the value written
        (new Dictionary<string, string> { ["k"] = "v1|v2" },            true),
        (new Dictionary<string, string> { ["k"] = "v2", ["z"] = "2" },  false),
    ];

    [Fact]
    public async Task A_cold_series_with_a_repeated_key_is_matched_on_the_last_value_of_its_run()
    {
        var items = new List<(SeriesKey, HotSeries)>
        {
            (new SeriesKey("golden.dup", MetricKind.Gauge, "", Duplicated()), new HotSeries([P(T0, 1)])),
        };
        var file = Assert.Single(MetricWriter.Write(_dir, items, MetricGranularity.Raw)).FilePath;

        // No matchers at all: the series comes back, both pairs intact — the stored set is not rewritten.
        var got = new List<MetricSeries>();
        await foreach (var s in MetricReader.ReadAsync(file, "golden.dup", long.MinValue, long.MaxValue, null, CancellationToken.None))
            got.Add(s);
        Assert.Equal(2 * 3, Assert.Single(got).Labels.Interleaved.Length);

        foreach (var (m, matches) in DupFilters())
        {
            int n = 0;
            await foreach (var _ in MetricReader.ReadAsync(file, "golden.dup", long.MinValue, long.MaxValue, m, CancellationToken.None)) n++;
            Assert.Equal(matches ? 1 : 0, n);
        }
    }

    /// <summary>
    /// API.md's example, pinned: a series stored with a point <c>service.name=api</c> under a resource
    /// <c>service.name=gateway</c> answers to <c>gateway</c> — the greater — though today's ingest
    /// would keep <c>api</c> (a point attribute wins).
    /// </summary>
    [Fact]
    public async Task A_stored_series_answers_to_its_ordinal_greatest_value_not_the_one_ingest_keeps_today()
    {
        await using var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance,
                                                         new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });
        engine.Ingest([new MetricIngestItem { Name = "golden.svc", Kind = MetricKind.Gauge,
                                              Labels = new([new("service.name", "api"), new("service.name", "gateway")]),
                                              TimestampUnixNano = T0, ScalarValue = 1 }]);

        int gateway = 0, api = 0;
        await foreach (var _ in engine.QueryAsync("golden.svc", labelMatchers: new Dictionary<string, string> { ["service.name"] = "gateway" })) gateway++;
        await foreach (var _ in engine.QueryAsync("golden.svc", labelMatchers: new Dictionary<string, string> { ["service.name"] = "api" })) api++;
        Assert.Equal((1, 0), (gateway, api));
    }

    [Fact]
    public async Task A_hot_series_with_a_repeated_key_is_matched_on_the_last_value_of_its_run()
    {
        await using var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance,
                                                         new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });
        engine.Ingest([new MetricIngestItem { Name = "golden.dup", Kind = MetricKind.Gauge, Labels = Duplicated(),
                                              TimestampUnixNano = T0, ScalarValue = 1 }]);

        int unfiltered = 0;
        await foreach (var _ in engine.QueryAsync("golden.dup", labelMatchers: null)) unfiltered++;
        Assert.Equal(1, unfiltered);

        foreach (var (m, matches) in DupFilters())
        {
            int n = 0, latest = 0;
            await foreach (var _ in engine.QueryAsync("golden.dup", labelMatchers: m)) n++;
            await foreach (var _ in engine.GetLatestAsync("golden.dup", m)) latest++;
            Assert.Equal(matches ? 1 : 0, n);
            Assert.Equal(matches ? 1 : 0, latest);
        }
    }
}
