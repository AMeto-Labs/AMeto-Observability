using Ameto.Metrics;
using Ameto.Metrics.Storage;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Storage.Tests;

/// <summary>
/// v3 .mts format: roundtrip fidelity, legacy-v2 readability, and the size win
/// over v2 on a corpus shaped like real OTel data (GUID-heavy label sets across
/// thousands of series, histograms with bucket counts).
/// </summary>
public sealed class MetricFormatV3Tests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mts-" + Guid.NewGuid().ToString("N"));

    public MetricFormatV3Tests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    // ── Corpus generation (deterministic) ─────────────────────────────────────

    private static readonly double[] Bounds =
        [0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];

    private static List<(SeriesKey, HotSeries)> HistogramCorpus(int seriesCount, int pointsPerSeries)
    {
        var rnd    = new Random(42);
        var routes = new[]
        {
            "console/api/{provider}/ProviderPayment/{providerPaymentId}",
            "api/payments/{id}/status", "api/kiosk/{kioskId}/heartbeat",
            "health", "metrics", "api/providers/{providerId}/balance",
        };
        var result = new List<(SeriesKey, HotSeries)>(seriesCount);
        for (int s = 0; s < seriesCount; s++)
        {
            var labels = new LabelSet(new Dictionary<string, string>
            {
                ["Environment"]                 = "Development",
                ["PR"]                          = "pr7170",
                ["service.name"]                = "MintRoute.API",
                ["service.instance.id"]         = new Guid(s, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).ToString(),
                ["http.route"]                  = routes[s % routes.Length],
                ["http.request.method"]         = s % 3 == 0 ? "GET" : "POST",
                ["http.response.status_code"]   = s % 7 == 0 ? "500" : "200",
                ["network.protocol.version"]    = "1.1",
                ["url.scheme"]                  = "http",
            });
            var pts = new List<MetricDataPoint>(pointsPerSeries);
            // Cumulative histogram, realistic idle-service shape: observations
            // arrive on ~15 % of exports and land in 1–3 adjacent buckets;
            // between them the cumulative state repeats unchanged.
            long ts = 1_784_800_000_000_000_000L + s * 1_000_000L;
            var cumulative = new long[Bounds.Length + 1];
            long cumCount = 0;
            double cumSum = 0;
            for (int p = 0; p < pointsPerSeries; p++)
            {
                ts += 60_000_000_000L + rnd.Next(-5000, 5000) * 1_000_000L;
                if (rnd.NextDouble() < 0.15)
                {
                    int hot = rnd.Next(2, 6); // typical latency bucket for this series
                    int n   = rnd.Next(1, 4);
                    for (int o = 0; o < n; o++)
                    {
                        int b = Math.Min(cumulative.Length - 1, hot + rnd.Next(0, 2));
                        cumulative[b]++;
                        cumCount++;
                        cumSum += Bounds[Math.Min(b, Bounds.Length - 1)];
                    }
                }
                pts.Add(new MetricDataPoint
                {
                    TimestampUnixNano = ts,
                    Value             = 0,
                    Count             = cumCount,
                    Sum               = cumSum,
                    BucketCounts      = (long[])cumulative.Clone(),
                });
            }
            result.Add((
                new SeriesKey("http.server.request.duration", MetricKind.Histogram, "s", labels),
                new HotSeries(pts, Bounds)));
        }
        return result;
    }

    private static List<(SeriesKey, HotSeries)> ScalarCorpus(int seriesCount, int pointsPerSeries)
    {
        var rnd    = new Random(7);
        var result = new List<(SeriesKey, HotSeries)>(seriesCount);
        for (int s = 0; s < seriesCount; s++)
        {
            var labels = new LabelSet(new Dictionary<string, string>
            {
                ["Environment"]         = "Development",
                ["PR"]                  = "pr7170",
                ["service.name"]        = "MintRoute.API",
                ["service.instance.id"] = new Guid(s, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0).ToString(),
            });
            var pts = new List<MetricDataPoint>(pointsPerSeries);
            long ts = 1_784_800_000_000_000_000L + s * 500_000L;
            long v  = 200_000_000 + s;
            for (int p = 0; p < pointsPerSeries; p++)
            {
                ts += 60_000_000_000L + rnd.Next(-5000, 5000) * 1_000_000L;
                v  += rnd.Next(0, 100_000);
                pts.Add(new MetricDataPoint { TimestampUnixNano = ts, Value = v });
            }
            result.Add((
                new SeriesKey("dotnet.gc.heap.total_allocated", MetricKind.Counter, "By", labels),
                new HotSeries(pts)));
        }
        return result;
    }

    // ── Roundtrip fidelity ────────────────────────────────────────────────────

    [Fact]
    public void V3_Roundtrip_PreservesEverything_ToMsPrecision()
    {
        var corpus = HistogramCorpus(50, 24).Concat(ScalarCorpus(50, 24)).ToList();
        var infos  = MetricWriter.Write(_dir, corpus, MetricGranularity.OneHour);
        Assert.Equal(2, infos.Count); // one file per metric name
        Assert.All(infos, i => Assert.Equal(3, i.FormatVersion));

        foreach (var info in infos)
        {
            var readBack = MetricReader.ReadAllSync(info.FilePath).ToList();
            var expected = corpus.Where(c => c.Item1.Name == info.MetricName).ToList();
            Assert.Equal(expected.Count, readBack.Count);

            for (int i = 0; i < expected.Count; i++)
            {
                var (key, hot) = expected[i];
                var got        = readBack[i];
                Assert.Equal(key.Kind, got.Kind);
                Assert.Equal(key.Unit, got.Unit);
                Assert.Equal(key.Labels, got.Labels);

                var pts = hot.GetPoints(long.MinValue, long.MaxValue);
                Assert.Equal(pts.Count, got.Points.Count);
                for (int p = 0; p < pts.Count; p++)
                {
                    // Timestamps survive to ms precision (the corpus is ms-aligned → exact).
                    Assert.Equal(pts[p].TimestampUnixNano / 1_000_000, got.Points[p].TimestampUnixNano / 1_000_000);
                    Assert.Equal(pts[p].Value, got.Points[p].Value);
                    Assert.Equal(pts[p].Count, got.Points[p].Count);
                    Assert.Equal(pts[p].Sum,   got.Points[p].Sum);
                    Assert.Equal(pts[p].BucketCounts ?? [], got.Points[p].BucketCounts ?? []);
                }
                if (key.Kind == MetricKind.Histogram)
                    Assert.Equal(Bounds, got.BucketBounds);
            }
        }
    }

    [Fact]
    public void V2_LegacyFiles_StillReadable()
    {
        var corpus = HistogramCorpus(20, 12).Concat(ScalarCorpus(20, 12)).ToList();
        foreach (var name in new[] { "http.server.request.duration", "dotnet.gc.heap.total_allocated" })
        {
            var items = corpus.Where(c => c.Item1.Name == name).ToList();
            var path  = Path.Combine(_dir, $"legacy-{name.Replace('.', '_')}.mts");
            WriteV2File(path, name, items);

            var info = MetricReader.ReadSegmentInfo(path);
            Assert.Equal(2, info.FormatVersion);
            Assert.Equal(name, info.MetricName);

            var readBack = MetricReader.ReadAllSync(path).ToList();
            Assert.Equal(items.Count, readBack.Count);
            for (int i = 0; i < items.Count; i++)
            {
                var pts = items[i].Item2.GetPoints(long.MinValue, long.MaxValue);
                Assert.Equal(items[i].Item1.Labels, readBack[i].Labels);
                Assert.Equal(pts.Count, readBack[i].Points.Count);
                // v2 keeps exact nano timestamps.
                Assert.Equal(pts[0].TimestampUnixNano, readBack[i].Points[0].TimestampUnixNano);
            }
        }
    }

    // ── Size comparison ───────────────────────────────────────────────────────

    [Fact]
    public void V3_IsMuchSmallerThanV2()
    {
        // Shape of the 1-h tier after merging: 7 days × 24 points per series.
        var histogram = HistogramCorpus(300, 168);
        var scalar    = ScalarCorpus(300, 168);

        long v2Size = 0, v3Size = 0;
        foreach (var (name, corpus) in new[]
        {
            ("http.server.request.duration", histogram),
            ("dotnet.gc.heap.total_allocated", scalar),
        })
        {
            var v2Path = Path.Combine(_dir, $"v2-{name.Replace('.', '_')}.mts");
            WriteV2File(v2Path, name, corpus);
            v2Size += new FileInfo(v2Path).Length;

            var infos = MetricWriter.Write(_dir, corpus, MetricGranularity.OneHour);
            v3Size += infos.Sum(i => new FileInfo(i.FilePath).Length);
            foreach (var i in infos) File.Delete(i.FilePath);
        }

        // The structural change (shared compression window + delta timestamps +
        // slim scalar points) must at least halve the on-disk size.
        Assert.True(v3Size * 2 <= v2Size,
            $"expected v3 ≤ ½ of v2, got v2={v2Size:N0} B vs v3={v3Size:N0} B");
    }

    /// <summary>
    /// The production pathology: before same-granularity merging, every rollup
    /// pass wrote one file per metric with ~1 point per series, repeating the
    /// full label sets each time. Compare 24 such v2 files against the single
    /// merged v3 file the compaction now produces.
    /// </summary>
    [Fact]
    public void MergedV3_CrushesThePerPassV2FilePileup()
    {
        var corpus = HistogramCorpus(300, 24);

        // 24 legacy files, one per "pass": each holds point p of every series.
        long pileupSize = 0;
        for (int p = 0; p < 24; p++)
        {
            var slice = corpus
                .Select(c =>
                {
                    var pt = c.Item2.GetPoints(long.MinValue, long.MaxValue)[p];
                    return (c.Item1, new HotSeries([pt], Bounds));
                })
                .ToList();
            var path = Path.Combine(_dir, $"pass-{p}.mts");
            WriteV2File(path, "http.server.request.duration", slice);
            pileupSize += new FileInfo(path).Length;
        }

        // One merged v3 file with the same data.
        var infos = MetricWriter.Write(_dir, corpus, MetricGranularity.OneHour);
        long mergedSize = infos.Sum(i => new FileInfo(i.FilePath).Length);

        Assert.True(mergedSize * 20 <= pileupSize,
            $"expected merged v3 ≤ 1/20 of the per-pass pileup, got pileup={pileupSize:N0} B vs merged={mergedSize:N0} B");
    }

    /// <summary>
    /// Full engine cycle: ingest histogram points → shutdown flush → cold read.
    /// Guards the flush regression where bucket bounds were dropped from the
    /// snapshot, silently breaking quantile/heatmap over anything cold.
    /// </summary>
    [Fact]
    public async Task EngineFlush_PreservesHistogramBounds()
    {
        var engine = new MetricStorageEngine(_dir,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<MetricStorageEngine>.Instance);

        var labels = new LabelSet(new Dictionary<string, string> { ["service.name"] = "T" });
        long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var items = new MetricIngestItem[3];
        for (int i = 0; i < items.Length; i++)
        {
            items[i] = new MetricIngestItem
            {
                Name              = "e2e.duration",
                Kind              = MetricKind.Histogram,
                Unit              = "s",
                Labels            = labels,
                TimestampUnixNano = ts + i * 60_000_000_000L,
                HistogramCount    = 2 + i,
                HistogramSum      = 0.5 * (i + 1),
                BucketBounds      = Bounds,
                BucketCounts      = Enumerable.Range(0, Bounds.Length + 1).Select(b => (long)(b == 3 ? 2 + i : 0)).ToArray(),
            };
        }
        engine.Ingest(items);
        await engine.DisposeAsync(); // final flush writes the cold file

        var file = Directory.EnumerateFiles(_dir, "*.mts").Single();
        var series = MetricReader.ReadAllSync(file).Single();
        Assert.Equal(Bounds, series.BucketBounds);          // ← the regression
        Assert.Equal(3, series.Points.Count);
        Assert.Equal(2, series.Points[0].Count);
        Assert.NotNull(series.Points[2].BucketCounts);
        Assert.Equal(4, series.Points[2].BucketCounts![3]);
    }

    // ── Legacy v2 writer (copied from the pre-v3 MetricWriter) ────────────────

    private static void WriteV2File(string filePath, string metricName, List<(SeriesKey, HotSeries)> items)
    {
        using var fs = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536);
        using var bw = new BinaryWriter(fs);

        long minNano = items.Min(i => i.Item2.GetPoints(long.MinValue, long.MaxValue)[0].TimestampUnixNano);
        long maxNano = items.Max(i => i.Item2.GetPoints(long.MinValue, long.MaxValue)[^1].TimestampUnixNano);

        bw.Write(0x52_44_4D_54u); // "RDMT"
        bw.Write((ushort)2);
        bw.Write((byte)MetricGranularity.OneHour);
        bw.Write((uint)items.Count);
        bw.Write(minNano);
        bw.Write(maxNano);
        bw.Write((byte)0);

        var blockOffsets = new List<long>(items.Count);
        foreach (var (key, hs) in items)
        {
            var buf = new System.Buffers.ArrayBufferWriter<byte>();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(6);
            w.Write("k");   w.Write((byte)key.Kind);
            w.Write("u");   w.Write(key.Unit);
            w.Write("lbs");
            var pairs = key.Labels.Pairs;
            w.WriteMapHeader(pairs.Count);
            foreach (var (k, v) in pairs) { w.Write(k); w.Write(v); }
            w.Write("bnds");
            var bounds = hs.Bounds;
            w.WriteArrayHeader(bounds?.Length ?? 0);
            if (bounds is not null) foreach (var b in bounds) w.Write(b);
            var pts = hs.GetPoints(long.MinValue, long.MaxValue);
            w.Write("pts");
            w.WriteArrayHeader(pts.Count);
            foreach (var p in pts)
            {
                w.WriteArrayHeader(5);
                w.Write(p.TimestampUnixNano);
                w.Write(p.Value);
                w.Write(p.Count);
                w.Write(p.Sum);
                if (p.BucketCounts is { Length: > 0 } bc)
                {
                    w.WriteArrayHeader(bc.Length);
                    foreach (var c in bc) w.Write(c);
                }
                else w.WriteNil();
            }
            w.Write("cnt"); w.Write((uint)pts.Count);
            w.Flush();

            var raw        = buf.WrittenSpan.ToArray();
            var compressed = LZ4Pickler.Pickle(raw);
            blockOffsets.Add(fs.Position);
            bw.Write((uint)raw.Length);
            bw.Write((uint)compressed.Length);
            bw.Write(compressed);
        }

        long nameIdxOffset = fs.Position;
        bw.Write((uint)1);
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(metricName);
        bw.Write((ushort)nameBytes.Length);
        bw.Write(nameBytes);
        bw.Write((ulong)blockOffsets[0]);
        bw.Write((uint)blockOffsets.Count);
        bw.Write((ulong)nameIdxOffset);
        bw.Write(0x52_44_4D_46u); // "RDMF"
    }
}
