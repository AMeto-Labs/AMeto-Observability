using System.Globalization;
using System.Text;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using static Ameto.Storage.Tests.MetricGolden;

namespace Ameto.Storage.Tests;

/// <summary>
/// The catalog a start seeds from the cold <c>.mts</c> files (#94). The seed decoded every point
/// of every file to learn each series' last timestamp; it now reads the series' identities only
/// and takes the time from the file's header. What it builds must be what the full decode built:
/// every entry's kind, unit, cardinality, last-seen millisecond, label keys and label values —
/// including the order-dependent parts (the last kind and the last non-empty unit win, the first
/// values up to the per-key cap and the first label sets up to the series cap are kept).
/// </summary>
public sealed class MetricCatalogSeedTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mseed-" + Guid.NewGuid().ToString("N"));

    public MetricCatalogSeedTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long S = 1_000_000_000L;
    private static readonly long T0 = DateTimeOffset.UtcNow.AddHours(-2).ToUnixTimeMilliseconds() * 1_000_000L;

    /// <summary>Caps small enough that the corpus overflows both, so the order they keep is tested.</summary>
    private static readonly MetricsOptions Caps = new() { MaxLabelValuesPerKey = 5, MaxTrackedSeriesPerMetric = 7 };

    private static LabelSet Labels(int s, int file) => new(new Dictionary<string, string>
    {
        ["service.name"] = "svc-" + (s % 4).ToString(CultureInfo.InvariantCulture),
        ["pod"]          = "pod-" + ((s * 7 + file) % 11).ToString(CultureInfo.InvariantCulture),
    });

    /// <summary>
    /// Points at uneven, sub-millisecond offsets, handed to the writer OUT of order (it sorts); none
    /// at all for every fifth series, which the writer still records when its file holds others.
    /// </summary>
    private static List<MetricDataPoint> Points(int s, int file, MetricKind kind)
    {
        var pts = new List<MetricDataPoint>();
        if (s % 5 == 4) return pts;
        for (int p = 0; p < 4 + s % 3; p++)
        {
            long ts = T0 + file * 600 * S + (p * 17 + s) * S + s * 123_457L;
            pts.Add(P(ts, s + p, count: kind == MetricKind.Histogram ? p : 0, sum: kind == MetricKind.Histogram ? p * 0.5 : 0,
                      buckets: kind == MetricKind.Histogram ? [p, 0, 1] : null));
        }
        pts.Reverse();
        return pts;
    }

    private void WriteCorpus()
    {
        double[] bounds = [0.1, 1];
        (string Name, MetricKind Kind, string Unit, int Series)[] layout =
        [
            ("seed.gauge",     MetricKind.Gauge,     "By", 9),
            ("seed.histogram", MetricKind.Histogram, "s",  6),
            ("seed.counter",   MetricKind.Counter,   "",   5),
        ];
        for (int file = 0; file < 4; file++)
        {
            var items = new List<(SeriesKey, HotSeries)>();
            foreach (var (name, kind, unit, series) in layout)
            {
                // The counter changes kind and gains a unit in its later files: the last seen wins.
                var k = name == "seed.counter" && file >= 2 ? MetricKind.Gauge : kind;
                var u = name == "seed.counter" && file == 1 ? "1" : unit;
                for (int s = 0; s < series + file; s++)
                    items.Add((new SeriesKey(name, k, u, Labels(s, file)),
                               new HotSeries(Points(s, file, k), k == MetricKind.Histogram ? bounds : null)));
            }
            MetricWriter.Write(_dir, items, file == 3 ? MetricGranularity.FiveMin : MetricGranularity.Raw);
        }

        // A legacy v2 file holding the histogram's LATEST points, with series in its own order.
        var v2 = new List<(SeriesKey, List<MetricDataPoint>, double[]?)>();
        for (int s = 8; s >= 0; s--)
        {
            var pts = Points(s, 9, MetricKind.Histogram);
            pts.Reverse();   // v2 was written sorted, as the old writer's GetPoints handed it over
            v2.Add((new SeriesKey("seed.histogram", MetricKind.Histogram, "s", Labels(s, 9)), pts, bounds));
        }
        WriteV2File(Path.Combine(_dir, "legacy-seed.histogram.mts"), "seed.histogram", MetricGranularity.Raw, v2);
    }

    /// <summary>
    /// The catalog the old seed built: every .mts in the order the startup scan lists them, every
    /// series fully decoded, its last point's millisecond (the header's max when it has none).
    /// </summary>
    private string FullDecodeCatalog()
    {
        var metas = new SortedDictionary<string, (MetricKind Kind, string Unit, long Last, SortedDictionary<string, List<string>> Values, HashSet<int> Series)>(StringComparer.Ordinal);
        foreach (var file in Directory.EnumerateFiles(_dir, "*.mts").OrderBy(f => f))
        {
            var seg = MetricReader.ReadSegmentInfo(file);
            foreach (var s in MetricReader.ReadAllSync(file))
            {
                if (!metas.TryGetValue(s.Name, out var m)) m = (default, string.Empty, 0, new(StringComparer.Ordinal), []);
                m.Kind = s.Kind;
                if (!string.IsNullOrEmpty(s.Unit)) m.Unit = s.Unit;
                long lastMs = (s.Points.Count > 0 ? s.Points[^1].TimestampUnixNano : seg.MaxNano) / 1_000_000L;
                if (lastMs > m.Last) m.Last = lastMs;
                foreach (var (k, v) in s.Labels)
                {
                    if (!m.Values.TryGetValue(k, out var values)) m.Values[k] = values = [];
                    if (values.Count < Caps.MaxLabelValuesPerKey && !values.Contains(v)) values.Add(v);
                }
                if (m.Series.Count < Caps.MaxTrackedSeriesPerMetric) m.Series.Add(s.Labels.GetHashCode());
                metas[s.Name] = m;
            }
        }

        var sb = new StringBuilder();
        foreach (var (name, m) in metas)
        {
            sb.Append(name).Append('|').Append(m.Kind).Append('|').Append(m.Unit).Append('|').Append(m.Series.Count)
              .Append('|').Append(m.Last).Append('|').AppendJoin(',', m.Values.Keys).AppendLine();
            foreach (var (k, values) in m.Values)
            {
                values.Sort(StringComparer.Ordinal);
                sb.Append("  ").Append(k).Append('=').AppendJoin(',', values).AppendLine();
            }
        }
        return sb.ToString();
    }

    private static string Catalog(MetricStorageEngine engine)
    {
        var sb = new StringBuilder();
        foreach (var c in engine.GetCatalog())
        {
            sb.Append(c.Name).Append('|').Append(c.Kind).Append('|').Append(c.Unit).Append('|').Append(c.Cardinality)
              .Append('|').Append(c.LastSeenMs).Append('|').AppendJoin(',', c.LabelKeys).AppendLine();
            foreach (var k in c.LabelKeys)
                sb.Append("  ").Append(k).Append('=').AppendJoin(',', engine.GetLabelValues(c.Name, k)).AppendLine();
        }
        return sb.ToString();
    }

    [Fact]
    public async Task The_seed_builds_the_catalog_the_full_decode_built()
    {
        WriteCorpus();
        string expected = FullDecodeCatalog();

        await using var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance, Caps);
        await engine.ColdLoadCompleted;

        Assert.Equal(expected, Catalog(engine));

        // And the corpus exercised what it is there for: both caps reached, the kind and the unit
        // changed along the way, the histogram's latest point living in the legacy v2 file.
        var byName = engine.GetCatalog().ToDictionary(c => c.Name);
        Assert.Equal(Caps.MaxTrackedSeriesPerMetric, byName["seed.gauge"].Cardinality);
        Assert.Equal(Caps.MaxLabelValuesPerKey, engine.GetLabelValues("seed.gauge", "pod").Count);
        Assert.Equal(MetricKind.Gauge, byName["seed.counter"].Kind);
        Assert.Equal("1", byName["seed.counter"].Unit);
        var legacy = MetricReader.ReadSegmentInfo(Path.Combine(_dir, "legacy-seed.histogram.mts"));
        Assert.Equal(legacy.MaxNano / 1_000_000L, byName["seed.histogram"].LastSeenMs);
    }

    /// <summary>
    /// The one place the two part ways, and the proof the seed no longer decodes points: a file
    /// whose FIRST series carries a point the decoder cannot read (a string where the timestamp
    /// goes — structurally sound, so walking past it finds nothing wrong). The full decode threw
    /// there and seeded none of the file's series; the identities are all read.
    /// </summary>
    [Fact]
    public async Task A_file_torn_inside_its_points_still_seeds_every_identity()
    {
        var items = new List<(SeriesKey, List<MetricDataPoint>, double[]?)>();
        for (int s = 0; s < 3; s++)
            items.Add((new SeriesKey("seed.torn", MetricKind.Gauge, "ms", Labels(s, 0)),
                       [P(T0 + s * S, s)], null));
        WriteV2File(Path.Combine(_dir, "torn.mts"), "seed.torn", MetricGranularity.Raw, items, corrupt: static i => i == 0);

        await using var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        await engine.ColdLoadCompleted;

        var entry = Assert.Single(engine.GetCatalog());
        Assert.Equal("seed.torn", entry.Name);
        Assert.Equal(3, entry.Cardinality);
        Assert.Equal(["svc-0", "svc-1", "svc-2"], engine.GetLabelValues("seed.torn", "service.name"));
    }
}
