using System.Buffers.Binary;
using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// Histogram series shaped like the 512 MB stand's <c>http.server.request.duration</c> (#125): the OTel
/// default explicit bounds (15 of them, so 16 bucket counts), eight labels of realistic length, and
/// cumulative state in the hundreds of thousands — five-byte msgpack integers, as a long-lived
/// exporter's are — so a point weighs on disk and in the heap what the stand's do (~112 B encoded,
/// ~192 B decoded).
/// </summary>
internal static class MetricHistogramShapes
{
    internal const string Metric = "http.server.request.duration";

    internal static readonly double[] Bounds = [0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];

    private static readonly string[] Routes =
    [
        "/api/v1/payments/{id}", "/api/v1/payments", "/api/v1/merchants/{id}", "/api/v1/merchants", "/api/v1/terminals/{id}",
        "/api/v1/terminals", "/api/v1/transactions/{id}", "/api/v1/transactions", "/api/v1/refunds/{id}", "/api/v1/refunds",
        "/api/v1/settlements/{id}", "/api/v1/settlements", "/api/v1/reports/{id}", "/api/v1/reports", "/api/v1/users/{id}",
        "/api/v1/users", "/api/v1/sessions", "/api/v1/auth/token", "/api/v1/auth/refresh", "/health", "/health/ready",
        "/metrics", "/api/v1/cards/{id}", "/api/v1/cards", "/api/v1/accounts/{id}", "/api/v1/accounts", "/api/v1/limits/{id}",
        "/api/v1/limits", "/api/v1/fees/{id}", "/api/v1/fees", "/api/v1/currencies", "/api/v1/rates", "/api/v1/webhooks/{id}",
        "/api/v1/webhooks", "/api/v1/audit", "/api/v1/disputes/{id}", "/api/v1/disputes",
    ];

    private static readonly string[] Methods = ["GET", "POST", "PUT", "DELETE"];
    private static readonly string[] Codes   = ["200", "201", "204", "400", "404", "500"];

    internal static LabelSet LabelsFor(int s) => new(new Dictionary<string, string>
    {
        ["service.name"]              = "ntp-svc-" + (s % 12).ToString(CultureInfo.InvariantCulture),
        ["http.route"]                = Routes[s % Routes.Length],
        ["http.request.method"]       = Methods[(s / 7) % Methods.Length],
        ["http.response.status_code"] = Codes[(s / 3) % Codes.Length],
        ["network.protocol.version"]  = (s & 1) == 0 ? "1.1" : "2",
        ["url.scheme"]                = "http",
        ["host.name"]                 = "ntp-svc-" + (s % 12).ToString(CultureInfo.InvariantCulture) + "-"
                                      + (s / 12).ToString("x6", CultureInfo.InvariantCulture),
        ["deployment.environment"]    = "production",
    });

    /// <summary>
    /// <paramref name="series"/> series; file f holds the next <paramref name="pointsPerFile"/>[f] points
    /// of EVERY series, a point every <paramref name="stepNano"/> from <paramref name="startNano"/>, so a
    /// series spans all the files. With probability <paramref name="activeFraction"/> a point's state
    /// moves (a FULL point: its own bucket array), otherwise it repeats the last state (a SLIM point).
    /// When <paramref name="digests"/> is given, every point written is added to it (see <see cref="Add"/>).
    /// </summary>
    internal static List<MetricSegmentInfo> Write(
        string dir, MetricGranularity granularity, int series, long startNano, long stepNano,
        int[] pointsPerFile, double activeFraction, int seed, Dictionary<string, SeriesDigest>? digests = null)
    {
        Directory.CreateDirectory(dir);
        var rng     = new Random(seed);
        var labels  = new LabelSet[series];
        var buckets = new long[series][];
        var sums    = new double[series];
        for (int s = 0; s < series; s++)
        {
            labels[s] = LabelsFor(s);
            var b     = new long[Bounds.Length + 1];
            for (int j = 0; j < b.Length; j++) b[j] = 50_000 + rng.Next(400_000);
            buckets[s] = b;
            long total = 0;
            foreach (long c in b) total += c;
            sums[s] = total * 0.0731;
        }

        var result = new List<MetricSegmentInfo>();
        long g = 0;
        foreach (int pf in pointsPerFile)
        {
            var items = new List<(SeriesKey, HotSeries)>(series);
            for (int s = 0; s < series; s++)
            {
                var pts = new List<MetricDataPoint>(pf);
                for (int p = 0; p < pf; p++)
                {
                    if (rng.NextDouble() < activeFraction)
                    {
                        var nb = (long[])buckets[s].Clone();
                        long inc = 0;
                        for (int j = 0; j < nb.Length; j++) { int d = rng.Next(0, 40); nb[j] += d; inc += d; }
                        buckets[s] = nb;
                        sums[s]   += inc * (0.01 + rng.NextDouble() * 0.2);
                    }
                    long count = 0;
                    foreach (long c in buckets[s]) count += c;
                    pts.Add(new MetricDataPoint
                    {
                        TimestampUnixNano = startNano + (g + p) * stepNano,
                        Value             = sums[s] / count,
                        Count             = count,
                        Sum               = sums[s],
                        BucketCounts      = buckets[s],
                    });
                }
                if (digests is not null) Add(digests, labels[s], pts);
                items.Add((new SeriesKey(Metric, MetricKind.Histogram, "s", labels[s]), new HotSeries(pts, Bounds)));
            }
            result.AddRange(MetricWriter.Write(dir, items, granularity));
            g += pf;
        }
        return result;
    }

    /// <summary>The section's uncompressed and compressed sizes, from a v3 header (offsets 28 and 32).</summary>
    internal static (uint Raw, uint Comp) BlockSizes(string path)
    {
        using var fs = File.OpenRead(path);
        Span<byte> b = stackalloc byte[8];
        fs.Seek(28, SeekOrigin.Begin);
        fs.ReadExactly(b);
        return (BinaryPrimitives.ReadUInt32LittleEndian(b), BinaryPrimitives.ReadUInt32LittleEndian(b[4..]));
    }

    /// <summary>
    /// What a series' points add up to: their count and an order-independent sum of a 64-bit mix of
    /// every field of every point — timestamp, value, count, sum and each bucket. Two sets of files hold
    /// the same points for a series exactly when the digests agree (the data these tests write has no
    /// duplicate timestamps, so a merge drops nothing).
    /// </summary>
    internal readonly record struct SeriesDigest(long Points, ulong Hash);

    internal static void Add(Dictionary<string, SeriesDigest> into, LabelSet labels, IReadOnlyList<MetricDataPoint> points)
    {
        string key = string.Join('\u0001', labels.Interleaved.ToArray());
        into.TryGetValue(key, out var d);
        ulong h = d.Hash;
        for (int i = 0; i < points.Count; i++)
        {
            var p = points[i];
            ulong x = Mix((ulong)p.TimestampUnixNano);
            x = Mix(x ^ (ulong)BitConverter.DoubleToInt64Bits(p.Value));
            x = Mix(x ^ (ulong)p.Count);
            x = Mix(x ^ (ulong)BitConverter.DoubleToInt64Bits(p.Sum));
            if (p.BucketCounts is { } bc)
                foreach (long c in bc) x = Mix(x ^ (ulong)c);
            h += x;
        }
        into[key] = new SeriesDigest(d.Points + points.Count, h);
    }

    /// <summary>Same series, each with the same points — order of the series not considered.</summary>
    internal static void AssertSame(Dictionary<string, SeriesDigest> expected, Dictionary<string, SeriesDigest> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (var (key, digest) in expected)
        {
            Assert.True(actual.TryGetValue(key, out var got), $"series {key.Replace('\u0001', ',')} is missing");
            Assert.Equal(digest, got);
        }
    }

    /// <summary>Every series of every file, digested one series at a time — a read that a refused block fails.</summary>
    internal static Dictionary<string, SeriesDigest> Digests(IEnumerable<string> files)
    {
        var into = new Dictionary<string, SeriesDigest>(StringComparer.Ordinal);
        foreach (string f in files)
            foreach (var s in MetricReader.ReadAllSync(f))
                Add(into, s.Labels, s.Points);
        return into;
    }

    private static ulong Mix(ulong z)
    {
        z += 0x9E3779B97F4A7C15UL;
        z  = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z  = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}
