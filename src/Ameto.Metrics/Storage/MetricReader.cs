using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Metrics.Storage;

internal static class MetricReader
{
    private const uint   Magic       = 0x52_44_4D_54; // "RDMT"
    private const uint   FooterMagic = 0x52_44_4D_46; // "RDMF"
    private const ushort Version     = 2;

    public static MetricSegmentInfo ReadSegmentInfo(string filePath)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        uint magic = br.ReadUInt32();
        if (magic != Magic) throw new InvalidDataException($"Invalid .mts magic in {filePath}");

        ushort version = br.ReadUInt16();
        if (version != Version) throw new InvalidDataException($"Unsupported .mts version {version} (expected {Version}) in {filePath}");
        var granularity = (MetricGranularity)br.ReadByte();
        br.ReadUInt32();  // seriesCount
        long minNano = br.ReadInt64();
        long maxNano = br.ReadInt64();

        // Read metric name from name index
        long nameIdxOffset = ReadNameIdxOffset(fs, br);
        fs.Seek(nameIdxOffset, SeekOrigin.Begin);
        br.ReadUInt32(); // nameCount
        ushort nameLen = br.ReadUInt16();
        string metricName = System.Text.Encoding.UTF8.GetString(br.ReadBytes(nameLen));

        return new MetricSegmentInfo
        {
            FilePath    = filePath,
            MetricName  = metricName,
            MinNano     = minNano,
            MaxNano     = maxNano,
            Granularity = granularity,
        };
    }

    public static async IAsyncEnumerable<MetricSeries> ReadAsync(
        string filePath,
        string metricName,
        long   fromNano,
        long   toNano,
        IReadOnlyDictionary<string, string>? labelMatchers,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var series in ReadAllSync(filePath))
        {
            ct.ThrowIfCancellationRequested();
            if (!series.Name.Equals(metricName, StringComparison.OrdinalIgnoreCase)) continue;
            if (labelMatchers is not null && !MatchesLabels(series.Labels, labelMatchers)) continue;

            var filtered = series.Points
                .Where(p => p.TimestampUnixNano >= fromNano && p.TimestampUnixNano <= toNano)
                .ToList();
            if (filtered.Count == 0) continue;

            yield return new MetricSeries
            {
                Name   = series.Name,
                Kind   = series.Kind,
                Unit   = series.Unit,
                Labels = series.Labels,
                Points = filtered,
            };
        }
        await Task.CompletedTask;
    }

    public static IEnumerable<MetricSeries> ReadAllSync(string filePath)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        uint magic = br.ReadUInt32();
        if (magic != Magic) yield break;

        ushort version = br.ReadUInt16();
        if (version != Version) yield break; // v1 — incompatible, skipped (deleted on load)
        br.ReadByte();   // granularity
        int seriesCount = (int)br.ReadUInt32();
        br.ReadInt64();  // minNano
        br.ReadInt64();  // maxNano
        br.ReadByte();   // flags

        long nameIdxOffset = ReadNameIdxOffset(fs, br);

        // Read metric name
        fs.Seek(nameIdxOffset, SeekOrigin.Begin);
        br.ReadUInt32(); // nameCount
        ushort nameLen = br.ReadUInt16();
        string metricName = System.Text.Encoding.UTF8.GetString(br.ReadBytes(nameLen));

        // Read blocks
        // Reset to after header (28 bytes)
        fs.Seek(28, SeekOrigin.Begin);
        for (int i = 0; i < seriesCount && fs.Position < nameIdxOffset; i++)
        {
            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();
            var  compBytes  = br.ReadBytes((int)compSize);
            var  raw        = LZ4Pickler.Unpickle(compBytes);

            var series = DeserializeSeries(metricName, raw);
            if (series is not null) yield return series;
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private static MetricSeries? DeserializeSeries(string metricName, byte[] raw)
    {
        var r = new MessagePackReader(raw);
        int fields = r.ReadMapHeader();

        MetricKind kind    = MetricKind.Counter;
        string     unit    = string.Empty;
        LabelSet   labels  = LabelSet.Empty;
        double[]?  bounds  = null;
        var        points  = new List<MetricDataPoint>();

        for (int i = 0; i < fields; i++)
        {
            var key = r.ReadString();
            switch (key)
            {
                case "k":    kind   = (MetricKind)r.ReadByte(); break;
                case "u":    unit   = r.ReadString() ?? string.Empty; break;
                case "lbs":  labels = ReadLabels(ref r); break;
                case "bnds": bounds = ReadBounds(ref r); break;
                case "pts":  points = ReadPoints(ref r); break;
                default:     r.Skip(); break;
            }
        }

        return new MetricSeries
        {
            Name         = metricName,
            Kind         = kind,
            Unit         = unit,
            Labels       = labels,
            BucketBounds = bounds,
            Points       = points,
        };
    }

    private static double[]? ReadBounds(ref MessagePackReader r)
    {
        int count = r.ReadArrayHeader();
        if (count == 0) return null;
        var b = new double[count];
        for (int i = 0; i < count; i++) b[i] = r.ReadDouble();
        return b;
    }

    private static LabelSet ReadLabels(ref MessagePackReader r)
    {
        int count = r.ReadMapHeader();
        var pairs = new List<KeyValuePair<string, string>>(count);
        for (int i = 0; i < count; i++)
        {
            var k = r.ReadString() ?? string.Empty;
            var v = r.ReadString() ?? string.Empty;
            pairs.Add(new KeyValuePair<string, string>(k, v));
        }
        return new LabelSet(pairs);
    }

    private static List<MetricDataPoint> ReadPoints(ref MessagePackReader r)
    {
        int count = r.ReadArrayHeader();
        var pts   = new List<MetricDataPoint>(count);
        for (int i = 0; i < count; i++)
        {
            int n = r.ReadArrayHeader(); // 5 fields in v2
            long   ts  = r.ReadInt64();
            double val = r.ReadDouble();
            long   cnt = r.ReadInt64();
            double sum = r.ReadDouble();
            long[]? buckets = null;
            if (n >= 5)
            {
                if (r.TryReadNil())
                {
                    // scalar point — no buckets
                }
                else
                {
                    int bn = r.ReadArrayHeader();
                    buckets = new long[bn];
                    for (int j = 0; j < bn; j++) buckets[j] = r.ReadInt64();
                }
            }
            pts.Add(new MetricDataPoint
            {
                TimestampUnixNano = ts,
                Value             = val,
                Count             = cnt,
                Sum               = sum,
                BucketCounts      = buckets,
            });
        }
        return pts;
    }

    private static bool MatchesLabels(
        LabelSet labels,
        IReadOnlyDictionary<string, string> matchers)
    {
        var dict = labels.Pairs.ToDictionary(t => t.Key, t => t.Value, StringComparer.Ordinal);
        foreach (var (k, v) in matchers)
        {
            if (!dict.TryGetValue(k, out var actual)) return false;
            // Exact, or '|'-delimited OR (e.g. service.name=A|B|C) for multi-select.
            if (v.IndexOf('|') < 0) { if (actual != v) return false; }
            else
            {
                bool any = false;
                foreach (var opt in v.Split('|')) if (actual == opt) { any = true; break; }
                if (!any) return false;
            }
        }
        return true;
    }

    private static long ReadNameIdxOffset(FileStream fs, BinaryReader br)
    {
        fs.Seek(-12, SeekOrigin.End);
        long offset = (long)br.ReadUInt64();
        uint magic  = br.ReadUInt32();
        if (magic != FooterMagic) throw new InvalidDataException("Invalid .mts footer magic");
        return offset;
    }

    private static FileStream OpenRead(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
}
