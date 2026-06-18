using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Metrics.Storage;

internal static class MetricReader
{
    private const uint Magic       = 0x52_44_4D_54; // "RDMT"
    private const uint FooterMagic = 0x52_44_4D_46; // "RDMF"

    public static MetricSegmentInfo ReadSegmentInfo(string filePath)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        uint magic = br.ReadUInt32();
        if (magic != Magic) throw new InvalidDataException($"Invalid .mts magic in {filePath}");

        br.ReadUInt16();  // version
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

        br.ReadUInt16(); // version
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
        var        points  = new List<MetricDataPoint>();

        for (int i = 0; i < fields; i++)
        {
            var key = r.ReadString();
            switch (key)
            {
                case "k":   kind   = (MetricKind)r.ReadByte(); break;
                case "u":   unit   = r.ReadString() ?? string.Empty; break;
                case "lbs": labels = ReadLabels(ref r); break;
                case "pts": points = ReadPoints(ref r); break;
                default:    r.Skip(); break;
            }
        }

        return new MetricSeries
        {
            Name   = metricName,
            Kind   = kind,
            Unit   = unit,
            Labels = labels,
            Points = points,
        };
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
            r.ReadArrayHeader(); // 4 fields
            pts.Add(new MetricDataPoint
            {
                TimestampUnixNano = r.ReadInt64(),
                Value             = r.ReadDouble(),
                Count             = r.ReadInt64(),
                Sum               = r.ReadDouble(),
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
            if (!dict.TryGetValue(k, out var actual) || actual != v) return false;
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
