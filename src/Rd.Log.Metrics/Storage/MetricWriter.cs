using K4os.Compression.LZ4;
using MessagePack;

namespace Rd.Log.Metrics.Storage;

/// <summary>
/// Writes metric series to <c>.mts</c> files.
///
/// <para>File format v1 — "RDMT":</para>
/// <code>
///   [Header]
///     0  Magic       : uint32  "RDMT"
///     4  Version     : uint16  1
///     6  Granularity : uint8
///     7  SeriesCount : uint32
///    11  MinNano     : int64
///    19  MaxNano     : int64
///    27  Flags       : byte
///   [Series blocks — LZ4-compressed msgpack]
///     N × { uncompSize uint32 | compSize uint32 | LZ4 bytes }
///   [MetricName index]
///     nameCount uint32
///     per-name: nameLen uint16 | name bytes | blockOffset uint64 | blockCount uint32
///   [Footer]
///     nameIdxOffset uint64
///     footerMagic   uint32  "RDMF"
/// </code>
/// </summary>
internal static class MetricWriter
{
    private const uint   Magic       = 0x52_44_4D_54; // "RDMT"
    private const uint   FooterMagic = 0x52_44_4D_46; // "RDMF"
    private const ushort Version     = 1;

    /// <summary>
    /// Writes one <c>.mts</c> file per distinct metric name found in <paramref name="series"/>.
    /// Returns metadata for all created files.
    /// </summary>
    public static List<MetricSegmentInfo> Write(
        string dataDir,
        IList<(SeriesKey Key, HotSeries Series)> series,
        MetricGranularity granularity = MetricGranularity.Raw)
    {
        var byMetric = series.GroupBy(s => s.Key.Name);
        var result   = new List<MetricSegmentInfo>();

        foreach (var group in byMetric)
        {
            var items = group.ToList();
            long minNano = long.MaxValue, maxNano = long.MinValue;
            foreach (var (_, hs) in items)
            {
                var pts = hs.GetPoints(long.MinValue, long.MaxValue);
                if (pts.Count > 0)
                {
                    minNano = Math.Min(minNano, pts[0].TimestampUnixNano);
                    maxNano = Math.Max(maxNano, pts[^1].TimestampUnixNano);
                }
            }
            if (minNano == long.MaxValue) continue;

            string suffix   = granularity == MetricGranularity.Raw ? "raw" : granularity.ToString().ToLower();
            string fileName = $"metrics-{SanitizeName(group.Key)}-{minNano}-{maxNano}-{suffix}.mts";
            string filePath = Path.Combine(dataDir, fileName);

            WriteFile(filePath, group.Key, granularity, items, minNano, maxNano);
            result.Add(new MetricSegmentInfo
            {
                FilePath    = filePath,
                MetricName  = group.Key,
                MinNano     = minNano,
                MaxNano     = maxNano,
                Granularity = granularity,
            });
        }

        return result;
    }

    private static void WriteFile(
        string filePath,
        string metricName,
        MetricGranularity granularity,
        List<(SeriesKey Key, HotSeries Series)> items,
        long minNano,
        long maxNano)
    {
        using var fs = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536);
        using var bw = new BinaryWriter(fs);

        // Header
        bw.Write(Magic);
        bw.Write(Version);
        bw.Write((byte)granularity);
        bw.Write((uint)items.Count);
        bw.Write(minNano);
        bw.Write(maxNano);
        bw.Write((byte)0); // flags

        // Series blocks
        var blockOffsets = new List<long>(items.Count);
        foreach (var (key, hs) in items)
        {
            var pts = hs.GetPoints(long.MinValue, long.MaxValue);
            blockOffsets.Add(fs.Position);
            WriteSeriesBlock(bw, key, pts);
        }

        // Name index
        long nameIdxOffset = fs.Position;
        bw.Write((uint)1); // only one distinct name per file
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(metricName);
        bw.Write((ushort)nameBytes.Length);
        bw.Write(nameBytes);
        bw.Write((ulong)blockOffsets[0]);
        bw.Write((uint)blockOffsets.Count);

        // Footer
        bw.Write((ulong)nameIdxOffset);
        bw.Write(FooterMagic);
    }

    private static void WriteSeriesBlock(
        BinaryWriter bw,
        SeriesKey key,
        IReadOnlyList<MetricDataPoint> points)
    {
        var bufWriter = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new MessagePackWriter(bufWriter);

        w.WriteMapHeader(5);
        w.Write("k");    w.Write((byte)key.Kind);
        w.Write("u");    w.Write(key.Unit);
        w.Write("lbs");  WriteLabels(ref w, key.Labels);
        w.Write("pts");  WritePoints(ref w, points);
        w.Write("cnt");  w.Write((uint)points.Count);
        w.Flush();

        var raw        = bufWriter.WrittenSpan.ToArray();
        var compressed = LZ4Pickler.Pickle(raw);

        bw.Write((uint)raw.Length);
        bw.Write((uint)compressed.Length);
        bw.Write(compressed);
    }

    private static void WriteLabels(ref MessagePackWriter w, LabelSet labels)
    {
        var pairs = labels.Pairs;
        w.WriteMapHeader(pairs.Count);
        foreach (var (k, v) in pairs)
        {
            w.Write(k);
            w.Write(v);
        }
    }

    private static void WritePoints(ref MessagePackWriter w, IReadOnlyList<MetricDataPoint> pts)
    {
        w.WriteArrayHeader(pts.Count);
        foreach (var p in pts)
        {
            w.WriteArrayHeader(4);
            w.Write(p.TimestampUnixNano);
            w.Write(p.Value);
            w.Write(p.Count);
            w.Write(p.Sum);
        }
    }

    private static string SanitizeName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_'));
}
