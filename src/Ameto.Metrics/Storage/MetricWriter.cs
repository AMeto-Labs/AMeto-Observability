using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Metrics.Storage;

/// <summary>
/// Writes metric series to <c>.mts</c> files.
///
/// <para>File format v3 — "RDMT":</para>
/// <code>
///   [Header]
///     0  Magic       : uint32  "RDMT"
///     4  Version     : uint16  3
///     6  Granularity : uint8
///     7  SeriesCount : uint32
///    11  MinNano     : int64
///    19  MaxNano     : int64
///    27  Flags       : byte
///   [Section — ALL series in ONE LZ4-HC block]
///     uncompSize uint32 | compSize uint32 | LZ4 bytes
///     msgpack: SeriesCount × { k, u, lbs, bnds(double[]), pts, cnt }
///       point: scalar     [ Δts_ms, value ]
///               histogram [ Δts_ms, value, count, sum, bucketCounts(long[] | nil) ]
///       Δts_ms: first point = full unix ms; the rest = varint delta to the previous.
///       Integral doubles are written as msgpack ints (varint) — the reader's
///       ReadDouble transparently accepts both.
///   [MetricName index]
///     nameCount uint32
///     per-name: nameLen uint16 | name bytes | blockOffset uint64 (0) | blockCount uint32
///   [Footer]
///     nameIdxOffset uint64
///     footerMagic   uint32  "RDMF"
/// </code>
/// <para>
/// v3 versus v2: the whole series section is compressed as a single LZ4-HC block
/// (v2 compressed each series separately, so label strings repeated across
/// thousands of series were never deduplicated and small blocks barely
/// compressed); timestamps are millisecond deltas instead of 9-byte nanosecond
/// ints; scalar points drop the always-zero count/sum/buckets fields. v2 files
/// remain readable (see <see cref="MetricReader"/>) and are migrated to v3 by the
/// background compaction in <see cref="MetricStorageEngine"/>.
/// </para>
/// </summary>
internal static class MetricWriter
{
    private const uint   Magic       = 0x52_44_4D_54; // "RDMT"
    private const uint   FooterMagic = 0x52_44_4D_46; // "RDMF"
    private const ushort Version     = 3;

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
                FilePath      = filePath,
                MetricName    = group.Key,
                MinNano       = minNano,
                MaxNano       = maxNano,
                Granularity   = granularity,
                FormatVersion = Version,
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
        // Serialize ALL series into one msgpack buffer, then compress it as a
        // single LZ4-HC block: repeated label keys/values across series (routes,
        // instance ids, GUIDs) deduplicate inside the shared compression window.
        var bufWriter = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new MessagePackWriter(bufWriter);
        foreach (var (key, hs) in items)
        {
            var pts = hs.GetPoints(long.MinValue, long.MaxValue);
            WriteSeries(ref w, key, hs.Bounds, pts);
        }
        w.Flush();

        var raw        = bufWriter.WrittenSpan.ToArray();
        // HC level: writes happen on background flush/rollup threads only, so we
        // trade CPU for the much better ratio.
        var compressed = LZ4Pickler.Pickle(raw, LZ4Level.L09_HC);

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

        // Section
        bw.Write((uint)raw.Length);
        bw.Write((uint)compressed.Length);
        bw.Write(compressed);

        // Name index
        long nameIdxOffset = fs.Position;
        bw.Write((uint)1); // only one distinct name per file
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(metricName);
        bw.Write((ushort)nameBytes.Length);
        bw.Write(nameBytes);
        bw.Write((ulong)0);                 // block offset — unused in v3 (single section)
        bw.Write((uint)items.Count);

        // Footer
        bw.Write((ulong)nameIdxOffset);
        bw.Write(FooterMagic);
    }

    private static void WriteSeries(
        ref MessagePackWriter w,
        SeriesKey key,
        double[]? bounds,
        IReadOnlyList<MetricDataPoint> points)
    {
        w.WriteMapHeader(6);
        w.Write("k");    w.Write((byte)key.Kind);
        w.Write("u");    w.Write(key.Unit);
        w.Write("lbs");  WriteLabels(ref w, key.Labels);
        w.Write("bnds"); WriteBounds(ref w, bounds);
        w.Write("pts");  WritePoints(ref w, points);
        w.Write("cnt");  w.Write((uint)points.Count);
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

    private static void WriteBounds(ref MessagePackWriter w, double[]? bounds)
    {
        if (bounds is null) { w.WriteArrayHeader(0); return; }
        w.WriteArrayHeader(bounds.Length);
        foreach (var b in bounds) w.Write(b);
    }

    private static void WritePoints(ref MessagePackWriter w, IReadOnlyList<MetricDataPoint> pts)
    {
        w.WriteArrayHeader(pts.Count);
        long prevMs = 0;
        long prevCount = 0;
        double prevSum = 0;
        long[]? prevBuckets = null;
        for (int i = 0; i < pts.Count; i++)
        {
            var p = pts[i];
            long ms = p.TimestampUnixNano / 1_000_000;

            // Slim shape when the histogram state (count / sum / cumulative
            // buckets) is unchanged from the previous point — the reader inherits
            // it. Covers scalar kinds (state is always zero) and every idle
            // export of a cumulative histogram, which on quiet services is the
            // overwhelming majority of points.
            bool same = p.Count == prevCount && p.Sum == prevSum && BucketsEqual(p.BucketCounts, prevBuckets);
            w.WriteArrayHeader(same ? 2 : 5);

            // First point: absolute unix ms; the rest: delta to the previous point.
            w.Write(i == 0 ? ms : ms - prevMs);
            prevMs = ms;

            WriteNumber(ref w, p.Value);
            if (same) continue;

            w.Write(p.Count);
            WriteNumber(ref w, p.Sum);
            if (p.BucketCounts is { Length: > 0 } bc)
            {
                w.WriteArrayHeader(bc.Length);
                foreach (var c in bc) w.Write(c);
            }
            else
            {
                w.WriteNil();
            }
            prevCount   = p.Count;
            prevSum     = p.Sum;
            prevBuckets = p.BucketCounts;
        }
    }

    /// <summary>Bucket equality where null and all-zero are equivalent (both mean "nothing observed").</summary>
    private static bool BucketsEqual(long[]? a, long[]? b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a is null) return !HasAnyCount(b);
        if (b is null) return !HasAnyCount(a);
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++) if (a[i] != b[i]) return false;
        return true;
    }

    private static bool HasAnyCount(long[]? buckets)
    {
        if (buckets is null) return false;
        foreach (var b in buckets) if (b != 0) return true;
        return false;
    }

    /// <summary>Writes an integral double as a msgpack varint (1–9 B instead of a fixed 9 B float64).</summary>
    private static void WriteNumber(ref MessagePackWriter w, double v)
    {
        if (double.IsFinite(v) && Math.Floor(v) == v && Math.Abs(v) <= 9.0e15)
            w.Write((long)v);
        else
            w.Write(v);
    }

    private static string SanitizeName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_'));
}
