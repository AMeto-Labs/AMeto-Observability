using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Metrics.Storage;

/// <summary>
/// Reads <c>.mts</c> files — both the current v3 format (whole-file LZ4-HC
/// section, ms-delta timestamps, kind-aware points) and the legacy v2 format
/// (per-series LZ4 blocks, absolute nanosecond timestamps). v2 files are
/// rewritten to v3 by the background compaction; v1 files are deleted on load.
/// </summary>
internal static class MetricReader
{
    private const uint   Magic       = 0x52_44_4D_54; // "RDMT"
    private const uint   FooterMagic = 0x52_44_4D_46; // "RDMF"

    public static MetricSegmentInfo ReadSegmentInfo(string filePath)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        uint magic = br.ReadUInt32();
        if (magic != Magic) throw new InvalidDataException($"Invalid .mts magic in {filePath}");

        ushort version = br.ReadUInt16();
        if (version is not (2 or 3)) throw new InvalidDataException($"Unsupported .mts version {version} in {filePath}");
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
            FilePath      = filePath,
            MetricName    = metricName,
            MinNano       = minNano,
            MaxNano       = maxNano,
            Granularity   = granularity,
            FormatVersion = version,
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
                Name         = series.Name,
                Kind         = series.Kind,
                Unit         = series.Unit,
                Labels       = series.Labels,
                BucketBounds = series.BucketBounds,
                Points       = filtered,
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
        if (version is not (2 or 3)) yield break; // v1 — incompatible, skipped (deleted on load)
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

        // Reset to after header (28 bytes)
        fs.Seek(28, SeekOrigin.Begin);

        if (version == 3)
        {
            // One LZ4 block holding every series back to back.
            br.ReadUInt32(); // uncompSize
            uint compSize = br.ReadUInt32();
            var  raw      = LZ4Pickler.Unpickle(br.ReadBytes((int)compSize));

            foreach (var series in DeserializeSection(metricName, raw, seriesCount))
                yield return series;
        }
        else
        {
            // v2: per-series LZ4 blocks.
            for (int i = 0; i < seriesCount && fs.Position < nameIdxOffset; i++)
            {
                br.ReadUInt32(); // uncompSize
                uint compSize = br.ReadUInt32();
                var  raw      = LZ4Pickler.Unpickle(br.ReadBytes((int)compSize));

                var series = DeserializeOne(metricName, raw, deltaMs: false);
                if (series is not null) yield return series;
            }
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    /// <summary>Parses every series out of a decompressed v3 section (ref-struct reader kept out of the iterator).</summary>
    private static List<MetricSeries> DeserializeSection(string metricName, byte[] raw, int seriesCount)
    {
        var result = new List<MetricSeries>(seriesCount);
        var r = new MessagePackReader(raw);
        for (int i = 0; i < seriesCount && !r.End; i++)
        {
            var series = DeserializeSeries(metricName, ref r, deltaMs: true);
            if (series is not null) result.Add(series);
        }
        return result;
    }

    /// <summary>Parses a single v2 series block.</summary>
    private static MetricSeries? DeserializeOne(string metricName, byte[] raw, bool deltaMs)
    {
        var r = new MessagePackReader(raw);
        return DeserializeSeries(metricName, ref r, deltaMs);
    }

    private static MetricSeries? DeserializeSeries(string metricName, ref MessagePackReader r, bool deltaMs)
    {
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
                case "pts":  points = deltaMs ? ReadPointsV3(ref r) : ReadPointsV2(ref r); break;
                default:     r.Skip(); break;
            }
        }

        // v3 stores idle histogram points (count=0, sum=0, all buckets 0) in the
        // slim scalar shape; reconstruct their all-zero bucket arrays here so the
        // roundtrip is lossless and delta chains in the aggregator stay intact.
        // One shared array per series — nothing downstream mutates BucketCounts.
        if (deltaMs && kind == MetricKind.Histogram && bounds is not null && points.Count > 0)
        {
            long[]? zeros = null;
            for (int i = 0; i < points.Count; i++)
            {
                var p = points[i];
                if (p.BucketCounts is not null || p.Count != 0 || p.Sum != 0) continue;
                zeros   ??= new long[bounds.Length + 1];
                points[i] = new MetricDataPoint
                {
                    TimestampUnixNano = p.TimestampUnixNano,
                    Value             = p.Value,
                    BucketCounts      = zeros,
                };
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

    /// <summary>
    /// v3 points: ms-delta timestamps; a 2-element (slim) point inherits the
    /// previous point's histogram state (count / sum / buckets) — for scalar
    /// series that state is always zero, for histograms it run-length-encodes
    /// idle stretches losslessly.
    /// </summary>
    private static List<MetricDataPoint> ReadPointsV3(ref MessagePackReader r)
    {
        int count  = r.ReadArrayHeader();
        var pts    = new List<MetricDataPoint>(count);
        long ms    = 0;
        long    cnt = 0;
        double  sum = 0;
        long[]? buckets = null;
        for (int i = 0; i < count; i++)
        {
            int n = r.ReadArrayHeader(); // 2 = slim (state unchanged), 5 = full
            ms = i == 0 ? r.ReadInt64() : ms + r.ReadInt64();

            double val = r.ReadDouble(); // transparently accepts msgpack ints
            if (n >= 5)
            {
                cnt = r.ReadInt64();
                sum = r.ReadDouble();
                if (r.TryReadNil())
                {
                    buckets = null; // state set but no buckets recorded
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
                TimestampUnixNano = ms * 1_000_000,
                Value             = val,
                Count             = cnt,
                Sum               = sum,
                BucketCounts      = buckets, // shared with the previous point when slim — nothing mutates it
            });
        }
        return pts;
    }

    /// <summary>v2 points: absolute nanosecond timestamps; always 5 fields.</summary>
    private static List<MetricDataPoint> ReadPointsV2(ref MessagePackReader r)
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
