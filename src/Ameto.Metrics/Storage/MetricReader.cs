using Ameto.Core;
using System.Buffers;
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
    /// <summary>Largest block this reader will decompress. The same ceiling SpanReader uses, and
    /// for the same reason: nothing on disk bounds what an LZ4 payload claims to expand to.</summary>
    private const int MaxBlockBytes = 64 * 1024 * 1024;

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
            SizeBytes     = fs.Length,
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
            // One LZ4 block holding every series back to back. Series are decoded one
            // at a time — materialising the whole section would make the caller's peak
            // the file's series count, which is unbounded in files written before the
            // 512-series cap (exactly what a high-cardinality deployment has on disk).
            // THE SAME RULE THE TRACE READERS FOLLOW, and this file was outside the scan that
            // enforces it — which is exactly how it kept its unbounded rents while nine sites one
            // project over were being fixed round after round. A compressed size taken from an
            // untrusted .mts header is a rent of that size before anything discovers the file is
            // shorter, and ArrayPool keeps a large bucket committed for the life of the process.
            br.ReadUInt32(); // uncompSize
            uint compSize = br.ReadUInt32();
            FileBounds.RequireLengthFits(compSize, fs.Length - fs.Position, "Series block", filePath);

            byte[] comp = ArrayPool<byte>.Shared.Rent((int)compSize);
            byte[]? raw  = null;
            try
            {
                fs.ReadExactly(comp, 0, (int)compSize);
                // The length INSIDE the payload, which the check above never saw: LZ4 carries the
                // decompressed size in its own header, so a short well-formed block can still ask
                // for gigabytes.
                int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                FileBounds.RequireLengthFits(rawLen, MaxBlockBytes, "Series block uncompressed", filePath);
                raw = ArrayPool<byte>.Shared.Rent(rawLen);
                LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), raw.AsSpan(0, rawLen));

                int offset = 0;
                for (int i = 0; i < seriesCount && offset < rawLen; i++)
                {
                    var series = DeserializeNext(metricName, raw, offset, rawLen, deltaMs: true, out offset);
                    if (series is not null) yield return series;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(comp);
                if (raw is not null) ArrayPool<byte>.Shared.Return(raw);
            }
        }
        else
        {
            // v2: per-series LZ4 blocks.
            for (int i = 0; i < seriesCount && fs.Position < nameIdxOffset; i++)
            {
                br.ReadUInt32(); // uncompSize
                uint compSize = br.ReadUInt32();
                FileBounds.RequireLengthFits(compSize, fs.Length - fs.Position, $"Series {i} block", filePath);

                byte[] comp = ArrayPool<byte>.Shared.Rent((int)compSize);
                byte[]? raw = null;
                MetricSeries? series;
                try
                {
                    fs.ReadExactly(comp, 0, (int)compSize);
                    int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                    FileBounds.RequireLengthFits(rawLen, MaxBlockBytes, $"Series {i} uncompressed", filePath);
                    raw = ArrayPool<byte>.Shared.Rent(rawLen);
                    LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), raw.AsSpan(0, rawLen));
                    series = DeserializeNext(metricName, raw, 0, rawLen, deltaMs: false, out _);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(comp);
                    if (raw is not null) ArrayPool<byte>.Shared.Return(raw);
                }

                if (series is not null) yield return series;
            }
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Decodes the series starting at <paramref name="offset"/> and reports where the next
    /// one begins. Non-iterator by necessity: <see cref="MessagePackReader"/> is a ref struct
    /// and cannot live across a <c>yield</c>, so the cursor is carried out as a plain int and
    /// the reader is rebuilt per call (it is a span wrapper — no allocation).
    /// </summary>
    private static MetricSeries? DeserializeNext(
        string metricName, byte[] raw, int offset, int length, bool deltaMs, out int next)
    {
        var r = new MessagePackReader(new ReadOnlyMemory<byte>(raw, offset, length - offset));
        var series = DeserializeSeries(metricName, ref r, deltaMs);
        next = offset + (int)r.Consumed;
        return series;
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
        // A header is a claim, not a measurement: one byte is the least a double can occupy in
        // MessagePack, so nothing in the block can hold more than its own remaining bytes. The
        // reservation is capped below that and grown as the values actually arrive, because a
        // count that survives the check can still be far larger than anything real.
        FileBounds.RequireCountFits(count, r.Sequence.Length - r.Consumed,
            fileBytesPerElement: 1, "Bucket bounds", "the series block");
        var b = new double[FileBounds.PreallocFor(count, heapBytesPerElement: sizeof(double))];
        for (int i = 0; i < count; i++)
        {
            if (i == b.Length) Array.Resize(ref b, Math.Min(count, Math.Max(4, b.Length * 2)));
            b[i] = r.ReadDouble();
        }
        return b;
    }

    private static LabelSet ReadLabels(ref MessagePackReader r)
    {
        int count = r.ReadMapHeader();
        // THE SAME RULE AS EVERY OTHER READER HERE, and this site went four rounds without it for a
        // reason worth naming: the convention test that enforces the rule could not SEE this line.
        // Its pattern read a generic argument list with `[^>]*`, which stops at the first `>`, so
        // `List<KeyValuePair<string, string>>` matched nothing at all — the file was scanned, the
        // shape was there, and the scanner walked past it. That blindness is fixed in
        // FileBoundsConventionTests; this is the allocation it was hiding.
        //
        // Two bounds, two questions. Could the file hold this many pairs — a pair is two msgpack
        // strings and the shortest legal one is a byte each, so two bytes on disk. And what may be
        // reserved up front for a count that passes: a map header torn to int.MaxValue was believed
        // whole and asked for 2.1 billion slots, 34 GB of references, before one pair was read.
        FileBounds.RequireCountFits(count, r.Sequence.Length - r.Consumed,
            fileBytesPerElement: 2, "Label set", "the series block");
        int cap   = FileBounds.PreallocFor(count, heapBytesPerElement: 16);
        var pairs = new List<KeyValuePair<string, string>>(cap);
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
        // A MessagePack array header is a number out of the file like any other: the block it
        // lives in is bounded, but the header can still claim far more points than the block
        // holds, and a capacity is reserved before a single one is read. Reserve modestly and
        // let the list grow into whatever is really there.
        int count  = r.ReadArrayHeader();
        var pts    = new List<MetricDataPoint>(FileBounds.PreallocFor(count, heapBytesPerElement: 64));
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
                    FileBounds.RequireCountFits(bn, r.Sequence.Length - r.Consumed,
                        fileBytesPerElement: 1, "Histogram buckets", "the series block");
                    var bk = new long[FileBounds.PreallocFor(bn, heapBytesPerElement: sizeof(long))];
                    for (int j = 0; j < bn; j++)
                    {
                        if (j == bk.Length) Array.Resize(ref bk, Math.Min(bn, Math.Max(4, bk.Length * 2)));
                        bk[j] = r.ReadInt64();
                    }
                    buckets = bk;
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
        var pts   = new List<MetricDataPoint>(FileBounds.PreallocFor(count, heapBytesPerElement: 64));
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
                    FileBounds.RequireCountFits(bn, r.Sequence.Length - r.Consumed,
                        fileBytesPerElement: 1, "Histogram buckets", "the series block");
                    var bk = new long[FileBounds.PreallocFor(bn, heapBytesPerElement: sizeof(long))];
                    for (int j = 0; j < bn; j++)
                    {
                        if (j == bk.Length) Array.Resize(ref bk, Math.Min(bn, Math.Max(4, bk.Length * 2)));
                        bk[j] = r.ReadInt64();
                    }
                    buckets = bk;
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

    /// <summary>
    /// Buffered at 64 KB, deliberately, even though the buffer is allocated per open and
    /// <c>RewriteMetricInChunks</c> reopens every source once per series chunk (~6 MB/min of
    /// gen0 in an allocation trace).
    ///
    /// <para>Dropping the buffer looks free because the v3 path reads a dozen header fields
    /// and then pulls whole LZ4 blocks into pooled buffers. The v2 path does not: it reads
    /// two <see cref="BinaryReader.ReadUInt32"/> per SERIES, and v2 files are exactly the
    /// ones with unbounded series counts. Measured on that shape, 5 000 series per file:</para>
    ///
    /// <code>
    ///   bufferSize     ms/open     alloc/open
    ///            0       20.54            238 B
    ///         4096        1.75          4.4 KB
    ///        65536        0.34         65.9 KB
    /// </code>
    ///
    /// <para>Unbuffered is 60x slower here — trading gen0 garbage, which the collector
    /// handles for almost nothing, for syscalls, which it cannot help with at all. The 6 MB
    /// is 2 % of this server's allocation; the syscalls are not 2 % of anything.</para>
    /// </summary>
    private static FileStream OpenRead(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
}
