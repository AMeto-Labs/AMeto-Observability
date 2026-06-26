using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Reads span records from <c>.trc</c> files written by <see cref="SpanWriter"/> (v2).
/// </summary>
internal static class SpanReader
{
    private const uint Magic       = 0x52_44_54_43; // "RDTC"
    private const uint FooterMagic = 0x52_44_54_46; // "RDTF"

    // ── Segment info ───────────────────────────────────────────────────────────

    public static SpanSegmentInfo ReadSegmentInfo(string filePath)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        uint magic = br.ReadUInt32();
        if (magic != Magic) throw new InvalidDataException($"Invalid .trc magic in {filePath}");

        br.ReadUInt16(); // version
        uint spanCount = br.ReadUInt32();
        long minNano   = br.ReadInt64();
        long maxNano   = br.ReadInt64();

        var (_, svcIdxOffset) = ReadFooter(fs, br);
        var services = ReadServicesFromIndex(fs, br, svcIdxOffset);

        return new SpanSegmentInfo
        {
            FilePath     = filePath,
            MinStartNano = minNano,
            MaxStartNano = maxNano,
            SpanCount    = (int)spanCount,
            Services     = services,
        };
    }

    // ── Trace lookup ───────────────────────────────────────────────────────────

    public static async IAsyncEnumerable<SpanRecord> ReadTraceAsync(
        string    filePath,
        TraceId   traceId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var offsets = ReadTraceOffsets(filePath, traceId);
        if (offsets.Count == 0) yield break;

        var all = ReadSpansFromFile(filePath);
        foreach (var o in offsets)
        {
            ct.ThrowIfCancellationRequested();
            if (o < (uint)all.Count) yield return all[(int)o];
        }
    }

    // ── Search with service-index block skip ───────────────────────────────────

    public static async IAsyncEnumerable<SpanRecord> SearchAsync(
        string           filePath,
        long             fromNano,
        long             toNano,
        string?          serviceName,
        string?          spanName,
        SpanStatusCode?  status,
        short?           httpStatusCode,
        long?            minDurationNanos,
        long?            maxDurationNanos,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        // Use service index to skip blocks that cannot contain the target service.
        HashSet<uint>? allowedBlocks = null;
        if (serviceName is not null)
        {
            allowedBlocks = ReadServiceBlockIndices(filePath, serviceName);
            if (allowedBlocks.Count == 0) yield break;
        }

        var spans = ReadSpansFromFile(filePath, allowedBlocks);
        foreach (var s in spans)
        {
            ct.ThrowIfCancellationRequested();
            if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
            if (serviceName      is not null && !s.ServiceName.Equals(serviceName, StringComparison.OrdinalIgnoreCase)) continue;
            if (spanName         is not null && !s.Name.Contains(spanName, StringComparison.OrdinalIgnoreCase)) continue;
            if (status           is not null && s.Status != status.Value) continue;
            if (httpStatusCode   is not null && s.HttpStatusCode != httpStatusCode.Value) continue;
            if (minDurationNanos is not null && s.DurationNanos < minDurationNanos.Value) continue;
            if (maxDurationNanos is not null && s.DurationNanos > maxDurationNanos.Value) continue;
            yield return s;
        }
    }

    // ── Stats sidecar ──────────────────────────────────────────────────────────

    /// <summary>
    /// Reads per-service stats from the companion <c>.stats</c> file.
    /// Returns empty list if sidecar is absent (older segments).
    /// </summary>
    public static List<ServiceSegmentStats> ReadStats(string trcFilePath)
    {
        var statsPath = Path.ChangeExtension(trcFilePath, ".stats");
        if (!File.Exists(statsPath)) return [];

        const uint StatsMagic = 0x52_44_54_53; // "RDTS"

        try
        {
            using var fs = new FileStream(statsPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != StatsMagic) return [];
            br.ReadUInt16(); // version
            uint count = br.ReadUInt32();

            var result = new List<ServiceSegmentStats>((int)count);
            for (uint i = 0; i < count; i++)
            {
                ushort nameLen  = br.ReadUInt16();
                string name     = Encoding.UTF8.GetString(br.ReadBytes(nameLen));
                uint   spans    = br.ReadUInt32();
                uint   errors   = br.ReadUInt32();
                long   minDur   = br.ReadInt64();
                long   maxDur   = br.ReadInt64();
                var    buckets  = new uint[HistogramBuckets.Count];
                for (int b = 0; b < HistogramBuckets.Count; b++)
                    buckets[b] = br.ReadUInt32();

                result.Add(new ServiceSegmentStats
                {
                    ServiceName      = name,
                    SpanCount        = spans,
                    ErrorCount       = errors,
                    MinDurationNanos = minDur,
                    MaxDurationNanos = maxDur,
                    Buckets          = buckets,
                });
            }
            return result;
        }
        catch
        {
            return [];
        }
    }

    // ── ReadAll (compaction) ───────────────────────────────────────────────────

    internal static List<SpanRecord> ReadAll(string filePath) => ReadSpansFromFile(filePath);

    // ── Core block reader ──────────────────────────────────────────────────────

    /// <param name="allowedBlocks">
    /// When non-null, only blocks whose 0-based index is in this set are decompressed.
    /// Non-allowed blocks are skipped via <c>Seek</c>, saving LZ4 + msgpack work.
    /// </param>
    private static List<SpanRecord> ReadSpansFromFile(
        string         filePath,
        HashSet<uint>? allowedBlocks = null)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        br.ReadUInt32(); // magic
        br.ReadUInt16(); // version
        int spanCount = (int)br.ReadUInt32();
        br.ReadInt64();  // minNano
        br.ReadInt64();  // maxNano
        br.ReadByte();   // flags

        var (traceIdxOffset, _) = ReadFooter(fs, br);
        fs.Seek(27, SeekOrigin.Begin); // reset to after header

        int capacity = allowedBlocks is null ? spanCount : spanCount / 2;
        var result   = new List<SpanRecord>(capacity);

        uint blockIdx = 0;
        while (fs.Position < traceIdxOffset)
        {
            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();

            if (allowedBlocks is not null && !allowedBlocks.Contains(blockIdx))
            {
                // Skip decompression + deserialization — pure seek, O(1)
                fs.Seek(compSize, SeekOrigin.Current);
            }
            else
            {
                var compBytes = br.ReadBytes((int)compSize);
                var raw       = LZ4Pickler.Unpickle(compBytes);
                var reader    = new MessagePackReader(raw);
                int cnt       = reader.ReadArrayHeader();
                for (int i = 0; i < cnt; i++)
                    result.Add(DeserializeSpan(ref reader));
            }

            blockIdx++;
        }

        return result;
    }

    // ── Index readers ──────────────────────────────────────────────────────────

    private static List<uint> ReadTraceOffsets(string filePath, TraceId traceId)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        var (traceIdxOffset, _) = ReadFooter(fs, br);
        fs.Seek(traceIdxOffset, SeekOrigin.Begin);

        uint traceCount = br.ReadUInt32();
        var  idBuf      = new byte[16];

        for (uint i = 0; i < traceCount; i++)
        {
            br.Read(idBuf, 0, 16);
            var candidate  = TraceId.Parse(idBuf);
            uint offsetCnt = br.ReadUInt32();

            if (candidate.Equals(traceId))
            {
                var offsets = new List<uint>((int)offsetCnt);
                for (uint j = 0; j < offsetCnt; j++) offsets.Add(br.ReadUInt32());
                return offsets;
            }
            fs.Seek(offsetCnt * 4L, SeekOrigin.Current);
        }
        return [];
    }

    /// <returns>0-based block indices containing at least one span from <paramref name="serviceName"/>.</returns>
    private static HashSet<uint> ReadServiceBlockIndices(string filePath, string serviceName)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        var (_, svcIdxOffset) = ReadFooter(fs, br);
        fs.Seek(svcIdxOffset, SeekOrigin.Begin);

        uint svcCount = br.ReadUInt32();
        for (uint i = 0; i < svcCount; i++)
        {
            ushort nameLen = br.ReadUInt16();
            var    name    = Encoding.UTF8.GetString(br.ReadBytes(nameLen));
            uint   blkCnt  = br.ReadUInt32();

            if (name.Equals(serviceName, StringComparison.OrdinalIgnoreCase))
            {
                var set = new HashSet<uint>((int)blkCnt);
                for (uint b = 0; b < blkCnt; b++) set.Add(br.ReadUInt32());
                return set;
            }
            fs.Seek(blkCnt * 4L, SeekOrigin.Current);
        }
        return [];
    }

    private static string[] ReadServicesFromIndex(FileStream fs, BinaryReader br, long svcIdxOffset)
    {
        fs.Seek(svcIdxOffset, SeekOrigin.Begin);
        uint count    = br.ReadUInt32();
        var  services = new string[count];
        for (uint i = 0; i < count; i++)
        {
            ushort nameLen = br.ReadUInt16();
            services[i]   = Encoding.UTF8.GetString(br.ReadBytes(nameLen));
            uint blkCnt   = br.ReadUInt32();
            fs.Seek(blkCnt * 4L, SeekOrigin.Current);
        }
        return services;
    }

    // ── Footer (20 bytes) ──────────────────────────────────────────────────────

    private static (long traceIdxOffset, long svcIdxOffset) ReadFooter(FileStream fs, BinaryReader br)
    {
        fs.Seek(-20, SeekOrigin.End);
        long traceIdx = (long)br.ReadUInt64();
        long svcIdx   = (long)br.ReadUInt64();
        uint magic    = br.ReadUInt32();
        if (magic != FooterMagic) throw new InvalidDataException($"Invalid .trc footer magic in {fs.Name}");
        return (traceIdx, svcIdx);
    }

    // ── Span deserialisation ───────────────────────────────────────────────────

    private static SpanRecord DeserializeSpan(ref MessagePackReader r)
    {
        int fields = r.ReadMapHeader();
        TraceId        traceId   = default;
        SpanId         spanId    = default;
        SpanId         parentId  = default;
        long           ts        = 0;
        long           dur       = 0;
        string         name      = string.Empty;
        string         svc       = string.Empty;
        SpanKind       kind      = SpanKind.Unspecified;
        SpanStatusCode status    = SpanStatusCode.Unset;
        short          httpSC    = 0;
        byte[]?        attrBytes = null;

        for (int i = 0; i < fields; i++)
        {
            var key = r.ReadString();
            switch (key)
            {
                case "tid":  { var b = ReadBytesFixed(ref r, 16); traceId  = TraceId.Parse(b); break; }
                case "sid":  { var b = ReadBytesFixed(ref r,  8); spanId   = SpanId.Parse(b);  break; }
                case "pid":  { var b = ReadBytesFixed(ref r,  8); parentId = SpanId.Parse(b);  break; }
                case "ts":   ts     = r.ReadInt64();                 break;
                case "dur":  dur    = r.ReadInt64();                 break;
                case "n":    name   = r.ReadString() ?? string.Empty; break;
                case "svc":  svc    = r.ReadString() ?? string.Empty; break;
                case "k":    kind   = (SpanKind)r.ReadByte();         break;
                case "st":   status = (SpanStatusCode)r.ReadByte();   break;
                case "hsc":  httpSC = r.ReadInt16();                  break;
                case "attr":
                {
                    var seq = r.ReadBytes();
                    if (seq.HasValue) attrBytes = System.Buffers.BuffersExtensions.ToArray(seq.Value);
                    break;
                }
                default: r.Skip(); break;
            }
        }

        return new SpanRecord
        {
            TraceId           = traceId,
            SpanId            = spanId,
            ParentSpanId      = parentId,
            StartTimeUnixNano = ts,
            DurationNanos     = dur,
            Name              = name,
            ServiceName       = svc,
            Kind              = kind,
            Status            = status,
            HttpStatusCode    = httpSC,
            Attributes        = attrBytes is { Length: > 0 } ? DeserializeAttributes(attrBytes) : null,
        };
    }

    private static byte[] ReadBytesFixed(ref MessagePackReader r, int expectedLen)
    {
        var seq = r.ReadBytes();
        if (seq is null) return new byte[expectedLen];
        var arr = new byte[seq.Value.Length];
        long pos = 0;
        foreach (var seg in seq.Value)
        {
            seg.Span.CopyTo(arr.AsSpan((int)pos));
            pos += seg.Length;
        }
        return arr;
    }

    private static IReadOnlyDictionary<string, object?>? DeserializeAttributes(byte[] bytes)
    {
        try { return MessagePackSerializer.Deserialize<Dictionary<string, object?>>(bytes); }
        catch { return null; }
    }

    private static FileStream OpenRead(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
}
