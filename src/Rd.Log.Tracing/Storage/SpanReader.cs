using System.Buffers.Binary;
using K4os.Compression.LZ4;
using MessagePack;

namespace Rd.Log.Tracing.Storage;

/// <summary>
/// Reads span records from <c>.trc</c> files written by <see cref="SpanWriter"/>.
/// </summary>
internal static class SpanReader
{
    private const uint Magic       = 0x52_44_54_43; // "RDTC"
    private const uint FooterMagic = 0x52_44_54_46; // "RDTF"

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

        return new SpanSegmentInfo
        {
            FilePath     = filePath,
            MinStartNano = minNano,
            MaxStartNano = maxNano,
            SpanCount    = (int)spanCount,
        };
    }

    public static async IAsyncEnumerable<SpanRecord> ReadTraceAsync(
        string    filePath,
        TraceId   traceId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var offsets = ReadTraceOffsets(filePath, traceId);
        if (offsets.Count == 0) yield break;

        var all = ReadAllSpansFromFile(filePath);
        foreach (var o in offsets)
        {
            ct.ThrowIfCancellationRequested();
            if (o < all.Count)
                yield return all[(int)o];
        }
    }

    public static async IAsyncEnumerable<SpanRecord> SearchAsync(
        string           filePath,
        long             fromNano,
        long             toNano,
        string?          serviceName,
        string?          spanName,
        SpanStatusCode?  status,
        long?            minDurationNanos,
        long?            maxDurationNanos,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var all = ReadAllSpansFromFile(filePath);
        foreach (var s in all)
        {
            ct.ThrowIfCancellationRequested();
            if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
            if (serviceName      is not null && !s.ServiceName.Equals(serviceName, StringComparison.OrdinalIgnoreCase)) continue;
            if (spanName         is not null && !s.Name.Contains(spanName, StringComparison.OrdinalIgnoreCase)) continue;
            if (status           is not null && s.Status != status.Value) continue;
            if (minDurationNanos is not null && s.DurationNanos < minDurationNanos.Value) continue;
            if (maxDurationNanos is not null && s.DurationNanos > maxDurationNanos.Value) continue;
            yield return s;
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private static List<uint> ReadTraceOffsets(string filePath, TraceId traceId)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        long traceIdxOffset = ReadTraceIdxOffset(fs, br);
        fs.Seek(traceIdxOffset, SeekOrigin.Begin);

        uint traceCount = br.ReadUInt32();
        var idBuf = new byte[16];

        for (uint i = 0; i < traceCount; i++)
        {
            br.Read(idBuf, 0, 16);
            var candidateId = TraceId.Parse(idBuf);
            uint offsetCount = br.ReadUInt32();

            if (candidateId.Equals(traceId))
            {
                var offsets = new List<uint>((int)offsetCount);
                for (uint j = 0; j < offsetCount; j++)
                    offsets.Add(br.ReadUInt32());
                return offsets;
            }
            // skip offsets
            fs.Seek(offsetCount * 4L, SeekOrigin.Current);
        }

        return [];
    }

    internal static List<SpanRecord> ReadAll(string filePath) => ReadAllSpansFromFile(filePath);

    private static List<SpanRecord> ReadAllSpansFromFile(string filePath)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        br.ReadUInt32(); // magic
        br.ReadUInt16(); // version
        int spanCount = (int)br.ReadUInt32();
        br.ReadInt64();  // minNano
        br.ReadInt64();  // maxNano
        br.ReadByte();   // flags

        long traceIdxOffset = ReadTraceIdxOffset(fs, br);
        // Reset to after header (27 bytes)
        fs.Seek(27, SeekOrigin.Begin);

        var result = new List<SpanRecord>(spanCount);

        while (fs.Position < traceIdxOffset)
        {
            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();
            var  compBytes  = br.ReadBytes((int)compSize);
            var  raw        = LZ4Pickler.Unpickle(compBytes);

            var reader = new MessagePackReader(raw);
            int blockCount = reader.ReadArrayHeader();

            for (int i = 0; i < blockCount; i++)
                result.Add(DeserializeSpan(ref reader));
        }

        return result;
    }

    private static long ReadTraceIdxOffset(FileStream fs, BinaryReader br)
    {
        // Footer is last 12 bytes: uint64 traceIdxOffset + uint32 footerMagic
        fs.Seek(-12, SeekOrigin.End);
        long offset = (long)br.ReadUInt64();
        uint magic  = br.ReadUInt32();
        if (magic != FooterMagic) throw new InvalidDataException("Invalid .trc footer magic");
        return offset;
    }

    private static SpanRecord DeserializeSpan(ref MessagePackReader r)
    {
        int fields = r.ReadMapHeader();
        TraceId        traceId    = default;
        SpanId         spanId     = default;
        SpanId         parentId   = default;
        long           ts         = 0;
        long           dur        = 0;
        string         name       = string.Empty;
        string         svc        = string.Empty;
        SpanKind       kind       = SpanKind.Unspecified;
        SpanStatusCode status     = SpanStatusCode.Unset;

        for (int i = 0; i < fields; i++)
        {
            var key = r.ReadString();
            switch (key)
            {
                case "tid": { var b = ReadBytesArray(ref r, 16); traceId  = TraceId.Parse(b); break; }
                case "sid": { var b = ReadBytesArray(ref r,  8); spanId   = SpanId.Parse(b);  break; }
                case "pid": { var b = ReadBytesArray(ref r,  8); parentId = SpanId.Parse(b);  break; }
                case "ts":  ts     = r.ReadInt64();   break;
                case "dur": dur    = r.ReadInt64();   break;
                case "n":   name   = r.ReadString() ?? string.Empty; break;
                case "svc": svc    = r.ReadString() ?? string.Empty; break;
                case "k":   kind   = (SpanKind)r.ReadByte();         break;
                case "st":  status = (SpanStatusCode)r.ReadByte();   break;
                default:    r.Skip(); break;
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
        };
    }

    private static byte[] ReadBytesArray(ref MessagePackReader r, int expectedLen)
    {
        var seq = r.ReadBytes();
        if (seq is null) return new byte[expectedLen];
        var arr = new byte[seq.Value.Length];
        long pos = 0;
        foreach (var segment in seq.Value)
        {
            segment.Span.CopyTo(arr.AsSpan((int)pos));
            pos += segment.Length;
        }
        return arr;
    }

    private static FileStream OpenRead(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
}
