using System.Buffers.Binary;
using K4os.Compression.LZ4;
using MessagePack;

namespace Rd.Log.Tracing.Storage;

/// <summary>
/// Writes a batch of <see cref="SpanRecord"/> objects to a <c>.trc</c> columnar file.
///
/// <para>
/// File format v1 — "RDTC":
/// <code>
///   [Header]
///     0   Magic      : uint32  "RDTC"
///     4   Version    : uint16  1
///     6   SpanCount  : uint32
///    10   MinStartNano: int64
///    18   MaxStartNano: int64
///    26   Flags      : byte
///   [Blocks — LZ4-compressed msgpack span array]
///     N blocks × { uncompSize uint32 | compSize uint32 | LZ4 bytes }
///   [TraceId Index]
///     traceCount uint32
///     per-trace: traceId 16 bytes | offsetCount uint32 | offsets uint32[]
///   [Footer]
///     traceIdxOffset uint64
///     footerMagic    uint32  "RDTF"
/// </code>
/// </para>
/// </summary>
internal static class SpanWriter
{
    private const uint   Magic       = 0x52_44_54_43; // "RDTC"
    private const uint   FooterMagic = 0x52_44_54_46; // "RDTF"
    private const ushort Version     = 1;
    private const int    BlockSize   = 4096; // spans per block

    public static SpanSegmentInfo Write(string dataDir, IList<SpanRecord> spans)
    {
        if (spans.Count == 0) throw new InvalidOperationException("Cannot write empty span batch.");

        long minNano = spans.Min(s => s.StartTimeUnixNano);
        long maxNano = spans.Max(s => s.StartTimeUnixNano);
        string fileName = $"spans-{minNano}-{maxNano}-{spans.Count}.trc";
        string filePath = Path.Combine(dataDir, fileName);

        using var fs = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536);
        using var bw = new BinaryWriter(fs);

        // ── Header ───────────────────────────────────────────────────────────
        bw.Write(Magic);
        bw.Write(Version);
        bw.Write((uint)spans.Count);
        bw.Write(minNano);
        bw.Write(maxNano);
        bw.Write((byte)0); // flags

        // ── Span blocks ───────────────────────────────────────────────────────
        // Build TraceId index while writing blocks
        var traceIndex = new Dictionary<TraceId, List<uint>>();

        int written = 0;
        var buf     = new byte[1024 * 1024]; // scratch for msgpack

        while (written < spans.Count)
        {
            int batchCount = Math.Min(BlockSize, spans.Count - written);
            var block      = WriteBlock(spans, written, batchCount, buf, traceIndex);
            bw.Write((uint)block.UncompressedSize);
            bw.Write((uint)block.CompressedBytes.Length);
            bw.Write(block.CompressedBytes);
            written += batchCount;
        }

        // ── TraceId index ─────────────────────────────────────────────────────
        long traceIdxOffset = fs.Position;

        bw.Write((uint)traceIndex.Count);
        var traceIdBuf = new byte[16];
        foreach (var (traceId, offsets) in traceIndex)
        {
            traceId.WriteTo(traceIdBuf);
            bw.Write(traceIdBuf);
            bw.Write((uint)offsets.Count);
            foreach (var o in offsets)
                bw.Write(o);
        }

        // ── Footer ────────────────────────────────────────────────────────────
        bw.Write((ulong)traceIdxOffset);
        bw.Write(FooterMagic);

        return new SpanSegmentInfo
        {
            FilePath     = filePath,
            MinStartNano = minNano,
            MaxStartNano = maxNano,
            SpanCount    = spans.Count,
        };
    }

    private static (byte[] CompressedBytes, int UncompressedSize) WriteBlock(
        IList<SpanRecord>              spans,
        int                            offset,
        int                            count,
        byte[]                         scratch,
        Dictionary<TraceId, List<uint>> traceIndex)
    {
        // Serialise block as msgpack array of maps
        var bufWriter = new System.Buffers.ArrayBufferWriter<byte>(scratch.Length);
        var writer = new MessagePackWriter(bufWriter);
        writer.WriteArrayHeader(count);

        for (int i = 0; i < count; i++)
        {
            var s = spans[offset + i];
            uint globalOffset = (uint)(offset + i);

            // Update trace index
            if (!traceIndex.TryGetValue(s.TraceId, out var offsets))
            {
                offsets = new List<uint>(4);
                traceIndex[s.TraceId] = offsets;
            }
            offsets.Add(globalOffset);

            // Serialize span as map with fixed keys
            writer.WriteMapHeader(9);
            writer.Write("tid");  WriteTraceId(ref writer, s.TraceId);
            writer.Write("sid");  WriteSpanId(ref writer, s.SpanId);
            writer.Write("pid");  WriteSpanId(ref writer, s.ParentSpanId);
            writer.Write("ts");   writer.Write(s.StartTimeUnixNano);
            writer.Write("dur");  writer.Write(s.DurationNanos);
            writer.Write("n");    writer.Write(s.Name);
            writer.Write("svc");  writer.Write(s.ServiceName);
            writer.Write("k");    writer.Write((byte)s.Kind);
            writer.Write("st");   writer.Write((byte)s.Status);
        }

        writer.Flush();
        var raw        = bufWriter.WrittenSpan.ToArray();
        var compressed = LZ4Pickler.Pickle(raw);
        return (compressed, raw.Length);
    }

    private static void WriteTraceId(ref MessagePackWriter w, TraceId id)
    {
        var buf = new byte[16];
        id.WriteTo(buf);
        w.Write(new ReadOnlySpan<byte>(buf));
    }

    private static void WriteSpanId(ref MessagePackWriter w, SpanId id)
    {
        var buf = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buf, id.RawValue);
        w.Write(new ReadOnlySpan<byte>(buf));
    }
}
