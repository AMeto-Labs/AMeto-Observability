using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Writes a batch of <see cref="SpanRecord"/> objects to a <c>.trc</c> columnar file
/// and a companion <c>.stats</c> sidecar with per-service duration histograms.
///
/// <para>File format v2 — "RDTC":</para>
/// <code>
///   [Header — 27 bytes]
///     0   Magic        : uint32  "RDTC"
///     4   Version      : uint16  2
///     6   SpanCount    : uint32
///    10   MinStartNano : int64
///    18   MaxStartNano : int64
///    26   Flags        : byte
///
///   [Span Blocks]
///     N × { uncompSize uint32 | compSize uint32 | LZ4-msgpack bytes }
///
///   [TraceId Index]
///     traceCount uint32
///     per trace: traceId 16B | offsetCount uint32 | offsets uint32[]
///
///   [Service Index]
///     serviceCount uint32
///     per service: nameLen uint16 | name UTF-8 | blockCount uint32 | blockIndices uint32[]
///
///   [Footer — 20 bytes]
///     traceIdxOffset uint64
///     svcIdxOffset   uint64
///     footerMagic    uint32  "RDTF"
/// </code>
/// </summary>
internal static class SpanWriter
{
    private const uint   Magic       = 0x52_44_54_43; // "RDTC"
    private const uint   FooterMagic = 0x52_44_54_46; // "RDTF"
    private const ushort Version     = 2;
    private const int    BlockSize   = 4096;

    public static SpanSegmentInfo Write(string dataDir, IList<SpanRecord> spans)
    {
        if (spans.Count == 0) throw new InvalidOperationException("Cannot write empty span batch.");

        long minNano = long.MaxValue, maxNano = long.MinValue;
        for (int i = 0; i < spans.Count; i++)
        {
            var ts = spans[i].StartTimeUnixNano;
            if (ts < minNano) minNano = ts;
            if (ts > maxNano) maxNano = ts;
        }

        string baseName = $"spans-{minNano}-{maxNano}-{spans.Count}";
        string trcPath  = Path.Combine(dataDir, baseName + ".trc");

        // Accumulate service→block mapping and stats in a single pass through WriteBlock
        var traceIndex  = new Dictionary<TraceId, List<uint>>(capacity: spans.Count / 4);
        var svcBlockMap = new Dictionary<string, SortedSet<uint>>(StringComparer.Ordinal);
        // Per-service stats accumulators (service → mutable stats)
        var svcStats    = new Dictionary<string, MutableServiceStats>(StringComparer.Ordinal);

        using var fs = new FileStream(trcPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536);
        using var bw = new BinaryWriter(fs);

        // ── Header ─────────────────────────────────────────────────────────────
        bw.Write(Magic);
        bw.Write(Version);
        bw.Write((uint)spans.Count);
        bw.Write(minNano);
        bw.Write(maxNano);
        bw.Write((byte)0); // flags

        // ── Span blocks ────────────────────────────────────────────────────────
        int written = 0;
        var scratch = new byte[1024 * 1024];

        while (written < spans.Count)
        {
            int batchCount = Math.Min(BlockSize, spans.Count - written);
            uint blockIdx  = (uint)(written / BlockSize);
            var block      = WriteBlock(spans, written, batchCount, scratch, blockIdx,
                                        traceIndex, svcBlockMap, svcStats);
            bw.Write((uint)block.UncompressedSize);
            bw.Write((uint)block.CompressedBytes.Length);
            bw.Write(block.CompressedBytes);
            written += batchCount;
        }

        // ── TraceId index ──────────────────────────────────────────────────────
        long traceIdxOffset = fs.Position;
        bw.Write((uint)traceIndex.Count);
        Span<byte> traceIdBuf = stackalloc byte[16];
        foreach (var (traceId, offsets) in traceIndex)
        {
            traceId.WriteTo(traceIdBuf);
            bw.Write(traceIdBuf.ToArray());
            bw.Write((uint)offsets.Count);
            foreach (var o in offsets) bw.Write(o);
        }

        // ── Service index ──────────────────────────────────────────────────────
        long svcIdxOffset = fs.Position;
        bw.Write((uint)svcBlockMap.Count);
        foreach (var (svcName, blocks) in svcBlockMap)
        {
            var nameBytes = Encoding.UTF8.GetBytes(svcName);
            bw.Write((ushort)nameBytes.Length);
            bw.Write(nameBytes);
            bw.Write((uint)blocks.Count);
            foreach (var b in blocks) bw.Write(b);
        }

        // ── Footer (20 bytes) ──────────────────────────────────────────────────
        bw.Write((ulong)traceIdxOffset);
        bw.Write((ulong)svcIdxOffset);
        bw.Write(FooterMagic);

        // ── .stats sidecar ─────────────────────────────────────────────────────
        string statsPath = Path.Combine(dataDir, baseName + ".stats");
        WriteStatsSidecar(statsPath, svcStats);

        // ── .svcgraph sidecar ──────────────────────────────────────────────────
        ServiceGraphSidecar.Write(trcPath, spans);

        var services = new string[svcBlockMap.Count];
        svcBlockMap.Keys.CopyTo(services, 0);

        return new SpanSegmentInfo
        {
            FilePath     = trcPath,
            MinStartNano = minNano,
            MaxStartNano = maxNano,
            SpanCount    = spans.Count,
            Services     = services,
        };
    }

    // ── Block serialisation ────────────────────────────────────────────────────

    private static (byte[] CompressedBytes, int UncompressedSize) WriteBlock(
        IList<SpanRecord>                        spans,
        int                                      offset,
        int                                      count,
        byte[]                                   scratch,
        uint                                     blockIdx,
        Dictionary<TraceId, List<uint>>          traceIndex,
        Dictionary<string, SortedSet<uint>>      svcBlockMap,
        Dictionary<string, MutableServiceStats>  svcStats)
    {
        var bufWriter = new System.Buffers.ArrayBufferWriter<byte>(scratch.Length);
        var writer    = new MessagePackWriter(bufWriter);
        writer.WriteArrayHeader(count);

        for (int i = 0; i < count; i++)
        {
            var s = spans[offset + i];
            uint globalOffset = (uint)(offset + i);

            // ── TraceId index ────────────────────────────────────────────────
            if (!traceIndex.TryGetValue(s.TraceId, out var tOffsets))
            {
                tOffsets = new List<uint>(4);
                traceIndex[s.TraceId] = tOffsets;
            }
            tOffsets.Add(globalOffset);

            // ── Service block map ────────────────────────────────────────────
            if (!svcBlockMap.TryGetValue(s.ServiceName, out var blocks))
            {
                blocks = new SortedSet<uint>();
                svcBlockMap[s.ServiceName] = blocks;
            }
            blocks.Add(blockIdx);

            // ── Per-service stats ────────────────────────────────────────────
            if (!svcStats.TryGetValue(s.ServiceName, out var st))
            {
                st = new MutableServiceStats();
                svcStats[s.ServiceName] = st;
            }
            st.SpanCount++;
            if (s.Status == SpanStatusCode.Error) st.ErrorCount++;
            if (s.DurationNanos < st.MinDuration) st.MinDuration = s.DurationNanos;
            if (s.DurationNanos > st.MaxDuration) st.MaxDuration = s.DurationNanos;
            st.Buckets[HistogramBuckets.IndexOf(s.DurationNanos)]++;

            // ── Msgpack span map ─────────────────────────────────────────────
            bool hasAttrs  = s.Attributes is { Count: > 0 };
            bool hasStatus = s.HttpStatusCode != 0;
            int  fieldCnt  = 9 + (hasAttrs ? 1 : 0) + (hasStatus ? 1 : 0);
            writer.WriteMapHeader(fieldCnt);

            writer.Write("tid");  WriteTraceId(ref writer, s.TraceId);
            writer.Write("sid");  WriteSpanId(ref writer, s.SpanId);
            writer.Write("pid");  WriteSpanId(ref writer, s.ParentSpanId);
            writer.Write("ts");   writer.Write(s.StartTimeUnixNano);
            writer.Write("dur");  writer.Write(s.DurationNanos);
            writer.Write("n");    writer.Write(s.Name);
            writer.Write("svc");  writer.Write(s.ServiceName);
            writer.Write("k");    writer.Write((byte)s.Kind);
            writer.Write("st");   writer.Write((byte)s.Status);

            if (hasStatus)
            {
                writer.Write("hsc");
                writer.Write(s.HttpStatusCode);
            }
            if (hasAttrs)
            {
                writer.Write("attr");
                var attrBytes = MessagePackSerializer.Serialize(
                    s.Attributes is Dictionary<string, object?> d
                        ? d : new Dictionary<string, object?>(s.Attributes!));
                writer.Write(new ReadOnlySpan<byte>(attrBytes));
            }
        }

        writer.Flush();
        var raw        = bufWriter.WrittenSpan.ToArray();
        var compressed = LZ4Pickler.Pickle(raw);
        return (compressed, raw.Length);
    }

    // ── Stats sidecar ──────────────────────────────────────────────────────────

    /// <summary>
    /// Binary format: Magic(4) Version(2) ServiceCount(4)
    ///   per-service: nameLen(2) name(UTF-8) spanCount(4) errorCount(4)
    ///                minDur(8) maxDur(8) buckets(4×20)
    /// </summary>
    private static void WriteStatsSidecar(
        string path, Dictionary<string, MutableServiceStats> stats)
    {
        if (stats.Count == 0) return;
        const uint StatsMagic = 0x52_44_54_53; // "RDTS"

        using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096);
        using var bw = new BinaryWriter(fs);

        bw.Write(StatsMagic);
        bw.Write((ushort)1); // version
        bw.Write((uint)stats.Count);

        foreach (var (name, st) in stats)
        {
            var nameBytes = Encoding.UTF8.GetBytes(name);
            bw.Write((ushort)nameBytes.Length);
            bw.Write(nameBytes);
            bw.Write(st.SpanCount);
            bw.Write(st.ErrorCount);
            bw.Write(st.MinDuration == long.MaxValue ? 0L : st.MinDuration);
            bw.Write(st.MaxDuration == long.MinValue ? 0L : st.MaxDuration);
            foreach (var b in st.Buckets) bw.Write(b);
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

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

    // ── Mutable accumulator (not exposed outside this class) ──────────────────

    private sealed class MutableServiceStats
    {
        public uint   SpanCount  = 0;
        public uint   ErrorCount = 0;
        public long   MinDuration = long.MaxValue;
        public long   MaxDuration = long.MinValue;
        public uint[] Buckets    = new uint[HistogramBuckets.Count];
    }
}
