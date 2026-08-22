using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Writes a batch of <see cref="SpanRecord"/> objects to a <c>.trc</c> columnar file
/// and a companion <c>.stats</c> sidecar with per-service duration histograms.
///
/// <para>File format v3 — "RDTC":</para>
/// <code>
///   [Header — 27 bytes]
///     0   Magic        : uint32  "RDTC"
///     4   Version      : uint16  3
///     6   SpanCount    : uint32
///    10   MinStartNano : int64
///    18   MaxStartNano : int64
///    26   Flags        : byte
///
///   [Span Blocks]
///     N × { uncompSize uint32 | compSize uint32 | LZ4-HC msgpack bytes }
///     span: positional 11-element array
///       [ tid bin16, sid bin8, pid bin8|nil, Δts, dur, name, svc, kind, status, hsc, attrs map|nil ]
///       Δts: first span of a block = absolute unix nanos, the rest = varint delta
///       to the previous span (spans are sorted by start time, so deltas stay tiny).
///       Blocks decompress independently — the delta chain restarts per block.
///       attrs are written inline (typed msgpack values), not as a nested blob.
///
///   [TraceId Index — one LZ4 block]
///     uncompSize uint32 | compSize uint32 | LZ4 bytes of:
///       traceCount uint32
///       per trace: traceId 16B | offsetCount uint32 | offsets uint32[]
///
///   [Service Index]
///     serviceCount uint32
///     per service: nameLen uint16 | name UTF-8 | blockCount uint32 | blockIndices uint32[]
///
///   [Bloom Index — per-block attribute blooms, see SpanBloom]
///     blockCount uint32
///     per block: byteLen uint32 | bitset bytes (0 bytes ⇒ no bloom, never skip)
///
///   [Footer — 28 bytes]
///     traceIdxOffset uint64
///     svcIdxOffset   uint64
///     bloomIdxOffset uint64
///     footerMagic    uint32  "RDTF"
/// </code>
/// <para>
/// v3 versus v2: positional arrays replace string-keyed maps (~40 B of repeated
/// key strings per span gone), timestamps are block-local deltas instead of
/// 9-byte absolutes, attributes serialize inline (no per-span nested buffer),
/// root spans store nil instead of an 8-byte zero parent id, blocks use LZ4-HC,
/// the trace index is compressed, and per-block attribute blooms let TraceQL
/// skip blocks. v2 files remain readable (see <see cref="SpanReader"/>) and
/// migrate to v3 through the background compaction in <see cref="TraceStorageEngine"/>.
/// </para>
/// </summary>
internal static class SpanWriter
{
    private const uint   Magic       = 0x52_44_54_43; // "RDTC"
    private const uint   FooterMagic = 0x52_44_54_46; // "RDTF"
    private const ushort Version     = 3;
    private const int    BlockSize   = 4096;

    public static SpanSegmentInfo Write(string dataDir, IList<SpanRecord> spans)
    {
        if (spans.Count == 0) throw new InvalidOperationException("Cannot write empty span batch.");

        // Sort by start time: keeps the per-block Δts encoding tiny and gives the
        // trace/service indices better block locality.
        var ordered = new List<SpanRecord>(spans);
        ordered.Sort(static (a, b) => a.StartTimeUnixNano.CompareTo(b.StartTimeUnixNano));
        spans = ordered;

        long minNano = spans[0].StartTimeUnixNano;
        long maxNano = spans[^1].StartTimeUnixNano;

        // Nonce keeps the name unique across a v2→v3 rewrite of the same span
        // batch (same min/max/count → same name) — otherwise two flushes would
        // collide on one path. The name is never parsed back; sidecars share this
        // base so they stay grouped.
        string nonce    = Guid.NewGuid().ToString("N").Substring(0, 8);
        string baseName = $"spans-{minNano}-{maxNano}-{spans.Count}-{nonce}";
        string trcPath  = Path.Combine(dataDir, baseName + ".trc");

        // TEMP-NAME PROTOCOL. All four files are built at .tmp names, fsynced, and only
        // then renamed into place — sidecars first, the .trc LAST: its appearance is the
        // commit the background cold scan keys on. Building at the final path had three
        // failure modes, all closed here: a power loss after the caller's WAL reset left
        // a truncated .trc as the only copy of the flush (the next start deleted it as
        // unreadable, and the WAL had nothing to replay — ~50k spans gone); the cold
        // scan could open a half-written file and, failing on the missing footer,
        // DELETE it — on Linux even under the writer's live handle; and a failed
        // sidecar left a complete-looking .trc behind while the flush reported failure,
        // so every span of it replayed as a duplicate from the next restart. Leftover
        // .tmp files are swept by the TraceStorageEngine constructor, where no writer
        // can be live.
        string trcTmp        = trcPath + ".tmp";
        string statsFinal    = Path.Combine(dataDir, baseName + ".stats");
        string svcgraphFinal = Path.ChangeExtension(trcPath, ".svcgraph");
        string tracesumFinal = Path.ChangeExtension(trcPath, ".tracesum");

        // Accumulate service→block mapping and stats in a single pass through WriteBlock
        var traceIndex  = new Dictionary<TraceId, List<uint>>(capacity: spans.Count / 4);
        var svcBlockMap = new Dictionary<string, SortedSet<uint>>(StringComparer.Ordinal);
        // Per-service stats accumulators (service → mutable stats)
        var svcStats    = new Dictionary<string, MutableServiceStats>(StringComparer.Ordinal);

        try
        {
            using (var fs = new FileStream(trcTmp, FileMode.Create, FileAccess.Write, FileShare.None, 65536))
            using (var bw = new BinaryWriter(fs))
            {
                // ── Header ─────────────────────────────────────────────────────
                bw.Write(Magic);
                bw.Write(Version);
                bw.Write((uint)spans.Count);
                bw.Write(minNano);
                bw.Write(maxNano);
                bw.Write((byte)0); // flags

                // ── Span blocks ────────────────────────────────────────────────
                // One block buffer for the whole file, reset (not reallocated) per block.
                // The old shape allocated a fresh 1 MB ArrayBufferWriter per block, grew it
                // by Array.Resize under large attributes, then copied the whole thing again
                // with WrittenSpan.ToArray() — three LOH-sized allocations per 4096 spans,
                // ~145 MB of flush-path LOH churn in a 7-minute allocation trace.
                int written  = 0;
                var blockBuf = new ArrayBufferWriter<byte>(1024 * 1024);
                var blooms   = new List<byte[]>();

                while (written < spans.Count)
                {
                    int batchCount = Math.Min(BlockSize, spans.Count - written);
                    uint blockIdx  = (uint)(written / BlockSize);
                    var block      = WriteBlock(spans, written, batchCount, blockBuf, blockIdx,
                                                traceIndex, svcBlockMap, svcStats, blooms);
                    bw.Write((uint)block.UncompressedSize);
                    bw.Write((uint)block.CompressedBytes.Length);
                    bw.Write(block.CompressedBytes);
                    written += batchCount;
                }

                // ── TraceId index (one LZ4 block) ──────────────────────────────
                long traceIdxOffset = fs.Position;
                {
                    var idxBuf = new MemoryStream(traceIndex.Count * 32);
                    var idxBw  = new BinaryWriter(idxBuf);
                    Span<byte> traceIdBuf = stackalloc byte[16];
                    idxBw.Write((uint)traceIndex.Count);
                    foreach (var (traceId, offsets) in traceIndex)
                    {
                        traceId.WriteTo(traceIdBuf);
                        idxBw.Write(traceIdBuf);
                        idxBw.Write((uint)offsets.Count);
                        foreach (var o in offsets) idxBw.Write(o);
                    }
                    // Compress from the stream's own backing array — ToArray() duplicated the
                    // whole index (LOH-sized on a busy segment) for nothing.
                    var raw        = idxBuf.GetBuffer().AsSpan(0, (int)idxBuf.Length);
                    var compressed = LZ4Pickler.Pickle(raw, LZ4Level.L09_HC);
                    bw.Write((uint)raw.Length);
                    bw.Write((uint)compressed.Length);
                    bw.Write(compressed);
                }

                // ── Service index ──────────────────────────────────────────────
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

                // ── Bloom index ────────────────────────────────────────────────
                long bloomIdxOffset = fs.Position;
                bw.Write((uint)blooms.Count);
                foreach (var b in blooms)
                {
                    bw.Write((uint)b.Length);
                    bw.Write(b);
                }

                // ── Footer (28 bytes) ──────────────────────────────────────────
                bw.Write((ulong)traceIdxOffset);
                bw.Write((ulong)svcIdxOffset);
                bw.Write((ulong)bloomIdxOffset);
                bw.Write(FooterMagic);

                bw.Flush();
                // To the platter, not the page cache: the caller resets the WAL on our
                // success, so from that moment these bytes are the only copy.
                fs.Flush(flushToDisk: true);
            }

            // ── Sidecars, at temp names (each fsyncs itself). Some legitimately write
            //    nothing — an empty stats/edge set produces no file at all.
            WriteStatsSidecar(statsFinal + ".tmp", svcStats);
            ServiceGraphSidecar.Write(trcPath, spans, svcgraphFinal + ".tmp");
            TraceSummarySidecar.Write(trcPath, spans, tracesumFinal + ".tmp");

            // ── Publish: sidecars first, the .trc last — a visible .trc implies its
            //    sidecars are complete.
            MoveIntoPlaceIfWritten(statsFinal);
            MoveIntoPlaceIfWritten(svcgraphFinal);
            MoveIntoPlaceIfWritten(tracesumFinal);
            File.Move(trcTmp, trcPath);
        }
        catch
        {
            // Nothing was published (the .trc rename is last) — clear our temp files so
            // the failure leaves no residue beyond what the constructor sweep collects.
            TryDelete(trcTmp);
            TryDelete(statsFinal + ".tmp");
            TryDelete(svcgraphFinal + ".tmp");
            TryDelete(tracesumFinal + ".tmp");
            throw;
        }

        var services = new string[svcBlockMap.Count];
        svcBlockMap.Keys.CopyTo(services, 0);

        return new SpanSegmentInfo
        {
            FilePath      = trcPath,
            MinStartNano  = minNano,
            MaxStartNano  = maxNano,
            SpanCount     = spans.Count,
            Services      = services,
            FormatVersion = Version,
        };
    }

    /// <summary>Renames <c>{finalPath}.tmp</c> into place when the sidecar actually wrote one.</summary>
    private static void MoveIntoPlaceIfWritten(string finalPath)
    {
        string tmp = finalPath + ".tmp";
        if (File.Exists(tmp)) File.Move(tmp, finalPath);
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { /* constructor sweep collects it */ }
    }

    // ── Block serialisation ────────────────────────────────────────────────────

    private static (byte[] CompressedBytes, int UncompressedSize) WriteBlock(
        IList<SpanRecord>                        spans,
        int                                      offset,
        int                                      count,
        ArrayBufferWriter<byte>                  bufWriter,
        uint                                     blockIdx,
        Dictionary<TraceId, List<uint>>          traceIndex,
        Dictionary<string, SortedSet<uint>>      svcBlockMap,
        Dictionary<string, MutableServiceStats>  svcStats,
        List<byte[]>                             blooms)
    {
        // Caller-owned buffer, reused across blocks: reset the index without
        // clearing the backing array (Clear() would memset the full capacity).
        bufWriter.ResetWrittenCount();
        var writer = new MessagePackWriter(bufWriter);
        writer.WriteArrayHeader(count);

        Span<byte> idBuf = stackalloc byte[16];
        long prevTs = 0;
        var bloomHashes = new HashSet<ulong>();

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

            // ── Positional span array ────────────────────────────────────────
            writer.WriteArrayHeader(11);

            s.TraceId.WriteTo(idBuf);
            writer.Write((ReadOnlySpan<byte>)idBuf);

            BinaryPrimitives.WriteUInt64BigEndian(idBuf, s.SpanId.RawValue);
            writer.Write((ReadOnlySpan<byte>)idBuf[..8]);

            if (s.ParentSpanId.IsEmpty)
            {
                writer.WriteNil(); // root span — no parent
            }
            else
            {
                BinaryPrimitives.WriteUInt64BigEndian(idBuf, s.ParentSpanId.RawValue);
                writer.Write((ReadOnlySpan<byte>)idBuf[..8]);
            }

            // First span of the block: absolute nanos; the rest: delta.
            writer.Write(i == 0 ? s.StartTimeUnixNano : s.StartTimeUnixNano - prevTs);
            prevTs = s.StartTimeUnixNano;

            writer.Write(s.DurationNanos);
            writer.Write(s.Name);
            writer.Write(s.ServiceName);
            writer.Write((byte)s.Kind);
            writer.Write((byte)s.Status);
            writer.Write(s.HttpStatusCode);

            if (s.Attributes is { Count: > 0 } attrs)
            {
                WriteAttributes(ref writer, attrs);
                foreach (var (k, v) in attrs)
                    SpanBloom.AddAttr(bloomHashes, k, v);
            }
            else
            {
                writer.WriteNil();
            }
        }

        blooms.Add(SpanBloom.Build(bloomHashes));

        writer.Flush();
        // Pickle straight from the written span — the ToArray() copy the old code made
        // here existed only to hand Pickle a byte[] it never kept.
        // HC level: this runs on background flush/compaction threads only.
        var raw        = bufWriter.WrittenSpan;
        var compressed = LZ4Pickler.Pickle(raw, LZ4Level.L09_HC);
        return (compressed, raw.Length);
    }

    /// <summary>Inline typed attribute map — no nested serializer, no per-span buffers.</summary>
    private static void WriteAttributes(ref MessagePackWriter w, IReadOnlyDictionary<string, object?> attrs)
    {
        w.WriteMapHeader(attrs.Count);
        foreach (var (k, v) in attrs)
        {
            w.Write(k);
            switch (v)
            {
                case null:       w.WriteNil();               break;
                case string str: w.Write(str);               break;
                case bool b:     w.Write(b);                 break;
                case long l:     w.Write(l);                 break;
                case int n:      w.Write((long)n);           break;
                case short sh:   w.Write((long)sh);          break;
                case byte by:    w.Write((long)by);          break;
                case double d:   w.Write(d);                 break;
                case float f:    w.Write((double)f);         break;
                default:         w.Write(v.ToString() ?? ""); break;
            }
        }
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

        // Create (not CreateNew): a temp name may carry the residue of a flush that died.
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 4096);
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

        bw.Flush();
        fs.Flush(flushToDisk: true); // durable before the caller renames and resets the WAL
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
