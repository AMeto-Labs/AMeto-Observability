using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Writes a cold-tier .seg file (v3, columnar) from a frozen HotTierSegment.
///
/// File layout — see ARCHITECTURE.md for full spec.
///
/// Block format (uncompressed, before LZ4):
///   uint32 eventCount
///   int64  blockMinTimestampTicks   (base for @t delta)
///   uint64 blockMinEventId          (base for @i delta)
///   uint8  columnCount              (= 6)
///   { uint8 columnId, uint32 byteLen, bytes[byteLen] } * columnCount
///
/// Columns:
///   1 @t  : int64[eventCount]                — ticks - blockMinTimestamp
///   2 @l  : byte [eventCount]
///   3 @i  : uint64[eventCount]               — id - blockMinEventId
///   4 @mt : string column                    — uint32[eventCount+1] offsets + utf8 bytes
///   5 @x  : nullable msgpack ExceptionInfo   — uint32[eventCount+1] offsets + bytes
///                                              (offset[i+1] == offset[i] ⇒ null)
///   6 props: nullable msgpack map            — uint32[eventCount+1] offsets + bytes
///
/// Block outer frame: uint32 uncompressedSize, uint32 compressedSize, bytes[compressedSize].
/// </summary>
public sealed class SegmentWriter : IDisposable
{
    private const uint   MagicHeader = 0x52_44_4C_47; // "RDLG"
    private const uint   MagicFooter = 0x52_44_46_54; // "RDFT"
    private const ushort SegVersion  = 4;             // v4: + TraceId/SpanId/ServiceName columns
    private const int    BlockSize   = 64 * 1024;      // 64 KB target uncompressed block size

    public  const byte   FlagCompressed = 0x01;

    private readonly string       _filePath;
    private readonly FileStream   _fs;
    private readonly BinaryWriter _bw;

    private readonly List<(long Offset, ulong FirstEventId)> _blockIndex = new();

    private long _invertedIndexOffset;
    private long _trigramIndexOffset;
    private long _bloomFilterOffset;
    private long _blockIndexOffset;

    private int      _eventsWritten;
    private long     _minTimestamp = long.MaxValue;
    private long     _maxTimestamp = long.MinValue;
    private LogLevel _minLevel     = LogLevel.Fatal;

    public SegmentWriter(string filePath)
    {
        _filePath = filePath;
        _fs       = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.SequentialScan);
        _bw       = new BinaryWriter(_fs);
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public void WriteEvents(HotTierSegment hot, StringInternPool templatePool)
    {
        _fs.Seek(SegmentFileHeader.Size, SeekOrigin.Begin);

        int count = hot.Count;
        if (count == 0) return;

        // Sort ascending by (TimestampUtcTicks, EventId)
        var order = new int[count];
        for (int i = 0; i < count; i++) order[i] = i;
        Array.Sort(order, (a, b) =>
        {
            ref var ha = ref hot.GetHeader(a);
            ref var hb = ref hot.GetHeader(b);
            int c = ha.TimestampUtcTicks.CompareTo(hb.TimestampUtcTicks);
            return c != 0 ? c : ha.Id.CompareTo(hb.Id);
        });

        var batch = new List<int>(1024);
        int approxBytes = 0;

        for (int oi = 0; oi < count; oi++)
        {
            int i = order[oi];
            ref var h = ref hot.GetHeader(i);

            long ts = h.TimestampUtcTicks;
            if (ts < _minTimestamp) _minTimestamp = ts;
            if (ts > _maxTimestamp) _maxTimestamp = ts;
            if (h.Level < _minLevel) _minLevel = h.Level;

            string template = hot.GetTemplate(i) ?? templatePool.Get(h.MessageTemplatePoolIndex) ?? string.Empty;
            int propsLen  = hot.GetPropertiesPayload(i).Length;
            int tmplLen   = Encoding.UTF8.GetByteCount(template);
            int excApprox = hot.GetException(i) is null ? 0 : 64;
            int rowCost   = 8 + 1 + 8 + 4 + tmplLen + 4 + excApprox + 4 + propsLen;

            if (batch.Count > 0 && approxBytes + rowCost > BlockSize)
            {
                FlushColumnarBlock(hot, templatePool, batch);
                batch.Clear();
                approxBytes = 0;
            }

            batch.Add(i);
            approxBytes += rowCost;
            _eventsWritten++;
        }

        if (batch.Count > 0)
            FlushColumnarBlock(hot, templatePool, batch);
    }

    public void WriteInvertedIndex(ReadOnlySpan<byte> indexBytes)
    {
        _invertedIndexOffset = _fs.Position;
        _bw.Write((uint)indexBytes.Length);
        _bw.Write(indexBytes);
    }

    public void WriteTrigramIndex(ReadOnlySpan<byte> indexBytes)
    {
        _trigramIndexOffset = _fs.Position;
        _bw.Write((uint)indexBytes.Length);
        _bw.Write(indexBytes);
    }

    public void WriteBloomFilter(ReadOnlySpan<byte> filterBytes)
    {
        _bloomFilterOffset = _fs.Position;
        _bw.Write((uint)filterBytes.Length);
        _bw.Write(filterBytes);
    }

    public SegmentInfo Finalise(NodeId nodeId, SegmentId segmentId)
    {
        _blockIndexOffset = _fs.Position;
        _bw.Write((uint)_blockIndex.Count);
        foreach (var (offset, firstId) in _blockIndex)
        {
            _bw.Write(offset);
            _bw.Write(firstId);
        }

        long footerOffset = _fs.Position;
        _bw.Write(_invertedIndexOffset);
        _bw.Write(_trigramIndexOffset);
        _bw.Write(_bloomFilterOffset);
        _bw.Write(_blockIndexOffset);
        _bw.Write(footerOffset);
        _bw.Write(MagicFooter);

        long totalSize = _fs.Position;
        _fs.Seek(0, SeekOrigin.Begin);

        var hdr = new SegmentFileHeader
        {
            Magic              = MagicHeader,
            Version            = SegVersion,
            NodeIdValue        = nodeId.Value,
            SegmentIdValue     = segmentId.Value,
            MinTimestampTicks  = _minTimestamp == long.MaxValue ? 0 : _minTimestamp,
            MaxTimestampTicks  = _maxTimestamp == long.MinValue ? 0 : _maxTimestamp,
            EventCount         = (uint)_eventsWritten,
            MinLevelValue      = (byte)_minLevel,
            Flags              = FlagCompressed,
        };
        WriteFileHeader(hdr);

        _bw.Flush();

        return new SegmentInfo
        {
            Id                = segmentId,
            NodeId            = nodeId,
            FilePath          = _filePath,
            MinTimestampTicks = hdr.MinTimestampTicks,
            MaxTimestampTicks = hdr.MaxTimestampTicks,
            EventCount        = (uint)_eventsWritten,
            MinLevel          = _minLevel,
            CompressedBytes   = totalSize,
            UncompressedBytes = totalSize,
        };
    }

    // ── Columnar block writer ─────────────────────────────────────────────────

    private void FlushColumnarBlock(HotTierSegment hot, StringInternPool templatePool, List<int> rowIndices)
    {
        int n = rowIndices.Count;

        long  blockMinTs = long.MaxValue;
        ulong blockMinId = ulong.MaxValue;
        for (int k = 0; k < n; k++)
        {
            ref var h = ref hot.GetHeader(rowIndices[k]);
            if (h.TimestampUtcTicks < blockMinTs) blockMinTs = h.TimestampUtcTicks;
            if (h.Id < blockMinId)               blockMinId = h.Id;
        }

        var colT = new byte[n * 8];
        var colL = new byte[n];
        var colI = new byte[n * 8];
        var colTr = new byte[n * 16];  // TraceId: Hi(8) + Lo(8) per event
        var colSp = new byte[n * 8];   // SpanId: 8 bytes per event

        var tmplOffsets  = new uint[n + 1];
        var tmplBytes    = new MemoryStream(1024);
        var excOffsets   = new uint[n + 1];
        var excBytes     = new MemoryStream(256);
        var propsOffsets = new uint[n + 1];
        var propsBytes   = new MemoryStream(2048);
        var svcOffsets   = new uint[n + 1];
        var svcBytes     = new MemoryStream(256);

        ulong firstEventId = 0;

        for (int k = 0; k < n; k++)
        {
            int i = rowIndices[k];
            ref var h = ref hot.GetHeader(i);

            if (k == 0) firstEventId = h.Id;

            BinaryPrimitives.WriteInt64LittleEndian(colT.AsSpan(k * 8), h.TimestampUtcTicks - blockMinTs);
            colL[k] = (byte)h.Level;
            BinaryPrimitives.WriteUInt64LittleEndian(colI.AsSpan(k * 8), h.Id - blockMinId);

            BinaryPrimitives.WriteUInt64LittleEndian(colTr.AsSpan(k * 16),     h.TraceIdHi);
            BinaryPrimitives.WriteUInt64LittleEndian(colTr.AsSpan(k * 16 + 8), h.TraceIdLo);
            BinaryPrimitives.WriteUInt64LittleEndian(colSp.AsSpan(k * 8),      h.SpanId);

            string template = hot.GetTemplate(i) ?? templatePool.Get(h.MessageTemplatePoolIndex) ?? string.Empty;
            tmplOffsets[k]  = (uint)tmplBytes.Length;
            if (template.Length > 0)
            {
                int byteLen = Encoding.UTF8.GetByteCount(template);
                var tmp     = ArrayPool<byte>.Shared.Rent(byteLen);
                try
                {
                    int written = Encoding.UTF8.GetBytes(template, 0, template.Length, tmp, 0);
                    tmplBytes.Write(tmp, 0, written);
                }
                finally { ArrayPool<byte>.Shared.Return(tmp); }
            }

            excOffsets[k] = (uint)excBytes.Length;
            var exc = hot.GetException(i);
            if (exc is not null)
            {
                var b = exc.ToBytes();
                excBytes.Write(b, 0, b.Length);
            }

            propsOffsets[k] = (uint)propsBytes.Length;
            var props = hot.GetPropertiesPayload(i);
            if (props.Length > 0)
                propsBytes.Write(props);

            // ServiceName string column
            svcOffsets[k] = (uint)svcBytes.Length;
            string? svcName = h.ServiceNamePoolIndex >= 0
                ? templatePool.Get(h.ServiceNamePoolIndex)
                : null;
            if (!string.IsNullOrEmpty(svcName))
            {
                int byteLen = Encoding.UTF8.GetByteCount(svcName);
                var tmp     = ArrayPool<byte>.Shared.Rent(byteLen);
                try
                {
                    int written = Encoding.UTF8.GetBytes(svcName, 0, svcName.Length, tmp, 0);
                    svcBytes.Write(tmp, 0, written);
                }
                finally { ArrayPool<byte>.Shared.Return(tmp); }
            }
        }
        tmplOffsets[n]  = (uint)tmplBytes.Length;
        excOffsets[n]   = (uint)excBytes.Length;
        propsOffsets[n] = (uint)propsBytes.Length;
        svcOffsets[n]   = (uint)svcBytes.Length;

        var blk = new MemoryStream(BlockSize);
        WriteUInt32(blk, (uint)n);
        WriteInt64(blk, blockMinTs);
        WriteUInt64(blk, blockMinId);
        blk.WriteByte(9);

        WriteColumn(blk, 1, colT);
        WriteColumn(blk, 2, colL);
        WriteColumn(blk, 3, colI);
        WriteStringColumn(blk, 4, tmplOffsets, tmplBytes);
        WriteStringColumn(blk, 5, excOffsets,  excBytes);
        WriteStringColumn(blk, 6, propsOffsets, propsBytes);
        WriteColumn(blk, 7, colTr);
        WriteColumn(blk, 8, colSp);
        WriteStringColumn(blk, 9, svcOffsets, svcBytes);

        byte[] uncompressed = blk.ToArray();
        int    maxOut       = LZ4Codec.MaximumOutputSize(uncompressed.Length);
        byte[] compBuf      = ArrayPool<byte>.Shared.Rent(maxOut);
        try
        {
            int compressedLen = LZ4Codec.Encode(uncompressed, 0, uncompressed.Length, compBuf, 0, maxOut, LZ4Level.L00_FAST);

            long blockOffset = _fs.Position;
            _blockIndex.Add((blockOffset, firstEventId));

            _bw.Write((uint)uncompressed.Length);
            _bw.Write((uint)compressedLen);
            _bw.Write(compBuf, 0, compressedLen);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(compBuf);
        }
    }

    private static void WriteColumn(MemoryStream dst, byte id, byte[] payload)
    {
        dst.WriteByte(id);
        WriteUInt32(dst, (uint)payload.Length);
        dst.Write(payload, 0, payload.Length);
    }

    private static void WriteStringColumn(MemoryStream dst, byte id, uint[] offsets, MemoryStream payload)
    {
        int offsetsByteLen = offsets.Length * 4;
        int totalLen       = offsetsByteLen + (int)payload.Length;
        dst.WriteByte(id);
        WriteUInt32(dst, (uint)totalLen);

        Span<byte> tmp4 = stackalloc byte[4];
        for (int i = 0; i < offsets.Length; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(tmp4, offsets[i]);
            dst.Write(tmp4);
        }
        payload.Position = 0;
        payload.CopyTo(dst);
    }

    private static void WriteUInt32(MemoryStream s, uint v)
    {
        Span<byte> tmp = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, v);
        s.Write(tmp);
    }

    private static void WriteInt64(MemoryStream s, long v)
    {
        Span<byte> tmp = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(tmp, v);
        s.Write(tmp);
    }

    private static void WriteUInt64(MemoryStream s, ulong v)
    {
        Span<byte> tmp = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(tmp, v);
        s.Write(tmp);
    }

    private void WriteFileHeader(in SegmentFileHeader h)
    {
        _bw.Write(h.Magic);
        _bw.Write(h.Version);
        _bw.Write(h.NodeIdValue);
        _bw.Write(h.SegmentIdValue);
        _bw.Write(h.MinTimestampTicks);
        _bw.Write(h.MaxTimestampTicks);
        _bw.Write(h.EventCount);
        _bw.Write(h.MinLevelValue);
        _bw.Write(h.Flags);
        _bw.Write((byte)0);
        _bw.Write((byte)0);
    }

    public void Dispose()
    {
        _bw.Dispose();
        _fs.Dispose();
    }

    private struct SegmentFileHeader
    {
        public const int Size = 46;

        public uint    Magic;
        public ushort  Version;
        public uint    NodeIdValue;
        public ulong   SegmentIdValue;
        public long    MinTimestampTicks;
        public long    MaxTimestampTicks;
        public uint    EventCount;
        public byte    MinLevelValue;
        public byte    Flags;
    }
}
