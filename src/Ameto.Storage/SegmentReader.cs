using System.Buffers;
using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Text;
using K4os.Compression.LZ4;
using Ameto.Core;
using Ameto.Core.Serialization;

namespace Ameto.Storage;

/// <summary>
/// Reads events from a cold-tier .seg file (v3, columnar) using memory-mapped I/O.
/// Older v1/v2 files are rejected — the data directory must be deleted on upgrade.
/// </summary>
public sealed class SegmentReader : ISegmentReader
{
    private const uint   MagicHeader      = 0x52_44_4C_47;
    private const uint   MagicFooter      = 0x52_44_46_54;
    // v4: TraceId/SpanId/ServiceName columns; block-index entry = (offset, firstId).
    // v5: block-index entry gains FirstOrdinal (file-order position of the block's first
    //     event) and index posting lists store file ordinals — enables candidate-driven
    //     block/row skipping. v4 segments remain readable (full scan, no skipping).
    private const ushort MinSupportedVersion = 4;
    private const ushort MaxSupportedVersion = 5;

    private readonly long _invertedIndexOffset;
    private readonly long _trigramIndexOffset;
    private readonly long _bloomFilterOffset;
    private readonly long _blockIndexOffset;

    private readonly (long FileOffset, ulong FirstEventId)[] _blocks;

    /// <summary>Per-block file ordinal of its first event (v5+); null for v4 segments.</summary>
    private readonly uint[]? _blockOrdinals;

    private readonly MemoryMappedFile         _mmf;
    private readonly MemoryMappedViewAccessor _view;
    private readonly long                     _fileSize;

    public SegmentInfo Info { get; }

    public static SegmentReader Open(string filePath)
    {
        var fi = new FileInfo(filePath);
        if (!fi.Exists) throw new FileNotFoundException("Segment file not found", filePath);

        long fileSize = fi.Length;
        var  mmf      = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        MemoryMappedViewAccessor? view = null;
        try
        {
            view = mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.Read);
            return new SegmentReader(filePath, mmf, view, fileSize);
        }
        catch
        {
            view?.Dispose();
            mmf.Dispose();
            throw;
        }
    }

    private SegmentReader(string filePath, MemoryMappedFile mmf, MemoryMappedViewAccessor view, long fileSize)
    {
        _mmf      = mmf;
        _view     = view;
        _fileSize = fileSize;

        const int footerSize = 44;
        long footerStart = fileSize - footerSize;

        _invertedIndexOffset = ReadInt64At(footerStart);
        _trigramIndexOffset  = ReadInt64At(footerStart + 8);
        _bloomFilterOffset   = ReadInt64At(footerStart + 16);
        _blockIndexOffset    = ReadInt64At(footerStart + 24);
        uint footerMagic     = (uint)ReadInt32At(footerStart + 40);
        if (footerMagic != MagicFooter)
            throw new InvalidDataException($"Segment footer magic mismatch in {filePath}");

        uint   magic    = (uint)ReadInt32At(0);
        ushort version  = (ushort)ReadInt16At(4);
        uint   nodeId   = (uint)ReadInt32At(6);
        ulong  segId    = (ulong)ReadInt64At(10);
        long   minTs    = ReadInt64At(18);
        long   maxTs    = ReadInt64At(26);
        uint   evCount  = (uint)ReadInt32At(34);
        byte   minLevel = ReadByteAt(38);

        if (magic != MagicHeader)
            throw new InvalidDataException($"Segment header magic mismatch in {filePath}");
        if (version is < MinSupportedVersion or > MaxSupportedVersion)
            throw new InvalidDataException($"Unsupported segment version {version} in {filePath}; expected {MinSupportedVersion}-{MaxSupportedVersion}. Delete the data directory and restart.");

        Info = new SegmentInfo
        {
            Id                = new SegmentId(segId),
            NodeId            = new NodeId(nodeId),
            FilePath          = filePath,
            MinTimestampTicks = minTs,
            MaxTimestampTicks = maxTs,
            EventCount        = evCount,
            MinLevel          = (LogLevel)minLevel,
            CompressedBytes   = fileSize,
            UncompressedBytes = fileSize,
        };

        int blockCount = ReadInt32At(_blockIndexOffset);
        _blocks        = new (long, ulong)[blockCount];
        _blockOrdinals = version >= 5 ? new uint[blockCount] : null;
        long pos    = _blockIndexOffset + 4;
        int  stride = version >= 5 ? 20 : 16;
        for (int i = 0; i < blockCount; i++)
        {
            long  offset  = ReadInt64At(pos);
            ulong firstId = (ulong)ReadInt64At(pos + 8);
            if (_blockOrdinals is not null)
                _blockOrdinals[i] = (uint)ReadInt32At(pos + 16);
            _blocks[i] = (offset, firstId);
            pos += stride;
        }
    }

    public async IAsyncEnumerable<LogEvent> ReadEventsAsync(
        uint[]? candidateOffsets,
        DateTimeOffset? from,
        DateTimeOffset? to,
        bool reversed = false,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        long fromTicks = from?.UtcTicks ?? long.MinValue;
        long toTicks   = to?.UtcTicks   ?? long.MaxValue;

        // v5 + index candidates: posting-list offsets are file ordinals, so we can skip
        // whole blocks with no candidate and, inside a touched block, materialise ONLY the
        // candidate rows — the dominant query-path saving (no Dictionary/string per
        // rejected event). Candidates may arrive unsorted (trigram set → array).
        uint[]? cands = null;
        if (candidateOffsets is { Length: > 0 } && _blockOrdinals is not null)
        {
            cands = (uint[])candidateOffsets.Clone();
            Array.Sort(cands);
        }

        int blockCount = _blocks.Length;
        for (int bi = 0; bi < blockCount; bi++)
        {
            int idx = reversed ? blockCount - 1 - bi : bi;
            ct.ThrowIfCancellationRequested();

            int candStart = 0, candEnd = 0;
            if (cands is not null)
            {
                uint first = _blockOrdinals![idx];
                uint next  = idx + 1 < blockCount ? _blockOrdinals[idx + 1] : Info.EventCount;
                candStart  = LowerBound(cands, first);
                candEnd    = LowerBound(cands, next);
                if (candStart == candEnd) continue;   // no candidate rows in this block
            }

            var events = cands is null
                ? ReadBlock(_blocks[idx].FileOffset, reversed)
                : ReadBlock(_blocks[idx].FileOffset, reversed, cands, candStart, candEnd, _blockOrdinals![idx]);
            foreach (var evt in events)
            {
                long ts = evt.Timestamp.UtcTicks;
                if (ts < fromTicks || ts > toTicks) continue;
                yield return evt;
            }
        }
        await Task.CompletedTask;
    }

    /// <summary>First index in ascending <paramref name="a"/> whose value is ≥ <paramref name="key"/>.</summary>
    private static int LowerBound(uint[] a, uint key)
    {
        int lo = 0, hi = a.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (a[mid] < key) lo = mid + 1; else hi = mid;
        }
        return lo;
    }

    // ── Header-only aggregation ─────────────────────────────────────────────────

    /// <summary>
    /// Counts events into <paramref name="agg"/> by decoding only the timestamp (@t), level (@l)
    /// and service (service.name) columns of each block. The message-template, exception and
    /// properties columns are skipped entirely — no UTF-8 template decode, no MessagePack
    /// deserialisation, no <see cref="LogEvent"/> allocation. The block still has to be
    /// LZ4-decompressed (all columns share one compressed frame in the v4 format), but that is a
    /// fraction of the cost of full materialisation.
    /// </summary>
    public void AggregateHeaders(LogVolumeAggregator agg, long fromTicks, long toTicks)
    {
        // Whole-segment fast reject on the file header's min/max timestamps.
        if (Info.MaxTimestampTicks < fromTicks || Info.MinTimestampTicks > toTicks) return;

        foreach (var (blockOffset, _) in _blocks)
            AggregateBlockHeaders(blockOffset, agg, fromTicks, toTicks);
    }

    private void AggregateBlockHeaders(long blockOffset, LogVolumeAggregator agg, long fromTicks, long toTicks)
    {
        int uncompressedSize = ReadInt32At(blockOffset);
        int compressedSize   = ReadInt32At(blockOffset + 4);

        byte[]? rentedComp   = null;
        byte[]? rentedUncomp = null;
        try
        {
            rentedComp   = ArrayPool<byte>.Shared.Rent(compressedSize);
            rentedUncomp = ArrayPool<byte>.Shared.Rent(uncompressedSize);

            _view.ReadArray(blockOffset + 8, rentedComp, 0, compressedSize);

            int decoded = LZ4Codec.Decode(rentedComp, 0, compressedSize, rentedUncomp, 0, uncompressedSize);
            if (decoded != uncompressedSize) return;

            ScanBlockHeaders(rentedUncomp.AsSpan(0, decoded), agg, fromTicks, toTicks);
        }
        finally
        {
            if (rentedComp   is not null) ArrayPool<byte>.Shared.Return(rentedComp);
            if (rentedUncomp is not null) ArrayPool<byte>.Shared.Return(rentedUncomp);
        }
    }

    /// <summary>Parses a decompressed columnar block, extracting only @t / @l / service.name.</summary>
    private static void ScanBlockHeaders(ReadOnlySpan<byte> span, LogVolumeAggregator agg, long fromTicks, long toTicks)
    {
        int   pos        = 0;
        int   eventCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(pos, 4)); pos += 4;
        long  blockMinTs = BinaryPrimitives.ReadInt64LittleEndian (span.Slice(pos, 8));      pos += 8;
        pos += 8; // skip blockMinId — @i is not needed for counting
        byte  colCount   = span[pos]; pos += 1;

        ReadOnlySpan<byte> colT = default, colL = default, colSvc = default;
        for (int c = 0; c < colCount; c++)
        {
            byte id      = span[pos]; pos += 1;
            int  byteLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(pos, 4)); pos += 4;
            var  data    = span.Slice(pos, byteLen); pos += byteLen;
            switch (id)
            {
                case 1: colT   = data; break;   // @t  int64 deltas
                case 2: colL   = data; break;   // @l  byte levels
                case 9: colSvc = data; break;   // service.name string column
                default: break;                 // @i/@mt/@x/props/@tr/@sp: skipped, never decoded
            }
        }

        // service.name is a string column: (eventCount+1) uint32 offsets, then the UTF-8 payload.
        int  offsetsBytes = (eventCount + 1) * 4;
        bool hasSvc       = colSvc.Length >= offsetsBytes;
        var  svcOffsets   = hasSvc ? colSvc.Slice(0, offsetsBytes) : default;
        var  svcPayload   = hasSvc ? colSvc.Slice(offsetsBytes)    : default;

        for (int i = 0; i < eventCount; i++)
        {
            long ts = blockMinTs + BinaryPrimitives.ReadInt64LittleEndian(colT.Slice(i * 8, 8));
            if (ts < fromTicks || ts > toTicks) continue;

            var level = (LogLevel)colL[i];

            ReadOnlySpan<byte> svc = default;
            if (hasSvc)
            {
                uint s = BinaryPrimitives.ReadUInt32LittleEndian(svcOffsets.Slice(i * 4, 4));
                uint e = BinaryPrimitives.ReadUInt32LittleEndian(svcOffsets.Slice((i + 1) * 4, 4));
                if (e > s) svc = svcPayload.Slice((int)s, (int)(e - s));
            }

            agg.AddByServiceUtf8(ts, level, svc);
        }
    }

    // ── Block reading ─────────────────────────────────────────────────────────

    private IEnumerable<LogEvent> ReadBlock(
        long blockOffset, bool reversed,
        uint[]? cands = null, int candStart = 0, int candEnd = 0, uint firstOrdinal = 0)
    {
        int uncompressedSize = ReadInt32At(blockOffset);
        int compressedSize   = ReadInt32At(blockOffset + 4);

        byte[]? rentedComp   = null;
        byte[]? rentedUncomp = null;
        List<LogEvent> events;
        try
        {
            rentedComp   = ArrayPool<byte>.Shared.Rent(compressedSize);
            rentedUncomp = ArrayPool<byte>.Shared.Rent(uncompressedSize);

            _view.ReadArray(blockOffset + 8, rentedComp, 0, compressedSize);

            int decoded = LZ4Codec.Decode(rentedComp, 0, compressedSize, rentedUncomp, 0, uncompressedSize);
            if (decoded != uncompressedSize)
                yield break;

            events = DecodeColumnarBlock(rentedUncomp.AsSpan(0, decoded), cands, candStart, candEnd, firstOrdinal);
        }
        finally
        {
            if (rentedComp   is not null) ArrayPool<byte>.Shared.Return(rentedComp);
            if (rentedUncomp is not null) ArrayPool<byte>.Shared.Return(rentedUncomp);
        }

        if (reversed) events.Reverse();
        foreach (var ev in events) yield return ev;
    }

    /// <summary>
    /// Decodes a columnar block into <see cref="LogEvent"/>s. When <paramref name="cands"/>
    /// is non-null, only rows whose file ordinal (<paramref name="firstOrdinal"/> + row) is in
    /// cands[candStart..candEnd) are materialised — rejected rows cost a couple of integer
    /// comparisons, no Dictionary / string / ExceptionInfo allocation.
    /// </summary>
    private static List<LogEvent> DecodeColumnarBlock(
        ReadOnlySpan<byte> span,
        uint[]? cands = null, int candStart = 0, int candEnd = 0, uint firstOrdinal = 0)
    {
        int    pos        = 0;
        int    eventCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(pos, 4));   pos += 4;
        long   blockMinTs = BinaryPrimitives.ReadInt64LittleEndian (span.Slice(pos, 8));        pos += 8;
        ulong  blockMinId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(pos, 8));        pos += 8;
        byte   colCount   = span[pos]; pos += 1;

        ReadOnlySpan<byte> colT  = default, colL = default, colI = default;
        ReadOnlySpan<byte> colMt = default, colEx = default, colPr = default;
        ReadOnlySpan<byte> colTr = default, colSp = default, colSvc = default;

        for (int c = 0; c < colCount; c++)
        {
            byte id      = span[pos]; pos += 1;
            int  byteLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(pos, 4)); pos += 4;
            var  data    = span.Slice(pos, byteLen); pos += byteLen;
            switch (id)
            {
                case 1: colT   = data; break;
                case 2: colL   = data; break;
                case 3: colI   = data; break;
                case 4: colMt  = data; break;
                case 5: colEx  = data; break;
                case 6: colPr  = data; break;
                case 7: colTr  = data; break;
                case 8: colSp  = data; break;
                case 9: colSvc = data; break;
                default: break; // unknown columns ignored for forward compat
            }
        }

        // For string columns: first (n+1)*4 bytes are offsets, rest is payload
        int offsetsBytes = (eventCount + 1) * 4;
        var mtOffsets    = colMt.Slice(0, offsetsBytes);
        var mtPayload    = colMt.Slice(offsetsBytes);
        var exOffsets    = colEx.Slice(0, offsetsBytes);
        var exPayload    = colEx.Slice(offsetsBytes);
        var prOffsets    = colPr.Slice(0, offsetsBytes);
        var prPayload    = colPr.Slice(offsetsBytes);
        var svcOffsets   = colSvc.Length >= offsetsBytes ? colSvc.Slice(0, offsetsBytes) : default;
        var svcPayload   = colSvc.Length >= offsetsBytes ? colSvc.Slice(offsetsBytes) : default;

        var list = new List<LogEvent>(cands is null ? eventCount : candEnd - candStart);
        int cp = candStart;   // two-pointer walk: rows ascend, cands sorted ascending
        for (int i = 0; i < eventCount; i++)
        {
            if (cands is not null)
            {
                uint ord = firstOrdinal + (uint)i;
                while (cp < candEnd && cands[cp] < ord) cp++;
                if (cp >= candEnd) break;             // no candidates left in this block
                if (cands[cp] != ord) continue;       // this row is not a candidate
            }

            long  tDelta = BinaryPrimitives.ReadInt64LittleEndian (colT.Slice(i * 8, 8));
            byte  lvl    = colL[i];
            ulong iDelta = BinaryPrimitives.ReadUInt64LittleEndian(colI.Slice(i * 8, 8));

            ulong trHi = colTr.Length >= (i + 1) * 16
                ? BinaryPrimitives.ReadUInt64LittleEndian(colTr.Slice(i * 16, 8))
                : 0;
            ulong trLo = colTr.Length >= (i + 1) * 16
                ? BinaryPrimitives.ReadUInt64LittleEndian(colTr.Slice(i * 16 + 8, 8))
                : 0;
            ulong spId = colSp.Length >= (i + 1) * 8
                ? BinaryPrimitives.ReadUInt64LittleEndian(colSp.Slice(i * 8, 8))
                : 0;

            string? svcName = null;
            if (svcOffsets.Length > 0)
            {
                uint sStart = BinaryPrimitives.ReadUInt32LittleEndian(svcOffsets.Slice(i * 4, 4));
                uint sEnd   = BinaryPrimitives.ReadUInt32LittleEndian(svcOffsets.Slice((i + 1) * 4, 4));
                if (sEnd > sStart)
                    svcName = Encoding.UTF8.GetString(svcPayload.Slice((int)sStart, (int)(sEnd - sStart)));
            }

            uint mtStart = BinaryPrimitives.ReadUInt32LittleEndian(mtOffsets.Slice(i * 4, 4));
            uint mtEnd   = BinaryPrimitives.ReadUInt32LittleEndian(mtOffsets.Slice((i + 1) * 4, 4));
            string mt    = mtEnd > mtStart ? Encoding.UTF8.GetString(mtPayload.Slice((int)mtStart, (int)(mtEnd - mtStart))) : string.Empty;

            uint exStart = BinaryPrimitives.ReadUInt32LittleEndian(exOffsets.Slice(i * 4, 4));
            uint exEnd   = BinaryPrimitives.ReadUInt32LittleEndian(exOffsets.Slice((i + 1) * 4, 4));
            ExceptionInfo? exc = exEnd > exStart
                ? ExceptionInfo.FromBytes(exPayload.Slice((int)exStart, (int)(exEnd - exStart)))
                : null;

            uint prStart = BinaryPrimitives.ReadUInt32LittleEndian(prOffsets.Slice(i * 4, 4));
            uint prEnd   = BinaryPrimitives.ReadUInt32LittleEndian(prOffsets.Slice((i + 1) * 4, 4));
            Dictionary<string, object?>? props = prEnd > prStart
                ? LogEventSerializer.DeserializePropertiesMap(prPayload.Slice((int)prStart, (int)(prEnd - prStart)))
                : null;

            list.Add(new LogEvent
            {
                Id              = new EventId(blockMinId + iDelta),
                Timestamp       = new DateTimeOffset(blockMinTs + tDelta, TimeSpan.Zero),
                Level           = (LogLevel)lvl,
                MessageTemplate = mt,
                Exception       = exc,
                Properties      = props,
                TraceIdHi       = trHi,
                TraceIdLo       = trLo,
                SpanId          = spId,
                ServiceName     = svcName,
            });
        }

        return list;
    }

    // ── Raw section access ────────────────────────────────────────────────────

    public byte[] ReadInvertedIndexBytes()  => ReadSection(_invertedIndexOffset);
    public byte[] ReadTrigramIndexBytes()   => ReadSection(_trigramIndexOffset);
    public byte[] ReadBloomFilterBytes()    => ReadSection(_bloomFilterOffset);

    private byte[] ReadSection(long offset)
    {
        if (offset <= 0) return [];
        uint len  = (uint)ReadInt32At(offset);
        var  data = new byte[len];
        _view.ReadArray(offset + 4, data, 0, (int)len);
        return data;
    }

    private int   ReadInt32At(long offset)  { int   v = 0; _view.Read(offset, out v); return v; }
    private long  ReadInt64At(long offset)  { long  v = 0; _view.Read(offset, out v); return v; }
    private short ReadInt16At(long offset)  { short v = 0; _view.Read(offset, out v); return v; }
    private byte  ReadByteAt(long offset)   { byte  v = 0; _view.Read(offset, out v); return v; }

    private bool _disposed;
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _view.Dispose();
        _mmf.Dispose();
    }
}
