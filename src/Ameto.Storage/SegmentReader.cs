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
    // v6: block-index entry carries the block's MinTs (time zone map).
    // v7: INDEX GROUPS — the three index sections repeat per group of blocks instead of once
    //     per file, and a group directory names their offsets and time bounds. v4-v6 files
    //     stay readable as ONE implicit group covering the whole file, so no data is wiped.
    private const ushort MinSupportedVersion = 4;
    private const ushort MaxSupportedVersion = 7;

    /// <summary>Block MinTs for a pre-v6 segment, which has no zone map — never prunes.</summary>
    private const long UnknownBlockMinTs = long.MinValue;

    /// <summary>Group-directory entry size on disk — one definition, shared with the writer.</summary>
    private const int GroupEntrySize = SegmentWriter.GroupEntrySize;

    private readonly long _blockIndexOffset;

    /// <summary>
    /// Index groups in file order. Exactly one synthetic entry for v4-v6, whose section
    /// offsets are the footer's file-level ones — so every consumer can be written against
    /// groups alone and old files need no special case beyond this constructor.
    /// </summary>
    private readonly SegmentIndexGroup[] _groups;

    /// <summary>
    /// Per-block (byte offset, min timestamp). MinTs is the v6 time zone map and is
    /// non-decreasing, because blocks are written in (ts, id) order — that ordering is
    /// what lets a windowed read skip blocks instead of decompressing them.
    /// <see cref="UnknownBlockMinTs"/> for pre-v6 files.
    /// </summary>
    private readonly (long FileOffset, long MinTs)[] _blocks;

    /// <summary>Per-block file ordinal of its first event (v5+); null for v4 segments.</summary>
    private readonly uint[]? _blockOrdinals;

    private readonly MemoryMappedFile         _mmf;
    private readonly MemoryMappedViewAccessor _view;
    private readonly long                     _fileSize;

    public SegmentInfo Info { get; }

    /// <param name="computeUncompressedBytes">
    /// When true, <see cref="SegmentInfo.UncompressedBytes"/> is the real sum of the
    /// blocks' uncompressed sizes (two 4-byte reads at each block offset, both in the
    /// same page — pages in a slice of every block). The merge planner's co-fit gate needs the honest value,
    /// so the catalog-building call sites (startup scan, replication import) pass
    /// true. Query/merge-read opens keep the default: they never read the field, and
    /// paying a page-fault walk over the whole file on every query open would undo
    /// the header+footer-only open this reader is designed around. With the default,
    /// UncompressedBytes falls back to the compressed file size (legacy value).
    /// </param>
    public static SegmentReader Open(string filePath, bool computeUncompressedBytes = false)
    {
        var fi = new FileInfo(filePath);
        if (!fi.Exists) throw new FileNotFoundException("Segment file not found", filePath);

        long fileSize = fi.Length;
        // A file too short to hold even a footer is not a segment in any version this reader
        // knows -- it is the torn write a power cut leaves behind. Named as corruption
        // (InvalidDataException) so callers that quarantine on it can tell bytes from
        // circumstance: without this guard a zero-byte file failed the MAPPING below with
        // ArgumentException and a sub-footer file failed the footer read with
        // ArgumentOutOfRangeException, neither of which names the file as the problem -- and
        // the more torn the file, the more certainly it dodged the quarantine.
        if (fileSize < 44)   // the footer is 44 bytes in every supported version
            throw new InvalidDataException(
                $"Segment file {filePath} is {fileSize} bytes -- too short to hold a segment footer; a torn or empty file.");
        var  mmf      = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        MemoryMappedViewAccessor? view = null;
        try
        {
            view = mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.Read);
            return new SegmentReader(filePath, mmf, view, fileSize, computeUncompressedBytes);
        }
        catch
        {
            view?.Dispose();
            mmf.Dispose();
            throw;
        }
    }

    private SegmentReader(string filePath, MemoryMappedFile mmf, MemoryMappedViewAccessor view, long fileSize,
                          bool computeUncompressedBytes)
    {
        _mmf      = mmf;
        _view     = view;
        _fileSize = fileSize;

        const int footerSize = 44;
        long footerStart = fileSize - footerSize;

        // The footer is parsed BEFORE the header, so its five int64 slots are read raw and
        // only interpreted once the version is known. v7 reuses slot 0 for the group
        // directory offset; v4-v6 spend slots 0-2 on the file-level section offsets.
        long slot0           = ReadInt64At(footerStart);
        long slot1           = ReadInt64At(footerStart + 8);
        long slot2           = ReadInt64At(footerStart + 16);
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

        int blockCount = ReadInt32At(_blockIndexOffset);
        _blocks        = new (long, long)[blockCount];
        _blockOrdinals = version >= 5 ? new uint[blockCount] : null;
        int stride = version >= 5 ? 20 : 16;

        // Pull the whole block index in ONE mapped read and parse it from the span. The
        // per-entry shape cost three MemoryMappedViewAccessor calls per block, so opening a
        // segment was O(segment size) in view reads — the cost that decides whether large
        // segments are cheap to open, and Open is on the query hot path.
        int idxBytes = blockCount * stride;
        byte[] rented = ArrayPool<byte>.Shared.Rent(Math.Max(idxBytes, 1));
        try
        {
            if (idxBytes > 0) _view.ReadArray(_blockIndexOffset + 4, rented, 0, idxBytes);
            var raw = rented.AsSpan(0, idxBytes);
            for (int i = 0; i < blockCount; i++)
            {
                var entry   = raw.Slice(i * stride, stride);
                long offset = BinaryPrimitives.ReadInt64LittleEndian(entry);
                // v6 stores the block's min timestamp in the slot v4/v5 spent on
                // FirstEventId (written, never read). Older files have no zone map.
                long blockMinTs = version >= 6
                    ? BinaryPrimitives.ReadInt64LittleEndian(entry.Slice(8, 8))
                    : UnknownBlockMinTs;
                if (_blockOrdinals is not null)
                    _blockOrdinals[i] = BinaryPrimitives.ReadUInt32LittleEndian(entry.Slice(16, 4));
                _blocks[i] = (offset, blockMinTs);
            }
        }
        finally { ArrayPool<byte>.Shared.Return(rented); }

        _groups = version >= 7
            ? ReadGroupDirectory(slot0, filePath)
            // v4-v6: ONE implicit group spanning the file, taking its section offsets from
            // the footer slots v7 repurposes. Everything above the reader then sees a single
            // uniform shape and no version branch survives past this constructor.
            : [new SegmentIndexGroup(0, blockCount, 0, evCount, minTs, maxTs, slot0, slot1, slot2)];

        // Honest uncompressed size: every block frame starts with a uint32
        // uncompressedSize (see ReadAllRaw) — sum them instead of reporting the
        // compressed file size. The merge planner's co-fit gate relies on this
        // to spot prop-dense segments that would overflow a tier chunk when
        // re-packed from slot 0. Same quantity SegmentWriter accumulates: event
        // blocks only, index sections excluded — the two sides must agree or a
        // segment's candidacy would flip across a restart. Opt-in (see Open):
        // only catalog-building opens pay the block walk.
        long uncompressedBytes = fileSize;
        if (computeUncompressedBytes)
        {
            uncompressedBytes = 0;
            foreach (var (blockOffset, _) in _blocks)
            {
                int u = ReadInt32At(blockOffset);
                int c = ReadInt32At(blockOffset + 4);
                // The fifth frame-read site, and the one that could not even throw: the
                // unsigned cast turned a torn negative into ~4 GB and handed it to every
                // caller of this overload -- the classification gates among them -- as a
                // healthy number the merge planner then sized batches by.
                ValidateBlockFrame(filePath, blockOffset, u, c);
                uncompressedBytes += (uint)u;
            }
        }

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
            UncompressedBytes = uncompressedBytes,
        };
    }

    /// <summary>
    /// Parses the v7 group directory in ONE mapped read, for the same reason the block index
    /// is read that way: Open is on the query hot path and must not cost view reads
    /// proportional to segment size.
    /// </summary>
    private SegmentIndexGroup[] ReadGroupDirectory(long directoryOffset, string filePath)
    {
        if (directoryOffset <= 0)
            throw new InvalidDataException($"v7 segment {filePath} has no group directory");

        int count = ReadInt32At(directoryOffset);
        if (count <= 0) return [];

        var groups = new SegmentIndexGroup[count];
        int bytes  = count * GroupEntrySize;
        byte[] rented = ArrayPool<byte>.Shared.Rent(bytes);
        try
        {
            _view.ReadArray(directoryOffset + 4, rented, 0, bytes);
            var raw = rented.AsSpan(0, bytes);
            for (int i = 0; i < count; i++)
            {
                var e = raw.Slice(i * GroupEntrySize, GroupEntrySize);
                groups[i] = new SegmentIndexGroup(
                    FirstBlock:     (int)BinaryPrimitives.ReadUInt32LittleEndian(e),
                    BlockCount:     (int)BinaryPrimitives.ReadUInt32LittleEndian(e.Slice(4, 4)),
                    FirstOrdinal:   BinaryPrimitives.ReadUInt32LittleEndian(e.Slice(8, 4)),
                    EventCount:     BinaryPrimitives.ReadUInt32LittleEndian(e.Slice(12, 4)),
                    MinTs:          BinaryPrimitives.ReadInt64LittleEndian(e.Slice(16, 8)),
                    MaxTs:          BinaryPrimitives.ReadInt64LittleEndian(e.Slice(24, 8)),
                    InvertedOffset: BinaryPrimitives.ReadInt64LittleEndian(e.Slice(32, 8)),
                    TrigramOffset:  BinaryPrimitives.ReadInt64LittleEndian(e.Slice(40, 8)),
                    BloomOffset:    BinaryPrimitives.ReadInt64LittleEndian(e.Slice(48, 8)));
            }
        }
        finally { ArrayPool<byte>.Shared.Return(rented); }
        return groups;
    }

    /// <summary>
    /// The segment's index groups, in file order. A prefilter must consult these one at a
    /// time: the whole reason v7 exists is that ONE bloom over a day prunes nothing, so the
    /// filter's selectivity now lives per group and a rejected group costs no section read.
    /// v4-v6 files expose exactly one group, so a caller needs no version branch.
    /// </summary>
    public ReadOnlySpan<SegmentIndexGroup> Groups => _groups;

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
        //
        // The sort below is REQUIRED, not defensive. Everything downstream — the
        // LowerBound block window and the single-cursor walk in DecodeColumnarBlock —
        // assumes ascending candidates, so one out-of-order pair makes the cursor step
        // past a candidate and drop a row the caller's index proved matches. The caller
        // makes no ordering promise: QueryExecutor's trigram narrowing returns a
        // HashSet's enumeration order. The clone is what lets that stay true without
        // mutating an array the caller still owns.
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

            if (SkipByWindow(idx, fromTicks, toTicks)) continue;

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

    /// <summary>
    /// True when block <paramref name="idx"/> provably holds nothing in [from, to], using
    /// the v6 zone map alone — no decompression.
    ///
    /// <para>Blocks are ordered by (ts, id), so block i's events all fall in
    /// [MinTs(i), MinTs(i+1)]. Two provable cases:</para>
    /// <list type="bullet">
    ///   <item>MinTs(i) &gt; to — every event in the block is past the window.</item>
    ///   <item>MinTs(i+1) &lt; from — every event in the block is before it. Strict &lt;,
    ///         because an event sitting exactly on `from` belongs to the window and may
    ///         live in block i.</item>
    /// </list>
    ///
    /// <para>This is an optimisation only: the per-event window check in the caller stays,
    /// so a wrong skip would be a bug rather than merely slower results — hence the
    /// conservative bounds and the pre-v6 opt-out.</para>
    /// </summary>
    private bool SkipByWindow(int idx, long fromTicks, long toTicks)
    {
        long minTs = _blocks[idx].MinTs;
        if (minTs == UnknownBlockMinTs) return false;          // pre-v6: no zone map
        if (minTs > toTicks) return true;
        if (idx + 1 < _blocks.Length)
        {
            long nextMinTs = _blocks[idx + 1].MinTs;
            if (nextMinTs != UnknownBlockMinTs && nextMinTs < fromTicks) return true;
        }
        return false;
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
        ValidateBlockFrame(Info.FilePath, blockOffset, uncompressedSize, compressedSize);

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
        ValidateBlockFrame(Info.FilePath, blockOffset, uncompressedSize, compressedSize);

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
            // Carry the msgpack through instead of decoding it here. Delivery re-serialises
            // it straight to JSON, and a filter still gets a dictionary on demand via
            // LogEvent.Properties — so an unfiltered scroll never builds one. The copy is
            // required: prPayload points into the block buffer, which goes back to the pool
            // as soon as this block is decoded.
            ReadOnlyMemory<byte> rawProps = prEnd > prStart
                ? prPayload.Slice((int)prStart, (int)(prEnd - prStart)).ToArray()
                : default;

            list.Add(new LogEvent
            {
                Id              = new EventId(blockMinId + iDelta),
                Timestamp       = new DateTimeOffset(blockMinTs + tDelta, TimeSpan.Zero),
                Level           = (LogLevel)lvl,
                MessageTemplate = mt,
                Exception       = exc,
                RawProperties   = rawProps,
                TraceIdHi       = trHi,
                TraceIdLo       = trLo,
                SpanId          = spId,
                ServiceName     = svcName,
            });
        }

        return list;
    }

    // ── Raw event access (compaction) ─────────────────────────────────────────

    /// <summary>
    /// Decodes every event of the file with its RAW payloads — properties/exception bytes are
    /// not deserialised into dictionaries. <paramref name="stringDedup"/> collapses repeated
    /// template / service strings across blocks and segments.
    ///
    /// <para>WHOLE-FILE: this materialises the segment, one <c>byte[]</c> per properties
    /// payload. Compaction used to be built on it and paid ~3× the batch for it; it streams
    /// through <see cref="ReadBlockInto"/> now. What is left is the byte-parity oracle the merge
    /// tests compare against — do not put it back on a production path.</para>
    /// </summary>
    internal List<RawSegmentEvent> ReadAllRaw(Dictionary<string, string> stringDedup)
    {
        var result = new List<RawSegmentEvent>(checked((int)Info.EventCount));
        foreach (var (blockOffset, _) in _blocks)
        {
            int uncompressedSize = ReadInt32At(blockOffset);
            int compressedSize   = ReadInt32At(blockOffset + 4);
            ValidateBlockFrame(Info.FilePath, blockOffset, uncompressedSize, compressedSize);

            byte[] rentedComp   = ArrayPool<byte>.Shared.Rent(compressedSize);
            byte[] rentedUncomp = ArrayPool<byte>.Shared.Rent(uncompressedSize);
            try
            {
                _view.ReadArray(blockOffset + 8, rentedComp, 0, compressedSize);
                if (LZ4Codec.Decode(rentedComp, 0, compressedSize, rentedUncomp, 0, uncompressedSize) != uncompressedSize)
                    throw new InvalidDataException($"Corrupt block at {blockOffset} in {Info.FilePath}");
                DecodeColumnarBlockRaw(rentedUncomp.AsSpan(0, uncompressedSize), result, stringDedup);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedComp);
                ArrayPool<byte>.Shared.Return(rentedUncomp);
            }
        }
        return result;
    }

    // ── Block streaming (compaction) ──────────────────────────────────────────

    /// <summary>Blocks in the file, in (ts, id) order — the unit a merge reads one at a time.</summary>
    internal int BlockCount => _blocks.Length;

    /// <summary>
    /// The stored block sizes are STRUCTURE and are validated as such, before either is
    /// handed to the pool. A torn four-byte field is uniform: about half come out negative
    /// and used to surface as ArgumentOutOfRangeException from Rent, the rest as absurd
    /// positives that surfaced as OutOfMemoryException -- both BEFORE the LZ4 length check
    /// that would have said InvalidDataException, so exactly the corruption the merge
    /// classifier's docstring names ("a block whose stored length does not match its
    /// frame") was classified as circumstance by every caller that quarantines on it.
    /// Called at every site that reads a block frame -- five, including the constructor's
    /// uncompressed-bytes sum, which used to cast a torn negative to a ~4 GB unsigned and
    /// return it as SegmentInfo.UncompressedBytes without throwing at all.
    /// </summary>
    private void ValidateBlockFrame(string filePath, long blockOffset, int uncompressedSize, int compressedSize)
    {
        // BOTH fields are bounded. compressedSize by the file that must contain the block;
        // uncompressedSize by LZ4's own maximum expansion -- a block cannot decompress to more
        // than 255x its compressed bytes, so a stored ratio above that is not a large block, it
        // is a torn field. The first version of this validator bounded only compressedSize, and
        // an absurd POSITIVE uncompressedSize sailed through to Rent as an
        // OutOfMemoryException -- classified as circumstance, retried forever.
        if (uncompressedSize <= 0 || compressedSize <= 0
            || blockOffset + 8 + compressedSize > _fileSize
            || uncompressedSize > 255L * compressedSize
            // The ratio stops binding once 255 * compressedSize outgrows int.MaxValue -- a
            // block compressed past ~8.4 MB (one near-chunk-sized incompressible event, which
            // Ingestion:MaxEventPayloadBytes can be configured to admit) lets ANY torn positive
            // int through the ratio test. The writer cannot produce a block above one hot-tier
            // chunk of payload plus framing, so twice the chunk is a ceiling no honest block
            // reaches and no torn word survives.
            || uncompressedSize > 2 * HotTierSegment.ChunkPayloadBytes)
            throw new InvalidDataException(
                $"Block at {blockOffset} in {filePath} stores sizes " +
                $"{uncompressedSize}/{compressedSize} that do not fit the file -- a torn block frame.");
    }

    /// <summary>
    /// Decompresses one block into <paramref name="buffer"/>, growing it from
    /// <see cref="ArrayPool{T}.Shared"/> when needed, and returns the uncompressed length.
    ///
    /// <para>This is what lets compaction read a segment as a STREAM: <see cref="ReadAllRaw"/>
    /// materialises every event of the file (plus a <c>byte[]</c> per properties payload) before
    /// the caller sees the first one, so a merge's peak was ~3× its batch and the batch had to be
    /// capped at 32 MB. One block at a time is ~64 KB, whatever the segment's size.</para>
    /// </summary>
    /// <summary>
    /// Test seam for the grow path's rent. The rent-before-return ordering below is a real
    /// safety property (see the comment in the grow), but the frame validator now intercepts
    /// every poisoned size before the pool is consulted, so no FILE can drive the rent to
    /// throw any more -- only genuine allocation failure can, and a test cannot arrange that.
    /// The seam is the honest replacement: a validated, honest frame reaches the grow, and the
    /// seam fails the rent there.
    /// </summary>
    internal Func<int, byte[]> _rentBlockBuffer = static size => ArrayPool<byte>.Shared.Rent(size);

    internal int ReadBlockInto(int index, ref byte[] buffer)
    {
        long blockOffset     = _blocks[index].FileOffset;
        int uncompressedSize = ReadInt32At(blockOffset);
        int compressedSize   = ReadInt32At(blockOffset + 4);
        ValidateBlockFrame(Info.FilePath, blockOffset, uncompressedSize, compressedSize);

        if (buffer.Length < uncompressedSize)
        {
            // RENT BEFORE RETURNING. `buffer` is the caller's own field taken by ref —
            // SegmentEventCursor._block — so returning first and renting second leaves a window
            // where the caller still references an array that is already back in the pool. If the
            // rent throws (a large block under memory pressure is the realistic way), the cursor
            // survives long enough to be disposed and returns that same array a second time.
            // A double return does not fault: the pool simply stacks the array twice and hands it
            // to two renters at once, so the failure is one buffer's contents appearing in an
            // unrelated reader's block. Holding both for the length of a copy costs one block.
            byte[] grown = _rentBlockBuffer(uncompressedSize);
            if (buffer.Length > 0) ArrayPool<byte>.Shared.Return(buffer);
            buffer = grown;
        }

        byte[] comp = ArrayPool<byte>.Shared.Rent(compressedSize);
        try
        {
            _view.ReadArray(blockOffset + 8, comp, 0, compressedSize);
            if (LZ4Codec.Decode(comp, 0, compressedSize, buffer, 0, uncompressedSize) != uncompressedSize)
                throw new InvalidDataException($"Corrupt block {index} at {blockOffset} in {Info.FilePath}");
        }
        finally { ArrayPool<byte>.Shared.Return(comp); }

        return uncompressedSize;
    }

    private static void DecodeColumnarBlockRaw(
        ReadOnlySpan<byte> span, List<RawSegmentEvent> result, Dictionary<string, string> dedup)
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
                default: break;
            }
        }

        int offsetsBytes = (eventCount + 1) * 4;
        var mtOffsets    = colMt.Slice(0, offsetsBytes);
        var mtPayload    = colMt.Slice(offsetsBytes);
        var exOffsets    = colEx.Slice(0, offsetsBytes);
        var exPayload    = colEx.Slice(offsetsBytes);
        var prOffsets    = colPr.Slice(0, offsetsBytes);
        var prPayload    = colPr.Slice(offsetsBytes);
        var svcOffsets   = colSvc.Length >= offsetsBytes ? colSvc.Slice(0, offsetsBytes) : default;
        var svcPayload   = colSvc.Length >= offsetsBytes ? colSvc.Slice(offsetsBytes) : default;

        for (int i = 0; i < eventCount; i++)
        {
            long  tDelta = BinaryPrimitives.ReadInt64LittleEndian (colT.Slice(i * 8, 8));
            byte  lvl    = colL[i];
            ulong iDelta = BinaryPrimitives.ReadUInt64LittleEndian(colI.Slice(i * 8, 8));

            ulong trHi = colTr.Length >= (i + 1) * 16 ? BinaryPrimitives.ReadUInt64LittleEndian(colTr.Slice(i * 16, 8)) : 0;
            ulong trLo = colTr.Length >= (i + 1) * 16 ? BinaryPrimitives.ReadUInt64LittleEndian(colTr.Slice(i * 16 + 8, 8)) : 0;
            ulong spId = colSp.Length >= (i + 1) * 8  ? BinaryPrimitives.ReadUInt64LittleEndian(colSp.Slice(i * 8, 8)) : 0;

            uint mtStart = BinaryPrimitives.ReadUInt32LittleEndian(mtOffsets.Slice(i * 4, 4));
            uint mtEnd   = BinaryPrimitives.ReadUInt32LittleEndian(mtOffsets.Slice((i + 1) * 4, 4));
            string mt = string.Empty;
            if (mtEnd > mtStart)
            {
                mt = Encoding.UTF8.GetString(mtPayload.Slice((int)mtStart, (int)(mtEnd - mtStart)));
                if (dedup.TryGetValue(mt, out var pooled)) mt = pooled; else dedup[mt] = mt;
            }

            string? svc = null;
            if (svcOffsets.Length > 0)
            {
                uint sStart = BinaryPrimitives.ReadUInt32LittleEndian(svcOffsets.Slice(i * 4, 4));
                uint sEnd   = BinaryPrimitives.ReadUInt32LittleEndian(svcOffsets.Slice((i + 1) * 4, 4));
                if (sEnd > sStart)
                {
                    svc = Encoding.UTF8.GetString(svcPayload.Slice((int)sStart, (int)(sEnd - sStart)));
                    if (dedup.TryGetValue(svc, out var pooled)) svc = pooled; else dedup[svc] = svc;
                }
            }

            uint exStart = BinaryPrimitives.ReadUInt32LittleEndian(exOffsets.Slice(i * 4, 4));
            uint exEnd   = BinaryPrimitives.ReadUInt32LittleEndian(exOffsets.Slice((i + 1) * 4, 4));
            ExceptionInfo? exc = exEnd > exStart
                ? ExceptionInfo.FromBytes(exPayload.Slice((int)exStart, (int)(exEnd - exStart)))
                : null;

            uint prStart = BinaryPrimitives.ReadUInt32LittleEndian(prOffsets.Slice(i * 4, 4));
            uint prEnd   = BinaryPrimitives.ReadUInt32LittleEndian(prOffsets.Slice((i + 1) * 4, 4));
            byte[]? props = prEnd > prStart ? prPayload.Slice((int)prStart, (int)(prEnd - prStart)).ToArray() : null;

            result.Add(new RawSegmentEvent
            {
                Id        = blockMinId + iDelta,
                TsTicks   = blockMinTs + tDelta,
                Level     = lvl,
                TraceIdHi = trHi,
                TraceIdLo = trLo,
                SpanId    = spId,
                Template  = mt,
                Service   = svc,
                Props     = props,
                Exception = exc,
            });
        }
    }

    // ── Raw section access ────────────────────────────────────────────────────

    // Sections belong to a GROUP, not to the file. The no-argument overloads name group 0,
    // which for a v4-v6 file (and for any segment small enough to seal in one group) is the
    // whole segment — that keeps single-group call sites unchanged.
    public byte[] ReadInvertedIndexBytes(int group = 0)  => ReadSection(_groups[group].InvertedOffset);
    public byte[] ReadTrigramIndexBytes(int group = 0)   => ReadSection(_groups[group].TrigramOffset);
    public byte[] ReadBloomFilterBytes(int group = 0)    => ReadSection(_groups[group].BloomOffset);

    // Pooled variants for the query prefilter: index sections run to several MB per
    // segment and are only needed transiently (every deserialiser copies out of the
    // span), so renting kills what used to be gigabytes of short-lived arrays when
    // prefiltering hundreds of segments in parallel.
    public PooledSection RentInvertedIndexBytes(int group = 0) => RentSection(_groups[group].InvertedOffset);
    public PooledSection RentTrigramIndexBytes(int group = 0)  => RentSection(_groups[group].TrigramOffset);
    public PooledSection RentBloomFilterBytes(int group = 0)   => RentSection(_groups[group].BloomOffset);

    private byte[] ReadSection(long offset)
    {
        if (offset <= 0) return [];
        uint len  = (uint)ReadInt32At(offset);
        var  data = new byte[len];
        _view.ReadArray(offset + 4, data, 0, (int)len);
        return data;
    }

    /// <summary>
    /// Pooled sections rented by any reader in this process since it started.
    ///
    /// <para>A DIAGNOSTIC, and the only way from outside to tell how many times a query read a
    /// group's sections — which is a question worth being able to ask, because renting the same
    /// section twice is invisible in every other instrument. Below 1 MB the pool absorbs the
    /// second rent and the cost hides in noise; above it <see cref="ArrayPool{T}.Shared"/> stops
    /// pooling altogether and serves each rent from a fresh LOH allocation that Return then drops,
    /// so the same redundant call goes from free to the most expensive thing on the prefilter
    /// path — once per group, per segment, in parallel across the whole catalog. The counter
    /// costs one interlocked increment against a read of the section's whole length out of a
    /// mapped view.</para>
    /// </summary>
    internal static long PooledSectionRents;

    private PooledSection RentSection(long offset)
    {
        if (offset <= 0) return default;
        uint len  = (uint)ReadInt32At(offset);
        var  data = ArrayPool<byte>.Shared.Rent((int)len);
        _view.ReadArray(offset + 4, data, 0, (int)len);
        Interlocked.Increment(ref PooledSectionRents);
        return new PooledSection(data, (int)len);
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

/// <summary>
/// One index group of a segment: a run of consecutive blocks plus the inverted/trigram/bloom
/// sections built over exactly those blocks' events.
///
/// <para><paramref name="FirstOrdinal"/> and <paramref name="EventCount"/> delimit the group's
/// rows in FILE ordinals — the same space posting lists use — so a candidate array needs no
/// rebasing when it crosses a group boundary. <paramref name="MinTs"/>/<paramref name="MaxTs"/>
/// bound the group's events exactly, making the directory a coarse time zone map above the
/// block-level one: a windowed query drops a whole group's index sections without reading
/// them.</para>
///
/// <para>A section offset of 0 means the section is absent (no index was built for the group),
/// which readers must treat as NO INFORMATION rather than "no matches".</para>
/// </summary>
public readonly record struct SegmentIndexGroup(
    int  FirstBlock,
    int  BlockCount,
    uint FirstOrdinal,
    uint EventCount,
    long MinTs,
    long MaxTs,
    long InvertedOffset,
    long TrigramOffset,
    long BloomOffset);

/// <summary>
/// One event decoded with its raw payloads intact; properties stay msgpack bytes (no dictionary
/// roundtrip). The materialised counterpart of <see cref="SegmentEventRef"/>, which is what the
/// streaming paths use — this one owns its bytes and so costs an allocation per event.
/// </summary>
internal struct RawSegmentEvent
{
    public ulong          Id;
    public long           TsTicks;
    public byte           Level;
    public ulong          TraceIdHi, TraceIdLo, SpanId;
    public string         Template;
    public string?        Service;
    public byte[]?        Props;
    public ExceptionInfo? Exception;
}

/// <summary>
/// A segment section backed by a pooled buffer. Dispose returns the buffer to
/// <see cref="ArrayPool{T}.Shared"/> — consume the <see cref="Span"/> first (every
/// index deserialiser copies out of it, so disposal right after deserialise is safe).
/// </summary>
public readonly struct PooledSection : IDisposable
{
    private readonly byte[]? _rented;
    private readonly int     _length;

    internal PooledSection(byte[]? rented, int length)
    {
        _rented = rented;
        _length = length;
    }

    public ReadOnlySpan<byte> Span => _rented is null ? default : _rented.AsSpan(0, _length);

    public void Dispose()
    {
        if (_rented is not null) ArrayPool<byte>.Shared.Return(_rented);
    }
}
