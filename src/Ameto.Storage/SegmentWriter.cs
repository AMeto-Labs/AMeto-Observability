using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Builds the three index sections for ONE index group: the events at file ordinals
/// <c>[firstOrdinal, firstOrdinal + eventCount)</c>, i.e. <c>order[firstOrdinal..]</c>.
/// Injected by <see cref="StorageEngine"/> so the writer can seal a group the moment its
/// payload budget is reached, without Ameto.Storage referencing the indexing layer.
///
/// <para>The implementation MUST emit posting-list offsets as FILE ordinals (base
/// <paramref name="firstOrdinal"/>, not 0) — see the ordinal contract on
/// <see cref="SegmentWriter"/>.</para>
/// </summary>
public delegate (byte[] Inverted, byte[] Trigram, byte[] Bloom) SegmentGroupIndexBuilder(
    int firstOrdinal, int eventCount);

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
///
/// v7 file layout:
///   header (46 B)
///   group 0: blocks… | inverted | trigram | bloom
///   group 1: blocks… | inverted | trigram | bloom
///   …
///   group directory: uint32 count + entries[56 B]
///     { uint32 firstBlock, blockCount, firstOrdinal, eventCount;
///       int64 minTs, maxTs, invertedOff, trigramOff, bloomOff }   (offset 0 = absent)
///   block index: uint32 count + entries[20 B]                     (unchanged since v6)
///   footer (44 B)
/// </summary>
public sealed class SegmentWriter : IDisposable
{
    private const uint   MagicHeader = 0x52_44_4C_47; // "RDLG"
    private const uint   MagicFooter = 0x52_44_46_54; // "RDFT"
    // v7: INDEX GROUPS. The three index sections are no longer one-per-file; the file is cut
    // into groups of blocks, each carrying its own inverted/trigram/bloom section and its own
    // time bounds. This decouples INDEX granularity from FILE granularity, which is what makes
    // a day-scale segment possible at all: the trigram accumulator costs ~7.6 B per posting and
    // scales with indexed text bytes, so one day of one level would need ~610 MB of managed
    // build state, and one bloom over 24 h is saturated enough to prune nothing.
    // v6: the block index carries the block's MIN TIMESTAMP in the slot v5 spent on
    // FirstEventId — which was written by every flush and read by nothing. That makes the
    // block index a time zone map, so a windowed query seeks to its blocks instead of
    // decompressing the file. Without it a segment's own Min/MaxTimestamp is the ONLY time
    // index there is, which is why segments have to stay small to be queryable.
    private const ushort SegVersion  = 7;             // v6: block-index MinTs zone map; v5: FirstOrdinal; v4: + TraceId/SpanId/ServiceName columns
    private const int    BlockSize   = 64 * 1024;      // 64 KB target uncompressed block size

    /// <summary>
    /// Uncompressed block bytes a group accumulates before it is sealed and its indexes built.
    ///
    /// <para>Set to the default hot-tier size deliberately. <c>StorageEngine</c> budgets
    /// <c>IndexBuildBytesPerEvent = 1400</c> managed bytes per in-flight event and sizes flush
    /// concurrency so that a 64 MB tier (~131 k events ⇒ ~184 MB of accumulators) fits three
    /// deep inside <c>FlushManagedBudgetBytes</c>. Sealing on the same quantity means the peak
    /// index-build state of a DAY-scale segment equals that of one of today's flushes — the
    /// budget that is already measured and already enforced. A larger group would silently
    /// break that ceiling; a smaller one would only cost extra sections and per-group bloom
    /// overhead for no reduction the flush path can spend.</para>
    /// </summary>
    public const long DefaultGroupPayloadBudgetBytes = 64L * 1024 * 1024;

    /// <summary>Group-directory entry size on disk (4×uint32 + 2×int64 bounds + 3×int64 offsets).</summary>
    internal const int GroupEntrySize = 56;

    public  const byte   FlagCompressed = 0x01;

    private readonly string       _filePath;
    private readonly FileStream   _fs;
    private readonly BinaryWriter _bw;
    private readonly long         _groupBudget;

    // v6 block-index entry: byte offset of the block, the block's MIN timestamp, and the
    // ordinal (file-order position, 0-based) of the block's first event. Index posting
    // lists store those ordinals, so the reader can map candidates → block → row; the
    // timestamp lets it skip whole blocks outside a query window without decompressing.
    // Blocks are written in (ts, id) order, so MinTs is non-decreasing across the index.
    private readonly List<(long Offset, long MinTs, uint FirstOrdinal)> _blockIndex = new();
    private int _eventsFlushed;

    // ── Index groups ──────────────────────────────────────────────────────────
    // A group is a run of consecutive blocks plus the three index sections built over
    // exactly those blocks' events. Posting offsets stay FILE ordinals (see the ordinal
    // contract on WriteEvents), so the reader needs no translation and the block index —
    // which already maps file ordinal → block — keeps working unchanged.
    private readonly List<GroupEntry> _groups = new();
    private int  _groupFirstBlock;
    private uint _groupFirstOrdinal;
    private uint _groupEventCount;
    private long _groupMinTs = long.MaxValue;
    private long _groupMaxTs = long.MinValue;
    private long _groupPayloadBytes;

    // Offsets of the CURRENT (open) group's sections; folded into its directory entry when
    // the group closes and reset to 0 (= section absent) for the next one.
    private long _invertedIndexOffset;
    private long _trigramIndexOffset;
    private long _bloomFilterOffset;
    private long _blockIndexOffset;
    private long _groupDirectoryOffset;

    private int      _eventsWritten;
    // Sum of the blocks' uncompressed sizes (the uint32 each block frame starts
    // with). This is the HONEST UncompressedBytes for SegmentInfo — before this,
    // it was set to the compressed file size, which made the merge planner blind
    // to prop-dense segments whose payload re-packs to far more than their file
    // size (the compaction poison-anchor bug).
    private long     _uncompressedBytes;
    private long     _minTimestamp = long.MaxValue;
    private long     _maxTimestamp = long.MinValue;
    private LogLevel _minLevel     = LogLevel.Fatal;

    public SegmentWriter(string filePath) : this(filePath, DefaultGroupPayloadBudgetBytes) { }

    /// <param name="groupPayloadBudgetBytes">
    /// Uncompressed block bytes per index group — see <see cref="DefaultGroupPayloadBudgetBytes"/>.
    /// Only honoured by the <see cref="SegmentGroupIndexBuilder"/> overload of
    /// <see cref="WriteEvents(HotTierSegment, StringInternPool, int[], SegmentGroupIndexBuilder?)"/>;
    /// a caller that writes sections itself always produces exactly one group.
    /// </param>
    public SegmentWriter(string filePath, long groupPayloadBudgetBytes)
    {
        _filePath    = filePath;
        _groupBudget = Math.Max(1, groupPayloadBudgetBytes);
        _fs          = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.SequentialScan);
        _bw          = new BinaryWriter(_fs);
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>
    /// File write order: event indices sorted ascending by (TimestampUtcTicks, EventId).
    /// The SAME array must be passed to <see cref="WriteEvents(HotTierSegment, StringInternPool, int[])"/>
    /// and <c>SegmentIndexBuilder.Build</c> so index posting-list offsets equal file ordinals.
    ///
    /// <para>ORDINAL CONTRACT (v7): posting offsets are FILE-GLOBAL ordinals even though the
    /// indexes are now per group. Group-local offsets would buy nothing — the codec is
    /// delta-encoded, so absolute magnitude costs no bytes — and would force every candidate
    /// array to be rebased on the way out, adding a group-boundary off-by-one to the one path
    /// where a mistake silently drops rows. File-global keeps the block index (which already
    /// maps file ordinal → block) as the single translation table for candidates.</para>
    /// </summary>
    public static int[] ComputeSortOrder(HotTierSegment hot)
    {
        int count = hot.Count;
        var order = new int[count];
        for (int i = 0; i < count; i++) order[i] = i;
        Array.Sort(order, (a, b) =>
        {
            ref var ha = ref hot.GetHeader(a);
            ref var hb = ref hot.GetHeader(b);
            int c = ha.TimestampUtcTicks.CompareTo(hb.TimestampUtcTicks);
            return c != 0 ? c : ha.Id.CompareTo(hb.Id);
        });
        return order;
    }

    public void WriteEvents(HotTierSegment hot, StringInternPool templatePool)
        => WriteEvents(hot, templatePool, ComputeSortOrder(hot));

    public void WriteEvents(HotTierSegment hot, StringInternPool templatePool, int[] order)
        => WriteEvents(hot, templatePool, order, null);

    /// <param name="order">
    /// Tier indices to write, in output order. It need NOT cover the whole tier: a subset
    /// lets one tier be written as several segments, which is how a flush is split by log
    /// level so that a segment holds exactly one level. The bound is the order's length.
    /// </param>
    /// <param name="indexBuilder">
    /// When supplied, the file is cut into index groups: every time the blocks written since
    /// the last cut exceed the group budget, the builder is called for exactly those events
    /// and the three sections it returns are written right there, between the group's last
    /// block and the next group's first.
    ///
    /// <para>This is the whole point of v7 — the builder's accumulators are reset per group,
    /// so peak index-build memory is O(group) instead of O(file). Passing null keeps the
    /// pre-v7 shape: one implicit group covering the file, with sections written by the
    /// caller via <see cref="WriteInvertedIndex"/> and friends.</para>
    /// </param>
    public void WriteEvents(HotTierSegment hot, StringInternPool templatePool, int[] order,
                            SegmentGroupIndexBuilder? indexBuilder)
    {
        _fs.Seek(SegmentFileHeader.Size, SeekOrigin.Begin);

        int count = order.Length;
        if (count == 0) return;

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

                // Cut only on a block boundary: a group owns whole blocks, so the block
                // index stays a single flat array and candidate → block resolution is
                // untouched by grouping.
                if (indexBuilder is not null && _groupPayloadBytes >= _groupBudget)
                    SealGroup(indexBuilder);
            }

            batch.Add(i);
            approxBytes += rowCost;
            _eventsWritten++;
        }

        if (batch.Count > 0)
            FlushColumnarBlock(hot, templatePool, batch);

        if (indexBuilder is not null)
            SealGroup(indexBuilder);
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

    /// <summary>Builds and writes the open group's index sections, then closes it.</summary>
    private void SealGroup(SegmentGroupIndexBuilder indexBuilder)
    {
        if (_groupEventCount == 0) return;

        var (inverted, trigram, bloom) = indexBuilder((int)_groupFirstOrdinal, (int)_groupEventCount);
        WriteInvertedIndex(inverted);
        WriteTrigramIndex(trigram);
        WriteBloomFilter(bloom);
        CloseGroup();
    }

    /// <summary>
    /// Folds the open group's accumulators into a directory entry and starts the next one.
    /// The section offsets are reset to 0, which the reader reads as "absent".
    /// </summary>
    private void CloseGroup()
    {
        _groups.Add(new GroupEntry
        {
            FirstBlock     = (uint)_groupFirstBlock,
            BlockCount     = (uint)(_blockIndex.Count - _groupFirstBlock),
            FirstOrdinal   = _groupFirstOrdinal,
            EventCount     = _groupEventCount,
            MinTs          = _groupMinTs == long.MaxValue ? 0 : _groupMinTs,
            MaxTs          = _groupMaxTs == long.MinValue ? 0 : _groupMaxTs,
            InvertedOffset = _invertedIndexOffset,
            TrigramOffset  = _trigramIndexOffset,
            BloomOffset    = _bloomFilterOffset,
        });

        _groupFirstBlock     = _blockIndex.Count;
        _groupFirstOrdinal   = (uint)_eventsFlushed;
        _groupEventCount     = 0;
        _groupMinTs          = long.MaxValue;
        _groupMaxTs          = long.MinValue;
        _groupPayloadBytes   = 0;
        _invertedIndexOffset = 0;
        _trigramIndexOffset  = 0;
        _bloomFilterOffset   = 0;
    }

    public SegmentInfo Finalise(NodeId nodeId, SegmentId segmentId)
    {
        // A caller that wrote its own sections (or none) leaves one open group covering the
        // whole file — the pre-v7 shape, expressed as a one-entry directory. An empty
        // segment still gets that entry so the directory is never zero-length.
        if (_groupEventCount > 0 || _groups.Count == 0)
            CloseGroup();

        _groupDirectoryOffset = _fs.Position;
        _bw.Write((uint)_groups.Count);
        foreach (var g in _groups)
        {
            _bw.Write(g.FirstBlock);
            _bw.Write(g.BlockCount);
            _bw.Write(g.FirstOrdinal);
            _bw.Write(g.EventCount);
            _bw.Write(g.MinTs);
            _bw.Write(g.MaxTs);
            _bw.Write(g.InvertedOffset);
            _bw.Write(g.TrigramOffset);
            _bw.Write(g.BloomOffset);
        }

        _blockIndexOffset = _fs.Position;
        _bw.Write((uint)_blockIndex.Count);
        foreach (var (offset, minTs, firstOrdinal) in _blockIndex)
        {
            _bw.Write(offset);
            _bw.Write(minTs);          // v6: time zone map (v5 wrote FirstEventId here)
            _bw.Write(firstOrdinal);   // maps index posting-list offsets → block
        }

        // Footer stays 44 B — the reader parses it BEFORE it knows the version, so its SIZE
        // is load-bearing across every format. v7 reinterprets slot 0 (v4-v6: the file-level
        // inverted-index offset) as the GROUP DIRECTORY offset; slots 1 and 2, which held the
        // file-level trigram/bloom offsets, no longer name anything file-wide and are zeroed.
        long footerOffset = _fs.Position;
        _bw.Write(_groupDirectoryOffset);
        _bw.Write(0L);
        _bw.Write(0L);
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
            UncompressedBytes = _uncompressedBytes,
        };
    }

    // ── Columnar block writer ─────────────────────────────────────────────────

    // ── Per-block scratch, reused across the ~1000 blocks of a segment flush ──
    // One SegmentWriter serialises one segment on one thread, so plain instance
    // fields suffice. Before this, every 64 KB block allocated ~10 arrays, five
    // MemoryStreams, a ToArray of the whole block and four Stream.CopyTo buffers —
    // hundreds of MB of (largely LOH) garbage per flushed tier, which slowed
    // flushes into the back-pressure budget and showed up as ingest drops.
    private byte[] _colT = [];
    private byte[] _colL = [];
    private byte[] _colI = [];
    private byte[] _colTr = [];
    private byte[] _colSp = [];
    private uint[] _tmplOffsets = [];
    private uint[] _excOffsets = [];
    private uint[] _propsOffsets = [];
    private uint[] _svcOffsets = [];
    private readonly MemoryStream _tmplBytes  = new(1024);
    private readonly MemoryStream _excBytes   = new(256);
    private readonly MemoryStream _propsBytes = new(4096);
    private readonly MemoryStream _svcBytes   = new(256);
    private readonly MemoryStream _blk        = new(BlockSize + 4096);

    private static void Ensure(ref byte[] buf, int size)
    {
        if (buf.Length < size) buf = new byte[Math.Max(size, buf.Length * 2)];
    }

    private static void Ensure(ref uint[] buf, int size)
    {
        if (buf.Length < size) buf = new uint[Math.Max(size, buf.Length * 2)];
    }

    private void FlushColumnarBlock(HotTierSegment hot, StringInternPool templatePool, List<int> rowIndices)
    {
        int n = rowIndices.Count;

        long  blockMinTs = long.MaxValue;
        long  blockMaxTs = long.MinValue;
        ulong blockMinId = ulong.MaxValue;
        for (int k = 0; k < n; k++)
        {
            ref var h = ref hot.GetHeader(rowIndices[k]);
            if (h.TimestampUtcTicks < blockMinTs) blockMinTs = h.TimestampUtcTicks;
            if (h.TimestampUtcTicks > blockMaxTs) blockMaxTs = h.TimestampUtcTicks;
            if (h.Id < blockMinId)               blockMinId = h.Id;
        }

        Ensure(ref _colT,  n * 8);
        Ensure(ref _colL,  n);
        Ensure(ref _colI,  n * 8);
        Ensure(ref _colTr, n * 16);   // TraceId: Hi(8) + Lo(8) per event
        Ensure(ref _colSp, n * 8);    // SpanId: 8 bytes per event
        Ensure(ref _tmplOffsets,  n + 1);
        Ensure(ref _excOffsets,   n + 1);
        Ensure(ref _propsOffsets, n + 1);
        Ensure(ref _svcOffsets,   n + 1);
        var colT = _colT;
        var colL = _colL;
        var colI = _colI;
        var colTr = _colTr;
        var colSp = _colSp;
        var tmplOffsets  = _tmplOffsets;
        var excOffsets   = _excOffsets;
        var propsOffsets = _propsOffsets;
        var svcOffsets   = _svcOffsets;
        var tmplBytes  = _tmplBytes;   tmplBytes.SetLength(0);
        var excBytes   = _excBytes;    excBytes.SetLength(0);
        var propsBytes = _propsBytes;  propsBytes.SetLength(0);
        var svcBytes   = _svcBytes;    svcBytes.SetLength(0);

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

        var blk = _blk;
        blk.SetLength(0);
        WriteUInt32(blk, (uint)n);
        WriteInt64(blk, blockMinTs);
        WriteUInt64(blk, blockMinId);
        blk.WriteByte(9);

        WriteColumn(blk, 1, colT, n * 8);
        WriteColumn(blk, 2, colL, n);
        WriteColumn(blk, 3, colI, n * 8);
        WriteStringColumn(blk, 4, tmplOffsets,  n + 1, tmplBytes);
        WriteStringColumn(blk, 5, excOffsets,   n + 1, excBytes);
        WriteStringColumn(blk, 6, propsOffsets, n + 1, propsBytes);
        WriteColumn(blk, 7, colTr, n * 16);
        WriteColumn(blk, 8, colSp, n * 8);
        WriteStringColumn(blk, 9, svcOffsets, n + 1, svcBytes);

        // Compress straight from the stream's internal buffer — no ToArray copy.
        int    uncompressedLen = (int)blk.Length;
        byte[] uncompressed    = blk.GetBuffer();
        int    maxOut          = LZ4Codec.MaximumOutputSize(uncompressedLen);
        byte[] compBuf         = ArrayPool<byte>.Shared.Rent(maxOut);
        try
        {
            int compressedLen = LZ4Codec.Encode(uncompressed, 0, uncompressedLen, compBuf, 0, maxOut, LZ4Level.L00_FAST);

            long blockOffset = _fs.Position;
            _blockIndex.Add((blockOffset, blockMinTs, (uint)_eventsFlushed));
            _eventsFlushed     += n;
            _uncompressedBytes += uncompressedLen;

            // Group accumulators. The budget is measured in UNCOMPRESSED payload because that
            // is what index-build memory tracks (postings scale with indexed text bytes), not
            // the compressed size — which varies by an order of magnitude with content.
            _groupEventCount   += (uint)n;
            _groupPayloadBytes += uncompressedLen;
            if (blockMinTs < _groupMinTs) _groupMinTs = blockMinTs;
            if (blockMaxTs > _groupMaxTs) _groupMaxTs = blockMaxTs;

            _bw.Write((uint)uncompressedLen);
            _bw.Write((uint)compressedLen);
            _bw.Write(compBuf, 0, compressedLen);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(compBuf);
        }
    }

    private static void WriteColumn(MemoryStream dst, byte id, byte[] payload, int length)
    {
        dst.WriteByte(id);
        WriteUInt32(dst, (uint)length);
        dst.Write(payload, 0, length);
    }

    private static void WriteStringColumn(MemoryStream dst, byte id, uint[] offsets, int offsetCount, MemoryStream payload)
    {
        int offsetsByteLen = offsetCount * 4;
        int totalLen       = offsetsByteLen + (int)payload.Length;
        dst.WriteByte(id);
        WriteUInt32(dst, (uint)totalLen);

        Span<byte> tmp4 = stackalloc byte[4];
        for (int i = 0; i < offsetCount; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(tmp4, offsets[i]);
            dst.Write(tmp4);
        }
        // Write from the payload stream's internal buffer — Stream.CopyTo allocates
        // an 80 KB transfer buffer per call (four calls per block before this).
        dst.Write(payload.GetBuffer(), 0, (int)payload.Length);
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

    /// <summary>One group-directory entry — <see cref="GroupEntrySize"/> bytes on disk.</summary>
    private struct GroupEntry
    {
        public uint FirstBlock;
        public uint BlockCount;
        public uint FirstOrdinal;
        public uint EventCount;
        public long MinTs;
        public long MaxTs;
        public long InvertedOffset;
        public long TrigramOffset;
        public long BloomOffset;
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
