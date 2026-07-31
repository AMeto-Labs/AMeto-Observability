using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Writes a cold-tier .seg file (v3, columnar) from an <see cref="ISegmentEventSource"/>.
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
///
/// <para>STREAMING: the writer holds exactly ONE BLOCK of staged rows plus the open index
/// group's accumulators. It never sees the segment. That is what decouples a segment's size
/// from RAM: compaction merges k sorted .seg files straight into a new one, where before it had
/// to materialise the whole batch into a <see cref="HotTierSegment"/> first (~3× the batch,
/// which is why merge budgets were 32 MB / 100k events).</para>
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

    /// <summary>
    /// Bytes per row assumed when sizing the FIRST index group's bloom filter and nothing has
    /// been measured yet. Only a starting point: from the second group on, the real
    /// <c>uncompressedBytes / events</c> of the file so far is used.
    /// </summary>
    private const long MinAssumedRowCostBytes = 32;

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

    /// <summary>Index sink for the OPEN group, created on its first event and dropped when sealed.</summary>
    private SegmentIndexSinkFactory? _sinkFactory;
    private ISegmentIndexSink?       _sink;

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
    /// Only honoured when a <see cref="SegmentIndexSinkFactory"/> is supplied; a caller that
    /// writes sections itself always produces exactly one group.
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
    /// level so that a segment holds exactly one level.
    /// </param>
    public void WriteEvents(HotTierSegment hot, StringInternPool templatePool, int[] order,
                            SegmentIndexSinkFactory? indexSink)
        => WriteEvents(new HotTierEventSource(hot, templatePool, order), indexSink);

    /// <param name="source">
    /// Events in file write order — ascending (timestamp, id). Read once, forward only; each
    /// event's payload is copied into the open block before the next is pulled, so a source may
    /// hand out spans over a buffer it reuses.
    /// </param>
    /// <param name="indexSink">
    /// When supplied, the file is cut into index groups: the writer pushes every event into the
    /// open group's sink as it stages it, and every time the blocks written since the last cut
    /// exceed the group budget it seals the sink and writes the three sections it returns right
    /// there, between the group's last block and the next group's first.
    ///
    /// <para>This is the whole point of v7 — a fresh sink per group means the accumulators die
    /// at the boundary, so peak index-build memory is O(group) instead of O(file). Passing null
    /// keeps the pre-v7 shape: one implicit group covering the file, with sections written by
    /// the caller via <see cref="WriteInvertedIndex"/> and friends.</para>
    /// </param>
    /// <param name="ct">
    /// Checked once per emitted block (~64 KB), which is free next to the LZ4 + msgpack work of
    /// filling one. A merged file can now be 512 MB / 4M events, so an un-cancellable write loop
    /// would mean shutdown waiting tens of seconds for a pass whose output is thrown away
    /// anyway: the file is still at <c>.seg.tmp</c> and no source has been touched, so throwing
    /// out of here unwinds to exactly the pre-merge state.
    /// </param>
    public void WriteEvents(ISegmentEventSource source, SegmentIndexSinkFactory? indexSink,
                            CancellationToken ct = default)
    {
        _fs.Seek(SegmentFileHeader.Size, SeekOrigin.Begin);
        _sinkFactory = indexSink;

        while (source.TryReadNext(out var ev))
        {
            int tmplBytes = Encoding.UTF8.GetByteCount(ev.MessageTemplate);
            int svcBytes  = ev.ServiceName is null ? 0 : Encoding.UTF8.GetByteCount(ev.ServiceName);
            // Approximate: the exception blob is only serialised once, at staging time.
            int rowCost   = 8 + 1 + 8 + 16 + 8 + 16
                          + tmplBytes + svcBytes + ev.Properties.Length
                          + (ev.Exception is null ? 0 : 64);

            if (_stagedCount > 0 && _stagedBytes + rowCost > BlockSize)
            {
                ct.ThrowIfCancellationRequested();
                EmitBlock();

                // Cut only on a block boundary: a group owns whole blocks, so the block
                // index stays a single flat array and candidate → block resolution is
                // untouched by grouping.
                if (_sinkFactory is not null && _groupPayloadBytes >= _groupBudget)
                    SealGroup();
            }

            // Push BEFORE staging, while the source's spans are still guaranteed valid. The
            // ordinal is the file ordinal (see the contract above) and groups always start on
            // a block boundary, so a group's postings never straddle the sink that owns them.
            EnsureSink(rowCost, source.RemainingEventHint)?.Add((uint)_eventsWritten, in ev);

            StageEvent(in ev, tmplBytes, svcBytes);
            _eventsWritten++;
        }

        if (_stagedCount > 0) EmitBlock();
        if (_sinkFactory is not null) SealGroup();
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

    /// <summary>
    /// Opens the group's index sink on its first event.
    ///
    /// <para>The bloom filter is allocated up front and cannot be resized, so the sink needs an
    /// event-count forecast the moment the group starts. Two bounds, whichever is tighter: what
    /// the source says is left, and what the group's PAYLOAD budget affords at the average row
    /// cost measured over the file so far (this event's own cost for the first group, which has
    /// nothing to measure). Over-forecasting only wastes bloom bits; under-forecasting
    /// saturates the filter and the prefilter stops rejecting — so both bounds are ceilings and
    /// neither is trusted alone.</para>
    /// </summary>
    private ISegmentIndexSink? EnsureSink(int rowCost, long remainingHint)
    {
        if (_sinkFactory is null) return null;
        if (_sink is not null)    return _sink;

        long avgRowCost = _eventsFlushed > 0
            ? Math.Max(MinAssumedRowCostBytes, _uncompressedBytes / _eventsFlushed)
            : Math.Max(MinAssumedRowCostBytes, rowCost);
        long byBudget = Math.Max(1, _groupBudget / avgRowCost);
        long estimate = Math.Clamp(Math.Min(remainingHint, byBudget), 1, int.MaxValue);
        return _sink = _sinkFactory((int)estimate);
    }

    /// <summary>Serialises the open group's index sections, writes them, then closes the group.</summary>
    private void SealGroup()
    {
        if (_sink is not null)
        {
            if (_groupEventCount > 0)
            {
                var (inverted, trigram, bloom) = _sink.Serialise();
                WriteInvertedIndex(inverted);
                WriteTrigramIndex(trigram);
                WriteBloomFilter(bloom);
            }
            _sink.Dispose();
            _sink = null;
        }
        if (_groupEventCount == 0) return;
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

    // ── Columnar block staging ────────────────────────────────────────────────

    // ── Per-block scratch, reused across the ~1000 blocks of a segment flush ──
    // One SegmentWriter serialises one segment on one thread, so plain instance
    // fields suffice. Before this, every 64 KB block allocated ~10 arrays, five
    // MemoryStreams, a ToArray of the whole block and four Stream.CopyTo buffers —
    // hundreds of MB of (largely LOH) garbage per flushed tier, which slowed
    // flushes into the back-pressure budget and showed up as ingest drops.
    //
    // Streaming turned this from scratch into STAGING: the block is filled event by
    // event as the source yields them, because there is nothing to re-read. @t and @i
    // are held raw and delta-encoded at emit, when the block's bases are finally known.
    private long[] _stgTs = [];
    private ulong[] _stgId = [];
    private byte[] _colL = [];
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

    private int   _stagedCount;
    private int   _stagedBytes;
    private long  _blockMinTs = long.MaxValue;
    private long  _blockMaxTs = long.MinValue;
    private ulong _blockMinId = ulong.MaxValue;

    /// <summary>
    /// Grows a staging column, PRESERVING what is already in it. Copying is not optional here:
    /// the pre-streaming writer sized every column once, up front, from a known block length —
    /// staging grows mid-block, so a plain reallocation silently drops the rows staged so far.
    /// </summary>
    private static void Ensure<T>(ref T[] buf, int size)
    {
        if (buf.Length < size) Array.Resize(ref buf, Math.Max(size, Math.Max(64, buf.Length * 2)));
    }

    /// <summary>Appends one event to the open block's columns.</summary>
    private void StageEvent(in SegmentEventRef ev, int tmplByteLen, int svcByteLen)
    {
        int k = _stagedCount;
        Ensure(ref _stgTs, k + 1);
        Ensure(ref _stgId, k + 1);
        Ensure(ref _colL,  k + 1);
        Ensure(ref _colTr, (k + 1) * 16);
        Ensure(ref _colSp, (k + 1) * 8);
        Ensure(ref _tmplOffsets,  k + 2);
        Ensure(ref _excOffsets,   k + 2);
        Ensure(ref _propsOffsets, k + 2);
        Ensure(ref _svcOffsets,   k + 2);

        long ts = ev.TimestampUtcTicks;
        if (ts < _minTimestamp) _minTimestamp = ts;
        if (ts > _maxTimestamp) _maxTimestamp = ts;
        if (ev.Level < _minLevel) _minLevel = ev.Level;
        if (ts < _blockMinTs)   _blockMinTs = ts;
        if (ts > _blockMaxTs)   _blockMaxTs = ts;
        if (ev.Id < _blockMinId) _blockMinId = ev.Id;

        _stgTs[k] = ts;
        _stgId[k] = ev.Id;
        _colL[k]  = (byte)ev.Level;
        BinaryPrimitives.WriteUInt64LittleEndian(_colTr.AsSpan(k * 16),     ev.TraceIdHi);
        BinaryPrimitives.WriteUInt64LittleEndian(_colTr.AsSpan(k * 16 + 8), ev.TraceIdLo);
        BinaryPrimitives.WriteUInt64LittleEndian(_colSp.AsSpan(k * 8),      ev.SpanId);

        _tmplOffsets[k] = (uint)_tmplBytes.Length;
        AppendUtf8(_tmplBytes, ev.MessageTemplate, tmplByteLen);

        _excOffsets[k] = (uint)_excBytes.Length;
        if (ev.Exception is not null)
        {
            var b = ev.Exception.ToBytes();
            _excBytes.Write(b, 0, b.Length);
        }

        _propsOffsets[k] = (uint)_propsBytes.Length;
        if (!ev.Properties.IsEmpty) _propsBytes.Write(ev.Properties);

        _svcOffsets[k] = (uint)_svcBytes.Length;
        AppendUtf8(_svcBytes, ev.ServiceName, svcByteLen);

        _stagedCount = k + 1;
        _stagedBytes += 8 + 1 + 8 + 16 + 8 + 16 + tmplByteLen + svcByteLen
                      + ev.Properties.Length + (int)(_excBytes.Length - _excOffsets[k]);
    }

    private static void AppendUtf8(MemoryStream dst, string? value, int byteLen)
    {
        if (byteLen == 0 || string.IsNullOrEmpty(value)) return;
        var tmp = ArrayPool<byte>.Shared.Rent(byteLen);
        try
        {
            int written = Encoding.UTF8.GetBytes(value, 0, value.Length, tmp, 0);
            dst.Write(tmp, 0, written);
        }
        finally { ArrayPool<byte>.Shared.Return(tmp); }
    }

    /// <summary>Delta-encodes, frames, compresses and writes the staged block, then resets staging.</summary>
    private void EmitBlock()
    {
        int n = _stagedCount;
        if (n == 0) return;

        _tmplOffsets[n]  = (uint)_tmplBytes.Length;
        _excOffsets[n]   = (uint)_excBytes.Length;
        _propsOffsets[n] = (uint)_propsBytes.Length;
        _svcOffsets[n]   = (uint)_svcBytes.Length;

        long  blockMinTs = _blockMinTs;
        ulong blockMinId = _blockMinId;

        var blk = _blk;
        blk.SetLength(0);
        WriteUInt32(blk, (uint)n);
        WriteInt64(blk, blockMinTs);
        WriteUInt64(blk, blockMinId);
        blk.WriteByte(9);

        WriteInt64DeltaColumn(blk, 1, _stgTs, n, blockMinTs);
        WriteColumn(blk, 2, _colL, n);
        WriteUInt64DeltaColumn(blk, 3, _stgId, n, blockMinId);
        WriteStringColumn(blk, 4, _tmplOffsets,  n + 1, _tmplBytes);
        WriteStringColumn(blk, 5, _excOffsets,   n + 1, _excBytes);
        WriteStringColumn(blk, 6, _propsOffsets, n + 1, _propsBytes);
        WriteColumn(blk, 7, _colTr, n * 16);
        WriteColumn(blk, 8, _colSp, n * 8);
        WriteStringColumn(blk, 9, _svcOffsets, n + 1, _svcBytes);

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
            if (blockMinTs  < _groupMinTs) _groupMinTs = blockMinTs;
            if (_blockMaxTs > _groupMaxTs) _groupMaxTs = _blockMaxTs;

            _bw.Write((uint)uncompressedLen);
            _bw.Write((uint)compressedLen);
            _bw.Write(compBuf, 0, compressedLen);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(compBuf);
        }

        _stagedCount = 0;
        _stagedBytes = 0;
        _blockMinTs  = long.MaxValue;
        _blockMaxTs  = long.MinValue;
        _blockMinId  = ulong.MaxValue;
        _tmplBytes.SetLength(0);
        _excBytes.SetLength(0);
        _propsBytes.SetLength(0);
        _svcBytes.SetLength(0);
    }

    private static void WriteColumn(MemoryStream dst, byte id, byte[] payload, int length)
    {
        dst.WriteByte(id);
        WriteUInt32(dst, (uint)length);
        dst.Write(payload, 0, length);
    }

    private static void WriteInt64DeltaColumn(MemoryStream dst, byte id, long[] src, int n, long baseValue)
    {
        dst.WriteByte(id);
        WriteUInt32(dst, (uint)(n * 8));
        Span<byte> tmp = stackalloc byte[8];
        for (int k = 0; k < n; k++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(tmp, src[k] - baseValue);
            dst.Write(tmp);
        }
    }

    private static void WriteUInt64DeltaColumn(MemoryStream dst, byte id, ulong[] src, int n, ulong baseValue)
    {
        dst.WriteByte(id);
        WriteUInt32(dst, (uint)(n * 8));
        Span<byte> tmp = stackalloc byte[8];
        for (int k = 0; k < n; k++)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(tmp, src[k] - baseValue);
            dst.Write(tmp);
        }
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
        // An abandoned write (source threw mid-stream) still owns the open group's
        // accumulators — the bloom filter is NativeMemory, so leaking it leaks off-heap.
        _sink?.Dispose();
        _sink = null;
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
