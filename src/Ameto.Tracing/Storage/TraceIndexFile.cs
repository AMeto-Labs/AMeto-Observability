using System.Buffers;
using System.Buffers.Binary;
using Ameto.Core;
using K4os.Compression.LZ4;

namespace Ameto.Tracing.Storage;

/// <summary>Where one trace's spans live: the segment, and their offsets inside it.</summary>
internal readonly record struct TraceIndexHit(ulong SegmentId, uint[] Offsets);

/// <summary>
/// What a run was able to say about a key. THREE ANSWERS, NOT TWO, and the third is the whole
/// point: a run that could not be read has not proved anything, and only a proof may be used to
/// SKIP a segment.
///
/// <para>Collapsing <see cref="Unreadable"/> into <see cref="NotPresent"/> is a silent loss with
/// no fault and no log line: the manifest still says the segment is covered, the caller finds no
/// hit for it, and its spans are dropped from the answer. Every way a run can fail to answer —
/// a torn block, a deleted or briefly locked file, a reader retired underneath the lookup — lands
/// here rather than on "no".</para>
/// </summary>
internal enum TraceIndexOutcome
{
    /// <summary>The run holds the key; its hits were appended.</summary>
    Found,
    /// <summary>The run was read and does not hold the key. The only answer that permits a skip.</summary>
    NotPresent,
    /// <summary>The run could not answer. The segments it covers must be read, not skipped.</summary>
    Unreadable,
}

/// <summary>
/// ONE SORTED RUN OF THE TRACE-ID INDEX — a <c>.tix</c> file.
///
/// <para>Answers "which segment holds this trace, and at what offsets" without opening the segment
/// and without the linear walk over every trace id that the per-segment index inside a
/// <c>.trc</c> costs today. What makes that possible is not the sort by itself but the split
/// between what is held in memory and what is not: the bloom filter and the sparse block index
/// live in RAM (about 12 KB and 16 bytes per block), the entries do not, so a lookup is a bloom
/// check that touches no disk at all, and at most one four-kilobyte read when it passes.</para>
///
/// <para>THE KEY IS THE FIRST 8 BYTES OF THE TRACE ID, not all 16, and that is a size decision
/// with a correctness argument behind it. Trace ids are uniformly random 128-bit values, so at
/// 2.8 million traces the chance any two share a 64-bit prefix is about n²/2⁶⁵ ≈ 2·10⁻⁷ — and a
/// collision here is a FALSE POSITIVE, never a wrong answer: the caller opens one extra segment,
/// reads the spans, finds the full id does not match, and moves on. Half the key is half the
/// index, paid for with an event that costs a single wasted read.</para>
///
/// <para>A KEY MAY LEGITIMATELY REPEAT, which is why every lookup returns a list. Spans of one
/// trace arrive over time and a flush can land between them, so one trace really does live in two
/// segments; a run produced by index compaction holds both. Entries are sorted by key and
/// duplicates sit adjacent, so returning all of them costs one scan of one block.</para>
///
/// <code>
///   [Header 28 B]  magic "RDTX" u32 | version u16 | level u16 | entryCount u32
///                  minKey u64 | maxKey u64
///   [Blocks]       N × { uncompSize u32 | compSize u32 | LZ4 of sorted entries }
///                  entry: key u64 | segId u64 | offsetCount uvarint | offsets uvarint (delta)
///   [Sparse index] blockCount u32 | per block: firstKey u64 | fileOffset u64
///   [Bloom]        byteLen u32 | bits          (Ameto.Core.SegmentBloomFilter)
///   [Footer 20 B]  sparseOffset u64 | bloomOffset u64 | magic "RDXF" u32
/// </code>
/// </summary>
internal static class TraceIndexFile
{
    internal const uint   Magic       = 0x5854_4452;   // "RDTX"
    internal const uint   FooterMagic = 0x4658_4452;   // "RDXF"
    internal const ushort Version     = 1;

    /// <summary>Header bytes, before the first block.</summary>
    internal const int HeaderBytes = 28;
    /// <summary>Footer bytes: two offsets and the magic.</summary>
    internal const int FooterBytes = 20;

    /// <summary>
    /// Uncompressed bytes a block is filled to before it is closed. Four kilobytes is one page,
    /// which is the unit a lookup actually pays for — the point of the whole file is that a hit
    /// costs one of these and a miss costs none.
    /// </summary>
    internal const int TargetBlockBytes = 4 * 1024;

    /// <summary>
    /// The most a single block may decompress to. Nothing on disk bounds what a payload inflates
    /// to, so this is the constant half of the rule <see cref="FileBounds"/> states — the other
    /// half, the compressed length, is bounded by the bytes actually left in the file.
    /// </summary>
    internal const int MaxBlockBytes = 8 * 1024 * 1024;

    /// <summary>
    /// The most entries one run will be believed to hold. A run is written from one segment's
    /// traces (or a merge of such runs), and this engine's largest segment is 200 000 spans; forty
    /// million entries is far past any real file and well short of what a torn u32 produces.
    /// </summary>
    internal const int MaxEntries = 40_000_000;

    /// <summary>The index key for a trace id: its first 8 bytes, big-endian, so sort order is id order.</summary>
    internal static ulong KeyOf(TraceId id) => id.High;
}

/// <summary>
/// Builds a <c>.tix</c>. Entries may be added in any order; the writer sorts before it writes,
/// because the reader's whole design depends on the order and a caller that promised it once
/// would eventually stop keeping the promise.
/// </summary>
internal sealed class TraceIndexWriter
{
    private readonly List<(ulong Key, ulong SegId, uint[] Offsets)> _entries = new();

    public int Count => _entries.Count;

    /// <summary>
    /// Records where one trace's spans are. <paramref name="offsets"/> is stored ASCENDING —
    /// sorted here if it is not already.
    ///
    /// <para>The block encoding writes offsets as unsigned deltas, which cannot express a step
    /// backwards; an earlier version tried to fall back to an absolute value for a descending
    /// step, and the reader — which can only ever add — turned <c>[200000, 199999]</c> into
    /// <c>[200000, 399999]</c>. Silent, and pointing a caller at spans that are not the trace's.
    /// The order carries no information anyway (these are positions in a file, and
    /// <c>ReadTraceAsync</c> sorts them again before walking), so the invariant is simply made
    /// true at the door.</para>
    ///
    /// <para>The caller's array is never mutated: an out-of-order one is copied first, because
    /// the offsets handed in here belong to a segment's own index and sorting them in place would
    /// reorder something somebody else is still reading.</para>
    /// </summary>
    public void Add(TraceId traceId, ulong segmentId, uint[] offsets)
    {
        if (!IsAscending(offsets))
        {
            var copy = (uint[])offsets.Clone();
            Array.Sort(copy);
            offsets = copy;
        }
        _entries.Add((TraceIndexFile.KeyOf(traceId), segmentId, offsets));
    }

    /// <summary>
    /// Copies an entry forward from another run, key already derived.
    ///
    /// <para>For the index compactor, which is moving entries between files rather than recording
    /// new ones — it has the key and must not re-derive it from a trace id it no longer holds. The
    /// offsets come from a run this writer produced, so they are already ascending; they are
    /// checked anyway, because "already sorted" is exactly the kind of promise that stops being
    /// true when somebody adds a second producer.</para>
    /// </summary>
    public void AddRaw(ulong key, ulong segmentId, uint[] offsets)
    {
        if (!IsAscending(offsets))
        {
            var copy = (uint[])offsets.Clone();
            Array.Sort(copy);
            offsets = copy;
        }
        _entries.Add((key, segmentId, offsets));
    }

    private static bool IsAscending(uint[] o)
    {
        for (int i = 1; i < o.Length; i++)
            if (o[i] < o[i - 1]) return false;
        return true;
    }

    /// <summary>
    /// Writes the run to a temp file, fsyncs it, and renames it into place — the same
    /// durability protocol the segment writer uses, and for the same reason: the name appearing is
    /// what makes the contents true, so the contents must be on the platter first.
    /// </summary>
    /// <returns>The run as the manifest should record it.</returns>
    public TraceIndexRun Write(string path, int level, ulong[] coveredSegments)
    {
        // Sorted by key, then by segment so a repeated key has a stable order — the reader walks
        // duplicates forward from the first match and a stable order keeps that walk cheap.
        _entries.Sort(static (a, b) =>
        {
            int c = a.Key.CompareTo(b.Key);
            return c != 0 ? c : a.SegId.CompareTo(b.SegId);
        });

        string tmp = path + ".tmp";
        ulong  minKey = _entries.Count > 0 ? _entries[0].Key   : 0;
        ulong  maxKey = _entries.Count > 0 ? _entries[^1].Key  : 0;

        var bloom       = SegmentBloomFilter.Create(Math.Max(1, _entries.Count));
        var blockFirst  = new List<ulong>();
        var blockOffset = new List<long>();

        try
        {
            using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024))
            {
                Span<byte> head = stackalloc byte[TraceIndexFile.HeaderBytes];
                BinaryPrimitives.WriteUInt32LittleEndian(head,        TraceIndexFile.Magic);
                BinaryPrimitives.WriteUInt16LittleEndian(head[4..],   TraceIndexFile.Version);
                BinaryPrimitives.WriteUInt16LittleEndian(head[6..],   (ushort)level);
                BinaryPrimitives.WriteInt32LittleEndian (head[8..],   _entries.Count);
                BinaryPrimitives.WriteUInt64LittleEndian(head[12..],  minKey);
                BinaryPrimitives.WriteUInt64LittleEndian(head[20..],  maxKey);
                fs.Write(head);

                var raw = new ByteBuffer(TraceIndexFile.TargetBlockBytes * 2);
                // Hoisted out of the fill loop below: the bloom takes a span, so the key it is
                // handed does not need to be a fresh array per entry. It was one, and at 200 000
                // entries that is 1.6 MB of eight-byte arrays per flush, paid again by every merge.
                Span<byte> keyBuf = stackalloc byte[8];
                int  i  = 0;
                while (i < _entries.Count)
                {
                    raw.Reset();
                    ulong first = _entries[i].Key;

                    // One block is filled to the target and then closed on an entry boundary. A
                    // block never splits an entry, so the reader never needs two blocks to answer.
                    while (i < _entries.Count && raw.Length < TraceIndexFile.TargetBlockBytes)
                    {
                        var (key, segId, offsets) = _entries[i++];
                        BinaryPrimitives.WriteUInt64BigEndian(keyBuf, key);
                        bloom.Add(keyBuf);
                        raw.U64(key);
                        raw.U64(segId);
                        raw.UVar((ulong)offsets.Length);
                        // Ascending, guaranteed by Add — see there. The encoding has no way to
                        // say "this one is absolute", so a descending step cannot be represented
                        // at all; the invariant is established once, at the door, rather than
                        // patched here where a fallback would be silently wrong.
                        uint prev = 0;
                        foreach (uint o in offsets)
                        {
                            raw.UVar(o - prev);
                            prev = o;
                        }
                    }

                    blockFirst.Add(first);
                    blockOffset.Add(fs.Position);
                    WriteBlock(fs, raw.Span);
                }

                long sparseAt = fs.Position;
                Span<byte> u32 = stackalloc byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(u32, blockFirst.Count);
                fs.Write(u32);
                Span<byte> pair = stackalloc byte[16];
                for (int b = 0; b < blockFirst.Count; b++)
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(pair,      blockFirst[b]);
                    BinaryPrimitives.WriteInt64LittleEndian (pair[8..], blockOffset[b]);
                    fs.Write(pair);
                }

                long bloomAt = fs.Position;
                byte[] bits  = bloom.Serialise();
                BinaryPrimitives.WriteInt32LittleEndian(u32, bits.Length);
                fs.Write(u32);
                fs.Write(bits);

                Span<byte> foot = stackalloc byte[TraceIndexFile.FooterBytes];
                BinaryPrimitives.WriteInt64LittleEndian (foot,       sparseAt);
                BinaryPrimitives.WriteInt64LittleEndian (foot[8..],  bloomAt);
                BinaryPrimitives.WriteUInt32LittleEndian(foot[16..], TraceIndexFile.FooterMagic);
                fs.Write(foot);

                fs.Flush(flushToDisk: true);
            }

            File.Move(tmp, path, overwrite: true);
        }
        finally
        {
            bloom.Dispose();
            if (File.Exists(tmp)) { try { File.Delete(tmp); } catch { /* best effort */ } }
        }

        return new TraceIndexRun(level, path, minKey, maxKey, _entries.Count, coveredSegments);
    }

    private static void WriteBlock(FileStream fs, ReadOnlySpan<byte> raw)
    {
        int max  = LZ4Codec.MaximumOutputSize(raw.Length);
        byte[] c = ArrayPool<byte>.Shared.Rent(max);
        try
        {
            int n = LZ4Codec.Encode(raw, c.AsSpan(0, max), LZ4Level.L00_FAST);
            Span<byte> hdr = stackalloc byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(hdr,      raw.Length);
            BinaryPrimitives.WriteInt32LittleEndian(hdr[4..], n);
            fs.Write(hdr);
            fs.Write(c.AsSpan(0, n));
        }
        finally { ArrayPool<byte>.Shared.Return(c); }
    }

    /// <summary>A growable little-endian buffer for one block. Reset per block, never reallocated.</summary>
    private sealed class ByteBuffer(int capacity)
    {
        private byte[] _buf = new byte[capacity];
        private int    _n;

        public int                Length => _n;
        public ReadOnlySpan<byte> Span   => _buf.AsSpan(0, _n);
        public void               Reset() => _n = 0;

        private Span<byte> Room(int n)
        {
            if (_n + n > _buf.Length) Array.Resize(ref _buf, Math.Max(_buf.Length * 2, _n + n));
            var s = _buf.AsSpan(_n, n);
            _n += n;
            return s;
        }

        public void U64(ulong v) => BinaryPrimitives.WriteUInt64LittleEndian(Room(8), v);

        public void UVar(ulong v)
        {
            while (v >= 0x80) { Room(1)[0] = (byte)(v | 0x80); v >>= 7; }
            Room(1)[0] = (byte)v;
        }
    }
}

/// <summary>
/// Reads a <c>.tix</c>. Holds the bloom filter and the sparse block index; everything else stays
/// on disk until a key that passes the bloom asks for it.
/// </summary>
internal sealed class TraceIndexReader : IDisposable
{
    private readonly string              _path;
    private readonly SegmentBloomFilter  _bloom;
    private readonly ulong[]             _blockFirstKey;
    private readonly long[]              _blockOffset;
    /// <summary>0 = live, 1 = freed. Volatile: written by whoever retires the run, read on every
    /// lookup from a request thread.</summary>
    private          bool                _disposed;
    /// <summary>Lookups currently holding this reader. Freed only at zero — see <see cref="Retire"/>.</summary>
    private          int                 _refs;
    /// <summary>Set when the store drops the run; the last release then does the freeing.</summary>
    private          bool                _retired;
    private readonly System.Threading.Lock _life = new();

    /// <summary>The run's own path — for the log line when it cannot answer.</summary>
    public string Path => _path;

    /// <summary>The segments this run indexes, so a failed lookup can name whom it failed for.</summary>
    public ulong[] CoveredSegments { get; set; } = [];

    public int   EntryCount { get; }
    public int   Level      { get; }
    public ulong MinKey     { get; }
    public ulong MaxKey     { get; }

    /// <summary>Bytes this reader keeps alive — for a cache that has to answer for its footprint.</summary>
    public long RetainedBytes => _bloom.RetainedBytes + _blockFirstKey.Length * 16L;

    private TraceIndexReader(string path, SegmentBloomFilter bloom, ulong[] firstKeys, long[] offsets,
                             int entryCount, int level, ulong minKey, ulong maxKey)
    {
        _path          = path;
        _bloom         = bloom;
        _blockFirstKey = firstKeys;
        _blockOffset   = offsets;
        EntryCount     = entryCount;
        Level          = level;
        MinKey         = minKey;
        MaxKey         = maxKey;
    }

    /// <summary>
    /// Opens a run, or returns null when it cannot be read.
    ///
    /// <para>NULL RATHER THAN A THROW, and the caller must treat it as "this run does not exist":
    /// a run that will not open is not a run that says a trace is absent. The segment it covers
    /// falls back to the full scan, which is the same answer the engine gave before any of this.
    /// </para>
    /// </summary>
    public static TraceIndexReader? Open(string path)
    {
        FileStream? fs = null;
        SegmentBloomFilter? bloom = null;
        try
        {
            fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024);
            if (fs.Length < TraceIndexFile.HeaderBytes + TraceIndexFile.FooterBytes) return null;

            Span<byte> head = stackalloc byte[TraceIndexFile.HeaderBytes];
            fs.ReadExactly(head);
            if (BinaryPrimitives.ReadUInt32LittleEndian(head) != TraceIndexFile.Magic) return null;
            if (BinaryPrimitives.ReadUInt16LittleEndian(head[4..]) != TraceIndexFile.Version) return null;
            int   level  = BinaryPrimitives.ReadUInt16LittleEndian(head[6..]);
            int   count  = BinaryPrimitives.ReadInt32LittleEndian (head[8..]);
            ulong minKey = BinaryPrimitives.ReadUInt64LittleEndian(head[12..]);
            ulong maxKey = BinaryPrimitives.ReadUInt64LittleEndian(head[20..]);
            if (count < 0 || count > TraceIndexFile.MaxEntries) return null;

            fs.Seek(-TraceIndexFile.FooterBytes, SeekOrigin.End);
            Span<byte> foot = stackalloc byte[TraceIndexFile.FooterBytes];
            fs.ReadExactly(foot);
            if (BinaryPrimitives.ReadUInt32LittleEndian(foot[16..]) != TraceIndexFile.FooterMagic) return null;
            long sparseAt = BinaryPrimitives.ReadInt64LittleEndian(foot);
            long bloomAt  = BinaryPrimitives.ReadInt64LittleEndian(foot[8..]);
            if (sparseAt < TraceIndexFile.HeaderBytes || sparseAt >= fs.Length) return null;
            if (bloomAt  <= sparseAt                  || bloomAt  >= fs.Length) return null;

            // ── sparse index ──
            fs.Seek(sparseAt, SeekOrigin.Begin);
            Span<byte> u32 = stackalloc byte[4];
            fs.ReadExactly(u32);
            int blocks = BinaryPrimitives.ReadInt32LittleEndian(u32);
            // AGAINST THE FILE, NOT A CONSTANT: the sparse index ends where the bloom begins, and
            // each entry is exactly sixteen bytes, so the file cannot claim more than it holds.
            if (!FileBounds.CountFits(blocks, bloomAt - fs.Position, fileBytesPerElement: 16)) return null;

            var firstKeys = new ulong[blocks];
            var offsets   = new long[blocks];
            Span<byte> pair = stackalloc byte[16];
            for (int i = 0; i < blocks; i++)
            {
                fs.ReadExactly(pair);
                firstKeys[i] = BinaryPrimitives.ReadUInt64LittleEndian(pair);
                offsets[i]   = BinaryPrimitives.ReadInt64LittleEndian (pair[8..]);
                if (offsets[i] < TraceIndexFile.HeaderBytes || offsets[i] >= sparseAt) return null;
                if (i > 0 && (firstKeys[i] < firstKeys[i - 1] || offsets[i] <= offsets[i - 1])) return null;
            }

            // ── bloom ──
            fs.Seek(bloomAt, SeekOrigin.Begin);
            fs.ReadExactly(u32);
            int bloomBytes = BinaryPrimitives.ReadInt32LittleEndian(u32);
            if (!FileBounds.LengthFits(bloomBytes, fs.Length - TraceIndexFile.FooterBytes - fs.Position))
                return null;
            byte[] bits = new byte[bloomBytes];
            fs.ReadExactly(bits);
            bloom = SegmentBloomFilter.Deserialise(bits);

            var reader = new TraceIndexReader(path, bloom, firstKeys, offsets, count, level, minKey, maxKey);
            bloom = null;   // owned by the reader now
            return reader;
        }
        catch
        {
            // Gone, locked, truncated, not a .tix: all one answer to the caller — no run here.
            return null;
        }
        finally
        {
            fs?.Dispose();
            bloom?.Dispose();
        }
    }

    /// <summary>
    /// Every entry in the run, in key order, one block at a time.
    ///
    /// <para>For the index compactor. Streaming rather than materialised, so THIS side costs one
    /// block per input — the merge's own writer then holds what it keeps, which is why the batch is
    /// capped by entry count in <c>TraceIndexCompactor.MaxEntriesPerMerge</c> rather than by run
    /// count alone.</para>
    ///
    /// <para>A block that will not decode THROWS. It used to <c>yield break</c>, and that is the
    /// difference between a short answer and a reported one: <see cref="Open"/> validates the
    /// header, footer, sparse index and bloom, so a run whose INTERIOR block is torn opens
    /// perfectly and enumerates fine right up to the bad block. A merge reading that as a normal
    /// end wrote a survivor claiming the union of its inputs' coverage while missing the tail it
    /// never copied, deleted the sources, and left the lost traces covered-with-no-entries —
    /// silent, and permanent, because coverage is never re-derived from a run's contents. Ending
    /// early is indistinguishable from finishing; throwing is not.</para>
    /// </summary>
    /// <exception cref="InvalidDataException">A block could not be decoded.</exception>
    public IEnumerable<(ulong Key, ulong SegmentId, uint[] Offsets)> EnumerateEntries()
    {
        for (int b = 0; b < _blockOffset.Length; b++)
        {
            var block = ReadWholeBlock(b)
                ?? throw new InvalidDataException(
                    $"Trace index run {_path} block {b} of {_blockOffset.Length} will not decode");
            foreach (var e in block) yield return e;
        }
    }

    /// <summary>One block, fully decoded, or null when it will not decode.</summary>
    private List<(ulong Key, ulong SegmentId, uint[] Offsets)>? ReadWholeBlock(int index)
    {
        byte[]? raw = null;
        try
        {
            using var fs = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read, 8 * 1024);
            fs.Seek(_blockOffset[index], SeekOrigin.Begin);

            Span<byte> hdr = stackalloc byte[8];
            fs.ReadExactly(hdr);
            int uncomp = BinaryPrimitives.ReadInt32LittleEndian(hdr);
            int comp   = BinaryPrimitives.ReadInt32LittleEndian(hdr[4..]);
            if (!FileBounds.LengthFits(comp, fs.Length - fs.Position)) return null;
            if (uncomp < 0 || uncomp > TraceIndexFile.MaxBlockBytes)   return null;

            int rawLen;
            byte[] c = ArrayPool<byte>.Shared.Rent(comp);
            try
            {
                fs.ReadExactly(c, 0, comp);
                raw    = ArrayPool<byte>.Shared.Rent(uncomp);
                rawLen = LZ4Codec.Decode(c.AsSpan(0, comp), raw.AsSpan(0, uncomp));
                if (rawLen < 0) return null;
            }
            finally { ArrayPool<byte>.Shared.Return(c); }

            var into = new List<(ulong, ulong, uint[])>();
            var cur  = new Cursor(raw.AsSpan(0, rawLen));
            while (cur.Remaining > 0)
            {
                if (!cur.TryU64(out ulong k) || !cur.TryU64(out ulong segId)) return null;
                if (!cur.TryUVar(out ulong n)) return null;
                if (!FileBounds.CountFits((long)n, cur.Remaining, fileBytesPerElement: 1)) return null;

                var offs = new uint[(int)n];
                uint prev = 0;
                for (ulong j = 0; j < n; j++)
                {
                    if (!cur.TryUVar(out ulong d)) return null;
                    prev = unchecked(prev + (uint)d);
                    offs[j] = prev;
                }
                into.Add((k, segId, offs));
            }
            return into;
        }
        catch { return null; }
        finally { if (raw is not null) ArrayPool<byte>.Shared.Return(raw); }
    }

    /// <summary>
    /// Whether this run could hold the key. No I/O: the bloom is in memory.
    ///
    /// <para>A RETIRED READER ANSWERS NEITHER WAY. It used to return false here, which the caller
    /// read as "this run does not hold the key" — and a retired run has read nothing and proved
    /// nothing. See <see cref="TraceIndexOutcome"/>.</para>
    /// </summary>
    public bool MightContain(ulong key)
    {
        if (Volatile.Read(ref _disposed) || _blockFirstKey.Length == 0) return false;
        if (key < MinKey || key > MaxKey) return false;
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(b, key);
        return _bloom.MightContain(b);
    }

    /// <summary>
    /// Every place this key is recorded, appended to <paramref name="into"/>. At most one block is
    /// read, and none at all when the bloom says no.
    ///
    /// <para>THE RETURN IS THREE-VALUED, and <see cref="TraceIndexOutcome.NotPresent"/> is a claim
    /// this method has to earn. It used to be a bool, so "the block would not decode", "the file
    /// vanished under me" and "I was retired mid-lookup" were all reported as "no" — and "no" from
    /// a covered run is exactly what lets the engine skip a segment without opening it. Anything
    /// short of a completed read is <see cref="TraceIndexOutcome.Unreadable"/> now.</para>
    ///
    /// <para>A partial read is also Unreadable rather than Found: when a key spans two blocks and
    /// only the second fails, the hits already appended are a TRUNCATED offset list, and handing
    /// that to the walk as if it were complete draws a waterfall missing half its spans. The
    /// appended hits are rolled back so the caller cannot use them by accident.</para>
    /// </summary>
    public TraceIndexOutcome Lookup(ulong key, List<TraceIndexHit> into)
    {
        if (Volatile.Read(ref _disposed)) return TraceIndexOutcome.Unreadable;
        if (!MightContain(key)) return TraceIndexOutcome.NotPresent;

        int b = FirstBlockThatCouldHold(key);
        if (b < 0) return TraceIndexOutcome.NotPresent;

        // FORWARD UNTIL A KEY STRICTLY GREATER IS SEEN, not until a block happens to end on the
        // key. One key can occupy many consecutive blocks (see FirstBlockThatCouldHold), and a
        // block in the middle of such a run contains nothing else — so "did this block end on the
        // key" is not the question. "Have we gone past it yet" is.
        int before = into.Count;
        while (b < _blockFirstKey.Length)
        {
            if (!ScanBlock(b, key, into, out bool wentPast))
            {
                into.RemoveRange(before, into.Count - before);   // a truncated list is worse than none
                return TraceIndexOutcome.Unreadable;
            }
            if (wentPast) break;
            b++;
        }
        return into.Count > before ? TraceIndexOutcome.Found : TraceIndexOutcome.NotPresent;
    }

    /// <summary>
    /// The lowest block that could hold <paramref name="key"/>, or -1 when none can.
    ///
    /// <para>NOT SIMPLY "THE LAST BLOCK WHOSE FIRST KEY IS AT OR BELOW IT", which is only right
    /// while keys are distinct. They need not be: a key is the first eight bytes of a trace id, and
    /// a producer that varies only the low half — or a plain collision — puts many entries under
    /// one key, enough to fill several consecutive blocks whose first key is all the same value.
    /// Landing on the last of them and walking forward finds the tail of the run and misses
    /// everything before it. Measured on a fixture where 300 traces shared a high half: two spans
    /// expected, zero returned, silently.</para>
    ///
    /// <para>So: step back off any block whose first key IS the wanted one, and then one more, to
    /// the block whose first key is below it — that block can still hold the start of the run in
    /// its tail. At most one block is scanned that turns out to hold nothing.</para>
    /// </summary>
    private int FirstBlockThatCouldHold(ulong key)
    {
        int lo = 0, hi = _blockFirstKey.Length - 1, ans = -1;
        while (lo <= hi)
        {
            int mid = (int)(((uint)lo + (uint)hi) >> 1);
            if (_blockFirstKey[mid] <= key) { ans = mid; lo = mid + 1; }
            else hi = mid - 1;
        }
        if (ans < 0) return -1;

        while (ans > 0 && _blockFirstKey[ans] == key) ans--;
        return ans;
    }

    /// <summary>
    /// Decodes one block and appends every entry matching <paramref name="key"/>.
    /// </summary>
    /// <param name="wentPast">
    /// True when this block held a key strictly greater than the wanted one — the sorted order
    /// then guarantees no later block can hold it either, so the walk stops.
    /// </param>
    /// <returns>False when the block could not be read — the caller stops rather than guesses.</returns>
    private bool ScanBlock(int index, ulong key, List<TraceIndexHit> into, out bool wentPast)
    {
        wentPast = false;
        byte[]? raw = null;
        int rawLen  = 0;
        try
        {
            using var fs = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read, 8 * 1024);
            fs.Seek(_blockOffset[index], SeekOrigin.Begin);

            Span<byte> hdr = stackalloc byte[8];
            fs.ReadExactly(hdr);
            int uncomp = BinaryPrimitives.ReadInt32LittleEndian(hdr);
            int comp   = BinaryPrimitives.ReadInt32LittleEndian(hdr[4..]);

            // Both lengths bounded before either is used to size anything: the compressed one by
            // the bytes actually left in the file, the uncompressed one by the constant, because
            // nothing on disk limits what a payload inflates to.
            if (!FileBounds.LengthFits(comp, fs.Length - fs.Position)) return false;
            if (uncomp < 0 || uncomp > TraceIndexFile.MaxBlockBytes)   return false;

            byte[] c = ArrayPool<byte>.Shared.Rent(comp);
            try
            {
                fs.ReadExactly(c, 0, comp);
                raw    = ArrayPool<byte>.Shared.Rent(uncomp);
                rawLen = LZ4Codec.Decode(c.AsSpan(0, comp), raw.AsSpan(0, uncomp));
                if (rawLen < 0) return false;
            }
            finally { ArrayPool<byte>.Shared.Return(c); }

            var cur = new Cursor(raw.AsSpan(0, rawLen));
            while (cur.Remaining > 0)
            {
                if (!cur.TryU64(out ulong k) || !cur.TryU64(out ulong segId)) return false;
                if (!cur.TryUVar(out ulong n)) return false;

                // AGAINST THE BLOCK, NOT A CONSTANT. This count comes out of the file and sizes
                // the array two lines down; every offset that follows it is at least one varint
                // byte, so a count past what is left of the block describes a block that cannot
                // exist. Said through FileBounds rather than by hand because that is the one place
                // this codebase keeps the rule, and because a bound written inline is a bound the
                // convention test cannot see.
                if (!FileBounds.CountFits((long)n, cur.Remaining, fileBytesPerElement: 1)) return false;

                if (k > key)
                {
                    wentPast = true;                  // sorted: nothing further can match
                    return true;
                }

                var offs = k == key ? new uint[(int)n] : null;
                uint prev = 0;
                for (ulong j = 0; j < n; j++)
                {
                    if (!cur.TryUVar(out ulong d)) return false;
                    prev = unchecked(prev + (uint)d);
                    if (offs is not null) offs[j] = prev;
                }
                if (offs is not null) into.Add(new TraceIndexHit(segId, offs));
            }
            return true;
        }
        catch { return false; }
        finally { if (raw is not null) ArrayPool<byte>.Shared.Return(raw); }
    }

    /// <summary>
    /// Takes a hold on this reader for the duration of one lookup, or refuses if it is already
    /// retired.
    ///
    /// <para>WITHOUT THIS THE BLOOM IS FREED UNDER A RUNNING LOOKUP. The store swaps its map and
    /// disposed the dropped readers immediately, while a lookup holds plain references to them
    /// across bloom probes and a 4 KB block read — <c>SegmentBloomFilter.Dispose</c> is a
    /// <c>NativeMemory.Free</c>, so the reachable outcomes were a read of freed memory or an
    /// ObjectDisposedException surfacing as a 500. The log engine reached the same conclusion:
    /// <c>SegmentIndexCache</c> carries RefCount/Doomed for exactly this.</para>
    /// </summary>
    public bool TryAcquire()
    {
        lock (_life)
        {
            if (_retired || _disposed) return false;
            _refs++;
            return true;
        }
    }

    /// <summary>Gives back a hold taken by <see cref="TryAcquire"/>, freeing if it was the last.</summary>
    public void Release()
    {
        lock (_life)
        {
            if (--_refs > 0 || !_retired || _disposed) return;
            _disposed = true;
        }
        _bloom.Dispose();
    }

    /// <summary>
    /// The store is done with this run. Frees now if nothing holds it, otherwise leaves the last
    /// <see cref="Release"/> to do it — a lookup already in flight finishes on live memory.
    /// </summary>
    public void Retire()
    {
        lock (_life)
        {
            _retired = true;
            if (_refs > 0 || _disposed) return;
            _disposed = true;
        }
        _bloom.Dispose();
    }

    public void Dispose() => Retire();

    /// <summary>A forward cursor that never reads past the block it was given.</summary>
    private ref struct Cursor(ReadOnlySpan<byte> data)
    {
        private readonly ReadOnlySpan<byte> _d = data;
        private int                         _i = 0;

        public int Remaining => _d.Length - _i;

        public bool TryU64(out ulong v)
        {
            if (Remaining < 8) { v = 0; return false; }
            v = BinaryPrimitives.ReadUInt64LittleEndian(_d[_i..]);
            _i += 8;
            return true;
        }

        public bool TryUVar(out ulong v)
        {
            v = 0;
            int shift = 0;
            while (true)
            {
                if (Remaining < 1 || shift > 63) return false;
                byte b = _d[_i++];
                v |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) return true;
                shift += 7;
            }
        }
    }
}
