using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Collections.Special;

namespace Ameto.Indexing;

/// <summary>
/// Trigram index for fast substring / prefix search on @mt (message template) and @m (rendered message).
///
/// Building phase: one <see cref="PostingArena.Bucket"/> per distinct folded trigram, found
/// through a direct-indexed table for ASCII trigrams (no hashing) and a dictionary for the rest;
/// postings live in the shared <see cref="PostingArena"/> already in wire form.
/// Serialisation: delta+varint via <see cref="SegmentBitmapCodec"/> (a copy of the arena bytes).
/// Deserialisation: decodes back into sorted arrays for fast intersection.
///
/// <para>ORDERING CONTRACT — the reason postings are an append log and not a set:
/// <c>SegmentIndexBuilder.Build</c> walks <c>pos = 0..hot.Count</c> and passes
/// <c>offset = pos</c>, so offsets arrive MONOTONICALLY NON-DECREASING. The only
/// duplicate a build can produce is a repeat of the CURRENT offset (the same trigram
/// occurring in both the template and a property value of one event), which a
/// last-offset check removes — exactly what <see cref="SegmentInvertedIndex"/> already does.
/// A <c>HashSet&lt;int&gt;</c> cost 18.4 B per (trigram, offset) pair against 7.6 B for a
/// list and ~1.5 B for the arena's varints, and the trigram index was ~84 % of the
/// flush-path index-build peak (560 MB of 663 MB for one 64 MB tier), so this is the single
/// largest lever on flush memory. Out-of-order offsets remain CORRECT —
/// <see cref="_unsorted"/> records the violation and serialisation sorts — they merely give
/// up the free ordering.</para>
///
/// <para>SECTION BYTES ARE PINNED: buckets are numbered in first-seen order and serialised in
/// that order, which is the insertion order the previous dictionary-backed build wrote, so a
/// segment written by this build is byte-identical to one written before it
/// (<c>IndexBuildParityTests</c>, <c>SegmentTrigramBuildParityTests</c>).</para>
///
/// <para>THREADING: the build side has no lock. One index belongs to one index group's
/// builder, and <c>SegmentWriter.WriteEvents</c> drives that from a single thread; the
/// build-mode <see cref="Lookup"/> is test-only. The lock that used to sit on every
/// <see cref="Add(uint, ReadOnlySpan{char})"/> was ~3-5 M uncontended acquisitions per
/// flush for a guarantee nothing used.</para>
/// </summary>
public sealed class SegmentTrigramIndex
{
    // ── Build phase ───────────────────────────────────────────────────────────
    //
    // Folded ASCII trigram (c0 << 14 | c1 << 7 | c2) → bucket id + 1 in a 2^21-entry table
    // (8 MB, rented, cleared on first use): three loads and a shift replace the tuple hash,
    // the bucket probe and the entry chase of a 300 k-entry dictionary, per posting. Non-ASCII
    // trigrams — a small minority even in Cyrillic-heavy logs, since every property value and
    // most templates are ASCII — go through a dictionary keyed on the three UTF-16 units.
    private const int AsciiTableSize = 1 << 21;
    private int[]?                 _ascii;
    private Dictionary<long, int>? _wide;

    private struct TriBucket
    {
        public PostingArena.Bucket Postings;
        public char C0, C1, C2;
    }

    private const int InitialBuckets = 1 << 15;
    private TriBucket[]  _buckets = Array.Empty<TriBucket>();
    private int          _bucketCount;
    private PostingArena _arena = new();
    private bool         _released;

    /// <summary>
    /// Build-mode view for <see cref="Lookup"/>: the test-only path that queries an index
    /// before it is serialised. Reads the arena through <see cref="PostingArena.Decode"/>.
    /// </summary>
    private readonly BuildView _sets;

    // Loaded phase (deserialised): sorted arrays for intersection
    private readonly Dictionary<(char, char, char), int[]> _loaded = new();

    /// <summary>Set when an offset arrived below its bucket's tail (contract violation).
    /// Serialisation then sorts before encoding, which requires ascending input.</summary>
    private bool _unsorted;

    public SegmentTrigramIndex() : this(0) { }

    /// <param name="expectedBuckets">Distinct trigrams the build is expected to hold — the
    /// previous group's count, from <see cref="IndexBuildHints"/>. Sizes the bucket array once
    /// instead of doubling up to it; nothing is rented before the first add.</param>
    public SegmentTrigramIndex(int expectedBuckets)
    {
        _sets = new BuildView(this);
        _initialBuckets = expectedBuckets <= InitialBuckets
            ? InitialBuckets
            : (int)Math.Min(1 << 28, System.Numerics.BitOperations.RoundUpToPowerOf2((uint)expectedBuckets));
    }

    private readonly int _initialBuckets;

    /// <summary>Distinct trigrams seen by this build.</summary>
    public int BucketCount => _bucketCount;

    /// <summary>
    /// Managed bytes the build state holds: the slot table, the bucket array and the arena's
    /// slabs. All of it is pooled, so this is what <see cref="ReleaseBuildBuffers"/> gives back.
    /// </summary>
    public long BuildRetainedBytes
        => (_ascii is null ? 0 : (long)AsciiTableSize * sizeof(int))
         + (long)_buckets.Length * Unsafe.SizeOf<TriBucket>()
         + _arena.RetainedBytes;

    // ── Build ─────────────────────────────────────────────────────────────────

    public void Add(uint localOffset, ReadOnlySpan<char> text)
    {
        if (text.Length < 3) return;

        // Lowercase in place into stack/pooled scratch — no per-add string allocations.
        char[]? rented = text.Length > 1024 ? ArrayPool<char>.Shared.Rent(text.Length) : null;
        Span<char> lower = rented ?? stackalloc char[text.Length];
        int n = text.ToLowerInvariant(lower);
        if (n < 0) { text.CopyTo(lower); n = text.Length; } // never (dest sized to source), but be safe

        AddFolded(localOffset, lower[..n]);

        if (rented is not null) ArrayPool<char>.Shared.Return(rented);
    }

    /// <summary>UTF-8 overload — decodes to chars on the stack, then indexes (no string alloc).</summary>
    public void Add(uint localOffset, ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty) return;
        int charCount = System.Text.Encoding.UTF8.GetCharCount(utf8);
        if (charCount < 3) return;
        char[]? rented = charCount > 1024 ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> chars = rented ?? stackalloc char[charCount];
        System.Text.Encoding.UTF8.GetChars(utf8, chars);
        Add(localOffset, (ReadOnlySpan<char>)chars[..charCount]);
        if (rented is not null) ArrayPool<char>.Shared.Return(rented);
    }

    /// <summary>
    /// Indexes text that is ALREADY case-folded (<c>ToLowerInvariant</c>). Callers that fold
    /// once for several consumers — the builder folds a value for the bloom and the trigram in
    /// one pass — come in here.
    /// </summary>
    internal void AddFolded(uint localOffset, ReadOnlySpan<char> lower)
    {
        int n = lower.Length;
        if (n < 3) return;
        ObjectDisposedException.ThrowIf(_released, this);
        int[] ascii = _ascii ?? RentAsciiTable();
        int offset  = (int)localOffset;

        for (int i = 0; i <= n - 3; i++)
        {
            char c0 = lower[i], c1 = lower[i + 1], c2 = lower[i + 2];
            int id;
            if ((c0 | c1 | c2) < 0x80)
            {
                int k = (c0 << 14) | (c1 << 7) | c2;
                id = ascii[k] - 1;
                if (id < 0) { id = NewBucket(c0, c1, c2); ascii[k] = id + 1; }
            }
            else
            {
                id = WideBucket(c0, c1, c2);
            }
            _arena.Append(ref _buckets[id].Postings, offset, ref _unsorted);
        }
    }

    /// <summary>
    /// Indexes ASCII text that is ALREADY case-folded, as bytes — the builder's fast path, which
    /// folds a raw UTF-8 value once (byte-wise, <c>Ascii.ToLower</c>) for the bloom and the
    /// trigram together. A byte below 0x80 IS its UTF-16 code unit, so the slot-table key is the
    /// same one <see cref="AddFolded"/> computes from chars, and the section is identical.
    /// The caller guarantees every byte is ASCII.
    /// </summary>
    internal void AddFoldedAscii(uint localOffset, ReadOnlySpan<byte> lower)
    {
        int n = lower.Length;
        if (n < 3) return;
        ObjectDisposedException.ThrowIf(_released, this);
        int[] ascii = _ascii ?? RentAsciiTable();
        int offset  = (int)localOffset;

        for (int i = 0; i <= n - 3; i++)
        {
            int k  = (lower[i] << 14) | (lower[i + 1] << 7) | lower[i + 2];
            int id = ascii[k] - 1;
            if (id < 0) { id = NewBucket((char)lower[i], (char)lower[i + 1], (char)lower[i + 2]); ascii[k] = id + 1; }
            _arena.Append(ref _buckets[id].Postings, offset, ref _unsorted);
        }
    }

    private int WideBucket(char c0, char c1, char c2)
    {
        long key = ((long)c0 << 32) | ((long)c1 << 16) | c2;
        var wide = _wide ??= new Dictionary<long, int>();
        if (!wide.TryGetValue(key, out int id))
        {
            id = NewBucket(c0, c1, c2);
            wide.Add(key, id);
        }
        return id;
    }

    private int NewBucket(char c0, char c1, char c2)
    {
        if (_bucketCount == _buckets.Length) GrowBuckets();
        int id = _bucketCount++;
        ref var b = ref _buckets[id];
        b.Postings = default;
        b.C0 = c0; b.C1 = c1; b.C2 = c2;
        return id;
    }

    private void GrowBuckets()
    {
        int size = _buckets.Length == 0 ? _initialBuckets : _buckets.Length * 2;
        var next = IndexBuildPool.Entries<TriBucket>().Rent(size);
        if (_buckets.Length > 0)
        {
            _buckets.AsSpan(0, _bucketCount).CopyTo(next);
            IndexBuildPool.Entries<TriBucket>().Return(_buckets);
        }
        _buckets = next;
    }

    private int[] RentAsciiTable()
    {
        var t = IndexBuildPool.Ints.Rent(AsciiTableSize);
        Array.Clear(t, 0, AsciiTableSize);
        return _ascii = t;
    }

    /// <summary>
    /// Hands the slot table, the bucket array and the posting slabs back to their pools. The
    /// index is unusable afterwards — <see cref="Add(uint, ReadOnlySpan{char})"/> and
    /// <see cref="Serialise"/> throw — because a pooled array that has been returned is
    /// somebody else's the moment they rent it, which is the same hazard
    /// <c>SegmentBloomFilter.Serialise</c> guards against for its native bits.
    /// </summary>
    public void ReleaseBuildBuffers()
    {
        if (_released) return;
        _released = true;
        if (_ascii is not null) { IndexBuildPool.Ints.Return(_ascii); _ascii = null; }
        if (_buckets.Length > 0) { IndexBuildPool.Entries<TriBucket>().Return(_buckets); _buckets = Array.Empty<TriBucket>(); }
        _bucketCount = 0;
        _wide = null;
        _arena.Release();
    }

    /// <summary>
    /// What <see cref="Lookup"/> reads in build mode. Shaped like the dictionary it replaced
    /// (<c>Count</c>, <c>TryGetValue</c>) so the query code does not know the build changed.
    /// </summary>
    private sealed class BuildView(SegmentTrigramIndex owner)
    {
        public int Count => owner._bucketCount;

        public bool TryGetValue((char, char, char) key, out int[] offsets)
        {
            var (c0, c1, c2) = key;
            int id = -1;
            if ((c0 | c1 | c2) < 0x80)
            {
                if (owner._ascii is not null) id = owner._ascii[(c0 << 14) | (c1 << 7) | c2] - 1;
            }
            else if (owner._wide is not null)
            {
                long k = ((long)c0 << 32) | ((long)c1 << 16) | c2;
                if (!owner._wide.TryGetValue(k, out id)) id = -1;
            }
            if (id < 0) { offsets = Array.Empty<int>(); return false; }

            ref readonly var b = ref owner._buckets[id].Postings;
            offsets = new int[b.Count];
            owner._arena.Decode(in b, offsets);
            return true;
        }
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns local offsets that might contain <paramref name="text"/>.
    /// Intersects all trigram sets/arrays. Returns null if text is too short.
    /// </summary>
    /// <summary>
    /// Approximate managed bytes the deserialised index retains — see
    /// <see cref="SegmentInvertedIndex.ApproxRetainedBytes"/> for why the cache budget
    /// uses this instead of the (far smaller) serialized section length.
    /// </summary>
    internal long ApproxRetainedBytes()
    {
        long bytes = 64;                             // dictionary shell
        foreach (var (_, offsets) in _loaded)
            bytes += 48 + 24 + 4L * offsets.Length;  // entry (inline tuple key) + int[]
        return bytes;
    }

    public uint[]? Lookup(ReadOnlySpan<char> text)
    {
        // An index with no trigrams at all was never built (e.g. a WAL-recovery
        // flush before the builder was wired) — that is "no information", not
        // "no matches". Only a POPULATED index may treat a missing trigram as
        // proof of absence.
        if (_loaded.Count == 0 && _sets.Count == 0) return null;

        string lower = text.ToString().ToLowerInvariant();
        if (lower.Length < 3) return null;

        HashSet<int>? result = null;

        for (int i = 0; i <= lower.Length - 3; i++)
        {
            var key = (lower[i], lower[i + 1], lower[i + 2]);

            IEnumerable<int>? candidates = null;
            if (_loaded.TryGetValue(key, out var arr))
                candidates = arr;
            else if (_sets.TryGetValue(key, out var set))
                candidates = set;

            if (candidates is null)
                return Array.Empty<uint>(); // missing trigram → no candidates

            if (result is null)
                result = new HashSet<int>(candidates);
            else
            {
                result.IntersectWith(candidates);
                if (result.Count == 0) return Array.Empty<uint>();
            }
        }

        if (result is null) return null;
        var sorted = result.ToArray();
        Array.Sort(sorted);
        return Array.ConvertAll(sorted, x => (uint)x);
    }

    // ── Serialisation ─────────────────────────────────────────────────────────

    /// <summary>V1 codec: 3 single-BYTE trigram chars + varint postings. Read-only now —
    /// the byte cast silently folded every non-ASCII char to '?' (see <see cref="CodecMagicV2"/>).</summary>
    private const uint CodecMagic = 0xFFFFFFFFu;

    /// <summary>
    /// V2 codec: 3 UTF-16 code units (little-endian uint16) per trigram + varint postings.
    ///
    /// <para>V1 wrote <c>(byte)c</c>, so every trigram char outside U+0000–U+00FF was
    /// truncated — Cyrillic 'п' (U+043F) landed on 0x3F '?'. All non-ASCII trigrams
    /// therefore collapsed onto the same '?' keys on disk, while <see cref="Lookup"/>
    /// builds its key from the real search text: the lookup missed, and a missing
    /// trigram is read as PROOF OF ABSENCE, so substring search over non-ASCII text
    /// returned no rows at all for any flushed segment. Widening the key fixes it;
    /// V1 blobs are still read (their non-ASCII content is unrecoverable, but ASCII
    /// search over pre-upgrade segments keeps working).</para>
    /// </summary>
    private const uint CodecMagicV2 = 0xFFFFFFFEu;

    public byte[] Serialise()
    {
        ObjectDisposedException.ThrowIf(_released, this);
        NormaliseIfUnsorted();
        // Exactly one allocation, of the final size: the arena knows every bucket's encoded
        // length without walking it, so there is no estimate to overshoot and no ToArray copy.
        var blob = new byte[ExactSerialisedSize()];
        var w    = new FixedBufferWriter(blob);
        WriteTo(w);
        return blob;
    }

    /// <summary>
    /// Streams the section into <paramref name="w"/> — the production path, where the writer
    /// hands in a buffer that drains straight to the segment file instead of a managed blob.
    /// </summary>
    public void WriteTo(IBufferWriter<byte> w)
    {
        ObjectDisposedException.ThrowIf(_released, this);
        NormaliseIfUnsorted();

        WriteUInt32(w, CodecMagicV2);
        WriteUInt32(w, (uint)_bucketCount);

        for (int id = 0; id < _bucketCount; id++)
        {
            ref readonly var b = ref _buckets[id];
            int n    = PostingArena.SerialisedSize(in b.Postings);
            var dest = w.GetSpan(6 + 4 + n);
            BinaryPrimitives.WriteUInt16LittleEndian(dest,          b.C0);
            BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(2), b.C1);
            BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(4), b.C2);
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(6), (uint)n);
            _arena.Write(in b.Postings, dest.Slice(10, n));
            w.Advance(10 + n);
        }
    }

    /// <summary>
    /// Bytes <see cref="WriteTo"/> will produce — exact, from the bucket bookkeeping. Normalises
    /// first, like <see cref="WriteTo"/> does: an out-of-order build's postings shrink when they
    /// are sorted and de-duplicated, and a length written ahead of a section that then comes
    /// out shorter is a corrupt file, not a wrong estimate.
    /// </summary>
    public long ExactSerialisedSize()
    {
        ObjectDisposedException.ThrowIf(_released, this);
        NormaliseIfUnsorted();
        long size = 8;
        for (int id = 0; id < _bucketCount; id++)
            size += 10 + PostingArena.SerialisedSize(in _buckets[id].Postings);
        return size;
    }

    /// <summary>
    /// Restores the ascending+distinct invariant the codec requires. A no-op on the
    /// build path (offsets are monotonic — see the ordering contract); only an
    /// out-of-order caller pays for it, and then only once, at serialisation: every bucket is
    /// decoded, sorted, de-duplicated and re-appended into a fresh arena.
    /// </summary>
    private void NormaliseIfUnsorted()
    {
        if (!_unsorted) return;
        var fresh   = new PostingArena();
        int[] tmp   = Array.Empty<int>();
        bool ignore = false;
        for (int id = 0; id < _bucketCount; id++)
        {
            ref var b = ref _buckets[id].Postings;
            if (b.Count < 2) continue;
            if (tmp.Length < b.Count) tmp = new int[Math.Max(b.Count, tmp.Length * 2)];
            int n = _arena.Decode(in b, tmp);
            var span = tmp.AsSpan(0, n);
            span.Sort();
            PostingArena.Bucket nb = default;
            for (int i = 0; i < span.Length; i++)
                if (i == 0 || span[i] != span[i - 1]) fresh.Append(ref nb, span[i], ref ignore);
            b = nb;
        }
        // Buckets with 0-1 postings never touched the old arena; swapping is complete.
        _arena.Release();
        _arena    = fresh;
        _unsorted = false;
    }

    private static void WriteUInt32(IBufferWriter<byte> w, uint v)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(w.GetSpan(4), v);
        w.Advance(4);
    }

    /// <summary>Legacy RoaringBitmap serialisation — retained only to generate blobs for the
    /// backward-compatibility read test.</summary>
    internal byte[] SerialiseRoaringV1()
    {
        ObjectDisposedException.ThrowIf(_released, this);
        NormaliseIfUnsorted();
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        bw.Write((uint)_bucketCount);
        for (int id = 0; id < _bucketCount; id++)
        {
            ref readonly var b = ref _buckets[id];
            bw.Write((byte)b.C0);
            bw.Write((byte)b.C1);
            bw.Write((byte)b.C2);
            int[] sortedArr = new int[b.Postings.Count];
            _arena.Decode(in b.Postings, sortedArr);
            Array.Sort(sortedArr);
            var bm = RoaringBitmap.Create(sortedArr);
            using var bitmapMs = new MemoryStream();
            RoaringBitmap.Serialize(bm, bitmapMs);
            var bitmapBytes = bitmapMs.ToArray();
            bw.Write((uint)bitmapBytes.Length);
            bw.Write(bitmapBytes);
        }
        return ms.ToArray();
    }

    public static SegmentTrigramIndex Deserialise(ReadOnlySpan<byte> data)
    {
        var idx = new SegmentTrigramIndex();
        if (data.IsEmpty) return idx;

        int pos    = 0;
        uint first = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
        bool wide  = first == CodecMagicV2;                  // 3 × uint16 trigram key
        bool codec = wide || first == CodecMagic;            // varint postings either way
        uint count = codec ? BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]) : first;
        if (codec) pos += 4;

        for (uint i = 0; i < count; i++)
        {
            char c0, c1, c2;
            if (wide)
            {
                c0 = (char)BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
                c1 = (char)BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
                c2 = (char)BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
            }
            else
            {
                // Legacy single-byte keys: non-ASCII was already destroyed at write time.
                c0 = (char)data[pos++];
                c1 = (char)data[pos++];
                c2 = (char)data[pos++];
            }

            uint bmLen  = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
            var bmBytes = data.Slice(pos, (int)bmLen); pos += (int)bmLen;

            idx._loaded[(c0, c1, c2)] = codec ? DecodeCodec(bmBytes) : DecodeRoaring(bmBytes);
        }

        return idx;
    }

    private static int[] DecodeCodec(ReadOnlySpan<byte> bytes)
    {
        int count = SegmentBitmapCodec.Count(bytes);
        if (count == 0) return Array.Empty<int>();
        var arr = new int[count];
        SegmentBitmapCodec.Decode(bytes, arr);
        return arr;
    }

    private static int[] DecodeRoaring(ReadOnlySpan<byte> bytes)
    {
        using var ms = new MemoryStream(bytes.ToArray());
        var bm = RoaringBitmap.Deserialize(ms);
        var list = new List<int>();
        foreach (int x in bm) list.Add(x);
        return list.ToArray();
    }
}
