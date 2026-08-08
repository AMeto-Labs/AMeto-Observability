using System.Buffers.Binary;
using Collections.Special;

namespace Ameto.Indexing;

/// <summary>
/// Trigram index for fast substring / prefix search on @mt (message template) and @m (rendered message).
///
/// Building phase: accumulates offsets in an ascending <see cref="List{T}"/> per trigram.
/// Serialisation: delta+varint via <see cref="SegmentBitmapCodec"/>.
/// Deserialisation: decodes back into sorted arrays for fast intersection.
///
/// <para>ORDERING CONTRACT — the reason this is a list and not a set:
/// <c>SegmentIndexBuilder.Build</c> walks <c>pos = 0..hot.Count</c> and passes
/// <c>offset = pos</c>, so offsets arrive MONOTONICALLY NON-DECREASING. The only
/// duplicate a build can produce is a repeat of the CURRENT offset (the same trigram
/// occurring in both the template and a property value of one event), which a
/// <c>list[^1] != offset</c> tail check removes — exactly what
/// <see cref="SegmentInvertedIndex"/> already does. A <c>HashSet&lt;int&gt;</c> cost
/// 18.4 B per (trigram, offset) pair against 7.6 B for the list, and the trigram index
/// was ~84 % of the flush-path index-build peak (560 MB of 663 MB for one 64 MB tier),
/// so this is the single largest lever on flush memory. Out-of-order offsets remain
/// CORRECT — <see cref="_unsorted"/> records the violation and serialisation sorts —
/// they merely give up the free ordering.</para>
/// </summary>
public sealed class SegmentTrigramIndex
{
    // Building phase: ascending, distinct offsets per trigram (see ordering contract above).
    private readonly Dictionary<(char, char, char), List<int>> _sets = new();

    // Loaded phase (deserialised): sorted arrays for intersection
    private readonly Dictionary<(char, char, char), int[]> _loaded = new();

    private readonly object _lock = new();

    /// <summary>Set when an offset arrived below its bucket's tail (contract violation).
    /// Serialisation then sorts before encoding, which requires ascending input.</summary>
    private bool _unsorted;

    // ── Build ─────────────────────────────────────────────────────────────────

    public void Add(uint localOffset, ReadOnlySpan<char> text)
    {
        if (text.Length < 3) return;

        // Lowercase in place into stack/pooled scratch — no per-add string allocations.
        char[]? rented = text.Length > 1024 ? System.Buffers.ArrayPool<char>.Shared.Rent(text.Length) : null;
        Span<char> lower = rented ?? stackalloc char[text.Length];
        int n = text.ToLowerInvariant(lower);
        if (n < 0) { text.CopyTo(lower); n = text.Length; } // never (dest sized to source), but be safe
        int offset = (int)localOffset;

        lock (_lock)
        {
            for (int i = 0; i <= n - 3; i++)
            {
                var key = (lower[i], lower[i + 1], lower[i + 2]);
                if (!_sets.TryGetValue(key, out var list))
                {
                    // Capacity 1: the overwhelming majority of trigrams in a segment are
                    // rare, and List's default growth handles the dense ones. The default
                    // capacity-4 array would waste 12 B on every one of the ~100k buckets.
                    list = new List<int>(1);
                    _sets[key] = list;
                }
                // Monotonic offsets ⇒ a tail check is a complete de-duplication.
                if (list.Count == 0)          list.Add(offset);
                else if (list[^1] < offset)   list.Add(offset);
                else if (list[^1] > offset) { list.Add(offset); _unsorted = true; }
                // list[^1] == offset ⇒ duplicate within one event, drop it.
            }
        }
        if (rented is not null) System.Buffers.ArrayPool<char>.Shared.Return(rented);
    }

    /// <summary>UTF-8 overload — decodes to chars on the stack, then indexes (no string alloc).</summary>
    public void Add(uint localOffset, ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty) return;
        int charCount = System.Text.Encoding.UTF8.GetCharCount(utf8);
        if (charCount < 3) return;
        char[]? rented = charCount > 1024 ? System.Buffers.ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> chars = rented ?? stackalloc char[charCount];
        System.Text.Encoding.UTF8.GetChars(utf8, chars);
        Add(localOffset, (ReadOnlySpan<char>)chars[..charCount]);
        if (rented is not null) System.Buffers.ArrayPool<char>.Shared.Return(rented);
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns local offsets that might contain <paramref name="text"/>.
    /// Intersects all trigram sets/arrays. Returns null if text is too short.
    /// </summary>
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
        lock (_lock)
        {
            NormaliseIfUnsorted();

            // Size the buffer up front: BinaryWriter over MemoryStream doubles its
            // internal array as it grows and ms.ToArray() then copies the whole blob
            // again — for a multi-MB index that is two LOH allocations plus every
            // intermediate doubling, all of which the Workstation GC never compacts.
            var w = new System.Buffers.ArrayBufferWriter<byte>(EstimateSerialisedSize());
            WriteUInt32(w, CodecMagicV2);
            WriteUInt32(w, (uint)_sets.Count);

            foreach (var ((c0, c1, c2), list) in _sets)
            {
                WriteUInt16(w, c0);
                WriteUInt16(w, c1);
                WriteUInt16(w, c2);

                // Already ascending and distinct by construction — encode straight out
                // of the list's backing store, no per-bucket ToArray()/Sort().
                var offsets = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(list);
                // checked: see SegmentInvertedIndex.Serialise — a truncated size request turns
                // Encode's -1 ("did not fit") into a corrupt posting-list length prefix.
                var dest    = w.GetSpan(checked((int)(4 + SegmentBitmapCodec.MaxEncodedSize(offsets.Length))));
                int n       = SegmentBitmapCodec.Encode(offsets, dest[4..]);
                System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)n);
                w.Advance(4 + n);
            }

            return w.WrittenSpan.ToArray();
        }
    }

    /// <summary>Rough upper bound so the writer allocates once: header + per-bucket key,
    /// length prefix and one varint byte per posting (the dense common case).</summary>
    private int EstimateSerialisedSize()
    {
        long postings = 0;
        foreach (var list in _sets.Values) postings += list.Count;
        return (int)Math.Min(int.MaxValue - 64, 8 + (long)_sets.Count * 10 + postings + 64);
    }

    /// <summary>
    /// Restores the ascending+distinct invariant the codec requires. A no-op on the
    /// build path (offsets are monotonic — see the ordering contract); only an
    /// out-of-order caller pays for it, and then only once, at serialisation.
    /// </summary>
    private void NormaliseIfUnsorted()
    {
        if (!_unsorted) return;
        foreach (var list in _sets.Values)
        {
            var span = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(list);
            span.Sort();
            int w = 0;
            for (int i = 0; i < span.Length; i++)
                if (i == 0 || span[i] != span[i - 1]) span[w++] = span[i];
            if (w != list.Count) list.RemoveRange(w, list.Count - w);
        }
        _unsorted = false;
    }

    private static void WriteUInt32(System.Buffers.ArrayBufferWriter<byte> w, uint v)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(w.GetSpan(4), v);
        w.Advance(4);
    }

    private static void WriteUInt16(System.Buffers.ArrayBufferWriter<byte> w, char c)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(w.GetSpan(2), c);
        w.Advance(2);
    }

    /// <summary>Legacy RoaringBitmap serialisation — retained only to generate blobs for the
    /// backward-compatibility read test.</summary>
    internal byte[] SerialiseRoaringV1()
    {
        lock (_lock)
        {
            NormaliseIfUnsorted();
            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms);
            bw.Write((uint)_sets.Count);
            foreach (var ((c0, c1, c2), list) in _sets)
            {
                bw.Write((byte)c0);
                bw.Write((byte)c1);
                bw.Write((byte)c2);
                int[] sortedArr = list.ToArray();
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
