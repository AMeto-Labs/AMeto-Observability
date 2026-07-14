namespace Ameto.Indexing;

/// <summary>
/// Zero-allocation codec for a segment index posting list — the set of ascending, distinct
/// local event offsets attached to one (property,value) bucket or trigram.
///
/// Replaces per-bucket <c>RoaringBitmap.Create + Serialize</c> (which allocates container
/// objects and an intermediate stream for every one of the ~hundreds-of-thousands of buckets
/// in a segment — the flush GC hot spot). Encoding is delta+varint:
///   varint count, then per offset a varint of <c>(offset - prev - 1)</c> (prev = -1 initially).
/// Distinct ascending offsets guarantee the gap ≥ 1, so the stored delta ≥ 0; a contiguous run
/// collapses to one zero byte per offset, and the common single-offset high-cardinality bucket
/// costs 2–6 bytes with no heap allocation at all.
///
/// Encode writes into a caller buffer; Decode / <see cref="Enumerate"/> read without allocating,
/// so both flush (encode) and query (decode/intersect) stay off the GC heap.
/// </summary>
public static class SegmentBitmapCodec
{
    /// <summary>Upper bound on encoded size for <paramref name="count"/> offsets (worst-case varints).</summary>
    public static int MaxEncodedSize(int count) => VarintMax + count * VarintMax;

    private const int VarintMax = 5; // a uint is at most 5 varint bytes

    /// <summary>
    /// Encodes ascending, distinct <paramref name="offsets"/> into <paramref name="dest"/>.
    /// Returns the number of bytes written, or -1 if <paramref name="dest"/> is too small
    /// (size it with <see cref="MaxEncodedSize"/>).
    /// </summary>
    public static int Encode(ReadOnlySpan<int> offsets, Span<byte> dest)
    {
        int pos = 0;
        if (!WriteVarint((uint)offsets.Length, dest, ref pos)) return -1;

        int prev = -1;
        for (int i = 0; i < offsets.Length; i++)
        {
            int o = offsets[i];
            // Distinct + ascending ⇒ o > prev ⇒ (o - prev - 1) >= 0.
            if (!WriteVarint((uint)(o - prev - 1), dest, ref pos)) return -1;
            prev = o;
        }
        return pos;
    }

    /// <summary>Number of offsets encoded in <paramref name="src"/> (reads only the leading count varint).</summary>
    public static int Count(ReadOnlySpan<byte> src)
    {
        int pos = 0;
        return (int)ReadVarint(src, ref pos);
    }

    /// <summary>
    /// Decodes offsets into <paramref name="dest"/> (must hold at least <see cref="Count"/> ints).
    /// Returns the number of offsets written.
    /// </summary>
    public static int Decode(ReadOnlySpan<byte> src, Span<int> dest)
    {
        int pos = 0;
        int count = (int)ReadVarint(src, ref pos);
        int prev = -1;
        for (int i = 0; i < count; i++)
        {
            int o = prev + 1 + (int)ReadVarint(src, ref pos);
            dest[i] = o;
            prev = o;
        }
        return count;
    }

    /// <summary>Allocation-free forward iterator over the encoded offsets (for query-time intersection).</summary>
    public static Enumerator Enumerate(ReadOnlySpan<byte> src) => new(src);

    public ref struct Enumerator
    {
        private readonly ReadOnlySpan<byte> _src;
        private int _pos;
        private int _remaining;
        private int _prev;

        internal Enumerator(ReadOnlySpan<byte> src)
        {
            _src       = src;
            _pos       = 0;
            _remaining = (int)ReadVarint(src, ref _pos);
            _prev      = -1;
            Current    = 0;
        }

        public int Current { get; private set; }

        public bool MoveNext()
        {
            if (_remaining == 0) return false;
            _remaining--;
            Current = _prev + 1 + (int)ReadVarint(_src, ref _pos);
            _prev   = Current;
            return true;
        }
    }

    // ── Varint (LEB128, unsigned) ───────────────────────────────────────────────

    private static bool WriteVarint(uint v, Span<byte> dest, ref int pos)
    {
        while (v >= 0x80)
        {
            if ((uint)pos >= (uint)dest.Length) return false;
            dest[pos++] = (byte)(v | 0x80);
            v >>= 7;
        }
        if ((uint)pos >= (uint)dest.Length) return false;
        dest[pos++] = (byte)v;
        return true;
    }

    private static uint ReadVarint(ReadOnlySpan<byte> src, ref int pos)
    {
        uint v = 0;
        int shift = 0;
        byte b;
        do
        {
            b = src[pos++];
            v |= (uint)(b & 0x7F) << shift;
            shift += 7;
        }
        while ((b & 0x80) != 0);
        return v;
    }
}
