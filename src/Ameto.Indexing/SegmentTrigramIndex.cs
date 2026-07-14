using System.Buffers.Binary;
using Collections.Special;

namespace Ameto.Indexing;

/// <summary>
/// Trigram index for fast substring / prefix search on @mt (message template) and @m (rendered message).
///
/// Building phase: accumulates offsets in <see cref="HashSet{T}"/> per trigram.
/// Serialisation: converts to <see cref="RoaringBitmap"/> for compact on-disk storage.
/// Deserialisation: iterates bitmap back into sorted arrays for fast intersection.
/// </summary>
public sealed class SegmentTrigramIndex
{
    // Building phase: mutable sets
    private readonly Dictionary<(char, char, char), HashSet<int>> _sets = new();

    // Loaded phase (deserialised): sorted arrays for intersection
    private readonly Dictionary<(char, char, char), int[]> _loaded = new();

    private readonly object _lock = new();

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
                if (!_sets.TryGetValue(key, out var set))
                {
                    set = new HashSet<int>();
                    _sets[key] = set;
                }
                set.Add(offset);
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

    /// <summary>Marks the codec posting-list format; a legacy blob starts with trigramCount (never this).</summary>
    private const uint CodecMagic = 0xFFFFFFFFu;

    public byte[] Serialise()
    {
        lock (_lock)
        {
            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms);
            byte[] code = System.Buffers.ArrayPool<byte>.Shared.Rent(1024);
            try
            {
                bw.Write(CodecMagic);
                bw.Write((uint)_sets.Count);

                foreach (var ((c0, c1, c2), set) in _sets)
                {
                    bw.Write((byte)c0);
                    bw.Write((byte)c1);
                    bw.Write((byte)c2);

                    int[] sortedArr = set.ToArray();
                    Array.Sort(sortedArr);
                    int need = SegmentBitmapCodec.MaxEncodedSize(sortedArr.Length);
                    if (need > code.Length)
                    {
                        System.Buffers.ArrayPool<byte>.Shared.Return(code);
                        code = System.Buffers.ArrayPool<byte>.Shared.Rent(need);
                    }
                    int n = SegmentBitmapCodec.Encode(sortedArr, code);
                    bw.Write((uint)n);
                    bw.Write(code, 0, n);
                }

                return ms.ToArray();
            }
            finally { System.Buffers.ArrayPool<byte>.Shared.Return(code); }
        }
    }

    /// <summary>Legacy RoaringBitmap serialisation — retained only to generate blobs for the
    /// backward-compatibility read test.</summary>
    internal byte[] SerialiseRoaringV1()
    {
        lock (_lock)
        {
            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms);
            bw.Write((uint)_sets.Count);
            foreach (var ((c0, c1, c2), set) in _sets)
            {
                bw.Write((byte)c0);
                bw.Write((byte)c1);
                bw.Write((byte)c2);
                int[] sortedArr = set.ToArray();
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
        bool codec = first == CodecMagic;
        uint count = codec ? BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]) : first;
        if (codec) pos += 4;

        for (uint i = 0; i < count; i++)
        {
            char c0 = (char)data[pos++];
            char c1 = (char)data[pos++];
            char c2 = (char)data[pos++];

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
