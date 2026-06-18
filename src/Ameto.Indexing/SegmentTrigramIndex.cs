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

        string lower  = text.ToString().ToLowerInvariant();
        int    offset = (int)localOffset;

        lock (_lock)
        {
            for (int i = 0; i <= lower.Length - 3; i++)
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

    public byte[] Serialise()
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
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;

        for (uint i = 0; i < count; i++)
        {
            char c0 = (char)data[pos++];
            char c1 = (char)data[pos++];
            char c2 = (char)data[pos++];

            uint bitmapLen    = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
            var bitmapBytes   = data.Slice(pos, (int)bitmapLen).ToArray(); pos += (int)bitmapLen;

            using var bitmapMs = new MemoryStream(bitmapBytes);
            var bm   = RoaringBitmap.Deserialize(bitmapMs);
            var list = new List<int>();
            foreach (int x in bm) list.Add(x);
            idx._loaded[(c0, c1, c2)] = list.ToArray();
        }

        return idx;
    }
}
