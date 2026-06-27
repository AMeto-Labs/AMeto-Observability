using System.Buffers.Binary;
using Collections.Special;
using Ameto.Core;

namespace Ameto.Indexing;

/// <summary>
/// Inverted index for a single segment: maps (propertyName, value) → sorted int list of local event offsets.
///
/// Building phase (hot-tier): accumulates offsets in <see cref="List{T}"/> per value bucket.
/// Serialisation: converts each list to a <see cref="RoaringBitmap"/> for compact storage.
/// Deserialisation: iterates bitmap values back into sorted arrays for fast lookup.
///
/// Thread safety: live index uses a lock for writes; cold index is read-only after load.
/// </summary>
public sealed class SegmentInvertedIndex : ISegmentIndex
{
    // Build-phase: propertyName → (serialisedValue → sorted offsets)
    private readonly Dictionary<string, Dictionary<string, List<int>>> _index
        = new(StringComparer.Ordinal);
    private readonly object _writeLock = new();

    // Query-phase (populated after Deserialise): keeps RoaringBitmaps in memory
    // instead of converting to List<int>, so AND-intersection is O(bitmap) not O(n×m).
    private Dictionary<string, Dictionary<string, RoaringBitmap>>? _bitmaps;

    // ── Build (hot path) ──────────────────────────────────────────────────────

    public void Add(uint localOffset, string propertyName, object? value)
    {
        string serialised = SerialiseValue(value);
        int offset        = (int)localOffset;

        lock (_writeLock)
        {
            if (!_index.TryGetValue(propertyName, out var values))
            {
                values = new Dictionary<string, List<int>>(StringComparer.Ordinal);
                _index[propertyName] = values;
            }

            if (!values.TryGetValue(serialised, out var list))
            {
                list = new List<int>();
                values[serialised] = list;
            }

            // Offsets arrive in monotonically increasing order during a single flush — no sort needed.
            if (list.Count == 0 || list[^1] != offset)
                list.Add(offset);
        }
    }

    public void AddEvent(uint localOffset, LogLevel level, Dictionary<string, object?>? properties)
    {
        Add(localOffset, "@l", level.ToSeqString());

        if (properties is null) return;
        foreach (var (k, v) in properties)
            Add(localOffset, k, v);
    }

    // ── ISegmentIndex ─────────────────────────────────────────────────────────

    public uint[]? Lookup(string propertyName, object? value)
    {
        string serialised = SerialiseValue(value);

        // Query mode: enumerate from bitmap (no List<int> kept after deserialization)
        if (_bitmaps is not null)
        {
            if (_bitmaps.TryGetValue(propertyName, out var bmValues) &&
                bmValues.TryGetValue(serialised, out var bm))
                return EnumerateBitmap(bm);
            return null;
        }

        // Build mode: enumerate from List<int>
        if (_index.TryGetValue(propertyName, out var values) &&
            values.TryGetValue(serialised, out var list))
            return list.Select(x => (uint)x).ToArray();

        return null;
    }

    public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates)
    {
        if (_bitmaps is null || predicates.Count == 0) return null;

        HashSet<uint>? result = null;
        foreach (var (prop, val) in predicates)
        {
            string serialised = SerialiseValue(val);
            if (!_bitmaps.TryGetValue(prop, out var bmValues) ||
                !bmValues.TryGetValue(serialised, out var bm))
                return Array.Empty<uint>(); // this predicate has zero matches → AND = empty

            if (result is null)
            {
                result = new HashSet<uint>();
                foreach (int x in bm) result.Add((uint)x);
            }
            else
            {
                // Intersect: keep only offsets present in both sets
                var keep = new HashSet<uint>(capacity: result.Count);
                foreach (int x in bm)
                {
                    uint u = (uint)x;
                    if (result.Contains(u)) keep.Add(u);
                }
                result = keep;
                if (result.Count == 0) return Array.Empty<uint>();
            }
        }

        if (result is null) return null;
        var arr = new uint[result.Count];
        result.CopyTo(arr);
        Array.Sort(arr); // offsets must be ascending for ScanSegmentAsync
        return arr;
    }

    public bool MightContain(string propertyName, object? value)
    {
        string serialised = SerialiseValue(value);

        if (_bitmaps is not null)
        {
            if (!_bitmaps.ContainsKey(propertyName)) return true; // property not indexed → can't conclude absence
            return _bitmaps[propertyName].ContainsKey(serialised);
        }

        if (!_index.TryGetValue(propertyName, out var values))
            return true;
        return values.ContainsKey(serialised);
    }

    public uint[]? LookupTrigram(ReadOnlySpan<char> text) => null; // handled by TrigramIndex

    // ── Serialisation ─────────────────────────────────────────────────────────

    /// <summary>
    /// Binary format:
    ///   uint32 propertyCount
    ///   per property:
    ///     uint16 nameLen, name utf8
    ///     uint32 valueCount
    ///     per value:
    ///       uint16 valueLen, value utf8
    ///       uint32 bitmapLen, RoaringBitmap bytes
    /// </summary>
    public byte[] Serialise()
    {
        lock (_writeLock)
        {
            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms);

            bw.Write((uint)_index.Count);

            foreach (var (propName, values) in _index)
            {
                var nameBytes = System.Text.Encoding.UTF8.GetBytes(propName);
                bw.Write((ushort)nameBytes.Length);
                bw.Write(nameBytes);
                bw.Write((uint)values.Count);

                foreach (var (valStr, list) in values)
                {
                    var valBytes = System.Text.Encoding.UTF8.GetBytes(valStr);
                    bw.Write((ushort)valBytes.Length);
                    bw.Write(valBytes);

                    // Build RoaringBitmap from sorted int list
                    var bm = RoaringBitmap.Create(list.ToArray());
                    using var bitmapMs = new MemoryStream();
                    RoaringBitmap.Serialize(bm, bitmapMs);
                    var bitmapBytes = bitmapMs.ToArray();
                    bw.Write((uint)bitmapBytes.Length);
                    bw.Write(bitmapBytes);
                }
            }

            return ms.ToArray();
        }
    }

    public static SegmentInvertedIndex Deserialise(ReadOnlySpan<byte> data)
    {
        var idx = new SegmentInvertedIndex();
        if (data.IsEmpty) return idx;

        idx._bitmaps = new Dictionary<string, Dictionary<string, RoaringBitmap>>(StringComparer.Ordinal);

        int pos        = 0;
        uint propCount = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;

        for (uint p = 0; p < propCount; p++)
        {
            ushort nameLen  = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
            string propName = System.Text.Encoding.UTF8.GetString(data.Slice(pos, nameLen)); pos += nameLen;

            uint valCount   = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
            var bmValues    = new Dictionary<string, RoaringBitmap>(StringComparer.Ordinal);

            for (uint v = 0; v < valCount; v++)
            {
                ushort valLen  = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
                string valStr  = System.Text.Encoding.UTF8.GetString(data.Slice(pos, valLen)); pos += valLen;

                uint bitmapLen  = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
                var bitmapBytes = data.Slice(pos, (int)bitmapLen).ToArray(); pos += (int)bitmapLen;

                using var bitmapMs = new MemoryStream(bitmapBytes);
                // Keep the RoaringBitmap in memory — no conversion to List<int>.
                // LookupIntersect() uses bitmaps directly for O(n) AND intersection.
                bmValues[valStr] = RoaringBitmap.Deserialize(bitmapMs);
            }

            idx._bitmaps[propName] = bmValues;
        }

        return idx;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static uint[] EnumerateBitmap(RoaringBitmap bm)
    {
        var list = new List<uint>();
        foreach (int x in bm) list.Add((uint)x);
        return [.. list];
    }

    private static string SerialiseValue(object? value) => value switch
    {
        null          => "\0null",
        bool b        => b ? "\0true" : "\0false",
        int i         => $"\0i{i}",
        long l        => $"\0l{l}",
        double d      => $"\0d{d:R}",
        float f       => $"\0f{f:R}",
        string s      => s,
        _             => value.ToString() ?? string.Empty,
    };
}
