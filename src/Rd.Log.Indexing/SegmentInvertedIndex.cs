using System.Buffers.Binary;
using Collections.Special;
using Rd.Log.Core;

namespace Rd.Log.Indexing;

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
    // propertyName → (serialisedValue → sorted offsets)
    private readonly Dictionary<string, Dictionary<string, List<int>>> _index
        = new(StringComparer.Ordinal);
    private readonly object _writeLock = new();

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
        if (_index.TryGetValue(propertyName, out var values) &&
            values.TryGetValue(serialised, out var list))
        {
            return list.Select(x => (uint)x).ToArray();
        }
        return null;
    }

    public bool MightContain(string propertyName, object? value)
    {
        // If the property was never added to this inverted index (e.g. @mt, which is
        // indexed only in the trigram/bloom structures), we cannot conclude absence —
        // return true so the segment is not falsely eliminated.
        if (!_index.TryGetValue(propertyName, out var values))
            return true;

        string serialised = SerialiseValue(value);
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

        int pos        = 0;
        uint propCount = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;

        for (uint p = 0; p < propCount; p++)
        {
            ushort nameLen  = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
            string propName = System.Text.Encoding.UTF8.GetString(data.Slice(pos, nameLen)); pos += nameLen;

            uint valCount   = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
            var values      = new Dictionary<string, List<int>>(StringComparer.Ordinal);

            for (uint v = 0; v < valCount; v++)
            {
                ushort valLen  = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
                string valStr  = System.Text.Encoding.UTF8.GetString(data.Slice(pos, valLen)); pos += valLen;

                uint bitmapLen    = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
                var bitmapBytes   = data.Slice(pos, (int)bitmapLen).ToArray(); pos += (int)bitmapLen;

                using var bitmapMs = new MemoryStream(bitmapBytes);
                var bm    = RoaringBitmap.Deserialize(bitmapMs);
                var list  = new List<int>();
                foreach (int x in bm) list.Add(x);

                values[valStr] = list;
            }

            idx._index[propName] = values;
        }

        return idx;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

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
