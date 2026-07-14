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

    // Query-phase (populated after Deserialise): ascending offset arrays per (name,value).
    // Decoded from the segment's posting lists — SegmentBitmapCodec for current segments,
    // RoaringBitmap for legacy ones (see Deserialise / the codec magic marker).
    private Dictionary<string, Dictionary<string, int[]>>? _postings;

    /// <summary>Marks the codec posting-list format; a legacy blob starts with propertyCount (never this).</summary>
    private const uint CodecMagic = 0xFFFFFFFFu;

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

    /// <summary>
    /// Span overload for the zero-alloc flush walk: <paramref name="serialisedValueUtf8"/> is the
    /// already-serialised value form (matching <see cref="SerialiseValue"/>, e.g. <c>\0l123</c>).
    /// Interns the property name and value key by span, so a string is allocated only the first
    /// time each distinct (name, value) is seen — not per event (both are low-cardinality).
    /// </summary>
    public void AddSpan(uint localOffset, ReadOnlySpan<char> propertyName, ReadOnlySpan<char> serialisedValueUtf8)
    {
        int offset = (int)localOffset;
        lock (_writeLock)
        {
            var outer = _index.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!outer.TryGetValue(propertyName, out var values))
            {
                values = new Dictionary<string, List<int>>(StringComparer.Ordinal);
                _index[new string(propertyName)] = values;
            }

            var inner = values.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!inner.TryGetValue(serialisedValueUtf8, out var list))
            {
                list = new List<int>();
                values[new string(serialisedValueUtf8)] = list;
            }

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

        // Query mode: return a copy of the decoded ascending offsets.
        if (_postings is not null)
        {
            if (_postings.TryGetValue(propertyName, out var values) &&
                values.TryGetValue(serialised, out var offsets))
                return ToUInt(offsets);
            return null;
        }

        // Build mode: enumerate from List<int>
        if (_index.TryGetValue(propertyName, out var buildValues) &&
            buildValues.TryGetValue(serialised, out var list))
            return list.Select(x => (uint)x).ToArray();

        return null;
    }

    public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates)
    {
        if (_postings is null || predicates.Count == 0) return null;

        // Gather each predicate's ascending offset array; empty match ⇒ AND is empty.
        var lists = new int[predicates.Count][];
        for (int i = 0; i < predicates.Count; i++)
        {
            string serialised = SerialiseValue(predicates[i].value);
            if (!_postings.TryGetValue(predicates[i].property, out var values) ||
                !values.TryGetValue(serialised, out var offsets))
                return Array.Empty<uint>();
            lists[i] = offsets;
        }

        // Intersect ascending arrays, smallest first (merge against the running result).
        Array.Sort(lists, static (a, b) => a.Length - b.Length);
        int[] acc = lists[0];
        for (int i = 1; i < lists.Length && acc.Length > 0; i++)
            acc = IntersectSorted(acc, lists[i]);

        return ToUInt(acc);
    }

    public bool MightContain(string propertyName, object? value)
    {
        string serialised = SerialiseValue(value);

        if (_postings is not null)
        {
            if (!_postings.TryGetValue(propertyName, out var values)) return true; // property not indexed
            return values.ContainsKey(serialised);
        }

        if (!_index.TryGetValue(propertyName, out var buildValues))
            return true;
        return buildValues.ContainsKey(serialised);
    }

    /// <summary>Intersects two ascending, distinct int arrays into a new ascending array.</summary>
    private static int[] IntersectSorted(int[] a, int[] b)
    {
        var outp = new int[Math.Min(a.Length, b.Length)];
        int i = 0, j = 0, k = 0;
        while (i < a.Length && j < b.Length)
        {
            int x = a[i], y = b[j];
            if      (x < y) i++;
            else if (x > y) j++;
            else { outp[k++] = x; i++; j++; }
        }
        return k == outp.Length ? outp : outp[..k];
    }

    private static uint[] ToUInt(int[] offsets)
    {
        var r = new uint[offsets.Length];
        for (int i = 0; i < offsets.Length; i++) r[i] = (uint)offsets[i];
        return r;
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
            byte[] utf8 = System.Buffers.ArrayPool<byte>.Shared.Rent(256);
            byte[] code = System.Buffers.ArrayPool<byte>.Shared.Rent(1024);
            try
            {
                bw.Write(CodecMagic);          // distinguishes the codec format from a legacy propertyCount
                bw.Write((uint)_index.Count);

                foreach (var (propName, values) in _index)
                {
                    WriteUtf8(bw, propName, ref utf8);
                    bw.Write((uint)values.Count);

                    foreach (var (valStr, list) in values)
                    {
                        WriteUtf8(bw, valStr, ref utf8);

                        // Encode the ascending offset list with the zero-alloc codec instead of
                        // RoaringBitmap.Create+Serialize — the flush allocation hot spot.
                        var offsets = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(list);
                        int need = SegmentBitmapCodec.MaxEncodedSize(offsets.Length);
                        if (need > code.Length)
                        {
                            System.Buffers.ArrayPool<byte>.Shared.Return(code);
                            code = System.Buffers.ArrayPool<byte>.Shared.Rent(need);
                        }
                        int n = SegmentBitmapCodec.Encode(offsets, code);
                        bw.Write((uint)n);
                        bw.Write(code, 0, n);
                    }
                }

                return ms.ToArray();
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(utf8);
                System.Buffers.ArrayPool<byte>.Shared.Return(code);
            }
        }
    }

    /// <summary>Legacy RoaringBitmap serialisation — retained only to generate blobs for the
    /// backward-compatibility read test (current writers emit the codec format above).</summary>
    internal byte[] SerialiseRoaringV1()
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

    /// <summary>Writes a length-prefixed UTF-8 string via a reused scratch buffer (no per-call byte[]).</summary>
    internal static void WriteUtf8(BinaryWriter bw, string s, ref byte[] scratch)
    {
        int max = System.Text.Encoding.UTF8.GetMaxByteCount(s.Length);
        if (max > scratch.Length)
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(scratch);
            scratch = System.Buffers.ArrayPool<byte>.Shared.Rent(max);
        }
        int n = System.Text.Encoding.UTF8.GetBytes(s, scratch);
        bw.Write((ushort)n);
        bw.Write(scratch, 0, n);
    }

    public static SegmentInvertedIndex Deserialise(ReadOnlySpan<byte> data)
    {
        var idx = new SegmentInvertedIndex();
        if (data.IsEmpty) return idx;

        idx._postings = new Dictionary<string, Dictionary<string, int[]>>(StringComparer.Ordinal);

        int pos    = 0;
        uint first = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
        bool codec = first == CodecMagic;                 // legacy blobs start with propertyCount
        uint propCount = codec ? BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]) : first;
        if (codec) pos += 4;

        for (uint p = 0; p < propCount; p++)
        {
            ushort nameLen  = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
            string propName = System.Text.Encoding.UTF8.GetString(data.Slice(pos, nameLen)); pos += nameLen;

            uint valCount   = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
            var values      = new Dictionary<string, int[]>(StringComparer.Ordinal);

            for (uint v = 0; v < valCount; v++)
            {
                ushort valLen  = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
                string valStr  = System.Text.Encoding.UTF8.GetString(data.Slice(pos, valLen)); pos += valLen;

                uint bmLen  = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
                var bmBytes = data.Slice(pos, (int)bmLen); pos += (int)bmLen;

                values[valStr] = codec ? DecodeCodec(bmBytes) : DecodeRoaring(bmBytes);
            }

            idx._postings[propName] = values;
        }

        return idx;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

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
        foreach (int x in bm) list.Add(x);  // RoaringBitmap enumerates ascending
        return list.ToArray();
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
