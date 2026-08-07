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
        // Query mode: return a copy of the decoded ascending offsets.
        if (_postings is not null)
        {
            var offsets = PostingsForKey(propertyName, value, out _);
            return offsets is null ? null : ToUInt(offsets);
        }

        // Build mode (test-only: production queries always read a deserialised index).
        if (_index.TryGetValue(propertyName, out var buildValues) &&
            buildValues.TryGetValue(SerialiseValue(value), out var list))
            return list.Select(x => (uint)x).ToArray();

        return null;
    }

    /// <summary>
    /// Intersects the posting lists of an AND-chain of equality predicates.
    ///
    /// <para>Return values are three different statements and the caller acts on each
    /// differently: <c>null</c> is "I cannot narrow this, scan the segment", a non-empty
    /// array is "these offsets and no others", and an EMPTY array is "no event in this
    /// segment matches" — which makes the caller drop the segment unread.</para>
    ///
    /// <para>A property this index never wrote is the first case, not the third. The index
    /// has no opinion about a bucket it does not have, and answering "no matches" for one
    /// turns every filter/index naming disagreement into missing rows instead of a slower
    /// query. The predicate is therefore dropped from the intersection and re-checked by the
    /// scan. A property that IS present with NO FORM of the value (see
    /// <see cref="IndexValueForms.Serialised"/>) is the third case: the bucket is complete, so
    /// its silence is proof.</para>
    /// </summary>
    public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates)
    {
        if (_postings is null || predicates.Count == 0) return null;

        int[][]? lists = null;
        int used = 0;
        for (int i = 0; i < predicates.Count; i++)
        {
            int[]? offsets = PostingsForKey(predicates[i].property, predicates[i].value, out bool known);
            if (!known) continue;                           // unknown property → no information
            if (offsets is null)
                return Array.Empty<uint>();                 // known bucket, absent value → proven empty

            lists ??= new int[predicates.Count][];
            lists[used++] = offsets;
        }

        if (used == 0) return null;                          // nothing usable → scan
        if (used < lists!.Length) Array.Resize(ref lists, used);

        // Intersect ascending arrays, smallest first (merge against the running result).
        Array.Sort(lists, static (a, b) => a.Length - b.Length);
        int[] acc = lists[0];
        for (int i = 1; i < lists.Length && acc.Length > 0; i++)
            acc = IntersectSorted(acc, lists[i]);

        return ToUInt(acc);
    }

    public bool MightContain(string propertyName, object? value)
    {
        if (_postings is not null)
        {
            var offsets = PostingsForKey(propertyName, value, out bool known);
            return !known || offsets is not null;    // property not indexed → no information
        }

        if (!_index.TryGetValue(propertyName, out var buildValues))
            return true;
        return buildValues.ContainsKey(SerialiseValue(value));
    }

    // ── Key identity: one filter path, two possible bucket names ───────────────

    /// <summary>
    /// Resolves a hint key against every bucket that could hold it and unions their postings.
    /// <paramref name="known"/> is false when NO candidate bucket exists — "no information,
    /// scan" — and the return is null when candidates exist but none holds the value, which
    /// is the only state that proves no event matches.
    ///
    /// <para>An encoded path such as <c>http</c>U+0001<c>method</c> can name two different
    /// things the builder writes differently: a nested <c>http</c> map (flattened to exactly
    /// that key) or ONE flat attribute literally named <c>http.method</c>, which is how every
    /// OTLP semantic-convention attribute arrives. <c>FilterEvaluator.GetValue</c> now accepts
    /// both, so the index must too — pruning on one spelling while the scan matches the other
    /// is the same silent false negative in a new place. The union is complete: a bucket that
    /// does not exist describes no event.</para>
    /// </summary>
    private int[]? PostingsForKey(string property, object? value, out bool known)
    {
        known = false;
        int[]? acc = null;

        if (_postings!.TryGetValue(property, out var values))
        {
            known = true;
            acc   = PostingsFor(values, value);
        }

        // The flat spelling, when the encoded path could also be one dotted key name.
        int sep = property.IndexOf(ClefFields.PropertyPathSeparator);
        if (sep >= 0 && property.IndexOf(PathIndexMarker) < 0)
        {
            string flat = property.Replace(ClefFields.PropertyPathSeparator, '.');
            if (_postings.TryGetValue(flat, out var flatValues))
            {
                known = true;
                if (PostingsFor(flatValues, value) is { } flatOffsets)
                    acc = acc is null ? flatOffsets : UnionAscending(acc, flatOffsets);
            }
        }

        return acc;
    }

    /// <summary>U+0002, the subscript marker <c>PropertyPath</c> writes for <c>Foo[0]</c>. A
    /// path carrying one is not a flat key name, so it gets no dotted alternate.</summary>
    private const char PathIndexMarker = (char)2;

    // ── Value identity: the index is typed, the scan is coercing ───────────────

    /// <summary>
    /// Unions the postings of every encoding of <paramref name="value"/> the bucket actually
    /// holds (see <see cref="IndexValueForms.Serialised"/>). Returns null only when it holds
    /// NONE of them — the single state a caller may read as proof that no event matches.
    ///
    /// <para>The union is complete, not a guess: an encoding with no bucket entry describes no
    /// event, so adding it changes nothing, and every encoding the scan would accept is
    /// probed.</para>
    /// </summary>
    private static int[]? PostingsFor(Dictionary<string, int[]> values, object? value)
    {
        IndexValueForms.Buffer buf = default;
        Span<string?> forms = buf;
        int n = IndexValueForms.Serialised(value, forms);

        int[]? acc = null;
        for (int i = 0; i < n; i++)
        {
            if (!values.TryGetValue(forms[i]!, out var offsets)) continue;
            acc = acc is null ? offsets : UnionAscending(acc, offsets);
        }
        return acc;
    }

    /// <summary>Unions two ascending, distinct int arrays into a new ascending array.</summary>
    private static int[] UnionAscending(int[] a, int[] b)
    {
        var outp = new int[a.Length + b.Length];
        int i = 0, j = 0, k = 0;
        while (i < a.Length && j < b.Length)
        {
            int x = a[i], y = b[j];
            if      (x < y) outp[k++] = a[i++];
            else if (x > y) outp[k++] = b[j++];
            else { outp[k++] = x; i++; j++; }
        }
        while (i < a.Length) outp[k++] = a[i++];
        while (j < b.Length) outp[k++] = b[j++];
        return k == outp.Length ? outp : outp[..k];
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
            // Written through an ArrayBufferWriter sized up front rather than
            // BinaryWriter-over-MemoryStream: the stream doubles its backing array as it
            // grows and ms.ToArray() then copies the finished blob a second time, so a
            // multi-MB index cost two Large Object Heap allocations plus every intermediate
            // doubling — and the Workstation GC this server runs never compacts the LOH,
            // so that garbage ratchets the resident set up flush after flush.
            var w = new System.Buffers.ArrayBufferWriter<byte>(EstimateSerialisedSize());
            WriteUInt32(w, CodecMagic);        // distinguishes the codec format from a legacy propertyCount
            WriteUInt32(w, (uint)_index.Count);

            foreach (var (propName, values) in _index)
            {
                WriteUtf8(w, propName);
                WriteUInt32(w, (uint)values.Count);

                foreach (var (valStr, list) in values)
                {
                    WriteUtf8(w, valStr);

                    // Encode the ascending offset list with the zero-alloc codec instead of
                    // RoaringBitmap.Create+Serialize — the flush allocation hot spot.
                    var offsets = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(list);
                    var dest    = w.GetSpan(4 + SegmentBitmapCodec.MaxEncodedSize(offsets.Length));
                    int n       = SegmentBitmapCodec.Encode(offsets, dest[4..]);
                    BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)n);
                    w.Advance(4 + n);
                }
            }

            return w.WrittenSpan.ToArray();
        }
    }

    /// <summary>Upper-bound estimate so the writer allocates its buffer once: header, then
    /// per bucket a length-prefixed key and one varint byte per posting (the dense case).</summary>
    private int EstimateSerialisedSize()
    {
        long size = 8;
        foreach (var (propName, values) in _index)
        {
            size += 2 + propName.Length * 3 + 4;
            foreach (var (valStr, list) in values)
                size += 2 + valStr.Length * 3 + 4 + list.Count + 8;
        }
        return (int)Math.Min(int.MaxValue - 64, size + 64);
    }

    private static void WriteUInt32(System.Buffers.ArrayBufferWriter<byte> w, uint v)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(w.GetSpan(4), v);
        w.Advance(4);
    }

    /// <summary>Length-prefixed UTF-8, encoded straight into the writer's buffer.</summary>
    private static void WriteUtf8(System.Buffers.ArrayBufferWriter<byte> w, string s)
    {
        var dest = w.GetSpan(2 + System.Text.Encoding.UTF8.GetMaxByteCount(s.Length));
        int n    = System.Text.Encoding.UTF8.GetBytes(s, dest[2..]);
        if (n > ushort.MaxValue)
        {
            // The prefix is 16-bit. Writing a truncated COUNT would desynchronise every
            // subsequent read and corrupt the whole blob; trimming the VALUE on a UTF-8
            // boundary only costs one over-long index term. Reachable only if the ingest
            // payload cap (Ingestion.MaxEventPayloadBytes) is raised above 64 KB.
            n = ushort.MaxValue;
            var body = dest[2..];
            while (n > 0 && (body[n] & 0xC0) == 0x80) n--;   // back off continuation bytes
        }
        BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)n);
        w.Advance(2 + n);
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

            // Case-insensitive on the QUERY side only: values are stored exactly as written,
            // but FilterEvaluator compares strings OrdinalIgnoreCase, so two buckets that
            // differ only in case are one bucket to a query. Keeping them apart let the index
            // answer "absent" for a value the scan would have matched — `@l = 'error'` against
            // a stored "Error" — and drop the segment. Property NAMES stay ordinal; the
            // evaluator resolves those case-sensitively.
            var values = new Dictionary<string, int[]>((int)valCount, StringComparer.OrdinalIgnoreCase);

            for (uint v = 0; v < valCount; v++)
            {
                ushort valLen  = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]); pos += 2;
                string valStr  = System.Text.Encoding.UTF8.GetString(data.Slice(pos, valLen)); pos += valLen;

                uint bmLen  = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]); pos += 4;
                var bmBytes = data.Slice(pos, (int)bmLen); pos += (int)bmLen;

                var decoded = codec ? DecodeCodec(bmBytes) : DecodeRoaring(bmBytes);

                // Merge, never overwrite: a case collision would otherwise lose one bucket's
                // postings outright, trading one silent false negative for another.
                values[valStr] = values.TryGetValue(valStr, out var prior)
                    ? UnionAscending(prior, decoded)
                    : decoded;
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

    /// <summary>The one encoding a stored value is filed under — see
    /// <see cref="IndexValueForms"/>, which also enumerates what a query may match it with.</summary>
    private static string SerialiseValue(object? value) => IndexValueForms.Serialise(value);
}
