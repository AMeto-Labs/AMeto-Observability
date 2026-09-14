using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Collections.Special;
using Ameto.Core;

namespace Ameto.Indexing;

/// <summary>
/// Inverted index for a single segment: maps (propertyName, value) → sorted int list of local event offsets.
///
/// Building phase: terms live as UTF-8 in pooled slabs; an open-addressing table keyed by
/// (property, hash, bytes) finds the entry; postings sit in a <see cref="PostingArena"/> already
/// in wire form, singletons inline. Nothing per distinct value is a heap object: the old build
/// kept a <c>string</c>, a <c>List&lt;int&gt;</c>, its <c>int[]</c> and a dictionary entry per
/// value — ~150 B and four objects for a 32-byte trace id, ×4 high-cardinality properties ×
/// 200 k events per group, with the dictionaries doubling their way onto the LOH — and
/// transcoded every key and value from UTF-8 to UTF-16 to look it up and back again to write it.
/// Serialisation: delta+varint via <see cref="SegmentBitmapCodec"/>, terms copied from the slabs.
/// Deserialisation: iterates postings back into sorted arrays for fast lookup.
///
/// <para>SECTION BYTES ARE PINNED: properties are written in first-seen order and each
/// property's values in first-seen order (a per-property chain through the entry array), which
/// is the dictionary insertion order the previous build wrote. <c>IndexBuildParityTests</c> and
/// <c>SegmentInvertedBuildParityTests</c> compare against an independent reference.</para>
///
/// Thread safety: the build side has NO lock — one index belongs to one index group's builder
/// and <c>SegmentWriter.WriteEvents</c> drives that from a single thread (the build-mode
/// <see cref="Lookup"/> is test-only). The lock that used to guard every add was ~3-5 M
/// uncontended acquisitions per flush for nothing. The cold index is read-only after load.
/// </summary>
public sealed class SegmentInvertedIndex : ISegmentIndex
{
    // ── Build phase ───────────────────────────────────────────────────────────

    private struct PropEntry
    {
        public int  NameOff, NameLen;     // into the term slabs
        public uint Hash;
        public int  First, Last, Count;   // chain of TermEntry ids in first-seen order (-1 = none)
    }

    private struct TermEntry
    {
        public int  Off, Len;             // into the term slabs
        public int  Prop;
        public int  Next;                 // next value of the same property, -1 = last
        public uint Hash;
        public PostingArena.Bucket Postings;
    }

    private const int SlabShift = 20;
    private const int SlabBytes = 1 << SlabShift;
    private const int SlabMask  = SlabBytes - 1;

    // Term slabs: rented 1 MB arrays, terms appended, an offset is slab << 20 | position.
    private byte[][] _slabs = Array.Empty<byte[]>();
    private int      _slabCount;
    private int      _slabUsed = SlabBytes;   // forces a first slab on the first append

    private PropEntry[] _props     = Array.Empty<PropEntry>();
    private int         _propCount;
    private int[]       _propTable = Array.Empty<int>();   // entry id + 1, 0 = empty

    private TermEntry[] _terms     = Array.Empty<TermEntry>();
    private int         _termCount;
    private int[]       _termTable = Array.Empty<int>();   // entry id + 1, 0 = empty

    private readonly int _expectedTerms;
    private PostingArena _arena = new();
    private bool         _unsorted;
    private bool         _released;

    // Query-phase (populated after Deserialise): ascending offset arrays per (name,value).
    // Decoded from the segment's posting lists — SegmentBitmapCodec for current segments,
    // RoaringBitmap for legacy ones (see Deserialise / the codec magic marker).
    private Dictionary<string, Dictionary<string, int[]>>? _postings;

    /// <summary>Marks the codec posting-list format; a legacy blob starts with propertyCount (never this).</summary>
    private const uint CodecMagic = 0xFFFFFFFFu;

    public SegmentInvertedIndex() { }

    /// <param name="expectedTerms">Distinct (property, value) pairs the build is expected to
    /// hold — the previous group's count, from <see cref="IndexBuildHints"/>. Sizes the term table
    /// and entry array once instead of doubling them up to that size; a wrong hint costs one
    /// rehash per doubling, never correctness. Nothing is rented until the first add, so a
    /// deserialised (query-side) instance pays for none of this.</param>
    public SegmentInvertedIndex(int expectedTerms) => _expectedTerms = expectedTerms;

    /// <summary>Distinct properties seen by this build.</summary>
    public int PropertyCount => _propCount;

    /// <summary>Distinct (property, value) pairs seen by this build.</summary>
    public int TermCount => _termCount;

    /// <summary>Managed bytes the pooled build state holds — what <see cref="ReleaseBuildBuffers"/> gives back.</summary>
    public long BuildRetainedBytes
        => (long)_slabCount * SlabBytes
         + (long)_props.Length * Unsafe.SizeOf<PropEntry>() + (long)_propTable.Length * sizeof(int)
         + (long)_terms.Length * Unsafe.SizeOf<TermEntry>() + (long)_termTable.Length * sizeof(int)
         + _arena.RetainedBytes;

    // ── Build (hot path) ──────────────────────────────────────────────────────

    /// <summary>
    /// Files one posting and says whether the bucket it went into is new. The builder feeds the
    /// bloom filter — a SET — only on <see cref="IndexAddOutcome.NewValue"/> /
    /// <see cref="IndexAddOutcome.NewProperty"/>: re-adding a term the filter already holds sets
    /// bits that are already set, at the price of a fold, a UTF-8 encode and three hash passes.
    /// About half of a prop-dense event's ~21 bloom adds were such repeats.
    /// </summary>
    public IndexAddOutcome Add(uint localOffset, string propertyName, object? value)
        => AddSpan(localOffset, propertyName, SerialiseValue(value));

    /// <summary>
    /// UTF-16 overload, transcoding to the UTF-8 path. For tests, probes and the reference
    /// build; the streaming builder hands in raw UTF-8 and never comes through here.
    /// </summary>
    public IndexAddOutcome AddSpan(uint localOffset, ReadOnlySpan<char> propertyName, ReadOnlySpan<char> serialisedValue)
    {
        int maxName = System.Text.Encoding.UTF8.GetMaxByteCount(propertyName.Length);
        int maxVal  = System.Text.Encoding.UTF8.GetMaxByteCount(serialisedValue.Length);
        byte[]? rented = maxName + maxVal > 1024 ? ArrayPool<byte>.Shared.Rent(maxName + maxVal) : null;
        Span<byte> buf = rented ?? stackalloc byte[maxName + maxVal];
        try
        {
            int n = System.Text.Encoding.UTF8.GetBytes(propertyName, buf);
            int v = System.Text.Encoding.UTF8.GetBytes(serialisedValue, buf[n..]);
            return AddUtf8(localOffset, buf[..n], buf.Slice(n, v));
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>The production entry point: property name and already-serialised value as raw UTF-8.</summary>
    public IndexAddOutcome AddUtf8(uint localOffset, ReadOnlySpan<byte> propertyName, ReadOnlySpan<byte> serialisedValue)
    {
        int prop = PropertyId(propertyName, out bool created);
        var r    = AddValue(localOffset, prop, serialisedValue);
        return created ? IndexAddOutcome.NewProperty : r;
    }

    /// <summary>
    /// Resolves (creating on first sight) the id of a property, for a caller that files several
    /// values under one flattened key or wants to cache the id.
    /// </summary>
    public int PropertyId(ReadOnlySpan<byte> nameUtf8, out bool created)
    {
        ObjectDisposedException.ThrowIf(_released, this);
        if (_propTable.Length == 0) InitTables();

        uint h    = Utf8Hash.Compute(nameUtf8);
        int  mask = _propTable.Length - 1;
        int  i    = (int)h & mask;
        while (true)
        {
            int slot = _propTable[i];
            if (slot == 0) break;
            ref var p = ref _props[slot - 1];
            if (p.Hash == h && p.NameLen == nameUtf8.Length && Term(p.NameOff, p.NameLen).SequenceEqual(nameUtf8))
            {
                created = false;
                return slot - 1;
            }
            i = (i + 1) & mask;
        }

        if (_propCount == _props.Length) Grow(ref _props, _propCount);
        int id = _propCount++;
        ref var np = ref _props[id];
        np.NameOff = Append(nameUtf8);
        np.NameLen = nameUtf8.Length;
        np.Hash    = h;
        np.First   = -1; np.Last = -1; np.Count = 0;
        _propTable[i] = id + 1;
        if (_propCount * 2 > _propTable.Length) RehashProps();
        created = true;
        return id;
    }

    /// <summary>Files <paramref name="localOffset"/> under (<paramref name="prop"/>, value).
    /// Returns <see cref="IndexAddOutcome.NewValue"/> on the value's first sighting.</summary>
    public IndexAddOutcome AddValue(uint localOffset, int prop, ReadOnlySpan<byte> serialisedValue)
    {
        uint h    = Utf8Hash.Compute(serialisedValue) ^ ((uint)prop * 0x9E3779B1u);
        int  mask = _termTable.Length - 1;
        int  i    = (int)h & mask;
        int  id;
        while (true)
        {
            int slot = _termTable[i];
            if (slot == 0)
            {
                id = NewTerm(prop, serialisedValue, h);
                _termTable[i] = id + 1;
                if (_termCount * 2 > _termTable.Length) RehashTerms();
                _arena.Append(ref _terms[id].Postings, (int)localOffset, ref _unsorted);
                return IndexAddOutcome.NewValue;
            }
            ref var e = ref _terms[slot - 1];
            if (e.Hash == h && e.Prop == prop && e.Len == serialisedValue.Length
                && Term(e.Off, e.Len).SequenceEqual(serialisedValue))
            {
                // Offsets arrive in monotonically increasing order during a single flush; the
                // arena drops a repeat of the current offset and flags anything out of order.
                _arena.Append(ref e.Postings, (int)localOffset, ref _unsorted);
                return IndexAddOutcome.Existing;
            }
            i = (i + 1) & mask;
        }
    }

    public void AddEvent(uint localOffset, LogLevel level, Dictionary<string, object?>? properties)
    {
        Add(localOffset, "@l", level.ToSeqString());

        if (properties is null) return;
        foreach (var (k, v) in properties)
            Add(localOffset, k, v);
    }

    private int NewTerm(int prop, ReadOnlySpan<byte> value, uint hash)
    {
        // Append first: nothing about the entry is published until every part of it exists, so
        // a throw here (out of memory is the only one left) leaves no half-initialised entry
        // for a rehash to read.
        int off = Append(value);
        if (_termCount == _terms.Length) Grow(ref _terms, _termCount);
        int id = _termCount++;
        ref var e = ref _terms[id];
        e.Off  = off;
        e.Len  = value.Length;
        e.Prop = prop;
        e.Next = -1;
        e.Hash = hash;
        e.Postings = default;

        ref var p = ref _props[prop];
        if (p.Last < 0) p.First = id; else _terms[p.Last].Next = id;
        p.Last = id;
        p.Count++;
        return id;
    }

    private void InitTables()
    {
        int terms = Math.Max(_expectedTerms, 1024);
        int cap   = (int)Math.Min(1 << 30, (uint)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)terms * 2));
        _termTable = RentCleared(cap);
        _terms     = IndexBuildPool.Entries<TermEntry>().Rent(terms);
        _propTable = RentCleared(256);
        _props     = IndexBuildPool.Entries<PropEntry>().Rent(64);
    }

    private static int[] RentCleared(int n)
    {
        var t = IndexBuildPool.Ints.Rent(n);
        // A rented array can be longer than asked; the table uses its FULL length as capacity
        // (mask = Length - 1), so it must be a power of two — trim by re-renting exact when not.
        if (!System.Numerics.BitOperations.IsPow2(t.Length))
        {
            IndexBuildPool.Ints.Return(t);
            t = new int[n];
            return t;
        }
        Array.Clear(t);
        return t;
    }

    private static void Grow<T>(ref T[] arr, int used) where T : struct
    {
        var next = IndexBuildPool.Entries<T>().Rent(Math.Max(1024, arr.Length * 2));
        arr.AsSpan(0, used).CopyTo(next);
        if (arr.Length > 0) IndexBuildPool.Entries<T>().Return(arr);
        arr = next;
    }

    private void RehashTerms()
    {
        var old = _termTable;
        var t   = RentCleared(old.Length * 2);
        int mask = t.Length - 1;
        for (int id = 0; id < _termCount; id++)
        {
            int i = (int)_terms[id].Hash & mask;
            while (t[i] != 0) i = (i + 1) & mask;
            t[i] = id + 1;
        }
        IndexBuildPool.Ints.Return(old);
        _termTable = t;
    }

    private void RehashProps()
    {
        var old = _propTable;
        var t   = RentCleared(old.Length * 2);
        int mask = t.Length - 1;
        for (int id = 0; id < _propCount; id++)
        {
            int i = (int)_props[id].Hash & mask;
            while (t[i] != 0) i = (i + 1) & mask;
            t[i] = id + 1;
        }
        IndexBuildPool.Ints.Return(old);
        _propTable = t;
    }

    /// <summary>Copies <paramref name="bytes"/> into the slabs and returns its offset.</summary>
    private int Append(ReadOnlySpan<byte> bytes)
    {
        // The second test is for an EMPTY term against a full slab: the sum test alone lets it
        // through and names offset SlabBytes of the current slab, i.e. position 0 of a slab that
        // may not exist. An empty key or value is a legal term (the old build filed "").
        if (_slabUsed + bytes.Length > SlabBytes || _slabUsed == SlabBytes)
        {
            // A term longer than a slab (only reachable if the ingest payload cap is raised past
            // 1 MB) gets a slab of its own at position 0, which the offset scheme still names.
            if (_slabCount == _slabs.Length) Array.Resize(ref _slabs, Math.Max(4, _slabs.Length * 2));
            _slabs[_slabCount++] = IndexBuildPool.Slabs.Rent(Math.Max(SlabBytes, bytes.Length));
            _slabUsed = 0;
        }
        int slab = _slabCount - 1;
        bytes.CopyTo(_slabs[slab].AsSpan(_slabUsed));
        int off = (slab << SlabShift) | _slabUsed;
        _slabUsed += bytes.Length;
        return off;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> Term(int off, int len) => _slabs[off >> SlabShift].AsSpan(off & SlabMask, len);

    /// <summary>
    /// Hands the slabs, tables, entries and posting arena back to their pools. The index is
    /// unusable afterwards — adds and <see cref="Serialise"/> throw — because a returned pooled
    /// array is somebody else's the moment they rent it (the freed-memory read
    /// <c>SegmentBloomFilter.Serialise</c> guards against, on the managed side).
    /// </summary>
    public void ReleaseBuildBuffers()
    {
        if (_released) return;
        _released = true;
        for (int i = 0; i < _slabCount; i++) { IndexBuildPool.Slabs.Return(_slabs[i]); _slabs[i] = null!; }
        _slabCount = 0; _slabUsed = SlabBytes;
        if (_props.Length > 0)     { IndexBuildPool.Entries<PropEntry>().Return(_props);  _props = Array.Empty<PropEntry>(); }
        if (_terms.Length > 0)     { IndexBuildPool.Entries<TermEntry>().Return(_terms);  _terms = Array.Empty<TermEntry>(); }
        if (_propTable.Length > 0) { IndexBuildPool.Ints.Return(_propTable);    _propTable = Array.Empty<int>(); }
        if (_termTable.Length > 0) { IndexBuildPool.Ints.Return(_termTable);    _termTable = Array.Empty<int>(); }
        _propCount = 0; _termCount = 0;
        _arena.Release();
    }

    // ── Build-mode reads (test-only) ──────────────────────────────────────────

    private int FindPropertyBuild(string name)
    {
        if (_propTable.Length == 0) return -1;
        ReadOnlySpan<byte> key = System.Text.Encoding.UTF8.GetBytes(name);
        uint h = Utf8Hash.Compute(key);
        int mask = _propTable.Length - 1;
        for (int i = (int)h & mask; _propTable[i] != 0; i = (i + 1) & mask)
        {
            ref var p = ref _props[_propTable[i] - 1];
            if (p.Hash == h && p.NameLen == key.Length && Term(p.NameOff, p.NameLen).SequenceEqual(key))
                return _propTable[i] - 1;
        }
        return -1;
    }

    private int FindTermBuild(int prop, string serialised)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(serialised);
        uint h = Utf8Hash.Compute(bytes) ^ ((uint)prop * 0x9E3779B1u);
        int mask = _termTable.Length - 1;
        for (int i = (int)h & mask; _termTable[i] != 0; i = (i + 1) & mask)
        {
            ref var e = ref _terms[_termTable[i] - 1];
            if (e.Hash == h && e.Prop == prop && e.Len == bytes.Length && Term(e.Off, e.Len).SequenceEqual(bytes))
                return _termTable[i] - 1;
        }
        return -1;
    }

    private int[]? BuildPostings(string name, string serialised)
    {
        int prop = FindPropertyBuild(name);
        if (prop < 0) return null;
        int id = FindTermBuild(prop, serialised);
        if (id < 0) return null;
        ref readonly var b = ref _terms[id].Postings;
        var arr = new int[b.Count];
        _arena.Decode(in b, arr);
        return arr;
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
        int[]? build = BuildPostings(propertyName, SerialiseValue(value));
        if (build is not null) return ToUInt(build);

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

    /// <summary>
    /// Approximate managed bytes this DESERIALISED index retains: postings expanded to
    /// int[] plus dictionary-entry and string overheads. The cache budget must be
    /// denominated in what an entry keeps alive — the varint-packed section it was decoded
    /// from is roughly 3-8x smaller, and budgeting by that pinned several times the
    /// configured bytes. Overheads are the CLR's typical 64-bit costs, an estimate by
    /// design: ±20% on an estimate beats 5x on an exact count of the wrong thing.
    /// </summary>
    internal long ApproxRetainedBytes()
    {
        if (_postings is null) return 0;
        long bytes = 64;                                     // outer dictionary shell
        foreach (var (prop, values) in _postings)
        {
            bytes += 48 + 24 + 2L * prop.Length + 64;        // entry + key string + inner shell
            foreach (var (val, offsets) in values)
                bytes += 48 + 24 + 2L * val.Length            // entry + key string
                       + 24 + 4L * offsets.Length;            // int[] header + payload
        }
        return bytes;
    }

    public bool MightContain(string propertyName, object? value)
    {
        if (_postings is not null)
        {
            var offsets = PostingsForKey(propertyName, value, out bool known);
            return !known || offsets is not null;    // property not indexed → no information
        }

        int prop = FindPropertyBuild(propertyName);
        if (prop < 0)
            return true;
        return FindTermBuild(prop, SerialiseValue(value)) >= 0;
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
        // Written into stack scratch and probed through the span alternate lookup: a string
        // per predicate per segment would be pure garbage for a bucket that usually is absent.
        if (property.Length <= MaxFlatKeyChars &&
            property.IndexOf(ClefFields.PropertyPathSeparator) >= 0 &&
            property.IndexOf(PathIndexMarker) < 0)
        {
            Span<char> flat = stackalloc char[MaxFlatKeyChars];
            for (int i = 0; i < property.Length; i++)
                flat[i] = property[i] == ClefFields.PropertyPathSeparator ? '.' : property[i];

            if (_postings.GetAlternateLookup<ReadOnlySpan<char>>()
                         .TryGetValue(flat[..property.Length], out var flatValues))
            {
                known = true;
                if (PostingsFor(flatValues, value) is { } flatOffsets)
                    acc = acc is null ? flatOffsets : UnionAscending(acc, flatOffsets);
            }
        }

        return acc;
    }

    /// <summary>Longest path given a flat alternate — matches
    /// <c>FilterEvaluator.MaxFlatKeyChars</c>, which decides the same thing on the scan side.</summary>
    private const int MaxFlatKeyChars = 512;

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
        ObjectDisposedException.ThrowIf(_released, this);
        NormaliseIfUnsorted();
        // One allocation of the final size — the tables know every length — and no ToArray copy.
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

        WriteUInt32(w, CodecMagic);        // distinguishes the codec format from a legacy propertyCount
        WriteUInt32(w, (uint)_propCount);

        for (int p = 0; p < _propCount; p++)
        {
            ref readonly var prop = ref _props[p];
            WriteUtf8(w, Term(prop.NameOff, prop.NameLen));
            WriteUInt32(w, (uint)prop.Count);

            for (int id = prop.First; id >= 0; id = _terms[id].Next)
            {
                ref readonly var e = ref _terms[id];
                WriteUtf8(w, Term(e.Off, e.Len));

                // The posting list is already in codec form in the arena: copy it out behind
                // its length prefix.
                int n    = PostingArena.SerialisedSize(in e.Postings);
                var dest = w.GetSpan(4 + n);
                BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)n);
                _arena.Write(in e.Postings, dest.Slice(4, n));
                w.Advance(4 + n);
            }
        }
    }

    /// <summary>Bytes <see cref="WriteTo"/> will produce — exact, from the entry bookkeeping.</summary>
    public long ExactSerialisedSize()
    {
        long size = 8;
        for (int p = 0; p < _propCount; p++)
        {
            ref readonly var prop = ref _props[p];
            size += 2 + Utf8Prefixed(Term(prop.NameOff, prop.NameLen)) + 4;
            for (int id = prop.First; id >= 0; id = _terms[id].Next)
            {
                ref readonly var e = ref _terms[id];
                size += 2 + Utf8Prefixed(Term(e.Off, e.Len)) + 4 + PostingArena.SerialisedSize(in e.Postings);
            }
        }
        return size;
    }

    /// <summary>
    /// Restores the ascending+distinct invariant the codec requires — only an out-of-order
    /// caller (never the writer, whose ordinals are monotonic) pays for it, once, here.
    /// </summary>
    private void NormaliseIfUnsorted()
    {
        if (!_unsorted) return;
        var fresh   = new PostingArena();
        int[] tmp   = Array.Empty<int>();
        bool ignore = false;
        for (int id = 0; id < _termCount; id++)
        {
            ref var b = ref _terms[id].Postings;
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
        _arena.Release();
        _arena    = fresh;
        _unsorted = false;
    }

    private static void WriteUInt32(IBufferWriter<byte> w, uint v)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(w.GetSpan(4), v);
        w.Advance(4);
    }

    /// <summary>
    /// Length the 16-bit prefix can carry. Writing a truncated COUNT would desynchronise every
    /// subsequent read and corrupt the whole blob; trimming the VALUE on a UTF-8 boundary only
    /// costs one over-long index term. Reachable only if the ingest payload cap
    /// (Ingestion.MaxEventPayloadBytes) is raised above 64 KB.
    /// </summary>
    private static int Utf8Prefixed(ReadOnlySpan<byte> s)
    {
        int n = s.Length;
        if (n <= ushort.MaxValue) return n;
        n = ushort.MaxValue;
        while (n > 0 && (s[n] & 0xC0) == 0x80) n--;   // back off continuation bytes
        return n;
    }

    /// <summary>Length-prefixed UTF-8, copied straight from the slab.</summary>
    private static void WriteUtf8(IBufferWriter<byte> w, ReadOnlySpan<byte> s)
    {
        int n    = Utf8Prefixed(s);
        var dest = w.GetSpan(2 + n);
        BinaryPrimitives.WriteUInt16LittleEndian(dest, (ushort)n);
        s[..n].CopyTo(dest[2..]);
        w.Advance(2 + n);
    }

    /// <summary>Legacy RoaringBitmap serialisation — retained only to generate blobs for the
    /// backward-compatibility read test (current writers emit the codec format above).</summary>
    internal byte[] SerialiseRoaringV1()
    {
        ObjectDisposedException.ThrowIf(_released, this);
        NormaliseIfUnsorted();
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        bw.Write((uint)_propCount);
        for (int p = 0; p < _propCount; p++)
        {
            ref readonly var prop = ref _props[p];
            var nameBytes = Term(prop.NameOff, prop.NameLen);
            bw.Write((ushort)nameBytes.Length);
            bw.Write(nameBytes);
            bw.Write((uint)prop.Count);
            for (int id = prop.First; id >= 0; id = _terms[id].Next)
            {
                ref readonly var e = ref _terms[id];
                var valBytes = Term(e.Off, e.Len);
                bw.Write((ushort)valBytes.Length);
                bw.Write(valBytes);
                var offsets = new int[e.Postings.Count];
                _arena.Decode(in e.Postings, offsets);
                var bm = RoaringBitmap.Create(offsets);
                using var bitmapMs = new MemoryStream();
                RoaringBitmap.Serialize(bm, bitmapMs);
                var bitmapBytes = bitmapMs.ToArray();
                bw.Write((uint)bitmapBytes.Length);
                bw.Write(bitmapBytes);
            }
        }
        return ms.ToArray();
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
