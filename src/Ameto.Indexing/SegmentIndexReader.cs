using System.Buffers;
using Ameto.Core;

namespace Ameto.Indexing;

/// <summary>
/// Where a <see cref="SegmentIndexReader"/> gets its group's packed sections when its memo cannot
/// answer. A memo filled through one source answers through another only if both hand out the
/// same bytes: a segment file is immutable, but its path can be reused for a different file, so
/// the cache pairs a memo with the <see cref="IndexGroupFingerprint"/> of the bytes it learned
/// from and never lends it to a source with a different one.
/// </summary>
internal interface IIndexSectionSource
{
    /// <summary>The group's packed inverted section; empty when the group has none.</summary>
    ReadOnlySpan<byte> InvertedSection();

    /// <summary>The group's packed trigram section; empty when the group has none.</summary>
    ReadOnlySpan<byte> TrigramSection();

    /// <summary>The group's bloom filter, deserialised. The source owns it.</summary>
    SegmentBloomFilter BloomFilter();
}

/// <summary>
/// The query side of one index group, answered LAZILY: from what it has already worked out, and
/// otherwise from the group's packed sections, reading only what the question needs.
///
/// <para><b>Why.</b> This used to deserialise the group's inverted and trigram sections whole — a
/// string per value and an <c>int[]</c> per posting list, every property and every trigram — to
/// answer a filter that names one property or spells five trigrams. A prop-dense group decodes to
/// about three times its sections, and <see cref="SegmentIndexCache"/> charged that, so on the #80
/// stand one Information group was a 128 MB entry against a 256 MB budget: the cache held two
/// groups of twenty-four, missed 98 % of the time, and each miss decoded everything again.</para>
///
/// <para><b>How.</b> The inverted section gets a catalog — where each property's value entries lie,
/// found by one walk over the length prefixes — and a value is found by scanning that property's
/// entries alone. A trigram is found by one pass over the bucket headers. Only matching posting
/// lists are decoded. Every answer is kept in a MEMO: a bucket's postings, a trigram's postings or
/// its absence, the bloom's verdict on a probed text. A repeated filter is answered from the memo
/// without reading a byte of the file, and the memo is what the cache holds: kilobytes per group
/// where the decoded index was tens of megabytes.</para>
///
/// <para><b>Same answers.</b> Every question is answered by the rules of
/// <see cref="SegmentInvertedIndex.Deserialise"/> and <see cref="SegmentTrigramIndex.Deserialise"/>,
/// which stay as the reference: names and values decoded with replacement, values compared
/// OrdinalIgnoreCase and case-collisions unioned, duplicate property names merged, a trigram
/// section's last duplicate key winning, legacy layouts read. <c>LazyIndexParityTests</c> compares
/// the two over every shape those rules exist for.</para>
///
/// <para><b>Two kinds.</b> <see cref="Load"/> copies a group's three sections and answers through
/// them — the self-contained reader tests and tools use, owning the bloom's native bits exactly as
/// before. A MEMO (<see cref="CreateMemo"/>) owns no section: it is what the query cache keeps, and it
/// is only ever asked through a <see cref="SegmentIndexView"/>, which lends it the sections of the
/// segment the query already has open. Asking a memo directly throws — it has nothing to answer
/// with.</para>
///
/// <para><b>Threads.</b> A cached memo is shared by every query that touches its group. The memo's
/// tables are read and grown under one lock, never held across a section read or a decode; two
/// queries that miss the same bucket at once both decode it and the first to finish is kept.
/// Arrays in the memo are never written once published, and every answer handed out is either a
/// fresh array or one of them read-only.</para>
///
/// <para><b>Budget.</b> <see cref="ApproxRetainedBytes"/> GROWS as the memo does; the cache
/// re-reads it when a lease is released and charges the difference.</para>
/// </summary>
public sealed class SegmentIndexReader : ISegmentIndex, IIndexSectionSource, IDisposable
{
    private int _disposed;

    // The sections a loaded reader owns. All three null for a memo, which borrows them per use.
    private readonly byte[]?             _inverted;
    private readonly byte[]?             _trigram;
    private readonly SegmentBloomFilter? _bloom;

    // The memo: grown under _gate; published arrays never written. Only the ring-bounded answers
    // below are ever forgotten.
    private readonly Lock                        _gate = new();
    private SegmentInvertedIndex.PackedCatalog?  _catalog;
    private SegmentTrigramIndex.PackedHeader?    _trigramHeader;
    private Dictionary<long, Kept<int[]?>>?      _trigrams;       // null value: the section has no such trigram
    private Dictionary<BucketKey, Kept<int[]?>>? _buckets;        // null value: the property has no such value
    private Dictionary<string, Kept<bool>>?      _bloomVerdicts;

    // The bound on answers that grow with the QUESTIONS rather than the data: bloom verdicts and
    // "absent" answers. A stream of one-off values (a trace id pasted into the search box, a GUID
    // per request) adds one of each per group per query, for ever, where postings grow only with
    // what a group actually holds. Unbounded, they would fill a 256 MB budget after ~2 000 unique
    // searches over 1 000 groups — ~350 at the stand's 46 MB — and then the cache evicts whole
    // memos, the repeated filters' postings with them. So they share one CLOCK ring per memo: an
    // answer a query re-asks is marked and survives a sweep, a one-off is recycled.
    private Slot[]                               _ring = [];
    private int                                  _ringUsed, _hand;
    private long                                 _memoBytes;

    /// <summary>
    /// A test seam: runs after a question has been answered from a section and before the answer
    /// is remembered — the window in which two queries that missed the same bucket race. Null in
    /// production, where it costs one field read per miss.
    /// </summary>
    internal Action? BeforeRemember;

    private SegmentIndexReader(byte[]? inverted, byte[]? trigram, SegmentBloomFilter? bloom)
    {
        _inverted         = inverted;
        _trigram          = trigram;
        _bloom            = bloom;
        ApproxNativeBytes = bloom?.RetainedBytes ?? 0;
    }

    /// <summary>
    /// A reader over copies of one group's sections. <paramref name="trigramBytes"/> may be
    /// empty when the caller has no substring predicate: a trigram lookup then answers "no
    /// information", as an index without trigrams always has.
    /// </summary>
    public static SegmentIndexReader Load(
        ReadOnlySpan<byte> invertedBytes,
        ReadOnlySpan<byte> trigramBytes,
        ReadOnlySpan<byte> bloomBytes)
    {
        var bloom = SegmentBloomFilter.Deserialise(bloomBytes);
        try
        {
            return new SegmentIndexReader(invertedBytes.ToArray(), trigramBytes.ToArray(), bloom);
        }
        catch
        {
            bloom.Dispose();
            throw;
        }
    }

    /// <summary>An empty memo for one (file, group): no section, nothing native, a few hundred
    /// bytes until a query teaches it something.</summary>
    internal static SegmentIndexReader CreateMemo() => new(null, null, null);

    /// <summary>True for a reader made by <see cref="Load"/>; false for a memo.</summary>
    internal bool OwnsSections => _bloom is not null;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0) _bloom?.Dispose();
    }

    // ── Size ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// The part of <see cref="ApproxRetainedBytes"/> that is NOT on the managed heap: a loaded
    /// reader's bloom bits, held in <c>NativeMemory</c> and freed only by <see cref="Dispose"/>.
    /// Zero for a memo, which keeps the bloom's VERDICTS and lets the bits go with the query that
    /// read them.
    ///
    /// <para>Reported apart from the total because the two are bounded by different limits and
    /// reclaimed by different means: no collection frees these bytes, and they do not count
    /// against the GC's hard limit.</para>
    /// </summary>
    public long ApproxNativeBytes { get; }

    /// <summary>The managed part: owned section copies, the catalog and the memo.</summary>
    public long ApproxManagedBytes => ApproxRetainedBytes - ApproxNativeBytes;

    /// <summary>
    /// Approximate bytes this reader keeps alive: the section copies a loaded reader owns, its
    /// bloom bits, and the memo — which grows with every question answered from a section, so
    /// this does too. The cache charges it at insert and re-reads it at every lease release.
    /// Overheads are the CLR's usual 64-bit costs; an estimate by design.
    /// </summary>
    public long ApproxRetainedBytes =>
        FixedBytes + (_inverted?.Length ?? 0) + (_trigram?.Length ?? 0) + ApproxNativeBytes
        + Interlocked.Read(ref _memoBytes);

    // MEASURED (Release, GC.GetTotalMemory over 20 000 instances): a bare memo — the reader and
    // its lock — retains 176 B, and each memo table, once made, a 176 B shell before its first
    // entry. The cache adds its own per-entry bookkeeping on top (SegmentIndexCache).
    private const long FixedBytes    = 176;
    private const long ShellBytes    = 176;   // a Dictionary and its first bucket and entry arrays
    private const long HeaderBytes   = 40;
    private const long EntryBytes    = 40;   // a dictionary entry, key and value inline
    private const long ArrayBytes    = 24;   // an int[]'s header
    private const long StringBytes   = 22;   // a string's header and terminator
    private const long SlotBytes     = 40;   // a ring slot: kind, mark, three references and a long

    // ── ISegmentIndex: a loaded reader answers through its own sections ──────

    /// <summary>
    /// The group's bloom filter. A loaded reader only: a memo keeps verdicts, not bits.
    /// </summary>
    public SegmentBloomFilter Bloom => _bloom ?? throw Unbound();

    /// <inheritdoc/>
    public uint[]? Lookup(string propertyName, object? value) => Lookup(propertyName, value, Own);

    /// <inheritdoc/>
    public bool MightContain(string propertyName, object? value) => MightContain(propertyName, value, Own);

    /// <inheritdoc/>
    public uint[]? LookupTrigram(ReadOnlySpan<char> text) => LookupTrigram(text, Own);

    /// <inheritdoc/>
    public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates) =>
        LookupIntersect(predicates, Own);

    private IIndexSectionSource Own => _bloom is not null ? this : throw Unbound();

    private static InvalidOperationException Unbound() =>
        new("This index is a query-cache memo and owns no section: ask it through a SegmentIndexView.");

    ReadOnlySpan<byte> IIndexSectionSource.InvertedSection() => _inverted;
    ReadOnlySpan<byte> IIndexSectionSource.TrigramSection()  => _trigram;
    SegmentBloomFilter IIndexSectionSource.BloomFilter()     => _bloom ?? throw Unbound();

    // ── Bloom ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Probes the bloom for every PLAIN text the builder could have stored for a value the
    /// scan would accept.
    ///
    /// <para>The bloom is keyless: it answers "did any value here look like this", and the
    /// only text it holds is what <c>SegmentIndexBuilder</c> added. One literal can match
    /// several of those — <c>Count = '5'</c> against a stored integer, and (before the index
    /// went invariant) <c>Ratio = '2.5'</c> against a <c>ru-KZ</c> host's <c>2,5</c>. Probing
    /// a single spelling made the cheap gate drop the segment before the inverted index,
    /// which by then knew better, was ever consulted.</para>
    /// </summary>
    public static bool MightContainValue(SegmentBloomFilter bloom, object? value)
    {
        IndexValueForms.Buffer buf = default;
        Span<string?> forms = buf;
        int n = IndexValueForms.Plain(value, forms);

        for (int i = 0; i < n; i++)
            if (bloom.MightContain(forms[i]!)) return true;
        return false;
    }

    /// <summary><see cref="MightContainValue(SegmentBloomFilter, object?)"/>, each text's verdict
    /// remembered: the bits are read (and deserialised) only for a text never probed here.</summary>
    internal bool MightContainValue(object? value, IIndexSectionSource src)
    {
        IndexValueForms.Buffer buf = default;
        Span<string?> forms = buf;
        int n = IndexValueForms.Plain(value, forms);

        for (int i = 0; i < n; i++)
            if (BloomSays(forms[i]!, src)) return true;
        return false;
    }

    private bool BloomSays(string text, IIndexSectionSource src)
    {
        lock (_gate)
        {
            if (_bloomVerdicts is not null && _bloomVerdicts.TryGetValue(text, out var known))
            {
                _ring[known.Slot].Referenced = true;
                return known.Value;
            }
        }

        bool verdict = src.BloomFilter().MightContain(text);
        BeforeRemember?.Invoke();
        lock (_gate)
        {
            if (_bloomVerdicts is null)
            {
                _bloomVerdicts = new Dictionary<string, Kept<bool>>(StringComparer.Ordinal);
                Interlocked.Add(ref _memoBytes, ShellBytes);
            }
            if (_bloomVerdicts.ContainsKey(text)) return verdict;   // a racing query remembered it
            int slot = TakeSlotLocked();
            _ring[slot] = new Slot { Kind = SlotKind.Verdict, Text = text };
            _bloomVerdicts.Add(text, new Kept<bool>(verdict, slot));
            Interlocked.Add(ref _memoBytes, VerdictBytes(text));
        }
        return verdict;
    }

    // ── Inverted ──────────────────────────────────────────────────────────────

    internal bool MightContain(string propertyName, object? value, IIndexSectionSource src)
    {
        // Bloom filter gives a cheap first gate; inverted index is the definitive check.
        if (!MightContainValue(value, src)) return false;
        var offsets = PostingsForKey(propertyName, value, src, out bool known);
        return !known || offsets is not null;    // property not indexed → no information
    }

    internal uint[]? Lookup(string propertyName, object? value, IIndexSectionSource src)
    {
        var offsets = PostingsForKey(propertyName, value, src, out _);
        return offsets is null ? null : SegmentInvertedIndex.ToUInt(offsets);
    }

    /// <summary>
    /// <see cref="SegmentInvertedIndex.LookupIntersect"/>'s three answers, from the memo: null
    /// is "cannot narrow, scan", a non-empty array is "these offsets and no others", and an EMPTY
    /// array is "nothing here matches". A property the section never wrote is the first case; a
    /// property present without the value is the third.
    /// </summary>
    internal uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates, IIndexSectionSource src)
    {
        if (predicates.Count == 0) return null;

        int[][]? lists = null;
        int used = 0;
        for (int i = 0; i < predicates.Count; i++)
        {
            int[]? offsets = PostingsForKey(predicates[i].property, predicates[i].value, src, out bool known);
            if (!known) continue;                           // unknown property → no information
            if (offsets is null)
                return Array.Empty<uint>();                 // known bucket, absent value → proven empty

            lists ??= new int[predicates.Count][];
            lists[used++] = offsets;
        }

        if (used == 0) return null;                          // nothing usable → scan
        return SegmentInvertedIndex.IntersectLists(lists!, used);
    }

    /// <summary>
    /// Every bucket a hint key could name — the key itself, and its dotted spelling when it has
    /// one (<see cref="SegmentInvertedIndex.TryFlatSpelling"/>) — unioned over every encoding of the
    /// value (<see cref="IndexValueForms.Serialised"/>). <paramref name="known"/> is false when no
    /// candidate property exists; the return is null when they exist and none holds the value.
    /// The same rules, in the same order, as the decoded index's <c>PostingsForKey</c>.
    /// </summary>
    private int[]? PostingsForKey(string property, object? value, IIndexSectionSource src, out bool known)
    {
        known = false;
        var catalog = Catalog(src);
        if (catalog.Properties.Count == 0) return null;

        IndexValueForms.Buffer buf = default;
        Span<string?> forms = buf;
        int n = -1;
        int[]? acc = null;

        if (catalog.Properties.TryGetValue(property, out var prop))
        {
            known = true;
            n   = IndexValueForms.Serialised(value, forms);
            acc = PostingsFor(prop, forms[..n], catalog.Codec, src);
        }

        Span<char> flat = stackalloc char[SegmentInvertedIndex.MaxFlatKeyChars];
        if (SegmentInvertedIndex.TryFlatSpelling(property, flat) &&
            catalog.BySpan.TryGetValue(flat[..property.Length], out var flatProp))
        {
            known = true;
            if (n < 0) n = IndexValueForms.Serialised(value, forms);
            if (PostingsFor(flatProp, forms[..n], catalog.Codec, src) is { } flatOffsets)
                acc = acc is null ? flatOffsets : SegmentInvertedIndex.UnionAscending(acc, flatOffsets);
        }

        return acc;
    }

    private int[]? PostingsFor(SegmentInvertedIndex.PackedProperty prop, ReadOnlySpan<string?> forms,
                               bool codec, IIndexSectionSource src)
    {
        int[]? acc = null;
        for (int i = 0; i < forms.Length; i++)
        {
            var offsets = Bucket(prop, forms[i]!, codec, src);
            if (offsets is null) continue;
            acc = acc is null ? offsets : SegmentInvertedIndex.UnionAscending(acc, offsets);
        }
        return acc;
    }

    /// <summary>One (property, encoded value) bucket: from the memo, or by scanning the
    /// property's entries in the section and remembering the answer — its absence included.</summary>
    private int[]? Bucket(SegmentInvertedIndex.PackedProperty prop, string form, bool codec, IIndexSectionSource src)
    {
        var key = new BucketKey(prop.Name, form);
        lock (_gate)
        {
            if (_buckets is not null && _buckets.TryGetValue(key, out var known))
            {
                if (known.Slot >= 0) _ring[known.Slot].Referenced = true;
                return known.Value;
            }
        }

        var found = SegmentInvertedIndex.ScanBucket(src.InvertedSection(), codec, prop.Runs, form);
        BeforeRemember?.Invoke();
        lock (_gate)
        {
            if (_buckets is null)
            {
                _buckets = [];
                Interlocked.Add(ref _memoBytes, ShellBytes);
            }
            if (_buckets.TryGetValue(key, out var raced)) return raced.Value;   // a racing query got there first
            int slot = -1;
            if (found is null)
            {
                slot = TakeSlotLocked();
                _ring[slot] = new Slot { Kind = SlotKind.AbsentBucket, Bucket = key };
            }
            _buckets.Add(key, new Kept<int[]?>(found, slot));
            Interlocked.Add(ref _memoBytes, BucketEntryBytes(key) + (found is null ? 0 : ArrayBytes + 4L * found.Length));
        }
        return found;
    }

    private SegmentInvertedIndex.PackedCatalog Catalog(IIndexSectionSource src)
    {
        var catalog = Volatile.Read(ref _catalog);
        if (catalog is not null) return catalog;

        catalog = SegmentInvertedIndex.ReadCatalog(src.InvertedSection());
        lock (_gate)
        {
            if (_catalog is not null) return _catalog;
            Volatile.Write(ref _catalog, catalog);
            Interlocked.Add(ref _memoBytes, catalog.RetainedBytes);
        }
        return catalog;
    }

    /// <summary>A bucket's name in the memo: the catalog's own property string, and an encoded
    /// value compared the way the scan compares it — OrdinalIgnoreCase, so spellings that differ
    /// only in case share one entry, as they share one bucket.</summary>
    private readonly struct BucketKey(string property, string form) : IEquatable<BucketKey>
    {
        private readonly string _property = property;
        private readonly string _form     = form;

        public int FormLength => _form.Length;

        public bool Equals(BucketKey other) =>
            string.Equals(_property, other._property, StringComparison.Ordinal) &&
            string.Equals(_form, other._form, StringComparison.OrdinalIgnoreCase);

        public override bool Equals(object? obj) => obj is BucketKey k && Equals(k);

        public override int GetHashCode() =>
            HashCode.Combine(_property.GetHashCode(), _form.GetHashCode(StringComparison.OrdinalIgnoreCase));
    }

    // ── Trigram ───────────────────────────────────────────────────────────────

    /// <summary>
    /// <see cref="SegmentTrigramIndex.Lookup"/>'s answer, from the memo: null when the group holds
    /// no trigram at all or the term is shorter than one, empty when some trigram of the term is
    /// absent, otherwise the rarest-first intersection of its trigrams' postings. Trigrams the
    /// memo lacks are found in ONE pass over the section's bucket headers, and decoded only when
    /// every one of them is present — one absent trigram is the whole answer.
    /// </summary>
    internal uint[]? LookupTrigram(ReadOnlySpan<char> text, IIndexSectionSource src)
    {
        // Both checks mean "no information", so their order cannot change an answer; the cheap
        // one first keeps a short term from reading the section just to learn its bucket count.
        if (text.Length < 3) return null;
        var header = TrigramHeader(src);
        if (header.Count == 0) return null;

        char[]?    rentedChars = text.Length > 1024 ? ArrayPool<char>.Shared.Rent(text.Length) : null;
        Span<char> lower       = rentedChars ?? stackalloc char[text.Length];
        int[][]?   posts       = null;
        try
        {
            int n = text.ToLowerInvariant(lower);
            if (n < 0) { text.CopyTo(lower); n = text.Length; }   // never (dest sized to source)
            if (n < 3) return null;

            int k       = n - 2;                                  // trigram count
            int unknown = 0;
            posts = ArrayPool<int[]>.Shared.Rent(k);
            lock (_gate)
            {
                for (int i = 0; i < k; i++)
                {
                    long key = SegmentTrigramIndex.PackedKey(lower[i], lower[i + 1], lower[i + 2]);
                    if (_trigrams is not null && _trigrams.TryGetValue(key, out var known))
                    {
                        if (known.Value is null)                   // absent: no candidates
                        {
                            _ring[known.Slot].Referenced = true;
                            return [];
                        }
                        posts[i] = known.Value;
                    }
                    else
                    {
                        posts[i] = null!;
                        unknown++;
                    }
                }
            }

            if (unknown > 0 && !Resolve(lower[..n], k, posts, header, src)) return [];
            return SegmentTrigramIndex.IntersectRarestFirst(posts, k);
        }
        finally
        {
            // Cleared: the rented array holds references to the memo's arrays, and a pooled
            // array outlives the call.
            if (posts is not null) ArrayPool<int[]>.Shared.Return(posts, clearArray: true);
            if (rentedChars is not null) ArrayPool<char>.Shared.Return(rentedChars);
        }
    }

    /// <summary>
    /// Fills the null slots of <paramref name="posts"/> from the section and remembers what it
    /// found. Returns false — having remembered every absent trigram and decoded nothing — when
    /// any is absent.
    /// </summary>
    private bool Resolve(ReadOnlySpan<char> lower, int k, int[][] posts,
                         SegmentTrigramIndex.PackedHeader header, IIndexSectionSource src)
    {
        long[] keys    = ArrayPool<long>.Shared.Rent(k);
        int[]  offsets = ArrayPool<int>.Shared.Rent(k);
        int[]  lengths = ArrayPool<int>.Shared.Rent(k);
        int[][]? decoded = null;
        try
        {
            int m = 0;
            for (int i = 0; i < k; i++)
                if (posts[i] is null) keys[m++] = SegmentTrigramIndex.PackedKey(lower[i], lower[i + 1], lower[i + 2]);

            var wanted = keys.AsSpan(0, m);
            wanted.Sort();
            m      = Distinct(wanted);
            wanted = wanted[..m];

            var section = src.TrigramSection();
            SegmentTrigramIndex.Locate(section, header, wanted, offsets, lengths);

            // One absent trigram is the whole answer, and ONE is all that is remembered: the first
            // in the term's own order, which the next asking of the same term reaches before any
            // other. Remembering every absent one let a single long miss — a pasted id spelling
            // dozens of trigrams the group lacks — take that many slots of the bounded ring, and
            // push out what a dashboard keeps re-asking.
            for (int i = 0; i < k; i++)
            {
                if (posts[i] is not null) continue;
                long key = SegmentTrigramIndex.PackedKey(lower[i], lower[i + 1], lower[i + 2]);
                if (offsets[SegmentTrigramIndex.IndexOf(wanted, key)] >= 0) continue;
                lock (_gate) RememberTrigramLocked(key, null);
                return false;
            }

            decoded = ArrayPool<int[]>.Shared.Rent(m);
            for (int j = 0; j < m; j++)
                decoded[j] = SegmentTrigramIndex.DecodePacked(section.Slice(offsets[j], lengths[j]), header.Kind);
            BeforeRemember?.Invoke();

            lock (_gate)
            {
                for (int j = 0; j < m; j++)
                    decoded[j] = RememberTrigramLocked(wanted[j], decoded[j])!;
            }

            for (int i = 0, j; i < k; i++)
            {
                if (posts[i] is not null) continue;
                j = SegmentTrigramIndex.IndexOf(wanted, SegmentTrigramIndex.PackedKey(lower[i], lower[i + 1], lower[i + 2]));
                posts[i] = decoded[j];
            }
            return true;
        }
        finally
        {
            if (decoded is not null) ArrayPool<int[]>.Shared.Return(decoded, clearArray: true);
            ArrayPool<long>.Shared.Return(keys);
            ArrayPool<int>.Shared.Return(offsets);
            ArrayPool<int>.Shared.Return(lengths);
        }
    }

    /// <summary>Remembers a trigram's postings (null: absent) unless a racing query already has,
    /// and returns what the memo holds.</summary>
    private int[]? RememberTrigramLocked(long key, int[]? postings)
    {
        if (_trigrams is null)
        {
            _trigrams = [];
            Interlocked.Add(ref _memoBytes, ShellBytes);
        }
        if (_trigrams.TryGetValue(key, out var raced)) return raced.Value;
        int slot = -1;
        if (postings is null)
        {
            slot = TakeSlotLocked();
            _ring[slot] = new Slot { Kind = SlotKind.AbsentTrigram, Trigram = key };
        }
        _trigrams.Add(key, new Kept<int[]?>(postings, slot));
        Interlocked.Add(ref _memoBytes, EntryBytes + (postings is null ? 0 : ArrayBytes + 4L * postings.Length));
        return postings;
    }

    // ── The ring bounding verdicts and absences ──────────────────────────────

    /// <summary>
    /// How many bloom verdicts and "absent" answers one memo keeps. A dashboard's filters re-ask
    /// theirs every refresh and keep them; one-off values cycle through the rest. At the cap a
    /// memo's bounded answers cost ~256 × 130 B plus the ring, ~45 KB.
    ///
    /// <para><b>The cliff.</b> The bound is on answers, not on filters, and one literal can leave
    /// several: a numeric one is probed in every encoding it might be stored under, ~7 verdicts and
    /// absences when the group holds none of them. So around 35 such literals re-asked against the
    /// same group — a wide dashboard — fill the ring with answers that are ALL re-asked, the clock
    /// can spare none, and they start evicting each other: those groups then read their sections
    /// again on every refresh — the miss path, a section rent and a scan, for that part of the
    /// dashboard. A cap by bytes, or sized per group from what is re-asked, is the follow-up if that
    /// shape turns up.</para>
    /// </summary>
    internal const int MaxBoundedAnswers = 256;

    /// <summary>Answers of the bounded kinds currently kept — for tests.</summary>
    internal int BoundedAnswers { get { lock (_gate) return _ringUsed; } }

    private enum SlotKind : byte { Free, Verdict, AbsentBucket, AbsentTrigram }

    /// <summary>One bounded answer: which table holds it, under what key, and whether a query has
    /// re-asked it since the clock hand last passed.</summary>
    private struct Slot
    {
        public SlotKind  Kind;
        public bool      Referenced;
        public string?   Text;
        public BucketKey Bucket;
        public long      Trigram;
    }

    /// <summary>A remembered answer and its ring slot; -1 for the kinds the ring does not bound.</summary>
    private readonly struct Kept<T>(T value, int slot)
    {
        public readonly T   Value = value;
        public readonly int Slot  = slot;
    }

    /// <summary>
    /// A ring slot for a new bounded answer: a fresh one while the ring is short of
    /// <see cref="MaxBoundedAnswers"/>, then the first the clock hand finds unreferenced — its
    /// answer forgotten, its bytes given back. A referenced slot is spared once and unmarked, so
    /// an answer survives exactly as long as queries keep coming back for it.
    /// </summary>
    private int TakeSlotLocked()
    {
        if (_ringUsed < MaxBoundedAnswers)
        {
            if (_ringUsed == _ring.Length)
            {
                int grown = Math.Min(MaxBoundedAnswers, Math.Max(8, _ring.Length * 2));
                Interlocked.Add(ref _memoBytes, (long)(grown - _ring.Length) * SlotBytes);
                Array.Resize(ref _ring, grown);
            }
            return _ringUsed++;
        }

        while (_ring[_hand].Referenced)
        {
            _ring[_hand].Referenced = false;
            _hand = (_hand + 1) % MaxBoundedAnswers;
        }
        int slot = _hand;
        _hand = (_hand + 1) % MaxBoundedAnswers;
        ForgetLocked(ref _ring[slot]);
        return slot;
    }

    private void ForgetLocked(ref Slot s)
    {
        long freed = 0;
        switch (s.Kind)
        {
            case SlotKind.Verdict:
                _bloomVerdicts!.Remove(s.Text!);
                freed = VerdictBytes(s.Text!);
                break;
            case SlotKind.AbsentBucket:
                _buckets!.Remove(s.Bucket);
                freed = BucketEntryBytes(s.Bucket);
                break;
            case SlotKind.AbsentTrigram:
                _trigrams!.Remove(s.Trigram);
                freed = EntryBytes;
                break;
        }
        Interlocked.Add(ref _memoBytes, -freed);
        s = default;
    }

    private static long VerdictBytes(string text)        => EntryBytes + StringBytes + 2L * text.Length;
    private static long BucketEntryBytes(BucketKey key) => EntryBytes + StringBytes + 2L * key.FormLength;

    private SegmentTrigramIndex.PackedHeader TrigramHeader(IIndexSectionSource src)
    {
        var header = Volatile.Read(ref _trigramHeader);
        if (header is not null) return header;

        header = SegmentTrigramIndex.ReadPackedHeader(src.TrigramSection());
        if (Interlocked.CompareExchange(ref _trigramHeader, header, null) is { } raced) return raced;
        Interlocked.Add(ref _memoBytes, HeaderBytes);
        return header;
    }

    /// <summary>Compacts an ascending span to its distinct values; returns how many.</summary>
    private static int Distinct(Span<long> sorted)
    {
        if (sorted.Length == 0) return 0;
        int w = 1;
        for (int r = 1; r < sorted.Length; r++)
            if (sorted[r] != sorted[w - 1]) sorted[w++] = sorted[r];
        return w;
    }
}
