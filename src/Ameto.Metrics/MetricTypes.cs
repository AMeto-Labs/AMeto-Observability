using System.Text;
using Ameto.Core;

namespace Ameto.Metrics;

/// <summary>
/// OTLP metric signal types.
/// </summary>
public enum MetricKind : byte
{
    /// <summary>Monotonically increasing counter (OTLP Sum isMonotonic=true).</summary>
    Counter   = 0,
    /// <summary>Instantaneous value that can go up or down (OTLP Gauge or non-monotonic Sum).</summary>
    Gauge     = 1,
    /// <summary>Distribution: count + sum + explicit buckets (OTLP Histogram).</summary>
    Histogram = 2,
}

/// <summary>
/// Immutable, comparable set of label key-value pairs.
/// Stored sorted by key, then by value, so two identical label sets have the same hash
/// regardless of the order their pairs arrived in.
///
/// <para><b>One interleaved <c>string[]</c> — k0, v0, k1, v1, … — not an array of pairs.</b>
/// The bytes are the same (two references a pair either way); what the flat shape buys is that
/// the pairs are plain spans of strings to the code that builds, sorts and compares them, with
/// no tuple copies and no comparison delegate.</para>
///
/// <para><b>Equality compares references first and values second, and must.</b> Label sets
/// built by the OTLP parsers hold strings canonicalised by <see cref="MetricLabelInterner"/>,
/// so two of them for the same series compare pointer by pointer — or are the very same
/// instance. Label sets rebuilt from disk (the metric WAL, <c>MetricReader</c>) or built by a
/// caller from its own strings hold uninterned copies, and they have to equal an interned set
/// with the same text: the hash is therefore computed from the VALUES (ordinal), never from
/// identities, and a reference miss falls through to an ordinal compare.</para>
/// </summary>
public sealed class LabelSet : IEquatable<LabelSet>
{
    public static readonly LabelSet Empty = new([]);

    /// <summary>Interleaved k0, v0, k1, v1, … sorted by key, then value (ordinal).</summary>
    private readonly string[] _kv;
    private readonly int _hash;

    public LabelSet(IEnumerable<KeyValuePair<string, string>> labels)
    {
        // Materialise once and sort in place. The obvious LINQ shape —
        // Select(...).OrderBy(...).ToArray() — allocates a projection iterator, an
        // EnumerableSorter, its index array and a comparison delegate for EVERY label set,
        // and a label set is built per ingested point AND again per series on every rollup
        // chunk pass. An allocation trace put that chain at ~10 MB/min on an idle server.
        string[] kv;
        if (labels is ICollection<KeyValuePair<string, string>> c)
        {
            kv = new string[c.Count * 2];
            int i = 0;
            foreach (var p in labels) { kv[i++] = p.Key; kv[i++] = p.Value; }
            // Count and enumeration can disagree if the source is mutated concurrently.
            // Yielding MORE throws above, which is loud and fine; yielding fewer would leave
            // trailing (null, null) pairs that sort and hash without complaining — a
            // silently wrong LabelSet. Trim instead.
            if (i != kv.Length) Array.Resize(ref kv, i);
        }
        else
        {
            var list = new List<string>();
            foreach (var p in labels) { list.Add(p.Key); list.Add(p.Value); }
            kv = list.ToArray();
        }

        // Ordered by key, then value. OrderBy was stable, so duplicate keys used to keep
        // their arrival order and two equal label sets built from differently-ordered input
        // hashed differently; comparing the value as well makes the layout canonical.
        SortInterleaved(kv, default);

        _kv   = kv;
        _hash = ComputeHash(kv);
    }

    /// <summary>Takes ownership of an already-sorted interleaved array and its value hash.</summary>
    private LabelSet(string[] sortedKv, int hash)
    {
        _kv   = sortedKv;
        _hash = hash;
    }

    /// <summary>
    /// A label set over a COPY of <paramref name="sortedInterleaved"/>, which must already be in
    /// canonical order (see <see cref="SortInterleaved"/>). The factory the interner uses on a
    /// miss; everything else goes through the constructor, which sorts.
    /// </summary>
    internal static LabelSet FromSorted(ReadOnlySpan<string> sortedInterleaved) =>
        sortedInterleaved.IsEmpty ? Empty : new LabelSet(sortedInterleaved.ToArray(), ComputeHash(sortedInterleaved));

    /// <summary>
    /// The value hash — the SAME function the pair-array layout used, over the same sequence,
    /// so nothing that relied on its distribution moves. Per process only (<see cref="HashCode"/>
    /// is randomly seeded); nothing persists it.
    /// </summary>
    private static int ComputeHash(ReadOnlySpan<string> kv)
    {
        var h = new HashCode();
        for (int i = 0; i < kv.Length; i++) h.Add(kv[i], StringComparer.Ordinal);
        return h.ToHashCode();
    }

    /// <summary>
    /// Sorts interleaved pairs by key, then value, ordinal — moving <paramref name="ids"/> (one
    /// per string, or empty) in step. Insertion sort: a label set is a handful of pairs that
    /// usually arrive in the exporter's own fixed order, so this is n - 1 compares on the
    /// common input and no delegate on any. Large sets (never seen from a real exporter, but
    /// legal) fall back to one <see cref="Array.Sort{T}(T[], Comparison{T})"/> over a pair array.
    /// </summary>
    internal static void SortInterleaved(Span<string> kv, Span<int> ids)
    {
        int n = kv.Length >> 1;
        if (n < 2) return;
        if (n > 64) { SortLarge(kv, ids); return; }

        for (int i = 1; i < n; i++)
        {
            string k = kv[2 * i], v = kv[2 * i + 1];
            int ik = ids.IsEmpty ? 0 : ids[2 * i], iv = ids.IsEmpty ? 0 : ids[2 * i + 1];
            int j = i - 1;
            while (j >= 0 && ComparePair(kv[2 * j], kv[2 * j + 1], k, v) > 0)
            {
                kv[2 * j + 2] = kv[2 * j];
                kv[2 * j + 3] = kv[2 * j + 1];
                if (!ids.IsEmpty) { ids[2 * j + 2] = ids[2 * j]; ids[2 * j + 3] = ids[2 * j + 1]; }
                j--;
            }
            kv[2 * j + 2] = k;
            kv[2 * j + 3] = v;
            if (!ids.IsEmpty) { ids[2 * j + 2] = ik; ids[2 * j + 3] = iv; }
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void SortLarge(Span<string> kv, Span<int> ids)
    {
        int n = kv.Length >> 1;
        var pairs = new (string Key, string Value, int KeyId, int ValueId)[n];
        for (int i = 0; i < n; i++)
            pairs[i] = (kv[2 * i], kv[2 * i + 1],
                        ids.IsEmpty ? 0 : ids[2 * i], ids.IsEmpty ? 0 : ids[2 * i + 1]);
        Array.Sort(pairs, static (a, b) => ComparePair(a.Key, a.Value, b.Key, b.Value));
        for (int i = 0; i < n; i++)
        {
            kv[2 * i] = pairs[i].Key;
            kv[2 * i + 1] = pairs[i].Value;
            if (!ids.IsEmpty) { ids[2 * i] = pairs[i].KeyId; ids[2 * i + 1] = pairs[i].ValueId; }
        }
    }

    private static int ComparePair(string ak, string av, string bk, string bv)
    {
        int k = string.CompareOrdinal(ak, bk);
        return k != 0 ? k : string.CompareOrdinal(av, bv);
    }

    /// <summary>Number of pairs.</summary>
    public int Count => _kv.Length >> 1;

    public string KeyAt(int index)   => _kv[2 * index];
    public string ValueAt(int index) => _kv[2 * index + 1];

    /// <summary>The pairs as one interleaved, canonically ordered span: k0, v0, k1, v1, ….</summary>
    public ReadOnlySpan<string> Interleaved => _kv;

    /// <summary>
    /// The pairs, for callers that want a list. A thin view over the interleaved array, built per
    /// call — 24 bytes; the ingest path uses <see cref="GetEnumerator"/> / <see cref="KeyAt"/>
    /// instead and allocates nothing.
    /// </summary>
    public IReadOnlyList<(string Key, string Value)> Pairs => _kv.Length == 0 ? [] : new PairList(_kv);

    /// <summary>Allocation-free <c>foreach (var (key, value) in labels)</c>.</summary>
    public PairEnumerator GetEnumerator() => new(_kv);

    public struct PairEnumerator
    {
        private readonly string[] _kv;
        private int _i;

        internal PairEnumerator(string[] kv) { _kv = kv; _i = -2; }

        public bool MoveNext() => (_i += 2) < _kv.Length;
        public readonly (string Key, string Value) Current => (_kv[_i], _kv[_i + 1]);
    }

    private sealed class PairList(string[] kv) : IReadOnlyList<(string Key, string Value)>
    {
        public int Count => kv.Length >> 1;
        public (string Key, string Value) this[int index] =>
            (uint)index < (uint)Count ? (kv[2 * index], kv[2 * index + 1])
                                      : throw new ArgumentOutOfRangeException(nameof(index));

        public IEnumerator<(string Key, string Value)> GetEnumerator()
        {
            for (int i = 0; i + 1 < kv.Length; i += 2) yield return (kv[i], kv[i + 1]);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// Whether this set holds exactly these interleaved, canonically ordered strings BY
    /// REFERENCE — the interner's hit test. A value-equal set of other instances answers false,
    /// which costs the caller one redundant label set and never a wrong one.
    /// </summary>
    internal bool SameReferences(ReadOnlySpan<string> sortedInterleaved)
    {
        var kv = _kv;
        if (kv.Length != sortedInterleaved.Length) return false;
        for (int i = 0; i < kv.Length; i++)
            if (!ReferenceEquals(kv[i], sortedInterleaved[i])) return false;
        return true;
    }

    public bool Equals(LabelSet? other)
    {
        if (ReferenceEquals(this, other)) return true;
        // The cached hashes first: both are value hashes, so a mismatch is a proof of
        // inequality and costs one compare instead of a string walk.
        if (other is null || _hash != other._hash) return false;

        var a = _kv;
        var b = other._kv;
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
        {
            string x = a[i], y = b[i];
            // Canonical strings match by reference; an uninterned one (from disk, from a caller,
            // or past the interner's cap) is compared by value, ordinal.
            if (!ReferenceEquals(x, y) && !string.Equals(x, y)) return false;
        }
        return true;
    }

    public override bool Equals(object? obj) => obj is LabelSet l && Equals(l);
    public override int  GetHashCode() => _hash;

    public override string ToString()
    {
        if (_kv.Length == 0) return "{}";
        var sb = new StringBuilder("{");
        for (int i = 0; i + 1 < _kv.Length; i += 2)
            sb.Append(_kv[i]).Append('=').Append('"').Append(_kv[i + 1]).Append('"').Append(',');
        sb[^1] = '}';
        return sb.ToString();
    }
}

/// <summary>
/// Canonical instances of metric label strings and label sets, so the ingest path stops
/// materialising a fresh copy of text the process already holds.
///
/// <para><b>Why.</b> An OTLP export re-sends every series every interval. A 500-point batch
/// decoded 8 000 label strings of which 26 were distinct — 99.7 % duplicates, ~900 B per point,
/// and the survivors promoted to gen2 because a <see cref="LabelSet"/> is retained by the hot
/// tier, the WAL's series registry, the catalog and the exemplar rings. Strings go through a
/// <see cref="StringInternPool"/> keyed by their UTF-8 bytes, which answers a hit with the
/// pooled instance and allocates nothing; label sets whose every string is pooled go through a
/// small lock-free cache keyed by those strings' pool ids, which answers a hit with the label
/// set built the first time.</para>
///
/// <para><b>Bounded, and degrading rather than dropping.</b> The string pool holds at most
/// <see cref="DefaultMaxStrings"/> distinct strings per epoch (see below) and does not
/// intern one longer than <see cref="MaxInternedUtf8Bytes"/> — worst case ≈ 16 384 ×
/// (≤ 278 B string + ~56 B of dictionary entry and slot) ≈ 5.5 MB, whatever the label
/// cardinality. Past the cap every new string is a plain <c>new string</c>, exactly what the
/// parser allocated before, and the epoch's <see cref="StringInternPool.PoolExhausted"/> fires once. The
/// label-set cache is a fixed <see cref="DefaultLabelSetSlots"/>-slot table that overwrites on
/// collision, so it never grows and a series nobody sends any more is simply displaced. A point
/// is never refused by either: a miss costs its allocation, not its data.</para>
///
/// <para>Its own pool, not <see cref="StringInternPool.Shared"/>: that one indexes log message
/// templates, and a high-cardinality label saturating it would make every later log event
/// carry its own template string.</para>
///
/// <para><b>Reset by epoch, because churn fills it (#88).</b> The pool never evicts, and a cluster
/// whose pods churn puts ~3.3 new strings into it per replaced pod (<c>k8s.pod.name</c>,
/// <c>k8s.pod.uid</c>, <c>container.id</c>, a share of <c>k8s.replicaset.name</c>):
/// <c>MetricLabelPoolChurnProbe</c> fills it after 4 737 replaced pods — 32 rollouts of each of 50
/// deployments. From then on every pod born later paid, on every export, a fresh string per identity
/// label and a fresh label set per point — 436 B/point against 165 — until the process restarted,
/// while the pool held the strings of pods long gone. So when a miss finds the pool FULL and at
/// least <see cref="ResetInterval"/> has passed since the last reset (or since construction), the
/// string pool and the label-set table are replaced by empty ones, whole, and live traffic
/// re-interns what it still sends. Nothing else changes: a <see cref="LabelSet"/> built before the
/// reset keeps its strings and stays equal BY VALUE to the one built after it (the hash is a value
/// hash, see <see cref="LabelSet"/>), so a series is the same series across the reset — only
/// reference equality with the new instances is lost, which costs a string compare where a pointer
/// compare was, never a split. The interval bounds the thrash when the live set itself outgrows the
/// pool: at most one reset — a re-intern of what is live, a few MB — per interval. A fixed policy,
/// no knob: <see cref="Resets"/> and <see cref="Saturations"/> are reported by
/// <c>/api/diagnostics</c>. Metric label ids are never persisted (the WAL and the <c>.mts</c> files
/// keep text), which is what makes a reset safe here and never for the log-template pool, whose
/// ids are on disk.</para>
/// </summary>
public sealed class MetricLabelInterner
{
    public const int DefaultMaxStrings     = 16_384;
    public const int DefaultLabelSetSlots  = 8_192;

    /// <summary>
    /// The least time between two resets of a full pool (see the class remarks). An hour: churn fills
    /// the pool over days, so the first reset after that comes within the hour, while a live set that
    /// by itself outgrows the pool — where a reset buys little — costs at most one re-intern an hour.
    /// </summary>
    public static readonly TimeSpan ResetInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// Longer strings are materialised, not pooled: a value that long is an id or a message,
    /// not a dimension that repeats, and the pool keeps what it holds for the life of the
    /// process. Measured in UTF-8 bytes on the byte path and in chars on the string path; for
    /// ASCII the two agree, and where they do not the only cost is one path pooling a string
    /// the other does not — equality is by value either way.
    /// </summary>
    public const int MaxInternedUtf8Bytes  = 128;

    /// <summary>The process-wide instance the OTLP parsers and the WAL replay share.</summary>
    public static readonly MetricLabelInterner Shared = new(DefaultMaxStrings, DefaultLabelSetSlots);

    /// <summary>
    /// The epoch's string pool and label-set table. A reset replaces both — the table first — and a
    /// reader may meet one of each epoch, or ids of two epochs in one label-set probe (a reset between
    /// a key's intern and its value's): that costs a miss at most, because a hit is confirmed string
    /// by string (<see cref="LabelSet.SameReferences"/>). Two fields rather than one epoch object, so
    /// the hot path loads what it loaded before the reset existed — no extra indirection per string.
    /// </summary>
    private volatile StringInternPool _strings;
    private volatile LabelSet?[]      _sets;
    private readonly int              _mask;
    private readonly int              _maxStrings;
    private readonly TimeProvider     _time;
    /// <summary><see cref="TimeProvider.GetTimestamp"/> of the last reset, or of construction.</summary>
    private long _lastReset;
    private int  _resets;
    private int  _saturations;

    public MetricLabelInterner(int maxStrings, int labelSetSlots) : this(maxStrings, labelSetSlots, TimeProvider.System) { }

    /// <param name="time">The clock <see cref="ResetInterval"/> is measured on — a seam for tests.</param>
    public MetricLabelInterner(int maxStrings, int labelSetSlots, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(time);
        int slots   = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)Math.Max(labelSetSlots, 2));
        _maxStrings = maxStrings;
        _mask       = slots - 1;
        _time       = time;
        _lastReset  = time.GetTimestamp();
        _sets       = new LabelSet?[slots];
        _strings    = NewPool();
    }

    /// <summary>A pool for a new epoch, whose first saturation is counted in <see cref="Saturations"/>.</summary>
    private StringInternPool NewPool()
    {
        var pool = new StringInternPool(_maxStrings);
        pool.PoolExhausted += _ => Interlocked.Increment(ref _saturations);   // once per pool, by the pool's own guard
        return pool;
    }

    /// <summary>
    /// The CURRENT epoch's string pool — for its cap, its fill and its <c>PoolExhausted</c> event. A
    /// reset replaces it: a subscriber to that event hears about this epoch only.
    /// </summary>
    public StringInternPool Strings => _strings;

    /// <summary>How many times the pool has been reset (see the class remarks).</summary>
    public int Resets => Volatile.Read(ref _resets);

    /// <summary>How many epochs' pools have filled up — one more than <see cref="Resets"/> while the current one is full.</summary>
    public int Saturations => Volatile.Read(ref _saturations);

    /// <summary>
    /// The id <c>Intern</c> answers for the empty string. <see cref="string.Empty"/> is one
    /// instance already, so it is canonical without a pool entry, and a label set holding an
    /// empty value stays cacheable. Never a real pool id: those stop at the pool's cap.
    /// </summary>
    public const int EmptyStringId = int.MaxValue;

    /// <summary>
    /// The canonical string for these UTF-8 bytes, and its id: a pool id,
    /// <see cref="EmptyStringId"/> for empty input, or -1 when the string is not pooled — too
    /// long, or the pool is full — in which case <paramref name="value"/> is a fresh string.
    /// Invalid UTF-8 decodes exactly as <c>Encoding.UTF8.GetString</c> does, U+FFFD per invalid
    /// sequence. Allocation-free on a hit.
    /// </summary>
    public int Intern(ReadOnlySpan<byte> utf8, out string value)
    {
        if (utf8.IsEmpty) { value = string.Empty; return EmptyStringId; }
        if (utf8.Length > MaxInternedUtf8Bytes) { value = Encoding.UTF8.GetString(utf8); return -1; }
        var pool = _strings;
        int id = pool.Intern(utf8, out value);
        if (id < 0) OnPoolFull(pool);
        return id;
    }

    /// <summary>As <see cref="Intern(ReadOnlySpan{byte}, out string)"/>, for a string the caller
    /// already holds: answers the pooled instance, or <paramref name="s"/> itself.</summary>
    public int Intern(string s, out string value)
    {
        if (s.Length == 0) { value = string.Empty; return EmptyStringId; }
        if (s.Length > MaxInternedUtf8Bytes) { value = s; return -1; }
        var pool = _strings;
        int id = pool.Intern(s, out value);
        if (id < 0) OnPoolFull(pool);
        return id;
    }

    /// <summary>
    /// A miss the pool could not take: it is full (a length the pool refuses never reaches it) — and
    /// its saturation has been counted, once, by the pool's own <c>PoolExhausted</c>. Resets when
    /// <see cref="ResetInterval"/> has passed since the last reset. Every miss on a full pool comes
    /// here, so the path only reads — a field and the clock — until the interval is up; the one
    /// compare-exchange that claims the reset happens once per interval. The string at hand stays
    /// unpooled; the NEXT miss meets the new epoch.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private void OnPoolFull(StringInternPool full)
    {
        if (full.ClaimedCount < full.MaxPoolSize) return;                    // not full: nothing to do

        long last = Volatile.Read(ref _lastReset);
        long now  = _time.GetTimestamp();
        if (_time.GetElapsedTime(last, now) < ResetInterval) return;
        if (!ReferenceEquals(_strings, full)) return;                          // reset already, by another thread
        if (Interlocked.CompareExchange(ref _lastReset, now, last) != last) return;

        _sets    = new LabelSet?[_mask + 1];
        _strings = NewPool();
        Interlocked.Increment(ref _resets);
    }

    /// <summary>
    /// LOOKUP ONLY: the pooled instance of these UTF-8 bytes and its id when the pool already
    /// holds the text; otherwise a fresh string (decoded exactly as <c>Encoding.UTF8.GetString</c>
    /// does) and -1 — and NOTHING is added. For text read back from disk (<c>MetricReader</c>):
    /// the pool never evicts, so a cold read that interned every value it met would fill it at
    /// startup with the dead values of the whole retention window, and every series started
    /// after that would be ingested at the uninterned cost. A value still being sent is in the
    /// pool already — the parsers put it there — and comes back shared. Empty input answers
    /// <see cref="EmptyStringId"/>, as <see cref="Intern(ReadOnlySpan{byte}, out string)"/> does.
    /// </summary>
    public int Lookup(ReadOnlySpan<byte> utf8, out string value)
    {
        if (utf8.IsEmpty) { value = string.Empty; return EmptyStringId; }
        if (utf8.Length > MaxInternedUtf8Bytes) { value = Encoding.UTF8.GetString(utf8); return -1; }

        // A UTF-8 byte decodes to at most one char (an invalid sequence to one U+FFFD), so the
        // bound above bounds the chars too: one decode, on the stack, serves the lookup and the miss.
        Span<char> chars = stackalloc char[MaxInternedUtf8Bytes];
        int n = Encoding.UTF8.GetChars(utf8, chars);
        if (_strings.TryGet(chars[..n], out string? pooled, out int id)) { value = pooled; return id; }
        value = new string(chars[..n]);
        return -1;
    }

    /// <summary>
    /// LOOKUP ONLY counterpart of <see cref="GetLabelSet"/>: sorts <paramref name="kv"/> and
    /// <paramref name="ids"/> into canonical order in place, answers the set the table holds for
    /// exactly these instances when there is one, and otherwise builds a fresh set WITHOUT
    /// publishing it — the same probe of the same two slots, and no write. See <see cref="Lookup"/>
    /// for why a cold read must not fill the table either.
    /// </summary>
    public LabelSet LookupLabelSet(Span<string> kv, Span<int> ids) => ProbeLabelSet(kv, ids, publish: false);

    /// <summary>The canonical instance of <paramref name="s"/> (or <paramref name="s"/> itself when
    /// it is not pooled). For names and units, which a <c>SeriesKey</c> holds for the series'
    /// life.</summary>
    public string Intern(string s)
    {
        Intern(s, out string value);
        return value;
    }

    /// <summary>
    /// The label set for interleaved pairs <paramref name="kv"/> (k0, v0, k1, v1, … in any order),
    /// with <paramref name="ids"/> holding each string's pool id from <c>Intern</c>. Both spans are
    /// sorted in place, in step, into canonical order.
    ///
    /// <para>When every id is a real one, the set is looked up by those ids in a fixed table and a
    /// set holding the very same instances is returned as is — the steady state, since an exporter
    /// re-sends the same series every interval. Otherwise, or on a miss, a new set is built; on a
    /// miss it is also published into the table, displacing whatever held the slot.</para>
    /// </summary>
    public LabelSet GetLabelSet(Span<string> kv, Span<int> ids) => ProbeLabelSet(kv, ids, publish: true);

    /// <summary>
    /// THE ONE PROBE behind <see cref="GetLabelSet"/> and <see cref="LookupLabelSet"/>: the argument
    /// checks, the canonical sort, the hash of the ids and the two-slot probe are the same code for
    /// both, and <paramref name="publish"/> decides only whether a miss is written into the table.
    /// They used to be two copies — and if the hash or the slot choice of one had moved, a set
    /// published by ingest would no longer be found by the cold reader's lookup: every cold read
    /// would then build fresh label sets, silently, with no test failing.
    /// </summary>
    private LabelSet ProbeLabelSet(Span<string> kv, Span<int> ids, bool publish)
    {
        if (kv.IsEmpty) return LabelSet.Empty;
        if (ids.Length != kv.Length || (kv.Length & 1) != 0)
            throw new ArgumentException("one id per string, and whole pairs", nameof(ids));
        LabelSet.SortInterleaved(kv, ids);

        uint h = 2166136261;
        for (int i = 0; i < ids.Length; i++)
        {
            int id = ids[i];
            if (id < 0) return LabelSet.FromSorted(kv);   // not all pooled: no identity to key on
            h = (h ^ (uint)id) * 16777619;
        }
        h ^= h >> 15; h *= 0x2C1B3C6D; h ^= h >> 12;

        var sets = _sets;
        int a = (int)(h & (uint)_mask);
        int b = (int)((h >> 16 | h << 16) & (uint)_mask);

        var hit = Volatile.Read(ref sets[a]);
        if (hit is not null && hit.SameReferences(kv)) return hit;
        hit = Volatile.Read(ref sets[b]);
        if (hit is not null && hit.SameReferences(kv)) return hit;

        var created = LabelSet.FromSorted(kv);
        if (!publish) return created;                     // lookup only: a miss writes nothing
        // Two choices, no relocation: an empty slot if either is, else the first. A race here
        // costs a redundant label set, never a wrong one — a reader tests every string of a
        // candidate before it keeps it.
        int target = Volatile.Read(ref sets[a]) is null || Volatile.Read(ref sets[b]) is not null ? a : b;
        Volatile.Write(ref sets[target], created);
        return created;
    }
}

/// <summary>
/// A single observed metric data point ready for ingestion.
/// </summary>
public sealed class MetricIngestItem
{
    /// <summary>Metric name (e.g. "http.server.request.duration").</summary>
    public string     Name              { get; init; } = string.Empty;

    /// <summary>Instrument type.</summary>
    public MetricKind Kind              { get; init; }

    /// <summary>Optional unit string (e.g. "ms", "By", "1").</summary>
    public string     Unit              { get; init; } = string.Empty;

    /// <summary>Label set identifying this time series.</summary>
    public LabelSet   Labels            { get; init; } = LabelSet.Empty;

    /// <summary>Data point timestamp, Unix nanoseconds.</summary>
    public long       TimestampUnixNano { get; init; }

    // ── Scalar (Counter / Gauge) ───────────────────────────────────────────────
    /// <summary>Scalar value for Counter or Gauge data points.</summary>
    public double     ScalarValue       { get; init; }

    // ── Histogram ─────────────────────────────────────────────────────────────
    public long       HistogramCount    { get; init; }
    public double     HistogramSum      { get; init; }
    /// <summary>Upper bounds of explicit histogram buckets (null for scalar metrics).</summary>
    public double[]?  BucketBounds      { get; init; }
    /// <summary>Counts per bucket, length == BucketBounds.Length + 1 (overflow bucket).</summary>
    public long[]?    BucketCounts      { get; init; }

    /// <summary>Sampled exemplars linking individual measurements to traces (may be null).</summary>
    public MetricExemplar[]? Exemplars   { get; init; }

    /// <summary>
    /// The stored form of this item — THE one definition of it.
    ///
    /// <para>The write-ahead log and the hot tier must agree on it to the bit, and they used to
    /// agree by the ingest loop computing it once and handing the same struct to both. Once the
    /// log takes a whole batch under one lock (see <c>MetricWriteAheadLog.Append(ReadOnlySpan&lt;
    /// MetricIngestItem&gt;)</c>) the two no longer share a call frame, and carrying a 40-byte
    /// struct per point through a pooled side array to keep them together cost more in memory
    /// traffic than the lock acquisitions the batch saved — measured at +140 ns/point on one
    /// thread. Deriving it twice from the item, which is in cache either way, costs a few field
    /// reads; having it written down once is what keeps the two derivations the same.</para>
    /// </summary>
    internal MetricDataPoint ToDataPoint() => new()
    {
        TimestampUnixNano = TimestampUnixNano,
        Value             = Kind == MetricKind.Histogram
                                ? (HistogramCount > 0 ? HistogramSum / HistogramCount : 0)
                                : ScalarValue,
        Count             = HistogramCount,
        Sum               = HistogramSum,
        BucketCounts      = BucketCounts,   // preserved for real percentiles + heatmap
    };
}

/// <summary>
/// An exemplar: a sampled measurement linked to the trace/span that produced it.
/// Enables jumping from a metric (e.g. a latency spike) straight to the exact trace.
/// </summary>
public sealed class MetricExemplar
{
    public long   TimestampUnixNano { get; init; }
    public double Value             { get; init; }
    /// <summary>32-char lowercase hex trace id (empty if absent).</summary>
    public string TraceId           { get; init; } = string.Empty;
    /// <summary>16-char lowercase hex span id (empty if absent).</summary>
    public string SpanId            { get; init; } = string.Empty;
}

/// <summary>
/// A stored data point in a time series.
/// </summary>
public struct MetricDataPoint
{
    /// <summary>Unix nanoseconds.</summary>
    public long   TimestampUnixNano;
    /// <summary>Scalar value (Counter / Gauge); for Histogram this is the mean (sum/count).</summary>
    public double Value;
    /// <summary>Histogram count; 0 for scalar metrics.</summary>
    public long   Count;
    /// <summary>Histogram sum; 0 for scalar metrics.</summary>
    public double Sum;
    /// <summary>
    /// Per-bucket counts for Histogram points (length == series BucketBounds.Length + 1,
    /// last entry is the +Inf overflow bucket). Null for scalar metrics. The bucket
    /// upper bounds themselves are stored once per series on <see cref="MetricSeries.BucketBounds"/>.
    /// </summary>
    public long[]? BucketCounts;
}

/// <summary>
/// Query result — one time series.
/// </summary>
public sealed class MetricSeries
{
    public string       Name   { get; init; } = string.Empty;
    public MetricKind   Kind   { get; init; }
    public string       Unit   { get; init; } = string.Empty;
    public LabelSet     Labels { get; init; } = LabelSet.Empty;

    /// <summary>
    /// Histogram bucket upper bounds (explicit-bucket boundaries), shared by every
    /// point in the series. Null/empty for scalar metrics. A point's
    /// <see cref="MetricDataPoint.BucketCounts"/> has length <c>BucketBounds.Length + 1</c>.
    /// </summary>
    public double[]?    BucketBounds { get; init; }

    /// <summary>Data points ordered by timestamp ascending.</summary>
    public IReadOnlyList<MetricDataPoint> Points { get; init; } = [];
}

/// <summary>
/// Catalog entry describing one metric stream (name) — fed to the Explore UI.
/// </summary>
public sealed class MetricCatalogEntry
{
    public string     Name        { get; init; } = string.Empty;
    public MetricKind Kind        { get; init; }
    public string     Unit        { get; init; } = string.Empty;
    /// <summary>Distinct label keys observed across the metric's series.</summary>
    public string[]   LabelKeys   { get; init; } = [];
    /// <summary>Approximate number of distinct time series (label-set cardinality).</summary>
    public int        Cardinality { get; init; }
    /// <summary>Most recent data-point timestamp (Unix ms), 0 if unknown.</summary>
    public long       LastSeenMs  { get; init; }
}

/// <summary>Server-side aggregation operator applied to a metric query.</summary>
public enum MetricAggregation : byte
{
    /// <summary>Raw values, no aggregation across time (downsample only).</summary>
    None     = 0,
    /// <summary>Per-second rate of a cumulative counter (reset-aware).</summary>
    Rate     = 1,
    /// <summary>Total increase of a cumulative counter over each step (reset-aware).</summary>
    Increase = 2,
    Avg      = 3,
    Min      = 4,
    Max      = 5,
    Last     = 6,
    Sum      = 7,
    /// <summary>Histogram percentile via histogram_quantile over bucket deltas.</summary>
    Quantile = 8,
}

/// <summary>A server-side aggregated metric query.</summary>
public sealed class MetricQueryRequest
{
    public string             Metric      { get; init; } = string.Empty;
    public DateTimeOffset?    From        { get; init; }
    public DateTimeOffset?    To          { get; init; }
    public TimeSpan?          Step        { get; init; }
    public MetricAggregation  Aggregation { get; init; } = MetricAggregation.None;
    /// <summary>Quantile in [0,1] when <see cref="Aggregation"/> is Quantile (e.g. 0.95).</summary>
    public double?            Quantile    { get; init; }
    /// <summary>Label keys to group by; series sharing these labels are combined.</summary>
    public string[]?          GroupBy     { get; init; }
    /// <summary>Exact label matchers (key → value) applied before aggregation.</summary>
    public IReadOnlyDictionary<string, string>? Filters { get; init; }
    /// <summary>Keep only the top-K resulting series by their latest value.</summary>
    public int?               TopK        { get; init; }
}

/// <summary>A stored exemplar returned from a query (sample + its series labels).</summary>
public sealed class ExemplarSample
{
    public long     TimestampUnixNano { get; init; }
    public double   Value             { get; init; }
    public string   TraceId           { get; init; } = string.Empty;
    public string   SpanId            { get; init; } = string.Empty;
    public LabelSet Labels            { get; init; } = LabelSet.Empty;
}

/// <summary>Binary operator for a metric expression (A op B).</summary>
public enum MetricExprOp { Div, Mul, Add, Sub }

/// <summary>
/// A binary metric expression: combine two aggregated queries pointwise by timestamp.
/// Each side is reduced to a single series (summed across its result series), then
/// <c>left op right</c> is computed per timestamp and optionally scaled (e.g. ×100 for %).
/// Enables ratios like error-rate = rate(5xx) / rate(total) × 100.
/// </summary>
public sealed class MetricExprRequest
{
    public required MetricQueryRequest Left  { get; init; }
    public required MetricQueryRequest Right { get; init; }
    public MetricExprOp                Op    { get; init; } = MetricExprOp.Div;
    /// <summary>Multiply the result (e.g. 100 to turn a ratio into a percentage).</summary>
    public double                      Scale { get; init; } = 1;
    public string?                     Name  { get; init; }
}

/// <summary>One column of a latency/distribution heatmap (one time step).</summary>
public sealed class HeatmapColumn
{
    /// <summary>Bucket start timestamp, Unix ms.</summary>
    public long   Ts     { get; init; }
    /// <summary>Per-bucket counts within this time step (reset-aware delta).</summary>
    public double[] Counts { get; init; } = [];
}

/// <summary>Histogram heatmap result: shared bucket bounds + per-step columns.</summary>
public sealed class HeatmapResult
{
    public double[]        Bounds  { get; init; } = [];
    public HeatmapColumn[] Columns { get; init; } = [];
    public string          Unit    { get; init; } = string.Empty;
}
