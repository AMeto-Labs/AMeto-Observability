namespace Ameto.Tracing.TraceQL;

/// <summary>
/// A TRACEQL FILTER, READY FOR THE SPANS OF ONE PAGE: every attribute it names is read out of a
/// span's msgpack map in ONE walk, however many of its predicates name one (#94).
///
/// <para><b>Why.</b> <see cref="AttributePredicate"/> finds its key with
/// <see cref="SpanAttributeBlob.TryFind"/>, and that walk cannot stop at the first hit: the LAST copy
/// of a key is the one that counts, because the OTLP mappers write the resource's attributes first
/// and the span's own after them, so that the span's shadow the resource's. Every attribute
/// predicate therefore read the whole map, and <c>{ .db.system = "x" &amp;&amp; .http.route = "y"
/// &amp;&amp; .peer.service = "z" }</c> was three walks of every span the page's scan handed back.
/// <see cref="SpanAttributeBlob.FindValues"/> already answers up to
/// <see cref="SpanAttributeBlob.MaxKeyAlternatives"/> keys in one walk, under the same rules. This
/// type collects the distinct keys a filter names once per page and asks it.</para>
///
/// <para><b>The same answer as each predicate alone</b>, and not by approximation:
/// <list type="bullet">
/// <item>the same last-copy-wins walk: <c>TryFind</c> walks on past a hit, <c>FindValues</c>
/// overwrites a key's slot at every later copy;</item>
/// <item>the same reading of a value nothing can compare (msgpack nil, an array, a nested map):
/// <c>TryFind</c> reports it found, with a kind <c>AttributePredicate.CompareAttr</c> answers unknown
/// for and <see cref="AttributePresencePredicate"/> counts as absent; <c>FindValues</c> clears its bit
/// for exactly those kinds, and a clear bit reads here as the same unknown and the same absent;</item>
/// <item>the same map that will not read: a walk that throws part-way is asked again one key at a
/// time, which IS the old walk — see <see cref="SpanAttributeBlob.TryFindValues"/> for the one value
/// that makes the difference;</item>
/// <item>the same comparison, <c>AttributePredicate.CompareAttr</c> itself, and the same keys, the
/// predicates' own UTF-8 bytes — told apart by those bytes and not by the strings, because the walk
/// compares bytes: two strings that encode alike (a lone surrogate becomes U+FFFD) are one key to it,
/// and as two slots the second would never be found;</item>
/// <item>and a record with no blob — one built from a dictionary — evaluated by the AST, whole, as
/// before.</item>
/// </list>
/// The connectives below are the AST's own truth tables line for line, and a subtree that names no
/// attribute is handed to the AST untouched. <c>SharedAttributeWalkTests</c> holds the two to one
/// answer over random maps (torn ones included) and random filters.</para>
///
/// <para><b>Lazy, so a short circuit still saves the walk.</b> A span's map is walked when the first
/// attribute predicate is reached, not before: <c>{ duration &gt; 1s &amp;&amp; .a = "x" }</c> walks
/// nothing for a span the duration has already rejected. A span is never walked more often than the
/// per-predicate evaluation walked it — one walk per batch reached, where that took one per predicate
/// reached. The one walk does a little more per pair, comparing each map key with every key of its
/// batch instead of one, which is what a filter whose first predicate decides every span pays for
/// the others: <c>TraceQlSharedWalkProbe</c> prints that case beside the ones this is for.</para>
///
/// <para><b>More keys than one walk takes.</b> The distinct keys are numbered in order of first
/// appearance and walked <see cref="SpanAttributeBlob.MaxKeyAlternatives"/> at a time, each batch on
/// the first ask for one of its keys: a filter naming twenty keys costs a span at most three walks
/// instead of twenty, and the count never throws.</para>
///
/// <para><b>One per page, never shared.</b> The span being evaluated and the values found in it live
/// in the instance, so it is not safe for two threads at once; <see cref="TraceQLExecutor"/> builds
/// one for each page, on that page's own flow, and nothing keeps it afterwards.</para>
/// </summary>
internal sealed class SpanPredicateEvaluator
{
    private const int KeysPerWalk = SpanAttributeBlob.MaxKeyAlternatives;

    private readonly SpanPredicate _predicate;

    /// <summary>The filter over the shared walk; null when it names no attribute and the AST is all there is.</summary>
    private readonly Node? _root;

    /// <summary>The distinct keys, UTF-8, in order of first appearance. Batch <c>b</c> is <c>[b·KeysPerWalk, (b+1)·KeysPerWalk)</c>.</summary>
    private readonly byte[][] _keys = [];

    /// <summary>One slot per key: what the last walk of its batch found.</summary>
    private readonly SpanAttrValue[] _values = [];

    /// <summary>Per batch: the found-bits of its last walk, one per key of the batch.</summary>
    private readonly int[] _found = [];

    /// <summary>Per batch: the ordinal of the span its bits and slots belong to. Any other is stale.</summary>
    private readonly long[] _walkedFor = [];

    private ReadOnlyMemory<byte> _blob;   // the map of the span being evaluated
    private long _span;                   // that span's ordinal: 1 for the first, never reused
    private long _walks;

    public SpanPredicateEvaluator(SpanPredicate predicate) : this(predicate, share: true) { }

    private SpanPredicateEvaluator(SpanPredicate predicate, bool share)
    {
        _predicate = predicate;
        if (!share) return;

        List<byte[]>?            keys  = null;
        Dictionary<byte[], int>? slots = null;
        _root = Compile(predicate, ref keys, ref slots);
        if (keys is null) return;

        _keys       = [.. keys];
        _values     = new SpanAttrValue[_keys.Length];
        int batches = (_keys.Length + KeysPerWalk - 1) / KeysPerWalk;
        _found      = new int[batches];
        _walkedFor  = new long[batches];
    }

    /// <summary>
    /// Test hook: an evaluator that shares nothing — every span goes to the AST and each attribute
    /// predicate walks the map for itself, as every page did before #94. The probe runs the same page
    /// through both, interleaved in one process, so its before and after are one machine's.
    /// </summary>
    internal static SpanPredicateEvaluator PerPredicateForTest(SpanPredicate predicate) =>
        new(predicate, share: false);

    /// <summary>The predicate this evaluates — what the executor extracts its scan hints from.</summary>
    public SpanPredicate Predicate => _predicate;

    /// <summary>Test hook: walks of a span's map so far — at most one per batch of keys per span.</summary>
    internal long WalksForTest => _walks;

    /// <summary>
    /// <see cref="SpanPredicate.Evaluate"/>'s three-valued answer for <paramref name="span"/>: true
    /// selects it, false rejects it, null is a span that cannot answer.
    /// </summary>
    public bool? Evaluate(SpanRecord span)
    {
        if (_root is null) return _predicate.Evaluate(span);

        // A record built from a dictionary — every fixture, the legacy migration — has no map to
        // walk, and the AST's dictionary path is the answer for it.
        var blob = span.AttributesBytes;
        if (blob.IsEmpty) return _predicate.Evaluate(span);

        _blob = blob;
        _span++;   // every batch is stale now: nothing found in the last span answers for this one
        return _root.Evaluate(span, this);
    }

    /// <summary>
    /// Whether the span's map holds slot <paramref name="slot"/>'s key with a value a predicate can
    /// read, walking the slot's batch first if this span has not had it walked.
    /// </summary>
    private bool Found(int slot)
    {
        int batch = slot / KeysPerWalk;
        if (_walkedFor[batch] != _span) Walk(batch);
        return (_found[batch] & (1 << (slot - batch * KeysPerWalk))) != 0;
    }

    private void Walk(int batch)
    {
        int first  = batch * KeysPerWalk;
        int count  = Math.Min(KeysPerWalk, _keys.Length - first);
        var keys   = _keys.AsSpan(first, count);
        var values = _values.AsSpan(first, count);

        _walks++;
        if (!SpanAttributeBlob.TryFindValues(_blob, keys, values, out int found))
            found = FindEachAlone(keys, values);

        _found[batch]     = found;
        _walkedFor[batch] = _span;
    }

    /// <summary>
    /// A MAP THE SHARED WALK COULD NOT READ TO ITS END, asked one key at a time — which is exactly
    /// how every predicate asked it before. A torn map then answers "not found" for each key, as it
    /// does in one walk; a value that steps over cleanly but will not decode answers "not found"
    /// for its own key only, where one walk would have lost the whole batch to it. Rare, and the
    /// cost is the old one: a walk per key.
    /// </summary>
    private int FindEachAlone(ReadOnlySpan<byte[]> keys, Span<SpanAttrValue> values)
    {
        int found = 0;
        for (int j = 0; j < keys.Length; j++)
        {
            _walks++;
            if (SpanAttributeBlob.TryFind(_blob, keys[j], out values[j])
                && values[j].Kind is not (SpanAttrKind.Null or SpanAttrKind.Other))
                found |= 1 << j;
        }
        return found;
    }

    // ── Compiling the filter ──────────────────────────────────────────────────

    /// <summary>
    /// The plan for <paramref name="p"/>, or null when nothing under it names an attribute — the
    /// caller then hands that subtree to the AST as it is.
    ///
    /// <para>Recursive to the AST's own depth, and no deeper than its <c>Evaluate</c> already goes:
    /// the parser bounds nesting at 64, and the longest flat chain its 8 KB cap admits — some 1 600
    /// terms of <c>a=1||</c> — is as deep here as it is there.</para>
    /// </summary>
    private static Node? Compile(SpanPredicate p, ref List<byte[]>? keys, ref Dictionary<byte[], int>? slots)
    {
        switch (p)
        {
            case AndPredicate and:
            {
                Node? l = Compile(and.Left,  ref keys, ref slots);
                Node? r = Compile(and.Right, ref keys, ref slots);
                return l is null && r is null
                    ? null
                    : new And(l ?? new Ast(and.Left), r ?? new Ast(and.Right));
            }

            case OrPredicate or:
            {
                Node? l = Compile(or.Left,  ref keys, ref slots);
                Node? r = Compile(or.Right, ref keys, ref slots);
                return l is null && r is null
                    ? null
                    : new Or(l ?? new Ast(or.Left), r ?? new Ast(or.Right));
            }

            case NotPredicate not:
                return Compile(not.Inner, ref keys, ref slots) is { } inner ? new Not(inner) : null;

            case AttributePredicate attr:
                return new Compare(SlotOf(attr.KeyUtf8, ref keys, ref slots), attr.Op, attr.Value);

            case AttributePresencePredicate presence:
                return new Presence(SlotOf(presence.KeyUtf8, ref keys, ref slots), presence.Present);

            default:
                // An intrinsic — duration, status, name, kind, service, the promoted HTTP status —
                // or a predicate this type does not know, which then reads what it reads its own way.
                return null;
        }
    }

    private static int SlotOf(byte[] keyUtf8, ref List<byte[]>? keys, ref Dictionary<byte[], int>? slots)
    {
        keys  ??= new List<byte[]>(4);
        slots ??= new Dictionary<byte[], int>(Utf8KeyComparer.Instance);
        if (!slots.TryGetValue(keyUtf8, out int slot))
        {
            slot = keys.Count;
            keys.Add(keyUtf8);
            slots.Add(keyUtf8, slot);
        }
        return slot;
    }

    /// <summary>Two keys are one when their BYTES are — what the walk compares. See the class summary.</summary>
    private sealed class Utf8KeyComparer : IEqualityComparer<byte[]>
    {
        public static readonly Utf8KeyComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] key)
        {
            var h = new HashCode();
            h.AddBytes(key);
            return h.ToHashCode();
        }
    }

    // ── The plan ──────────────────────────────────────────────────────────────

    private abstract class Node
    {
        public abstract bool? Evaluate(SpanRecord span, SpanPredicateEvaluator walk);
    }

    /// <summary>A subtree that names no attribute: the AST answers it, exactly as before.</summary>
    private sealed class Ast(SpanPredicate predicate) : Node
    {
        public override bool? Evaluate(SpanRecord span, SpanPredicateEvaluator walk) => predicate.Evaluate(span);
    }

    /// <summary><see cref="AndPredicate"/>'s table: false decides, unknown survives only when nothing does.</summary>
    private sealed class And(Node left, Node right) : Node
    {
        public override bool? Evaluate(SpanRecord span, SpanPredicateEvaluator walk)
        {
            bool? l = left.Evaluate(span, walk);
            if (l == false) return false;
            bool? r = right.Evaluate(span, walk);
            if (r == false) return false;
            return l is null || r is null ? null : true;
        }
    }

    /// <summary><see cref="OrPredicate"/>'s table: true decides, its mirror image.</summary>
    private sealed class Or(Node left, Node right) : Node
    {
        public override bool? Evaluate(SpanRecord span, SpanPredicateEvaluator walk)
        {
            bool? l = left.Evaluate(span, walk);
            if (l == true) return true;
            bool? r = right.Evaluate(span, walk);
            if (r == true) return true;
            return l is null || r is null ? null : false;
        }
    }

    /// <summary><see cref="NotPredicate"/>'s table: unknown negates to unknown.</summary>
    private sealed class Not(Node inner) : Node
    {
        public override bool? Evaluate(SpanRecord span, SpanPredicateEvaluator walk) => inner.Evaluate(span, walk) switch
        {
            true  => false,
            false => true,
            null  => null,
        };
    }

    /// <summary><see cref="AttributePredicate"/> over its slot: a key the walk did not find is unknown.</summary>
    private sealed class Compare(int slot, TraceQLOp op, TraceQLValue value) : Node
    {
        private readonly int          _slot  = slot;
        private readonly TraceQLOp    _op    = op;
        private readonly TraceQLValue _value = value;

        public override bool? Evaluate(SpanRecord span, SpanPredicateEvaluator walk) =>
            walk.Found(_slot)
                ? AttributePredicate.CompareAttr(in walk._values[_slot], _op, in _value)
                : null;
    }

    /// <summary><see cref="AttributePresencePredicate"/> over its slot: always answers, never unknown.</summary>
    private sealed class Presence(int slot, bool present) : Node
    {
        private readonly int  _slot    = slot;
        private readonly bool _present = present;

        public override bool? Evaluate(SpanRecord span, SpanPredicateEvaluator walk) => walk.Found(_slot) == _present;
    }
}
