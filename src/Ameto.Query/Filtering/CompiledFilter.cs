using Ameto.Core;

namespace Ameto.Query.Filtering;

/// <summary>
/// A parsed, reusable filter with helper methods for index acceleration.
///
/// Index hints: the query executor uses <see cref="TryGetIndexHint"/> to skip entire
/// segments without decompressing blocks when the segment index says there are no
/// matching events.
/// </summary>
public sealed class CompiledFilter : IPreparedFilter
{
    private readonly FilterNode _root;

    // Pre-computed at compile time so QueryExecutor pays zero allocation per query.
    private readonly IReadOnlyList<(string property, string text)>    _trigramHints;
    private readonly IReadOnlyList<(string property, object? value)>  _invertedHints;
    private readonly IReadOnlyList<(string property, object? value)>  _invertedHintsBesideExceptionPresence;

    // The single bloom/MightContain hint, also resolved once at compile time — the
    // prefilter asks for it inside a per-segment parallel loop.
    private readonly bool    _hasHint;
    private readonly string  _hintProperty;
    private readonly object? _hintValue;

    private readonly HeaderPredicate? _headerPredicate;

    // How deep an index this filter's hints may rely on (see ForIndexDepth), and the one
    // shallower copy of it a server asks for, made on first use.
    private readonly int     _indexDepth;
    private CompiledFilter?  _atShallowerDepth;

    private CompiledFilter(string? expression, FilterNode root)
        : this(expression, RewriteTimeCompares(root), FullIndexDepth) { }

    private CompiledFilter(string? expression, FilterNode root, int indexDepth)
    {
        Expression     = expression;
        _root          = root;
        _indexDepth    = indexDepth;
        _trigramHints  = BuildTrigramHints(root, indexDepth);
        _invertedHints = BuildInvertedHints(root, indexDepth);
        _invertedHintsBesideExceptionPresence = BesideExceptionPresence(_invertedHints);
        _hasHint       = TryExtract(root, indexDepth, out _hintProperty, out _hintValue);
        _headerPredicate = Filtering.HeaderPredicate.TryBuild(root);

        long? min = null, max = null;
        CollectTimeBounds(root, ref min, ref max);
        MinTimestampTicks = min;
        MaxTimestampTicks = max;
    }

    public static CompiledFilter Compile(string? expression) =>
        new(expression, FilterParser.Parse(expression));

    /// <summary>
    /// This filter as asked of an index that flattened nested properties only
    /// <paramref name="depth"/> map levels below the top (<c>Indexing.MaxPropertyFlattenDepth</c>,
    /// which <c>SegmentIndexBuilder</c> stops at): the same predicate — <see cref="Matches"/> and
    /// everything else about the filter are unchanged — with only the hints such an index holds
    /// the values for.
    ///
    /// <para>A hint on a user property path with more levels than that, a substring hint on one,
    /// and free text (which <see cref="FilterEvaluator"/> searches <see cref="FullIndexDepth"/>
    /// deep whatever the index holds) are left out: the bloom and the trigram index are keyless,
    /// so the absence of a value the builder never walked down to reads as proof that no row
    /// holds it, and the group was dropped with the row in it. Without the hint the rows are read
    /// instead. Header fields are filed at any depth and keep their hints.</para>
    ///
    /// <para>At <see cref="FullIndexDepth"/> or deeper this is the filter itself — every hint it
    /// gives asks for no more than that, and a deeper index holds more, never less. Below it, a
    /// copy is made the first time and kept: the server's depth is one number, so a filter that
    /// is asked again (the live tail's prepared filter, once per poll) pays for it once. Never
    /// adds a hint this filter does not give.</para>
    /// </summary>
    internal CompiledFilter ForIndexDepth(int depth)
    {
        if (depth >= _indexDepth) return this;

        var kept = Volatile.Read(ref _atShallowerDepth);
        if (kept is not null && kept._indexDepth == depth) return kept;

        // A race makes two equal copies and keeps the last; either answers the same.
        var made = new CompiledFilter(Expression, _root, depth);
        Volatile.Write(ref _atShallowerDepth, made);
        return made;
    }

    /// <inheritdoc/>
    public string? Expression { get; }

    // ── Header pushdown ───────────────────────────────────────────────────────

    /// <summary>
    /// The part of this filter the hot-tier scan can answer from the event HEADER before it
    /// materialises anything — level, trace / span id, service — or null when the AND-chain
    /// constrains none of those. Three-valued: it only ever says "cannot match"; every event
    /// it lets through is still put to <see cref="Matches"/>. See <see cref="HeaderPredicate"/>.
    /// </summary>
    public IHotHeaderPredicate? HeaderPredicate => _headerPredicate;

    /// <summary>
    /// The set of levels the filter's AND-chain admits, or null when it does not constrain
    /// the level. Exact — computed by running the evaluator's own comparison per level — so
    /// it can stand in for a <c>levels</c> parameter the caller did not pass: the cold
    /// prefilter then prunes level-split segments through their posting lists for
    /// <c>@l in ('Error', 'Fatal')</c> or <c>not @l = 'Debug'</c> the way it already does for
    /// an explicit level list. May be empty (<c>@l = 'Info'</c> matches nothing: the alias
    /// is not the spelling the evaluator compares against).
    /// </summary>
    public HashSet<LogLevel>? DerivedLevels => _headerPredicate?.DerivedLevels;

    // ── @t bounds ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Tightest lower/upper bound (UTC ticks, inclusive) the filter's AND-chain places on
    /// <c>@t</c>, or null. The executor intersects these with the request window, so the
    /// catalog filter, the zone map and the hot-tier header scan prune with them — a
    /// <c>@t &gt;= '...'</c> filter used to be evaluated per event over the whole catalog.
    /// The evaluator still re-checks every event (OR/NOT branches keep their own nodes),
    /// so these may only ever SKIP work.
    /// </summary>
    public long? MinTimestampTicks { get; }

    /// <inheritdoc cref="MinTimestampTicks"/>
    public long? MaxTimestampTicks { get; }

    /// <summary>
    /// Rewrites <c>@t op parseable-literal</c> comparisons into <see cref="TimeCompareNode"/>
    /// so the literal is parsed once and comparison is chronological (see the node's doc).
    /// Nodes are rebuilt only on the paths that actually changed.
    /// </summary>
    private static FilterNode RewriteTimeCompares(FilterNode node)
    {
        switch (node)
        {
            case CompareNode { RightProperty: null } cmp
                when cmp.Value is string s
                  && BuiltinFields.TryResolve(cmp.Property, out var field)
                  && field == BuiltinField.Timestamp
                  && TryParseTimeLiteral(s, out long ticks):
                return new TimeCompareNode(cmp.Op, ticks);

            // `@tr = 'hex32'` / `@sp = 'hex16'` (and !=): the id is compared as the integers
            // the header stores (see TraceIdCompareNode). Only a literal that IS the exact
            // rendering — fixed width, every character a hex digit — is rewritten; the
            // helper's parser tolerates whitespace, so the shape is checked here first.
            case CompareNode { RightProperty: null, Op: CompareOp.Eq or CompareOp.Ne } idCmp
                when idCmp.Value is string idText
                  && BuiltinFields.TryResolve(idCmp.Property, out var idField)
                  && (idField is BuiltinField.TraceId or BuiltinField.SpanId)
                  && TryParseIdLiteral(idText, idField == BuiltinField.SpanId, out ulong hi, out ulong lo):
                return new TraceIdCompareNode(idCmp.Op, idField == BuiltinField.SpanId, hi, lo, idCmp.Property);

            case AndNode and:
            {
                var l = RewriteTimeCompares(and.Left);
                var r = RewriteTimeCompares(and.Right);
                return ReferenceEquals(l, and.Left) && ReferenceEquals(r, and.Right) ? and : new AndNode(l, r);
            }
            case OrNode or:
            {
                var l = RewriteTimeCompares(or.Left);
                var r = RewriteTimeCompares(or.Right);
                return ReferenceEquals(l, or.Left) && ReferenceEquals(r, or.Right) ? or : new OrNode(l, r);
            }
            case NotNode not:
            {
                var inner = RewriteTimeCompares(not.Operand);
                return ReferenceEquals(inner, not.Operand) ? not : new NotNode(inner);
            }
            default:
                return node;
        }
    }

    /// <summary>
    /// Exactly 32 (trace) or 16 (span) hex digits, nothing else — the shape
    /// <c>TraceIdHelper.FormatTraceId</c> / <c>FormatSpanId</c> render, so an ordinal-ignore-case
    /// string equality against the rendering and an integer equality agree.
    /// </summary>
    private static bool TryParseIdLiteral(string s, bool isSpan, out ulong hi, out ulong lo)
    {
        hi = lo = 0;
        if (s.Length != (isSpan ? 16 : 32)) return false;
        foreach (char c in s)
            if (!char.IsAsciiHexDigit(c)) return false;
        return isSpan
            ? TraceIdHelper.TryParseSpanId(s, out lo)
            : TraceIdHelper.TryParseTraceId(s, out hi, out lo);
    }

    /// <summary>Invariant, offset-less literals assumed UTC — matching how events are stored.</summary>
    private static bool TryParseTimeLiteral(string s, out long ticks)
    {
        if (DateTimeOffset.TryParse(s, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AllowWhiteSpaces,
                out var dto))
        {
            ticks = dto.UtcTicks;
            return true;
        }
        ticks = 0;
        return false;
    }

    private static void CollectTimeBounds(FilterNode node, ref long? min, ref long? max)
    {
        switch (node)
        {
            case TimeCompareNode t:
                switch (t.Op)
                {
                    case CompareOp.Ge: min = Math.Max(min ?? long.MinValue, t.Ticks); break;
                    // Ticks are integral: a > b ⇔ a >= b+1 (clamped at the type edge).
                    case CompareOp.Gt: min = Math.Max(min ?? long.MinValue, t.Ticks == long.MaxValue ? long.MaxValue : t.Ticks + 1); break;
                    case CompareOp.Le: max = Math.Min(max ?? long.MaxValue, t.Ticks); break;
                    case CompareOp.Lt: max = Math.Min(max ?? long.MaxValue, t.Ticks == long.MinValue ? long.MinValue : t.Ticks - 1); break;
                    case CompareOp.Eq:
                        min = Math.Max(min ?? long.MinValue, t.Ticks);
                        max = Math.Min(max ?? long.MaxValue, t.Ticks);
                        break;
                    // Ne bounds nothing.
                }
                break;

            case AndNode and:
                CollectTimeBounds(and.Left,  ref min, ref max);
                CollectTimeBounds(and.Right, ref min, ref max);
                break;

            // OR/NOT: a bound taken from one branch would exclude the other's matches.
        }
    }

    public bool IsMatchAll => _root is MatchAllNode;

    public bool Matches(LogEvent ev) => FilterEvaluator.Matches(_root, ev);

    // ── Index hints ───────────────────────────────────────────────────────────

    /// <summary>
    /// The first equality constraint whose VALUE the segment bloom filter actually holds,
    /// used for the cheap phase-1 segment skip. Resolved once in the constructor; this is
    /// a field read.
    ///
    /// <para>A predicate on a field the bloom never saw (the exception message or stack,
    /// <c>@t</c>, <c>@id</c>) must not be offered here: the bloom would answer "no" for a
    /// value it was never given and the whole segment would be dropped. Nor may a value the
    /// bloom was never given for that field: the empty string of a header field
    /// (<see cref="IsUnfiledHeaderValue"/>).</para>
    ///
    /// <para><paramref name="propertyName"/> is the inverted bucket the phase-2
    /// <c>MightContain</c> asks for the value — or EMPTY for a field the bloom holds and no
    /// bucket does, the message template: its hint is the bloom's alone to answer. A bucket
    /// named after the filter's own spelling of it (<c>Message</c>, <c>@mt</c>) is a CLIENT
    /// property's wherever one goes by that name, and it answered for the template: known
    /// bucket, no such value, the group dropped with the row in it. No hint key is ever empty
    /// otherwise — <see cref="TryIndexKey"/> refuses an empty property name.</para>
    /// </summary>
    public bool TryGetIndexHint(out string propertyName, out object? value)
    {
        propertyName = _hintProperty;
        value        = _hintValue;
        return _hasHint;
    }

    private static bool TryExtract(FilterNode node, int indexDepth, out string prop, out object? val)
    {
        prop = string.Empty;
        val  = null;

        switch (node)
        {
            case CompareNode { Op: CompareOp.Eq, RightProperty: null } cmp when cmp.Value is not null:
                if (IsUnfiledHeaderValue(cmp.Property, cmp.Value)
                    || !TryBloomHintKey(cmp.Property, indexDepth, out prop)) { prop = string.Empty; return false; }
                val = cmp.Value;
                return true;

            // The hint the CompareNode it replaced would have given, in the index's spelling.
            case TraceIdCompareNode { Op: CompareOp.Eq } tid:
                if (!TryBloomHintKey(tid.Property, indexDepth, out prop)) { prop = string.Empty; return false; }
                val = tid.Canonical;
                return true;

            case LevelNode lvl:
                prop = ClefFields.Level;
                val  = lvl.Level.ToSeqString();
                return true;

            case InNode inNode when inNode.Values.Length == 1 && inNode.Values[0] is { } only:
                if (IsUnfiledHeaderValue(inNode.Property, only)
                    || !TryBloomHintKey(inNode.Property, indexDepth, out prop)) { prop = string.Empty; return false; }
                val = only;
                return true;

            case AndNode and:
                if (TryExtract(and.Left, indexDepth, out prop, out val))  return true;
                if (TryExtract(and.Right, indexDepth, out prop, out val)) return true;
                return false;

            default:
                return false;
        }
    }

    // ── Header-only shape ─────────────────────────────────────────────────────

    /// <summary>
    /// True when this filter can be answered from event HEADERS alone — the level and the
    /// service name — reporting which of each it selects. Null <paramref name="levels"/>
    /// means every level; null <paramref name="service"/> means every service.
    ///
    /// <para>What it buys: a caller that only needs a COUNT can then use the header
    /// aggregator, which decodes three columns per block instead of materialising every
    /// event. That is the whole cost of the alert evaluator's most common rule — "errors
    /// in service X over the last five minutes" — repeated for every rule every fifteen
    /// seconds.</para>
    ///
    /// <para>Deliberately conservative: anything not recognised answers false and the
    /// caller falls back to the scan. A wrong "true" here would silently change what an
    /// alert fires on, so the shapes accepted are exactly the ones that can be proven
    /// equivalent — an AND-chain of level constraints and one service equality, plus
    /// OR-groups whose every leaf is a level constraint.</para>
    /// </summary>
    public bool TryGetHeaderOnlyShape(out HashSet<LogLevel>? levels, out string? service)
    {
        levels  = null;
        service = null;
        return CollectHeaderShape(_root, ref levels, ref service);
    }

    private static bool CollectHeaderShape(FilterNode node, ref HashSet<LogLevel>? levels, ref string? service)
    {
        switch (node)
        {
            case MatchAllNode:
                return true;

            case LevelNode lvl:
                IntersectLevels(ref levels, [lvl.Level]);
                return true;

            case CompareNode { Op: CompareOp.Eq, RightProperty: null } cmp when cmp.Value is string s:
                if (IsBuiltin(cmp.Property, BuiltinField.Level))
                {
                    if (!TryCanonicalLevel(s, out var parsed)) return false;
                    IntersectLevels(ref levels, [parsed]);
                    return true;
                }
                if (IsBuiltin(cmp.Property, BuiltinField.ServiceName))
                {
                    if (!IsUsableServiceLiteral(s)) return false;
                    // Two different services ANDed match nothing; the header aggregator
                    // cannot express that, so hand it back to the scan.
                    if (service is not null && !service.Equals(s, StringComparison.OrdinalIgnoreCase))
                        return false;
                    service = s;
                    return true;
                }
                return false;

            case InNode inNode when IsBuiltin(inNode.Property, BuiltinField.Level):
            {
                var set = new HashSet<LogLevel>();
                foreach (var v in inNode.Values)
                {
                    if (v is not string sv || !TryCanonicalLevel(sv, out var l)) return false;
                    set.Add(l);
                }
                IntersectLevels(ref levels, set);
                return true;
            }

            case AndNode and:
                return CollectHeaderShape(and.Left,  ref levels, ref service)
                    && CollectHeaderShape(and.Right, ref levels, ref service);

            case OrNode or:
            {
                // Only a union of LEVELS: an OR that reaches any other field (or a service)
                // is a set this shape cannot describe.
                HashSet<LogLevel>? branch  = null;
                string?            noSvc   = null;
                if (!CollectLevelUnion(or, ref branch, ref noSvc) || branch is null || noSvc is not null)
                    return false;
                IntersectLevels(ref levels, branch);
                return true;
            }

            default:
                return false;
        }
    }

    /// <summary>Collects an OR-tree whose every leaf constrains the level, as a union.</summary>
    private static bool CollectLevelUnion(FilterNode node, ref HashSet<LogLevel>? union, ref string? service)
    {
        if (node is OrNode or)
            return CollectLevelUnion(or.Left, ref union, ref service)
                && CollectLevelUnion(or.Right, ref union, ref service);

        HashSet<LogLevel>? leaf = null;
        string?            svc  = null;
        if (!CollectHeaderShape(node, ref leaf, ref svc) || leaf is null || svc is not null) return false;

        (union ??= new HashSet<LogLevel>()).UnionWith(leaf);
        return true;
    }

    /// <summary>
    /// A level literal is usable here ONLY if it is the canonical spelling the scan would
    /// match. <c>LogLevelExtensions.TryParse</c> also accepts aliases — 'Info', 'Warn' —
    /// but the evaluator compares the literal against <c>Level.ToSeqString()</c>, so those
    /// spellings match NOTHING. Reading them as their level would turn a rule that has
    /// never fired into one that counts the entire level.
    /// </summary>
    private static bool TryCanonicalLevel(string literal, out LogLevel level)
        => LogLevelExtensions.TryParse(literal.AsSpan(), out level)
        && level.ToSeqString().Equals(literal, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Service literals the header aggregator would read as something other than "this
    /// exact service": the empty string, which it treats as NO filter (counting every
    /// service), and its own display name for events that carry no service, which the
    /// scan can never match because it compares against a null.
    /// </summary>
    private static bool IsUsableServiceLiteral(string literal)
        => literal.Length > 0
        && !literal.Equals("(unknown)", StringComparison.OrdinalIgnoreCase);

    private static void IntersectLevels(ref HashSet<LogLevel>? levels, IReadOnlyCollection<LogLevel> with)
    {
        if (levels is null) { levels = new HashSet<LogLevel>(with); return; }
        levels.IntersectWith(with);
    }

    private static bool IsBuiltin(string property, BuiltinField expected) =>
        BuiltinFields.TryResolve(property, out var f) && f == expected;

    // ── Filter name → index key ───────────────────────────────────────────────

    /// <summary>
    /// The deepest index a hint ever relies on: <c>SegmentIndexBuilder</c>'s default flatten
    /// depth, five map levels below the top. A user property path is hinted only while it has at
    /// most that many levels below its first segment (six segments, array subscripts not
    /// counted), and free text — which <see cref="FilterEvaluator"/> searches exactly this deep —
    /// relies on all of them. Filters are compiled for an index this deep; a server configured to
    /// flatten less asks through <see cref="ForIndexDepth"/>, and one configured to flatten more
    /// holds more than any hint asks for.
    /// </summary>
    internal const int FullIndexDepth = IndexingOptions.DefaultMaxPropertyFlattenDepth;

    /// <summary>
    /// Translates a filter-side property name into the inverted-index bucket that holds it,
    /// or returns false when the index holds nothing for it.
    ///
    /// <para>This is the one place a hint key is decided. Built-ins go through
    /// <see cref="BuiltinFields"/>, the same table the evaluator resolves aliases with, so
    /// <c>@x</c>, <c>Exception</c>, <c>@x.type</c> and <c>Exception.Type</c> all land on the
    /// bucket the builder wrote (<c>@x.type</c>) instead of on their own spelling. User
    /// property paths already share the evaluator's separator with the index, and only need
    /// their array subscripts removed — and must lie within the depth the index was flattened
    /// to (<see cref="TryFlattenedKey"/>).</para>
    ///
    /// <para>Returning false means "emit no hint": the segment gets scanned, which is
    /// slower and correct. Emitting a key the index does not have is neither.</para>
    /// </summary>
    private static bool TryIndexKey(string property, int indexDepth, out string indexKey)
    {
        if (BuiltinFields.TryResolve(property, out var field))
        {
            string? key = BuiltinFields.InvertedKey(field);
            indexKey    = key ?? string.Empty;
            return key is not null;
        }

        return TryFlattenedKey(property, indexDepth, out indexKey);
    }

    /// <summary>
    /// Same mapping for the bloom gate, which is keyless: what matters is whether the
    /// field's VALUE was ever handed to the filter. The returned key is only used for the
    /// inverted <c>MightContain</c> that follows, and is EMPTY for the field the bloom holds and
    /// no bucket does — the message template (see <see cref="TryGetIndexHint"/>).
    /// </summary>
    private static bool TryBloomHintKey(string property, int indexDepth, out string key)
    {
        if (BuiltinFields.TryResolve(property, out var field))
        {
            if (!BuiltinFields.IsBloomIndexed(field)) { key = string.Empty; return false; }
            key = BuiltinFields.InvertedKey(field) ?? string.Empty;
            return true;
        }

        // A user property's value is bloomed exactly when the property is indexed at all.
        return TryIndexKey(property, indexDepth, out key);
    }

    /// <summary>
    /// The bucket a USER property path is filed under, when an index flattened to
    /// <paramref name="indexDepth"/> files it at all: the path without its array subscripts —
    /// <c>SegmentIndexBuilder</c> files every element of an array under the bare key, and an
    /// array adds no level — with at most <paramref name="indexDepth"/> map levels below its
    /// first segment, the deepest the builder walks down to. Deeper, the builder skips the map
    /// unread: no bucket, no bloom text, no trigram holds its values. Also asked by the
    /// substring hints (<see cref="IsTrigramCovered"/>), which rely on the same walk.
    /// </summary>
    private static bool TryFlattenedKey(string property, int indexDepth, out string key)
    {
        key = PropertyPath.StripIndexSegments(property);
        if (key.Length == 0 || PropertyPath.SegmentCount(key) - 1 > indexDepth)
        {
            key = string.Empty;
            return false;
        }
        return true;
    }

    /// <summary>
    /// True for the one literal no header field is ever filed under: the empty string, on a
    /// built-in field. <c>SegmentIndexBuilder</c> files the template, the service and the
    /// exception types only when they are not empty, and a level, a trace id or a span id never
    /// is — so no header bucket holds the empty value, and the bloom holds the empty text only
    /// where some PROPERTY in the group happened to give it (a nil, an empty string). The scan,
    /// meanwhile, matches <c>@mt = ''</c> against an event that has no template — the filter the
    /// client's "find all like this" builds for one — and <c>@x.type = ''</c> and
    /// <c>@x.inner.type = ''</c> against an exception whose type is empty. So such a predicate
    /// gives no hint and is answered by reading the rows; another conjunct of the filter still
    /// narrows them. A user property's empty string IS filed, and keeps its hint.
    ///
    /// <para>The rule is the builder's, not the scan's, so it covers every built-in alike —
    /// including those no event can match with <c>''</c> (an event logged without a service
    /// carries none, not an empty one): those read their rows to find nothing, which costs
    /// time on a filter nobody means, where a rule per field would cost rows the day an ingest
    /// path starts handing one of them an empty value.</para>
    /// </summary>
    private static bool IsUnfiledHeaderValue(string property, object value) =>
        value is string { Length: 0 } && BuiltinFields.TryResolve(property, out _);

    /// <summary>
    /// Returns text tokens suitable for trigram index lookup (from Contains / Like nodes).
    /// Pre-computed once at <see cref="Compile"/> time — zero allocation per query.
    /// </summary>
    public IReadOnlyList<(string property, string text)> GetTrigramHints() => _trigramHints;

    /// <summary>
    /// Returns all equality predicates from the AND-chain of the filter.
    /// Used by QueryExecutor to call <see cref="ISegmentIndex.LookupIntersect"/> for
    /// event-level offset narrowing inside a segment (not just segment-level skip).
    /// </summary>
    public IReadOnlyList<(string property, object? value)> GetInvertedHints() => _invertedHints;

    private static IReadOnlyList<(string, object?)> BuildInvertedHints(FilterNode root, int indexDepth)
    {
        var list = new List<(string, object?)>(4);
        CollectInvertedHints(root, indexDepth, list);
        return list.Count == 0 ? Array.Empty<(string, object?)>() : list;
    }

    private static void CollectInvertedHints(FilterNode node, int indexDepth, List<(string, object?)> out_)
    {
        // Asked first, because the node kinds it reads are ones the switch below gives no hint
        // for (has / isDefined) or the wrong one (`@x.exists = …`, whose field has no value
        // bucket to hint).
        if (SelectsEventsWithAnException(node))
        {
            out_.Add(ExceptionPresentHint);
            return;
        }

        switch (node)
        {
            // `X = null` is deliberately not hinted: the evaluator treats a missing property
            // as null and matches, but the index only files events that carried an explicit
            // nil, so the posting list is a subset of the matches. `A = B` is not hinted
            // either — its right side is read off the event, so there is no value to look up.
            // Nor is a header field's empty string, which no bucket holds (IsUnfiledHeaderValue).
            case CompareNode { Op: CompareOp.Eq, RightProperty: null } cmp when cmp.Value is not null:
                if (!IsUnfiledHeaderValue(cmp.Property, cmp.Value) && TryIndexKey(cmp.Property, indexDepth, out string cmpKey))
                    out_.Add((cmpKey, cmp.Value));
                break;
            case TraceIdCompareNode { Op: CompareOp.Eq } tid:
                if (TryIndexKey(tid.Property, indexDepth, out string tidKey)) out_.Add((tidKey, tid.Canonical));
                break;
            case LevelNode lvl:
                out_.Add((ClefFields.Level, lvl.Level.ToSeqString()));
                break;
            case InNode inNode when inNode.Values.Length == 1 && inNode.Values[0] is { } only:
                if (!IsUnfiledHeaderValue(inNode.Property, only) && TryIndexKey(inNode.Property, indexDepth, out string inKey))
                    out_.Add((inKey, only));
                break;
            case AndNode and:
                CollectInvertedHints(and.Left,  indexDepth, out_);
                CollectInvertedHints(and.Right, indexDepth, out_);
                break;
            // Deliberately skip OrNode — OR is too broad for intersection
        }
    }

    // ── Exception presence: the one question the marker bucket can answer ─────

    /// <summary>
    /// "Which rows carry an exception", as an inverted hint: the bucket
    /// <c>SegmentIndexBuilder</c> files every such row under, and the one value it files there.
    ///
    /// <para>This is NOT the field <c>@x.exists</c> being hinted by value — that field has no
    /// value bucket (see its row in <see cref="BuiltinFields"/>): the marker holds "true" and
    /// nothing else, so asking it for <c>false</c> would be answered "no such row" about a
    /// group full of them. It is the single positive question, emitted only for a predicate
    /// that <see cref="SelectsEventsWithAnException"/> proves is that question. And it is
    /// honoured only by an index group whose builder vouched that the marker is complete
    /// (<c>SegmentInvertedIndex.IsExceptionMarker</c>): there the list is the answer and its
    /// absence drops the group; an older group says "no information" and is scanned.</para>
    /// </summary>
    internal static readonly (string Property, object? Value) ExceptionPresentHint = (ClefFields.ExceptionExists, "true");

    /// <summary>
    /// True when <see cref="ExceptionPresentHint"/> is among <see cref="GetInvertedHints"/>: the
    /// filter cannot match an event that carries no exception.
    ///
    /// <para>The narrowing treats that one hint apart from the rest
    /// (<c>QueryExecutor.TryNarrowWithIndex</c>): its posting list is counted before it is
    /// built, and where it names nearly the whole of a group it is not built at all — the other
    /// hints then narrow without it (<see cref="GetInvertedHintsBesideExceptionPresence"/>).
    /// Both are resolved here, once per filter, because the narrowing asks once per group.</para>
    /// </summary>
    public bool HintsExceptionPresence => !ReferenceEquals(_invertedHintsBesideExceptionPresence, _invertedHints);

    /// <summary>
    /// <see cref="GetInvertedHints"/> without the exception marker's hint; the same list when
    /// that hint is not in it.
    /// </summary>
    public IReadOnlyList<(string property, object? value)> GetInvertedHintsBesideExceptionPresence() =>
        _invertedHintsBesideExceptionPresence;

    private static IReadOnlyList<(string, object?)> BesideExceptionPresence(
        IReadOnlyList<(string property, object? value)> hints)
    {
        List<(string, object?)>? rest = null;
        for (int i = 0; i < hints.Count; i++)
        {
            if (IsExceptionPresentHint(hints[i]))
            {
                if (rest is null)
                {
                    rest = new List<(string, object?)>(hints.Count);
                    for (int j = 0; j < i; j++) rest.Add(hints[j]);
                }
            }
            else rest?.Add(hints[i]);
        }
        if (rest is null) return hints;
        return rest.Count == 0 ? Array.Empty<(string, object?)>() : rest;
    }

    /// <summary>
    /// The hint itself, key AND value — not merely a hint under the marker's name. Today no other
    /// is ever filed there (the field of that name has no value bucket; see
    /// <see cref="CollectInvertedHints"/>), but <see cref="HintsExceptionPresence"/> is what lets
    /// the narrowing DROP a group whose marker lists no row, and that is only right for a filter
    /// that cannot match without an exception. A hint for the marker's other answer, should one
    /// ever be added, must not be taken for this one.
    /// </summary>
    private static bool IsExceptionPresentHint((string property, object? value) hint) =>
        string.Equals(hint.property, ExceptionPresentHint.Property, StringComparison.Ordinal) &&
        hint.value is string text &&
        string.Equals(text, (string)ExceptionPresentHint.Value!, StringComparison.Ordinal);

    /// <summary>
    /// True when <paramref name="node"/> matches an event if and only if the event carries an
    /// exception — the predicates the scan answers from <c>LogEvent.HasException</c>, which is
    /// exactly what the index's marker records.
    ///
    /// <para><c>has(@x)</c> and <c>isDefined(@x)</c> under every spelling
    /// <c>FilterEvaluator.HasProperty</c> answers that way, and <c>@x.exists</c> compared with a
    /// literal that leaves only the events WITH an exception: <c>= true</c>, <c>= 'TRUE'</c>,
    /// <c>!= false</c>, <c>in [true]</c>. Which literals those are is not decided here. The
    /// evaluator's own comparison is run on both values the field can take, so this cannot come
    /// to disagree with the scan about what <c>=</c> means for a bool — the way
    /// <see cref="Filtering.HeaderPredicate"/> computes a level mask. Anything that also admits
    /// an event without an exception (<c>= false</c>, <c>!= true</c>, <c>!= null</c>) or admits
    /// none is not this question and gets no hint.</para>
    /// </summary>
    private static bool SelectsEventsWithAnException(FilterNode node)
    {
        switch (node)
        {
            case HasNode has:
                return IsBuiltin(has.Property, BuiltinField.ExceptionType);

            case IsDefinedNode defined:
                return IsBuiltin(defined.Property, BuiltinField.ExceptionType);

            case CompareNode { RightProperty: null } cmp when IsBuiltin(cmp.Property, BuiltinField.ExceptionExists):
                return FilterEvaluator.CompareValues(true, cmp.Value, cmp.Op)
                   && !FilterEvaluator.CompareValues(false, cmp.Value, cmp.Op);

            case InNode inNode when IsBuiltin(inNode.Property, BuiltinField.ExceptionExists):
                return FilterEvaluator.InValues(true, inNode.Values)
                   && !FilterEvaluator.InValues(false, inNode.Values);

            default:
                return false;
        }
    }

    private static IReadOnlyList<(string, string)> BuildTrigramHints(FilterNode root, int indexDepth)
    {
        var list = new List<(string, string)>(4);
        CollectTrigram(root, indexDepth, list);
        return list.Count == 0 ? Array.Empty<(string, string)>() : list;
    }

    /// <summary>
    /// Whether <c>SegmentIndexBuilder</c> hands this filter-side property's text to the
    /// trigram index — the <see cref="TryIndexKey"/> question, asked of the other index.
    ///
    /// <para>It has to be asked separately and it has to be asked at all:
    /// <c>SegmentTrigramIndex.Lookup</c> returns an empty array for a trigram it does not
    /// hold, and <c>QueryExecutor</c> reads that as proof and drops the segment. So a
    /// <c>contains</c> on <c>@tr</c>, <c>@sp</c>, <c>@l</c>, <c>@id</c>, <c>@service</c>
    /// or the exception stack — none of which is ever trigrammed — returned rows while the
    /// events were hot and zero the moment the segment flushed. Same shape as the seed
    /// defect, different index.</para>
    ///
    /// <para>User properties are covered as far down as the builder flattens them, which is
    /// as far as it trigrams them: every scalar it walks to, and nothing below. The trigram
    /// index is global text, so a hint on a deeper path — <c>contains(a.b.c.d.e.f.g, …)</c> on a
    /// seven-level value — looked up text no group holds and dropped every one, while
    /// <see cref="TryIndexKey"/> had capped the equality on the same path all along.</para>
    /// </summary>
    private static bool IsTrigramCovered(string property, int indexDepth) =>
        BuiltinFields.TryResolve(property, out var field)
            ? BuiltinFields.IsTrigramIndexed(field)
            : TryFlattenedKey(property, indexDepth, out _);

    /// <summary>
    /// Both SQL wildcards, so a LIKE pattern is cut into the literal runs the event text
    /// really contains. Splitting on <c>%</c> alone handed <c>_</c> to the trigram index as
    /// if it were a character of the value: <c>Region like 'ae_dxb'</c> looked up the
    /// trigrams of the PATTERN, found none, and dropped the segment — while the scan's
    /// <c>LikeMatchFast</c> happily matched the stored <c>ae-dxb</c>.
    /// </summary>
    private static readonly char[] LikeWildcards = ['%', '_'];

    private static void CollectTrigram(FilterNode node, int indexDepth, List<(string, string)> out_)
    {
        switch (node)
        {
            case ContainsNode ct when ct.Text.Length >= 3 && IsTrigramCovered(ct.Property, indexDepth):
                out_.Add((ct.Property, ct.Text));
                break;

            case StartsWithNode sw when sw.Prefix.Length >= 3 && IsTrigramCovered(sw.Property, indexDepth):
                out_.Add((sw.Property, sw.Prefix));
                break;

            case LikeNode like when IsTrigramCovered(like.Property, indexDepth):
                // Extract every non-wildcard run long enough for the trigram index
                foreach (var p in like.Pattern.Split(LikeWildcards))
                    if (p.Length >= 3) out_.Add((like.Property, p));
                break;

            // Each 3+ char term narrows candidate blocks via the trigram index (property is
            // ignored by LookupTrigram — the index is global text). Only from an index that
            // trigrammed every level free text searches: FilterEvaluator matches a term in a
            // string FullIndexDepth levels down, and an index flattened less never held it.
            case FreeTextNode ft when indexDepth >= FilterEvaluator.MaxFlattenDepth:
                foreach (var term in ft.Terms)
                    if (term.Length >= 3) out_.Add((string.Empty, term));
                break;

            case AndNode and:
                CollectTrigram(and.Left,  indexDepth, out_);
                CollectTrigram(and.Right, indexDepth, out_);
                break;

            // Deliberately skip OrNode — the consumer intersects every hint as a
            // conjunct (TryNarrowWithIndex), so hints gathered across OR branches
            // narrowed `contains(a) or contains(b)` to rows containing BOTH, and an
            // OR branch whose term was absent from a group dropped the whole group.
            // Same rule, same reason as CollectInvertedHints above.
        }
    }
}
