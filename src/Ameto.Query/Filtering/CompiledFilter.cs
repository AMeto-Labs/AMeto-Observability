using Ameto.Core;

namespace Ameto.Query.Filtering;

/// <summary>
/// A parsed, reusable filter with helper methods for index acceleration.
///
/// Index hints: the query executor uses <see cref="TryGetIndexHint"/> to skip entire
/// segments without decompressing blocks when the segment index says there are no
/// matching events.
/// </summary>
public sealed class CompiledFilter
{
    private readonly FilterNode _root;

    // Pre-computed at compile time so QueryExecutor pays zero allocation per query.
    private readonly IReadOnlyList<(string property, string text)>    _trigramHints;
    private readonly IReadOnlyList<(string property, object? value)>  _invertedHints;

    // The single bloom/MightContain hint, also resolved once at compile time — the
    // prefilter asks for it inside a per-segment parallel loop.
    private readonly bool    _hasHint;
    private readonly string  _hintProperty;
    private readonly object? _hintValue;

    private CompiledFilter(FilterNode root)
    {
        root           = RewriteTimeCompares(root);
        _root          = root;
        _trigramHints  = BuildTrigramHints(root);
        _invertedHints = BuildInvertedHints(root);
        _hasHint       = TryExtract(root, out _hintProperty, out _hintValue);

        long? min = null, max = null;
        CollectTimeBounds(root, ref min, ref max);
        MinTimestampTicks = min;
        MaxTimestampTicks = max;
    }

    public static CompiledFilter Compile(string? expression) =>
        new(FilterParser.Parse(expression));

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
    /// value it was never given and the whole segment would be dropped.</para>
    /// </summary>
    public bool TryGetIndexHint(out string propertyName, out object? value)
    {
        propertyName = _hintProperty;
        value        = _hintValue;
        return _hasHint;
    }

    private static bool TryExtract(FilterNode node, out string prop, out object? val)
    {
        prop = string.Empty;
        val  = null;

        switch (node)
        {
            case CompareNode { Op: CompareOp.Eq, RightProperty: null } cmp when cmp.Value is not null:
                if (!TryBloomHintKey(cmp.Property, out prop)) { prop = string.Empty; return false; }
                val = cmp.Value;
                return true;

            case LevelNode lvl:
                prop = ClefFields.Level;
                val  = lvl.Level.ToSeqString();
                return true;

            case InNode inNode when inNode.Values.Length == 1 && inNode.Values[0] is not null:
                if (!TryBloomHintKey(inNode.Property, out prop)) { prop = string.Empty; return false; }
                val = inNode.Values[0];
                return true;

            case AndNode and:
                if (TryExtract(and.Left, out prop, out val))  return true;
                if (TryExtract(and.Right, out prop, out val)) return true;
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
                    if (!LogLevelExtensions.TryParse(s.AsSpan(), out var parsed)) return false;
                    IntersectLevels(ref levels, [parsed]);
                    return true;
                }
                if (IsBuiltin(cmp.Property, BuiltinField.ServiceName))
                {
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
                    if (v is not string sv || !LogLevelExtensions.TryParse(sv.AsSpan(), out var l)) return false;
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

    private static void IntersectLevels(ref HashSet<LogLevel>? levels, IReadOnlyCollection<LogLevel> with)
    {
        if (levels is null) { levels = new HashSet<LogLevel>(with); return; }
        levels.IntersectWith(with);
    }

    private static bool IsBuiltin(string property, BuiltinField expected) =>
        BuiltinFields.TryResolve(property, out var f) && f == expected;

    // ── Filter name → index key ───────────────────────────────────────────────

    /// <summary>
    /// Deepest property path the segment index flattens (mirrors
    /// <c>ServerOptions.MaxPropertyFlattenDepth</c> and
    /// <c>FilterEvaluator.MaxFlattenDepth</c>): a flat key may carry at most this many
    /// separators, so a path with more segments than that reaches no bucket.
    /// </summary>
    private const int MaxIndexedPathSegments = 6;

    /// <summary>
    /// Translates a filter-side property name into the inverted-index bucket that holds it,
    /// or returns false when the index holds nothing for it.
    ///
    /// <para>This is the one place a hint key is decided. Built-ins go through
    /// <see cref="BuiltinFields"/>, the same table the evaluator resolves aliases with, so
    /// <c>@x</c>, <c>Exception</c>, <c>@x.type</c> and <c>Exception.Type</c> all land on the
    /// bucket the builder wrote (<c>@x.type</c>) instead of on their own spelling. User
    /// property paths already share the evaluator's separator with the index, and only need
    /// their array subscripts removed.</para>
    ///
    /// <para>Returning false means "emit no hint": the segment gets scanned, which is
    /// slower and correct. Emitting a key the index does not have is neither.</para>
    /// </summary>
    private static bool TryIndexKey(string property, out string indexKey)
    {
        if (BuiltinFields.TryResolve(property, out var field))
        {
            string? key = BuiltinFields.InvertedKey(field);
            indexKey    = key ?? string.Empty;
            return key is not null;
        }

        indexKey = PropertyPath.StripIndexSegments(property);
        if (indexKey.Length == 0 || PropertyPath.SegmentCount(indexKey) > MaxIndexedPathSegments)
        {
            indexKey = string.Empty;
            return false;
        }
        return true;
    }

    /// <summary>
    /// Same mapping for the bloom gate, which is keyless: what matters is whether the
    /// field's VALUE was ever handed to the filter. The returned key is only used for the
    /// inverted <c>MightContain</c> that follows.
    /// </summary>
    private static bool TryBloomHintKey(string property, out string key)
    {
        if (BuiltinFields.TryResolve(property, out var field))
        {
            if (!BuiltinFields.IsBloomIndexed(field)) { key = string.Empty; return false; }
            key = BuiltinFields.InvertedKey(field) ?? property;
            return true;
        }

        // A user property's value is bloomed exactly when the property is indexed at all.
        return TryIndexKey(property, out key);
    }

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

    private static IReadOnlyList<(string, object?)> BuildInvertedHints(FilterNode root)
    {
        var list = new List<(string, object?)>(4);
        CollectInvertedHints(root, list);
        return list.Count == 0 ? Array.Empty<(string, object?)>() : list;
    }

    private static void CollectInvertedHints(FilterNode node, List<(string, object?)> out_)
    {
        switch (node)
        {
            // `X = null` is deliberately not hinted: the evaluator treats a missing property
            // as null and matches, but the index only files events that carried an explicit
            // nil, so the posting list is a subset of the matches. `A = B` is not hinted
            // either — its right side is read off the event, so there is no value to look up.
            case CompareNode { Op: CompareOp.Eq, RightProperty: null } cmp when cmp.Value is not null:
                if (TryIndexKey(cmp.Property, out string cmpKey)) out_.Add((cmpKey, cmp.Value));
                break;
            case LevelNode lvl:
                out_.Add((ClefFields.Level, lvl.Level.ToSeqString()));
                break;
            case InNode inNode when inNode.Values.Length == 1 && inNode.Values[0] is not null:
                if (TryIndexKey(inNode.Property, out string inKey)) out_.Add((inKey, inNode.Values[0]));
                break;
            case AndNode and:
                CollectInvertedHints(and.Left,  out_);
                CollectInvertedHints(and.Right, out_);
                break;
            // Deliberately skip OrNode — OR is too broad for intersection
        }
    }

    private static IReadOnlyList<(string, string)> BuildTrigramHints(FilterNode root)
    {
        var list = new List<(string, string)>(4);
        CollectTrigram(root, list);
        return list.Count == 0 ? Array.Empty<(string, string)>() : list;
    }

    /// <summary>
    /// Whether <c>SegmentIndexBuilder</c> hands this filter-side property's text to the
    /// trigram index — the <see cref="TryIndexKey"/> question, asked of the other index.
    ///
    /// <para>It has to be asked separately and it has to be asked at all:
    /// <c>SegmentTrigramIndex.Lookup</c> returns an empty array for a trigram it does not
    /// hold, and <c>QueryExecutor</c> reads that as proof and drops the segment. So a
    /// <c>contains</c> on <c>@tr</c>, <c>@sp</c>, <c>@l</c>, <c>@id</c>, <c>service.name</c>
    /// or the exception stack — none of which is ever trigrammed — returned rows while the
    /// events were hot and zero the moment the segment flushed. Same shape as the seed
    /// defect, different index.</para>
    ///
    /// <para>User properties are covered: the builder trigrams every scalar it flattens.</para>
    /// </summary>
    private static bool IsTrigramCovered(string property) =>
        !BuiltinFields.TryResolve(property, out var field) || BuiltinFields.IsTrigramIndexed(field);

    /// <summary>
    /// Both SQL wildcards, so a LIKE pattern is cut into the literal runs the event text
    /// really contains. Splitting on <c>%</c> alone handed <c>_</c> to the trigram index as
    /// if it were a character of the value: <c>Region like 'ae_dxb'</c> looked up the
    /// trigrams of the PATTERN, found none, and dropped the segment — while the scan's
    /// <c>LikeMatchFast</c> happily matched the stored <c>ae-dxb</c>.
    /// </summary>
    private static readonly char[] LikeWildcards = ['%', '_'];

    private static void CollectTrigram(FilterNode node, List<(string, string)> out_)
    {
        switch (node)
        {
            case ContainsNode ct when ct.Text.Length >= 3 && IsTrigramCovered(ct.Property):
                out_.Add((ct.Property, ct.Text));
                break;

            case StartsWithNode sw when sw.Prefix.Length >= 3 && IsTrigramCovered(sw.Property):
                out_.Add((sw.Property, sw.Prefix));
                break;

            case LikeNode like when IsTrigramCovered(like.Property):
                // Extract every non-wildcard run long enough for the trigram index
                foreach (var p in like.Pattern.Split(LikeWildcards))
                    if (p.Length >= 3) out_.Add((like.Property, p));
                break;

            case FreeTextNode ft:
                // Each 3+ char term narrows candidate blocks via the trigram index
                // (property is ignored by LookupTrigram — the index is global text).
                foreach (var term in ft.Terms)
                    if (term.Length >= 3) out_.Add((string.Empty, term));
                break;

            case AndNode and:
                CollectTrigram(and.Left,  out_);
                CollectTrigram(and.Right, out_);
                break;

            // Deliberately skip OrNode — the consumer intersects every hint as a
            // conjunct (TryNarrowWithIndex), so hints gathered across OR branches
            // narrowed `contains(a) or contains(b)` to rows containing BOTH, and an
            // OR branch whose term was absent from a group dropped the whole group.
            // Same rule, same reason as CollectInvertedHints above.
        }
    }
}
