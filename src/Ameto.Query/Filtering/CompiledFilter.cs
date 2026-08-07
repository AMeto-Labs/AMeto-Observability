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
        _root          = root;
        _trigramHints  = BuildTrigramHints(root);
        _invertedHints = BuildInvertedHints(root);
        _hasHint       = TryExtract(root, out _hintProperty, out _hintValue);
    }

    public static CompiledFilter Compile(string? expression) =>
        new(FilterParser.Parse(expression));

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
            case CompareNode { Op: CompareOp.Eq } cmp when cmp.Value is not null:
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
            // nil, so the posting list is a subset of the matches.
            case CompareNode { Op: CompareOp.Eq } cmp when cmp.Value is not null:
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

            case OrNode or:
                CollectTrigram(or.Left,  out_);
                CollectTrigram(or.Right, out_);
                break;
        }
    }
}
