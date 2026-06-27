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

    private CompiledFilter(FilterNode root)
    {
        _root          = root;
        _trigramHints  = BuildTrigramHints(root);
        _invertedHints = BuildInvertedHints(root);
    }

    public static CompiledFilter Compile(string? expression) =>
        new(FilterParser.Parse(expression));

    public bool IsMatchAll => _root is MatchAllNode;

    public bool Matches(LogEvent ev) => FilterEvaluator.Matches(_root, ev);

    // ── Index hints ───────────────────────────────────────────────────────────

    /// <summary>
    /// Walks the AST to extract the first equality constraint that can be used
    /// against the inverted index.
    ///
    /// Returns true and sets <paramref name="propertyName"/>/<paramref name="value"/>
    /// if a usable hint is found.
    /// </summary>
    public bool TryGetIndexHint(out string propertyName, out object? value)
    {
        return TryExtract(_root, out propertyName, out value);
    }

    private static bool TryExtract(FilterNode node, out string prop, out object? val)
    {
        prop = string.Empty;
        val  = null;

        switch (node)
        {
            case CompareNode { Op: CompareOp.Eq } cmp:
                prop = cmp.Property;
                val  = cmp.Value;
                return true;

            case LevelNode lvl:
                prop = "@l";
                val  = lvl.Level.ToSeqString();
                return true;

            case InNode inNode when inNode.Values.Length == 1:
                prop = inNode.Property;
                val  = inNode.Values[0];
                return true;

            case AndNode and:
                if (TryExtract(and.Left, out prop, out val))  return true;
                if (TryExtract(and.Right, out prop, out val)) return true;
                return false;

            default:
                return false;
        }
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
            case CompareNode { Op: CompareOp.Eq } cmp:
                out_.Add((cmp.Property, cmp.Value));
                break;
            case LevelNode lvl:
                out_.Add(("@l", lvl.Level.ToSeqString()));
                break;
            case InNode inNode when inNode.Values.Length == 1:
                out_.Add((inNode.Property, inNode.Values[0]));
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

    private static void CollectTrigram(FilterNode node, List<(string, string)> out_)
    {
        switch (node)
        {
            case ContainsNode ct when ct.Text.Length >= 3:
                out_.Add((ct.Property, ct.Text));
                break;

            case StartsWithNode sw when sw.Prefix.Length >= 3:
                out_.Add((sw.Property, sw.Prefix));
                break;

            case LikeNode like:
                // Extract every non-wildcard segment long enough for the trigram index
                foreach (var p in like.Pattern.Split('%'))
                    if (p.Length >= 3) out_.Add((like.Property, p));
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
