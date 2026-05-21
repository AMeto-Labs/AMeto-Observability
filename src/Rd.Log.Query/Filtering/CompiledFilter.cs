using Rd.Log.Core;

namespace Rd.Log.Query.Filtering;

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

    private CompiledFilter(FilterNode root) => _root = root;

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
    /// </summary>
    public IEnumerable<(string property, string text)> GetTrigramHints()
    {
        foreach (var hint in WalkTrigram(_root))
            yield return hint;
    }

    private static IEnumerable<(string, string)> WalkTrigram(FilterNode node)
    {
        switch (node)
        {
            case ContainsNode ct when ct.Text.Length >= 3:
                yield return (ct.Property, ct.Text);
                break;

            case StartsWithNode sw when sw.Prefix.Length >= 3:
                yield return (sw.Property, sw.Prefix);
                break;

            case LikeNode like:
                // Extract longest non-wildcard segment
                var parts = like.Pattern.Split('%');
                foreach (var p in parts)
                    if (p.Length >= 3) yield return (like.Property, p);
                break;

            case AndNode and:
                foreach (var h in WalkTrigram(and.Left))  yield return h;
                foreach (var h in WalkTrigram(and.Right)) yield return h;
                break;

            case OrNode or:
                foreach (var h in WalkTrigram(or.Left))  yield return h;
                foreach (var h in WalkTrigram(or.Right)) yield return h;
                break;
        }
    }
}
