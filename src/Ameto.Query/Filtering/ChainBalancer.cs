using System.Numerics;

namespace Ameto.Query.Filtering;

/// <summary>
/// Keeps the tree of a compiled filter shallow, so that nothing which walks it recurses once
/// per clause.
///
/// <para>The parser reads <c>a and b and c and …</c> with a loop and builds it left-deep:
/// n clauses, n − 1 levels. Everything downstream descends that tree by recursion — the
/// <c>@t</c> rewrite, the hint builders, the header predicate, and
/// <see cref="FilterEvaluator.Matches"/> for every candidate event — so the length of a chain
/// was the depth of the stack, and the length of a chain is whatever the text says.
/// <see cref="FilterParser.MaxNestingDepth"/> does not bound it: a chain is not nesting.</para>
///
/// <para>Here a long chain is re-associated into a balanced tree — n clauses, ⌈log₂ n⌉ levels —
/// which is the same filter. <c>and</c> and <c>or</c> are associative, the clauses keep their
/// order, and every walker reads them left to right, so each one sees the same clauses in the
/// same sequence and the evaluator short-circuits at the same clause it did before. A chain
/// of <see cref="LongChain"/> clauses or fewer is left exactly as the parser built it: that
/// is nearly every filter anyone writes, and they compile to the tree they always did.</para>
///
/// <para>Only <see cref="CompiledFilter"/> balances. <see cref="FilterParser.Parse"/> still
/// answers with the left-deep tree: that tree is public and its shape can be observed — a
/// caller may be comparing two parses node for node — so it is left as it was.</para>
///
/// <para><see cref="MaxDepth"/> is the backstop, and what makes the bound unconditional:
/// whatever the text, a tree that comes out of here is no deeper than that, or the filter is
/// refused.</para>
/// </summary>
internal static class ChainBalancer
{
    /// <summary>
    /// The deepest tree a compiled filter may hold, counted in levels below the root.
    ///
    /// <para>Not a limit anyone meets by writing a filter: a chain of ten thousand clauses is
    /// fourteen levels deep once balanced, and a hundred groups each joined to the next by one
    /// operator are a hundred. Reaching it takes a dozen or more groups one inside another, the
    /// operators alternating on the way down, with a chain of both at every level: nineteen
    /// groups if the chains are the longest left unbalanced and each opens with the next group,
    /// fewer if they are long enough to be balanced. The costliest walker measured takes about
    /// 650 bytes of stack per level in a Debug build, so this is some 170 KB at the very worst.</para>
    /// </summary>
    internal const int MaxDepth = 256;

    /// <summary>
    /// A chain of more clauses than this is balanced; a shorter one keeps the parser's shape.
    /// </summary>
    internal const int LongChain = 8;

    /// <summary>
    /// The same filter as <paramref name="root"/> with its long chains balanced. Returns
    /// <paramref name="root"/> itself, allocating nothing, when there is no long chain in it.
    /// </summary>
    /// <exception cref="FormatException">The tree is deeper than <see cref="MaxDepth"/> even so.</exception>
    public static FilterNode Balance(FilterNode root) => Visit(root, level: 0);

    private static FilterNode Visit(FilterNode node, int level)
    {
        // Checked on the way DOWN, so this walk is bounded by the limit it enforces: it gives
        // up at the first node past it rather than after having followed the tree to the bottom.
        // No position in the sentence: a node does not carry one, and the levels counted are
        // those of the tree as it will be walked, which is not something to point at in the text.
        if (level > MaxDepth)
            throw new FormatException(
                $"The filter is nested too deeply — its groups, with the 'and' / 'or' chains inside them, " +
                $"stack more than {MaxDepth} levels deep. Use fewer groups inside one another.");

        switch (node)
        {
            case NotNode not:
            {
                var operand = Visit(not.Operand, level + 1);
                return ReferenceEquals(operand, not.Operand) ? not : new NotNode(operand);
            }

            case AndNode or OrNode:
            {
                bool isAnd = node is AndNode;
                int  count = CountClauses(node, isAnd);
                if (count > LongChain)
                    return BalanceChain(node, isAnd, count, level);

                Children(node, out var left, out var right);
                var l = Visit(left,  level + 1);
                var r = Visit(right, level + 1);
                return ReferenceEquals(l, left) && ReferenceEquals(r, right) ? node : Join(isAnd, l, r);
            }

            default:
                return node;
        }
    }

    /// <summary>
    /// Clauses on the left spine of <paramref name="chain"/> — the way the parser builds one.
    /// A loop, like everything here that follows a spine: the spine is as long as the chain.
    ///
    /// <para>Parentheses leave nothing in the tree, so a group of the same operator that OPENS
    /// a chain — <c>(a or b) or c</c> — is that chain's own spine and is counted with it. One
    /// on the RIGHT (<c>a and (b and c)</c>) is a single clause here and a chain of its own
    /// below; it took a parenthesis to write, and parentheses are bounded where they are parsed.</para>
    /// </summary>
    private static int CountClauses(FilterNode chain, bool isAnd)
    {
        int count = 2;
        Children(chain, out var left, out _);
        while (isAnd ? left is AndNode : left is OrNode)
        {
            count++;
            Children(left, out left, out _);
        }
        return count;
    }

    private static FilterNode BalanceChain(FilterNode chain, bool isAnd, int count, int level)
    {
        // The clauses, in the order they were written.
        var clauses = new FilterNode[count];
        var at = chain;
        for (int i = count - 1; i >= 1; i--)
        {
            Children(at, out var left, out clauses[i]);
            at = left;
        }
        clauses[0] = at;

        // Build() puts every clause at most ⌈log₂ count⌉ levels under the chain's root. Some
        // sit one level higher than that; counting them all at the deepest keeps the depth
        // check a ceiling, which is the side to err on.
        int below = level + BitOperations.Log2((uint)(count - 1)) + 1;
        for (int i = 0; i < count; i++)
            clauses[i] = Visit(clauses[i], below);

        return Build(clauses, 0, count, isAnd);
    }

    /// <summary>
    /// The balanced tree over <c>clauses[from..to)</c>, in order: ⌈log₂ n⌉ levels for n clauses.
    /// Recursive, and that deep at most.
    /// </summary>
    private static FilterNode Build(FilterNode[] clauses, int from, int to, bool isAnd)
    {
        if (to - from == 1) return clauses[from];

        int mid = from + (to - from + 1) / 2;
        return Join(isAnd, Build(clauses, from, mid, isAnd), Build(clauses, mid, to, isAnd));
    }

    private static void Children(FilterNode chain, out FilterNode left, out FilterNode right)
    {
        if (chain is AndNode and) { left = and.Left; right = and.Right; }
        else                      { var or = (OrNode)chain; left = or.Left; right = or.Right; }
    }

    private static FilterNode Join(bool isAnd, FilterNode left, FilterNode right) =>
        isAnd ? new AndNode(left, right) : new OrNode(left, right);
}
