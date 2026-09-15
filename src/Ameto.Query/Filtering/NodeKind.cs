using System.Collections.Frozen;

namespace Ameto.Query.Filtering;

/// <summary>
/// Dispatch tag for the node types the evaluator sees on the HOT path.
///
/// <para><see cref="FilterEvaluator.Matches"/> ran a fifty-arm type switch, which the compiler
/// lowers to a sequential chain of <c>isinst</c> tests: a <c>LikeNode</c> was the tenth, so a
/// contains-query paid ten type checks per node per candidate event before reaching the work.
/// A tag read from a field turns that into one jump table.</para>
///
/// <para>Only the shapes a log query actually produces in bulk are tagged. Everything else —
/// the several dozen function predicates (<c>fromJson</c>, <c>round</c>, <c>toBase64</c>, …) —
/// keeps <see cref="NodeKind.Other"/> and falls through to the type switch, which stays the
/// single place that knows how to evaluate them. That fallback is also the SAFETY property of
/// this design: a node type added later and not listed here is dispatched correctly by the
/// switch, just without the shortcut. Nothing silently stops matching.</para>
/// </summary>
internal enum NodeKind : byte
{
    Other = 0,
    MatchAll,
    And,
    Or,
    Not,
    Level,
    Has,
    IsDefined,
    Compare,
    TimeCompare,
    /// <summary>
    /// <c>@tr</c> / <c>@sp</c> equality, rewritten from a CompareNode at compile time. A bulk
    /// shape: the trace-logs and span-logs endpoints issue nothing else. It keeps its arm in
    /// <c>MatchesRare</c> as well, which is what an untagged instance reaches.
    /// </summary>
    TraceIdCompare,
    Like,
    StartsWith,
    Contains,
    EndsWith,
    In,
    FreeText,
}

/// <summary>
/// Type to <see cref="NodeKind"/>, resolved ONCE per node when the node is constructed —
/// that is, once per filter compile, not per event.
/// </summary>
internal static class NodeKinds
{
    private static readonly FrozenDictionary<Type, NodeKind> Map = new Dictionary<Type, NodeKind>
    {
        [typeof(MatchAllNode)]   = NodeKind.MatchAll,
        [typeof(AndNode)]        = NodeKind.And,
        [typeof(OrNode)]         = NodeKind.Or,
        [typeof(NotNode)]        = NodeKind.Not,
        [typeof(LevelNode)]      = NodeKind.Level,
        [typeof(HasNode)]        = NodeKind.Has,
        [typeof(IsDefinedNode)]  = NodeKind.IsDefined,
        [typeof(CompareNode)]    = NodeKind.Compare,
        [typeof(TimeCompareNode)]= NodeKind.TimeCompare,
        [typeof(TraceIdCompareNode)] = NodeKind.TraceIdCompare,
        [typeof(LikeNode)]       = NodeKind.Like,
        [typeof(StartsWithNode)] = NodeKind.StartsWith,
        [typeof(ContainsNode)]   = NodeKind.Contains,
        [typeof(EndsWithNode)]   = NodeKind.EndsWith,
        [typeof(InNode)]         = NodeKind.In,
        [typeof(FreeTextNode)]   = NodeKind.FreeText,
    }.ToFrozenDictionary();

    public static NodeKind Of(Type nodeType) => Map.GetValueOrDefault(nodeType, NodeKind.Other);
}
