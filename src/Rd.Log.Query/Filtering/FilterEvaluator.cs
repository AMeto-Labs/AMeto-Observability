using System.Collections;
using Rd.Log.Core;

namespace Rd.Log.Query.Filtering;

/// <summary>
/// Evaluates a compiled <see cref="FilterNode"/> AST against a single <see cref="LogEvent"/>.
///
/// All evaluation is zero-allocation where possible — comparisons operate on boxed
/// values from the event's Properties dictionary (already heap-allocated).
/// </summary>
public static class FilterEvaluator
{
    /// <summary>Returns true if <paramref name="ev"/> matches the filter.</summary>
    public static bool Matches(FilterNode filter, LogEvent ev)
    {
        return filter switch
        {
            MatchAllNode                  => true,
            AndNode and                   => Matches(and.Left, ev)  && Matches(and.Right, ev),
            OrNode or                     => Matches(or.Left, ev)   || Matches(or.Right, ev),
            NotNode not                   => !Matches(not.Operand, ev),
            LevelNode lvl                 => ev.Level == lvl.Level,
            HasNode has                   => HasProperty(ev, has.Property),
            IsDefinedNode def             => HasProperty(ev, def.Property),
            CompareNode cmp               => EvalCompare(cmp, ev),
            LikeNode like                 => EvalLike(like, ev),
            StartsWithNode sw             => EvalStartsWith(sw, ev),
            ContainsNode ct               => EvalContains(ct, ev),
            InNode inNode                 => EvalIn(inNode, ev),
            _                             => false,
        };
    }

    // ── Property access ───────────────────────────────────────────────────────

    private static bool HasProperty(LogEvent ev, string prop) =>
        GetValue(ev, prop) is not null;

    private static object? GetValue(LogEvent ev, string prop)
    {
        // Built-in CLEF fields. Keys with a dot in the user grammar arrive
        // here as PropertyPath.Separator-joined segments (e.g. "@x\u0001type").
        switch (prop)
        {
            case "@l" or "Level":            return ev.Level.ToSeqString();
            case "@mt" or "MessageTemplate": return ev.MessageTemplate;
            case "@m" or "Message":          return ev.MessageTemplate;
            case "@x" or "Exception":        return ev.Exception?.Type;
            case "@x\u0001type"          or "Exception\u0001Type":          return ev.Exception?.Type;
            case "@x\u0001message"       or "Exception\u0001Message":       return ev.Exception?.Message;
            case "@x\u0001stack"         or "Exception\u0001StackTrace":    return ev.Exception?.StackTrace;
            case "@x\u0001inner\u0001type"    or "Exception\u0001Inner\u0001Type":    return ev.Exception?.Inner?.Type;
            case "@x\u0001inner\u0001message" or "Exception\u0001Inner\u0001Message": return ev.Exception?.Inner?.Message;
            case "@t" or "Timestamp":        return ev.Timestamp.ToString("O");
            case "@tr" or "TraceId":         return TraceIdHelper.FormatTraceId(ev.TraceIdHi, ev.TraceIdLo);
            case "@sp" or "SpanId":          return TraceIdHelper.FormatSpanId(ev.SpanId);
            case ClefFields.ServiceName:     return ev.ServiceName;
        }

        if (ev.Properties is null) return null;

        // Fast path: top-level key (no nested path)
        int sep = prop.IndexOf(PropertyPath.Separator);
        if (sep < 0)
            return ev.Properties.TryGetValue(prop, out var v) ? v : null;

        // Nested path: walk dictionary tree segment-by-segment.
        return WalkPath(ev.Properties, prop.AsSpan());
    }

    /// <summary>
    /// Walks a separator-delimited path through nested
    /// <see cref="Dictionary{TKey,TValue}"/> objects and <see cref="IList"/>
    /// arrays. A segment carrying <see cref="PropertyPath.IndexMarker"/> is
    /// always treated as a numeric index (valid only against a list); all
    /// other segments are dictionary keys, except that a plain numeric-looking
    /// segment is also accepted as an index when the current node is a list
    /// (so <c>Foo.0</c> still works once <c>Foo</c> resolves to an array).
    /// Stops early and returns null if any segment is missing or the current
    /// node type does not support the requested lookup.
    /// </summary>
    private static object? WalkPath(Dictionary<string, object?> root, ReadOnlySpan<char> path)
    {
        object? current = root;
        while (path.Length > 0)
        {
            int sep   = path.IndexOf(PropertyPath.Separator);
            var seg   = sep < 0 ? path : path[..sep];
            path      = sep < 0 ? default : path[(sep + 1)..];

            bool isIndex = PropertyPath.IsIndexSegment(seg);
            var rawSeg   = PropertyPath.SegmentValue(seg);

            if (current is IList list && current is not byte[])
            {
                if (!int.TryParse(rawSeg, System.Globalization.NumberStyles.Integer,
                        System.Globalization.CultureInfo.InvariantCulture, out int idx))
                    return null;
                if (idx < 0 || idx >= list.Count) return null;
                current = list[idx];
                continue;
            }

            if (isIndex) return null; // numeric index against non-list

            if (current is not Dictionary<string, object?> dict) return null;
            if (!dict.TryGetValue(rawSeg.ToString(), out current)) return null;
        }
        return current;
    }

    // ── Comparison ────────────────────────────────────────────────────────────

    private static bool EvalCompare(CompareNode node, LogEvent ev)
    {
        object? actual = GetValue(ev, node.Property);
        return MatchAny(actual, e => Compare(e, node.Value, node.Op));
    }

    /// <summary>
    /// Applies <paramref name="predicate"/> to <paramref name="value"/>.
    /// When the value is an array/list (but not a byte array, which represents
    /// binary payloads), returns true if <em>any</em> element satisfies the
    /// predicate — enabling expressions like <c>Tags = 'foo'</c> against an
    /// array property.
    /// </summary>
    private static bool MatchAny(object? value, Func<object?, bool> predicate)
    {
        if (value is IList list && value is not byte[] && value is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (predicate(list[i])) return true;
            return false;
        }
        return predicate(value);
    }

    private static bool Compare(object? left, object? right, CompareOp op)
    {
        if (left is null && right is null) return op is CompareOp.Eq;
        if (left is null || right is null) return op is CompareOp.Ne;

        // Bool comparison — must come before numeric path; ToDouble(bool) → NaN
        if (left is bool lb)
        {
            bool rb = right is bool rb2 ? rb2
                    : string.Equals(right?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
            return op is CompareOp.Eq ? lb == rb
                 : op is CompareOp.Ne ? lb != rb
                 : false;
        }

        // String comparison
        if (left is string ls)
        {
            string rs = right?.ToString() ?? string.Empty;
            int cmp   = string.Compare(ls, rs, StringComparison.OrdinalIgnoreCase);
            return op switch
            {
                CompareOp.Eq => cmp == 0,
                CompareOp.Ne => cmp != 0,
                CompareOp.Lt => cmp <  0,
                CompareOp.Le => cmp <= 0,
                CompareOp.Gt => cmp >  0,
                CompareOp.Ge => cmp >= 0,
                _            => false,
            };
        }

        // Numeric comparison — coerce both sides to double
        double lNum = ToDouble(left);
        double rNum = ToDouble(right);
        return op switch
        {
            CompareOp.Eq => Math.Abs(lNum - rNum) < 1e-15,
            CompareOp.Ne => Math.Abs(lNum - rNum) >= 1e-15,
            CompareOp.Lt => lNum <  rNum,
            CompareOp.Le => lNum <= rNum,
            CompareOp.Gt => lNum >  rNum,
            CompareOp.Ge => lNum >= rNum,
            _            => false,
        };
    }

    // ── String predicates ─────────────────────────────────────────────────────

    private static bool EvalLike(LikeNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e => e?.ToString() is { } s && LikeMatch(s, node.Pattern));
    }

    private static bool EvalStartsWith(StartsWithNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        var cmp = node.CaseInsensitive ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return MatchAny(val, e => e?.ToString() is { } s && s.StartsWith(node.Prefix, cmp));
    }

    private static bool EvalContains(ContainsNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        var cmp = node.CaseInsensitive ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return MatchAny(val, e => e?.ToString() is { } s && s.Contains(node.Text, cmp));
    }

    private static bool EvalIn(InNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        if (val is null) return false;
        return MatchAny(val, e =>
        {
            string elemStr = e?.ToString() ?? string.Empty;
            foreach (var item in node.Values)
            {
                string itemStr = item?.ToString() ?? string.Empty;
                if (string.Equals(elemStr, itemStr, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        });
    }

    // ── LIKE pattern matching ─────────────────────────────────────────────────
    // Supports % (any sequence) and _ (single char), case-insensitive.

    private static bool LikeMatch(string text, string pattern)
    {
        // Convert LIKE pattern to simple scan
        text    = text.ToLowerInvariant();
        pattern = pattern.ToLowerInvariant();

        // Fast paths
        if (pattern == "%") return true;
        if (!pattern.Contains('%') && !pattern.Contains('_'))
            return text == pattern;

        return LikeMatchRecursive(text.AsSpan(), pattern.AsSpan());
    }

    private static bool LikeMatchRecursive(ReadOnlySpan<char> text, ReadOnlySpan<char> pattern)
    {
        while (!pattern.IsEmpty)
        {
            char pc = pattern[0];

            if (pc == '%')
            {
                pattern = pattern[1..];
                if (pattern.IsEmpty) return true;
                for (int i = 0; i <= text.Length; i++)
                {
                    if (LikeMatchRecursive(text[i..], pattern)) return true;
                }
                return false;
            }

            if (text.IsEmpty) return false;

            if (pc != '_' && pc != text[0]) return false;

            text    = text[1..];
            pattern = pattern[1..];
        }
        return text.IsEmpty;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static double ToDouble(object? v) => v switch
    {
        double d  => d,
        float  f  => f,
        long   l  => l,
        int    i  => i,
        string s  => double.TryParse(s, System.Globalization.NumberStyles.Any,
                         System.Globalization.CultureInfo.InvariantCulture, out double d) ? d : double.NaN,
        _         => double.NaN,
    };
}
