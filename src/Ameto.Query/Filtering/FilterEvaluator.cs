using System.Collections;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using System.Xml.XPath;
using Ameto.Core;

namespace Ameto.Query.Filtering;

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
            EndsWithNode ew               => EvalEndsWith(ew, ev),
            LengthCompareNode len         => EvalLengthCompare(len, ev),
            ToJsonCompareNode toJson      => EvalToJsonCompare(toJson, ev),
            FromJsonCompareNode fromJson  => EvalFromJsonCompare(fromJson, ev),
            FromJsonPathCompareNode fjp   => EvalFromJsonPathCompare(fjp, ev),
            FromJsonPathLikeNode fjLike   => EvalFromJsonPathLike(fjLike, ev),
            FromJsonPathInNode fjIn       => EvalFromJsonPathIn(fjIn, ev),
            FromJsonPathHasNode fjHas     => EvalFromJsonPathHas(fjHas, ev),
            FromJsonPathStringPredicateNode fjStr => EvalFromJsonPathString(fjStr, ev),
            CoalesceCompareNode coalesce  => EvalCoalesceCompare(coalesce, ev),
            ToLowerCompareNode toLower    => EvalToLowerCompare(toLower, ev),
            ToUpperCompareNode toUpper    => EvalToUpperCompare(toUpper, ev),
            ToNumberCompareNode toNumber  => EvalToNumberCompare(toNumber, ev),
            SubstringCompareNode sub      => EvalSubstringCompare(sub, ev),
            IndexOfCompareNode idx        => EvalIndexOfCompare(idx, ev),
            ReplaceCompareNode repl       => EvalReplaceCompare(repl, ev),
            ConcatCompareNode concat      => EvalConcatCompare(concat, ev),
            TypeOfCompareNode typeOf      => EvalTypeOfCompare(typeOf, ev),
            ElementAtCompareNode element  => EvalElementAtCompare(element, ev),
            KeysCompareNode keys          => EvalKeysCompare(keys, ev),
            ValuesCompareNode values      => EvalValuesCompare(values, ev),
            RoundCompareNode round        => EvalRoundCompare(round, ev),
            NowCompareNode now            => EvalNowCompare(now),
            DateTimeCompareNode dateTime  => EvalDateTimeCompare(dateTime, ev),
            ToIsoStringCompareNode iso    => EvalToIsoStringCompare(iso, ev),
            DatePartCompareNode datePart  => EvalDatePartCompare(datePart, ev),
            TimeOfDayCompareNode tod      => EvalTimeOfDayCompare(tod, ev),
            TimeSpanCompareNode span      => EvalTimeSpanCompare(span, ev),
            TotalMillisecondsCompareNode ms => EvalTotalMillisecondsCompare(ms, ev),
            ToTimeStringCompareNode tstr  => EvalToTimeStringCompare(tstr, ev),
            ToHexStringCompareNode hex    => EvalToHexStringCompare(hex, ev),
            BucketCompareNode bucket      => EvalBucketCompare(bucket, ev),
            OffsetInCompareNode off       => EvalOffsetInCompare(off, ev),
            ArrivedCompareNode arrived    => EvalArrivedCompare(arrived, ev),
            FromXmlCompareNode fromXml    => EvalFromXmlCompare(fromXml, ev),
            FromBase64CompareNode fb64    => EvalFromBase64Compare(fb64, ev),
            ToBase64CompareNode tb64      => EvalToBase64Compare(tb64, ev),
            RegexMatchNode rxm            => EvalRegexMatch(rxm, ev),
            RegexExtractCompareNode rxe   => EvalRegexExtract(rxe, ev),
            InNode inNode                 => EvalIn(inNode, ev),
            FreeTextNode ft               => EvalFreeText(ft, ev),
            _                             => false,
        };
    }

    // ── Free-text search ────────────────────────────────────────────────────────

    /// <summary>
    /// Maximum property-nesting depth searched by free-text — MUST stay equal to
    /// <c>ServerOptions.MaxPropertyFlattenDepth</c> (default 5), the depth to which
    /// <c>SegmentIndexBuilder</c> flattens string values into the trigram index. A value
    /// nested deeper than this is not indexed, so it must not be matched here either, or
    /// cold-tier (index-skipped) and hot-tier (full-scan) queries would diverge.
    /// </summary>
    private const int MaxFlattenDepth = 5;

    /// <summary>
    /// Bare free-text: every term must appear (case-insensitive substring) in one of
    /// the trigram-indexed text fields — message template, exception type/message, or
    /// any flattened string property value. The field set is kept in lock-step with
    /// <c>SegmentIndexBuilder</c>'s trigram coverage so hot-tier scans and cold-tier
    /// index skips return the same events.
    /// </summary>
    private static bool EvalFreeText(FreeTextNode ft, LogEvent ev)
    {
        foreach (var term in ft.Terms)
            if (!EventContainsTerm(ev, term))
                return false;
        return true;
    }

    private static bool EventContainsTerm(LogEvent ev, string term)
    {
        if (Ci(ev.MessageTemplate, term)) return true;

        if (ev.Exception is { } ex)
        {
            if (Ci(ex.Type, term))    return true;
            if (Ci(ex.Message, term)) return true;
        }

        return ev.Properties is { } props && PropsContainTerm(props, term, depth: 0);
    }

    /// <summary>
    /// Recursively scans property values for <paramref name="term"/>, mirroring
    /// <c>SegmentIndexBuilder.FlattenValue</c>: nested maps are walked, arrays/lists are
    /// expanded, and only STRING scalars are matched — numbers and bools are not
    /// trigram-indexed, so matching them would desync the cold-tier index fast-path.
    /// The <paramref name="depth"/> guard mirrors the index's flatten-depth limit so a
    /// value nested deeper than <see cref="MaxFlattenDepth"/> (which the index never
    /// trigrammed) is not matched here either.
    /// </summary>
    private static bool PropsContainTerm(Dictionary<string, object?> dict, string term, int depth)
    {
        // Mirrors SegmentIndexBuilder.FlattenProperties' `if (depth > _maxFlattenDepth) return;`.
        if (depth > MaxFlattenDepth) return false;

        foreach (var v in dict.Values)
            if (ValueContainsTerm(v, term, depth))
                return true;
        return false;
    }

    private static bool ValueContainsTerm(object? v, string term, int depth)
    {
        switch (v)
        {
            case string s:
                return Ci(s, term);
            case Dictionary<string, object?> nested:
                return PropsContainTerm(nested, term, depth + 1); // nested map → +1 (arrays don't add depth)
            case IEnumerable seq: // arrays / lists — string is already handled above
                foreach (var item in seq)
                    if (ValueContainsTerm(item, term, depth)) return true;
                return false;
            default:
                return false; // numbers, bools, null — not trigram-indexed
        }
    }

    private static bool Ci(string? haystack, string term) =>
        haystack is not null && haystack.Contains(term, StringComparison.OrdinalIgnoreCase);

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
            case "@id" or "Id":              return ev.Id.RawValue;
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
            // Use span-based alternate lookup (available in .NET 9+) to avoid
            // allocating a string for rawSeg on every nested-path segment.
            var lookup = dict.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!lookup.TryGetValue(rawSeg, out current)) return null;
        }
        return current;
    }

    // ── Comparison ────────────────────────────────────────────────────────────

    private static bool EvalCompare(CompareNode node, LogEvent ev)
    {
        // Inline MatchAny: avoids closure/delegate allocation on every event
        object? actual = GetValue(ev, node.Property);
        if (actual is IList list && actual is not byte[] && actual is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (Compare(list[i], node.Value, node.Op)) return true;
            return false;
        }
        return Compare(actual, node.Value, node.Op);
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

        if (left is IDictionary || right is IDictionary ||
            (left is IList && left is not byte[] && left is not string) ||
            (right is IList && right is not byte[] && right is not string))
        {
            if (op is not (CompareOp.Eq or CompareOp.Ne)) return false;
            string l = JsonSerializer.Serialize(left);
            string r = JsonSerializer.Serialize(right);
            bool eq = string.Equals(l, r, StringComparison.Ordinal);
            return op == CompareOp.Eq ? eq : !eq;
        }

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
        // Inline MatchAny + use pre-lowercased pattern: no closure, no per-event ToLowerInvariant on pattern
        object? val = GetValue(ev, node.Property);
        if (val is IList list && val is not byte[] && val is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (list[i]?.ToString() is { } s && LikeMatchFast(s, node)) return true;
            return false;
        }
        return val?.ToString() is { } sv && LikeMatchFast(sv, node);
    }

    private static bool EvalStartsWith(StartsWithNode node, LogEvent ev)
    {
        // Inline MatchAny: no closure allocation per event
        object? val = GetValue(ev, node.Property);
        var cmp = node.CaseInsensitive ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (val is IList list && val is not byte[] && val is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (list[i]?.ToString() is { } s && s.StartsWith(node.Prefix, cmp)) return true;
            return false;
        }
        return val?.ToString() is { } sv && sv.StartsWith(node.Prefix, cmp);
    }

    private static bool EvalContains(ContainsNode node, LogEvent ev)
    {
        // Inline MatchAny: no closure allocation per event
        object? val = GetValue(ev, node.Property);
        var cmp = node.CaseInsensitive ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (val is IList list && val is not byte[] && val is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (list[i]?.ToString() is { } s && s.Contains(node.Text, cmp)) return true;
            return false;
        }
        return val?.ToString() is { } sv && sv.Contains(node.Text, cmp);
    }

    private static bool EvalEndsWith(EndsWithNode node, LogEvent ev)
    {
        // Inline MatchAny: no closure allocation per event
        object? val = GetValue(ev, node.Property);
        var cmp = node.CaseInsensitive ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (val is IList list && val is not byte[] && val is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (list[i]?.ToString() is { } s && s.EndsWith(node.Suffix, cmp)) return true;
            return false;
        }
        return val?.ToString() is { } sv && sv.EndsWith(node.Suffix, cmp);
    }

    private static bool EvalLengthCompare(LengthCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            object? length = e switch
            {
                string s => s.Length,
                IList list when e is not byte[] => list.Count,
                _ => null,
            };
            return Compare(length, node.Value, node.Op);
        });
    }

    private static bool EvalToJsonCompare(ToJsonCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            string json = JsonSerializer.Serialize(e);
            return Compare(json, node.Value, node.Op);
        });
    }

    private static bool EvalFromJsonCompare(FromJsonCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e is not string s || s.Length == 0) return false;
            try
            {
                using var doc = JsonDocument.Parse(s);
                var parsed = ConvertJsonElement(doc.RootElement);
                return Compare(parsed, node.Value, node.Op);
            }
            catch { return false; }
        });
    }

    private static bool EvalFromJsonPathCompare(FromJsonPathCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            // Fast guard: only attempt JSON parse if the string looks like JSON.
            // GetValue returns null when property is absent → MatchAny returns false
            // without ever reaching this lambda, so no parse is attempted.
            if (e is not string s || !LooksLikeJson(s)) return false;
            try
            {
                using var doc = JsonDocument.Parse(s);
                var element = WalkJsonElementPath(doc.RootElement, node.Path);
                if (element is null) return false;
                var converted = ConvertJsonElement(element.Value);
                return Compare(converted, node.Value, node.Op);
            }
            catch { return false; }
        });
    }

    // Shared fast-guard: skip JsonDocument.Parse for strings that cannot be JSON.
    // Called before every fromJson eval. MatchAny handles the null/absent case.
    private static bool LooksLikeJson(string s)
    {
        if (s.Length == 0) return false;
        var c = s[0];
        return c == '{' || c == '[' || c == '"'
            || c == 't' || c == 'f' || c == 'n'
            || (c >= '0' && c <= '9') || c == '-';
    }

    private static bool EvalFromJsonPathLike(FromJsonPathLikeNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e is not string s || !LooksLikeJson(s)) return false;
            try
            {
                using var doc = JsonDocument.Parse(s);
                var element = WalkJsonElementPath(doc.RootElement, node.Path);
                if (element is null) return false;
                var str = GetElementString(element.Value);
                return str is not null && LikeMatch(str, node.Pattern);
            }
            catch { return false; }
        });
    }

    private static bool EvalFromJsonPathIn(FromJsonPathInNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e is not string s || !LooksLikeJson(s)) return false;
            try
            {
                using var doc = JsonDocument.Parse(s);
                var element = WalkJsonElementPath(doc.RootElement, node.Path);
                if (element is null) return false;
                var converted = ConvertJsonElement(element.Value);
                string elemStr = converted?.ToString() ?? string.Empty;
                foreach (var item in node.Values)
                    if (string.Equals(elemStr, item?.ToString() ?? string.Empty, StringComparison.OrdinalIgnoreCase))
                        return true;
                return false;
            }
            catch { return false; }
        });
    }

    private static bool EvalFromJsonPathHas(FromJsonPathHasNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e is not string s || !LooksLikeJson(s)) return false;
            try
            {
                using var doc = JsonDocument.Parse(s);
                var element = WalkJsonElementPath(doc.RootElement, node.Path);
                return element is not null && element.Value.ValueKind != JsonValueKind.Null;
            }
            catch { return false; }
        });
    }

    private static bool EvalFromJsonPathString(FromJsonPathStringPredicateNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        var cmp = node.CaseInsensitive ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return MatchAny(val, e =>
        {
            if (e is not string s || !LooksLikeJson(s)) return false;
            try
            {
                using var doc = JsonDocument.Parse(s);
                var element = WalkJsonElementPath(doc.RootElement, node.Path);
                if (element is null) return false;
                string? str = GetElementString(element.Value);
                if (str is null) return false;
                return node.Kind switch
                {
                    FromJsonPathPredicateKind.StartsWith => str.StartsWith(node.Arg, cmp),
                    FromJsonPathPredicateKind.Contains   => str.Contains(node.Arg, cmp),
                    FromJsonPathPredicateKind.EndsWith   => str.EndsWith(node.Arg, cmp),
                    _ => false,
                };
            }
            catch { return false; }
        });
    }

    /// <summary>
    /// Walks a path array through a <see cref="JsonElement"/> tree.
    /// String segments are treated as object keys; numeric strings as array indices.
    /// Returns null when any segment is missing or the node type does not support the lookup.
    /// </summary>
    private static JsonElement? WalkJsonElementPath(JsonElement current, string[] path)
    {
        foreach (var seg in path)
        {
            switch (current.ValueKind)
            {
                case JsonValueKind.Object:
                    if (!current.TryGetProperty(seg, out var child))
                        return null;
                    current = child;
                    break;

                case JsonValueKind.Array:
                    if (!int.TryParse(seg, out int idx) || idx < 0 || idx >= current.GetArrayLength())
                        return null;
                    current = current[idx];
                    break;

                default:
                    return null;
            }
        }
        return current;
    }

    private static bool EvalCoalesceCompare(CoalesceCompareNode node, LogEvent ev)
    {
        object? coalesced = null;
        bool found = false;

        foreach (var arg in node.Args)
        {
            object? candidate = arg switch
            {
                CoalescePropertyArgNode p => GetValue(ev, p.Property),
                CoalesceLiteralArgNode l  => l.Value,
                _ => null,
            };

            if (candidate is not null)
            {
                coalesced = candidate;
                found = true;
                break;
            }
        }

        if (!found) return false;
        return Compare(coalesced, node.Value, node.Op);
    }

    private static bool EvalToLowerCompare(ToLowerCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e => e?.ToString() is { } s && Compare(s.ToLowerInvariant(), node.Value, node.Op));
    }

    private static bool EvalToUpperCompare(ToUpperCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e => e?.ToString() is { } s && Compare(s.ToUpperInvariant(), node.Value, node.Op));
    }

    private static bool EvalToNumberCompare(ToNumberCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            double parsed = ToDouble(e);
            if (double.IsNaN(parsed)) return false;
            return Compare(parsed, node.Value, node.Op);
        });
    }

    private static bool EvalSubstringCompare(SubstringCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e?.ToString() is not { } s) return false;
            if (node.Start < 0 || node.Start > s.Length) return false;
            if (node.Length is int len)
            {
                if (len < 0) return false;
                int take = Math.Min(len, s.Length - node.Start);
                return Compare(s.Substring(node.Start, take), node.Value, node.Op);
            }
            return Compare(s.Substring(node.Start), node.Value, node.Op);
        });
    }

    private static bool EvalIndexOfCompare(IndexOfCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e?.ToString() is not { } s) return false;
            int idx = node.LastIndex
                ? s.LastIndexOf(node.Substring, StringComparison.Ordinal)
                : s.IndexOf(node.Substring, StringComparison.Ordinal);
            return Compare(idx, node.Value, node.Op);
        });
    }

    private static bool EvalReplaceCompare(ReplaceCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e?.ToString() is not { } s) return false;
            return Compare(s.Replace(node.Substring, node.Replacement, StringComparison.Ordinal), node.Value, node.Op);
        });
    }

    private static bool EvalConcatCompare(ConcatCompareNode node, LogEvent ev)
    {
        var sb = new StringBuilder();
        foreach (var arg in node.Args)
        {
            object? value = arg switch
            {
                ConcatPropertyArgNode p => GetValue(ev, p.Property),
                ConcatLiteralArgNode l  => l.Value,
                _ => null,
            };
            if (value is null) return false;
            if (value is not string s) return false;
            sb.Append(s);
        }
        return Compare(sb.ToString(), node.Value, node.Op);
    }

    private static bool EvalTypeOfCompare(TypeOfCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        string type = GetTypeName(val);
        return Compare(type, node.Value, node.Op);
    }

    private static bool EvalElementAtCompare(ElementAtCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        object? element = null;
        if (val is IList list && val is not byte[] && val is not string)
        {
            int idx = (int)ToDouble(node.Index);
            if (idx >= 0 && idx < list.Count) element = list[idx];
        }
        else if (val is Dictionary<string, object?> dict)
        {
            string key = node.Index?.ToString() ?? string.Empty;
            dict.TryGetValue(key, out element);
        }
        return Compare(element, node.Value, node.Op);
    }

    private static bool EvalKeysCompare(KeysCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        if (val is not Dictionary<string, object?> dict) return false;
        // Avoid Cast+ToList allocation: only Eq/Ne make sense on a key collection;
        // serialize directly via Count or as JSON for structural compare
        if (node.Op is CompareOp.Ne) return dict.Count > 0 ? !CompareKeysJson(dict, node.Value) : false;
        if (node.Op is CompareOp.Eq) return CompareKeysJson(dict, node.Value);
        return false;
    }

    private static bool EvalValuesCompare(ValuesCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        if (val is not Dictionary<string, object?> dict) return false;
        if (node.Op is CompareOp.Ne) return dict.Count > 0 ? !CompareValuesJson(dict, node.Value) : false;
        if (node.Op is CompareOp.Eq) return CompareValuesJson(dict, node.Value);
        return false;
    }

    private static bool CompareKeysJson(Dictionary<string, object?> dict, object? right)
    {
        if (right is null) return dict.Count == 0;
        // Compare serialized keys array against the right-hand value
        string l = JsonSerializer.Serialize(dict.Keys);
        string r = JsonSerializer.Serialize(right);
        return string.Equals(l, r, StringComparison.Ordinal);
    }

    private static bool CompareValuesJson(Dictionary<string, object?> dict, object? right)
    {
        if (right is null) return dict.Count == 0;
        string l = JsonSerializer.Serialize(dict.Values);
        string r = JsonSerializer.Serialize(right);
        return string.Equals(l, r, StringComparison.Ordinal);
    }

    private static bool EvalRoundCompare(RoundCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            double d = ToDouble(e);
            if (double.IsNaN(d)) return false;
            double rounded = Math.Round(d, node.Places, MidpointRounding.AwayFromZero);
            return Compare(rounded, node.Value, node.Op);
        });
    }

    private static bool EvalNowCompare(NowCompareNode node)
    {
        long ticks = DateTimeOffset.UtcNow.UtcTicks;
        return Compare(ticks, node.Value, node.Op);
    }

    private static bool EvalDateTimeCompare(DateTimeCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e?.ToString() is not { } s) return false;
            if (!DateTimeOffset.TryParse(s, out var dt)) return false;
            return Compare(dt.UtcTicks, node.Value, node.Op);
        });
    }

    private static bool EvalToIsoStringCompare(ToIsoStringCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            long ticks;
            if (e is long l) ticks = l;
            else if (e is string s && DateTimeOffset.TryParse(s, out var parsed)) ticks = parsed.UtcTicks;
            else return false;

            var utc = new DateTimeOffset(ticks, TimeSpan.Zero);
            var withOffset = node.OffsetHours is int h ? utc.ToOffset(TimeSpan.FromHours(h)) : utc;
            return Compare(withOffset.ToString("O"), node.Value, node.Op);
        });
    }

    private static bool EvalDatePartCompare(DatePartCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (!TryGetInstant(e, out var dt)) return false;
            if (node.OffsetHours is int h) dt = dt.ToOffset(TimeSpan.FromHours(h));

            int part = node.Part.ToLowerInvariant() switch
            {
                "year" => dt.Year,
                "month" => dt.Month,
                "day" => dt.Day,
                "hour" => dt.Hour,
                "minute" => dt.Minute,
                "second" => dt.Second,
                "weekday" => ((int)dt.DayOfWeek + 6) % 7 + 1,
                _ => int.MinValue,
            };
            if (part == int.MinValue) return false;
            return Compare(part, node.Value, node.Op);
        });
    }

    private static bool EvalTimeOfDayCompare(TimeOfDayCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (!TryGetInstant(e, out var dt)) return false;
            var local = dt.ToOffset(TimeSpan.FromHours(node.OffsetHours));
            return Compare(local.TimeOfDay.Ticks, node.Value, node.Op);
        });
    }

    private static bool EvalTimeSpanCompare(TimeSpanCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e is string s && System.TimeSpan.TryParse(s, out var ts))
                return Compare(ts.Ticks, node.Value, node.Op);
            return false;
        });
    }

    private static bool EvalTotalMillisecondsCompare(TotalMillisecondsCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e is string s && System.TimeSpan.TryParse(s, out var ts))
                return Compare(ts.TotalMilliseconds, node.Value, node.Op);

            double ticks = ToDouble(e);
            if (double.IsNaN(ticks)) return false;
            return Compare(ticks / System.TimeSpan.TicksPerMillisecond, node.Value, node.Op);
        });
    }

    private static bool EvalToTimeStringCompare(ToTimeStringCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            double ticks = ToDouble(e);
            if (double.IsNaN(ticks)) return false;
            var ts = new System.TimeSpan((long)ticks);
            return Compare(ts.ToString("c"), node.Value, node.Op);
        });
    }

    private static bool EvalToHexStringCompare(ToHexStringCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            double d = ToDouble(e);
            if (double.IsNaN(d)) return false;
            long whole = (long)d;
            string hex = $"0x{whole:x}";
            return Compare(hex, node.Value, node.Op);
        });
    }

    private static bool EvalBucketCompare(BucketCompareNode node, LogEvent ev)
    {
        if (node.Error <= 0d) return false;
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            double x = ToDouble(e);
            if (double.IsNaN(x)) return false;
            if (x == 0d) return Compare(0d, node.Value, node.Op);

            double mag = Math.Pow(10d, Math.Floor(Math.Log10(Math.Abs(x))));
            double step = node.Error * mag;
            if (step <= 0d || double.IsNaN(step) || double.IsInfinity(step)) return false;
            double bucket = Math.Round(x / step) * step;
            return Compare(bucket, node.Value, node.Op);
        });
    }

    private static bool EvalOffsetInCompare(OffsetInCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.InstantProperty);
        if (!TryGetInstant(val, out var instant)) return false;

        try
        {
            var tz = TimeZoneInfo.FindSystemTimeZoneById(node.TimeZoneId);
            var offset = tz.GetUtcOffset(instant.UtcDateTime);
            return Compare(offset.Ticks, node.Value, node.Op);
        }
        catch
        {
            return false;
        }
    }

    private static bool EvalArrivedCompare(ArrivedCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        if (val is null) return false;
        return Compare(ToDouble(val), node.Value, node.Op);
    }

    private static bool EvalIn(InNode node, LogEvent ev)
    {
        // Inline MatchAny: no closure; string items in Values avoid ToString() call
        object? val = GetValue(ev, node.Property);
        if (val is null) return false;
        if (val is IList list && val is not byte[] && val is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (MatchesInValues(list[i], node.Values)) return true;
            return false;
        }
        return MatchesInValues(val, node.Values);
    }

    private static bool MatchesInValues(object? element, object?[] values)
    {
        string elemStr = element?.ToString() ?? string.Empty;
        foreach (var item in values)
        {
            // Avoid ToString() boxing for the common case where item is already a string
            string itemStr = item is string si ? si : item?.ToString() ?? string.Empty;
            if (string.Equals(elemStr, itemStr, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    // ── LIKE pattern matching ─────────────────────────────────────────────────
    // Supports % (any sequence) and _ (single char), case-insensitive.

    // Fast path: pattern already lowercased + flags pre-computed in LikeNode constructor.
    // Avoids two ToLowerInvariant() + two Contains() calls per event.
    private static bool LikeMatchFast(string text, LikeNode node)
    {
        if (node.IsMatchAll) return true;
        text = text.ToLowerInvariant();
        if (node.IsLiteral) return text == node.PatternLower;
        return LikeMatchRecursive(text.AsSpan(), node.PatternLower.AsSpan());
    }

    // Kept for callers outside of LikeNode context (e.g. fromJson path LIKE).
    private static bool LikeMatch(string text, string pattern)
    {
        text = text.ToLowerInvariant();
        string lp = pattern.ToLowerInvariant();
        if (lp == "%") return true;
        if (!lp.Contains('%') && !lp.Contains('_')) return text == lp;
        return LikeMatchRecursive(text.AsSpan(), lp.AsSpan());
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
        decimal m => (double)m,
        long   l  => l,
        ulong  ul => ul,
        int    i  => i,
        short  s16 => s16,
        byte   b => b,
        string s  => double.TryParse(s, System.Globalization.NumberStyles.Any,
                         System.Globalization.CultureInfo.InvariantCulture, out double d) ? d : double.NaN,
        _         => double.NaN,
    };

    private static object? ConvertJsonElement(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Object => JsonSerializer.Deserialize<Dictionary<string, object?>>(el.GetRawText()),
            JsonValueKind.Array => JsonSerializer.Deserialize<List<object?>>(el.GetRawText()),
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out long l) ? l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null or JsonValueKind.Undefined => null,
            _ => null,
        };
    }

    private static string? GetElementString(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.String => el.GetString(),
        JsonValueKind.Number => el.GetRawText(),
        JsonValueKind.True   => "true",
        JsonValueKind.False  => "false",
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        _ => el.GetRawText(),
    };

    // ── FromXml / Base64 / Regex ──────────────────────────────────────────────

    private static bool EvalFromXmlCompare(FromXmlCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e is not string s) return false;
            try
            {
                var doc = XDocument.Parse(s);
                var result = doc.XPathEvaluate(node.XPath);
                object? extracted = result switch
                {
                    IEnumerable<object?> seq => seq.FirstOrDefault() switch
                    {
                        XElement el   => el.Value,
                        XAttribute at => at.Value,
                        string str    => str,
                        var other     => other?.ToString(),
                    },
                    _ => result?.ToString(),
                };
                return Compare(extracted, node.Value, node.Op);
            }
            catch { return false; }
        });
    }

    private static bool EvalFromBase64Compare(FromBase64CompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e is not string s) return false;
            try
            {
                var bytes = Convert.FromBase64String(s);
                var decoded = Encoding.UTF8.GetString(bytes);
                return Compare(decoded, node.Value, node.Op);
            }
            catch { return false; }
        });
    }

    private static bool EvalToBase64Compare(ToBase64CompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e?.ToString() is not { } s) return false;
            var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(s));
            return Compare(encoded, node.Value, node.Op);
        });
    }

    private static bool EvalRegexMatch(RegexMatchNode node, LogEvent ev)
    {
        var options = node.IgnoreCase
            ? RegexOptions.IgnoreCase | RegexOptions.CultureInvariant
            : RegexOptions.CultureInvariant;
        object? val = GetValue(ev, node.Property);
        if (val is IList list && val is not byte[] && val is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (list[i]?.ToString() is { } s && Regex.IsMatch(s, node.Pattern, options)) return true;
            return false;
        }
        return val?.ToString() is { } sv && Regex.IsMatch(sv, node.Pattern, options);
    }

    private static bool EvalRegexExtract(RegexExtractCompareNode node, LogEvent ev)
    {
        var options = node.IgnoreCase
            ? RegexOptions.IgnoreCase | RegexOptions.CultureInvariant
            : RegexOptions.CultureInvariant;
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e?.ToString() is not { } s) return false;
            var m = Regex.Match(s, node.Pattern, options);
            if (!m.Success || node.Group >= m.Groups.Count) return false;
            return Compare(m.Groups[node.Group].Value, node.Value, node.Op);
        });
    }

    private static string GetTypeName(object? value)
    {
        return value switch
        {
            null => "null",
            Dictionary<string, object?> => "object",
            IList when value is not byte[] and not string => "array",
            string => "string",
            bool => "bool",
            sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal => "number",
            _ => "undefined",
        };
    }

    private static bool TryGetInstant(object? value, out DateTimeOffset instant)
    {
        instant = default;
        if (value is null) return false;

        if (value is string s)
        {
            if (DateTimeOffset.TryParse(s, out instant)) return true;
            return false;
        }

        if (value is long l)
        {
            try
            {
                instant = new DateTimeOffset(l, TimeSpan.Zero);
                return true;
            }
            catch { return false; }
        }

        if (value is double d && !double.IsNaN(d) && !double.IsInfinity(d))
        {
            try
            {
                instant = new DateTimeOffset((long)d, TimeSpan.Zero);
                return true;
            }
            catch { return false; }
        }

        return false;
    }
}
