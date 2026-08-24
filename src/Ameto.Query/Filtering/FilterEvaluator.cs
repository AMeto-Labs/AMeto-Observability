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
            TimeCompareNode tc            => EvalTimeCompare(tc, ev),
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

    /// <summary>
    /// Reads one property the way a predicate reads it — built-in aliases, dotted paths, the
    /// flat-key reading of an OTLP attribute, all of it. Aggregation group keys go through
    /// here so that <c>group by @l</c> and <c>@l = 'Error'</c> can never disagree about what
    /// <c>@l</c> names.
    /// </summary>
    /// <param name="prop">An encoded path — <see cref="PropertyPath.Separator"/>-joined.</param>
    public static object? ReadProperty(LogEvent ev, string prop) => GetValue(ev, prop);

    private static object? GetValue(LogEvent ev, string prop)
    {
        // Built-in CLEF fields. Which spellings count as built-in lives in BuiltinFields —
        // the same table CompiledFilter consults to learn which index bucket a field is
        // written to, so an alias accepted here can never be hinted under a name the index
        // has never heard of. Keys with a dot in the user grammar arrive here as
        // PropertyPath.Separator-joined segments (e.g. "@x\u0001type").
        if (BuiltinFields.TryResolve(prop, out var field))
            return ReadBuiltin(ev, field);

        // Fast path: top-level key (no nested path). Probed straight off the event's
        // msgpack — the dictionary is never built for it, which is the difference between
        // one value decoded and the whole property map materialised, per scanned event.
        int sep = prop.IndexOf(PropertyPath.Separator);
        if (sep < 0)
            return ev.TryGetProperty(prop, out var v) ? v : null;

        // A dotted path has two readings, and the order between them is load-bearing (see
        // the flat-key note below). But the walk can only succeed if its FIRST segment is
        // present, and that is one probe — so when it is absent the map still never
        // materialises, which is the common case for OTLP attributes (http.request.method
        // is one flat key, not a three-level tree).
        if (!ev.PropertiesMaterialised)
        {
            var head = prop.AsSpan(0, sep);
            // Presence only — decoding the head would build the very subtree this check
            // exists to avoid building.
            if (PropertyPath.IsIndexSegment(head)
                || !ev.HasNonNullProperty(PropertyPath.SegmentValue(head)))
                return ReadFlatKeyOrNull(ev, prop);
        }

        if (ev.Properties is null) return null;

        // Nested path: walk dictionary tree segment-by-segment.
        object? nested = WalkPath(ev.Properties, prop.AsSpan());
        if (nested is not null) return nested;

        // ...or ONE key that happens to contain dots. Every OTLP semantic-convention
        // attribute is spelled that way (http.request.method, k8s.pod.name), and the grammar
        // read a dotted name only as a walk, so those filters matched nothing at all unless
        // the user knew to write ['http.request.method']. Tried second, so a genuinely nested
        // map keeps the meaning it had.
        return ReadFlatKeyOrNull(ev, prop);
    }

    /// <summary>The dotted name read as ONE key, when the grammar allows that reading.</summary>
    private static object? ReadFlatKeyOrNull(LogEvent ev, string prop) =>
        PropertyPath.MayBeFlatKey(prop) ? ReadFlatKey(ev, prop) : null;

    /// <summary>
    /// Longest flat property key probed through the stack. Beyond this the walk's answer
    /// stands; a key this long is not a name anyone types.
    /// </summary>
    private const int MaxFlatKeyChars = 512;

    /// <summary>
    /// Reads a property whose NAME contains the dots the parser split on. Allocation-free —
    /// this runs per event for every dotted-attribute filter, so the flattened spelling is
    /// built in stack scratch and probed through the dictionary's span alternate lookup.
    /// </summary>
    private static object? ReadFlatKey(LogEvent ev, string prop)
    {
        if (prop.Length > MaxFlatKeyChars) return null;

        Span<char> flat = stackalloc char[MaxFlatKeyChars];
        int n = PropertyPath.WriteFlatKey(prop.AsSpan(), flat);
        if (n < 0) return null;

        // Through the event, so an unmaterialised map is probed rather than built — the
        // dotted OTLP spelling is the case that reaches here on every scanned event.
        return ev.TryGetProperty(flat[..n], out var v) ? v : null;
    }

    /// <summary>
    /// Reads one resolved built-in field off the event. A switch over the enum compiles to a
    /// jump table, so the per-event cost is the frozen-dictionary probe in
    /// <see cref="BuiltinFields.TryResolve"/> plus one indirect branch — no allocation.
    /// </summary>
    private static object? ReadBuiltin(LogEvent ev, BuiltinField field) => field switch
    {
        BuiltinField.Level                 => ev.Level.ToSeqString(),
        BuiltinField.MessageTemplate       => ev.MessageTemplate,
        BuiltinField.ExceptionType         => ev.Exception?.Type,
        BuiltinField.ExceptionMessage      => ev.Exception?.Message,
        BuiltinField.ExceptionStack        => ev.Exception?.StackTrace,
        BuiltinField.InnerExceptionType    => ev.Exception?.Inner?.Type,
        BuiltinField.InnerExceptionMessage => ev.Exception?.Inner?.Message,
        BuiltinField.Timestamp             => ev.Timestamp.ToString("O"),
        BuiltinField.TraceId               => TraceIdHelper.FormatTraceId(ev.TraceIdHi, ev.TraceIdLo),
        BuiltinField.SpanId                => TraceIdHelper.FormatSpanId(ev.SpanId),
        BuiltinField.EventId               => ev.Id.RawValue,
        BuiltinField.ServiceName           => ev.ServiceName,
        _                                  => null,
    };

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

    /// <summary>
    /// Chronological <c>@t</c> comparison — tick math against the literal parsed once at
    /// compile time (see <see cref="TimeCompareNode"/>), replacing a per-event ISO render
    /// plus ordinal string compare.
    /// </summary>
    private static bool EvalTimeCompare(TimeCompareNode node, LogEvent ev)
    {
        long a = ev.Timestamp.UtcTicks, b = node.Ticks;
        return node.Op switch
        {
            CompareOp.Eq => a == b,
            CompareOp.Ne => a != b,
            CompareOp.Lt => a <  b,
            CompareOp.Le => a <= b,
            CompareOp.Gt => a >  b,
            CompareOp.Ge => a >= b,
            _            => false,
        };
    }

    private static bool EvalCompare(CompareNode node, LogEvent ev)
    {
        // Inline MatchAny: avoids closure/delegate allocation on every event
        object? actual = GetValue(ev, node.Property);

        // `A = B`: the right operand is read off the same event, not off the AST.
        object? expected = node.RightProperty is { } rhs ? GetValue(ev, rhs) : node.Value;

        if (actual is IList list && actual is not byte[] && actual is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (Compare(list[i], expected, node.Op)) return true;
            return false;
        }
        return Compare(actual, expected, node.Op);
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

    /// <summary>
    /// Deep structural equality for the values MessagePack deserialisation produces
    /// (null / string / bool / numbers / byte[] / List / Dictionary). Mirrors the old
    /// JSON-string equality: dictionary entries compare in enumeration order, and
    /// numbers compare across types (1L == 1.0, as both serialised to "1").
    /// </summary>
    private static bool StructuralEquals(object? a, object? b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a is null || b is null) return false;

        // byte[] first — it is an IList and must not fall into the list branch.
        if (a is byte[] ab) return b is byte[] bb && ab.AsSpan().SequenceEqual(bb);
        if (b is byte[]) return false;

        if (a is string sa) return b is string sb && string.Equals(sa, sb, StringComparison.Ordinal);
        if (b is string) return false;

        if (a is bool ba) return b is bool bb2 && ba == bb2;
        if (b is bool) return false;

        bool aNum = IsNumeric(a), bNum = IsNumeric(b);
        if (aNum || bNum) return aNum && bNum && ToDouble(a) == ToDouble(b);

        if (a is IDictionary da)
        {
            if (b is not IDictionary db || da.Count != db.Count) return false;
            var ea = da.GetEnumerator();
            var eb = db.GetEnumerator();
            while (ea.MoveNext())
            {
                if (!eb.MoveNext()) return false;
                if (!StructuralEquals(ea.Key, eb.Key)) return false;
                if (!StructuralEquals(ea.Value, eb.Value)) return false;
            }
            return !eb.MoveNext();
        }
        if (b is IDictionary) return false;

        if (a is IList la)
        {
            if (b is not IList lb || la.Count != lb.Count) return false;
            for (int i = 0; i < la.Count; i++)
                if (!StructuralEquals(la[i], lb[i])) return false;
            return true;
        }
        if (b is IList) return false;

        return a.Equals(b);
    }

    private static bool IsNumeric(object o) =>
        o is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;

    private static bool Compare(object? left, object? right, CompareOp op)
    {
        if (left is null && right is null) return op is CompareOp.Eq;
        if (left is null || right is null) return op is CompareOp.Ne;

        if (left is IDictionary || right is IDictionary ||
            (left is IList && left is not byte[] && left is not string) ||
            (right is IList && right is not byte[] && right is not string))
        {
            if (op is not (CompareOp.Eq or CompareOp.Ne)) return false;
            // Structural comparison — same semantics as the old serialise-both-sides-to-JSON
            // equality (ordered dictionary entries, cross-type numeric equality), without
            // allocating two JSON strings per compared event.
            bool eq = StructuralEquals(left, right);
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
        // No ToLowerInvariant() copy of the value: the pattern is already lowercased, so
        // the walk lowercases one character at a time. That copy was an allocation per
        // scanned value, which is per event of every LIKE query.
        if (node.IsLiteral) return EqualsFolded(text.AsSpan(), node.PatternLower.AsSpan());
        return LikeMatchLower(text.AsSpan(), node.PatternLower.AsSpan());
    }

    // Kept for callers outside of LikeNode context (e.g. fromJson path LIKE).
    private static bool LikeMatch(string text, string pattern)
    {
        string lp = pattern.ToLowerInvariant();
        if (lp == "%") return true;
        if (!lp.Contains('%') && !lp.Contains('_'))
            return EqualsFolded(text.AsSpan(), lp.AsSpan());
        return LikeMatchLower(text.AsSpan(), lp.AsSpan());
    }

    /// <summary>
    /// Equality under the SAME folding the wildcard matcher uses, so the literal and
    /// wildcard paths can never disagree about which characters are equal.
    /// <c>OrdinalIgnoreCase</c> is not that folding — it leaves the Kelvin sign and its
    /// friends unequal to the letters <c>string.ToLowerInvariant()</c> maps them onto,
    /// which is what the pattern was lowered with.
    /// </summary>
    private static bool EqualsFolded(ReadOnlySpan<char> text, ReadOnlySpan<char> patternLower)
    {
        int t = 0, p = 0;
        while (t < text.Length && p < patternLower.Length)
        {
            if (!FoldedCharsMatch(text, ref t, patternLower, ref p)) return false;
        }
        return t == text.Length && p == patternLower.Length;
    }

    /// <summary>
    /// Compares ONE folded character of the value against one of the (already folded)
    /// pattern, advancing both. A surrogate pair is folded as the code point it forms —
    /// <c>char.ToLowerInvariant</c> cannot see astral letters, while the pattern was
    /// lowered by <c>string.ToLowerInvariant()</c>, which can; folding only half of a pair
    /// would make a byte-identical pattern stop matching its own value.
    /// </summary>
    private static bool FoldedCharsMatch(
        ReadOnlySpan<char> text, ref int t, ReadOnlySpan<char> pattern, ref int p)
    {
        if (char.IsHighSurrogate(text[t]) && t + 1 < text.Length && char.IsLowSurrogate(text[t + 1]))
        {
            var folded = System.Text.Rune.ToLowerInvariant(
                new System.Text.Rune(text[t], text[t + 1]));
            Span<char> buf = stackalloc char[2];
            int n = folded.EncodeToUtf16(buf);
            if (p + n > pattern.Length || !pattern.Slice(p, n).SequenceEqual(buf[..n])) return false;
            t += 2;
            p += n;
            return true;
        }

        if (pattern[p] != char.ToLowerInvariant(text[t])) return false;
        t++;
        p++;
        return true;
    }

    /// <summary>
    /// SQL LIKE over a value against an already-lowercased pattern.
    ///
    /// <para>Iterative with a single backtrack point per <c>%</c>, so it is O(text × pattern)
    /// at worst and linear in practice. The recursive version it replaces branched at every
    /// position after every wildcard, which is exponential in the number of <c>%</c>: a
    /// pattern like <c>'%a%a%a%a%a%'</c> against a long non-matching value could occupy a
    /// core for the rest of the query — and a filter box is exactly where such a pattern
    /// gets typed by accident.</para>
    /// </summary>
    private static bool LikeMatchLower(ReadOnlySpan<char> text, ReadOnlySpan<char> pattern)
    {
        int t = 0, p = 0;
        int starP = -1, starT = 0;   // last '%' seen, and where it started consuming

        while (t < text.Length)
        {
            // The WILDCARD is tested first, and that order is the whole correctness of the
            // loop: '%' compares equal to itself, so testing the literal first let a '%'
            // IN THE VALUE consume the pattern's wildcard as an ordinary character. The
            // backtrack point was then never recorded and `Url like '%users'` stopped
            // matching "%2fapi%2fusers" — a URL-encoded path, or anything with a per-cent
            // sign in it, quietly dropped out of every contains-query.
            if (p < pattern.Length && pattern[p] == '%')
            {
                starP = p++;         // remember it and try to match the rest with zero chars
                starT = t;
            }
            else if (p < pattern.Length &&
                     (pattern[p] == '_' ? Advance(text, ref t, ref p) : FoldedCharsMatch(text, ref t, pattern, ref p)))
            {
                // consumed by the branch
            }
            else if (starP >= 0)
            {
                p = starP + 1;       // let the last '%' swallow one more character
                t = ++starT;
            }
            else return false;
        }

        while (p < pattern.Length && pattern[p] == '%') p++;
        return p == pattern.Length;
    }

    /// <summary>'_' consumes exactly one UTF-16 unit, as the recursive matcher did.</summary>
    private static bool Advance(ReadOnlySpan<char> text, ref int t, ref int p)
    {
        t++;
        p++;
        return true;
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
        object? val = GetValue(ev, node.Property);
        if (val is IList list && val is not byte[] && val is not string)
        {
            for (int i = 0; i < list.Count; i++)
                if (list[i]?.ToString() is { } s && IsMatch(node.Compiled, s)) return true;
            return false;
        }
        return val?.ToString() is { } sv && IsMatch(node.Compiled, sv);
    }

    /// <summary>
    /// One value against a pre-compiled pattern. A timeout can only come from the
    /// backtracking fallback — the linear engine is compiled without one precisely so that
    /// a big value on a loaded box cannot go missing — and there it means this ONE value
    /// defeated a pattern that is capable of catastrophic backtracking. Answering "no
    /// match" for it keeps the query alive instead of killing it halfway through a stream.
    /// </summary>
    private static bool IsMatch(Regex rx, string input)
    {
        try { return rx.IsMatch(input); }
        catch (RegexMatchTimeoutException) { return false; }
    }

    private static bool EvalRegexExtract(RegexExtractCompareNode node, LogEvent ev)
    {
        object? val = GetValue(ev, node.Property);
        return MatchAny(val, e =>
        {
            if (e?.ToString() is not { } s) return false;
            Match m;
            try { m = node.Compiled.Match(s); }
            catch (RegexMatchTimeoutException) { return false; }
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
