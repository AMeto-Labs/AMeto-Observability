namespace Ameto.Tracing.TraceQL;

public enum TraceQLOp { Eq, Neq, Lt, Lte, Gt, Gte }

/// <summary>
/// Scalar value from a TraceQL literal (string, number, or pre-converted duration nanos).
/// </summary>
public readonly struct TraceQLValue
{
    public readonly string? StringVal;
    public readonly double  Number;
    public readonly bool    IsNumber;
    public readonly bool    IsDuration; // Number holds nanoseconds

    /// <summary>
    /// The value was a bare word, not a quoted string — <c>nil</c> and <c>error</c> rather than
    /// <c>"nil"</c>. Kept apart because <c>{ .foo = nil }</c> asks about presence while
    /// <c>{ .foo = "nil" }</c> asks about a span whose attribute is literally the text "nil", and
    /// without this flag the two arrive here as the same value.
    /// </summary>
    public readonly bool    IsIdent;

    private TraceQLValue(string? s, double n, bool isNum, bool isDur, bool isIdent = false)
    { StringVal = s; Number = n; IsNumber = isNum; IsDuration = isDur; IsIdent = isIdent; }

    public static TraceQLValue FromString(string s)   => new(s,    0,   false, false);
    public static TraceQLValue FromNumber(double n)   => new(null, n,   true,  false);
    public static TraceQLValue FromDuration(long ns)  => new(null, ns,  true,  true);
    public static TraceQLValue FromIdent(string s)    => new(s,    0,   false, false, isIdent: true);

    public override string ToString() =>
        IsNumber ? (IsDuration ? $"{Number}ns" : Number.ToString()) : StringVal ?? "";
}

/// <summary>
/// A predicate evaluated against a single <see cref="SpanRecord"/>.
///
/// <para>THREE-VALUED, BECAUSE A SPAN CAN FAIL TO ANSWER. <c>true</c> is a match, <c>false</c> is a
/// definite non-match, and <c>null</c> is UNKNOWN: the field or attribute this predicate asks about
/// is not on this span, so it has no answer to give rather than a negative one. Only <c>true</c>
/// selects a span — see <c>TraceQLExecutor</c> — so an unknown behaves like a non-match at the top
/// level, which is what makes this a refinement of the old <c>bool</c> rather than a change of
/// meaning for any query that never meets an absent field.</para>
///
/// <para>WHAT IT BUYS IS THAT NEGATION STOPS LYING. With two values an absent field had to be
/// folded into <c>false</c>, and <c>!</c> then turned that into <c>true</c> — so
/// <c>{ .http.status_code != 200 }</c> excluded a span with no HTTP while
/// <c>{ !(.http.status_code = 200) }</c> included it, though a reader takes the two for the same
/// question. That was issue #74, and the half of it that reached users was #66: "everything that is
/// not a 200" answered with the whole application's traffic.</para>
///
/// <para>AND IT CANNOT BE DONE LOCALLY, which is why this is on the base class and not inside the
/// one predicate that has an absent state. Teaching <see cref="NotPredicate"/> to ask its inner
/// predicate "do you apply here?" breaks on composition: with <c>A</c> unknown and <c>B</c> false,
/// <c>A AND B</c> is FALSE — the conjunction is decided even though an operand is not — so
/// <c>NOT (A AND B)</c> is <c>true</c>, not unknown. Unknownness is a property of each connective's
/// truth table, not something inherited from the operands, so every connective has to carry it.</para>
/// </summary>
public abstract class SpanPredicate
{
    /// <summary>True = matches, false = does not, null = the span cannot answer (absent field).</summary>
    public abstract bool? Evaluate(SpanRecord span);
}

/// <summary>Always-true placeholder (empty filter body <c>{ }</c>).</summary>
public sealed class TruePredicate : SpanPredicate
{
    public static readonly TruePredicate Instance = new();
    public override bool? Evaluate(SpanRecord _) => true;
}

/// <summary>
/// Three-valued conjunction: false wins over unknown, because one false operand decides the
/// conjunction whether or not the other could answer.
/// </summary>
public sealed class AndPredicate(SpanPredicate left, SpanPredicate right) : SpanPredicate
{
    public SpanPredicate Left  = left;
    public SpanPredicate Right = right;

    public override bool? Evaluate(SpanRecord s)
    {
        bool? l = Left.Evaluate(s);
        if (l == false) return false;          // short-circuits, and decides even if Right is unknown
        bool? r = Right.Evaluate(s);
        if (r == false) return false;
        return l is null || r is null ? null : true;
    }
}

/// <summary>
/// Three-valued disjunction: true wins over unknown, for the mirror reason — one true operand
/// decides the disjunction on its own.
/// </summary>
public sealed class OrPredicate(SpanPredicate left, SpanPredicate right) : SpanPredicate
{
    public SpanPredicate Left  = left;
    public SpanPredicate Right = right;

    public override bool? Evaluate(SpanRecord s)
    {
        bool? l = Left.Evaluate(s);
        if (l == true) return true;            // short-circuits, and decides even if Right is unknown
        bool? r = Right.Evaluate(s);
        if (r == true) return true;
        return l is null || r is null ? null : false;
    }
}

/// <summary>
/// Three-valued negation: unknown negates to unknown. This is the line the whole change exists for
/// — a span that cannot answer a question cannot answer its opposite either.
/// </summary>
public sealed class NotPredicate(SpanPredicate inner) : SpanPredicate
{
    public SpanPredicate Inner = inner;
    public override bool? Evaluate(SpanRecord s) => Inner.Evaluate(s) switch
    {
        true  => false,
        false => true,
        null  => null,
    };
}

/// <summary>Matches a span attribute by key — falls back to attributes dict (may deserialise msgpack).</summary>
public sealed class AttributePredicate(string key, TraceQLOp op, TraceQLValue value) : SpanPredicate
{
    public readonly string       Key   = key;
    public readonly TraceQLOp    Op    = op;
    public readonly TraceQLValue Value = value;

    /// <summary>
    /// UNKNOWN WHEN THE ATTRIBUTE IS NOT THERE. This is the general case of the same shape #66
    /// reported on the promoted HTTP status: a span that never carried <c>.foo</c> has no answer to
    /// <c>.foo = "bar"</c>, and folding that into <c>false</c> made <c>{ !(.foo = "bar") }</c>
    /// select every span in the system that had never heard of <c>.foo</c>.
    ///
    /// <para>PRESENT BUT INCOMPARABLE IS ALSO UNKNOWN — issue #76, and the argument that decided it
    /// is that "has the field" was never the question. What <c>null</c> reports is that THIS SPAN
    /// CANNOT ANSWER THIS COMPARISON, and a tenant of "bananas" cannot answer <c>&gt; 5</c> any more
    /// than a missing tenant can. Reading it as false left the #66 shape standing in a narrower
    /// place: <c>{ !(.tenant &gt; 5) }</c> returned every span whose tenant is a name.</para>
    ///
    /// <para>Nothing changes for the plain form — <c>{ .tenant &gt; 5 }</c> selected no such span
    /// before and selects none now, because unknown does not match at the top level either. The
    /// answers that move are the negated and composed ones, which is the whole point.</para>
    ///
    /// <para>THE MIRROR CASE IS NOT THE SAME QUESTION and is deliberately untouched: a numeric
    /// attribute met by a STRING comparison (<c>{ .count &gt; "5" }</c>) is compared as text, and
    /// lexical order on "42" versus "5" may surprise — but the span can and does answer, so there
    /// is no third value to give. That is a complaint about what comparison MEANS, not about
    /// three-valued logic, and it is not what #76 asked.</para>
    /// </summary>
    public override bool? Evaluate(SpanRecord s)
    {
        if (s.Attributes is null) return null;
        s.Attributes.TryGetValue(Key, out var raw);
        return CompareAttr(raw, Op, Value);
    }

    internal static bool? CompareAttr(object? raw, TraceQLOp op, in TraceQLValue qv)
    {
        if (raw is null) return null;

        if (qv.IsNumber)
        {
            // NULL RATHER THAN NaN AS THE "NOT A NUMBER HERE" SIGNAL, because the two are different
            // facts and the old code spelled them the same. `_ => double.NaN` meant "this value has
            // no numeric reading", but an attribute whose value genuinely IS NaN reached the same
            // branch, and both were answered false. Only the first is a span that cannot answer.
            double? attrNum = raw switch
            {
                long   l => l,
                int    i => i,
                double d => d,
                string str when double.TryParse(str,
                    System.Globalization.NumberStyles.Any,
                    System.Globalization.CultureInfo.InvariantCulture, out var v) => v,
                _ => null,
            };

            // The attribute is there but has no numeric reading — issue #76. Answering false here
            // put the #66 shape back in a narrower place: `{ .tenant > 5 }` on "bananas" was false,
            // so `{ !(.tenant > 5) }` was TRUE and a query for "not the big tenants" returned every
            // span whose tenant is a name. A comparison the span cannot answer is unknown, exactly
            // as an absent field is, and negation then carries it.
            if (attrNum is null) return null;

            // A real NaN, on the other hand, IS an answer, and IEEE gives a self-consistent one:
            // `NaN = 5` is false, `NaN != 5` is true, and `!(NaN = 5)` is true — so the two
            // spellings agree and nothing has to be special-cased.
            return CompareOp(attrNum.Value, op, qv.Number);
        }

        string attrStr = raw.ToString() ?? string.Empty;
        int cmp = string.Compare(attrStr, qv.StringVal, StringComparison.OrdinalIgnoreCase);
        return op switch
        {
            TraceQLOp.Eq  => cmp == 0,
            TraceQLOp.Neq => cmp != 0,
            TraceQLOp.Lt  => cmp < 0,
            TraceQLOp.Lte => cmp <= 0,
            TraceQLOp.Gt  => cmp > 0,
            TraceQLOp.Gte => cmp >= 0,
            _             => false,
        };
    }

    private static bool CompareOp(double a, TraceQLOp op, double b) => op switch
    {
        TraceQLOp.Eq  => a == b,
        TraceQLOp.Neq => a != b,
        TraceQLOp.Lt  => a < b,
        TraceQLOp.Lte => a <= b,
        TraceQLOp.Gt  => a > b,
        TraceQLOp.Gte => a >= b,
        _             => false,
    };
}

/// <summary>Matches duration in nanoseconds. Query value holds nanos.</summary>
public sealed class DurationPredicate(TraceQLOp op, long nanos) : SpanPredicate
{
    public readonly TraceQLOp Op    = op;
    public readonly long      Nanos = nanos;

    public override bool? Evaluate(SpanRecord s)
    {
        long d = s.DurationNanos;
        return Op switch
        {
            TraceQLOp.Eq  => d == Nanos,
            TraceQLOp.Neq => d != Nanos,
            TraceQLOp.Lt  => d <  Nanos,
            TraceQLOp.Lte => d <= Nanos,
            TraceQLOp.Gt  => d >  Nanos,
            TraceQLOp.Gte => d >= Nanos,
            _             => false,
        };
    }
}

/// <summary>Matches <see cref="SpanRecord.Status"/>.</summary>
public sealed class StatusPredicate(TraceQLOp op, SpanStatusCode value) : SpanPredicate
{
    public readonly TraceQLOp      Op    = op;
    public readonly SpanStatusCode Value = value;

    public override bool? Evaluate(SpanRecord s) => Op switch
    {
        TraceQLOp.Eq  => s.Status == Value,
        TraceQLOp.Neq => s.Status != Value,
        _             => false,
    };
}

/// <summary>Matches <see cref="SpanRecord.ServiceName"/>.</summary>
public sealed class ServicePredicate(TraceQLOp op, string value) : SpanPredicate
{
    public readonly TraceQLOp Op    = op;
    public readonly string    Value = value;

    public override bool? Evaluate(SpanRecord s)
    {
        int cmp = string.Compare(s.ServiceName, Value, StringComparison.OrdinalIgnoreCase);
        return Op switch
        {
            TraceQLOp.Eq  => cmp == 0,
            TraceQLOp.Neq => cmp != 0,
            _             => false,
        };
    }
}

/// <summary>Matches <see cref="SpanRecord.Name"/> (substring or exact via op).</summary>
public sealed class NamePredicate(TraceQLOp op, string value) : SpanPredicate
{
    public readonly TraceQLOp Op    = op;
    public readonly string    Value = value;

    public override bool? Evaluate(SpanRecord s)
    {
        int cmp = string.Compare(s.Name, Value, StringComparison.OrdinalIgnoreCase);
        return Op switch
        {
            TraceQLOp.Eq  => cmp == 0,
            TraceQLOp.Neq => cmp != 0,
            _             => false,
        };
    }
}

/// <summary>Matches <see cref="SpanRecord.Kind"/>.</summary>
public sealed class KindPredicate(TraceQLOp op, SpanKind kind) : SpanPredicate
{
    public readonly TraceQLOp Op   = op;
    public readonly SpanKind  Kind = kind;

    public override bool? Evaluate(SpanRecord s) => Op switch
    {
        TraceQLOp.Eq  => s.Kind == Kind,
        TraceQLOp.Neq => s.Kind != Kind,
        _             => false,
    };
}

/// <summary>
/// Matches promoted <see cref="SpanRecord.HttpStatusCode"/>.
///
/// <para>NO STATUS IS NOT STATUS ZERO. The field is a plain <c>short</c>, so it is 0 on every span
/// that carries no HTTP at all — a database call, a queue consumer, an internal operation — and
/// comparing it straight made <c>&lt; 500</c>, <c>&lt;= 404</c> and <c>!= 200</c> true for all of
/// them. A user asking for "everything that is not a 200" was handed the whole application's
/// traffic, with no error and nothing in the log to say so.</para>
///
/// <para>THE ABSENT FIELD SATISFIES NO COMPARISON, which is the same answer a presence flag would
/// give without one: 0 is not a valid HTTP status — the range is 100-599, and the hint path in
/// <c>TraceQLExecutor</c> already bounds it to 100-999 — so on this field 0 can only mean absent.
/// Deciding it here rather than in <see cref="SpanRecord"/> is also what makes the fix reach data
/// already on disk: the segment format persists this field, so every <c>.trc</c> ever written
/// records "no HTTP" and "status 0" as the same byte, and no new flag can tell them apart
/// afterwards.</para>
///
/// <para>ABSENT ANSWERS UNKNOWN, NOT FALSE — issue #74, and the reason <see cref="NotPredicate"/>
/// now agrees with <c>!=</c>. The first version of this fix folded absence into <c>false</c>,
/// which closed <c>{ .http.status_code != 200 }</c> and left <c>{ !(.http.status_code = 200) }</c>
/// selecting the whole non-HTTP half of the traffic. Returning <c>null</c> puts the decision in the
/// connectives, where negation can carry it: SQL has no such gap because <c>NOT (x = 200)</c> with
/// <c>x IS NULL</c> does not match either.</para>
///
/// <para>Asking for the spans that have no HTTP status is <c>{ .http.status_code = nil }</c> — see
/// <see cref="HttpStatusPresencePredicate"/>. Before #74 that question had no spelling at all;
/// before #73 it was answered by <c>{ .http.status_code = 0 }</c>, by accident and as a side
/// effect of the defect.</para>
/// </summary>
public sealed class HttpStatusCodePredicate(TraceQLOp op, short code) : SpanPredicate
{
    public readonly TraceQLOp Op   = op;
    public readonly short     Code = code;

    public override bool? Evaluate(SpanRecord s)
    {
        if (s.HttpStatusCode == 0) return null;

        return Op switch
        {
            TraceQLOp.Eq  => s.HttpStatusCode == Code,
            TraceQLOp.Neq => s.HttpStatusCode != Code,
            TraceQLOp.Lt  => s.HttpStatusCode <  Code,
            TraceQLOp.Lte => s.HttpStatusCode <= Code,
            TraceQLOp.Gt  => s.HttpStatusCode >  Code,
            TraceQLOp.Gte => s.HttpStatusCode >= Code,
            _             => false,
        };
    }
}

/// <summary>
/// Tests whether a span carries an attribute at all: <c>{ .foo != nil }</c> for present,
/// <c>{ .foo = nil }</c> for absent.
///
/// <para>THE QUESTION THREE-VALUED LOGIC MAKES NECESSARY. Once an absent attribute answers
/// <c>null</c> to every comparison, no comparison can be used to find one — that is the point of
/// <c>null</c>, and it is why "give me the spans missing a tenant id" stopped being expressible the
/// moment <see cref="AttributePredicate"/> stopped lying about absence. This predicate is the
/// spelling that replaces the accident: it asks about presence directly and so is itself always
/// answerable, never <c>null</c>.</para>
/// </summary>
public sealed class AttributePresencePredicate(string key, bool present) : SpanPredicate
{
    public readonly string Key     = key;
    /// <summary>True for <c>!= nil</c> (must be present), false for <c>= nil</c> (must be absent).</summary>
    public readonly bool   Present = present;

    public override bool? Evaluate(SpanRecord s)
    {
        bool has = s.Attributes is not null
                && s.Attributes.TryGetValue(Key, out var raw)
                && raw is not null;
        return has == Present;
    }
}

/// <summary>
/// Presence of the promoted <see cref="SpanRecord.HttpStatusCode"/>: <c>{ .http.status_code = nil }</c>.
///
/// <para>DECIDED FROM THE PROMOTED FIELD, NOT FROM THE ATTRIBUTE MAP, and that has to be said
/// because the two are different storage. <see cref="HttpStatusCodePredicate"/> reads absence as
/// <c>HttpStatusCode == 0</c>; if presence were answered out of the attributes dictionary instead,
/// the two would disagree on any span whose status was promoted out of the map — the query
/// <c>{ .http.status_code = nil }</c> would return spans that <c>{ .http.status_code = 200 }</c>
/// also returns. One field, one source of truth.</para>
/// </summary>
public sealed class HttpStatusPresencePredicate(bool present) : SpanPredicate
{
    /// <summary>True for <c>!= nil</c> (must be present), false for <c>= nil</c> (must be absent).</summary>
    public readonly bool Present = present;

    public override bool? Evaluate(SpanRecord s) => (s.HttpStatusCode != 0) == Present;
}
