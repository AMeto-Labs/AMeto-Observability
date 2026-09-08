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

    private TraceQLValue(string? s, double n, bool isNum, bool isDur)
    { StringVal = s; Number = n; IsNumber = isNum; IsDuration = isDur; }

    public static TraceQLValue FromString(string s)   => new(s,    0,   false, false);
    public static TraceQLValue FromNumber(double n)   => new(null, n,   true,  false);
    public static TraceQLValue FromDuration(long ns)  => new(null, ns,  true,  true);
    public static TraceQLValue FromIdent(string s)    => new(s,    0,   false, false);

    public override string ToString() =>
        IsNumber ? (IsDuration ? $"{Number}ns" : Number.ToString()) : StringVal ?? "";
}

/// <summary>A predicate evaluated against a single <see cref="SpanRecord"/>.</summary>
public abstract class SpanPredicate
{
    public abstract bool Evaluate(SpanRecord span);
}

/// <summary>Always-true placeholder (empty filter body <c>{ }</c>).</summary>
public sealed class TruePredicate : SpanPredicate
{
    public static readonly TruePredicate Instance = new();
    public override bool Evaluate(SpanRecord _) => true;
}

public sealed class AndPredicate(SpanPredicate left, SpanPredicate right) : SpanPredicate
{
    public SpanPredicate Left  = left;
    public SpanPredicate Right = right;
    public override bool Evaluate(SpanRecord s) => Left.Evaluate(s) && Right.Evaluate(s);
}

public sealed class OrPredicate(SpanPredicate left, SpanPredicate right) : SpanPredicate
{
    public SpanPredicate Left  = left;
    public SpanPredicate Right = right;
    public override bool Evaluate(SpanRecord s) => Left.Evaluate(s) || Right.Evaluate(s);
}

public sealed class NotPredicate(SpanPredicate inner) : SpanPredicate
{
    public SpanPredicate Inner = inner;
    public override bool Evaluate(SpanRecord s) => !Inner.Evaluate(s);
}

/// <summary>Matches a span attribute by key — falls back to attributes dict (may deserialise msgpack).</summary>
public sealed class AttributePredicate(string key, TraceQLOp op, TraceQLValue value) : SpanPredicate
{
    public readonly string       Key   = key;
    public readonly TraceQLOp    Op    = op;
    public readonly TraceQLValue Value = value;

    public override bool Evaluate(SpanRecord s)
    {
        if (s.Attributes is null) return false;
        s.Attributes.TryGetValue(Key, out var raw);
        return CompareAttr(raw, Op, Value);
    }

    internal static bool CompareAttr(object? raw, TraceQLOp op, in TraceQLValue qv)
    {
        if (raw is null) return false;

        if (qv.IsNumber)
        {
            double attrNum = raw switch
            {
                long   l => (double)l,
                int    i => (double)i,
                double d => d,
                string str when double.TryParse(str,
                    System.Globalization.NumberStyles.Any,
                    System.Globalization.CultureInfo.InvariantCulture, out var v) => v,
                _ => double.NaN,
            };
            if (double.IsNaN(attrNum)) return false;
            return CompareOp(attrNum, op, qv.Number);
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

    public override bool Evaluate(SpanRecord s)
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

    public override bool Evaluate(SpanRecord s) => Op switch
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

    public override bool Evaluate(SpanRecord s)
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

    public override bool Evaluate(SpanRecord s)
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

    public override bool Evaluate(SpanRecord s) => Op switch
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
/// <para>THE NEGATION IS STILL TWO-VALUED, AND THAT ASYMMETRY IS DELIBERATE HERE RATHER THAN
/// OVERLOOKED. This predicate now has three outcomes — true, false, and "the field is not on this
/// span" collapsed into false — while <see cref="NotPredicate"/> is a plain <c>!</c> over two. So
/// <c>{ .http.status_code != 200 }</c> excludes a span with no HTTP and
/// <c>{ !(.http.status_code = 200) }</c> includes it, though a reader takes the two for the same
/// question. SQL has no such gap because <c>NOT</c> there is defined over three values, and
/// <c>NOT (x = 200)</c> with <c>x IS NULL</c> does not match either.</para>
///
/// <para>Both forms answered "include it" before this change, so the <c>!</c> form is not a
/// regression — but closing one door and not the other is what makes the second one easy to miss,
/// which is how the original defect survived as long as it did. Making the whole AST three-valued
/// (<c>Evaluate</c> returning <c>bool?</c>, with three-valued tables on And/Or/Not) is the real
/// answer and is its own change — issue #74; until then the gap is pinned by
/// <c>TraceQLHttpStatusAbsenceTests.Negation_over_an_absent_field_is_still_two_valued</c>, which
/// fails the moment somebody fixes it and forces the decision to be a deliberate one.</para>
///
/// <para>One consequence worth knowing: with 0 read as absent there is no longer any way to ask
/// the language for "spans that carry no HTTP status". <c>{ .http.status_code = 0 }</c> used to
/// answer it, by accident and as a side effect of the defect. An explicit presence test belongs
/// with the three-valued work — issue #74.</para>
/// </summary>
public sealed class HttpStatusCodePredicate(TraceQLOp op, short code) : SpanPredicate
{
    public readonly TraceQLOp Op   = op;
    public readonly short     Code = code;

    public override bool Evaluate(SpanRecord s)
    {
        if (s.HttpStatusCode == 0) return false;

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
