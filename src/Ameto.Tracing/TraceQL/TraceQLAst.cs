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

/// <summary>Matches promoted <see cref="SpanRecord.HttpStatusCode"/>.</summary>
public sealed class HttpStatusCodePredicate(TraceQLOp op, short code) : SpanPredicate
{
    public readonly TraceQLOp Op   = op;
    public readonly short     Code = code;

    public override bool Evaluate(SpanRecord s) => Op switch
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
