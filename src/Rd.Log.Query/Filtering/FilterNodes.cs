namespace Rd.Log.Query.Filtering;

// ── Abstract base ─────────────────────────────────────────────────────────────

/// <summary>Base class for all filter expression AST nodes.</summary>
public abstract class FilterNode { }

// ── Logical ───────────────────────────────────────────────────────────────────

public sealed class AndNode : FilterNode
{
    public FilterNode Left  { get; }
    public FilterNode Right { get; }
    public AndNode(FilterNode left, FilterNode right) { Left = left; Right = right; }
}

public sealed class OrNode : FilterNode
{
    public FilterNode Left  { get; }
    public FilterNode Right { get; }
    public OrNode(FilterNode left, FilterNode right) { Left = left; Right = right; }
}

public sealed class NotNode : FilterNode
{
    public FilterNode Operand { get; }
    public NotNode(FilterNode operand) => Operand = operand;
}

// ── Comparisons ───────────────────────────────────────────────────────────────

public enum CompareOp { Eq, Ne, Lt, Le, Gt, Ge }

/// <summary>property op value  e.g. Level = 'Error'</summary>
public sealed class CompareNode : FilterNode
{
    public string     Property { get; }
    public CompareOp  Op       { get; }
    public object?    Value    { get; }
    public CompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

// ── String predicates ─────────────────────────────────────────────────────────

/// <summary>@mt like '%hello%'  or  Prop like 'prefix%'</summary>
public sealed class LikeNode : FilterNode
{
    public string Property { get; }
    public string Pattern  { get; }
    public LikeNode(string property, string pattern) { Property = property; Pattern = pattern; }
}

/// <summary>@mt ci_startsWith 'Hello'  (case-insensitive prefix)</summary>
public sealed class StartsWithNode : FilterNode
{
    public string  Property        { get; }
    public string  Prefix          { get; }
    public bool    CaseInsensitive { get; }
    public StartsWithNode(string property, string prefix, bool ci = true)
    {
        Property        = property;
        Prefix          = prefix;
        CaseInsensitive = ci;
    }
}

/// <summary>@mt ci_contains 'hello'  (case-insensitive contains)</summary>
public sealed class ContainsNode : FilterNode
{
    public string Property        { get; }
    public string Text            { get; }
    public bool   CaseInsensitive { get; }
    public ContainsNode(string property, string text, bool ci = true)
    {
        Property        = property;
        Text            = text;
        CaseInsensitive = ci;
    }
}

// ── Level shorthand ───────────────────────────────────────────────────────────

/// <summary>@l = 'Error'  (sugar — resolved by parser from bare level names)</summary>
public sealed class LevelNode : FilterNode
{
    public Rd.Log.Core.LogLevel Level { get; }
    public LevelNode(Rd.Log.Core.LogLevel level) => Level = level;
}

// ── Existence ─────────────────────────────────────────────────────────────────

/// <summary>IsDefined(PropName)</summary>
public sealed class IsDefinedNode : FilterNode
{
    public string Property { get; }
    public IsDefinedNode(string property) => Property = property;
}

/// <summary>Has(PropName)  — alias for IsDefined</summary>
public sealed class HasNode : FilterNode
{
    public string Property { get; }
    public HasNode(string property) => Property = property;
}

// ── In list ───────────────────────────────────────────────────────────────────

/// <summary>Level in ['Error', 'Fatal']</summary>
public sealed class InNode : FilterNode
{
    public string    Property { get; }
    public object?[] Values   { get; }
    public InNode(string property, object?[] values) { Property = property; Values = values; }
}

// ── Wildcard ──────────────────────────────────────────────────────────────────

/// <summary>Match-all — returned when filter string is null/empty.</summary>
public sealed class MatchAllNode : FilterNode { }
