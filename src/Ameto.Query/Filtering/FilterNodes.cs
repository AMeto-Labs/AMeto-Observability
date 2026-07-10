namespace Ameto.Query.Filtering;

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

    // Pre-computed at parse time to avoid per-event allocations in hot path:
    internal readonly string PatternLower; // ToLowerInvariant once, not per event
    internal readonly bool   IsMatchAll;   // pattern == "%"
    internal readonly bool   IsLiteral;    // no % or _ wildcards — plain equality

    public LikeNode(string property, string pattern)
    {
        Property     = property;
        Pattern      = pattern;
        PatternLower = pattern.ToLowerInvariant();
        IsMatchAll   = PatternLower == "%";
        IsLiteral    = !IsMatchAll && !PatternLower.Contains('%') && !PatternLower.Contains('_');
    }
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

/// <summary>@mt endsWith 'done'  or  ci_endsWith equivalent via CaseInsensitive=true.</summary>
public sealed class EndsWithNode : FilterNode
{
    public string Property        { get; }
    public string Suffix          { get; }
    public bool   CaseInsensitive { get; }
    public EndsWithNode(string property, string suffix, bool ci = false)
    {
        Property        = property;
        Suffix          = suffix;
        CaseInsensitive = ci;
    }
}

// ── Scalar function compare nodes ───────────────────────────────────────────

public abstract class CoalesceArgNode { }

public sealed class CoalescePropertyArgNode : CoalesceArgNode
{
    public string Property { get; }
    public CoalescePropertyArgNode(string property) => Property = property;
}

public sealed class CoalesceLiteralArgNode : CoalesceArgNode
{
    public object? Value { get; }
    public CoalesceLiteralArgNode(object? value) => Value = value;
}

public sealed class LengthCompareNode : FilterNode
{
    public string    Property { get; }
    public CompareOp Op       { get; }
    public object?   Value    { get; }
    public LengthCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class ToJsonCompareNode : FilterNode
{
    public string    Property { get; }
    public CompareOp Op       { get; }
    public object?   Value    { get; }
    public ToJsonCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class FromJsonCompareNode : FilterNode
{
    public string    Property { get; }
    public CompareOp Op       { get; }
    public object?   Value    { get; }
    public FromJsonCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

/// <summary>fromJson(Prop).path = value  — direct comparison after path navigation.</summary>
public sealed class FromJsonPathCompareNode : FilterNode
{
    public string    Property { get; }
    public string[]  Path     { get; }
    public CompareOp Op       { get; }
    public object?   Value    { get; }
    public FromJsonPathCompareNode(string property, string[] path, CompareOp op, object? value)
    {
        Property = property;
        Path     = path;
        Op       = op;
        Value    = value;
    }
}

/// <summary>fromJson(Prop).path like 'pattern%'</summary>
public sealed class FromJsonPathLikeNode : FilterNode
{
    public string   Property { get; }
    public string[] Path     { get; }
    public string   Pattern  { get; }
    public FromJsonPathLikeNode(string property, string[] path, string pattern)
    {
        Property = property;
        Path     = path;
        Pattern  = pattern;
    }
}

/// <summary>fromJson(Prop).path in ['a','b']</summary>
public sealed class FromJsonPathInNode : FilterNode
{
    public string    Property { get; }
    public string[]  Path     { get; }
    public object?[] Values   { get; }
    public FromJsonPathInNode(string property, string[] path, object?[] values)
    {
        Property = property;
        Path     = path;
        Values   = values;
    }
}

/// <summary>has(fromJson(Prop).path)  — field existence check inside parsed JSON.</summary>
public sealed class FromJsonPathHasNode : FilterNode
{
    public string   Property { get; }
    public string[] Path     { get; }
    public FromJsonPathHasNode(string property, string[] path)
    {
        Property = property;
        Path     = path;
    }
}

public enum FromJsonPathPredicateKind { StartsWith, Contains, EndsWith }

/// <summary>startsWith/contains/endsWith on a navigated fromJson path field.</summary>
public sealed class FromJsonPathStringPredicateNode : FilterNode
{
    public string                    Property        { get; }
    public string[]                  Path            { get; }
    public string                    Arg             { get; }
    public bool                      CaseInsensitive { get; }
    public FromJsonPathPredicateKind Kind            { get; }
    public FromJsonPathStringPredicateNode(string property, string[] path, string arg, bool ci, FromJsonPathPredicateKind kind)
    {
        Property        = property;
        Path            = path;
        Arg             = arg;
        CaseInsensitive = ci;
        Kind            = kind;
    }
}

public sealed class CoalesceCompareNode : FilterNode
{
    public IReadOnlyList<CoalesceArgNode> Args { get; }
    public CompareOp                       Op   { get; }
    public object?                         Value { get; }
    public CoalesceCompareNode(IReadOnlyList<CoalesceArgNode> args, CompareOp op, object? value)
    {
        Args  = args;
        Op    = op;
        Value = value;
    }
}

public sealed class ToLowerCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ToLowerCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class ToUpperCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ToUpperCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class ToNumberCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ToNumberCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class SubstringCompareNode : FilterNode
{
    public string Property { get; }
    public int Start       { get; }
    public int? Length     { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public SubstringCompareNode(string property, int start, int? length, CompareOp op, object? value)
    {
        Property = property;
        Start    = start;
        Length   = length;
        Op       = op;
        Value    = value;
    }
}

public sealed class IndexOfCompareNode : FilterNode
{
    public string Property  { get; }
    public string Substring { get; }
    public bool LastIndex   { get; }
    public CompareOp Op     { get; }
    public object? Value    { get; }
    public IndexOfCompareNode(string property, string substring, bool lastIndex, CompareOp op, object? value)
    {
        Property  = property;
        Substring = substring;
        LastIndex = lastIndex;
        Op        = op;
        Value     = value;
    }
}

public sealed class ReplaceCompareNode : FilterNode
{
    public string Property    { get; }
    public string Substring   { get; }
    public string Replacement { get; }
    public CompareOp Op       { get; }
    public object? Value      { get; }
    public ReplaceCompareNode(string property, string substring, string replacement, CompareOp op, object? value)
    {
        Property    = property;
        Substring   = substring;
        Replacement = replacement;
        Op          = op;
        Value       = value;
    }
}

public abstract class ConcatArgNode { }

public sealed class ConcatPropertyArgNode : ConcatArgNode
{
    public string Property { get; }
    public ConcatPropertyArgNode(string property) => Property = property;
}

public sealed class ConcatLiteralArgNode : ConcatArgNode
{
    public object? Value { get; }
    public ConcatLiteralArgNode(object? value) => Value = value;
}

public sealed class ConcatCompareNode : FilterNode
{
    public IReadOnlyList<ConcatArgNode> Args { get; }
    public CompareOp Op                      { get; }
    public object? Value                     { get; }
    public ConcatCompareNode(IReadOnlyList<ConcatArgNode> args, CompareOp op, object? value)
    {
        Args  = args;
        Op    = op;
        Value = value;
    }
}

public sealed class TypeOfCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public TypeOfCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class ElementAtCompareNode : FilterNode
{
    public string Property { get; }
    public object? Index   { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ElementAtCompareNode(string property, object? index, CompareOp op, object? value)
    {
        Property = property;
        Index    = index;
        Op       = op;
        Value    = value;
    }
}

public sealed class KeysCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public KeysCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class ValuesCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ValuesCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class RoundCompareNode : FilterNode
{
    public string Property { get; }
    public int Places      { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public RoundCompareNode(string property, int places, CompareOp op, object? value)
    {
        Property = property;
        Places   = places;
        Op       = op;
        Value    = value;
    }
}

public sealed class NowCompareNode : FilterNode
{
    public CompareOp Op  { get; }
    public object? Value { get; }
    public NowCompareNode(CompareOp op, object? value)
    {
        Op    = op;
        Value = value;
    }
}

public sealed class DateTimeCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public DateTimeCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

public sealed class ToIsoStringCompareNode : FilterNode
{
    public string Property { get; }
    public int? OffsetHours { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ToIsoStringCompareNode(string property, int? offsetHours, CompareOp op, object? value)
    {
        Property = property;
        OffsetHours = offsetHours;
        Op       = op;
        Value    = value;
    }
}

public sealed class DatePartCompareNode : FilterNode
{
    public string Property { get; }
    public string Part     { get; }
    public int? OffsetHours { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public DatePartCompareNode(string property, string part, int? offsetHours, CompareOp op, object? value)
    {
        Property = property;
        Part = part;
        OffsetHours = offsetHours;
        Op = op;
        Value = value;
    }
}

public sealed class TimeOfDayCompareNode : FilterNode
{
    public string Property { get; }
    public int OffsetHours { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public TimeOfDayCompareNode(string property, int offsetHours, CompareOp op, object? value)
    {
        Property = property;
        OffsetHours = offsetHours;
        Op = op;
        Value = value;
    }
}

public sealed class TimeSpanCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public TimeSpanCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op = op;
        Value = value;
    }
}

public sealed class TotalMillisecondsCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public TotalMillisecondsCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op = op;
        Value = value;
    }
}

public sealed class ToTimeStringCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ToTimeStringCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op = op;
        Value = value;
    }
}

public sealed class ToHexStringCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ToHexStringCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op = op;
        Value = value;
    }
}

public sealed class BucketCompareNode : FilterNode
{
    public string Property { get; }
    public double Error    { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public BucketCompareNode(string property, double error, CompareOp op, object? value)
    {
        Property = property;
        Error = error;
        Op = op;
        Value = value;
    }
}

public sealed class OffsetInCompareNode : FilterNode
{
    public string TimeZoneId { get; }
    public string InstantProperty { get; }
    public CompareOp Op      { get; }
    public object? Value     { get; }
    public OffsetInCompareNode(string timeZoneId, string instantProperty, CompareOp op, object? value)
    {
        TimeZoneId = timeZoneId;
        InstantProperty = instantProperty;
        Op = op;
        Value = value;
    }
}

public sealed class ArrivedCompareNode : FilterNode
{
    public string Property { get; }
    public CompareOp Op    { get; }
    public object? Value   { get; }
    public ArrivedCompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op = op;
        Value = value;
    }
}

// ── Level shorthand ───────────────────────────────────────────────────────────

/// <summary>@l = 'Error'  (sugar — resolved by parser from bare level names)</summary>
public sealed class LevelNode : FilterNode
{
    public Ameto.Core.LogLevel Level { get; }
    public LevelNode(Ameto.Core.LogLevel level) => Level = level;
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

// ── Free-text search ──────────────────────────────────────────────────────────

/// <summary>
/// Bare free-text search — produced by the parser when the user types plain words
/// into the filter box (e.g. <c>balance</c> or <c>user balance</c>) rather than a
/// structured expression. Matches when EVERY term is found, case-insensitively and
/// as a substring, in one of the event's trigram-indexed text fields: the message
/// template, the exception type/message, or any (recursively flattened) string
/// property value.
///
/// Terms of 3+ characters are surfaced as trigram hints
/// (<see cref="CompiledFilter.GetTrigramHints"/>), so cold-tier segments are skipped
/// via the index instead of decompressed and scanned — that is what keeps the search
/// fast. The matched field set is deliberately kept identical to what
/// <c>SegmentIndexBuilder</c> feeds the trigram index, so index-accelerated
/// (cold-tier) and full-scan (hot-tier) queries always return the same events.
/// </summary>
public sealed class FreeTextNode : FilterNode
{
    /// <summary>Search terms — AND semantics, all must match. Original case preserved.</summary>
    public string[] Terms { get; }
    public FreeTextNode(string[] terms) => Terms = terms;
}

// ── New scalar nodes ──────────────────────────────────────────────────────────

/// <summary>fromXml(Prop, 'xpath') op value — extract value via XPath from XML string property.</summary>
public sealed class FromXmlCompareNode : FilterNode
{
    public string    Property { get; }
    public string    XPath    { get; }
    public CompareOp Op       { get; }
    public object?   Value    { get; }
    public FromXmlCompareNode(string property, string xpath, CompareOp op, object? value)
    {
        Property = property;
        XPath    = xpath;
        Op       = op;
        Value    = value;
    }
}

/// <summary>fromBase64(Prop) op value — decode Base64 string then compare.</summary>
public sealed class FromBase64CompareNode : FilterNode
{
    public string    Property { get; }
    public CompareOp Op       { get; }
    public object?   Value    { get; }
    public FromBase64CompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

/// <summary>toBase64(Prop) op value — encode property value as Base64 then compare.</summary>
public sealed class ToBase64CompareNode : FilterNode
{
    public string    Property { get; }
    public CompareOp Op       { get; }
    public object?   Value    { get; }
    public ToBase64CompareNode(string property, CompareOp op, object? value)
    {
        Property = property;
        Op       = op;
        Value    = value;
    }
}

/// <summary>regexMatch(Prop, '/pattern/') — returns true when the property matches.</summary>
public sealed class RegexMatchNode : FilterNode
{
    public string Property { get; }
    public string Pattern  { get; }
    public bool   IgnoreCase { get; }
    public RegexMatchNode(string property, string pattern, bool ignoreCase = false)
    {
        Property   = property;
        Pattern    = pattern;
        IgnoreCase = ignoreCase;
    }
}

/// <summary>regexExtract(Prop, '/pattern/', group?) op value — extract a regex capture group then compare.</summary>
public sealed class RegexExtractCompareNode : FilterNode
{
    public string    Property { get; }
    public string    Pattern  { get; }
    public int       Group    { get; }
    public bool      IgnoreCase { get; }
    public CompareOp Op       { get; }
    public object?   Value    { get; }
    public RegexExtractCompareNode(string property, string pattern, int group, bool ignoreCase, CompareOp op, object? value)
    {
        Property   = property;
        Pattern    = pattern;
        Group      = group;
        IgnoreCase = ignoreCase;
        Op         = op;
        Value      = value;
    }
}
