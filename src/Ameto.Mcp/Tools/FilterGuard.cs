using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Ameto.Core;
using Ameto.Mcp.Masking;
using Ameto.Query.Filtering;

namespace Ameto.Mcp.Tools;

/// <summary>
/// What stands between a filter the AI wrote and the query engine.
///
/// <para><b>A filter is a question, and a question can be about a hidden value.</b>
/// <c>CardNumber like '4400%'</c> returns no card number — and tells, by whether anything came
/// back, that one starts with 4400; a few hundred such questions spell the number out. So a
/// field whose value never leaves as stored may be tested for equality only (<c>=</c>,
/// <c>!=</c>, <c>in</c>, <c>has()</c>): against a token from an earlier answer, or against a
/// value the person already has. Free text is not restricted here — it has no field to judge it
/// by — and a profile that must not have even that is given published tools only.</para>
///
/// <para><b>Tokens become values on the server.</b> <c>'h:91c0e2aa5d17'</c> in a filter is
/// replaced by the value it stands for, from the <see cref="TokenVault"/>, after the filter as
/// written has been parsed and judged. Every message this class returns is about the filter AS
/// WRITTEN: the rewritten text holds raw values and is never quoted back.</para>
/// </summary>
internal static partial class FilterGuard
{
    /// <summary>The service the AI's own calls are logged under; no AI query returns its events unless a profile names it.</summary>
    public const string AuditService = "ameto-mcp";

    /// <summary>A filter ready to run, or the sentence that says why it will not.</summary>
    internal sealed record Prepared(string? Filter, bool FreeText, string? Error, RefusalKind Kind = RefusalKind.None)
    {
        /// <summary>The filter cannot be read: it does not parse, or it is not a filter at all.</summary>
        public static Prepared Unreadable(string error) => new(null, false, error, RefusalKind.Input);

        /// <summary>The filter reads well and asks what the masking rules do not answer.</summary>
        public static Prepared Hidden(string error) => new(null, false, error, RefusalKind.Masking);
    }

    [GeneratedRegex(@"'(h:[0-9a-f]{12})'", RegexOptions.CultureInvariant)]
    private static partial Regex TokenLiteral();

    /// <summary>Parses, judges and rewrites a search filter. An empty filter is ready as it is.</summary>
    public static Prepared Prepare(string? filter, MaskingPolicy policy, TokenVault vault)
    {
        if (string.IsNullOrWhiteSpace(filter)) return new Prepared(null, false, null);
        filter = filter.Trim();

        try
        {
            if (AggregationParser.TryParse(filter, out _))
                return Prepared.Unreadable("This is an aggregation: ask logs_aggregate for it.");
        }
        catch (Exception e)
        {
            return Prepared.Unreadable($"Invalid aggregation: {e.Message}");
        }

        FilterNode root;
        try
        {
            CompiledFilter.Compile(filter);
            root = FilterParser.Parse(filter);
        }
        catch (Exception e)
        {
            return Prepared.Unreadable($"Invalid filter: {e.Message}");
        }

        if (FindRefusal(root, policy) is { } refusal) return Prepared.Hidden(refusal);
        if (!TryResolveTokens(filter, vault, out string resolved, out string? unknown))
            return Prepared.Hidden(UnknownToken(unknown!));
        return new Prepared(resolved, root is FreeTextNode, null);
    }

    /// <summary>
    /// The same for a <c>select …</c> query: the where-clause is judged like a filter, and an
    /// aggregate other than a count may not be taken over a hidden field — <c>min(CardNumber)</c>
    /// is the card number.
    /// </summary>
    public static (AggregationQuery? Query, string? Error, RefusalKind Kind) PrepareAggregation(
        string? text, MaskingPolicy policy, TokenVault vault)
    {
        if (string.IsNullOrWhiteSpace(text))
            return (null, "'query' is empty. Example: select count(*) where @l = 'Error' group by @service", RefusalKind.Input);

        AggregationQuery asWritten;
        try { asWritten = AggregationParser.Parse(text); }
        catch (Exception e) { return (null, $"Invalid aggregation: {e.Message}", RefusalKind.Input); }

        foreach (var aggregate in asWritten.Aggregates)
            if (aggregate.Property is { } property && aggregate.Kind != AggregateKind.Count && IsHidden(policy, property))
                return (null, $"{PropertyPath.ToDisplay(property)} is a hidden field: it can be counted, not summed, averaged or ranged.",
                        RefusalKind.Masking);

        if (!string.IsNullOrWhiteSpace(asWritten.FilterText))
        {
            FilterNode where;
            try { where = FilterParser.Parse(asWritten.FilterText); }
            catch (Exception e) { return (null, $"Invalid filter: {e.Message}", RefusalKind.Input); }
            if (FindRefusal(where, policy) is { } refusal) return (null, refusal, RefusalKind.Masking);
        }

        if (!TryResolveTokens(text, vault, out string resolved, out string? unknown))
            return (null, UnknownToken(unknown!), RefusalKind.Masking);
        try { return (ReferenceEquals(resolved, text) ? asWritten : AggregationParser.Parse(resolved), null, RefusalKind.None); }
        catch (Exception) { return (null, "The query could not be run with the values its tokens stand for.", RefusalKind.Input); }
    }

    /// <summary>
    /// <paramref name="filter"/> narrowed to what a profile may see: its services, or — for a
    /// profile that sees every service — everything but the AI's own audit trail. Null when the
    /// filter is free text, which no predicate can be joined to: the caller then drops the rows
    /// itself.
    /// </summary>
    public static string? Scope(string? filter, bool freeText, McpProfile profile)
    {
        if (freeText) return null;

        var scope = new StringBuilder();
        if (profile.Services.Count == 0)
            scope.Append("@service <> '").Append(AuditService).Append('\'');
        else
        {
            scope.Append("@service in [");
            for (int i = 0; i < profile.Services.Count; i++)
            {
                if (i > 0) scope.Append(", ");
                scope.Append(Literal(profile.Services[i], isNumber: false));
            }
            scope.Append(']');
        }
        return string.IsNullOrWhiteSpace(filter) ? scope.ToString() : $"({filter}) and {scope}";
    }

    /// <summary>True when the profile may ask about <paramref name="service"/> by name.</summary>
    public static bool MaySee(McpProfile profile, string service) => profile.Services.Count == 0
        ? !service.Equals(AuditService, StringComparison.OrdinalIgnoreCase)
        : profile.SeesService(service);

    /// <summary><paramref name="value"/> as a string literal of the filter language.</summary>
    public static string Quote(string value) => Literal(value, isNumber: false);

    /// <summary>Two filter clauses joined; either may be absent.</summary>
    public static string? And(string? left, string? right) =>
        string.IsNullOrWhiteSpace(left) ? right : string.IsNullOrWhiteSpace(right) ? left : $"({left}) and {right}";

    /// <summary>True when the profile may be shown this event at all.</summary>
    public static bool InScope(McpProfile profile, LogEvent ev) => profile.Services.Count == 0
        ? !string.Equals(ev.ServiceName, AuditService, StringComparison.OrdinalIgnoreCase)
        : profile.SeesService(ev.ServiceName);

    // ── Tokens ────────────────────────────────────────────────────────────────

    private static bool TryResolveTokens(string text, TokenVault vault, out string resolved, out string? unknown)
    {
        resolved = text;
        unknown  = null;
        if (!text.Contains("'h:", StringComparison.Ordinal)) return true;

        string? missing = null;
        resolved = TokenLiteral().Replace(text, match =>
        {
            string token = match.Groups[1].Value;
            if (MaskToken.TryParse(token, out ulong bits) && vault.TryResolve(bits, out var entry))
                return Literal(entry.Text, entry.IsNumber);
            missing ??= token;
            return match.Value;
        });
        unknown = missing;
        return missing is null;
    }

    private static string UnknownToken(string token) =>
        $"{token} is not a token this server remembers. Tokens can be searched for a limited time after an answer contained them: run the search that produced it again.";

    /// <summary>A value as a filter literal. The lexer takes a backslash as "the next character as it is".</summary>
    private static string Literal(string value, bool isNumber)
    {
        if (isNumber && double.TryParse(value, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double number) && double.IsFinite(number))
            return value;
        return string.Concat("'", value.Replace("\\", "\\\\").Replace("'", "\\'"), "'");
    }

    // ── Hidden fields ─────────────────────────────────────────────────────────

    private static string? FindRefusal(FilterNode node, MaskingPolicy policy)
    {
        switch (node)
        {
            case AndNode and: return FindRefusal(and.Left, policy) ?? FindRefusal(and.Right, policy);
            case OrNode or:   return FindRefusal(or.Left, policy)  ?? FindRefusal(or.Right, policy);
            case NotNode not: return FindRefusal(not.Operand, policy);

            case CompareNode compare:
                if (compare.RightProperty is { } right)
                {
                    // Two fields compared with each other: neither may be a hidden one.
                    if (IsHidden(policy, compare.Property)) return Refusal(compare.Property);
                    if (IsHidden(policy, right))            return Refusal(right);
                    return null;
                }
                return compare.Op is CompareOp.Eq or CompareOp.Ne || !IsHidden(policy, compare.Property)
                    ? null
                    : Refusal(compare.Property);

            // Equality, presence, and the nodes that name no user property.
            case InNode or HasNode or IsDefinedNode or MatchAllNode or LevelNode or FreeTextNode
                or TimeCompareNode or TraceIdCompareNode:
                return null;

            default:
                // Everything else — like, contains, startsWith, length, substring, regex, the
                // several dozen function predicates — reads the value in a way that tells
                // something about it.
                foreach (string property in PropertiesOf(node))
                    if (IsHidden(policy, property)) return Refusal(property);
                return null;
        }
    }

    private static string Refusal(string property) =>
        $"{PropertyPath.ToDisplay(property)} is a hidden field: a filter may only test it with =, !=, in or has(). " +
        "Compare it with a token from an earlier answer (for example 'h:91c0e2aa5d17') or with a value you already have.";

    private static bool IsHidden(MaskingPolicy policy, string encodedPath)
    {
        if (BuiltinFields.TryResolve(encodedPath, out _)) return false;
        string display = PropertyPath.ToDisplay(PropertyPath.StripIndexSegments(encodedPath));
        return policy.HidesField(display);
    }

    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> NodeMembers = new();

    /// <summary>
    /// The properties a node reads, found by shape rather than by a list of node types: a string
    /// member whose name ends in <c>Property</c>, or a list of argument nodes that have one.
    /// A node type added to the query language later is covered without anyone remembering this
    /// file, which is the point — a list would fail open.
    /// </summary>
    private static IEnumerable<string> PropertiesOf(object node)
    {
        var members = NodeMembers.GetOrAdd(node.GetType(),
            static type => [.. type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                   .Where(static p => p.GetIndexParameters().Length == 0)]);
        foreach (var member in members)
        {
            if (member.PropertyType == typeof(string))
            {
                if (member.Name.EndsWith("Property", StringComparison.Ordinal) && member.GetValue(node) is string name)
                    yield return name;
            }
            else if (typeof(IEnumerable).IsAssignableFrom(member.PropertyType) && member.GetValue(node) is IEnumerable items)
            {
                foreach (object? item in items)
                {
                    if (item is null or string || item.GetType().IsPrimitive) continue;
                    foreach (string nested in PropertiesOf(item)) yield return nested;
                }
            }
        }
    }
}
