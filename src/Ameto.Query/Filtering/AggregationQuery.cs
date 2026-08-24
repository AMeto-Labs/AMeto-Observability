namespace Ameto.Query.Filtering;

/// <summary>What an aggregate does to the events in a group.</summary>
public enum AggregateKind
{
    /// <summary>How many events. With a property, how many carry a value for it.</summary>
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// <summary>
/// One column of an aggregation. <see cref="Property"/> is the encoded path the evaluator
/// reads (see <c>PropertyPath.Separator</c>) and is null only for <c>count(*)</c>.
/// </summary>
public sealed class AggregateSpec(AggregateKind kind, string? property, string alias)
{
    public AggregateKind Kind     { get; } = kind;
    public string?       Property { get; } = property;
    /// <summary>Column name on the wire — the source spelling, or whatever <c>as</c> renamed it to.</summary>
    public string        Alias    { get; } = alias;
}

/// <summary>One column the rows are grouped by.</summary>
public sealed class GroupKeySpec(string property, string alias)
{
    /// <summary>Encoded path, resolved exactly as a filter predicate resolves it.</summary>
    public string Property { get; } = property;
    public string Alias    { get; } = alias;
}

/// <summary>
/// A query that answers with a TABLE rather than a list of events:
/// <c>select count(*) where @l = 'Error' group by ['service.name'] limit 20</c>.
///
/// <para>The <c>where</c> clause is kept as its original text rather than a parsed tree, so the
/// scan it drives is the ordinary one — same compilation, same index hints, same level pruning,
/// same time-bound folding. An aggregation is a different way of reporting a scan, not a
/// different way of doing it.</para>
/// </summary>
public sealed class AggregationQuery
{
    /// <summary>Default cap on rows returned when the query does not say.</summary>
    public const int DefaultLimit = 100;

    public required IReadOnlyList<AggregateSpec> Aggregates { get; init; }
    public required IReadOnlyList<GroupKeySpec>  Keys       { get; init; }

    /// <summary>The <c>where</c> clause verbatim, or null. Compiled by the ordinary path.</summary>
    public string? FilterText { get; init; }

    /// <summary>Rows to return after ordering. Groups beyond it are counted, not sent.</summary>
    public int Limit { get; init; } = DefaultLimit;

    /// <summary>True when there is no <c>group by</c> — a single row over the whole window.</summary>
    public bool IsScalar => Keys.Count == 0;
}
