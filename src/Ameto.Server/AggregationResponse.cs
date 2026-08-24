using System.Text.Json.Serialization;

namespace Ameto.Server;

/// <summary>One row of an aggregation table: key values, then computed values.</summary>
internal sealed class AggregationRowDto
{
    /// <summary>Group key values, aligned with <see cref="AggregationResponse.KeyColumns"/>.
    /// Null means the event carried no value for that key.</summary>
    public string?[] Key { get; init; } = [];

    /// <summary>Computed values, aligned with <see cref="AggregationResponse.ValueColumns"/>.
    /// Null where the group had nothing to compute from — an average over no numbers is not 0.</summary>
    public double?[] Values { get; init; } = [];
}

/// <summary>Response body for <c>GET /api/events/aggregate</c>.</summary>
internal sealed class AggregationResponse
{
    public string From { get; init; } = "";
    public string To   { get; init; } = "";

    /// <summary>Column headings for the <c>group by</c> keys, in order.</summary>
    public string[] KeyColumns   { get; init; } = [];
    /// <summary>Column headings for the computed values, in order.</summary>
    public string[] ValueColumns { get; init; } = [];

    public AggregationRowDto[] Rows { get; init; } = [];

    /// <summary>Events read to produce this. Not the number matched.</summary>
    public long Scanned { get; init; }

    /// <summary>Distinct groups seen, which exceeds <see cref="Rows"/> when a limit applied.</summary>
    public int GroupsFound { get; init; }

    /// <summary>
    /// True when the numbers are floors rather than totals. A partial aggregation presented as
    /// a complete one is the failure this field exists to prevent: unlike a truncated list of
    /// events, a wrong total looks exactly like a right one.
    /// </summary>
    public bool Partial { get; init; }

    /// <summary>Why it is partial, in a sentence the client can show. Omitted when it is not.</summary>
    public string? PartialReason { get; init; }
}

/// <summary>Reflection-free serialization metadata for the aggregation endpoint.</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy   = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(AggregationResponse))]
internal partial class AggregationJsonContext : JsonSerializerContext;
