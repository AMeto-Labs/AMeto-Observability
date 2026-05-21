namespace Rd.Log.Core;

/// <summary>
/// Metadata about a cold-tier segment file.
/// </summary>
public sealed class SegmentInfo
{
    public required SegmentId  Id               { get; init; }
    public required NodeId     NodeId           { get; init; }
    public required string     FilePath         { get; init; }
    public required long       MinTimestampTicks { get; init; }
    public required long       MaxTimestampTicks { get; init; }
    public required uint       EventCount        { get; init; }
    public required LogLevel   MinLevel          { get; init; }
    public required long       CompressedBytes   { get; init; }
    public required long       UncompressedBytes { get; init; }

    public DateTimeOffset MinTimestamp => new(MinTimestampTicks, TimeSpan.Zero);
    public DateTimeOffset MaxTimestamp => new(MaxTimestampTicks, TimeSpan.Zero);

    /// <summary>True when all events in this segment are past their retention deadline.</summary>
    public bool IsExpired(RetentionPolicy policy, DateTimeOffset now) =>
        new DateTimeOffset(MaxTimestampTicks, TimeSpan.Zero)
            .Add(policy.GetTtl(MinLevel)) < now;
}

/// <summary>
/// Describes the query range for retrieving events.
/// </summary>
public sealed class QueryRequest
{
    public string?         Filter        { get; init; }  // Seq Filter Expression
    public DateTimeOffset? FromUtc       { get; init; }
    public DateTimeOffset? ToUtc         { get; init; }
    public int             Count         { get; init; } = 100;
    public QueryDirection  Direction     { get; init; } = QueryDirection.Backward;
    public EventId?        AfterEventId  { get; init; }  // cursor: tiebreaker EventId (paired with AfterTimestampTicks)
    /// <summary>
    /// Cursor: timestamp of the last delivered event (UtcTicks). Combined with
    /// <see cref="AfterEventId"/> as a lexicographic <c>(ts, id)</c> pair, which is the
    /// only ordering that is consistent across hot tier, late-arriving events, and
    /// k-way merge across overlapping cold-tier segments.
    /// </summary>
    public long?           AfterTimestampTicks { get; init; }
    /// <summary>Optional allow-list of log levels. Null = all levels.</summary>
    public HashSet<LogLevel>? Levels      { get; init; }
}

public enum QueryDirection
{
    Backward,   // newest first (default, like Seq)
    Forward,    // oldest first (for live tail / streaming)
}
