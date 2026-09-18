namespace Ameto.Core;

/// <summary>
/// A pre-check the hot-tier scan runs on the fixed-size <see cref="LogEventHeader"/> BEFORE
/// it materialises the event. Three-valued on purpose: <c>false</c> means the event cannot
/// match the filter it was derived from, <c>true</c> means it MAY — the evaluator still runs
/// on every event the scan yields, so an implementation can only ever skip work, never add
/// a match or drop one.
///
/// <para>Why it exists: without it, every hot event inside the window was materialised
/// (a <c>LogEvent</c>, a payload copy, pool lookups) only for a filter such as
/// <c>@l = 'Error'</c> to reject 99 % of them. The header already carries the level, the
/// service pool index and the 128-bit trace / 64-bit span ids, so those predicates can be
/// answered from the 64-byte header alone.</para>
///
/// <para>The service part is split out because the header holds a pool INDEX, not the name;
/// the scan resolves it and memoises the verdict per index, so a query over three services
/// pays three string comparisons, not one per event. Implementations must be immutable and
/// safe to share across concurrent scans.</para>
/// </summary>
public interface IHotHeaderPredicate
{
    /// <summary>Level / trace id / span id part. <c>false</c> = cannot match.</summary>
    bool MayMatch(in LogEventHeader header);

    /// <summary>True when <see cref="ServiceMayMatch"/> must be consulted as well.</summary>
    bool HasServicePredicate { get; }

    /// <summary>
    /// Verdict on the resolved <c>service.name</c> alone (<c>null</c> when the event carries
    /// none — the same value the materialised event would expose). <c>false</c> = cannot match.
    /// </summary>
    bool ServiceMayMatch(string? serviceName);
}
