using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Header-level sorted scan over hot tiers: filters (@t window, level, pagination cursor)
/// and sorts on the fixed-size <see cref="LogEventHeader"/>s in native memory, then
/// materialises <see cref="LogEvent"/>s lazily in result order.
///
/// The previous query path materialised EVERY hot event (Dictionary + strings + payload
/// copy) on every query/live-poll and LINQ-sorted the objects; a typical page query now
/// allocates one candidate array plus only the events actually yielded.
/// </summary>
public static class HotTierScan
{
    private readonly record struct Candidate(HotTierSegment Tier, int Index, long Ts, ulong Id);

    private static readonly Comparison<Candidate> Asc = static (a, b) =>
    {
        int c = a.Ts.CompareTo(b.Ts);
        return c != 0 ? c : a.Id.CompareTo(b.Id);
    };

    private static readonly Comparison<Candidate> Desc = static (a, b) =>
    {
        int c = b.Ts.CompareTo(a.Ts);
        return c != 0 ? c : b.Id.CompareTo(a.Id);
    };

    /// <summary>
    /// Sorted, filtered scan across <paramref name="frozen"/> tiers plus
    /// <paramref name="current"/>. Sort key (@t, id) matches the cold-tier order, so the
    /// executor's k-way merge semantics are unchanged.
    /// </summary>
    public static IEnumerable<LogEvent> ReadSorted(
        HotTierSegment current,
        IReadOnlyList<HotTierSegment> frozen,
        StringInternPool? pool,
        long fromTicks, long toTicks,
        long? afterTsTicks, ulong? afterIdRaw, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels)
    {
        // Pre-size to the upper bound (all events pass the header filter): one exact
        // allocation instead of List growth churn — with tens of thousands of hot events
        // the doubling re-allocations were the scan's dominant remaining allocation.
        int upperBound = current.Count;
        for (int t = 0; t < frozen.Count; t++) upperBound += frozen[t].Count;

        var candidates = new List<Candidate>(upperBound);
        for (int t = 0; t < frozen.Count; t++)
            Collect(frozen[t], candidates, fromTicks, toTicks, afterTsTicks, afterIdRaw, forward, levels);
        Collect(current, candidates, fromTicks, toTicks, afterTsTicks, afterIdRaw, forward, levels);

        candidates.Sort(forward ? Asc : Desc);

        foreach (var c in candidates)
            yield return c.Tier.Materialise(c.Index, pool);
    }

    private static void Collect(
        HotTierSegment tier, List<Candidate> into,
        long fromTicks, long toTicks,
        long? afterTs, ulong? afterId, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels)
    {
        // Snapshot the count once: writers publish an event by incrementing Count after
        // the slot is fully written, so every index below the snapshot is safe to read.
        int n = tier.Count;
        for (int i = 0; i < n; i++)
        {
            ref readonly var h = ref tier.GetHeader(i);
            long ts = h.TimestampUtcTicks;
            if (ts < fromTicks || ts > toTicks) continue;
            if (levels is not null && !levels.Contains(h.Level)) continue;
            if (!QueryCursor.After(ts, h.Id, afterTs, afterId, forward)) continue;
            into.Add(new Candidate(tier, i, ts, h.Id));
        }
    }
}
