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
        var candidates = CollectAll(current, frozen, fromTicks, toTicks, afterTsTicks, afterIdRaw, forward, levels);
        candidates.Sort(forward ? Asc : Desc);

        foreach (var c in candidates)
            yield return c.Tier.Materialise(c.Index, pool);
    }

    /// <summary>
    /// Two passes over the fixed-size headers: count the matches, then fill an
    /// exactly-sized list. Pre-sizing to the WHOLE tier was one multi-MB LOH allocation
    /// per query and per 250 ms live-tail tick regardless of selectivity; the extra
    /// header walk is cheap (sequential native reads, nothing materialised) next to the
    /// growth churn the exact sizing was introduced against. Counts are snapshotted per
    /// tier once, so a publish landing between the passes cannot desynchronise them —
    /// writers publish an event by incrementing Count after the slot is fully written,
    /// so every index below the snapshot is safe to read.
    /// </summary>
    private static List<Candidate> CollectAll(
        HotTierSegment current,
        IReadOnlyList<HotTierSegment> frozen,
        long fromTicks, long toTicks,
        long? afterTs, ulong? afterId, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels)
    {
        int nTiers = frozen.Count + 1;
        var caps   = new int[nTiers];
        for (int t = 0; t < frozen.Count; t++) caps[t] = frozen[t].Count;
        caps[nTiers - 1] = current.Count;

        int matched = 0;
        for (int t = 0; t < frozen.Count; t++)
            matched += CountMatches(frozen[t], caps[t], fromTicks, toTicks, afterTs, afterId, forward, levels);
        matched += CountMatches(current, caps[nTiers - 1], fromTicks, toTicks, afterTs, afterId, forward, levels);

        var candidates = new List<Candidate>(matched);
        for (int t = 0; t < frozen.Count; t++)
            Collect(frozen[t], caps[t], candidates, fromTicks, toTicks, afterTs, afterId, forward, levels);
        Collect(current, caps[nTiers - 1], candidates, fromTicks, toTicks, afterTs, afterId, forward, levels);
        return candidates;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static bool Matches(
        in LogEventHeader h,
        long fromTicks, long toTicks,
        long? afterTs, ulong? afterId, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels)
    {
        long ts = h.TimestampUtcTicks;
        if (ts < fromTicks || ts > toTicks) return false;
        if (levels is not null && !levels.Contains(h.Level)) return false;
        return QueryCursor.After(ts, h.Id, afterTs, afterId, forward);
    }

    private static int CountMatches(
        HotTierSegment tier, int n,
        long fromTicks, long toTicks,
        long? afterTs, ulong? afterId, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels)
    {
        int matched = 0;
        for (int i = 0; i < n; i++)
            if (Matches(in tier.GetHeader(i), fromTicks, toTicks, afterTs, afterId, forward, levels))
                matched++;
        return matched;
    }

    private static void Collect(
        HotTierSegment tier, int n, List<Candidate> into,
        long fromTicks, long toTicks,
        long? afterTs, ulong? afterId, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels)
    {
        for (int i = 0; i < n; i++)
        {
            ref readonly var h = ref tier.GetHeader(i);
            if (Matches(in h, fromTicks, toTicks, afterTs, afterId, forward, levels))
                into.Add(new Candidate(tier, i, h.TimestampUtcTicks, h.Id));
        }
    }
}
