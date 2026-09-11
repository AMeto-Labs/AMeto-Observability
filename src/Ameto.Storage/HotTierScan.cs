using System.Runtime.CompilerServices;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Header-level sorted scan over hot tiers: filters (@t window, level, pagination cursor,
/// and whatever of the filter the header can answer) and sorts on the fixed-size
/// <see cref="LogEventHeader"/>s in native memory, then materialises <see cref="LogEvent"/>s
/// lazily in result order.
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
        IReadOnlySet<Ameto.Core.LogLevel>? levels,
        IHotHeaderPredicate? headerPredicate = null)
    {
        var candidates = CollectAll(current, frozen, pool, fromTicks, toTicks, afterTsTicks, afterIdRaw, forward, levels, headerPredicate);
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
        StringInternPool? pool,
        long fromTicks, long toTicks,
        long? afterTs, ulong? afterId, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels,
        IHotHeaderPredicate? pred)
    {
        int nTiers = frozen.Count + 1;
        var caps   = new int[nTiers];
        for (int t = 0; t < frozen.Count; t++) caps[t] = frozen[t].Count;
        caps[nTiers - 1] = current.Count;

        var scan = new ScanState(pool, fromTicks, toTicks, afterTs, afterId, forward, levels, pred);

        int matched = 0;
        for (int t = 0; t < frozen.Count; t++)
            matched += CountMatches(frozen[t], caps[t], ref scan);
        matched += CountMatches(current, caps[nTiers - 1], ref scan);

        var candidates = new List<Candidate>(matched);
        for (int t = 0; t < frozen.Count; t++)
            Collect(frozen[t], caps[t], candidates, ref scan);
        Collect(current, caps[nTiers - 1], candidates, ref scan);
        return candidates;
    }

    /// <summary>
    /// Everything a header is tested against, plus the per-scan memo of service verdicts.
    /// A struct on the scan's frame: the predicate itself stays immutable and shareable,
    /// the mutable part lives here for the duration of one scan.
    /// </summary>
    private struct ScanState
    {
        public readonly StringInternPool?         Pool;
        public readonly long                      FromTicks, ToTicks;
        public readonly long?                     AfterTs;
        public readonly ulong?                    AfterId;
        public readonly bool                      Forward;
        public readonly IReadOnlySet<LogLevel>?   Levels;
        public readonly ulong                     LevelMask;   // bit per level < 64; ~0 = unconstrained
        public readonly IHotHeaderPredicate?      Pred;
        public readonly bool                      HasService;
        private ServiceMemo                       _memo;

        public ScanState(
            StringInternPool? pool, long fromTicks, long toTicks, long? afterTs, ulong? afterId,
            bool forward, IReadOnlySet<LogLevel>? levels, IHotHeaderPredicate? pred)
        {
            Pool       = pool;
            FromTicks  = fromTicks;
            ToTicks    = toTicks;
            AfterTs    = afterTs;
            AfterId    = afterId;
            Forward    = forward;
            Levels     = levels;
            LevelMask  = BuildLevelMask(levels);
            Pred       = pred;
            HasService = pred is not null && pred.HasServicePredicate;
            _memo      = default;
        }

        /// <summary>
        /// One bit per allowed level so the per-header check is a shift, not a
        /// <see cref="HashSet{T}.Contains"/>; a level byte ≥ 64 (never produced by ingest)
        /// falls back to the set.
        /// </summary>
        private static ulong BuildLevelMask(IReadOnlySet<LogLevel>? levels)
        {
            if (levels is null) return ~0UL;
            ulong mask = 0;
            foreach (var l in levels)
                if ((byte)l < 64) mask |= 1UL << (byte)l;
            return mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Matches(in LogEventHeader h)
        {
            long ts = h.TimestampUtcTicks;
            if (ts < FromTicks || ts > ToTicks) return false;

            int lvl = (byte)h.Level;
            if (lvl < 64 ? ((LevelMask >> lvl) & 1) == 0 : Levels is not null && !Levels.Contains(h.Level))
                return false;

            if (!QueryCursor.After(ts, h.Id, AfterTs, AfterId, Forward)) return false;

            // Filter pushdown — after the cheap tests, before anything is materialised.
            if (Pred is not null)
            {
                if (!Pred.MayMatch(in h)) return false;
                if (HasService && !ServiceMayMatch(h.ServiceNamePoolIndex)) return false;
            }
            return true;
        }

        /// <summary>
        /// The service verdict per pool index, memoised: a tier holds a handful of distinct
        /// services, so the string comparison runs once per service, not once per event.
        /// The name is resolved exactly as <c>HotTierSegment.MaterialiseEvent</c> resolves it,
        /// so the verdict is the one the evaluator would reach on the materialised event.
        /// A pool miss (empty string) is not memoised — it is the one resolution that can
        /// change while the scan runs.
        /// </summary>
        private bool ServiceMayMatch(int poolIndex)
        {
            int slot = poolIndex & (ServiceMemo.Slots - 1);
            if (_memo.Verdicts[slot] != 0 && _memo.Keys[slot] == poolIndex)
                return _memo.Verdicts[slot] == ServiceMemo.Maybe;

            string? name = poolIndex >= 0 && Pool is not null ? Pool.Get(poolIndex) : null;
            bool verdict = Pred!.ServiceMayMatch(name);
            if (name is not { Length: 0 })
            {
                _memo.Keys[slot]     = poolIndex;
                _memo.Verdicts[slot] = verdict ? ServiceMemo.Maybe : ServiceMemo.No;
            }
            return verdict;
        }
    }

    /// <summary>Direct-mapped, 16 entries, no heap: keys and verdicts in inline arrays.</summary>
    private struct ServiceMemo
    {
        public const int  Slots = 16;
        public const byte No    = 1;
        public const byte Maybe = 2;

        public Keys16     Keys;
        public Verdicts16 Verdicts;

        [InlineArray(Slots)] public struct Keys16     { private int  _e0; }
        [InlineArray(Slots)] public struct Verdicts16 { private byte _e0; }
    }

    private static int CountMatches(HotTierSegment tier, int n, ref ScanState scan)
    {
        int matched = 0;
        for (int i = 0; i < n; i++)
            if (scan.Matches(in tier.GetHeader(i)))
                matched++;
        return matched;
    }

    private static void Collect(HotTierSegment tier, int n, List<Candidate> into, ref ScanState scan)
    {
        for (int i = 0; i < n; i++)
        {
            ref readonly var h = ref tier.GetHeader(i);
            if (scan.Matches(in h))
                into.Add(new Candidate(tier, i, h.TimestampUtcTicks, h.Id));
        }
    }
}
