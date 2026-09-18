using System.Buffers;
using System.Runtime.CompilerServices;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Header-level sorted scan over hot tiers: filters (@t window, level, pagination cursor,
/// and whatever of the filter the header can answer) and orders on the fixed-size
/// <see cref="LogEventHeader"/>s in native memory, then materialises <see cref="LogEvent"/>s
/// lazily in result order.
///
/// <para>Cost model, per call: the headers of every CHUNK whose zone map
/// (<see cref="HotTierSegment.ChunkMayOverlap"/>) intersects the window are walked once —
/// a live-tail poll with its cursor in the newest chunk reads that chunk, not the tier;
/// candidates go into a pooled buffer; ordering is a heap built in O(n) and popped
/// lazily, so a page of k costs O(n + k log n), not a full sort of every candidate. Only
/// the events actually yielded are materialised.</para>
/// </summary>
public static class HotTierScan
{
    /// <summary>24 bytes, no references: the tier is an ordinal (frozen first, current last).</summary>
    private readonly record struct Candidate(int Tier, int Index, long Ts, ulong Id);

    private const int ChunkCap = HotTierSegment.ChunkEventCapacity;

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
        // Rented here, inside the frame the finally below protects, so a throw anywhere in
        // the collection (the predicate, a growth) still returns it.
        var buf = ArrayPool<Candidate>.Shared.Rent(256);
        try
        {
            int n = Collect(current, frozen, pool, fromTicks, toTicks, afterTsTicks, afterIdRaw, forward, levels, headerPredicate, ref buf);
            if (n == 0) yield break;

            // Min-heap in the requested order: the next event to yield is always at the
            // root. Building it is O(n); each pop O(log n).
            Heapify(buf, n, forward);

            int remaining = n, popped = 0;
            while (remaining > 0)
            {
                // Once a sizeable share has been popped the consumer is evidently reading
                // deep (a wide page, an aggregation), and one sort of what is left beats
                // paying a cache-missing pop per remaining element. Same order either way.
                if (remaining > 64 && popped * 8 > remaining)
                {
                    var rest = buf.AsSpan(0, remaining);
                    if (forward) rest.Sort(default(AscComparer)); else rest.Sort(default(DescComparer));
                    for (int i = 0; i < remaining; i++)
                        yield return Materialise(current, frozen, pool, in buf[i]);
                    yield break;
                }

                var top = buf[0];
                remaining--;
                if (remaining > 0)
                {
                    buf[0] = buf[remaining];
                    SiftDown(buf, 0, remaining, forward);
                }
                popped++;
                yield return Materialise(current, frozen, pool, in top);
            }
        }
        finally
        {
            ArrayPool<Candidate>.Shared.Return(buf);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static LogEvent Materialise(HotTierSegment current, IReadOnlyList<HotTierSegment> frozen, StringInternPool? pool, in Candidate c)
        => (c.Tier < frozen.Count ? frozen[c.Tier] : current).Materialise(c.Index, pool);

    // ── Collection ────────────────────────────────────────────────────────────

    /// <summary>
    /// One pass over the headers of every chunk the zone map cannot rule out. Counts are
    /// snapshotted per tier once; writers publish an event by incrementing Count after
    /// the slot is fully written, so every index below the snapshot is safe to read. The
    /// buffer comes from the shared pool and grows by doubling — a selective predicate
    /// over a large window ends with a small array, a wide unfiltered one with at most
    /// 2n slots, and neither leaves a multi-MB list for the GC on every poll.
    /// </summary>
    private static int Collect(
        HotTierSegment current,
        IReadOnlyList<HotTierSegment> frozen,
        StringInternPool? pool,
        long fromTicks, long toTicks,
        long? afterTs, ulong? afterId, bool forward,
        IReadOnlySet<Ameto.Core.LogLevel>? levels,
        IHotHeaderPredicate? pred,
        ref Candidate[] buf)
    {
        var scan = new ScanState(pool, fromTicks, toTicks, afterTs, afterId, forward, levels, pred);

        // The cursor is a bound too: forward, nothing before its tick can pass the cursor
        // check (ties go through the id, which is why the header check keeps the exact
        // test); so a chunk entirely before it is skipped like one outside the window.
        // Mirror image backward.
        long zoneFrom = fromTicks, zoneTo = toTicks;
        if (afterTs is long cursor)
        {
            if (forward) { if (cursor > zoneFrom) zoneFrom = cursor; }
            else         { if (cursor < zoneTo)   zoneTo   = cursor; }
        }

        int n = 0;
        int nFrozen = frozen.Count;
        for (int t = 0; t < nFrozen; t++)
            CollectTier(frozen[t], t, ref scan, zoneFrom, zoneTo, ref buf, ref n);
        CollectTier(current, nFrozen, ref scan, zoneFrom, zoneTo, ref buf, ref n);
        return n;
    }

    private static void CollectTier(
        HotTierSegment tier, int tierOrdinal, ref ScanState scan,
        long zoneFrom, long zoneTo,
        ref Candidate[] buf, ref int n)
    {
        int count = tier.Count;                       // volatile: publishes prior writes
        for (int ci = 0; ci * ChunkCap < count; ci++)
        {
            if (!tier.ChunkMayOverlap(ci, zoneFrom, zoneTo)) continue;

            int first   = ci * ChunkCap;
            var headers = tier.ChunkHeaders(ci, Math.Min(ChunkCap, count - first));
            for (int si = 0; si < headers.Length; si++)
            {
                ref readonly var h = ref headers[si];
                if (!scan.Matches(in h)) continue;

                if (n == buf.Length) Grow(ref buf);
                buf[n++] = new Candidate(tierOrdinal, first + si, h.TimestampUtcTicks, h.Id);
            }
        }
    }

    private static void Grow(ref Candidate[] buf)
    {
        var bigger = ArrayPool<Candidate>.Shared.Rent(buf.Length * 2);
        buf.AsSpan().CopyTo(bigger);
        ArrayPool<Candidate>.Shared.Return(buf);
        buf = bigger;
    }

    // ── Ordering ──────────────────────────────────────────────────────────────

    /// <summary>True when <paramref name="a"/> is yielded before <paramref name="b"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Before(in Candidate a, in Candidate b, bool forward)
    {
        if (a.Ts != b.Ts) return forward ? a.Ts < b.Ts : a.Ts > b.Ts;
        return forward ? a.Id < b.Id : a.Id > b.Id;
    }

    private static void Heapify(Candidate[] a, int n, bool forward)
    {
        for (int i = (n >> 1) - 1; i >= 0; i--)
            SiftDown(a, i, n, forward);
    }

    private static void SiftDown(Candidate[] a, int i, int n, bool forward)
    {
        var item = a[i];
        while (true)
        {
            int child = 2 * i + 1;
            if (child >= n) break;
            if (child + 1 < n && Before(in a[child + 1], in a[child], forward)) child++;
            if (!Before(in a[child], in item, forward)) break;
            a[i] = a[child];
            i    = child;
        }
        a[i] = item;
    }

    private struct AscComparer : IComparer<Candidate>
    {
        public int Compare(Candidate a, Candidate b)
        {
            int c = a.Ts.CompareTo(b.Ts);
            return c != 0 ? c : a.Id.CompareTo(b.Id);
        }
    }

    private struct DescComparer : IComparer<Candidate>
    {
        public int Compare(Candidate a, Candidate b)
        {
            int c = b.Ts.CompareTo(a.Ts);
            return c != 0 ? c : b.Id.CompareTo(a.Id);
        }
    }

    // ── Per-header test ───────────────────────────────────────────────────────

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
}
