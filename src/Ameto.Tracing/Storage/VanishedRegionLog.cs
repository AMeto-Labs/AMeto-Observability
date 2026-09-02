namespace Ameto.Tracing.Storage;

/// <summary>
/// THE ENGINE'S MEMORY OF THE SEGMENTS THAT VANISHED — the time ranges a read will never be able
/// to cover again, kept where the loss is, which is the process and not the request.
///
/// <para>WHY IT EXISTS AT ALL. A cold segment whose file has gone is dropped from the snapshot by
/// the reader that discovers it, so the fault is DISCOVERABLE EXACTLY ONCE: the next request finds
/// no segment, meets no fault, and — having read out every file that still exists — makes the
/// strong positive claim that it read the window out. Measured through <c>/api/traces/stream</c>:
/// two 50-trace segments, the older one unlinked, the SAME request twice — <c>query-error</c> and
/// 50 rows, then <c>done {"complete":true}</c> and 50 rows. Half the window, labelled complete.
/// Worse in the product than in the transcript, because the control the truncation banner sits
/// next to is REFRESH: one click converts the warning into a list that claims to be whole.</para>
///
/// <para>So the bit that says "part of this window is unreadable" cannot be scoped to a stream.
/// It is recorded here, against the RANGE the dead segment occupied, and every later read whose
/// window overlaps that range is told. <see cref="TraceListPage.Unreadable"/> spells out why the
/// consumer may never decide a later page has made the fault good: nothing re-reads a file that is
/// gone.</para>
///
/// <para>WHAT IS NOT RECORDED, and this is the case that would otherwise dominate. Compaction
/// publishes its merged output INTO THE SNAPSHOT and unlinks its sources afterwards; retention
/// does the same. A reader holding a snapshot from before that swap therefore meets missing files
/// as a matter of routine, on a healthy server, at whatever rate it compacts — and the data is not
/// lost at all, it is in the replacement. Recording those would make a busy install report
/// truncation for ever, which is the same lie as the one this class is fixing, pointing the other
/// way. <see cref="TraceStorageEngine.RemoveColdSegment"/> makes that distinction with the one
/// piece of evidence that cannot be guessed from ranges: whether the segment was still IN the
/// snapshot when the reader tripped over it. If it was, nobody retired it and the file is simply
/// gone; if it was not, the engine had already replaced or expired it on purpose.</para>
///
/// <para>WHAT BOUNDS IT — both halves, because a memory of faults that only ever grows is a leak
/// and a memory that never forgets is a server that reports truncation for ever:</para>
/// <list type="bullet">
///   <item>IN SIZE, by <see cref="MaxRegions"/>. Overflow COALESCES the two closest neighbours
///   into the interval that spans them rather than dropping either. Coalescing only ever WIDENS
///   what is reported, so the error it can introduce is over-reporting truncation — never the
///   silent under-report this class exists to prevent. WHAT THE WIDENING ACTUALLY COSTS, measured,
///   because "only widens" is not a number and 32 is a number somebody has to judge: 200 losses of
///   one millisecond each, one per minute, coalesce to 32 regions with 200 of 200 recorded losses
///   still reported — and 168 of the 199 HEALTHY minutes between them now reported unreadable
///   too. So past the cap this stops being a map of the damage and becomes a report that the
///   damaged PERIOD cannot be trusted, which is a defensible thing to say and a bad thing to say
///   by accident. Read that next to the fact that a directory-level fault used to record one
///   region per cold segment in a single request: a 40-segment install could cross the cap on one
///   mount blip, which is why <c>TraceStorageEngine</c> no longer records anything for one;</item>
///   <item>IN TIME, by retention. <see cref="Forget"/> is called from <c>PruneAsync</c> with the
///   same cutoff that deletes the segments, so a recorded fault lives exactly as long as the data
///   it describes could have been queried. A hole in a window whose spans have all expired is not
///   a hole anybody can look through.</item>
/// </list>
///
/// <para>BOTH BOUNDS RUN ON <see cref="Record"/>'s INPUT, NOT ON ITS CALLER'S GOOD INTENTIONS.
/// The ranges arrive from <c>SpanSegmentInfo</c>, which for a cold-loaded segment is copied out of
/// a file header — and a header field torn to <c>long.MaxValue</c> produced a range that
/// <see cref="Forget"/> is structurally unable to drop, because its test is <c>Max &lt;
/// cutoff</c>. Measured: after that, EVERY window on the install reported unreadable, for the life
/// of the process, and <c>PruneAsync</c> reported <c>pruned=0, regions=1</c> for ever. The reader
/// now repairs such a header, and <see cref="Record"/> clamps regardless — one guard is a fix, two
/// are a property.</para>
///
/// <para>Every method takes one lock. The list holds at most <see cref="MaxRegions"/> intervals,
/// the operations are linear over that, and they run once per removed segment and once per page —
/// never per row, per span or per segment scanned.</para>
/// </summary>
internal sealed class VanishedRegionLog
{
    /// <summary>
    /// How many disjoint ranges are kept before overflow starts coalescing. Segments vanish behind
    /// the engine's back only when something outside it deletes them, so the steady state is zero
    /// and any number here is generous; it is a bound on the damage, not a working set.
    /// </summary>
    private const int MaxRegions = 32;

    private readonly object _gate = new();

    /// <summary>Disjoint, non-adjacent, sorted ascending by <c>Min</c>. Both invariants are
    /// re-established by <see cref="Record"/> on every insert, and <see cref="Forget"/> preserves
    /// them by removing whole entries.</summary>
    private readonly List<(long Min, long Max)> _regions = new(MaxRegions);

    /// <summary>Test hook: how many disjoint ranges are currently remembered.</summary>
    internal int CountForTest { get { lock (_gate) return _regions.Count; } }

    /// <summary>
    /// Remembers that a segment covering <c>[minNano, maxNano]</c> is gone. Merges into any range
    /// it touches, so repeatedly losing files in one part of the timeline costs one entry.
    ///
    /// <para>THE RANGE IS CLAMPED BEFORE IT IS BELIEVED, and this is not defensive habit: the
    /// caller's numbers come from a file header, so a torn field arrives here as a claim about the
    /// year 2262. <see cref="Forget"/> drops a range only when its <c>Max</c> is below retention's
    /// cutoff, so such a claim is not merely wrong — it is UNFORGETTABLE, and every later query on
    /// the install ends "a storage segment inside this window could not be read". A segment cannot
    /// hold spans from after the moment it was lost, so the ceiling is real rather than arbitrary,
    /// and clamping to it keeps the record inside the only band retention can reach.</para>
    /// </summary>
    /// <summary>
    /// The paths of segments already established as lost. Kept SEPARATELY from the ranges because
    /// the two answer different questions, and conflating them cost a healthy server a red banner:
    /// a range says "this window is missing something", which is what a reader needs; a path says
    /// "this exact file is gone", which is what a CLASSIFIER needs. Asking the ranges instead —
    /// does this segment's time span overlap a recorded loss? — cannot tell the file somebody just
    /// lost from a different file in the same hour, and cold segments overlap in time by design.
    /// Measured: after one real loss, the next ordinary compaction handover in that band came back
    /// Lost, so an untouched server reported deleted-or-damaged for the whole retention TTL.
    ///
    /// <para>Bounded like the ranges, by the same argument: dropping the oldest entry only means a
    /// later reader falls back to Handover for a file nobody is going to meet again.</para>
    /// </summary>
    private const int MaxPaths = 64;
    private readonly Queue<string> _lostPathOrder = new();
    private readonly HashSet<string> _lostPaths = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Remembers that this exact file is the one that went missing.</summary>
    public void RecordPath(string filePath)
    {
        lock (_gate)
        {
            if (!_lostPaths.Add(filePath)) return;
            _lostPathOrder.Enqueue(filePath);
            while (_lostPathOrder.Count > MaxPaths) _lostPaths.Remove(_lostPathOrder.Dequeue());
        }
    }

    /// <summary>Whether THIS file has already been established as lost by an earlier reader.</summary>
    public bool WasLost(string filePath)
    {
        lock (_gate) return _lostPaths.Contains(filePath);
    }

    public void Record(long minNano, long maxNano, long ceilingNano)
    {
        if (minNano > maxNano) (minNano, maxNano) = (maxNano, minNano);

        // THE CEILING IS THE CALLER'S, AND IT IS THE FILE'S OWN LAST-WRITE TIME. Two weaker
        // choices were tried and both are wrong in a way a test now pins:
        //   * "now plus a day of clock slack" — the slack BECAME the recorded Max, so one torn
        //     header plus one lost file answered every LIVE window with "a storage segment inside
        //     this window could not be read" for the next twenty-four hours, over windows holding
        //     none of the lost data, and Forget could not reach it because Forget keys on Max;
        //   * "now" — better, but a segment written a month ago and lost today still stretches its
        //     region across the whole month up to live traffic.
        // A file is written after the spans in it arrived, so its mtime is a real upper bound on
        // what it could have held, and it is the only evidence still available once the file
        // itself is gone.
        // TWO CEILINGS, AND THE LOWER USABLE ONE WINS. `now` is the structural one and it always
        // holds: no range whose Max is in the future is storable, so no record is unforgettable by
        // Forget. The caller's is evidence about this particular file — its own write time, which
        // bounds what it could have held.
        //
        // BUT ONLY WHILE IT IS EVIDENCE. A mtime BELOW the segment's own Min is not a statement
        // about the data, it is a statement that the mtime is unusable: files restored by rsync -at,
        // tar -xp or a filesystem snapshot carry a write time older than everything in them. An
        // earlier version clamped to it anyway and then floored the result at Min — which did stop
        // the range inverting, by collapsing it to the single point [Min, Min]. Measured: a 4.75-hour
        // segment lost with a five-day-old mtime recorded the point [-6h, -6h], and the very next
        // query over the LAST THREE HOURS came back Unreadable=False Capped=False over a window that
        // had just lost eight of that segment's twenty traces. A silent complete:true over lost
        // data, produced by the guard against silent completes, in the direction this class's own
        // docstring calls impossible.
        long nowNano  = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        long ceiling  = ceilingNano >= minNano ? Math.Min(ceilingNano, nowNano) : nowNano;

        if (maxNano > ceiling) maxNano = ceiling;
        if (minNano < 0)       minNano = 0;
        if (minNano > maxNano) minNano = maxNano;   // the whole claim was in the future

        lock (_gate)
        {
            // Absorb every range this one meets. Merging is transitive — a new range can bridge
            // two that were disjoint — so this is a sweep, not a first-hit search.
            for (int i = _regions.Count - 1; i >= 0; i--)
            {
                var r = _regions[i];
                if (r.Min > maxNano || r.Max < minNano) continue;   // disjoint, and stays so
                if (r.Min < minNano) minNano = r.Min;
                if (r.Max > maxNano) maxNano = r.Max;
                _regions.RemoveAt(i);
            }

            int at = 0;
            while (at < _regions.Count && _regions[at].Min < minNano) at++;
            _regions.Insert(at, (minNano, maxNano));

            if (_regions.Count > MaxRegions) CoalesceClosestPairLocked();
        }
    }

    /// <summary>
    /// True when any remembered range overlaps <c>[fromNano, toNano]</c> — i.e. when this query is
    /// asking about a stretch of time part of which is on no disk any more.
    /// </summary>
    public bool Overlaps(long fromNano, long toNano)
    {
        lock (_gate)
        {
            // Sorted by Min, so the walk can stop at the first range that starts above the window.
            foreach (var r in _regions)
            {
                if (r.Min > toNano) return false;
                if (r.Max >= fromNano) return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Drops every range that lies entirely below <paramref name="cutoffNano"/>, and TRIMS the one
    /// range that can still straddle it. Called with retention's own cutoff: the segments in that
    /// band have been deleted deliberately, so a query reaching there is asking about data the
    /// operator chose not to keep, and answering it with "a file was lost" would be describing the
    /// wrong event for ever.
    ///
    /// <para>WHY THE TRIM, which is the asymmetry the drop alone leaves behind. The condition is
    /// <c>Max &lt; cutoff</c>, so this method keys entirely on the TOP of a range and never looks
    /// at the bottom — which is why a torn <c>Min</c> was always harmless (the real <c>Max</c>
    /// still ages out; measured, <c>regions</c> went 1 → 0 on the next pass) and a torn <c>Max</c>
    /// was fatal. Trimming makes retention act on both ends: the part of a surviving range that
    /// lies below the cutoff describes spans retention has now deleted on purpose, so continuing
    /// to report it is the same wrong sentence, just about less of the timeline. It also drains
    /// the over-report that coalescing and a repaired-to-zero <c>Min</c> both create, instead of
    /// leaving it to sit until the whole range finally expires.</para>
    ///
    /// <para>AT MOST ONE RANGE NEEDS IT, and the invariants prove it rather than the loop assuming
    /// it: the list is sorted ascending by <c>Min</c> and disjoint, so every survivor past the
    /// first has <c>Min[i] &gt; Max[i-1] ≥ cutoff</c>. Trimming only ever RAISES a <c>Min</c>
    /// toward a <c>Max</c> that is already at or above the cutoff, so the range stays non-empty,
    /// stays sorted and cannot grow into its neighbour.</para>
    /// </summary>
    /// <returns>How many ranges were dropped outright. A trim is not a drop.</returns>
    public int Forget(long cutoffNano)
    {
        lock (_gate)
        {
            // DROPPED WHOLE OR KEPT WHOLE. Trimming a straddler's Min up to the cutoff was the
            // second narrowing path in a class whose contract is that it only ever widens, and it
            // was wrong for a reason the trim's own justification missed: retention deletes a
            // segment only when its MaxStartNano is below the cutoff, so a segment STRADDLING the
            // cutoff keeps every span it holds below that line, on disk and queryable. Measured
            // with a seven-day TTL and a region [cutoff-12h, cutoff+12h]: Overlaps(min, cutoff-1h)
            // was true before Forget and false after, while neighbouring segments went on serving
            // exactly that band — so a user querying it got a short list with done{complete:true}
            // where a minute earlier they got a truncation banner.
            //
            // "The part below the cutoff describes spans retention deleted on purpose" is true only
            // of ranges ENTIRELY below it, and those are precisely the ones dropped here.
            return _regions.RemoveAll(r => r.Max < cutoffNano);
        }
    }

    /// <summary>
    /// Merges the two ranges separated by the smallest gap into the interval that spans them,
    /// which is the cheapest widening available and keeps the list sorted and disjoint. Called
    /// only on overflow, and only ever loses PRECISION — the union still covers everything both
    /// halves covered, so a window that would have been reported still is.
    /// </summary>
    private void CoalesceClosestPairLocked()
    {
        int  best    = 0;
        long bestGap = long.MaxValue;
        for (int i = 1; i < _regions.Count; i++)
        {
            // The list is sorted and disjoint, so Min[i] > Max[i-1] and the true gap is positive.
            // A NEGATIVE result is therefore arithmetic overflow — two ranges at opposite ends of
            // the representable timeline, which is the largest gap there is, not the smallest.
            long gap = _regions[i].Min - _regions[i - 1].Max;
            if (gap < 0) gap = long.MaxValue;
            if (gap < bestGap) { bestGap = gap; best = i; }
        }

        var lo = _regions[best - 1];
        var hi = _regions[best];
        _regions[best - 1] = (lo.Min, hi.Max > lo.Max ? hi.Max : lo.Max);
        _regions.RemoveAt(best);
    }
}
