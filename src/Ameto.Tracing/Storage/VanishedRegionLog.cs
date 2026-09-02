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
/// reader deliberately does NOT repair such a header — correcting a range at load hid readable
/// spans, because these two fields decide which segments a walk opens — so this clamp is the
/// only guard, and it is written to be sufficient on its own.
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

    /// <summary>How many lost paths are remembered. See <see cref="RecordPath"/>.</summary>
    /// <summary>How far ahead of this clock a producer may legitimately be. Beyond it, a start
    /// time is not a claim about when spans arrived.</summary>
    private const long OrdinarySkewNanos = 24L * 3600 * 1_000_000_000;

    private const int MaxPaths = 64;
    private readonly Queue<string> _lostPathOrder = new();
    private readonly HashSet<string> _lostPaths = new(StringComparer.OrdinalIgnoreCase);

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

    /// <summary>
    /// Remembers that a segment covering <c>[minNano, maxNano]</c> is gone. Merges into any range
    /// it touches, so repeatedly losing files in one part of the timeline costs one entry.
    ///
    /// <para>The range is bounded before it is believed, because these numbers come from a file
    /// header and a torn field arrives here as a claim about the year 2262 — which
    /// <see cref="Forget"/> could never age out, since it keys on Max. The clamp and the reasoning
    /// behind choosing the clock over every other candidate are in the body.</para>
    /// </summary>
    public void Record(long minNano, long maxNano)
    {
        if (minNano > maxNano) (minNano, maxNano) = (maxNano, minNano);

        // ONE LINE, AND NO EVIDENCE BUT THE CLOCK. Three ceilings were tried here and every one of
        // them NARROWED the record off the loss it described — in a class whose whole contract is
        // that it only ever widens:
        //   * the mtime, floored at Min, collapsed a restored backup to the point [Min, Min];
        //   * the mtime when at or above Min still clamps whenever the mtime falls BETWEEN Min and
        //     Max, which is ordinary for any segment written over a span of time;
        //   * the mtime gated on a suspect header is the same mistake once more, because a file
        //     restored with an older write time IS a suspect header by that test.
        // Each was a three-state input answered by a two-state test, and each answer cost real
        // reported loss: measured, Overlaps over a band the segment demonstrably held came back
        // False after every one of them.
        //
        // The only thing a ceiling has to prevent is a Max that Forget can never reach — Forget
        // drops a range when its Max is below retention cutoff, so a Max torn to long.MaxValue
        // answers every query on the install for the life of the process. That is decidable from
        // the clock alone: a time in the FUTURE is not a time a segment can hold spans at.
        //
        // Never below Min, because a segment that reached Min demonstrably held something there,
        // and a point inside the lost band still reports part of the loss while a point outside it
        // reports none. A producer clocked minutes ahead lands here and keeps the inside point.
        //
        // The cost, stated rather than hidden: a torn Max on a segment lost a month ago now
        // records up to NOW, so a live window can carry a truncation banner for data it never
        // held, until retention ages the range out. That is over-reporting, and this class exists
        // because the other direction — a window quietly told it is whole — is the one that cannot
        // be recovered from. The header that caused it is separately logged by name.
        long nowNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        // A Min IN THE FUTURE IS NOT A FLOOR — and Math.Max let it become the ceiling, which put the
        // stored Max in the future and made the record unforgettable all over again: Forget tests
        // Max < cutoff, and no retention cutoff can be past a date a century out. Measured:
        // Record(now + 100 years, ...) survived Forget(now) and Forget(now + 1 year).
        //
        // The floor exists for the near case — a producer clocked minutes ahead is ordinary, and
        // keeping its point INSIDE the lost band is why Math.Max is here at all. Beyond ordinary
        // skew the header is not describing a time at all, so the whole claim collapses to now,
        // where retention can reach it.
        if (minNano > nowNano + OrdinarySkewNanos) minNano = maxNano = nowNano;
        else maxNano = Math.Min(maxNano, Math.Max(nowNano, minNano));

        if (minNano < 0) minNano = 0;

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
    /// Drops every range that lies ENTIRELY below <paramref name="cutoffNano"/>, and leaves every
    /// other range exactly as it is. Called with retention's own cutoff: the segments in that band
    /// were deleted deliberately, so a query reaching there is asking about data the operator chose
    /// not to keep, and answering it with "a file was lost" would describe the wrong event for ever.
    ///
    /// <para>WHOLE OR NOTHING, and an earlier version of this method trimmed a straddler's Min up
    /// to the cutoff instead. Its justification — the part below the cutoff describes spans
    /// retention has now deleted — is true only of ranges entirely below it, which are exactly the
    /// ones dropped here. Retention removes a SEGMENT only when its MaxStartNano is below the
    /// cutoff, so a segment straddling the line keeps every span it holds beneath that line, on
    /// disk and queryable, and so do its neighbours. Measured: Overlaps(min, cutoff-1h) went from
    /// true to false after a trim while other segments went on serving exactly that band — a
    /// truncation banner replaced by a short list calling itself complete, which is the one
    /// direction this class must never move in.</para>
    ///
    /// <para>The condition keys entirely on the TOP of a range, which is why a torn Min was always
    /// harmless (the real Max still ages out) and a torn Max was fatal until Record bounded it.</para>
    /// </summary>
    /// <returns>How many ranges were dropped.</returns>
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
