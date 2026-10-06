using System.Buffers;

namespace Ameto.Tracing.Storage;

// ── The stream fetchers' reads of the unflushed spans (#94) ─────────────────────
//
// Both SSE fetchers page backwards through a window, and every page used to walk EVERY unflushed
// span: the trace list merged every in-window trace to return `limit` of them, and the TraceQL
// fetch pushed every match through a top-K heap — inside the engine's read lock, which the
// drainer's write holds queue behind. The pieces here read the two runs UnflushedRunsLocked
// captures, after the lock is gone, with the start index (SpanStartIndex) beside each run.

public sealed partial class TraceStorageEngine
{
    /// <summary>
    /// The hot-tier filter of <see cref="SearchSpansAsync"/>: the window and the scalar hints,
    /// exactly the test the hot pass has always applied (the cold pass hands the same values to
    /// <see cref="SpanReader.SearchAsync"/>).
    /// </summary>
    private readonly struct SpanMatch
    {
        public readonly long            FromNano;
        public readonly long            ToNano;
        private readonly string?         _serviceName;
        private readonly string?         _spanName;
        private readonly SpanStatusCode? _status;
        private readonly short?          _httpStatusCode;
        private readonly long?           _minDurationNanos;
        private readonly long?           _maxDurationNanos;

        public SpanMatch(long fromNano, long toNano, string? serviceName, string? spanName, SpanStatusCode? status,
                         short? httpStatusCode, long? minDurationNanos, long? maxDurationNanos)
        {
            FromNano          = fromNano;
            ToNano            = toNano;
            _serviceName      = serviceName;
            _spanName         = spanName;
            _status           = status;
            _httpStatusCode   = httpStatusCode;
            _minDurationNanos = minDurationNanos;
            _maxDurationNanos = maxDurationNanos;
        }

        public bool Matches(SpanRecord s) =>
            s.StartTimeUnixNano >= FromNano &&
            s.StartTimeUnixNano <= ToNano   &&
            (_serviceName      is null || s.ServiceName.Equals(_serviceName, StringComparison.OrdinalIgnoreCase)) &&
            (_spanName         is null || s.Name.Contains(_spanName, StringComparison.OrdinalIgnoreCase)) &&
            (_status           is null || s.Status == _status.Value) &&
            (_httpStatusCode   is null || s.HttpStatusCode == _httpStatusCode.Value) &&
            (_minDurationNanos is null || s.DurationNanos >= _minDurationNanos.Value) &&
            (_maxDurationNanos is null || s.DurationNanos <= _maxDurationNanos.Value);
    }

    /// <summary>
    /// A hot span's place in the newest-first order: its start, then its position in tier order
    /// (the snapshot's spans first, then the live tier's). Ascending is OLDEST first, and among
    /// equal starts the LATER position sorts lower — so a full heap evicts it first, and a tie
    /// at the cut is decided by arrival rather than by which block the walk happened to read
    /// first. A total order: two spans never compare equal.
    /// </summary>
    private readonly struct HotKey(long start, int position) : IComparable<HotKey>
    {
        public readonly long Start    = start;
        public readonly int  Position = position;

        public int CompareTo(HotKey other)
        {
            int byStart = Start.CompareTo(other.Start);
            return byStart != 0 ? byStart : other.Position.CompareTo(Position);
        }
    }

    /// <summary>
    /// One block of a run the walk may have to read: the largest start in it — the key the walk
    /// orders blocks by and stops on — and its span range. The smallest start is consulted once,
    /// to leave out a block that misses the window, and is not carried.
    /// </summary>
    private struct HotBlock
    {
        public long Max;
        public int  Run;    // 0 = the detached flush snapshot, 1 = the live tier
        public int  From;   // first span, inclusive
        public int  To;     // last span, exclusive
    }

    /// <summary>
    /// Test seam: the TraceQL hot pass is about to walk — the read lock already released. A test
    /// parks it here to prove a writer can take the lock meanwhile.
    /// </summary>
    internal Action? _hotSearchPassForTest;

    /// <summary>Test seam: how many unflushed spans the TraceQL hot pass read, reported once per pass.</summary>
    internal Action<int>? _hotSearchVisitedForTest;

    /// <summary>
    /// THE NEWEST <paramref name="limit"/> UNFLUSHED SPANS <paramref name="match"/> ACCEPTS, newest
    /// first, and whether a match had to be turned away to keep them — <see cref="SearchSpansAsync"/>'s
    /// hot pass. One copy per (trace, span) id: a re-sent span or a WAL replay must not cost a slot.
    ///
    /// <para><b>OFF THE LOCK.</b> The read lock is held only to capture the two runs and their
    /// start indexes (<see cref="UnflushedRunsLocked"/>, O(1)); the walk runs after it, on the
    /// argument <see cref="UnflushedRuns"/> makes for the aggregates. The heap used to be filled
    /// INSIDE the lock — 49 000 spans, every one of them an EnqueueDequeue once the heap was full,
    /// because the tier is in arrival order and every new span is newer than the oldest kept:
    /// ~5 ms of read-lock hold per page of a TraceQL stream (Release, <c>TraceStreamPageProbe</c>),
    /// which the drainer's write holds queue behind (<c>TraceAggregateLockProbe</c>'s search reader
    /// took ingest from ~700 to ~18 000 ns/span). Moving it out was once rejected because the
    /// snapshot it would need was an O(M) copy of the tier; the captured runs are not a copy.</para>
    ///
    /// <para><b>BOUNDED — WHEN SPANS ARRIVE IN ORDER.</b> The walk reads blocks of
    /// <see cref="SpanStartIndex.BlockSize"/> spans in order of their LARGEST start, so it meets the
    /// newest spans first however late they arrived, and it stops at the first block whose largest
    /// start is below the oldest span it has already kept: no span there, or in any block after it,
    /// can make the cut. With spans arriving roughly in start order a page therefore reads the top of
    /// its window — about <c>limit</c> matches and one block more — instead of the tier.</para>
    ///
    /// <para><b>A CLOCK RUNNING AHEAD TAKES THAT BACK</b> (#122 review L1). A block's largest start
    /// covers all 128 of its spans, so one span from a clock running ahead lifts its whole block above
    /// everything kept: the block is always read, and the walk never stops at it. Interleaved with the
    /// rest of the traffic such spans poison many blocks — at one span in a hundred, 30 s ahead, nearly
    /// every block holds one, and a page reads most of the tier again (the review measured 20 000,
    /// 18 304, 16 256 and 14 208 of 20 000 on pages 0-3; <c>TraceStreamPageProbe</c> prints it).
    /// Never more than before this walk existed, and still after the lock. Keeping such outliers out
    /// of the block bounds — as one-span blocks of their own — is the follow-up that would restore
    /// the bound.</para>
    ///
    /// <para><b>THE FLOOR STILL HAS TO BE HONEST.</b> "A match was turned away" is what tells the
    /// pager the page did not read its window out, and stopping early must not lose it: if nothing
    /// was turned away by then, the remaining blocks are searched for one match that is not a copy
    /// of a span already kept — the first one settles it. That is exactly the old answer, because
    /// every remaining match would have been turned away and nothing in the heap would have
    /// moved.</para>
    ///
    /// <para><b>THE SAME SPANS AS THE HEAP THAT WALKED THE TIER IN ORDER</b> whenever no two kept
    /// spans share a start nanosecond — <c>TraceHotTierWindowTests</c> pins it against that heap,
    /// verbatim. At a tie the order is now <see cref="HotKey"/>'s, which is total; the old one was
    /// whatever the heap's layout made of the arrival sequence.</para>
    /// </summary>
    private List<SpanRecord> SelectHotMatches(in SpanMatch match, int limit, out bool evicted)
    {
        UnflushedRuns runs;
        _lock.EnterReadLock();
        try     { runs = UnflushedRunsLocked(); }
        finally { _lock.ExitReadLock(); }

        _hotSearchPassForTest?.Invoke();
        var kept = NewestMatches(runs.Flushing, runs.FlushingStarts, runs.Hot, runs.HotStarts,
                                 match, limit, out evicted, out int visited);
        _hotSearchVisitedForTest?.Invoke(visited);
        return kept;
    }

    private static List<SpanRecord> NewestMatches(
        ReadOnlySpan<SpanRecord> flushing, SpanStartView flushingStarts,
        ReadOnlySpan<SpanRecord> hot,      SpanStartView hotStarts,
        in SpanMatch match, int limit, out bool evicted, out int visited)
    {
        int capacity = BlockCount(flushing.Length, flushingStarts) + BlockCount(hot.Length, hotStarts);
        var blocks = ArrayPool<HotBlock>.Shared.Rent(Math.Max(1, capacity));
        try
        {
            int n = AddBlocks(blocks, 0, run: 0, flushing.Length, flushingStarts, match.FromNano, match.ToNano);
            n     = AddBlocks(blocks, n, run: 1, hot.Length,      hotStarts,      match.FromNano, match.ToNano);

            // Newest block first. The tie-break only makes the visiting order reproducible — the
            // result does not depend on it, because HotKey is a total order.
            blocks.AsSpan(0, n).Sort(static (a, b) =>
                a.Max != b.Max ? b.Max.CompareTo(a.Max)
              : a.Run != b.Run ? b.Run.CompareTo(a.Run)
              :                  b.From.CompareTo(a.From));

            // SIZED FOR WHAT THE PAGE FINDS, AHEAD OF IT — see NextHeapCapacity. The heap and its id
            // set grow together, before the heap is full, to sizes this method chooses: left to
            // themselves, the id set of a 2 000-span page ends at 2 729 entries — 87 KB, one array
            // over the large-object threshold, every page — and the heap re-copies itself nine
            // times on the way. `candidates` is the window's span count, an upper bound on matches.
            int candidates = 0;
            for (int k = 0; k < n; k++) candidates += blocks[k].To - blocks[k].From;
            int finalSize = Math.Min(limit, candidates);

            var top      = new PriorityQueue<SpanRecord, HotKey>();
            var present  = new HashSet<(TraceId Trace, ulong Span)>();
            int sizedFor = 0;
            int inWindow = 0;   // spans read that could match at all — the match rate's denominator
            evicted = false;
            visited = 0;

            int next = 0;
            for (; next < n; next++)
            {
                ref readonly var block = ref blocks[next];

                // Full, and this block — like every block after it — starts below the oldest span
                // kept: nothing left can make the cut.
                if (top.Count >= limit && top.TryPeek(out _, out var oldest) && block.Max < oldest.Start) break;

                var run      = block.Run == 0 ? flushing : hot;
                int position = block.Run == 0 ? 0 : flushing.Length;
                for (int i = block.From; i < block.To; i++)
                {
                    var s = run[i];
                    visited++;
                    if (s.StartTimeUnixNano < match.FromNano || s.StartTimeUnixNano > match.ToNano) continue;
                    inWindow++;
                    if (!match.Matches(s)) continue;
                    if (top.Count == sizedFor && sizedFor < finalSize)
                    {
                        sizedFor = NextHeapCapacity(sizedFor, inWindow, candidates, finalSize);
                        top.EnsureCapacity(sizedFor);
                        present.EnsureCapacity(sizedFor + 1);   // + the copy AdmitHot adds before it decides
                    }
                    evicted |= AdmitHot(top, present, s, new HotKey(s.StartTimeUnixNano, position + i), limit);
                }
            }

            if (next < n && !evicted)
                evicted = AnyMatchNotKept(blocks.AsSpan(next, n - next), flushing, hot, match, present, ref visited);

            // The heap drains oldest-first; the caller wants newest-first.
            var kept = new List<SpanRecord>(top.Count);
            while (top.TryDequeue(out var s, out _)) kept.Add(s);
            kept.Reverse();
            return kept;
        }
        finally
        {
            ArrayPool<HotBlock>.Shared.Return(blocks);
        }
    }

    /// <summary>The heap size at which a hot pass stops doubling and sizes by its match rate.</summary>
    private const int SizeHeapAt = 256;

    /// <summary>
    /// What a full heap of <paramref name="capacity"/> grows to — never past <paramref name="finalSize"/>
    /// (the page's limit, or the window's span count when that is smaller).
    ///
    /// <para><b>BY THE PAGE'S OWN MATCH RATE, ONCE IT HAS ONE</b> (#122 review L2). Below
    /// <see cref="SizeHeapAt"/> the heap doubles. Its first time full at that size, the rate it
    /// has matched at so far — <paramref name="capacity"/> matches among the
    /// <paramref name="inWindowRead"/> spans it has read inside the window — is extrapolated over the
    /// window's <paramref name="candidates"/>, plus a quarter; if that proves short, it doubles
    /// again. This used to jump straight to the limit, and a page ending near 300 matches paid for
    /// 2 000: 185 KB, against main's 76.</para>
    ///
    /// <para><b>INSIDE THE WINDOW</b>, because a disordered tier makes the walk read whole blocks
    /// that are mostly outside it — a block kept in every page by one late span, its other 127 spans
    /// above the ceiling. Counted against everything read, the rate came out low and a full page
    /// grew in doubling steps: 379 KB instead of 259 on the third page of a stream down
    /// <c>TraceStreamPageProbe</c>'s disordered tier (Debug), 367-395 KB instead of 275 in Release.</para>
    ///
    /// <para><b>WHY NOT DOUBLING ALL THE WAY</b>, which never pays for more than twice what a page
    /// finds: the page that fills its limit pays for every step instead — 354 KB against 259 — and
    /// that is the page every attribute or OR query is in this pass, where nothing narrows the
    /// walk but the window. Measured (Debug, 20 000 spans, a page of 2 000; KB): matches spread
    /// through the window, ~300 / ~600 / ~1 000 / all — main 76 / 159 / 298 / 622, doubling
    /// 78 / 153 / 173 / 354, this 75 / 111 / 172 / 259.</para>
    ///
    /// <para><b>WHAT IT GETS WRONG, AND HOW BADLY:</b> a page whose matches crowd the top of its
    /// window. The walk reads newest first, so the burst looks, 256 matches in, exactly like a page
    /// about to fill its limit, and it is sized for the limit — 176 / 189 / 208 KB for bursts of
    /// 300 / 600 / 1 000, which doubling keeps to 78 / 153 / 173 and main's heap paid about as for
    /// the spread-out shapes. Never more than a full page, and the cost the jump to the limit put on
    /// every selective page; no rule decided at 256 matches can tell the two apart.</para>
    /// </summary>
    private static int NextHeapCapacity(int capacity, int inWindowRead, int candidates, int finalSize)
    {
        long next = capacity < SizeHeapAt  ? Math.Max(16L, 2L * capacity)
                  : capacity == SizeHeapAt ? Math.Max(capacity + 1L, (long)capacity * candidates / Math.Max(1, inWindowRead) * 5 / 4)
                  :                          2L * capacity;
        return (int)Math.Min(finalSize, next);
    }

    /// <summary>Blocks a run contributes at most: its index's, or one for a run read unindexed.</summary>
    private static int BlockCount(int spans, SpanStartView starts) =>
        spans == 0 ? 0 : starts.IsIndexed ? starts.Blocks : 1;

    /// <summary>
    /// Appends the blocks of one run whose start range meets <c>[fromNano, toNano]</c>. An
    /// unindexed run is one block that may hold anything — read whole, and never the reason the
    /// walk stops.
    /// </summary>
    private static int AddBlocks(HotBlock[] blocks, int n, int run, int spans, SpanStartView starts,
                                 long fromNano, long toNano)
    {
        if (spans == 0) return n;
        if (!starts.IsIndexed)
        {
            blocks[n++] = new HotBlock { Max = long.MaxValue, Run = run, From = 0, To = spans };
            return n;
        }
        for (int b = 0, from = 0; from < spans; b++, from += SpanStartIndex.BlockSize)
        {
            if (!starts.Overlaps(b, fromNano, toNano)) continue;
            blocks[n++] = new HotBlock
            {
                Max = starts.MaxOf(b), Run = run, From = from, To = Math.Min(from + SpanStartIndex.BlockSize, spans),
            };
        }
        return n;
    }

    /// <summary>
    /// Offers one hot match to the heap, by the rules of the cold pass's <c>Admit</c>: a copy of a
    /// span already kept costs nothing, a full heap keeps the higher <see cref="HotKey"/>. Returns
    /// true when a match was turned away or pushed out — what the floor is made of.
    /// </summary>
    private static bool AdmitHot(PriorityQueue<SpanRecord, HotKey> top, HashSet<(TraceId Trace, ulong Span)> present,
                                 SpanRecord r, HotKey key, int limit)
    {
        var  id         = (r.TraceId, r.SpanId.RawValue);
        bool identified = !r.SpanId.IsEmpty;
        if (identified && !present.Add(id)) return false;     // a second copy of something already kept

        if (top.Count < limit) { top.Enqueue(r, key); return false; }

        if (!top.TryPeek(out _, out var lowest) || key.CompareTo(lowest) <= 0)
        {
            if (identified) present.Remove(id);               // not kept: `present` mirrors the heap
            return true;
        }

        var pushedOut = top.EnqueueDequeue(r, key);
        if (!pushedOut.SpanId.IsEmpty) present.Remove((pushedOut.TraceId, pushedOut.SpanId.RawValue));
        return true;
    }

    // ── The trace list ────────────────────────────────────────────────────────────

    /// <summary>Test seam: how many unflushed spans a trace-list page read, reported once per page.</summary>
    internal Action<int>? _listHotVisitedForTest;

    /// <summary>
    /// Merges every span of one run that starts inside <c>[fromNano, toNano]</c>, in tier order —
    /// skipping each block whose start range misses the window without reading one of its
    /// records. Returns the spans it read.
    ///
    /// <para><b>EVERY IN-WINDOW SPAN, AND IN TIER ORDER, BECAUSE THE PAGE IS MADE OF BOTH.</b> A
    /// row merges ALL of its trace's in-window spans (count, services, error, earliest), the first
    /// root in tier order is the root, the first span at the earliest start names the service, and
    /// the services are listed in the order they were met; the merge's size is also what the cold
    /// walk's scan cap counts. So the list cannot stop early the way the TraceQL pass does — its
    /// filters run after the merge and can reject any number of traces — and it may not reorder.
    /// What it can skip is a block NOTHING of which is in the window: a page deep in a stream has
    /// its ceiling far below the newest blocks, and those cost no record reads at all.</para>
    ///
    /// <para><b>WHEN SPANS ARRIVE IN ORDER</b> (#122 review L1). One long span, reported when it
    /// ends, lowers its block's smallest start by its whole duration, and that block can then not be
    /// skipped by any page whose ceiling lies within that duration. The review measured a page five
    /// seconds deep at 5 120 spans read in order, 6 400 with one span in 1 000 reported 10 s late,
    /// and 14 976 with one in 50; <c>TraceStreamPageProbe</c> prints it.</para>
    /// </summary>
    private static int MergeRunInto(Dictionary<TraceId, MergedTrace> merged, ReadOnlySpan<SpanRecord> run,
                                    SpanStartView starts, long fromNano, long toNano)
    {
        if (!starts.IsIndexed)
        {
            foreach (var s in run)
                if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano) MergeSpanInto(merged, s);
            return run.Length;
        }

        int read = 0;
        for (int b = 0, from = 0; from < run.Length; b++, from += SpanStartIndex.BlockSize)
        {
            if (!starts.Overlaps(b, fromNano, toNano)) continue;
            var block = run.Slice(from, Math.Min(SpanStartIndex.BlockSize, run.Length - from));
            read += block.Length;
            foreach (var s in block)
                if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano) MergeSpanInto(merged, s);
        }
        return read;
    }

    /// <summary>
    /// Whether the blocks the walk stopped before hold a match the full walk would have turned
    /// away: any match that is not a copy of a span the heap kept. Nothing in them could have
    /// entered the heap, so the heap — and with it <paramref name="present"/> — is exactly what
    /// the full walk would have ended with.
    /// </summary>
    private static bool AnyMatchNotKept(ReadOnlySpan<HotBlock> rest, ReadOnlySpan<SpanRecord> flushing,
                                        ReadOnlySpan<SpanRecord> hot, in SpanMatch match,
                                        HashSet<(TraceId Trace, ulong Span)> present, ref int visited)
    {
        foreach (ref readonly var block in rest)
        {
            var run = block.Run == 0 ? flushing : hot;
            for (int i = block.From; i < block.To; i++)
            {
                var s = run[i];
                visited++;
                if (!match.Matches(s)) continue;
                if (s.SpanId.IsEmpty || !present.Contains((s.TraceId, s.SpanId.RawValue))) return true;
            }
        }
        return false;
    }
}
