using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query;

/// <summary>
/// Executes a <see cref="QueryRequest"/> against the storage engine.
///
/// Execution pipeline for cold-tier segments:
///   1. Time-range filter on <see cref="SegmentInfo"/> (skip segments outside window).
///   2. Index fast-skip: open each group's <see cref="SegmentIndexView"/> — the cached memo of
///      what earlier queries worked out, reading a section only for a new question — and call
///      <see cref="ISegmentIndex.MightContain"/>; skip groups where the index says no match.
///   3. Candidate narrowing: trigram (<see cref="ISegmentIndex.LookupTrigram"/>) and
///      inverted (<see cref="ISegmentIndex.LookupIntersect"/>) posting lists yield
///      candidate event ordinals (file order, v5 segments) — the reader skips blocks
///      without candidates and materialises only candidate rows.
///   4. Block decode: LZ4 decompress via <see cref="SegmentReader"/>.
///   5. Per-event AST evaluation via <see cref="FilterEvaluator"/>.
///
/// Hot-tier events are scanned directly (no index) and merged with the cold stream on
/// the same (Timestamp, EventId) key — one globally ordered stream feeds the limit and
/// the pagination cursor, whatever tier an event happens to live in.
/// Results are emitted in the requested <see cref="QueryDirection"/> order.
/// </summary>
public sealed class QueryExecutor : IQueryExecutor
{
    private readonly ISegmentProvider         _segments;
    private readonly SegmentIndexReaderFactory _indexFactory;
    private readonly ILogger<QueryExecutor>   _logger;
    private readonly SegmentIndexCache?       _indexCache;

    public QueryExecutor(
        ISegmentProvider         segments,
        SegmentIndexReaderFactory indexFactory,
        ILogger<QueryExecutor>   logger,
        SegmentIndexCache?       indexCache = null)
    {
        _segments     = segments;
        _indexFactory = indexFactory;
        _logger       = logger;
        _indexCache   = indexCache is { Enabled: true } ? indexCache : null;
    }

    /// <summary>
    /// How much SYNCHRONOUS scanning a query may do before it hands its consumer a pending step
    /// (<see cref="ScanPace"/>). Settable only so a test can make every clock read a yield and see
    /// the yields on a fixture far too small to take the default's 50 ms.
    /// </summary>
    internal TimeSpan ScanYieldInterval { get; init; } = ScanPace.DefaultInterval;

    // ── IQueryExecutor ────────────────────────────────────────────────────────

    public async IAsyncEnumerable<LogEvent> ExecuteAsync(
        QueryRequest request,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // A caller that runs the same request shape over and over (the live tail, once per
        // poll) compiles the filter once and carries it on the request; it is trusted only
        // when it was compiled from this request's own text, so a mismatched pair costs a
        // compile, never a wrong filter.
        var filter = request.Prepared is CompiledFilter prepared
                     && string.Equals(prepared.Expression, request.Filter, StringComparison.Ordinal)
            ? prepared
            : CompiledFilter.Compile(request.Filter);
        int limit  = request.Count;
        int count  = 0;

        bool forward  = request.Direction == QueryDirection.Forward;
        var  from     = request.FromUtc;
        var  to       = request.ToUtc;
        var  afterId  = request.AfterEventId;
        var  afterTs  = request.AfterTimestampTicks;
        var  levels   = request.Levels;

        // The cursor is the tightest time bound the request carries: everything strictly
        // past it in the query direction was already served, so page k must not re-open
        // and re-decode the segments and blocks page k-1 already walked. Clamp INCLUSIVE
        // at the cursor tick — events sharing the timestamp are tie-broken by id inside
        // the cursor check, and the window checks are themselves inclusive. Guarded to
        // the DateTime range: the cursor comes off the wire, and a garbage value should
        // keep matching nothing (as before) rather than throw from the ctor.
        if (afterTs is long cursorTicks && cursorTicks >= 0 && cursorTicks <= DateTime.MaxValue.Ticks)
        {
            if (forward)
            {
                if (from is null || from.Value.UtcTicks < cursorTicks)
                    from = new DateTimeOffset(cursorTicks, TimeSpan.Zero);
            }
            else
            {
                if (to is null || to.Value.UtcTicks > cursorTicks)
                    to = new DateTimeOffset(cursorTicks, TimeSpan.Zero);
            }
        }

        // @t predicates from the filter's AND-chain are exact bounds too — folded into the
        // window so the catalog filter, zone map and hot header scan prune with them (a
        // `@t >= '...'` filter used to be evaluated per event over the whole catalog). The
        // evaluator re-checks every event regardless, so this can only skip work; the
        // DateTime-range guard mirrors the cursor's.
        if (filter.MinTimestampTicks is long boundMin && boundMin >= 0 && boundMin <= DateTime.MaxValue.Ticks
            && (from is null || from.Value.UtcTicks < boundMin))
            from = new DateTimeOffset(boundMin, TimeSpan.Zero);
        if (filter.MaxTimestampTicks is long boundMax && boundMax >= 0 && boundMax <= DateTime.MaxValue.Ticks
            && (to is null || to.Value.UtcTicks > boundMax))
            to = new DateTimeOffset(boundMax, TimeSpan.Zero);

        // The level set the filter's AND-chain admits is a `levels` parameter the caller
        // did not spell out — the same exactness (it is computed with the evaluator's own
        // comparison), the same pruning: level-split cold segments drop through their
        // posting lists, hot headers through the mask. The evaluator still re-checks every
        // event, so like the bounds above this can only skip work. A caller's explicit set
        // is kept as given.
        levels ??= filter.DerivedLevels;

        // ── Hot tier ──────────────────────────────────────────────────────────
        // Window/cursor/level filtering and the (@t, id) sort happen at HEADER level
        // inside the reader (HotTierScan) — events are materialised lazily in result
        // order, so a page query allocates ~limit events, not the whole tier.
        //
        // The hot stream is ONE MORE ENTRANT of the k-way merge below, not a prefix
        // of the response. Emitting it wholesale before the cold tier violated the
        // global (ts, id) order whenever the tiers interleave in time (late-arriving
        // events, replicated peer segments), and the violation was not cosmetic: the
        // client's next-page keyset cursor is the last emitted (ts, id), so every
        // not-yet-served cold event on the wrong side of a mis-ordered boundary
        // failed the cursor on all subsequent pages — silently unreachable rows.
        using var hotReader = _segments.OpenHotTierReader();
        var covered   = hotReader.CoveredSegmentKeys;

        // ONE pace for every source below, the hot tier and each segment scan: the scan hands its
        // consumer a pending step at least every ScanYieldInterval of synchronous work, wherever
        // in the merge that work is spent. Without it no step of a real scan is ever pending, and
        // a consumer that sends while the scan works never gets the chance (see ScanPace).
        var pace      = new ScanPace(ScanYieldInterval);
        var hotStream = HotEventsAsync(hotReader, filter, from, to, afterTs, afterId, forward, levels, pace, ct);

        // ── Cold-tier segments (k-way merge) ─────────────────────────────────
        // After Variant B, every segment's blocks are individually sorted by @t,
        // but two segments can overlap in [MinTs..MaxTs] (e.g. a flush that captured
        // some late-arriving events whose @t falls inside a previously flushed
        // segment's window). To preserve a global @t order across segments we run a
        // small k-way merge over per-segment iterators, keyed on (Timestamp, EventId).
        // Segments whose events were already served via the hot reader's frozen
        // tiers are excluded by `covered` to prevent duplicates during the flush
        // window (registered cold segment + still-frozen hot tier overlap).
        long fromTicksGlobal = from?.UtcTicks ?? long.MinValue;
        long toTicksGlobal   = to?.UtcTicks   ?? long.MaxValue;
        var segInfos = _segments.GetSegments(from, to)
            .Where(s => !covered.Contains(SegmentKey.Of(s)))
            .Where(s => s.MaxTimestampTicks >= fromTicksGlobal && s.MinTimestampTicks <= toTicksGlobal)
            .ToList();

        await foreach (var ev in MergeSourcesAsync(hotStream, segInfos, filter, levels, from, to, afterTs, afterId, forward, pace, ct))
        {
            if (ct.IsCancellationRequested || count >= limit) yield break;
            yield return ev;
            count++;
        }
    }

    /// <summary>
    /// The hot tier as a (ts, id)-sorted async source for the merge. ReadSorted already
    /// applies window, cursor and level filtering at header level, plus whatever of the
    /// filter the header can answer (<see cref="CompiledFilter.HeaderPredicate"/>: level,
    /// trace / span id, service); the compiled filter then runs here, per materialised
    /// event, as the correctness gate — the same division of labour the cold scan uses.
    /// </summary>
    private static async IAsyncEnumerable<LogEvent> HotEventsAsync(
        IHotTierReader                hotReader,
        CompiledFilter                filter,
        DateTimeOffset?               from,
        DateTimeOffset?               to,
        long?                         afterTs,
        Ameto.Core.EventId?           afterId,
        bool                          forward,
        HashSet<Ameto.Core.LogLevel>? levels,
        ScanPace                      pace,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var ev in hotReader.ReadSorted(
                     from?.UtcTicks ?? long.MinValue, to?.UtcTicks ?? long.MaxValue,
                     afterTs, afterId?.RawValue, forward, levels, filter.HeaderPredicate))
        {
            if (ct.IsCancellationRequested) yield break;
            if (pace.Due()) await ScanPace.Yield();
            if (!filter.Matches(ev)) continue;
            yield return ev;
        }
    }

    // ── Cold-tier k-way merge ─────────────────────────────────────────────────

    private static readonly Comparer<(long ts, ulong id)> MergeAsc =
        Comparer<(long ts, ulong id)>.Create(static (a, b) =>
            a.ts != b.ts ? a.ts.CompareTo(b.ts) : a.id.CompareTo(b.id));

    private static readonly Comparer<(long ts, ulong id)> MergeDesc =
        Comparer<(long ts, ulong id)>.Create(static (a, b) =>
            a.ts != b.ts ? b.ts.CompareTo(a.ts) : b.id.CompareTo(a.id));

    /// <summary>
    /// Merges the hot-tier stream and events from multiple cold-tier segments preserving
    /// a global (Timestamp, EventId) order. For sorted segments (file format v2+) we
    /// stream blocks lazily; for unsorted legacy segments (v1) we materialise the whole
    /// segment, sort it once, then merge with the rest.
    /// </summary>
    private async IAsyncEnumerable<LogEvent> MergeSourcesAsync(
        IAsyncEnumerable<LogEvent>           hotStream,
        IReadOnlyList<SegmentInfo>           segInfos,
        CompiledFilter                       filter,
        HashSet<Ameto.Core.LogLevel>?       levels,
        DateTimeOffset?                      from,
        DateTimeOffset?                      to,
        long?                                afterTs,
        Ameto.Core.EventId?                 afterId,
        bool                                 forward,
        ScanPace                             pace,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        // Open one async iterator per segment that survives index/trigram fast-skip.
        // Run the prefilter (bloom/inverted + trigram-offsets) in parallel across
        // segments — each segment opens its mmap independently and we typically
        // discard most of them via bloom. Doing this sequentially across hundreds
        // of segments was the dominant query cost (~7-10s for 217 segments).
        List<PrefilterResult> prefiltered = segInfos.Count == 0
            ? []
            : await PrefilterSegmentsAsync(
                  segInfos, filter, levels,
                  from?.UtcTicks ?? long.MinValue, to?.UtcTicks ?? long.MaxValue, ct);

        // From here on the prefilter's readers are owned by this method's finally, so
        // everything that could throw has to be inside the try — building the priming
        // order included. The finally releases them through `prefiltered`, so they are
        // released even when the ordered array below was never built.
        var iterators = new List<IAsyncEnumerator<LogEvent>>(prefiltered.Count + 1);
        try
        {
            // Priming order: the merge front moves one way through time, so segments are
            // consumed in that order too — newest MaxTs first going backward, oldest MinTs
            // first going forward.
            // Stable like the OrderBy it replaces (ties keep prefilter order = catalog order):
            // the key carries the input index, so an unstable Array.Sort cannot reorder ties.
            var ordered = new PrimeEntry[prefiltered.Count];
            for (int i = 0; i < ordered.Length; i++)
            {
                var p = prefiltered[i];
                ordered[i] = new PrimeEntry(forward ? p.Info.MinTimestampTicks : p.Info.MaxTimestampTicks, i, p);
            }
            ordered.AsSpan().Sort(new PrimeOrder(descending: !forward));

            // PriorityQueue ordered by (ts, id). For backward (newest-first) we invert
            // the comparer; .NET's PriorityQueue is a min-heap.
            var comparer = forward ? MergeAsc : MergeDesc;
            var heap = new PriorityQueue<IAsyncEnumerator<LogEvent>, (long ts, ulong id)>(comparer);
            int next = 0;

            // The hot tier primes unconditionally: it is RAM-resident (priming costs a
            // header walk, no I/O) and its time bounds are not in the catalog, so it
            // cannot take part in the could-beat check that gates the cold segments.
            var hotIt = hotStream.GetAsyncEnumerator(ct);
            if (await hotIt.MoveNextAsync())
            {
                iterators.Add(hotIt);
                heap.Enqueue(hotIt, (hotIt.Current.Timestamp.UtcTicks, hotIt.Current.Id.RawValue));
            }
            else
            {
                await hotIt.DisposeAsync();
            }

            // Open segments LAZILY. Priming every surviving segment up front is what made a
            // page cost the whole catalog: an unfiltered `count=50` takes GetSegments(null,
            // null) — every segment there is — memory-maps all of them and decompresses a
            // block from each, only to serve 50 events off the top of the heap. On the
            // sandbox stand that is 291 opens for one page.
            //
            // A segment can only matter while it could still beat the merge front: going
            // backward its MaxTs is an upper bound on anything it can produce, so once the
            // heap's best is newer than that, neither it nor any later segment (they are
            // ordered) can contribute. Ties prime, so equal timestamps are never dropped.
            async ValueTask PrimeAsync()
            {
                while (next < ordered.Length)
                {
                    if (heap.Count > 0 && heap.TryPeek(out _, out var best))
                    {
                        var info = ordered[next].Entry.Info;
                        bool couldBeat = forward
                            ? info.MinTimestampTicks <= best.ts
                            : info.MaxTimestampTicks >= best.ts;
                        if (!couldBeat) return;
                    }

                    var (segInfo, candidateOffsets, segReader) = ordered[next++].Entry;
                    // The reader is BORROWED — the finally below owns every one of them,
                    // primed or not, so the scan must not dispose what it did not open.
                    var stream = ScanSegmentAsync(segInfo, filter, levels, candidateOffsets, segReader,
                                                  from, to, afterTs, afterId, !forward, pace, ct);
                    var newIt = stream.GetAsyncEnumerator(ct);
                    if (await newIt.MoveNextAsync())
                    {
                        iterators.Add(newIt);
                        heap.Enqueue(newIt, (newIt.Current.Timestamp.UtcTicks, newIt.Current.Id.RawValue));
                    }
                    else
                    {
                        await newIt.DisposeAsync();
                    }
                }
            }

            await PrimeAsync();

            while (heap.Count > 0)
            {
                if (ct.IsCancellationRequested) yield break;

                var it = heap.Dequeue();
                yield return it.Current;

                if (await it.MoveNextAsync())
                    heap.Enqueue(it, (it.Current.Timestamp.UtcTicks, it.Current.Id.RawValue));
                else
                    await it.DisposeAsync();

                // The front just moved; a segment that could not contribute before may be
                // able to now.
                await PrimeAsync();
            }
        }
        finally
        {
            // Anything still in the heap was already disposed when drained, but if the
            // consumer broke out early we must release the remaining mmap handles.
            foreach (var it in iterators)
            {
                try { await it.DisposeAsync(); } catch { /* best-effort */ }
            }

            // …and every reader the prefilter opened, INCLUDING the segments that never
            // primed — most of them, for a small page. This is the only owner: the scan
            // borrows, the iterator above closes only what it opened itself. Until this
            // runs, those files cannot be deleted on Windows; the merge already handles a
            // source held open by an in-flight query (manifest kept, recovery sweep
            // finishes), and the hold is bounded by this query either way.
            foreach (var p in prefiltered)
            {
                if (p.Reader is { } r)
                {
                    try { r.Dispose(); } catch { /* best-effort */ }
                }
            }
        }
    }

    // ── Index fast-skip + trigram pre-filter (combined, parallel) ────────────

    /// <summary>
    /// Result of the per-segment prefilter: the segment to scan, optional candidate block
    /// offsets from the trigram index (null = scan all blocks), and the reader the prefilter
    /// already opened.
    ///
    /// <para>OWNERSHIP: <paramref name="Reader"/> belongs to <see cref="MergeSourcesAsync"/>,
    /// which disposes every one of them in its finally — including the segments that never
    /// prime. The scan borrows it and must not dispose it. Null means the prefilter opened
    /// nothing (the no-hint passthrough, or a segment that failed to open) and the scan opens
    /// and closes its own.</para>
    ///
    /// <para>LIFETIME, and the reason this is not a cache: a mapped file cannot be deleted on
    /// Windows, and retention and the merge delete segments while queries run. Bounded by the
    /// QUERY, a held reader is the same hazard the merge already documents and handles — the
    /// catalog entry goes, <c>File.Delete</c> fails, the manifest survives and the recovery
    /// sweep finishes the job once the reader closes. What changes is which segments are held:
    /// the survivors rather than only the primed ones. Anything longer-lived than a query would
    /// need refcounting against the catalog, which is deliberately not attempted here.</para>
    /// </summary>
    private readonly record struct PrefilterResult(SegmentInfo Info, uint[]? CandidateOffsets, SegmentReader? Reader);

    /// <summary>A prefilter survivor keyed for the priming order (see <see cref="PrimeOrder"/>).</summary>
    private readonly record struct PrimeEntry(long Key, int Index, PrefilterResult Entry);

    /// <summary>
    /// The priming order — MinTs ascending going forward, MaxTs descending going backward
    /// — with the input index as the tiebreak, so the sort is stable like the LINQ OrderBy
    /// it replaced without the keyed comparer, the iterator chain and the list per query.
    /// </summary>
    private readonly struct PrimeOrder(bool descending) : IComparer<PrimeEntry>
    {
        public int Compare(PrimeEntry a, PrimeEntry b)
        {
            int c = descending ? b.Key.CompareTo(a.Key) : a.Key.CompareTo(b.Key);
            return c != 0 ? c : a.Index.CompareTo(b.Index);
        }
    }

    /// <summary>
    /// Runs bloom/inverted fast-skip and trigram offset lookup for every cold
    /// segment in parallel, opening each segment's mmap exactly once. Survivors come
    /// back in <paramref name="segInfos"/> order — results are written into a slot per
    /// input index, so the parallel completion order does not leak out — but that is
    /// only determinism, not a priority: <see cref="MergeColdSegmentsAsync"/> re-sorts
    /// by MinTs or MaxTs for the lazy-priming order before it opens anything, so
    /// nothing downstream reads any meaning into the order returned here.
    ///
    /// <para>The unit of prefiltering is the INDEX GROUP, not the file. A single bloom
    /// stretched over 24 h answers "maybe" to everything, so a day-scale segment would
    /// survive every query and the fast-skip would stop being a skip; per group the filter
    /// keeps the ~10 bits/term it is sized for. A rejected group costs one bloom read and
    /// never touches its multi-MB inverted/trigram sections. v4-v6 segments expose exactly
    /// one group, so they take the same path with the same result as before.</para>
    /// </summary>
    private async Task<List<PrefilterResult>> PrefilterSegmentsAsync(
        IReadOnlyList<SegmentInfo>    segInfos,
        CompiledFilter                filter,
        HashSet<Ameto.Core.LogLevel>? levels,
        long                          fromTicks,
        long                          toTicks,
        CancellationToken             ct)
    {
        // GetTrigramHints() returns a pre-computed list — no .ToList() allocation needed.
        var trigramHints   = filter.GetTrigramHints();
        var invertedHints  = filter.GetInvertedHints();
        bool hasIndexHint  = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);
        bool hasInvHints   = invertedHints.Count > 0;

        // The levels PARAMETER prunes through the same index the `@l = '...'` FILTER
        // uses. It used to prune nothing: an errors-only dashboard query decoded every
        // block of every surviving segment only for the per-event level check to reject
        // nearly all of it. Per group, each level of the set either has a posting list,
        // is provably absent, or answers "no information" (a pre-index segment) — the
        // union across the set is definitive unless any level answers the latter.
        (string, object?)[][]? levelHints = null;
        if (levels is { Count: > 0 })
        {
            levelHints = new (string, object?)[levels.Count][];
            int li = 0;
            foreach (var l in levels)
                levelHints[li++] = [(ClefFields.Level, l.ToSeqString())];
        }

        // Fast path: nothing to prefilter — pass every segment through.
        if (!hasIndexHint && !hasInvHints && trigramHints.Count == 0 && levelHints is null)
        {
            var passthrough = new List<PrefilterResult>(segInfos.Count);
            foreach (var info in segInfos)
                passthrough.Add(new PrefilterResult(info, null, null));
            return passthrough;
        }

        var results = new PrefilterResult?[segInfos.Count];

        // Bound parallelism conservatively — each in-flight prefilter holds
        // index byte arrays (inverted + trigram can be several MB per segment),
        // so a high degree of parallelism over hundreds of segments blows
        // working-set memory into the gigabytes. ProcessorCount, capped at 8,
        // is a good balance between throughput and RAM.
        int degree = Math.Min(Math.Min(Environment.ProcessorCount, 8), segInfos.Count);
        if (degree < 1) degree = 1;

        try
        {
            await Parallel.ForEachAsync(
                Enumerable.Range(0, segInfos.Count),
                new ParallelOptions { MaxDegreeOfParallelism = degree, CancellationToken = ct },
                (i, innerCt) =>
                {
                    var info = segInfos[i];
                    // NOT a `using`: a surviving segment hands its reader to the scan through
                    // PrefilterResult (see the record's ownership note) and the merge disposes it.
                    // Every other exit from this body disposes it here — `keep` is the one flag
                    // that decides which, and it is set exactly where the result is stored.
                    SegmentReader? reader = null;
                    bool keep = false;
                    try
                    {
                        reader = SegmentReader.Open(info.FilePath);

                        // Accumulated across the surviving groups. Posting offsets are FILE
                        // ordinals in every group (SegmentIndexBuilder.Build writes
                        // firstOrdinal + pos), so the groups' candidate arrays simply
                        // concatenate — no rebasing.
                        //
                        // The ORDER of what comes out is STILL not a promise this method makes.
                        // Each group's array is ascending — TryNarrowWithIndex merges sorted
                        // posting lists now, where it used to drain a HashSet whose enumeration
                        // order is unspecified by contract — but groups are appended in file
                        // order and a later group's ordinals are all above an earlier one's only
                        // because SegmentIndexBuilder.Build writes firstOrdinal + pos. Nothing
                        // downstream may assume more than "the reader will handle it".
                        //
                        // SegmentReader.ReadEventsAsync verifies the order before its two-pointer
                        // walk and sorts a copy when it has to, and that check is the contract,
                        // not a belt-and-braces extra: the walk advances a single cursor through
                        // the candidates alongside ascending row ordinals, so one descending pair
                        // makes it step past a candidate it will never come back to and the query
                        // silently loses rows the index proved it should return. Do not delete it
                        // on the strength of a comment here.
                        List<uint>? candidates = null;
                        bool anyGroupSurvived  = false;
                        // A surviving group that could not narrow (its index sections are absent —
                        // e.g. a WAL-recovery flush that ran before the builder was wired) is NO
                        // information about its rows. Candidates would then silently exclude them,
                        // so the whole segment falls back to a full scan.
                        bool unnarrowedGroup   = false;
                        // THE THIRD NARROWING STATE: groups that contribute EVERY one of their
                        // rows (see TryNarrowWithIndex — the level union that IS the group). Their
                        // ordinals are the contiguous range [FirstOrdinal, +EventCount), so they
                        // are not materialised here at all; they are generated only if some OTHER
                        // group genuinely narrows and the segment therefore has to name ordinals.
                        List<(uint First, uint Count)>? everyRowGroups = null;
                        // Groups this segment could contribute rows from, and groups that survived.
                        // When they agree, every row of the segment is a candidate, and the honest
                        // way to say so is to hand the scan no candidate list at all.
                        int groupsConsidered = 0, groupsAccepted = 0;

                        // Appends the pending whole-group ranges. Called before a narrowed group's
                        // own ordinals go in, so the list stays ascending: groups are visited in
                        // ordinal order and everything pending came from an earlier one.
                        void FlushEveryRowGroups()
                        {
                            if (everyRowGroups is null || candidates is null) return;
                            for (int r = 0; r < everyRowGroups.Count; r++)
                            {
                                var (first, count) = everyRowGroups[r];
                                for (uint o = 0; o < count; o++) candidates.Add(first + o);
                            }
                            everyRowGroups.Clear();
                        }

                        void Accept(uint[]? groupCandidates, bool everyRow, uint firstOrdinal, uint eventCount)
                        {
                            anyGroupSurvived = true;
                            groupsAccepted++;

                            if (everyRow)
                            {
                                (everyRowGroups ??= new List<(uint, uint)>(4)).Add((firstOrdinal, eventCount));
                            }
                            else if (groupCandidates is null)
                            {
                                unnarrowedGroup = true;
                            }
                            else
                            {
                                candidates ??= new List<uint>(groupCandidates.Length);
                                FlushEveryRowGroups();
                                candidates.AddRange(groupCandidates);
                            }
                        }

                        var groups = reader.Groups;
                        for (int g = 0; g < groups.Length; g++)
                        {
                            ref readonly var grp = ref groups[g];
                            if (grp.EventCount == 0) continue;
                            // Group time bounds are exact, so this drops a group's index sections
                            // without reading them. The reader's per-event window check remains
                            // the correctness gate. NOT counted as a dropped group below: the
                            // reader prunes by the same window itself, so leaving such a group out
                            // of an explicit candidate list buys nothing.
                            if (grp.MaxTs < fromTicks || grp.MinTs > toTicks) continue;

                            groupsConsidered++;

                            // The group's index as this query sees it: the cached memo for (file,
                            // group) when there is a cache — found, or created empty — else a
                            // throwaway one. It answers from what earlier queries worked out and
                            // reads a section of the group, out of the reader opened above, only for
                            // a question it has not been asked before; then it decodes the one
                            // bucket or trigram asked for, never the section (#80). Disposed at the
                            // end of this iteration, `continue` included: the rented sections go
                            // back to the pool, a bloom it had to deserialise is freed, and the
                            // cache learns whether this group was a hit (no section read) and what
                            // the memo grew by.
                            using var index = _indexFactory.OpenGroup(_indexCache, info.FilePath, g, reader);

                            // Phase 1: the bloom gate on the equality hint — and on the level set —
                            // before any inverted or trigram lookup. The verdict is per probed text
                            // and remembered, so a repeated filter pays nothing here; a new value
                            // reads this group's bloom section once.
                            //
                            // "Cheap" is relative: MEASURED by BloomSizingProbe, bloom is 15.6 % of
                            // a prop-dense group's three sections and 26.6 % of a thin one's. What
                            // keeps it on the path is not its size but its answer: it is keyless, so
                            // it rejects a group where the value appears under no property at all —
                            // including groups where the inverted index would only have said "this
                            // property is unknown here, scan". The gate lives in PassesBloomGate
                            // rather than inline: it has to probe every value form the scan would
                            // accept, and an inline copy of that decision is exactly what once let
                            // the index prune rows a scan would have matched.
                            if (hasIndexHint && !PassesBloomGate(filter, index))
                                continue;
                            // No level of the set can be present in this group (no false negatives
                            // in the bloom) — skip it before the inverted lookups.
                            if (levelHints is not null && !AnyLevelMaybePresent(levelHints, index))
                                continue;

                            // Phase 2: trigram offsets and the inverted-index narrowing. The fast
                            // path above already returned for a filter with no hint of any kind, so
                            // every group reaching here has something to look up. A group that is
                            // provably empty for this filter is skipped, not the whole segment —
                            // the next group may still hold matches.
                            if (!TryNarrowWithIndex(filter, index, levelHints, grp.EventCount,
                                                    out var groupCandidates, out bool groupEveryRow))
                                continue;

                            Accept(groupCandidates, groupEveryRow, grp.FirstOrdinal, grp.EventCount);
                        }

                        // Every group rejected ⇒ the segment holds nothing this query can match.
                        if (!anyGroupSurvived) return ValueTask.CompletedTask;

                        uint[]? candidateOffsets;
                        if (unnarrowedGroup)
                        {
                            // One group with no information sends the whole segment to a full scan,
                            // its narrowed groups included — a candidate list would exclude rows
                            // nothing proved absent.
                            candidateOffsets = null;
                        }
                        else if (candidates is not null)
                        {
                            // Something genuinely narrowed, so this segment has to name its rows —
                            // and a whole-group contributor names its contiguous range, generated
                            // directly rather than unioned out of its posting lists.
                            FlushEveryRowGroups();
                            candidateOffsets = candidates.ToArray();
                        }
                        else if (everyRowGroups is null)
                        {
                            candidateOffsets = null;                      // nothing narrowed at all
                        }
                        else if (groupsAccepted == groupsConsidered)
                        {
                            // EVERY group of this segment contributes EVERY one of its rows: the
                            // candidate list would be 0,1,2,…,n-1. Saying so by handing back no
                            // list is the same scan over the same rows, and it is the difference
                            // between a plain block walk and a group-sized uint[] built by a
                            // posting-list union, copied into a List, copied out again, and then
                            // resolved by a binary search per block and a cursor step per row —
                            // to select all of them. This is the level-only filter's normal shape
                            // against a level-split store.
                            candidateOffsets = null;
                        }
                        else
                        {
                            // Some group WAS rejected, so the survivors' rows still have to be
                            // named — but as their plain ranges, with no posting lists involved.
                            candidateOffsets = ContiguousOrdinals(everyRowGroups);
                        }

                        results[i] = new PrefilterResult(info, candidateOffsets, reader);
                        keep = true;
                    }
                    catch (Exception ex)
                    {
                        // On error, don't skip the segment — fall back to a full scan
                        // so we never silently lose data due to a transient I/O hiccup. The
                        // reader is NOT carried over: whatever went wrong may be the mapping
                        // itself, and the scan's own Open is the retry.
                        _logger.LogDebug(ex, "Index prefilter failed for segment {Id}, falling back to full scan", info.Id);
                        results[i] = new PrefilterResult(info, null, null);
                    }
                    finally { if (!keep) reader?.Dispose(); }
                    return ValueTask.CompletedTask;
                }).ConfigureAwait(false);
        }
        catch
        {
            // Cancellation, or a fault Parallel.ForEachAsync surfaces after some bodies have
            // already stored their reader. Nothing downstream will ever see those results, so
            // this is the only place that can close them.
            DisposeReaders(results);
            throw;
        }

        var surviving = new List<PrefilterResult>(segInfos.Count);
        for (int i = 0; i < results.Length; i++)
            if (results[i] is { } r)
                surviving.Add(r);
        return surviving;
    }

    /// <summary>Closes every reader a prefilter result still owns. Best-effort and idempotent
    /// per slot — a double dispose on <see cref="SegmentReader"/> is harmless, an unclosed
    /// mapping is a file that cannot be deleted.</summary>
    private static void DisposeReaders(PrefilterResult?[] results)
    {
        for (int i = 0; i < results.Length; i++)
        {
            if (results[i] is { Reader: { } r })
            {
                try { r.Dispose(); } catch { /* best-effort */ }
                results[i] = null;
            }
        }
    }

    /// <summary>
    /// Phase 1 of the prefilter: the cheap bloom-only gate on the equality hint. False drops
    /// the segment without ever loading the multi-megabyte index sections.
    ///
    /// <para>Factored out of the parallel body together with <see cref="TryNarrowWithIndex"/>
    /// so the tests that assert "the index never costs a query its rows" can run the decision
    /// this method makes instead of a copy of it. A copy passed while the original was
    /// reverted, which is the one thing those tests exist to catch.</para>
    /// </summary>
    internal static bool PassesBloomGate(CompiledFilter filter, SegmentBloomFilter bloom)
    {
        if (!filter.TryGetIndexHint(out _, out object? hintVal)) return true;
        return SegmentIndexReader.MightContainValue(bloom, hintVal);
    }

    /// <summary>
    /// The same gate through a group's view: the same value forms probed, each verdict answered
    /// from the group's memo when an earlier query already asked, from its bloom section when not.
    /// </summary>
    internal static bool PassesBloomGate(CompiledFilter filter, SegmentIndexView index)
    {
        if (!filter.TryGetIndexHint(out _, out object? hintVal)) return true;
        return index.MightContainValue(hintVal);
    }

    /// <summary>
    /// Phase 2: the definitive <c>MightContain</c> gate, trigram offset lookup and inverted
    /// posting-list narrowing, against an already-loaded index.
    ///
    /// <para>Returns false when the segment is provably empty for this filter — the caller
    /// then never reads it. <paramref name="candidates"/> is null for "scan every block" and
    /// otherwise the exact local offsets worth deserialising.</para>
    /// </summary>
    internal static bool TryNarrowWithIndex(CompiledFilter filter, ISegmentIndex idx, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints: null, out candidates);

    /// <summary>True when at least one level of the set might be present per the group's bloom.</summary>
    private static bool AnyLevelMaybePresent((string, object?)[][] levelHints, SegmentIndexView index)
    {
        for (int i = 0; i < levelHints.Length; i++)
            if (index.MightContainValue(levelHints[i][0].Item2))
                return true;
        return false;
    }

    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints, groupEventCount: 0, out candidates);

    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints,
        uint groupEventCount, out uint[]? candidates)
        => TryNarrowWithIndex(filter, idx, levelHints, groupEventCount, out candidates, out _);

    /// <param name="groupEventCount">
    /// Events in the group being narrowed, or 0 for "unknown". Used for ONE decision: a level
    /// union whose posting lists already account for every event in the group is the identity,
    /// and intersecting with the identity is work with no result. See below.
    /// </param>
    /// <param name="everyRow">
    /// True when the group contributes EVERY one of its rows — the third narrowing state,
    /// distinct from both "these ordinals" (<paramref name="candidates"/>) and "no information"
    /// (a null <paramref name="candidates"/> with this false). <paramref name="candidates"/> is
    /// left null, because the answer is the contiguous range the caller already knows from the
    /// group directory; materialising it is what this state exists to avoid.
    /// </param>
    internal static bool TryNarrowWithIndex(
        CompiledFilter filter, ISegmentIndex idx, (string, object?)[][]? levelHints,
        uint groupEventCount, out uint[]? candidates, out bool everyRow)
    {
        candidates = null;
        everyRow   = false;

        var trigramHints  = filter.GetTrigramHints();
        var invertedHints = filter.GetInvertedHints();
        bool hasIndexHint = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);

        // Definitive inverted-index check (bloom can have false positives)
        if (hasIndexHint
            && filter.TryGetIndexHint(out string prop, out object? val)
            && !idx.MightContain(prop, val))
            return false;

        // EVERY posting list below arrives ASCENDING and DISTINCT — SegmentBitmapCodec's
        // delta encoding cannot decode backwards, and both LookupTrigram and LookupIntersect
        // now hand back merged sorted arrays. So every combination here is a two-pointer merge.
        // It used to be a HashSet per step: `new HashSet<uint>(offsets)` for the trigram seed,
        // one for the inverted result and one for the level union — and for a level-split Error
        // segment that last one is the WHOLE group, ~2 MB of buckets per group per query,
        // built to hash data that was already in order. The sorted-array result is also what
        // lets SegmentReader.ReadEventsAsync replace its clone-and-sort with one scan.
        if (trigramHints.Count > 0)
        {
            uint[]? acc = null;
            foreach (var (_, text) in trigramHints)
            {
                var offsets = idx.LookupTrigram(text);
                if (offsets is null) continue;              // no information from this hint
                acc = acc is null ? offsets : IntersectSorted(acc, offsets);
                if (acc.Length == 0) return false;
            }
            candidates = acc;
        }

        // Inverted-index event-level narrowing: AND posting lists for all equality
        // predicates. This gives exact event offsets within the segment — the reader will
        // only deserialise those events.
        if (invertedHints.Count > 0)
        {
            var invOffsets = idx.LookupIntersect(invertedHints);
            if (invOffsets is not null)
            {
                if (invOffsets.Length == 0) return false;

                candidates = candidates is null ? invOffsets : IntersectSorted(candidates, invOffsets);
                if (candidates.Length == 0) return false;
            }
        }

        // The levels parameter, through the same posting lists a `@l = '...'` filter
        // uses. Union across the set — an event matches ANY allowed level — and only
        // definitive when every level answered (null = the group predates the index or
        // the bucket is unknown: no information, scan). An empty union proves the group
        // holds none of the allowed levels. The per-event level re-check in the scan
        // stays the correctness gate, as with every other narrowing here.
        if (levelHints is not null)
        {
            uint[]? union     = null;
            long    totalOffs = 0;
            bool    definitive = true;
            foreach (var hint in levelHints)
            {
                var offs = idx.LookupIntersect(hint);
                if (offs is null) { definitive = false; break; }
                totalOffs += offs.Length;
                if (offs.Length > 0) union = union is null ? offs : UnionSorted(union, offs);
            }
            if (definitive)
            {
                if (union is null) return false;

                // LEVEL-PURE GROUP: an event has exactly one level, so a group's level posting
                // lists are disjoint and their lengths sum to at most its event count. Equality
                // therefore proves the union IS the group — the normal case for a level-split
                // Error segment, where `@l = Error` names all ~100k of its rows. Intersecting
                // an existing candidate set with the identity cannot remove anything, so the
                // merge over the whole group is skipped and the candidates stand.
                //
                // Only when something else already narrowed. With NO other hint the union is
                // still the whole answer this group contributes — and the honest way to say so
                // is `everyRow`, not the array. Returning the array made a level-only filter
                // (`@l != 'Error'`, which the compiled filter now derives a level set from even
                // when the request names none) pay, per group and per query, for a group-sized
                // uint[] built by a posting-list union, copied into a List and copied out again
                // — three copies of the group — which the reader then resolved with two binary
                // searches per block and a cursor step per row, to select 100 % of them.
                // Returning null instead is not available here: null means UNNARROWED, and one
                // unnarrowed group sends the whole segment, its narrowed groups included, to a
                // full scan. Hence the third state.
                bool identity = groupEventCount != 0 && totalOffs == groupEventCount;
                if (candidates is null)
                {
                    if (identity) everyRow = true;
                    else          candidates = union;
                }
                else if (!identity)
                {
                    candidates = IntersectSorted(candidates, union);
                    if (candidates.Length == 0) return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// The ordinals of whole-group contributors, as plain ascending ranges. Groups are visited
    /// in ordinal order, so concatenating their ranges is already sorted — no posting list is
    /// read and no merge runs to produce what the group directory already states.
    /// </summary>
    private static uint[] ContiguousOrdinals(List<(uint First, uint Count)> ranges)
    {
        long total = 0;
        for (int r = 0; r < ranges.Count; r++) total += ranges[r].Count;

        var outp = new uint[total];
        int k = 0;
        for (int r = 0; r < ranges.Count; r++)
        {
            var (first, count) = ranges[r];
            for (uint o = 0; o < count; o++) outp[k++] = first + o;
        }
        return outp;
    }

    /// <summary>Intersects two ascending, distinct arrays into a new ascending array.</summary>
    private static uint[] IntersectSorted(uint[] a, uint[] b)
    {
        var outp = new uint[Math.Min(a.Length, b.Length)];
        int i = 0, j = 0, k = 0;
        while (i < a.Length && j < b.Length)
        {
            uint x = a[i], y = b[j];
            if      (x < y) i++;
            else if (x > y) j++;
            else { outp[k++] = x; i++; j++; }
        }
        return k == outp.Length ? outp : outp[..k];
    }

    /// <summary>Unions two ascending, distinct arrays into a new ascending array.</summary>
    private static uint[] UnionSorted(uint[] a, uint[] b)
    {
        var outp = new uint[a.Length + b.Length];
        int i = 0, j = 0, k = 0;
        while (i < a.Length && j < b.Length)
        {
            uint x = a[i], y = b[j];
            if      (x < y) outp[k++] = a[i++];
            else if (x > y) outp[k++] = b[j++];
            else { outp[k++] = x; i++; j++; }
        }
        while (i < a.Length) outp[k++] = a[i++];
        while (j < b.Length) outp[k++] = b[j++];
        return k == outp.Length ? outp : outp[..k];
    }

    // ── Segment scan ──────────────────────────────────────────────────────────

    /// <param name="borrowed">
    /// The reader the prefilter already opened for this segment, or null. When non-null this
    /// method does NOT dispose it — <see cref="MergeSourcesAsync"/> owns every prefilter reader
    /// and closes them all in one place, because a segment that never primes has no iterator to
    /// close it. Opening the file twice per segment per query was the cost being removed: a
    /// FileInfo stat, a CreateFromFile, a CreateViewAccessor over the whole file and a
    /// block-index parse, 40 of them for a 20-segment query.
    /// </param>
    private static async IAsyncEnumerable<LogEvent> ScanSegmentAsync(
        SegmentInfo info,
        CompiledFilter filter,
        HashSet<Ameto.Core.LogLevel>? levels,
        uint[]? candidateOffsets,
        SegmentReader? borrowed,
        DateTimeOffset? from,
        DateTimeOffset? to,
        long? afterTs,
        Ameto.Core.EventId? afterId,
        bool reversed,
        ScanPace pace,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        SegmentReader? opened = null;
        if (borrowed is null)
        {
            try
            {
                opened = SegmentReader.Open(info.FilePath);
            }
            catch
            {
                yield break;
            }
        }

        var reader = borrowed ?? opened!;
        try
        {
            // Every segment is v2+: events inside each block are sorted by @t and
            // blocks themselves are sorted, so we can stream lazily without buffering.
            await foreach (var ev in reader.ReadEventsAsync(candidateOffsets, from, to, reversed, ct))
            {
                if (pace.Due()) await ScanPace.Yield();
                if (!InWindow(ev, from, to)) continue;
                if (!AfterCursor(ev, afterTs, afterId, !reversed)) continue;
                if (levels != null && !levels.Contains(ev.Level)) continue;
                if (!filter.Matches(ev)) continue;
                yield return ev;
            }
        }
        finally { opened?.Dispose(); }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static bool InWindow(LogEvent ev, DateTimeOffset? from, DateTimeOffset? to)
    {
        if (from.HasValue && ev.Timestamp < from.Value) return false;
        if (to.HasValue   && ev.Timestamp > to.Value)   return false;
        return true;
    }

    /// <summary>(timestamp, eventId) pagination cursor — see <see cref="QueryCursor.After"/>.</summary>
    private static bool AfterCursor(LogEvent ev, long? afterTs, Ameto.Core.EventId? afterId, bool forward)
        => QueryCursor.After(ev.Timestamp.UtcTicks, ev.Id.RawValue, afterTs, afterId?.RawValue, forward);
}
