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
///   2. Index fast-skip: load <see cref="SegmentIndexReader"/> and call
///      <see cref="ISegmentIndex.MightContain"/> — skip segments where index says no match.
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

    public QueryExecutor(
        ISegmentProvider         segments,
        SegmentIndexReaderFactory indexFactory,
        ILogger<QueryExecutor>   logger)
    {
        _segments     = segments;
        _indexFactory = indexFactory;
        _logger       = logger;
    }

    // ── IQueryExecutor ────────────────────────────────────────────────────────

    public async IAsyncEnumerable<LogEvent> ExecuteAsync(
        QueryRequest request,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var filter = CompiledFilter.Compile(request.Filter);
        int limit  = request.Count;
        int count  = 0;

        bool forward  = request.Direction == QueryDirection.Forward;
        var  from     = request.FromUtc;
        var  to       = request.ToUtc;
        var  afterId  = request.AfterEventId;
        var  afterTs  = request.AfterTimestampTicks;
        var  levels   = request.Levels;

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
        var hotStream = HotEventsAsync(hotReader, filter, from, to, afterTs, afterId, forward, levels, ct);

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

        await foreach (var ev in MergeSourcesAsync(hotStream, segInfos, filter, levels, from, to, afterTs, afterId, forward, ct))
        {
            if (ct.IsCancellationRequested || count >= limit) yield break;
            yield return ev;
            count++;
        }
    }

    /// <summary>
    /// The hot tier as a (ts, id)-sorted async source for the merge. ReadSorted already
    /// applies window, cursor and level filtering at header level; only the compiled
    /// filter runs here, per materialised event — the same division of labour the cold
    /// scan uses.
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
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var ev in hotReader.ReadSorted(
                     from?.UtcTicks ?? long.MinValue, to?.UtcTicks ?? long.MaxValue,
                     afterTs, afterId?.RawValue, forward, levels))
        {
            if (ct.IsCancellationRequested) yield break;
            if (!filter.Matches(ev)) continue;
            yield return ev;
        }
        await Task.CompletedTask; // the source is synchronous; async only to fit the merge
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
                  segInfos, filter,
                  from?.UtcTicks ?? long.MinValue, to?.UtcTicks ?? long.MaxValue, ct);

        // Priming order: the merge front moves one way through time, so segments are
        // consumed in that order too — newest MaxTs first going backward, oldest MinTs
        // first going forward.
        var ordered = forward
            ? prefiltered.OrderBy(p => p.Info.MinTimestampTicks).ToList()
            : prefiltered.OrderByDescending(p => p.Info.MaxTimestampTicks).ToList();

        var iterators = new List<IAsyncEnumerator<LogEvent>>(ordered.Count + 1);
        try
        {
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
                while (next < ordered.Count)
                {
                    if (heap.Count > 0 && heap.TryPeek(out _, out var best))
                    {
                        var info = ordered[next].Info;
                        bool couldBeat = forward
                            ? info.MinTimestampTicks <= best.ts
                            : info.MaxTimestampTicks >= best.ts;
                        if (!couldBeat) return;
                    }

                    var (segInfo, candidateOffsets) = ordered[next++];
                    var stream = ScanSegmentAsync(segInfo, filter, levels, candidateOffsets,
                                                  from, to, afterTs, afterId, !forward, ct);
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
        }
    }

    // ── Index fast-skip + trigram pre-filter (combined, parallel) ────────────

    /// <summary>
    /// Result of the per-segment prefilter: the segment to scan and optional
    /// candidate block offsets from the trigram index (null = scan all blocks).
    /// </summary>
    private readonly record struct PrefilterResult(SegmentInfo Info, uint[]? CandidateOffsets);

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
        IReadOnlyList<SegmentInfo> segInfos,
        CompiledFilter             filter,
        long                       fromTicks,
        long                       toTicks,
        CancellationToken          ct)
    {
        // GetTrigramHints() returns a pre-computed list — no .ToList() allocation needed.
        var trigramHints   = filter.GetTrigramHints();
        var invertedHints  = filter.GetInvertedHints();
        bool hasIndexHint  = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);
        bool hasInvHints   = invertedHints.Count > 0;

        // Fast path: nothing to prefilter — pass every segment through.
        if (!hasIndexHint && !hasInvHints && trigramHints.Count == 0)
        {
            var passthrough = new List<PrefilterResult>(segInfos.Count);
            foreach (var info in segInfos)
                passthrough.Add(new PrefilterResult(info, null));
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

        await Parallel.ForEachAsync(
            Enumerable.Range(0, segInfos.Count),
            new ParallelOptions { MaxDegreeOfParallelism = degree, CancellationToken = ct },
            (i, innerCt) =>
            {
                var info = segInfos[i];
                try
                {
                    using var reader = SegmentReader.Open(info.FilePath);

                    // Accumulated across the surviving groups. Posting offsets are FILE
                    // ordinals in every group (SegmentIndexBuilder.Build writes
                    // firstOrdinal + pos), so the groups' candidate arrays simply
                    // concatenate — no rebasing.
                    //
                    // The ORDER of what comes out is not guaranteed and must not be relied
                    // on. TryNarrowWithIndex builds its trigram result in a HashSet<uint>
                    // and hands back acc.ToArray(); HashSet enumeration order is unspecified
                    // by contract. It happens to come out ascending today — measured over
                    // 2000 random inputs, single-hint and intersected alike, because the set
                    // is filled from an already-sorted array and thereafter only ever has
                    // entries removed, and removal neither reorders survivors nor frees a
                    // slot that a later Add could reuse — but that is a property of one
                    // implementation of HashSet, not of this code.
                    //
                    // SegmentReader.ReadEventsAsync clones and Array.Sorts before its
                    // two-pointer walk, and that sort is the contract, not a belt-and-braces
                    // extra: the walk advances a single cursor through the candidates
                    // alongside ascending row ordinals, so one descending pair makes it step
                    // past a candidate it will never come back to and the query silently
                    // loses rows the index proved it should return. Do not delete that sort
                    // on the strength of a comment here.
                    List<uint>? candidates = null;
                    bool anyGroupSurvived  = false;
                    // A surviving group that could not narrow (its index sections are absent —
                    // e.g. a WAL-recovery flush that ran before the builder was wired) is NO
                    // information about its rows. Candidates would then silently exclude them,
                    // so the whole segment falls back to a full scan.
                    bool unnarrowedGroup   = false;

                    var groups = reader.Groups;
                    for (int g = 0; g < groups.Length; g++)
                    {
                        ref readonly var grp = ref groups[g];
                        if (grp.EventCount == 0) continue;
                        // Group time bounds are exact, so this drops a group's index sections
                        // without reading them. The reader's per-event window check remains
                        // the correctness gate.
                        if (grp.MaxTs < fromTicks || grp.MinTs > toTicks) continue;

                        // Both phases read the bloom section, so it is rented ONCE per group and
                        // held across them. It used to be rented twice, and a section is not a
                        // small thing to rent twice: ArrayPool<byte>.Shared does not pool arrays
                        // over 1 MB — it satisfies the rent with a fresh allocation and drops it
                        // on Return — so above that size every rent is an LOH allocation the
                        // pooling was there to avoid, once per group, per segment, in parallel
                        // across the catalog.
                        //
                        // Unconditional: the fast path above already returned for a filter with
                        // no hint of any kind, so every group reaching here reads this section in
                        // one phase or the other.
                        using var bloomSec = reader.RentBloomFilterBytes(g);

                        // Phase 1: the cheap bloom-only check for the equality hint. For a
                        // high-cardinality value (e.g. a GUID), bloom rejects ~99% of groups
                        // here without ever loading the inverted and trigram sections.
                        //
                        // "Cheap" is relative and was once documented as absolute — "bloom bytes
                        // are ~a few KB; inverted/trigram can be MB". They are all MB. MEASURED
                        // by BloomSizingProbe over an 8-source merge, as a share of a group's
                        // three index sections: bloom is 15.6% of a prop-dense group and 26.6%
                        // of a thin-event one, so rejecting in phase 1 costs 6.4x and 3.8x less
                        // than reaching phase 2 — not the three orders of magnitude the old
                        // comment implied, but the right side of a decision that is taken once
                        // per group of every segment a query touches.
                        //
                        // That ratio is something the write side has to keep earning. The filter
                        // is sized from a forecast, and while that forecast assumed a fixed 64
                        // terms per event, a thin-event group's bloom was 62% of its own index
                        // and phase 1 saved only 1.6x — the split had very nearly stopped paying
                        // for itself. Sizing groups from measured terms (SegmentWriter.EnsureSink)
                        // is what puts it back at 3.8x.
                        //
                        // The gate itself lives in PassesBloomGate rather than inline: it has
                        // to fold case and probe every value form the scan would accept, and
                        // an inline copy of that decision is exactly what let the index prune
                        // rows a scan would have matched.
                        if (hasIndexHint)
                        {
                            using var bloom = SegmentBloomFilter.Deserialise(bloomSec.Span);
                            if (!PassesBloomGate(filter, bloom))
                                continue;
                        }

                        // Phase 2: only groups that survived (or filters without
                        // an equality hint) load the big indexes for trigram offset
                        // lookup and the inverted-index definitive check.
                        uint[]? groupCandidates = null;
                        if (trigramHints.Count > 0 || hasIndexHint || hasInvHints)
                        {
                            // Pooled: sections are copied out inside the deserialisers, so the
                            // rented buffers go back to the pool as soon as the index is built.
                            using var invSec = reader.RentInvertedIndexBytes(g);
                            // The trigram section is the biggest thing in the file (~43% of it)
                            // and every posting list is materialised into int[] on load. Only
                            // pay for it when the filter actually has a substring predicate —
                            // an `@l = 'Error'` query used to deserialise the whole thing.
                            using var triSec = trigramHints.Count > 0
                                ? reader.RentTrigramIndexBytes(g)
                                : default;
                            using var idx = _indexFactory.Create(invSec.Span, triSec.Span, bloomSec.Span);

                            // A group that is provably empty for this filter is skipped, not
                            // the whole segment — the next group may still hold matches.
                            if (!TryNarrowWithIndex(filter, idx, out groupCandidates))
                                continue;
                        }

                        anyGroupSurvived = true;
                        if (groupCandidates is null) unnarrowedGroup = true;
                        else
                        {
                            candidates ??= new List<uint>(groupCandidates.Length);
                            candidates.AddRange(groupCandidates);
                        }
                    }

                    // Every group rejected ⇒ the segment holds nothing this query can match.
                    if (!anyGroupSurvived) return ValueTask.CompletedTask;

                    results[i] = new PrefilterResult(
                        info, unnarrowedGroup ? null : candidates?.ToArray());
                }
                catch (Exception ex)
                {
                    // On error, don't skip the segment — fall back to a full scan
                    // so we never silently lose data due to a transient I/O hiccup.
                    _logger.LogDebug(ex, "Index prefilter failed for segment {Id}, falling back to full scan", info.Id);
                    results[i] = new PrefilterResult(info, null);
                }
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);

        var surviving = new List<PrefilterResult>(segInfos.Count);
        for (int i = 0; i < results.Length; i++)
            if (results[i] is { } r)
                surviving.Add(r);
        return surviving;
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
    /// Phase 2: the definitive <c>MightContain</c> gate, trigram offset lookup and inverted
    /// posting-list narrowing, against an already-loaded index.
    ///
    /// <para>Returns false when the segment is provably empty for this filter — the caller
    /// then never reads it. <paramref name="candidates"/> is null for "scan every block" and
    /// otherwise the exact local offsets worth deserialising.</para>
    /// </summary>
    internal static bool TryNarrowWithIndex(CompiledFilter filter, ISegmentIndex idx, out uint[]? candidates)
    {
        candidates = null;

        var trigramHints  = filter.GetTrigramHints();
        var invertedHints = filter.GetInvertedHints();
        bool hasIndexHint = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);

        // Definitive inverted-index check (bloom can have false positives)
        if (hasIndexHint
            && filter.TryGetIndexHint(out string prop, out object? val)
            && !idx.MightContain(prop, val))
            return false;

        if (trigramHints.Count > 0)
        {
            HashSet<uint>? acc = null;
            foreach (var (_, text) in trigramHints)
            {
                var offsets = idx.LookupTrigram(text);
                if (offsets is null) continue;
                if (acc is null) acc = new HashSet<uint>(offsets);
                else             acc.IntersectWith(offsets);
                if (acc.Count == 0) return false;
            }
            candidates = acc?.ToArray();
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

                if (candidates is null)
                {
                    candidates = invOffsets;
                }
                else
                {
                    var invSet = new HashSet<uint>(invOffsets);
                    var merged = new List<uint>(Math.Min(candidates.Length, invOffsets.Length));
                    foreach (var o in candidates)
                        if (invSet.Contains(o)) merged.Add(o);
                    if (merged.Count == 0) return false;
                    candidates = [.. merged];
                }
            }
        }

        return true;
    }

    // ── Segment scan ──────────────────────────────────────────────────────────

    private static async IAsyncEnumerable<LogEvent> ScanSegmentAsync(
        SegmentInfo info,
        CompiledFilter filter,
        HashSet<Ameto.Core.LogLevel>? levels,
        uint[]? candidateOffsets,
        DateTimeOffset? from,
        DateTimeOffset? to,
        long? afterTs,
        Ameto.Core.EventId? afterId,
        bool reversed,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        SegmentReader? reader = null;
        try
        {
            reader = SegmentReader.Open(info.FilePath);
        }
        catch
        {
            yield break;
        }

        using (reader)
        {
            // Every segment is v2+: events inside each block are sorted by @t and
            // blocks themselves are sorted, so we can stream lazily without buffering.
            await foreach (var ev in reader.ReadEventsAsync(candidateOffsets, from, to, reversed, ct))
            {
                if (!InWindow(ev, from, to)) continue;
                if (!AfterCursor(ev, afterTs, afterId, !reversed)) continue;
                if (levels != null && !levels.Contains(ev.Level)) continue;
                if (!filter.Matches(ev)) continue;
                yield return ev;
            }
        }
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
