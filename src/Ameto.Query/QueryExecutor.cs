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
/// Hot-tier events are scanned directly (no index).
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
        using var hotReader = _segments.OpenHotTierReader();
        var covered = hotReader.CoveredSegmentIds;
        foreach (var ev in hotReader.ReadSorted(
                     from?.UtcTicks ?? long.MinValue, to?.UtcTicks ?? long.MaxValue,
                     afterTs, afterId?.RawValue, forward, levels))
        {
            if (ct.IsCancellationRequested || count >= limit) yield break;
            if (!filter.Matches(ev)) continue;
            yield return ev;
            count++;
        }

        if (count >= limit || ct.IsCancellationRequested) yield break;

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
            .Where(s => !covered.Contains(s.Id.Value))
            .Where(s => s.MaxTimestampTicks >= fromTicksGlobal && s.MinTimestampTicks <= toTicksGlobal)
            .ToList();

        if (segInfos.Count == 0) yield break;

        await foreach (var ev in MergeColdSegmentsAsync(segInfos, filter, levels, from, to, afterTs, afterId, forward, ct))
        {
            if (ct.IsCancellationRequested || count >= limit) yield break;
            yield return ev;
            count++;
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
    /// Merges events from multiple cold-tier segments preserving a global
    /// (Timestamp, EventId) order. For sorted segments (file format v2+) we stream
    /// blocks lazily; for unsorted legacy segments (v1) we materialise the whole
    /// segment, sort it once, then merge with the rest.
    /// </summary>
    private async IAsyncEnumerable<LogEvent> MergeColdSegmentsAsync(
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
        var prefiltered = await PrefilterSegmentsAsync(segInfos, filter, ct);

        var iterators = new List<IAsyncEnumerator<LogEvent>>(prefiltered.Count);
        try
        {
            foreach (var (info, candidateOffsets) in prefiltered)
            {
                var stream = ScanSegmentAsync(info, filter, levels, candidateOffsets, from, to, afterTs, afterId, !forward, ct);
                var it = stream.GetAsyncEnumerator(ct);
                if (await it.MoveNextAsync())
                    iterators.Add(it);
                else
                    await it.DisposeAsync();
            }

            // PriorityQueue ordered by (ts, id). For backward (newest-first) we invert
            // the comparer; .NET's PriorityQueue is a min-heap.
            var comparer = forward ? MergeAsc : MergeDesc;

            var heap = new PriorityQueue<IAsyncEnumerator<LogEvent>, (long ts, ulong id)>(comparer);
            foreach (var it in iterators)
                heap.Enqueue(it, (it.Current.Timestamp.UtcTicks, it.Current.Id.RawValue));

            while (heap.Count > 0)
            {
                if (ct.IsCancellationRequested) yield break;

                var it = heap.Dequeue();
                yield return it.Current;

                if (await it.MoveNextAsync())
                    heap.Enqueue(it, (it.Current.Timestamp.UtcTicks, it.Current.Id.RawValue));
                else
                    await it.DisposeAsync();
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
    /// segment in parallel, opening each segment's mmap exactly once. Returns
    /// the surviving segments in the original (descending-Max-ts) order so the
    /// k-way merge sees a deterministic priority.
    /// </summary>
    private async Task<List<PrefilterResult>> PrefilterSegmentsAsync(
        IReadOnlyList<SegmentInfo> segInfos,
        CompiledFilter             filter,
        CancellationToken          ct)
    {
        // GetTrigramHints() returns a pre-computed list — no .ToList() allocation needed.
        var trigramHints   = filter.GetTrigramHints();
        var invertedHints  = filter.GetInvertedHints();
        bool hasIndexHint  = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);
        bool hasInvHints   = invertedHints.Count > 0;

        // Fast path: nothing to prefilter — pass every segment through.
        if (!hasIndexHint && trigramHints.Count == 0)
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

                    // Phase 1: cheap bloom-only check for the equality hint.
                    // Bloom bytes are ~a few KB; inverted/trigram can be MB.
                    // For a high-cardinality value (e.g. a GUID), bloom rejects
                    // ~99% of segments here without ever loading the big indexes.
                    if (hasIndexHint
                        && filter.TryGetIndexHint(out string hintProp, out object? hintVal))
                    {
                        using var bloomSec = reader.RentBloomFilterBytes();
                        using var bloom    = SegmentBloomFilter.Deserialise(bloomSec.Span);
                        string valStr = hintVal?.ToString() ?? string.Empty;
                        if (!bloom.MightContain(valStr))
                            return ValueTask.CompletedTask;
                        _ = hintProp; // inverted check happens via the full index below
                    }

                    // Phase 2: only segments that survived (or filters without
                    // an equality hint) load the big indexes for trigram offset
                    // lookup and the inverted-index definitive check.
                    uint[]? candidates = null;
                    if (trigramHints.Count > 0 || hasIndexHint)
                    {
                        // Pooled: sections are copied out inside the deserialisers, so the
                        // rented buffers go back to the pool as soon as the index is built.
                        using var invSec = reader.RentInvertedIndexBytes();
                        using var triSec = reader.RentTrigramIndexBytes();
                        using var bloSec = reader.RentBloomFilterBytes();
                        var idx = _indexFactory.Create(invSec.Span, triSec.Span, bloSec.Span);

                        // Definitive inverted-index check (bloom can have false positives)
                        if (hasIndexHint
                            && filter.TryGetIndexHint(out string prop, out object? val)
                            && !idx.MightContain(prop, val))
                        {
                            return ValueTask.CompletedTask;
                        }

                        if (trigramHints.Count > 0)
                        {
                            HashSet<uint>? acc = null;
                            foreach (var (_, text) in trigramHints)
                            {
                                var offsets = idx.LookupTrigram(text);
                                if (offsets is null) continue;
                                if (acc is null) acc = new HashSet<uint>(offsets);
                                else             acc.IntersectWith(offsets);
                                if (acc.Count == 0) return ValueTask.CompletedTask;
                            }
                            candidates = acc?.ToArray();
                        }

                        // Inverted-index event-level narrowing: AND posting lists for all
                        // equality predicates. This gives exact event offsets within the
                        // segment — the reader will only deserialise those events.
                        if (hasInvHints)
                        {
                            var invOffsets = idx.LookupIntersect(invertedHints);
                            if (invOffsets is not null)
                            {
                                if (invOffsets.Length == 0)
                                    return ValueTask.CompletedTask;

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
                                    if (merged.Count == 0) return ValueTask.CompletedTask;
                                    candidates = [.. merged];
                                }
                            }
                        }
                    }

                    results[i] = new PrefilterResult(info, candidates);
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
