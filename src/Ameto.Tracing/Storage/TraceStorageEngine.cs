using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using MessagePack;
using Microsoft.Extensions.Logging;
using Ameto.Core;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Coordinates hot-tier span storage and cold-tier flush.
///
/// Hot tier: an in-memory list of <see cref="SpanRecord"/> objects with a
/// <c>TraceId → List&lt;int&gt;</c> inverted index for fast trace assembly.
///
/// Cold tier: flushed as <c>.trc</c> files by <see cref="SpanWriter"/>
/// when the hot segment reaches its size/time threshold.
/// </summary>
public sealed class TraceStorageEngine : ITraceProvider, ITraceStatsProvider, IServiceGraphProvider, ITraceSummaryProvider, IRetentionTarget, IDisposable
{
    // ── Hot tier ─────────────────────────────────────────────────────────────
    // _hotSpans is SWAPPED at flush start (the snapshot goes to the writer, a fresh list
    // takes its place), so it is not readonly; _traceIdx is cleared and refilled in place.
    private          List<SpanRecord>                          _hotSpans  = new();
    private readonly Dictionary<TraceId, List<int>>            _traceIdx  = new();
    private readonly ReaderWriterLockSlim                      _lock      = new();

    // ── In-flight flush ──────────────────────────────────────────────────────
    // The segment build (sort + LZ4-HC + four indexes + three sidecars) used to run
    // UNDER the exclusive lock on the drainer's thread: ingest and every query stalled
    // for its full duration — at the stand's 100k spans/s the 65k ring gave <0.7s of
    // headroom, so each 50k-span flush overflowed it. Now the flush snapshots the tier
    // under the lock (fast), builds the segment off it, and re-acquires only to publish
    // and commit the WAL (see SpanWriteAheadLog.BeginFlush for the crash story).
    private bool _flushInProgress;                                      // under _lock(write)
    private Task? _flushTask;                                           // under _lock(write)
    // Readers' view of the snapshot while it is being written: without it the spans
    // would be invisible for the build's duration (they left _hotSpans, their segment
    // is not registered yet). GetTraceAsync/SearchSpansAsync scan it; their span-id
    // dedupe absorbs the publish overlap. The aggregate paths (stats/volume/list)
    // accept the window's skew, as they already do for WAL-replay duplicates.
    private volatile List<SpanRecord>? _flushingSpans;
    // Path of a segment that is on disk under its final name but not yet registered — the
    // cold scan must not adopt it while _flushingSpans still holds the same spans.
    private volatile string? _publishingSegmentPath;

    // 0 = live, 1 = disposed. Guards against multiple Dispose calls: the engine
    // is registered as several singleton interfaces, so the DI container captures
    // and disposes the same instance more than once at shutdown.
    private int _disposed;

    // ── Cold tier ─────────────────────────────────────────────────────────────
    private readonly string                                    _dataDir;
    // Immutable snapshot, swapped under _lock's WRITE lock on every mutation
    // (flush/compaction/retention/self-heal). Readers grab the field once and
    // iterate without locks — a concurrent swap can never fault them, and a
    // deleted file surfaces as a per-segment skip, not a request failure.
    private volatile SpanSegmentInfo[]                         _coldSegments = [];

    /// <summary>Test hook: cold segments currently registered.</summary>
    internal int ColdSegmentCountForTest => _coldSegments.Length;

    /// <summary>
    /// Test hook: called on the flush thread with the tier already detached, just before the
    /// segment build. Blocking in it holds the engine in the exact window the off-lock flush
    /// opened — spans in neither tier — which is the only way to assert that readers still
    /// see them.
    /// </summary>
    internal Action? _beforeSegmentWrite;

    /// <summary>Test hook: joins the in-flight background flush, if any.</summary>
    internal void WaitForFlushForTest()
    {
        Task? t;
        _lock.EnterReadLock();
        try { t = _flushTask; }
        finally { _lock.ExitReadLock(); }
        try { t?.Wait(); } catch { /* CompleteFlush logged it */ }
    }
    private readonly ILogger<TraceStorageEngine>               _logger;

    private const int HotFlushThreshold    = 50_000;  // spans before flush
    private const int CompactionThreshold  = 10_000;  // merge cold segments smaller than this
    private const int MaxSegmentsPerPass   = 20;       // merge at most N oldest small segments per run
    private const int MaxSpansPerPass      = 200_000;  // hard cap on spans loaded into memory per run

    // ── Flush policy ──────────────────────────────────────────────────────────
    // Durability belongs to the WAL, not to the segment writer, so a timed flush no
    // longer has to run just to avoid losing spans. A .trc costs a sort, LZ4-HC over the
    // blocks and the trace index, four index structures and a .stats sidecar — that is a
    // price worth paying for a real batch and pure waste for five spans. Below the
    // minimum the hot tier simply keeps accumulating; the hard age bound still lands a
    // trickle on disk so it becomes eligible for compaction and retention.
    private const int MinSegmentSpans = 500;
    private static readonly TimeSpan MaxHotAge = TimeSpan.FromHours(1);

    /// <summary>When the oldest span currently in the hot tier arrived. Null = tier empty.</summary>
    private DateTime? _hotSince;

    /// <summary>
    /// Write-ahead log for the hot tier. Every span lands here before it is visible in
    /// memory, so an unflushed tier survives a crash without a segment per 30 seconds.
    /// </summary>
    private readonly SpanWriteAheadLog _wal;

    public TraceStorageEngine(string dataDir, ILogger<TraceStorageEngine> logger)
    {
        _dataDir = dataDir;
        _logger  = logger;
        Directory.CreateDirectory(dataDir);

        // Residue of flushes that died mid-write, plus segments whose RENAME did not
        // survive a power loss. Handled in the constructor for the same reason the metric
        // engine sweeps there — no writer can be live yet, so nothing is touched out from
        // under one (the background cold scan must never delete, it can race a flush).
        RecoverOrSweepTempFiles(dataDir);

        // Cold-segment discovery is deliberately NOT done here: the constructor
        // runs before Kestrel binds, and scanning thousands of .trc files would
        // delay ingest availability. TraceCompactionWorker calls
        // LoadColdSegments() in the background right after startup.

        // The WAL, by contrast, must be open and replayed before the first span is
        // accepted, or a restart would interleave recovered and live spans. Replay is a
        // sequential walk of one mmap'd file bounded by the flush thresholds, so it costs
        // milliseconds even at the 50k ceiling.
        _wal = SpanWriteAheadLog.Open(Path.Combine(dataDir, "spans.wal"));
        RecoverFromWal();
    }

    /// <summary>
    /// Startup pass over <c>spans-*.tmp</c>: a COMPLETE <c>.trc.tmp</c> is renamed into
    /// place (with its sidecars); everything else is deleted.
    ///
    /// <para>The recovery half exists because a rename is not durable on its own. The
    /// writer fsyncs each file before renaming, but on Linux the directory entry itself
    /// needs an fsync of the parent directory, which .NET cannot issue portably — so a
    /// power loss just after a flush can leave a fully written, fsynced segment back under
    /// its temp name. Deleting it there would destroy the whole flush, and the WAL cannot
    /// always save it: the commit that dropped those spans from the log may well have
    /// persisted. Parsing the file is the test — a footer that reads means every byte
    /// landed. If instead the commit did NOT persist, the log replays the same spans and
    /// the read paths' span-id dedupe covers the overlap; duplicates beat loss.</para>
    /// </summary>
    private void RecoverOrSweepTempFiles(string dataDir)
    {
        foreach (var tmp in Directory.EnumerateFiles(dataDir, "spans-*.trc.tmp"))
        {
            string final = tmp[..^".tmp".Length];                 // …/spans-….trc
            string baseP = final[..^".trc".Length];

            bool complete = false;
            if (!File.Exists(final))
            {
                try { SpanReader.ReadSegmentInfo(tmp); complete = true; }
                catch { /* torn or half-written — nothing to recover */ }
            }

            if (complete)
            {
                try
                {
                    // Sidecars first, the .trc last: the same order the writer publishes in,
                    // so a crash here still leaves a segment whose sidecars are complete.
                    foreach (var ext in new[] { ".stats", ".svcgraph", ".tracesum" })
                    {
                        string sTmp = baseP + ext + ".tmp";
                        if (File.Exists(sTmp) && !File.Exists(baseP + ext))
                            File.Move(sTmp, baseP + ext);
                    }
                    File.Move(tmp, final);
                    _logger.LogWarning(
                        "Recovered span segment {File}: it was complete on disk but its rename did not survive the last stop",
                        final);
                    continue;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not recover span segment {File} — removing the temp copy", final);
                }
            }

            try { File.Delete(tmp); } catch { /* locked/AV-scanned — retried next start */ }
        }

        // Whatever is left is residue: sidecar temps of a flush that never produced a
        // recoverable segment, and .trc temps the loop above could not rename.
        foreach (var tmp in Directory.EnumerateFiles(dataDir, "spans-*.tmp"))
            try { File.Delete(tmp); } catch { /* retried next start */ }
    }

    /// <summary>
    /// Rebuilds the hot tier from the log left behind by an unclean shutdown. Recovered
    /// spans are NOT re-appended to the log — they are already in it, and the log keeps
    /// writing after the last valid entry.
    /// </summary>
    private void RecoverFromWal()
    {
        List<SpanIngestItem> recovered;
        try { recovered = _wal.ReadAll(); }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Span WAL replay failed — continuing with an empty hot tier");
            return;
        }

        if (recovered.Count == 0) return;

        _lock.EnterWriteLock();
        try
        {
            for (int i = 0; i < recovered.Count; i++)
                AddToHotTierLocked(recovered[i]);

            // Date the tier by the data, not by this restart. Leaving _hotSince at "now"
            // restarts the MaxHotAge clock on every start, so a crash-restart loop could
            // keep spans out of a segment indefinitely. Clamped to now because the start
            // time is the client's to report, and a skewed clock must not push the tier
            // into the future — an implausibly old one merely flushes a little early.
            DateTime oldest = DateTime.UtcNow;
            for (int i = 0; i < recovered.Count; i++)
            {
                long nano = recovered[i].StartTimeUnixNano;
                if (nano <= 0) continue;
                var at = DateTimeOffset.FromUnixTimeMilliseconds(nano / 1_000_000L).UtcDateTime;
                if (at < oldest) oldest = at;
            }
            _hotSince = oldest;
        }
        finally { _lock.ExitWriteLock(); }

        _logger.LogInformation("Recovered {Count} span(s) from the write-ahead log", recovered.Count);
    }

    // ── Ingestion (called by SpanDrainer) ─────────────────────────────────────

    internal void WriteSpan(SpanIngestItem item)
    {
        _lock.EnterWriteLock();
        try
        {
            // Write-ahead: the span is durable before it is queryable. Held under the same
            // write lock as the hot tier so a flush's Begin can never interleave with an
            // append — that ordering is what makes the generation stamps trustworthy.
            _wal.Append(item);
            AddToHotTierLocked(item);

            if (_hotSpans.Count >= HotFlushThreshold)
                TryStartFlushLocked();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Materialises a span into the hot tier and its trace index. Shared by live ingest
    /// and WAL replay — replay must not append to the log it is reading from.
    /// </summary>
    private void AddToHotTierLocked(SpanIngestItem item)
    {
        var record = new SpanRecord
        {
            TraceId           = item.TraceId,
            SpanId            = item.SpanId,
            ParentSpanId      = item.ParentSpanId,
            StartTimeUnixNano = item.StartTimeUnixNano,
            DurationNanos     = item.DurationNanos,
            Name              = item.Name,
            ServiceName       = item.ServiceName,
            Kind              = item.Kind,
            Status            = item.Status,
            HttpStatusCode    = item.HttpStatusCode,  // promoted — no attrs deserialization
            Attributes        = item.AttributesBytes.Length > 0
                                    ? DeserializeAttributes(item.AttributesBytes)
                                    : null,
        };

        int offset = _hotSpans.Count;
        _hotSpans.Add(record);

        if (!_traceIdx.TryGetValue(item.TraceId, out var offsets))
        {
            offsets = new List<int>(4);
            _traceIdx[item.TraceId] = offsets;
        }
        offsets.Add(offset);

        _hotSince ??= DateTime.UtcNow;
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    public async IAsyncEnumerable<SpanRecord> GetTraceAsync(
        TraceId traceId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // A span can legitimately reach a reader twice. The WAL replays spans into the hot
        // tier that a segment written just before the crash may already hold, and a crash
        // between a compaction's merge write and its source deletion leaves the same spans
        // in two cold files. Span id identifies a span within a trace in the OTel model, so
        // a second copy is one span reported twice and the waterfall must show it once.
        // Empty ids are never folded together: a producer that omits the field would
        // otherwise collapse every such span into one, which is data loss, not de-duplication.
        var seen = new HashSet<ulong>();

        // Hot tier
        // Hot tier AND the snapshot of an in-flight flush, gathered in ONE lock hold. Read
        // separately, the pair has a hole: a flush that publishes (or fails and restores)
        // between the two reads can leave the spans in neither view, and the trace comes
        // back empty or half-built.
        _lock.EnterReadLock();
        List<SpanRecord>? hotResults = null;
        try
        {
            if (_traceIdx.TryGetValue(traceId, out var offsets))
            {
                hotResults = new List<SpanRecord>(offsets.Count);
                foreach (var o in offsets)
                    hotResults.Add(_hotSpans[o]);
            }
            if (_flushingSpans is { } flushing)
                foreach (var r in flushing)
                    if (r.TraceId.Equals(traceId))
                        (hotResults ??= new List<SpanRecord>()).Add(r);
        }
        finally
        {
            _lock.ExitReadLock();
        }

        if (hotResults != null)
            foreach (var r in hotResults.OrderBy(s => s.StartTimeUnixNano))
            {
                if (!r.SpanId.IsEmpty && !seen.Add(r.SpanId.RawValue)) continue;
                yield return r;
            }

        // Cold tier — scan the snapshot in parallel (bounded): a by-id lookup has
        // no time bounds, so every segment must be consulted, and doing that
        // sequentially took whole seconds once small segments piled up. A file
        // that compaction/retention deleted mid-flight is skipped (and healed out
        // of the snapshot) instead of failing the whole request.
        var segs = _coldSegments;
        if (segs.Length == 0) yield break;

        using var gate = new SemaphoreSlim(Math.Clamp(Environment.ProcessorCount / 2, 2, 8));
        var tasks = new Task<List<SpanRecord>?>[segs.Length];
        for (int i = 0; i < segs.Length; i++)
        {
            var seg = segs[i];
            tasks[i] = Task.Run(async () =>
            {
                await gate.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    List<SpanRecord>? found = null;
                    await foreach (var r in SpanReader.ReadTraceAsync(seg.FilePath, traceId, ct).ConfigureAwait(false))
                        (found ??= new List<SpanRecord>()).Add(r);
                    return found;
                }
                catch (OperationCanceledException) { throw; }
                catch (FileNotFoundException)
                {
                    RemoveColdSegment(seg);   // deleted behind our back — heal the snapshot
                    return null;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Trace lookup: skipping unreadable segment {File}", seg.FilePath);
                    return null;
                }
                finally { gate.Release(); }
            }, ct);
        }

        var cold = new List<SpanRecord>();
        foreach (var t in tasks)
            if (await t.ConfigureAwait(false) is { } part)
                cold.AddRange(part);

        foreach (var r in cold.OrderBy(s => s.StartTimeUnixNano))
        {
            if (!r.SpanId.IsEmpty && !seen.Add(r.SpanId.RawValue)) continue;
            yield return r;
        }
    }

    /// <summary>Drops a segment whose file no longer exists from the snapshot.</summary>
    private void RemoveColdSegment(SpanSegmentInfo seg)
    {
        _lock.EnterWriteLock();
        try
        {
            _coldSegments = Array.FindAll(_coldSegments, s => !ReferenceEquals(s, seg));
        }
        finally { _lock.ExitWriteLock(); }
        _logger.LogWarning("Cold span segment {File} vanished from disk — removed from the segment list", seg.FilePath);
    }

    public async IAsyncEnumerable<SpanRecord> SearchSpansAsync(
        DateTimeOffset?   from             = null,
        DateTimeOffset?   to               = null,
        string?           serviceName      = null,
        string?           spanName         = null,
        SpanStatusCode?   status           = null,
        long?             minDurationNanos = null,
        long?             maxDurationNanos = null,
        short?            httpStatusCode   = null,
        int               limit            = 200,
        IReadOnlyList<AttrHint>? attrHints = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        long fromNano = from.HasValue ? from.Value.ToUnixTimeMilliseconds() * 1_000_000L : long.MinValue;
        long toNano   = to.HasValue   ? to.Value.ToUnixTimeMilliseconds()   * 1_000_000L : long.MaxValue;

        int yielded = 0;

        // Same duplicate sources as GetTraceAsync, but results here cross traces, so the
        // identity is the pair. Every Add below sits on a path that yields, so this stays
        // bounded by `limit` — recording spans that were merely READ would put the match
        // count back into memory through this set after the segment buffer stopped doing it.
        var seen = new HashSet<(TraceId Trace, ulong Span)>();

        bool Match(SpanRecord s) =>
            s.StartTimeUnixNano >= fromNano &&
            s.StartTimeUnixNano <= toNano   &&
            (serviceName      is null || s.ServiceName.Equals(serviceName, StringComparison.OrdinalIgnoreCase)) &&
            (spanName         is null || s.Name.Contains(spanName, StringComparison.OrdinalIgnoreCase)) &&
            (status           is null || s.Status == status.Value) &&
            (httpStatusCode   is null || s.HttpStatusCode == httpStatusCode.Value) &&
            (minDurationNanos is null || s.DurationNanos >= minDurationNanos.Value) &&
            (maxDurationNanos is null || s.DurationNanos <= maxDurationNanos.Value);

        // Hot tier plus any in-flight flush snapshot (newest first), in ONE lock hold —
        // see GetTraceAsync for why the pair must not be read separately.
        _lock.EnterReadLock();
        List<SpanRecord> candidates;
        try
        {
            candidates = UnflushedSpansLocked()
                .Where(Match)
                .OrderByDescending(s => s.StartTimeUnixNano)
                .Take(limit)
                .ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }

        foreach (var r in candidates)
        {
            if (!r.SpanId.IsEmpty && !seen.Add((r.TraceId, r.SpanId.RawValue))) continue;
            if (yielded++ >= limit) yield break;
            yield return r;
        }

        if (yielded >= limit) yield break;

        // Cold tier — segment-level service pre-filter, then block-level skip inside SpanReader
        foreach (var seg in _coldSegments.OrderByDescending(s => s.MaxStartNano))
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            // Skip entire segment if it doesn't contain the requested service
            if (serviceName is not null && seg.Services.Length > 0 &&
                !Array.Exists(seg.Services, s => s.Equals(serviceName, StringComparison.OrdinalIgnoreCase)))
                continue;

            ct.ThrowIfCancellationRequested();

            // Manual enumeration so a segment deleted/corrupted mid-scan skips the
            // segment (yield inside try-catch is not allowed by the language).
            // A segment's file is written oldest-first, so streaming it under a global span
            // cap kept the OLD side of whichever segment the cap landed in and dropped its
            // new side — the caller then sorted and truncated an already old-shifted pool,
            // and its "newest page" quietly was not.
            //
            // Fixed by ordering, but the first version of that fix buffered EVERY match in
            // the segment before yielding any — and a month-wide query on a busy service
            // matches far more than a page. A SpanRecord carries two strings and, whenever
            // the query touches an attribute (`.db.system = "mssql"`), a decoded attribute
            // dictionary: on the order of a kilobyte each. Several hundred thousand matches
            // is therefore several hundred megabytes, and a 512 MB server died on exactly
            // that shape of query.
            //
            // Only the newest `limit` of this segment can ever be yielded, so that is all
            // this keeps: a min-heap on start time, evicting its oldest once full. Memory
            // is O(limit) again — the ordering the fix was for, without the buffer it cost.
            var top = new PriorityQueue<SpanRecord, long>();
            await using var e = SpanReader.SearchAsync(
                seg.FilePath, fromNano, toNano,
                serviceName, spanName, status, httpStatusCode,
                minDurationNanos, maxDurationNanos, attrHints, ct).GetAsyncEnumerator(ct);
            while (true)
            {
                SpanRecord r;
                try
                {
                    if (!await e.MoveNextAsync().ConfigureAwait(false)) break;
                    r = e.Current;
                }
                catch (OperationCanceledException) { throw; }
                catch (FileNotFoundException) { RemoveColdSegment(seg); break; }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Span search: skipping unreadable segment {File}", seg.FilePath);
                    break;
                }
                // Checked here, ADDED at the yield below: a span that loses its place in the
                // heap is never returned, and recording it would grow `seen` with the match
                // count rather than with the result — the same unbounded growth in the other
                // structure.
                if (!r.SpanId.IsEmpty && seen.Contains((r.TraceId, r.SpanId.RawValue))) continue;

                if (top.Count < limit) top.Enqueue(r, r.StartTimeUnixNano);
                else if (top.TryPeek(out _, out long oldestKept) && r.StartTimeUnixNano > oldestKept)
                    top.EnqueueDequeue(r, r.StartTimeUnixNano);
            }

            // The heap drains oldest-first; the caller wants newest-first.
            var segMatches = new List<SpanRecord>(top.Count);
            while (top.TryDequeue(out var kept, out _)) segMatches.Add(kept);
            segMatches.Reverse();

            foreach (var r in segMatches)
            {
                if (!r.SpanId.IsEmpty && !seen.Add((r.TraceId, r.SpanId.RawValue))) continue;
                if (yielded++ >= limit) yield break;
                yield return r;
            }
        }
    }

    // ── Flush ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// SYNCHRONOUSLY flushes the in-memory hot tier to a cold segment — waits out any
    /// in-flight background flush first, then runs the write on the calling thread.
    /// No-op when empty. Used on shutdown and by tests; the periodic path is
    /// <see cref="FlushIfDue"/>.
    /// </summary>
    internal void FlushHotTier()
    {
        while (true)
        {
            Task? inflight;
            List<SpanRecord>?     snapshot   = null;
            TaskCompletionSource? inlineDone = null;
            _lock.EnterWriteLock();
            try
            {
                inflight = _flushTask;
                if (!_flushInProgress)
                {
                    if (_hotSpans.Count == 0) return;
                    snapshot = TakeSnapshotLocked();
                    // Publish the INLINE flush as the in-flight one as well. Without a task
                    // to wait on, a second caller (the drainer's dispose overlapping the
                    // engine's) saw _flushInProgress with _flushTask still null and spun the
                    // write lock flat out for the whole multi-second build.
                    inlineDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    _flushTask = inlineDone.Task;
                }
            }
            finally { _lock.ExitWriteLock(); }

            if (snapshot is not null)
            {
                // On this thread — the caller wants it durable NOW.
                try     { CompleteFlush(snapshot); }
                finally { inlineDone!.TrySetResult(); }
                return;
            }
            try { inflight?.Wait(); } catch { /* CompleteFlush logged it */ }
        }
    }

    /// <summary>
    /// Flushes only when the hot tier has earned a segment: enough spans to be worth the
    /// index build and compression, or old enough that it should become compactable and
    /// retention-eligible regardless. Called on <see cref="Ingestion.SpanDrainer"/>'s tick;
    /// spans that do not meet either bar stay in memory, durable through the WAL.
    /// </summary>
    internal void FlushIfDue()
    {
        _lock.EnterWriteLock();
        try
        {
            if (_hotSpans.Count == 0) return;
            bool due = _hotSpans.Count >= MinSegmentSpans
                    || (_hotSince is { } since && DateTime.UtcNow - since >= MaxHotAge);
            if (due) TryStartFlushLocked();
        }
        finally { _lock.ExitWriteLock(); }
    }

    /// <summary>
    /// Detaches the hot tier for a flush: the snapshot goes to the writer, a fresh list
    /// takes its place, and the WAL opens its two-generation window. Caller holds the
    /// write lock and MUST hand the snapshot to <see cref="CompleteFlush"/>.
    /// </summary>
    private List<SpanRecord> TakeSnapshotLocked()
    {
        // The log opens its window FIRST: if BeginFlush throws, the tier must still be
        // where it was — detaching first would strand the snapshot with no flush to carry
        // it and no caller holding a reference.
        _wal.BeginFlush();

        var snapshot = _hotSpans;
        _hotSpans = new List<SpanRecord>();
        _traceIdx.Clear();
        _hotSince = null;
        _flushInProgress = true;
        _flushingSpans   = snapshot;
        return snapshot;
    }

    /// <summary>
    /// Starts a background flush unless one is already running — or the engine is shutting
    /// down. The disposed check is the FENCE that lets Dispose free the lock safely: without
    /// it a span arriving between Dispose's final drain and <c>_lock.Dispose()</c> could
    /// start a flush that publishes its segment and then faults trying to commit the WAL,
    /// leaving a segment on disk whose spans the log still replays — permanent duplicates.
    /// Spans refused here stay in the hot tier AND in the WAL, so the next start replays them.
    /// </summary>
    private void TryStartFlushLocked()
    {
        if (Volatile.Read(ref _disposed) != 0 || _flushInProgress || _hotSpans.Count == 0) return;
        var snapshot = TakeSnapshotLocked();
        _flushTask = Task.Run(() => CompleteFlush(snapshot));
    }

    /// <summary>
    /// The heavy half of a flush, OFF the engine lock: builds the segment (sort, LZ4-HC,
    /// indexes, sidecars, fsync — see SpanWriter), then re-acquires only to publish and
    /// commit. On failure the snapshot returns to the hot tier — the WAL still holds
    /// every span of it (Abandon keeps its generation live), so nothing is lost and the
    /// next due-check retries.
    /// </summary>
    private void CompleteFlush(List<SpanRecord> snapshot)
    {
        SpanSegmentInfo? info    = null;
        Exception?       failure = null;
        try
        {
            _beforeSegmentWrite?.Invoke();   // test seam: parks a flush mid-build
            // The guard is published as soon as the NAME exists, before the rename that makes
            // the file visible. Setting it from the return value happened after that rename, so
            // the startup cold scan could adopt the segment in between — while _flushingSpans
            // still held the same spans, which the stats, service-graph and volume paths add to
            // the cold tiers without de-duplicating. Narrow (microseconds, once per process) and
            // free to close.
            info = SpanWriter.Write(_dataDir, snapshot,
                                    onNamed: path => _publishingSegmentPath = path);
        }
        catch (Exception ex) { failure = ex; }

        _publishingSegmentPath = info?.FilePath ?? _publishingSegmentPath;
        try
        {
            // ── Publish (short lock hold). _flushInProgress deliberately STAYS set: it is
            //    what stops another flush opening a WAL cycle before this one commits.
            _lock.EnterWriteLock();
            try
            {
                if (info is { } written)
                {
                    // Deduped by path: the segment became VISIBLE on disk (renamed) before
                    // this publish, so the cold scan may already have registered it. Two
                    // entries for one file double-count every aggregate and let compaction
                    // merge the file with itself.
                    if (!Array.Exists(_coldSegments,
                            s => string.Equals(s.FilePath, written.FilePath, StringComparison.Ordinal)))
                        _coldSegments = [.. _coldSegments, written];
                }
                else
                {
                    _wal.AbandonFlush();          // flag-only, no I/O — fine under the lock
                    RestoreSnapshotLocked(snapshot);
                }
                _flushingSpans = null;            // the segment (or the restored tier) now carries them
            }
            finally { _lock.ExitWriteLock(); }

            // ── Commit the log OFF the lock: it relocates the tail and issues two
            //    whole-mapping device flushes. Under the exclusive lock that would stall
            //    every ingest and query for the duration — reinstating exactly the stall
            //    this whole design removed. A failure here is survivable and reported: the
            //    header keeps the flushed generation, so those spans replay next start as
            //    duplicates of a segment that is already durable, which the read paths
            //    dedupe by span id.
            if (info is not null)
            {
                try { _wal.CommitFlush(); }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Span WAL commit failed after publishing {File} — its spans will replay on the next start",
                        info.FilePath);
                }
            }
        }
        finally
        {
            // Reopening the gate is the LAST thing and it always happens: an exception
            // anywhere above would otherwise leave _flushInProgress wedged true — no
            // further flush would ever start, and FlushHotTier/Dispose would spin.
            _lock.EnterWriteLock();
            try
            {
                _flushInProgress       = false;
                _flushTask             = null;
                _publishingSegmentPath = null;
            }
            finally { _lock.ExitWriteLock(); }
        }

        if (failure is null)
            _logger.LogInformation("Flushed {Count} spans to {File}", snapshot.Count, info!.FilePath);
        else
            _logger.LogError(failure, "Failed to flush hot-tier spans to cold storage — spans returned to the hot tier");
    }

    /// <summary>
    /// Every span not yet carried by a REGISTERED cold segment: the live hot tier, plus the
    /// snapshot a flush has detached but not yet published. <b>Caller holds the read lock.</b>
    ///
    /// <para>Aggregates must count from this, not from <c>_hotSpans</c> alone. Once the
    /// segment build moved off the lock, the detached snapshot — up to
    /// <see cref="HotFlushThreshold"/> spans — belonged to neither tier for the build's whole
    /// duration, so the trace list, per-service stats, volume sparkline and service graph
    /// each carried a rolling hole just behind the live edge. At load, where flushes run
    /// back to back, that hole was close to permanent: rows visibly vanished at snapshot
    /// time and reappeared at publish.</para>
    ///
    /// <para>Ordering is oldest-first (the detached snapshot left the tier before anything
    /// now in it arrived), matching what the callers assume of <c>_hotSpans</c>.</para>
    /// </summary>
    private IEnumerable<SpanRecord> UnflushedSpansLocked()
    {
        if (_flushingSpans is { } flushing)
            foreach (var s in flushing) yield return s;
        foreach (var s in _hotSpans) yield return s;
    }

    /// <summary>Spans in <see cref="UnflushedSpansLocked"/>. Caller holds the read lock.</summary>
    private int UnflushedCountLocked() => _hotSpans.Count + (_flushingSpans?.Count ?? 0);

    /// <summary>
    /// Puts a failed flush's snapshot back in front of whatever arrived since, and
    /// rebuilds the trace index over the combined list. Under _lock(write). A NEW list —
    /// never the snapshot itself: readers may still be iterating it through the
    /// <see cref="_flushingSpans"/> reference they took lock-free, and mutating a list
    /// under a live enumerator faults them.
    /// </summary>
    private void RestoreSnapshotLocked(List<SpanRecord> snapshot)
    {
        var combined = new List<SpanRecord>(snapshot.Count + _hotSpans.Count);
        combined.AddRange(snapshot);
        combined.AddRange(_hotSpans);
        _hotSpans = combined;

        _traceIdx.Clear();
        for (int i = 0; i < _hotSpans.Count; i++)
        {
            var r = _hotSpans[i];
            if (!_traceIdx.TryGetValue(r.TraceId, out var offsets))
                _traceIdx[r.TraceId] = offsets = new List<int>(4);
            offsets.Add(i);
        }
        _hotSince ??= DateTime.UtcNow;
    }

    // ── Cold segment discovery ─────────────────────────────────────────────────

    /// <summary>
    /// Discovers existing cold segments. Runs in the background (see
    /// <c>TraceCompactionWorker</c>) — ingest and queries work from second zero,
    /// cold trace data becomes queryable when this completes. Merges with any
    /// segments flushed while the scan was running.
    /// </summary>
    internal void LoadColdSegments()
    {
        var sw     = System.Diagnostics.Stopwatch.StartNew();
        var loaded = new List<SpanSegmentInfo>();
        foreach (var file in Directory.EnumerateFiles(_dataDir, "*.trc").OrderBy(f => f))
        {
            // A flush that has renamed its segment into place but not yet published it owns
            // this file: its spans are ALSO still in _flushingSpans, so registering it here
            // would count them twice in every aggregate until the publish lands.
            if (string.Equals(file, _publishingSegmentPath, StringComparison.Ordinal)) continue;

            try
            {
                loaded.Add(SpanReader.ReadSegmentInfo(file));
            }
            catch (Exception ex)
            {
                // v1 files (12-byte footer) will fail with wrong footer magic — delete them.
                _logger.LogWarning(ex, "Unreadable segment {File} — deleting (likely format v1)", file);
                DeleteSegmentFiles(file);
            }
        }

        _lock.EnterWriteLock();
        try
        {
            // Segments flushed while we were scanning are already in the snapshot;
            // keep them and add the discovered ones (dedup by path).
            var known = new HashSet<string>(_coldSegments.Select(s => s.FilePath), StringComparer.Ordinal);
            var next  = new List<SpanSegmentInfo>(loaded.Count + _coldSegments.Length);
            next.AddRange(loaded.Where(s => !known.Contains(s.FilePath)));
            next.AddRange(_coldSegments);
            _coldSegments = next.ToArray();
        }
        finally { _lock.ExitWriteLock(); }

        _logger.LogInformation("Loaded {Count} cold span segments in {Ms} ms",
            _coldSegments.Length, sw.ElapsedMilliseconds);
    }

    /// <summary>
    /// Merges small cold segments until the backlog is drained. Each pass stays
    /// memory-bounded (≤ MaxSegmentsPerPass files, ≤ MaxSpansPerPass spans), but
    /// passes repeat until nothing small remains — one hourly run used to merge a
    /// single batch of 20, which on a busy instance was slower than the flush rate
    /// produced new files, so the backlog only ever grew.
    /// </summary>
    internal void CompactSmallSegments()
    {
        const int MaxPasses = 500;   // safety valve, ~10k merged segments per run
        int passes = 0;
        while (CompactOnePass() && ++passes < MaxPasses) { }
        if (passes > 0)
            _logger.LogInformation("Compaction run finished: {Passes} pass(es), {Count} cold segments remain",
                passes, _coldSegments.Length);
    }

    /// <summary>
    /// Picks the next compaction batch: the oldest segments of COMPARABLE SIZE whose
    /// combined time range stays inside a 24-hour window.
    ///
    /// <para>Trace retention deletes a file only when its NEWEST span is past the TTL, so
    /// an unbounded batch span would keep old spans alive past their deadline — and a
    /// merged file that stays small on a quiet server would keep re-merging with newer
    /// files, advancing its MaxStartNano forever and never expiring at all. Hence the
    /// window.</para>
    ///
    /// <para>The size tier is what keeps the rewrite bounded. Merging strictly by age let
    /// one accumulator file absorb every new arrival hour after hour, rewriting all of its
    /// spans each time, until it finally crossed <see cref="CompactionThreshold"/> — about
    /// nine bytes written per byte of data retained. Restricting a batch to one tier means
    /// a file only ever merges with peers of its own magnitude, so it roughly doubles per
    /// merge and a span is rewritten O(log) times instead of O(n).</para>
    ///
    /// Empty result = nothing worth compacting.
    /// </summary>
    internal static List<SpanSegmentInfo> SelectCompactionBatch(SpanSegmentInfo[] segments)
    {
        const long MaxSpanNanos = 24L * 3600 * 1_000_000_000; // 24 h

        var candidates = segments
            .Where(s => s.SpanCount < CompactionThreshold || s.FormatVersion < 3)
            .OrderBy(s => s.MinStartNano)
            .ToList();

        // Oldest candidate first, so old data still drains ahead of new. Each seed offers
        // its own tier and 24 h window; peers outside either are left for a later pass.
        for (int i = 0; i < candidates.Count; i++)
        {
            var  seed        = candidates[i];
            int  tier        = TierOf(seed.SpanCount);
            long windowStart = seed.MinStartNano;

            var batch = new List<SpanSegmentInfo>(MaxSegmentsPerPass) { seed };
            for (int j = i + 1; j < candidates.Count && batch.Count < MaxSegmentsPerPass; j++)
            {
                var s = candidates[j];

                // MinStartNano is the sort key, so once it clears the window nothing later
                // can qualify — the only sound place to stop early.
                if (s.MinStartNano - windowStart > MaxSpanNanos) break;

                // MaxStartNano is NOT monotonic in that order: one wide segment (a long time
                // range, still under the compaction threshold) says nothing about the ones
                // behind it. Stopping here would strand same-tier peers that sit well inside
                // the window — with the tier filter narrowing matches, often to the point of
                // selecting nothing at all.
                if (s.MaxStartNano - windowStart > MaxSpanNanos) continue;
                if (TierOf(s.SpanCount) != tier) continue;                // wrong magnitude
                batch.Add(s);
            }

            if (batch.Count >= 2) return batch;
            // A lone legacy file has no peer to wait for — it is rewritten to migrate it,
            // not to merge it, so the tier rule does not apply.
            if (seed.FormatVersion < 3) return [seed];
        }
        return [];
    }

    /// <summary>
    /// Size bucket of a segment: <c>floor(log4(spanCount))</c> by integer division, so a
    /// tier covers a 4× range of sizes. Four is a compromise — a smaller ratio merges more
    /// eagerly and rewrites more, a larger one leaves more files lying around for queries
    /// to open.
    /// </summary>
    private static int TierOf(int spanCount)
    {
        const int TierRatio = 4;
        int tier = 0;
        for (int n = Math.Max(1, spanCount); n >= TierRatio; n /= TierRatio) tier++;
        return tier;
    }

    private bool CompactOnePass()
    {
        // Bounded pass: take only the oldest small segments and cap the spans loaded
        // into memory. Compaction used to merge ALL small segments at once, which on a
        // memory-limited container exhausted the heap (tiny allocations threw OOM) and
        // left the segments un-compacted — so they piled up and every pass failed worse.
        // Legacy-v2 files are selected regardless of size so old data migrates to the
        // v3 format (and shrinks) in the background.
        var small = SelectCompactionBatch(_coldSegments);
        if (small.Count == 0) return false;

        var allSpans  = new List<SpanRecord>();
        var processed = new List<SpanSegmentInfo>(small.Count);
        foreach (var seg in small)
        {
            if (allSpans.Count >= MaxSpansPerPass) break;   // memory cap reached — stop taking more
            try
            {
                allSpans.AddRange(SpanReader.ReadAll(seg.FilePath));
                processed.Add(seg);
            }
            catch (Exception ex) { _logger.LogWarning(ex, "Compaction: failed to read {File}", seg.FilePath); }
        }

        // A single v3 file needs no rewrite; a single v2 file still migrates.
        if (allSpans.Count == 0) return false;
        if (processed.Count < 2 && processed.All(s => s.FormatVersion >= 3)) return false;

        try
        {
            // recoverable:false — the sources are still on disk until the swap below, so a
            // merge temp resurrected after a crash would publish a SECOND copy of every
            // span it merged. Only a hot-tier flush's temp is worth recovering.
            var merged = SpanWriter.Write(_dataDir, allSpans, recoverable: false);
            _logger.LogInformation("Compacted {Count} small segments → {File} ({Spans} spans)",
                processed.Count, Path.GetFileName(merged.FilePath), allSpans.Count);

            // Swap the snapshot first (readers stop picking the old files up),
            // delete the merged-away files after. An in-flight reader that still
            // holds the old snapshot skips the deleted file gracefully.
            _lock.EnterWriteLock();
            try
            {
                var next = new List<SpanSegmentInfo>(_coldSegments.Length);
                foreach (var s in _coldSegments)
                    if (!processed.Contains(s)) next.Add(s);
                next.Add(merged);
                _coldSegments = next.ToArray();
            }
            finally { _lock.ExitWriteLock(); }

            foreach (var seg in processed)   // delete only the segments we actually merged
                DeleteSegmentFiles(seg.FilePath);   // .trc + all companion sidecars
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Compaction: failed to write merged segment");
            return false;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static IReadOnlyDictionary<string, object?>? DeserializeAttributes(byte[] bytes)
    {
        try
        {
            return MessagePackSerializer.Deserialize<Dictionary<string, object?>>(bytes);
        }
        catch
        {
            return null;
        }
    }

    // ── ITraceStatsProvider ────────────────────────────────────────────────────

    public Task<IReadOnlyList<ServiceSegmentStats>> GetAggregateStatsAsync(
        DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;

        // Accumulator: service name → mutable bucket array + counters
        var agg = new Dictionary<string, (uint[] Buckets, uint Spans, uint Errors, long MinDur, long MaxDur)>(
            StringComparer.OrdinalIgnoreCase);

        void Merge(ServiceSegmentStats s)
        {
            if (!agg.TryGetValue(s.ServiceName, out var a))
            {
                var b = new uint[HistogramBuckets.Count];
                a = (b, 0, 0, long.MaxValue, long.MinValue);
            }
            for (int i = 0; i < HistogramBuckets.Count; i++) a.Buckets[i] += s.Buckets[i];
            a.Spans  += s.SpanCount;
            a.Errors += s.ErrorCount;
            if (s.MinDurationNanos < a.MinDur) a.MinDur = s.MinDurationNanos;
            if (s.MaxDurationNanos > a.MaxDur) a.MaxDur = s.MaxDurationNanos;
            agg[s.ServiceName] = a;
        }

        // Hot tier — compute on demand (≤50K spans, fast). The cold array is snapshotted
        // under the SAME lock hold: taken separately, a flush publishing in between would
        // be counted twice (once from the in-flight snapshot, once from its sidecar).
        SpanSegmentInfo[] statsSegs;
        _lock.EnterReadLock();
        try
        {
            statsSegs = _coldSegments;
            foreach (var s in UnflushedSpansLocked())
            {
                if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
                Merge(new ServiceSegmentStats
                {
                    ServiceName      = s.ServiceName,
                    SpanCount        = 1,
                    ErrorCount       = s.Status == SpanStatusCode.Error ? 1u : 0u,
                    MinDurationNanos = s.DurationNanos,
                    MaxDurationNanos = s.DurationNanos,
                    Buckets          = BucketOf(s.DurationNanos),
                });
            }
        }
        finally { _lock.ExitReadLock(); }

        // Cold tier — read .stats sidecar files only (no span deserialization)
        foreach (var seg in statsSegs)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            foreach (var s in SpanReader.ReadStats(seg.FilePath))
                Merge(s);
        }

        var result = new List<ServiceSegmentStats>(agg.Count);
        foreach (var (name, (buckets, spans, errors, minDur, maxDur)) in agg)
            result.Add(new ServiceSegmentStats
            {
                ServiceName      = name,
                SpanCount        = spans,
                ErrorCount       = errors,
                MinDurationNanos = minDur == long.MaxValue ? 0 : minDur,
                MaxDurationNanos = maxDur == long.MinValue ? 0 : maxDur,
                Buckets          = buckets,
            });

        return Task.FromResult<IReadOnlyList<ServiceSegmentStats>>(result);
    }

    // ── IServiceGraphProvider ──────────────────────────────────────────────────

    public Task<ServiceGraphDto> GetServiceGraphAsync(
        DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;

        // Accumulate edges: (from, to) → (calls, errors, buckets)
        var edgeAgg = new Dictionary<(string, string), (uint Calls, uint Errors, uint[] Buckets)>(32);

        void MergeEdge(string f, string t, uint calls, uint errors, uint[] buckets)
        {
            var key = (f, t);
            if (!edgeAgg.TryGetValue(key, out var acc))
                acc = (0, 0, new uint[HistogramBuckets.Count]);
            acc.Calls  += calls;
            acc.Errors += errors;
            for (int i = 0; i < HistogramBuckets.Count; i++) acc.Buckets[i] += buckets[i];
            edgeAgg[key] = acc;
        }

        // Hot tier: derive edges on-demand (all spans in memory, accurate). Includes the
        // in-flight flush snapshot, and snapshots the cold array under the same hold — see
        // GetAggregateStatsAsync for why both halves must come from one instant.
        SpanSegmentInfo[] graphSegs;
        _lock.EnterReadLock();
        try
        {
            graphSegs = _coldSegments;
            if (UnflushedCountLocked() > 0)
            {
                // Materialised ONCE. The graph needs two passes — parents have to be known
                // before edges can be drawn — and calling the iterator twice allocated a second
                // one for no reason, inside the read lock, in the window that is effectively
                // permanent when flushes run back to back.
                var unflushed = new List<SpanRecord>(UnflushedCountLocked());
                unflushed.AddRange(UnflushedSpansLocked());

                var spanSvc = new Dictionary<SpanId, string>(unflushed.Count);
                foreach (var s in unflushed)
                    if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano)
                        spanSvc[s.SpanId] = s.ServiceName;

                foreach (var s in unflushed)
                {
                    if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
                    if (s.ParentSpanId.IsEmpty) continue;
                    if (!spanSvc.TryGetValue(s.ParentSpanId, out var psvc)) continue;
                    if (string.Equals(psvc, s.ServiceName, StringComparison.Ordinal)) continue;
                    MergeEdge(psvc, s.ServiceName, 1,
                              s.Status == SpanStatusCode.Error ? 1u : 0u,
                              BucketOf(s.DurationNanos));
                }
            }
        }
        finally { _lock.ExitReadLock(); }

        // Cold tier — read .svcgraph sidecars (no span deserialization)
        foreach (var seg in graphSegs)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            foreach (var e in ServiceGraphSidecar.ReadEdges(seg.FilePath))
                MergeEdge(e.From, e.To, e.CallCount, e.ErrorCount, e.Buckets);
        }

        // Build edges list
        var edgeDtos = new List<ServiceEdgeDto>(edgeAgg.Count);
        foreach (var ((from2, to2), (calls, errors, buckets)) in edgeAgg)
            edgeDtos.Add(new ServiceEdgeDto
            {
                From      = from2,
                To        = to2,
                CallCount = calls,
                ErrorCount= errors,
                ErrorRate = calls > 0 ? (double)errors / calls : 0,
                P95Ms     = HistogramBuckets.Percentile(buckets, 0.95),
            });

        // Derive nodes from edges + stats provider
        // Union all service names from edges
        var nodeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var e in edgeDtos) { nodeNames.Add(e.From); nodeNames.Add(e.To); }

        // Load per-service stats for node metrics (reuse existing stats aggregation)
        // We call GetAggregateStatsAsync synchronously since it's Task.FromResult internally
        var statsTask = GetAggregateStatsAsync(from, to, ct);
        var statsMap  = new Dictionary<string, ServiceSegmentStats>(StringComparer.OrdinalIgnoreCase);
        // statsTask is already completed (Task.FromResult)
        foreach (var s in statsTask.Result)
            statsMap[s.ServiceName] = s;

        var nodeDtos = new List<ServiceNodeDto>(nodeNames.Count);
        foreach (var name in nodeNames)
        {
            statsMap.TryGetValue(name, out var st);
            nodeDtos.Add(new ServiceNodeDto
            {
                ServiceName = name,
                SpanCount   = st?.SpanCount ?? 0,
                ErrorRate   = st is { SpanCount: > 0 } ? (double)st.ErrorCount / st.SpanCount : 0,
                P95Ms       = st is not null ? HistogramBuckets.Percentile(st.Buckets, 0.95) : 0,
            });
        }

        return Task.FromResult(new ServiceGraphDto
        {
            Nodes = [.. nodeDtos],
            Edges = [.. edgeDtos],
        });
    }

    // ── ITraceSummaryProvider ──────────────────────────────────────────────────

    private static readonly string[] MethodKeys = { "http.request.method", "http.method" };
    private static readonly string[] PathKeys   = { "url.path", "http.target", "http.route", "url.full", "http.url" };

    /// <summary>
    /// Trace volume + sparkline over [from,to]. Cold tiers are served purely from the
    /// tiny <c>.tracesum</c> volume headers (no span deserialisation); the hot tier is
    /// grouped live. Bounded by (segments × grid-cells) — cheap for any window width.
    /// </summary>
    public async Task<TraceVolume> GetTraceVolumeAsync(
        DateTimeOffset from, DateTimeOffset to, int buckets, CancellationToken ct = default)
    {
        long fromNano  = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano    = to.ToUnixTimeMilliseconds()   * 1_000_000L;
        long rangeNano = Math.Max(1L, toNano - fromNano);
        if (buckets < 1) buckets = 1;

        var total = new double[buckets];
        var error = new double[buckets];
        int totalTraces = 0, errorTraces = 0;

        void Add(long startNano, uint traces, uint errors)
        {
            if (startNano < fromNano || startNano > toNano) return;
            int b = (int)Math.Clamp((startNano - fromNano) * (long)buckets / rangeNano, 0, buckets - 1);
            total[b]    += traces;
            error[b]    += errors;
            totalTraces += (int)traces;
            errorTraces += (int)errors;
        }

        // Snapshot cold segments + aggregate hot tier under one short read-lock.
        SpanSegmentInfo[] segs;
        _lock.EnterReadLock();
        try
        {
            segs = _coldSegments.ToArray();

            // Includes the in-flight flush snapshot: without it a refresh mid-build shows a
            // dip in the sparkline exactly where the newest traces are.
            if (UnflushedCountLocked() > 0)
            {
                var hot = new Dictionary<TraceId, HotVolAcc>(_traceIdx.Count);
                foreach (var s in UnflushedSpansLocked()) AccumulateVolume(hot, s);
                foreach (var a in hot.Values) Add(a.HasRoot ? a.RootStart : a.Earliest, 1, a.Err ? 1u : 0u);
            }
        }
        finally { _lock.ExitReadLock(); }

        long half = TraceSummarySidecar.GridNanos / 2;
        foreach (var seg in segs)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            ct.ThrowIfCancellationRequested();

            var vs = TraceSummarySidecar.ReadVolume(seg.FilePath);
            if (vs is not null)
            {
                foreach (var e in vs.Buckets)
                    Add(e.GridIndex * TraceSummarySidecar.GridNanos + half, e.TraceCount, e.ErrorCount);
            }
            else
            {
                // Legacy segment written before .tracesum existed — derive volume from spans
                // (bounded per segment). Such segments vanish as retention/compaction ages them out.
                var legacy = new Dictionary<TraceId, HotVolAcc>();
                await foreach (var s in SpanReader.SearchAsync(
                    seg.FilePath, fromNano, toNano, null, null, null, null, null, null, null, ct))
                    AccumulateVolume(legacy, s);
                foreach (var a in legacy.Values) Add(a.HasRoot ? a.RootStart : a.Earliest, 1, a.Err ? 1u : 0u);
            }
        }

        return new TraceVolume
        {
            TotalTraces    = totalTraces,
            ErrorTraces    = errorTraces,
            TotalSparkline = total,
            ErrorSparkline = error,
        };
    }

    private static void AccumulateVolume(Dictionary<TraceId, HotVolAcc> acc, SpanRecord s)
    {
        ref var a = ref CollectionsMarshal.GetValueRefOrAddDefault(acc, s.TraceId, out _);
        if (!a.Init) { a.Init = true; a.Earliest = long.MaxValue; }
        if (s.Status == SpanStatusCode.Error) a.Err = true;
        if (s.StartTimeUnixNano < a.Earliest) a.Earliest = s.StartTimeUnixNano;
        if (s.ParentSpanId.IsEmpty && !a.HasRoot) { a.HasRoot = true; a.RootStart = s.StartTimeUnixNano; }
    }

    /// <summary>
    /// Newest-first, filtered trace rows. Cold tiers are served from <c>.tracesum</c> bodies
    /// (no span deserialisation); the hot tier is grouped live. Traces are merged by id across
    /// tiers, then the cheap filters are applied and the newest <paramref name="limit"/> kept.
    /// </summary>
    public async Task<IReadOnlyList<TraceSummary>> GetTraceListAsync(
        DateTimeOffset   from,
        DateTimeOffset   to,
        string?          serviceName,
        string?          spanName,
        SpanStatusCode?  status,
        long?            minDurationNanos,
        long?            maxDurationNanos,
        int              limit,
        CancellationToken ct = default)
    {
        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;
        int  scanCap  = Math.Max(limit * 5, 500);

        var merged = new Dictionary<TraceId, MergedTrace>(scanCap);

        // Hot tier — group live spans (newest data) under read-lock. Snapshot cold too.
        SpanSegmentInfo[] segs;
        _lock.EnterReadLock();
        try
        {
            segs = _coldSegments.ToArray();
            // Includes the in-flight flush snapshot — otherwise the newest rows disappear
            // from the trace list for the duration of every segment build.
            foreach (var s in UnflushedSpansLocked())
            {
                if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
                MergeSpanInto(merged, s);
            }
        }
        finally { _lock.ExitReadLock(); }

        // Cold — newest-first. .tracesum bodies where present, else legacy span read.
        Array.Sort(segs, static (a, b) => b.MaxStartNano.CompareTo(a.MaxStartNano));
        foreach (var seg in segs)
        {
            if (merged.Count >= scanCap) break;
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            if (serviceName is not null && seg.Services.Length > 0 &&
                !Array.Exists(seg.Services, x => x.Equals(serviceName, StringComparison.OrdinalIgnoreCase)))
                continue;

            ct.ThrowIfCancellationRequested();

            if (TraceSummarySidecar.Exists(seg.FilePath))
            {
                foreach (var r in TraceSummarySidecar.ReadSummaries(seg.FilePath))
                {
                    if (r.RootStartNano < fromNano || r.RootStartNano > toNano) continue;
                    MergeSummaryInto(merged, r);
                }
            }
            else
            {
                // Legacy segment — fall back to the span read (bounded by segment + filters).
                await foreach (var s in SpanReader.SearchAsync(
                    seg.FilePath, fromNano, toNano, serviceName, spanName, status, null,
                    minDurationNanos, maxDurationNanos, null, ct))
                    MergeSpanInto(merged, s);
            }
        }

        // Filter + sort newest-first + take limit.
        var list = new List<TraceSummary>(merged.Count);
        foreach (var m in merged.Values)
        {
            var rowStatus = m.HasError ? SpanStatusCode.Error : m.RootStatus;
            if (status is not null && rowStatus != status.Value) continue;
            if (serviceName is not null && !ServiceMatch(m, serviceName)) continue;
            if (spanName is not null && !m.Name.Contains(spanName, StringComparison.OrdinalIgnoreCase)) continue;
            if (minDurationNanos is not null && m.DurationNanos < minDurationNanos.Value) continue;
            if (maxDurationNanos is not null && m.DurationNanos > maxDurationNanos.Value) continue;
            list.Add(m.ToSummary());
        }

        list.Sort(static (a, b) => b.RootStartNano.CompareTo(a.RootStartNano));
        if (list.Count > limit) list.RemoveRange(limit, list.Count - limit);

        return list;
    }

    private static MergedTrace GetOrAdd(Dictionary<TraceId, MergedTrace> merged, TraceId id)
    {
        if (!merged.TryGetValue(id, out var m)) { m = new MergedTrace { TraceId = id }; merged[id] = m; }
        return m;
    }

    private static void MergeSpanInto(Dictionary<TraceId, MergedTrace> merged, SpanRecord s)
    {
        var m = GetOrAdd(merged, s.TraceId);
        m.SpanCount++;
        if (s.Status == SpanStatusCode.Error) m.HasError = true;
        m.Services.Add(s.ServiceName);
        if (s.StartTimeUnixNano < m.EarliestNano) { m.EarliestNano = s.StartTimeUnixNano; m.EarliestService = s.ServiceName; }
        if (s.ParentSpanId.IsEmpty && !m.HasRoot)
        {
            m.HasRoot        = true;
            m.RootSpanId     = s.SpanId;
            m.RootStartNano  = s.StartTimeUnixNano;
            m.DurationNanos  = s.DurationNanos;
            m.RootStatus     = s.Status;
            m.HttpStatusCode = s.HttpStatusCode;
            m.Name           = s.Name;
            m.ServiceName    = s.ServiceName;
            m.HttpMethod     = GetAttr(s.Attributes, MethodKeys);
            m.HttpPath       = GetAttr(s.Attributes, PathKeys);
        }
    }

    private static void MergeSummaryInto(Dictionary<TraceId, MergedTrace> merged, TraceSummary r)
    {
        var m = GetOrAdd(merged, r.TraceId);
        m.SpanCount += r.SpanCount;
        if (r.HasError) m.HasError = true;
        foreach (var sv in r.Services) m.Services.Add(sv);
        if (r.RootStartNano < m.EarliestNano) { m.EarliestNano = r.RootStartNano; m.EarliestService = r.ServiceName; }
        if (r.HasRoot && !m.HasRoot)
        {
            m.HasRoot        = true;
            m.RootSpanId     = r.RootSpanId;
            m.RootStartNano  = r.RootStartNano;
            m.DurationNanos  = r.DurationNanos;
            m.RootStatus     = r.RootStatus;
            m.HttpStatusCode = r.HttpStatusCode;
            m.Name           = r.Name;
            m.ServiceName    = r.ServiceName;
            m.HttpMethod     = r.HttpMethod;
            m.HttpPath       = r.HttpPath;
        }
    }

    private static bool ServiceMatch(MergedTrace m, string service)
    {
        if (m.ServiceName.Equals(service, StringComparison.OrdinalIgnoreCase)) return true;
        foreach (var sv in m.Services)
            if (sv.Equals(service, StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    private static string GetAttr(IReadOnlyDictionary<string, object?>? attrs, string[] keys)
    {
        if (attrs is null) return string.Empty;
        foreach (var k in keys)
            if (attrs.TryGetValue(k, out var v) && v is not null)
                return v.ToString() ?? string.Empty;
        return string.Empty;
    }

    private struct HotVolAcc
    {
        public bool Init;
        public long Earliest;
        public bool HasRoot;
        public long RootStart;
        public bool Err;
    }

    private sealed class MergedTrace
    {
        public TraceId         TraceId;
        public uint            SpanCount;
        public bool            HasError;
        public long            EarliestNano = long.MaxValue;
        public string          EarliestService = string.Empty;

        public bool            HasRoot;
        public SpanId          RootSpanId;
        public long            RootStartNano;
        public long            DurationNanos;
        public SpanStatusCode  RootStatus;
        public short           HttpStatusCode;
        public string          Name        = string.Empty;
        public string          ServiceName = string.Empty;
        public string          HttpMethod  = string.Empty;
        public string          HttpPath    = string.Empty;

        public readonly HashSet<string> Services = new(2, StringComparer.Ordinal);

        public TraceSummary ToSummary() => new()
        {
            TraceId        = TraceId,
            RootSpanId     = RootSpanId,
            RootStartNano  = HasRoot ? RootStartNano : EarliestNano,
            DurationNanos  = DurationNanos,
            SpanCount      = SpanCount,
            HasRoot        = HasRoot,
            HasError       = HasError,
            RootStatus     = RootStatus,
            HttpStatusCode = HttpStatusCode,
            Name           = Name,
            ServiceName    = HasRoot ? ServiceName : EarliestService,
            HttpMethod     = HttpMethod,
            HttpPath       = HttpPath,
            Services       = [.. Services],
        };
    }

    private static uint[] BucketOf(long durationNanos)
    {
        var b = new uint[HistogramBuckets.Count];
        b[HistogramBuckets.IndexOf(durationNanos)] = 1;
        return b;
    }

    public void Dispose()
    {
        // The store fences new background flushes (TryStartFlushLocked refuses once it is
        // set), so after the drain below no task can still be holding the lock we free.
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0) return;

        // Waits out an in-flight background flush, then drains the tier synchronously —
        // a clean stop commits the WAL and leaves nothing to replay. If the final flush
        // fails, the WAL still holds every span (Abandon keeps its generation live).
        try { FlushHotTier(); }
        catch (Exception ex) { _logger.LogError(ex, "Final span flush failed — the WAL replays the tier next start"); }

        // The WAL's disposal carries the log's last fsync, so nothing above may be allowed
        // to skip it. ReaderWriterLockSlim.Dispose throws if a thread still holds or waits
        // on the lock — a retention pass, a compaction or a straggling query can — and that
        // throw used to take the WAL's close with it. The lock's own resources are trivial;
        // the log's durability is not.
        try { _lock.Dispose(); }
        catch (SynchronizationLockException ex) { _logger.LogWarning(ex, "Trace engine lock still in use at shutdown — left to the finalizer"); }
        finally { _wal.Dispose(); }
    }

    // ── IRetentionTarget ───────────────────────────────────────────────────

    public string RetentionKey => "traces";

    public Task<int> PruneAsync(TimeSpan ttl, CancellationToken ct = default)
    {
        var cutoffNano = DateTimeOffset.UtcNow.Subtract(ttl).ToUnixTimeMilliseconds() * 1_000_000L;

        List<SpanSegmentInfo> toDelete;
        _lock.EnterWriteLock();
        try
        {
            toDelete = _coldSegments.Where(s => s.MaxStartNano < cutoffNano).ToList();
            if (toDelete.Count > 0)
                _coldSegments = _coldSegments.Where(s => s.MaxStartNano >= cutoffNano).ToArray();
        }
        finally { _lock.ExitWriteLock(); }

        foreach (var s in toDelete)
            DeleteSegmentFiles(s.FilePath);

        if (toDelete.Count > 0)
            _logger.LogInformation("Retention pruned {Count} trace file(s) older than {Days} days",
                toDelete.Count, (int)ttl.TotalDays);

        return Task.FromResult(toDelete.Count);
    }

    /// <summary>Deletes a cold segment's <c>.trc</c> plus every companion sidecar. Best-effort.</summary>
    private static void DeleteSegmentFiles(string trcPath)
    {
        TryDelete(trcPath);
        TryDelete(Path.ChangeExtension(trcPath, ".stats"));
        TryDelete(Path.ChangeExtension(trcPath, ".svcgraph"));
        TryDelete(Path.ChangeExtension(trcPath, ".tracesum"));

        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); }
            catch { /* best effort */ }
        }
    }
}

/// <summary>Metadata about a cold-tier span segment file.</summary>
public sealed class SpanSegmentInfo
{
    public string   FilePath     { get; init; } = string.Empty;
    public long     MinStartNano { get; init; }
    public long     MaxStartNano { get; init; }
    public int      SpanCount    { get; init; }
    /// <summary>Service names present in this segment — enables O(1) cold-tier pre-filter.</summary>
    public string[] Services     { get; init; } = [];
    /// <summary>On-disk format version (2 = legacy string-keyed maps, 3 = current). Drives v2→v3 migration.</summary>
    public ushort   FormatVersion { get; init; } = 3;
}
