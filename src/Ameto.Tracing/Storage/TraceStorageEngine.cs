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
    //
    // INVARIANT: sorted by MaxStartNano DESCENDING. Every writer goes through
    // SortedByMaxStartDesc (or preserves order, as a filter does); readers take the field as it
    // is. Two of them — the trace-list walk and the span search — need that order to name the
    // segment they stopped BEFORE, and both used to clone the whole array and re-sort it on
    // every call: O(n log n) over every segment on the box, per page, of every stream, where the
    // SSE loop now runs pages back to back. The field is only ever REPLACED, never mutated, so
    // sorting it once at the swap is the same work done once instead of once per reader.
    private volatile SpanSegmentInfo[]                         _coldSegments = [];

    // The time ranges of segments that vanished from disk behind the engine's back. Removing such
    // a segment from the snapshot is what makes the fault undiscoverable by every later request —
    // this is where the fault goes instead, so a window overlapping one is still told. Bounded in
    // size and pruned by retention; see VanishedRegionLog for both bounds and for the compaction
    // race it deliberately does NOT record.
    private readonly VanishedRegionLog                         _vanished = new();

    /// <summary>
    /// Segment identity, and the record of what a trace-id index may answer for. Loaded once in
    /// the constructor and never replaced; an unreadable or absent manifest loads as an empty
    /// catalog, which is why nothing below has to handle its failure — see
    /// <see cref="TraceManifest.Load"/>.
    /// </summary>
    private readonly TraceManifest                             _manifest;

    /// <summary>
    /// The open trace-id index runs. Answers "which segment holds this trace"; whether that answer
    /// may be believed as a NEGATIVE is decided in <see cref="GetTraceAsync"/>, against the
    /// manifest's coverage set — see the store's docstring for why the two are kept apart.
    /// </summary>
    private readonly TraceIndexStore                            _index;

    /// <summary>Test hook: how many segments the catalog names, and how many the index vouches for.</summary>
    internal (int Segments, int Covered) CatalogCountsForTest => (_manifest.Segments.Count, _manifest.CoveredCount);

    /// <summary>Test hook: open index runs and the memory they hold.</summary>
    internal (int Runs, long RetainedBytes) IndexStatsForTest => _index.Stats;

    /// <summary>Test hook: entries across every run — what a merge drops is only visible here.</summary>
    internal int IndexEntryCountForTest
    {
        get { int n = 0; foreach (var r in _manifest.Runs) n += r.EntryCount; return n; }
    }

    /// <summary>
    /// The rollback, exercised: withdraw every claim of coverage and close every run. Costs speed
    /// and nothing else — no span is rewritten, no <c>.trc</c> is opened, and the next lookup is
    /// the scan this engine did before the index existed.
    /// </summary>
    internal void DisableTraceIndexForTest()
    {
        var paths = _manifest.Runs.Select(r => r.FilePath).ToList();
        _manifest.ClearCoverage();
        _index.Remove(paths);
    }

    /// <summary>
    /// Counts what the last trace lookup actually did, so a test can prove the index SAVED the
    /// work rather than merely returned the right answer. A correct-but-still-scanning index is
    /// the failure this whole branch exists to avoid, and it is invisible from the result.
    /// </summary>
    internal int SegmentsOpenedByLastTraceLookup;
    internal int SegmentsSkippedByLastTraceLookup;

    /// <summary>Test hook: cold segments currently registered.</summary>
    internal int ColdSegmentCountForTest => _coldSegments.Length;

    /// <summary>Test hook: how many vanished-segment ranges the engine currently remembers.</summary>
    /// <summary>
    /// A cold segment that could not be loaded at startup and is therefore not in the snapshot at
    /// all — so, unlike a vanished segment, there is no range to remember and no window this can be
    /// narrowed to. Every read has to say so.
    ///
    /// <para>The alternative was measured and is the failure this endpoint exists to prevent: a
    /// .trc held open by an antivirus or a backup agent during load left segments=0, rows=0 and
    /// Unreadable=FALSE — a positive claim that the window was read out, for the life of the
    /// process, over a file sitting on the disk. The same lock met on a REQUEST reports Capped, so
    /// one fault was loud at one door and silent at the other.</para>
    ///
    /// <para>WHICH READS SAY SO, precisely: the trace LIST and the span SEARCH, which are the two
    /// that have somewhere to put it. <c>GetTraceAsync</c>, <c>GetTraceVolumeAsync</c> and
    /// <c>GetAggregateStatsAsync</c> return plain data with no channel for a fault, so a trace
    /// detail view still renders a truncated trace as a whole one — the same gap those three have
    /// for a vanished region, and not one this flag introduced. Naming it here because an earlier
    /// version of this comment claimed every read reports it, which was not true.</para>
    ///
    /// <para>Process-wide and never cleared, because nothing rescans: LoadColdSegments runs once.
    /// A restart is the recovery, and that is what the log line says.</para>
    /// </summary>
    private volatile bool _coldTierIncomplete;

    internal bool ColdTierIncompleteForTest => _coldTierIncomplete;

    internal int VanishedRegionCountForTest => _vanished.CountForTest;

    /// <summary>Retention's own call, reachable from a test: the wall clock cannot be moved, so
    /// proving a record is forgettable means handing Forget a cutoff past it.</summary>
    internal int ForgetVanishedForTest(long cutoffNano) => _vanished.Forget(cutoffNano);

    /// <summary>
    /// Test hook: does what a reader does when it opens a segment its own snapshot names and finds
    /// no file there. Compaction and retention both publish their snapshot change BEFORE unlinking
    /// anything, so a reader holding a slightly older snapshot meets missing files as a matter of
    /// routine — and a test that waits for that race to happen by luck is a test that usually does
    /// not run. This reproduces it exactly, from the reader's side.
    /// </summary>
    /// <returns>True when the engine judged it a genuine loss rather than a handover.</returns>
    internal bool MeetMissingSegmentFileForTest(SpanSegmentInfo seg) => RemoveColdSegment(seg);

    /// <summary>The classifier's own verdict — a test needs the three-way answer, not the removal.</summary>
    internal ColdReadFault MeetMissingSegmentFileVerdictForTest(SpanSegmentInfo seg) => MeetMissingSegmentFile(seg);

    /// <summary>
    /// Test hook: called by both cold walks once per segment, IMMEDIATELY BEFORE that segment is
    /// opened and while the walk is already committed to its own snapshot.
    ///
    /// <para>That instant is the compaction handover race, and it is the only place a test can
    /// stand in it. Both walks snapshot <c>_coldSegments</c> once and then iterate, so a segment
    /// retired and unlinked between the snapshot and the open is met by a reader holding a list
    /// nobody maintains any more — routine on any install that compacts, and the shape that had
    /// 1 in 60 racing requests reporting a data loss on a healthy server. Waiting for it to happen
    /// by luck is a test that usually does not run.</para>
    /// </summary>
    internal Action<SpanSegmentInfo>? _beforeColdSegmentRead;

    /// <summary>
    /// Test hook: the registered cold segments, IN SNAPSHOT ORDER — which is the invariant two
    /// read paths depend on and neither re-establishes, so a test has to be able to see it. Also
    /// how a test reaches a segment's file, to delete or corrupt it behind the engine's back:
    /// not an exotic fault but the ordinary shape of compaction, whose sources are unlinked while
    /// older snapshots still name them.
    /// </summary>
    internal SpanSegmentInfo[] ColdSegmentsForTest => _coldSegments;

    /// <summary>
    /// Test hook: called on the flush thread with the tier already detached, just before the
    /// segment build. Blocking in it holds the engine in the exact window the off-lock flush
    /// opened — spans in neither tier — which is the only way to assert that readers still
    /// see them.
    /// </summary>
    internal Action? _beforeSegmentWrite;

    /// <summary>
    /// Test hook: called at the top of <see cref="GetTraceListAsync"/>, BLOCKING, with the
    /// caller's cancellation token — before any await, so it runs synchronously on whatever
    /// thread invoked the fetch.
    ///
    /// <para>That is deliberately the real shape of this path: <c>SpanReader</c> contains no
    /// await tokens at all, the <c>.tracesum</c> sidecar read is a blocking FileStream plus LZ4
    /// plus parse, and the async iterators hand back synchronously-completed ValueTasks. A slow
    /// page fetch therefore does not YIELD, it OCCUPIES — which is exactly what made the SSE
    /// keepalive inert, and what a test cannot reproduce with <c>Task.Delay</c>.</para>
    /// </summary>
    internal Action<CancellationToken>? _beforeTraceListScan;

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

        // Before anything else touches the directory: the catalog is what names the segments the
        // sweep and the WAL replay are about to work over. It cannot fail — every damaged form
        // loads as an empty catalog, and an empty catalog is exactly how this engine behaved
        // before there was one.
        _manifest = TraceManifest.Load(dataDir, logger);
        _index    = new TraceIndexStore(logger);

        // Open whatever runs the catalog names. A run that will not open is withdrawn from the
        // coverage set right here rather than left standing: the claim "the index answers for this
        // segment" must not outlive the discovery that it cannot.
        foreach (ulong unusable in _index.Sync(_manifest))
            _manifest.WithdrawCoverage(unusable);

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
        _walPath = Path.Combine(dataDir, "spans.wal");
        _wal = SpanWriteAheadLog.Open(_walPath);
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

        // Cold tier — scan the snapshot in parallel (bounded): a by-id lookup has
        // no time bounds, so every segment must be consulted, and doing that
        // sequentially took whole seconds once small segments piled up. A file
        // that compaction/retention deleted mid-flight is skipped (and healed out
        // of the snapshot) instead of failing the whole request.
        var segs = _coldSegments;
        List<SpanRecord>? cold = null;

        // ── THE INDEX, AND THE ONE RULE FOR BELIEVING IT ──────────────────────────
        //
        // A segment is skipped only when BOTH hold: the catalog says the index covers it, and no
        // open run named this trace in it. Either half alone is not enough, and the reason is the
        // whole design. A run that names nothing proves nothing about a segment nobody indexed —
        // that is the silent under-report this engine has spent every review round closing — while
        // coverage without a lookup is just a flag.
        //
        // Everything else is a hint. A hit hands over the span offsets, which lets the walk skip
        // ReadTraceOffsets — the read-and-inflate of the segment's whole trace index, 38% of the
        // file, and the entire reason this branch exists. The hint is still verified: the walk
        // checks each span's FULL trace id, so a hit on the truncated key that turns out to be a
        // collision yields nothing rather than another trace's spans.
        var hits = _index.HasRuns ? _index.Lookup(traceId) : null;
        int opened = 0, skipped = 0;

        if (segs.Length > 0)
        {
            using var gate = new SemaphoreSlim(Math.Clamp(Environment.ProcessorCount / 2, 2, 8));
            var tasks = new Task<List<SpanRecord>?>[segs.Length];
            for (int i = 0; i < segs.Length; i++)
            {
                var seg = segs[i];

                List<uint>? known = null;
                if (hits is not null && seg.SegmentId != 0 && _manifest.IsCovered(seg.SegmentId))
                {
                    foreach (var h in hits)
                        if (h.SegmentId == seg.SegmentId) (known ??= new List<uint>()).AddRange(h.Offsets);

                    if (known is null)
                    {
                        skipped++;
                        tasks[i] = Task.FromResult<List<SpanRecord>?>(null);
                        continue;
                    }
                }
                opened++;

                tasks[i] = Task.Run(async () =>
                {
                    await gate.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        List<SpanRecord>? found = null;
                        var walk = known is null
                            ? SpanReader.ReadTraceAsync(seg.FilePath, traceId, ct)
                            : SpanReader.ReadTraceAtAsync(seg.FilePath, traceId, known, ct);
                        await foreach (var r in walk.ConfigureAwait(false))
                            (found ??= new List<SpanRecord>()).Add(r);
                        return found;
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (FileNotFoundException)
                    {
                        // Classified through the same door as the other two cold walks, so a
                        // directory fault cannot heal a segment out of the snapshot here either.
                        // This walk reports no fault bit of its own — a trace lookup either finds
                        // the trace or does not — so the verdict is used only for its side effect.
                        MeetMissingSegmentFile(seg);
                        return null;
                    }
                    catch (DirectoryNotFoundException ex)
                    {
                        _logger.LogWarning(ex,
                            "Trace lookup: could not reach segment {File} — the data directory is not " +
                            "there; the segment stays in the snapshot", seg.FilePath);
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

            cold = new List<SpanRecord>();
            foreach (var t in tasks)
                if (await t.ConfigureAwait(false) is { } part)
                    cold.AddRange(part);
        }

        SegmentsOpenedByLastTraceLookup  = opened;
        SegmentsSkippedByLastTraceLookup = skipped;

        // ── ONE ORDERED SEQUENCE, ACROSS BOTH TIERS ───────────────────────────────
        //
        // ITraceProvider.GetTraceAsync promises spans "ordered by StartTimeUnixNano", and this
        // method used to sort each tier and then CONCATENATE them: every hot span, then every
        // cold one. The hot tier holds the NEWEST spans by construction, so any trace straddling
        // the two came back inverted — measured through GET /api/traces/{id} on a seven-span
        // trace with five flushed and two still hot: hot(+20 ms), hot(+21 ms), then cold(+1),
        // (+3), (+5), (+12), (+14). The waterfall renders what it is handed, so the root arrived
        // last and the trace drew upside down.
        //
        // Sorting the union is not a new cost: the cold fan-out already buffers every matching
        // span before the first of them is yielded, and the hot list is rooted for the whole
        // call, so the peak was always one trace either way. What it does change is WHEN the
        // first span is handed over — after the cold reads rather than before them. That is the
        // price of the contract; a caller that wanted the hot spans early would have to be
        // willing to sort, and both callers here (the detail view and the flamegraph) collect
        // everything before drawing anything.
        //
        // WHICH COPY SURVIVES, decided where it can actually be decided. This comment used to say
        // that "OrderBy is a STABLE sort and the hot spans are added first, so the HOT copy is the
        // one that survives the dedupe below" — and the dedupe runs AFTER the sort, so what
        // actually survived was the EARLIER-STARTING copy, whichever tier it came from. Measured
        // through the endpoint: a cold copy of span S at +1 ms and a hot copy of the same id at
        // +30 ms returned the COLD one. Stability orders EQUAL keys; it says nothing about two
        // records with different start times, which is exactly the pair a re-written span is.
        //
        // The hot copy is the one to keep. Both duplicate sources — the flush handover and a WAL
        // replay putting back spans a segment already holds — put the ENGINE'S MOST RECENT view of
        // the span in the hot tier, so the cold copy is the stale one by construction.
        //
        // So the dedupe runs FIRST, over the concatenation in tier order, and the sort runs on the
        // survivors. That also makes the sort cheaper by whatever the duplicates were.
        var ordered = hotResults;
        if (cold is { Count: > 0 })
        {
            if (ordered is null) ordered = cold;
            else                 ordered.AddRange(cold);
        }
        if (ordered is null) yield break;

        // In place, front-to-back, which is hot-then-cold: List.RemoveAll visits in index order,
        // so the first copy of each span id is the one `seen` admits. `ordered` is always a list
        // this method built (hotResults or cold), never a caller's.
        ordered.RemoveAll(r => !r.SpanId.IsEmpty && !seen.Add(r.SpanId.RawValue));

        // SORTED IN PLACE, because OrderBy cannot. Enumerating an OrderedEnumerable copies the
        // whole list into a Buffer<T>, builds a long[] of keys and an int[] index map, and hands
        // back an iterator — three throwaway arrays per request, and past about ten thousand spans
        // the key array alone clears the 85 KB LOH threshold. That is the exact allocation shape
        // the rest of this branch exists to remove; leaving it on the detail-view path while
        // rewriting the list path for it would be answering the measurement selectively.
        //
        // The list is a list this method built (hotResults or cold, never a caller's), so it can
        // be reordered. Stability is not lost either: the RemoveAll above already left one record
        // per span id, and two records with different start times were never equal keys.
        ordered.Sort(static (a, b) => a.StartTimeUnixNano.CompareTo(b.StartTimeUnixNano));
        foreach (var r in ordered)
            yield return r;
    }

    /// <summary>
    /// The one way a new <see cref="_coldSegments"/> array is built: sorted by MaxStartNano
    /// DESCENDING, which every reader then relies on and none of them re-establishes. Filtering
    /// an already-sorted array preserves the order, so removals (self-heal, retention,
    /// compaction's drop list) do not need this.
    /// </summary>
    private static SpanSegmentInfo[] SortedByMaxStartDesc(List<SpanSegmentInfo> segs)
    {
        var arr = segs.ToArray();
        Array.Sort(arr, static (a, b) => b.MaxStartNano.CompareTo(a.MaxStartNano));
        return arr;
    }

    /// <summary>
    /// Drops a segment whose file no longer exists from the snapshot — and remembers the range it
    /// covered IF, and only if, the file was gone for a reason the engine did not choose.
    ///
    /// <para>THE REMOVAL IS WHAT COSTS THE FAULT ITS SECOND SIGHTING. Once this segment is out of
    /// <c>_coldSegments</c>, no later read can open it, fail on it, or report the hole it leaves;
    /// the request that got here is the only one that will ever know. That is why the range goes
    /// into <see cref="_vanished"/>, and why the removal still happens — a segment left in the
    /// snapshot to keep re-announcing itself would fail every page of every stream for ever.</para>
    ///
    /// <para>TELLING A LOSS FROM A HANDOVER, which is the whole judgement in this method and the
    /// case that decides whether the record is useful or noise. Compaction writes its merged
    /// output, SWAPS THE SNAPSHOT, and only then unlinks its sources; retention removes and then
    /// deletes in the same order. So a source's absence from the CURRENT snapshot is the engine's
    /// own signed statement that it retired the file on purpose and that the data is either in the
    /// replacement or deliberately expired. A reader tripping over such a file has raced a healthy
    /// server, and on an install that compacts every hour that race is the common case: recording
    /// it would have this server reporting truncation over its own compaction window for ever —
    /// the same false statement as the silent <c>done</c>, told the other way round.</para>
    ///
    /// <para>A segment that is STILL LISTED when its file turns out to be missing is the other
    /// story entirely. Nothing in the engine retired it, so nothing wrote a replacement: the file
    /// was removed by something outside — an operator clearing space, a half-restored backup, a
    /// volume that dropped writes — and the spans it held are not anywhere. That is the case worth
    /// a permanent record, and it is the case this test admits.</para>
    ///
    /// <para>The presence test and the removal are ONE operation under the write lock, so two
    /// readers meeting the same dead file cannot both conclude they were first.</para>
    ///
    /// <para>THE VERDICT IS RETURNED, not just acted on, and that is what the callers were missing.
    /// This method decided the question correctly for the MEMORY and then told nobody, so each
    /// caller set its own per-request fault bit unconditionally: a request that raced a healthy
    /// compaction recorded nothing (<c>regions=0</c>, right) and still reported a data loss
    /// (<c>Unreadable=true</c>, wrong). Measured over 60 concurrent list/compaction races on an
    /// undamaged server, 1 request came back that way — about 1.7% of requests overlapping a
    /// compaction pass, each one a red "deleted or damaged" banner and a frozen list.</para>
    /// </summary>
    /// <returns>
    /// TRUE when this was a genuine LOSS — the segment was still listed, so nothing in the engine
    /// retired it and the range has been recorded. FALSE when it was a HANDOVER: compaction or
    /// retention had already replaced or expired the file, the data is in the replacement or
    /// deliberately gone, and the only thing the caller may conclude is that IT did not read those
    /// rows — which is a floor, not a fault.
    /// </returns>
    private bool RemoveColdSegment(SpanSegmentInfo seg)
    {
        bool wasListed;
        _lock.EnterWriteLock();
        try
        {
            var next  = Array.FindAll(_coldSegments, s => !ReferenceEquals(s, seg));
            wasListed = next.Length != _coldSegments.Length;
            if (wasListed) _coldSegments = next;
        }
        finally { _lock.ExitWriteLock(); }

        if (!wasListed)
        {
            // Already retired by compaction or retention (or by another reader that met the same
            // fault first and recorded it). Debug, not Warning: on a compacting server this is
            // ordinary, and a warning per race trains operators to ignore the level that carries
            // the real one.
            _logger.LogDebug(
                "Cold span segment {File} was already retired before a reader met its missing file — " +
                "compaction or retention handover, not a loss", seg.FilePath);
            return false;
        }

        _vanished.Record(seg.MinStartNano, seg.MaxStartNano);
        _vanished.RecordPath(seg.FilePath);
        _logger.LogWarning(
            "Cold span segment {File} vanished from disk — removed from the segment list; reads " +
            "over [{MinNano}, {MaxNano}] will report truncation until retention passes that range",
            seg.FilePath, seg.MinStartNano, seg.MaxStartNano);
        return true;
    }

    /// <summary>
    /// WHAT A COLD WALK IS ALLOWED TO CONCLUDE ABOUT A SEGMENT IT COULD NOT READ. Three of the
    /// engine's walks meet these faults and each used to classify them slightly differently; the
    /// verdicts are named here so they cannot drift apart again.
    /// </summary>
    internal enum ColdReadFault
    {
        /// <summary>
        /// Compaction or retention had already retired the file. NOTHING IS LOST — the rows are in
        /// the replacement, or were expired on purpose — but this walk was holding the older
        /// snapshot and did not read them, so it owes the caller a floor and nothing else.
        /// </summary>
        Handover,

        /// <summary>
        /// The file is gone and the engine did not retire it: an operator clearing space, a
        /// half-restored backup, a volume that dropped writes. The spans are not anywhere, the
        /// range is now in <see cref="_vanished"/>, and this is the one verdict that is permanent.
        /// </summary>
        Lost,

        /// <summary>
        /// The read could not REACH the file this time — the data directory itself was not there.
        /// Says nothing whatever about whether the data exists, so it records no range and drops
        /// no segment: the next request tries again and, when the mount is back, succeeds.
        /// </summary>
        Transient,

        /// <summary>
        /// The file is present and will not parse. It stays in the snapshot deliberately (removing
        /// a file that still exists is compaction's and retention's decision, never a read's), so
        /// it fails again on every page — which is what keeps the window from ever being reported
        /// as read out.
        /// </summary>
        Corrupt,
    }

    /// <summary>
    /// Classifies a cold segment whose FILE turned out to be missing, and performs the snapshot
    /// heal when that is the right answer.
    ///
    /// <para>A DIRECTORY THAT IS MISSING IS NOT THE SAME EVIDENCE AS A FILE THAT IS MISSING WHILE
    /// ITS DIRECTORY IS INTACT, and collapsing the two is what made a mount blip permanent. The
    /// engine unlinks its own files, so a gone file inside a live directory has exactly two
    /// stories and <see cref="RemoveColdSegment"/> tells them apart. A gone DIRECTORY has neither
    /// story behind it: no engine path removes the data directory, so its absence is the mount, not
    /// the data. Measured on an engine rooted at a junction — 100 rows over 2 segments healthy,
    /// then the junction deleted for ONE request and immediately re-created with every .trc still
    /// present: rows=0, Unreadable=true, segs=0, regions=2, and it stayed that way, because
    /// LoadColdSegments runs once at startup and nothing rescans. A bind-mount, SMB or iSCSI blip
    /// cost the whole cold tier for the life of the process and claimed a data loss that had not
    /// happened.</para>
    /// </summary>
    internal ColdReadFault MeetMissingSegmentFile(SpanSegmentInfo seg)
    {
        // Probed rather than assumed, because this runs on the path that has just failed: if the
        // directory is not there, the file's absence is not evidence about the file at all.
        try
        {
            if (!Directory.Exists(_dataDir))
            {
                _logger.LogWarning(
                    "Cold span segment {File} could not be reached — the data directory {Dir} is not " +
                    "there. Treated as a transient fault: the segment stays in the snapshot and the " +
                    "next request retries it", seg.FilePath, _dataDir);
                return ColdReadFault.Transient;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not probe the data directory {Dir} — treating {File} as transient",
                _dataDir, seg.FilePath);
            return ColdReadFault.Transient;
        }

        // THE DIRECTORY IS THERE, BUT IS ANYTHING? A volume that has re-attached and not populated
        // yet answers Exists with a yes over an empty stub — a container restarted before its
        // volume binds, an NFS or SMB mount landing on the mountpoint underneath, a fresh disk in
        // the slot. Every listed segment is then "missing" at once, and classifying them Lost
        // drops the whole cold tier and writes a permanent claim that survives the data coming
        // back. Losing every segment in one instant is not how deletion behaves; it is how a mount
        // behaves. Erring to Transient here cannot hide anything either: the segments STAY in the
        // snapshot, so a directory that really is empty for ever keeps faulting and keeps
        // reporting, which is "ask me again", never "it is gone".
        // TWO SEGMENTS GONE AT ONCE AND NO SEGMENT FILE LEFT ANYWHERE — restored, after removing it
        // proved far worse than keeping it.
        //
        // I took it out on the reasoning that over-reporting a loss is the recoverable direction.
        // That reasoning does not hold HERE, and the measurement is what settles it: nothing
        // rescans the data directory (LoadColdSegments runs once, at startup), and retention ages
        // out the recorded REGION, not the removal from the snapshot. So a mount that flickers for
        // a tenth of a second — long enough for one request to trip over it — costs every segment
        // that request touched, permanently, and the only recovery is a process restart. Measured
        // against the previous commit: with the guard, rows=20 and a whole cold tier once the files
        // came back; without it, rows=0 Unreadable=True segs=0 across every later request and two
        // PruneAsync passes. On a forty-segment install one blip also blows straight through the
        // thirty-two-region cap, and coalescing then calls about four fifths of healthy time
        // unreadable — a banner that is always on is a banner nobody reads.
        //
        // So this errs rarely and the alternative errs constantly. What it costs is named rather
        // than hidden: a genuine wholesale delete is reported as truncation (Capped, "ask me
        // again") instead of loss, until a restart finds the files really gone. That is a worse
        // SENTENCE for a rare event; the removal was a worse OUTCOME for a common one.
        //
        // WHAT SEPARATES THE TWO CASES IS THE WAL, NOT A SEGMENT COUNT. The first version asked
        // for two or more listed segments, on the reasoning that losing everything at once is not
        // how deletion behaves — but with ONE cold segment "everything at once" and "that file was
        // deleted" are the same observation, so the guard simply did not apply. Measured on a
        // single-segment install, which is what a quiet stand plus the hourly CompactSmallSegments
        // converges to: a 0.1s mount blip gave rows=0 Unreadable=True permanently, and after one
        // retention pass rows=0 Unreadable=FALSE regions=0 over twenty rows still on the disk —
        // the exact silent under-report this branch is named for, reachable by a flickering mount.
        //
        // spans.wal answers the question the count was standing in for. It is opened at
        // construction and removed only by an explicit reset, so this process holding a handle to
        // it means the file is in the directory; if the directory no longer has it, the directory
        // is not the one we opened. Nothing ordinary — retention, compaction, an operator deleting
        // a segment — can take it away. And erring here is still the recoverable direction: the
        // segments STAY in the snapshot, so a directory that really is empty keeps faulting and
        // keeps reporting, which is "ask me again", never "it is gone".
        if (NoSegmentFilesLeft(_dataDir) && !WalFileStillThere())
        {
            _logger.LogWarning(
                "Neither a segment file nor the engine's own spans.wal is left under {Dir} while " +
                "{Count} cold segment(s) are still listed — treating {File} as an unpopulated " +
                "volume rather than a deletion; the snapshot is kept and the next request retries",
                _dataDir, _coldSegments.Length, seg.FilePath);
            return ColdReadFault.Transient;
        }

        if (RemoveColdSegment(seg)) return ColdReadFault.Lost;

        // NOT LISTED — which usually means the engine retired it itself, and a compaction handover
        // is a healthy server's own work rather than a loss. But two readers can meet the same
        // genuinely deleted file, and RemoveColdSegment is atomic, so exactly one of them wins the
        // removal and the loser lands here. The memory settles which case this is.
        //
        // BY PATH, NOT BY TIME RANGE. The first version asked whether the segment's span OVERLAPPED
        // a recorded loss — and cold segments overlap in time by design, as comments throughout this
        // file say, so it could not tell the file somebody just lost from a different file in the
        // same hour. Measured: after one real loss, the very next ordinary handover in that band
        // came back Lost instead of Handover, and a completely healthy server showed a red
        // deleted-or-damaged banner for the whole retention TTL.
        if (_vanished.WasLost(seg.FilePath)) return ColdReadFault.Lost;

        return ColdReadFault.Handover;
    }

    /// <summary>
    /// The engine's own write-ahead log, inside the data directory. It is opened OpenOrCreate at
    /// construction and removed only by an explicit reset, so while this process is alive the file
    /// IS THERE — which makes its absence evidence about the directory rather than about any
    /// segment. See <see cref="MeetMissingSegmentFile"/>.
    /// </summary>
    private readonly string _walPath;

    /// <summary>
    /// One more chance for a segment whose file was busy rather than broken. Returns null when it
    /// is still unreadable; the caller then has to admit the cold tier is short.
    /// </summary>
    /// <summary>
    /// Time the whole load may spend waiting on busy files, TOTAL. Per-file retries looked cheap
    /// and are not: at six hundred milliseconds each, forty segments inside a backup window parks
    /// startup for twenty-four seconds with the cold tier unavailable throughout.
    /// </summary>
    private static readonly TimeSpan LoadRetryBudget = TimeSpan.FromSeconds(2);
    private long _loadRetryUsedTicks;

    private SpanSegmentInfo? RetryReadSegmentInfo(string file)
    {
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            if (_loadRetryUsedTicks >= LoadRetryBudget.Ticks) return null;
            var waited = TimeSpan.FromMilliseconds(100 * attempt);
            _loadRetryUsedTicks += waited.Ticks;
            Thread.Sleep(waited);
            try
            {
                var info = SpanReader.ReadSegmentInfo(file);
                _logger.LogWarning(
                    "Cold segment {File} was busy at startup and read on attempt {Attempt}", file, attempt + 1);
                return info;
            }
            catch { /* still busy, or now broken — the caller decides */ }
        }
        return null;
    }

    /// <summary>
    /// Whether the directory holds no segment file at all. Probe failures answer FALSE — an I/O
    /// error is not evidence of absence, and answering TRUE would turn an unreadable mount into
    /// "keep retrying for ever" over data that may really be gone.
    /// </summary>
    /// <summary>
    /// Whether the log this engine opened is still in the data directory. A probe failure answers
    /// TRUE — the same direction as <see cref="NoSegmentFilesLeft"/>, because an I/O error is not
    /// evidence that the volume was swapped, and answering otherwise would turn a busy disk into a
    /// permanent loss claim.
    /// </summary>
    private bool WalFileStillThere()
    {
        try { return File.Exists(_walPath); }
        catch { return true; }
    }

    private static bool NoSegmentFilesLeft(string dir)
    {
        try
        {
            // Disposed: FileSystemEnumerator holds a native find handle that only the finaliser
            // would release, on a path that runs once per faulted segment per page — exactly the
            // sick-storage moment when handles are scarce.
            using var it = Directory.EnumerateFiles(dir, "*.trc").GetEnumerator();
            return !it.MoveNext();
        }
        catch { return false; }
    }

    /// <summary>
    /// WHAT THE EXCEPTION ACTUALLY SAYS, rather than "anything I did not name is corruption".
    ///
    /// <para>Both cold walks used to end their catch chain by calling everything left
    /// <see cref="ColdReadFault.Corrupt"/>, which <see cref="IsPermanentFault"/> treats as
    /// permanent — so a red "deleted or damaged" claim, for the life of the record, was the answer
    /// to: a Windows sharing violation while an antivirus or a File.Move holds the .trc open; an
    /// <c>IOException "The specified network name is no longer available"</c> from an SMB or iSCSI
    /// blip, which is how a mount blip presents AT THE FILE LEVEL and therefore never reaches the
    /// directory probe; handle exhaustion under GetTraceAsync's eight-way fan-out; and an
    /// <c>UnauthorizedAccessException</c> while a container volume remounts. Before the fault bit
    /// existed all of these were reported as truncation, which is what they are.</para>
    ///
    /// <para>Corruption is a claim about CONTENT, so only the exceptions that describe content
    /// earn it. Note the in-repo trigger for getting this wrong: SpanReader's own MaxBlockBytes
    /// throws <see cref="InvalidDataException"/> on a segment holding a block over 64 MB — a file
    /// this engine could have written — so that one really is about the file, and really is
    /// permanent until someone rewrites it.</para>
    /// </summary>
    private ColdReadFault ClassifyReadFailure(Exception ex, string what, string filePath)
    {
        bool content = FileBounds.DescribesContent(ex);

        if (content)
        {
            _logger.LogWarning(ex, "{What}: segment {File} will not parse — treated as damaged", what, filePath);
            return ColdReadFault.Corrupt;
        }

        _logger.LogWarning(ex,
            "{What}: segment {File} could not be read right now — treated as transient, the segment " +
            "stays in the snapshot and the next request retries it", what, filePath);
        return ColdReadFault.Transient;
    }

    /// <summary>
    /// True when this verdict means rows are MISSING AND NOTHING WILL BRING THEM BACK — the only
    /// two that may raise a page's <c>Unreadable</c> bit and the red banner behind it. A handover
    /// is a healthy server's own compaction; a transient fault is answered by the next request.
    /// Both of those are floors: "ask me again", not "it is gone".
    /// </summary>
    private static bool IsPermanentFault(ColdReadFault fault) =>
        fault is ColdReadFault.Lost or ColdReadFault.Corrupt;

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
        SpanScanFloor?    scanFloor        = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        long fromNano = from.HasValue ? from.Value.ToUnixTimeMilliseconds() * 1_000_000L : long.MinValue;
        long toNano   = to.HasValue   ? to.Value.ToUnixTimeMilliseconds()   * 1_000_000L : long.MaxValue;

        // A SEGMENT THIS WINDOW LOST ON SOME EARLIER REQUEST. The catch below removes a vanished
        // file from the snapshot, so this scan will not find it, will not fail on it, and would
        // report a window it read out in full — for every request after the one that discovered
        // the fault. Asserted HERE, at the top, because this method leaves through half a dozen
        // `yield break`s and the statement is true on all of them. Bit only, no floor: see
        // SpanScanFloor.MetUnreadableRegion.
        // A segment this window lost on an earlier request, OR a segment startup could not load at
        // all. The second has no range to test against — it never reached the snapshot — so it
        // answers for every window there is, which is the honest reading of "part of the cold tier
        // is not here".
        if (_coldTierIncomplete || _vanished.Overlaps(fromNano, toNano)) scanFloor?.MetUnreadableRegion();

        int yielded = 0;

        // Same duplicate sources as GetTraceAsync, but results here cross traces, so the
        // identity is the pair. Every Add below sits on a path that yields, so this stays
        // bounded by `limit` — recording spans that were merely READ would put the match
        // count back into memory through this set after the segment buffer stopped doing it.
        var seen = new HashSet<(TraceId Trace, ulong Span)>();

        // Offers one span to a bounded top-K heap. `present` is the identity of what is IN that
        // heap right now.
        //
        // A DUPLICATE MUST NOT COST A SLOT. The dedupe check used to happen on the way in
        // (against `seen`) while the recording happened on the way out, so two copies of one
        // (TraceId, SpanId) arriving inside a single tier or segment both entered the heap —
        // neither was in `seen` yet — and together evicted a distinct older span to make room.
        // The second copy was then discarded at the drain, and the tier yielded fewer than
        // `limit` DISTINCT spans although more existed. Both duplicate sources are ordinary:
        // UnflushedSpansLocked concatenates the hot tier with the in-flight flush snapshot, and
        // a segment can hold spans a WAL replay put back.
        //
        // `present` is bounded by the heap it mirrors (at most `limit`), never by what was read
        // — that is the unbounded growth 3fc5472 removed and it must not come back through here.
        //
        // RETURNS TRUE WHEN IT DROPPED A MATCH, which is the one thing a caller paging behind
        // this scan has to be told. A tier that evicted has decided nothing about anything below
        // the oldest span still in its heap, and a pager that is not told treats "the newest
        // `limit`" as "all of them".
        bool Admit(PriorityQueue<SpanRecord, long> top, HashSet<(TraceId Trace, ulong Span)> present, SpanRecord r)
        {
            var  id         = (r.TraceId, r.SpanId.RawValue);
            bool identified = !r.SpanId.IsEmpty;

            if (identified)
            {
                // `seen` is empty for the whole of the hot-tier pass (nothing has been yielded
                // yet) and that pass is the biggest walk in the method, so the probe is skipped
                // rather than performed against an empty set — the same answer, one hash and one
                // bucket lookup cheaper, inside the read lock WriteSpan contends with.
                if (seen.Count > 0 && seen.Contains(id)) return false;   // yielded by an earlier tier or segment
                if (!present.Add(id))  return false;   // a second copy of something already in the heap
            }

            if (top.Count < limit) { top.Enqueue(r, r.StartTimeUnixNano); return false; }

            if (!top.TryPeek(out _, out long oldestKept) || r.StartTimeUnixNano <= oldestKept)
            {
                // Too old to make the cut — take the identity back out, or `present` would
                // outgrow the heap and start rejecting spans that are not in it.
                if (identified) present.Remove(id);
                return true;
            }

            var evicted = top.EnqueueDequeue(r, r.StartTimeUnixNano);
            if (!evicted.SpanId.IsEmpty) present.Remove((evicted.TraceId, evicted.SpanId.RawValue));
            return true;
        }

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
        //
        // Bounded top-K, exactly as the cold segment scan below: a min-heap on start time that
        // evicts its oldest once full.
        //
        // WHAT THIS BOUGHT IS MEMORY, AND ONLY MEMORY. An earlier version of this comment
        // justified the rewrite by LOCK HOLD TIME, and that was wrong in the direction that
        // matters. `Where().OrderByDescending().Take(limit)` has gone through IPartition since
        // .NET Core 3.0: buffering is an array append per match and the finish is a partial
        // quickselect, not an O(M log M) sort. What replaced it costs a `seen` probe plus a
        // `present` insert per match — a HashCode.Combine over two ulongs and a bucket probe
        // each — plus a TryPeek, and for anything admitted an O(log limit) EnqueueDequeue and a
        // `present` removal. Over the ~100k spans UnflushedSpansLocked can walk (50k hot plus a
        // 50k in-flight flush snapshot) that is several milliseconds of READ lock against about
        // one before, and WriteSpan takes the WRITE side of it for every ingested span while the
        // SSE loop runs these scans back to back. The lock hold got WORSE.
        //
        // It is still the right trade, for the reason the segment loop below spells out: the
        // ordering buffer was O(M) SpanRecords — a kilobyte each once a query touches attributes
        // — and several hundred thousand matches is what killed a 512 MB server. O(limit) is the
        // fix; the lock hold is what it cost.
        //
        // Moving the heap outside the lock over a taken snapshot was considered and rejected:
        // the snapshot is an O(M) copy of exactly the references the heap exists to stop
        // materialising, allocated per scan, back to back, straight onto the LOH at 100k
        // entries. The cheap part is taken instead — see `Admit`, which skips the `seen` probe
        // while `seen` is empty, and it always is for this tier.
        var hotTop     = new PriorityQueue<SpanRecord, long>();
        var hotPresent = new HashSet<(TraceId Trace, ulong Span)>();
        bool hotEvicted = false;
        _lock.EnterReadLock();
        try
        {
            foreach (var s in UnflushedSpansLocked())
            {
                if (!Match(s)) continue;
                hotEvicted |= Admit(hotTop, hotPresent, s);
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        // The cold list, walked BY INDEX because the floor below has to name the segment the walk
        // stopped BEFORE — which `OrderByDescending` in a foreach cannot say. Sorted by
        // MaxStartNano descending, so nothing in segments [i..] starts above
        // ordered[i].MaxStartNano: that is the whole basis of the floor.
        //
        // Taken AS IT IS. The order is an invariant of the field (see _coldSegments), maintained
        // where the array is built, so this no longer clones every segment on the box and re-sorts
        // it on every page of every stream.
        var ordered = _coldSegments;

        bool Relevant(SpanSegmentInfo s) =>
            s.MaxStartNano >= fromNano && s.MinStartNano <= toNano &&
            (serviceName is null || s.Services.Length == 0 ||
             Array.Exists(s.Services, x => x.Equals(serviceName, StringComparison.OrdinalIgnoreCase)));

        // The highest start time a segment the walk never opened could still hold inside the
        // window. Segments out of range, or without the requested service, are DECIDED rather
        // than skipped — they provably hold nothing this call would have returned.
        long UnvisitedCeiling(int fromIndex)
        {
            for (int j = fromIndex; j < ordered.Length; j++)
                if (Relevant(ordered[j])) return Math.Min(ordered[j].MaxStartNano, toNano);
            return long.MinValue;
        }

        // The heap drains oldest-first; the caller wants newest-first.
        var candidates = new List<SpanRecord>(hotTop.Count);
        while (hotTop.TryDequeue(out var kept, out _)) candidates.Add(kept);
        candidates.Reverse();

        // The tier held more matches than a page can carry, so everything below the oldest one
        // it kept is undecided — including spans the cold walk will never be reached to read.
        if (hotEvicted && candidates.Count > 0)
            scanFloor?.StoppedAbove(candidates[^1].StartTimeUnixNano);

        foreach (var r in candidates)
        {
            if (!r.SpanId.IsEmpty && !seen.Add((r.TraceId, r.SpanId.RawValue))) continue;
            if (yielded >= limit)
            {
                // Stopped ON this span: everything strictly above it was handed over, it was not.
                scanFloor?.StoppedAbove(r.StartTimeUnixNano);
                scanFloor?.StoppedAbove(UnvisitedCeiling(0));
                yield break;
            }
            yielded++;
            yield return r;
        }

        if (yielded >= limit)
        {
            // The page filled exactly at the end of the tier — every cold segment is unread.
            scanFloor?.StoppedAbove(UnvisitedCeiling(0));
            yield break;
        }

        // Cold tier — segment-level service pre-filter, then block-level skip inside SpanReader
        for (int i = 0; i < ordered.Length; i++)
        {
            var seg = ordered[i];
            if (!Relevant(seg)) continue;

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
            _beforeColdSegmentRead?.Invoke(seg);

            var top      = new PriorityQueue<SpanRecord, long>();
            var present  = new HashSet<(TraceId Trace, ulong Span)>();
            bool evicted = false;
            ColdReadFault? segFault = null;
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
                catch (FileNotFoundException)
                {
                    segFault = MeetMissingSegmentFile(seg);
                    break;
                }
                catch (DirectoryNotFoundException)
                {
                    // THE ASYMMETRY, CLOSED. This walk caught only FileNotFoundException, so a
                    // directory-level fault fell through to the generic catch below and reported
                    // itself as an unreadable segment — the same event the trace list was calling
                    // a permanent loss, described by the sibling walk as a corrupt file. Neither
                    // was right, and the two streams disagreed about one blip.
                    _logger.LogWarning(
                        "Span search: could not reach segment {File} — the data directory is not there. " +
                        "The segment stays in the snapshot for the next request", seg.FilePath);
                    segFault = ColdReadFault.Transient;
                    break;
                }
                catch (Exception ex)
                {
                    segFault = ClassifyReadFailure(ex, "Span search", seg.FilePath);
                    break;
                }
                // `seen` is still ADDED to at the yield below, never here: a span that loses its
                // place in the heap is never returned, and recording it would grow `seen` with
                // the match count rather than with the result — the same unbounded growth in the
                // other structure. Duplicates within THIS segment are held off by `present`,
                // which is bounded by the heap; see Admit.
                evicted |= Admit(top, present, r);
            }

            // The heap drains oldest-first; the caller wants newest-first.
            var segMatches = new List<SpanRecord>(top.Count);
            while (top.TryDequeue(out var kept, out _)) segMatches.Add(kept);
            segMatches.Reverse();

            // A file that vanished or would not parse was abandoned part-read, so nothing in it
            // was decided. Reported as truncation rather than swallowed: results missing because
            // a segment is corrupt are still results missing — and reported as a FAULT rather
            // than as a floor, because a vanished segment is removed from the snapshot by the
            // catch above and no later page can rediscover it. See TraceListPage.Unreadable.
            //
            // The FAULT half is now conditional, for the reason spelled out on the trace list's
            // copy of this decision: a handover and a mount blip stopped this scan without losing
            // anything, so they name a height and nothing more. StoppedAboveUnreadable sets both
            // at once, which is exactly why it may not be the unconditional call.
            if (segFault is { } fault)
            {
                long ceiling = Math.Min(seg.MaxStartNano, toNano);
                if (IsPermanentFault(fault)) scanFloor?.StoppedAboveUnreadable(ceiling);
                else                         scanFloor?.StoppedAbove(ceiling);
            }
            else if (evicted && segMatches.Count > 0)
                scanFloor?.StoppedAbove(segMatches[^1].StartTimeUnixNano);

            foreach (var r in segMatches)
            {
                if (!r.SpanId.IsEmpty && !seen.Add((r.TraceId, r.SpanId.RawValue))) continue;
                if (yielded >= limit)
                {
                    scanFloor?.StoppedAbove(r.StartTimeUnixNano);
                    scanFloor?.StoppedAbove(UnvisitedCeiling(i + 1));
                    yield break;
                }
                yielded++;
                yield return r;
            }

            if (yielded >= limit)
            {
                scanFloor?.StoppedAbove(UnvisitedCeiling(i + 1));
                yield break;
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

        // The writer hands over the trace-to-offsets map it built anyway. Taken from there rather
        // than read back out of the finished file, because an index derived from a second,
        // independent pass is an index that can disagree with the segment it describes.
        Dictionary<TraceId, List<uint>>? traceIndex = null;

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
                                    onNamed:      path => _publishingSegmentPath = path,
                                    onTraceIndex: map  => traceIndex = map);
        }
        catch (Exception ex) { failure = ex; }

        _publishingSegmentPath = info?.FilePath ?? _publishingSegmentPath;

        // NAMED AFTER THE RENAME, BEFORE THE SNAPSHOT SWAP. The file is durable by this point, so
        // an id recorded here always describes something that exists; and it is recorded before
        // any reader can see the segment, so no reader ever meets a segment the catalog has not
        // heard of. A crash between the two leaves a file the catalog does not name, which is the
        // ordinary adopt-on-load case that LoadColdSegments already has to handle for any segment
        // written before this catalog existed.
        //
        // Nothing here can fail the flush: the catalog is a convenience over the directory, and a
        // segment without an id is a segment read exactly as it was read before ids existed.
        if (info is { } named)
        {
            try
            {
                ulong segId = _manifest.AllocateSegmentId();

                // The run goes to disk BEFORE the coverage claim, and the claim is what AddSegment
                // makes when it is handed one. A crash between them leaves an orphan .tix and an
                // uncovered segment — a wasted file and today's speed. The other order would leave
                // a segment the index is trusted for and has no run behind.
                TraceIndexRun? run = WriteIndexRun(named, segId, traceIndex);

                _manifest.AddSegment(
                    new TraceSegmentEntry(segId, named.FilePath, named.MinStartNano,
                                          named.MaxStartNano, named.SpanCount),
                    run);
                if (run is { } r) _index.Add(r);
                info = named.WithSegmentId(segId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Could not record {File} in the trace catalog — the segment is published and "
                  + "queryable, and will be adopted on the next start", named.FilePath);
            }
        }

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
                        _coldSegments = SortedByMaxStartDesc([.. _coldSegments, written]);
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
                var info = SpanReader.ReadSegmentInfo(file);
                loaded.Add(info);

                // A header time range that cannot be true. The segment is kept and queried on the
                // range as written — correcting it here is what hid readable spans, and refusing
                // the file would DELETE it (see the catch below) — but a volume that dropped
                // writes is worth an operator's attention, and this is the only place that knows.
                if (info.HeaderRangeSuspect)
                    _logger.LogWarning(
                        "Cold span segment {File} declares an impossible header time range "
                      + "[{MinNano}, {MaxNano}] — it stays queryable on that range as written; "
                      + "suspect the volume it was written to",
                        file, info.MinStartNano, info.MaxStartNano);
            }
            catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
            {
                // GONE IS NOT BUSY. Between EnumerateFiles above and ReadSegmentInfo here, a
                // compaction can publish its merged output and unlink the sources — the ordinary
                // race this engine is built around, and the one RemoveColdSegment, MeetMissingSegment
                // File and ColdReadFault.Handover exist to classify. Treating it as a busy file spent
                // six hundred milliseconds retrying a path that does not exist and then raised the
                // process-wide incomplete flag, so every later query answered Unreadable for the
                // life of the process over a handover that lost nothing.
                _logger.LogDebug("Cold segment {File} vanished while loading — retired by the engine", file);
                continue;
            }
            catch (Exception ex) when (ClassifyReadFailure(ex, "Cold segment load", file) is not ColdReadFault.Corrupt)
            {
                // Retried first, because most of what lands here clears by itself: an antivirus
                // scanning a freshly written file, a backup agent's handle, the compactor's own
                // File.Move. A few hundred milliseconds is the difference between a segment that
                // is missing for this process and one that was never really unavailable.
                var recovered = RetryReadSegmentInfo(file);
                if (recovered is not null) { loaded.Add(recovered); continue; }

                // Still not readable. It is NOT deleted — that was the old answer and it took the
                // sidecars with it — but it is also not in the snapshot, so no read can honestly
                // call a window complete until a restart picks it up.
                _coldTierIncomplete = true;
                // NOT EVERY FAILURE TO OPEN IS A REASON TO DESTROY. This catch answered anything at
                // all with DeleteSegmentFiles, so a segment held open by an antivirus, a backup
                // agent or the compactor's own File.Move was deleted at startup along with its
                // .stats, .svcgraph and .tracesum — measured, and on Linux, where File.Delete
                // succeeds against an open handle, the .trc goes too. The query path was taught in
                // this same change that a sharing violation is not damage; the startup path was
                // still answering it with destruction.
                //
                // Skipped rather than loaded: the file is not readable NOW, and nothing rescans, so
                // the next restart is what picks it up. That is a cold-tier gap until then, which
                // is strictly better than a deletion that cannot be undone.
                _logger.LogError(ex,
                    "Cold segment {File} could not be read at startup and is left on disk, not deleted — "
                  + "but it is missing from this run's cold tier, so every trace query will report an "
                  + "unreadable region on the list and span-search paths until the service is restarted", file);
                continue;
            }
            catch (Exception ex)
            {
                // CONTENT-SHAPED DAMAGE — the catch above has already ruled out everything that is
                // about the MACHINE rather than the file. v1 segments (12-byte footer) land here on
                // the footer magic, and deleting them is the migration path they have always had.
                //
                // WHAT WAS MISSING IS THE RECORD. Deleting is a decision about disk; it is also, and
                // silently, a decision about every answer this process will ever give. The window
                // the file covered is now on no disk at all, so every later page over that band
                // reads out every file that still exists and makes the strong positive claim that
                // it read the window — `done {"complete":true}`. That claim is exactly what
                // VanishedRegionLog was added in this same branch to make impossible, and the query
                // path was taught it while the startup path went on destroying data behind its back.
                //
                // The range comes from the 27-byte header, which is intact in every version this
                // engine has written and is readable even when the rest of the file is not — see
                // SpanReader.TryReadHeaderRange. Recorded BEFORE the delete, because after it there
                // is nothing left to ask.
                if (SpanReader.TryReadHeaderRange(file, out long minNano, out long maxNano))
                {
                    _vanished.Record(minNano, maxNano);
                    _vanished.RecordPath(file);
                    _logger.LogWarning(ex,
                        "Unreadable segment {File} — deleting (likely format v1). The window "
                      + "[{MinNano}, {MaxNano}] it covered is recorded as unreadable, so queries over "
                      + "that range will report truncation rather than claim to be complete",
                        file, minNano, maxNano);
                    DeleteSegmentFiles(file);
                }
                else
                {
                    // NOT BEING ABLE TO RECORD A LOSS IS NOT A LICENCE TO CAUSE ONE. Without a range
                    // the deletion would be unreportable: no region to overlap, no path to classify,
                    // and every later window silently whole. The file stays, and the process-wide
                    // flag says the cold tier is short — which is loud, recoverable by a restart
                    // once someone moves the file, and true.
                    _coldTierIncomplete = true;
                    _logger.LogError(ex,
                        "Segment {File} is unreadable AND its header range cannot be read, so deleting "
                      + "it would lose a window nothing could report. It is left on disk and excluded "
                      + "from this run's cold tier; every trace query will report an unreadable region "
                      + "until the file is moved aside and the service restarted", file);
                }
            }
        }

        loaded = ReconcileCatalog(loaded);

        _lock.EnterWriteLock();
        try
        {
            // Segments flushed while we were scanning are already in the snapshot;
            // keep them and add the discovered ones (dedup by path).
            var known = new HashSet<string>(_coldSegments.Select(s => s.FilePath), StringComparer.Ordinal);
            var next  = new List<SpanSegmentInfo>(loaded.Count + _coldSegments.Length);
            next.AddRange(loaded.Where(s => !known.Contains(s.FilePath)));
            next.AddRange(_coldSegments);
            _coldSegments = SortedByMaxStartDesc(next);
        }
        finally { _lock.ExitWriteLock(); }

        _logger.LogInformation("Loaded {Count} cold span segments in {Ms} ms",
            _coldSegments.Length, sw.ElapsedMilliseconds);
    }

    /// <summary>The <c>.tix</c> that sits beside a segment: same base name, different extension.</summary>
    private static string IndexPathFor(string trcPath) => Path.ChangeExtension(trcPath, ".tix");

    /// <summary>
    /// Segments the backfill has tried and failed on. Without it a segment whose index cannot be
    /// read is picked again on every pass, for ever, at whatever rate the worker runs.
    /// </summary>
    private readonly HashSet<ulong> _backfillFailed = new();

    /// <summary>
    /// What the trace-id index is currently worth, in the numbers an operator needs to answer two
    /// questions: is the migration finished, and what is it costing.
    ///
    /// <para>These were the numbers nobody had. The whole reason the fan-out went unnoticed is
    /// that the segment count and the size of their indexes were invisible from outside — so the
    /// first thing this feature owes anyone is a way to see them, before and after.</para>
    /// </summary>
    public TraceIndexReport DescribeIndex()
    {
        var segs = _coldSegments;
        int covered = 0;
        long spans = 0;
        foreach (var s in segs)
        {
            spans += s.SpanCount;
            if (s.SegmentId != 0 && _manifest.IsCovered(s.SegmentId)) covered++;
        }

        long runBytes = 0;
        foreach (var run in _manifest.Runs)
        {
            try { runBytes += new FileInfo(run.FilePath).Length; }
            catch { /* a run being replaced right now; the total is a report, not an invariant */ }
        }

        var (runs, retained) = _index.Stats;
        return new TraceIndexReport
        {
            ColdSegments      = segs.Length,
            CoveredSegments   = covered,
            ColdSpans         = spans,
            OpenRuns          = runs,
            IndexBytesOnDisk  = runBytes,
            IndexBytesInMemory= retained,
            CatalogGeneration = _manifest.Generation,
        };
    }

    /// <summary>How much of the cold tier the trace-id index answers for.</summary>
    internal (int Covered, int Total) IndexCoverage
    {
        get
        {
            var segs = _coldSegments;
            int covered = 0;
            foreach (var s in segs)
                if (s.SegmentId != 0 && _manifest.IsCovered(s.SegmentId)) covered++;
            return (covered, segs.Length);
        }
    }

    /// <summary>
    /// Merges index runs into fewer, bigger ones when a level has accumulated enough. Returns
    /// whether it did any work.
    ///
    /// <para>ONE MERGE PER CALL, like the backfill, and for the same reason: the caller owns the
    /// pace. Unlike the backfill this can be skipped forever with no consequence but memory —
    /// which is exactly the trade it manages, so it only runs when a level is genuinely full.</para>
    ///
    /// <para>THE MANIFEST IS TOUCHED ONCE, AFTER THE RENAME. Coverage is unchanged by construction
    /// (the merged run carries the union of its inputs'), so there is no instant at which a segment
    /// is vouched for by a file that does not exist, and a crash anywhere before the manifest write
    /// leaves a temp file nobody names.</para>
    /// </summary>
    internal bool CompactIndexOnce(CancellationToken ct = default)
    {
        var batch = TraceIndexCompactor.SelectMergeBatch(_manifest.Runs);
        if (batch.Count == 0) return false;

        ct.ThrowIfCancellationRequested();

        var live = new HashSet<ulong>();
        foreach (var s in _coldSegments) if (s.SegmentId != 0) live.Add(s.SegmentId);

        var merged = new TraceIndexCompactor(_dataDir, _logger).Merge(batch, live);
        if (merged is not { } run)
        {
            // Nothing usable came out. Leaving the manifest alone leaves coverage exactly as it
            // was, which is the whole safety story of this operation.
            return false;
        }

        var oldPaths = batch.Select(static r => r.FilePath).ToList();
        _manifest.ReplaceRuns(oldPaths, [run]);

        // Open the new one BEFORE closing the old ones: a lookup racing this must never find the
        // key in neither. Both are open for an instant, which costs a duplicate hit the read path
        // already tolerates (a trace legitimately lives in two segments).
        _index.Add(run);
        _index.Remove(oldPaths);
        foreach (var p in oldPaths)
        {
            try { if (File.Exists(p)) File.Delete(p); }
            catch (Exception ex) { _logger.LogDebug(ex, "Could not delete merged-away index run {Path}", p); }
        }
        return true;
    }

    /// <summary>
    /// Indexes ONE segment that has no run yet, and returns whether it did any work.
    ///
    /// <para>ONE AT A TIME, ON PURPOSE. Building a run means reading a segment's whole trace index
    /// — the expensive read this feature exists to abolish — so the backfill is the one place that
    /// still pays it. Paid once per segment in the background it buys every later lookup; paid for
    /// forty segments in a row on a 512 MB box it competes with ingest. The caller decides the
    /// pace; this method decides nothing but which segment is next.</para>
    ///
    /// <para>The order is: build the run, fsync it, rename it, and only THEN claim coverage. A
    /// crash anywhere before the last step costs a rebuilt run. The other order would leave a
    /// segment the index is trusted for with nothing behind the trust.</para>
    /// </summary>
    internal bool BackfillNextSegment(CancellationToken ct = default)
    {
        var segs = _coldSegments;
        SpanSegmentInfo? next = null;
        foreach (var s in segs)
        {
            if (s.SegmentId == 0 || _manifest.IsCovered(s.SegmentId)) continue;
            lock (_backfillFailed) { if (_backfillFailed.Contains(s.SegmentId)) continue; }
            next = s;
            break;
        }
        if (next is null) return false;

        ct.ThrowIfCancellationRequested();
        try
        {
            var map = SpanReader.ReadTraceIndex(next.FilePath);
            if (map.Count == 0)
            {
                // A v2 segment, or one with no traces. Nothing to index and nothing to retry —
                // compaction migrates v2 to v3 in the background and it becomes eligible then.
                lock (_backfillFailed) _backfillFailed.Add(next.SegmentId);
                return true;
            }

            var run = WriteIndexRun(next, next.SegmentId, map);
            if (run is not { } r)
            {
                lock (_backfillFailed) _backfillFailed.Add(next.SegmentId);
                return true;
            }

            _manifest.MarkCovered(next.SegmentId, r);
            _index.Add(r);
            _logger.LogDebug("Trace index backfilled {File}: {Traces} traces",
                Path.GetFileName(next.FilePath), map.Count);
            return true;
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            // Nothing here may cost the engine anything. The segment stays readable and uncovered,
            // and is not tried again — a file whose index will not parse will not parse next time.
            lock (_backfillFailed) _backfillFailed.Add(next.SegmentId);
            _logger.LogWarning(ex,
                "Trace index backfill skipped {File} — it stays queryable by scanning", next.FilePath);
            return true;
        }
    }

    /// <summary>
    /// Writes the per-segment index run, or returns null when it cannot be written.
    ///
    /// <para>NULL IS A COMPLETE ANSWER, and the caller must pass it on: a segment recorded without
    /// a run is a segment outside the coverage set, which is a segment read exactly as it was read
    /// before the index existed. Nothing here is allowed to fail a flush — the spans are already
    /// durable by this point, and an index is an optimisation over data that is safe either
    /// way.</para>
    /// </summary>
    private TraceIndexRun? WriteIndexRun(
        SpanSegmentInfo segment, ulong segmentId, Dictionary<TraceId, List<uint>>? traceIndex)
    {
        if (traceIndex is null) return null;
        try
        {
            var w = new TraceIndexWriter();
            foreach (var (traceId, offsets) in traceIndex)
                w.Add(traceId, segmentId, [.. offsets]);
            return w.Write(IndexPathFor(segment.FilePath), level: 1, coveredSegments: [segmentId]);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Could not write the trace-id index for {File} — the segment stays outside the "
              + "index's coverage and is read by scanning, as before", segment.FilePath);
            return null;
        }
    }

    /// <summary>
    /// Reconciles the catalog against what is actually on disk, and hands back the discovered
    /// segments carrying their ids.
    ///
    /// <para>THE DIRECTORY WINS, ALWAYS. That is the whole shape of this method and the reason the
    /// catalog can never become the single point of truth the scan is today. A file with no entry
    /// is ADOPTED — it gets an id, which is how every segment written before this catalog existed
    /// enters it, and how a segment survives a crash between its rename and its manifest write. An
    /// entry with no file is DROPPED, and with it any claim that the index covers it, because an
    /// index vouching for a file that is not there is the silent under-report this engine keeps
    /// closing.</para>
    ///
    /// <para>"No file" means <c>File.Exists</c> said so, not merely "absent from this scan". The
    /// scan deliberately skips the segment a flush is publishing, and a flush that lands while the
    /// scan runs is already in the snapshot — neither is gone, and dropping either would retire an
    /// id that something may already have recorded against.</para>
    /// </summary>
    private List<SpanSegmentInfo> ReconcileCatalog(List<SpanSegmentInfo> loaded)
    {
        try
        {
            var byPath = new Dictionary<string, ulong>(StringComparer.Ordinal);
            foreach (var (id, entry) in _manifest.Segments) byPath[entry.FilePath] = id;

            var named   = new List<SpanSegmentInfo>(loaded.Count);
            int adopted = 0;
            foreach (var seg in loaded)
            {
                if (byPath.TryGetValue(seg.FilePath, out ulong known))
                {
                    named.Add(seg.WithSegmentId(known));
                    continue;
                }
                ulong fresh = _manifest.AllocateSegmentId();
                _manifest.AddSegment(new TraceSegmentEntry(
                    fresh, seg.FilePath, seg.MinStartNano, seg.MaxStartNano, seg.SpanCount));
                named.Add(seg.WithSegmentId(fresh));
                adopted++;
            }

            var vanished = _manifest.Segments
                .Where(kv => !File.Exists(kv.Value.FilePath))
                .Select(kv => kv.Key)
                .ToList();
            if (vanished.Count > 0) _manifest.RemoveSegments(vanished);

            if (adopted > 0 || vanished.Count > 0)
                _logger.LogInformation(
                    "Trace catalog reconciled: {Adopted} segment(s) adopted, {Dropped} entry(ies) "
                  + "dropped for files that are gone; {Total} named, {Covered} covered by the index",
                    adopted, vanished.Count, _manifest.Segments.Count, _manifest.CoveredCount);

            return named;
        }
        catch (Exception ex)
        {
            // The catalog is a convenience. Failing to keep it is worth a log line and nothing
            // more — every segment below simply carries id 0, which every read path already reads
            // as "not covered", which is the scan this engine did before ids existed.
            _logger.LogWarning(ex,
                "Trace catalog could not be reconciled — segments are queryable and unnamed for "
              + "this run, and the trace-id index will not be consulted");
            return loaded;
        }
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
            Dictionary<TraceId, List<uint>>? mergedTraceIndex = null;
            var merged = SpanWriter.Write(_dataDir, allSpans, recoverable: false,
                                          onTraceIndex: map => mergedTraceIndex = map);
            _logger.LogInformation("Compacted {Count} small segments → {File} ({Spans} spans)",
                processed.Count, Path.GetFileName(merged.FilePath), allSpans.Count);

            // Swap the snapshot first (readers stop picking the old files up),
            // delete the merged-away files after. An in-flight reader that still
            // holds the old snapshot skips the deleted file gracefully.
            // The catalog moves in ONE generation: the sources leave, the merged file arrives.
            // Done before the snapshot swap for the same reason the flush does it — a reader must
            // never see a segment the catalog has not heard of — and it takes the sources' coverage
            // with them, because an index vouching for a file that is about to be unlinked is the
            // silent-loss shape this whole design exists to prevent.
            try
            {
                ulong mergedId = _manifest.AllocateSegmentId();
                var   run      = WriteIndexRun(merged, mergedId, mergedTraceIndex);

                _manifest.ReplaceSegments(
                    processed.Select(static s => s.SegmentId).Where(static id => id != 0).ToList(),
                    new TraceSegmentEntry(mergedId, merged.FilePath,
                                          merged.MinStartNano, merged.MaxStartNano, merged.SpanCount),
                    run);

                // The sources' runs are closed here and their files deleted below with the rest of
                // the sidecars; the merged run opens in their place. Order matters only in that a
                // reader must not hold a handle to a file about to be unlinked.
                _index.Remove(processed.Select(static s => IndexPathFor(s.FilePath)));
                if (run is { } r) _index.Add(r);

                merged = merged.WithSegmentId(mergedId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Could not record the merged segment {File} in the trace catalog — it is "
                  + "published and queryable, and will be adopted on the next start", merged.FilePath);
            }

            _lock.EnterWriteLock();
            try
            {
                var next = new List<SpanSegmentInfo>(_coldSegments.Length);
                foreach (var s in _coldSegments)
                    if (!processed.Contains(s)) next.Add(s);
                next.Add(merged);
                _coldSegments = SortedByMaxStartDesc(next);
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
    public async Task<TraceListPage> GetTraceListAsync(
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
        _beforeTraceListScan?.Invoke(ct);

        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;
        int  scanCap  = Math.Max(limit * 5, 500);

        var merged = new Dictionary<TraceId, MergedTrace>(scanCap);

        // Hot tier — group live spans (newest data) under read-lock. Snapshot cold too.
        // The snapshot is taken AS IT IS: `_coldSegments` is maintained sorted by MaxStartNano
        // DESCENDING wherever it is built, so neither this walk nor SearchSpansAsync has to
        // clone-and-sort an array of every segment on the box on every page of every stream.
        SpanSegmentInfo[] segs;
        _lock.EnterReadLock();
        try
        {
            segs = _coldSegments;
            // Includes the in-flight flush snapshot — otherwise the newest rows disappear
            // from the trace list for the duration of every segment build.
            foreach (var s in UnflushedSpansLocked())
            {
                if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
                MergeSpanInto(merged, s);
            }
        }
        finally { _lock.ExitReadLock(); }

        // THE HEIGHT ABOVE WHICH THIS PAGE SETTLED ITS WINDOW — never a minimum over what it
        // merged, and the difference is the whole finding. Cold segments OVERLAP in time, so one
        // WIDE segment walked first (it sorts first: the order is by MaxStartNano) can trip the
        // cap all by itself while a NARROWER segment nested entirely inside its range is still
        // unread. The wide segment's own oldest row says nothing whatever about that.
        //
        // The sound statement is the one the walk can actually make: the list is ordered by
        // MaxStartNano DESCENDING, so nothing in an unvisited segment starts above the FIRST
        // unvisited segment's MaxStartNano. Everything strictly above that was examined, and
        // every match in it is in the returned rows.
        //
        // FLOORS COMPOSE BY MAXIMUM. Each one names a height above which some part of the work
        // is settled, so only the highest is a claim all the others sit under — and a floor
        // placed too LOW is a licence for the caller to page over rows nobody read. There are
        // three sources below: the budget break, a segment that could not be read, and the
        // `limit` cut at the end.
        long scanFloor  = long.MinValue;
        bool visitedAny = false;

        // A FAULT, not a height, and tracked apart from the floor for the reason spelled out on
        // TraceListPage.Unreadable: the catch blocks below HEAL the snapshot, so the floor a
        // vanished segment records is recorded exactly once and every later page finds no
        // segment, no fault and nothing to report.
        bool unreadable = false;

        for (int i = 0; i < segs.Length; i++)
        {
            var seg = segs[i];

            // Both skips come BEFORE the cap test on purpose. A segment holding nothing in
            // [from, to], or provably nothing for this service, is DECIDED rather than skipped
            // — walking past it costs two comparisons and leaves nothing owed. Testing the cap
            // first (as this loop used to) stopped on whichever segment happened to come next,
            // so "the walk was capped" could be recorded over a segment that was irrelevant
            // anyway, and the floor derived from it claimed less than the walk had earned.
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            // NOTE: this treats "the segment does not list the service" as "holds nothing this
            // query wants", which is exactly what the ROWS below already assume — a trace whose
            // only spans in the window live in a service-skipped segment is invisible to this
            // method with or without a floor. The floor is therefore precisely as sound as the
            // page it describes, which is the property that matters to a pager.
            if (serviceName is not null && seg.Services.Length > 0 &&
                !Array.Exists(seg.Services, x => x.Equals(serviceName, StringComparison.OrdinalIgnoreCase)))
                continue;

            // Out of room, with a segment in front of us that had something to contribute.
            // Clamped to `to` because nothing above the window's own ceiling is at stake.
            //
            // `visitedAny` buys the caller FORWARD PROGRESS, and it is not an optimisation. The
            // hot tier is merged before this loop and is not subject to the cap, so on a busy
            // server it can fill the budget by itself — and then the very first cold segment
            // trips this break untouched. Its MaxStartNano is at or above the window ceiling
            // (it is the newest segment), the floor clamps to `to`, and a pager whose cursor is
            // already `to` cannot move: one page of hot rows and a truthful but useless "these
            // results are truncated" over a month of cold data nobody looked at. Reading one
            // segment costs one segment's worth of summaries — bounded, and the same order as
            // the budget itself — and it puts the floor below the hot tier where the cursor can
            // get a grip.
            if (visitedAny && merged.Count >= scanCap)
            {
                scanFloor = Math.Max(scanFloor, Math.Min(seg.MaxStartNano, toNano));
                break;
            }
            visitedAny = true;

            ct.ThrowIfCancellationRequested();

            // PER-SEGMENT FAILURE HANDLING, which this walk was the only cold walk in the engine
            // to lack — GetTraceAsync and SearchSpansAsync have both had it for as long as they
            // have existed, and the whole capped/floor contract the SSE stream is built on is
            // derived from THIS method. Two shapes, both routine:
            //
            //   * a segment that VANISHED. CompactOnePass publishes its merged output and THEN
            //     unlinks its sources, so a scan holding a slightly older snapshot meets deleted
            //     files BY DESIGN — and SelectCompactionBatch takes everything under 10 000
            //     spans, which on a quiet install is every freshly flushed segment, often the
            //     newest one there is. The legacy branch below had no catch at all, so the
            //     FileNotFoundException escaped this method, escaped the Task.Run behind it, and
            //     reached the stream handler's outer catch as "the trace list failed while
            //     streaming results": a banner, a frozen list, and none of the rows from the
            //     segments that were perfectly readable;
            //   * a .trc or .tracesum a power cut truncated. Left in the snapshot deliberately —
            //     removing a file that still exists is a decision for compaction and retention,
            //     not for a read — so it fails again on every page. What it must NOT do is
            //     produce a false ending, and the floor is what stops that: with the segment's
            //     ceiling recorded, the page is capped, and once the cursor descends past that
            //     ceiling the stream reports the band as unexamined instead of exhausted.
            //
            // Either way the cost is ONE SEGMENT. A read that cannot see part of the window says
            // so through the floor; it does not fail the request and it does not stay silent.
            _beforeColdSegmentRead?.Invoke(seg);

            ColdReadFault? segFault = null;
            try
            {
                if (TraceSummarySidecar.Exists(seg.FilePath))
                {
                    // Range-bounded: the caller used to discard the out-of-window rows one at a
                    // time, having paid for a TraceSummary, a services array and three decoded
                    // strings for every trace in a segment compaction may have grown to 200 000
                    // spans — against a budget of `scanCap` rows.
                    if (TraceSummarySidecar.TryReadSummaries(seg.FilePath, fromNano, toNano, out var summaries))
                        foreach (var r in summaries) MergeSummaryInto(merged, r);
                    else
                        // THE BRANCH THE COMPACTION RACE ACTUALLY ARRIVES ON, and it used to be the
                        // one place with no classification at all. TryReadSummaries swallows every
                        // exception and reports false, so a sidecar that vanished between the
                        // Exists probe above and the open — which is precisely what compaction
                        // produces, publishing its merged output before unlinking its sources —
                        // reached the catch blocks below never, and set the fault bit outright.
                        // Re-probing is what tells the two apart: a sidecar that is no longer there
                        // is the missing-file question, a sidecar that is there and will not read
                        // is corrupt.
                        // Reached only when the sidecar itself said "this will not parse" — every
                        // other failure now propagates and lands in the catch chain below, where
                        // ClassifyReadFailure can tell a damaged file from a locked one. It used to
                        // be hardcoded here, so a lock that clears in seconds was a permanent
                        // deleted-or-damaged claim with nothing in the log.
                        segFault = TraceSummarySidecar.Exists(seg.FilePath)
                                 ? ColdReadFault.Corrupt
                                 : MeetMissingSegmentFile(seg);
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
            catch (OperationCanceledException) { throw; }
            catch (FileNotFoundException)
            {
                segFault = MeetMissingSegmentFile(seg);   // loss, handover or unreachable mount
            }
            catch (DirectoryNotFoundException)
            {
                // NOT a removal. This catch used to call RemoveColdSegment, which on a directory
                // fault is a claim the engine has no evidence for — see MeetMissingSegmentFile.
                _logger.LogWarning(
                    "Trace list: could not reach segment {File} — the data directory is not there. " +
                    "The segment stays in the snapshot for the next request", seg.FilePath);
                segFault = ColdReadFault.Transient;
            }
            catch (Exception ex)
            {
                segFault = ClassifyReadFailure(ex, "Trace list", seg.FilePath);
            }

            // Abandoned part-read, so nothing in it was decided. Reported as truncation rather
            // than swallowed: rows missing because a segment is unreadable are still rows
            // missing, and the sibling SearchSpansAsync has always said so for the same two
            // conditions — leaving this one silent made the two streams disagree about the same
            // fault, one calling it truncation and the other calling the window exhausted.
            //
            // The FLOOR is what a pager needs; the BIT is what a stream needs, and the two are
            // not the same statement. The floor says "below here I settled nothing", which the
            // next, narrower page can and usually does make good. The bit says "a file in this
            // window could not be read", and nothing makes that good — least of all the
            // RemoveColdSegment above, which guarantees the next page will not even find the
            // segment to fail on. See TraceListPage.Unreadable.
            //
            // EVERY FAULT OWES THE FLOOR; ONLY A PERMANENT ONE OWES THE BIT. That split is the
            // whole of this round's fix here. A compaction handover and a mount blip both mean
            // THIS walk did not read those rows — the floor, exactly — while the rows themselves
            // are in the replacement or still on the disk that came back. Setting the bit for them
            // told a user of a completely healthy server that their data was deleted or damaged,
            // and the client answers that bit with a red banner and a frozen list.
            if (segFault is { } fault)
            {
                scanFloor  = Math.Max(scanFloor, Math.Min(seg.MaxStartNano, toNano));
                unreadable |= IsPermanentFault(fault);
            }
        }

        // THE FAULT THIS WALK COULD NOT MEET. A segment that vanished was dropped from the
        // snapshot by whichever request tripped over it, so from the next one onwards the loop
        // above walks a clean list, finds every file it looks for, and — truthfully, as far as it
        // can see — reports a window it read out. The engine remembers what the loop cannot; see
        // VanishedRegionLog.
        //
        // THE BIT WITHOUT A FLOOR, deliberately, and it is the one asymmetry in this method. The
        // floor is a height a LATER, NARROWER page could settle: it says "I stopped here, ask me
        // again lower down". There is nothing to ask. This walk did not abandon a segment
        // part-read — it opened every file that exists and finished all of them — so no height
        // names work left undone, and raising one would send the pager down through band after
        // empty band to arrive at "the search could not be advanced", which is both the wrong
        // sentence and the wrong advice for a file that is simply gone. The BIT is the whole
        // statement: part of this window is on no disk, and no width of window will help.
        // TraceListPage.Unreadable exists precisely because those two are not the same fact.
        // See SearchSpansAsync: a segment startup could not load has no range, so it makes every
        // window unreadable rather than a narrower one.
        unreadable |= _coldTierIncomplete || _vanished.Overlaps(fromNano, toNano);

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

        if (list.Count > limit)
        {
            // The `limit` cut is the THIRD place this call stopped short. Rows under it were
            // merged and then thrown away without ever being handed to the caller, so the page
            // does not speak for them — and the height it settled down to is therefore the
            // OLDEST ROW IT KEPT, not the oldest it merged.
            //
            // This one costs the caller nothing, and the pair is worth seeing together: a pager
            // whose cursor is its own oldest returned row lands exactly ON this floor, so the
            // rows under the cut come back on the next page. It is the OTHER two floors — the
            // budget break and the unreadable segment — that can sit above such a cursor, and
            // that is precisely the gap the caller has to be told about.
            scanFloor = Math.Max(scanFloor, list[Math.Max(0, limit - 1)].RootStartNano);
            list.RemoveRange(limit, list.Count - limit);
        }

        // CAPPED and "there is a floor" are the same statement, so they are computed once from
        // one another. A floor above long.MinValue is exactly a region of [from, to] this page
        // does not speak for; no floor is exactly "the window was read out". Deriving one from
        // the other is what stops a caller ever seeing the contradictory pair — capped, no rows,
        // and a floor claiming nothing is left — which has no honest ending at all.
        return new TraceListPage(list, scanFloor != long.MinValue, scanFloor, unreadable);
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
        // The index holds native memory (the bloom bits) behind every open run, so it is released
        // whatever else fails below.
        try { _index.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Trace index store failed to close"); }

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

        // The catalog first, then the files. A catalog entry outliving its file would have the
        // index vouching for data that is gone; a file outliving its entry is only a file nobody
        // has a name for yet, which the next load adopts.
        try
        {
            _manifest.RemoveSegments(
                toDelete.Select(static s => s.SegmentId).Where(static id => id != 0).ToList());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not update the trace catalog while pruning — "
                                 + "the stale entries are dropped on the next start");
        }

        // Close the runs before their files go: a reader holding an open handle to a deleted .tix
        // is a handle to nothing, and on Windows it is a delete that fails.
        _index.Remove(toDelete.Select(static s => IndexPathFor(s.FilePath)));

        foreach (var s in toDelete)
            DeleteSegmentFiles(s.FilePath);

        // THE OTHER BOUND ON THE FAULT RECORD, and the one that keeps it from being a leak that
        // also never shuts up. A remembered vanished range is a statement that part of a window
        // cannot be served; once retention has passed that range, no part of that window can be
        // served, by design, and every query reaching there is outside the TTL anyway. Keeping the
        // record past this point would have the server explaining a lost file to users asking
        // about data it was told to throw away. Same cutoff as the deletion above, so the record
        // lives exactly as long as the data it describes could have been asked for.
        int forgotten = _vanished.Forget(cutoffNano);

        if (toDelete.Count > 0 || forgotten > 0)
            _logger.LogInformation(
                "Retention pruned {Count} trace file(s) older than {Days} days " +
                "(and forgot {Forgotten} vanished-segment range(s) below the cutoff)",
                toDelete.Count, (int)ttl.TotalDays, forgotten);

        return Task.FromResult(toDelete.Count);
    }

    /// <summary>Deletes a cold segment's <c>.trc</c> plus every companion sidecar. Best-effort.</summary>
    private static void DeleteSegmentFiles(string trcPath)
    {
        TryDelete(trcPath);
        TryDelete(Path.ChangeExtension(trcPath, ".stats"));
        TryDelete(Path.ChangeExtension(trcPath, ".svcgraph"));
        TryDelete(Path.ChangeExtension(trcPath, ".tracesum"));
        TryDelete(Path.ChangeExtension(trcPath, ".tix"));

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

    /// <summary>
    /// TRUE when this segment's <c>[MinStartNano, MaxStartNano]</c> is NOT what its file header
    /// said. <c>SpanReader.SaneHeaderRange</c> replaces a range no clock could have produced —
    /// negative, inverted, or past the file's own last-write time — because every later decision
    /// (window skip, sort order, retention's cutoff, and the range a vanished segment hands to
    /// <c>VanishedRegionLog</c>) is taken on these two numbers as if they were facts.
    ///
    /// <para>OBSERVED AND REPORTED, NEVER REPAIRED. An earlier version of this branch clamped an
    /// implausible Max down to the file's mtime plus a day, and hid readable spans: these two
    /// fields decide which segments a walk OPENS, so a value invented at load time can only close
    /// a door the data is behind — silently, because the skip leaves no fault and no floor. The
    /// range is believed as written; the hazard a torn field really creates is bounded where it
    /// does damage, in <c>VanishedRegionLog.Record</c>.</para>
    ///
    /// <para>So <c>Min</c> and <c>Max</c> ARE NOT SANITISED before use, and nothing downstream may
    /// assume they are — this flag reports, it does not repair. It is true when the range is
    /// negative, inverted, or later than the file's own last-write time by more than a day of
    /// clock slack; that third test is the one that catches a <c>Max</c> torn to
    /// <c>long.MaxValue</c>, which is neither of the first two and is the tear that motivated all
    /// of this.</para>
    /// </summary>
    public bool     HeaderRangeSuspect { get; init; }

    /// <summary>
    /// The segment file's last-write time in Unix nanoseconds, read when the file was opened and
    /// carried because the moment it is needed — the segment has vanished — is the moment it can
    /// no longer be read. The ceiling for the range handed to <c>VanishedRegionLog</c>.
    /// </summary>
    public long     LastWriteNano { get; init; }

    /// <summary>
    /// The catalog's name for this segment, or 0 when it has none.
    ///
    /// <para>A FILE PATH IS NOT AN IDENTITY. Compaction writes a merged file and unlinks its
    /// sources, so a path names something that stops existing; anything wanting to record "trace X
    /// is in segment Y" needs a Y that survives that. Allocated once by
    /// <see cref="TraceManifest"/> and carried here so the read paths can ask whether the trace-id
    /// index vouches for this segment.</para>
    ///
    /// <para>0 IS A FIRST-CLASS ANSWER, not a missing value: it means this segment is outside the
    /// catalog — no manifest, a manifest that would not parse, or a file adopted since the last
    /// reconcile. Every read path treats 0 as "not covered", which is the full scan it does today.
    /// The catalog is a convenience over the directory, never a replacement for it.</para>
    /// </summary>
    public ulong    SegmentId { get; init; }

    /// <summary>The same segment, named. Used where the id is learned after the file was read.</summary>
    public SpanSegmentInfo WithSegmentId(ulong id) => new()
    {
        FilePath           = FilePath,
        MinStartNano       = MinStartNano,
        MaxStartNano       = MaxStartNano,
        SpanCount          = SpanCount,
        Services           = Services,
        FormatVersion      = FormatVersion,
        HeaderRangeSuspect = HeaderRangeSuspect,
        LastWriteNano      = LastWriteNano,
        SegmentId          = id,
    };
}
