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
public sealed class TraceStorageEngine : ITraceProvider, ITraceStatsProvider, IServiceGraphProvider, ITraceSummaryProvider, IRetentionTarget, IAsyncDisposable, IDisposable
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

    // ── Shutdown gate ────────────────────────────────────────────────────────
    //
    // Ported from Ameto.Storage.StorageEngine, which closed this exact shape first. Before it
    // there were three `_disposed` reads in three and a half thousand lines, and every one of the
    // following was true at once: the engine is registered under six singleton interfaces, so the
    // container disposes it six times and callers 2-6 returned on the exchange WHILE caller 1 was
    // still inside a multi-hundred-millisecond flush; compaction, retention, the index backfill
    // and the index merge checked nothing, so they could unlink .trc files and reopen index runs
    // after the teardown had freed the lock they take; and SpanWriteAheadLog.Append answered a
    // post-dispose append with `return;` while the very next line still put the span in the hot
    // tier — durable nowhere and queryable at once.

    /// <summary>
    /// 0 = live, 1 = disposed. The exchange that elects ONE teardown; the other five callers
    /// await <see cref="_disposeCompleted"/> rather than returning into a half-torn engine.
    /// </summary>
    private int _disposed;

    /// <summary>
    /// Completed when the teardown has finished. Awaited by every later disposer — the container
    /// holds this instance under six interfaces and a hosted service disposes it as well.
    /// </summary>
    private readonly TaskCompletionSource _disposeCompleted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// 1 once <see cref="DisposeCoreAsync"/> has shut the door: <see cref="WriteSpan"/> refuses,
    /// no NEW heavy phase starts, and no NEW reader enters. Set with a full fence, because the
    /// other half of every one of those handshakes is a lock-free read.
    ///
    /// <para>It gates reads as well as writes on purpose. Kestrel outlives the hosted services —
    /// the metrics engine learned this the same way — so a trace query really does arrive after
    /// the teardown, and the honest answer is an empty page, not an
    /// <see cref="ObjectDisposedException"/> out of the middle of a response.</para>
    /// </summary>
    private int _writesClosed;

    /// <summary>
    /// Phases that hold something the teardown frees: a flush publishing a segment, a compaction
    /// rewriting the catalog and unlinking its sources, a retention pass, the index backfill, an
    /// index merge, the cold scan. Raised only by a phase that passed the close check, so once
    /// <see cref="_writesClosed"/> is set it can only fall.
    /// </summary>
    private int _heavyPhases;

    /// <summary>Installed by the teardown; completed by the decrement that takes <see cref="_heavyPhases"/> to zero.</summary>
    private TaskCompletionSource? _heavyPhasesDrained;

    /// <summary>
    /// Callers inside the engine's lock-taking surface: the six read paths AND the write path.
    /// Writers count here too because <c>_lock.Dispose()</c> is what the count protects, and a
    /// span arriving one instruction before it is exactly as fatal as a query.
    /// </summary>
    private int _activeReaders;

    /// <summary>Installed by the teardown; completed by the release that takes <see cref="_activeReaders"/> to zero.</summary>
    private TaskCompletionSource? _readersDrained;

    /// <summary>
    /// Shared by both waits. Running out of it leaves the lock, the index and the log ALLOCATED,
    /// with an Error naming what is still running — freeing them under a wedged compaction is the
    /// use-after-free this gate exists to prevent, and a hung shutdown is not an improvement on it.
    /// </summary>
    internal TimeSpan _shutdownWaitBudget = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Test seam: when set, the shutdown budget is spent when THIS is cancelled rather than
    /// <see cref="_shutdownWaitBudget"/> after the teardown begins. The budget is one span of time
    /// the final flush and both waits share, so on a clock a slow final flush decides how much of it
    /// the heavy-phase wait gets — a test that means to judge the wait cannot let a disk's fsync
    /// latency decide that. Never set in production.
    /// </summary>
    internal CancellationTokenSource? _shutdownBudgetForTest;

    /// <summary>Test seam: the teardown is about to wait for running heavy phases. Only fires when there is one.</summary>
    internal Action? _onWaitingForHeavyPhases;

    /// <summary>Test seam: the teardown is about to wait for open readers. Only fires when there is one.</summary>
    internal Action? _onWaitingForReaders;

    /// <summary>
    /// Test seam: the teardown has just shut the door, so from here every read answers empty — the
    /// moment at which a consumer still running (the alert evaluator) would read "no spans".
    /// </summary>
    internal Action? _onWritesClosedForTest;

    /// <summary>Test hook: heavy phases in flight (see <see cref="_heavyPhases"/>).</summary>
    internal int HeavyPhasesInFlight => Volatile.Read(ref _heavyPhases);

    /// <summary>Test hook: callers inside the engine (see <see cref="_activeReaders"/>).</summary>
    internal int ActiveReadersForTest => Volatile.Read(ref _activeReaders);

    /// <summary>Test hook: true once the teardown has shut the write path.</summary>
    internal bool WritesClosedForTest => Volatile.Read(ref _writesClosed) != 0;

    /// <summary>
    /// Test hook: true once the teardown has actually freed the lock, the index and the log.
    /// FALSE is the interesting value — it is how a test sees "left frozen" rather than
    /// inferring it from a file handle the OS may or may not have released yet.
    /// </summary>
    internal bool ResourcesFreedForTest { get; private set; }

    /// <summary>Test hook: bytes the span WAL currently holds. A refused span must not move it.</summary>
    internal long WalWrittenBytesForTest => _wal.WrittenBytes;

    /// <summary>
    /// Test seam: called on the compaction thread with the heavy-phase slot ALREADY claimed and
    /// before any merging starts. Blocking in it is the wedged compaction the shutdown budget
    /// exists for — the one heavy phase this engine cannot reach through a flush seam, because
    /// nothing in the teardown joins it.
    /// </summary>
    internal Action? _inCompactionRunForTest;

    /// <summary>
    /// Claims a heavy-phase slot, or refuses because the teardown has begun. The re-check after
    /// the increment is not belt-and-braces: the teardown samples the counter AFTER setting the
    /// flag, so a phase that incremented on the other side of that store would run unwatched.
    /// </summary>
    private bool TryBeginHeavyPhase()
    {
        if (Volatile.Read(ref _writesClosed) != 0) return false;
        Interlocked.Increment(ref _heavyPhases);
        if (Volatile.Read(ref _writesClosed) == 0) return true;
        EndHeavyPhase();
        return false;
    }

    /// <summary>Releases a heavy-phase slot and wakes the teardown if it was the last one.</summary>
    private void EndHeavyPhase()
    {
        if (Interlocked.Decrement(ref _heavyPhases) == 0)
            Volatile.Read(ref _heavyPhasesDrained)?.TrySetResult();
    }

    /// <summary>Enters the engine as a reader or a writer, or refuses because the teardown has begun.</summary>
    private bool TryEnterEngine()
    {
        if (Volatile.Read(ref _writesClosed) != 0) return false;
        Interlocked.Increment(ref _activeReaders);
        if (Volatile.Read(ref _writesClosed) == 0) return true;
        ExitEngine();
        return false;
    }

    /// <summary>Leaves the engine and wakes the teardown if it was the last caller inside.</summary>
    private void ExitEngine()
    {
        if (Interlocked.Decrement(ref _activeReaders) == 0)
            Volatile.Read(ref _readersDrained)?.TrySetResult();
    }

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

    /// <summary>
    /// Test seam: called with the path of an index run the instant after it is written and
    /// renamed, BEFORE anything tries to open it. That instant is the sharing violation an
    /// antivirus produces on Windows, and it is the only way to reach the failed-open branch
    /// without waiting for one.
    /// </summary>
    internal Action<string>? _afterIndexRunWrittenForTest;

    /// <summary>
    /// Test seam: runs between the coverage snapshot and the index lookup, which is the window a
    /// writer has to move through to make a stale snapshot dangerous. Null in production.
    /// </summary>
    internal Action? _betweenCoverageAndLookupForTest;

    /// <summary>Test seam: publish the next segment(s) with no index run, so a test can build the
    /// mixed covered/uncovered state a still-migrating install is in.</summary>
    internal bool SuppressIndexRunsForTest;

    /// <summary>
    /// Test seam: called inside <c>CompleteFlush</c> at the instant the catalog knows a segment and
    /// <c>_coldSegments</c> does not. Index compaction takes no engine lock, so it really can run
    /// there; reaching that window any other way is a matter of luck.
    /// </summary>
    internal Action? _inCatalogNotYetInSnapshotForTest;

    /// <summary>Test hook: every segment the manifest currently vouches for.</summary>
    internal IReadOnlyCollection<ulong> CoveredSegmentIdsForTest =>
        _manifest.Segments.Keys.Where(_manifest.IsCovered).ToList();

    /// <summary>Test hook: every segment named by SOME open run — coverage's counterpart.</summary>
    internal IReadOnlyCollection<ulong> IndexRunCoverageForTest =>
        _manifest.Runs.SelectMany(r => r.CoveredSegments).Distinct().ToList();

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
    internal void DisableTraceIndexForTest() => DisableTraceIndex();

    /// <summary>
    /// The rollback, reachable by an operator: <c>Ameto:Traces:IndexEnabled=false</c> and a
    /// restart. Withdraws every claim of coverage and closes every run, in one generation.
    ///
    /// <para>The <c>.tix</c> files stay on disk. They name nothing while the switch is off, and the
    /// startup sweep leaves the per-segment ones alone because a segment could adopt them again —
    /// so turning the switch back on costs a re-backfill and no data.</para>
    /// </summary>
    private void DisableTraceIndex()
    {
        // THE READERS FIRST, AND THE MANIFEST WRITE MUST NOT BE ABLE TO STOP THE SERVER.
        //
        // This is called from the constructor, and TraceStorageEngine is resolved by three hosted
        // services — so a throw here does not fail one request, it fails the HOST. ClearCoverage
        // ends in a File.Move over the live traces.manifest, which is exactly where an antivirus
        // or backup agent on Windows gives a sharing violation; every OTHER manifest mutation in
        // this engine is wrapped for that reason and this one was not. An operator reaching for
        // the emergency switch because trace lookups look short would have gone from a degraded
        // server to one that will not boot.
        //
        // Closing the runs first is what makes the failure harmless rather than merely survivable:
        // the read path is gated on _index.HasRuns, so with nothing open the index takes no part
        // in any decision even if the coverage set stays on disk. The claim is then withdrawn
        // again at the next start, and the next one, until the write lands.
        var paths = _manifest.Runs.Select(r => r.FilePath).ToList();
        _index.Remove(paths);
        try
        {
            _manifest.ClearCoverage();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "The trace-id index is off and its runs are closed, but the coverage claim could "
              + "not be cleared from the catalog. Nothing uses it — the index takes no part in a "
              + "lookup with no runs open — and the next start will try again");
        }
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

    /// <summary>The .trc format this engine writes — see the constructor parameter.</summary>
    private readonly ushort                                    _segmentVersion;
    private readonly bool                                      _indexEnabled;

    /// <summary>
    /// Spans before a flush is forced, WHATEVER THEY WEIGH — the count half of
    /// <c>max(spanCount ≥ this, hotBytes ≥ budget)</c>. The byte half is
    /// <see cref="_hotTierBudgetBytes"/>, and on a host with room for its cap the two meet at the
    /// ordinary eight-attribute span: 27 MB is 50 000 of them (see
    /// <see cref="MemoryBudgets.TraceHotTierCapBytes"/>). The count stays as a ceiling of its own
    /// because some of what a span costs is not in its bytes — a trace-index entry, a list slot —
    /// and a flood of attribute-less spans must not buy a tier of millions of them.
    /// </summary>
    private const int HotFlushThreshold    = 50_000;

    // ── Memory budgets (TS#3) ────────────────────────────────────────────────
    //
    // A SPAN COUNT IS THE WRONG UNIT, and the comments below this block said so for most of the
    // engine's life: the same 50 000-span tier was 27 MB of ordinary spans and 250-500 MB of
    // spans carrying a SQL statement or a stack, and a 512 MB stand got exactly the caps a 64 GB
    // box got. The tier now flushes on bytes as well as on a count, and compaction plans and
    // loads against bytes; the budgets come from TracesOptions, which defaults them to
    // MemoryBudgets' trace shares — min(the old constant, a share of the managed-heap limit).

    /// <summary>
    /// What one span costs the hot tier beyond its attribute blob: the <see cref="SpanRecord"/>,
    /// its slot in <c>_hotSpans</c>, its share of <c>_traceIdx</c>, and the blob array's own
    /// header. <c>TraceHotTierProbe</c> measures an attribute-less span at 140 B retained and an
    /// eight-attribute one (375-byte blob) at 532 B, so 160 + the blob length prices the ordinary
    /// span at 535 B — within 1 % of what it weighs — and 27 MB at 50 467 of them, just past the
    /// count cap, so a large host still flushes on the count.
    ///
    /// <para>THE NAME AND THE SERVICE ARE NOT CHARGED, deliberately, although the plan sketched
    /// <c>nameLen + serviceLen + attrLen</c>. Both are shared strings in the tier — the service
    /// by construction, one per resource block, and both through the intern pools — so charging
    /// them per span would count one string fifty thousand times; and a length-only sum would
    /// have missed the ~140 B a span costs with no bytes at all, which is a quarter of the
    /// ordinary span. A name or service the pool could NOT share — a full pool keeps the span's own
    /// string — is charged at its object size (<see cref="SpanStringPools.UnpooledStringBytes"/>),
    /// because then it really is one string per span.</para>
    /// </summary>
    internal const int HotSpanOverheadBytes = 160;

    /// <summary>A span's weight in the hot tier's byte budget. See <see cref="HotSpanOverheadBytes"/>.</summary>
    internal static long HotSpanBytes(int attributeBytes) => HotSpanOverheadBytes + (long)attributeBytes;

    /// <summary>
    /// What a span weighs once a compaction pass has read it back out of a segment, beyond its
    /// blob — calibrated so the ordinary span (375-byte blob) is 608 B, which is what
    /// <see cref="MemoryBudgets.TraceMergeCapBytes"/> was: 73 MB over the 120 000 spans a pass
    /// always held, and what <c>TraceCompactionMemoryProbe</c> still measures (607 B/span retained,
    /// Release, at this commit; one cold run of it read 661). Keeping the cap's own calibration is
    /// also what makes a host with room for the cap plan EXACTLY the pairs it planned in spans —
    /// see <see cref="EstimatedSegmentBytes"/>.
    /// </summary>
    internal const int ReadBackSpanOverheadBytes = 233;

    /// <summary>A span's weight in a compaction pass. See <see cref="ReadBackSpanOverheadBytes"/>.</summary>
    internal static long ReadBackSpanBytes(int attributeBytes) => ReadBackSpanOverheadBytes + (long)attributeBytes;

    /// <summary>The read-back weight of a run of spans — what a segment of exactly these would cost a pass.</summary>
    internal static long ReadBackBytesOf(ReadOnlySpan<SpanRecord> spans)
    {
        long bytes = (long)spans.Length * ReadBackSpanOverheadBytes;
        foreach (var s in spans) bytes += s.AttributesBytes.Length;
        return bytes;
    }

    /// <summary>
    /// The spans a pass held when it was capped in spans, and the figure the merge cap is 73 MB
    /// of. A segment of unknown weight is priced at <c>cap / this</c> per span.
    /// </summary>
    private const int SpansPerPassAtCap = 120_000;

    /// <summary>
    /// A segment's read-back weight: measured when this process wrote it, else priced from its span
    /// count at the merge cap's own per-span figure — <c>SpanCount × 73 MB / 120 000</c>, which
    /// makes the byte planner, on a host whose budget is the cap, admit and pair EXACTLY the
    /// segments the 60 000 / 120 000-span planner did (<c>CompactionThresholdTests</c>).
    /// </summary>
    internal static long EstimatedSegmentBytes(SpanSegmentInfo s) =>
        s.WeightBytes > 0 ? s.WeightBytes : (long)s.SpanCount * MemoryBudgets.TraceMergeCapBytes / SpansPerPassAtCap;

    /// <summary>
    /// The byte half of the flush trigger — <see cref="TracesOptions.HotTierMaxBytes"/>, by default
    /// <see cref="MemoryBudgets.TraceHotTierBytes"/>: 27 MB on a large host, ~20 MB on the 512 MB
    /// stand (5 % of its 384 MB heap limit).
    /// </summary>
    private readonly long _hotTierBudgetBytes;

    /// <summary>
    /// One compaction pass's working set — <see cref="TracesOptions.MergeBudgetBytes"/>, by default
    /// <see cref="MemoryBudgets.TraceMergeBytes"/>: 73 MB on a large host, ~24 MB on the stand.
    /// The candidate threshold is half of it (<see cref="CompactionThresholdBytesFor"/>).
    /// </summary>
    private readonly long _mergeBudgetBytes;

    /// <summary>
    /// The span-name and service intern pools (TI#5): the tier keeps one shared string per distinct
    /// name and service instead of one per span. See <see cref="SpanStringPools"/>.
    /// </summary>
    private readonly SpanStringPools _pools;

    /// <summary>Test hook: the intern pools this engine resolves names and services through.</summary>
    internal SpanStringPools PoolsForTest => _pools;

    /// <summary>Test hook: the live tier's records, copied under the read lock.</summary>
    internal List<SpanRecord> HotSpansForTest
    {
        get { _lock.EnterReadLock(); try { return [.. _hotSpans]; } finally { _lock.ExitReadLock(); } }
    }

    /// <summary>Bytes the live tier holds, by <see cref="HotSpanBytes"/>. Under the write lock.</summary>
    private long _hotBytes;

    /// <summary>
    /// What the detached snapshot held when it left the tier, so a failed flush that puts the
    /// snapshot back puts its bytes back with it. Under the write lock.
    /// </summary>
    private long _flushingBytes;

    /// <summary>Test hook: the live tier's bytes by <see cref="HotSpanBytes"/>.</summary>
    internal long HotBytesForTest { get { _lock.EnterReadLock(); try { return _hotBytes; } finally { _lock.ExitReadLock(); } } }

    /// <summary>Test hook: the byte half of the flush trigger this engine was built with.</summary>
    internal long HotTierBudgetBytesForTest => _hotTierBudgetBytes;

    /// <summary>Test hook: one compaction pass's byte budget this engine was built with.</summary>
    internal long MergeBudgetBytesForTest => _mergeBudgetBytes;

    /// <summary>
    /// A cold segment weighing less than this is a compaction candidate: HALF the pass budget, so
    /// the two largest candidates always fit one pass together — see the history below, told in
    /// the spans it was once written in. On a host with room for the cap that is 36.5 MB =
    /// 60 000 ordinary spans, the old threshold exactly.
    ///
    /// <para><b>ON A HOST WHOSE PASS BUDGET IS SMALL, A FULL FLUSH IS NOT A CANDIDATE, AND THAT IS
    /// THE BUDGET SPEAKING.</b> The 512 MB stand's pass budget is 6 % of a 384 MB heap limit,
    /// 24 MB, and its tier budget 5 %, 20 MB — a pass there can afford 1.2 tiers read back, not
    /// the two a pair of full flushes needs. So the stand merges its small segments (the timed
    /// flushes of a quiet hour) and leaves full ones as they are: one ~20 MB segment per tier,
    /// which is one pass's worth either way. Shrinking the tier to half a pass would let full
    /// flushes pair up, produce the same number of segments at rest, and pay a second rewrite of
    /// every span to get there.</para>
    /// </summary>
    internal static long CompactionThresholdBytesFor(long mergeBudgetBytes) => mergeBudgetBytes / 2;

    // ── THE THRESHOLD AND THE PASS CAP, AS THEY WERE WRITTEN — in spans. Kept because the byte budgets above
    // inherit every constraint these taught, and CompactionThresholdTests still speaks in them: on a host whose
    // pass budget is the cap, 36.5 MB and 73 MB ARE 60 000 and 120 000 ordinary spans (EstimatedSegmentBytes). ──
    // THE HISTORY OF THE THRESHOLD, in the unit it was written in. A cold segment smaller than
    // 60 000 spans was a compaction candidate.
    //
    // <para>IT HAS TO BE ABOVE <see cref="HotFlushThreshold"/>, AND FOR MOST OF THIS ENGINE'S LIFE
    // IT WAS BELOW IT. At 10 000 against a flush that writes 50 000, the segment an ordinary flush
    // produces was never a candidate: never a seed, never in a batch, never merged with anything
    // until retention deleted it. Segment count therefore grew with ingest and never fell, and
    // since a trace lookup consults every cold segment, that count is the multiplier on the cost
    // of opening any trace. On a quiet install <see cref="MaxHotAge"/> alone put a floor of
    // twenty-four new segments a day under it.</para>
    //
    // <para>PEAK MEMORY DOES NOT MOVE WITH THIS NUMBER — but only because raising it exposed that
    // the cap was in the wrong place, and the cap was moved. <see cref="MaxSpansPerPass"/> was
    // enforced by the LOADER, which stopped once it had ALREADY read that many spans and so
    // overshot by whatever the last segment held: at most 10 000 before, at most 50 000 after.
    // <see cref="SelectCompactionBatch"/> now applies it while it plans, so a batch of four
    // fifty-thousand-span segments holds exactly the two hundred thousand a batch of twenty
    // ten-thousand-span ones did.</para>
    //
    // <para>That cap is also the real ceiling on how much this buys: four ordinary segments become
    // one, and the result — a hundred and fifty to two hundred thousand spans — is above this
    // threshold and stops merging. So it is roughly a fourfold cut in segment count, not more.
    // Going further means a merge that streams instead of materialising every span, which is a
    // change to a crash-safety-critical path and belongs in its own piece of work.</para>
    //   [CompactionThreshold was 60 000 spans.]

    private const int MaxSegmentsPerPass   = 20;       // merge at most N oldest small segments per run

    // Hard cap on spans loaded into memory per merge pass — the whole memory story of compaction,
    // since <c>CompactOnePass</c> materialises every span it merges.
    //
    // <para>EXACTLY TWO FULL-SIZE CANDIDATES, and the equality is the point rather than a round
    // number. Below it the largest tier cannot merge at all — two 59 999-span segments would not
    // fit and would sit there forever — and above it the pass just costs more for no extra
    // progress, because a third candidate of that size cannot be admitted either way. So the cap
    // is DERIVED from <see cref="CompactionThreshold"/> and moves with it.</para>
    //
    // <para>LOWERED FROM 200 000, because raising the threshold changed how often this is reached
    // even though it did not change the number. <c>SpanSearchBoundTests</c> measures an ordinary
    // eight-attribute OTel span at about 1 749 bytes live: 200 000 is roughly 350 MB, and on the
    // 512 MB deployment this branch exists to keep alive that is most of the process. It used to
    // be unreachable in practice for the worst possible reason — a 50 000-span flush was not a
    // candidate, so nothing an install produced at volume ever merged. Fixing that made the peak
    // routine. 120 000 is about 210 MB, and the price is that ordinary segments merge two at a
    // time instead of four: a twofold cut in segment count per pass instead of fourfold, which is
    // a cost the trace-id index has largely stopped charging for.</para>
    //
    // <para>A SPAN COUNT IS THE WRONG UNIT and this only makes it a smaller wrong unit — the same
    // 120 000 is 24 MB of bare spans or 210 MB of attribute-heavy ones. The real fix is a byte
    // budget, or a merge that streams instead of materialising, and both are changes to a
    // crash-safety-critical path that belong in their own piece of work.</para> [The byte
    // budget is the budgets block above (TS#3); the streaming merge is still its own work.]
    //   [MaxSpansPerPass was 2 x CompactionThreshold = 120 000 spans.]

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

    /// <param name="writeSegmentFormatV4">
    /// Whether flushes and merges write the v4 segment format, which omits the per-segment trace
    /// index and is ~40% smaller. Reading v4 is unconditional; WRITING it is a one-way door,
    /// because a binary older than this one deletes segments whose version it does not know.
    /// See <c>SpanWriter.DefaultVersion</c>.
    /// </param>
    /// <param name="indexEnabled">
    /// Whether the trace-id index may answer at all. False withdraws every coverage claim at
    /// startup and stops new ones being made — the operator-reachable rollback. See
    /// <c>TracesOptions.IndexEnabled</c>.
    /// </param>
    /// <param name="options">
    /// The memory knobs — <see cref="TracesOptions.HotTierMaxBytes"/> and
    /// <see cref="TracesOptions.MergeBudgetBytes"/>. Null, or unset knobs, take
    /// <see cref="MemoryBudgets"/>' trace shares of THIS process's limits, read once here.
    /// </param>
    public TraceStorageEngine(string dataDir, ILogger<TraceStorageEngine> logger,
                              bool writeSegmentFormatV4 = false, bool indexEnabled = true,
                              TracesOptions? options = null)
        : this(dataDir, logger, writeSegmentFormatV4, indexEnabled, options, pools: null)
    {
    }

    /// <param name="pools">
    /// The span-name and service intern pools, shared with the ingest ring when the container
    /// builds both. Null: the engine keeps its own.
    /// </param>
    internal TraceStorageEngine(string dataDir, ILogger<TraceStorageEngine> logger,
                                bool writeSegmentFormatV4, bool indexEnabled,
                                TracesOptions? options, SpanStringPools? pools)
    {
        _pools = pools ?? new SpanStringPools();
        options ??= new TracesOptions();
        var budgets = MemoryBudgets.Current();
        _hotTierBudgetBytes = options.HotTierMaxBytesFor(budgets);
        _mergeBudgetBytes   = options.MergeBudgetBytesFor(budgets);

        _segmentVersion = writeSegmentFormatV4 ? SpanWriter.NewestVersion : SpanWriter.DefaultVersion;
        _indexEnabled   = indexEnabled;
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
        if (_indexEnabled)
        {
            _manifest.DropRuns(_index.Sync(_manifest));
        }
        else
        {
            // The operator's rollback. Nothing is opened, every claim goes, and the engine reads
            // exactly the way it did before the index existed — which is the property the whole
            // coverage rule exists to keep true.
            DisableTraceIndex();
            _logger.LogWarning(
                "The trace-id index is DISABLED by configuration: every coverage claim has been "
              + "withdrawn and trace lookups scan the cold segments. Set Ameto:Traces:IndexEnabled "
              + "to true and restart to bring it back");
        }

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
        // A v1 log whose upgrade cannot complete opens as v1 rather than throwing out of here —
        // a throw from this constructor fails the host (see SpanWriteAheadLog.Open).
        _wal = SpanWriteAheadLog.Open(_walPath, logger: logger);
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

        // MERGED INDEX RUNS ARE NOT NAMED "spans-…", so the sweep above never saw them. A
        // per-segment run is spans-….tix and its temp spans-….tix.tmp, both caught by the pattern;
        // index compaction writes tix-L{level}-{guid}.tix, which is not. A crash between that
        // rename and the manifest write leaves a file nothing names and nothing deletes — harmless
        // to correctness, because TraceIndexStore only ever opens runs the manifest names, and a
        // slow disk leak all the same.
        //
        // Deleted against the manifest rather than by age: the catalog is loaded by now, so "is
        // this run named?" is a question with an answer, and the alternative — a heuristic on
        // write time — would eventually delete a live run on a machine whose clock moved.
        var namedRuns = new HashSet<string>(
            _manifest.Runs.Select(static r => Path.GetFullPath(r.FilePath)), StringComparer.OrdinalIgnoreCase);
        int orphans = 0;
        foreach (var run in Directory.EnumerateFiles(dataDir, "tix-L*.tix")
                                     .Concat(Directory.EnumerateFiles(dataDir, "tix-L*.tix.tmp")))
        {
            if (namedRuns.Contains(Path.GetFullPath(run))) continue;
            try { File.Delete(run); orphans++; } catch { /* retried next start */ }
        }
        if (orphans > 0)
            _logger.LogInformation("Swept {Count} orphaned trace-index run(s) no manifest names", orphans);
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

    /// <summary>
    /// Takes one span into the log and the hot tier. Returns false when the teardown has closed
    /// the write path — the caller keeps the span, exactly as it keeps one the ring refused.
    /// A batch of one: <see cref="WriteSpans"/> is the path, this is its single-span spelling.
    /// </summary>
    internal bool WriteSpan(SpanIngestItem item) =>
        WriteSpans(new ReadOnlySpan<SpanIngestItem>(in item)) == 1;

    /// <summary>
    /// The most spans taken under ONE hold of the engine's write lock (and of the log's append
    /// lock inside it). See <see cref="WriteSpans"/> for why a drained batch is split at all.
    /// </summary>
    internal const int MaxSpansPerWriteHold = 128;

    /// <summary>Test/probe seam: <see cref="MaxSpansPerWriteHold"/>, overridable so a probe can price the cap.</summary>
    internal int _maxSpansPerWriteHold = MaxSpansPerWriteHold;

    /// <summary>
    /// Test seam: called after each write-lock hold of <see cref="WriteSpans"/> is RELEASED, with
    /// the number of spans the call has taken so far. A reader can run from inside it — which is
    /// the property the cap exists for.
    /// </summary>
    internal Action<int>? _afterWriteHoldForTest;

    /// <summary>
    /// Takes a drained batch into the log and the hot tier: ONE engine write lock and ONE log
    /// append lock per <see cref="MaxSpansPerWriteHold"/> spans, instead of both locks — four
    /// interlocked round trips and a point at which a reader could interleave — per span.
    /// Returns how many spans were taken: all of them, or 0 when the teardown has closed the
    /// write path (the gate is checked once, above both halves, so a batch is never split
    /// across the close). A log failure part-way throws AFTER the spans already logged have
    /// joined the hot tier, so the log and the tier still hold exactly the same spans.
    ///
    /// <para>WHY THE BATCH IS SPLIT. A write-lock hold is a wait for every reader:
    /// <c>ReaderWriterLockSlim</c> lets a waiting writer block new readers, so a point lookup
    /// that arrives behind the drainer waits out the whole hold. The drainer hands over up to
    /// 512 spans; one hold for all of them is ~150 µs on the eight-attribute shape, 128 of them
    /// ~40 µs. <c>TraceAggregateLockProbe</c> prices both against a concurrent reader: the cap
    /// keeps nearly all of the amortisation (the locks are paid once per 128 spans instead of
    /// once per span) and gives the reader a quarter of the worst-case wait. A cap alone is not
    /// enough, though: holds back to back leave a sleeping reader no gap to wake into, so between
    /// two holds <see cref="LetQueuedWaitersIn"/> hands the lock to any reader or writer already queued.</para>
    ///
    /// <para>WHAT ONE HOLD STILL GUARANTEES. The log's generation stamps are taken inside the
    /// same engine hold as the hot-tier insert, and a flush's <c>BeginFlush</c> can only run
    /// under that lock too — so no flush can open between a span's append and its insert, and
    /// the stamp is still exactly what decides whether the span is replayed. The flush trigger
    /// moves to the end of each hold, so a tier can pass <see cref="HotFlushThreshold"/> by at
    /// most one hold's worth of spans before the flush starts.</para>
    ///
    /// <para>REFUSED BEFORE THE APPEND, AND THAT IS THE FIX FROM THE SHUTDOWN GATE. The log used
    /// to answer a post-dispose append with <c>return;</c>, silently, while the very next line
    /// still added the span to the hot tier: unrecoverable and queryable at the same instant.
    /// One gate above both halves is the only arrangement in which the two cannot disagree.</para>
    /// </summary>
    internal int WriteSpans(ReadOnlySpan<SpanIngestItem> items)
    {
        var batch = new ItemSpanBatch(items);
        try     { return WriteBatch(ref batch); }
        finally { batch.Dispose(); }
    }

    /// <summary>
    /// THE DRAINER'S DOOR (TI#3): a batch taken out of the raw span ring, its payload still in the
    /// ring's arena. Everything <see cref="WriteSpans"/> says holds, and one thing more — nothing the
    /// tier keeps points into the arena: <see cref="ISpanBatch.AttributesForTier"/> copies the blob
    /// out and the name and service become pool strings, all BEFORE the drainer releases the batch.
    /// That is what keeps the lock-free aggregate passes (which walk captured
    /// <see cref="SpanRecord"/>s long after the lock is gone) clear of the arena's reuse.
    /// </summary>
    internal int WriteRaw<TBatch>(ref TBatch batch) where TBatch : ISpanBatch, allows ref struct =>
        WriteBatch(ref batch);

    /// <summary>
    /// The one write core. Per hold: the tier's strings and blob copies are resolved OUTSIDE the
    /// lock (an intern lookup and a copy per span have no business in a hold every reader waits
    /// out), then the log and the tier take them under ONE hold, exactly as before.
    /// </summary>
    private int WriteBatch<TBatch>(ref TBatch batch) where TBatch : ISpanBatch, allows ref struct
    {
        int count = batch.Count;
        if (count == 0) return 0;
        if (!TryEnterEngine()) return 0;
        int taken = 0;

        // A hold is at most 4 096 spans whatever a probe sets (the production cap is 128), so the
        // per-hold scratch is bounded by a literal the convention scan can read.
        int perHold = Math.Clamp(_maxSpansPerWriteHold, 1, 4_096);
        int scratch = Math.Min(perHold, count);
        string[] names    = System.Buffers.ArrayPool<string>.Shared.Rent(Math.Min(scratch, 4_096));
        string[] services = System.Buffers.ArrayPool<string>.Shared.Rent(Math.Min(scratch, 4_096));
        var      blobs    = System.Buffers.ArrayPool<ReadOnlyMemory<byte>>.Shared.Rent(Math.Min(scratch, 4_096));
        bool[]   pooled   = System.Buffers.ArrayPool<bool>.Shared.Rent(Math.Min(2 * scratch, 8_192));
        try
        {
            while (taken < count)
            {
                int n        = Math.Min(perHold, count - taken);
                int appended = 0;

                for (int j = 0; j < n; j++)
                {
                    names[j]    = batch.Name(taken + j, _pools, out pooled[2 * j]);
                    services[j] = batch.Service(taken + j, _pools, out pooled[2 * j + 1]);
                    blobs[j]    = batch.AttributesForTier(taken + j);
                }

                _lock.EnterWriteLock();
                try
                {
                    // The log's append lock, taken WITHOUT waiting while the engine lock is held:
                    // the one holder that keeps it for long is CommitFlush's persistence barrier
                    // (a drive flush, up to milliseconds), and waiting for it in here would hold
                    // every reader of the hot tier behind that fsync. So on a busy log the engine
                    // lock is let go, the wait happens outside it, and the hold starts over.
                    // Nothing was taken yet, so starting over changes nothing.
                    SpanWriteAheadLog.AppendScope scope;
                    while (!_wal.TryEnterAppendScope(out scope))
                    {
                        _lock.ExitWriteLock();
                        try { _wal.WaitUntilAppendable(); }
                        finally { _lock.EnterWriteLock(); }
                    }

                    // Write-ahead: every span is in the log before it is queryable. If the log
                    // fails part-way, the spans it did take still join the tier — the finally —
                    // and the ones it did not stay out of both.
                    // THE LOG TAKES THE BYTES AS THEY ARRIVED: the ring's UTF-8 name and service go
                    // straight to the UTF-8 append overload, with no string in between (the
                    // item adapter transcodes its strings once, into scratch, for the tests).
                    try
                    {
                        for (; appended < n; appended++)
                        {
                            int i = taken + appended;
                            var h = batch.Header(i);
                            scope.Append(h.TraceId, h.SpanId, h.ParentSpanId, h.StartTimeUnixNano, h.DurationNanos,
                                         h.Kind, h.Status, h.HttpStatusCode,
                                         batch.NameUtf8(i), batch.ServiceUtf8(i), batch.Attributes(i));
                        }
                    }
                    finally
                    {
                        scope.Dispose();
                        for (int j = 0; j < appended; j++)
                            AddToHotTierLocked(batch.Header(taken + j), names[j], pooled[2 * j],
                                               services[j], pooled[2 * j + 1], blobs[j]);
                        taken += appended;
                    }

                    // max(spanCount ≥ N, hotBytes ≥ budget): whichever the tier reaches first. The
                    // count bounds what a span costs beyond its bytes; the bytes bound what a
                    // span carrying a SQL statement or a stack would otherwise make of 50 000.
                    if (_hotSpans.Count >= HotFlushThreshold || _hotBytes >= _hotTierBudgetBytes)
                        TryStartFlushLocked();
                    _insideWriteHoldForTest?.Invoke(taken);
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
                LetQueuedWaitersIn();
                _afterWriteHoldForTest?.Invoke(taken);
            }
        }
        finally
        {
            Array.Clear(names, 0, scratch);    // the pool must not keep a tier's strings alive
            Array.Clear(services, 0, scratch);
            Array.Clear(blobs, 0, scratch);
            System.Buffers.ArrayPool<string>.Shared.Return(names);
            System.Buffers.ArrayPool<string>.Shared.Return(services);
            System.Buffers.ArrayPool<ReadOnlyMemory<byte>>.Shared.Return(blobs);
            System.Buffers.ArrayPool<bool>.Shared.Return(pooled);
            ExitEngine();
        }
        return taken;
    }

    /// <summary>
    /// One span into a held log scope, from an ingest ITEM: the log takes UTF-8 and the item
    /// carries strings, so the name and service are transcoded here, into the stack (or a pooled
    /// buffer for a pathological name). The production path no longer comes through here — the
    /// drainer hands the ring's UTF-8 straight to the log (<see cref="WriteRaw"/>) — and it stays
    /// for the WAL tests' single-span helper, which appends an item the way the engine once did.
    /// </summary>
    internal static void AppendTranscoded(in SpanWriteAheadLog.AppendScope scope, SpanIngestItem item)
    {
        string name    = item.Name        ?? string.Empty;
        string service = item.ServiceName ?? string.Empty;

        // UTF-8 never needs more than 3 bytes per UTF-16 unit (a surrogate pair is 4 for 2).
        int maxBytes = (name.Length + service.Length) * 3;
        byte[]? rented = null;
        Span<byte> buf = maxBytes <= 1024
            ? stackalloc byte[1024]
            : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent((name.Length + service.Length) * 3));
        try
        {
            int n = System.Text.Encoding.UTF8.GetBytes(name, buf);
            int s = System.Text.Encoding.UTF8.GetBytes(service, buf[n..]);
            scope.Append(item.TraceId, item.SpanId, item.ParentSpanId, item.StartTimeUnixNano, item.DurationNanos,
                         item.Kind, item.Status, item.HttpStatusCode,
                         buf[..n], buf.Slice(n, s), item.AttributesBytes);
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// The longest <see cref="WriteSpans"/> waits, after a hold, for readers or a writer that queued
    /// up behind it to get in — 100 µs, about one hold. See <see cref="LetQueuedWaitersIn"/>.
    /// </summary>
    internal static readonly long ReaderHandoffTicks = System.Diagnostics.Stopwatch.Frequency / 10_000;

    /// <summary>Test seam: <see cref="ReaderHandoffTicks"/>, so a test can make the hand-off wait as long as its reader needs.</summary>
    internal long _readerHandoffTicks = ReaderHandoffTicks;

    /// <summary>
    /// Called between two write holds: if readers are queued on the engine lock, spins — never
    /// sleeps — until one of them is in, or until <see cref="ReaderHandoffTicks"/> has passed.
    ///
    /// <para>WITHOUT IT A READER STARVES FOR AS LONG AS THE DRAINER IS BUSY, and that was measured,
    /// not assumed: <c>TraceAggregateLockProbe</c> completed ONE point lookup in 29 ms of batched
    /// ingest, against 881 with a hold per span. <c>ReaderWriterLockSlim</c> wakes the queued
    /// readers when a writer exits, but a woken reader needs microseconds to run and the drainer
    /// re-enters within nanoseconds — the lock has no fairness to stop it barging. A hold per span
    /// hid this because the gaps between holds were as frequent as the holds, and a SPINNING reader
    /// caught one; holds of 128 back to back leave no gap a sleeping reader can reach. Under
    /// sustained overload, which is when the drainer never parks, the trace UI would stop answering.
    /// </para>
    ///
    /// <para>ONE READER IN IS ENOUGH. The next <c>EnterWriteLock</c> then waits for it — and for every
    /// reader the same wake-up let in — while blocking NEW readers, which is the lock's ordinary
    /// writer preference. So readers and the drainer alternate in groups, and a reader waits for at
    /// most one hold instead of for the whole backlog. Bounded, because a queued reader may have been
    /// cancelled or descheduled, and the drainer must not idle for a reader that is not coming.</para>
    ///
    /// <para>A QUEUED WRITER IS BARGED THE SAME WAY, and is let in the same way. The engine's other
    /// writers are the flush publishing a segment and the compaction and retention passes swapping
    /// the cold set. <c>ReaderWriterLockSlim</c> wakes one of them when the drainer exits, and the
    /// drainer's next <c>EnterWriteLock</c> can take the lock before the woken thread does (measured:
    /// 5 of 5 warm runs, <c>A_writer_queued_behind_a_hold_gets_in_before_the_next_one</c>), so a flush
    /// publish — and the WAL commit and tier release behind it — waited for ingest to go quiet. The
    /// spin ends when the waiting-writer count falls below what it was: the woken writer has left its
    /// wait, and with the drainer outside the lock nothing stands between it and the lock.</para>
    /// </summary>
    private void LetQueuedWaitersIn()
    {
        int writersQueued = _lock.WaitingWriteCount;
        if (writersQueued == 0 && _lock.WaitingReadCount == 0) return;
        long deadline = System.Diagnostics.Stopwatch.GetTimestamp() + _readerHandoffTicks;
        var  spin     = new SpinWait();
        while ((writersQueued > 0 && _lock.WaitingWriteCount >= writersQueued)          // none of them in yet
            || (_lock.WaitingReadCount > 0 && _lock.CurrentReadCount == 0))              // no reader in yet
        {
            if (System.Diagnostics.Stopwatch.GetTimestamp() >= deadline) return;
            spin.SpinOnce(sleep1Threshold: -1);   // never Sleep(1): a millisecond is ten holds
        }
    }

    /// <summary>Test hook: the engine lock itself, so a test can queue a reader on it at an exact point.</summary>
    internal ReaderWriterLockSlim LockForTest => _lock;

    /// <summary>Test seam: called INSIDE each write hold of <see cref="WriteSpans"/>, after the spans joined the tier.</summary>
    internal Action<int>? _insideWriteHoldForTest;

    /// <summary>
    /// Test hook: the live log. Lets a test hold the log and the hot tier side by side — the pair
    /// the write path must keep equal — and reach the log's own seams.
    /// </summary>
    internal SpanWriteAheadLog WalForTest => _wal;

    /// <summary>
    /// Materialises a span into the hot tier and its trace index — the ONE insert, shared by live
    /// ingest (either door: the ring's raw batches and the item adapter) and WAL replay. Replay
    /// must not append to the log it is reading from, so it comes in here directly.
    ///
    /// <para><paramref name="attributes"/> is memory the tier keeps for the tier's whole life —
    /// the item's own array, or a copy taken out of the ring's arena. Never a view of the arena:
    /// the lock-free aggregate passes read these records after the lock is gone, and an arena
    /// chunk is reused the instant its last span is drained.</para>
    /// </summary>
    private void AddToHotTierLocked(
        in SpanHeader h, string name, bool namePooled, string service, bool servicePooled,
        ReadOnlyMemory<byte> attributes)
    {
        var record = new SpanRecord
        {
            TraceId           = h.TraceId,
            SpanId            = h.SpanId,
            ParentSpanId      = h.ParentSpanId,
            StartTimeUnixNano = h.StartTimeUnixNano,
            DurationNanos     = h.DurationNanos,
            // ONE STRING PER DISTINCT NAME AND SERVICE IN THE TIER (TI#5), the pool's shared
            // instance rather than the fresh copy every span arrives with. A full pool hands the
            // span's own string back and the span is kept all the same; the budget then charges it.
            Name              = name,
            ServiceName       = service,
            Kind              = h.Kind,
            Status            = h.Status,
            HttpStatusCode    = h.HttpStatusCode,  // promoted — no attrs deserialization

            // THE BLOB, NOT A DICTIONARY, AND THAT IS WHAT THIS LOCK HOLD IS. The mapper already
            // produced these bytes; inflating them here into a Dictionary plus a string per key
            // and a box per value cost 3.5 µs and 1 496 B per span — 68 % of the CPU and 91 % of
            // the allocation of a WriteSpan — inside the engine's EXCLUSIVE write lock, to
            // reproduce a map nothing on the ingest path ever reads. SpanRecord.Attributes decodes
            // it on demand at the four sites that do (TraceQL, GetAttr on ROOT spans, the trace
            // detail DTO), and the flush hands the same bytes to SpanWriter untouched.
            AttributesBytes   = attributes,
        };

        int offset = _hotSpans.Count;
        _hotSpans.Add(record);
        _hotBytes += HotSpanBytes(attributes.Length)
                   + (namePooled    ? 0 : SpanStringPools.UnpooledStringBytes(name))
                   + (servicePooled ? 0 : SpanStringPools.UnpooledStringBytes(service));

        if (!_traceIdx.TryGetValue(h.TraceId, out var offsets))
        {
            offsets = new List<int>(4);
            _traceIdx[h.TraceId] = offsets;
        }
        offsets.Add(offset);

        _hotSince ??= DateTime.UtcNow;
    }

    /// <summary>A replayed item, through the one insert: interned exactly as live ingest is.</summary>
    private void AddToHotTierLocked(SpanIngestItem item)
    {
        string name    = _pools.Name(item.Name ?? string.Empty, out bool namePooled);
        string service = _pools.Service(item.ServiceName ?? string.Empty, out bool servicePooled);
        AddToHotTierLocked(HeaderOf(item), name, namePooled, service, servicePooled, item.AttributesBytes ?? []);
    }

    /// <summary>An item's fixed fields as a <see cref="SpanHeader"/> — the lengths and arena fields are the ring's and stay 0.</summary>
    internal static SpanHeader HeaderOf(SpanIngestItem item) => new()
    {
        TraceId           = item.TraceId,
        SpanId            = item.SpanId,
        ParentSpanId      = item.ParentSpanId,
        StartTimeUnixNano = item.StartTimeUnixNano,
        DurationNanos     = item.DurationNanos,
        Kind              = item.Kind,
        Status            = item.Status,
        HttpStatusCode    = item.HttpStatusCode,
    };

    // ── Query ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The read paths are wrapped rather than gated in place, so the count is raised for the
    /// whole ENUMERATION and not merely for the call that returns the iterator. A trace detail
    /// is read lazily out of cold segments; a reader counted only at the first MoveNext would
    /// leave the teardown free to dispose the lock between two of them.
    /// </summary>
    public async IAsyncEnumerable<SpanRecord> GetTraceAsync(
        TraceId traceId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!TryEnterEngine()) yield break;   // shut down: an empty trace, not an ObjectDisposedException
        try
        {
            await foreach (var s in GetTraceCoreAsync(traceId, ct).ConfigureAwait(false))
                yield return s;
        }
        finally { ExitEngine(); }
    }

    private async IAsyncEnumerable<SpanRecord> GetTraceCoreAsync(
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
        // COVERAGE IS SAMPLED ON BOTH SIDES OF THE LOOKUP, AND IT TAKES BOTH.
        //
        // BEFORE, because every writer publishes run-then-claim: a segment covered at that instant
        // is guaranteed to have had an open run a moment later. Sampling only afterwards admits the
        // state the write order rules out — the backfill's MarkCovered flips a segment false→true
        // after the lookup, and its spans are skipped on the strength of a run the lookup never
        // saw.
        //
        // AFTER, because the previous version of this comment claimed "coverage shrinking between
        // the two is harmless — a segment is merely read", and that is false for both writers that
        // shrink it. PruneAsync and CompactOnePass both withdraw coverage FIRST and close the
        // readers second, so the safe state is "uncovered, run still open" — and a stale snapshot
        // lands in the opposite one. Take a trace living in segments 5 and 6, each with its own
        // run: the request samples coverage, is preempted, and compaction runs to completion —
        // merged run opened, 5 and 6 out of the catalog with their runs, old readers closed. The
        // lookup then sees only the merged run, whose entries carry the MERGED segment id, so
        // there are no hits for 5 or 6 and nothing lands in Unanswerable either. The stale snapshot
        // still says "covered", both segments are skipped, and the merged segment is not in this
        // request's snapshot. HTTP 200, no spans, no exception, no log line.
        //
        // AND NOW THE PROOF COMES OUT OF THE LOOKUP ITSELF, WHICH RETIRES THE ARGUMENT ABOVE.
        //
        // Requiring coverage at both ends stopped the loss, and it stopped it with a timing
        // argument: a segment covered before AND after must have had an open run in between. True,
        // but it is reasoning about what could have happened between two samples, and the thing it
        // was reasoning around is that a skip was inferred from the mere ABSENCE of a hit —
        // Unanswerable speaks only for a run that was ASKED and failed, so a run already gone from
        // the store looked exactly like a healthy one that had cleared the segment.
        //
        // TraceIndexAnswer.AnsweredFor is the fact that replaces the argument: the segments some
        // run was acquired for, read, and gave a verdict on, collected in the same instant as the
        // hits. A run that vanished before the lookup is not in it, so its segments cannot be
        // skipped, whatever any coverage sample says. The second manifest sample is gone with the
        // argument it supported; coveredAtLookup stays as the MANIFEST half of the rule, because
        // the store's word alone was never the whole rule — see the two-part statement above.
        bool useIndex = _index.HasRuns;
        var  coveredAtLookup = useIndex ? _manifest.CoverageSnapshot() : null;
        _betweenCoverageAndLookupForTest?.Invoke();
        var  answer = useIndex ? _index.Lookup(traceId) : default;
        int  opened = 0, skipped = 0;

        // Segments of THIS request's snapshot whose file was already gone when it went to read.
        // Compaction unlinks its sources, so this is the signal that the snapshot has been
        // overtaken — see the recovery pass after the scan.
        int vanished = 0;

        if (segs.Length > 0)
        {
            using var gate = new SemaphoreSlim(Math.Clamp(Environment.ProcessorCount / 2, 2, 8));
            var tasks = new Task<List<SpanRecord>?>[segs.Length];
            for (int i = 0; i < segs.Length; i++)
            {
                var seg = segs[i];

                List<uint>? known = null;
                if (useIndex && seg.SegmentId != 0
                    && coveredAtLookup!.Contains(seg.SegmentId)
                    // AND A RUN ACTUALLY ANSWERED FOR IT, in this lookup, not according to any
                    // sample taken beside it. A run that had already left the store is absent from
                    // this set, so its segments are read.
                    && answer.AnsweredFor?.Contains(seg.SegmentId) == true
                    // A run that could not be read has proved nothing about the segments it covers,
                    // so for this request they are not covered at all.
                    && answer.Unanswerable?.Contains(seg.SegmentId) != true)
                {
                    foreach (var h in answer.Hits)
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
                        Interlocked.Increment(ref vanished);
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
                        // ANY FAILED READ COUNTS AS AN OVERTAKEN SNAPSHOT, not only a missing file.
                        // On Windows a source that compaction has unlinked keeps its directory
                        // entry in delete-pending while another process holds it open with
                        // FILE_SHARE_DELETE — an antivirus or backup agent, the same class of
                        // interference TraceIndexStore.Add and CompleteFlush already document —
                        // and opening that name gives UnauthorizedAccessException, not
                        // FileNotFoundException. Landing here without setting the flag left the
                        // recovery pass switched off and the segment that now holds those spans
                        // unread, which is the exact loss this pass exists to prevent.
                        //
                        // A false positive is nearly free: the pass only looks at segments that
                        // were not in this request's snapshot, so with no replacement to find it
                        // costs one set comparison.
                        Interlocked.Increment(ref vanished);
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

        // ── THE SNAPSHOT THAT WAS OVERTAKEN ──────────────────────────────────────
        //
        // A VANISHED SEGMENT MAY HAVE BEEN REPLACED RATHER THAN DELETED, and until this the
        // difference was invisible. This walk takes one snapshot of _coldSegments and keeps it for
        // the whole request; compaction publishes the merged segment and unlinks its sources, so a
        // request that started first reads paths that no longer exist and never hears about the
        // file that now holds those spans. Every source contributes nothing, the replacement is not
        // in the list, and the trace comes back short — HTTP 200, no exception, no log line. Older
        // than the trace-id index and reproducible without it; the note above the scan called it
        // "skipped (and healed out of the snapshot)", which described the bookkeeping and not the
        // answer.
        //
        // Retention looks identical from here and needs no repair: its segments are deleted, not
        // replaced, so nothing new appears and this pass finds nothing to do.
        //
        // BOUNDED BY CONSTRUCTION rather than by a retry count. It runs only when a file actually
        // vanished, and it reads only segments that were NOT in the original snapshot — which can
        // only be what flushes and compactions published during this one request, a handful at
        // worst. A second overtaking during the recovery pass is left alone deliberately: the loss
        // it could cause is one more compaction deep and the next request answers in full, whereas
        // looping here would put an unbounded amount of work behind an event the caller cannot see.
        if (Volatile.Read(ref vanished) > 0)
        {
            var scanned = new HashSet<string>(segs.Length, StringComparer.Ordinal);
            foreach (var s in segs) scanned.Add(s.FilePath);

            List<SpanSegmentInfo>? appeared = null;
            foreach (var s in _coldSegments)
                if (!scanned.Contains(s.FilePath)) (appeared ??= new List<SpanSegmentInfo>()).Add(s);

            if (appeared is not null)
            {
                _logger.LogDebug(
                    "Trace lookup: {Gone} segment(s) of the snapshot were replaced mid-request; "
                  + "consulting the {New} segment(s) that appeared since", vanished, appeared.Count);

                // THE SAME RULE AS THE MAIN PASS, ON A FRESH ANSWER. The first version of this
                // skipped nothing, on the reasoning that "skipping is what got us here" — which
                // conflated two different skips. What lost the trace was skipping on a STALE
                // snapshot; skipping on an answer taken right here, from runs read right here, is
                // exactly as sound as the decision thirty lines up and rests on the same two
                // halves. Without it every appeared segment that does NOT hold the trace was read
                // end to end — for v4 a walk over every block of the file — to learn what one
                // bloom probe already knew.
                //
                // AND THE GUARD IS ON THE ANSWER, NOT ON THE SEGMENT ID. `default(TraceIndexAnswer)`
                // has a null Hits, and a non-zero SegmentId says nothing about whether any run is
                // open: ids are handed out unconditionally by the flush and the compaction, while
                // runs are not written at all when the index is switched off. So on an engine
                // started with Ameto:Traces:IndexEnabled=false — the documented operator rollback,
                // whose whole promise is that it costs speed and nothing else — a compaction
                // completing inside a request reached this loop with `late.Hits` null and threw a
                // NullReferenceException out of GetTraceAsync, past a try that starts below it,
                // into the pipeline as a 500. A repair written to stop a silently short answer
                // turned it into a crash in exactly the configuration reached by someone already
                // worried about short answers.
                bool useLate = _index.HasRuns;
                var  coveredLate = useLate ? _manifest.CoverageSnapshot() : null;
                var  late = useLate ? _index.Lookup(traceId) : default;

                foreach (var s in appeared)
                {
                    List<uint>? known = null;
                    if (useLate && s.SegmentId != 0
                        && coveredLate!.Contains(s.SegmentId)
                        && late.AnsweredFor?.Contains(s.SegmentId) == true
                        && late.Unanswerable?.Contains(s.SegmentId) != true)
                    {
                        foreach (var h in late.Hits)
                            if (h.SegmentId == s.SegmentId) (known ??= new List<uint>()).AddRange(h.Offsets);

                        if (known is null) { skipped++; continue; }
                    }

                    opened++;
                    try
                    {
                        var walk = known is null
                            ? SpanReader.ReadTraceAsync(s.FilePath, traceId, ct)
                            : SpanReader.ReadTraceAtAsync(s.FilePath, traceId, known, ct);
                        await foreach (var r in walk.ConfigureAwait(false))
                            (cold ??= new List<SpanRecord>()).Add(r);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex,
                            "Trace lookup: could not read {File}, which replaced a segment that went "
                          + "away mid-request", s.FilePath);
                    }
                }
            }
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
        if (!TryEnterEngine()) yield break;
        try
        {
            await foreach (var s in SearchSpansCoreAsync(from, to, serviceName, spanName, status,
                                                         minDurationNanos, maxDurationNanos,
                                                         httpStatusCode, limit, attrHints,
                                                         scanFloor, ct).ConfigureAwait(false))
                yield return s;
        }
        finally { ExitEngine(); }
    }

    private async IAsyncEnumerable<SpanRecord> SearchSpansCoreAsync(
        DateTimeOffset?   from,
        DateTimeOffset?   to,
        string?           serviceName,
        string?           spanName,
        SpanStatusCode?   status,
        long?             minDurationNanos,
        long?             maxDurationNanos,
        short?            httpStatusCode,
        int               limit,
        IReadOnlyList<AttrHint>? attrHints,
        SpanScanFloor?    scanFloor,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
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
        // A heavy phase for its whole duration — it publishes a segment and commits the log,
        // which is precisely what the teardown must not free the lock underneath. Refused once
        // shutdown has begun: the teardown has already taken the final flush, and a second one
        // arriving from the drainer's own disposal (DI decides which of the two runs first) would
        // be racing it for the same tier.
        if (!TryBeginHeavyPhase()) return;
        try { FlushHotTierCore(); }
        finally { EndHeavyPhase(); }
    }

    private void FlushHotTierCore()
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
        _flushingBytes = _hotBytes;       // travels with the snapshot, back into the tier if it fails
        _hotBytes      = 0;
        // SHED ON FLUSH: the next tier interns into an empty name pool, so the pool never holds
        // more than one tier's distinct names. Safe at any moment — the tier stores the shared
        // instances, never pool indices, so nothing that was handed out can change meaning.
        _pools.ShedNames();
        _flushInProgress = true;
        _flushingSpans   = snapshot;
        _unflushedGeneration++;           // the tier was swapped, not appended to: see AggregateKey
        return snapshot;
    }

    /// <summary>
    /// Starts a background flush unless one is already running — or the engine is shutting
    /// down. <see cref="TryBeginHeavyPhase"/> is the FENCE that lets the teardown free the lock
    /// safely: without it a span arriving between the final drain and <c>_lock.Dispose()</c>
    /// could start a flush that publishes its segment and then faults trying to commit the WAL,
    /// leaving a segment on disk whose spans the log still replays — permanent duplicates.
    /// Spans refused here stay in the hot tier AND in the WAL, so the next start replays them.
    /// </summary>
    private void TryStartFlushLocked()
    {
        if (_flushInProgress || _hotSpans.Count == 0) return;
        // The slot is claimed BEFORE the tier is detached. Claimed after, there would be an
        // instant in which the spans are in neither tier and nothing counts the task that holds
        // them — the one state the teardown must never mistake for quiet.
        if (!TryBeginHeavyPhase()) return;
        var snapshot = TakeSnapshotLocked();
        _flushTask = Task.Run(() =>
        {
            try     { CompleteFlush(snapshot); }
            finally { EndHeavyPhase(); }
        });
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
        bool registered = false;

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
                                    onTraceIndex: map  => traceIndex = map,
                                    version:      _segmentVersion);
            // Weighed while the spans are still at hand, so the compaction planner prices this
            // segment by what it holds rather than by its span count.
            info = info.WithWeight(ReadBackBytesOf(CollectionsMarshal.AsSpan(snapshot)));
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

                // OPENED BEFORE IT IS CLAIMED. AddSegment with a run records the coverage claim,
                // and a claim whose run then fails to open leaves the manifest saying "covered"
                // and the store holding nothing — a lookup racing that omits the segment's spans,
                // and it heals only at the next restart. Open first, claim on success, and pass
                // null otherwise: the segment is then simply uncovered, which is the scan this
                // engine did before the index existed.
                if (run is { } r && !_index.Add(r)) run = null;

                _manifest.AddSegment(
                    new TraceSegmentEntry(segId, named.FilePath, named.MinStartNano,
                                          named.MaxStartNano, named.SpanCount),
                    run);
                info = named.WithSegmentId(segId);
                registered = true;

                // THE WINDOW. The catalog now knows this segment and vouches for it; _coldSegments
                // does not, and will not until the swap below. Index compaction takes no engine
                // lock, so it can run right here — and if it judged liveness by the snapshot it
                // would call this segment dead. The seam exists because that window is otherwise
                // only reachable by luck.
                _inCatalogNotYetInSnapshotForTest?.Invoke();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Could not record {File} in the trace catalog — the segment is published and "
                  + "queryable, and will be adopted on the next start", named.FilePath);
            }

            // RETRIED BY THE BACKFILL, NOT ONLY BY THE NEXT START. Any throw above — and both
            // manifest calls end in a File.Move over the live file, which is where an antivirus
            // gives a sharing violation on Windows — left the segment with SegmentId 0 for the
            // life of the process: the backfill skips id 0, and nothing else re-adopts a segment
            // already in the snapshot. The log line said "adopted on the next start" and meant it
            // literally. Queued here instead, so the background worker picks it up in seconds.
            if (!registered && info is { } unnamed)
            {
                lock (_unnamedSegments) _unnamedSegments.Add(unnamed.FilePath);
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
                _flushingBytes = 0;
                _unflushedGeneration++;
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
        _hotBytes += _flushingBytes;      // the snapshot's bytes come back with its spans
        _unflushedGeneration++;

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
        if (!TryBeginHeavyPhase()) return;
        try { LoadColdSegmentsCore(); }
        finally { EndHeavyPhase(); }
    }

    private void LoadColdSegmentsCore()
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
                // A VERSION THIS BUILD DOES NOT KNOW IS NOT DAMAGE — IT IS THE FUTURE, AND DELETING
                // IT IS HOW A ROLLBACK DESTROYS DATA. `Unsupported .trc version N` is an
                // InvalidDataException like any other, so it classified as Corrupt and landed here,
                // where TryReadHeaderRange checks only the magic and therefore SUCCEEDS on a
                // perfectly good newer segment — and the file went, with its three sidecars, logged
                // as "likely format v1". Roll a binary back after a day of writing a newer format
                // and that day is gone, unrecoverably, on first start.
                //
                // A newer file is left alone and the cold tier says it is short: loud, reversible
                // by rolling forward again, and true. Deletion stays for files whose header cannot
                // be read at all, which is where the v1 migration path actually lives.
                if (SpanReader.LooksLikeNewerFormat(file))
                {
                    _coldTierIncomplete = true;
                    _logger.LogError(ex,
                        "Segment {File} was written by a NEWER format than this build understands. "
                      + "It is left untouched — this build cannot read it, and deleting it would "
                      + "destroy data a newer build can. Every trace query reports an unreadable "
                      + "region until this node runs a build that knows the format", file);
                }
                else if (SpanReader.TryReadHeaderRange(file, out long minNano, out long maxNano))
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
    /// Closes and deletes runs the catalog has just stopped naming.
    ///
    /// <para>A merged run outlives all of its segments eventually, and until this existed the only
    /// thing that noticed was the startup sweep — so between two restarts an install accumulated
    /// open readers holding a bloom in native memory and handles on files no lookup would ever
    /// consult. Per-segment runs were always cleaned up by name because their path is derivable
    /// from the segment's; <c>tix-L*.tix</c> has no segment to derive it from, which is exactly why
    /// the catalog now hands the paths back.</para>
    /// </summary>
    private void RetireDroppedRuns(IReadOnlyList<string> paths)
    {
        // deleteFiles, not a delete loop here: the file is unlinked by the last hold on its reader,
        // so a lookup already inside one never meets a path that has stopped existing. See
        // TraceIndexStore.Remove.
        if (paths.Count > 0) _index.Remove(paths, deleteFiles: true);
    }

    /// <summary>
    /// Segments the backfill has tried and failed on. Without it a segment whose index cannot be
    /// read is picked again on every pass, for ever, at whatever rate the worker runs.
    /// </summary>
    private readonly HashSet<ulong> _backfillFailed = new();

    /// <summary>
    /// Segments that are published and queryable but never made it into the catalog, because a
    /// manifest write threw during their flush. Retried by the backfill rather than left until the
    /// next restart: an unnamed segment is invisible to the index for as long as the process
    /// lives, and being v4 it has no per-segment trace index either, so every lookup that reaches
    /// it pays a full span scan.
    /// </summary>
    private readonly HashSet<string> _unnamedSegments = new(StringComparer.Ordinal);

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
    /// Gives a catalog id to any segment whose flush-time registration threw, and puts it back in
    /// the snapshot carrying it. Cheap and usually a no-op: the set is empty on a healthy engine.
    ///
    /// <para>CALLED WHATEVER THE BACKFILL MODE IS, and it used to sit inside
    /// <see cref="BackfillNextSegment"/>, which the worker gates on the mode. So
    /// <c>IndexBackfill: Off</c> — an option whose documentation says the only difference is speed
    /// — quietly turned off the one thing that repairs a segment stuck at id 0, along with every
    /// other "this heals by itself" this engine claims. It is not an index operation: a segment
    /// with no id is invisible to the catalog, to retention's accounting and to any future feature
    /// that needs identity, whether or not anything is indexing it.</para>
    /// </summary>
    internal void AdoptUnnamedSegments()
    {
        if (!TryBeginHeavyPhase()) return;
        try { AdoptUnnamedSegments(_coldSegments); }
        finally { EndHeavyPhase(); }
    }

    private void AdoptUnnamedSegments(SpanSegmentInfo[] segs)
    {
        string[] pending;
        lock (_unnamedSegments)
        {
            if (_unnamedSegments.Count == 0) return;
            pending = [.. _unnamedSegments];
        }

        foreach (string path in pending)
        {
            var seg = Array.Find(segs, s => string.Equals(s.FilePath, path, StringComparison.Ordinal));
            if (seg is null || seg.SegmentId != 0 || !File.Exists(path))
            {
                lock (_unnamedSegments) _unnamedSegments.Remove(path);
                continue;
            }
            try
            {
                ulong id = _manifest.AllocateSegmentId();
                _manifest.AddSegment(new TraceSegmentEntry(
                    id, seg.FilePath, seg.MinStartNano, seg.MaxStartNano, seg.SpanCount));
                RenameSegmentInSnapshot(seg, seg.WithSegmentId(id));
                lock (_unnamedSegments) _unnamedSegments.Remove(path);
                _logger.LogInformation(
                    "Trace catalog adopted {File}, whose flush-time registration had failed", path);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Retrying catalog registration of {File} later", path);
            }
        }
    }

    /// <summary>Swaps one entry of the cold snapshot for an updated copy, under the write lock.</summary>
    private void RenameSegmentInSnapshot(SpanSegmentInfo oldSeg, SpanSegmentInfo newSeg)
    {
        _lock.EnterWriteLock();
        try
        {
            var next = new List<SpanSegmentInfo>(_coldSegments.Length);
            foreach (var s in _coldSegments) next.Add(ReferenceEquals(s, oldSeg) ? newSeg : s);
            _coldSegments = SortedByMaxStartDesc(next);
        }
        finally { _lock.ExitWriteLock(); }
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
        if (!_indexEnabled) return false;
        if (!TryBeginHeavyPhase()) return false;
        try { return CompactIndexOnceCore(ct); }
        finally { EndHeavyPhase(); }
    }

    private bool CompactIndexOnceCore(CancellationToken ct)
    {
        var batch = TraceIndexCompactor.SelectMergeBatch(_manifest.Runs);
        if (batch.Count == 0) return false;

        ct.ThrowIfCancellationRequested();

        // LIVENESS COMES FROM THE CATALOG, NOT FROM THE SNAPSHOT, and the difference is a window
        // wide enough to lose a segment in permanently.
        //
        // A flush publishes into Segments / Runs / Covered and only then swaps _coldSegments, and
        // this method takes no engine lock — the backfill worker calls it straight through. Judging
        // liveness by _coldSegments therefore lets a merge run inside that window, decide the
        // freshly published segment is dead, drop every entry of its run as garbage, exclude it
        // from the merged run's CoveredSegments, and then ReplaceRuns takes its run away. When the
        // swap lands, that segment is covered with no run anywhere holding its entries, and
        // GetTraceAsync's covered/no-hit branch skips it: every trace in it returns empty.
        //
        // The other coverage faults in this design heal at startup. That one does not — nothing
        // re-derives Covered from the runs. Building the set from _manifest.Segments, which is the
        // same structure the coverage claim lives in, is what makes this method's "coverage is
        // unchanged by construction" true rather than nearly true.
        var live = new HashSet<ulong>(_manifest.Segments.Keys);

        var merged = new TraceIndexCompactor(_dataDir, _logger).Merge(batch, live);
        if (merged is not { } run)
        {
            // Nothing usable came out. Leaving the manifest alone leaves coverage exactly as it
            // was, which is the whole safety story of this operation.
            return false;
        }

        // OPEN THE MERGED RUN BEFORE THE MANIFEST HEARS ABOUT IT. ReplaceRuns is what transfers the
        // coverage claim onto it; if the open then fails, every segment the batch covered is
        // claimed with nothing serving it — and unlike the other coverage faults here, the sources
        // are already gone from the manifest, so a restart cannot put it right. Failing before the
        // manifest is touched leaves the old runs exactly as they were, which is the state the
        // merge was trying to improve on and is always safe.
        if (!_index.Add(run))
        {
            try { if (File.Exists(run.FilePath)) File.Delete(run.FilePath); }
            catch (Exception ex) { _logger.LogDebug(ex, "Could not delete the unusable merged run {Path}", run.FilePath); }
            return false;
        }

        var oldPaths = batch.Select(static r => r.FilePath).ToList();
        try
        {
            _manifest.ReplaceRuns(oldPaths, [run]);
        }
        catch (Exception ex)
        {
            // THE ONE STEP THAT CAN FAIL AFTER THE READER IS OPEN. Commit saves before it
            // publishes, so a throw here leaves memory and disk both still naming the OLD runs —
            // correct, and complete. What is left over is this process's reader and the merged
            // file, which no manifest names: the startup sweep would eventually take the file, but
            // the reader would hold its bloom and its handle until the process ended, and every
            // later merge would add another. Undoing it here costs one deleted file.
            _logger.LogWarning(ex,
                "Could not record the merged trace-index run — the existing runs are untouched "
              + "and the merge will be retried");
            _index.Remove([run.FilePath]);
            try { if (File.Exists(run.FilePath)) File.Delete(run.FilePath); }
            catch (Exception del) { _logger.LogDebug(del, "Could not delete the unrecorded merged run {Path}", run.FilePath); }
            return false;
        }

        // The old ones close only now: for the instant both are open a lookup can see the key
        // twice, which the read path already tolerates (a trace legitimately lives in two
        // segments). Closing them first would let a lookup find it in neither.
        //
        // AND THE FILES GO WITH THE LAST HOLD, NOT HERE. Unlinking them on this line undid the
        // overlap the two lines above just bought: a retired reader keeps no handle — ScanBlock
        // REOPENS the run for every block — so a lookup still holding one reads a path that no
        // longer exists. Under the old bool protocol that was swallowed as "not present"; under
        // the tri-state one it is Unreadable, and the store then treats EVERY segment the run
        // covered as unanswerable. For an L3 merge that is a thousand segments dropping to a full
        // scan because a file was deleted on purpose a microsecond earlier.
        _index.Remove(oldPaths, deleteFiles: true);
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
        if (!_indexEnabled) return false;
        if (!TryBeginHeavyPhase()) return false;
        try { return BackfillNextSegmentCore(ct); }
        finally { EndHeavyPhase(); }
    }

    private bool BackfillNextSegmentCore(CancellationToken ct)
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
            if (map is null)
            {
                // THE READ COULD NOT ACCOUNT FOR EVERY SPAN. A footer whose traceIdxOffset was
                // torn short still decodes and still ends the walk inside the file, so the map
                // that came back is well-formed and INCOMPLETE. Indexing it would claim coverage
                // — permission to skip the segment — over spans the scan never reached, and the
                // failure mode of that is a trace that quietly returns fewer spans than it has.
                // Uncovered is the safe state: the segment stays on the scanning path, whole.
                lock (_backfillFailed) _backfillFailed.Add(next.SegmentId);
                _logger.LogWarning(
                    "Trace index backfill refused {File}: its span count does not match its header, "
                  + "so an index built from it would vouch for spans that were never read. The "
                  + "segment stays uncovered and fully queryable by scanning",
                    Path.GetFileName(next.FilePath));
                return true;
            }
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

            // Opened before the claim: MarkCovered is what lets this segment be skipped, and a
            // claim whose run will not open omits the segment's spans until the next restart.
            if (!_index.Add(r))
            {
                lock (_backfillFailed) _backfillFailed.Add(next.SegmentId);
                return true;
            }
            _manifest.MarkCovered(next.SegmentId, r);
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
        if (traceIndex is null || !_indexEnabled || SuppressIndexRunsForTest) return null;
        try
        {
            var w = new TraceIndexWriter();
            foreach (var (traceId, offsets) in traceIndex)
                w.Add(traceId, segmentId, [.. offsets]);
            var written = w.Write(IndexPathFor(segment.FilePath), level: 1, coveredSegments: [segmentId]);
            _afterIndexRunWrittenForTest?.Invoke(written.FilePath);
            return written;
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

            // ONE GENERATION FOR THE WHOLE ADOPTION. Every commit rewrites and fsyncs the entire
            // manifest, and adopting per segment was two of them each — O(N²) bytes and 2N flushes
            // on exactly the start that has the most to adopt, the first one after the upgrade.
            var named    = new List<SpanSegmentInfo>(loaded.Count);
            var unknown  = new List<SpanSegmentInfo>();
            var drafts   = new List<TraceSegmentEntry>();
            foreach (var seg in loaded)
            {
                if (byPath.TryGetValue(seg.FilePath, out ulong known))
                {
                    named.Add(seg.WithSegmentId(known));
                    continue;
                }
                unknown.Add(seg);
                drafts.Add(new TraceSegmentEntry(
                    0, seg.FilePath, seg.MinStartNano, seg.MaxStartNano, seg.SpanCount));
            }

            var fresh = _manifest.AdoptSegments(drafts);
            for (int i = 0; i < unknown.Count; i++) named.Add(unknown[i].WithSegmentId(fresh[i]));
            int adopted = unknown.Count;

            var vanished = _manifest.Segments
                .Where(kv => !File.Exists(kv.Value.FilePath))
                .Select(kv => kv.Key)
                .ToList();
            if (vanished.Count > 0) RetireDroppedRuns(_manifest.RemoveSegments(vanished));

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
        // ONE heavy phase for the whole run, not one per pass. A merge that wedges wedges inside
        // a pass, and the teardown has to see it as busy for as long as it is — a per-pass count
        // would show zero in the gap between two passes and let the lock go under the next one.
        // TraceCompactionWorker starts this with Task.Run(..., ct), and ct does not stop a pass
        // once it has begun: this counter is what shutdown actually waits on.
        if (!TryBeginHeavyPhase()) return;
        try
        {
            _inCompactionRunForTest?.Invoke();   // test seam: parks a run inside its heavy phase
            const int MaxPasses = 500;   // safety valve, ~10k merged segments per run
            int passes = 0;
            while (CompactOnePass() && ++passes < MaxPasses) { }
            if (passes > 0)
                _logger.LogInformation("Compaction run finished: {Passes} pass(es), {Count} cold segments remain",
                    passes, _coldSegments.Length);
        }
        finally { EndHeavyPhase(); }
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
    ///
    /// <para><b>IN BYTES</b> (TS#3). A candidate weighs less than
    /// <see cref="CompactionThresholdBytesFor"/> the pass budget, and a batch stops before its
    /// weight would pass the budget. A segment's weight is <see cref="EstimatedSegmentBytes"/>:
    /// measured when this process wrote it, else priced from its span count at the cap's own
    /// per-span figure — so on a host whose budget IS the cap, this admits and pairs exactly the
    /// segments the 60 000 / 120 000-span planner did. The size TIER stays in spans: it bounds
    /// how often a span is rewritten, which is a matter of counts, not of memory.</para>
    /// </summary>
    /// <remarks>This overload plans against the cap — a host with room for it.</remarks>
    internal static List<SpanSegmentInfo> SelectCompactionBatch(SpanSegmentInfo[] segments) =>
        SelectCompactionBatch(segments, MemoryBudgets.TraceMergeCapBytes);

    /// <inheritdoc cref="SelectCompactionBatch(SpanSegmentInfo[])"/>
    internal static List<SpanSegmentInfo> SelectCompactionBatch(SpanSegmentInfo[] segments, long mergeBudgetBytes)
    {
        const long MaxSpanNanos = 24L * 3600 * 1_000_000_000; // 24 h

        long thresholdBytes = CompactionThresholdBytesFor(mergeBudgetBytes);
        var candidates = segments
            .Where(s => EstimatedSegmentBytes(s) < thresholdBytes || s.FormatVersion < 3)
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

            // THE PLAN CARRIES THE MEMORY BOUND, not the loader. CompactOnePass stopped adding
            // once it had ALREADY read MaxSpansPerPass, which overshoots by whatever the last
            // segment held — invisible while candidates were capped at 10 000 spans, and a quarter
            // of the budget once they can be 50 000. A batch that describes more than a pass may
            // hold is not a plan; it is a number the loader has to argue with.
            long batchBytes = EstimatedSegmentBytes(seed);

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
                long weight = EstimatedSegmentBytes(s);
                if (batchBytes + weight > mergeBudgetBytes) break;         // past what a pass may hold
                batch.Add(s);
                batchBytes += weight;
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
        var small = SelectCompactionBatch(_coldSegments, _mergeBudgetBytes);
        if (small.Count == 0) return false;

        var  allSpans    = new List<SpanRecord>();
        var  processed   = new List<SpanSegmentInfo>(small.Count);
        long loadedBytes = 0;
        foreach (var seg in small)
        {
            // THE LOADER'S OWN GUARD, IN BYTES, AS WELL AS THE PLAN. The plan is exact for a
            // segment this process wrote and weighed; one found on disk at startup is priced from
            // its span count, and a segment of spans carrying SQL statements weighs ten times that
            // estimate. So the pass stops taking segments once what it has ACTUALLY read reaches
            // the budget — overshooting by at most the last segment, which the tier's own byte
            // budget bounds for anything a flush wrote.
            if (loadedBytes >= _mergeBudgetBytes) break;
            try
            {
                int before = allSpans.Count;
                allSpans.AddRange(SpanReader.ReadAll(seg.FilePath));
                loadedBytes += ReadBackBytesOf(CollectionsMarshal.AsSpan(allSpans)[before..]);
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
                                          onTraceIndex: map => mergedTraceIndex = map,
                                          version: _segmentVersion)
                                   .WithWeight(loadedBytes);   // weighed as it was read
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
            TraceIndexRun? run = null;
            try
            {
                ulong mergedId = _manifest.AllocateSegmentId();
                run = WriteIndexRun(merged, mergedId, mergedTraceIndex);

                // Opened before it is claimed — same reason as the flush path. ReplaceSegments with
                // a run is what makes the coverage claim, and a claim whose run will not open omits
                // the merged segment's spans until the next restart.
                if (run is { } fresh && !_index.Add(fresh)) run = null;

                var orphaned = _manifest.ReplaceSegments(
                    processed.Select(static s => s.SegmentId).Where(static id => id != 0).ToList(),
                    new TraceSegmentEntry(mergedId, merged.FilePath,
                                          merged.MinStartNano, merged.MaxStartNano, merged.SpanCount),
                    run);

                // The sources' runs close here and their files are deleted below with the rest of
                // the sidecars. After the merged run is open, so a reader never finds the key in
                // neither. Any MERGED run whose last segment left with this batch goes too — it has
                // no segment to derive its path from, so the catalog is the only thing that knows.
                _index.Remove(processed.Select(static s => IndexPathFor(s.FilePath)));
                RetireDroppedRuns(orphaned);

                merged = merged.WithSegmentId(mergedId);
            }
            catch (Exception ex)
            {
                // THE SAME TWO REPAIRS CompactIndexOnce GOT, ON THE PATH THAT NEEDED THEM MORE.
                //
                // Rollback first. ReplaceSegments ends in a File.Move over the live manifest, and
                // by the time it can throw the merged run is already OPEN. Leaving it open leaks a
                // bloom in native memory behind a file no manifest names — and worse, the two lines
                // that close the SOURCES' runs are skipped, so their readers stay open while
                // DeleteSegmentFiles below unlinks the .tix underneath them. The merged run's path
                // is spans-*.tix, which the startup sweep does not touch (it takes tix-L* only),
                // so nothing would ever collect it either.
                if (run is { } stranded) _index.Remove([stranded.FilePath], deleteFiles: true);

                // And the queue, because the log line below used to be a promise this engine had
                // stopped keeping. "Adopted on the next start" was literal: BackfillNextSegment
                // skips id 0, and ReconcileCatalog runs once per process. The cost here is higher
                // than on the flush path — this file holds the spans of every source, the sources
                // are already gone, and every GET /api/traces/{id} would scan the largest file in
                // the directory until somebody restarted.
                lock (_unnamedSegments) _unnamedSegments.Add(merged.FilePath);

                _logger.LogWarning(ex,
                    "Could not record the merged segment {File} in the trace catalog — it is "
                  + "published and queryable, and is queued for adoption by the background worker",
                    merged.FilePath);
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

    // ── ITraceStatsProvider ────────────────────────────────────────────────────

    // ── Aggregates off the read lock (TS#6) ───────────────────────────────────
    //
    // The four aggregate reads — per-service stats, the service graph, the volume sparkline and
    // the trace list — used to walk every unflushed span INSIDE the read lock: up to two flush
    // thresholds' worth, with a uint[19] allocated per span for the stats and the graph and a
    // copy of the whole tier for the graph's two passes. Every one of those milliseconds is a
    // wait for the drainer, whose write hold ReaderWriterLockSlim queues behind any reader already
    // in. Now the lock is held only to take the view below and the cold array; the passes run
    // after it is released, accumulating into one histogram per service or edge.

    /// <summary>
    /// The unflushed spans as two runs — the detached flush snapshot, then the live tier — taken
    /// UNDER the read lock and walked AFTER it. Taking it is O(1) and allocates nothing.
    ///
    /// <para><b>WHY A RUN IS SAFE TO WALK WITHOUT THE LOCK.</b> Each is a prefix of a
    /// <c>List&lt;SpanRecord&gt;</c>'s backing array, and nothing ever writes inside that prefix:
    /// <c>_hotSpans</c> only grows (an <c>Add</c> writes past the captured count, or into a NEW
    /// array, leaving this one as it was); a flush SWAPS it for a fresh list instead of clearing it,
    /// and <see cref="RestoreSnapshotLocked"/> builds a new list too; the detached snapshot is never
    /// mutated — the segment writer sorts a copy (<c>SpanWriter.Write</c>), which readers already
    /// rely on when they take <see cref="_flushingSpans"/> lock-free. A <c>SpanRecord</c> is
    /// init-only. So the elements a run covers cannot change while it is walked.</para>
    /// </summary>
    private readonly ref struct UnflushedRuns(ReadOnlySpan<SpanRecord> flushing, ReadOnlySpan<SpanRecord> hot)
    {
        public readonly ReadOnlySpan<SpanRecord> Flushing = flushing;
        public readonly ReadOnlySpan<SpanRecord> Hot      = hot;
    }

    /// <summary>See <see cref="UnflushedRuns"/>. Caller holds the read lock.</summary>
    private UnflushedRuns UnflushedRunsLocked() => new(
        _flushingSpans is { } flushing ? CollectionsMarshal.AsSpan(flushing) : default,
        CollectionsMarshal.AsSpan(_hotSpans));

    /// <summary>
    /// Bumped (under the write lock) whenever the unflushed spans change other than by an append:
    /// a flush detaching the tier, a failed flush restoring it, a publish dropping the snapshot.
    /// Together with the live tier's count and the cold array's identity it names one state of
    /// everything an aggregate reads — the memo key below.
    /// </summary>
    private long _unflushedGeneration;

    /// <summary>
    /// What an aggregate was computed over: the window, the cold array (every change to the cold
    /// tier publishes a NEW array, so identity is enough), and the unflushed state. Two calls with
    /// equal keys would read exactly the same spans and sidecars.
    /// </summary>
    private readonly record struct AggregateKey(
        long FromMs, long ToMs, SpanSegmentInfo[] Cold, long UnflushedGeneration, int HotCount);

    private sealed class AggregateMemo<T>(AggregateKey key, T value, long storedAt)
    {
        public readonly AggregateKey Key      = key;
        public readonly T            Value    = value;
        public readonly long         StoredAt = storedAt;
    }

    /// <summary>
    /// How long a memoised aggregate is served, 1 s. The key is exact, so this is not a staleness
    /// bound — an append changes the key at once — but a bound on how long one result (and a cold
    /// sidecar that failed to read into it) is kept.
    /// </summary>
    internal long _aggregateMemoTicks = System.Diagnostics.Stopwatch.Frequency;

    private AggregateMemo<IReadOnlyList<ServiceSegmentStats>>? _statsMemo;
    private AggregateMemo<ServiceGraphDto>?                    _graphMemo;

    /// <summary>
    /// Test seam: called at the start of each aggregate's pass over the unflushed spans, with the
    /// read lock already released — a test asserts from inside it that the calling thread holds no
    /// read lock. Names the aggregate.
    /// </summary>
    internal Action<string>? _aggregatePassForTest;

    private bool TryMemo<T>(AggregateMemo<T>? memo, in AggregateKey key, out T value)
    {
        if (memo is not null && memo.Key == key
            && System.Diagnostics.Stopwatch.GetTimestamp() - memo.StoredAt < _aggregateMemoTicks)
        {
            value = memo.Value;
            return true;
        }
        value = default!;
        return false;
    }

    private static AggregateMemo<T> NewMemo<T>(in AggregateKey key, T value) =>
        new(key, value, System.Diagnostics.Stopwatch.GetTimestamp());

    /// <summary>One service's running totals. The histogram is this accumulator's own array — never a sidecar's.</summary>
    private sealed class ServiceAcc
    {
        public readonly uint[] Buckets = new uint[HistogramBuckets.Count];
        public uint Spans, Errors;
        public long MinDur = long.MaxValue, MaxDur = long.MinValue;
    }

    /// <summary>One directed edge's running totals. Same ownership rule as <see cref="ServiceAcc"/>.</summary>
    private sealed class EdgeAcc
    {
        public readonly uint[] Buckets = new uint[HistogramBuckets.Count];
        public uint Calls, Errors;
    }

    private static ServiceAcc AccFor(Dictionary<string, ServiceAcc> agg, string service)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(agg, service, out _);
        return slot ??= new ServiceAcc();
    }

    private static EdgeAcc EdgeFor(Dictionary<(string, string), EdgeAcc> agg, string from, string to)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(agg, (from, to), out _);
        return slot ??= new EdgeAcc();
    }

    /// <summary>
    /// Adds the in-window spans of a run to the per-service totals, in place: a bucket increment
    /// where there used to be a <c>new uint[19]</c> per span. Runs of one service skip the lookup.
    /// </summary>
    private static void AccumulateStats(
        Dictionary<string, ServiceAcc> agg, ReadOnlySpan<SpanRecord> spans, long fromNano, long toNano)
    {
        string?     lastService = null;
        ServiceAcc? last        = null;
        foreach (var s in spans)
        {
            if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
            if (!ReferenceEquals(s.ServiceName, lastService))
            {
                lastService = s.ServiceName;
                last        = AccFor(agg, lastService);
            }
            var a = last!;
            a.Spans++;
            if (s.Status == SpanStatusCode.Error) a.Errors++;
            if (s.DurationNanos < a.MinDur) a.MinDur = s.DurationNanos;
            if (s.DurationNanos > a.MaxDur) a.MaxDur = s.DurationNanos;
            a.Buckets[BucketIndex(s.DurationNanos)]++;
        }
    }

    public Task<IReadOnlyList<ServiceSegmentStats>> GetAggregateStatsAsync(
        DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        if (!TryEnterEngine()) return Task.FromResult<IReadOnlyList<ServiceSegmentStats>>([]);
        try { return Task.FromResult(GetAggregateStatsCore(from, to)); }
        finally { ExitEngine(); }   // the core is synchronous to its last statement
    }

    private IReadOnlyList<ServiceSegmentStats> GetAggregateStatsCore(DateTimeOffset from, DateTimeOffset to)
    {
        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;

        // The cold array and the unflushed view come from ONE hold: taken separately, a flush
        // publishing in between would be counted twice (once from the detached snapshot, once from
        // its sidecar). That is all the lock is held for.
        SpanSegmentInfo[] statsSegs;
        UnflushedRuns     runs;
        AggregateKey      key;
        _lock.EnterReadLock();
        try
        {
            statsSegs = _coldSegments;
            runs      = UnflushedRunsLocked();
            key       = new(from.ToUnixTimeMilliseconds(), to.ToUnixTimeMilliseconds(),
                            statsSegs, _unflushedGeneration, _hotSpans.Count);
        }
        finally { _lock.ExitReadLock(); }

        if (TryMemo(_statsMemo, in key, out var memoised)) return memoised;

        _aggregatePassForTest?.Invoke(nameof(GetAggregateStatsAsync));
        var agg = new Dictionary<string, ServiceAcc>(StringComparer.OrdinalIgnoreCase);
        AccumulateStats(agg, runs.Flushing, fromNano, toNano);
        AccumulateStats(agg, runs.Hot,      fromNano, toNano);

        // Cold tier — the .stats sidecars only (no span deserialisation). Their arrays are ADDED
        // into this call's own accumulators, never adopted: a sidecar's array must not become a
        // total that the next segment is then summed into.
        foreach (var seg in statsSegs)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            foreach (var s in SpanReader.ReadStats(seg.FilePath))
            {
                var a = AccFor(agg, s.ServiceName);
                var b = s.Buckets;
                for (int i = 0; i < HistogramBuckets.Count; i++) a.Buckets[i] += b[i];
                a.Spans  += s.SpanCount;
                a.Errors += s.ErrorCount;
                if (s.MinDurationNanos < a.MinDur) a.MinDur = s.MinDurationNanos;
                if (s.MaxDurationNanos > a.MaxDur) a.MaxDur = s.MaxDurationNanos;
            }
        }

        var result = new List<ServiceSegmentStats>(agg.Count);
        foreach (var (name, a) in agg)
            result.Add(new ServiceSegmentStats
            {
                ServiceName      = name,
                SpanCount        = a.Spans,
                ErrorCount       = a.Errors,
                MinDurationNanos = a.MinDur == long.MaxValue ? 0 : a.MinDur,
                MaxDurationNanos = a.MaxDur == long.MinValue ? 0 : a.MaxDur,
                Buckets          = a.Buckets,
            });

        // SHARED between callers for as long as it is memoised: every consumer (the stats and
        // latency endpoints, the alert evaluator, the graph's nodes) only reads it.
        _statsMemo = NewMemo(in key, (IReadOnlyList<ServiceSegmentStats>)result);
        return result;
    }

    // ── IServiceGraphProvider ──────────────────────────────────────────────────

    public Task<ServiceGraphDto> GetServiceGraphAsync(
        DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        if (!TryEnterEngine()) return Task.FromResult(new ServiceGraphDto());
        try { return Task.FromResult(GetServiceGraphCore(from, to)); }
        finally { ExitEngine(); }
    }

    /// <summary>Records each in-window span's service by span id — the parents an edge is drawn from.</summary>
    private static void IndexServices(
        Dictionary<SpanId, string> spanSvc, ReadOnlySpan<SpanRecord> spans, long fromNano, long toNano)
    {
        foreach (var s in spans)
            if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano)
                spanSvc[s.SpanId] = s.ServiceName;
    }

    /// <summary>Adds an edge for every in-window span whose parent is known and in another service, in place.</summary>
    private static void AccumulateEdges(
        Dictionary<(string, string), EdgeAcc> edges, Dictionary<SpanId, string> spanSvc,
        ReadOnlySpan<SpanRecord> spans, long fromNano, long toNano)
    {
        foreach (var s in spans)
        {
            if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
            if (s.ParentSpanId.IsEmpty) continue;
            if (!spanSvc.TryGetValue(s.ParentSpanId, out var psvc)) continue;
            if (string.Equals(psvc, s.ServiceName, StringComparison.Ordinal)) continue;
            var e = EdgeFor(edges, psvc, s.ServiceName);
            e.Calls++;
            if (s.Status == SpanStatusCode.Error) e.Errors++;
            e.Buckets[BucketIndex(s.DurationNanos)]++;
        }
    }

    private ServiceGraphDto GetServiceGraphCore(DateTimeOffset from, DateTimeOffset to)
    {
        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;

        // Includes the in-flight flush snapshot, and takes the cold array under the same hold —
        // see GetAggregateStatsCore for why both halves must come from one instant.
        SpanSegmentInfo[] graphSegs;
        UnflushedRuns     runs;
        AggregateKey      key;
        _lock.EnterReadLock();
        try
        {
            graphSegs = _coldSegments;
            runs      = UnflushedRunsLocked();
            key       = new(from.ToUnixTimeMilliseconds(), to.ToUnixTimeMilliseconds(),
                            graphSegs, _unflushedGeneration, _hotSpans.Count);
        }
        finally { _lock.ExitReadLock(); }

        if (TryMemo(_graphMemo, in key, out var memoised)) return memoised;

        _aggregatePassForTest?.Invoke(nameof(GetServiceGraphAsync));
        var edgeAgg = new Dictionary<(string, string), EdgeAcc>(32);

        // Two passes over the unflushed spans — parents must be known before edges can be drawn —
        // straight over the two runs, where the locked version first copied the whole tier into a
        // list so that it could walk it twice.
        if (runs.Flushing.Length + runs.Hot.Length > 0)
        {
            var spanSvc = new Dictionary<SpanId, string>(runs.Flushing.Length + runs.Hot.Length);
            IndexServices(spanSvc, runs.Flushing, fromNano, toNano);
            IndexServices(spanSvc, runs.Hot,      fromNano, toNano);
            AccumulateEdges(edgeAgg, spanSvc, runs.Flushing, fromNano, toNano);
            AccumulateEdges(edgeAgg, spanSvc, runs.Hot,      fromNano, toNano);
        }

        // Cold tier — read .svcgraph sidecars (no span deserialization). Added, never adopted.
        foreach (var seg in graphSegs)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            foreach (var e in ServiceGraphSidecar.ReadEdges(seg.FilePath))
            {
                var acc = EdgeFor(edgeAgg, e.From, e.To);
                acc.Calls  += e.CallCount;
                acc.Errors += e.ErrorCount;
                var b = e.Buckets;
                for (int i = 0; i < HistogramBuckets.Count; i++) acc.Buckets[i] += b[i];
            }
        }

        var edgeDtos  = new ServiceEdgeDto[edgeAgg.Count];
        var nodeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        int k = 0;
        foreach (var ((from2, to2), e) in edgeAgg)
        {
            edgeDtos[k++] = new ServiceEdgeDto
            {
                From       = from2,
                To         = to2,
                CallCount  = e.Calls,
                ErrorCount = e.Errors,
                ErrorRate  = e.Calls > 0 ? (double)e.Errors / e.Calls : 0,
                P95Ms      = HistogramBuckets.Percentile(e.Buckets, 0.95),
            };
            nodeNames.Add(from2);
            nodeNames.Add(to2);
        }

        // Node metrics from the per-service stats — the same window, so on a dashboard that asked
        // for the stats first this is the memoised result rather than a second pass over the tier.
        var statsMap = new Dictionary<string, ServiceSegmentStats>(StringComparer.OrdinalIgnoreCase);
        foreach (var s in GetAggregateStatsCore(from, to))
            statsMap[s.ServiceName] = s;

        var nodeDtos = new ServiceNodeDto[nodeNames.Count];
        k = 0;
        foreach (var name in nodeNames)
        {
            statsMap.TryGetValue(name, out var st);
            nodeDtos[k++] = new ServiceNodeDto
            {
                ServiceName = name,
                SpanCount   = st?.SpanCount ?? 0,
                ErrorRate   = st is { SpanCount: > 0 } ? (double)st.ErrorCount / st.SpanCount : 0,
                P95Ms       = st is not null ? HistogramBuckets.Percentile(st.Buckets, 0.95) : 0,
            };
        }

        var graph = new ServiceGraphDto { Nodes = nodeDtos, Edges = edgeDtos };
        _graphMemo = NewMemo(in key, graph);
        return graph;
    }

    // ── ITraceSummaryProvider ──────────────────────────────────────────────────

    // ONE list, shared with TraceQLExecutor.BuildRow — see HttpSemconvKeys for why the two readers
    // are not allowed their own copies.
    private static readonly string[] MethodKeys = HttpSemconvKeys.MethodKeys;
    private static readonly string[] PathKeys   = HttpSemconvKeys.PathKeys;

    /// <summary>
    /// Trace volume + sparkline over [from,to]. Cold tiers are served purely from the
    /// tiny <c>.tracesum</c> volume headers (no span deserialisation); the hot tier is
    /// grouped live. Bounded by (segments × grid-cells) — cheap for any window width.
    /// </summary>
    public async Task<TraceVolume> GetTraceVolumeAsync(
        DateTimeOffset from, DateTimeOffset to, int buckets, CancellationToken ct = default)
    {
        if (!TryEnterEngine()) return new TraceVolume();
        try { return await GetTraceVolumeCoreAsync(from, to, buckets, ct).ConfigureAwait(false); }
        finally { ExitEngine(); }
    }

    private async Task<TraceVolume> GetTraceVolumeCoreAsync(
        DateTimeOffset from, DateTimeOffset to, int buckets, CancellationToken ct)
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

        // The cold array and the unflushed view under one short read-lock; the grouping after it.
        // Includes the in-flight flush snapshot: without it a refresh mid-build shows a dip in the
        // sparkline exactly where the newest traces are.
        SpanSegmentInfo[] segs;
        UnflushedRuns     runs;
        int               hotTraces;
        _lock.EnterReadLock();
        try
        {
            segs      = _coldSegments;       // published whole and never mutated: no copy needed
            runs      = UnflushedRunsLocked();
            hotTraces = _traceIdx.Count;
        }
        finally { _lock.ExitReadLock(); }

        if (runs.Flushing.Length + runs.Hot.Length > 0)
        {
            _aggregatePassForTest?.Invoke(nameof(GetTraceVolumeAsync));
            // Presized to the live trace index's count, read under the lock above: an in-memory
            // figure, not a file field (the FileBounds scan cannot tell a captured local apart).
            var hot = new Dictionary<TraceId, HotVolAcc>();
            hot.EnsureCapacity(hotTraces);
            foreach (var s in runs.Flushing) AccumulateVolume(hot, s);
            foreach (var s in runs.Hot)      AccumulateVolume(hot, s);
            foreach (var a in hot.Values) Add(a.HasRoot ? a.RootStart : a.Earliest, 1, a.Err ? 1u : 0u);
        }

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
        // An empty page, not a half-read one: Capped stays false because nothing was abandoned
        // part-way — there is no storage left to abandon.
        if (!TryEnterEngine()) return new TraceListPage([], false, long.MinValue);
        try
        {
            return await GetTraceListCoreAsync(from, to, serviceName, spanName, status,
                                               minDurationNanos, maxDurationNanos, limit, ct)
                        .ConfigureAwait(false);
        }
        finally { ExitEngine(); }
    }

    private async Task<TraceListPage> GetTraceListCoreAsync(
        DateTimeOffset   from,
        DateTimeOffset   to,
        string?          serviceName,
        string?          spanName,
        SpanStatusCode?  status,
        long?            minDurationNanos,
        long?            maxDurationNanos,
        int              limit,
        CancellationToken ct)
    {
        _beforeTraceListScan?.Invoke(ct);

        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;
        int  scanCap  = Math.Max(limit * 5, 500);

        var merged = new Dictionary<TraceId, MergedTrace>(scanCap);

        // The cold array and the unflushed view under one short read-lock; the grouping of the live
        // spans after it (see UnflushedRuns). The cold snapshot is taken AS IT IS: `_coldSegments`
        // is maintained sorted by MaxStartNano DESCENDING wherever it is built, so neither this walk
        // nor SearchSpansAsync has to clone-and-sort an array of every segment on the box on every
        // page of every stream.
        SpanSegmentInfo[] segs;
        UnflushedRuns     runs;
        _lock.EnterReadLock();
        try
        {
            segs = _coldSegments;
            runs = UnflushedRunsLocked();
        }
        finally { _lock.ExitReadLock(); }

        // Includes the in-flight flush snapshot — otherwise the newest rows disappear from the
        // trace list for the duration of every segment build.
        if (runs.Flushing.Length + runs.Hot.Length > 0) _aggregatePassForTest?.Invoke(nameof(GetTraceListAsync));
        foreach (var s in runs.Flushing)
            if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano) MergeSpanInto(merged, s);
        foreach (var s in runs.Hot)
            if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano) MergeSpanInto(merged, s);

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
            SetHttpAttrs(s, m);
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

    /// <summary>
    /// THE TRACE LIST READS TWO KEYS, SO IT READS TWO KEYS — not a whole attribute map, and above
    /// all not a whole attribute map from inside <c>_lock.EnterReadLock()</c>.
    ///
    /// <para><see cref="MergeSpanInto"/> runs under the read lock over every unflushed span and
    /// asks this for the first root span of each trace. Reaching the answer through
    /// <see cref="SpanRecord.Attributes"/> made that ask the FIRST touch of the record's blob, so
    /// the lazy decode ran right there: a <c>Dictionary</c>, a key string and a box per attribute
    /// per root span of the tier, inside a lock the drainer's <c>WriteSpan</c> has to wait out —
    /// and memoised on the record afterwards, so a tier that had been listed once stayed that much
    /// heavier until it flushed. Release, 20 000-span tier, 2 000 traces: 2 023 → 623 B allocated
    /// per root span, and 1 888 → 498 B LEFT ON THE TIER, which at the 50 000-span threshold is
    /// 9,0 → 2,4 MB the tier never gives back, on the first page after every flush.</para>
    ///
    /// <para>WHAT IT COSTS, because it is not free: the decode was memoised and this walk is not,
    /// so a second page over the same tier pays it again — the probe measures page 2 at 5,1 ms
    /// against the memoised path's 3,0 ms, ≈ 1 µs per root span of read-lock hold per page, and
    /// 96 B per root span for the one <c>GetString</c> the dictionary had already paid for. The
    /// trade is deliberate and it is the round's stated order — resident memory first, and the
    /// 512 MB stand died of the live set, not of a millisecond. Memoising the two strings on the
    /// record instead is the SSE hot-tier re-walk, which the plan gives to WP9.</para>
    ///
    /// <para>ONE WALK, BOTH QUESTIONS, AND ONE COPY OF IT: the walk itself is
    /// <see cref="HttpSemconvKeys.Resolve"/>, beside the key lists it reads, because
    /// <c>TraceQLExecutor.BuildRow</c> asks the same question of the same records of the same
    /// tier and must not answer it a second way.</para>
    /// </summary>
    private static void SetHttpAttrs(SpanRecord s, MergedTrace m) =>
        HttpSemconvKeys.Resolve(s, out m.HttpMethod, out m.HttpPath);

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

    /// <summary>
    /// The histogram bounds, read ONCE. <c>HistogramBuckets.Bounds</c> is a <c>ReadOnlySpan&lt;long&gt;</c> property
    /// over <c>new long[] { ... }</c>, and measured it allocates on every access: 72 B per
    /// <c>HistogramBuckets.IndexOf</c>, 720 KB for the stats pass over 10 000 spans. The values still come
    /// from that one list; only the access is cached.
    /// </summary>
    private static readonly long[] BucketBounds = HistogramBuckets.Bounds.ToArray();

    /// <summary><c>HistogramBuckets.IndexOf</c> without the allocation — see <see cref="BucketBounds"/>.</summary>
    private static int BucketIndex(long durationNanos)
    {
        var bounds = BucketBounds;
        for (int i = 0; i < bounds.Length; i++)
            if (durationNanos < bounds[i]) return i;
        return bounds.Length;
    }

    /// <summary>
    /// The teardown. One caller runs it; the other five — the container holds this instance
    /// under six interfaces, and <c>TraceStorageHostedService</c> disposes it as well — await it.
    /// Returning early on the exchange is what let a test fixture delete the data directory, or
    /// a process exit, run on top of a flush that was still writing a segment.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            await _disposeCompleted.Task.ConfigureAwait(false);
            return;
        }

        try { await DisposeCoreAsync().ConfigureAwait(false); }
        finally { _disposeCompleted.TrySetResult(); }
    }

    /// <summary>
    /// The synchronous bridge, kept because <see cref="IDisposable"/> is how most of this
    /// engine's callers still stop it. It blocks on the same teardown; nothing on that path
    /// captures a synchronisation context, so there is none to deadlock against.
    /// </summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// In the order the lifetime invariant needs: final flush (while writes are still open, so
    /// it is an ordinary heavy phase and the wait below covers it) → close the door on writes,
    /// new heavy phases and new readers → wait out running heavy phases → wait out callers
    /// inside the engine → free the lock, the index and the log. Both waits share one budget.
    ///
    /// <para>RUNNING OUT OF THE BUDGET FREES NOTHING. A compaction wedged on a network volume is
    /// still holding <c>_lock</c>, <c>_index</c> and the manifest; disposing them underneath it
    /// is the use-after-free this gate exists to prevent, and hanging the host instead is not an
    /// improvement on it. The engine is left allocated with an Error naming what is still
    /// running — the process is on its way out, and a leaked lock for its last second costs
    /// nothing.</para>
    ///
    /// <para>ONE BUDGET, AND THE FINAL FLUSH SPENDS IT TOO — deliberately. What the budget bounds is
    /// how long this teardown holds up the host's stop, and that is one number however the time is
    /// divided. A slow final flush leaving the heavy-phase wait little or nothing is the right
    /// trade: running out is SAFE by construction (the engine is left frozen, not freed, and the WAL
    /// replays whatever the flush did not commit), and a disk slow enough to eat the budget in a
    /// flush is the same disk the wedged compaction is on. A fresh budget per wait would double the
    /// worst-case stop to a minute — twice what the host allots its whole shutdown, and far past a
    /// container runtime's kill — to reach the same frozen state later. So the budget stays shared, and is a token rather
    /// than a deadline only so that a test can decide the instant it is spent
    /// (<see cref="_shutdownBudgetForTest"/>) instead of a disk's fsync latency deciding it.</para>
    /// </summary>
    private async Task DisposeCoreAsync()
    {
        using var clockBudget = _shutdownBudgetForTest is null
            ? new CancellationTokenSource(_shutdownWaitBudget > TimeSpan.Zero ? _shutdownWaitBudget : TimeSpan.Zero)
            : null;
        CancellationToken budget = (clockBudget ?? _shutdownBudgetForTest!).Token;

        // ── Final flush, BEFORE the close, so it goes through the ordinary heavy-phase path.
        //    It waits out an in-flight background flush and then drains the tier — a clean stop
        //    commits the WAL and leaves nothing to replay. A failure leaves every span in the
        //    log (Abandon keeps its generation live). Off this thread and bounded, because the
        //    wait inside it is a blocking Task.Wait on whatever flush is already running.
        try { await Task.Run(FlushHotTier).WaitAsync(budget).ConfigureAwait(false); }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
            _logger.LogError(
                "The final span flush did not finish within {Budget}s — the WAL replays the tier "
              + "on the next start", _shutdownWaitBudget.TotalSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Final span flush failed — the WAL replays the tier next start");
        }

        // ── Close the door. A full fence: the other half of each handshake (WriteSpan,
        //    TryBeginHeavyPhase, TryEnterEngine) is a lock-free read, and a store sinking below
        //    those loads would let a phase start after shutdown had stopped counting.
        Interlocked.Exchange(ref _writesClosed, 1);
        Interlocked.MemoryBarrier();
        _onWritesClosedForTest?.Invoke();

        // ── Wait for heavy phases. Only a phase that passed the close check is counted, so from
        //    here the number can only fall.
        bool phasesEnded = true;
        var heavyDrained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Volatile.Write(ref _heavyPhasesDrained, heavyDrained);
        Interlocked.MemoryBarrier();   // the decrement's read of the source must see it, or this read must see the zero
        if (Volatile.Read(ref _heavyPhases) != 0)
        {
            _onWaitingForHeavyPhases?.Invoke();
            phasesEnded = await CompletesBy(heavyDrained.Task, budget).ConfigureAwait(false);
        }

        // ── Wait for callers inside the engine: a query still scanning, a span still between
        //    EnterWriteLock and ExitWriteLock.
        bool callersEnded = true;
        var readersDrained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Volatile.Write(ref _readersDrained, readersDrained);
        Interlocked.MemoryBarrier();
        if (Volatile.Read(ref _activeReaders) != 0)
        {
            _onWaitingForReaders?.Invoke();
            callersEnded = await CompletesBy(readersDrained.Task, budget).ConfigureAwait(false);
        }

        if (!phasesEnded || !callersEnded)
        {
            _logger.LogError(
                "Shutdown: {Phases} trace heavy phase(s) and {Callers} caller(s) still inside the "
              + "engine after {Budget}s — the engine lock, the trace-id index and the span WAL are "
              + "left frozen rather than freed under them",
                Volatile.Read(ref _heavyPhases), Volatile.Read(ref _activeReaders),
                _shutdownWaitBudget.TotalSeconds);
            return;
        }

        // ── Free. The index holds native memory (the bloom bits) behind every open run, so it
        //    goes whatever else fails; the WAL's disposal carries the log's last fsync, so
        //    nothing above may be allowed to skip it.
        try { _index.Dispose(); } catch (Exception ex) { _logger.LogWarning(ex, "Trace index store failed to close"); }

        try { _lock.Dispose(); }
        catch (SynchronizationLockException ex) { _logger.LogWarning(ex, "Trace engine lock still in use at shutdown — left to the finalizer"); }
        finally { _wal.Dispose(); ResourcesFreedForTest = true; }
    }

    /// <summary>True if <paramref name="task"/> completes before the shutdown <paramref name="budget"/> is spent.</summary>
    private static async Task<bool> CompletesBy(Task task, CancellationToken budget)
    {
        try
        {
            await task.WaitAsync(budget).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested) { return false; }
    }

    // ── IRetentionTarget ───────────────────────────────────────────────────

    public string RetentionKey => "traces";

    public Task<int> PruneAsync(TimeSpan ttl, CancellationToken ct = default)
    {
        // RetentionService holds this engine as an IRetentionTarget and can call at any time,
        // including after the teardown — where it used to unlink .trc files through a disposed
        // lock. Gated, a late pass is a no-op and the next start expires the same segments.
        if (!TryBeginHeavyPhase()) return Task.FromResult(0);
        try { return PruneCore(ttl, ct); }
        finally { EndHeavyPhase(); }
    }

    private Task<int> PruneCore(TimeSpan ttl, CancellationToken ct)
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
        IReadOnlyList<string> orphanedRuns = [];
        try
        {
            orphanedRuns = _manifest.RemoveSegments(
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
        RetireDroppedRuns(orphanedRuns);

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

    /// <summary>
    /// What this segment's spans weigh once a compaction pass has read them back, in bytes —
    /// known for a segment THIS process wrote (the flush and the merge weigh what they wrote,
    /// see <c>TraceStorageEngine.ReadBackSpanBytes</c>), 0 for one found on disk at startup.
    ///
    /// <para>0 is not "empty": the planner then estimates the weight from <see cref="SpanCount"/>
    /// at the per-span figure the merge budget was calibrated on, which is exactly the arithmetic
    /// the span-count planner did — so a restart plans the way the old engine did, and the loader's
    /// own byte guard is what bounds a pass whose spans turn out heavier than that. Nothing on disk
    /// records it; a format change for a planning hint was not worth its one-way door.</para>
    /// </summary>
    public long WeightBytes { get; init; }

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
        WeightBytes        = WeightBytes,
    };

    /// <summary>The same segment, weighed. Used where the writer has just told us what it wrote.</summary>
    public SpanSegmentInfo WithWeight(long weightBytes) => new()
    {
        FilePath           = FilePath,
        MinStartNano       = MinStartNano,
        MaxStartNano       = MaxStartNano,
        SpanCount          = SpanCount,
        Services           = Services,
        FormatVersion      = FormatVersion,
        HeaderRangeSuspect = HeaderRangeSuspect,
        LastWriteNano      = LastWriteNano,
        SegmentId          = SegmentId,
        WeightBytes        = weightBytes,
    };
}

/// <summary>
/// A run of spans the write path takes into the log and the hot tier — the ONE shape the engine
/// consumes (TI#3), whichever door the spans came through: the raw ring's drained batch, or the
/// item adapter the tests and the WAL-era callers use. Implemented by <c>ref struct</c>s and
/// consumed through a generic constraint, so no batch is ever boxed.
/// </summary>
internal interface ISpanBatch
{
    int Count { get; }

    /// <summary>The span's fixed fields; only ids, times, kind, status and HTTP status are read.</summary>
    SpanHeader Header(int i);

    /// <summary>The name as UTF-8 — valid until the next <see cref="NameUtf8"/> call on this batch.</summary>
    ReadOnlySpan<byte> NameUtf8(int i);

    /// <summary>The service as UTF-8 — valid until the next <see cref="ServiceUtf8"/> call on this batch.</summary>
    ReadOnlySpan<byte> ServiceUtf8(int i);

    /// <summary>The msgpack attribute blob, for the log.</summary>
    ReadOnlySpan<byte> Attributes(int i);

    /// <summary>The name as the tier keeps it: through the pools.</summary>
    string Name(int i, SpanStringPools pools, out bool pooled);

    /// <summary>The service as the tier keeps it: through the pools.</summary>
    string Service(int i, SpanStringPools pools, out bool pooled);

    /// <summary>
    /// The blob as the tier keeps it — memory that lives as long as the record does. A batch whose
    /// bytes live in memory that is REUSED (the ring's arena) must copy here; see
    /// <c>TraceStorageEngine.WriteRaw</c> for why.
    /// </summary>
    ReadOnlyMemory<byte> AttributesForTier(int i);
}

/// <summary>
/// <see cref="SpanIngestItem"/>s as a <see cref="ISpanBatch"/>: the names and services are the items'
/// strings, transcoded to UTF-8 into pooled scratch for the log (one buffer for names, one for
/// services, so both are valid at the append); the blob is the item's own array.
/// </summary>
internal ref struct ItemSpanBatch : ISpanBatch
{
    private readonly ReadOnlySpan<SpanIngestItem> _items;
    private byte[]? _nameScratch;
    private byte[]? _serviceScratch;

    public ItemSpanBatch(ReadOnlySpan<SpanIngestItem> items) => _items = items;

    public readonly int Count => _items.Length;

    public readonly SpanHeader Header(int i) => TraceStorageEngine.HeaderOf(_items[i]);

    public ReadOnlySpan<byte> NameUtf8(int i)    => Utf8(_items[i].Name, ref _nameScratch);

    public ReadOnlySpan<byte> ServiceUtf8(int i) => Utf8(_items[i].ServiceName, ref _serviceScratch);

    public readonly ReadOnlySpan<byte> Attributes(int i) => _items[i].AttributesBytes;

    public readonly string Name(int i, SpanStringPools pools, out bool pooled) =>
        pools.Name(_items[i].Name ?? string.Empty, out pooled);

    public readonly string Service(int i, SpanStringPools pools, out bool pooled) =>
        pools.Service(_items[i].ServiceName ?? string.Empty, out pooled);

    public readonly ReadOnlyMemory<byte> AttributesForTier(int i) => _items[i].AttributesBytes ?? [];

    private static ReadOnlySpan<byte> Utf8(string? s, scoped ref byte[]? scratch)
    {
        if (string.IsNullOrEmpty(s)) return default;
        int max = System.Text.Encoding.UTF8.GetMaxByteCount(s.Length);
        if (scratch is null || scratch.Length < max)
        {
            if (scratch is not null) System.Buffers.ArrayPool<byte>.Shared.Return(scratch);
            scratch = System.Buffers.ArrayPool<byte>.Shared.Rent(Math.Max(256, (s.Length + 1) * 3));
        }
        int n = System.Text.Encoding.UTF8.GetBytes(s, scratch);
        return scratch.AsSpan(0, n);
    }

    public void Dispose()
    {
        if (_nameScratch    is not null) System.Buffers.ArrayPool<byte>.Shared.Return(_nameScratch);
        if (_serviceScratch is not null) System.Buffers.ArrayPool<byte>.Shared.Return(_serviceScratch);
        _nameScratch = _serviceScratch = null;
    }
}
