using System.Buffers;
using System.Collections.Concurrent;
using System.Numerics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// What <see cref="StorageEngine.ImportSegment"/> did with the file it was handed. The caller
/// that wrote that file owns it on anything but <see cref="Registered"/> — nothing in the engine
/// refers to it, so leaving it in the segments directory means a file no query, no retention pass
/// and no merge will ever touch again.
/// </summary>
public enum SegmentImportOutcome
{
    /// <summary>
    /// In the catalog: either the key was free, or the same segment was re-pushed under it — in
    /// which case the entry and the bytes already held stay and the pushed body is discarded.
    /// </summary>
    Registered,

    /// <summary>
    /// Refused. A DIFFERENT segment — read and compared, not guessed — already holds this
    /// (node, id): in the catalog, or on disk at the final path the boot scan has not reached
    /// yet. It was kept; the incoming one was not registered. Two nodes are configured with the
    /// same NodeId — the one ambiguity the key cannot resolve, and a configuration error rather
    /// than a race.
    /// </summary>
    ConflictDifferentSegment,

    /// <summary>
    /// Refused. The final path is occupied by a file this node COULD NOT READ, so nothing about
    /// "different" or "same" was established — adjudicating a torn leftover is the boot sweep's
    /// job, not a push's. The file on disk was kept and nothing was registered. Unlike the two
    /// conflicts above this says nothing about NodeId configuration, and it is not necessarily
    /// permanent: it clears when the unreadable file is removed.
    /// </summary>
    ConflictUnreadableIncumbent,

    /// <summary>
    /// Refused. The id falls inside the span this node's own allocator has already handed out:
    /// a local flush, merge or WAL replay holds it or is about to publish under it, and none of
    /// those can give a key back. Same deployment fault as above, different evidence — split so
    /// the endpoint can tell the sender which one it saw.
    /// </summary>
    ConflictAllocatedLocally,

    /// <summary>The file could not be opened as a segment. Nothing was registered.</summary>
    Unreadable,
}

/// <summary>
/// Manages the full lifecycle of storage segments:
///   - Maintains the active hot-tier segment
///   - Triggers flush (hot → cold) when size/age thresholds are exceeded
///   - Manages cold-tier segment catalog
///   - Enforces retention policy (deletes expired segments)
///
/// This class is the central coordinator — it implements ISegmentProvider for
/// the query layer and ISegmentManager for the admin API.
/// </summary>
public sealed class StorageEngine : ISegmentProvider, ISegmentManager, IAsyncDisposable
{
    /// <summary>
    /// Creates the index sink for ONE INDEX GROUP. Injected by the Indexing layer at startup to
    /// avoid a circular project reference; null until it is wired, which is why merge waits.
    ///
    /// <para>A FRESH sink per group is the mechanism that keeps peak index-build memory
    /// O(group) rather than O(segment) — the accumulators die at the boundary. The sink must
    /// emit posting-list offsets as the FILE ordinals the writer hands it, not group-local
    /// ones.</para>
    /// </summary>
    public SegmentIndexSinkFactory? IndexSinkFactory { get; set; }

    /// <summary>
    /// Optional hook called on the write path after each event is accepted into the hot tier.
    /// Used by the Alerts layer to evaluate rules without a circular project reference.
    /// The callback must be fast and non-blocking.
    /// Provides the event header and resolved message template string.
    /// </summary>
    public Action<LogEventHeader, string>? EventWritten { get; set; }

    /// <summary>
    /// Optional hook called after a hot-tier segment has been written to cold storage.
    /// Used by the Cluster layer to replicate the segment to followers.
    /// </summary>
    public Action<SegmentInfo>? SegmentFlushed { get; set; }

    private readonly ServerOptions                        _options;
    private readonly RetentionStore                       _retentionStore;
    private readonly ILogger<StorageEngine>               _logger;
    private readonly string                               _dataDir;
    private readonly string                               _walDir;
    private readonly string                               _segDir;

    // Hot tier + WAL travel as ONE immutable triple, swapped by a single volatile store.
    // TryWrite runs on no lock while the flush swap mutated the two old fields one at a
    // time — which gave every rotation a window where an accepted event hit the new tier
    // while the WAL reference was still null (never journaled, lost on crash) or already
    // disposed (append through a released mapping: AccessViolation). A reference can't
    // tear, so a writer always sees a tier WITH the WAL that journals it.
    private volatile WriteState                           _write;

    private sealed class WriteState(HotTierSegment hot, WriteAheadLog? wal, ulong walSegId)
    {
        public readonly HotTierSegment Hot = hot;
        public readonly WriteAheadLog? Wal = wal;
        /// <summary>
        /// First id of the block RESERVED FOR THE LIVE WAL, i.e. the ids its events will
        /// occupy once they are flushed. The WAL file is named from it, and startup uses
        /// that name to decide whether a WAL still holds unflushed events.
        ///
        /// <para>It is a reservation, not a peek at <c>_nextSegmentId</c>, and that is the
        /// whole point. The WAL used to be named from whatever <c>_nextSegmentId</c>
        /// happened to be, which is also the id a MERGE takes — so the first merge after
        /// any flush published a segment carrying the live WAL's id, the restart check
        /// "a segment with this id exists ⇒ this WAL was already flushed" fired on it, and
        /// every un-flushed event in that WAL was deleted. Measured before this change:
        /// WAL id 25, merged segment id 25, 30 events written, 0 recovered, on 3 of 3 runs.</para>
        /// </summary>
        public readonly ulong WalSegId = walSegId;
    }
    // Serialises only the fast hot-tier/WAL *swap* — NOT the heavy cold-segment write.
    // WaitAsync(0) drops a redundant trigger: whoever holds it swaps the whole tier.
    private readonly SemaphoreSlim                        _flushLock  = new(1, 1);
    // Bounds how many cold-segment builds (index + compress + write) run concurrently.
    // The swap hands off to this so multiple segments persist in parallel on idle cores
    // instead of serialising behind one flush (the 50k/s ingest-drop bottleneck).
    private readonly SemaphoreSlim                        _flushConcurrency;
    // Back-pressure gate: caps how many frozen-but-not-yet-persisted hot tiers may be
    // in flight, bounding RAM (≈ slots × HotTier.MaxSizeBytes). When exhausted the swap
    // is skipped, so the full hot tier back-pressures the drainer instead of buffering
    // unbounded tiers in memory. Acquired non-blocking at swap, released after persist.
    private readonly SemaphoreSlim                        _flushSlots;
    // In-flight parallel cold-flush tasks, so DisposeAsync can await them before the
    // tiers they read are freed. Self-pruning via ContinueWith on completion.
    private readonly ConcurrentDictionary<Task, byte>    _inFlightFlushes = new();
    /// <summary>
    /// Segments the header aggregation has already warned it could not read, so a torn file is
    /// named at Warning ONCE rather than on every histogram poll and alert tick that meets it.
    ///
    /// <para>Bounded by <see cref="WarnedUnreadableSegmentCap"/>, and never cleared to make room:
    /// clearing it once full made every unreadable segment past the cap warn again on every poll.
    /// Once full, a segment not already in it is logged at Debug only (the count still reports
    /// it). A key leaves when <see cref="DeleteSegmentAsync"/> removes its segment, and an add
    /// that finds its segment already removed takes itself back (see
    /// <see cref="LogUnreadableSegment"/>), so the set holds segments the catalog still serves,
    /// not every torn file this process ever met.</para>
    /// </summary>
    private readonly ConcurrentDictionary<SegmentKey, byte> _warnedUnreadableSegments = new();
    /// <summary>Makes the cap check and the add in <see cref="LogUnreadableSegment"/> one step.</summary>
    private readonly System.Threading.Lock _warnedUnreadableGate = new();
    /// <summary>How many unreadable segments are named at Warning; internal so a test can lower it.</summary>
    internal int WarnedUnreadableSegmentCap = 1024;
    /// <summary>
    /// Segments a merge has taken out of the catalog, by key, with the number of the record that
    /// named each and the key of the output that now holds its events. The header aggregation
    /// consults it when a segment in its snapshot is gone by the time a worker opens it, because
    /// the two ways a segment leaves mid-scan mean opposite things for the count. Retention's
    /// removal took the events out of the store, so leaving them out IS the answer. A merge's
    /// removal moved them into its output, which a snapshot taken before the merge published
    /// does not list, so leaving them out gives a low total — and presented as complete, a wrong
    /// one.
    ///
    /// <para>The output is kept because "before the merge published" is not every snapshot that
    /// lists a source. The merge publishes its output first and deletes its sources after, so a
    /// snapshot taken in between lists both — and a scan over it reads the source's events in
    /// the output. Missing the source there loses nothing, and calling the total a floor would
    /// make an exact count look partial.</para>
    ///
    /// <para>Written by the merge (<see cref="RecordMergedAwaySegment"/>), not by
    /// <see cref="DeleteSegmentAsync"/>: every caller of the delete — retention, the merge's source
    /// cleanup, anything calling the public method — arrives with nothing but a key. And written
    /// BEFORE the delete, so a scan that finds the entry gone always finds the record too.</para>
    ///
    /// <para>Bounded by <see cref="MergedAwaySegmentCap"/>, oldest record out first. Eviction is
    /// not allowed to turn a merge back into a silent low count: <see cref="_mergedAwayEvictedThrough"/>
    /// says how far it has reached, and a scan that may have lost a record to it reports the
    /// removal as a merge (see <see cref="MayHaveBeenMergedAway"/>). Everything here is under
    /// <see cref="_mergedAwayGate"/>, a leaf, taken only by the merge's cleanup and by a scan
    /// that has already failed to read a segment.</para>
    /// </summary>
    private readonly Dictionary<SegmentKey, MergedAwayRecord> _mergedAwaySegments = new();
    /// <summary>One entry of <see cref="_mergedAwaySegments"/>: which record named the source, and where its events went.</summary>
    private readonly record struct MergedAwayRecord(long Number, SegmentKey Output);
    /// <summary>Record <c>n</c>'s key at <c>[(n - 1) % Length]</c>; allocated by the first merge.</summary>
    private SegmentKey[]? _mergedAwayRing;
    /// <summary>Records ever made. Written under the gate; a scan reads it without, as its mark.</summary>
    private long _mergedAwayRecorded;
    /// <summary>Number of the newest record evicted from <see cref="_mergedAwaySegments"/>; 0 while none has been.</summary>
    private long _mergedAwayEvictedThrough;
    private readonly System.Threading.Lock _mergedAwayGate = new();
    /// <summary>
    /// How many merged-away keys are kept: eight full merge batches, a few hundred KB once full. A
    /// record only has to outlive the scans already running when it was made, and a scan that
    /// outlives this many records calls any removal it meets a merge rather than guess. Internal
    /// so a test can lower it before the first merge.
    /// </summary>
    internal int MergedAwaySegmentCap = 8 * MergeMaxSources;
    /// <summary>Pause between attempts to persist a frozen tier whose flush failed.</summary>
    private static readonly TimeSpan FlushRetryDelay = TimeSpan.FromSeconds(15);
    /// <summary>True while the live WAL is refusing appends — gates the once-per-episode error log (writer thread only).</summary>
    private bool _walFaulted;
    /// <summary>EventWritten subscriber faults, for throttled logging (writer thread only).</summary>
    private long _hookFaults;
    private readonly CancellationTokenSource               _cts        = new();
    private readonly Task                                  _flushLoop;
    /// <summary>Timer msyncing the live WAL (see <see cref="RunWalFlushLoopAsync"/>).</summary>
    private readonly Task                                  _walFlushLoop;
    /// <summary>Low-priority cold-tier loop: merge recovery, then small-segment merges.</summary>
    private readonly Task                                  _maintenanceLoop;
    /// <summary>Test hook: lets merge run without an index builder (tests verify the scan fallback).</summary>
    internal bool _allowIndexlessMerge;
    /// <summary>Test hook: shrinks the index-group budget so a small segment still spans several groups.</summary>
    internal long _groupPayloadBudgetBytes = SegmentWriter.DefaultGroupPayloadBudgetBytes;
    /// <summary>
    /// Test hook: called with the level whose segment has just been MOVED into place — the exact
    /// seam at which a crash can split a multi-level flush. Throwing from it reproduces a kill
    /// between two <c>File.Move</c>s without needing a second process.
    /// </summary>
    internal Action<int>? _afterLevelPublished;
    /// <summary>
    /// Test hook: called with the manifest on disk and the sources owned but not yet streamed —
    /// the window in which a merge can be cancelled or fail transiently. Cancelling a token from
    /// it reproduces a shutdown landing between the flush-slot wait and the merge task; throwing
    /// from it reproduces a source that dies mid-stream, without corrupting a file to get there.
    /// </summary>
    internal Action? _beforeMergeStream;
    /// <summary>
    /// Test hook: called inside <see cref="ImportSegment(string, string)"/> between reading the
    /// catalog and writing to it — the window in which one of the four writers that know nothing
    /// about <c>_importLock</c> can claim the key. Blocking in it lets the boot scan's
    /// registration land there on purpose, which is the only way to tell a compare-and-swap from
    /// a store: with a store the interleaving is silent, and a test that merely calls the two in
    /// sequence passes either way.
    /// </summary>
    internal Action? _beforeImportPublish;
    /// <summary>
    /// Test hook: called inside <see cref="ImportSegment(string, string)"/> after the entry is
    /// published and before <c>File.Move</c> lands the file, under <c>_importLock</c> — the window
    /// in which the catalog names a path that does not exist yet.
    /// </summary>
    internal Action? _afterImportPublish;
    /// <summary>
    /// Test hook: called by the header aggregation's cold scan just before it opens a segment
    /// from its catalog snapshot, so a test can remove the segment in between, as a merge does.
    /// </summary>
    internal Action<SegmentInfo>? _beforeHeaderSegmentOpen;
    /// <summary>
    /// Test hook: called by the header aggregation for a segment it could not read and that the
    /// catalog still served, before it takes a place under the warning cap — the window in which
    /// a delete can remove the segment after the catalog was checked.
    /// </summary>
    internal Action<SegmentInfo>? _beforeUnreadableSegmentWarned;
    /// <summary>Test hook: first id of the block reserved for the live WAL (see <see cref="WriteState.WalSegId"/>).</summary>
    internal ulong LiveWalSegmentId => _write.WalSegId;
    /// <summary>
    /// Test hook: scales the merged-file target. What determines how many files a bucket ends
    /// with is the RATIO of the bucket's payload to this, so dividing both by the same factor
    /// reproduces a stand's file geometry at a fraction of its volume.
    /// </summary>
    internal long _mergeTargetPayloadBytes = MergeTargetPayloadBytes;

    // ── Flush memory budgets (see the constructor for how these combine) ───────

    /// <summary>
    /// Managed bytes one in-flight index build retains per event. Measured on the flush
    /// path by <c>tests/Ameto.Perf/IndexBuildRetentionProbe</c>: 147 MB of accumulators +
    /// 28 MB of serialised blobs for a 130k-event trace-carrying tier ≈ 1.35 KB/event;
    /// 123 MB for the same tier without trace ids ≈ 0.95 KB/event. Budget the worse case.
    /// </summary>
    private const long IndexBuildBytesPerEvent = 1_400;

    /// <summary>
    /// Managed index-build state ONE MERGE holds per byte of its group payload budget.
    ///
    /// <para>A merge takes the same flush slot as an ingest flush (see <c>MergeToColdAsync</c>)
    /// but is not sized by the tier: its writer forecasts a group from the GROUP PAYLOAD BUDGET,
    /// and its source hint is every source segment's event count, so the tier-shaped figure above
    /// does not bound it. MEASURED (<c>tests/Ameto.Perf/IndexBuildPoolProbe</c>, prop-dense
    /// trace-carrying events): 30 MB held for 16 MB groups and 90 MB for 64 MB ones — about
    /// 1.5 bytes of build state per byte of group payload.</para>
    /// </summary>
    private const double MergeBuildBytesPerGroupByte = 1.5;

    /// <summary>
    /// Share of the managed build budget one merge's GROUP may be worth — a quarter, so that at
    /// the ratio above a merge build costs about a third of the budget and stays inside the slot
    /// the admission arithmetic priced. The default 64 MB group is the ceiling, so a host with
    /// room merges exactly as it always did.
    /// </summary>
    private const int MergeGroupBudgetDivisor = 4;

    /// <summary>Floor on the group payload budget: below this a group stops being worth its index sections.</summary>
    private const long MinGroupPayloadBudgetBytes = 8L * 1024 * 1024;

    /// <summary>
    /// Ceilings on managed index-build state and on native frozen-tier memory, derived once at
    /// construction from what this process may actually use — see <see cref="MemoryBudgets"/>.
    ///
    /// <para>On a host with room they are the constants they always were: 640 MB of concurrent
    /// builds, which at the default 64 MB tier (131,072 events ⇒ ~184 MB per build) yields a
    /// width of 3 — enough to stay ahead of ingest (a tier fills in ~0.9 s at 150k events/s, a
    /// build takes ~1.3 s, so 3 in flight clears one every ~0.44 s) — and 512 MB of frozen
    /// tiers. In a 512 MB container they become 115 MB (30 % of the GC's 384 MB heap limit —
    /// index builds are managed) and 128 MB (25 % of the container — frozen tiers are native and
    /// not under the heap limit), which is the difference between back-pressure and an OOM kill.
    /// Override the width with <c>HotTier.FlushConcurrency</c>
    /// when trading RAM for throughput deliberately.</para>
    /// </summary>
    private readonly MemoryBudgets _budgets;

    /// <summary>
    /// Concurrent index builds the constructor settled on (the <c>_flushConcurrency</c> count).
    /// Internal so a test can see the budgets actually reach the engine.
    /// </summary>
    internal int FlushWidth { get; }

    /// <summary>
    /// Frozen tiers allowed in flight at once (the <c>_flushSlots</c> count). Internal for the
    /// same reason as <see cref="FlushWidth"/>.
    /// </summary>
    internal int FlushSlots { get; }
    /// <summary>
    /// Window anchors that produced no usable merge batch — excluded so the sweep advances
    /// (reset on restart). Keyed by <see cref="SegmentKey"/> for the same reason the catalog is:
    /// an id alone names a segment on one node only, so skip-listing an unreadable local file
    /// would also have excluded a healthy peer replica that shared its id.
    /// </summary>
    internal readonly HashSet<SegmentKey> _mergeSkip = new();   // internal for the quarantine test, as LoadSegmentCatalog is for the scan tests

    /// <summary>
    /// Anchors deferred for the REMAINDER OF THE CURRENT PASS, cleared at the top of every
    /// pass. A bucket whose batch cannot assemble defers its anchor: the candidate set
    /// strictly shrinks within the pass and re-selection falls through to the next bucket —
    /// but a broken bucket still costs one of the pass's MergeWindowAttempts re-selections,
    /// so deferral alone protects the catalog only from FEWER broken buckets than that.
    /// <see cref="_mergeDeferStrikes"/> is the other half: a bucket deferred pass after pass
    /// escalates to the skip-list and stops costing attempts. Until it does, a gone-for-good
    /// file costs two Debug lines and one attempt per pass.
    /// </summary>
    private readonly HashSet<SegmentKey> _mergePassDeferred = new();

    /// <summary>
    /// Deferrals since this anchor last merged. (Not consecutive passes: a pass in which the
    /// bucket simply was not selected does not reset it — only a merge does. Anything that
    /// defers three times without ever merging is broken by any reading.) At
    /// <see cref="MergeDeferEscalationPasses"/> it escalates into <see cref="_mergeSkip"/>:
    /// per-pass deferral alone only moved the stall to the window threshold — each broken
    /// bucket still ate one of the <see cref="MergeWindowAttempts"/> re-selections every pass,
    /// so MergeWindowAttempts broken buckets ahead of a healthy one starved it forever. A
    /// blink (an import mid-rename, a briefly held file) clears in a pass or two and never
    /// reaches the limit; a bucket that is broken pass after pass stops costing attempts.
    /// Strikes are erased when the segment merges. The sets are not disjoint — a corrupt
    /// anchor of a two-segment bucket lands in the skip-list AND the pass deferral — and do
    /// not need to be: the skip-list simply wins.
    /// </summary>
    internal readonly Dictionary<SegmentKey, int> _mergeDeferStrikes = new();   // internal for the bookkeeping test, as _mergeSkip is

    /// <summary>Deferred passes before an anchor is treated as broken rather than blinking.</summary>
    private const int MergeDeferEscalationPasses = 3;

    /// <summary>
    /// Segment ids reserved per flushed tier: one per <see cref="LogLevel"/>, so a tier
    /// can be written as one segment PER LEVEL and the level's id is always
    /// <c>firstId + (byte)level</c>.
    ///
    /// <para>Level-pure segments are what makes retention exact. Expiry is
    /// <c>MaxTimestamp + Ttl(MinLevel)</c>, and MinLevel is the lowest severity VALUE in
    /// the segment — but TTL is not monotonic in that value (Debug 3 d sits below
    /// Information 90 d), so one Debug event in a mixed segment used to drag every Error
    /// beside it to a 3-day deadline. Measured on the sandbox stand before this change:
    /// 279 segments / 1116 MB inside 3 days, 10 segments / ~2 MB older — a clean cliff
    /// exactly where Debug's TTL falls.</para>
    /// </summary>
    private const int LevelSegmentSlots = 6;   // Verbose..Fatal

    // Cold-tier catalog (thread-safe).
    //
    // Keyed by (node, id), not by id. Segment ids are monotonic PER NODE and every node's
    // counter starts at 1, so once a peer replicates anything the two series overlap as a matter
    // of course. Keyed by id alone the second registration simply evicted the first —
    // the flush assigns unconditionally and so, then, did the import — and because the file NAMES
    // cannot collide (a replica is {node}-{id}.seg, a locally written segment
    // {node}-{id}-{minTs}-{maxTs}.seg) both files stayed on disk while only one was in here.
    //
    // All three consumers read this dictionary rather than the directory, so the evicted file
    // left every one of them at once: queries (GetSegments/ListSegments), retention (Values
    // where IsExpired) and the merge planner (foreach over Values). It was therefore never
    // served, never expired and never compacted — disk held for the life of the install, with
    // nothing logged. LoadSegmentCatalog re-ran the same collision on every restart in
    // directory-enumeration order, so which of the two survived could change from boot to boot.
    private readonly SegmentCatalog _segments = new();
    /// <summary>Background catalog scan started by the ctor (kept to observe faults).</summary>
    private readonly Task _catalogLoad;

    /// <summary>
    /// Completes when the constructor's catalog scan has finished publishing every segment it
    /// found. Until it does, <see cref="ListSegments"/> reports whatever has been added SO FAR —
    /// the scan calls TryAdd one file at a time — so a count taken before this task completes is
    /// a snapshot of a partial catalog, not of the directory.
    ///
    /// <para>Exposed because the absence of it was the whole bug. Tests reached for the only
    /// signal there was: poll until ListSegments() is non-empty, then assert on the total. On an
    /// idle machine the enumeration of a handful of small files finishes inside the first 25 ms
    /// tick and the two are indistinguishable; on a loaded CI runner they are not, and the
    /// assertion sees four, six, eight of ten. Others called LoadSegmentCatalog() by hand to
    /// "drive the scan to completion", which does not replace the background one — it runs a
    /// second scan alongside it, over the same directory.</para>
    ///
    /// <para>Faults are not swallowed: awaiting a scan that threw rethrows here, which is the
    /// right answer for a test asking whether the catalog is ready.</para>
    /// </summary>
    internal Task CatalogLoaded => _catalogLoad;

    // Hot tiers that have been frozen but whose cold-tier segment file is still
    // being written (or has just been registered but we haven't released the
    // reference yet). Queries must read from these to avoid a visibility gap
    // during flush. Mutated under <see cref="_frozenLock"/>.
    private readonly List<(HotTierSegment Tier, ulong SegId)> _frozenHot = new();
    private readonly object                                   _frozenLock = new();

    // Frozen hot tiers whose cold segment has been written and which are no longer in
    // _frozenHot, but which an in-flight query may still hold a reference to. Disposed
    // once _activeReaders hits zero (see RetireHotTier / DrainRetired). A list (not a
    // single slot) because parallel flushes can retire several tiers concurrently.
    private readonly List<HotTierSegment>                     _retired    = new();
    private readonly object                                   _retireLock = new();

    // Number of HotTierReaderSnapshot instances currently in-flight. Incremented
    // by OpenHotTierReader, decremented by snapshot.Dispose(). Used to eagerly
    // release retired hot tiers when no query could possibly observe them.
    private int _activeReaders;

    // Monotonic segment-id allocator. Every id a segment file can ever carry comes from
    // here — flush blocks, merges, WAL-recovery segments — so an id is never reused.
    //
    // NEVER touch this field directly. Go through AllocateSegmentId /
    // AllocateSegmentIdBlock / AdvanceSegmentIdFloor, which hold _segIdLock.
    private          ulong                                _nextSegmentId = 1;

    /// <summary>
    /// Guards <see cref="_nextSegmentId"/>. Its own lock rather than <c>_flushLock</c> because the
    /// writers do not share a thread model: flush and merge reserve ids while holding the async
    /// flush lock, but <see cref="ImportSegment"/> runs synchronously on whatever thread the
    /// replication endpoint handed it, and making that path wait on a <c>SemaphoreSlim</c> would
    /// either block a request thread behind a swap or force the import path to become async.
    ///
    /// <para>Every critical section under it is a read, an add and a store — no I/O, no await, so
    /// the lock is never held across a suspension point and the ordering (_flushLock outside,
    /// _segIdLock inside) is the only one that exists.</para>
    /// </summary>
    private readonly System.Threading.Lock                _segIdLock = new();

    /// <summary>
    /// Serialises <see cref="ImportSegment(string, string)"/> against other imports AND against
    /// <see cref="DeleteSegmentAsync(SegmentKey, CancellationToken)"/>: two peers pushing
    /// different segments under one key would otherwise both find it free and both rename onto
    /// the same path, and a delete landing between an import's publish and its rename would
    /// orphan the file the rename lands. On the collision path the section also reads the
    /// incumbent's header and footer (never its blocks) to judge it — so the hold is bounded by
    /// metadata reads, not by file size.
    ///
    /// <para>Taken by <c>ImportSegment</c> and by <c>DeleteSegmentAsync</c> — a delete
    /// landing inside an import's publish-to-rename window would otherwise orphan the file the
    /// rename lands — and it still excludes none of the other writers into the catalog: flush
    /// publication holds <c>_frozenLock</c>, merge publication and WAL recovery hold nothing,
    /// and the boot scan runs on a background task while the replication endpoint is already
    /// serving. That is why the registration itself is a compare-and-swap
    /// rather than a store: holding this lock says nothing about whether the value read at the top
    /// of the section is still there at the bottom of it.</para>
    ///
    /// <para>Nothing is taken under it — the allocator floor is raised before it is entered — so
    /// it participates in no ordering. The section is a dictionary exchange and a rename; the
    /// segment's contents were read before it, and the path is only ever reached once per
    /// replicated segment.</para>
    /// </summary>
    private readonly System.Threading.Lock                _importLock = new();

    // The live WAL's reserved id block lives in WriteState.WalSegId (see its doc) so it
    // swaps atomically with the WAL it names.

    // Time-sortable event id generator (Snowflake layout). Assigns EventId.RawValue
    // on the write path so sorting by Id ≡ sorting by ingest time.
    private readonly EventIdGenerator                    _idGen;

    // String intern pool shared with ingestion
    public StringInternPool TemplatePool { get; } = new();

    public StorageEngine(IOptions<ServerOptions> options, RetentionStore retentionStore, ILogger<StorageEngine> logger)
        : this(options, retentionStore, logger, MemoryBudgets.Current())
    {
    }

    /// <summary>
    /// Takes the memory budgets instead of reading them from this process, so a test can build
    /// the engine a 512 MB container would get on a machine that is not one. Not public: the DI
    /// container only sees the constructor above.
    /// </summary>
    internal StorageEngine(
        IOptions<ServerOptions> options, RetentionStore retentionStore, ILogger<StorageEngine> logger,
        MemoryBudgets budgets)
    {
        _options        = options.Value;
        _retentionStore = retentionStore;
        _logger         = logger;
        _budgets        = budgets;
        // ── Flush RAM budgets ────────────────────────────────────────────────────
        // A flush costs memory in two separate places, and each needs its own bound:
        //
        //   managed — the index build (inverted + trigram + bloom accumulators, then the
        //             serialised blobs). Measured at ~1.15 KB/event for trace-carrying
        //             events, ~0.8 KB/event without trace ids
        //             (tests/Ameto.Perf/IndexBuildRetentionProbe). One of these is live
        //             per CONCURRENT flush, so it scales with _flushConcurrency.
        //   native  — the frozen tier itself, held until its cold segment is written.
        //             Scales with _flushSlots.
        //
        // Sizing the width off Environment.ProcessorCount alone (the old
        // ProcessorCount / 2, capped 8) ignored the managed half entirely: on a 20-core
        // host that is 8 concurrent builds, i.e. 8 × ~150 MB of index state on top of the
        // frozen tiers — the observed ~1 GB sawtooth, at ~40 % CPU for the length of the
        // burst. The width is now the smaller of the core-based figure and what the
        // managed budget affords.
        long tierFootprint  = HotTierSegment.NativeBytesFor(Math.Max(1, _options.HotTier.MaxSizeBytes));
        int  eventCapacity  = HotTierSegment.EventCapacityFor(Math.Max(1, _options.HotTier.MaxSizeBytes));
        long perFlushManaged = Math.Max(1L, (long)eventCapacity * IndexBuildBytesPerEvent);

        // The OTHER workload this semaphore admits. A merge runs the same index build through the
        // same slot, but its size comes from the group payload budget rather than from the tier:
        // at the 64 MB default that is a heavier build than the stand's whole 16 MB tier, so the
        // width — which exists to bound concurrent builds — was computed for the lighter of the
        // two, and the ceiling logged below was not the ceiling enforced. Both halves are fixed
        // here: the group budget is scaled by what the managed budget affords, and the width is
        // taken from the HEAVIER build.
        _groupPayloadBudgetBytes = Math.Clamp(
            _budgets.ManagedBuildBytes / MergeGroupBudgetDivisor,
            MinGroupPayloadBudgetBytes,
            SegmentWriter.DefaultGroupPayloadBudgetBytes);
        long perMergeManaged = Math.Max(1L, (long)(_groupPayloadBudgetBytes * MergeBuildBytesPerGroupByte));
        long perBuildManaged = Math.Max(perFlushManaged, perMergeManaged);

        int widthByMemory = (int)Math.Clamp(_budgets.ManagedBuildBytes / perBuildManaged, 1, 64);
        int flushWidth = _options.HotTier.FlushConcurrency > 0
            ? Math.Min(_options.HotTier.FlushConcurrency, 64)
            : Math.Clamp(Math.Min(Environment.ProcessorCount / 2, widthByMemory), 1, 8);
        _flushConcurrency = new SemaphoreSlim(flushWidth);

        // In-flight tier cap: bound the frozen-tier backlog by REAL native footprint.
        // The previous 1.4 × MaxSizeBytes estimate under-counted by up to 17x on small
        // events, so the "1 GB" budget it computed could hold multiple GB in practice.
        // Floored at the flush width so every concurrent flush can still hold a slot.
        int flushSlots = Math.Clamp((int)(_budgets.NativeTierBytes / tierFootprint), flushWidth, 64);
        _flushSlots = new SemaphoreSlim(flushSlots, flushSlots);
        FlushWidth  = flushWidth;
        FlushSlots  = flushSlots;

        // Report the ceilings these settings actually produce, not just the inputs — an
        // explicit HotTier.FlushConcurrency override raises them, and that should be
        // visible in the journal rather than inferred.
        long managedCeiling = (long)flushWidth * perBuildManaged;
        long nativeCeiling  = (long)flushSlots * tierFootprint;

        _logger.LogInformation(
            "Flush budgets: width={Width} (×{PerBuild} MB managed = {ManagedCeiling} MB; " +
            "a flush holds {PerFlush} MB, a merge {PerMerge} MB in {GroupBudget} MB groups), " +
            "slots={Slots} (×{Tier} MB native = {NativeCeiling} MB), tier={Events} events / {Payload} MB payload; " +
            "derived from a {ManagedLimit} MB managed-heap limit and {PhysicalLimit} MB physical: " +
            "managed≤{ManagedBudget} MB, native≤{NativeBudget} MB ({Source}); " +
            "index cache≤{CacheBudget} MB ({CacheSource}), native≤{CacheNativeBudget} MB, idle evict {IdleEvict}",
            flushWidth, perBuildManaged / 1048576, managedCeiling / 1048576,
            perFlushManaged / 1048576, perMergeManaged / 1048576, _groupPayloadBudgetBytes / 1048576,
            flushSlots, tierFootprint / 1048576, nativeCeiling / 1048576,
            eventCapacity, _options.HotTier.MaxSizeBytes / 1048576,
            _budgets.ManagedLimitBytes / 1048576,
            _budgets.PhysicalLimitBytes / 1048576,
            _budgets.ManagedBuildBytes / 1048576,
            _budgets.NativeTierBytes / 1048576,
            _budgets.IsConstrained ? "host-constrained" : "fixed ceilings",
            // The EFFECTIVE figure, not the derivation: Query.IndexCacheBytes overrides it, and
            // this line is the only place an operator is told what the cache will hold. Reporting
            // the derivation meant a stand configured to 48 MB was told 57, and a big host
            // configured to 4 GB was told 256 — under-reporting, which is the direction that
            // ends in an OOM kill. /api/diagnostics was given exactly this treatment in this same
            // round (indexCacheBudgetBytes reports what the cache was BUILT with); the startup
            // line was not. Resolving SegmentIndexCache itself is not available here: storage is
            // registered before the query services that construct it.
            _options.Query.EffectiveIndexCacheBytes / 1048576,
            _options.Query.IndexCacheBytes.HasValue ? "configured" : "derived",
            // The cache's OTHER ceiling, on the same line for the same reason: an operator can now
            // move it — a configured budget raises it, clamped to the host's share — and these are
            // the bytes the GC cannot see. Reading it off a running server's /api/diagnostics is
            // the harder road on exactly the constrained hosts this line was added for.
            _options.Query.EffectiveIndexCacheNativeBytes / 1048576,
            _options.Query.IndexCacheIdleEvict);

        // Both clamps are floored so at least one flush can always proceed. That floor
        // WINS over the budget: at a large MaxSizeBytes a single tier no longer fits, and
        // the engine quietly runs above the ceiling rather than refusing to start. The
        // budget is a target, not a guarantee — say so instead of letting the line above
        // read like one.
        if (perBuildManaged > _budgets.ManagedBuildBytes || tierFootprint > _budgets.NativeTierBytes)
            _logger.LogWarning(
                "A single build of a {Payload} MB tier ({PerFlush} MB managed + {Tier} MB native) does not fit " +
                "the flush budget ({ManagedBudget} MB managed / {NativeBudget} MB native). One flush must always " +
                "be allowed to run, so these budgets cannot be honoured at this tier size — peak RAM will exceed " +
                "them. Lower HotTier.MaxSizeBytes to bring the peak down.",
                _options.HotTier.MaxSizeBytes / 1048576, perBuildManaged / 1048576, tierFootprint / 1048576,
                _budgets.ManagedBuildBytes / 1048576, _budgets.NativeTierBytes / 1048576);
        _idGen    = new EventIdGenerator(_options.NodeId);
        _dataDir  = _options.DataDirectory;
        _walDir   = Path.Combine(_dataDir, "wal");
        _segDir   = Path.Combine(_dataDir, "segments");

        Directory.CreateDirectory(_walDir);
        Directory.CreateDirectory(_segDir);

        // Surface intern-pool saturation: past it, every event stores its own template
        // string instead of a pool index, so per-event memory rises permanently.
        TemplatePool.PoolExhausted += size => _logger.LogWarning(
            "Message-template intern pool exhausted at {Size} entries. Templates and service " +
            "names are no longer de-duplicated — per-event memory will rise. This usually means " +
            "message templates are being built by interpolation (a distinct template per event) " +
            "rather than passed as structured parameters.", size);

        _write = new WriteState(CreateHotTier(), null, 0);
        // The next segment id MUST be known before any flush, but it lives in the
        // file NAMES ({node}-{segId}-{minTs}-{maxTs}.seg) — a cheap directory
        // listing, no file opens. The expensive part (opening every segment to
        // read its catalog entry) runs in the background: ingest and the HTTP
        // endpoints come up immediately; cold segments become queryable as the
        // scan progresses (the catalog is a ConcurrentDictionary keyed by id, so
        // concurrent flush registrations are safe).
        InitNextSegmentIdFromFileNames();
        // Leftover temp files are swept HERE, not in the background catalog scan, because the
        // scan runs with everything else already up. Two live writers produce files this sweep's
        // masks match: the replication endpoint staging a body under {node}-{id}.{nonce}.seg.tmp
        // -- written by a request thread holding no lock -- and the WAL replay two lines down. A
        // background sweep deleted them out from under their writers; on Linux the unlink
        // succeeds beneath the open handle and a healthy push then fails on a file that no
        // longer has a name. In the constructor there is nothing to race: no endpoint is bound,
        // no recovery has started, no merge can be running. Same reasoning and same shape as the
        // metrics engine's constructor sweep.
        SweepLeftoverTempFiles();
        // Quarantined segments are invisible to everything else on purpose -- the sweep walks
        // *.seg.tmp, the scan and the planner walk *.seg, retention walks the catalog -- so
        // without this line they would be mentioned exactly once, at the moment of quarantine,
        // and an operator a year later would see disk usage disagree with retention with no
        // hint as to why. One Warning per start: nothing is deleted, the silence is.
        var corrupt = Directory.GetFiles(_segDir, "*.seg.corrupt");
        if (corrupt.Length > 0)
            _logger.LogWarning(
                "Quarantined segments present: {Count} file(s), {Bytes:N0} bytes in {Dir} — " +
                "unreadable at some earlier start, kept for an operator to inspect or remove.",
                corrupt.Length, corrupt.Sum(static f => new FileInfo(f).Length), _segDir);
        _catalogLoad = Task.Run(LoadSegmentCatalog);
        ReplayOrphanedWals();
        var (bootWal, bootSegId) = OpenWalCore();
        _write = new WriteState(_write.Hot, bootWal, bootSegId);

        // Age-based flush loop
        _flushLoop = RunFlushLoopAsync(_cts.Token);
        // Periodic WAL msync — the durability contract for acknowledged-but-unflushed events
        _walFlushLoop = RunWalFlushLoopAsync(_cts.Token);
        // Cold-tier maintenance: interrupted-merge recovery + small-segment merges
        _maintenanceLoop = RunColdMaintenanceLoopAsync(_cts.Token);
    }

    /// <summary>
    /// Cold-tier maintenance: finish any interrupted merge, then MERGE small segments into
    /// large ones. A long-running server accumulates thousands of ~100 KB age-flush segments
    /// and per-file index/catalog overhead dwarfs their payload. Paced by a pause between
    /// batches; the flush path stays untouched, so ingest is unaffected.
    ///
    /// <para>This loop also ran a one-shot LZ4-HC sweep over cold segments, removed once the
    /// merge started writing <see cref="SegmentCompression.High"/> itself. The sweep could
    /// only rewrite v4/v5 envelopes, so on a v7 catalog it walked every segment on every tick
    /// and rewrote none — see the commit that deleted it for why a v7-capable transform is
    /// not worth having.</para>
    /// </summary>
    private async Task RunColdMaintenanceLoopAsync(CancellationToken ct)
    {
        // Wait for the catalog ENUMERATION, not for a guess at how long it takes. It opens every
        // .seg with computeUncompressedBytes: true, which is slowest in exactly the
        // thousands-of-small-segments case compaction exists for — so a fixed delay can expire
        // mid-scan, and a source this sweep deletes then gets re-registered by the enumeration
        // still running behind it, leaving a catalog entry pointing at a file that is gone.
        // (Not data loss: RecoverInterruptedMerges does run before the enumeration. But the
        // resurrected entry becomes a merge candidate, fails to open and is skip-listed until
        // restart.)
        try { await _catalogLoad.WaitAsync(ct); }
        catch (OperationCanceledException) { return; }
        catch (Exception ex) { _logger.LogWarning(ex, "Segment catalog load faulted — maintenance continues"); }

        try { await Task.Delay(TimeSpan.FromMinutes(3), ct); } // let startup settle
        catch (OperationCanceledException) { return; }

        while (!ct.IsCancellationRequested)
        {
            // One batch per iteration, short pause while a backlog exists.
            bool merged;
            try { merged = await RunColdMaintenancePassAsync(ct); }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _logger.LogError(ex, "Segment merge pass failed"); merged = false; }
            if (merged)
            {
                // A merge briefly holds the batch, its native tier copy and the
                // index builders. Hand that back to the OS instead of letting the
                // allocator sit on it until the next burst (RSS otherwise ratchets
                // up across passes and never comes down on an idle server).
                //
                // Gated on a merge having HAPPENED, which is what keeps an idle server from
                // paying for this. The removed HC sweep had its own release behind a bytes
                // floor for the same reason: its idle pass rewrote the one or two tiny
                // segments flushed since the last tick ("saved 0.0 MB") and a day's log
                // showed that shape paying a blocking compacting gen2 plus a working-set
                // dump six times an hour, around the clock, the working set sawing between
                // ~50 and ~130 MB. With the sweep gone an idle tick does no work at all, so
                // there is nothing to release and no floor to test.
                ReleaseMaintenanceMemory();
                try { await Task.Delay(TimeSpan.FromSeconds(15), ct); }
                catch (OperationCanceledException) { break; }
                continue;
            }

            try { await Task.Delay(TimeSpan.FromSeconds(600), ct); }
            catch (OperationCanceledException) { break; }
        }
    }

    /// <summary>
    /// One pass of <see cref="RunColdMaintenanceLoopAsync"/>: the two sweeps, then one merge batch.
    /// True when a batch was merged. The merge's exceptions, cancellation included, reach the
    /// loop; the sweeps log their own and never stop the merge behind them.
    ///
    /// <para>Internal so a test can run a pass without the loop's three-minute settle. Past the
    /// background retry's window the deferred-delete sweep below and retention's are the only
    /// retries a parked file gets, so dropping either must fail a test and not only a stand.</para>
    /// </summary>
    internal Task<bool> RunColdMaintenancePassAsync(CancellationToken ct)
    {
        // Finish any merge whose source deletion was blocked by an open reader
        // (the manifest survives until every source file is gone).
        try { RecoverInterruptedMerges(); }
        catch (Exception ex) { _logger.LogWarning(ex, "Merge recovery sweep failed"); }

        // Segment files whose delete outlasted the background retry (see DeleteSegmentAsync).
        try { RetryPendingSegmentDeletes(); }
        catch (Exception ex) { _logger.LogWarning(ex, "Deferred segment delete sweep failed"); }

        // Handed back, not awaited: the merge is already async, and a second state machine around
        // it would add only its own allocation.
        return TryMergeSmallSegmentsOnceAsync(ct);
    }

    /// <summary>
    /// Returns the memory a maintenance burst just used. The TRIGGER is background,
    /// but the pause is not: a blocking compacting gen2 stops every thread, ingest
    /// and query included, so this is visible to clients no matter which thread asks
    /// for it. Routed through <see cref="AggressiveGcGate"/> so bursts from the other
    /// maintenance paths can't line up several such pauses back to back; skipping is
    /// fine — the next natural gen2 reclaims the garbage either way, aggressive
    /// collection only accelerates handing pages back to the OS.
    /// </summary>
    private static void ReleaseMaintenanceMemory()
    {
        if (AggressiveGcGate.TryCollect(TimeSpan.FromMinutes(2)))
            WorkingSetTrimmer.TryTrim();
    }

    // ── ISegmentProvider ──────────────────────────────────────────────────────

    /// <summary>
    /// Newest MaxTs first. Off the catalog's cached sorted snapshot (see
    /// <see cref="SegmentCatalog"/>): the walk + LINQ sort + list per call used to be
    /// paid by every query and every live-tail poll for an answer that changes only
    /// when a segment is added or removed.
    /// </summary>
    public IReadOnlyList<SegmentInfo> GetSegments(DateTimeOffset? from, DateTimeOffset? to)
        => _segments.GetOverlapping(from?.UtcTicks ?? long.MinValue, to?.UtcTicks ?? long.MaxValue);

    /// <summary>
    /// Total native bytes held by the hot tier right now: the live segment plus
    /// any frozen segments still being drained to cold storage during a flush
    /// overlap. This is process RSS that lives outside the GC heap.
    /// </summary>
    public long HotTierAllocatedBytes
    {
        get
        {
            lock (_frozenLock)
            {
                long total = _write.Hot.AllocatedBytes;
                for (int i = 0; i < _frozenHot.Count; i++)
                    total += _frozenHot[i].Tier.AllocatedBytes;
                return total;
            }
        }
    }

    public IHotTierReader OpenHotTierReader()
    {
        var (current, frozen, covered) = SnapshotTiers();
        return new HotTierReaderSnapshot(current, frozen, covered, TemplatePool, this);
    }

    /// <summary>
    /// Captures the current hot tier plus any frozen-but-not-yet-released tiers, along with the
    /// set of cold segment ids those frozen tiers still cover (to avoid double counting during a
    /// flush overlap). Increments the active-reader count so a concurrent flush cannot free a
    /// captured tier's native memory while it is being scanned — callers <b>must</b> pair this
    /// with exactly one <see cref="OnReaderDisposed"/> when finished.
    /// </summary>
    private (HotTierSegment Current, HotTierSegment[] Frozen, IReadOnlySet<SegmentKey> Covered) SnapshotTiers()
    {
        HotTierSegment    current;
        HotTierSegment[]  frozen;
        IReadOnlySet<SegmentKey> covered;
        lock (_frozenLock)
        {
            current = _write.Hot;
            if (_frozenHot.Count == 0)
            {
                // The common case — no flush in progress — covers nothing, and every poll
                // took this branch and allocated an empty set to say so.
                frozen  = Array.Empty<HotTierSegment>();
                covered = EmptyCoveredSet.Instance;
            }
            else
            {
                frozen  = new HotTierSegment[_frozenHot.Count];
                var set = new HashSet<SegmentKey>(_frozenHot.Count * LevelSegmentSlots);
                covered = set;
                for (int i = 0; i < _frozenHot.Count; i++)
                {
                    frozen[i] = _frozenHot[i].Tier;
                    // The tier flushes to one segment per level, so every id in its
                    // reserved block is covered — otherwise a query would serve the
                    // already-registered per-level segments AND the still-frozen tier.
                    ulong first = _frozenHot[i].SegId;
                    for (int s = 0; s < LevelSegmentSlots; s++)
                        set.Add(new SegmentKey(_options.NodeId, new SegmentId(first + (ulong)s)));
                }
            }
            Interlocked.Increment(ref _activeReaders);
        }
        return (current, frozen, covered);
    }

    /// <summary>
    /// Near-zero-allocation log-volume aggregation: buckets <c>(bucket, service, level)</c> event
    /// counts by scanning event <b>headers</b> across the hot tier and cold-tier segments in
    /// <c>[fromUtc, toUtc]</c>, never materialising a <see cref="LogEvent"/>. Backs
    /// <c>GET /api/events/counts</c>. Bucketing parameters are supplied by the caller so the axis
    /// matches the endpoint's column-cap logic.
    ///
    /// <para>A cold segment that throws while being read is skipped rather than failing the
    /// whole aggregate, and counted in <see cref="LogVolumeCounts.SkippedSegments"/> when the
    /// catalog still serves it: when that is non-zero every total is a floor, and a caller that
    /// reports a count as a fact must say so. A segment that left the catalog while the scan ran
    /// is a race, not damage, and is never counted there. A retention delete is not counted at
    /// all, because its events are gone; a merge's source is counted in
    /// <see cref="LogVolumeCounts.MergedAwaySegments"/>, because its events are in an output this
    /// scan's snapshot does not list. See <see cref="OnHeaderSegmentUnreadable"/>.</para>
    /// </summary>
    /// <param name="totalsOnly">
    /// Opt-in shortcut for a caller that wants ONE number (the alert evaluator, which runs this
    /// every 15 s per rule over a window that can span hundreds of segments). A cold segment is
    /// immutable and its catalog <c>EventCount</c> is exact, so a segment lying entirely inside
    /// the window contributes that count with no mmap and no LZ4 decode at all — for a 24 h
    /// window only the two boundary segments and the hot tier are still read. In exchange
    /// <see cref="LogVolumeCounts.Services"/> and <see cref="LogVolumeCounts.Levels"/> stop
    /// summing to <see cref="LogVolumeCounts.Total"/>, which is why it is off by default and
    /// ignored whenever a <paramref name="serviceFilter"/> is set — the catalog cannot say how
    /// many of a segment's events belong to one service, so the shortcut would over-count.
    /// </param>
    public async ValueTask<LogVolumeCounts> AggregateLogVolumeAsync(
        DateTimeOffset fromUtc, DateTimeOffset toUtc,
        long minBucket, int bucketSeconds, int nBuckets,
        string? serviceFilter, CancellationToken ct = default, bool totalsOnly = false)
    {
        long fromTicks = fromUtc.UtcTicks;
        long toTicks   = toUtc.UtcTicks;

        // Taken BEFORE the segment snapshot below, so a merge that removes a segment the snapshot
        // lists has recorded it at or after this mark; MayHaveBeenMergedAway relies on that when
        // the record has had to evict.
        long mergedAwayMark = Interlocked.Read(ref _mergedAwayRecorded);

        var agg = new LogVolumeAggregator(
            fromTicks, toTicks, minBucket, bucketSeconds, nBuckets, serviceFilter, TemplatePool);

        // Hold a reader snapshot for the whole scan so frozen tiers stay mapped.
        var (current, frozen, covered) = SnapshotTiers();
        try
        {
            // Hot tier: direct header walk (frozen tiers hold the older events, current the newest).
            for (int i = 0; i < frozen.Length; i++)
                frozen[i].AggregateInto(agg, fromTicks, toTicks);
            current.AggregateInto(agg, fromTicks, toTicks);

            // Cold tier: segments overlapping the window, minus those still covered by frozen hot
            // tiers. CPU-bound (mmap reads + LZ4 decode), so it runs on the thread pool and IN
            // PARALLEL across segments — serially, a wide dashboard window over hundreds of
            // segments was the whole latency of /api/events/counts. Each worker feeds its OWN
            // aggregator (the per-event path stays lock-free) and folds it into the shared one
            // once, at worker exit. Bounded below the prefilter's parallelism: this backs a
            // 10-second dashboard poll and must not saturate the box.
            var segInfos = GetSegments(fromUtc, toUtc);
            if (segInfos.Count > 0)
            {
                string? svcFilter = serviceFilter;
                // Whole-segment counting is only sound when nothing per-service or per-level is
                // read back out — see the totalsOnly parameter. With a service filter the
                // catalog cannot answer at all, so the shortcut turns itself off.
                bool wholeSegments = totalsOnly && svcFilter is null;
                // The snapshot's keys as a set, built only if a worker meets a merged-away source
                // (see SnapshotLists). Declared beside the other captured locals so it shares
                // their closure rather than adding one.
                HashSet<SegmentKey>? snapshotKeys = null;
                await Task.Run(() =>
                {
                    int degree = Math.Clamp(Environment.ProcessorCount / 2, 2, 8);
                    Parallel.ForEach(
                        segInfos,
                        new ParallelOptions { MaxDegreeOfParallelism = degree, CancellationToken = ct },
                        localInit: () => new LogVolumeAggregator(
                            fromTicks, toTicks, minBucket, bucketSeconds, nBuckets, svcFilter, TemplatePool),
                        body: (info, _, local) =>
                        {
                            if (covered.Contains(SegmentKey.Of(info))) return local;
                            if (info.MaxTimestampTicks < fromTicks || info.MinTimestampTicks > toTicks) return local;

                            // Entirely inside the window: every event in it is in the answer, and
                            // the catalog already knows how many there are. No mmap, no block
                            // index read, no LZ4 decode — the whole cost of this segment is one
                            // comparison. Boundary segments still have to be decoded, because
                            // only the headers say which of their events fall in the window.
                            if (wholeSegments &&
                                info.MinTimestampTicks >= fromTicks && info.MaxTimestampTicks <= toTicks)
                            {
                                local.AddWholeSegment(info.EventCount);
                                return local;
                            }

                            try
                            {
                                _beforeHeaderSegmentOpen?.Invoke(info);
                                using var reader = OpenForHeaderScan(info);
                                reader.AggregateHeaders(local, fromTicks, toTicks);
                            }
                            catch (Exception ex)
                            {
                                // Never lose the whole aggregate over one bad/racing segment file —
                                // but never HIDE a bad one either. Whatever the segment yielded
                                // before the throw stays in; it is real data.
                                OnHeaderSegmentUnreadable(ex, info, local, mergedAwayMark, segInfos, ref snapshotKeys);
                            }
                            return local;
                        },
                        localFinally: local => { lock (agg) agg.MergeFrom(local); });
                }, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            OnReaderDisposed();
        }

        return agg.Build();
    }

    /// <summary>
    /// Opens a cold segment for the header scan, waiting out an import that has published the
    /// entry but not yet landed its file.
    ///
    /// <para><see cref="ImportSegment(string, string)"/> publishes the catalog entry BEFORE its
    /// <c>File.Move</c> (see the comment there for why that order), and holds <c>_importLock</c>
    /// across both. A scan whose snapshot caught the entry in that window finds no file. Waiting
    /// for the lock waits the rename out, and one more open then reads the segment that was
    /// always going to be there. If that open fails too, the caller decides what it means: the
    /// import may have withdrawn its entry, or a delete (which takes the same lock) may have
    /// removed it in the meantime.</para>
    ///
    /// <para>Only a missing file waits. A torn file is not something an import can be in the
    /// middle of fixing, and every other caller that removes a file removes its entry first.</para>
    /// </summary>
    private SegmentReader OpenForHeaderScan(SegmentInfo info)
    {
        try { return SegmentReader.Open(info.FilePath); }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            lock (_importLock) { }   // wait out an import between its publish and its move
            return SegmentReader.Open(info.FilePath);
        }
    }

    /// <summary>
    /// Sorts a failed header read into a race or a real unreadable segment, and the race into
    /// the two kinds that mean opposite things for the count.
    ///
    /// <para><b>A race</b>: the catalog no longer holds this segment under its key and path. The
    /// scan works from a snapshot, and a merge publishes its output and then deletes its sources,
    /// and retention deletes expired ones, while a poll is still walking that snapshot. Nothing
    /// is damaged, so neither kind is a skip and neither warns: counting every race as a skip made
    /// every merge that overlapped a histogram poll warn about a corruption that did not exist,
    /// and the once-per-key warning could not help, since each merge removes new keys.</para>
    ///
    /// <para>But the kinds differ in where the events went. <b>Retention</b> removed them from the
    /// store: leaving them out is the right answer, so the race is only logged at Debug.
    /// <b>A merge</b> moved them into its output. When this snapshot does not list that output
    /// the total is low; it is counted in <see cref="LogVolumeCounts.MergedAwaySegments"/> for a
    /// caller that presents the total as a fact to call it a floor. Silencing that case too made
    /// <c>select count(*)</c> over a wide window report a low number as complete whenever a merge
    /// landed under it. When the snapshot DOES list the output — taken after the merge published
    /// and before it deleted this source — the scan reads the events there, so the race is as
    /// silent as retention's: counted, it turned an exact total into a floor.</para>
    ///
    /// <para>Told apart, rather than answered by scanning the window again with a fresh snapshot.
    /// A rescan doubles the decode cost of exactly the wide windows a merge is most likely to land
    /// under. It can race too — while a backlog lasts the maintenance loop merges again every
    /// 15 s — so it would still need this verdict. And the next poll, or a rerun, reads the merged
    /// output anyway.</para>
    ///
    /// <para><b>Unreadable</b>: the catalog still serves it — checked again once its place under
    /// the warning cap is taken, since a delete can land in between. The skip is counted, so a
    /// caller that presents the total as a fact can say it is a floor, and named once at
    /// Warning.</para>
    /// </summary>
    private void OnHeaderSegmentUnreadable(
        Exception ex, SegmentInfo info, LogVolumeAggregator local,
        long mergedAwayMark, IReadOnlyList<SegmentInfo> snapshot, ref HashSet<SegmentKey>? snapshotKeys)
    {
        var key = SegmentKey.Of(info);

        // Counted as a skip only AFTER LogUnreadableSegment has taken the segment's place under
        // the warning cap and found it still served. A delete can land between this catalog
        // check and that place; counted up front, a segment retention removed in that window was
        // a skip, and the partial reason pointed at a Warning the take-back never let be written,
        // for a count that was exact. It is the same race the check here catches, one step
        // later, so it is sorted the same way.
        if (CatalogServes(key, info) && LogUnreadableSegment(ex, info, key))
        {
            local.AddSkippedSegment();
            return;
        }

        // The catalog first and the record second, never the other way round: the merge
        // records a key before it deletes the segment, so an entry seen gone by a merge is
        // always already recorded. Read in the opposite order, a merge landing between the
        // two reads would be found in neither and pass for retention.
        if (MayHaveBeenMergedAway(key, mergedAwayMark, snapshot, ref snapshotKeys))
        {
            local.AddMergedAwaySegment();
            _logger.LogDebug(ex,
                "Header aggregation skipped segment {NodeId}-{Id}: a merge rewrote it while the scan ran, so counts over its window are a floor",
                info.NodeId, info.Id);
        }
        else
            _logger.LogDebug(ex,
                "Header aggregation skipped segment {NodeId}-{Id}: it left the catalog while the scan ran (retention, delete, or a merge whose output the scan reads)",
                info.NodeId, info.Id);
    }

    /// <summary>Whether the catalog still holds this segment under its key AND at its path.</summary>
    private bool CatalogServes(SegmentKey key, SegmentInfo info) =>
        _segments.TryGetValue(key, out var current)
        && string.Equals(current.FilePath, info.FilePath, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Records that a merge is about to delete <paramref name="key"/>, whose events its published
    /// output <paramref name="output"/> now holds. Called for each source immediately before its
    /// delete, so the record is in place before the catalog entry goes (see
    /// <see cref="_mergedAwaySegments"/>).
    /// </summary>
    private void RecordMergedAwaySegment(SegmentKey key, SegmentKey output)
    {
        lock (_mergedAwayGate)
        {
            var  ring = _mergedAwayRing ??= new SegmentKey[Math.Max(1, MergedAwaySegmentCap)];
            long n    = _mergedAwayRecorded + 1;
            int  slot = (int)((n - 1) % ring.Length);

            if (n > ring.Length)
            {
                // The slot holds the oldest record kept. Its key leaves the lookup only if no
                // later record names it again, and the eviction point moves either way: what a
                // scan needs to know is how far eviction has reached, not whether this key
                // survived it.
                long evicted = n - ring.Length;
                var  old     = ring[slot];
                if (_mergedAwaySegments.TryGetValue(old, out var at) && at.Number == evicted)
                    _mergedAwaySegments.Remove(old);
                _mergedAwayEvictedThrough = evicted;
            }

            ring[slot] = key;
            _mergedAwaySegments[key] = new MergedAwayRecord(n, output);
            Interlocked.Exchange(ref _mergedAwayRecorded, n);   // the scan's mark reads it without the gate
        }
    }

    /// <summary>
    /// Whether a segment that left the catalog during the scan whose mark is
    /// <paramref name="mark"/> and whose snapshot is <paramref name="snapshot"/> may have been a
    /// merge's source whose events that scan does not read — rather than a retention delete, or
    /// a merge whose output the snapshot lists.
    ///
    /// <para>True when the record names it and the snapshot does not list the output it names.
    /// A listed output was published before the snapshot was taken, so the scan reads the
    /// source's events there (or, if the output cannot be read either, sorts THAT failure on its
    /// own). The snapshot's keys are looked up only here, after the record has named an output,
    /// so a scan that meets no merged-away source builds nothing.</para>
    ///
    /// <para>Also true when eviction may have dropped its record,
    /// which is the conservative direction: a retention delete called a merge makes one count a
    /// floor that was exact, where a merge called retention makes a low count look exact.</para>
    ///
    /// <para>"May have" is decided by the mark, not by whether anything was ever evicted — on a
    /// server that has merged more than <see cref="MergedAwaySegmentCap"/> sources in its life
    /// something always has been, and every retention delete racing a scan would then be called
    /// a merge. Merges run one at a time on the maintenance loop, and each records a source
    /// immediately before deleting it, with nothing between the two (the delete completes
    /// synchronously), so no other record is made between a key's record and its entry leaving
    /// the catalog. The scan read its mark before its snapshot listed the key, so before the
    /// entry left. If the key's record came before the mark, the mark was read in that gap and
    /// equals the record's number; otherwise the number is above the mark. Either way it is at
    /// least the mark, and eviction that has not reached the mark cannot have dropped it.</para>
    /// </summary>
    private bool MayHaveBeenMergedAway(
        SegmentKey key, long mark, IReadOnlyList<SegmentInfo> snapshot, ref HashSet<SegmentKey>? snapshotKeys)
    {
        SegmentKey output;
        lock (_mergedAwayGate)
        {
            // An evicted record took its output with it: no telling whether the snapshot lists
            // it, so the cautious answer stands.
            if (!_mergedAwaySegments.TryGetValue(key, out var record))
                return _mergedAwayEvictedThrough > 0 && _mergedAwayEvictedThrough >= mark;
            output = record.Output;
        }

        // Outside the gate: the first lookup a scan makes builds a set of its snapshot, and the
        // merge's cleanup must not wait on that.
        return !SnapshotLists(snapshot, output, ref snapshotKeys);
    }

    /// <summary>
    /// Whether <paramref name="snapshot"/> lists <paramref name="key"/>. The key set is built by
    /// the first call a scan makes and shared by its parallel workers: published whole by a
    /// compare-exchange and never written after, so concurrent lookups are safe. Two workers
    /// racing to build it each build one and one wins, and both answer from a complete set.
    /// A merge output's key is freshly allocated, so the key alone names it.
    /// </summary>
    private static bool SnapshotLists(IReadOnlyList<SegmentInfo> snapshot, SegmentKey key, ref HashSet<SegmentKey>? keys)
    {
        var set = Volatile.Read(ref keys);
        if (set is null)
        {
            var built = new HashSet<SegmentKey>(snapshot.Count);
            for (int i = 0; i < snapshot.Count; i++) built.Add(SegmentKey.Of(snapshot[i]));
            set = Interlocked.CompareExchange(ref keys, built, null) ?? built;
        }
        return set.Contains(key);
    }

    /// <summary>
    /// A segment the header aggregation could not read is now visible to users — the query
    /// language reports the count as partial because of it — so its cause has to be findable in
    /// the server log at a level that is kept: Warning, with the segment id and the exception.
    /// Once per segment, because the histogram polls every few seconds and every alert rule ticks
    /// every 15 s, and a torn file stays in the catalog until the next start quarantines it; a
    /// warning per poll would bury everything else. Repeats go to Debug, as before, and so does
    /// every segment past <see cref="WarnedUnreadableSegmentCap"/>.
    ///
    /// <para>Returns whether the catalog still served the segment once its place was taken: true
    /// means a real skip, logged here; false means it left the catalog after the caller's check,
    /// nothing is logged, and the caller sorts the race as it sorts one its own check caught.</para>
    /// </summary>
    private bool LogUnreadableSegment(Exception ex, SegmentInfo info, SegmentKey key)
    {
        _beforeUnreadableSegmentWarned?.Invoke(info);

        // Count first: a full set never grows and is never cleared, so past the cap each poll
        // costs a Debug line rather than a Warning per segment. The check and the add are one
        // step under a gate, or parallel workers would all see room and all add: this runs only
        // for a segment that failed to read, so the gate costs the scan nothing. A delete
        // removing a key outside it can only make room.
        bool warn;
        lock (_warnedUnreadableGate)
            warn = _warnedUnreadableSegments.Count < WarnedUnreadableSegmentCap
                && _warnedUnreadableSegments.TryAdd(key, 0);

        // Added, THEN checked against the catalog, and taken back if the segment has gone. The
        // caller's catalog check came before the add, and DeleteSegmentAsync evicts the key
        // without the gate: a delete landing between the two evicted a key not yet added, and
        // the add then left a key nothing would ever remove, holding one of the capped places
        // for a segment that no longer exists. In this order a delete either evicts after the
        // add or removed the entry before this check, which sees it gone.
        //
        // Rather than the delete evicting under the gate and the check moving inside it: that
        // closes the same window, but only by adding a lock to DeleteSegmentAsync's nest for a
        // log line, and the ordering here needs no lock at all. The gate stays a leaf.
        //
        // Checked whether or not a place was taken: a segment gone by now is gone for the count
        // too, cap or no cap, and giving the place back is only the half of it that needs one.
        if (!CatalogServes(key, info))
        {
            if (warn) _warnedUnreadableSegments.TryRemove(key, out _);
            return false;
        }

        if (warn)
            _logger.LogWarning(ex,
                "Header aggregation could not read segment {NodeId}-{Id} ({File}); counts over its window are partial",
                info.NodeId, info.Id, info.FilePath);
        else
            _logger.LogDebug(ex, "Header aggregation skipped segment {Id}", info.Id);
        return true;
    }

    private void OnReaderDisposed()
    {
        if (Interlocked.Decrement(ref _activeReaders) == 0)
            DrainRetired();
    }

    /// <summary>
    /// Non-owning read-only view of the current hot tier plus any tiers that
    /// have been frozen but not yet released. Resolves message templates via
    /// the engine's <see cref="StringInternPool"/>.
    /// <see cref="Dispose"/> is intentionally a no-op — tiers are owned by
    /// <see cref="StorageEngine"/> and must outlive individual query operations.
    /// </summary>
    private sealed class HotTierReaderSnapshot(
        HotTierSegment   current,
        HotTierSegment[] frozen,
        IReadOnlySet<SegmentKey> covered,
        StringInternPool pool,
        StorageEngine    owner) : IHotTierReader
    {
        private int _disposed;

        public IEnumerable<LogEvent> ReadAll()
        {
            // Older events (already-frozen tiers) first, then current.
            for (int i = 0; i < frozen.Length; i++)
                foreach (var ev in frozen[i].ReadAll(pool))
                    yield return ev;
            foreach (var ev in current.ReadAll(pool))
                yield return ev;
        }

        /// <summary>
        /// Header-level filtered + sorted scan (see <see cref="HotTierScan"/>): only the
        /// events actually yielded are materialised, instead of the whole tier per query.
        /// </summary>
        public IEnumerable<LogEvent> ReadSorted(
            long fromTicks, long toTicks,
            long? afterTsTicks, ulong? afterIdRaw, bool forward,
            IReadOnlySet<Ameto.Core.LogLevel>? levels)
            => HotTierScan.ReadSorted(current, frozen, pool, fromTicks, toTicks, afterTsTicks, afterIdRaw, forward, levels);

        /// <summary>Same scan, with the filter's header-level part applied before materialisation.</summary>
        public IEnumerable<LogEvent> ReadSorted(
            long fromTicks, long toTicks,
            long? afterTsTicks, ulong? afterIdRaw, bool forward,
            IReadOnlySet<Ameto.Core.LogLevel>? levels,
            IHotHeaderPredicate? headerPredicate)
            => HotTierScan.ReadSorted(current, frozen, pool, fromTicks, toTicks, afterTsTicks, afterIdRaw, forward, levels, headerPredicate);

        public IReadOnlySet<SegmentKey> CoveredSegmentKeys => covered;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                owner.OnReaderDisposed();
        }
    }

    // ── Write path (called by ingestion) ──────────────────────────────────────

    /// <summary>
    /// Writes a single event header + properties payload into the hot tier and WAL.
    /// Assigns a monotonic <see cref="EventId"/> to the header before writing.
    /// Returns false if the hot tier is full (caller should trigger async flush).
    /// <paramref name="template"/>: optional message-template string. When supplied
    /// it is stored alongside the event so cold-tier flush can persist it even if
    /// the <see cref="TemplatePool"/> entry is later missing.
    /// </summary>
    public bool TryWrite(in LogEventHeader header, ReadOnlySpan<byte> propertiesPayload, string? template = null, ExceptionInfo? exception = null)
    {
        // Assign time-sortable, monotonic event id.
        // Time component is derived from the event's own @t (TimestampUtcTicks), not
        // server ingest time, so sorting by Id matches the timestamp shown in the UI.
        // The generator clamps to prevMs+1 for late-arriving events, preserving
        // strict per-node monotonicity (cursor pagination by Id remains correct).
        var h = header;
        h.Id  = _idGen.Next(header.TimestampUtcTicks);

        // ONE capture: tier and WAL are used from the same immutable state, so the event
        // can never land in a tier whose WAL this call does not also hold.
        var w = _write;

        if (!w.Hot.TryWrite(h, propertiesPayload, template, exception))
        {
            // Hot tier full (or frozen mid-swap) — schedule async flush and signal
            // back-pressure; the caller retries the SAME event (drainer pending slot).
            ScheduleFlush();
            return false;
        }

        // ── The event is COMMITTED from here on. Nothing below may throw out of TryWrite:
        //    the drainer treats a thrown TryWrite as "not written" and retries the same
        //    event — which would insert another copy (fresh id) into the tier per attempt.
        // The WAL entry's index is 16 bits and all 65 536 values are real pool ids, so an event
        // outside the pool (-1 once it is full, or a claim at/past 65 536) is passed through
        // as-is and Append logs it with the Unpooled flag instead of an index. It writes NO
        // pool row for it: a row 0 carrying the unpooled text would be force-interned by
        // recovery and become every genuine index-0 event's template. And without the flag,
        // recovery gave the unpooled event itself index 0's template whenever the pool file
        // held any row. The hook still gets the attached text; the WAL never stores it.
        string tmplStr = template
                         ?? (h.MessageTemplatePoolIndex >= 0 ? TemplatePool.Get(h.MessageTemplatePoolIndex) : string.Empty);
        try
        {
            w.Wal?.Append(h.TimestampUtcTicks, h.Level, h.MessageTemplatePoolIndex, tmplStr, propertiesPayload, exception);
            _walFaulted = false;
        }
        catch (ObjectDisposedException)
        {
            // Extreme descheduling only: this state was captured just before a rotation
            // and the flush thread disposed the old WAL between our hot-tier write and
            // this append. The event IS in the (now frozen) tier, so the flush persists
            // it — only the crash-recovery copy of this one event is missing.
        }
        catch (Exception ex)
        {
            // Disk full growing the WAL, a wedged mapping after a failed grow, a pool-file
            // write error. The flush persists the event regardless — only crash-durability
            // is degraded until rotation replaces the WAL, so force one and say so once per
            // episode (the age loop keeps re-attempting the swap until the disk recovers).
            if (!_walFaulted)
            {
                _walFaulted = true;
                _logger.LogError(ex,
                    "WAL append failed — ingest continues with reduced crash-durability until the WAL rotates");
                ScheduleFlush();
            }
        }

        // Notify subscribers (e.g. alert evaluator) — must be fast, and must not be able
        // to fault ingest: a throwing subscriber used to kill the drain task outright.
        var hook = EventWritten;
        if (hook is not null)
        {
            try { hook(h, tmplStr); }
            catch (Exception ex)
            {
                long n = ++_hookFaults;
                if (n == 1 || n % 10_000 == 0)
                    _logger.LogWarning(ex, "EventWritten subscriber threw ({Count} total) — subscriber faults are ignored", n);
            }
        }

        // Check size threshold
        if (w.Hot.IsFull)
            ScheduleFlush();

        return true;
    }

    // ── ISegmentManager ───────────────────────────────────────────────────────

    public async Task FlushHotTierAsync(CancellationToken ct = default) =>
        await TryFlushAsync(ct);

    /// <summary>
    /// Deletes the segment under <paramref name="key"/>, whichever node produced it.
    ///
    /// <para>By the pair, and with no id-only overload beside it: an id names a segment only
    /// within one node, so a delete taking one alone would unlink whichever of the overlapping
    /// series happened to hold it. Every caller here already has a <see cref="SegmentInfo"/> and
    /// passes <c>SegmentKey.Of(info)</c>.</para>
    /// </summary>
    public Task DeleteSegmentAsync(SegmentKey key, CancellationToken ct = default)
    {
        // Under _importLock, because an import PUBLISHES its entry before its File.Move lands
        // the file -- the reverse order was the earlier bug, the rename being the irreversible
        // half. A delete landing inside that window removed the fresh entry and failed to
        // delete a file that was not there yet; the import's move then produced a file no
        // entry names -- served by nobody, expired by nothing, compacted by no merge. Retention
        // is the caller that can land there (an imported segment may already be past its TTL);
        // the merge's source cleanup also passes through here, deleting up to a batch in
        // sequence, so it can queue behind an import's rename -- brief and bounded, and the
        // waiting is the point.
        //
        // And under _scanDeleteGate, the one lock the boot catalog scan takes (it must never
        // take _importLock -- see LoadSegmentCatalog). Removing the entry, recording the path for
        // a running scan, unlinking the file and parking a failed unlink are then one step to the
        // scan: it cannot register this path after the entry has gone but before the record or
        // the park that tells it to leave the path alone.
        lock (_importLock)
        lock (_scanDeleteGate)
        {
            if (_segments.TryRemove(key, out var info))
            {
                // Whatever the unlink below does -- succeeds, parks, or fails outright -- a
                // running catalog scan must not register this path again. Null, and so free,
                // once no scan runs (see _deletedDuringCatalogScan).
                _deletedDuringCatalogScan?.Add(info.FilePath);

                // Its place under the header aggregation's warning cap goes with it: the set
                // bounds unreadable segments still SERVED, not every one this process has met.
                _warnedUnreadableSegments.TryRemove(key, out _);

                _afterSegmentEntryRemoved?.Invoke();

                // The merge bookkeeping (_mergeDeferStrikes, _mergeSkip) is deliberately NOT
                // touched here. Both are plain collections owned lock-free by the maintenance
                // thread, and this method also runs on retention's threads — a background
                // service and an HTTP endpoint — where a Remove would be a concurrent mutation
                // that _importLock does not cover (the merge side never takes it). Keys the
                // delete orphans are pruned at the top of the next merge pass, on the owner.
                try { _deleteSegmentFile(info.FilePath); }
                catch (Exception ex) when (IsAlreadyGone(ex))
                {
                    // Gone is what the delete wanted. File.Delete is already silent about a
                    // missing file; this only guards against a runtime that is not. A missing
                    // DIRECTORY is not "gone": it is an unreachable one, and parks below.
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    // Windows: a query still maps the file (a prefilter reader lives for the
                    // whole query). The entry stays removed, so no NEW query picks the segment,
                    // and the unlink is retried once the reader is gone. The same exceptions
                    // also mean a read-only volume or a denied ACL, which no retry fixes; the
                    // pending set is capped for that. See ParkSegmentDelete.
                    ParkSegmentDelete(key, info.FilePath, ex);
                }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete segment {Key}", key); }
            }
        }
        return Task.CompletedTask;
    }

    // ── Deferred segment-file deletes ─────────────────────────────────────────

    /// <summary>
    /// Segment files whose catalog entry is gone but whose <c>File.Delete</c> failed, by path,
    /// with the key the entry had. On Windows a file cannot be unlinked while any reader maps
    /// it, and a query holds its readers for its whole duration, so a retention or merge delete
    /// racing a query used to log a warning and leave the file behind for good. No entry named
    /// it any more, so nothing expired or deleted it, and after a restart the catalog scan
    /// loaded the expired segment back and served it until the next retention pass.
    ///
    /// <para>A path is in here from the failed delete until an attempt deletes it, finds it
    /// already gone, or finds the catalog naming it again. ONE background loop
    /// (<see cref="RunSegmentDeleteRetryLoopAsync"/>) serves every path for
    /// <see cref="SegmentDeleteRetryWindow"/> after it was parked; what outlasts that is retried
    /// by <see cref="RetryPendingSegmentDeletes"/> from every maintenance and retention pass. It
    /// used to be one task per path, each with its own 120 s of backoff.</para>
    ///
    /// <para>Bounded by <see cref="PendingSegmentDeleteCap"/>. The exceptions an open reader
    /// causes are also what a read-only volume or a denied ACL throws, and nothing about the
    /// failure tells them apart. Unbounded, such a volume parked every expired segment for the
    /// life of the process, and every maintenance and retention pass retried them all, one
    /// <c>_importLock</c> each. Past the cap a failed delete is logged and left on disk, where
    /// the next start's catalog scan and retention pass find it again.</para>
    ///
    /// <para>Added only under <c>_importLock</c> and <see cref="_scanDeleteGate"/>, and removed
    /// only under <c>_importLock</c>, so the cap check and the add are one step.</para>
    /// </summary>
    private readonly ConcurrentDictionary<string, PendingSegmentDelete> _pendingSegmentDeletes = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>One parked path: the key its entry had, and when its delete failed.</summary>
    private sealed class PendingSegmentDelete(SegmentKey key, long parkedAtTimestamp)
    {
        public readonly SegmentKey Key               = key;
        public readonly long       ParkedAtTimestamp = parkedAtTimestamp;

        /// <summary>
        /// Set once the background loop has given up on this path and said so; the maintenance
        /// and retention passes still retry it. Written only by the loop, of which one runs at a
        /// time, and read by it (a stale read costs one extra attempt at most).
        /// </summary>
        public bool LeftToMaintenance;
    }

    /// <summary>
    /// Serialises the boot catalog scan's check-and-register with <see cref="DeleteSegmentAsync"/>'s
    /// remove, record, unlink and park, and guards <see cref="_deletedDuringCatalogScan"/> and
    /// <see cref="_catalogScansRunning"/>. Its own lock and not <c>_importLock</c>, because an
    /// import holds that one across its publish and the scan must still be able to land inside
    /// that window (see <see cref="ImportSegment(string, string)"/>). Taken inside
    /// <c>_importLock</c> by the delete; nothing is taken under it.
    /// </summary>
    private readonly System.Threading.Lock _scanDeleteGate = new();

    /// <summary>
    /// Paths whose catalog entry <see cref="DeleteSegmentAsync"/> removed while a catalog scan was
    /// running, whatever became of the unlink; null while none runs. The scan skips a path in
    /// here, as it skips a parked one.
    ///
    /// <para>A record the delete keeps, not a question put to the filesystem. The scan used to
    /// skip a path when <c>File.Exists</c> said false, and that says false for any error too: EIO
    /// or ESTALE on NFS, a bad network path during an SMB hiccup. A LIVE segment skipped on such an
    /// answer stayed out of queries, retention and merges until the next restart. Only a delete
    /// writes here, so a path in here was deleted.</para>
    ///
    /// <para>Created when a scan starts and dropped when the last one ends, both under
    /// <see cref="_scanDeleteGate"/>, so it holds one boot scan's deletes and cannot grow for the
    /// life of the process.</para>
    /// </summary>
    private HashSet<string>? _deletedDuringCatalogScan;

    /// <summary>Catalog scans running: the boot scan, and any a test runs beside it. Under <see cref="_scanDeleteGate"/>.</summary>
    private int _catalogScansRunning;

    /// <summary>Paths recorded for running catalog scans (tests); 0 once none runs.</summary>
    internal int DeletedDuringCatalogScanCount
    {
        get { lock (_scanDeleteGate) return _deletedDuringCatalogScan?.Count ?? 0; }
    }

    /// <summary>
    /// Test hook: called by <see cref="DeleteSegmentAsync"/> after the entry is removed and
    /// before the file is unlinked, under both of its locks.
    /// </summary>
    internal Action? _afterSegmentEntryRemoved;

    /// <summary>
    /// Test hook: called by <see cref="LoadSegmentCatalog"/> with a file's path after it has read
    /// and closed the file and before it takes <see cref="_scanDeleteGate"/> to register it: the
    /// window in which retention can delete the segment under the scan.
    /// </summary>
    internal Action<string>? _beforeScanRegistersSegment;

    /// <summary>The running background retry loop, or the last one to have run.</summary>
    private Task _segmentDeleteRetryLoop = Task.CompletedTask;

    /// <summary>1 while a retry loop runs; a compare-and-swap from 0 is what starts one.</summary>
    private int _segmentDeleteRetryLoopRunning;

    /// <summary>First pause of the background delete retry; it doubles up to <see cref="SegmentDeleteRetryMaxDelay"/>.</summary>
    internal TimeSpan SegmentDeleteRetryInitialDelay = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan SegmentDeleteRetryMaxDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long after a path is parked the background loop keeps trying it before leaving it to
    /// the maintenance and retention passes: twice the query timeout, since a query holding the
    /// mapping is bounded by it (60 s when unbounded). Internal and settable so a test can make
    /// the give-up path fast.
    /// </summary>
    internal TimeSpan? SegmentDeleteRetryWindowOverride;

    private TimeSpan SegmentDeleteRetryWindow =>
        SegmentDeleteRetryWindowOverride
        ?? 2 * (_options.Query.Timeout > TimeSpan.Zero ? _options.Query.Timeout : TimeSpan.FromSeconds(60));

    /// <summary>
    /// Most paths parked at once. Reaching it takes a thousand deleted segment files held open at
    /// one moment by queries, a volume that refuses deletes, or a segments directory an operator
    /// deleted outright (on Windows every delete under it throws DirectoryNotFoundException, and
    /// parks: see <see cref="IsAlreadyGone"/>); the last two are the likelier.
    /// Past it a failed delete behaves as it did before deletes were retried: logged, and left
    /// on disk, for the next start's catalog scan and retention pass to find (a merge's source
    /// is also retried by the merge recovery sweep, from its manifest). Internal so a test can
    /// lower it.
    /// </summary>
    internal int PendingSegmentDeleteCap = 1024;

    /// <summary>1 once the cap's Warning is logged; cleared when the set drains to half the cap.</summary>
    private int _pendingSegmentDeleteCapWarned;

    /// <summary>Paths still waiting for their file to be unlinked (tests).</summary>
    internal int PendingSegmentDeleteCount => _pendingSegmentDeletes.Count;

    /// <summary>The running background retry loop, or the last one to have run (tests).</summary>
    internal Task SegmentDeleteRetryLoop => Volatile.Read(ref _segmentDeleteRetryLoop);

    /// <summary>
    /// Whether a segment-file delete that threw <paramref name="ex"/> found nothing to delete,
    /// which is success. Only <see cref="FileNotFoundException"/>, and File.Delete does not
    /// normally throw even that: the runtime swallows ERROR_FILE_NOT_FOUND and ENOENT and returns.
    /// It is kept so a runtime that does throw it is still not parked.
    ///
    /// <para>NEVER <see cref="DirectoryNotFoundException"/>. With a missing file under a directory
    /// that exists, File.Delete returns without throwing, so this exception already means a
    /// directory on the path could not be reached: a missing drive letter, or a broken junction or
    /// symlink during a volume outage (ERROR_PATH_NOT_FOUND), with the file still there behind a
    /// directory that will come back. It is parked like any other IO failure. Counted as gone,
    /// the path was dropped with no park and no log, the file leaked until restart, and the boot
    /// scan then served the expired segment again until the first retention pass.</para>
    ///
    /// <para>It used to count as gone while <c>Directory.Exists</c> said the parent was there.
    /// That probe cannot tell: when the segments directory is itself a junction or symlink whose
    /// target volume went offline, Windows reports on the link, the parent "exists", and the file
    /// leaked all the same. A segments directory an operator deleted outright now parks every
    /// delete instead, which <see cref="PendingSegmentDeleteCap"/> bounds.</para>
    /// </summary>
    private static bool IsAlreadyGone(Exception ex) => ex is FileNotFoundException;

    /// <summary>
    /// Unlinks a segment file for <see cref="DeleteSegmentAsync"/> and the parked retry: File.Delete,
    /// except in a test that swaps in a failure it cannot stage on disk, such as an unreachable
    /// directory whose link still resolves. Costs production one field read.
    /// </summary>
    internal Action<string> _deleteSegmentFile = File.Delete;

    /// <summary>
    /// Parks a failed delete and makes sure the background loop runs. The caller holds
    /// <c>_importLock</c> and <see cref="_scanDeleteGate"/>. Never blocks.
    /// </summary>
    private void ParkSegmentDelete(SegmentKey key, string path, Exception ex)
    {
        // Already parked: that entry's retries cover this failure too.
        if (_pendingSegmentDeletes.ContainsKey(path)) return;

        if (_pendingSegmentDeletes.Count >= PendingSegmentDeleteCap)
        {
            if (Interlocked.Exchange(ref _pendingSegmentDeleteCapWarned, 1) == 0)
                _logger.LogWarning(
                    "{Count} segment files are already waiting to be deleted, the most that are retried. A segment " +
                    "file whose delete fails from now on is not retried: each is logged at Information and stays on " +
                    "disk until the next start's catalog scan and retention pass find it. This many failing at once " +
                    "usually means the volume refuses deletes (read-only, permissions), not that queries hold the files.",
                    _pendingSegmentDeletes.Count);
            _logger.LogInformation(ex,
                "Segment file {File} could not be deleted and is not retried: the pending-delete set is full", path);
            return;
        }

        _pendingSegmentDeletes.TryAdd(path, new PendingSegmentDelete(key, System.Diagnostics.Stopwatch.GetTimestamp()));
        _logger.LogDebug(ex, "Segment {Key} could not be deleted (still open?) — its file delete is retried in the background", key);
        EnsureSegmentDeleteRetryLoop();
    }

    /// <summary>Starts the background retry loop unless one is running or shutdown has begun.</summary>
    private void EnsureSegmentDeleteRetryLoop()
    {
        // A delete after shutdown began (a late retention call) stays parked: DisposeAsync makes
        // one last attempt, and nothing else in this process will.
        if (Volatile.Read(ref _disposed) != 0) return;
        if (Interlocked.CompareExchange(ref _segmentDeleteRetryLoopRunning, 1, 0) != 0) return;

        CancellationToken ct;
        try { ct = _cts.Token; }
        catch (ObjectDisposedException)
        {
            Volatile.Write(ref _segmentDeleteRetryLoopRunning, 0);
            return;
        }

        Volatile.Write(ref _segmentDeleteRetryLoop,
            Task.Run(() => RunSegmentDeleteRetryLoopAsync(ct), CancellationToken.None));
    }

    /// <summary>
    /// The one background retry loop. Each pass tries every path it has not yet left to
    /// maintenance, with exponential backoff between passes; a path still failing
    /// <see cref="SegmentDeleteRetryWindow"/> after it was parked is left to the maintenance and
    /// retention passes, with one Warning. The loop ends when no path is left to it, and the next
    /// park starts another. It holds <c>_importLock</c> only around each attempt.
    /// </summary>
    private async Task RunSegmentDeleteRetryLoopAsync(CancellationToken ct)
    {
        TimeSpan delay = SegmentDeleteRetryInitialDelay;
        while (true)
        {
            try { await Task.Delay(delay, ct).ConfigureAwait(false); }
            catch (OperationCanceledException)
            {
                Volatile.Write(ref _segmentDeleteRetryLoopRunning, 0);
                return;   // shutdown: DisposeAsync makes one last attempt
            }

            bool more;
            try { more = RetrySegmentDeletesInBackground(); }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Deferred segment delete pass failed");
                more = true;
            }

            if (more)
            {
                delay = delay * 2 < SegmentDeleteRetryMaxDelay ? delay * 2 : SegmentDeleteRetryMaxDelay;
                continue;
            }

            // Nothing left to this loop. Stand down, then look once more: a path parked after the
            // pass above saw this loop still running, and so did not start another.
            Volatile.Write(ref _segmentDeleteRetryLoopRunning, 0);
            if (!HasSegmentDeletesForBackground()
                || Interlocked.CompareExchange(ref _segmentDeleteRetryLoopRunning, 1, 0) != 0)
                return;
            delay = SegmentDeleteRetryInitialDelay;
        }
    }

    /// <summary>One background pass. True while some path is still inside its window.</summary>
    private bool RetrySegmentDeletesInBackground()
    {
        TimeSpan window   = SegmentDeleteRetryWindow;
        bool     inWindow = false;
        foreach (var (path, pending) in _pendingSegmentDeletes)
        {
            if (pending.LeftToMaintenance || TryCompletePendingSegmentDelete(path)) continue;

            TimeSpan waited = System.Diagnostics.Stopwatch.GetElapsedTime(pending.ParkedAtTimestamp);
            if (waited < window)
            {
                inWindow = true;
                continue;
            }

            pending.LeftToMaintenance = true;
            _logger.LogWarning(
                "Segment file {File} still could not be deleted {Seconds:F0}s after its catalog entry was removed — " +
                "no longer retrying in the background; every maintenance and retention pass retries it",
                path, waited.TotalSeconds);
        }
        return inWindow;
    }

    private bool HasSegmentDeletesForBackground()
    {
        foreach (var (_, pending) in _pendingSegmentDeletes)
            if (!pending.LeftToMaintenance) return true;
        return false;
    }

    /// <summary>
    /// Retries every parked segment-file delete once. Called from the maintenance loop, from
    /// retention, and at shutdown; internal so tests can drive it deterministically.
    /// </summary>
    /// <returns>How many paths are still pending afterwards.</returns>
    internal int RetryPendingSegmentDeletes()
    {
        if (_pendingSegmentDeletes.IsEmpty) return 0;   // the common case costs no enumeration
        foreach (var (path, _) in _pendingSegmentDeletes)
            TryCompletePendingSegmentDelete(path);
        return _pendingSegmentDeletes.Count;
    }

    /// <summary>
    /// One attempt at a deferred delete. True when the path is settled: deleted, already gone,
    /// failed for a reason a retry will not fix, or named by the catalog again.
    /// </summary>
    private bool TryCompletePendingSegmentDelete(string path)
    {
        if (!_pendingSegmentDeletes.TryGetValue(path, out var pending)) return true;   // settled elsewhere

        // Under _importLock, the lock DeleteSegmentAsync and ImportSegment take. An import
        // publishes its entry BEFORE its File.Move lands the file, so checking the catalog
        // outside the lock could pass, let a re-import of the same segment to the same path
        // publish and land its file, and then unlink the file that import just registered.
        // Only the check and the unlink are under it; the waits between attempts are not.
        lock (_importLock)
        {
            // Named again: a re-import of the segment to the same path, and the file is live and
            // belongs to that entry now. A stale retry must not touch it. (Not the boot catalog
            // scan: it skips a parked path, and cannot slip in ahead of the park -- see
            // LoadSegmentCatalog. Flushes and merges only ever write new names.)
            if (_segments.TryGetValue(pending.Key, out var current)
                && string.Equals(current.FilePath, path, StringComparison.OrdinalIgnoreCase))
            {
                Unpark(path);
                return true;
            }

            try { _deleteSegmentFile(path); }   // silent when the file is already gone
            catch (Exception ex) when (IsAlreadyGone(ex))
            {
                // Nothing left to delete: settled. (A missing directory is an unreachable one,
                // and stays parked below.)
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                return false;            // still held open, or refused — try again later
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete segment file {File} — not retried", path);
            }
            Unpark(path);
            return true;
        }
    }

    /// <summary>Removes a settled path, re-arming the cap's Warning once the set has drained to half.</summary>
    private void Unpark(string path)
    {
        _pendingSegmentDeletes.TryRemove(path, out _);
        if (_pendingSegmentDeletes.Count <= PendingSegmentDeleteCap / 2)
            Volatile.Write(ref _pendingSegmentDeleteCapWarned, 0);
    }

    public IReadOnlyList<SegmentInfo> ListSegments() => _segments.Values.ToList();

    // ── Flush loop ────────────────────────────────────────────────────────────

    private async Task RunFlushLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_options.HotTier.MaxAge);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try { await TryFlushAsync(ct); }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    // A full disk fails the SWAP itself (creating the successor 64 MB WAL).
                    // The loop must survive it: this tick is the retry mechanism for swap
                    // failures, and a faulted loop would also detonate DisposeAsync's await.
                    _logger.LogError(ex, "Age-based flush failed — retried next tick");
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    // ── Flush ─────────────────────────────────────────────────────────────────

    /// <summary>Fire-and-forget a parallel flush, tracked so shutdown can await it.</summary>
    private void ScheduleFlush()
    {
        var t = Task.Run(() => TryFlushAsync());
        _inFlightFlushes[t] = 0;
        _ = t.ContinueWith(
            static (x, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(x, out _),
            _inFlightFlushes, CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private async Task TryFlushAsync(CancellationToken ct = default)
    {
        // ── SWAP PHASE — serialised (via _flushLock) and fast. Freezes the current
        //    hot tier, publishes it to the frozen list, installs a fresh hot tier and
        //    rotates the WAL, then releases the lock so the NEXT full tier can be
        //    swapped while this one is still being persisted by the heavy phase.
        HotTierSegment? oldHot     = null;
        WriteAheadLog?  oldWal     = null;
        string?         oldWalPath = null;
        ulong           reservedSegId = 0;

        if (!await _flushLock.WaitAsync(0, ct)) return; // a swap is already in progress
        try
        {
            var oldState = _write;
            if (oldState.Hot.Count == 0) return;

            // Back-pressure gate: if the in-flight tier budget is exhausted, skip the swap.
            // The hot tier stays full → TryWrite returns false → the drainer parks (ring
            // back-pressure) rather than letting frozen tiers pile up unbounded in RAM.
            if (!_flushSlots.Wait(0)) return;
            try
            {
                // Open the SUCCESSOR first, so the swap installs a complete (tier, WAL)
                // pair in one store: there is never a window in which a writer sees a
                // live tier with no WAL (the old null-then-open order silently skipped
                // the WAL for events accepted during it, once per rotation). Opening the
                // next WAL also reserves the next id block, so the WAL on disk always
                // names the ids ITS events will occupy. The OLD WAL is disposed in the
                // heavy phase, off the swap lock — disposing flushes up to 64 MB of
                // dirty mmap pages to disk, and doing that here stalled every writer
                // long enough to overflow the ingest ring under sustained 100k/s load.
                var (newWal, newSegId) = OpenWalCore();
                WriteState newState;
                try { newState = new WriteState(CreateHotTier(), newWal, newSegId); }
                catch { try { newWal.Delete(); } catch { } throw; }

                reservedSegId = oldState.WalSegId;
                oldState.Hot.Freeze();

                // Publish oldHot AND install the successor under the lock queries snapshot
                // from, so a concurrent query sees oldHot exactly once — as current before
                // the store, as frozen after it, never both — and skips the reserved cold
                // segment ids (no duplicates during the register/remove overlap). A tier
                // flushes to ONE SEGMENT PER LEVEL, and the block of ids for exactly that
                // was reserved when this tier's WAL was opened — the level's segment is
                // always firstId + (byte)level. Levels absent from the tier simply never
                // become files; a burnt id costs nothing.
                lock (_frozenLock)
                {
                    _frozenHot.Add((oldState.Hot, reservedSegId));
                    _write = newState;
                }

                oldHot     = oldState.Hot;
                oldWal     = oldState.Wal;
                oldWalPath = oldWal?.FilePath;
            }
            catch
            {
                // Successor could not be built (disk full creating the WAL, native OOM on
                // the tier): nothing was swapped, so the slot must not stay consumed.
                _flushSlots.Release();
                throw;
            }
        }
        finally { _flushLock.Release(); }

        if (oldHot is null) return; // hot tier was empty — nothing swapped (no slot taken)

        // Nobody writes to the old WAL any more (writers see the new _wal) — close its
        // handles before the flush so File.Delete below succeeds afterwards.
        oldWal?.Dispose();

        // ── HEAVY PHASE — parallel, bounded by _flushConcurrency. Builds the inverted/
        //    trigram/bloom indexes, compresses and writes the cold segment. Runs off the
        //    swap lock so several segments persist at once on otherwise idle cores. The
        //    back-pressure slot (taken at swap) is held until the tier is fully persisted.
        bool slotTransferred = false;
        try
        {
            await _flushConcurrency.WaitAsync(ct).ConfigureAwait(false);
            List<SegmentInfo> written;
            try
            {
                written = await FlushTierByLevelAsync(oldHot, reservedSegId, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A failed flush used to be TERMINAL: the tier stayed frozen in RAM with no
                // retry for the life of the process, while its back-pressure slot was
                // released — so under a full disk the engine leaked one native tier per
                // interval, unboundedly, precisely when a log store must ride the pressure
                // out. Now the slot's ownership moves to a background retry task (bounded
                // RAM: slot exhaustion skips further swaps → ring back-pressure) that
                // re-attempts until the disk recovers or the engine shuts down — in which
                // case the tier's WAL replays it on the next start.
                _logger.LogError(ex,
                    "Segment flush failed — tier stays frozen and queryable; retrying in background every {Delay}s",
                    FlushRetryDelay.TotalSeconds);
                ScheduleFlushRetry(oldHot, oldWalPath, reservedSegId);
                slotTransferred = true;
                return;
            }
            finally { _flushConcurrency.Release(); }

            PublishFlushedTier(written, oldHot, oldWalPath, reservedSegId);
        }
        finally { if (!slotTransferred) _flushSlots.Release(); }
    }

    /// <summary>
    /// Registers a persisted tier's cold segments, unlists the frozen tier, deletes its WAL
    /// (authorised by the completion marker) and retires the native memory. Shared by the
    /// normal heavy phase and the failure-retry task.
    /// </summary>
    private void PublishFlushedTier(List<SegmentInfo> written, HotTierSegment oldHot, string? oldWalPath, ulong reservedSegId)
    {
        // Register the cold segments AND drop oldHot from the frozen list atomically.
        lock (_frozenLock)
        {
            foreach (var w in written) PublishLocalSegment(w);
            _frozenHot.RemoveAll(f => ReferenceEquals(f.Tier, oldHot));
        }
        _logger.LogInformation("Flushed {Segments} level segment(s), {Count} events total",
            written.Count, written.Sum(w => (long)w.EventCount));

        foreach (var w in written) SegmentFlushed?.Invoke(w);

        if (oldWalPath is not null)
        {
            try { File.Delete(oldWalPath); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete WAL {Path}", oldWalPath); }
            try { File.Delete(oldWalPath + ".pool"); } catch { /* best-effort */ }
            // The marker is what AUTHORISES the delete above, so it outlives it: dropping
            // it first would leave a WAL that recovery has to replay to find out its
            // levels are all published. A crash in between leaves a marker with no WAL,
            // which the startup sweep clears.
            try { File.Delete(FlushMarkerPath(reservedSegId)); } catch { /* swept at startup */ }
        }

        RetireHotTier(oldHot);
    }

    /// <summary>
    /// Background retry for a frozen tier whose flush failed. Owns the tier's back-pressure
    /// slot until the tier is persisted or the engine shuts down (then the slot is released
    /// and the tier's WAL replays it on the next start). Tracked in
    /// <see cref="_inFlightFlushes"/> so DisposeAsync awaits it after cancelling.
    /// </summary>
    private void ScheduleFlushRetry(HotTierSegment oldHot, string? oldWalPath, ulong reservedSegId)
    {
        var t = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    await Task.Delay(FlushRetryDelay, _cts.Token).ConfigureAwait(false);
                    try
                    {
                        await _flushConcurrency.WaitAsync(_cts.Token).ConfigureAwait(false);
                        List<SegmentInfo> written;
                        try
                        {
                            // The failed attempt may have MOVED some level files into place
                            // without registering them (the publish block never ran, so no
                            // query, merge or replication can hold them). Delete the
                            // leftovers and rewrite the whole block — simpler to prove
                            // correct than resuming, and the block's ids are reserved to
                            // this WAL so nothing else can have produced these files.
                            DeleteUnpublishedLevelFiles(reservedSegId);
                            written = await FlushTierByLevelAsync(oldHot, reservedSegId, _cts.Token);
                        }
                        finally { _flushConcurrency.Release(); }

                        PublishFlushedTier(written, oldHot, oldWalPath, reservedSegId);
                        _logger.LogInformation("Frozen-tier flush retry succeeded for block {Block}", reservedSegId);
                        return;
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                            "Frozen-tier flush retry failed for block {Block} — next attempt in {Delay}s",
                            reservedSegId, FlushRetryDelay.TotalSeconds);
                    }
                }
            }
            catch (OperationCanceledException) { /* shutdown — WAL replays the tier next start */ }
            catch (ObjectDisposedException)    { /* raced DisposeAsync's CTS teardown — same outcome */ }
            finally
            {
                // The slot semaphore can already be disposed when this task was spawned by a
                // late ScheduleFlush during shutdown; the release is then moot, not an error.
                try { _flushSlots.Release(); } catch (ObjectDisposedException) { }
            }
        });
        _inFlightFlushes[t] = 0;
        _ = t.ContinueWith(
            static (x, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(x, out _),
            _inFlightFlushes, CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    /// <summary>Removes level files of a reserved block that were moved into place by a flush attempt that failed before publishing.</summary>
    private void DeleteUnpublishedLevelFiles(ulong firstSegId)
    {
        for (ulong s = 0; s < (ulong)LevelSegmentSlots; s++)
        {
            foreach (var f in Directory.EnumerateFiles(_segDir, $"{_options.NodeId.Value}-{firstSegId + s}-*.seg"))
            {
                try { File.Delete(f); }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete unpublished level file {File}", f); }
            }
        }
    }

    /// <summary>
    /// Frees a flushed hot tier once no query can still be reading it. Any query holding
    /// a reference snapshotted it (and incremented <see cref="_activeReaders"/>) under
    /// <see cref="_frozenLock"/> before it was removed from <see cref="_frozenHot"/>, so
    /// <c>_activeReaders == 0</c> proves no reader holds this — or any earlier-retired —
    /// tier. A list (not one slot) because parallel flushes retire tiers concurrently.
    /// </summary>
    private void RetireHotTier(HotTierSegment tier)
    {
        lock (_retireLock)
        {
            _retired.Add(tier);
            if (Volatile.Read(ref _activeReaders) == 0)
            {
                foreach (var t in _retired) t.Dispose();
                _retired.Clear();
            }
        }
    }

    /// <summary>Disposes retired tiers once the last concurrent reader finishes.</summary>
    private void DrainRetired()
    {
        lock (_retireLock)
        {
            if (_retired.Count == 0 || Volatile.Read(ref _activeReaders) != 0) return;
            foreach (var t in _retired) t.Dispose();
            _retired.Clear();
        }
    }

    // ── Compaction: SIZE-TIERED RUNS INSIDE AN (EXPIRY BUCKET, LEVEL) ─────────
    //
    // The goal is a catalog whose file count is proportional to the RETENTION WINDOW, not to
    // uptime, at a WRITE AMPLIFICATION that stops climbing. Level purity comes from the flush
    // (one segment per level) and is what keeps expiry exact; the bucket bounds how far a merge
    // may move a row's deadline; the size tier bounds how often a byte is rewritten.
    //
    // Buckets are ALIGNED, not sliding, and a segment's bucket is floor(MaxTimestamp / width).
    // MAX, not Min, because Max is the only timestamp retention reads: expiry is
    // MaxTimestamp + Ttl(MinLevel), so grouping by Max makes the merge's effect on retention
    // exact — every source's deadline moves by less than one bucket width, by construction.
    // Bucketing by MIN could not say that, and it had no home for a segment whose Min and Max
    // fall either side of a boundary: measured, 6 such segments produced 0 merges and kept a
    // 32.2-day span against a 7-day bound, because the span guard discarded every partner they
    // could have had. Under Max bucketing they land with the data they were flushed beside and
    // compact normally.
    //
    // Inside a bucket the planner takes a TIME-CONTIGUOUS RUN OF SIMILARLY-SIZED FILES. Both
    // halves are load-bearing:
    //   - similarly sized (within MergeRunSizeRatio, and the run's largest no more than
    //     MergeGrowthFactor of its total) is what makes a straggler cost the straggler. The
    //     previous rule dropped the size guard entirely for a sealed bucket, so one late row and
    //     the bucket's collapsed file were admissible together: measured, five one-event flushes
    //     into a collapsed bucket cost five merges and 7151 KB of writes, and the cost never
    //     decayed because the rewritten file was still under the maximal threshold.
    //   - time-contiguous is what makes the pieces of an over-large bucket partition it rather
    //     than interleave it (see MergeTargetPayloadBytes), and it is why the run BREAKS at the
    //     first file it cannot take instead of skipping past it.
    //
    // Requiring BOTH of a single run, however, is what left a bucket with no terminal state at
    // all: a file whose time-neighbours are all more than MergeRunSizeRatio away in size can
    // never join anything, not even a file of its own exact size elsewhere in the bucket,
    // because the run would have to step over the neighbour. Measured on the shape a bursty
    // producer makes on its own — 240 flushes alternating 200 and 40 events into a bucket that
    // sealed 30 days ago — 240 files and ZERO merges, at fixpoint, forever. So the run planner
    // gets a SECOND source of candidates, tried only when the first finds nothing anywhere:
    // the bucket's files GROUPED BY SIZE TIER (floor(log_ratio(payload))), each tier still taken
    // as a time-contiguous run of its own members. Same three conditions, a strictly smaller
    // input, and same-size files therefore always find each other. The same 240 flushes leave
    // 2 files. The cost is that two files of a bucket may overlap in time — which is why it is
    // a FALLBACK: overlap costs a query one extra open file, where the alternative costs it one
    // open file per flush that ever landed in the window.
    //
    // The tiers are also what bounds the terminal state, and the bound is a property of the
    // geometry rather than of the workload: at fixpoint a tier holds fewer files than the
    // fanout (three same-tier files always satisfy the growth rule, since the largest is under
    // MergeRunSizeRatio of the smallest), so a bucket holds at most
    // fanout × log_ratio(maximal / flush size) files whatever the size distribution.

    /// <summary>
    /// Uncompressed payload one merged file aims for.
    ///
    /// <para>This is a POLICY number now, not a memory one — the streaming merge holds one
    /// block per source plus one index group, so peak is flat in the merged size. It has to be
    /// at least ONE DAY of the busiest level, or the (day, level) bucket the whole design
    /// targets cannot land in a single file: the sandbox stand's entire log corpus is ~370 MB
    /// of payload per day across all six levels, Information dominant, so 512 MB leaves the
    /// dominant level roughly 1.7× headroom. Above that the return diminishes — per-file index
    /// and catalog overhead has already vanished — while the unit an expiry deletes, and the
    /// work a single interrupted pass throws away, keep growing.</para>
    /// </summary>
    private const long MergeTargetPayloadBytes = 512L * 1024 * 1024;

    /// <summary>
    /// A segment at or past this is MAXIMAL: never a merge source again, whatever else lands in
    /// its bucket. It is the TOP RUNG of the size ladder, and having a reachable one is what
    /// makes write amplification a constant rather than a function of how long the server has
    /// been running — a byte is rewritten log(maximal / flush-segment size) times and then never
    /// again. Measured with the top rung out of reach (a 64 MB target against 69 KB segments):
    /// 2.06x at 80 flushes, 3.20x at 160, 3.34x at 400, still climbing. Half the target, so two
    /// eligible files can always be combined without overshooting.
    /// </summary>
    private const long MergeSealedSourceBytes = MergeTargetPayloadBytes / 2;

    /// <summary>
    /// A merge batch may not mix sizes further apart than this ratio — in an open bucket AND in
    /// a sealed one.
    ///
    /// <para>This, with <see cref="MergeGrowthFactor"/>, is the whole answer to write
    /// amplification. A byte enters at flush-segment size and leaves the policy at
    /// <see cref="MergeSealedSourceBytes"/>; a run of <see cref="MergeMinSources"/> same-size
    /// files multiplies it by that fanout each time it is rewritten, so a byte is rewritten
    /// about log₄(maximal / flush size) times.</para>
    ///
    /// <para>THAT IS A FUNCTION OF THE DEPLOYMENT'S GEOMETRY, NOT A CONSTANT OF THE POLICY, and
    /// the published figure has to be read that way. Measured over 6000 flushes with stragglers
    /// at maximal/flush ≈ 235 — the marginal cost of each stretch, which is the only reading
    /// that says whether the policy has settled, since the cumulative one is a running average
    /// and lags: 1.80x, 1.80x, 3.13x, 2.81x, 3.05x, 2.98x, 3.05x per stretch at 80 / 160 / 400 /
    /// 1000 / 2000 / 3000 / 4000 / 6000 flushes. A BAND of 2.81–3.13, flat over 15× the data,
    /// not a staircase and not a point. A quieter server with 20 KB flush segments has two more
    /// rungs on the same ladder and pays for them. The figure also counts REWRITES ONLY — the
    /// device sees 1 + that per ingested byte.</para>
    ///
    /// <para>It is STRICTLY BELOW <see cref="MergeMinSources"/>, and that inequality is the
    /// point. At ratio 8 with a fanout of 8, a merge's own output is exactly 8× its sources and
    /// therefore admissible beside the very next batch of flush segments — so the freshly
    /// written file is rewritten again for the next 8 arrivals, at double the cost, forever.
    /// MEASURED over 1000 flushes with stragglers: 3.34x steady state at ratio 8 against 2.81x
    /// at ratio 4, for the same fanout and the same data.</para>
    ///
    /// <para>Dropping it for sealed buckets — the previous rule — is what made a one-row
    /// straggler cost a full bucket rewrite. It is kept for sealed buckets now; what a sealed
    /// bucket relaxes is only the FANOUT (see <see cref="MergeSealedMinSources"/>).</para>
    ///
    /// <para>It is also the base of the SIZE TIER (see <see cref="SizeTier"/>), which is why it
    /// is expressed as a shift: two files are in the same tier exactly when neither is more than
    /// this ratio from the tier's own floor, so a tier's members satisfy the spread rule by
    /// construction and the fallback planner needs no separate check.</para>
    /// </summary>
    private const int MergeRunSizeShift = 2;
    internal const int MergeRunSizeRatio = 1 << MergeRunSizeShift;

    /// <summary>
    /// Which rung of the size ladder a file is on: <c>floor(log₍ratio₎ payload)</c>, computed as
    /// a shift of its bit length so the planner can bucket by it without a division or a
    /// floating-point log.
    ///
    /// <para>The tier is what lets same-sized files find each other when time-contiguity keeps
    /// them apart, and its rigidity is deliberate: two files either side of a rung boundary are
    /// within 1× of each other and still never merge DIRECTLY — but each coalesces with its own
    /// tier and climbs, so the pair costs at most one extra file, never a stall.</para>
    /// </summary>
    internal static int SizeTier(long payload) =>
        BitOperations.Log2((ulong)Math.Max(1, payload)) / MergeRunSizeShift;

    /// <summary>
    /// A merge must grow its largest source by at least this factor, expressed as the fraction
    /// of that source the REST of the batch has to add up to (1/2 ⇒ the output is ≥ 1.5× the
    /// largest input).
    ///
    /// <para><see cref="MergeRunSizeRatio"/> alone does not bound amplification once a bucket
    /// holds one big file and a trickle of small ones: at a ratio of 8 the big file becomes
    /// admissible again as soon as the trickle reaches an eighth of it, so it is rewritten once
    /// per (size/8) bytes of new data — an amplification of 8 that grows with the file. This
    /// says instead that a merge is only worth doing when the data it is ADDING is a real
    /// fraction of the data it is rewriting, which caps that per-rewrite cost at 3× and, being
    /// a multiplicative floor on file growth, also caps the number of rewrites a byte can ever
    /// see at log₁.₅(maximal / flush size).</para>
    /// </summary>
    private const int MergeGrowthFactor = 2;

    /// <summary>
    /// A bucket is SEALED this long after its window ends — capped at one bucket width. Sealing
    /// only lowers the FANOUT a batch needs (<see cref="MergeSealedMinSources"/>); it no longer
    /// lifts the size guard, so there is nothing left that has to happen exactly once and the
    /// grace can be short.
    ///
    /// <para>Capping at the width is what keeps the arithmetic sane for a short-TTL level: at a
    /// flat 48 h, Debug — whose entire TTL is 3 days — spent two thirds of its data's life
    /// waiting for stragglers that a 6 h bucket has no room for anyway.</para>
    /// </summary>
    private const long MergeBucketGraceTicks = 48L * TimeSpan.TicksPerHour;

    private static long MergeSealGraceTicks(long bucketWidthTicks) =>
        Math.Min(MergeBucketGraceTicks, bucketWidthTicks);

    /// <summary>
    /// A bucket covers at most <c>Ttl(level) / 12</c>, so a merge moves no row's expiry by more
    /// than 8.3 % of that row's own TTL.
    ///
    /// <para>Expiry is <c>MaxTimestamp + Ttl(MinLevel)</c>, so the bucket width is exactly how
    /// much extra retention compaction can buy a row. A flat 24 h reads as the safe choice and
    /// is, for a busy level — but for a RARE level it is the reason the catalog fills with
    /// near-empty files: a service that logs four Fatals a week gets one file per day
    /// regardless, each carrying a full index and catalog entry for a handful of rows. One
    /// twelfth is chosen because 8.3 % is smaller than the error already baked into a retention
    /// policy expressed in whole days, and it buys a 7× reduction in file count at the default
    /// 90-day TTLs.</para>
    ///
    /// <para>It is a CEILING, and for busy levels it is not the binding one:
    /// <see cref="MergeTargetPayloadBytes"/> stops a batch long before 7 days of Information
    /// have accumulated, so the dominant level's files still span about a day and over-retain
    /// by ~1 %. The fraction only bites where there is too little data to fill a file, which is
    /// exactly where it should.</para>
    ///
    /// <para>The 8.3 % holds for any TTL at or above 12 MINUTES; below that the grid's own
    /// one-minute floor binds and over-retention is <c>1 min / Ttl</c> instead (see
    /// <see cref="MergeBucketTicks"/>). It is stated because TTLs are settable at runtime, not
    /// because any shipped default is near it — the smallest is Debug's 3 days.</para>
    /// </summary>
    private const int MergeSpanTtlDivisor = 12;

    /// <summary>
    /// Sources an OPEN bucket needs before a merge is worth doing. This is the fanout: the run
    /// it gates is what multiplies a file's size by ~<see cref="MergeRunSizeRatio"/>, and the
    /// number of rewrites a byte sees is log of the size range in that multiplier. Eight trades
    /// ~2.5 rewrites per byte at the stand's geometry against holding up to eight uncompacted
    /// flush segments per level in the catalog.
    /// </summary>
    internal const int MergeMinSources = 8;

    /// <summary>
    /// Sources a SEALED bucket needs. Two, because a quiet day leaves a handful of tiny segments
    /// that a fanout of eight would strand forever (observed live: ~1,000 files parked that
    /// way), and because a low fanout is no longer dangerous — <see cref="MergeGrowthFactor"/>
    /// is what stops a pair being "the bucket's big file plus one straggler", which is the shape
    /// that used to make this number costly.
    ///
    /// <para>What a fanout of two does cost is a MERGE RATE, and the rate is per arriving FLUSH,
    /// not per late row: measured, 200 one-row stragglers into a collapsed 1430 KB bucket cost
    /// 148 merges — 701 B rewritten each, so the bytes are bounded, but each is still a manifest
    /// write-through, an index build, a segment write and fsync, two unlinks and a
    /// <c>_flushConcurrency</c> slot. There is no byte floor guarding it deliberately: a floor
    /// would strand exactly the files this fanout exists to rescue (a service logging four Fatals
    /// a week produces genuinely tiny segments, and they never grow). The rate is bounded instead
    /// by the two things that already bound it — one flush emits at most one segment per level
    /// however many late rows it carries, and the maintenance pass runs on its own timer, so
    /// stragglers that arrive between two passes coalesce in a single merge of their size tier
    /// rather than pairwise. The measured 148 is the pathological reading taken by compacting to
    /// exhaustion after every single flush.</para>
    /// </summary>
    internal const int MergeSealedMinSources = 2;
    // Each source contributes one open reader and one decompressed block (~64 KB) for the
    // length of the merge — the k-way merge's only per-source cost, ~36 MB at this cap.
    private const int MergeMaxSources = 512;
    /// <summary>
    /// Events per merged file. Interruptibility is handled by the writer's per-block
    /// cancellation check, so this exists only to keep one file's block index and group
    /// directory a sane size for workloads whose events are far smaller than the stand's ~2 KB.
    /// </summary>
    private const int MergeMaxEvents = 4_000_000;
    private const int MergeWindowAttempts = 4;  // bucket re-selections per pass after an anchor skip

    /// <summary>
    /// Widths a sub-day bucket may take. Every one DIVIDES a day, which is the property that
    /// matters: ticks run from a midnight, so a width that divides a day puts a boundary on
    /// every UTC midnight and the grid stays aligned with the whole-day widths above it.
    /// </summary>
    private static readonly long[] SubDayBucketWidths =
    [
        1 * TimeSpan.TicksPerMinute,  2 * TimeSpan.TicksPerMinute,  3 * TimeSpan.TicksPerMinute,
        4 * TimeSpan.TicksPerMinute,  5 * TimeSpan.TicksPerMinute,  6 * TimeSpan.TicksPerMinute,
        10 * TimeSpan.TicksPerMinute, 12 * TimeSpan.TicksPerMinute, 15 * TimeSpan.TicksPerMinute,
        20 * TimeSpan.TicksPerMinute, 30 * TimeSpan.TicksPerMinute,
        1 * TimeSpan.TicksPerHour,  2 * TimeSpan.TicksPerHour,  3 * TimeSpan.TicksPerHour,
        4 * TimeSpan.TicksPerHour,  6 * TimeSpan.TicksPerHour,  8 * TimeSpan.TicksPerHour,
        12 * TimeSpan.TicksPerHour,
    ];

    /// <summary>
    /// Width of the expiry bucket for a level with this TTL: the largest aligned width at or
    /// below <c>Ttl / <see cref="MergeSpanTtlDivisor"/></c>.
    ///
    /// <para>The old whole-day floor made the divisor a claim the code did not keep. Debug's TTL
    /// is 3 days, so its share is 6 h — floored to a day, its rows lived 4 days instead of 3,
    /// i.e. 33 % of over-retention advertised as 8.3 %, on the level that is usually the largest
    /// on disk. Sub-day widths that divide a day give Debug its 6 h and leave every 90-day level
    /// exactly where it was (7 days, 7.8 %).</para>
    ///
    /// <para>The ladder runs down to ONE MINUTE, not to one hour. TTLs are settable at runtime,
    /// and a 1 h floor was the same hidden floor one order of magnitude down: it over-retained a
    /// 3 h level by 33 %, a 1 h level by 100 % and a 30 min level by 200 %, all while the
    /// divisor's doc comment said 8.3 %. The minute floor keeps the guarantee down to a 12-minute
    /// TTL, which is below anything a retention policy is written to mean.</para>
    /// </summary>
    internal static long MergeBucketTicks(TimeSpan ttl)
    {
        long budget = ttl.Ticks / MergeSpanTtlDivisor;
        if (budget >= TimeSpan.TicksPerDay)
            return budget / TimeSpan.TicksPerDay * TimeSpan.TicksPerDay;

        long width = SubDayBucketWidths[0];
        foreach (long candidate in SubDayBucketWidths)
            if (candidate <= budget) width = candidate;
        return width;
    }

    /// <summary>
    /// What a segment costs a merge: its uncompressed payload, which is what the target budget
    /// and the index build both scale with. Falls back to the file size for a catalog entry
    /// opened cheaply (the reader only walks the blocks when asked to).
    /// </summary>
    private static long SegmentPayloadBytes(SegmentInfo s) =>
        Math.Max(s.UncompressedBytes, s.CompressedBytes);

    /// <summary>
    /// Picks the next batch: the oldest (level, expiry bucket) group that holds a mergeable run,
    /// and as many of that run's segments as one merged file affords. Null when nothing is worth
    /// merging — which is the steady state, not a failure: every bucket has either reached
    /// <see cref="MergeSealedSourceBytes"/> or holds only files no run can legally combine.
    ///
    /// <para>Two candidate sources, in strict order. A TIME-CONTIGUOUS run
    /// (<see cref="SelectMergeRun"/>) everywhere it can be found, because its output never
    /// overlaps a file it left behind; and only when that finds nothing in the whole catalog, a
    /// run inside one SIZE TIER (<see cref="SelectMergeTierRun"/>), which may overlap and is
    /// what gives a bucket of unevenly sized files a terminal state at all.</para>
    ///
    /// <para>Chosen from CATALOG METADATA alone; no file is opened, let alone decoded, until
    /// the merge itself streams it.</para>
    /// </summary>
    private List<SegmentInfo>? SelectMergeBatch()
    {
        var  policy  = _retentionStore.GetPolicy();
        long now     = DateTimeOffset.UtcNow.UtcTicks;
        long target  = _mergeTargetPayloadBytes;
        long maximal = target / 2;   // MergeSealedSourceBytes — scaled with the target, not fixed

        // Group by (level, aligned bucket start). Grouping by LEVEL rather than by TTL class
        // matters even though same-level implies same TTL: Information and Error share the
        // 90-day class, and merging them would hand the merged file back the mixed-level shape
        // whose retention the level-split flush exists to make exact.
        var buckets = new Dictionary<(Ameto.Core.LogLevel Level, long Start), List<SegmentInfo>>();
        foreach (var s in _segments.Values)
        {
            if (_mergeSkip.Contains(SegmentKey.Of(s)) || _mergePassDeferred.Contains(SegmentKey.Of(s))) continue;
            // A replicated peer's segment is a merge candidate here, the same as a local one, and
            // MergeToColdAsync stamps its output with THIS node's id — so merging one drops the
            // provenance that SegmentInfo.NodeId carried, and a later re-push of the same (node, id)
            // is no longer recognised as already held: its events land a second time, permanently,
            // since nothing on this node can ever match the re-pushed pair against the merged copy.
            //
            // Wholly independent of the catalog key, and measured that way: a fixture with no
            // local segment at all — hence no collision to win or lose — reproduces it identically
            // on the single-keyspace build, at 12 events where 8 exist. EVERY replicated segment
            // is exposed, not only one whose id happens to clash, so an id-keyed catalog does not
            // narrow it and this key does not widen it.
            //
            // Left as-is deliberately: excluding foreign segments would strand a peer's small
            // files uncompacted on this node forever, and the honest repair is a durable
            // consumed-(node, id) record that ImportSegment consults — which needs a lifetime
            // rule of its own (when may a pair be forgotten? retention deletes the merged output
            // eventually, and a re-push after that is legitimate again), so it belongs to how
            // cluster mode is meant to work overall rather than to a rider on the key change.

            // A maximal segment is done — it is the OUTPUT of this policy, not an input to it.
            if (SegmentPayloadBytes(s) >= maximal) continue;

            long width = MergeBucketTicks(policy.GetTtl(s.MinLevel));
            long start = s.MaxTimestampTicks / width * width;

            var key = (Level: s.MinLevel, Start: start);
            if (!buckets.TryGetValue(key, out var list)) buckets[key] = list = new List<SegmentInfo>(8);
            list.Add(s);
        }
        if (buckets.Count == 0) return null;

        // Oldest bucket first, so the settled past consolidates and then stays consolidated.
        var keys = new List<(Ameto.Core.LogLevel Level, long Start)>(buckets.Keys);
        keys.Sort((a, b) => a.Start != b.Start ? a.Start.CompareTo(b.Start) : ((byte)a.Level).CompareTo((byte)b.Level));

        foreach (var key in keys)
        {
            var  list  = buckets[key];
            if (list.Count < 2) continue;

            // Oldest first, so a run is a time-contiguous slice of the bucket. A bucket too big
            // for one file has to be cut somehow, and cutting it by time is what makes the
            // pieces useful: files that partition their bucket prune by window and expire in
            // sequence, where files that interleave it all span the whole bucket, are all opened
            // by every query into it, and all carry the bucket's newest timestamp — so its
            // oldest events over-retain by the full bucket width instead of by one file's share
            // of it. Measured on the stand shape at 1/16 (see DayBucketCompactionProbe): 5.72 d
            // of span per Information file when the batch was picked smallest-first, 1.6 d when
            // picked oldest-first.
            list.Sort(static (a, b) => a.MinTimestampTicks.CompareTo(b.MinTimestampTicks));

            var run = SelectMergeRun(list, MinSourcesFor(key, policy, now), target, maximal);
            if (run is not null) return run;
        }

        // NOTHING in the whole catalog can be merged without stepping over a source. Only now is
        // the overlap worth it: group each bucket by size tier and take a run inside one tier.
        // Second loop rather than second branch, so a bucket that can still be compacted
        // cleanly is ALWAYS preferred over one that cannot — the fallback fires at what would
        // otherwise be a permanent fixpoint, which is the only place its cost is the lesser one.
        foreach (var key in keys)
        {
            var list = buckets[key];        // already sorted by Min above
            if (list.Count < 2) continue;

            var run = SelectMergeTierRun(list, MinSourcesFor(key, policy, now), target, maximal);
            if (run is not null) return run;
        }
        return null;

        // A bucket past its window plus a grace needs only a pair: nothing more is coming that
        // could make a bigger batch, so waiting for the open fanout would strand what is there.
        static int MinSourcesFor((Ameto.Core.LogLevel Level, long Start) key, RetentionPolicy policy, long now)
        {
            long width = MergeBucketTicks(policy.GetTtl(key.Level));
            return now - (key.Start + width) >= MergeSealGraceTicks(width)
                ? MergeSealedMinSources
                : MergeMinSources;
        }
    }

    /// <summary>
    /// The first time-contiguous run in <paramref name="byTime"/> that is worth rewriting, or
    /// null if the bucket holds none.
    ///
    /// <para>A run is grown from each start in turn and STOPS at the first file it cannot take —
    /// it never steps over one. Skipping was the previous behaviour and it broke the property
    /// the oldest-first ordering exists to give: the merged file spanned right across the source
    /// it had skipped, so the two overlapped and every query into that window opened both.</para>
    ///
    /// <para>Three conditions decide whether the run is worth it, and each answers a measured
    /// failure:</para>
    /// <list type="bullet">
    /// <item>SIZE SPREAD — the run's largest may be at most <see cref="MergeRunSizeRatio"/>×
    ///       its smallest. Without it a single late row was admissible beside the bucket's
    ///       collapsed file (5 stragglers ⇒ 5 merges, 7151 KB written, cost never decaying).</item>
    /// <item>GROWTH — the rest of the run must add up to at least
    ///       1/<see cref="MergeGrowthFactor"/> of its largest member, so a merge always makes
    ///       real progress up the size ladder and a big file is only rewritten when a comparable
    ///       amount of new data has arrived to pay for it.</item>
    /// <item>FANOUT — <paramref name="minSources"/> files, OR a payload that already fills the
    ///       target. The fallback is not a loophole, it is the fix for a stall: the count used to
    ///       be tested AFTER the payload budget had truncated the batch, so an open bucket whose
    ///       files had grown past target/8 could never assemble a legal batch again (measured: 40
    ///       segments, 0 merges). A run that fills the target produces a MAXIMAL file, which is
    ///       the last rewrite those bytes will ever get — always worth doing.</item>
    /// </list>
    /// </summary>
    internal static List<SegmentInfo>? SelectMergeRun(
        List<SegmentInfo> byTime, int minSources, long target, long maximal)
    {
        // ONE scratch list for the whole scan. Every start index has to be tried — a run that
        // stops at j says nothing about the starts inside (start, j): with payloads [1, 4, 16]
        // the run from 0 is [1, 4] because 16 > 1 × ratio, while the run from 1 is the legal
        // [4, 16] — so the O(n²) walk is load-bearing and stays. Allocating a list per attempt
        // made it O(n²) GARBAGE as well: measured 5.5 MB and 2.4 ms per planner pass at 1600
        // files in one bucket, every pass returning null. The list escapes on exactly one path,
        // and the method allocates a fresh one per call, so the caller owns what it gets.
        //
        // The n that O(n²) is quadratic in is BOUNDED, which is why it needs no memoisation on
        // top: a bucket only survives a pass with files left over if every tier of it holds
        // fewer than minSources (3 same-tier files always satisfy growth), so at fixpoint a
        // (level, bucket) group holds under minSources × 32 files whatever arrives — 224 in an
        // open bucket, 64 in a sealed one. Thousands of stuck files in one group was a symptom
        // of the stranding bug, not a state the planner can now reach.
        var run = new List<SegmentInfo>(Math.Min(byTime.Count, MergeMaxSources));

        for (int start = 0; start + 1 < byTime.Count; start++)
        {
            run.Clear();
            long payload = 0, events = 0, runMin = long.MaxValue, runMax = 0;

            for (int i = start; i < byTime.Count && run.Count < MergeMaxSources; i++)
            {
                var  s = byTime[i];
                long p = Math.Max(1, SegmentPayloadBytes(s));
                long lo = Math.Min(runMin, p), hi = Math.Max(runMax, p);
                if (run.Count > 0 && (hi > lo * MergeRunSizeRatio ||
                                      payload + p > target ||
                                      events + s.EventCount > MergeMaxEvents)) break;

                run.Add(s);
                payload += p;
                events  += s.EventCount;
                runMin = lo; runMax = hi;
            }

            if (run.Count < 2) continue;
            if (payload - runMax < runMax / MergeGrowthFactor) continue;
            if (run.Count < minSources && payload < maximal) continue;
            return run;
        }
        return null;
    }

    /// <summary>
    /// The fallback: the first run worth rewriting among the files of ONE SIZE TIER, or null.
    ///
    /// <para>Time-contiguity across the whole bucket is what
    /// <see cref="SelectMergeRun"/> gives up here, and only here. A file whose time-neighbours
    /// are all more than <see cref="MergeRunSizeRatio"/> away in size is unmergeable under the
    /// contiguous rule — it cannot even reach a file of its own exact size, because the run
    /// would have to step over the neighbour — and a producer whose volume swings between
    /// adjacent flushes builds that shape by itself. MEASURED at 30b0d93: 240 flushes
    /// alternating 200 and 40 events into a bucket sealed 30 days ago left 240 files and 0
    /// merges, at fixpoint; six byte-identical 172 B files in one bucket refused to coalesce
    /// because a 244 KB file sat between each pair in time order.</para>
    ///
    /// <para>Grouping by tier restores the property the size rule was supposed to have: files
    /// of a size merge with files of that size. The three conditions are unchanged — the tier
    /// simply satisfies the spread one by construction — and the run inside a tier is still
    /// time-contiguous WITHIN the tier, so the pieces of an over-large tier still partition it.
    /// Smallest tier first: it is the cheapest progress per file removed, and it is what lets a
    /// straggler ladder coalesce among itself while the bucket's collapsed file, several tiers
    /// up, is never a partner for it.</para>
    /// </summary>
    internal static List<SegmentInfo>? SelectMergeTierRun(
        List<SegmentInfo> byTime, int minSources, long target, long maximal)
    {
        int lowest = int.MaxValue, highest = int.MinValue;
        foreach (var s in byTime)
        {
            int t = SizeTier(SegmentPayloadBytes(s));
            if (t < lowest)  lowest  = t;
            if (t > highest) highest = t;
        }
        if (lowest == highest) return null;   // one tier ⇒ SelectMergeRun already saw this list

        var tier = new List<SegmentInfo>(byTime.Count);
        for (int t = lowest; t <= highest; t++)
        {
            tier.Clear();
            foreach (var s in byTime)
                if (SizeTier(SegmentPayloadBytes(s)) == t) tier.Add(s);
            if (tier.Count < 2) continue;

            // byTime is ordered by Min, so tier is too: the run is still oldest-first.
            var run = SelectMergeRun(tier, minSources, target, maximal);
            if (run is not null) return run;
        }
        return null;
    }

    /// <summary>
    /// Merges one batch of small, time-adjacent cold segments into a single large segment by
    /// STREAMING them: the sources are read as sorted event streams, merged with a heap on
    /// (timestamp, id) and written straight through <see cref="SegmentWriter"/>, preserving
    /// event ids, timestamps and raw property/exception payloads.
    ///
    /// <para>Nothing is materialised. The previous shape — read every source with
    /// <c>ReadAllRaw</c>, copy the batch into a <see cref="HotTierSegment"/>, index it whole —
    /// peaked at ~3× the batch, which is why the batch had to be capped at 32 MB and why the
    /// tier's fixed chunk geometry excluded dense segments from compaction entirely.</para>
    ///
    /// <para>The batch comes from <see cref="SelectMergeBatch"/>: a time-contiguous run of
    /// similarly-sized files inside one (level, expiry bucket) group. Level purity keeps expiry
    /// exact — expiry is <c>MaxTimestamp + Ttl(MinLevel)</c>, and merging a 3-day Debug segment
    /// into a 90-day one would either delete its neighbours early or keep the Debug rows 30×
    /// longer — while the size run is what lets the sweep FINISH: every merge multiplies its
    /// sources' size, so a file reaches <see cref="MergeSealedSourceBytes"/> after a bounded
    /// number of rewrites and then leaves the candidate set for good.</para>
    ///
    /// <para>Crash-safe, and the ORDER is the proof. A manifest listing the source files is
    /// written first; the merged file is built at <c>.seg.tmp</c> (which the startup scan
    /// deletes) and only then moved to a name the catalog can see; the sources are deleted
    /// after that; the manifest is dropped only once every one of them is confirmed gone. So a
    /// merged file never exists beside its un-deleted sources without a manifest naming them,
    /// and a manifest never names sources that are not already duplicated. Recovery reads both
    /// halves: merged file present ⇒ finish deleting, absent ⇒ the merge never committed.</para>
    ///
    /// Returns true when a batch was merged.
    /// </summary>
    internal async Task<bool> TryMergeSmallSegmentsOnceAsync(CancellationToken ct)
    {
        // Never produce index-less segments: the builder is wired by a hosted
        // service shortly after startup — if it isn't there yet, just wait.
        if (IndexSinkFactory is null && !_allowIndexlessMerge) return false;

        // A skipped bucket used to burn the whole maintenance pause (600 s) on a
        // single discarded anchor. Skips are rare — what remains is unreadable or
        // empty segments — so when one happens, re-select immediately. Bounded and
        // livelock-free: every failed attempt either quarantines (corruption) or defers its
        // anchor for the rest of THIS pass, so the candidate set strictly shrinks within the
        // window loop either way. The deferral set is cleared at the top of the pass.
        _mergePassDeferred.Clear();

        // Prune bookkeeping for segments that no longer exist. Retention deletes catalog
        // entries from its own threads, so the delete site cannot touch these two non-
        // thread-safe collections; this is the one place that runs on the maintenance
        // thread alone. Without it, a segment that expired carrying strikes — or sitting
        // in the skip-list — left its key there for the life of the process. _segments is
        // a ConcurrentDictionary, safe to read from any thread; Dictionary and HashSet
        // both permit Remove during their own enumeration.
        foreach (var stale in _mergeDeferStrikes)
            if (!_segments.ContainsKey(stale.Key))
                _mergeDeferStrikes.Remove(stale.Key);
        foreach (var stale in _mergeSkip)
            if (!_segments.ContainsKey(stale))
                _mergeSkip.Remove(stale);

        List<SegmentInfo>?   consumed = null;
        List<SegmentReader>? readers  = null;
        for (int attempt = 0; attempt < MergeWindowAttempts && consumed is null; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            // Recomputed per attempt: _mergeSkip may have grown.
            var sources = SelectMergeBatch();
            if (sources is null) return false;

            // Open every source BEFORE anything is written. An unreadable file is skip-listed
            // individually — a persistently corrupt segment would otherwise be re-selected by
            // every future pass — and the batch continues with what opened, exactly as the
            // read-everything planner did. Discovering it after the manifest is on disk would
            // mean unwinding published state instead of simply choosing a different window.
            var opened = new List<SegmentReader>(sources.Count);
            var usable = new List<SegmentInfo>(sources.Count);
            long usableEvents = 0;
            foreach (var seg in sources)
            {
                try
                {
                    opened.Add(SegmentReader.Open(seg.FilePath));
                    usable.Add(seg);
                    usableEvents += seg.EventCount;
                }
                catch (Exception ex)
                {
                    // Quarantine is for corruption -- a property of the bytes, which will not
                    // heal. Anything else is circumstance: a file mid-rename inside an import's
                    // publish window, a share violation from a concurrent reader. Those keep
                    // their place and are retried next pass -- the same rule the streaming merge
                    // applies to a source that fails mid-stream.
                    if (IsSourceCorruption(ex))
                    {
                        _mergeSkip.Add(SegmentKey.Of(seg));
                        _logger.LogWarning(ex, "Merge: quarantining corrupt segment {File}", seg.FilePath);
                    }
                    else
                        // Debug, not Warning: this repeats every pass until the circumstance
                        // clears, and the maintenance loop walks every 15 seconds -- the old
                        // line warned exactly once because the skip-list also stopped the
                        // reselection, and a Warning that fires forever drowns what it carries.
                        // There is no retry limit, deliberately: the skip-list is the only
                        // permanent exclusion state, and it is reserved for what will not heal.
                        _logger.LogDebug(ex, "Merge: segment {File} failed to open -- kept for the next pass", seg.FilePath);
                }
            }

            // Anti-stall: a bucket whose anchor can't produce even a 2-segment batch would be
            // re-selected forever — defer the anchor for the pass, and escalate to the
            // skip-list after MergeDeferEscalationPasses (the branch below tells the story).
            // Debug, not Warning: this is the planner's EXPECTED outcome whenever there is
            // simply nothing to merge (a day's log carried 54 of these, every anchor
            // different) — at WRN it drowns the signal it was meant to be.
            if (usable.Count < 2 || usableEvents == 0)
            {
                foreach (var r in opened) r.Dispose();
                // Deferred for the PASS, with an escalation counter. Deferral alone fixed one
                // broken bucket and moved the stall to the window threshold: each broken bucket
                // still ate a re-selection attempt every pass, so MergeWindowAttempts of them
                // starved everything behind. A bucket deferred MergeDeferEscalationPasses
                // passes in a row is not blinking -- it joins the skip-list, stops costing
                // attempts, and says so at Warning once. (Its first replacement, a batch-wide
                // "was anything transient" flag, excluded nothing on FileNotFound and one
                // operator-deleted file stalled the whole catalog; permanent quarantine before
                // that made a mid-rename blink permanent. This is the middle both rounds were
                // reaching for.) Corruption is already in the skip-list from the loop above.
                var anchorKey = SegmentKey.Of(sources[0]);
                int strikes = _mergeDeferStrikes.GetValueOrDefault(anchorKey) + 1;
                if (strikes >= MergeDeferEscalationPasses)
                {
                    _mergeDeferStrikes.Remove(anchorKey);
                    _mergeSkip.Add(anchorKey);
                    _logger.LogWarning(
                        "Merge: bucket anchored at {File} has failed to assemble a batch for {Passes} passes — " +
                        "anchor skip-listed until restart so it stops costing re-selection attempts",
                        Path.GetFileName(sources[0].FilePath), strikes);
                }
                else
                    _mergeDeferStrikes[anchorKey] = strikes;
                _mergePassDeferred.Add(anchorKey);
                _logger.LogDebug("Merge: bucket anchored at {File} yields no usable batch — anchor skipped",
                    Path.GetFileName(sources[0].FilePath));
                continue;
            }
            consumed = usable;
            readers  = opened;
        }
        if (consumed is null || readers is null) return false;

        // Reserve a segment id from the same allocator the flush path uses. Safe now only
        // because the live WAL holds a RESERVED block (see _walSegId): the allocator is
        // already past it, so a merged file can never be handed the id a WAL is named from.
        //
        // The flush lock is no longer what makes the reservation atomic — AllocateSegmentId
        // holds _segIdLock for that, and has to, because the import path cannot take a
        // SemaphoreSlim. It is still taken here because this await is the one cancellable step
        // between opening the sources and taking ownership of them, and losing that would leak
        // mapped views on shutdown (see the catch).
        ulong reserved;
        try
        {
            await _flushLock.WaitAsync(ct);
        }
        catch
        {
            // Unguarded, shutdown left up to MergeMaxSources mapped views alive for the life of
            // the process — and on Windows a mapped file cannot be unlinked, so those segments
            // could then be neither compacted nor expired.
            foreach (var r in readers) r.Dispose();
            throw;
        }
        try { reserved = AllocateSegmentId(); }
        finally { _flushLock.Release(); }

        var  segId        = new SegmentId(reserved);
        long expectEvents = 0, minTs = long.MaxValue, maxTs = long.MinValue;
        foreach (var s in consumed)
        {
            expectEvents += s.EventCount;
            if (s.MinTimestampTicks < minTs) minTs = s.MinTimestampTicks;
            if (s.MaxTimestampTicks > maxTs) maxTs = s.MaxTimestampTicks;
        }
        var segPath = Path.Combine(_segDir, $"{_options.NodeId.Value}-{segId.Value}-{minTs}-{maxTs}.seg");
        string manifestPath = segPath + ".mergemanifest";

        // ── MANIFEST FIRST. The merged segment only becomes visible to the catalog when it
        //    is MOVED to segPath, and that move happens after this line — so at no instant
        //    does a .seg exist on disk whose sources are still there without a manifest
        //    naming them. Recovery reads it both ways: manifest without the merged file =
        //    a merge that never committed (drop the manifest, the sources are untouched);
        //    manifest WITH it = the sources are already duplicated (finish deleting them).
        //    Writing it after publication, as this did before, left a window where a crash
        //    resurrected every source alongside the merged file — duplicate events, forever.
        //    Written THROUGH to the platter, for the same reason the merged file is: the whole
        //    protocol is an ordering between this file and a set of unlinks, and an ordering
        //    only the page cache observes does not survive a power loss.
        try
        {
            await using (var mf = new FileStream(manifestPath, FileMode.Create, FileAccess.Write, FileShare.None,
                                                 4096, FileOptions.WriteThrough))
            using (var mw = new StreamWriter(mf))
            {
                foreach (var s in consumed) await mw.WriteLineAsync(Path.GetFileName(s.FilePath));
                await mw.FlushAsync(ct);
                mf.Flush(flushToDisk: true);
            }
        }
        catch
        {
            // The readers were opened by the planner; MergeToColdAsync takes ownership of them
            // and it is never reached from here.
            foreach (var r in readers) r.Dispose();
            try { File.Delete(manifestPath); } catch { }
            throw;
        }

        // Take a flush slot for the heavy phase: a merge runs the SAME index build +
        // compress + write pipeline as an ingest flush. Running it outside _flushConcurrency
        // meant the ceiling logged at startup (width × per-flush managed) was not the
        // ceiling actually enforced. _flushSlots is deliberately NOT taken — that gate is
        // the ingest back-pressure signal, and parking compaction behind it would let
        // sustained ingest starve the sweep that keeps the segment count down.
        SegmentInfo info;
        try
        {
            await _flushConcurrency.WaitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            foreach (var r in readers) r.Dispose();
            try { File.Delete(manifestPath); } catch { }
            throw;
        }
        try
        {
            _beforeMergeStream?.Invoke();
            info = await MergeToColdAsync(readers, segId, segPath, expectEvents, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown is not a verdict on the batch. Swept up with everything else it logged a
            // WARNING on every single stop, swallowed the cancellation the maintenance loop
            // stops on, and quarantined the window — so the segments a clean restart had every
            // reason to merge first were the ones it then refused to look at.
            foreach (var r in readers) r.Dispose();
            try { File.Delete(manifestPath); } catch { /* recovery drops it anyway */ }
            throw;
        }
        catch (Exception ex)
        {
            // Nothing has been deleted and the merged file never reached segPath, so the only
            // state to undo is the manifest.
            //
            // Skip-listing is QUARANTINE and it lasts until the process restarts, so it belongs
            // to a batch that CANNOT be merged, not to one that could not be merged now. Corrupt
            // sources are the first kind: the stream fails the same way on every future pass,
            // and because the merge reads all of them interleaved there is no telling which file
            // is the bad one, so the window goes as a whole. A disk that filled up or a network
            // volume that blinked is the second: retiring up to MergeMaxSources segments over a
            // condition that clears itself would end compaction for that bucket for the life of
            // the process, and the small-file backlog those segments form is the exact thing the
            // sweep exists to remove. Left in the candidate set, the next pass simply retries.
            //
            // Disposing here as well as in the merge task is deliberate, and it is the same
            // discipline the two waits above follow: MergeToColdAsync only takes ownership of the
            // readers once its delegate is entered, and from out here there is no way to know
            // whether it was. Dispose is idempotent, so the rule can simply be that every exit
            // from this method that does not publish closes them.
            foreach (var r in readers) r.Dispose();
            if (IsSourceCorruption(ex))
            {
                foreach (var s in consumed) _mergeSkip.Add(SegmentKey.Of(s));
                _logger.LogWarning(ex,
                    "Merge: {Count} source(s) unreadable while streaming — sources left intact, batch skip-listed until restart",
                    consumed.Count);
            }
            else
            {
                // Debug for the same reason the planner's open loop speaks at Debug on a retry:
                // this repeats every maintenance pass until the circumstance clears, and there
                // is no retry limit here -- the skip-list is the only PERMANENT exclusion
                // state, reserved for corruption and for anchors that struck out (see
                // _mergeDeferStrikes). A Warning here fired on every pass, which is the exact
                // noise this change-set claims to have removed.
                _logger.LogDebug(ex,
                    "Merge: aborted while streaming {Count} source(s) — sources left intact, batch retried next pass",
                    consumed.Count);
            }
            try { File.Delete(manifestPath); } catch { /* recovery drops it anyway */ }
            return false;
        }
        finally { _flushConcurrency.Release(); }

        PublishLocalSegment(info);
        foreach (var seg in consumed)
        {
            _mergeDeferStrikes.Remove(SegmentKey.Of(seg));
            // Before the delete, so a header scan that finds the entry gone finds the record
            // too, and calls the count it gives a floor rather than presenting it as complete:
            // this source's events are in the output just published, which a scan already
            // running does not list. The output is named with it, because a scan that started
            // after the publish above DOES list it and reads the events there. Retention deletes
            // are not recorded; their events are gone.
            RecordMergedAwaySegment(SegmentKey.Of(seg), SegmentKey.Of(info));
            await DeleteSegmentAsync(SegmentKey.Of(seg), ct);
        }

        // Drop the manifest only when every source file is confirmed gone. A
        // source held open by an in-flight query survives File.Delete — the
        // manifest then stays behind and the recovery sweep (each maintenance
        // iteration + startup) finishes the deletion once the reader closes.
        // Deleting it unconditionally would resurrect those files as
        // duplicate segments after a restart.
        bool allGone = consumed.All(s => !File.Exists(s.FilePath));
        if (allGone)
            try { File.Delete(manifestPath); } catch { /* re-processed harmlessly later */ }
        else
            _logger.LogWarning("Merge: {Count} source file(s) still held open — manifest kept for the recovery sweep",
                consumed.Count(s => File.Exists(s.FilePath)));

        _logger.LogInformation(
            "Merged {Sources} small segments ({Events} events) into {File} ({Mb:F1} MB)",
            consumed.Count, info.EventCount, Path.GetFileName(segPath), info.CompressedBytes / 1048576.0);
        return true;
    }

    /// <summary>
    /// Tells a batch that will never merge from one that merely did not merge this time.
    ///
    /// <para><see cref="InvalidDataException"/> is what every structural check throws — footer
    /// and header magic, an unsupported version, a block whose stored length does not match its
    /// frame, a file too short to hold a footer, and the merge's own event-count verification.
    /// All properties of the bytes on disk, still true on the next pass. Everything else — no
    /// space left, an I/O error, a file momentarily locked or mid-rename — is the machine's
    /// condition rather than the segment's, and gets another attempt.
    /// <see cref="EndOfStreamException"/> is thrown by the MessagePack decoder on a truncated
    /// exception column — corruption that <c>SegmentReader.Open</c> PASSES, because the file's
    /// structure (header, footer, block index, LZ4 frames) is intact and only the column's
    /// payload is torn. It was removed from this list once on the strength of a grep over src/,
    /// which does not see into dependencies; the streaming merge then retried such a file
    /// forever, warning every pass.</para>
    /// </summary>
    private static bool IsSourceCorruption(Exception ex) => ex is InvalidDataException or EndOfStreamException;

    /// <summary>
    /// Streams the sources through a k-way merge straight into a new segment file.
    ///
    /// <para>Nothing between the source blocks and the output block is retained: the writer
    /// pulls one event at a time, copies it into the open block and pushes it into the open
    /// index group's sink. Peak is one decompressed block per source plus one index group —
    /// flat in the merged segment's size, which is the whole point.</para>
    /// </summary>
    /// <param name="expectEvents">
    /// Sum of the sources' header event counts. Verified while the merged file is still at
    /// <c>.seg.tmp</c> — BEFORE the move that makes it catalog-visible, because recovery decides
    /// on <c>File.Exists</c> alone: a crash between a move and a later check would commit an
    /// unverified merge and recovery would then finish deleting its sources for it.
    /// </param>
    /// <remarks>
    /// Internal rather than private so its reader-ownership contract can be tested where it is
    /// actually made. Driven through <see cref="TryMergeSmallSegmentsOnceAsync"/> the contract is
    /// invisible: that method's own abort paths close the readers too, deliberately, so a test
    /// that cancels a whole merge pass cannot tell which of the two did it — and passes just as
    /// well when this one does nothing at all.
    /// </remarks>
    internal Task<SegmentInfo> MergeToColdAsync(
        List<SegmentReader> readers, SegmentId segId, string segPath, long expectEvents, CancellationToken ct)
    {
        var  sinkFactory = IndexSinkFactory;
        long groupBudget = _groupPayloadBudgetBytes;

        return Task.Run(() =>
        {
            // The .tmp suffix is load-bearing: the catalog scan deletes leftover *.seg.tmp at
            // startup, so a crash any time before the Move leaves nothing to recover from.
            string tmpPath = segPath + ".tmp";
            MergingSegmentEventSource? source = null;
            try
            {
                // Cancellation is observed HERE, and the token is deliberately NOT handed to
                // Task.Run: a Task.Run whose token is already signalled transitions the task
                // straight to Canceled WITHOUT ever invoking the delegate — and the delegate is
                // the only place the readers this method took ownership of are closed. Cancelling
                // in the window between _flushConcurrency.WaitAsync returning and the delegate
                // being scheduled therefore left up to MergeMaxSources mapped views alive for the
                // life of the process, and a mapped file on Windows can be neither compacted nor
                // unlinked by retention. That is the very hazard the wait's own catch guards
                // against one step earlier; this closes the second door into it.
                ct.ThrowIfCancellationRequested();

                SegmentInfo info;
                source = new MergingSegmentEventSource(readers);
                // HC HAPPENS HERE, and only here. A merge already rewrites every block it reads,
                // it is off the ingest path, and its output is the long-lived file — so the extra
                // encode is paid once, on a background pass, against a file that is read and kept
                // until retention deletes it. The flush path stays on the fast level: its output
                // is short-lived and this merge is what rewrites it.
                //
                // MEASURED (MergeCompressionProbe, 48k trace-carrying prop-dense events): the
                // blocks shrink 12.8 % (26.5 % on body-logging events), the FILE shrinks 2.8 %
                // because index sections are 78-90 % of it, and the merge costs ~18 % more wall
                // clock — on the stand's volume, seconds of one core a day.
                using (var writer = new SegmentWriter(tmpPath, groupBudget, SegmentCompression.High))
                {
                    writer.WriteEvents(source, sinkFactory, ct);
                    info = writer.Finalise(_options.NodeId, segId);
                }
                // Close the readers BEFORE the caller starts deleting sources: on Windows a
                // mapped file cannot be unlinked, and a leaked view would leave the merge
                // permanently stuck in its "sources still held open" recovery path.
                source.Dispose();
                source = null;
                foreach (var r in readers) r.Dispose();

                // Refuse to publish unless every source event is in the merged file. Counts come
                // from file headers on both sides, so a mismatch means the stream lost or
                // duplicated rows. Throwing here leaves the file at .seg.tmp — invisible to the
                // catalog, deleted by the startup sweep — and the caller drops the manifest, so
                // the pre-merge state is restored exactly.
                if (info.EventCount != expectEvents)
                    throw new InvalidDataException(
                        $"merge wrote {info.EventCount} events but its sources hold {expectEvents}");

                ct.ThrowIfCancellationRequested();
                File.Move(tmpPath, segPath, overwrite: false);
                return new SegmentInfo
                {
                    Id                = info.Id,
                    NodeId            = info.NodeId,
                    FilePath          = segPath,
                    MinTimestampTicks = info.MinTimestampTicks,
                    MaxTimestampTicks = info.MaxTimestampTicks,
                    EventCount        = info.EventCount,
                    MinLevel          = info.MinLevel,
                    CompressedBytes   = info.CompressedBytes,
                    UncompressedBytes = info.UncompressedBytes,
                };
            }
            catch
            {
                try { File.Delete(tmpPath); } catch { /* best-effort cleanup */ }
                throw;
            }
            finally
            {
                source?.Dispose();
                foreach (var r in readers) r.Dispose();   // idempotent — safe after the happy path
            }
        });
    }

    /// <summary>
    /// Finishes merges interrupted mid-deletion: a manifest whose merged segment
    /// exists means the listed source files are already duplicated — delete them.
    /// A manifest without its merged segment is a merge that never committed.
    /// </summary>
    private void RecoverInterruptedMerges()
    {
        foreach (var manifest in Directory.EnumerateFiles(_segDir, "*.mergemanifest"))
        {
            try
            {
                string mergedSeg = manifest[..^".mergemanifest".Length];
                bool   allGone   = true;
                if (File.Exists(mergedSeg))
                {
                    foreach (var name in File.ReadAllLines(manifest))
                    {
                        var src = Path.Combine(_segDir, name);
                        try { if (File.Exists(src)) File.Delete(src); }
                        catch (Exception ex) { _logger.LogWarning(ex, "Merge recovery: failed to delete {File}", src); }
                        if (File.Exists(src)) allGone = false;
                    }
                    if (allGone)
                        _logger.LogInformation("Merge recovery: completed interrupted merge for {File}", Path.GetFileName(mergedSeg));
                }
                // Keep the manifest while any duplicate source survives (a reader
                // may still hold it open) — the next sweep retries.
                if (allGone) File.Delete(manifest);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Merge recovery failed for {Manifest}", manifest);
            }
        }
    }

    /// <summary>
    /// Writes a frozen tier as ONE SEGMENT PER LOG LEVEL, so every segment holds a single
    /// level and its retention deadline is exact rather than governed by whichever level
    /// happened to have the lowest enum value inside it.
    ///
    /// <para>The tier is sorted once; the order is then partitioned by level, which keeps
    /// each level's subsequence sorted by (ts, id) — everything downstream (block order,
    /// the query k-way merge, cursor pagination) is unaffected. Levels absent from the
    /// tier produce no file. The level's id is <c>firstSegId + (byte)level</c>, matching
    /// the block of ids reserved at freeze so a concurrent query skips them all.</para>
    ///
    /// <para>CRASH-SAFE VIA A COMPLETION MARKER, and like the merge protocol the ORDER is the
    /// proof. Each level is built at <c>.seg.tmp</c> and moved into place one at a time, so
    /// between the first move and the last the directory holds a PARTIAL set — and the WAL that
    /// still holds every one of those events is only deleted afterwards. Recovery therefore
    /// cannot read "some segment of this block exists" as "the flush finished": that predicate
    /// answers true for a set of one, and the WAL it then deletes is the only copy of the levels
    /// that never got published. So the last thing this method does, after every move, is write
    /// and FSYNC <c>{nodeId}-{firstSegId}.flushed</c>. A present marker — nothing else — means
    /// the block is complete, and it is dropped together with the WAL it authorises.</para>
    /// </summary>
    /// <param name="skipPublishedLevels">
    /// Recovery only. A level is published by ONE move of ONE whole file, so a segment already
    /// sitting at <c>firstSegId + level</c> means that level is fully persisted and its rows must
    /// not be written a second time — the replayed tier fills in the missing levels and nothing
    /// else. This is what makes replaying a markerless WAL idempotent, which in turn is what lets
    /// a data directory written by the previous build (complete flush, no marker anywhere) be
    /// replayed without producing a single duplicate.
    /// </param>
    private async Task<List<SegmentInfo>> FlushTierByLevelAsync(
        HotTierSegment hot, ulong firstSegId, CancellationToken ct, bool skipPublishedLevels = false)
    {
        int[] order = SegmentWriter.ComputeSortOrder(hot);

        var perLevel = new int[LevelSegmentSlots][];
        var counts   = new int[LevelSegmentSlots];
        var written  = new List<SegmentInfo>(LevelSegmentSlots);
        try
        {
            SplitOrderByLevel(hot, order, perLevel, counts);

            for (int lvl = 0; lvl < LevelSegmentSlots; lvl++)
            {
                int n = counts[lvl];
                if (n == 0) continue;

                var segId = new SegmentId(firstSegId + (ulong)lvl);
                if (skipPublishedLevels && SegmentFileExists(segId.Value))
                {
                    _logger.LogInformation(
                        "WAL recovery: level {Level} is already published as segment {Id} — {Count} event(s) not rewritten",
                        (Ameto.Core.LogLevel)lvl, segId.Value, n);
                    continue;
                }

                // A RENTED array is longer than its level's event count, so every consumer
                // below is told how much of it is real. The writer would otherwise stage the
                // rent's tail — stale tier indices from a previous flush — as events.
                var subset  = perLevel[lvl];
                var segPath = BuildSegmentPath(segId, hot, subset, n);
                written.Add(await FlushToColdAsync(hot, segId, segPath, ct, subset, n));
                _afterLevelPublished?.Invoke(lvl);
            }
        }
        finally
        {
            // Returned only here: each level's array is read by the write it was handed to,
            // and that write is awaited inside the loop, so nothing below still holds one.
            for (int lvl = 0; lvl < LevelSegmentSlots; lvl++)
                if (perLevel[lvl] is { } a) ArrayPool<int>.Shared.Return(a);
        }

        // ── MARKER LAST. Every level is on disk and fsynced (SegmentWriter.Finalise flushes to
        //    the platter before the move), so this file is the durable "the block is complete"
        //    record the WAL delete keys off. Written THROUGH for the same reason the merge
        //    manifest is: the protocol is an ordering between these files and an unlink, and an
        //    ordering only the page cache observes does not survive a power loss.
        WriteFlushCompletionMarker(firstSegId, written);
        return written;
    }

    /// <summary>
    /// Partitions a tier's sort order by log level: <paramref name="perLevel"/>[l] comes back
    /// holding <paramref name="counts"/>[l] tier indices, still ascending by (timestamp, id).
    ///
    /// <para>COUNT, THEN FILL, into POOLED arrays. The straightforward
    /// <c>(perLevel[lvl] ??= new List&lt;int&gt;()).Add(…)</c> followed by <c>ToArray()</c> cost
    /// the dominant level three large-object copies per flush: the list doubling its way up to
    /// ~800 KB — every step past 85 KB an LOH allocation of immediately-dead bytes — and then
    /// one more full-size copy to hand the writer an array. Counting first costs one extra pass
    /// over a rented BYTE array, which is where the first pass parks each event's level, so the
    /// expensive part (a random <c>GetHeader</c> into a ~12.5 MB header set) still happens
    /// exactly once per event.</para>
    ///
    /// <para>The arrays come from <see cref="ArrayPool{T}"/> and are therefore LONGER than their
    /// level's count — every consumer must be told how much of one is real, or it stages the
    /// rent's tail (stale tier indices from an earlier flush) as events. The caller returns them.</para>
    /// </summary>
    internal static void SplitOrderByLevel(HotTierSegment hot, int[] order, int[]?[] perLevel, int[] counts)
    {
        var levels = ArrayPool<byte>.Shared.Rent(order.Length);
        try
        {
            for (int oi = 0; oi < order.Length; oi++)
            {
                int lvl = (int)hot.GetHeader(order[oi]).Level;
                if ((uint)lvl >= LevelSegmentSlots) lvl = (int)Ameto.Core.LogLevel.Information;   // defensive
                levels[oi] = (byte)lvl;
                counts[lvl]++;
            }

            for (int lvl = 0; lvl < LevelSegmentSlots; lvl++)
                if (counts[lvl] > 0) perLevel[lvl] = ArrayPool<int>.Shared.Rent(counts[lvl]);

            Span<int> fill = stackalloc int[LevelSegmentSlots];
            for (int oi = 0; oi < order.Length; oi++)
            {
                int lvl = levels[oi];
                perLevel[lvl]![fill[lvl]++] = order[oi];
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(levels);
        }
    }

    /// <summary>True when a segment file carrying <paramref name="segId"/> is on disk.</summary>
    private bool SegmentFileExists(ulong segId)
    {
        foreach (var _ in Directory.EnumerateFiles(_segDir, $"{_options.NodeId.Value}-{segId}-*.seg"))
            return true;
        return false;
    }

    /// <summary>
    /// Path of the record that a tier's whole level block reached disk. It lives beside the WAL
    /// it certifies rather than among the segments: its lifetime is exactly that WAL's — created
    /// after the last level is published, deleted immediately after the WAL — and the segment
    /// directory is enumerated by the catalog load, the merge planner and every retention pass.
    /// </summary>
    private string FlushMarkerPath(ulong firstSegId) =>
        Path.Combine(_walDir, $"{_options.NodeId.Value}-{firstSegId}.flushed");

    /// <summary>
    /// Writes and fsyncs the completion marker for a level block. Failure to write it is NOT
    /// fatal to the flush — the segments are already durable — it only costs a re-flush of the
    /// levels the next restart cannot prove were published, so it is logged and swallowed.
    /// </summary>
    private void WriteFlushCompletionMarker(ulong firstSegId, List<SegmentInfo> written)
    {
        string path = FlushMarkerPath(firstSegId);
        try
        {
            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None,
                                          4096, FileOptions.WriteThrough);
            using (var w = new StreamWriter(fs, leaveOpen: true))
            {
                // The names are for diagnostics only — recovery decides on the file's EXISTENCE,
                // exactly as merge recovery decides on the manifest's.
                for (int i = 0; i < written.Count; i++) w.WriteLine(Path.GetFileName(written[i].FilePath));
                w.Flush();
            }
            fs.Flush(flushToDisk: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to write flush completion marker {Path} — the WAL will be replayed on restart", path);
        }
    }

    /// <param name="orderCount">
    /// How many of <paramref name="order_"/>'s entries are this segment's, or -1 for all of it.
    /// A level-split flush hands over a POOLED array, which is longer than the level it holds.
    /// </param>
    private Task<SegmentInfo> FlushToColdAsync(
        HotTierSegment hot, SegmentId segId, string segPath, CancellationToken ct,
        int[]? order_ = null, int orderCount = -1)
    {
        // Capture delegate reference before entering Task.Run
        var sinkFactory  = IndexSinkFactory;
        long groupBudget = _groupPayloadBudgetBytes;
        return Task.Run(() =>
        {
            // One sort order shared by the index build and the block writer: posting-list
            // offsets become file ordinals, which the reader maps back to blocks/rows.
            // A caller-supplied order may be a SUBSET of the tier (level-split flush).
            int[] order = order_ ?? SegmentWriter.ComputeSortOrder(hot);
            int   count = orderCount >= 0 ? orderCount : order.Length;

            // The writer drives the index build now, one INDEX GROUP at a time: it knows
            // where the group's payload budget falls, and only it can interleave a group's
            // sections between its own blocks. Building the whole file up front is what
            // made index memory scale with segment size — the ceiling that kept segments
            // small in the first place.

            // Write to a temp file first; rename to final path only after Finalise()
            // succeeds. This prevents corrupt .seg files when the process is killed mid-flush.
            string tmpPath = segPath + ".tmp";
            try
            {
                SegmentInfo info;
                using (var writer = new SegmentWriter(tmpPath, groupBudget))
                {
                    writer.WriteEvents(new HotTierEventSource(hot, TemplatePool, order, 0, count), sinkFactory);
                    info = writer.Finalise(_options.NodeId, segId);
                } // FileStream closed here before Move
                File.Move(tmpPath, segPath, overwrite: false);
                // SegmentWriter captured tmpPath as FilePath; rewrite it to
                // point at the final segment file so subsequent queries can
                // open it. Without this, queries silently fail (file not
                // found) until the next restart re-scans the segment dir.
                return new SegmentInfo
                {
                    Id                = info.Id,
                    NodeId            = info.NodeId,
                    FilePath          = segPath,
                    MinTimestampTicks = info.MinTimestampTicks,
                    MaxTimestampTicks = info.MaxTimestampTicks,
                    EventCount        = info.EventCount,
                    MinLevel          = info.MinLevel,
                    CompressedBytes   = info.CompressedBytes,
                    UncompressedBytes = info.UncompressedBytes,
                };
            }
            catch
            {
                try { File.Delete(tmpPath); } catch { /* best-effort cleanup */ }
                throw;
            }
        }, ct);
    }

    // ── Retention ─────────────────────────────────────────────────────────────

    public async Task<RetentionRunResult> EnforceRetentionAsync(CancellationToken ct = default)
    {
        // Files an earlier pass could not unlink because a query held them open.
        RetryPendingSegmentDeletes();

        var now     = DateTimeOffset.UtcNow;
        var policy  = _retentionStore.GetPolicy();
        var expired = _segments.Values
            .Where(s => s.IsExpired(policy, now))
            .ToList();

        foreach (var seg in expired)
        {
            await DeleteSegmentAsync(SegmentKey.Of(seg), ct);
            _logger.LogInformation("Retention: deleted segment {Id} (expires {Max})", seg.Id, seg.MaxTimestamp);
        }

        return new RetentionRunResult(expired.Count, expired.Sum(s => s.CompressedBytes), 0, 0, now);
    }

    // ── Startup recovery ──────────────────────────────────────────────────────

    /// <summary>
    /// Seeds <see cref="_nextSegmentId"/> from segment file NAMES only — must run
    /// synchronously before the first flush so new segments never reuse an id,
    /// while the expensive per-file catalog load happens in the background.
    ///
    /// <para>Counts EVERY node's files, replicas included, which is deliberate rather than
    /// left over. Since the catalog is keyed by <see cref="SegmentKey"/> a peer's id is no
    /// longer something the local counter has to avoid, but <see cref="ImportSegment"/> raises
    /// the floor past an imported id at run time, and a restart that did not would let the
    /// allocator fall back behind ids it had already skipped.</para>
    /// </summary>
    private void InitNextSegmentIdFromFileNames()
    {
        foreach (var file in Directory.EnumerateFiles(_segDir, "*.seg"))
        {
            // {nodeId}-{segId}-{minTs}-{maxTs}.seg, or {nodeId}-{segId}.seg for a replica
            var parts = Path.GetFileNameWithoutExtension(file).Split('-');
            if (parts.Length >= 2 && ulong.TryParse(parts[1], out var segId))
                AdvanceSegmentIdFloor(segId + 1);
        }
    }

    /// <summary>
    /// Deletes temp files left behind by an interrupted flush, merge or re-compression.
    /// Called ONCE, from the constructor, before the background catalog scan is started and
    /// before any endpoint can stage a body -- see the constructor comment for why running
    /// this any later races the two live writers whose files match these masks.
    /// </summary>
    private void SweepLeftoverTempFiles()
    {
        foreach (var pattern in new[] { "*.seg.tmp", "*.hctmp" })
            foreach (var tmp in Directory.EnumerateFiles(_segDir, pattern))
            {
                try { File.Delete(tmp); }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete leftover temp segment {File}", tmp); }
            }
    }

    /// <summary>
    /// Rebuilds the catalog from the segments directory. Started by the constructor as a
    /// BACKGROUND task, so it runs with ingest and the HTTP endpoints already up.
    ///
    /// <para>internal, not private, so a test can put it under contention with an import
    /// directly — the same reason the segment-id allocator's three entry points are internal.
    /// Through the constructor there is nothing to time against: the scan is a task nobody can
    /// step, and by the time a test holds the engine it has usually finished.</para>
    /// </summary>
    internal void LoadSegmentCatalog()
    {
        // From before the directory is listed until the scan ends, every delete records its path
        // for it (see _deletedDuringCatalogScan). A delete before this line either unlinked its
        // file before the listing or parked it, and the scan sees both.
        lock (_scanDeleteGate)
        {
            _catalogScansRunning++;
            _deletedDuringCatalogScan ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
        try
        {
            LoadSegmentCatalogCore();
        }
        finally
        {
            // Recording stops with the last scan, and what it recorded goes with it.
            lock (_scanDeleteGate)
            {
                if (--_catalogScansRunning == 0) _deletedDuringCatalogScan = null;
            }
        }
    }

    private void LoadSegmentCatalogCore()
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Finish merges interrupted between publishing the merged segment and
        // deleting its sources — otherwise those events would be served twice.
        RecoverInterruptedMerges();

        // Sorted, and a key is claimed once. The assignment this replaces was unconditional and
        // ran in Directory.EnumerateFiles order, so a directory already holding two files under
        // one key dropped one of them from the catalog at every start — silently, and not
        // necessarily the same one twice. Leaving the catalog is leaving queries, retention and
        // the merge planner at once, since all three read it rather than the directory, so the
        // dropped file was served by nobody, expired by nothing and compacted by no merge while
        // holding its bytes for the life of the install. That is bug #43 in full, reached without
        // any import running: an install upgraded from a build that registered the intruder, an
        // endpoint whose unlink of a refused body failed, a restore from backup, an operator copy.
        //
        // One arrangement can produce it — a locally written {node}-{id}-{min}-{max}.seg and a
        // replica {node}-{id}.seg — because two files can share the replica name only by being
        // the same file. Ordinal order puts the local one first ('-' sorts below '.'), and that
        // is the file this node cannot get back: a replica's owner still holds it and can push it
        // again. It is also the same rule the import applies while running, which is what makes a
        // refusal survive the restart that follows it — the segment already held keeps the key.
        //
        // The loser is not deleted. Which of two files is the wrong one is not this node's to
        // decide, and the cost of being wrong is unrecoverable; the cost of keeping it is disk
        // and an error at every start, which is how an operator finds out at all.
        var files = Directory.GetFiles(_segDir, "*.seg");
        Array.Sort(files, StringComparer.Ordinal);
        foreach (var file in files)
        {
            try
            {
                // Closed before the gate: nothing below reads the file, and on Windows a mapping
                // held while waiting for the gate is what made a delete of this very file fail
                // its unlink and park.
                SegmentInfo info;
                using (var reader = SegmentReader.Open(file, computeUncompressedBytes: true))
                    info = reader.Info;
                var key = SegmentKey.Of(info);

                _beforeScanRegistersSegment?.Invoke(file);

                // Retention runs while this scan is still walking the directory, so a segment the
                // catalog has already let go can reach this line in two states, and neither may
                // be registered again:
                // - PARKED: the delete removed the entry, but its unlink failed because a query,
                //   or this scan while reading the file, held it open. Registering it put the
                //   expired segment back in service, and the parked retry then found the catalog
                //   naming its path, took that for a re-import and dropped the delete, until the
                //   next retention pass.
                // - DELETED: the delete ran while this scan was running, and recorded the path
                //   in _deletedDuringCatalogScan whatever its unlink did. Typically the unlink
                //   succeeded after this scan opened the file (Linux unlinks a mapped file;
                //   everywhere, once the reader above is closed), so nothing was parked, and
                //   registering it added an entry for a file that no longer exists: every header
                //   count over its window was Partial and every merge that picked it failed,
                //   until the next retention pass removed the entry. The record, not File.Exists,
                //   is what tells: File.Exists also says false when NFS or SMB fails the probe,
                //   and a live segment skipped on that stayed unserved until the next restart.
                //
                // Under _scanDeleteGate, which DeleteSegmentAsync holds across removing the entry,
                // recording the path, unlinking the file and parking a failed unlink. What that
                // gives is atomicity against a delete, not a fresh reading: the info above was
                // read before the gate and says nothing about a delete since. A delete of this key
                // is instead either wholly before these checks -- and left a park or a record,
                // both seen here -- or wholly after the add, and removes the entry the add made.
                // A delete from before the scan began recorded nothing: its unlink either
                // succeeded before the directory was listed, or was parked. What that does not
                // cover is such a delete whose unlink failed and was NOT parked (past
                // PendingSegmentDeleteCap, or an exception no retry fixes): the file is still on
                // disk, is registered again, and the next retention pass deletes it again. (The
                // same failure during the scan is recorded, so the file stays on disk unserved
                // until the next start, as it does when no scan runs.) Not under _importLock,
                // which an import holds across its publish while this scan must still be able to
                // land (see ImportSegment).
                bool parked, deleted, added;
                lock (_scanDeleteGate)
                {
                    parked  = _pendingSegmentDeletes.ContainsKey(file);
                    deleted = !parked && _deletedDuringCatalogScan?.Contains(file) == true;
                    added   = !parked && !deleted && _segments.TryAdd(key, info);
                }
                if (parked)
                {
                    _logger.LogDebug("Segment {File} is waiting for its delete to complete; the catalog scan skips it", file);
                    continue;
                }
                if (deleted)
                {
                    _logger.LogDebug("Segment {File} was deleted while the catalog scan was running; the scan skips it", file);
                    continue;
                }
                if (added) continue;

                // A live flush or import may have registered this very file while the scan was
                // running — the scan is a background task, not a barrier — so only a genuinely
                // different segment under the key is the collision being reported.
                if (_segments.TryGetValue(key, out var kept))
                {
                    if (!IsTheSameSegment(kept, info))
                        _logger.LogError(
                            "Segment {File} carries {Key}, which is already held by {Existing}. Two nodes " +
                            "are configured as NodeId {Node}, or a file was copied in from another node. " +
                            "The first is served, retained and merged; this one is NOT, and holds its disk " +
                            "until one of the two is removed or the nodes are renumbered.",
                            file, key, kept.FilePath, info.NodeId);
                    else
                        // The same FILE: an import or a flush publication beat the scan to the
                        // key, and the five-field comparison includes the path, so two DISTINCT
                        // files can never land here -- they differ in path and take the error
                        // branch above. Debug, because this is the expected overlap of a
                        // background scan with a live writer, and nothing is lost or unserved.
                        _logger.LogDebug(
                            "Segment {File} was already registered under {Key} before the scan reached it.",
                            file, key);
                }
            }
            catch (Exception ex)
            {
                // Renamed aside, NOT deleted. The delete was written when "unreadable" meant a
                // header or footer that nothing could ever parse; the reader now also throws on
                // one torn block FRAME -- four bad bytes in a file whose every other block is
                // readable -- and an unlink here turned that into losing the whole segment at
                // the next start. Error, not Warning: an operator must decide what a
                // .seg.corrupt file is worth, and nothing else will ever mention it again --
                // the catalog scan, the merge planner and retention all enumerate *.seg.
                _logger.LogError(ex, "Quarantining unreadable segment {File} as .corrupt — kept on disk, served by nobody", file);
                try
                {
                    string aside = file + ".corrupt";
                    if (File.Exists(aside)) File.Delete(aside);   // keep one generation of quarantine
                    File.Move(file, aside);
                }
                catch (Exception mvEx) { _logger.LogWarning(mvEx, "Failed to quarantine corrupt segment {File}", file); }
            }
        }
        _logger.LogInformation("Loaded {Count} segments from {Dir} in {Ms} ms", _segments.Count, _segDir, sw.ElapsedMilliseconds);
    }

    /// <summary>
    /// Replays every WAL left behind by a previous process, one at a time, then clears markers
    /// whose WAL is already gone.
    ///
    /// <para>A WAL is dropped only against a COMPLETION MARKER. It used to be dropped against
    /// "some segment inside its reserved block exists", which was exact only while a tier
    /// flushed to a single file. It now flushes to one file per level, published one move at a
    /// time, so that predicate answers true the instant the FIRST level lands — and the WAL it
    /// then deleted was the only copy of every level that had not been published yet. A crash,
    /// or an exception on one level, between the first move and the last was silent, total loss
    /// of the rest of the tier.</para>
    /// </summary>
    private void ReplayOrphanedWals()
    {
        foreach (var walFile in Directory.EnumerateFiles(_walDir, "*.wal"))
        {
            // Per WAL, so one unreadable log cannot strand the others behind it.
            try { ReplayOrphanedWal(walFile); }
            catch (Exception ex) { _logger.LogError(ex, "WAL recovery failed for {File}", walFile); }
        }

        // Markers whose WAL is gone: the delete pair is WAL-then-marker, so a crash between
        // them leaves this. Harmless but unbounded if never collected.
        foreach (var marker in Directory.EnumerateFiles(_walDir, "*.flushed"))
        {
            string wal = marker[..^".flushed".Length] + ".wal";
            if (File.Exists(wal)) continue;
            try { File.Delete(marker); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete stale flush marker {File}", marker); }
        }
    }

    private void ReplayOrphanedWal(string walFile)
    {
        string poolPath   = walFile + ".pool";
        var (segId, entries) = WriteAheadLog.ReadForRecovery(walFile);

        // Empty or corrupt WAL — clean up
        if (segId == 0 || entries.Count == 0)
        {
            try { File.Delete(walFile); } catch { }
            try { File.Delete(poolPath); } catch { }
            if (segId != 0)
            {
                ReserveWalBlock(segId);
                try { File.Delete(FlushMarkerPath(segId)); } catch { }
            }
            return;
        }

        // The block this WAL names is spent whatever happens next — its levels are either
        // already on disk or about to be written below. Burning it here stops a later flush or
        // merge being handed an id that already carries a file: the allocator is seeded from
        // segment file NAMES, and a block whose flush never produced one leaves it behind.
        ReserveWalBlock(segId);

        // The flush of this WAL completed — every level reached disk before the marker did.
        // Read off the FILESYSTEM, not off _segments: the catalog load runs in the background
        // (it opens every file), so a catalog lookup here races the enumeration that would
        // answer it, and losing that race replays a WAL whose events are already cold.
        string marker = FlushMarkerPath(segId);
        if (File.Exists(marker))
        {
            _logger.LogInformation("WAL {File} already flushed — removing", walFile);
            try { File.Delete(walFile); } catch { }
            try { File.Delete(poolPath); } catch { }
            try { File.Delete(marker); } catch { }
            return;
        }

        // Load template pool. An empty pool used to discard the ENTIRE WAL — dropping events
        // whose payloads DID reach disk because their template strings did not. Replay them
        // template-less instead: timestamp, level, properties and exception all survive,
        // only @mt is lost, and the index below must not alias a live pool entry.
        //
        // Empty has TWO causes and the warning must name both. Pool writes are only fsynced
        // periodically, so power loss can zero the file — that is the one this started as. But
        // since the template pool stopped writing a row for an event it could not intern, a WAL
        // every one of whose events arrived past a SATURATED pool writes no rows either, and its
        // .pool file stays at 0 bytes with nothing wrong. The replay is right in both cases;
        // only an operator reading "no template pool" as corruption would be wrong.
        var pool         = WriteAheadLog.LoadPool(poolPath);
        bool poolMissing = pool.Count == 0;
        if (poolMissing)
            _logger.LogWarning(
                "Orphaned WAL {File}: no template pool rows — either power loss before the pool was fsynced, "
              + "or every event in it was outside a saturated template pool. Replaying {Count} events without templates",
                walFile, entries.Count);

        // Restore templates into TemplatePool
        foreach (var (idx, tmpl) in pool)
            TemplatePool.ForceIntern(idx, tmpl);

        // Replay entries into a hot tier of this WAL's own — one WAL per tier, so the recovered
        // events flush into the block the WAL is NAMED from and recovery obeys exactly the same
        // marker protocol as the live flush. Pooling several WALs into one tier could not: their
        // events would land in some other block, no marker would ever be written for the WAL's
        // own, and a crash before the WAL was unlinked would replay it into duplicates.
        using var recoveredHot = CreateHotTier();
        int replayed = 0;
        foreach (var entry in entries)
        {
            // Unpooled: the event was outside a saturated pool when it was logged. Its index
            // field is 0 and names nothing, and its template text was never stored, so it
            // replays with no template. Resolving the 0 attached index 0's template to it.
            bool noTemplate = poolMissing || entry.Unpooled;
            var header = new LogEventHeader
            {
                Id                       = _idGen.Next(entry.TimestampTicks),
                TimestampUtcTicks        = entry.TimestampTicks,
                Level                    = entry.Level,
                // With no pool the stored index points at whatever the LIVE pool holds
                // at that slot — resolving it would stamp a random template onto every
                // recovered event. -1 = "no template", persisted as an empty @mt.
                MessageTemplatePoolIndex = noTemplate ? -1 : entry.TemplateIndex,
                // EXPLICITLY -1. The WAL entry format carries no service name, so "absent" is
                // the only honest value — but the field is a plain int on a struct, and its
                // default of 0 is a VALID pool index, not the sentinel every reader tests for
                // (`ServiceNamePoolIndex >= 0`). The pool is shared by templates and service
                // names and recovery force-interns this WAL's own rows into it, so slot 0 is
                // ordinarily this WAL's first template: every recovered event was stamped with
                // it, and the flush below wrote that string permanently into the recovery
                // segment's @svc column, where it answers service.name queries and skews
                // per-service counts.
                ServiceNamePoolIndex     = -1,
            };
            // Resolve template via the freshly restored pool and attach it
            // to the hot tier so the recovery flush persists @mt correctly.
            string tmpl = noTemplate ? string.Empty : TemplatePool.Get(entry.TemplateIndex);
            if (recoveredHot.TryWrite(header, entry.Payload, tmpl, entry.Exception))
                replayed++;
        }
        _logger.LogInformation("WAL recovery: replayed {Count} events from {File}", replayed, walFile);

        // Flush recovered events to cold segments (no index — acceptable for crash recovery),
        // ONE PER LEVEL like the live flush path. Writing the recovered tier as a single
        // mixed-level segment reopened the data loss the level split exists to prevent:
        // expiry is Ttl(MinLevel), TTL is not monotonic in the level's value, so a
        // recovered tier holding one Debug event put every Error beside it on a 3-day
        // deadline. Crash recovery is exactly when that is least acceptable.
        //
        // skipPublishedLevels: the interrupted flush may have got some levels out. Those files
        // are whole — a level is one move of one file — so they are kept and only the missing
        // levels are written. That is what makes this replay idempotent, and it is also the
        // answer for a data directory written by the PREVIOUS build, which carries no marker at
        // all: its complete flush replays to zero new segments and the WAL is simply dropped.
        if (recoveredHot.Count > 0)
        {
            var written = FlushTierByLevelAsync(recoveredHot, segId, CancellationToken.None, skipPublishedLevels: true)
                              .GetAwaiter().GetResult();
            long recovered = 0;
            for (int i = 0; i < written.Count; i++)
            {
                PublishLocalSegment(written[i]);
                recovered += written[i].EventCount;
            }
            _logger.LogInformation("WAL recovery: wrote {Segments} level segment(s), {Count} events",
                written.Count, recovered);
        }
        // A tier that accepted nothing needs no marker: replaying it again writes nothing again.

        try { File.Delete(walFile); } catch { }
        try { File.Delete(poolPath); } catch { }
        try { File.Delete(FlushMarkerPath(segId)); } catch { }
    }

    // ── Segment-id allocator ──────────────────────────────────────────────────
    //
    // The three entry points below are the ONLY code allowed to read or write
    // _nextSegmentId. They exist because the field had four writers on three thread models —
    // startup recovery, the flush swap, the merge planner and the replication import — and
    // only the middle two were synchronised with each other. See ImportSegment.
    //
    // internal, not private, so SegmentIdAllocatorTests can put them under contention directly.
    // Driving the race through ImportSegment does not reproduce it: that path opens and reads a
    // segment file before it touches the counter, which is thousands of times longer than the
    // read-modify-write it has to land inside, so an end-to-end test passes with or without the
    // lock. The property is real regardless of whether a test can hit it by luck, so it is
    // asserted where it can be hit on purpose.

    /// <summary>Reserves ONE id — the merge path's unit.</summary>
    internal ulong AllocateSegmentId()
    {
        lock (_segIdLock) return _nextSegmentId++;
    }

    /// <summary>
    /// Reserves a whole level block and returns its first id — the flush path's unit, since a
    /// tier writes one segment per level at <c>first + (byte)level</c>.
    /// </summary>
    internal ulong AllocateSegmentIdBlock()
    {
        lock (_segIdLock)
        {
            ulong first = _nextSegmentId;
            _nextSegmentId += LevelSegmentSlots;
            return first;
        }
    }

    /// <summary>
    /// Raises the allocator to at least <paramref name="floor"/>, for the callers that learn an id
    /// is spent from somewhere other than the allocator — a file name on disk, a WAL's reserved
    /// block, a segment received from a peer. Monotonic: it can only ever move forward, so a
    /// caller arriving with stale information cannot hand out an id twice.
    /// </summary>
    internal void AdvanceSegmentIdFloor(ulong floor)
    {
        lock (_segIdLock)
        {
            if (floor > _nextSegmentId) _nextSegmentId = floor;
        }
    }

    /// <summary>
    /// Claims <paramref name="id"/> for a segment that arrived carrying THIS node's own NodeId,
    /// i.e. from a peer misconfigured with our identity — the one case
    /// <see cref="SegmentKey"/> cannot separate. Answers false when the allocator has already
    /// handed that id out or passed it, and raises the floor past it when it has not.
    ///
    /// <para>The test and the raise are one critical section because separating them is the
    /// window itself: probe, and a flush swap takes a block containing the id before the raise
    /// lands. Together they give the property the import path claimed from the floor alone and
    /// never had — that the ids this node publishes under and the ids it accepts under its own
    /// NodeId are DISJOINT, forever. The raise closes the future (nothing is handed out at or
    /// below the imported id afterwards); the test closes the past (nothing already handed out
    /// can be imported over).</para>
    ///
    /// <para>"Already handed out" is deliberately coarser than "still outstanding". An id below
    /// the counter may be published, reserved by the live WAL's block, reserved by a frozen tier
    /// mid-flush, reserved by a merge that is still streaming, or merely burnt — a level that
    /// produced no file. Only the first of those is visible in the catalog, and tracking the
    /// other four would mean a release site on every flush, merge and recovery exit, each miss
    /// leaving a permanent phantom. The counter already separates them from what has never been
    /// handed out, and refusing the whole span costs nothing a correct deployment can notice: a
    /// peer with its OWN NodeId never reaches this method, and one wearing ours is the
    /// deployment error the 409 exists to report.</para>
    /// </summary>
    private bool TryClaimLocalSegmentId(ulong id)
    {
        lock (_segIdLock)
        {
            if (id < _nextSegmentId) return false;
            _nextSegmentId = id + 1;
            return true;
        }
    }

    /// <summary>Pushes the id allocator past a WAL's reserved level block so it is never reused.</summary>
    private void ReserveWalBlock(ulong walSegId) => AdvanceSegmentIdFloor(walSegId + LevelSegmentSlots);

    /// <summary>
    /// Registers a segment file received from a peer, taking ownership of it. The file is STAGED
    /// at <paramref name="stagedPath"/> and moves to <paramref name="finalPath"/> only once it
    /// has been accepted; on any other outcome nothing on disk is touched and the staged file is
    /// still the caller's to unlink. <see cref="ImportSegment(string)"/> is this same call for a
    /// file that already sits where it belongs.
    ///
    /// <para>That the move is on THIS side of the verdict is the reason for the two paths. The
    /// receiving endpoint derives the final name from the ROUTE — <c>{nodeId}-{segmentId}.seg</c>
    /// — so two peers misconfigured with one NodeId push two different segments to one path. While
    /// the endpoint moved first and asked afterwards, the second push had already overwritten the
    /// first peer's bytes before anything compared anything, and the comparison then found a path
    /// equal to itself: a re-push, refresh the entry, 204. Both senders recorded a successful
    /// push, the receiver logged nothing, and the first peer's events existed nowhere.</para>
    ///
    /// <para>Which is also why a re-push is not recognised BY its path. Under a duplicated NodeId
    /// a path says nothing about who sent it, so the file already held and the file arriving are
    /// compared as segments: the same key, at the same path, over the same time span, with the
    /// same event count and the same minimum level. A sender re-pushing its own segment matches
    /// all of that and refreshes the entry — its bytes may still differ, since a re-compression
    /// rewrites a file without changing what is in it — while a stranger's segment differs in the
    /// span or the count and is refused, the incumbent untouched in the catalog and on disk. Two
    /// distinct segments would have to agree on their first and last event to 100 ns and on how
    /// many events lie between to be taken for one, and that outcome is the overwrite this method
    /// already performs for a genuine re-push.</para>
    ///
    /// <para>Runs on a request thread, concurrently with everything: the flush swap reserving a
    /// level block, the merge planner reserving a single id, another import. The allocator update
    /// below therefore goes through <see cref="AdvanceSegmentIdFloor"/> like every other one — it
    /// used to be a bare read-compare-write on a field whose other writers hold <c>_flushLock</c>,
    /// which is not a lock this path can take.</para>
    ///
    /// <para>An imported id landing on one this node has already used is no longer a collision:
    /// the catalog is keyed by <see cref="SegmentKey"/>, so the peer's segment and the local one
    /// occupy separate slots and both stay queryable, expirable and mergeable. It used to evict
    /// the local entry — the file surviving on disk (the names cannot collide) but leaving
    /// queries, retention and the merge planner at once, since all three read the catalog rather
    /// than the directory, and a restart re-ran the same race in directory order.</para>
    ///
    /// <para>The allocator is load-bearing here rather than tidy, and in two different ways
    /// depending on whose NodeId is in the header. For a peer with its own id, raising the floor
    /// costs nothing and keeps the directory readable to a human. For a peer wearing OURS it is
    /// the verdict: <see cref="TryClaimLocalSegmentId"/> refuses an id the allocator has already
    /// handed out. The floor by itself never gave that. It is monotonic, so it cannot retract a
    /// reservation already made — <c>OpenWal</c> takes a block of six ids the moment a WAL opens
    /// and the flush publishes into that block much later, a merge takes an id and publishes
    /// after streaming towards a 512 MB target — and an import is not HANDED an id by the
    /// allocator, it arrives carrying one. So "the floor stops a local flush being handed an id
    /// an import occupies" was true only of ids handed out after the import; the reserved-but-
    /// unpublished ones were exactly the window bug #43 came back through. It also still matters
    /// for <c>SegmentFileExists</c> and the WAL block probe — both of which match
    /// <c>{localNode}-{id}-*.seg</c> — which would otherwise be looking at somebody else's
    /// file.</para>
    /// </summary>
    /// <returns>
    /// What happened to the file, so the caller that WROTE it can act: see
    /// <see cref="SegmentImportOutcome"/>.
    /// </returns>
    public SegmentImportOutcome ImportSegment(string stagedPath, string finalPath)
    {
        SegmentInfo staged;
        try
        {
            using var reader = SegmentReader.Open(stagedPath, computeUncompressedBytes: true);
            staged = reader.Info;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to import replicated segment {File}", stagedPath);
            return SegmentImportOutcome.Unreadable;
        }

        var key = SegmentKey.Of(staged);

        // Before the catalog, and outside _importLock so that the only lock ordering in this
        // method is no ordering at all.
        //
        // A segment from a peer with its own NodeId can never share a key with a local one, so
        // the floor is raised for it and nothing else: monotonic, so an import that is about to
        // be refused for some other reason can only ever move it forward, and it keeps the
        // directory readable to a human.
        //
        // Our OWN NodeId on an incoming segment is the case the key cannot separate, and there
        // the floor is not a note — it is the decision. See TryClaimLocalSegmentId: an id the
        // allocator has already handed out belongs to a local segment that is published, or to
        // one of the reserved blocks a flush, a merge or WAL recovery is about to publish into,
        // and none of those three can refuse a key. Accepting such an id put the peer's entry in
        // the catalog for exactly as long as it took the local writer to reach its own
        // publication, which is bug #43 with the sender told 204.
        bool claimed = true;
        if (staged.NodeId.Value == _options.NodeId.Value)
            claimed = TryClaimLocalSegmentId(staged.Id.Value);
        else
            AdvanceSegmentIdFloor(staged.Id.Value + 1);

        bool inPlace = string.Equals(stagedPath, finalPath, StringComparison.OrdinalIgnoreCase);
        var  info    = inPlace ? staged : AtPath(staged, finalPath);

        // First segment under a key keeps it. The assignment this replaces warned about the clash
        // and then performed it anyway, which is the same eviction the key change removed, in the
        // one case the key cannot separate — two nodes carrying the same NodeId. Leaving the
        // catalog is leaving queries, retention and the merge planner at once, so the refused file
        // held disk for the life of the install, never served, never expired, never compacted.
        //
        // The lock is NOT what makes the decision safe, and the compare-and-swap below is not
        // redundant with it. _importLock is taken here and in DeleteSegmentAsync, while four
        // other writers reach this dictionary knowing nothing about it: flush publication (under _frozenLock),
        // merge publication, the boot catalog scan and WAL recovery. A probe followed by a store
        // therefore decides on a value that another writer can replace in between — and the
        // catalog scan is a BACKGROUND task, so a replication POST is live while it is still
        // walking the directory. The import finds the key free because the scan has not reached
        // the local {node}-{id}-{min}-{max}.seg yet; the scan then adds it; the store overwrites
        // it. That is bug #43 again — the local segment out of queries, retention and the merge
        // planner at once with its file still on disk — and silently, because the conflict branch
        // never fired and the scan's error branch had already been passed.
        //
        // So the WRITE decides: TryAdd when the key looked free, TryUpdate against the exact
        // instance that was read when it did not. Losing the exchange means somebody landed in
        // the window, and the loop re-reads and judges again rather than assuming the worst — the
        // interleaving is not always a conflict. A peer re-pushing a segment this node already
        // holds on disk, while the boot scan registers that very file, loses the exchange to an
        // entry describing the same segment, and answering 409 there would tell a healthy sender
        // that two nodes share its id.
        //
        // The lock stays, and the rename stays under it, but the rename now happens only AFTER
        // the exchange is won. It is the irreversible half of this method — overwrite: true — and
        // deciding it on the stale probe is what let a refusal arrive with the other peer's bytes
        // already destroyed. Registering first opens a window in the other direction, where the
        // entry names a path the file has not reached. For the two QUERY consumers that is
        // harmless -- QueryExecutor yields nothing for an unreadable path, and the header
        // aggregation waits for this lock on a missing file and opens it again (see
        // OpenForHeaderScan), so it neither drops the segment nor calls its count partial --
        // but retention and the merge planner act on the ENTRY, not the file:
        // retention could remove it and orphan the file the move then lands, and the planner
        // could quarantine the not-yet-arrived path until restart. Both are held off explicitly
        // instead of argued away: DeleteSegmentAsync takes this same lock, and the planner
        // quarantines only corruption, never a file that failed to open.
        SegmentInfo? refreshEntryOutsideLock = null;
        try
        {
        lock (_importLock)
        {
            while (true)
            {
                // The bool is redundant: a null value is never stored, so "absent" and "null"
                // are the same state, and one variable then carries both the answer and the
                // value the exchange below has to be made against.
                _segments.TryGetValue(key, out var existing);

                if (existing is not null && !IsTheSameSegment(existing, info))
                {
                    _logger.LogError(
                        "Refused replicated segment {File}: {Key} is already held by {Existing}, which " +
                        "carries the same node id AND the same segment id. Two nodes appear to be " +
                        "configured as NodeId {Node} — a deployment error no id space can resolve. The " +
                        "segment already being served was kept and the incoming one was NOT registered.",
                        staged.FilePath, key, existing.FilePath, info.NodeId);
                    return SegmentImportOutcome.ConflictDifferentSegment;
                }

                if (existing is not null)
                {
                    // Same segment, by the header five-tuple. The old path refreshed the bytes
                    // here with File.Move(overwrite: true) -- betting the file already being
                    // served on a heuristic its own docstring calls uncertain: the header
                    // carries no digest, and under a duplicated NodeId the path comes from the
                    // route, so five coinciding fields were licence to destroy the one copy
                    // this node serves. A re-push needs no refresh -- the entry and the bytes
                    // already held stay, the staged body is dropped, and the sender hears
                    // success, which for an idempotent push it is. If the five fields coincided
                    // on genuinely DIFFERENT segments, the incoming one is not stored and the
                    // line below is the only trace -- keeping the served file is the safe side
                    // of a bet this method cannot avoid making.
                    if (!inPlace)
                        try { File.Delete(stagedPath); }
                        catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete staged body {File}", stagedPath); }
                    _logger.LogInformation(
                        "Re-push of {Key} matched the segment already held ({File}) by its header " +
                        "fields; existing bytes kept, staged body discarded.",
                        key, existing.FilePath);
                    return SegmentImportOutcome.Registered;
                }

                // A free key is not the same thing as an available one. The claim above failed,
                // so this id is inside the span the local allocator has already handed out — the
                // entry that will occupy this key has not been written yet, or its file has been
                // written and not yet published, and either way the writer holding it cannot
                // stand down for us. Registering here would be answered 204 and then stored over.
                //
                // Below the equality test on purpose: a re-push of a segment this node has
                // already accepted fails the claim too — the first push raised the floor past it
                // — and that is ordinary traffic, so the incumbent decides it. Only a key with
                // nothing under it is refused on the claim alone.
                if (!claimed)
                {
                    _logger.LogError(
                        "Refused replicated segment {File}: {Key} names a segment id this node has " +
                        "already allocated for itself, so a local flush, merge or WAL replay either " +
                        "holds it or is about to publish under it — and none of those can give a key " +
                        "back. Two nodes appear to be configured as NodeId {Node}. Nothing was " +
                        "registered and the body was left where it was staged.",
                        staged.FilePath, key, info.NodeId);
                    return SegmentImportOutcome.ConflictAllocatedLocally;
                }

                _beforeImportPublish?.Invoke();

                // Only a FREE key reaches this line -- both occupied outcomes returned above --
                // so the publication is a plain TryAdd. Losing it means somebody landed in the
                // window; the loop re-reads and judges again rather than assuming the worst.
                if (!_segments.TryAdd(key, info)) continue;

                break;
            }

            _afterImportPublish?.Invoke();

            if (!inPlace)
            {
                // No overwrite: a free KEY is a statement about the catalog, and the catalog is
                // still being built while the boot scan runs -- a file can sit at the final path
                // with no entry naming it yet. overwrite: true here was a bet that a free key
                // means a free path, and losing that bet destroyed a peer's bytes with the only
                // trace a Debug line whose text says nothing was lost. The move now refuses, and
                // an incumbent ON DISK is judged exactly as an incumbent in the catalog is.
                try { File.Move(stagedPath, finalPath); }
                catch (IOException) when (File.Exists(finalPath))
                {
                    SegmentInfo onDisk;
                    try
                    {
                        // No block walk here: this runs under _importLock with retention and the
                        // merge cleanup queued behind it, and the comparison below reads none of
                        // what the walk computes. The entry stored for the incumbent then carries
                        // the file size as its uncompressed size -- conservative until the next
                        // start's scan reads the honest value.
                        using var incumbent = SegmentReader.Open(finalPath);
                        onDisk = incumbent.Info;
                    }
                    catch (Exception ex)
                    {
                        // Unreadable incumbent: refuse rather than replace -- and SAY that,
                        // rather than a diagnosis this branch just failed to make. It used to
                        // return ConflictDifferentSegment here, and the endpoint then told the
                        // sender "already held by a different file... two nodes appear to be
                        // configured with NodeId {N}" -- claims nothing established, quoted at
                        // Error by the sender and marked permanent by the contract, when the
                        // real cause is local and clears with the file.
                        _segments.TryRemove(new KeyValuePair<SegmentKey, SegmentInfo>(key, info));
                        _logger.LogError(ex,
                            "Refused replicated segment {File}: the final path {Final} is occupied by a " +
                            "file that could not be read, and a push does not adjudicate that. Nothing " +
                            "was registered and the file on disk was kept.",
                            stagedPath, finalPath);
                        return SegmentImportOutcome.ConflictUnreadableIncumbent;
                    }

                    if (IsTheSameSegment(onDisk, info))
                    {
                        // A re-push landing after a restart, before the scan reached this file:
                        // the same segment already sits at the final path, so the body is
                        // dropped -- and the entry is exchanged for one describing the file that
                        // SURVIVED. The published info was built from the staged body, and this
                        // branch exists precisely because the two may differ in bytes (a
                        // re-compression), so leaving info in place kept a catalog entry whose
                        // sizes belonged to a file this same branch just deleted.
                        //
                        // onDisk was read without the block walk (this is under the import
                        // lock), so its UncompressedBytes is the file size -- an UNDERSTATED
                        // value, and understatement here is the harmful direction: the merge
                        // planner takes max(Uncompressed, Compressed) into its batch budget, so
                        // a shrunken entry lets a batch overfill. The honest value is re-read
                        // below, outside the lock, and swapped in.
                        _segments.TryUpdate(key, onDisk, info);
                        try { File.Delete(stagedPath); }
                        catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete staged body {File}", stagedPath); }
                        _logger.LogInformation(
                            "Re-push of {Key} matched the file already at {Final}; existing bytes kept, " +
                            "staged body discarded.",
                            key, finalPath);
                        refreshEntryOutsideLock = onDisk;
                        return SegmentImportOutcome.Registered;
                    }

                    _segments.TryRemove(new KeyValuePair<SegmentKey, SegmentInfo>(key, info));
                    _logger.LogError(
                        "Refused replicated segment {File}: {Key} was free in the catalog but {Final} is " +
                        "occupied by a different segment the boot scan has not reached yet. Two nodes " +
                        "appear to be configured as NodeId {Node}. The file on disk was kept and nothing " +
                        "was registered.",
                        stagedPath, key, finalPath, info.NodeId);
                    return SegmentImportOutcome.ConflictDifferentSegment;
                }
                catch
                {
                    // The entry is already published and names a file that never arrived. Nothing
                    // else would ever take it back — the catalog is not rebuilt until the next
                    // start — so a disk error here would leave a permanent entry for a path
                    // holding either nothing or the previous segment. Conditional both ways: only
                    // an entry that is still OURS is withdrawn.
                    _segments.TryRemove(new KeyValuePair<SegmentKey, SegmentInfo>(key, info));
                    throw;
                }
            }
        }

        _logger.LogInformation("Imported replicated segment {Key} ({Events} events)", key, info.EventCount);
        return SegmentImportOutcome.Registered;
        }
        finally
        {
            // The honest uncompressed size, computed off the lock: the block walk pages in a
            // slice of every block and has no business inside a section retention queues on.
            // Conditional swap -- if retention deleted the entry in between, it stays deleted.
            if (refreshEntryOutsideLock is { } placeholder)
            {
                try
                {
                    using var honest = SegmentReader.Open(placeholder.FilePath, computeUncompressedBytes: true);
                    _segments.TryUpdate(key, honest.Info, placeholder);
                }
                catch { /* the file may already be gone again; the placeholder stays conservative */ }
            }
        }
    }

    /// <summary>
    /// Registers a segment file that already sits at its final path in the segments directory.
    /// </summary>
    public SegmentImportOutcome ImportSegment(string filePath) => ImportSegment(filePath, filePath);

    /// <summary>
    /// Whether two catalog entries describe ONE segment rather than two that collided on a key.
    /// Deliberately not a byte comparison: a segment re-pushed after a re-compression is the same
    /// segment carrying different bytes, and the header holds no digest to compare instead.
    /// </summary>
    private static bool IsTheSameSegment(SegmentInfo a, SegmentInfo b) =>
        string.Equals(a.FilePath, b.FilePath, StringComparison.OrdinalIgnoreCase) &&
        a.MinTimestampTicks == b.MinTimestampTicks &&
        a.MaxTimestampTicks == b.MaxTimestampTicks &&
        a.EventCount        == b.EventCount        &&
        a.MinLevel          == b.MinLevel;

    /// <summary>
    /// Publishes a segment THIS node produced — a flush's level segment, a merged segment, a
    /// segment rebuilt by WAL recovery. The three of them are the writers into the catalog that
    /// <see cref="ImportSegment"/> cannot be: their events exist in no other file, so refusing a
    /// key is not something they are able to do, and this method always ends with the entry
    /// theirs.
    ///
    /// <para>What it does not do any more is get there in SILENCE. All three used to be a bare
    /// <c>_segments[key] = w</c>, which is why making the import decide by compare-and-swap
    /// closed bug #43 in one direction only: an import that found the key free, WON its exchange,
    /// was answered <c>Registered</c> and told its peer 204 could still have its entry stored
    /// over by any of them a moment later. The peer's <c>.seg</c> then sat in the segments
    /// directory in no catalog — never served by a query, never expired by retention, never
    /// picked by the merge planner — with a successful push recorded at the other end and not one
    /// line logged at this one. That is the whole of #43, reached through the one route the
    /// compare-and-swap does not cover, and it needed no race at all: an import into the block a
    /// live WAL had already reserved, then an ordinary flush.</para>
    ///
    /// <para><see cref="TryClaimLocalSegmentId"/> is what now stops that arrangement arising —
    /// an import may not take an id this node has already been handed. This method is the
    /// backstop for what the claim cannot see, which is the catalog scan: it runs as a background
    /// task while <see cref="ReplayOrphanedWals"/> is still publishing, so a peer file already in
    /// the directory under <c>{localNode}-{id}.seg</c> can be registered by the scan and then
    /// displaced by a recovered level segment carrying that id.</para>
    ///
    /// <para>So the displacement stays possible and stops being invisible. Which file left the
    /// catalog is the only remedy this node has for two peers wearing one NodeId — it cannot
    /// renumber either of them — so it is logged with both paths, and the file itself is left
    /// alone: its owner still holds it and can push it again once the ids are fixed.</para>
    /// </summary>
    private void PublishLocalSegment(SegmentInfo written)
    {
        var key = SegmentKey.Of(written);

        while (true)
        {
            if (_segments.TryAdd(key, written)) return;

            // TryAdd lost, so somebody holds the key — read WHO, and exchange against that exact
            // reference. A plain store would decide on a value another writer can replace in
            // between, which is the defect this method exists to end rather than to repeat.
            if (!_segments.TryGetValue(key, out var existing)) continue;   // removed under us

            if (!IsTheSameSegment(existing, written))
                // One placeholder per argument: a repeated name in a structured template is a
                // second positional slot, not a second rendering of the first.
                _logger.LogError(
                    "Registering {File} under {Key} displaced {Existing}, which carries the same node id " +
                    "AND the same segment id. Two nodes appear to be configured as NodeId {Node} — a " +
                    "deployment error no id space can resolve. This node wrote the incoming file itself " +
                    "and its events are in no other file, so it MUST be registered; the displaced one is " +
                    "no longer served, expired or merged, and its bytes stay on disk until it is " +
                    "re-pushed by its owner or removed.",
                    written.FilePath, key, existing.FilePath, written.NodeId);

            if (_segments.TryUpdate(key, written, existing)) return;
        }
    }

    /// <summary>The same segment, described at the path it is about to occupy.</summary>
    private static SegmentInfo AtPath(SegmentInfo info, string filePath) => new()
    {
        Id                = info.Id,
        NodeId            = info.NodeId,
        FilePath          = filePath,
        MinTimestampTicks = info.MinTimestampTicks,
        MaxTimestampTicks = info.MaxTimestampTicks,
        EventCount        = info.EventCount,
        MinLevel          = info.MinLevel,
        CompressedBytes   = info.CompressedBytes,
        UncompressedBytes = info.UncompressedBytes,
    };

    // ── Helpers ───────────────────────────────────────────────────────────────

    private HotTierSegment CreateHotTier()
    {
        long payloadCapacity = _options.HotTier.MaxSizeBytes;
        // Event cap derived from the chunk geometry rather than a flat 2,000,000: chunks
        // are allocated whole, so a payload-only bound let a "64 MB" tier reach 1.1 GB
        // resident on small events (see HotTierSegment.ChunksFor). Both limits now cap the
        // same chunk count, making MaxSizeBytes a genuine ceiling on native memory.
        return new HotTierSegment(HotTierSegment.EventCapacityFor(payloadCapacity), payloadCapacity);
    }

    /// <summary>
    /// Opens the next WAL, RESERVING the block of segment ids its events will flush into.
    /// The reservation is what keeps the WAL's name meaningful: nothing else — no merge, no
    /// recovery segment — can subsequently be HANDED an id inside it.
    ///
    /// <para>Handed is the exact word, and for a while it was the whole of the guarantee. A
    /// replicated segment does not ask the allocator for an id, it arrives with one, so a peer
    /// misconfigured with this node's NodeId could push straight into this block and be
    /// registered — for as long as it took the flush to reach its own publication, which is the
    /// entire life of the current hot tier. <see cref="TryClaimLocalSegmentId"/> is what closes
    /// that half, by refusing an arriving id the allocator has already passed.</para>
    /// </summary>
    private (WriteAheadLog Wal, ulong SegId) OpenWalCore()
    {
        ulong walSegId = AllocateSegmentIdBlock();

        var segId   = new SegmentId(walSegId);
        var walPath = Path.Combine(_walDir, $"{_options.NodeId.Value}-{segId.Value}.wal");
        return (WriteAheadLog.Open(walPath, _options.NodeId, segId), walSegId);
    }

    /// <summary>
    /// Periodic WAL msync. Appends land in the OS page cache; without this the engine's
    /// real durability contract was "whenever the OS writes back" — a power loss silently
    /// forfeited up to a whole hot tier (5 minutes / 64 MB) of ACKNOWLEDGED events. One
    /// msync per interval bounds that loss window at the interval, Elasticsearch-translog
    /// style, without an fsync per event.
    /// </summary>
    private async Task RunWalFlushLoopAsync(CancellationToken ct)
    {
        var interval = _options.HotTier.WalFlushInterval;
        if (interval <= TimeSpan.Zero) return; // explicitly disabled

        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try { _write.Wal?.Flush(); }
                catch (ObjectDisposedException) { /* raced a rotation — next tick hits the new WAL */ }
                catch (Exception ex) { _logger.LogWarning(ex, "Periodic WAL flush failed"); }
            }
        }
        catch (OperationCanceledException) { }
    }

    /// <param name="order">
    /// Tier indices this segment will contain; null = the whole tier. The file name
    /// carries the range, and retention reads MaxTimestamp out of it, so a level-split
    /// segment must be named from ITS OWN events rather than the tier's.
    /// </param>
    /// <param name="orderCount">
    /// How many of <paramref name="order"/>'s entries belong to this segment, or -1 for all of
    /// it. A level-split flush passes a POOLED array whose tail is another flush's leftovers.
    /// </param>
    private string BuildSegmentPath(SegmentId segId, HotTierSegment hot, int[]? order = null, int orderCount = -1)
    {
        long minTs = long.MaxValue, maxTs = long.MinValue;
        int n = order is null ? hot.Count : (orderCount >= 0 ? orderCount : order.Length);
        for (int k = 0; k < n; k++)
        {
            int i = order?[k] ?? k;
            ref var h = ref hot.GetHeader(i);
            if (h.TimestampUtcTicks < minTs) minTs = h.TimestampUtcTicks;
            if (h.TimestampUtcTicks > maxTs) maxTs = h.TimestampUtcTicks;
        }
        if (minTs == long.MaxValue) minTs = DateTimeOffset.UtcNow.UtcTicks;
        if (maxTs == long.MinValue) maxTs = minTs;

        return Path.Combine(_segDir,
            $"{_options.NodeId.Value}-{segId.Value}-{minTs}-{maxTs}.seg");
    }

    private int _disposed;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _cts.CancelAsync();
        try { await _flushLoop; }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        try { await _walFlushLoop; }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        try { await _maintenanceLoop; }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        // Deferred segment deletes: cancellation ends the retry loop's delay at once, so this
        // waits out at most one pass in progress, never a backoff. Then one last attempt each;
        // what is still held stays on disk and the next start's retention pass expires it again.
        try { await Volatile.Read(ref _segmentDeleteRetryLoop); } catch { /* best-effort */ }
        try { RetryPendingSegmentDeletes(); } catch { /* best-effort */ }

        // Await all in-flight parallel flushes before freeing the frozen tiers they
        // read — disposing their native memory mid-flush faults (AccessViolation).
        // No new flush can start: the age loop is stopped and no writes remain.
        try { await Task.WhenAll(_inFlightFlushes.Keys.ToArray()); } catch { /* best-effort */ }

        if (_write.Hot.Count > 0)
        {
            try { await TryFlushAsync(); } catch { /* best-effort final flush */ }
            // TryFlushAsync's heavy phase runs to completion inline here (we awaited it),
            // but a concurrent trigger may have scheduled another — drain those too.
            try { await Task.WhenAll(_inFlightFlushes.Keys.ToArray()); } catch { }
        }

        _write.Hot.Dispose();
        lock (_retireLock)
        {
            foreach (var t in _retired) t.Dispose();
            _retired.Clear();
        }
        lock (_frozenLock)
        {
            foreach (var (tier, _) in _frozenHot) tier.Dispose();
            _frozenHot.Clear();
        }
        _write.Wal?.Dispose();
        _flushConcurrency.Dispose();
        _flushSlots.Dispose();
        _flushLock.Dispose();
        _cts.Dispose();
    }
}
