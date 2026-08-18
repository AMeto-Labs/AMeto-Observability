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
    /// In the catalog: either the key was free, or the same file was re-pushed under it and the
    /// entry was refreshed.
    /// </summary>
    Registered,

    /// <summary>
    /// Refused. A DIFFERENT file already holds this (node, id) and was kept; the incoming one was
    /// not registered. Two nodes are configured with the same NodeId — the one ambiguity the key
    /// cannot resolve, and a configuration error rather than a race.
    /// </summary>
    Conflict,

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

    // Hot tier
    private          HotTierSegment                       _hot;
    private          WriteAheadLog?                       _wal;
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
    private readonly CancellationTokenSource               _cts        = new();
    private readonly Task                                  _flushLoop;
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
    /// <summary>Test hook: first id of the block reserved for the live WAL (see <see cref="_walSegId"/>).</summary>
    internal ulong LiveWalSegmentId => _walSegId;
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
    /// Ceiling on managed index-build state across all concurrent flushes. At the default
    /// 64 MB tier (131,072 events ⇒ ~184 MB per build) this yields a width of 3 — enough to
    /// stay ahead of ingest (a tier fills in ~0.9 s at 150k events/s, a build takes ~1.3 s,
    /// so 3 in flight clears one every ~0.44 s) while capping the burst near 550 MB instead
    /// of the 8 × 300 MB the old core-count heuristic allowed. Override with
    /// <c>HotTier.FlushConcurrency</c> when trading RAM for throughput deliberately.
    /// </summary>
    private const long FlushManagedBudgetBytes = 640L * 1024 * 1024;

    /// <summary>Ceiling on native memory held by frozen-but-not-yet-persisted tiers.</summary>
    private const long FlushNativeBudgetBytes = 512L * 1024 * 1024;
    /// <summary>
    /// Window anchors that produced no usable merge batch — excluded so the sweep advances
    /// (reset on restart). Keyed by <see cref="SegmentKey"/> for the same reason the catalog is:
    /// an id alone names a segment on one node only, so skip-listing an unreadable local file
    /// would also have excluded a healthy peer replica that shared its id.
    /// </summary>
    private readonly HashSet<SegmentKey> _mergeSkip = new();

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
    // ImportSegment and the flush both assign unconditionally — and because the file NAMES
    // cannot collide (a replica is {node}-{id}.seg, a locally written segment
    // {node}-{id}-{minTs}-{maxTs}.seg) both files stayed on disk while only one was in here.
    //
    // All three consumers read this dictionary rather than the directory, so the evicted file
    // left every one of them at once: queries (GetSegments/ListSegments), retention (Values
    // where IsExpired) and the merge planner (foreach over Values). It was therefore never
    // served, never expired and never compacted — disk held for the life of the install, with
    // nothing logged. LoadSegmentCatalog re-ran the same collision on every restart in
    // directory-enumeration order, so which of the two survived could change from boot to boot.
    private readonly ConcurrentDictionary<SegmentKey, SegmentInfo> _segments = new();
    /// <summary>Background catalog scan started by the ctor (kept to observe faults).</summary>
    private readonly Task _catalogLoad;

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
    /// Serialises <see cref="ImportSegment(string, string)"/>: the catalog lookup that decides an
    /// import and the rename that carries it out must be one step, or two peers pushing different
    /// segments under one key would both find it free and both rename onto the same path, leaving
    /// one peer's bytes gone from disk whichever entry the catalog ended up with.
    ///
    /// <para>Nothing is taken under it — the allocator floor is raised before it is entered — so
    /// it participates in no ordering. The section is a dictionary lookup and a rename; the
    /// segment's contents were read before it, and the path is only ever reached once per
    /// replicated segment.</para>
    /// </summary>
    private readonly System.Threading.Lock                _importLock = new();

    /// <summary>
    /// First id of the block RESERVED FOR THE LIVE WAL, i.e. the ids its events will occupy
    /// once they are flushed. The WAL file is named from it, and startup uses that name to
    /// decide whether a WAL still holds unflushed events.
    ///
    /// <para>It is a reservation, not a peek at <see cref="_nextSegmentId"/>, and that is the
    /// whole point. The WAL used to be named from whatever <c>_nextSegmentId</c> happened to
    /// be, which is also the id a MERGE takes — so the first merge after any flush published a
    /// segment carrying the live WAL's id, the restart check "a segment with this id exists ⇒
    /// this WAL was already flushed" fired on it, and every un-flushed event in that WAL was
    /// deleted. Measured before this change: WAL id 25, merged segment id 25, 30 events
    /// written, 0 recovered, on 3 of 3 runs.</para>
    /// </summary>
    private          ulong                                _walSegId;

    // Time-sortable event id generator (Snowflake layout). Assigns EventId.RawValue
    // on the write path so sorting by Id ≡ sorting by ingest time.
    private readonly EventIdGenerator                    _idGen;

    // String intern pool shared with ingestion
    public StringInternPool TemplatePool { get; } = new();

    public StorageEngine(IOptions<ServerOptions> options, RetentionStore retentionStore, ILogger<StorageEngine> logger)
    {
        _options        = options.Value;
        _retentionStore = retentionStore;
        _logger         = logger;
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

        int widthByMemory = (int)Math.Clamp(FlushManagedBudgetBytes / perFlushManaged, 1, 64);
        int flushWidth = _options.HotTier.FlushConcurrency > 0
            ? Math.Min(_options.HotTier.FlushConcurrency, 64)
            : Math.Clamp(Math.Min(Environment.ProcessorCount / 2, widthByMemory), 1, 8);
        _flushConcurrency = new SemaphoreSlim(flushWidth);

        // In-flight tier cap: bound the frozen-tier backlog by REAL native footprint.
        // The previous 1.4 × MaxSizeBytes estimate under-counted by up to 17x on small
        // events, so the "1 GB" budget it computed could hold multiple GB in practice.
        // Floored at the flush width so every concurrent flush can still hold a slot.
        int flushSlots = Math.Clamp((int)(FlushNativeBudgetBytes / tierFootprint), flushWidth, 64);
        _flushSlots = new SemaphoreSlim(flushSlots, flushSlots);

        // Report the ceilings these settings actually produce, not just the inputs — an
        // explicit HotTier.FlushConcurrency override raises them, and that should be
        // visible in the journal rather than inferred.
        long managedCeiling = (long)flushWidth * perFlushManaged;
        long nativeCeiling  = (long)flushSlots * tierFootprint;

        _logger.LogInformation(
            "Flush budgets: width={Width} (×{PerFlush} MB managed = {ManagedCeiling} MB), " +
            "slots={Slots} (×{Tier} MB native = {NativeCeiling} MB), tier={Events} events / {Payload} MB payload",
            flushWidth, perFlushManaged / 1048576, managedCeiling / 1048576,
            flushSlots, tierFootprint / 1048576, nativeCeiling / 1048576,
            eventCapacity, _options.HotTier.MaxSizeBytes / 1048576);

        // Both clamps are floored so at least one flush can always proceed. That floor
        // WINS over the budget: at a large MaxSizeBytes a single tier no longer fits, and
        // the engine quietly runs above the ceiling rather than refusing to start. The
        // budget is a target, not a guarantee — say so instead of letting the line above
        // read like one.
        if (perFlushManaged > FlushManagedBudgetBytes || tierFootprint > FlushNativeBudgetBytes)
            _logger.LogWarning(
                "A single flush of a {Payload} MB tier ({PerFlush} MB managed + {Tier} MB native) does not fit " +
                "the flush budget ({ManagedBudget} MB managed / {NativeBudget} MB native). One flush must always " +
                "be allowed to run, so these budgets cannot be honoured at this tier size — peak RAM will exceed " +
                "them. Lower HotTier.MaxSizeBytes to bring the peak down.",
                _options.HotTier.MaxSizeBytes / 1048576, perFlushManaged / 1048576, tierFootprint / 1048576,
                FlushManagedBudgetBytes / 1048576, FlushNativeBudgetBytes / 1048576);
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

        _hot = CreateHotTier();
        // The next segment id MUST be known before any flush, but it lives in the
        // file NAMES ({node}-{segId}-{minTs}-{maxTs}.seg) — a cheap directory
        // listing, no file opens. The expensive part (opening every segment to
        // read its catalog entry) runs in the background: ingest and the HTTP
        // endpoints come up immediately; cold segments become queryable as the
        // scan progresses (the catalog is a ConcurrentDictionary keyed by id, so
        // concurrent flush registrations are safe).
        InitNextSegmentIdFromFileNames();
        _catalogLoad = Task.Run(LoadSegmentCatalog);
        ReplayOrphanedWals();
        OpenWal();

        // Age-based flush loop
        _flushLoop = RunFlushLoopAsync(_cts.Token);
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
            // Finish any merge whose source deletion was blocked by an open reader
            // (the manifest survives until every source file is gone).
            try { RecoverInterruptedMerges(); }
            catch (Exception ex) { _logger.LogWarning(ex, "Merge recovery sweep failed"); }

            // One batch per iteration, short pause while a backlog exists.
            bool merged;
            try { merged = await TryMergeSmallSegmentsOnceAsync(ct); }
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

    public IReadOnlyList<SegmentInfo> GetSegments(DateTimeOffset? from, DateTimeOffset? to)
    {
        long fromTicks = from?.UtcTicks ?? long.MinValue;
        long toTicks   = to?.UtcTicks   ?? long.MaxValue;

        return _segments.Values
            .Where(s => s.MaxTimestampTicks >= fromTicks && s.MinTimestampTicks <= toTicks)
            .OrderByDescending(s => s.MaxTimestampTicks)
            .ToList();
    }

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
                long total = _hot.AllocatedBytes;
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
    private (HotTierSegment Current, HotTierSegment[] Frozen, HashSet<SegmentKey> Covered) SnapshotTiers()
    {
        HotTierSegment    current;
        HotTierSegment[]  frozen;
        HashSet<SegmentKey> covered;
        lock (_frozenLock)
        {
            current = _hot;
            if (_frozenHot.Count == 0)
            {
                frozen  = Array.Empty<HotTierSegment>();
                covered = new HashSet<SegmentKey>();
            }
            else
            {
                frozen  = new HotTierSegment[_frozenHot.Count];
                covered = new HashSet<SegmentKey>(_frozenHot.Count * LevelSegmentSlots);
                for (int i = 0; i < _frozenHot.Count; i++)
                {
                    frozen[i] = _frozenHot[i].Tier;
                    // The tier flushes to one segment per level, so every id in its
                    // reserved block is covered — otherwise a query would serve the
                    // already-registered per-level segments AND the still-frozen tier.
                    ulong first = _frozenHot[i].SegId;
                    for (int s = 0; s < LevelSegmentSlots; s++)
                        covered.Add(new SegmentKey(_options.NodeId, new SegmentId(first + (ulong)s)));
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
    /// </summary>
    public async ValueTask<LogVolumeCounts> AggregateLogVolumeAsync(
        DateTimeOffset fromUtc, DateTimeOffset toUtc,
        long minBucket, int bucketSeconds, int nBuckets,
        string? serviceFilter, CancellationToken ct = default)
    {
        long fromTicks = fromUtc.UtcTicks;
        long toTicks   = toUtc.UtcTicks;

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
            // tiers. Offloaded to the thread pool — it is CPU-bound (mmap reads + LZ4 decode) and we
            // do not want to occupy the request thread. Single-threaded feed keeps the aggregator
            // lock-free; the short-TTL response cache absorbs repeated range toggles.
            var segInfos = GetSegments(fromUtc, toUtc);
            if (segInfos.Count > 0)
            {
                await Task.Run(() =>
                {
                    foreach (var info in segInfos)
                    {
                        ct.ThrowIfCancellationRequested();
                        if (covered.Contains(SegmentKey.Of(info))) continue;
                        if (info.MaxTimestampTicks < fromTicks || info.MinTimestampTicks > toTicks) continue;
                        try
                        {
                            using var reader = SegmentReader.Open(info.FilePath);
                            reader.AggregateHeaders(agg, fromTicks, toTicks);
                        }
                        catch (Exception ex)
                        {
                            // Never lose the whole aggregate over one bad/racing segment file.
                            _logger.LogDebug(ex, "Header aggregation skipped segment {Id}", info.Id);
                        }
                    }
                }, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            OnReaderDisposed();
        }

        return agg.Build();
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
        HashSet<SegmentKey> covered,
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

        public IReadOnlySet<SegmentKey> CoveredSegmentIds => covered;

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

        if (!_hot.TryWrite(h, propertiesPayload, template, exception))
        {
            // Hot tier full — schedule async flush and signal back-pressure
            ScheduleFlush();
            return false;
        }

        ushort tmplIdx = h.MessageTemplatePoolIndex >= 0 ? (ushort)h.MessageTemplatePoolIndex : (ushort)0;
        string tmplStr = template
                         ?? (h.MessageTemplatePoolIndex >= 0 ? TemplatePool.Get(h.MessageTemplatePoolIndex) : string.Empty);
        _wal?.Append(h.TimestampUtcTicks, h.Level, tmplIdx, tmplStr, propertiesPayload, exception);

        // Notify subscribers (e.g. alert evaluator) — must be fast
        var hook = EventWritten;
        if (hook is not null)
        {
            hook(h, tmplStr);
        }

        // Check size threshold
        if (_hot.IsFull)
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
        if (_segments.TryRemove(key, out var info))
        {
            try { File.Delete(info.FilePath); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete segment {Key}", key); }
        }
        return Task.CompletedTask;
    }

    public IReadOnlyList<SegmentInfo> ListSegments() => _segments.Values.ToList();

    // ── Flush loop ────────────────────────────────────────────────────────────

    private async Task RunFlushLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_options.HotTier.MaxAge);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
                await TryFlushAsync(ct);
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
            if (_hot.Count == 0) return;

            // Back-pressure gate: if the in-flight tier budget is exhausted, skip the swap.
            // The hot tier stays full → TryWrite returns false → the drainer parks (ring
            // back-pressure) rather than letting frozen tiers pile up unbounded in RAM.
            if (!_flushSlots.Wait(0)) return;

            oldHot     = _hot;
            oldWal     = _wal;
            oldWalPath = oldWal?.FilePath;
            oldHot.Freeze();

            // Publish oldHot under the lock queries snapshot from, so a concurrent query
            // sees oldHot's events AND skips the reserved cold segment ids (no duplicates
            // during the register/remove overlap). A tier flushes to ONE SEGMENT PER LEVEL,
            // and the block of ids for exactly that was reserved when this tier's WAL was
            // opened — the level's segment is always firstId + (byte)level. Levels absent
            // from the tier simply never become files; a burnt id costs nothing.
            reservedSegId = _walSegId;
            lock (_frozenLock) { _frozenHot.Add((oldHot, reservedSegId)); }

            _hot = CreateHotTier();

            // Rotate the WAL: opening the next one reserves the next block, so the WAL on
            // disk always names the ids ITS events will occupy. The OLD WAL is disposed in
            // the heavy phase, off the swap lock — disposing flushes up to 64 MB of dirty
            // mmap pages to disk, and doing that here stalled every writer (hot tier stays
            // full for the whole swap) long enough to overflow the ingest ring under
            // sustained 100k/s load.
            _wal = null;
            OpenWal();
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
        try
        {
            await _flushConcurrency.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                List<SegmentInfo> written;
                try
                {
                    written = await FlushTierByLevelAsync(oldHot, reservedSegId, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Segment flush failed");
                    // Leave oldHot in _frozenHot so its events stay queryable; the WAL on
                    // disk replays them on restart. Do not retire — still referenced.
                    return;
                }

                // Register the cold segments AND drop oldHot from the frozen list atomically.
                lock (_frozenLock)
                {
                    foreach (var w in written) _segments[SegmentKey.Of(w)] = w;
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
            finally { _flushConcurrency.Release(); }
        }
        finally { _flushSlots.Release(); }
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
            if (_mergeSkip.Contains(SegmentKey.Of(s))) continue;
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
        // livelock-free: every failed attempt adds to _mergeSkip first, so the
        // candidate set strictly shrinks.
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
                    _mergeSkip.Add(SegmentKey.Of(seg));
                    _logger.LogWarning(ex, "Merge: skipping unreadable segment {File}", seg.FilePath);
                }
            }

            // Anti-stall: a bucket whose anchor can't produce even a 2-segment batch
            // would be re-selected forever — exclude the anchor until restart.
            // Debug, not Warning: this is the planner's EXPECTED outcome whenever there is
            // simply nothing to merge (a day's log carried 54 of these, every anchor
            // different) — at WRN it drowns the signal it was meant to be.
            if (usable.Count < 2 || usableEvents == 0)
            {
                foreach (var r in opened) r.Dispose();
                _mergeSkip.Add(SegmentKey.Of(sources[0]));
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
                _logger.LogWarning(ex,
                    "Merge: aborted while streaming {Count} source(s) — sources left intact, batch retried next pass",
                    consumed.Count);
            }
            try { File.Delete(manifestPath); } catch { /* recovery drops it anyway */ }
            return false;
        }
        finally { _flushConcurrency.Release(); }

        _segments[SegmentKey.Of(info)] = info;
        foreach (var seg in consumed)
            await DeleteSegmentAsync(SegmentKey.Of(seg), ct);

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
    /// frame, and the merge's own event-count verification. <see cref="EndOfStreamException"/> is
    /// a truncated file. Both are properties of the bytes on disk and will still be true on the
    /// next pass. Everything else — no space left, an I/O error, a file momentarily locked — is
    /// the machine's condition rather than the segments', and gets another attempt.</para>
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

        var perLevel = new List<int>[LevelSegmentSlots];
        for (int oi = 0; oi < order.Length; oi++)
        {
            int lvl = (int)hot.GetHeader(order[oi]).Level;
            if ((uint)lvl >= LevelSegmentSlots) lvl = (int)Ameto.Core.LogLevel.Information;   // defensive
            (perLevel[lvl] ??= new List<int>()).Add(order[oi]);
        }

        var written = new List<SegmentInfo>(LevelSegmentSlots);
        for (int lvl = 0; lvl < LevelSegmentSlots; lvl++)
        {
            var idx = perLevel[lvl];
            if (idx is null || idx.Count == 0) continue;

            var segId = new SegmentId(firstSegId + (ulong)lvl);
            if (skipPublishedLevels && SegmentFileExists(segId.Value))
            {
                _logger.LogInformation(
                    "WAL recovery: level {Level} is already published as segment {Id} — {Count} event(s) not rewritten",
                    (Ameto.Core.LogLevel)lvl, segId.Value, idx.Count);
                continue;
            }

            var subset  = idx.ToArray();
            var segPath = BuildSegmentPath(segId, hot, subset);
            written.Add(await FlushToColdAsync(hot, segId, segPath, ct, subset));
            _afterLevelPublished?.Invoke(lvl);
        }

        // ── MARKER LAST. Every level is on disk and fsynced (SegmentWriter.Finalise flushes to
        //    the platter before the move), so this file is the durable "the block is complete"
        //    record the WAL delete keys off. Written THROUGH for the same reason the merge
        //    manifest is: the protocol is an ordering between these files and an unlink, and an
        //    ordering only the page cache observes does not survive a power loss.
        WriteFlushCompletionMarker(firstSegId, written);
        return written;
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

    private Task<SegmentInfo> FlushToColdAsync(
        HotTierSegment hot, SegmentId segId, string segPath, CancellationToken ct, int[]? order_ = null)
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
                    writer.WriteEvents(new HotTierEventSource(hot, TemplatePool, order), sinkFactory);
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

    private void LoadSegmentCatalog()
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Clean up leftover temp files from interrupted flushes / re-compressions
        foreach (var pattern in new[] { "*.seg.tmp", "*.hctmp" })
            foreach (var tmp in Directory.EnumerateFiles(_segDir, pattern))
            {
                try { File.Delete(tmp); }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete leftover temp segment {File}", tmp); }
            }

        // Finish merges interrupted between publishing the merged segment and
        // deleting its sources — otherwise those events would be served twice.
        RecoverInterruptedMerges();

        foreach (var file in Directory.EnumerateFiles(_segDir, "*.seg"))
        {
            try
            {
                using var reader = SegmentReader.Open(file, computeUncompressedBytes: true);
                var info = reader.Info;
                _segments[SegmentKey.Of(info)] = info;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Skipping corrupt segment {File}", file);
                try { File.Delete(file); }
                catch (Exception delEx) { _logger.LogWarning(delEx, "Failed to delete corrupt segment {File}", file); }
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

        // Load template pool
        var pool = WriteAheadLog.LoadPool(poolPath);
        if (pool.Count == 0)
        {
            _logger.LogWarning("Orphaned WAL {File}: no template pool, discarding {Count} events",
                walFile, entries.Count);
            try { File.Delete(walFile); } catch { }
            try { File.Delete(poolPath); } catch { }
            return;
        }

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
            var header = new LogEventHeader
            {
                Id                       = _idGen.Next(entry.TimestampTicks),
                TimestampUtcTicks        = entry.TimestampTicks,
                Level                    = entry.Level,
                MessageTemplatePoolIndex = entry.TemplateIndex,
            };
            // Resolve template via the freshly restored pool and attach it
            // to the hot tier so the recovery flush persists @mt correctly.
            string tmpl = TemplatePool.Get(entry.TemplateIndex);
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
                _segments[SegmentKey.Of(written[i])] = written[i];
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
    /// <para>The allocator floor is raised whatever the catalog then does with the entry, and it
    /// is load-bearing rather than tidy. It costs nothing, it keeps the directory readable to a
    /// human, and it is what stops the OTHER writer into the catalog — a local flush, which must
    /// always register its own segment and so cannot refuse anything — from ever being handed an
    /// id an import already occupies. It also still matters for the one case the key cannot
    /// separate: a peer misconfigured with this node's own id, where <c>SegmentFileExists</c> and
    /// the WAL block probe — both of which match <c>{localNode}-{id}-*.seg</c> — would otherwise
    /// be looking at somebody else's file.</para>
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

        // Before the catalog, and unconditionally — see the remark above. Monotonic, so an
        // import that is about to be refused can only ever move it forward. Taken outside
        // _importLock so that the only lock ordering in this method is no ordering at all.
        AdvanceSegmentIdFloor(staged.Id.Value + 1);

        bool inPlace = string.Equals(stagedPath, finalPath, StringComparison.OrdinalIgnoreCase);
        var  info    = inPlace ? staged : AtPath(staged, finalPath);

        // First segment under a key keeps it. The assignment this replaces warned about the clash
        // and then performed it anyway, which is the same eviction the key change removed, in the
        // one case the key cannot separate — two nodes carrying the same NodeId. Leaving the
        // catalog is leaving queries, retention and the merge planner at once, so the refused file
        // held disk for the life of the install, never served, never expired, never compacted.
        //
        // A lock rather than a compare-and-swap on the dictionary, because the decision and the
        // rename have to be one step: two request threads importing different segments under one
        // key would otherwise both find it free, both rename onto the same final path, and the
        // loser's bytes would be gone from disk whatever the catalog then said. The critical
        // section is a dictionary lookup and a rename — no read of the file's contents, nothing
        // awaited — and imports arrive one per replicated segment, not one per event.
        lock (_importLock)
        {
            if (_segments.TryGetValue(key, out var existing) && !IsTheSameSegment(existing, info))
            {
                _logger.LogError(
                    "Refused replicated segment {File}: {Key} is already held by {Existing}, which " +
                    "carries the same node id AND the same segment id. Two nodes appear to be " +
                    "configured as NodeId {Node} — a deployment error no id space can resolve. The " +
                    "segment already being served was kept and the incoming one was NOT registered.",
                    staged.FilePath, key, existing.FilePath, info.NodeId);
                return SegmentImportOutcome.Conflict;
            }

            if (!inPlace) File.Move(stagedPath, finalPath, overwrite: true);
            _segments[key] = info;
        }

        _logger.LogInformation("Imported replicated segment {Key} ({Events} events)", key, info.EventCount);
        return SegmentImportOutcome.Registered;
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
    /// recovery segment — can subsequently be handed an id inside it.
    /// </summary>
    private void OpenWal()
    {
        _walSegId = AllocateSegmentIdBlock();

        var segId   = new SegmentId(_walSegId);
        var walPath = Path.Combine(_walDir, $"{_options.NodeId.Value}-{segId.Value}.wal");
        _wal        = WriteAheadLog.Open(walPath, _options.NodeId, segId);
    }

    /// <param name="order">
    /// Tier indices this segment will contain; null = the whole tier. The file name
    /// carries the range, and retention reads MaxTimestamp out of it, so a level-split
    /// segment must be named from ITS OWN events rather than the tier's.
    /// </param>
    private string BuildSegmentPath(SegmentId segId, HotTierSegment hot, int[]? order = null)
    {
        long minTs = long.MaxValue, maxTs = long.MinValue;
        int n = order?.Length ?? hot.Count;
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
        try { await _maintenanceLoop; }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }

        // Await all in-flight parallel flushes before freeing the frozen tiers they
        // read — disposing their native memory mid-flush faults (AccessViolation).
        // No new flush can start: the age loop is stopped and no writes remain.
        try { await Task.WhenAll(_inFlightFlushes.Keys.ToArray()); } catch { /* best-effort */ }

        if (_hot.Count > 0)
        {
            try { await TryFlushAsync(); } catch { /* best-effort final flush */ }
            // TryFlushAsync's heavy phase runs to completion inline here (we awaited it),
            // but a concurrent trigger may have scheduled another — drain those too.
            try { await Task.WhenAll(_inFlightFlushes.Keys.ToArray()); } catch { }
        }

        _hot.Dispose();
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
        _wal?.Dispose();
        _flushConcurrency.Dispose();
        _flushSlots.Dispose();
        _flushLock.Dispose();
        _cts.Dispose();
    }
}
