using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage;

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
    /// <summary>Low-priority sweep re-compressing cold segments from fast-LZ4 to HC.</summary>
    private readonly Task                                  _recompressLoop;
    /// <summary>Test hook: lets merge run without an index builder (tests verify the scan fallback).</summary>
    internal bool _allowIndexlessMerge;
    /// <summary>Test hook: shrinks the index-group budget so a small segment still spans several groups.</summary>
    internal long _groupPayloadBudgetBytes = SegmentWriter.DefaultGroupPayloadBudgetBytes;
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
    /// <summary>Window anchors that produced no usable merge batch — excluded so the sweep advances (reset on restart).</summary>
    private readonly HashSet<ulong> _mergeSkip = new();

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

    // Cold-tier catalog (thread-safe)
    private readonly ConcurrentDictionary<ulong, SegmentInfo> _segments = new();
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
    private          ulong                                _nextSegmentId = 1;

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
        // Cold-segment HC re-compression sweep (one-time per segment, ~20-30 % smaller)
        _recompressLoop = RunRecompressLoopAsync(_cts.Token);
    }

    /// <summary>
    /// Cold-tier maintenance: first MERGE small segments into large ones (a
    /// long-running server accumulates thousands of ~100 KB age-flush segments —
    /// per-file index/catalog overhead dwarfs the payload), then re-compress
    /// whatever segments remain on the fast flush-path LZ4 level to LZ4-HC
    /// (see <see cref="SegmentRecompressor"/>). Both are one-shot per segment and
    /// paced by bytes; the flush path stays untouched, so ingest is unaffected.
    /// </summary>
    private async Task RunRecompressLoopAsync(CancellationToken ct)
    {
        const long MaxBytesPerPass = 96L * 1024 * 1024;
        const int  MaxAttempts     = 3;   // per segment; a persistently locked file must not stall the sweep
        // A pass below the shared floor does NOT get a maintenance release. The idle
        // steady-state pass recompresses the one or two tiny segments flushed since the
        // last tick ("saved 0.0 MB") — a day's log showed that shape paying a blocking
        // compacting gen2 + working-set dump six times an hour, around the clock, with
        // the working set sawing between ~50 and ~130 MB. The burst worth returning is
        // proportional to the bytes a pass actually chewed through, so gate on that.
        var attempts = new Dictionary<ulong, int>();

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

            // Merge takes priority: HC-ing a tiny file that a later merge rewrites
            // anyway would be wasted work. One batch per iteration, short pause
            // while a backlog exists.
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
                ReleaseMaintenanceMemory();
                try { await Task.Delay(TimeSpan.FromSeconds(15), ct); }
                catch (OperationCanceledException) { break; }
                continue;
            }
            int  done = 0;
            long passBytes = 0, savedTotal = 0;
            foreach (var (segId, info) in _segments)
            {
                if (ct.IsCancellationRequested || passBytes >= MaxBytesPerPass) break;
                int tried = attempts.GetValueOrDefault(segId);
                if (tried >= MaxAttempts) continue;
                if (!SegmentRecompressor.IsCandidate(info.FilePath))
                {
                    attempts[segId] = MaxAttempts; // done already / not applicable — never re-check
                    continue;
                }

                long? saved = await Task.Run(
                    () => SegmentRecompressor.Recompress(info.FilePath, _logger, ct), ct);
                done++;
                passBytes += Math.Max(info.CompressedBytes, 1);
                if (saved is > 0)
                {
                    attempts[segId] = MaxAttempts;
                    savedTotal += saved.Value;
                    // Keep the catalog's size accurate for diagnostics.
                    _segments.TryUpdate(segId, new SegmentInfo
                    {
                        Id                = info.Id,
                        NodeId            = info.NodeId,
                        FilePath          = info.FilePath,
                        MinTimestampTicks = info.MinTimestampTicks,
                        MaxTimestampTicks = info.MaxTimestampTicks,
                        EventCount        = info.EventCount,
                        MinLevel          = info.MinLevel,
                        CompressedBytes   = Math.Max(0, info.CompressedBytes - saved.Value),
                        UncompressedBytes = info.UncompressedBytes,
                    }, info);
                }
                else
                {
                    attempts[segId] = tried + 1; // busy or skipped — bounded retries
                }
            }

            if (savedTotal > 0)
                _logger.LogInformation("Recompressed {Count} log segment(s), saved {Mb:F1} MB",
                    done, savedTotal / 1048576.0);

            if (done > 0 && passBytes >= AggressiveGcGate.MaintenancePassBytesFloor) ReleaseMaintenanceMemory();

            try { await Task.Delay(TimeSpan.FromSeconds(passBytes >= MaxBytesPerPass ? 30 : 600), ct); }
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
    private (HotTierSegment Current, HotTierSegment[] Frozen, HashSet<ulong> Covered) SnapshotTiers()
    {
        HotTierSegment    current;
        HotTierSegment[]  frozen;
        HashSet<ulong>    covered;
        lock (_frozenLock)
        {
            current = _hot;
            if (_frozenHot.Count == 0)
            {
                frozen  = Array.Empty<HotTierSegment>();
                covered = new HashSet<ulong>();
            }
            else
            {
                frozen  = new HotTierSegment[_frozenHot.Count];
                covered = new HashSet<ulong>(_frozenHot.Count * LevelSegmentSlots);
                for (int i = 0; i < _frozenHot.Count; i++)
                {
                    frozen[i] = _frozenHot[i].Tier;
                    // The tier flushes to one segment per level, so every id in its
                    // reserved block is covered — otherwise a query would serve the
                    // already-registered per-level segments AND the still-frozen tier.
                    ulong first = _frozenHot[i].SegId;
                    for (int s = 0; s < LevelSegmentSlots; s++) covered.Add(first + (ulong)s);
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
                        if (covered.Contains(info.Id.Value)) continue;
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
        HashSet<ulong>   covered,
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

        public IReadOnlySet<ulong> CoveredSegmentIds => covered;

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

    public Task DeleteSegmentAsync(SegmentId segmentId, CancellationToken ct = default)
    {
        if (_segments.TryRemove(segmentId.Value, out var info))
        {
            try { File.Delete(info.FilePath); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete segment {Id}", segmentId); }
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
                    foreach (var w in written) _segments[w.Id.Value] = w;
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
    //   - similarly sized (within MergeOpenSizeRatio, and the run's largest no more than
    //     MergeGrowthFactor of its total) is what makes a straggler cost the straggler. The
    //     previous rule dropped the size guard entirely for a sealed bucket, so one late row and
    //     the bucket's collapsed file were admissible together: measured, five one-event flushes
    //     into a collapsed bucket cost five merges and 7151 KB of writes, and the cost never
    //     decayed because the rewritten file was still under the maximal threshold.
    //   - time-contiguous is what makes the pieces of an over-large bucket partition it rather
    //     than interleave it (see MergeTargetPayloadBytes), and it is why the run BREAKS at the
    //     first file it cannot take instead of skipping past it.

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
    /// log₈(maximal / flush size) ≈ 2.6 times at the stand's ~1.3 MB flush segments and 512 MB
    /// target. MEASURED over 1000 flushes with stragglers, at the same size ratio: 1.70x,
    /// 1.80x, 2.86x, 2.92x per stretch — flat, not a staircase.</para>
    ///
    /// <para>It is STRICTLY BELOW <see cref="MergeMinSources"/>, and that inequality is the
    /// point. At ratio 8 with a fanout of 8, a merge's own output is exactly 8× its sources and
    /// therefore admissible beside the very next batch of flush segments — so the freshly
    /// written file is rewritten again for the next 8 arrivals, at double the cost, forever.
    /// MEASURED over 1000 flushes with stragglers: 3.34x steady state at ratio 8 against 2.92x
    /// at ratio 4, for the same fanout and the same data.</para>
    ///
    /// <para>Dropping it for sealed buckets — the previous rule — is what made a one-row
    /// straggler cost a full bucket rewrite. It is kept for sealed buckets now; what a sealed
    /// bucket relaxes is only the FANOUT (see <see cref="MergeSealedMinSources"/>).</para>
    /// </summary>
    private const int MergeOpenSizeRatio = 4;

    /// <summary>
    /// A merge must grow its largest source by at least this factor, expressed as the fraction
    /// of that source the REST of the batch has to add up to (1/2 ⇒ the output is ≥ 1.5× the
    /// largest input).
    ///
    /// <para><see cref="MergeOpenSizeRatio"/> alone does not bound amplification once a bucket
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
    /// </summary>
    private const int MergeSpanTtlDivisor = 12;

    /// <summary>
    /// Sources an OPEN bucket needs before a merge is worth doing. This is the fanout: the run
    /// it gates is what multiplies a file's size by ~<see cref="MergeOpenSizeRatio"/>, and the
    /// number of rewrites a byte sees is log of the size range in that multiplier. Eight trades
    /// ~2.5 rewrites per byte at the stand's geometry against holding up to eight uncompacted
    /// flush segments per level in the catalog.
    /// </summary>
    private const int MergeMinSources = 8;

    /// <summary>
    /// Sources a SEALED bucket needs. Two, because a quiet day leaves a handful of tiny segments
    /// that a fanout of eight would strand forever (observed live: ~1,000 files parked that
    /// way), and because a low fanout is no longer dangerous — <see cref="MergeGrowthFactor"/>
    /// is what stops a pair being "the bucket's big file plus one straggler", which is the shape
    /// that used to make this number costly.
    /// </summary>
    private const int MergeSealedMinSources = 2;
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
    /// </summary>
    internal static long MergeBucketTicks(TimeSpan ttl)
    {
        long budget = Math.Max(TimeSpan.TicksPerHour, ttl.Ticks / MergeSpanTtlDivisor);
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
            if (_mergeSkip.Contains(s.Id.Value)) continue;

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
            long width = MergeBucketTicks(policy.GetTtl(key.Level));
            bool isSealed = now - (key.Start + width) >= MergeSealGraceTicks(width);

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

            var run = SelectMergeRun(list, isSealed ? MergeSealedMinSources : MergeMinSources, target, maximal);
            if (run is not null) return run;
        }
        return null;
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
    /// <item>SIZE SPREAD — the run's largest may be at most <see cref="MergeOpenSizeRatio"/>×
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
    private static List<SegmentInfo>? SelectMergeRun(
        List<SegmentInfo> byTime, int minSources, long target, long maximal)
    {
        for (int start = 0; start + 1 < byTime.Count; start++)
        {
            var  run     = new List<SegmentInfo>(Math.Min(byTime.Count - start, MergeMaxSources));
            long payload = 0, events = 0, runMin = long.MaxValue, runMax = 0;

            for (int i = start; i < byTime.Count && run.Count < MergeMaxSources; i++)
            {
                var  s = byTime[i];
                long p = Math.Max(1, SegmentPayloadBytes(s));
                long lo = Math.Min(runMin, p), hi = Math.Max(runMax, p);
                if (run.Count > 0 && (hi > lo * MergeOpenSizeRatio ||
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
                    _mergeSkip.Add(seg.Id.Value);
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
                _mergeSkip.Add(sources[0].Id.Value);
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
        ulong reserved;
        try
        {
            await _flushLock.WaitAsync(ct);
        }
        catch
        {
            // The only cancellable step between opening the sources and taking ownership of
            // them. Unguarded, shutdown left up to MergeMaxSources mapped views alive for the
            // life of the process — and on Windows a mapped file cannot be unlinked, so those
            // segments could then be neither compacted nor expired.
            foreach (var r in readers) r.Dispose();
            throw;
        }
        try { reserved = _nextSegmentId; _nextSegmentId++; }
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
            info = await MergeToColdAsync(readers, segId, segPath, expectEvents, ct);
        }
        catch (Exception ex)
        {
            // Nothing has been deleted and the merged file never reached segPath, so the
            // only state to undo is the manifest. A source that failed mid-stream is
            // skip-listed so the next pass does not re-select the same doomed window.
            _logger.LogWarning(ex, "Merge: aborted while streaming {Count} source(s) — sources left intact", consumed.Count);
            foreach (var s in consumed) _mergeSkip.Add(s.Id.Value);
            try { File.Delete(manifestPath); } catch { /* recovery drops it anyway */ }
            return false;
        }
        finally { _flushConcurrency.Release(); }

        _segments[segId.Value] = info;
        foreach (var seg in consumed)
            await DeleteSegmentAsync(seg.Id, ct);

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
    private Task<SegmentInfo> MergeToColdAsync(
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
                SegmentInfo info;
                source = new MergingSegmentEventSource(readers);
                using (var writer = new SegmentWriter(tmpPath, groupBudget))
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
        }, ct);
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
    /// </summary>
    private async Task<List<SegmentInfo>> FlushTierByLevelAsync(
        HotTierSegment hot, ulong firstSegId, CancellationToken ct)
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

            var subset  = idx.ToArray();
            var segId   = new SegmentId(firstSegId + (ulong)lvl);
            var segPath = BuildSegmentPath(segId, hot, subset);
            written.Add(await FlushToColdAsync(hot, segId, segPath, ct, subset));
        }
        return written;
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
            await DeleteSegmentAsync(seg.Id, ct);
            _logger.LogInformation("Retention: deleted segment {Id} (expires {Max})", seg.Id, seg.MaxTimestamp);
        }

        return new RetentionRunResult(expired.Count, expired.Sum(s => s.CompressedBytes), 0, 0, now);
    }

    // ── Startup recovery ──────────────────────────────────────────────────────

    /// <summary>
    /// Seeds <see cref="_nextSegmentId"/> from segment file NAMES only — must run
    /// synchronously before the first flush so new segments never reuse an id,
    /// while the expensive per-file catalog load happens in the background.
    /// </summary>
    private void InitNextSegmentIdFromFileNames()
    {
        foreach (var file in Directory.EnumerateFiles(_segDir, "*.seg"))
        {
            // {nodeId}-{segId}-{minTs}-{maxTs}.seg
            var parts = Path.GetFileNameWithoutExtension(file).Split('-');
            if (parts.Length >= 2 && ulong.TryParse(parts[1], out var segId) && segId >= _nextSegmentId)
                _nextSegmentId = segId + 1;
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
                _segments[info.Id.Value] = info;
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
    /// True when the flush of the WAL named <paramref name="walSegId"/> left at least one
    /// segment file behind.
    ///
    /// <para>The whole reserved BLOCK is probed, not just the first id: a tier writes one
    /// segment per level and only for the levels it actually holds, so a tier of pure
    /// Information events produces <c>walSegId + 2</c> and nothing at <c>walSegId</c>.
    /// Checking the first id alone reads "never flushed" for that tier and replays a WAL
    /// whose events are already cold.</para>
    /// </summary>
    private bool FlushedSegmentExistsOnDisk(ulong walSegId)
    {
        for (ulong id = walSegId; id < walSegId + LevelSegmentSlots; id++)
            foreach (var _ in Directory.EnumerateFiles(_segDir, $"{_options.NodeId.Value}-{id}-*.seg"))
                return true;
        return false;
    }

    private void ReplayOrphanedWals()
    {
        var walFiles = Directory.EnumerateFiles(_walDir, "*.wal").ToList();
        if (walFiles.Count == 0) return;

        HotTierSegment? recoveredHot = null;
        try
        {
            foreach (var walFile in walFiles)
            {
                string poolPath = walFile + ".pool";
                var (segId, entries) = WriteAheadLog.ReadForRecovery(walFile);

                // Empty or corrupt WAL — clean up
                if (segId == 0 || entries.Count == 0)
                {
                    try { File.Delete(walFile); } catch { }
                    try { File.Delete(poolPath); } catch { }
                    continue;
                }

                // WAL already flushed — delete the orphan. "Flushed" is read off the segment
                // DIRECTORY, not off _segments: the catalog load runs in the background (it
                // opens every file), so a catalog lookup here races the enumeration that
                // would answer it, and losing that race replays a WAL whose events are
                // already in cold storage — duplicates.
                if (FlushedSegmentExistsOnDisk(segId))
                {
                    _logger.LogInformation("WAL {File} already flushed — removing", walFile);
                    try { File.Delete(walFile); } catch { }
                    try { File.Delete(poolPath); } catch { }
                    continue;
                }

                // Load template pool
                var pool = WriteAheadLog.LoadPool(poolPath);
                if (pool.Count == 0)
                {
                    _logger.LogWarning("Orphaned WAL {File}: no template pool, discarding {Count} events",
                        walFile, entries.Count);
                    try { File.Delete(walFile); } catch { }
                    continue;
                }

                // Restore templates into TemplatePool
                foreach (var (idx, tmpl) in pool)
                    TemplatePool.ForceIntern(idx, tmpl);

                // Replay entries into recovered hot tier
                recoveredHot ??= CreateHotTier();
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
                try { File.Delete(walFile); } catch { }
                try { File.Delete(poolPath); } catch { }
            }

            // Flush recovered events to cold segments (no index — acceptable for crash recovery),
            // ONE PER LEVEL like the live flush path. Writing the recovered tier as a single
            // mixed-level segment reopened the data loss the level split exists to prevent:
            // expiry is Ttl(MinLevel), TTL is not monotonic in the level's value, so a
            // recovered tier holding one Debug event put every Error beside it on a 3-day
            // deadline. Crash recovery is exactly when that is least acceptable.
            if (recoveredHot?.Count > 0)
            {
                ulong firstSegId = _nextSegmentId;
                _nextSegmentId  += LevelSegmentSlots;
                var written = FlushTierByLevelAsync(recoveredHot, firstSegId, CancellationToken.None)
                                  .GetAwaiter().GetResult();
                foreach (var info in written) _segments[info.Id.Value] = info;
                _logger.LogInformation("WAL recovery: wrote {Segments} level segment(s), {Count} events",
                    written.Count, written.Sum(w => (long)w.EventCount));
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "WAL recovery failed");
        }
        finally
        {
            recoveredHot?.Dispose();
        }
    }

    /// <summary>
    /// Registers a replicated segment file received from the cluster leader.
    /// The file must already reside in the segments directory.
    /// </summary>
    public void ImportSegment(string filePath)
    {
        try
        {
            using var reader = SegmentReader.Open(filePath, computeUncompressedBytes: true);
            var info = reader.Info;
            _segments[info.Id.Value] = info;
            if (info.Id.Value >= _nextSegmentId)
                _nextSegmentId = info.Id.Value + 1;
            _logger.LogInformation("Imported replicated segment {Id} ({Events} events)", info.Id, info.EventCount);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to import replicated segment {File}", filePath);
        }
    }

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
        _walSegId      = _nextSegmentId;
        _nextSegmentId += LevelSegmentSlots;

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
        try { await _recompressLoop; }
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
