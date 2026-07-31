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

    // Monotonic segment counter
    private          ulong                                _nextSegmentId = 1;

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

        try { await Task.Delay(TimeSpan.FromMinutes(3), ct); } // let startup + catalog load settle
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

            // Publish oldHot + reserve its segment ids under the lock queries snapshot
            // from, so a concurrent query sees oldHot's events AND skips the reserved
            // cold segment ids (no duplicates during the register/remove overlap).
            // A tier flushes to ONE SEGMENT PER LEVEL, so a whole block of ids is
            // reserved and the level's segment is always firstId + (byte)level. Levels
            // absent from the tier simply never become files; a burnt id costs nothing.
            reservedSegId = _nextSegmentId;
            lock (_frozenLock) { _frozenHot.Add((oldHot, reservedSegId)); }

            _hot = CreateHotTier();

            // Rotate the WAL: bump the counter first so the new WAL uses the *next* id.
            // The OLD WAL is disposed in the heavy phase, off the swap lock — disposing
            // flushes up to 64 MB of dirty mmap pages to disk, and doing that here
            // stalled every writer (hot tier stays full for the whole swap) long enough
            // to overflow the ingest ring under sustained 100k/s load.
            _nextSegmentId += LevelSegmentSlots;
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

    // ── Small-segment merge (compaction) ──────────────────────────────────────

    private const long MergeCandidateMaxBytes = 8L * 1024 * 1024;   // only merge segments smaller than this

    // ── Batch budgets: POLICY, not memory ─────────────────────────────────────
    //
    // These used to be a memory bound. A merge read every source with ReadAllRaw into a
    // managed List<RawSegmentEvent>, copied it into a HotTierSegment and indexed the whole
    // batch, so peak ≈ 3× the batch — 32 MB / 100k events was chosen to make one merge cost
    // about one flush. Two further gates existed only because of the TIER: a hot tier divides
    // by FIXED chunks (idx / 16384, 8 MB payload each), so a prop-dense batch could overflow
    // chunk 0 no matter how small it was in total. That forced a co-fit gate
    // (UncompressedBytes <= 4 MB, EventCount <= 8192) which excluded dense segments from
    // compaction ENTIRELY — on the sandbox stand, most of the files.
    //
    // The writer now consumes a k-way merged STREAM (MergingSegmentEventSource) and holds one
    // block plus one index group, so peak is flat in the merged size and there is no chunk
    // geometry left to fit. What remains is a policy choice about how big a merged file should
    // be: big enough that per-file index/catalog overhead disappears, small enough that one
    // pass is interruptible and one expiry deletes a sensible unit. Sources are still consumed
    // whole, and the 24 h span cap still bounds how much longer the oldest events outlive their
    // per-segment deadline.
    private const long MergeTargetPayloadBytes = 256L * 1024 * 1024; // uncompressed payload per merged file
    private const int  MergeMinSources = 8;                          // don't bother below this
    // Each source contributes one open reader and one decompressed block (~64 KB) for the
    // length of the merge — the k-way merge's only per-source cost, ~36 MB at this cap.
    private const int  MergeMaxSources = 512;
    private const int  MergeMaxEvents  = 2_000_000;
    private const long MergeMaxSpanTicks = 24L * TimeSpan.TicksPerHour; // retention granularity per merged file
    private const int  MergeWindowAttempts = 4;  // window re-selections per pass after an anchor skip

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
    /// <para>Retention-aware selection: segment expiry is
    /// <c>MaxTimestamp + Ttl(MinLevel)</c>, so a batch only combines segments of
    /// the SAME retention TTL class (merging a 3-day-TTL debug segment into a
    /// 90-day one would either delete the neighbours' events early or keep the
    /// debug ones 30× longer), and a batch never spans more than
    /// <see cref="MergeMaxSpanTicks"/> — the span is exactly how much longer the
    /// oldest events can outlive their per-segment deadline.</para>
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

        long settledCutoff = DateTimeOffset.UtcNow.AddHours(-48).UtcTicks;

        // A skipped window used to burn the whole maintenance pause (600 s) on a
        // single discarded anchor. Skips are rare — what remains is unreadable or
        // empty segments — so when one happens, re-select immediately. Bounded and
        // livelock-free: every failed attempt adds to _mergeSkip first, so the
        // candidate set strictly shrinks.
        List<SegmentInfo>?   consumed = null;
        List<SegmentReader>? readers  = null;
        for (int attempt = 0; attempt < MergeWindowAttempts && consumed is null; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            // Oldest-first so the stable "past" consolidates and stays consolidated;
            // grouped by retention TTL so a merge never changes any event's expiry
            // class. Every group gets a chance — an exhausted oldest group must not
            // starve the others. Recomputed per attempt: _mergeSkip may have grown.
            var groups = _segments.Values
                .Where(s => s.CompressedBytes < MergeCandidateMaxBytes
                         && !_mergeSkip.Contains(s.Id.Value))
                // Group by LEVEL, not by TTL class. Segments are level-pure as written
                // (see FlushTierByLevelAsync), and grouping by TTL would merge Information
                // with Error — same 90-day class, different levels — handing the merged
                // file back the mixed-level shape whose retention this change exists to
                // make exact. Same-level implies same TTL, so the old invariant still holds.
                .GroupBy(s => s.MinLevel)
                .Select(g => g.OrderBy(s => s.MinTimestampTicks).ToList())
                .Where(g => g.Count >= 2)
                .OrderBy(g => g[0].MinTimestampTicks);

            // Bound the batch's time span (two-pointer over each sorted group). A
            // RECENT window needs MergeMinSources segments (no point churning while
            // flushes still arrive), but a SETTLED window (older than 48 h) merges
            // from 2 sources — quiet days produce a handful of tiny age-flush
            // segments per day, and "not worth it" thresholds would strand them as
            // unmergeable forever (observed live: ~1,000 files parked this way).
            List<SegmentInfo>? window = null;
            foreach (var group in groups)
            {
                int start = 0;
                while (start <= group.Count - 2)
                {
                    long windowStart = group[start].MinTimestampTicks;
                    var w = group.Skip(start)
                        .TakeWhile(s => s.MaxTimestampTicks - windowStart <= MergeMaxSpanTicks)
                        .Take(MergeMaxSources)
                        .ToList();
                    int minSources = w.Count > 0 && w[^1].MaxTimestampTicks < settledCutoff
                        ? 2 : MergeMinSources;
                    if (w.Count >= minSources) { window = w; break; }
                    start += Math.Max(1, w.Count);
                }
                if (window is not null) break;
            }
            if (window is null) return false;
            var candidates = window;

            // Pick the batch from CATALOG METADATA alone — no file is opened, let alone
            // decoded, until the merge itself streams it. Segments are consumed whole: a
            // batch never contains part of a segment, so a source is either fully duplicated
            // into the merged file or not consumed at all, which is what makes the delete
            // step safe to interrupt.
            var sources    = new List<SegmentInfo>();
            long payload   = 0;
            long eventCount = 0;
            foreach (var seg in candidates)
            {
                if (sources.Count > 0 &&
                    (eventCount >= MergeMaxEvents || payload >= MergeTargetPayloadBytes))
                    break;
                sources.Add(seg);
                payload    += Math.Max(seg.UncompressedBytes, seg.CompressedBytes);
                eventCount += seg.EventCount;
            }

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

            // Anti-stall: a window whose anchor can't produce even a 2-segment batch
            // would be re-selected forever — exclude the anchor until restart.
            // Debug, not Warning: this is the planner's EXPECTED outcome whenever there is
            // simply nothing to merge (a day's log carried 54 of these, every anchor
            // different) — at WRN it drowns the signal it was meant to be.
            if (usable.Count < 2 || usableEvents == 0)
            {
                foreach (var r in opened) r.Dispose();
                _mergeSkip.Add(candidates[0].Id.Value);
                _logger.LogDebug("Merge: window anchored at {File} yields no usable batch — anchor skipped",
                    Path.GetFileName(candidates[0].FilePath));
                continue;
            }
            consumed = usable;
            readers  = opened;
        }
        if (consumed is null || readers is null) return false;

        // Reserve a segment id from the same counter the flush path uses.
        ulong reserved;
        await _flushLock.WaitAsync(ct);
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
        try
        {
            await File.WriteAllLinesAsync(manifestPath, consumed.Select(s => Path.GetFileName(s.FilePath)), ct);
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
            info = await MergeToColdAsync(readers, segId, segPath, ct);
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

        // Refuse to delete the sources unless every one of their events is in the merged
        // file. The counts come from file headers on both sides, so a mismatch means the
        // stream lost or duplicated rows — abort loudly rather than delete the originals.
        if (info.EventCount != expectEvents)
        {
            _logger.LogError(
                "Merge: wrote {Written} events but sources hold {Expected} — merged file discarded, sources kept",
                info.EventCount, expectEvents);
            foreach (var s in consumed) _mergeSkip.Add(s.Id.Value);
            try { File.Delete(segPath); }      catch { /* left for the tmp sweep */ }
            try { File.Delete(manifestPath); } catch { }
            return false;
        }

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
    private Task<SegmentInfo> MergeToColdAsync(
        List<SegmentReader> readers, SegmentId segId, string segPath, CancellationToken ct)
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
                    writer.WriteEvents(source, sinkFactory);
                    info = writer.Finalise(_options.NodeId, segId);
                }
                // Close the readers BEFORE the caller starts deleting sources: on Windows a
                // mapped file cannot be unlinked, and a leaked view would leave the merge
                // permanently stuck in its "sources still held open" recovery path.
                source.Dispose();
                source = null;
                foreach (var r in readers) r.Dispose();

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

                // WAL already flushed (segment exists) — delete orphaned WAL
                if (_segments.ContainsKey(segId))
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

            // Flush recovered events to a cold segment (no index — acceptable for crash recovery)
            if (recoveredHot?.Count > 0)
            {
                var segId   = new SegmentId(_nextSegmentId++);
                var segPath = BuildSegmentPath(segId, recoveredHot);
                var info    = FlushToColdAsync(recoveredHot, segId, segPath, CancellationToken.None)
                                  .GetAwaiter().GetResult();
                _segments[segId.Value] = info;
                _logger.LogInformation("WAL recovery: wrote segment {Id} with {Count} events", segId, info.EventCount);
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

    private void OpenWal()
    {
        var segId   = new SegmentId(_nextSegmentId);
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
