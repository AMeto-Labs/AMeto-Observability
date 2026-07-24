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
    /// Optional delegate that builds all index byte sections for a frozen hot-tier segment.
    /// Injected by the Indexing layer at startup to avoid a circular project reference.
    /// Returns (invertedIndexBytes, trigramIndexBytes, bloomFilterBytes).
    /// </summary>
    /// <summary>
    /// Builds (inverted, trigram, bloom) index bytes for a frozen tier. The third argument
    /// is the file write order (<see cref="SegmentWriter.ComputeSortOrder"/>) — the builder
    /// must emit posting-list offsets in that order so they equal .seg file ordinals.
    /// </summary>
    public Func<HotTierSegment, StringInternPool, int[], (byte[], byte[], byte[])>? IndexBuilder { get; set; }

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
        // Parallel cold-flush width: concurrent index+compress+write jobs saturate idle
        // cores so flush throughput exceeds ingest and the backlog drains. Configurable
        // (HotTier.FlushConcurrency); 0 = auto ≈ processor count / 2, capped 2–8.
        int flushWidth = _options.HotTier.FlushConcurrency > 0
            ? Math.Min(_options.HotTier.FlushConcurrency, 64)
            : Math.Clamp(Environment.ProcessorCount / 2, 1, 8);
        _flushConcurrency = new SemaphoreSlim(flushWidth);
        // In-flight tier cap: bound the frozen-tier backlog to ~1 GB of RSS. A frozen tier
        // costs roughly 1.4 × MaxSizeBytes resident (payload + headers + partial last chunk),
        // so budget against that, floored at the flush width so every concurrent flush can
        // hold a slot. Smaller tiers ⇒ a deeper backlog for the same ceiling (smoother).
        long tierFootprint = (long)(Math.Max(1, _options.HotTier.MaxSizeBytes) * 1.4);
        int  flushSlots    = Math.Clamp((int)(1_073_741_824L / tierFootprint), flushWidth, 64);
        _flushSlots = new SemaphoreSlim(flushSlots, flushSlots);
        _idGen    = new EventIdGenerator(_options.NodeId);
        _dataDir  = _options.DataDirectory;
        _walDir   = Path.Combine(_dataDir, "wal");
        _segDir   = Path.Combine(_dataDir, "segments");

        Directory.CreateDirectory(_walDir);
        Directory.CreateDirectory(_segDir);

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

            try { await Task.Delay(TimeSpan.FromSeconds(passBytes >= MaxBytesPerPass ? 30 : 600), ct); }
            catch (OperationCanceledException) { break; }
        }
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
                covered = new HashSet<ulong>(_frozenHot.Count);
                for (int i = 0; i < _frozenHot.Count; i++)
                {
                    frozen[i] = _frozenHot[i].Tier;
                    covered.Add(_frozenHot[i].SegId);
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

            // Publish oldHot + reserve its segment id under the lock queries snapshot
            // from, so a concurrent query sees oldHot's events AND skips the reserved
            // cold segment id (no duplicates during the register/remove overlap).
            reservedSegId = _nextSegmentId;
            lock (_frozenLock) { _frozenHot.Add((oldHot, reservedSegId)); }

            _hot = CreateHotTier();

            // Rotate the WAL: bump the counter first so the new WAL uses the *next* id.
            // The OLD WAL is disposed in the heavy phase, off the swap lock — disposing
            // flushes up to 64 MB of dirty mmap pages to disk, and doing that here
            // stalled every writer (hot tier stays full for the whole swap) long enough
            // to overflow the ingest ring under sustained 100k/s load.
            _nextSegmentId++;
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
                var segId   = new SegmentId(reservedSegId);
                var segPath = BuildSegmentPath(segId, oldHot);

                SegmentInfo info;
                try
                {
                    info = await FlushToColdAsync(oldHot, segId, segPath, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Segment flush failed");
                    // Leave oldHot in _frozenHot so its events stay queryable; the WAL on
                    // disk replays them on restart. Do not retire — still referenced.
                    return;
                }

                // Register the cold segment AND drop oldHot from the frozen list atomically.
                lock (_frozenLock)
                {
                    _segments[segId.Value] = info;
                    _frozenHot.RemoveAll(f => ReferenceEquals(f.Tier, oldHot));
                }
                _logger.LogInformation("Flushed segment {Id}: {Count} events → {Path}", segId, info.EventCount, segPath);

                SegmentFlushed?.Invoke(info);

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
    private const long MergeTargetPayloadBytes = 96L * 1024 * 1024; // uncompressed payload budget per batch
    private const int  MergeMinSources = 8;                          // don't bother below this
    private const int  MergeMaxSources = 512;
    private const int  MergeMaxEvents  = 500_000;
    private const long MergeMaxSpanTicks = 24L * TimeSpan.TicksPerHour; // retention granularity per merged file

    /// <summary>
    /// Merges one batch of small, time-adjacent cold segments into a single large
    /// segment via the regular flush pipeline (shared sort + index build + writer),
    /// preserving event ids, timestamps and raw property/exception payloads.
    ///
    /// <para>Retention-aware selection: segment expiry is
    /// <c>MaxTimestamp + Ttl(MinLevel)</c>, so a batch only combines segments of
    /// the SAME retention TTL class (merging a 3-day-TTL debug segment into a
    /// 90-day one would either delete the neighbours' events early or keep the
    /// debug ones 30× longer), and a batch never spans more than
    /// <see cref="MergeMaxSpanTicks"/> — the span is exactly how much longer the
    /// oldest events can outlive their per-segment deadline.</para>
    ///
    /// Crash-safe: a manifest written next to the merged file lists the source
    /// files, so an interrupted deletion is finished on the next startup. Returns
    /// true when a batch was merged.
    /// </summary>
    internal async Task<bool> TryMergeSmallSegmentsOnceAsync(CancellationToken ct)
    {
        var policy = _retentionStore.GetPolicy();

        // Oldest-first so the stable "past" consolidates and stays consolidated;
        // grouped by retention TTL so a merge never changes any event's expiry class.
        var candidates = _segments.Values
            .Where(s => s.CompressedBytes < MergeCandidateMaxBytes)
            .GroupBy(s => policy.GetTtl(s.MinLevel))
            .Select(g => g.OrderBy(s => s.MinTimestampTicks).ToList())
            .Where(g => g.Count >= MergeMinSources)
            .OrderBy(g => g[0].MinTimestampTicks)
            .FirstOrDefault();
        if (candidates is null) return false;

        // Bound the batch's time span (two-pointer over the sorted group): find
        // the first 24 h window holding at least MergeMinSources segments.
        int start = 0;
        List<SegmentInfo>? window = null;
        while (start < candidates.Count - MergeMinSources + 1)
        {
            long windowStart = candidates[start].MinTimestampTicks;
            var w = candidates.Skip(start)
                .TakeWhile(s => s.MaxTimestampTicks - windowStart <= MergeMaxSpanTicks)
                .Take(MergeMaxSources)
                .ToList();
            if (w.Count >= MergeMinSources) { window = w; break; }
            start += Math.Max(1, w.Count);
        }
        if (window is null) return false;
        candidates = window;

        // Read raw events segment-by-segment until a budget is hit. Segments are
        // consumed whole — a batch never contains part of a segment.
        var dedup    = new Dictionary<string, string>(StringComparer.Ordinal);
        var events   = new List<RawSegmentEvent>();
        var consumed = new List<SegmentInfo>();
        long payload = 0;
        foreach (var seg in candidates)
        {
            ct.ThrowIfCancellationRequested();
            if (consumed.Count > 0 &&
                (events.Count >= MergeMaxEvents || payload >= MergeTargetPayloadBytes))
                break;
            List<RawSegmentEvent> segEvents;
            try
            {
                using var reader = SegmentReader.Open(seg.FilePath);
                segEvents = reader.ReadAllRaw(dedup);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Merge: skipping unreadable segment {File}", seg.FilePath);
                continue;
            }
            events.AddRange(segEvents);
            foreach (var e in segEvents) payload += e.Props?.Length ?? 0;
            consumed.Add(seg);
        }
        if (consumed.Count < MergeMinSources || events.Count == 0) return false;

        // The tier's chunks hold 16 K events / 8 MB payload each; verify the batch
        // fits BEFORE writing so TryWrite can never fail halfway through.
        if (!FitsChunkLayout(events))
        {
            _logger.LogWarning("Merge: batch of {Count} events exceeds chunk payload density — skipped", events.Count);
            return false;
        }

        var tier = new HotTierSegment(events.Count + 1, payload + (16L << 20));
        try
        {
            foreach (var e in events)
            {
                var header = new LogEventHeader
                {
                    Id                       = e.Id,
                    TimestampUtcTicks        = e.TsTicks,
                    Level                    = (Ameto.Core.LogLevel)e.Level,
                    MessageTemplatePoolIndex = TemplatePool.Intern(e.Template),
                    ServiceNamePoolIndex     = e.Service is null ? -1 : TemplatePool.Intern(e.Service),
                    TraceIdHi                = e.TraceIdHi,
                    TraceIdLo                = e.TraceIdLo,
                    SpanId                   = e.SpanId,
                };
                if (!tier.TryWrite(header, e.Props ?? ReadOnlySpan<byte>.Empty, template: null, exception: e.Exception))
                {
                    _logger.LogError("Merge: tier rejected an event ({Count} total in batch) — batch aborted", events.Count);
                    return false;
                }
            }
            tier.Freeze();

            // Reserve a segment id from the same counter the flush path uses.
            ulong reserved;
            await _flushLock.WaitAsync(ct);
            try { reserved = _nextSegmentId; _nextSegmentId++; }
            finally { _flushLock.Release(); }

            var segId   = new SegmentId(reserved);
            var segPath = BuildSegmentPath(segId, tier);
            var info    = await FlushToColdAsync(tier, segId, segPath, ct);

            // Manifest before publication: if we crash mid-deletion, startup
            // recovery deletes the remaining source files instead of serving
            // duplicate events forever.
            string manifestPath = segPath + ".mergemanifest";
            await File.WriteAllLinesAsync(manifestPath, consumed.Select(s => Path.GetFileName(s.FilePath)), ct);

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
                consumed.Count, events.Count, Path.GetFileName(segPath), info.CompressedBytes / 1048576.0);
            return true;
        }
        finally
        {
            tier.Dispose();
        }
    }

    /// <summary>Simulates the tier's chunk assignment (16 K events / 8 MB payload per chunk).</summary>
    private static bool FitsChunkLayout(List<RawSegmentEvent> events)
    {
        const int  ChunkEvents  = 16_384;
        const long ChunkPayload = 8L * 1024 * 1024;
        long chunkBytes = 0;
        for (int i = 0; i < events.Count; i++)
        {
            if (i % ChunkEvents == 0) chunkBytes = 0;
            chunkBytes += events[i].Props?.Length ?? 0;
            if (chunkBytes > ChunkPayload) return false;
        }
        return true;
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

    private Task<SegmentInfo> FlushToColdAsync(HotTierSegment hot, SegmentId segId, string segPath, CancellationToken ct)
    {
        // Capture delegate reference before entering Task.Run
        var indexBuilder = IndexBuilder;
        return Task.Run(() =>
        {
            byte[] invertedBytes = Array.Empty<byte>();
            byte[] trigramBytes  = Array.Empty<byte>();
            byte[] bloomBytes    = Array.Empty<byte>();

            // One sort order shared by the index build and the block writer: posting-list
            // offsets become file ordinals, which the reader maps back to blocks/rows.
            int[] order = SegmentWriter.ComputeSortOrder(hot);

            if (indexBuilder is not null)
            {
                (invertedBytes, trigramBytes, bloomBytes) = indexBuilder(hot, TemplatePool, order);
            }

            // Write to a temp file first; rename to final path only after Finalise()
            // succeeds. This prevents corrupt .seg files when the process is killed mid-flush.
            string tmpPath = segPath + ".tmp";
            try
            {
                SegmentInfo info;
                using (var writer = new SegmentWriter(tmpPath))
                {
                    writer.WriteEvents(hot, TemplatePool, order);
                    writer.WriteInvertedIndex(invertedBytes);
                    writer.WriteTrigramIndex(trigramBytes);
                    writer.WriteBloomFilter(bloomBytes);
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
                using var reader = SegmentReader.Open(file);
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
            using var reader = SegmentReader.Open(filePath);
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
        const int maxEvents      = 2_000_000;       // ~100k/sec × 20s headroom
        long payloadCapacity     = _options.HotTier.MaxSizeBytes;
        return new HotTierSegment(maxEvents, payloadCapacity);
    }

    private void OpenWal()
    {
        var segId   = new SegmentId(_nextSegmentId);
        var walPath = Path.Combine(_walDir, $"{_options.NodeId.Value}-{segId.Value}.wal");
        _wal        = WriteAheadLog.Open(walPath, _options.NodeId, segId);
    }

    private string BuildSegmentPath(SegmentId segId, HotTierSegment hot)
    {
        long minTs = long.MaxValue, maxTs = long.MinValue;
        for (int i = 0; i < hot.Count; i++)
        {
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
