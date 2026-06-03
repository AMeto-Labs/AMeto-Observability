using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Rd.Log.Core;

namespace Rd.Log.Storage;

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
    public Func<HotTierSegment, StringInternPool, (byte[], byte[], byte[])>? IndexBuilder { get; set; }

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
    private readonly SemaphoreSlim                        _flushLock  = new(1, 1);
    private readonly CancellationTokenSource               _cts        = new();
    private readonly Task                                  _flushLoop;

    // Cold-tier catalog (thread-safe)
    private readonly ConcurrentDictionary<ulong, SegmentInfo> _segments = new();

    // Hot tiers that have been frozen but whose cold-tier segment file is still
    // being written (or has just been registered but we haven't released the
    // reference yet). Queries must read from these to avoid a visibility gap
    // during flush. Mutated under <see cref="_frozenLock"/>.
    private readonly List<(HotTierSegment Tier, ulong SegId)> _frozenHot = new();
    private readonly object                                   _frozenLock = new();

    // Previously-frozen hot tier awaiting disposal. Disposed on the next flush
    // cycle so any in-flight query that captured the reference has time to drain.
    private HotTierSegment?                                   _retiredHot;

    // Number of HotTierReaderSnapshot instances currently in-flight. Incremented
    // by OpenHotTierReader, decremented by snapshot.Dispose(). Used to eagerly
    // release the retired hot tier when no query could possibly observe it.
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
        _idGen    = new EventIdGenerator(_options.NodeId);
        _dataDir  = _options.DataDirectory;
        _walDir   = Path.Combine(_dataDir, "wal");
        _segDir   = Path.Combine(_dataDir, "segments");

        Directory.CreateDirectory(_walDir);
        Directory.CreateDirectory(_segDir);

        _hot = CreateHotTier();
        LoadSegmentCatalog();
        ReplayOrphanedWals();
        OpenWal();

        // Age-based flush loop
        _flushLoop = RunFlushLoopAsync(_cts.Token);
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

    public IHotTierReader OpenHotTierReader()
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
        return new HotTierReaderSnapshot(current, frozen, covered, TemplatePool, this);
    }

    private void OnReaderDisposed()
    {
        Interlocked.Decrement(ref _activeReaders);
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
            _ = Task.Run(() => TryFlushAsync());
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
            _ = Task.Run(() => TryFlushAsync());

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

    private async Task TryFlushAsync(CancellationToken ct = default)
    {
        if (!await _flushLock.WaitAsync(0, ct)) return; // another flush in progress

        HotTierSegment? oldHot     = null;
        WriteAheadLog?  oldWal     = null;
        string?         oldWalPath = null;
        bool            addedToFrozen = false;
        ulong           reservedSegId = 0;
        try
        {
            if (_hot.Count == 0)
                return;

            // Atomically swap hot tier
            oldHot     = _hot;
            oldWal     = _wal;
            oldWalPath = oldWal?.FilePath;
            oldHot.Freeze();

            // Reserve the next segment id and publish oldHot to the frozen-tier
            // list under the same lock that queries use to snapshot. Queries
            // opened from this point on will see oldHot's events AND skip the
            // reserved cold segment id (which will exist briefly during the
            // overlap between segment registration and frozen-list removal).
            reservedSegId = _nextSegmentId;
            lock (_frozenLock)
            {
                _frozenHot.Add((oldHot, reservedSegId));
                addedToFrozen = true;
            }

            _hot = CreateHotTier();

            // Advance segment counter and open new WAL *before* releasing old WAL,
            // so the drain loop never writes to a disposed WAL.
            // Bump _nextSegmentId first so OpenWal uses the *next* id, not the same one.
            _nextSegmentId++;
            _wal = null;          // guard: if OpenWal throws, drain loop stops writing
            oldWal?.Dispose();    // release file lock before opening new file
            oldWal = null;
            OpenWal();

            // Flush old hot tier to cold segment (no index yet — Indexing layer adds them)
            var segId   = new SegmentId(reservedSegId);
            var segPath = BuildSegmentPath(segId, oldHot);
            var info    = await FlushToColdAsync(oldHot, segId, segPath, ct);

            // Atomically: register the cold segment AND drop oldHot from the
            // frozen list. The reader's CoveredSegmentIds still includes the
            // segment id (snapshot was taken earlier), so the cold scan will
            // skip it and we avoid duplicates.
            lock (_frozenLock)
            {
                _segments[segId.Value] = info;
                _frozenHot.RemoveAll(f => ReferenceEquals(f.Tier, oldHot));
                addedToFrozen = false;
            }
            _logger.LogInformation("Flushed segment {Id}: {Count} events → {Path}", segId, info.EventCount, segPath);

            // Notify cluster replicator
            SegmentFlushed?.Invoke(info);

            // Delete WAL and companion pool file after successful flush
            if (oldWalPath is not null)
            {
                try { File.Delete(oldWalPath); }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete WAL {Path}", oldWalPath); }
                try { File.Delete(oldWalPath + ".pool"); } catch { /* best-effort */ }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Segment flush failed");
            // On failure, leave oldHot in _frozenHot so its events remain
            // visible to queries; the WAL on disk lets recovery replay them on
            // restart. Skip the retirement step below — oldHot is still in use.
            if (addedToFrozen) oldHot = null;
        }
        finally
        {
            // Retirement: if no readers are active, dispose oldHot immediately;
            // otherwise rotate it through _retiredHot so any in-flight query
            // that captured the reference before we removed it from _frozenHot
            // has time to drain (released on the next flush cycle).
            HotTierSegment? toDispose;
            if (Volatile.Read(ref _activeReaders) == 0)
            {
                toDispose   = _retiredHot;
                _retiredHot = null;
                oldHot?.Dispose();
            }
            else
            {
                toDispose   = _retiredHot;
                _retiredHot = oldHot;
            }
            toDispose?.Dispose();
            oldWal?.Dispose();
            _flushLock.Release();
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

            if (indexBuilder is not null)
            {
                (invertedBytes, trigramBytes, bloomBytes) = indexBuilder(hot, TemplatePool);
            }

            // Write to a temp file first; rename to final path only after Finalise()
            // succeeds. This prevents corrupt .seg files when the process is killed mid-flush.
            string tmpPath = segPath + ".tmp";
            try
            {
                SegmentInfo info;
                using (var writer = new SegmentWriter(tmpPath))
                {
                    writer.WriteEvents(hot, TemplatePool);
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

    private void LoadSegmentCatalog()
    {
        // Clean up leftover temp files from interrupted flushes
        foreach (var tmp in Directory.EnumerateFiles(_segDir, "*.seg.tmp"))
        {
            try { File.Delete(tmp); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete leftover temp segment {File}", tmp); }
        }

        foreach (var file in Directory.EnumerateFiles(_segDir, "*.seg"))
        {
            try
            {
                using var reader = SegmentReader.Open(file);
                var info = reader.Info;
                _segments[info.Id.Value] = info;
                if (info.Id.Value >= _nextSegmentId)
                    _nextSegmentId = info.Id.Value + 1;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Skipping corrupt segment {File}", file);
                try { File.Delete(file); }
                catch (Exception delEx) { _logger.LogWarning(delEx, "Failed to delete corrupt segment {File}", file); }
            }
        }
        _logger.LogInformation("Loaded {Count} segments from {Dir}", _segments.Count, _segDir);
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

        if (_hot.Count > 0)
        {
            try { await TryFlushAsync(); }
            catch { /* best-effort final flush */ }
        }

        _hot.Dispose();
        _retiredHot?.Dispose();
        lock (_frozenLock)
        {
            foreach (var (tier, _) in _frozenHot) tier.Dispose();
            _frozenHot.Clear();
        }
        _wal?.Dispose();
        _flushLock.Dispose();
        _cts.Dispose();
    }
}
