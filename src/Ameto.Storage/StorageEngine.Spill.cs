using System.Collections.Concurrent;
using Ameto.Core;
using Microsoft.Extensions.Logging;

namespace Ameto.Storage;

// ── SPILL: WHEN THE FLUSH IS BEHIND, THE DISK TAKES WHAT RAM CANNOT ─────────────────────────────
//
// A full hot tier is swapped for a fresh one only when a flush slot is free: the slots bound the
// frozen tiers held in RAM (MemoryBudgets.NativeTierBytes), and a frozen tier is native memory that
// cannot be given back until its segments are written. With every slot busy there is no room for
// another tier — and before this, no room for another event: ingest backed up into the ring, and the
// ring dropped.
//
// Now the engine SPILLS instead. The full tier stays the live tier (queryable, unfrozen, still
// owning its WAL), and writes go to a SPILL TARGET: a WAL of its own, in the same format, with no
// tier in memory. Its events are acknowledged exactly as durably as any other — they are in a WAL —
// but they are not searchable until the block is DESPILLED: replayed into a tier and flushed into
// segments, the way crash recovery replays a WAL, once a slot is free.
//
// THE PROTOCOL IS THE WAL'S, and that is what makes this safe to add. A spill target reserves a level
// block exactly as a WAL does (OpenWalCore), holds at most what one tier admits (HotTierAdmission),
// and is flushed into that block under the same completion marker; despill deletes it against the
// marker as PublishFlushedTier deletes a WAL. A process that dies with spill files on disk finds them
// at the next start and despills them then, with skipPublishedLevels, so a block half-written before
// the crash is finished, not duplicated.
//
// What it costs: spilled events show up in queries late (after their despill), and get their event
// ids then — ids order by timestamp, so they sort where they belong. The disk is the limit:
// HotTier.SpillMinFreeDiskBytes is the floor below which no spill file is opened, and below it
// ingest waits and answers 503 as it would with spilling off.

public sealed partial class StorageEngine
{
    /// <summary>The extension of a spill target's WAL, under <c>wal/</c>. Its pool is <c>.spill.pool</c>.</summary>
    internal const string SpillExtension = ".spill";

    /// <summary>
    /// A block being spilled into: a WAL with no tier, admitting at most what one tier would.
    /// Everything mutable is written under <see cref="_writerLock"/>; a swap reads
    /// <see cref="Full"/> without it, and a stale read only means a later trigger rotates it.
    /// </summary>
    private sealed class SpillTarget(WriteAheadLog wal, ulong segId, HotTierAdmission admission)
    {
        public readonly WriteAheadLog Wal = wal;

        /// <summary>The level block this WAL reserved and its events will be flushed into.</summary>
        public readonly ulong SegId = segId;

        /// <summary>What a tier would have taken so far — the bound that keeps the despill one tier.</summary>
        public HotTierAdmission Admission = admission;

        /// <summary>Events written.</summary>
        public int Count;

        /// <summary>Set once the admission refused an event (or an append failed): the swap rotates it.</summary>
        public volatile bool Full;

        public string Path => Wal.FilePath;
    }

    /// <summary>A spill file waiting for its despill: its block and its WAL's path.</summary>
    private readonly record struct SpillFile(ulong SegId, string Path);

    /// <summary>Spilled blocks waiting to be despilled, oldest block first. Under <see cref="_spillQueueLock"/>.</summary>
    private readonly SortedDictionary<ulong, string> _spillQueue = new();
    private readonly System.Threading.Lock           _spillQueueLock = new();

    /// <summary>Wakes the despill loop: a block was queued, a slot came free, the spill ended.</summary>
    private readonly SemaphoreSlim _despillSignal = new(0, 1);

    /// <summary>The background loop despilling queued blocks (<see cref="RunDespillLoopAsync"/>).</summary>
    private readonly Task _despillLoop;

    /// <summary>Spill files being closed off the swap (see <see cref="CloseSpill"/>), so shutdown can await them.</summary>
    private readonly ConcurrentDictionary<Task, byte> _spillCloses = new();

    /// <summary>The spill floor was hit and said so; cleared when a spill file opens again. Under <c>_flushLock</c>.</summary>
    private bool _spillFloorReported;

    /// <summary>A spill episode is running and has been announced. Under <c>_flushLock</c>.</summary>
    private bool _spillAnnounced;

    /// <summary>A spill append failed and said so; cleared by the next one that succeeds. Under <see cref="_writerLock"/>.</summary>
    private bool _spillAppendFaulted;

    private long _spilledEvents;
    private long _despilledEvents;
    private long _spillFilesOpened;

    /// <summary>True while writes go to a spill target rather than the live tier.</summary>
    public bool IsSpilling => _write.Spill is not null;

    /// <summary>Events written to spill targets since start.</summary>
    public long SpilledEvents => Interlocked.Read(ref _spilledEvents);

    /// <summary>Spilled events written into segments since start (an earlier run's included).</summary>
    public long DespilledEvents => Interlocked.Read(ref _despilledEvents);

    /// <summary>Spill files opened since start.</summary>
    public long SpillFilesOpened => Interlocked.Read(ref _spillFilesOpened);

    /// <summary>Spilled blocks closed and waiting for a flush slot — the backlog on disk.</summary>
    public int SpillFilesPending { get { lock (_spillQueueLock) return _spillQueue.Count; } }

    /// <summary>
    /// Test seam: the free space a spill measures against its floor, in place of the volume's.
    /// Null in production.
    /// </summary>
    internal Func<long>? _spillFreeDiskForTest;

    // ── Writing ───────────────────────────────────────────────────────────────

    /// <summary>
    /// One write into the spill target; the caller holds <see cref="_writerLock"/>. The event gets
    /// no id here — the despill assigns it, from the same timestamp.
    /// </summary>
    private WriteOutcome SpillLocked(SpillTarget spill, in LogEventHeader h, ReadOnlySpan<byte> payload,
                                     string? template, ExceptionInfo? exception)
    {
        if (spill.Full || !spill.Admission.TryAdmit(payload.Length))
        {
            // A tier would refuse this event: the block is closed here and the next one opened by
            // the swap, so the despill replays it into ONE tier. The event waits for that one.
            spill.Full = true;
            ScheduleFlush();
            return WriteOutcome.NoRoom;
        }

        string  tmplStr = template ?? (h.MessageTemplatePoolIndex >= 0 ? TemplatePool.Get(h.MessageTemplatePoolIndex) : string.Empty);
        string? svcStr  = h.ServiceNamePoolIndex >= 0 ? TemplatePool.Get(h.ServiceNamePoolIndex) : null;
        try
        {
            spill.Wal.Append(h.TimestampUtcTicks, h.Level, h.MessageTemplatePoolIndex, tmplStr, payload, exception,
                             h.ServiceNamePoolIndex, svcStr, h.TraceIdHi, h.TraceIdLo, h.SpanId);
            _spillAppendFaulted = false;
        }
        catch (Exception ex)
        {
            // The WAL is the ONLY copy of a spilled event, so a failed append is an event not
            // written, unlike on the live tier's path. The admission above has counted it, which
            // only closes this block an event early. The block is rotated; a disk that is out of
            // room stays out of it, and the rotation's floor check is what turns that into a wait.
            spill.Full = true;
            if (!_spillAppendFaulted)
            {
                _spillAppendFaulted = true;
                _logger.LogError(ex, "Spill append to {File} failed — the event is not written; rotating the spill file", spill.Path);
            }
            ScheduleFlush();
            return WriteOutcome.NoRoom;
        }

        spill.Count++;
        Interlocked.Increment(ref _spilledEvents);
        return WriteOutcome.Written;
    }

    // ── Opening, rotating, closing (under _flushLock, from the swap) ──────────

    /// <summary>
    /// The swap found no flush slot. Opens a spill target when the live tier is refusing writes, or
    /// a fresh one when the current target is full; does nothing otherwise — a timed flush of a tier
    /// with room has no reason to send later events where nobody can search them. Caller holds
    /// <c>_flushLock</c>.
    /// </summary>
    private void SpillWithoutSlot(WriteState state)
    {
        if (!_options.HotTier.SpillEnabled) return;

        bool rotate = state.Spill is { Full: true };
        bool enter  = state.Spill is null && Volatile.Read(ref state.HotRefused);
        if (!rotate && !enter) return;

        if (!HasSpillRoom(out long free))
        {
            if (!_spillFloorReported)
            {
                _spillFloorReported = true;
                _logger.LogWarning(
                    "Ingest is waiting: the hot tier is full, every flush slot is busy, and spilling would take the " +
                    "data volume below its floor ({Free:N0} B free, HotTier.SpillMinFreeDiskBytes {Floor:N0} B plus " +
                    "one tier). Requests that find no room within Ingestion.BackPressureWait are answered 503",
                    free, _options.HotTier.SpillMinFreeDiskBytes);
            }
            return;
        }

        SpillTarget next;
        try { next = OpenSpillTarget(); }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not open a spill file — ingest waits for a flush slot instead");
            return;
        }
        _spillFloorReported = false;

        var nextState = new WriteState(state.Hot, state.Wal, state.WalSegId, next) { HotRefused = true };
        InstallWriteState(nextState);

        if (enter && !_spillAnnounced)
        {
            _spillAnnounced = true;
            _logger.LogWarning(
                "The flush is behind: every flush slot ({Slots}) is busy and the hot tier is full. Ingest goes on " +
                "into spill files under {Dir} — acknowledged and durable, written into segments as slots come free; " +
                "events spilled are not searchable until then",
                FlushSlots, _walDir);
        }

        if (state.Spill is { } old) CloseSpill(old);
    }

    /// <summary>
    /// A spill ends: the swap has a slot again and installs a fresh tier. Caller holds
    /// <c>_flushLock</c>. Says so once per episode, with what the episode left to despill.
    /// </summary>
    private void SpillEnded(SpillTarget last)
    {
        CloseSpill(last);
        if (_spillAnnounced)
        {
            _spillAnnounced = false;
            _logger.LogInformation(
                "Spill ended: ingest is back on the hot tier. {Pending} spilled block(s) to write into segments " +
                "({Spilled} event(s) spilled since start)",
                SpillFilesPending + 1, SpilledEvents);
        }
    }

    /// <summary>Whether the data volume can take another spill file and still keep its floor.</summary>
    private bool HasSpillRoom(out long free)
    {
        free = -1;
        try
        {
            free = _spillFreeDiskForTest?.Invoke() ?? new DriveInfo(Path.GetFullPath(_walDir)).AvailableFreeSpace;
        }
        catch
        {
            // The volume could not say. The open below finds out the hard way, and a failure there
            // is the same wait as the floor.
            return true;
        }
        return free >= _options.HotTier.SpillMinFreeDiskBytes + SpillFileBudgetBytes;
    }

    /// <summary>What one spill file can come to: a tier's payload plus a header and trace context an event.</summary>
    private long SpillFileBudgetBytes =>
        _options.HotTier.MaxSizeBytes
        + (long)HotTierSegment.EventCapacityFor(_options.HotTier.MaxSizeBytes) * 50;

    /// <summary>Opens the next spill target, reserving its level block as a WAL does.</summary>
    private SpillTarget OpenSpillTarget()
    {
        ulong segId = AllocateSegmentIdBlock();
        string path = Path.Combine(_walDir, $"{_options.NodeId.Value}-{segId}{SpillExtension}");
        long   cap  = Math.Max(1L << 20, SpillFileBudgetBytes);
        var    wal  = WriteAheadLog.Open(path, _options.NodeId, new SegmentId(segId), cap);
        Interlocked.Increment(ref _spillFilesOpened);
        long tier = _options.HotTier.MaxSizeBytes;
        return new SpillTarget(wal, segId, new HotTierAdmission(HotTierSegment.EventCapacityFor(tier), tier));
    }

    /// <summary>
    /// Closes a spill target no writer can reach any more (it has been replaced under
    /// <see cref="_writerLock"/>) and queues it for its despill. Off the swap: disposing a mapping
    /// writes its dirty pages back, which is the stall the WAL's own rotation keeps off the lock.
    /// During shutdown it is done inline, so nothing is left running past the teardown.
    /// </summary>
    private void CloseSpill(SpillTarget spill)
    {
        if (Volatile.Read(ref _writesClosed) != 0)
        {
            Close(spill);
            return;
        }

        var t = Task.Run(() => Close(spill));
        _spillCloses[t] = 0;
        _ = t.ContinueWith(
            static (x, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(x, out _),
            _spillCloses, CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        void Close(SpillTarget s)
        {
            if (s.Count == 0)
            {
                // Opened and replaced before anything was written into it: there is nothing to keep.
                try { s.Wal.Delete(); }
                catch (Exception ex) { _logger.LogWarning(ex, "Could not delete the empty spill file {File}", s.Path); }
                return;
            }
            try { s.Wal.Dispose(); }
            catch (Exception ex) { _logger.LogWarning(ex, "Closing the spill file {File} failed", s.Path); }
            if (Volatile.Read(ref _writesClosed) == 0)
                EnqueueSpill(new SpillFile(s.SegId, s.Path));
        }
    }

    private void EnqueueSpill(SpillFile file)
    {
        lock (_spillQueueLock) _spillQueue[file.SegId] = file.Path;
        WakeDespill();
    }

    private void WakeDespill()
    {
        if (_despillSignal.CurrentCount != 0) return;
        try { _despillSignal.Release(); }
        catch (SemaphoreFullException) { /* already woken */ }
    }

    /// <summary>
    /// A flush slot came free. If the engine is spilling, or its live tier is refusing writes, a
    /// swap can now install a fresh tier; and the despill may have a slot to take.
    /// </summary>
    private void OnFlushSlotReleased()
    {
        if (Volatile.Read(ref _writesClosed) != 0) return;
        var w = _write;
        if (w.Spill is not null || Volatile.Read(ref w.HotRefused)) ScheduleFlush();
        WakeDespill();
    }

    // ── Startup ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Queues the spill files an earlier run left — a spill that outlived its process, or one shutdown
    /// closed — for the despill loop, and reserves their blocks so nothing written meanwhile takes an
    /// id they will flush into. Called by the constructor after WAL recovery and before the first
    /// WAL is opened.
    /// </summary>
    private void DiscoverSpillFiles()
    {
        int found = 0;
        foreach (var file in Directory.EnumerateFiles(_walDir, "*" + SpillExtension + "*"))
        {
            if (!file.EndsWith(SpillExtension, StringComparison.OrdinalIgnoreCase)) continue;   // its .pool
            if (!TryParseSpillBlock(file, out ulong segId))
            {
                _logger.LogWarning("Spill file {File} does not carry a block id in its name — left where it is", file);
                continue;
            }
            ReserveWalBlock(segId);
            lock (_spillQueueLock) _spillQueue[segId] = file;
            found++;
        }
        if (found > 0)
        {
            _logger.LogInformation(
                "{Count} spilled block(s) from an earlier run found under {Dir}; they are written into segments in the background",
                found, _walDir);
            WakeDespill();
        }
    }

    /// <summary><c>{node}-{block}.spill</c> → the block.</summary>
    private static bool TryParseSpillBlock(string file, out ulong segId)
    {
        segId = 0;
        var stem = Path.GetFileNameWithoutExtension(file);
        int dash = stem.LastIndexOf('-');
        return dash > 0 && ulong.TryParse(stem.AsSpan(dash + 1), System.Globalization.NumberStyles.None,
                                          System.Globalization.CultureInfo.InvariantCulture, out segId) && segId != 0;
    }

    /// <summary>The spill file of the block <paramref name="segId"/>, or null when there is none.</summary>
    private string? SpillFileOf(ulong segId)
    {
        string path = Path.Combine(_walDir, $"{_options.NodeId.Value}-{segId}{SpillExtension}");
        return File.Exists(path) ? path : null;
    }

    // ── Despill ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Despills queued blocks, oldest first, one per free flush slot — and none while the engine is
    /// spilling: a slot that comes free then goes to the swap that ends the spill, so new events are
    /// searchable again first and the backlog drains behind them.
    /// </summary>
    private async Task RunDespillLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // The timeout is a safety net, not the cadence: every event that can make work possible
            // wakes the signal.
            try { await _despillSignal.WaitAsync(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }

            while (!ct.IsCancellationRequested && Volatile.Read(ref _writesClosed) == 0)
            {
                if (_write.Spill is not null) break;

                SpillFile next = default;
                lock (_spillQueueLock)
                {
                    foreach (var (segId, path) in _spillQueue)
                    {
                        next = new SpillFile(segId, path);
                        break;
                    }
                }
                if (next.Path is null) break;

                if (!_flushSlots.Wait(0)) break;   // released by the despill, or by its retry
                lock (_spillQueueLock) _spillQueue.Remove(next.SegId);

                try { await DespillAsync(next, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
                catch (Exception ex)
                {
                    // DespillAsync handles its own failures; this is the loop's own guard. The file
                    // stays on disk and the next start finds it.
                    _logger.LogError(ex, "Despill of {File} failed — left on disk for the next start", next.Path);
                }
            }
        }
    }

    /// <summary>Test hook: the despills that have completed, including those that found nothing to write.</summary>
    internal int DespillsCompletedForTest => Volatile.Read(ref _despillsCompleted);
    private int _despillsCompleted;

    /// <summary>
    /// Replays one spilled block into a tier and flushes it into the block, exactly as a frozen tier
    /// is flushed: listed as frozen (so queries see it) for its heavy phase, published with the same
    /// completion marker, its WAL deleted against it. Owns the flush slot the caller took.
    /// </summary>
    private async Task DespillAsync(SpillFile file, CancellationToken ct)
    {
        bool slotTransferred = false;
        bool counted         = false;
        try
        {
            // Flushed in full by an earlier life, which stopped before the unlink.
            if (File.Exists(FlushMarkerPath(file.SegId)))
            {
                DeleteSpillFiles(file);
                return;
            }

            var tier = LoadSpill(file);
            if (tier is null) return;

            // Counted and listed under the swap lock, as a swap counts its tier: shutdown takes this
            // lock after closing the write path, and from then on the count may only fall.
            await _flushLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (Volatile.Read(ref _writesClosed) != 0)
                {
                    tier.Dispose();   // never listed: nothing can be reading it. The file is the next start's.
                    return;
                }
                Interlocked.Increment(ref _heavyPhases);
                counted = true;
                lock (_frozenLock) _frozenHot.Add((tier, file.SegId));
            }
            finally { _flushLock.Release(); }

            await _flushConcurrency.WaitAsync(ct).ConfigureAwait(false);
            List<SegmentInfo> written;
            try
            {
                // skipPublishedLevels: a block a crash left half-written keeps the levels it got out.
                written = await FlushTierByLevelAsync(tier, file.SegId, ct, skipPublishedLevels: true).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex,
                    "Despill of {File} failed — its tier stays frozen and queryable; retrying in background every {Delay}s",
                    file.Path, FlushRetryDelay.TotalSeconds);
                ScheduleFlushRetry(tier, file.Path, file.SegId);   // owns the slot and the count from here
                slotTransferred = true;
                return;
            }
            finally { _flushConcurrency.Release(); }

            long events = 0;
            foreach (var w in written) events += w.EventCount;
            PublishFlushedTier(written, tier, file.Path, file.SegId);
            Interlocked.Add(ref _despilledEvents, events);
            _logger.LogInformation("Despilled block {Block}: {Events} event(s) into {Segments} segment(s)",
                file.SegId, events, written.Count);
        }
        finally
        {
            if (!slotTransferred)
            {
                _flushSlots.Release();
                if (counted) EndHeavyPhase();
                OnFlushSlotReleased();
            }
            Interlocked.Increment(ref _despillsCompleted);
        }
    }

    /// <summary>
    /// Reads a spilled block back into a tier, or returns null — after deleting it — when it holds
    /// nothing to write, or leaving it where it is when a later release wrote it.
    ///
    /// <para>Templates and services are resolved from the block's OWN pool file and interned into
    /// the live pool by their text, never force-interned by index: the live pool is in use, and a
    /// block from an earlier run numbered its strings in that run's pool. Ids are assigned here,
    /// from each event's own timestamp, as the live path does.</para>
    /// </summary>
    private HotTierSegment? LoadSpill(SpillFile file)
    {
        var (segId, entries) = WriteAheadLog.ReadForRecovery(file.Path, out ushort version, out _);
        if (version > WriteAheadLog.FormatVersion)
        {
            _logger.LogError(
                "Spill file {File} is format v{Version}, newer than this release reads (v{Current}): left in place, " +
                "not despilled. Its events come back when the release that wrote it starts again.",
                file.Path, version, WriteAheadLog.FormatVersion);
            return null;
        }
        if (segId == 0 || entries.Count == 0)
        {
            DeleteSpillFiles(file);
            return null;
        }

        var  rows    = WriteAheadLog.LoadPool(file.Path + ".pool");
        long payload = 0;
        foreach (var e in entries) payload += e.Payload.Length;

        var tier    = CreateRecoveryTier(entries.Count, payload);
        int refused = 0;
        try
        {
            foreach (var e in entries)
            {
                string? tmpl = !e.Unpooled && rows.TryGetValue(e.TemplateIndex, out var t) && t.Length != 0 ? t : null;
                int tmplIdx  = tmpl is null ? -1 : TemplatePool.Intern(tmpl, out tmpl);
                int svcAt    = e.ServiceIndexIn(rows);
                int svcIdx   = svcAt >= 0 ? TemplatePool.Intern(rows[(ushort)svcAt]) : -1;

                var header = new LogEventHeader
                {
                    Id                       = _idGen.Next(e.TimestampTicks),
                    TimestampUtcTicks        = e.TimestampTicks,
                    Level                    = e.Level,
                    MessageTemplatePoolIndex = tmplIdx,
                    ServiceNamePoolIndex     = svcIdx,
                    TraceIdHi                = e.TraceIdHi,
                    TraceIdLo                = e.TraceIdLo,
                    SpanId                   = e.SpanId,
                };
                if (tier.TryWrite(header, e.Payload, tmpl, e.Exception))
                    NotifyEventWritten(in header, tmpl ?? string.Empty);
                else
                    refused++;
            }
        }
        catch
        {
            tier.Dispose();
            throw;
        }

        if (refused > 0)
            // Cannot happen while the tier is sized from the block (CreateRecoveryTier) and the block
            // admitted only what a tier takes (HotTierAdmission) — and it would be a loss if it did,
            // so it is said loudly rather than counted quietly.
            _logger.LogError("Despill of {File}: {Refused} of {Count} event(s) did not fit the tier and were not written",
                file.Path, refused, entries.Count);
        return tier;
    }

    private void DeleteSpillFiles(SpillFile file)
    {
        try { File.Delete(file.Path); }
        catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete spill file {File}", file.Path); }
        try { File.Delete(file.Path + ".pool"); } catch { /* best-effort */ }
        try { File.Delete(FlushMarkerPath(file.SegId)); } catch { /* swept at startup */ }
    }

    /// <summary>
    /// A tier that holds <paramref name="events"/> events of <paramref name="payloadBytes"/> payload
    /// whatever <c>HotTier.MaxSizeBytes</c> says today: no smaller than a live tier, and larger when
    /// the block was written under a larger setting. A replay into a tier sized from the
    /// configuration alone dropped whatever did not fit, without a word, after a restart that had
    /// lowered it. Admission is monotone in the limits (<see cref="HotTierAdmission"/>), so what the
    /// writing tier — or a spill's admission — took in order, this one takes too.
    /// </summary>
    private HotTierSegment CreateRecoveryTier(int events, long payloadBytes)
    {
        long cfgPayload = _options.HotTier.MaxSizeBytes;
        int  cfgEvents  = HotTierSegment.EventCapacityFor(cfgPayload);
        return new HotTierSegment(Math.Max(cfgEvents, events), Math.Max(cfgPayload, payloadBytes + 1));
    }
}
