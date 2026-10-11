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
// at the next start and despills them then, skipping the levels the start found already published,
// so a block half-written before the crash is finished, not duplicated (see DiscoverSpillFiles).
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

    /// <summary>
    /// A spill file waiting for its despill: its block, its WAL's path, and the levels of the block
    /// an earlier run published (see <see cref="QueuedSpill.PublishedLevels"/>).
    /// </summary>
    private readonly record struct SpillFile(ulong SegId, string Path, byte PublishedLevels = 0);

    /// <summary>
    /// A spilled block not yet in segments. It stays in <see cref="_spillQueue"/> — and in
    /// <see cref="SpillFilesPending"/> — until its despill SETTLES it: published, found already
    /// published, or left for a later release. A block used to leave the queue when the loop took
    /// it, so the one being replayed, or waiting on a flush retry, read as nothing pending, and one
    /// whose read failed was never tried again in this process.
    /// </summary>
    private sealed class QueuedSpill(string path, byte publishedLevels = 0)
    {
        public readonly string Path = path;

        /// <summary>
        /// The levels of the block an EARLIER run had already published (bit <c>1 &lt;&lt; level</c>;
        /// a level is one whole file at block + level), as found when this run started — before
        /// any merge or retention pass could take one away. The despill skips exactly these; 0 for
        /// a block spilled in this run. See <see cref="DiscoverSpillFiles"/>.
        /// </summary>
        public readonly byte PublishedLevels = publishedLevels;

        /// <summary>Taken by the despill loop: being despilled, or with its flush retry. Under <see cref="_spillQueueLock"/>.</summary>
        public bool InFlight;

        /// <summary><see cref="Environment.TickCount64"/> before which a despill that failed is not tried again. Under <see cref="_spillQueueLock"/>.</summary>
        public long NotBefore;

        /// <summary>Despills of the block that failed in a row: each doubles the wait before the next. Under <see cref="_spillQueueLock"/>.</summary>
        public int Failures;
    }

    /// <summary>Spilled blocks not yet in segments, oldest block first. Under <see cref="_spillQueueLock"/>.</summary>
    private readonly SortedDictionary<ulong, QueuedSpill> _spillQueue = new();
    private readonly System.Threading.Lock                _spillQueueLock = new();

    /// <summary>
    /// How long a despill that failed before its flush waits before it is tried again — the first
    /// time: each failure in a row doubles it, up to <see cref="_despillRetryCap"/>. Internal so a
    /// test can shorten it.
    /// </summary>
    internal TimeSpan _despillRetryDelay = FlushRetryDelay;

    /// <summary>
    /// The longest a block whose despill keeps failing waits between attempts. Every attempt reads
    /// the whole block again — a block that cannot fit the heap (written under a larger tier, or
    /// heavy with exceptions) is an out-of-memory for the whole process each time — and the attempts
    /// used to come every 15 s for ever. Internal so a test can shorten it.
    /// </summary>
    internal TimeSpan _despillRetryCap = TimeSpan.FromMinutes(10);

    /// <summary>Wakes the despill loop: a block was queued, a slot came free, the spill ended.</summary>
    private readonly SemaphoreSlim _despillSignal = new(0, 1);

    /// <summary>The background loop despilling queued blocks (<see cref="RunDespillLoopAsync"/>).</summary>
    private readonly Task _despillLoop;

    /// <summary>Spill files being closed off the swap (see <see cref="CloseSpill"/>), so shutdown can await them.</summary>
    private readonly ConcurrentDictionary<Task, byte> _spillCloses = new();

    /// <summary>The spill floor was hit and said so; cleared when a spill file opens again. Under <c>_flushLock</c>.</summary>
    private bool _spillFloorReported;

    /// <summary>An open of a spill file failed and said so; cleared when one opens again. Under <c>_flushLock</c>.</summary>
    private bool _spillOpenFailureReported;

    /// <summary><see cref="Environment.TickCount64"/> before which no spill file is opened, after an open failed; 0 when none has. Under <c>_flushLock</c>.</summary>
    private long _spillOpenRetryAt;

    /// <summary>How long a failed spill open is left before the next attempt.</summary>
    private static readonly TimeSpan SpillOpenRetryDelay = TimeSpan.FromSeconds(1);

    /// <summary>Test hook: called as a spill file is about to be opened; throwing from it is an open that failed.</summary>
    internal Action? _beforeSpillOpenForTest;

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

    /// <summary>
    /// Spilled blocks closed and not yet in segments — waiting for a flush slot, being despilled,
    /// or waiting to be tried again: the backlog on disk. Zero means every spilled event this run
    /// knows of is in segments.
    /// </summary>
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
            if (!spill.Wal.Append(h.TimestampUtcTicks, h.Level, h.MessageTemplatePoolIndex, tmplStr, payload, exception,
                                  h.ServiceNamePoolIndex, svcStr, h.TraceIdHi, h.TraceIdLo, h.SpanId))
            {
                // The file is as full as the floor check counted it — large exceptions, which the
                // admission does not weigh, fill it before a tier's worth of events. Closed here
                // like a block the admission refused: the swap rotates it, and checks the floor
                // before it opens the next. The admission counted this event; that only closes the
                // block an event early.
                spill.Full = true;
                ScheduleFlush();
                return WriteOutcome.NoRoom;
            }
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

        // An open that failed a moment ago is left alone until SpillOpenRetryDelay has passed: the
        // writers wait meanwhile, as at the floor. Every swap attempt used to try again — and with
        // the slots busy, each waiting writer re-asks ten times a second — so a volume the floor
        // check cannot see (a UNC path, where DriveInfo throws; a mount folder or junction, where it
        // measures the root volume) turned a full disk into hundreds of failing opens a second,
        // each a Warning with its stack, each burning a block of segment ids.
        if (_spillOpenRetryAt != 0 && Environment.TickCount64 < _spillOpenRetryAt) return;

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
            _spillOpenRetryAt = Environment.TickCount64 + (long)SpillOpenRetryDelay.TotalMilliseconds;
            if (!_spillOpenFailureReported)
            {
                _spillOpenFailureReported = true;
                _logger.LogWarning(ex,
                    "Could not open a spill file — ingest waits for a flush slot instead. Another is tried at most every " +
                    "{Delay}s, and a failure is reported again only once a spill file has opened", SpillOpenRetryDelay.TotalSeconds);
            }
            return;
        }
        _spillFloorReported       = false;
        _spillOpenFailureReported = false;
        _spillOpenRetryAt         = 0;

        // Same tier, same WAL: what is known about them comes along.
        var nextState = new WriteState(state.Hot, state.Wal, state.WalSegId, next) { HotRefused = true, WalFaulted = state.WalFaulted };
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
        return free >= _options.HotTier.SpillMinFreeDiskBytes + SpillFileCapacityBytes;
    }

    /// <summary>
    /// What one spill file is, and — since it never grows (see <see cref="WriteAheadLog.Open"/>) —
    /// the most it can come to: a tier's payload plus a header and trace context an event, and at
    /// least 1 MB. A block whose entries weigh more (large exceptions, long inline templates) is
    /// closed when its file is full, and the next one is checked against the floor before it opens.
    /// </summary>
    internal long SpillFileCapacityBytes => Math.Max(1L << 20,
        _options.HotTier.MaxSizeBytes + (long)HotTierSegment.EventCapacityFor(_options.HotTier.MaxSizeBytes) * 50);

    /// <summary>Opens the next spill target, reserving its level block as a WAL does.</summary>
    private SpillTarget OpenSpillTarget()
    {
        ulong segId = AllocateSegmentIdBlock();
        string path = Path.Combine(_walDir, $"{_options.NodeId.Value}-{segId}{SpillExtension}");
        _beforeSpillOpenForTest?.Invoke();
        // spill: never grows past its capacity, the space HasSpillRoom counted, and is trimmed when closed.
        var    wal  = WriteAheadLog.Open(path, _options.NodeId, new SegmentId(segId), SpillFileCapacityBytes, spill: true);
        Interlocked.Increment(ref _spillFilesOpened);
        long tier = _options.HotTier.MaxSizeBytes;
        return new SpillTarget(wal, segId, new HotTierAdmission(HotTierSegment.EventCapacityFor(tier), tier));
    }

    /// <summary>
    /// Closes a spill target no writer can reach any more (it has been replaced under
    /// <see cref="_writerLock"/>) and queues it for its despill. Off the swap: disposing a mapping
    /// writes its dirty pages back, which is the stall the WAL's own rotation keeps off the lock.
    /// During shutdown it is done inline, so nothing is left running past the teardown.
    ///
    /// <para>Runs after a swap has been installed, so it must not leave the block in limbo when
    /// memory is short: a close that cannot even be queued is done here, inline, and a block that
    /// cannot be queued for its despill says so — the next start finds the file and despills it.</para>
    /// </summary>
    private void CloseSpill(SpillTarget spill)
    {
        if (Volatile.Read(ref _writesClosed) != 0)
        {
            Close(spill);
            return;
        }

        Task t;
        try { t = Task.Run(() => Close(spill)); }
        catch (Exception)
        {
            Close(spill);   // slower, on the swap, but closed and queued
            return;
        }
        try
        {
            _spillCloses[t] = 0;
            _ = t.ContinueWith(
                static (x, s) => ((ConcurrentDictionary<Task, byte>)s!).TryRemove(x, out _),
                _spillCloses, CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
        catch (Exception) { /* only shutdown's wait for this close is lost; the close itself is running */ }

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
            if (Volatile.Read(ref _writesClosed) != 0) return;
            try { EnqueueSpill(new SpillFile(s.SegId, s.Path)); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Spilled block {File} could not be queued for its despill — it is written into " +
                                     "segments at the next start", s.Path);
            }
        }
    }

    private void EnqueueSpill(SpillFile file)
    {
        lock (_spillQueueLock) _spillQueue[file.SegId] = new QueuedSpill(file.Path);
        WakeDespill();
    }

    /// <summary>The block's despill is over — published, found already published, or not this release's to read.</summary>
    private void SettleSpill(ulong segId)
    {
        lock (_spillQueueLock) _spillQueue.Remove(segId);
    }

    /// <summary>
    /// The block's despill failed before its flush: it stays queued, and is tried again after
    /// <see cref="_despillRetryDelay"/>, doubled for each failure in a row and at most
    /// <see cref="_despillRetryCap"/>.
    /// </summary>
    private void RetrySpillLater(ulong segId)
    {
        lock (_spillQueueLock)
        {
            if (!_spillQueue.TryGetValue(segId, out var q)) return;
            q.InFlight  = false;
            q.Failures++;
            q.NotBefore = Environment.TickCount64 + (long)DespillRetryDelayAfter(q.Failures).TotalMilliseconds;
        }
        WakeDespill();   // the loop recomputes its wait from the new retry time
    }

    /// <summary>The wait after the <paramref name="failures"/>-th failure in a row: the first delay, doubled each time, capped.</summary>
    private TimeSpan DespillRetryDelayAfter(int failures)
    {
        double ms = _despillRetryDelay.TotalMilliseconds * Math.Pow(2, Math.Clamp(failures - 1, 0, 30));
        return TimeSpan.FromMilliseconds(Math.Min(ms, Math.Max(_despillRetryCap.TotalMilliseconds, _despillRetryDelay.TotalMilliseconds)));
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
    ///
    /// <para>A swap that failed earlier gets this slot first, at once: what it failed on may have
    /// cleared — the flush or despill that just ended freed disk and memory — so its failure is
    /// forgotten here, and the despill yields to it again until it fails once more.</para>
    /// </summary>
    private void OnFlushSlotReleased()
    {
        if (Volatile.Read(ref _writesClosed) != 0) return;
        Volatile.Write(ref _swapFailedAt, 0);
        Volatile.Write(ref _swapRetryAt, 0);
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
    ///
    /// <para>And it notes, for each block, which of its levels an earlier run had already
    /// published — a despill the process died in moves one whole file per level. That is decided
    /// HERE, from the disk as this run finds it, and not by the despill: the boot catalog scan
    /// registers those files as ordinary segments, nothing keeps a merge or a retention pass off
    /// them while the despill waits in the background (behind other blocks, or a new spill), and
    /// the despill used to ask the disk only when it ran — finding a level a merge had taken into
    /// another segment gone, and writing its events a second time, for good. The maintenance loop
    /// does not start before this has run (it is the constructor's).</para>
    /// </summary>
    private void DiscoverSpillFiles()
    {
        int found = 0;
        HashSet<ulong>? onDisk = null;
        foreach (var file in Directory.EnumerateFiles(_walDir, "*" + SpillExtension + "*"))
        {
            if (!file.EndsWith(SpillExtension, StringComparison.OrdinalIgnoreCase)) continue;   // its .pool
            if (!TryParseBlockId(file, out ulong segId))
            {
                _logger.LogWarning("Spill file {File} does not carry a block id in its name — left where it is", file);
                continue;
            }
            ReserveWalBlock(segId);
            onDisk ??= LocalSegmentIdsOnDisk();
            byte published = 0;
            for (int lvl = 0; lvl < LevelSegmentSlots; lvl++)
                if (onDisk.Contains(segId + (ulong)lvl)) published |= (byte)(1 << lvl);
            lock (_spillQueueLock) _spillQueue[segId] = new QueuedSpill(file, published);
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

    /// <summary>The ids of this node's segment files on disk, read off their names (<c>{node}-{id}-{min}-{max}.seg</c>) in one listing.</summary>
    private HashSet<ulong> LocalSegmentIdsOnDisk()
    {
        var    ids    = new HashSet<ulong>();
        string prefix = $"{_options.NodeId.Value}-";
        foreach (var path in Directory.EnumerateFiles(_segDir, prefix + "*.seg"))
        {
            var name = Path.GetFileName(path.AsSpan());
            if (!name.StartsWith(prefix, StringComparison.Ordinal)) continue;
            var rest = name[prefix.Length..];
            int dash = rest.IndexOf('-');
            if (dash > 0 && ulong.TryParse(rest[..dash], System.Globalization.NumberStyles.None,
                                           System.Globalization.CultureInfo.InvariantCulture, out ulong id))
                ids.Add(id);
        }
        return ids;
    }

    /// <summary><c>{node}-{block}.spill</c> or <c>{node}-{block}.wal</c> → the block: a log is named after the block it flushes into.</summary>
    private static bool TryParseBlockId(string file, out ulong segId)
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
    /// Despills queued blocks, oldest first, one at a time, each in a free flush slot — and none while the live tier
    /// needs the slot: while the engine is spilling, or while the live tier is refusing writes. A
    /// slot that comes free then goes to the swap, so new events are searchable again first and the
    /// backlog drains behind them — unless that swap has just failed (OnSwapFailed), when the slot
    /// would otherwise sit idle.
    /// </summary>
    private async Task RunDespillLoopAsync(Task? heldUntil, CancellationToken ct)
    {
        if (heldUntil is not null)
        {
            try { await heldUntil.WaitAsync(ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }

        while (!ct.IsCancellationRequested)
        {
            // The timeout is a safety net, not the cadence: every event that can make work possible
            // wakes the signal — save a failed despill's retry time, which the wait is cut to.
            try { await _despillSignal.WaitAsync(DespillWaitTimeout(), ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }

            while (!ct.IsCancellationRequested && Volatile.Read(ref _writesClosed) == 0)
            {
                // The live tier comes first: a spill in progress, or a tier refusing writes — full
                // with spilling off or the disk at its floor, or in the moment before a spill is
                // installed — is waiting for exactly the slot this would take. It used to yield
                // only to an installed spill, and after each despill it took the next slot inline,
                // ahead of the swap the release had queued with Task.Run: with spilling off and a
                // backlog of spill files left by an earlier run, every slot that came free went to
                // the next despill while requests waited out BackPressureWait and were answered 503.
                // But not to a swap that cannot install: one that has failed since the last slot
                // came free (a disk too full for the next WAL, say) would leave the slot idle, and
                // the backlog — whose despill is what frees that disk — unwritten for as long as
                // it went on failing.
                var live = _write;
                if ((live.Spill is not null || Volatile.Read(ref live.HotRefused)) && !SwapFailing) break;

                SpillFile next = default;
                lock (_spillQueueLock)
                {
                    long now = Environment.TickCount64;
                    foreach (var (segId, q) in _spillQueue)
                    {
                        if (q.InFlight || q.NotBefore > now) continue;
                        next = new SpillFile(segId, q.Path, q.PublishedLevels);
                        break;
                    }
                }
                if (next.Path is null) break;

                if (!_flushSlots.Wait(0)) break;   // released by the despill, or by its retry
                lock (_spillQueueLock)
                    if (_spillQueue.TryGetValue(next.SegId, out var taken)) taken.InFlight = true;

                try { await DespillAsync(next, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
                catch (Exception ex)
                {
                    // A despill that failed before its flush — the block could not be read (out of
                    // memory materialising it, the file held by a scanner), or not be listed. It is
                    // still queued (DespillAsync's finally) and tried again after a delay; it used to
                    // be dropped from the queue, its acknowledged events unsearchable — and counted
                    // nowhere — until the process restarted. The delay doubles with each failure in
                    // a row, and only the first of them is an Error with its stack: a block that
                    // cannot be read now usually cannot be read in 15 s either, and the same Error
                    // every 15 s, for ever, buried the line that said why.
                    int failures;
                    lock (_spillQueueLock) failures = _spillQueue.TryGetValue(next.SegId, out var q) ? q.Failures : 0;
                    try
                    {
                        if (failures <= 1)
                            _logger.LogError(ex,
                                "Despill of {File} failed — tried again in {Delay}s, then less and less often while it keeps " +
                                "failing (at least once every {Cap}s), each failure after this one logged at Debug; the file stays on disk meanwhile",
                                next.Path, DespillRetryDelayAfter(1).TotalSeconds, _despillRetryCap.TotalSeconds);
                        else
                            _logger.LogDebug(
                                "Despill of {File} failed again ({Failures} times in a row): {Error} — tried again in {Delay}s",
                                next.Path, failures, ex.Message, DespillRetryDelayAfter(failures).TotalSeconds);
                    }
                    catch (Exception) { /* the block is queued for its retry either way */ }
                }
            }
        }
    }

    /// <summary>How long the loop may sleep: until the earliest retry time of a failed despill, and never past the safety net.</summary>
    private TimeSpan DespillWaitTimeout()
    {
        const long SafetyNetMs = 5000;
        long now = Environment.TickCount64, soonest = long.MaxValue;
        lock (_spillQueueLock)
            foreach (var q in _spillQueue.Values)
                if (!q.InFlight && q.NotBefore > now && q.NotBefore < soonest) soonest = q.NotBefore;
        return TimeSpan.FromMilliseconds(soonest == long.MaxValue ? SafetyNetMs : Math.Clamp(soonest - now, 1, SafetyNetMs));
    }

    /// <summary>
    /// Test seam: an engine constructed while this holds a task starts despilling only once that
    /// task completes — how a test sets its hooks before the despill of a block an earlier run left,
    /// which otherwise begins as soon as the engine exists. An <see cref="AsyncLocal{T}"/>, as
    /// <see cref="HoldCatalogScanForTest"/> is, so it reaches only the engines the setting test
    /// constructs. Null in production.
    /// </summary>
    internal static readonly AsyncLocal<Task?> HoldDespillForTest = new();

    /// <summary>Test hook: the despills that have completed, including those that found nothing to write.</summary>
    internal int DespillsCompletedForTest => Volatile.Read(ref _despillsCompleted);
    private int _despillsCompleted;

    /// <summary>
    /// Test hook: called with the block's id as a despill that holds its flush slot is about to read
    /// the spill file. Blocking it holds the slot; throwing from it is a read that failed.
    /// </summary>
    internal Action<ulong>? _beforeLoadSpillForTest;

    /// <summary>
    /// Replays one spilled block into a tier and flushes it into the block, exactly as a frozen tier
    /// is flushed: listed as frozen (so queries see it) for its heavy phase, published with the same
    /// completion marker, its WAL deleted against it. Owns the flush slot the caller took.
    /// </summary>
    private async Task DespillAsync(SpillFile file, CancellationToken ct)
    {
        bool slotTransferred = false;
        bool counted         = false;
        bool settled         = false;   // published, found published already, or not this release's to read
        HotTierSegment? unlisted = null; // read but not yet listed as frozen: freed here, whatever way we leave
        try
        {
            // Flushed in full by an earlier life, which stopped before the unlink.
            if (File.Exists(FlushMarkerPath(file.SegId)))
            {
                DeleteSpillFiles(file);
                settled = true;
                return;
            }

            _beforeLoadSpillForTest?.Invoke(file.SegId);
            var tier = LoadSpill(file);
            if (tier is null)
            {
                settled = true;
                return;
            }
            unlisted = tier;

            // Counted and listed under the swap lock, as a swap counts its tier: shutdown takes this
            // lock after closing the write path, and from then on the count may only fall. A
            // shutdown that cancels this wait used to leave the tier — up to a tier's native memory —
            // unfreed: it is still `unlisted`, and the finally frees it.
            await _flushLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (Volatile.Read(ref _writesClosed) != 0) return;   // never listed; the file is the next start's
                lock (_frozenLock)
                {
                    _frozenHot.EnsureCapacity(_frozenHot.Count + 1);   // what can fail, before the count
                    Interlocked.Increment(ref _heavyPhases);
                    counted = true;
                    _frozenHot.Add((tier, file.SegId));
                }
                unlisted = null;
            }
            finally { ReleaseFlushLock(); }

            await _flushConcurrency.WaitAsync(ct).ConfigureAwait(false);
            List<SegmentInfo> written;
            try
            {
                // A block a crash left half-written keeps the levels it got out — the ones found on
                // disk when this run STARTED, not now: the boot scan registers them as ordinary
                // segments, and a merge or retention pass may have taken one since. Asking the disk
                // here found it gone and wrote its events a second time.
                written = await FlushTierByLevelAsync(tier, file.SegId, ct, publishedLevels: file.PublishedLevels).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex,
                    "Despill of {File} failed — its tier stays frozen and queryable; retrying in background every {Delay}s",
                    file.Path, FlushRetryDelay.TotalSeconds);
                // Owns the slot and the count from here; the block stays queued, in flight, until the
                // retry publishes it — or shutdown leaves it for the next start.
                ScheduleFlushRetry(tier, file.Path, file.SegId,
                                   onPublished: retried => DespillPublished(file.SegId, retried, afterRetry: true),
                                   publishedLevels: file.PublishedLevels);
                slotTransferred = true;
                return;
            }
            finally { _flushConcurrency.Release(); }

            PublishFlushedTier(written, tier, file.Path, file.SegId);
            DespillPublished(file.SegId, written, afterRetry: false);
            settled = true;
        }
        finally
        {
            unlisted?.Dispose();
            if (!slotTransferred)
            {
                // Before the slot goes back, so the loop that takes it sees this block as gone, or
                // as not ready yet.
                if (settled) SettleSpill(file.SegId);
                else         RetrySpillLater(file.SegId);
                _flushSlots.Release();
                if (counted) EndHeavyPhase();
                OnFlushSlotReleased();
            }
            Interlocked.Increment(ref _despillsCompleted);
        }
    }

    /// <summary>A despilled block is in segments: counted, out of the queue, and said so.</summary>
    private void DespillPublished(ulong segId, List<SegmentInfo> written, bool afterRetry)
    {
        long events = 0;
        foreach (var w in written) events += w.EventCount;
        Interlocked.Add(ref _despilledEvents, events);
        int failures;
        lock (_spillQueueLock) failures = _spillQueue.TryGetValue(segId, out var q) ? q.Failures : 0;
        SettleSpill(segId);
        // The end of an episode of failed despills says so: the failures after its first were at Debug.
        string retried = afterRetry ? ", after a retry" : "";
        if (failures > 0) retried += $", after {failures} failed attempt(s)";
        _logger.LogInformation("Despilled block {Block}: {Events} event(s) into {Segments} segment(s){Retried}",
            segId, events, written.Count, retried);
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
                // A template with no pool row — outside a saturated pool, or longer than a row holds
                // — is in the entry itself (v6); the spill file is the event's only copy, and such a
                // template used to come back empty, or cut at 65 535 bytes.
                string? tmpl = e.Template
                               ?? (!e.Unpooled && rows.TryGetValue(e.TemplateIndex, out var t) && t.Length != 0 ? t : null);
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
