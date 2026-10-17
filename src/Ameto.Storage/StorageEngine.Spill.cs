using System.Collections.Concurrent;
using System.Runtime.InteropServices;
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

        /// <summary>
        /// The text of its events' own its tier held when a despill found no room for it beside the frozen tiers'
        /// (round-6 review R5-M11), so the loop leaves it until there is: 0 when not known. Under <see cref="_spillQueueLock"/>.
        /// </summary>
        public long TextBytes;
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

    /// <summary>
    /// <see cref="Environment.TickCount64"/> before which no spill file is opened, after an open
    /// failed; 0 until one fails, and again once one opens. Written under <c>_flushLock</c>; read
    /// without it by <see cref="SpillCanTakeTheWriters"/>, to which non-zero means "the last open failed".
    /// </summary>
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
    /// Test seam: the free space a spill measures against its floor, in place of the volume's;
    /// throwing from it is a volume that cannot say. Null in production.
    /// </summary>
    internal Func<long>? _spillFreeDiskForTest;

    // ── Writing ───────────────────────────────────────────────────────────────

    /// <summary>
    /// One write into the spill target; the caller holds <see cref="_writerLock"/>. The event gets
    /// no id here — the despill assigns it, from the same timestamp.
    /// </summary>
    private WriteOutcome SpillLocked(SpillTarget spill, in LogEventHeader h, ReadOnlySpan<byte> payload,
                                     string? template, ExceptionInfo? exception, string? service = null)
    {
        if (spill.Full || !spill.Admission.TryAdmit(payload.Length, OwnManagedBytes(h, template, exception, service)))
        {
            // A tier would refuse this event: the block is closed here and the next one opened by
            // the swap, so the despill replays it into ONE tier. The event waits for that one. The
            // text the events bring of their own counts as the tier counts it (round-5 H-D): the
            // despill's tier counts none, so the block is what holds it to a tier's worth.
            spill.Full = true;
            ScheduleFlush();
            return WriteOutcome.NoRoom;
        }

        string  tmplStr = template ?? (h.MessageTemplatePoolIndex >= 0 ? TemplatePool.Get(h.MessageTemplatePoolIndex) : string.Empty);
        string? svcStr  = h.ServiceNamePoolIndex >= 0 ? TemplatePool.Get(h.ServiceNamePoolIndex) : service;   // one outside a full pool: by its text
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
        // the slots busy, each waiting writer re-asks ten times a second — so a full disk the floor
        // check could not see (a volume that cannot say; it was every UNC path, and the root volume
        // stood in for a mount folder or a junction, before VolumeAvailableBytes) turned into
        // hundreds of failing opens a second, each a Warning with its stack, each burning a block of
        // segment ids.
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

    /// <summary>
    /// Whether the data volume can take another spill file and still keep its floor. A volume that
    /// cannot say counts as one that can: the open finds out the hard way, and a failure there is the
    /// same wait as the floor (SpillWithoutSlot holds the next open back).
    /// </summary>
    private bool HasSpillRoom(out long free)
    {
        free = SpillVolumeFreeBytes();
        return free < 0 || free >= SpillRoomBytes;
    }

    /// <summary>What the floor check needs free: the floor, a spill file with its pool, and the two rows a refused append can leave past it.</summary>
    private long SpillRoomBytes =>
        _options.HotTier.SpillMinFreeDiskBytes + SpillFileCapacityBytes + WriteAheadLog.MaxRowsPastCapacity;

    /// <summary>
    /// The free space the floor check measures (<see cref="VolumeAvailableBytes"/> of the WAL directory), or
    /// -1 when the volume cannot say — as last measured, never measured here (round-5 review L-5). The
    /// callers hold the swap lock (<see cref="SpillWithoutSlot"/>) or run the despill loop
    /// (<see cref="SpillCanTakeTheWriters"/>), and the measurement can block: on an unreachable UNC server
    /// GetDiskFreeSpaceEx waits out the SMB connect timeout, ~21 s measured, and every swap, despill and
    /// merge waited with it. A measurement older than <see cref="SpillFreeMaxAgeMs"/> (or none yet) asks for
    /// the next one, which runs on the thread pool, one at a time, off every lock; until one has finished,
    /// the volume is one that cannot say. And the WAL loop asks for one each tick once the last is that old
    /// (<see cref="RefreshSpillFreeMeasure"/>), so what a check reads is about a WAL flush interval, or that second if
    /// longer, old at the most, spilling or not.
    /// </summary>
    private long SpillVolumeFreeBytes()
    {
        if (_spillFreeDiskForTest is { } seam)
        {
            try { return seam(); }
            catch (Exception) { return -1; }
        }
        long free = Volatile.Read(ref _spillFreeMeasured);
        if (free == long.MinValue || Environment.TickCount64 - Volatile.Read(ref _spillFreeMeasuredAt) > SpillFreeMaxAgeMs)
            StartSpillFreeMeasure();
        return free == long.MinValue ? -1 : free;
    }

    /// <summary>How old a measurement of the spill volume's free space may be before the next is asked for.</summary>
    private const long SpillFreeMaxAgeMs = 1_000;

    /// <summary>The spill volume's free bytes as last measured (-1: it cannot say), <see cref="long.MinValue"/> before the first.</summary>
    private long _spillFreeMeasured = long.MinValue;

    /// <summary><see cref="Environment.TickCount64"/> of the last measurement.</summary>
    private long _spillFreeMeasuredAt;

    /// <summary>1 while a measurement runs.</summary>
    private int _spillFreeMeasuring;

    /// <summary>
    /// Keeps the spill volume's measurement fresh whether or not anything spills (round-6 review, round-5 L-5): the
    /// floor check reads the last measurement, and only a check asked for the next — and checks run only as a spill is
    /// entered or rotated, or the despill weighs a failed swap. So the first check after a quiet period read a value
    /// as old as the period, the constructor's after days without a spill: a spill file opened past the floor, or held
    /// back from a volume freed since. Called by the WAL loop each tick; one measurement at a time, off every lock
    /// (<see cref="StartSpillFreeMeasure"/>). With the WAL loop off (<c>WalFlushInterval</c> 0), only the checks ask, as
    /// before. Never throws.
    /// </summary>
    private void RefreshSpillFreeMeasure()
    {
        if (!_options.HotTier.SpillEnabled) return;
        if (Environment.TickCount64 - Volatile.Read(ref _spillFreeMeasuredAt) > SpillFreeMaxAgeMs) StartSpillFreeMeasure();
    }

    /// <summary>Test hook: the last measurement of the spill volume's free space, read without asking for the next.</summary>
    internal long SpillFreeMeasuredForTest => Volatile.Read(ref _spillFreeMeasured);

    /// <summary>Starts a measurement of the spill volume's free space unless one runs. Never throws, never waits.</summary>
    private void StartSpillFreeMeasure()
    {
        if (Interlocked.Exchange(ref _spillFreeMeasuring, 1) != 0) return;
        try { ThreadPool.UnsafeQueueUserWorkItem(static e => e.MeasureSpillFree(), this, preferLocal: false); }
        catch (Exception) { Volatile.Write(ref _spillFreeMeasuring, 0); }   // asked again by the next look
    }

    /// <summary>One measurement, on a pool thread: whatever it takes, it holds nothing but this thread.</summary>
    private void MeasureSpillFree()
    {
        try
        {
            long free;
            try { free = _volumeMeasureForTest?.Invoke(_walDir) ?? VolumeAvailableBytes(_walDir); }
            catch (Exception) { free = -1; }
            Volatile.Write(ref _spillFreeMeasuredAt, Environment.TickCount64);
            Volatile.Write(ref _spillFreeMeasured, free);
        }
        finally { Volatile.Write(ref _spillFreeMeasuring, 0); }
    }

    /// <summary>Test seam: the measurement itself, of the WAL directory, in place of <see cref="VolumeAvailableBytes"/>.</summary>
    internal Func<string, long>? _volumeMeasureForTest;

    /// <summary>
    /// Whether a spill could take the writers, were the despill to take the free slot now
    /// (<see cref="SwapFailing"/>): spilling is on, no spill open has failed since one last opened,
    /// and the volume has been MEASURED to keep its floor with another spill file. It was
    /// <see cref="HasSpillRoom"/> alone, which says yes for a volume that cannot say and knows nothing
    /// of a failed open — so a swap that failed for a reason that failed the spill open too handed the
    /// slot to the despill at its first failure, for a whole block, while the writers could neither
    /// swap nor spill (503). A failed open is forgotten only when one succeeds, not when its hold
    /// runs out: until then the despill waits for <see cref="SwapFailuresBeforeDespillTakesTheSlot"/>
    /// failures, about 0.7 s, which is the cheap side to be wrong on. <see cref="_spillOpenRetryAt"/>
    /// is read without the lock it is written under: a stale read only moves the moment the despill
    /// takes the slot.
    /// </summary>
    private bool SpillCanTakeTheWriters =>
        _options.HotTier.SpillEnabled
        && Volatile.Read(ref _spillOpenRetryAt) == 0
        && SpillVolumeFreeBytes() >= SpillRoomBytes;

    /// <summary>
    /// The bytes this process may still write on the volume that holds <paramref name="directory"/>,
    /// or -1 when the volume cannot say. On Windows the directory itself is asked
    /// (GetDiskFreeSpaceEx): DriveInfo, which the floor check used, takes a drive's root alone — it
    /// threw on a UNC path, so the floor never held there, and measured the root volume for a
    /// directory behind a mount folder or a junction. Elsewhere DriveInfo is given the directory,
    /// and statvfs measures the file system that holds it.
    /// </summary>
    internal static long VolumeAvailableBytes(string directory)
    {
        try
        {
            if (!OperatingSystem.IsWindows()) return new DriveInfo(Path.GetFullPath(directory)).AvailableFreeSpace;

            // The extended-length form, which a path past MAX_PATH needs; a UNC name must end in a separator.
            string path = DurableFile.ExtendedPath(directory);
            if (!Path.EndsInDirectorySeparator(path)) path += '\\';
            return Native.GetDiskFreeSpaceEx(path, out ulong available, out _, out _)
                ? (long)Math.Min(available, (ulong)long.MaxValue)
                : -1;
        }
        catch (Exception) { return -1; }
    }

    private static partial class Native
    {
        [LibraryImport("kernel32.dll", EntryPoint = "GetDiskFreeSpaceExW", StringMarshalling = StringMarshalling.Utf16, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static partial bool GetDiskFreeSpaceEx(string directoryName, out ulong freeBytesAvailableToCaller,
                                                        out ulong totalNumberOfBytes, out ulong totalNumberOfFreeBytes);
    }

    /// <summary>
    /// What one spill file is — its log and its pool file together — and, since it never grows (see
    /// <see cref="WriteAheadLog.Open"/>), the most it can come to: a tier's payload plus a header and
    /// trace context an event, and at least 1 MB. A block whose entries weigh more (large exceptions,
    /// long inline templates, many long pool rows) is closed when its file is full, and the next one
    /// is checked against the floor before it opens.
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
            NoteKeptFlushFailures(s.Wal);   // its last fsyncs: it is not ticked again (round-6 review)
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

    /// <summary>
    /// Queues a block again whose despill found no room for its tier's own text beside the frozen tiers' (round-6
    /// review R5-M11): not a failure — no delay, nothing counted — but its text is kept, and the loop leaves the block
    /// until the frozen tiers have room for that much (a slot given back with its text wakes the loop). Never throws.
    /// </summary>
    private void WaitForFrozenText(ulong segId, long textBytes)
    {
        lock (_spillQueueLock)
        {
            if (!_spillQueue.TryGetValue(segId, out var q)) return;
            q.InFlight  = false;
            q.TextBytes = textBytes;
        }
    }

    /// <summary>The wait after the <paramref name="failures"/>-th failure in a row: the first delay, doubled each time, capped.</summary>
    private TimeSpan DespillRetryDelayAfter(int failures)
    {
        double ms = _despillRetryDelay.TotalMilliseconds * Math.Pow(2, Math.Clamp(failures - 1, 0, 30));
        return TimeSpan.FromMilliseconds(Math.Min(ms, Math.Max(_despillRetryCap.TotalMilliseconds, _despillRetryDelay.TotalMilliseconds)));
    }

    /// <summary>
    /// Wakes the despill loop. Never throws: it runs in finally blocks that release a slot or the
    /// swap lock, and <see cref="SemaphoreSlim.Release()"/> allocates when a timed waiter is parked.
    /// A wake that is lost is the loop's safety-net timeout later.
    /// </summary>
    private void WakeDespill()
    {
        try
        {
            _wakeDespillForTest?.Invoke();
            if (_despillSignal.CurrentCount != 0) return;
            _despillSignal.Release();
        }
        catch (SemaphoreFullException) { /* already woken */ }
        catch (Exception) { /* out of memory: the loop wakes at its timeout instead */ }
    }

    /// <summary>Test seam: runs as the despill loop is woken; throwing from it is the wake running out of memory.</summary>
    internal Action? _wakeDespillForTest;

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
        // Never throws: it ends the finally blocks that give a slot back. A swap that could not be
        // scheduled (out of memory) is asked for again by the next write, release or age pass.
        if (w.Spill is not null || Volatile.Read(ref w.HotRefused))
        {
            try { ScheduleFlush(); }
            catch (Exception) { /* asked again by the next request */ }
        }
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
            if (!IsUsableBlockId(segId))
            {
                // Its levels' ids would wrap past zero onto other blocks (see ReserveWalBlock).
                _logger.LogError("Spill file {File} names block {Block}, past the last block id this store can use: left where it is",
                    file, segId);
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
        var ids = new HashSet<ulong>();
        foreach (var path in Directory.EnumerateFiles(_segDir, $"{_options.NodeId.Value}-*.seg"))
            if (TryParseLocalSegmentId(path, out ulong id)) ids.Add(id);
        return ids;
    }

    /// <summary><c>{node}-{id}-{min}-{max}.seg</c> of THIS node → the id.</summary>
    private bool TryParseLocalSegmentId(string path, out ulong id)
    {
        id = 0;
        string prefix = $"{_options.NodeId.Value}-";
        var    name   = Path.GetFileName(path.AsSpan());
        if (!name.StartsWith(prefix, StringComparison.Ordinal)) return false;
        var rest = name[prefix.Length..];
        int dash = rest.IndexOf('-');
        return dash > 0 && ulong.TryParse(rest[..dash], System.Globalization.NumberStyles.None,
                                          System.Globalization.CultureInfo.InvariantCulture, out id);
    }

    /// <summary>
    /// This node's segment ids whose file the boot catalog scan set aside as torn (<c>.seg.corrupt</c>).
    /// Written by the scan, read once it has completed (<see cref="_catalogLoad"/>). Under its own lock.
    /// </summary>
    private readonly HashSet<ulong> _bootQuarantined = [];

    /// <summary>
    /// The levels of a block an earlier run published that the despill keeps — the ones the boot
    /// scan ACCEPTED. The mask is taken before the scan has run (see <see cref="DiscoverSpillFiles"/>),
    /// and the scan may find one of those files torn and set it aside: kept in the mask, that level
    /// was skipped, and its events were left in a <c>.seg.corrupt</c> file, served by nobody. Called
    /// once the scan has completed.
    /// </summary>
    private byte LevelsTheScanKept(ulong segId, byte published)
    {
        lock (_bootQuarantined)
        {
            for (int lvl = 0; lvl < LevelSegmentSlots; lvl++)
                if ((published & (1 << lvl)) != 0 && _bootQuarantined.Contains(segId + (ulong)lvl))
                    published &= (byte)~(1 << lvl);
        }
        return published;
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

    // ── Despill ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Despills queued blocks, oldest first, one at a time, each in a free flush slot — and none while the live tier
    /// needs the slot: while the engine is spilling, or while the live tier is refusing writes. A
    /// slot that comes free then goes to the swap, so new events are searchable again first and the
    /// backlog drains behind them — unless that swap has just failed (OnSwapFailed), when the slot
    /// would otherwise sit idle.
    ///
    /// <para>Built with the owner builder (round 7): a registration on a task that runs out of memory is made again.
    /// One that still fails reaches a catch below and the loop goes on with the compiler's state still that
    /// suspension's, which skips every finally — a lock statement's exit too — until a suspension resumes: so the
    /// queue is walked and changed only in methods of their own (<see cref="NextDespillable"/>,
    /// <see cref="MarkDespillInFlight"/>, <see cref="DespillFailuresOf"/>), whose locks are theirs. The catch after the
    /// await of a despill took <c>_spillQueueLock</c> itself, and on that road kept its monitor for good: no spill,
    /// despill or settle again.</para>
    /// </summary>
    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerTaskMethodBuilder))]
    private async Task RunDespillLoopAsync(Task? heldUntil, CancellationToken ct)
    {
        if (heldUntil is not null)
        {
            try { await heldUntil.WaitAsync(ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }

        // The server's engine waits for the indexing layer's sink before it writes a block: until
        // then a despilled block's segments got no index sections (see _indexSinkWait).
        if (_indexSinkWait > TimeSpan.Zero && IndexSinkFactory is null)
        {
            try { await _indexSinkWired.Task.WaitAsync(_indexSinkWait, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                // Timed out — or the wait could not even be set up (out of memory): either way the loop
                // goes on without the sink rather than end, and says so (guarded: the line allocates).
                try
                {
                    _logger.LogWarning(ex is TimeoutException ? null : ex,
                        "The index sink was not wired within {Wait}s of startup — spilled blocks are written into segments " +
                        "without index sections until it is (their searches scan; a merge rebuilds the index)",
                        _indexSinkWait.TotalSeconds);
                }
                catch (Exception) { /* the line is lost; the loop is not */ }
            }
        }

        while (!ct.IsCancellationRequested)
        {
            try
            {
                _despillLoopIterationForTest?.Invoke();
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

                    SpillFile next = NextDespillable(_catalogLoad.IsCompleted);
                    if (next.Path is null) break;

                    if (!TryTakeFlushSlot()) break;   // released by the despill, or by its retry
                    MarkDespillInFlight(next.SegId);

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
                        int failures = DespillFailuresOf(next.SegId);
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
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                // The loop itself failed, outside any one block's despill: out of memory setting up its
                // wait or walking its queue, a log line. It used to end there: no block was despilled
                // again in this process, and shutdown's await of the loop threw before its final flush.
                // A pause, then round again; with nothing to allocate a timer with, a blocked thread.
                try { _logger.LogError(ex, "The despill loop failed outside any one block's despill; it goes on in a second"); }
                catch (Exception) { /* the line is lost; the loop is not */ }
                try
                {
                    _despillLoopPauseForTest?.Invoke();
                    await Task.Delay(DespillLoopFaultPause, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
                catch (Exception) { Thread.Sleep(DespillLoopFaultPause); }
            }
        }
    }

    /// <summary>
    /// The oldest queued block a despill may take now: not in flight, not waiting out a retry delay, not one an earlier
    /// run half-wrote while the boot scan has not completed (<paramref name="scanned"/>), and not one whose own text finds
    /// no room beside the frozen tiers'. None (no path) when there is no such block.
    /// </summary>
    private SpillFile NextDespillable(bool scanned)
    {
        lock (_spillQueueLock)
        {
            long now = Environment.TickCount64;
            foreach (var (segId, q) in _spillQueue)
            {
                if (q.InFlight || q.NotBefore > now) continue;
                // A block an earlier run half-wrote waits for the boot scan: the levels it keeps
                // are the ones the scan accepted (LevelsTheScanKept). The scan's end wakes the loop.
                if (q.PublishedLevels != 0 && !scanned) continue;
                // One whose tier's own text found no room beside the frozen tiers' waits for room
                // (R5-M11): a slot given back with its text wakes the loop. The others go on meanwhile.
                if (q.TextBytes > 0 && !FrozenTextHasRoomFor(q.TextBytes)) continue;
                return new SpillFile(segId, q.Path, q.PublishedLevels);
            }
        }
        return default;
    }

    /// <summary>Marks a queued block as taken by a despill (see <see cref="RunDespillLoopAsync"/>).</summary>
    private void MarkDespillInFlight(ulong segId)
    {
        lock (_spillQueueLock)
            if (_spillQueue.TryGetValue(segId, out var taken)) taken.InFlight = true;
    }

    /// <summary>The failures in a row of a queued block's despills; 0 when it is not queued.</summary>
    private int DespillFailuresOf(ulong segId)
    {
        lock (_spillQueueLock) return _spillQueue.TryGetValue(segId, out var q) ? q.Failures : 0;
    }

    /// <summary>The pause after the despill loop itself failed (see <see cref="RunDespillLoopAsync"/>).</summary>
    private static readonly TimeSpan DespillLoopFaultPause = TimeSpan.FromSeconds(1);

    /// <summary>Test seam: runs at the top of each round of the despill loop; throwing from it is the loop itself failing. Null in production.</summary>
    internal Action? _despillLoopIterationForTest;

    /// <summary>Test seam: runs as the loop sets up its pause after failing; throwing from it is a pause that cannot be set up. Null in production.</summary>
    internal Action? _despillLoopPauseForTest;

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

    /// <summary>Test hook: the spill queue's lock, for a test to see that nothing kept it.</summary>
    internal object SpillQueueLockForTest => _spillQueueLock;

    /// <summary>Test hook: the despills that have completed, including those that found nothing to write.</summary>
    internal int DespillsCompletedForTest => Volatile.Read(ref _despillsCompleted);

    /// <summary>Test hook: the tiers listed as frozen — swapped or despilled, not yet published.</summary>
    internal int FrozenTierCountForTest { get { lock (_frozenLock) return _frozenHot.Count; } }

    /// <summary>Test hook: the events in the live tier.</summary>
    internal int LiveTierCountForTest => _write.Hot.Count;

    /// <summary>
    /// Gives up a despilled tier whose flush — or whose publish — failed and whose retry could not be
    /// started, or ended without finishing: unlisted, retired (freed once no reader holds it), and the
    /// levels its attempt moved into place unlinked (those an earlier run published kept), so the
    /// block's next despill, from its file, starts clean. True when it did: the caller then queues the
    /// block again. Never throws; the caller gives the slot back and ends the heavy phase either way.
    ///
    /// <para>The block's completion marker first (round-5 H-A). A flush that completed wrote it before
    /// the publish that failed, and the levels used to be unlinked under it: the block's next despill
    /// took the marker for a block written by an earlier life and deleted the spill file unread — every
    /// event of the block lost. A marker that cannot be removed for good is kept, and with it all it
    /// vouches for: the levels, whole on disk; the tier, listed — served — until shutdown frees it, as a
    /// swapped tier with no retry is, under the same cap (<see cref="KeptTiersCap"/>); and the block, in flight, despilled by nobody in this run. False
    /// then. The next start finds the block's levels (and the marker, if it is still there) and finishes
    /// it as it finishes a block a crash left: the despill keeps the levels the boot scan accepted and
    /// writes the rest, or, under the marker, only removes the spill file.</para>
    ///
    /// <para>Only when this attempt may have LEFT a marker (round-6 review R5-M7): <paramref name="mayHaveMarker"/>
    /// is false for a give-up before the flush, and for a flush that failed — the marker is its last step,
    /// and it never throws (WriteFlushCompletionMarker) — and true for a publish that failed and for a retry
    /// that ended. It parked on any throw of the removal, which allocated (and on Linux fsynced) whether or
    /// not there was a marker: in an out of memory every despilled block parked, a tier each, until the
    /// process was killed. The marker's paths are made before the despill (<paramref name="marker"/>,
    /// <paramref name="voided"/>), and at most <see cref="KeptTiersCap"/> tiers are kept, swapped ones included: past
    /// that a tier is let go — unlisted and retired — and the block waits for the next start all the same, whole on
    /// disk (its spill file, and its levels with the marker over them), its events unserved meanwhile.</para>
    /// </summary>
    private bool AbandonDespilledTier(HotTierSegment tier, SpillFile file, bool mayHaveMarker, string? marker, string? voided)
    {
        if (mayHaveMarker)
        {
            try { RemoveFlushMarkerDurably(file.SegId, marker, voided); }
            catch (Exception ex)
            {
                bool kept = TryKeepTier();
                if (!kept) LetGoOfTier(tier);
                try
                {
                    _logger.LogError(ex, kept
                        ? "Despill of {File}: its completion marker could not be removed after the despill failed — its tier stays in " +
                          "memory, served, until shutdown, and the block is finished by the next start"
                        : "Despill of {File}: its completion marker could not be removed after the despill failed — with the most tiers " +
                          "kept so already, its tier is let go: the block's events are on disk and come back at the next start",
                        file.Path);
                }
                catch (Exception) { /* the line is lost; the tier and the block are kept all the same */ }
                return false;
            }
        }
        LetGoOfTier(tier);
        try { DeleteUnpublishedLevelFiles(file.SegId, file.PublishedLevels); }
        catch (Exception) { /* the next despill's flush meets a level still there, fails, and its retry unlinks it */ }
        return true;
    }

    /// <summary>Unlists a despilled tier and retires it (freed once no reader holds it). Never throws.</summary>
    private void LetGoOfTier(HotTierSegment tier)
    {
        try
        {
            lock (_frozenLock)
                for (int i = _frozenHot.Count - 1; i >= 0; i--)
                    if (ReferenceEquals(_frozenHot[i].Tier, tier)) _frozenHot.RemoveAt(i);
        }
        catch (Exception) { /* not listed */ }
        try { RetireHotTier(tier); }
        catch (Exception) { /* its native memory is lost */ }
    }

    /// <summary>
    /// The tiers kept listed — served — with no flush to come, until shutdown frees them: a despilled tier whose
    /// marker could not be removed (<see cref="AbandonDespilledTier"/>, round-6 review R5-M7), and a swapped tier no
    /// retry could flush (<see cref="HandFailedFlushToRetry"/>, or a retry that ended unfinished). One cap for both,
    /// <see cref="KeptTiersCap"/> (round-6 review R6-M7): a swapped tier was kept with no cap at all — 12 of them held
    /// 117 MB of native memory and 172 MB of heap, with their slots, counts and text given back, against a budget
    /// that counted none of it — and the despill's cap, a flush slot's worth, sat on top of the slots in flight, up to
    /// twice the native budget.
    /// </summary>
    private int _keptTiers;

    /// <summary>
    /// The most tiers kept with no flush to come (see <see cref="_keptTiers"/>): one, beyond the flush slots' tiers —
    /// what the budget derivation holds one extra tier to. Past it a tier is let go, unlisted and retired: its events
    /// are unserved until the next start reads them back from disk (a swap's WAL, a despill's spill file and levels),
    /// where they stay either way.
    /// </summary>
    internal const int KeptTiersCap = 1;

    /// <summary>Counts one more kept tier when the cap allows it; false — nothing counted — when it does not. Never throws.</summary>
    private bool TryKeepTier()
    {
        if (Interlocked.Increment(ref _keptTiers) <= KeptTiersCap) return true;
        Interlocked.Decrement(ref _keptTiers);
        return false;
    }

    /// <summary>Test hook: the tiers kept with no flush to come (see <see cref="_keptTiers"/>).</summary>
    internal int KeptTiersForTest => Volatile.Read(ref _keptTiers);

    private int _despillsCompleted;

    /// <summary>
    /// Test hook: called with the block's id as a despill that holds its flush slot is about to read
    /// the spill file. Blocking it holds the slot; throwing from it is a read that failed.
    /// </summary>
    internal Action<ulong>? _beforeLoadSpillForTest;

    /// <summary>
    /// Test hook: called with the block's id once the despill has read the spill file into a tier,
    /// before anything else is done with it — on the thread that read it, so it and
    /// <see cref="_beforeLoadSpillForTest"/> bracket the read.
    /// </summary>
    internal Action<ulong>? _afterLoadSpillForTest;

    /// <summary>
    /// Replays one spilled block into a tier and flushes it into the block, exactly as a frozen tier
    /// is flushed: listed as frozen (so queries see it) for its heavy phase, published with the same
    /// completion marker, its WAL deleted against it. Owns the flush slot the caller took.
    /// </summary>
    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerTaskMethodBuilder))]
    private async Task DespillAsync(SpillFile file, CancellationToken ct)
    {
        // Boxed first (see TryFlushAsync). Its first real suspension was the wait for the swap lock or
        // the heavy-phase permit, or the await of the flush: a box that could not be allocated there
        // skipped every finally below — the slot, the count and the tier held for good, a lock or a
        // permit granted to a waiter that was gone — or was caught as a failed FLUSH while the flush
        // ran on, and the tier freed under it. Here the slot (the loop took it for this despill) is
        // all it owns, and this catch, which the compiler does not guard as it guards a finally,
        // gives it back and queues the block again.
        try { await Task.Yield(); }
        catch (Exception)
        {
            RetrySpillLater(file.SegId);
            GiveBackSlot(endHeavyPhase: false);
            Interlocked.Increment(ref _despillsCompleted);
            throw;
        }

        bool slotTransferred = false;
        bool counted         = false;
        bool settled         = false;   // published, found published already, or not this release's to read
        bool parked          = false;   // given up with its marker kept: in flight until the next start (AbandonDespilledTier)
        long noRoomForText   = 0;       // its tier's own text, when the frozen tiers had no room for it: queued to wait
        HotTierSegment? unlisted = null; // read but not yet listed as frozen: freed here, whatever way we leave
        HotTierSegment? charged  = null; // listed and counted against the frozen tiers' text: given back with the slot
        bool ended           = false;   // EndDespill has run: by a catch at a pause (round 7), or by the finally
        try
        {
            // The marker's paths, made here, before anything is owned: a give-up removes the marker in the
            // out of memory that stopped its retry, and made them there (round-6 review R5-M7).
            string marker = FlushMarkerPath(file.SegId), voided = VoidedMarkerPath(file.SegId);

            // Flushed in full by an earlier life, which stopped before the unlink. Asked as a look (round-6
            // review R5-M6): File.Exists answered false for a marker it could not look at as well, and the
            // block was despilled — every level written a second time. One that cannot be looked at
            // answers neither way: the block is tried again later, read by nothing meanwhile.
            switch (Look(marker))
            {
                case Presence.There:
                    DeleteSpillFiles(file);
                    settled = true;
                    return;
                case Presence.Unknown:
                    throw new IOException(
                        $"Whether block {file.SegId} was flushed cannot be told: its completion marker could not be looked at");
            }

            // Of the levels an earlier run published, the ones the boot scan accepted (the loop takes
            // such a block only once the scan has completed).
            if (file.PublishedLevels != 0) file = file with { PublishedLevels = LevelsTheScanKept(file.SegId, file.PublishedLevels) };

            _beforeLoadSpillForTest?.Invoke(file.SegId);
            var tier = LoadSpill(file, out long ownText);
            if (tier is null)
            {
                settled = true;
                return;
            }
            unlisted = tier;
            _afterLoadSpillForTest?.Invoke(file.SegId);

            // Counted and listed under the swap lock, as a swap counts its tier: shutdown takes this
            // lock after closing the write path, and from then on the count may only fall. A
            // shutdown that cancels this wait used to leave the tier — up to a tier's native memory —
            // unfreed: it is still `unlisted`, and the finally frees it.
            // Never queued in the lock or the permit below (OwnerPoll; round-5 M-B).
            ct.ThrowIfCancellationRequested();
            try
            {
                for (int pause = 1; !_flushLock.Wait(0); )
                    await OwnerPoll(_flushLock, ref pause, 0, ct).ConfigureAwait(false);
            }
            catch
            {
                // Ended HERE (ultra review #3, round 7): a pause whose continuation could not be registered (out of
                // memory) throws into this method with the finally below skipped, and the tier just read was never
                // freed, the slot never given back and the block never queued again: in flight, read by nobody, for
                // the life of the process. A catch runs on every road.
                ended = true;
                EndDespill(file, unlisted, slotTransferred, settled, noRoomForText, parked, counted, charged);
                throw;
            }
            try
            {
                if (Volatile.Read(ref _writesClosed) != 0) return;   // never listed; the file is the next start's

                // Its events' own text, against the frozen tiers' as a swapped tier's is, under the same lock (round-6
                // review R5-M11). A despilled tier was never counted, and while flushes failed each one went to a
                // retry that kept it, and the loop took the next block: 111.9 MB of text against a 16 MB budget in the
                // stand's shape. With no room it is not listed: the block waits — queued again, not failed — until a
                // slot given back with its text makes room; never past a backlog that holds none, so one always can.
                if (!FrozenTextHasRoomFor(ownText))
                {
                    noRoomForText = ownText;
                    return;
                }
                lock (_frozenLock)
                {
                    _frozenHot.EnsureCapacity(_frozenHot.Count + 1);   // what can fail, before the count
                    Interlocked.Increment(ref _heavyPhases);
                    counted = true;
                    _frozenHot.Add((tier, file.SegId));
                }
                unlisted = null;
                ChargeFrozenText(tier, ownText);   // allocates nothing: given back with the slot, whoever gives it
                charged = tier;
            }
            finally { ReleaseFlushLock(); }

            try
            {
                ct.ThrowIfCancellationRequested();
                for (int pause = 1; !_flushConcurrency.Wait(0); )
                    await OwnerPoll(_flushConcurrency, ref pause, 0, ct).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Listed and counted, and nothing of it written: given up as a despill whose retry could
                // not start (AbandonDespilledTier), its block queued again by the finally. Left listed —
                // as a wait that threw here left it — it was served for good beside the block's next
                // despill. No flush ran: no marker of this attempt to remove (R5-M7).
                parked = !AbandonDespilledTier(tier, file, mayHaveMarker: false, marker, voided);
                // And ended here, not by the finally (ultra review #3, round 7): a pause whose continuation could not
                // be registered skips it, and the slot, the heavy-phase count and the text charge were never given
                // back — shutdown waited its whole budget for the count, and the frozen tiers stayed closed to that
                // much text for the life of the process — nor the block queued again.
                ended = true;
                EndDespill(file, unlisted, slotTransferred, settled, noRoomForText, parked, counted, charged);
                throw;
            }
            List<SegmentInfo> written;
            try
            {
                // A block a crash left half-written keeps the levels it got out — the ones found on
                // disk when this run STARTED, not now: the boot scan registers them as ordinary
                // segments, and a merge or retention pass may have taken one since. Asking the disk
                // here found it gone and wrote its events a second time.
                //
                // In this frame, not awaited (see FlushTierByLevel). The permit goes back by plain code and in the
                // catches, not in a finally (R6-M6; see TryFlushAsync's heavy phase).
                written = FlushTierByLevel(tier, file.SegId, ct, publishedLevels: file.PublishedLevels);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A flush that failed wrote no marker: the marker is its last step, and never throws (R5-M7).
                slotTransferred = HandFailedDespillToRetry(ex, tier, file, mayHaveMarker: false, marker, voided, out parked);
                ReleaseHeavyPermit();
                return;
            }
            catch
            {
                ReleaseHeavyPermit();
                throw;
            }
            ReleaseHeavyPermit();

            // Published all or nothing (PublishFlushedTier): one that failed registered nothing, and the
            // tier goes to a retry as a failed flush's does. Left to the finally, the block was queued
            // again while its tier stayed listed for good, served beside the block's second despill.
            try { PublishFlushedTier(written, tier, file.Path, file.SegId); }
            catch (Exception ex)
            {
                // The flush completed: its marker is there (R5-M7).
                slotTransferred = HandFailedDespillToRetry(ex, tier, file, mayHaveMarker: true, marker, voided, out parked);
                return;
            }
            DespillPublished(file.SegId, written, afterRetry: false);
            settled = true;
        }
        finally
        {
            if (!ended) EndDespill(file, unlisted, slotTransferred, settled, noRoomForText, parked, counted, charged);
        }
    }

    /// <summary>
    /// A despill's end, however it ends: frees a tier it read and never listed; unless a retry took the slot over,
    /// settles the block, leaves it to wait for text room or queues it again, and gives the slot back — with the
    /// heavy-phase count and the text charge when it had them; then counts the despill. Never throws. Run once: by
    /// <see cref="DespillAsync"/>'s finally, or by its catch at a pause whose continuation may not have been
    /// registered (round 7) — such a failure skips the finally, and runs the catch.
    /// </summary>
    private void EndDespill(SpillFile file, HotTierSegment? unlisted, bool slotTransferred, bool settled, long noRoomForText,
                            bool parked, bool counted, HotTierSegment? charged)
    {
        if (unlisted is not null)
        {
            try { unlisted.Dispose(); }
            catch (Exception) { /* its native memory is lost; the slot still goes back */ }
        }
        if (!slotTransferred)
        {
            // Before the slot goes back, so the loop that takes it sees this block as gone, or
            // as not ready yet. A parked block stays in flight: its tier is kept, listed, and a
            // despill of its file would serve its events twice.
            if (settled)                SettleSpill(file.SegId);
            else if (noRoomForText > 0) WaitForFrozenText(file.SegId, noRoomForText);
            else if (!parked)           RetrySpillLater(file.SegId);
            // With its text (R5-M11), the frozen tiers' room comes back as the slot does. A parked tier's goes
            // back too: it is held until shutdown, and kept counted it would close the frozen tiers to every
            // tier of text until then — its text is outside the budget, at most FlushSlots tiers' (R5-M7).
            GiveBackSlot(endHeavyPhase: counted, charged);
        }
        Interlocked.Increment(ref _despillsCompleted);
    }

    /// <summary>
    /// A despilled tier whose flush — or whose publishing — failed goes to a background retry. True
    /// when the retry took the slot and the count over; false when none could be started, and the
    /// tier has been given up here instead (the caller's finally queues the block again) — or, when
    /// <paramref name="parked"/>, kept with its block in flight (see <see cref="AbandonDespilledTier"/>).
    /// Never throws: the tier is listed and counted, and a throw out of here — its own log line, which
    /// allocates — reached the finally, which gave the slot back, ended the heavy phase and queued the
    /// block again, leaving the tier listed for good, served beside the block's second despill.
    /// </summary>
    /// <param name="mayHaveMarker">Whether the failed attempt may have left the block's completion marker: its publish failed (see <see cref="AbandonDespilledTier"/>).</param>
    /// <param name="marker">The block's marker path, made before the despill (<see cref="FlushMarkerPath"/>).</param>
    /// <param name="voided">Where its removal moves it on Windows, made before the despill (<see cref="VoidedMarkerPath"/>).</param>
    private bool HandFailedDespillToRetry(Exception ex, HotTierSegment tier, SpillFile file, bool mayHaveMarker,
                                          string? marker, string? voided, out bool parked)
    {
        parked = false;
        try
        {
            _logger.LogError(ex,
                "Despill of {File} failed — its tier stays frozen and queryable; retrying in background every {Delay}s",
                file.Path, FlushRetryDelay.TotalSeconds);
        }
        catch (Exception) { /* the line is lost; the hand-over below is not */ }
        // A retry that started owns the slot and the count from here; the block stays queued, in
        // flight, until it publishes the block — or shutdown leaves it for the next start. Exactly one
        // side gives the tier up: the retry once it has started (and TryScheduleFlushRetry throws
        // nothing after that), this method when it has not.
        bool started = false;
        try
        {
            var (onPublished, onAbandoned) = DespillRetryCallbacks(tier, file, marker, voided);
            started = TryScheduleFlushRetry(tier, file.Path, file.SegId, onPublished, file.PublishedLevels, onAbandoned);
        }
        catch (Exception) { /* its callbacks could not be allocated: nothing started */ }
        if (!started)
            // No retry could be started (out of memory): the block is read from its file again later
            // instead, so the tier is given up here, unlisted and retired, with the levels the attempt
            // moved into place, as the retry would — unless its marker stays, and then so does it.
            parked = !AbandonDespilledTier(tier, file, mayHaveMarker, marker, voided);
        return started;
    }

    /// <summary>
    /// The callbacks a despill hands its flush retry: on publishing, count the block and take it out
    /// of the queue; on ending without finishing (see <c>FlushRetryOwners</c>), give the tier up and
    /// queue the block again, as the despill does when no retry starts. Built here so their closure
    /// is allocated inside the try that hands over: written as lambdas in DespillAsync, their capture
    /// of the block was allocated on its entry, before any try, and an out-of-memory there left the
    /// slot the loop took for it held for good.
    /// </summary>
    private (Action<List<SegmentInfo>> OnPublished, Action OnAbandoned) DespillRetryCallbacks(
        HotTierSegment tier, SpillFile file, string? marker, string? voided)
    {
        _despillRetryCallbacksForTest?.Invoke();
        return (retried => DespillPublished(file.SegId, retried, afterRetry: true),
                () =>
                {
                    // A tier kept with its marker keeps its block in flight (AbandonDespilledTier). A retry
                    // that ended may have left one: its last attempt's flush, and a publish that failed.
                    if (AbandonDespilledTier(tier, file, mayHaveMarker: true, marker, voided)) RetrySpillLater(file.SegId);
                });
    }

    /// <summary>Test seam: runs where a despill's retry callbacks are allocated; throwing from it is that allocation failing.</summary>
    internal Action? _despillRetryCallbacksForTest;

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
        // Never throws: the block is published (its caller marks it settled next).
        try
        {
            string retried = afterRetry ? ", after a retry" : "";
            if (failures > 0) retried += $", after {failures} failed attempt(s)";
            _logger.LogInformation("Despilled block {Block}: {Events} event(s) into {Segments} segment(s){Retried}",
                segId, events, written.Count, retried);
        }
        catch (Exception) { /* the line is lost */ }
    }

    /// <summary>
    /// Reads a spilled block back into a tier, or returns null — after deleting it — when it holds
    /// nothing to write, or leaving it where it is when a later release wrote it.
    ///
    /// <para>Templates and services are resolved from the block's OWN pool file and interned into
    /// the live pool by their text, never force-interned by index: the live pool is in use, and a
    /// block from an earlier run numbered its strings in that run's pool. Ids are assigned here,
    /// from each event's own timestamp, as the live path does.</para>
    ///
    /// <para><paramref name="ownText"/> is the text the tier's events hold of their own, as a live tier counts it
    /// (<see cref="HotTierSegment.ManagedBytes"/>; round-6 review R5-M11): the tier itself counts none — the block's
    /// admission bounded it, and a recovery tier must refuse nothing — so the despill counts it against the frozen
    /// tiers' text from this.</para>
    /// </summary>
    private HotTierSegment? LoadSpill(SpillFile file, out long ownText)
    {
        ownText = 0;
        // Read where it lies, one entry at a time, straight into the tier (see ReplayOrphanedWal):
        // the despill used to hold the whole block twice, a copy in the heap beside the tier.
        using var reader = WriteAheadLog.RecoveryReader.Open(file.Path);
        if (reader.Version > WriteAheadLog.FormatVersion)
        {
            _logger.LogError(
                "Spill file {File} is format v{Version}, newer than this release reads (v{Current}): left in place, " +
                "not despilled. Its events come back when the release that wrote it starts again.",
                file.Path, reader.Version, WriteAheadLog.FormatVersion);
            return null;
        }
        if (reader.SegmentId == 0 || !reader.TryReadNext(out var e))
        {
            reader.Dispose();   // unmapped before the delete: on Windows a mapped file cannot be deleted
            DeleteSpillFiles(file);
            return null;
        }

        var rows    = WriteAheadLog.LoadPool(file.Path + ".pool", reader.Version);   // as the replay (R5-M5)
        var tier    = CreateRecoveryTier(reader.DataBytes);
        int count   = 0;
        int refused = 0;
        var undecoded = new UndecodedExceptions();
        try
        {
            do
            {
                count++;
                undecoded.Note(in e);
                // A template with no pool row — outside a saturated pool, or longer than a row holds
                // — is in the entry itself (v6); the spill file is the event's only copy, and such a
                // template used to come back empty, or cut at 65 535 bytes.
                string? tmpl = e.Template
                               ?? (!e.Unpooled && rows.TryGetValue(e.TemplateIndex, out var t) && t.Length != 0 ? t : null);
                int tmplIdx  = tmpl is null ? -1 : TemplatePool.Intern(tmpl, out tmpl);
                // The service by its row, or by the text the entry carries (logged outside a full
                // pool): interned into the live pool by text, and kept beside the event when that
                // pool is full too — the name used to be dropped there, the event despilled with none.
                int     svcAt   = e.ServiceIndexIn(rows);
                string? svcText = svcAt >= 0 ? rows[(ushort)svcAt] : e.Service;
                int     svcIdx  = -1;
                string? svcKept = null;
                if (!string.IsNullOrEmpty(svcText))
                {
                    svcIdx = TemplatePool.Intern(svcText, out string canonicalSvc);
                    if (svcIdx < 0) svcKept = canonicalSvc;
                }

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
                if (tier.TryWrite(header, e.Payload, tmpl, e.Exception, svcKept))
                {
                    ownText += OwnManagedBytes(in header, tmpl, e.Exception, svcKept);
                    NotifyEventWritten(in header, tmpl ?? string.Empty);
                }
                else
                    refused++;
            }
            while (reader.TryReadNext(out e));
        }
        catch
        {
            tier.Dispose();
            throw;
        }

        // The tier is handed to the caller below, and nothing between may throw past it: a line that
        // cannot be written (a logger out of memory) is lost, the tier is not.
        try
        {
            undecoded.Report(_logger, file.Path);
            if (refused > 0)
                // Cannot happen while the tier is sized from the block (CreateRecoveryTier) and the block
                // admitted only what a tier takes (HotTierAdmission) — and it would be a loss if it did,
                // so it is said loudly rather than counted quietly.
                _logger.LogError("Despill of {File}: {Refused} of {Count} event(s) did not fit the tier and were not written",
                    file.Path, refused, count);
        }
        catch (Exception) { /* the lines are lost */ }
        return tier;
    }

    /// <summary>
    /// The events of one log a walk read without their exception, whose decode failed on bytes that
    /// verified (<see cref="WalRecord.ExceptionDecodeError"/>): counted as the walk goes, and named in
    /// one Error at its end — the startup replay's and the despill's. Such an entry used to fail the
    /// walk at every reading, its whole log replayed at no start and despilled by no retry.
    /// </summary>
    private struct UndecodedExceptions
    {
        private int                _count;
        private long               _firstTicks;
        private Ameto.Core.LogLevel _firstLevel;
        private Exception?         _first;

        internal void Note(in WalRecord record)
        {
            if (record.ExceptionDecodeError is not { } fault) return;
            if (_count++ == 0)
            {
                _first      = fault;
                _firstTicks = record.TimestampTicks;
                _firstLevel = record.Level;
            }
        }

        internal readonly void Report(ILogger logger, string file)
        {
            if (_count == 0) return;
            logger.LogError(_first,
                "Log {File}: {Count} event(s) are written to segments without their exception, which could not be " +
                "decoded — the first stamped {Time:o} ({Level}). Their bytes verified, so the log is not torn: the rest " +
                "of each event is kept, and the log is written and removed as usual",
                file, _count, new DateTime(_firstTicks, DateTimeKind.Utc), _firstLevel);
        }
    }

    /// <summary>The spill file, its pool and — once the file is gone, not before (see <see cref="DeleteLogAndMarker"/>) — its marker.</summary>
    private void DeleteSpillFiles(SpillFile file) => DeleteLogAndMarker(file.Path, file.SegId);

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

    /// <summary>
    /// A recovery tier for a log whose data area is <paramref name="logDataBytes"/> long
    /// (<see cref="WriteAheadLog.RecoveryReader.DataBytes"/>), sized before a byte of it is read: the
    /// log's bytes bound its payloads, and its events at one per <see cref="MinLogEntryBytes"/>. The
    /// tier takes its native chunks as events arrive, so the bounds cost nothing the events do not.
    /// </summary>
    private HotTierSegment CreateRecoveryTier(long logDataBytes)
    {
        var tier = CreateRecoveryTier((int)Math.Min(int.MaxValue - 1, logDataBytes / MinLogEntryBytes + 1), logDataBytes);
        RecoveryTierCreatedForTest.Value?.Invoke(tier);
        return tier;
    }

    /// <summary>
    /// Test seam: handed each tier a replay or a despill reads a log into — how a test sees that a
    /// walk that failed part way freed it. An <see cref="AsyncLocal{T}"/>: the replay runs in the
    /// constructor. Null in production.
    /// </summary>
    internal static readonly AsyncLocal<Action<HotTierSegment>?> RecoveryTierCreatedForTest = new();

    /// <summary>The smallest entry any log format holds: a v3 header and nothing else.</summary>
    private const int MinLogEntryBytes = 20;
}
