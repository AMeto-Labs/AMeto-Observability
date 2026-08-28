using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Ameto.Metrics.Storage;

/// <summary>
/// What <see cref="MetricWriteAheadLog.CommitFlush"/> did with a generation, told to the one
/// caller that can still act on the answer: the flush holding the files.
/// </summary>
internal enum MetricWalCommit
{
    /// <summary>
    /// The watermark names this generation or a later one. The log will not replay its points,
    /// so the caller's files are now the only copy of them — publish them and say nothing.
    /// </summary>
    Committed,

    /// <summary>
    /// The watermark does NOT name it, and nothing was reclaimed. Its records are still in the
    /// log, intact and uncompacted, so its points remain durable HERE — which makes the
    /// caller's files a second copy that the next start would replay beside. The caller either
    /// unwrites them, leaving the log the single copy, or accepts a duplicate that no restart
    /// resolves; what it must not do is treat this like a commit, which is what silence meant.
    /// </summary>
    Refused,
}

/// <summary>
/// Write-ahead log for the metric hot tier, backed by a memory-mapped file.
/// Built along the same lines as <c>Ameto.Storage.WriteAheadLog</c> (logs tier), including
/// its companion-file trick for data that repeats across entries.
///
/// <para>Why it exists: the hot tier had no log, so durability came from flushing every
/// 60 seconds regardless of how little had arrived — and a flush writes one <c>.mts</c> file
/// PER METRIC NAME. A deployment with 40 instrument names therefore produced 40 files a
/// minute, ~57 600 a day, which the rollup then had to chew through at four metric names per
/// five-minute pass. Appending here is a struct store into a mapped page, so the flush is
/// free to wait for a batch worth writing.</para>
///
/// <para>Two files. The log itself holds one fixed-size entry per data point; the series a
/// point belongs to — name, kind, unit, labels, histogram bounds — is registered once in a
/// companion <c>.pool</c> and referenced afterwards by index. Without that, every point
/// would carry its full label set, which is the bulk of a metric's bytes.</para>
///
/// <code>
///   metrics.wal
///     [File Header — 32 bytes]
///       0   Magic               uint32  "RDMW"
///       4   Version             uint16  1
///       6   _pad                uint16
///       8   WriteOffset         int64
///      16   Generation          uint64  stamped on new appends
///      24   CommittedGeneration uint64  everything at or below this is already in files
///     [Entry — 48 bytes, Pack = 1][BucketCounts: BucketCount × int64]
///
///   metrics.wal.pool
///     [index uint32][byteLen uint32][kind, name, unit, labels, bounds]  (repeated)
/// </code>
///
/// <para><b>Flush protocol.</b> A flush is two-phase, because points keep arriving while the
/// files are being written and their log records must survive:</para>
/// <list type="number">
/// <item><see cref="BeginFlush"/>, called while the caller holds whatever lock makes the
/// hot-tier snapshot atomic, closes the current generation and opens the next. Everything
/// snapshotted carries generation G; everything appended from now on carries G+1.</item>
/// <item><see cref="CommitFlush"/>, called once the files are durable, records G as
/// committed and then compacts the log — the generations at or below G form a prefix
/// (generation is non-decreasing in append order), so reclaiming them is one move of the
/// surviving tail to the front.</item>
/// </list>
///
/// <para><b>One flush at a time, and why the log insists on it.</b> The watermark is a single
/// u64 and <see cref="CommitFlush"/> frees everything at or below it, so committing G+1 also
/// frees G. That is only sound if G's files were already written. With two flushes open at
/// once it is not: the later one's commit reclaims the earlier one's records while the earlier
/// one is still inside its file write, and a failure there leaves those points in neither a
/// file nor the log — durable nowhere, with nothing logged and nothing thrown. The class used
/// to leave that to its caller; it now enforces it. <see cref="BeginFlush"/> refuses to open a
/// second flush, and <see cref="CommitFlush"/> refuses a generation that is not the open one,
/// so the watermark can never pass a flush that has not finished writing. A flush that gives
/// its generation back to the tier says so with <see cref="AbandonFlush"/>. And it refuses on
/// the same terms when it cannot open a generation AT ALL — a closed or unmapped log — because
/// a generation the caller believes it holds and nobody opened is not a milder failure than two
/// open at once: it is the duplicate of everything that flush goes on to write.</para>
///
/// <para>The same fault reaches the other end of the two phases, and there the flush is already
/// holding files. A log that loses its mapping between <see cref="BeginFlush"/> and <see
/// cref="CommitFlush"/> cannot move the watermark, so its records survive and its points are
/// replayed at the next start beside the very .mts files that already hold them. That is not
/// unavoidable and it is not silent any more: <see cref="CommitFlush"/> answers <see
/// cref="MetricWalCommit"/> rather than nothing, so the caller learns that its files are a
/// second copy while it is still the only party able to delete them — and a duplicate needs
/// two copies. Deleting the files leaves the points durable exactly once, in the log, and the
/// replay that would have duplicated them restores them instead.</para>
///
/// <para>A flush that fails simply never commits: the snapshot's records still sit in the
/// log below an unchanged watermark, so a crash before the retry replays them. This is what
/// makes "durable before queryable" true for a point that arrives mid-flush — an earlier
/// design zeroed the whole log after writing the files and destroyed exactly those
/// records. Its points are restored to the hot tier and NOT re-logged, so the abandoned
/// generation's records are what keeps them durable until the next flush snapshots them and
/// commits a generation above it. That coupling is why the failure path must abandon rather
/// than simply return.</para>
///
/// <para><b>What "the flush failed" has to mean for that to be true.</b> Leaving a generation
/// replayable while its points are also in a file is a duplicate, not a rescue, so only a
/// failure that put NO file on disk may abandon. That is a condition on the caller, and it was
/// not met: the writer wrote one file per metric name straight to its final path and kept
/// whatever it had finished when a later one threw, and the flush treated every step after the
/// write — publishing the files, logging — as a failed write too. Both are closed at the
/// source: <c>MetricWriter.Write</c> builds every file at <c>.mts.tmp</c> and renames it only
/// once it is complete and closed, and deletes the ones it has already renamed before it
/// rethrows — so a throw leaves nothing of its output at any path a reader scans; and
/// <c>MetricStorageEngine.FlushHotTierAsync</c> restores and abandons only around the write
/// itself, committing on every path where a file survives.</para>
///
/// <para><b>Crash recovery.</b> Recovery keeps entries whose generation is ABOVE the
/// committed watermark. The watermark is written before any bytes move, so a crash during
/// compaction cannot resurrect cold points; the relocated tail is terminated with a
/// generation-0 slot before the new write offset is stored, so such a crash cannot return
/// the survivors twice either. The generation is assigned here, under this
/// class's own lock; nothing derived from the data (a point's timestamp, say) would do,
/// because those come from the instrumented client and are not monotonic in append order.
/// Once the caller's side of the abandon contract holds (see above), a crash landing between
/// the moment the first file becomes visible and the commit is the only thing left that can
/// duplicate points, and every point in a file published before the crash is duplicated, not
/// some of them. That window is the writer's publish pass — one rename per file, all of them
/// after the last byte of the last file is on the platter — plus the return and this call. It
/// was much wider while each file was renamed as it finished: the whole write of files 2..N,
/// seconds under a large flush, with file 1 already visible throughout.</para>
///
/// <para>Generation 0 is never written by an append, so it also marks the end of real data —
/// a zero-filled region is otherwise indistinguishable from a valid entry whose point
/// carries a zero timestamp and a zero value, both of which are legal.</para>
///
/// <para>Exemplars are deliberately not logged: they live in a bounded in-memory ring and
/// are not written to cold files either, so replaying them would restore state that a normal
/// flush never persisted.</para>
/// </summary>
internal sealed unsafe class MetricWriteAheadLog : IDisposable
{
    private const uint   MagicNumber     = 0x52_44_4D_57; // "RDMW"
    private const ushort WalVersion      = 1;
    private const int    FileHeaderSize  = 32;
    private const int    EntryHeaderSize = 48;
    private const ulong  FirstGeneration = 1;

    /// <summary>
    /// How far above the recovered header counter an entry's generation may run and still
    /// count as real data (the header page can lag the data pages across a power loss —
    /// nothing msyncs this log). Torn entries decode to effectively random u64 values, so
    /// the margin rejects them while a legitimately lagging header stays replayable; the
    /// airtight fix is a per-entry CRC in a v2 entry layout.
    /// </summary>
    private const ulong  GenerationSanityMargin = 1_000_000;

    /// <summary>8 MB holds ~150k scalar points; the log is reset on every flush.</summary>
    private const long DefaultCapacity = 8 * 1024 * 1024;

    /// <summary>Bucket counts per histogram point are capped so the 16-bit length field holds.</summary>
    private const int MaxBucketCounts = ushort.MaxValue;

    [StructLayout(LayoutKind.Sequential, Size = FileHeaderSize)]
    private struct WalFileHeader
    {
        public uint   Magic;
        public ushort Version;
        private ushort _pad;
        public long   WriteOffset;
        public ulong  Generation;
        public ulong  CommittedGeneration;
    }

    /// <summary>
    /// Pack = 1 pins the fields at 46 bytes inside the 48-byte stride. Without it the 8-byte
    /// members would align and push the tail past the stride into the payload area.
    /// A 64-bit generation removes any need to reason about wrap-around.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = EntryHeaderSize)]
    private struct MetricWalEntryHeader
    {
        public ulong  Generation;        // 0 = unwritten; see the class remarks
        public uint   SeriesIndex;
        public long   TimestampUnixNano;
        public double Value;
        public long   Count;
        public double Sum;
        public ushort BucketCount;       // number of int64 bucket counts that follow
    }

    /// <summary>A point recovered from the log, with the series it belongs to.</summary>
    internal readonly record struct RecoveredPoint(
        string Name, MetricKind Kind, string Unit, LabelSet Labels,
        double[]? Bounds, MetricDataPoint Point);

    private readonly string   _filePath;
    private readonly string   _poolPath;
    private readonly Lock     _writeLock = new();
    private readonly ILogger? _logger;

    private FileStream?               _file;   // held for the log's whole life; see Map

    /// <summary>
    /// Test seam invoked with the target file size just before a resize touches the file — the
    /// disk that fills at exactly the wrong moment. Fired on BOTH legs of Grow and ShrinkLocked
    /// (the resize and its restore), because the state under test is the log that could do
    /// neither. It exists because the fault the tests used to inject died with the lifetime
    /// handle: marking the file read-only no longer bites, since Windows enforces the attribute
    /// at CreateFile time and resizes no longer reopen the file. Null in production.
    /// </summary>
    internal Action<long>? BeforeResize;
    private MemoryMappedFile?         _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private byte*                     _ptr;
    private long                      _capacity;
    private long                      _floorCapacity = DefaultCapacity;   // never shrink below what Open asked for
    private long                      _writeOffset;   // logical, excludes the file header
    private ulong                     _generation;
    private ulong                     _committedGeneration;
    private bool                      _disposed;

    /// <summary>
    /// The generation handed out by <see cref="BeginFlush"/> that has not yet been committed or
    /// abandoned; 0 = none open. A scalar rather than a set, because ONE open flush is the
    /// invariant this class enforces — see the flush-protocol remarks above. In-memory only:
    /// a crash kills every open flush by definition, and every open generation is by
    /// construction ABOVE the persisted watermark, so recovery replays it. Persisting it would
    /// describe flushes that no longer exist.
    ///
    /// <para>Which is why <see cref="BeginFlush"/> hands a generation out only when it records
    /// one HERE. That "replaying it is safe" argument covers open generations, and the one
    /// state it does not describe is a generation the caller holds that was never opened: its
    /// records are above the watermark like any other, but its files are on disk, so replay
    /// duplicates them instead of rescuing them. The dead-log paths used to produce exactly
    /// that, returning <c>_generation</c> unregistered; they throw now, and every value this
    /// method returns is a generation somebody opened.</para>
    /// </summary>
    private ulong _openFlush;

    // Series registry for the CURRENT generation. Cleared on reset together with the pool
    // file, so neither grows across the life of the process.
    private readonly ConcurrentDictionary<SeriesKey, uint> _seriesIndex = new();
    private uint        _nextSeriesIndex;
    private FileStream? _poolStream;

    /// <summary>
    /// One past the highest <c>SeriesIndex</c> the reopen walk saw in the log's OWN entries;
    /// 0 when the walk saw none. Held in u64 so that an index of <c>uint.MaxValue</c> cannot
    /// wrap it back to 0 — a wrapped seed is the collision the seeding exists to prevent.
    /// The pool file cannot supply this on its own: see the seeding in
    /// <see cref="OpenOrCreate"/> for why both sources are needed.
    /// </summary>
    private ulong _survivorSeriesSeed;

    public string FilePath => _filePath;

    /// <summary>Bytes of point data currently held. Diagnostics and tests only.</summary>
    public long WrittenBytes { get { lock (_writeLock) return _writeOffset; } }

    private MetricWriteAheadLog(string filePath, ILogger? logger)
    {
        _filePath = filePath;
        _poolPath = filePath + ".pool";
        _logger   = logger;
    }

    public static MetricWriteAheadLog Open(string filePath, long initialCapacity = DefaultCapacity,
                                           ILogger? logger = null)
    {
        var wal = new MetricWriteAheadLog(filePath, logger);
        wal.OpenOrCreate(initialCapacity);
        return wal;
    }

    private void OpenOrCreate(long initialCapacity)
    {
        _floorCapacity = Math.Max(initialCapacity, 1);
        bool exists   = File.Exists(_filePath);
        long fileSize = FileHeaderSize + initialCapacity;

        // ONE handle, held until Dispose, and every mapping is created over it. Reopening by
        // path on each resize is a race with anything that reads the file — and "anything"
        // includes every naive reader on the machine, because FileShare.Read is FileStream's
        // default. A reader landing inside the unmapped window made the reopen throw a sharing
        // violation, the restore reopen throw the same one, and the log go permanently dark on
        // what was a routine resize. Measured, not imagined: a retrying reader killed it on its
        // first attempt. Resizes now run against this handle, which nothing can contend.
        _file = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        if (_file.Length < fileSize) _file.SetLength(fileSize);
        else                         fileSize = _file.Length;   // reopen an already-grown log

        _capacity = fileSize - FileHeaderSize;
        Map(fileSize);

        ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
        if (!exists || hdr.Magic != MagicNumber || hdr.Version != WalVersion)
        {
            hdr.Magic               = MagicNumber;
            hdr.Version             = WalVersion;
            hdr.WriteOffset         = FileHeaderSize;
            hdr.Generation          = FirstGeneration;
            hdr.CommittedGeneration = 0;
            _writeOffset            = 0;
            _generation             = FirstGeneration;
            _committedGeneration    = 0;
        }
        else
        {
            _writeOffset         = Math.Max(0, hdr.WriteOffset - FileHeaderSize);
            _generation          = hdr.Generation;
            _committedGeneration = hdr.CommittedGeneration;
            if (_writeOffset > _capacity) _writeOffset = _capacity;

            // The counter must LEAD the watermark. BeginFlush only ever hands out _generation
            // and CommitFlush only ever names a generation it was handed, so a header where the
            // two have crossed makes the first commit reclaim nothing and — before CommitFlush
            // was fixed to close the flush first — wedged the log for the life of the process.
            // Both fields sit inside the same 32-byte header, hence the same sector, so a torn
            // write cannot separate them: this is a restored, copied or bit-rotted file. Repair
            // it here rather than carry it, and write the repair back so a crash before the next
            // flush does not re-read the same value. `_committedGeneration + 1` is exactly
            // FirstGeneration when the watermark is 0, which is where the coercion this replaces
            // (`Generation == 0 ? FirstGeneration : …`) landed for the only case it covered.
            if (_generation <= _committedGeneration)
            {
                _generation    = _committedGeneration + 1;
                hdr.Generation = _generation;
            }

            // The header's WriteOffset is a CLAIM, and the poisoned-log incident is what
            // believing it uncritically costs: 8.45 GiB claimed, real data ending at byte 128,
            // and the 8.4 GiB in between never mentioned by anyone. Walk the entries and let
            // the data itself say where it ends.
            ReconcileDataEndLocked(ref hdr);

            // A file that grew stays grown across restarts otherwise ("reopen an already-grown
            // log" above) — the incident's 8 GiB corpse outlived both its cause and the fix for
            // it. Shrinking is safe here for the same reason the walk above is: everything past
            // the reconciled offset is by definition not data.
            long target = FitCapacity(_writeOffset);
            if (_capacity > target)
                ShrinkLocked(target, "open");
        }

        _poolStream = new FileStream(_poolPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _poolStream.Seek(0, SeekOrigin.End);

        // An empty log references nothing, so a pool carried over from before it emptied is
        // pure weight — the stand's was 29.5 MB, kept alive because truncation only ever ran
        // on a flush that emptied the log, which the poisoning prevented from ever happening.
        if (_writeOffset == 0 && _poolStream.Length > 0)
        {
            try
            {
                _poolStream.SetLength(0);
                _poolStream.Flush();
            }
            catch { /* best-effort, exactly like the truncation in Compact */ }
        }

        // Survivors remain, so their pool records must keep their indices — and the in-memory
        // registry restarts empty, so without this the next new series would take index 0
        // again. LoadPool resolves collisions later-records-win, which then attributes the
        // SURVIVING entries to the new series on the next replay: wrong name, wrong labels,
        // wrong bounds, and nothing anywhere says so.
        //
        // BOTH sources are consulted because either can survive the crash without the other,
        // and each alone leaves a survivor's index re-issuable. The pool goes out through a
        // FileStream to the OS cache, one flushed record per series; the entries are un-msynced
        // stores into a mapped page. So a lost pool tail hides the index of a survivor whose
        // entry is perfectly intact — seeding from the pool alone then hands that index straight
        // back out — and a lost data page hides an entry whose pool record is on disk. Taking
        // the max of the two makes a collision impossible rather than merely unlikely.
        //
        // All of it in u64: a survivor at index uint.MaxValue makes "+1" wrap to 0 in uint,
        // which is the collision again, in the one case where it is guaranteed rather than
        // possible. The saturating cast parks the counter at uint.MaxValue instead, where the
        // next registration reuses that index — a log with 4 billion live series has run out of
        // index space by any route, and reuse at the ceiling beats reuse from zero.
        if (_writeOffset > 0)
        {
            ulong poolMaxPlusOne = 0;
            var   pool           = LoadPool(out long cleanPoolEnd);
            foreach (var index in pool.Keys)
                if (index + 1UL > poolMaxPlusOne)
                    poolMaxPlusOne = index + 1UL;

            _nextSeriesIndex = (uint)Math.Min(uint.MaxValue,
                                              Math.Max(poolMaxPlusOne, _survivorSeriesSeed));

            // A torn pool tail is not just a lost record: the next WritePoolRecord appends
            // AFTER the garbage, so every record from then on is read at the wrong offset and
            // the whole pool decodes into nonsense. Cutting the file back to the last clean
            // boundary costs one already-unreadable record and keeps the file parseable.
            if (cleanPoolEnd < _poolStream.Length)
            {
                try
                {
                    _poolStream.SetLength(cleanPoolEnd);
                    _poolStream.Flush();
                    _poolStream.Seek(0, SeekOrigin.End);
                }
                catch { /* best-effort, exactly like the truncation in Compact */ }
            }
        }
    }

    /// <summary>
    /// Walks the entries from the front and truncates the logical end of data to where the walk
    /// stops, when the header claims more. The stop conditions are the same three every other
    /// scan in this class uses — a torn length, the generation-0 end marker, a generation no
    /// append could have stamped — so after this the header, the scans and the data agree.
    ///
    /// <para>Also plants the end marker at the reconciled offset. Without it the truncation
    /// exists only in the header, and the header page is the one nothing msyncs: a crash could
    /// resurrect the old claim and re-orphan the same bytes on the next start.</para>
    /// </summary>
    private void ReconcileDataEndLocked(ref WalFileHeader hdr)
    {
        byte* data = _ptr + FileHeaderSize;

        long    pos    = 0;
        string? reason = null;
        bool    corrupt = false;
        ulong   seed    = 0;

        while (pos + EntryHeaderSize <= _writeOffset)
        {
            ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(data + pos);
            long total = (long)EntryHeaderSize + eh.BucketCount * sizeof(long);

            if (total <= 0 || pos + total > _writeOffset) { reason = "a torn final entry"; break; }
            // NOT flagged as corruption: two legitimate crash shapes present exactly this way.
            // Compact plants the marker and stores the header afterwards, so a crash between
            // the two reopens with the old, larger claim over the survivors' originals; and
            // nothing msyncs this log, so a lost data page under a persisted header reads as
            // zeros — the marker — below the claimed end.
            if (eh.Generation == 0) { reason = "an end-of-data marker"; break; }
            if (eh.Generation > _generation + GenerationSanityMargin)
            {
                reason  = $"an entry whose generation ({eh.Generation}) no append could have stamped";
                corrupt = true;
                break;
            }

            // Every entry the walk accepts pins its series index, committed ones included: a
            // committed entry that is still physically here is replayed by nothing, but the
            // NEXT compaction reads it, and either way handing its index to a new series while
            // the bytes carrying it are still in the file costs nothing to avoid. A gap in the
            // indices is harmless — the pool is a map, not an array — a collision is not.
            if (eh.SeriesIndex + 1UL > seed) seed = eh.SeriesIndex + 1UL;

            pos += total;
        }

        // Before every return below, including the one that finds the header's claim intact:
        // that walk saw the survivors too, and its seed is the only record of their indices
        // when the pool tail was lost.
        _survivorSeriesSeed = seed;

        if (reason is null)
        {
            if (pos == _writeOffset) return;   // the claim checks out — the normal case
            reason = "a partial entry shorter than a header";
        }

        long orphaned = _writeOffset - pos;

        // A torn tail or an early end marker is what an ordinary crash leaves, and replay has
        // always discarded both — worth a line, not an alarm. A generation no append could
        // have stamped is the incident: outright corruption, and the bytes behind it grow the
        // file until someone notices by disk usage.
        if (corrupt)
            _logger?.LogError(
                "Metric WAL header claims {Claimed} bytes of data but the walk stopped at {Actual} on {Reason} — " +
                "truncating the {Orphaned} unreachable byte(s). This is corruption being repaired, not data loss: " +
                "nothing could replay those bytes either.",
                _writeOffset, pos, reason, orphaned);
        else
            _logger?.LogWarning(
                "Metric WAL: discarding {Orphaned} byte(s) of {Reason} left by an unclean shutdown " +
                "(data ends at {Actual}, header claimed {Claimed}).",
                orphaned, reason, pos, _writeOffset);

        _writeOffset    = pos;
        hdr.WriteOffset = FileHeaderSize + pos;

        if (pos + EntryHeaderSize <= _capacity)
            Unsafe.AsRef<MetricWalEntryHeader>(data + pos).Generation = 0;
    }

    /// <summary>
    /// The capacity a log holding <paramref name="dataBytes"/> needs: what <see cref="Open"/>
    /// was asked for, doubled until it fits — the same ladder <see cref="Grow"/> climbs, so
    /// shrink and growth move along one set of sizes instead of two.
    /// </summary>
    private long FitCapacity(long dataBytes)
    {
        long capacity = _floorCapacity;
        while (capacity < dataBytes) capacity *= 2;
        return capacity;
    }

    /// <summary>
    /// Shrinks the file to <paramref name="targetCapacity"/> — <see cref="Grow"/> run downward,
    /// with one deliberate difference: a failure here is swallowed, not thrown. Growth failing
    /// means an append cannot proceed and the caller must hear it; a shrink is reclamation of
    /// space nothing references, and the log is exactly as correct at the old size.
    /// </summary>
    private void ShrinkLocked(long targetCapacity, string when)
    {
        long oldCapacity = _capacity;
        long newFileSize = FileHeaderSize + targetCapacity;

        Unmap();
        try
        {
            BeforeResize?.Invoke(newFileSize);
            _file!.SetLength(newFileSize);
            Map(newFileSize);
            _capacity = targetCapacity;
            _logger?.LogInformation(
                "Metric WAL shrunk from {OldMiB:F0} MiB to {NewMiB:F0} MiB at {When} — the grown capacity was " +
                "no longer backed by data.",
                oldCapacity / 1048576.0, targetCapacity / 1048576.0, when);
        }
        catch (Exception ex)
        {
            // Re-derive from the file rather than assuming which step failed: SetLength may or
            // may not have happened. If even the restore fails, the log is left unmapped and
            // Append/BeginFlush already refuse that state honestly.
            try
            {
                BeforeResize?.Invoke(oldCapacity + FileHeaderSize);
                long actual = _file!.Length;
                Map(actual);
                _capacity = actual - FileHeaderSize;
                _logger?.LogWarning(ex,
                    "Metric WAL shrink at {When} failed; continuing at {MiB:F0} MiB.",
                    when, _capacity / 1048576.0);
            }
            catch (Exception restoreEx)
            {
                _logger?.LogError(restoreEx,
                    "Metric WAL shrink at {When} failed and the mapping could not be restored — " +
                    "the log is no longer accepting appends.", when);
            }
        }
    }

    private void Map(long fileSize)
    {
        // Over the lifetime handle, never by path — see OpenOrCreate. leaveOpen: the mapping
        // comes and goes on every resize; the handle outlives them all.
        _mmf      = MemoryMappedFile.CreateFromFile(_file!, null, fileSize, MemoryMappedFileAccess.ReadWrite,
                                                    HandleInheritability.None, leaveOpen: true);
        _accessor = _mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.ReadWrite);
        _ptr      = null;
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
    }

    // ── Append ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Logs one data point. The series is registered in the pool on first sight and referred
    /// to by index afterwards, so the per-point cost is a struct store plus the histogram
    /// bucket counts — no managed allocation on the steady-state path.
    /// </summary>
    public void Append(MetricIngestItem item, in MetricDataPoint point)
    {
        var key = new SeriesKey(item.Name ?? string.Empty, item.Kind, item.Unit ?? string.Empty,
                                item.Labels ?? LabelSet.Empty);

        long[]? buckets = point.BucketCounts;
        int bucketCount = buckets is null ? 0 : Math.Min(buckets.Length, MaxBucketCounts);
        int entrySize   = EntryHeaderSize + bucketCount * sizeof(long);

        lock (_writeLock)
        {
            if (_disposed) return;                          // shutdown race — dropping is correct
            if (_ptr is null)
                throw new InvalidOperationException(
                    "Metric WAL has no mapping; the log is not accepting appends.");

            uint seriesIdx = RegisterSeriesLocked(key, item.BucketBounds);

            while (_writeOffset + entrySize > _capacity)
                Grow();

            byte* dest = _ptr + FileHeaderSize + _writeOffset;

            ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(dest);
            eh.Generation        = _generation;
            eh.SeriesIndex       = seriesIdx;
            eh.TimestampUnixNano = point.TimestampUnixNano;
            eh.Value             = point.Value;
            eh.Count             = point.Count;
            eh.Sum               = point.Sum;
            eh.BucketCount       = (ushort)bucketCount;

            if (bucketCount > 0)
                buckets.AsSpan(0, bucketCount)
                       .CopyTo(new Span<long>(dest + EntryHeaderSize, bucketCount));

            _writeOffset += entrySize;
            Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + _writeOffset;
        }
    }

    /// <summary>Assigns (and persists, once) the pool index for a series. Caller holds the lock.</summary>
    private uint RegisterSeriesLocked(SeriesKey key, double[]? bounds)
    {
        if (_seriesIndex.TryGetValue(key, out uint existing)) return existing;

        uint index = _nextSeriesIndex++;
        _seriesIndex[key] = index;
        WritePoolRecord(index, key, bounds);
        return index;
    }

    private void WritePoolRecord(uint index, SeriesKey key, double[]? bounds)
    {
        if (_poolStream is null) return;

        var body = new ArrayBufferWriterLite();
        body.WriteByte((byte)key.Kind);
        body.WriteString(key.Name);
        body.WriteString(key.Unit);

        var pairs = key.Labels.Pairs;
        body.WriteUInt16((ushort)Math.Min(pairs.Count, ushort.MaxValue));
        for (int i = 0; i < pairs.Count && i < ushort.MaxValue; i++)
        {
            body.WriteString(pairs[i].Key);
            body.WriteString(pairs[i].Value);
        }

        int boundsLen = bounds is null ? 0 : Math.Min(bounds.Length, ushort.MaxValue);
        body.WriteUInt16((ushort)boundsLen);
        for (int i = 0; i < boundsLen; i++) body.WriteDouble(bounds![i]);

        Span<byte> head = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(head, index);
        BinaryPrimitives.WriteUInt32LittleEndian(head[4..], (uint)body.Length);
        _poolStream.Write(head);
        _poolStream.Write(body.Written);
        _poolStream.Flush();
    }

    // ── Reset ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Closes the generation being flushed and opens the next one. Call while holding the
    /// lock that makes the hot-tier snapshot atomic, so that every point in the snapshot has
    /// already been stamped with the returned generation and every point that arrives while
    /// the files are written gets the next one.
    /// </summary>
    /// <returns>The generation the snapshot belongs to — pass it to <see cref="CommitFlush"/>.</returns>
    /// <exception cref="InvalidOperationException">
    /// This call cannot open a generation, for one of two reasons, and both are the same fact
    /// to the caller: no generation was handed out, so it must not drain. Either a flush opened
    /// by an earlier <see cref="BeginFlush"/> has neither committed nor been abandoned —
    /// committing this one would reclaim that one's records while its files are still being
    /// written, the loss this guard exists to make impossible — or the log has no mapping to
    /// stamp the new generation into (see <see cref="Grow"/>). Throwing happens BEFORE the
    /// caller's snapshot is drained, so nothing is in flight to lose.
    /// </exception>
    /// <exception cref="ObjectDisposedException">
    /// The log is closed. A subclass of <see cref="InvalidOperationException"/>, so a caller
    /// that treats "BeginFlush threw" as "do not drain" needs no second handler.
    /// </exception>
    public ulong BeginFlush()
    {
        lock (_writeLock)
        {
            // Neither of these may hand a generation back the way they used to — unregistered
            // and unbumped, which reads to the caller exactly like a generation it now owns.
            // It drains the tier, writes the files, and CommitFlush refuses the same value on
            // the same condition, so the watermark never moves: the points are in a .mts AND
            // above the watermark, and every restart replays them beside their own file. Not
            // loss — duplicates, once per start, for as long as the state lasts.
            //
            // _disposed alone is covered by the drain in MetricStorageEngine.DisposeAsync, which
            // is why this was survivable in practice. _ptr null WITHOUT _disposed is not: Grow
            // unmaps before it extends, and if the extend and the restoring re-map both fail
            // (a full disk, which is when a log grows) the object stays alive with no mapping
            // and no disposal, for good. Append throws from there, so ingest fails honestly
            // while a flush would have kept writing files nobody ever commits.
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_ptr is null)
                throw new InvalidOperationException(
                    "Metric WAL has no mapping; no generation can be opened. Grow could neither " +
                    "extend the file nor restore the previous mapping, and appends are already " +
                    "failing for the same reason.");

            ulong flushing = _generation;

            if (_openFlush != 0)
                throw new InvalidOperationException(
                    $"Metric WAL flush {_openFlush} is still open; a second flush cannot begin " +
                    "until it commits or is abandoned. Committing the later one would reclaim " +
                    "the earlier one's records while its files are still being written.");

            _openFlush  = flushing;
            _generation = flushing + 1;
            Unsafe.AsRef<WalFileHeader>(_ptr).Generation = _generation;
            return flushing;
        }
    }

    /// <summary>
    /// Gives an opened generation back without committing it: the flush failed, its points went
    /// back into the hot tier, and the NEXT flush's snapshot will carry them. Its records stay
    /// in the log below an unchanged watermark, so they are what keeps those points durable
    /// until that later generation commits and covers them.
    ///
    /// <para>The failure path used to signal this by calling nothing at all, which is why the
    /// coupling was invisible. Saying it out loud is also what keeps the one-open-flush guard
    /// from wedging the log after a failed flush.</para>
    /// </summary>
    public void AbandonFlush(ulong flushedGeneration)
    {
        lock (_writeLock)
        {
            // Tolerant of a generation that is no longer the open one, because the caller is
            // built to abandon unconditionally: MetricStorageEngine's `finally` releases
            // whatever it opened whichever way the flush left, and the empty-snapshot path
            // abandons on its way out and then reaches that same `finally`. Every such second
            // call names a generation this method or CommitFlush has already closed, and the
            // one thing it must not do is close a LATER flush that has since opened.
            //
            // It used to say the tolerance was for a generation handed out by the disposed
            // path without being opened. There is no such path: BeginFlush throws on a closed
            // or unmapped log, and has since the commit that deleted the sentence this
            // replaces. A comment describing a route the code no longer has is worse than
            // none — it says the class still produces unregistered generations, which is the
            // exact state the throw exists to make impossible.
            if (_openFlush == flushedGeneration) _openFlush = 0;
        }
    }

    /// <summary>
    /// Marks everything up to <paramref name="flushedGeneration"/> as durable elsewhere and
    /// reclaims its space. Call only after the files carrying those points are on disk; a
    /// flush that failed must call <see cref="AbandonFlush"/> instead, leaving its records
    /// replayable.
    ///
    /// <para>A generation that is not the currently open flush is REFUSED. That is the direct
    /// statement of the watermark's rule — it may only ever name a generation whose data is
    /// already in files — and it holds even if a caller reintroduces concurrent flushes: the
    /// later one's commit cannot reclaim the earlier one's records.</para>
    ///
    /// <para>Refusing is not the same as leaving the flush open. Of the refusals below, exactly
    /// one belongs to somebody else's flush and must not touch it; every other one belongs to
    /// the caller's own, whose flush is over whatever the watermark says. So the ownership test
    /// is the ONLY test above the close, and the close is above everything else. That ordering
    /// is an invariant being written down, NOT the repair of an observed wedge, and the
    /// difference is worth stating because an earlier version of this comment claimed the
    /// second: both states the close now stands in front of are unreachable in this class as it
    /// is. See the comment on the order.</para>
    ///
    /// <para><b>The answer, and why it is a value and not silence.</b> The caller has files on
    /// disk holding the generation's points, and its next move depends entirely on whether the
    /// watermark now covers them. <see cref="MetricWalCommit.Committed"/> means it does, so the
    /// log will not replay them and the files are the only copy. <see
    /// cref="MetricWalCommit.Refused"/> means it does not: nothing was reclaimed, the
    /// generation's records are intact in the log, and the caller's files are a SECOND copy of
    /// points that are still durable here — which the next start replays beside them unless the
    /// caller takes its files back. Returning nothing made those two states identical from the
    /// outside, and the second was reached by a real route: a flush opens a generation, the log
    /// loses its mapping while the files are being written (see <see cref="Grow"/>), and the
    /// commit then returned exactly as if it had succeeded.</para>
    ///
    /// <para>The boundary the answer is defined at is the watermark store, and this method
    /// throws only from BEYOND it. Everything before it — the tests, the close, the header
    /// write — cannot throw; the reclaim after it can, in principle, and a throw there is not a
    /// refusal. Its generation IS committed and its files ARE the durable copy, so a caller
    /// that treated the exception as "not committed" and unwrote them would turn an un-reclaimed
    /// log into lost points. Hence: a returned value distinguishes covered from not covered, and
    /// an exception can only ever mean covered.</para>
    /// </summary>
    public MetricWalCommit CommitFlush(ulong flushedGeneration)
    {
        lock (_writeLock)
        {
            // Not the flush that is writing: reclaims nothing AND leaves the open flush alone,
            // which is the whole point — closing somebody else's flush here is the
            // reclaim-while-writing this class exists to refuse.
            //
            // Answered by the watermark rather than flatly refused, because "not the open
            // flush" and "not covered" are different facts and the caller acts on the second.
            // A generation the watermark already names is durable in files whoever put it
            // there, and telling its flush otherwise would cost it the very files that carry
            // it. Unreachable through this engine, which commits each generation once; the two
            // lines are what stop it being a trap for the next caller.
            if (flushedGeneration != _openFlush)
                return flushedGeneration <= _committedGeneration
                    ? MetricWalCommit.Committed
                    : MetricWalCommit.Refused;

            // From here the generation is the caller's own, so its flush is over and the flag it
            // set comes down unconditionally — BEFORE every remaining test, none of which can
            // return in front of it.
            //
            // What this ordering is, stated plainly, because the commit that introduced it said
            // something stronger and the something stronger is not true. NEITHER of the two tests
            // below can be reached with a flush open, so moving the close above them repaired no
            // wedge anybody could have had. Both were reverted and MetricWalTests +
            // MetricFormatV3Tests + MetricChunkedRewriteTests stayed at Failed 0, Passed 61,
            // which is the honest state of the cover: nothing pins this, and nothing can without
            // a seam that would exist only to be pinned.
            //
            // The watermark test is dead here by an invariant the class enforces at both ends.
            // OpenOrCreate normalises a crossed header to _generation = _committedGeneration + 1,
            // BeginFlush hands out _generation and raises it, and _committedGeneration only ever
            // moves inside this method's own success path — where it is set to the generation
            // that is open and the flush is closed in the same critical section. So an open flush
            // is always ABOVE the watermark. The dead-log test is dead in a different way: it can
            // be reached (dispose or a failed Grow between BeginFlush and here), but a log in
            // either state refuses the next BeginFlush on its own account and can never leave it
            // — Append does not re-map and Grow is only called from Append — so a stuck flag has
            // nothing left to wedge.
            //
            // It stays above them anyway, and is worth a paragraph rather than a shrug, because
            // what it costs is one statement's position and what it buys is that the flag's
            // lifetime does not depend on either of those arguments continuing to hold. A change
            // to the crossed-header repair, or a caller that commits a generation it was not
            // handed, makes the first reachable; and BeginFlush throwing forever is a failure
            // both flush paths merely LOG — the periodic loop catches it at Error once a tick,
            // the threshold continuation once per crossing — while no .mts is written again and
            // the log grows by doubling until Grow throws into ingest.
            _openFlush = 0;

            // Nothing left to reclaim: the watermark already names this generation or a later
            // one. Unreachable from here today (see above) and kept as the definition of what
            // the answer means rather than as a live branch: a generation the watermark covers
            // is committed, whoever put it there. Committed, and truthfully so — the caller's
            // points are covered.
            if (flushedGeneration <= _committedGeneration) return MetricWalCommit.Committed;

            // The log cannot record anything: closed, or left without a mapping by a Grow that
            // could neither extend the file nor restore what it had. Both leave the watermark
            // where it was, so this generation's records stay in the log, uncompacted — the
            // reclaim below is the only thing that removes them and it is not reached. The
            // caller is told, because it is holding the only other copy and only it can decide
            // what to do with it.
            if (_disposed || _ptr is null) return MetricWalCommit.Refused;

            _committedGeneration = flushedGeneration;

            // The watermark lands before a single byte moves: a crash mid-compaction then
            // still replays exactly the survivors, never the points already in files. It is
            // also the line this method's answer is defined at — past it the generation is
            // committed no matter what the reclaim does.
            ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            hdr.CommittedGeneration = flushedGeneration;

            Compact(flushedGeneration);
            return MetricWalCommit.Committed;
        }
    }

    /// <summary>
    /// Moves entries above the watermark to the front. Generation is non-decreasing in append
    /// order, so the committed entries are a prefix and the survivors one contiguous tail —
    /// a single move, bounded by whatever arrived while the files were being written.
    ///
    /// <para>That physical prefix is a property of the LAYOUT and holds for any watermark
    /// value: the generation is stamped inside <c>_writeLock</c>, is only ever raised inside
    /// the same lock, and the write offset advances under it too, so entries at or below any
    /// threshold occupy a physical prefix whatever order flushes complete in. What the
    /// one-open-flush rule adds is the SEMANTIC half — that the set of generations already in
    /// files has no holes, so a single u64 watermark can express it. Both halves are needed;
    /// this method's single move rests on the first, and its correctness on the second.</para>
    /// </summary>
    private void Compact(ulong committed)
    {
        byte* data = _ptr + FileHeaderSize;

        long firstSurvivor = _writeOffset;
        long pos = 0;
        while (pos + EntryHeaderSize <= _writeOffset)
        {
            ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(data + pos);
            long total = (long)EntryHeaderSize + eh.BucketCount * sizeof(long);
            if (total <= 0 || pos + total > _writeOffset) break;
            if (eh.Generation == 0) break;
            // A generation FAR above the header counter cannot have been stamped by any
            // append. It is a torn/corrupt entry, and everything past it parses off garbage
            // lengths: treat it as end-of-data so it truncates away. Without this, one such
            // entry (a real incident: a torn first entry decoding to generation ~155e9) is
            // forever "above the watermark" — never compacted, the log never empties, and
            // the mmap doubles without bound (8 GiB observed). The margin, not a strict
            // `> _generation`: nothing msyncs this log, so after power loss the header page
            // can LAG the data pages — compaction relocates gen-(G+1) survivors below
            // offsets an older header snapshot (Generation = G) already covers, and a
            // strict guard would truncate that legitimate replay to nothing. Torn entries
            // decode to effectively random 64-bit values, which the margin still rejects.
            if (eh.Generation > _generation + GenerationSanityMargin) break;
            if (eh.Generation > committed) { firstSurvivor = pos; break; }
            pos += total;
        }

        long surviving = _writeOffset - firstSurvivor;
        if (surviving > 0 && firstSurvivor > 0)
            Buffer.MemoryCopy(data + firstSurvivor, data, _capacity, surviving);

        _writeOffset = Math.Max(0, surviving);

        // The move does not erase its source. A crash before the offset store below would
        // therefore leave the old, larger offset covering BOTH the relocated survivors and
        // the originals they were copied from, and replay would return each twice. Marking
        // the slot past the new end with generation 0 makes such a scan stop exactly where
        // the data now ends — ReadAll already treats 0 as end-of-data. (No room for the
        // marker means the log is at capacity, where the next append grows it anyway.)
        if (_writeOffset + EntryHeaderSize <= _capacity)
            Unsafe.AsRef<MetricWalEntryHeader>(data + _writeOffset).Generation = 0;

        Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + _writeOffset;

        // The pool is only reclaimable once nothing references it. Survivors still carry
        // their series indices, so it is truncated on the flushes that empty the log — which
        // is the normal case — and simply kept otherwise, bounded by the series cardinality.
        if (_writeOffset == 0)
        {
            _seriesIndex.Clear();
            _nextSeriesIndex = 0;
            try
            {
                _poolStream?.SetLength(0);
                _poolStream?.Flush();
            }
            catch { /* best-effort: stale records are tolerated by replay */ }

            // A log that grew and then emptied gives the space back NOW, not at the next
            // restart: the incident machine ran for weeks growing 1.3 GB a day, and a shrink
            // that only fires at open would have waited for all of them. Last in this method,
            // deliberately — ShrinkLocked remaps, so every pointer taken above (`data`, the
            // header ref) is dead past this line, and nothing may follow it.
            if (_capacity > _floorCapacity)
                ShrinkLocked(_floorCapacity, "commit");
        }
    }

    // ── Recovery ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Replays every complete point above the committed watermark — that is, everything not
    /// yet known to be in a file, including a snapshot whose flush never completed. Points
    /// whose series is missing from the pool are skipped, since they cannot be reconstructed,
    /// and reported through <paramref name="unresolved"/> rather than silently dropped.
    /// </summary>
    public List<RecoveredPoint> ReadAll(out int unresolved)
    {
        var result = new List<RecoveredPoint>();
        unresolved = 0;

        lock (_writeLock)
        {
            if (_ptr is null) return result;

            var pool = LoadPool();
            long pos = 0;
            long end = _writeOffset;

            while (pos + EntryHeaderSize <= end)
            {
                byte* src = _ptr + FileHeaderSize + pos;
                ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(src);

                long total = (long)EntryHeaderSize + eh.BucketCount * sizeof(long);
                if (total <= 0 || pos + total > end) break;   // torn tail
                if (eh.Generation == 0) break;                // end of real data
                // No append can stamp a generation far above the header counter — such an
                // entry is torn/corrupt, and its fields are garbage: stop instead of
                // replaying them into the hot tier (from where a flush writes them into
                // permanent files). Margin semantics: see Compact.
                if (eh.Generation > _generation + GenerationSanityMargin) break;

                if (eh.Generation > _committedGeneration)
                {
                    if (pool.TryGetValue(eh.SeriesIndex, out var series))
                    {
                        long[]? buckets = null;
                        if (eh.BucketCount > 0)
                        {
                            buckets = new long[eh.BucketCount];
                            new ReadOnlySpan<long>(src + EntryHeaderSize, eh.BucketCount).CopyTo(buckets);
                        }

                        result.Add(new RecoveredPoint(
                            series.Name, series.Kind, series.Unit, series.Labels, series.Bounds,
                            new MetricDataPoint
                            {
                                TimestampUnixNano = eh.TimestampUnixNano,
                                Value             = eh.Value,
                                Count             = eh.Count,
                                Sum               = eh.Sum,
                                BucketCounts      = buckets,
                            }));
                    }
                    else unresolved++;
                }

                pos += total;
            }
        }

        return result;
    }

    private readonly record struct PoolEntry(
        string Name, MetricKind Kind, string Unit, LabelSet Labels, double[]? Bounds);

    /// <summary>
    /// Reads the companion pool. Later records win for a given index, which is what makes a
    /// reset that failed to truncate the file harmless.
    /// </summary>
    private Dictionary<uint, PoolEntry> LoadPool() => LoadPool(out _);

    /// <summary>
    /// <see cref="LoadPool()"/>, and also reports where the clean records stop:
    /// <paramref name="cleanEnd"/> is the offset just past the LAST record that parsed whole,
    /// which is 0 for a pool whose very first record is torn and the file length for an intact
    /// one. The loop already stops at a torn head or body; this only says WHERE it stopped, so
    /// that <see cref="OpenOrCreate"/> can append the next record at that boundary instead of
    /// after the torn bytes — appending past them leaves every later record misaligned by the
    /// width of the garbage, which breaks pool parsing for every series registered afterwards,
    /// not just the one that was lost.
    /// </summary>
    private Dictionary<uint, PoolEntry> LoadPool(out long cleanEnd)
    {
        var map = new Dictionary<uint, PoolEntry>();
        cleanEnd = 0;
        if (!File.Exists(_poolPath)) return map;

        try
        {
            _poolStream?.Flush();
            using var fs = new FileStream(_poolPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            Span<byte> head = stackalloc byte[8];

            while (fs.Read(head) == 8)
            {
                uint index = BinaryPrimitives.ReadUInt32LittleEndian(head);
                uint len   = BinaryPrimitives.ReadUInt32LittleEndian(head[4..]);
                if (len == 0 || len > 8 * 1024 * 1024) break;    // torn or bogus record

                var body = new byte[len];
                if (fs.Read(body) != len) break;                 // truncated tail

                var r = new SpanCursor(body);
                var kind = (MetricKind)r.ReadByte();
                string name = r.ReadString();
                string unit = r.ReadString();

                int labelCount = r.ReadUInt16();
                var pairs = new KeyValuePair<string, string>[labelCount];
                for (int i = 0; i < labelCount; i++)
                {
                    string k = r.ReadString();
                    string v = r.ReadString();
                    pairs[i] = new KeyValuePair<string, string>(k, v);
                }

                int boundsLen = r.ReadUInt16();
                double[]? bounds = null;
                if (boundsLen > 0)
                {
                    bounds = new double[boundsLen];
                    for (int i = 0; i < boundsLen; i++) bounds[i] = r.ReadDouble();
                }

                map[index]  = new PoolEntry(name, kind, unit, new LabelSet(pairs), bounds);
                cleanEnd    = fs.Position;   // this record parsed whole; the boundary is here
            }
        }
        catch { /* best-effort: whatever resolved stays usable, the rest is reported */ }

        return map;
    }

    // ── Grow ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Doubles the mapped capacity. Windows refuses to resize a mapped file, so the old
    /// mapping must go first — which means a failure here (a full disk, i.e. exactly when a
    /// log grows) could leave the object alive with no mapping, silently refusing every
    /// later append. The old mapping is therefore restored before the exception escapes.
    /// </summary>
    private void Grow()
    {
        long oldFileSize = FileHeaderSize + _capacity;
        long newCapacity = _capacity * 2;
        long newFileSize = FileHeaderSize + newCapacity;

        Unmap();
        try
        {
            BeforeResize?.Invoke(newFileSize);
            _file!.SetLength(newFileSize);
            Map(newFileSize);
            _capacity = newCapacity;
        }
        catch
        {
            try
            {
                BeforeResize?.Invoke(oldFileSize);
                if (_file!.Length < oldFileSize) _file.SetLength(oldFileSize);
                Map(oldFileSize);
            }
            catch { /* nothing left to restore to — the throw below is the honest signal */ }
            throw;
        }
    }

    private void Unmap()
    {
        if (_accessor is not null)
        {
            try { _accessor.SafeMemoryMappedViewHandle.ReleasePointer(); } catch { }
            _accessor.Dispose();
        }
        _mmf?.Dispose();
        _accessor = null;
        _mmf      = null;
        _ptr      = null;
    }

    // ── Dispose ──────────────────────────────────────────────────────────────

    public void Dispose()
    {
        lock (_writeLock)
        {
            if (_disposed) return;
            _disposed = true;
            try { _poolStream?.Flush(); _poolStream?.Dispose(); } catch { }
            _poolStream = null;
            Unmap();
            try { _file?.Dispose(); } catch { }
            _file = null;
        }
    }

    /// <summary>Closes and removes both files. Used by tests and by a data-directory reset.</summary>
    public void Delete()
    {
        Dispose();
        try { if (File.Exists(_filePath)) File.Delete(_filePath); } catch { }
        try { if (File.Exists(_poolPath)) File.Delete(_poolPath); } catch { }
    }

    // ── Tiny binary helpers ──────────────────────────────────────────────────

    /// <summary>Growable little-endian writer for pool records (cold path, once per series).</summary>
    private sealed class ArrayBufferWriterLite
    {
        private byte[] _buf = new byte[256];
        private int    _len;

        public int                Length  => _len;
        public ReadOnlySpan<byte> Written => _buf.AsSpan(0, _len);

        private Span<byte> Take(int n)
        {
            if (_len + n > _buf.Length) Array.Resize(ref _buf, Math.Max(_buf.Length * 2, _len + n));
            var s = _buf.AsSpan(_len, n);
            _len += n;
            return s;
        }

        public void WriteByte(byte b)      => Take(1)[0] = b;
        public void WriteUInt16(ushort v)  => BinaryPrimitives.WriteUInt16LittleEndian(Take(2), v);
        public void WriteDouble(double v)  => BinaryPrimitives.WriteDoubleLittleEndian(Take(8), v);

        public void WriteString(string? s)
        {
            s ??= string.Empty;
            int n = Math.Min(Encoding.UTF8.GetByteCount(s), ushort.MaxValue);
            WriteUInt16((ushort)n);
            if (n > 0) Encoding.UTF8.GetBytes(s, Take(n));
        }
    }

    /// <summary>Forward-only reader over a pool record body.</summary>
    private ref struct SpanCursor(ReadOnlySpan<byte> data)
    {
        private readonly ReadOnlySpan<byte> _data = data;
        private int _pos;

        public byte ReadByte() => _pos < _data.Length ? _data[_pos++] : (byte)0;

        public ushort ReadUInt16()
        {
            if (_pos + 2 > _data.Length) { _pos = _data.Length; return 0; }
            var v = BinaryPrimitives.ReadUInt16LittleEndian(_data[_pos..]);
            _pos += 2;
            return v;
        }

        public double ReadDouble()
        {
            if (_pos + 8 > _data.Length) { _pos = _data.Length; return 0; }
            var v = BinaryPrimitives.ReadDoubleLittleEndian(_data[_pos..]);
            _pos += 8;
            return v;
        }

        public string ReadString()
        {
            int n = ReadUInt16();
            if (n == 0 || _pos + n > _data.Length) { _pos = Math.Min(_pos + n, _data.Length); return string.Empty; }
            var s = Encoding.UTF8.GetString(_data.Slice(_pos, n));
            _pos += n;
            return s;
        }
    }
}
