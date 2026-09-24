using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Ameto.Core;
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
///   metrics.wal (v2)
///     [File Header — 32 bytes]
///       0   Magic               uint32  "RDMW"
///       4   Version             uint16  2   (1 is still READ: see Open)
///       6   _pad                uint16
///       8   WriteOffset         int64
///      16   Generation          uint64  stamped on new appends
///      24   CommittedGeneration uint64  everything at or below this is already in files
///     [Entry Header — 48 bytes, Pack = 1, the fields of MetricWalEntryHeader]
///     [Crc          — uint32, CRC32C over the 48 header bytes + the bucket counts]
///     [BucketCounts — BucketCount × int64]
///
///   metrics.wal.pool (records of both shapes may sit in one file)
///     v1: [index uint32][byteLen uint32][kind, name, unit, labels, bounds]
///     v2: [index uint32][byteLen uint32, top byte 0xC5][crc uint32][same body]
///         crc = CRC32C over the first 8 bytes of the record + the body
/// </code>
///
/// <para><b>v2 is v1 plus a checksum per entry and per pool record, and nothing else.</b> v1
/// protected an entry only by judgement — the generation margin, the series-index cap, the
/// header's claimed end checked against the walk — so a torn entry whose fields happened to
/// decode as plausible (a generation inside the margin, a small series index) replayed as a
/// point, and a flush then wrote it into a permanent <c>.mts</c>, the way the #56 incident's
/// year-2116 garbage point did on every restart. The CRC is stored after the 48 header bytes and
/// written LAST, and every walk that decides what the data is — the open-time reconciliation,
/// both passes of <see cref="ReadAll"/> — stops at the first entry that does not verify. The
/// judgement checks stay, ahead of it: they still classify what they catch as corruption (an
/// Error and a quarantine copy) where a checksum failure is reported as the torn write an
/// unclean stop leaves, and they are all a v1 log has (see <see cref="Open"/>).</para>
///
/// <para><b>Durability, stated because it is a choice.</b> Nothing msyncs this log: not an
/// append, not a timer — THERE IS NO PERIODIC FSYNC BETWEEN FLUSHES — and not a commit either.
/// A flushed point's durable copy is its <c>.mts</c>, which the writer forces to disk before the
/// commit; the log's own pages reach the disk when the OS writes them back, and at
/// <see cref="Dispose"/>. So the death of the PROCESS loses nothing (the page cache is the
/// file's), and the death of the MACHINE loses whatever had not been written back, in whatever
/// page order the OS chose. Program order is what the checksum rides on: an entry's CRC is
/// stored after its last field and bucket, and the header's write offset only after the whole
/// batch (<see cref="WriteBatchLocked"/>), so a process that dies inside an append leaves the
/// batch unclaimed, and every claimed entry verifies. After a power loss the checksum turns the
/// loss into a clean cut at the first entry that does not verify instead of a replay of
/// garbage. What it cannot do is make a commit's relocation durable: the moved survivors and
/// the header that names them sit on different pages, so a power loss can keep the header and
/// lose the move — the points that arrived during that flush are then lost, not
/// misreplayed.</para>
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
    private const ushort WalVersion      = 2;
    private const ushort WalVersionV1    = 1;
    private const int    FileHeaderSize  = 32;
    private const ulong  FirstGeneration = 1;

    /// <summary>The 48 bytes of <see cref="MetricWalEntryHeader"/> — every byte the entry checksum covers besides the buckets. All of a v1 entry header.</summary>
    private const int    ChecksummedHeaderBytes = 48;

    /// <summary>A v2 entry header: the checksummed 48 bytes, then the CRC32C over them and the bucket counts.</summary>
    private const int    EntryHeaderSize   = ChecksummedHeaderBytes + sizeof(uint);

    /// <summary>A v1 entry header: no checksum. Read and appended only by a log that is v1 on disk; see <see cref="Open"/>.</summary>
    private const int    EntryHeaderSizeV1 = ChecksummedHeaderBytes;

    /// <summary>
    /// How far above the recovered header counter an entry's generation may run and still
    /// count as real data (the header page can lag the data pages across a power loss —
    /// nothing msyncs this log). Torn entries decode to effectively random u64 values, so
    /// the margin rejects them while a legitimately lagging header stays replayable. The
    /// per-entry CRC (v2) is what catches the torn entry whose generation lands INSIDE the
    /// margin; this stays as the check that tells corruption from a torn write, and as all a
    /// v1 log has.
    /// </summary>
    private const ulong  GenerationSanityMargin = 1_000_000;

    /// <summary>
    /// A series index at or above this is garbage, not data: indices are issued sequentially
    /// from zero, one per distinct series, and the largest catalog ever observed is five
    /// thousand times smaller. The open-time walk stops on one the way it stops on an
    /// impossible generation — a torn entry's generation can land on a plausible value while
    /// the NEXT field decodes to junk, and that junk would otherwise flow into the seed,
    /// park the counter at uint.MaxValue, and hand the wrap-to-zero collision to the second
    /// registration after the restart.
    /// </summary>
    private const uint   SeriesIndexSanityCap = 1u << 30;

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
    /// Pack = 1 pins the fields at exactly 48 bytes. Without it the 8-byte members would align
    /// and push the tail past the stride into the payload area. A 64-bit generation removes any
    /// need to reason about wrap-around. In v2 the entry's CRC follows these 48 bytes; it is not
    /// a field here, so a v1 entry and a v2 entry share this struct byte for byte.
    ///
    /// <para><see cref="Reserved"/> is the two bytes v1 left as padding and never wrote. A v2
    /// append writes it as zero, because the checksum covers it and may be computed from the
    /// point in hand rather than from the map (see <see cref="PrecomputeChecksums"/>); a v1
    /// entry's padding holds whatever the region held before it.</para>
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = ChecksummedHeaderBytes)]
    private struct MetricWalEntryHeader
    {
        public ulong  Generation;        // 0 = unwritten; see the class remarks
        public uint   SeriesIndex;
        public long   TimestampUnixNano;
        public double Value;
        public long   Count;
        public double Sum;
        public ushort BucketCount;       // number of int64 bucket counts that follow
        public ushort Reserved;          // 0 in v2; unwritten padding in v1
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
    /// disk that fills at exactly the wrong moment. Fired once by GrowTo (which touches nothing
    /// before it, so has nothing to restore) and on BOTH legs of ShrinkLocked
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
    /// The entry stride this log reads and appends, and whether its entries and pool records
    /// carry checksums: v2 (<see cref="EntryHeaderSize"/>, checksummed) unless the file on disk
    /// is a v1 log (<see cref="EntryHeaderSizeV1"/>, no checksum), which stays v1 for the life of
    /// the process (see <see cref="Open"/>). Set by <see cref="OpenOrCreate"/> before the first
    /// walk and never again, so the lock-free size pass of an append may read it.
    /// </summary>
    private int  _entryHeaderSize = EntryHeaderSize;
    private bool _checksummed     = true;

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
    /// Bumped whenever <see cref="Compact"/> empties the log and clears the registry. An index
    /// resolved outside <c>_writeLock</c> (see <see cref="Append(ReadOnlySpan{MetricIngestItem})"/>)
    /// is only usable if this has not moved since: past a clear the pool file is truncated and
    /// indices are re-issued from 0, so a carried-over index names a record that is gone.
    /// Written under the lock, read without it, hence <see cref="Volatile"/>.
    /// </summary>
    private long _seriesEpoch;

    /// <summary>"Not in the registry when it was looked up" — <see cref="uint.MaxValue"/> is
    /// beyond <see cref="SeriesIndexSanityCap"/>, so it can never be a real index.</summary>
    private const uint Unregistered = uint.MaxValue;

    /// <summary>
    /// Test seam fired between the lock-free series lookups and the acquisition of the write
    /// lock — the window in which a commit can clear the registry under a batch that has already
    /// resolved its indices. Null in production. The alternative is a timing race, and a
    /// concurrency test judged by a timer is not judged.
    /// </summary>
    internal Action? OnSeriesResolvedForTest;

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

    /// <summary>
    /// What the pool's strings and label sets are replayed through: <see cref="MetricLabelInterner.Shared"/>,
    /// the instance the OTLP parsers use, unless <see cref="Open"/> was handed another — which
    /// only a test does, so that what it checks about the replay does not hang on how full the
    /// rest of its process has made the shared one.
    /// </summary>
    internal MetricLabelInterner Interner { get; }

    private MetricWriteAheadLog(string filePath, ILogger? logger, MetricLabelInterner? interner)
    {
        _filePath = filePath;
        _poolPath = filePath + ".pool";
        _logger   = logger;
        Interner  = interner ?? MetricLabelInterner.Shared;
    }

    /// <summary>
    /// Opens the log, creating it if absent, and reconciles what a crash left (see
    /// <see cref="OpenOrCreate"/>).
    ///
    /// <para><b>A v1 LOG IS READ IN ITS OWN LAYOUT, NOT DISCARDED.</b> The release before this one
    /// wrote v1, and an unknown version is re-initialised as a foreign file — which, for the v1 log
    /// the first start of this release finds, would silently drop every point the previous process
    /// logged and never flushed. So a v1 file is opened with the v1 stride, reconciled exactly as
    /// that release would have (the #59 repairs are version-blind), and kept v1 for the life of the
    /// process: every point it holds replays, new points are appended in the v1 layout, and its
    /// pool gets v1 records. That is exactly the log the previous release ran with, and so exactly
    /// the file a rollback to it can still read. What it does not get is a checksum.</para>
    ///
    /// <para><b>ROLLING BACK is not symmetric.</b> A release older than v2 treats a v2 log as a
    /// foreign file and re-initialises it, which empties it and, with it, the pool. That costs
    /// nothing only when the log held nothing unflushed — after a clean stop whose final flush
    /// ran and no point arrived behind it.</para>
    /// </summary>
    public static MetricWriteAheadLog Open(string filePath, long initialCapacity = DefaultCapacity,
                                           ILogger? logger = null, Action<long>? beforeResize = null,
                                           MetricLabelInterner? interner = null)
    {
        var wal = new MetricWriteAheadLog(filePath, logger, interner)
        {
            // Armed before OpenOrCreate, or the open-time shrink would be the one resize the seam
            // cannot reach — and its double-failure path is exactly what needs the coverage.
            BeforeResize = beforeResize,
        };
        try { wal.OpenOrCreate(initialCapacity); }
        catch { wal.Dispose(); throw; }                  // the lifetime handle, not left to the finalizer
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
        bool known = exists && hdr.Magic == MagicNumber && (hdr.Version == WalVersion || hdr.Version == WalVersionV1);
        if (known && hdr.Version == WalVersionV1)
        {
            // Left in the v1 layout, and read with it — BEFORE the walk below, which has to step
            // with the file's own stride. See Open.
            _entryHeaderSize = EntryHeaderSizeV1;
            _checksummed     = false;
        }

        if (!known)
        {
            // New, foreign or future-versioned file — re-initialise in place, as v2. Anything
            // already there cannot be replayed under a layout this build does not know.
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

        }

        // A file that grew stays grown across restarts otherwise ("reopen an already-grown log"
        // above) — the incident's 8 GiB corpse outlived both its cause and the fix for it.
        // OUTSIDE the header branch, deliberately: a grown file whose Magic or Version rotted
        // lands in the fresh-header branch above, and it needs the shrink MORE, not less — its
        // write offset is zero, so nothing later would ever empty-and-shrink it on a trickle
        // deployment. Shrinking is safe for the same reason the walk is: everything past the
        // reconciled offset is by definition not data.
        long target = FitCapacity(_writeOffset);
        if (_capacity > target)
            ShrinkLocked(target, "open");

        // ShrinkLocked swallows failures because mid-run a log at the old size is exactly as
        // correct — but at OPEN a double failure (the resize and its restore both refused)
        // leaves no mapping at all, and an engine built over that would come up, accept
        // traffic, recover nothing, and throw from every single append. A log that cannot
        // hold one entry does not get to open; better one clear line at startup.
        if (_ptr is null)
            throw new IOException(
                "Metric WAL could not restore its mapping after a failed shrink at open — " +
                "refusing to open a log that cannot accept an append. See the shrink error above.");

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
        //
        // THE INDICES ONLY. Seeding needs the largest index of every record, dead or alive, and
        // nothing about their text: the walk reads each record's head and steps over its body, so
        // opening the log interns nothing (see LoadPool for why that matters).
        if (_writeOffset > 0)
        {
            ReadPoolRecords(keep: null, out long cleanPoolEnd, out ulong poolMaxPlusOne);

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
    /// stops, when the header claims more. The stop conditions are <see cref="EntryAt"/>'s — the
    /// one definition every scan in this class steps with: a torn length, the generation-0 end
    /// marker, a generation no append could have stamped, a series index no registration could
    /// have issued, and (v2) a checksum that does not verify — so after this the header, the
    /// scans and the data agree, and everything below the reconciled end verifies.
    ///
    /// <para>Also plants the end marker at the reconciled offset. Without it the truncation
    /// exists only in the header, and the header page is the one nothing msyncs: a crash could
    /// resurrect the old claim and re-orphan the same bytes on the next start.</para>
    /// </summary>
    private void ReconcileDataEndLocked(ref WalFileHeader hdr)
    {
        byte* data = _ptr + FileHeaderSize;

        long     pos  = 0;
        ulong    seed = 0;
        WalkStop stop;
        long     total;

        while ((total = EntryAt(data, pos, _writeOffset, verify: true, out stop)) > 0)
        {
            ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(data + pos);

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

        if (pos == _writeOffset) return;   // the claim checks out — the normal case

        ref var at = ref Unsafe.AsRef<MetricWalEntryHeader>(data + pos);
        string reason = stop switch
        {
            WalkStop.Short          => "a partial entry shorter than a header",
            WalkStop.Torn           => "a torn final entry",
            // NOT flagged as corruption: two legitimate crash shapes present exactly this way.
            // Compact plants the marker and stores the header afterwards, so a crash between
            // the two reopens with the old, larger claim over the survivors' originals; and
            // nothing msyncs this log, so a lost data page under a persisted header reads as
            // zeros — the marker — below the claimed end.
            WalkStop.EndMarker      => "an end-of-data marker",
            WalkStop.BadGeneration  => $"an entry whose generation ({at.Generation}) no append could have stamped",
            WalkStop.BadSeriesIndex => $"an entry whose series index ({at.SeriesIndex}) no registration could have issued",
            // NOT corruption either, by the same argument: a power loss writes the pages of an
            // append back in any order, so a claimed entry whose own page did not land is the
            // ordinary torn write of an unclean stop — v1 replayed it, garbage and all.
            _                       => "an entry that fails its checksum",
        };
        bool corrupt = stop is WalkStop.BadGeneration or WalkStop.BadSeriesIndex;

        long orphaned = _writeOffset - pos;

        // A torn tail or an early end marker is what an ordinary crash leaves, and replay has
        // always discarded both — worth a line, not an alarm. A generation no append could
        // have stamped is the incident: outright corruption, and the bytes behind it grow the
        // file until someone notices by disk usage.
        if (corrupt)
        {
            // Forensics before repair, capped. The discarded region is unreachable by any
            // replay, so a full copy would be safety theatre — at 8 GiB, on a disk the growth
            // may already be filling, actively harmful theatre. But reconciliation is
            // judgement, not proof (the header's own counter is the margin's reference, and
            // nothing bounds it from above), so the head of what is about to be cut is kept
            // where a person can look at it. Best-effort, like everything else that must not
            // stand between a damaged log and a working one.
            string quarantine = _filePath + ".quarantine";
            long   kept       = Math.Min(orphaned, 4 * 1024 * 1024);
            try
            {
                var snapshot = new byte[kept];
                new ReadOnlySpan<byte>(data + pos, (int)kept).CopyTo(snapshot);
                File.WriteAllBytes(quarantine, snapshot);
            }
            catch { kept = 0; }

            _logger?.LogError(
                "Metric WAL header claims {Claimed} bytes of data but the walk stopped at {Actual} on {Reason} — " +
                "truncating the {Orphaned} unreachable byte(s). This is corruption being repaired, not data loss: " +
                "nothing could replay those bytes either. The first {Kept} of them are preserved at {Quarantine}.",
                _writeOffset, pos, reason, orphaned, kept, quarantine);
        }
        else
            _logger?.LogWarning(
                "Metric WAL: discarding {Orphaned} byte(s) of {Reason} left by an unclean shutdown " +
                "(data ends at {Actual}, header claimed {Claimed}).",
                orphaned, reason, pos, _writeOffset);

        _writeOffset    = pos;
        hdr.WriteOffset = FileHeaderSize + pos;

        if (pos + _entryHeaderSize <= _capacity)
            Unsafe.AsRef<MetricWalEntryHeader>(data + pos).Generation = 0;
    }

    /// <summary>Why <see cref="EntryAt"/> ended a walk.</summary>
    private enum WalkStop : byte
    {
        None,
        /// <summary>Less than a header left before the end.</summary>
        Short,
        /// <summary>The entry's buckets run past the end.</summary>
        Torn,
        /// <summary>Generation 0: never written, or the terminator a compaction or a repair planted.</summary>
        EndMarker,
        /// <summary>A generation far above the header counter — no append stamped it.</summary>
        BadGeneration,
        /// <summary>A series index past <see cref="SeriesIndexSanityCap"/> — no registration issued it.</summary>
        BadSeriesIndex,
        /// <summary>v2: the CRC does not match the bytes.</summary>
        BadChecksum,
    }

    /// <summary>
    /// THE ONE STEP EVERY WALK TAKES: the length of the entry at logical offset
    /// <paramref name="pos"/> of <paramref name="data"/>, or 0 where the data ends before
    /// <paramref name="end"/>, with <paramref name="stop"/> saying why. The checks run in this
    /// order and each only reads what the ones before it proved is inside the range: a header
    /// that does not fit, buckets that do not fit, the generation-0 marker, a generation or a
    /// series index nothing could have written, and — last, and only for a checksummed log with
    /// <paramref name="verify"/> — the CRC.
    ///
    /// <para><see cref="Compact"/> steps with <paramref name="verify"/> false, deliberately: every
    /// entry below <c>_writeOffset</c> of a live log was either written by this process under the
    /// write lock, checksum last, or verified by the open-time walk that set <c>_writeOffset</c>,
    /// and re-hashing the committed prefix there would add a pass over up to the whole log to a
    /// commit that holds every lock the log has.</para>
    /// </summary>
    private long EntryAt(byte* data, long pos, long end, bool verify, out WalkStop stop)
    {
        int headerSize = _entryHeaderSize;
        if (pos + headerSize > end) { stop = WalkStop.Short; return 0; }

        ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(data + pos);
        long total = (long)headerSize + eh.BucketCount * sizeof(long);

        if (pos + total > end)                                    { stop = WalkStop.Torn;           return 0; }
        if (eh.Generation == 0)                                   { stop = WalkStop.EndMarker;      return 0; }
        if (eh.Generation > _generation + GenerationSanityMargin) { stop = WalkStop.BadGeneration;  return 0; }
        if (eh.SeriesIndex >= SeriesIndexSanityCap)               { stop = WalkStop.BadSeriesIndex; return 0; }
        if (verify && _checksummed
            && Unsafe.ReadUnaligned<uint>(data + pos + ChecksummedHeaderBytes) != EntryChecksum(data + pos, eh.BucketCount))
                                                                  { stop = WalkStop.BadChecksum;    return 0; }
        stop = WalkStop.None;
        return total;
    }

    /// <summary>
    /// The v2 checksum of the entry at <paramref name="entry"/>, from the bytes where they sit:
    /// CRC32C over its 48 header bytes and then its <paramref name="bucketCount"/> bucket counts,
    /// which follow the CRC slot. Not the slot itself.
    /// </summary>
    private static uint EntryChecksum(byte* entry, int bucketCount)
    {
        uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(entry, ChecksummedHeaderBytes));
        return bucketCount == 0
            ? crc
            : Crc32c.Append(crc, new ReadOnlySpan<byte>(entry + EntryHeaderSize, bucketCount * sizeof(long)));
    }

    /// <summary>
    /// The capacity a log holding <paramref name="dataBytes"/> needs: what <see cref="Open"/>
    /// was asked for, climbed along <see cref="NextCapacity"/> until it fits — the ladder <see cref="GrowTo"/> climbs, so
    /// shrink and growth move along one set of sizes instead of two.
    /// </summary>
    private long FitCapacity(long dataBytes)
    {
        long capacity = _floorCapacity;
        while (capacity < dataBytes) capacity = NextCapacity(capacity);
        return capacity;
    }

    /// <summary>
    /// Shrinks the file to <paramref name="targetCapacity"/> — <see cref="GrowTo"/> run downward,
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
    /// Logs one data point, with the stored form supplied by the caller. Kept for the tests and
    /// for any caller that genuinely has one point; it is the one-element case of
    /// <see cref="Append(ReadOnlySpan{MetricIngestItem})"/> and runs the same code, so the two
    /// cannot drift.
    /// </summary>
    public void Append(MetricIngestItem item, in MetricDataPoint point)
    {
        AppendCore(MemoryMarshal.CreateReadOnlySpan(ref item, 1),
                   MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef(in point), 1),
                   default, 0);
        PreGrowIfClaimed();
    }

    /// <summary>
    /// Logs a whole ingest batch under ONE acquisition of the write lock.
    ///
    /// <para><b>The lock was taken per point, and it is the process-global one.</b> An OTLP
    /// export is 500–1 000 points and every Kestrel thread handling a POST was queueing on this
    /// monitor once per point: measured 714 ns/point on one thread and 4 091 ns/point each on
    /// eight, with aggregate throughput flat — every core past the first bought nothing. Taking
    /// it once per batch is the same work behind one uncontended acquisition, and it matches how
    /// the caller actually calls: <c>MetricStorageEngine.Ingest</c> already has the whole batch
    /// in hand.</para>
    ///
    /// <para><b>The file-header write offset is stored once, at the end.</b> That is what makes
    /// this all-or-nothing rather than a prefix: recovery never walks past the offset the header
    /// claims (<see cref="ReconcileDataEndLocked"/> only ever truncates DOWN from it), so a
    /// throw part-way through — <see cref="GrowTo"/> on a full disk is the one that can — leaves
    /// the entries written so far unclaimed and therefore unreadable, and <c>_writeOffset</c>
    /// unmoved, so the next append overwrites them. The caller's contract is unchanged and
    /// strictly cleaner: it applies NOTHING to the hot tier for a batch whose log append threw,
    /// where the per-point shape left the prefix logged and visible and the rest neither.</para>
    ///
    /// <para>The stored point of each item is derived here, through
    /// <see cref="MetricIngestItem.ToDataPoint"/> — the same one definition the hot tier uses,
    /// so the log and the tier still agree to the bit without a 40-byte struct per point being
    /// carried between them through a side array.</para>
    /// </summary>
    public void Append(ReadOnlySpan<MetricIngestItem> items)
    {
        if (items.IsEmpty) return;

        uint[] rented = ArrayPool<uint>.Shared.Rent(items.Length);
        try
        {
            long epoch = SeriesEpoch;
            for (int i = 0; i < items.Length; i++) rented[i] = ResolveSeries(items[i]);
            AppendResolved(items, rented.AsSpan(0, items.Length), epoch);
            PreGrowIfClaimed();
        }
        finally { ArrayPool<uint>.Shared.Return(rented); }
    }

    /// <summary>
    /// SERIES LOOKUP WITHOUT THE LOCK. <c>_seriesIndex</c> is a <see cref="ConcurrentDictionary{
    /// TKey,TValue}"/> and a hit needs no exclusion at all, yet it was probed from INSIDE the
    /// exclusive lock, once per point — a hash of the name, a hash of the unit, a bucket walk and
    /// a compare, all of it serialising every other ingest thread in the process. In the steady
    /// state every point hits, so a batch that arrives here pre-resolved leaves the critical
    /// section with no dictionary work and no <see cref="SeriesKey"/> construction in it: it
    /// reads an int out of a span and stores 48 bytes.
    ///
    /// <para>Exposed so the CALLER can fold it into a pass it already makes over the batch — the
    /// engine's future-skew scan. A pass of its own here re-touched every item's name, unit and
    /// label set a second time, and at OTLP batch sizes that object graph does not stay in L2.</para>
    ///
    /// <para><see cref="Unregistered"/> when the series is not in the registry yet; the append
    /// registers it under the lock.</para>
    /// </summary>
    public uint ResolveSeries(MetricIngestItem item) =>
        _seriesIndex.TryGetValue(KeyOf(item), out uint known) ? known : Unregistered;

    /// <summary>
    /// The epoch a <see cref="ResolveSeries"/> answer is only valid in. Read it BEFORE the
    /// resolutions and hand it back to <see cref="AppendResolved"/>, which re-checks it under
    /// the lock: <see cref="Compact"/> clears the registry and truncates the pool file when a
    /// commit empties the log, and re-issues indices from 0, so an index resolved before such a
    /// clear names a pool record that no longer exists and replay could not resolve the series.
    /// A bumped epoch throws the whole batch's pre-resolution away and re-registers under the
    /// lock, which is correct rather than merely rare.
    /// </summary>
    public long SeriesEpoch => Volatile.Read(ref _seriesEpoch);

    /// <summary>
    /// <see cref="Append(ReadOnlySpan{MetricIngestItem})"/> over series the caller resolved with
    /// <see cref="ResolveSeries"/>. <b>It does not grow the log ahead of need</b> — it only claims
    /// that growth (<see cref="WantsPreGrowLocked"/>); the caller runs it with
    /// <see cref="PreGrowIfClaimed"/> once it holds none of its OWN locks. The engine calls this
    /// under its snapshot read lock, and a growth run there held off the threshold flush's write
    /// lock for the length of a file extension.
    /// </summary>
    public void AppendResolved(ReadOnlySpan<MetricIngestItem> items, ReadOnlySpan<uint> resolved, long epoch) =>
        AppendCore(items, default, resolved, epoch);

    private static SeriesKey KeyOf(MetricIngestItem item) =>
        new(item.Name ?? string.Empty, item.Kind, item.Unit ?? string.Empty, item.Labels ?? LabelSet.Empty);

    /// <summary>
    /// The one append. <paramref name="points"/> empty means "derive each item's stored point";
    /// non-empty means positional, <c>points[i]</c> for <c>items[i]</c>, which is the single-point
    /// overload's contract. <paramref name="preResolved"/> empty means "resolve every series
    /// under the lock"; otherwise <see cref="Unregistered"/> marks the ones that still must be.
    /// </summary>
    private void AppendCore(ReadOnlySpan<MetricIngestItem> items, ReadOnlySpan<MetricDataPoint> points,
                            ReadOnlySpan<uint> preResolved, long resolvedAtEpoch)
    {
        if (items.IsEmpty) return;

        // THE BATCH'S SIZE AND ITS CHECKSUMS, KNOWN BEFORE THE LOCK — so the log is big enough
        // before the batch starts, growing it never happens inside the append's critical section
        // (see GrowTo), and neither does hashing it (see PrecomputeChecksums). Only a batch whose
        // series were resolved outside the lock can be hashed here: an entry's series index is
        // one of the bytes the checksum covers.
        uint[]? crcs          = null;
        ulong   crcGeneration = 0;
        if (_checksummed && !preResolved.IsEmpty)
        {
            crcs          = ArrayPool<uint>.Shared.Rent(items.Length);
            crcGeneration = Volatile.Read(ref _generation);
        }

        try
        {
            long batchBytes = PrecomputeChecksums(items, points, preResolved,
                                                  crcs is null ? default : crcs.AsSpan(0, items.Length), crcGeneration);

            // The window between the lock-free work — the series lookups, the checksums — and the
            // write lock: a commit can clear the registry in it, and a flush can open a generation.
            if (!preResolved.IsEmpty) OnSeriesResolvedForTest?.Invoke();

            while (true)
            {
                long needed;
                lock (_writeLock)
                {
                    if (_disposed) return;                      // shutdown race — dropping is correct
                    if (_ptr is null)
                        throw new InvalidOperationException(
                            "Metric WAL has no mapping; the log is not accepting appends.");

                    needed = _writeOffset + batchBytes;
                    if (needed <= _capacity)
                    {
                        WriteBatchLocked(items, points, preResolved, resolvedAtEpoch,
                                         crcs is null ? default : crcs.AsSpan(0, items.Length), crcGeneration);
                        // Claimed here, RUN by the caller once it holds no lock: see AppendResolved.
                        WantsPreGrowLocked();
                        return;
                    }
                }

                // Did not fit. Grow OUTSIDE the write lock — every other thread whose batch fits keeps
                // appending into the mapping it has meanwhile — and try again: somebody else's batch
                // may have taken the room in between, which the loop simply measures again.
                lock (_resizeLock) GrowTo(needed);
            }
        }
        finally { if (crcs is not null) ArrayPool<uint>.Shared.Return(crcs); }
    }

    /// <summary>
    /// The batch's size in log bytes, and — into <paramref name="crcs"/>, when it is not empty —
    /// each entry's checksum, computed from the point in hand rather than from the map, under
    /// <paramref name="generation"/> and the series index the caller resolved. No lock.
    ///
    /// <para><b>WHY OUTSIDE THE LOCK.</b> Every byte an entry's CRC covers is known before the
    /// write lock except two: the generation, which only <see cref="BeginFlush"/> moves, and the
    /// series index, which a series not yet registered gets only under the lock. So the hash is
    /// taken here, against the generation read now and the index resolved before, and
    /// <see cref="WriteBatchLocked"/> stores it only where both still hold — otherwise it hashes
    /// that entry from the map, under the lock. Measured on the log alone
    /// (<c>MetricWalAppendContentionProbe</c>, medians of ten runs): hashing every entry from the
    /// map under the lock cost ~20 ns a point per thread at one and two threads (141 and 151 ns
    /// against 121 and 134 without a checksum); hashing here, nothing the runs could separate
    /// from none (117 and 127). Nothing about the entry's POSITION is in the checksum, so neither
    /// a commit relocating the tail nor a growth swapping the mapping in the window can make a
    /// precomputed value wrong.</para>
    ///
    /// <para><b>What makes "from the point in hand" the same bytes as the map.</b> The locked
    /// write derives the point through the same <see cref="MetricIngestItem.ToDataPoint"/>, copies
    /// the same bucket array, and writes <see cref="MetricWalEntryHeader.Reserved"/> as the zero
    /// this struct carries. The buckets are the caller's, and a caller that mutated them between
    /// the two would get an entry the next replay stops at; a Debug build checks every precomputed
    /// value against the map as it is stored.</para>
    /// </summary>
    private long PrecomputeChecksums(ReadOnlySpan<MetricIngestItem> items, ReadOnlySpan<MetricDataPoint> points,
                                     ReadOnlySpan<uint> preResolved, Span<uint> crcs, ulong generation)
    {
        int  headerSize = _entryHeaderSize;
        long batchBytes = 0;
        for (int i = 0; i < items.Length; i++)
        {
            if (crcs.IsEmpty || preResolved[i] == Unregistered)
            {
                long[]? b = points.IsEmpty ? items[i].BucketCounts : points[i].BucketCounts;
                batchBytes += headerSize + (b is null ? 0 : Math.Min(b.Length, MaxBucketCounts)) * sizeof(long);
                continue;
            }

            var point       = points.IsEmpty ? items[i].ToDataPoint() : points[i];
            int bucketCount = point.BucketCounts is null ? 0 : Math.Min(point.BucketCounts.Length, MaxBucketCounts);
            batchBytes     += headerSize + bucketCount * sizeof(long);

            var h = new MetricWalEntryHeader
            {
                Generation        = generation,
                SeriesIndex       = preResolved[i],
                TimestampUnixNano = point.TimestampUnixNano,
                Value             = point.Value,
                Count             = point.Count,
                Sum               = point.Sum,
                BucketCount       = (ushort)bucketCount,
            };
            uint crc = Crc32c.Append(0, MemoryMarshal.AsBytes(new ReadOnlySpan<MetricWalEntryHeader>(in h)));
            if (bucketCount > 0)
                crc = Crc32c.Append(crc, MemoryMarshal.AsBytes(new ReadOnlySpan<long>(point.BucketCounts, 0, bucketCount)));
            crcs[i] = crc;
        }
        return batchBytes;
    }

    /// <summary>
    /// Test seam fired under the write lock once every entry of a batch — its fields, its buckets
    /// and its checksum — is in the map, and BEFORE the file header's write offset claims them,
    /// with the batch's end offset. A process that dies here leaves nothing a replay reads. Null
    /// in production.
    /// </summary>
    internal Action<long>? OnBatchWrittenForTest;

    /// <summary>
    /// Writes a batch the caller has checked fits. Caller holds <c>_writeLock</c>. Nothing in here
    /// can resize the mapping, so <c>_ptr</c> is taken once per batch off a view that stays put.
    ///
    /// <para><b>The order of the stores is the crash contract.</b> Per entry: its fields and
    /// buckets, then its checksum — computed from the bytes now in the map unless
    /// <see cref="PrecomputeChecksums"/> already has it for exactly these bytes. Per batch: every
    /// entry, then the file header's write offset. So the header never claims an entry whose
    /// checksum is not already down, in program order — which is what a process death sees.
    /// Across a power loss the pages go in any order, and the checksum is what tells a claimed
    /// entry that did not land from one that did.</para>
    /// </summary>
    private void WriteBatchLocked(ReadOnlySpan<MetricIngestItem> items, ReadOnlySpan<MetricDataPoint> points,
                                  ReadOnlySpan<uint> preResolved, long resolvedAtEpoch,
                                  ReadOnlySpan<uint> crcs, ulong crcGeneration)
    {
        // Block kept from the lock body this was lifted out of, so the diff stays reviewable.
        {
            // The running end of data, committed to the field and the header only once the whole
            // batch is down. See the remarks above for why the partial state must stay unclaimed.
            long offset     = _writeOffset;
            int  headerSize = _entryHeaderSize;
            byte* data      = _ptr + FileHeaderSize;

            // Did the registry turn over between the caller's lock-free lookups and this lock?
            bool staleResolution = preResolved.IsEmpty || _seriesEpoch != resolvedAtEpoch;

            // The checksums computed before the lock are this batch's only if what they were
            // computed over is what lands: the same series indices (not stale) and the same
            // generation (a BeginFlush in the window stamps the next one).
            bool precomputed = !crcs.IsEmpty && !staleResolution && crcGeneration == _generation;

            for (int i = 0; i < items.Length; i++)
            {
                var item  = items[i];
                var point = points.IsEmpty ? item.ToDataPoint() : points[i];

                long[]? buckets = point.BucketCounts;
                int bucketCount = buckets is null ? 0 : Math.Min(buckets.Length, MaxBucketCounts);
                int entrySize   = headerSize + bucketCount * sizeof(long);

                bool resolved  = !staleResolution && preResolved[i] != Unregistered;
                uint seriesIdx = resolved
                    ? preResolved[i]
                    : RegisterSeriesLocked(KeyOf(item), item.BucketBounds);

                byte* dest = data + offset;

                ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(dest);
                eh.Generation        = _generation;
                eh.SeriesIndex       = seriesIdx;
                eh.TimestampUnixNano = point.TimestampUnixNano;
                eh.Value             = point.Value;
                eh.Count             = point.Count;
                eh.Sum               = point.Sum;
                eh.BucketCount       = (ushort)bucketCount;
                eh.Reserved          = 0;

                if (bucketCount > 0)
                    buckets.AsSpan(0, bucketCount)
                           .CopyTo(new Span<long>(dest + headerSize, bucketCount));

                // THE CHECKSUM LAST.
                if (_checksummed)
                {
                    uint crc = precomputed && resolved ? crcs[i] : EntryChecksum(dest, bucketCount);
                    Debug.Assert(crc == EntryChecksum(dest, bucketCount),
                                 "a checksum computed before the lock does not match the bytes it was stored for");
                    Unsafe.WriteUnaligned(dest + ChecksummedHeaderBytes, crc);
                }

                offset += entrySize;
            }

            OnBatchWrittenForTest?.Invoke(offset);

            _writeOffset = offset;
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

    /// <summary>
    /// The top byte of a v2 pool record's length field. A v1 length never reaches it — records
    /// over <see cref="MaxPoolRecordBytes"/> are refused as torn by every reader — so each record
    /// says which shape it is, one file can hold both, and a reader never takes the pool's version
    /// from the log beside it: the two files reach the disk independently.
    /// </summary>
    private const uint PoolRecordTagV2   = 0xC5u << 24;
    private const uint PoolLengthMask    = 0x00FF_FFFF;
    private const int  PoolHeadV1        = 8;                  // index, length
    private const int  PoolHeadV2        = PoolHeadV1 + 4;     // index, tagged length, crc
    private const int  MaxPoolRecordBytes = 8 * 1024 * 1024;

    /// <summary>
    /// Appends the record for a new series. A checksummed log writes the v2 shape — the CRC32C
    /// over the index, the tagged length and the body sits after the length — and a log that is
    /// still v1 (see <see cref="Open"/>) writes the v1 shape, so that the release a rollback goes
    /// back to can still read every record of the log it can still read.
    /// </summary>
    private void WritePoolRecord(uint index, SeriesKey key, double[]? bounds)
    {
        if (_poolStream is null) return;

        var body = new ArrayBufferWriterLite();
        body.WriteByte((byte)key.Kind);
        body.WriteString(key.Name);
        body.WriteString(key.Unit);

        var labels = key.Labels;
        body.WriteUInt16((ushort)Math.Min(labels.Count, ushort.MaxValue));
        for (int i = 0; i < labels.Count && i < ushort.MaxValue; i++)
        {
            body.WriteString(labels.KeyAt(i));
            body.WriteString(labels.ValueAt(i));
        }

        int boundsLen = bounds is null ? 0 : Math.Min(bounds.Length, ushort.MaxValue);
        body.WriteUInt16((ushort)boundsLen);
        for (int i = 0; i < boundsLen; i++) body.WriteDouble(bounds![i]);

        Span<byte> head = stackalloc byte[PoolHeadV2];
        BinaryPrimitives.WriteUInt32LittleEndian(head, index);
        if (_checksummed)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(head[4..], PoolRecordTagV2 | ((uint)body.Length & PoolLengthMask));
            BinaryPrimitives.WriteUInt32LittleEndian(head[PoolHeadV1..],
                Crc32c.Append(Crc32c.Append(0, head[..PoolHeadV1]), body.Written));
            _poolStream.Write(head);
        }
        else
        {
            BinaryPrimitives.WriteUInt32LittleEndian(head[4..], (uint)body.Length);
            _poolStream.Write(head[..PoolHeadV1]);
        }
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
    /// stamp the new generation into (see <see cref="GrowTo"/>). Throwing happens BEFORE the
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
            // is why this was survivable in practice. _ptr null WITHOUT _disposed is not: a resize
            // that unmaps first (the shrink; growth used to, see GrowTo), if the resize and the restoring re-map both fail
            // (a full disk) leaves the object alive with no mapping
            // and no disposal, for good. Append throws from there, so ingest fails honestly
            // while a flush would have kept writing files nobody ever commits.
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_ptr is null)
                throw new InvalidOperationException(
                    "Metric WAL has no mapping; no generation can be opened. A resize could neither " +
                    "change the file nor restore the previous mapping, and appends are already " +
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
    /// loses its mapping while the files are being written (see <see cref="GrowTo"/>), and the
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
        lock (_resizeLock)   // the commit may shrink, i.e. replace the mapping; see _resizeLock
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
            // be reached (dispose or a failed shrink-and-restore between BeginFlush and here), but a log in
            // either state refuses the next BeginFlush on its own account and can never leave it
            // — growth never unmaps before it has a new mapping — so a stuck flag has
            // nothing left to wedge.
            //
            // It stays above them anyway, and is worth a paragraph rather than a shrug, because
            // what it costs is one statement's position and what it buys is that the flag's
            // lifetime does not depend on either of those arguments continuing to hold. A change
            // to the crossed-header repair, or a caller that commits a generation it was not
            // handed, makes the first reachable; and BeginFlush throwing forever is a failure
            // both flush paths merely LOG — the periodic loop catches it at Error once a tick,
            // the threshold continuation once per crossing — while no .mts is written again and
            // the log grows until GrowTo throws into ingest.
            _openFlush = 0;

            // Nothing left to reclaim: the watermark already names this generation or a later
            // one. Unreachable from here today (see above) and kept as the definition of what
            // the answer means rather than as a live branch: a generation the watermark covers
            // is committed, whoever put it there. Committed, and truthfully so — the caller's
            // points are covered.
            if (flushedGeneration <= _committedGeneration) return MetricWalCommit.Committed;

            // The log cannot record anything: closed, or left without a mapping by a resize that
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
        long pos = 0, total;
        // Every stop EntryAt makes is end-of-data here. The one that matters most: a generation
        // FAR above the header counter cannot have been stamped by any append. It is a
        // torn/corrupt entry, and everything past it parses off garbage lengths: treat it as
        // end-of-data so it truncates away. Without this, one such entry (a real incident: a torn
        // first entry decoding to generation ~155e9) is forever "above the watermark" — never
        // compacted, the log never empties, and the mmap doubles without bound (8 GiB observed).
        // The margin, not a strict `> _generation`: nothing msyncs this log, so after power loss
        // the header page can LAG the data pages — compaction relocates gen-(G+1) survivors below
        // offsets an older header snapshot (Generation = G) already covers, and a strict guard
        // would truncate that legitimate replay to nothing. Torn entries decode to effectively
        // random 64-bit values, which the margin still rejects. No checksum here: see EntryAt.
        while ((total = EntryAt(data, pos, _writeOffset, verify: false, out _)) > 0)
        {
            if (Unsafe.AsRef<MetricWalEntryHeader>(data + pos).Generation > committed) { firstSurvivor = pos; break; }
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
        // marker means the log is at capacity, where the next append grows it anyway.) The v2
        // checksum does not make the marker redundant: the originals are the very bytes that
        // were moved, and they verify exactly as well as their copies do.
        if (_writeOffset + _entryHeaderSize <= _capacity)
            Unsafe.AsRef<MetricWalEntryHeader>(data + _writeOffset).Generation = 0;

        Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + _writeOffset;

        // The pool is only reclaimable once nothing references it. Survivors still carry
        // their series indices, so it is truncated on the flushes that empty the log — which
        // is the normal case — and simply kept otherwise, bounded by the series cardinality.
        if (_writeOffset == 0)
        {
            _seriesIndex.Clear();
            _nextSeriesIndex = 0;

            // Every index any thread resolved without the lock is void from here: the pool
            // records behind them are about to be truncated and the next registration re-issues
            // index 0. See _seriesEpoch.
            Volatile.Write(ref _seriesEpoch, _seriesEpoch + 1);
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

            long end = _writeOffset;

            // Two passes over the same entries, stepped by the same stride so they stop at the same
            // place: the first names the series a surviving entry references, and only those are
            // read out of the pool — see LoadPool.
            var referenced = new HashSet<uint>();
            for (long pos = 0, total; (total = ReplayStrideLocked(pos, end)) > 0; pos += total)
            {
                ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(_ptr + FileHeaderSize + pos);
                if (eh.Generation > _committedGeneration) referenced.Add(eh.SeriesIndex);
            }

            var pool = LoadPool(referenced);

            for (long pos = 0, total; (total = ReplayStrideLocked(pos, end)) > 0; pos += total)
            {
                byte* src = _ptr + FileHeaderSize + pos;
                ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(src);

                if (eh.Generation > _committedGeneration)
                {
                    if (pool.TryGetValue(eh.SeriesIndex, out var series))
                    {
                        long[]? buckets = null;
                        if (eh.BucketCount > 0)
                        {
                            buckets = new long[eh.BucketCount];
                            new ReadOnlySpan<long>(src + _entryHeaderSize, eh.BucketCount).CopyTo(buckets);
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
            }
        }

        return result;
    }

    /// <summary>
    /// The replay's stride: the size of the complete entry at <paramref name="pos"/>, or 0 where
    /// the real data ends before <paramref name="end"/>. Both passes of <see cref="ReadAll"/> step
    /// with it, so the pass that decides which series to load and the pass that replays them
    /// cannot disagree about which entries exist. Caller holds the lock.
    ///
    /// <para>It is <see cref="EntryAt"/> WITH the checksum: an entry that does not verify ends the
    /// replay, so its fields — garbage, in the torn write this exists for — never reach the hot
    /// tier, from where a flush would write them into a permanent file. The same holds for a
    /// generation no append could have stamped (margin semantics: see Compact). The open-time
    /// walk has already cut the log at the first such entry, so on the engine's one call, right
    /// after <see cref="Open"/>, this stops exactly where the data ends; it checks again because
    /// replay is the step whose output becomes points.</para>
    /// </summary>
    private long ReplayStrideLocked(long pos, long end) =>
        EntryAt(_ptr + FileHeaderSize, pos, end, verify: true, out _);

    private readonly record struct PoolEntry(
        string Name, MetricKind Kind, string Unit, LabelSet Labels, double[]? Bounds);

    /// <summary>
    /// The series <paramref name="referenced"/> names, read out of the companion pool. Later
    /// records win for a given index, which is what makes a reset that failed to truncate the
    /// file harmless.
    ///
    /// <para><b>ONLY WHAT A SURVIVING ENTRY REFERENCES IS INTERNED.</b> The replay goes through
    /// <see cref="Interner"/> — <see cref="MetricLabelInterner.Shared"/>, which holds 16 384
    /// strings for the life of the process and never evicts — so a replayed series holds the very
    /// strings and label set the live path builds for it. But the pool is truncated only when a
    /// commit empties the log or an empty log is opened, so after a crash or an OOM kill it holds
    /// every series registered since the log was last empty: under continuous ingest, the whole
    /// previous run's, churned pod and container ids included. Interning every record filled the
    /// shared pool at boot with dead values, and every series started after the restart was then
    /// ingested at the uninterned cost for the life of the process — issue #88's failure mode, the
    /// one WP7 removed from <c>MetricReader</c>. A record no surviving entry references is not
    /// decoded at all, because nothing reads it: the replay resolves only referenced indices, and
    /// the seeding in <see cref="OpenOrCreate"/> needs only the indices, which the walk reads from
    /// the record heads.</para>
    ///
    /// <para>Chosen over rewriting the pool down to the referenced records at open: the same boot,
    /// and no second file to write, fsync and swap under a log that is mid-recovery. The dead
    /// records stay in the file until the next commit that empties the log truncates it, as they
    /// always did.</para>
    /// </summary>
    private Dictionary<uint, PoolEntry> LoadPool(HashSet<uint> referenced)
    {
        var bodies = ReadPoolRecords(referenced, out _, out _);
        var map    = new Dictionary<uint, PoolEntry>(bodies.Count);
        var interner = Interner;
        foreach (var (index, body) in bodies)
            map[index] = DecodePoolRecord(body, interner);
        return map;
    }

    /// <summary>
    /// Through the same interner the OTLP parsers use, so a replayed series holds the very strings
    /// — and, when they are all pooled, the very label set — that the live path builds for it: its
    /// SeriesKey then matches the next live point by reference, and the process does not keep a
    /// second copy of every label.
    /// </summary>
    private static PoolEntry DecodePoolRecord(byte[] body, MetricLabelInterner interner)
    {
        var r = new SpanCursor(body);
        var kind = (MetricKind)r.ReadByte();
        string name = r.ReadString(interner, out _);
        string unit = r.ReadString(interner, out _);

        int labelCount = r.ReadUInt16();
        var kv  = new string[labelCount * 2];
        var ids = new int[labelCount * 2];
        for (int i = 0; i < kv.Length; i++)
            kv[i] = r.ReadString(interner, out ids[i]);

        int boundsLen = r.ReadUInt16();
        double[]? bounds = null;
        if (boundsLen > 0)
        {
            bounds = new double[boundsLen];
            for (int i = 0; i < boundsLen; i++) bounds[i] = r.ReadDouble();
        }

        return new PoolEntry(name, kind, unit, interner.GetLabelSet(kv, ids), bounds);
    }

    /// <summary>
    /// Walks the companion pool's records: the body of the LAST record of each index in
    /// <paramref name="keep"/> comes back undecoded, and every other body is stepped over unread
    /// (<paramref name="keep"/> null: all of them). Also reports <paramref name="indexCeiling"/>,
    /// one past the largest index of any record that parsed whole, and where the clean records
    /// stop: <paramref name="cleanEnd"/> is the offset just past the LAST record that parsed whole,
    /// which is 0 for a pool whose very first record is torn and the file length for an intact
    /// one. The loop already stops at a torn head or body; this only says WHERE it stopped, so
    /// that <see cref="OpenOrCreate"/> can append the next record at that boundary instead of
    /// after the torn bytes — appending past them leaves every later record misaligned by the
    /// width of the garbage, which breaks pool parsing for every series registered afterwards,
    /// not just the one that was lost.
    ///
    /// <para><b>A v2 record that does not verify ends the walk like a torn one</b>, and is read
    /// whole to find out — kept or not, because its checksum covers the body. So a record whose
    /// length landed and whose body did not is never decoded into a series with a garbage name and
    /// labels that the points referencing it would then replay under; they come back as
    /// unresolved instead, which is honest. A v1 record (see <see cref="PoolRecordTagV2"/>) has
    /// only the length check it always had, and its body is still stepped over unread.</para>
    /// </summary>
    private Dictionary<uint, byte[]> ReadPoolRecords(HashSet<uint>? keep, out long cleanEnd, out ulong indexCeiling)
    {
        var bodies = new Dictionary<uint, byte[]>();
        cleanEnd     = 0;
        indexCeiling = 0;
        if (!File.Exists(_poolPath)) return bodies;

        byte[]? scratch = null;
        try
        {
            _poolStream?.Flush();
            using var fs = new FileStream(_poolPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            long length = fs.Length;
            Span<byte> head = stackalloc byte[PoolHeadV2];

            // ReadAtLeast/ReadExactly, never a bare Read: Stream.Read may legally return fewer
            // bytes than asked for reasons that are not end-of-file, and this loop's stop is no
            // longer just "give up on the map" — OpenOrCreate TRUNCATES the pool to where it
            // stops. A bare Read turned one under-filled buffer on a slow volume into a
            // permanent cut through real records. Only an actual end-of-stream ends the walk.
            while (fs.ReadAtLeast(head[..PoolHeadV1], PoolHeadV1, throwOnEndOfStream: false) == PoolHeadV1)
            {
                uint index    = BinaryPrimitives.ReadUInt32LittleEndian(head);
                uint lenField = BinaryPrimitives.ReadUInt32LittleEndian(head[4..]);
                bool v2       = (lenField & ~PoolLengthMask) == PoolRecordTagV2;
                uint len      = v2 ? lenField & PoolLengthMask : lenField;
                if (len == 0 || len > MaxPoolRecordBytes) break;             // torn or bogus record
                if (v2 && fs.ReadAtLeast(head[PoolHeadV1..], 4, throwOnEndOfStream: false) != 4) break;
                if (len > length - fs.Position) break;                       // truncated tail

                bool kept = keep is not null && keep.Contains(index);
                if (kept || v2)
                {
                    byte[] body;
                    if (kept) body = new byte[len];
                    else
                    {
                        if (scratch is null || scratch.Length < len)
                        {
                            if (scratch is not null) ArrayPool<byte>.Shared.Return(scratch);
                            scratch = ArrayPool<byte>.Shared.Rent((int)len);
                        }
                        body = scratch;
                    }

                    try { fs.ReadExactly(body, 0, (int)len); }
                    catch (EndOfStreamException) { break; }                  // genuinely truncated tail

                    if (v2 && BinaryPrimitives.ReadUInt32LittleEndian(head[PoolHeadV1..])
                              != Crc32c.Append(Crc32c.Append(0, head[..PoolHeadV1]), body.AsSpan(0, (int)len)))
                        break;                                               // torn: the checksum says so

                    if (kept) bodies[index] = body;                          // later records win
                }
                else
                    fs.Seek(len, SeekOrigin.Current);                        // stepped over, not read

                if (index + 1UL > indexCeiling) indexCeiling = index + 1UL;
                cleanEnd = fs.Position;   // this record parsed whole; the boundary is here
            }
        }
        catch { /* best-effort: whatever resolved stays usable, the rest is reported */ }
        finally { if (scratch is not null) ArrayPool<byte>.Shared.Return(scratch); }

        return bodies;
    }

    // ── Grow ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Where growth stops doubling and starts adding. Doubling a 64 MiB log to 128 MiB, then 256,
    /// 512 … reserves as much disk again as the log holds, on exactly the host whose flushes have
    /// fallen behind; past this size the log grows by this much at a time instead.
    /// </summary>
    internal const long GrowthStepBytes = 64L * 1024 * 1024;

    /// <summary>The growth ladder: doubling up to <see cref="GrowthStepBytes"/>, then linear.</summary>
    internal static long NextCapacity(long capacity) =>
        capacity < GrowthStepBytes ? capacity * 2 : capacity + GrowthStepBytes;

    /// <summary>
    /// Serialises everything that REPLACES the mapping — <see cref="GrowTo"/>, the commit-time
    /// shrink (<see cref="CommitFlush"/>) and <see cref="Dispose"/> — WITHOUT holding
    /// <c>_writeLock</c> while the new mapping is built. Lock order: this, then
    /// <c>_writeLock</c>; nothing holding <c>_writeLock</c> ever waits for this.
    /// </summary>
    private readonly Lock _resizeLock = new();

    /// <summary>
    /// <see cref="PreGrowNone"/>, <see cref="PreGrowClaimed"/> (an append crossed the mark, under
    /// <c>_writeLock</c>) or <see cref="PreGrowRunning"/> (one caller took the claim in
    /// <see cref="PreGrowIfClaimed"/> and is running it). Only that caller clears it, and no new
    /// claim is made until it has. See <see cref="WantsPreGrowLocked"/>.
    /// </summary>
    private int _preGrowQueued;

    private const int PreGrowNone    = 0;
    private const int PreGrowClaimed = 1;
    private const int PreGrowRunning = 2;

    /// <summary>
    /// The capacity a pre-grow last FAILED at, so a full disk is tried once per
    /// capacity rather than once per append; the synchronous path still tries, and throws, when a
    /// batch genuinely does not fit. -1 = none. Under <c>_writeLock</c>.
    /// </summary>
    private long _preGrowFailedAt = -1;

    /// <summary>
    /// Test seam fired by <see cref="GrowTo"/> with the new mapping built and the old one still in
    /// use — the window in which other appends must keep going. Null in production.
    /// </summary>
    internal Action? OnGrowMappedForTest;

    /// <summary>
    /// Test seam fired on the calling thread as it enters the pre-grow, before
    /// <c>_resizeLock</c> — i.e. by the one call that owns a claimed growth, and by nobody else.
    /// Null in production.
    /// </summary>
    internal Action? OnPreGrowForTest;

    /// <summary>
    /// Grows the mapped capacity to at least <paramref name="needed"/> bytes of data. <b>Caller
    /// holds <c>_resizeLock</c> and NOT <c>_writeLock</c>.</b>
    ///
    /// <para><b>It used to run inside the append's <c>lock (_writeLock)</c></b> and unmap, resize
    /// and re-map there, so every ingest thread in the process queued behind one file resize —
    /// measured ~35 ms per growth for every thread appending at the time
    /// (<c>MetricWalGrowthProbe</c>). Now the new, larger mapping is built beside the old one,
    /// outside the write lock — two mappings of one file are coherent — and the write lock is
    /// held only to swap the pointer. Every entry is written under the write lock through
    /// <c>_ptr</c>, so after the swap nothing can still be using the old view — which is RETIRED,
    /// not unmapped, because unmapping a big dirty view stalls every thread's page faults; see
    /// <see cref="_retired"/>.</para>
    ///
    /// <para><b>A failure leaves the log as it was.</b> The old mapping is never unmapped until a
    /// new one exists, so a full disk (the moment a log grows) now fails the append that needed
    /// the room and nothing else — where the unmap-first shape could lose the mapping altogether
    /// and leave the log refusing every later append. <see cref="BeforeResize"/> fires with the
    /// target size before anything is touched.</para>
    /// </summary>
    private void GrowTo(long needed)
    {
        long capacity;
        lock (_writeLock)
        {
            if (_disposed || _ptr is null) return;         // the retry will say why, honestly
            capacity = _capacity;
        }
        if (capacity >= needed) return;                     // somebody else grew it meanwhile

        long target = capacity;
        while (target < needed) target = NextCapacity(target);
        long newFileSize = FileHeaderSize + target;

        MemoryMappedFile?         mmf  = null;
        MemoryMappedViewAccessor? view = null;
        bool                      acquired = false;
        try
        {
            BeforeResize?.Invoke(newFileSize);

            // A capacity past the file's length extends the file (both platforms) — with the old
            // mapping still in place, which an explicit SetLength could not be sure of on Windows.
            mmf  = MemoryMappedFile.CreateFromFile(_file!, null, newFileSize, MemoryMappedFileAccess.ReadWrite,
                                                   HandleInheritability.None, leaveOpen: true);
            view = mmf.CreateViewAccessor(0, newFileSize, MemoryMappedFileAccess.ReadWrite);
            byte* ptr = null;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            acquired = true;

            OnGrowMappedForTest?.Invoke();

            lock (_writeLock)
            {
                if (_disposed) return;                      // finally releases the new mapping

                // The old mapping is RETIRED, not released: see _retired for what unmapping it
                // here cost every other thread.
                if (_mmf is not null && _accessor is not null)
                    (_retired ??= []).Add((_mmf, _accessor));
                _mmf      = mmf;
                _accessor = view;
                _ptr      = ptr;
                _capacity = target;
                mmf  = null;                                // owned by the log now
                view = null;
            }
        }
        finally
        {
            if (view is not null)
            {
                if (acquired) try { view.SafeMemoryMappedViewHandle.ReleasePointer(); } catch { }
                view.Dispose();
            }
            mmf?.Dispose();
        }
    }

    /// <summary>
    /// A quarter of the log left: CLAIM a growth, to be run while there is still room for the
    /// appends that arrive meanwhile — so under steady ingest ONE call pays for a growth, after
    /// its own batch is down, and every other thread keeps appending into the quarter that is
    /// left. Caller holds <c>_writeLock</c>. The claim is taken and run by
    /// <see cref="PreGrowIfClaimed"/>, which the public appends call on their way out and the
    /// engine calls once it has left its snapshot read lock; see there for why "one" needed the
    /// claim to be taken rather than just read.
    ///
    /// <para>On the caller's thread and outside every lock, deliberately. On the thread pool it
    /// was background work racing the log's disposal and competing with the engine's own
    /// threshold flush for pool threads, and it made the file's size after a burst depend on
    /// scheduling; under the engine's snapshot read lock it would hold the flush's write lock off
    /// for the length of a file extension. What it does NOT change: on a CPU-starved box a
    /// single-threaded ingest loop that never blocks can outrun the scheduled flush, and a growth
    /// is one of the few places such a loop used to block — by accident of the log's size, never
    /// as a bound. MetricBudgetWiringTests' histogram fact read the point at which the tier
    /// drained and leaned on that accident; it now stops at the crossing the engine reports
    /// (<c>MetricStorageEngine.OnThresholdFlushScheduledForTest</c>).</para>
    /// </summary>
    private void WantsPreGrowLocked()
    {
        long cap = _capacity;
        if (cap - _writeOffset >= cap / 4 || cap == _preGrowFailedAt || _preGrowQueued != PreGrowNone) return;
        _preGrowQueued = PreGrowClaimed;
    }

    /// <summary>
    /// Runs a growth an append claimed (<see cref="WantsPreGrowLocked"/>), if there is one and
    /// nobody has taken it yet. Call holding NO lock of your own. A failure is logged, once per
    /// capacity, and never thrown: the appends that claimed it are already down.
    ///
    /// <para><b>Taken, not merely seen.</b> This read "a growth is wanted" and went to run it, so
    /// while one call was inside the growth — holding <c>_resizeLock</c> for the length of a file
    /// extension — every other append on its way out read the same flag, entered the pre-grow
    /// and queued on that lock, only to find the room already made. The call that crossed the
    /// mark was meant to be the only one that paid; every call that finished during the growth
    /// paid too. Now the claim moves 1 → 2 by compare-exchange and only the call that moved it
    /// runs the growth and clears it; the rest read 2, or lose the exchange, and walk past.
    /// Which call wins is whichever gets here first after the claim — normally the claimant
    /// itself, and exactly one either way.</para>
    /// </summary>
    public void PreGrowIfClaimed()
    {
        if (Volatile.Read(ref _preGrowQueued) == PreGrowClaimed
            && Interlocked.CompareExchange(ref _preGrowQueued, PreGrowRunning, PreGrowClaimed) == PreGrowClaimed)
            PreGrow();
    }

    private void PreGrow()
    {
        try
        {
            OnPreGrowForTest?.Invoke();
            lock (_resizeLock)
            {
                long needed;
                lock (_writeLock)
                {
                    if (_disposed || _ptr is null) return;
                    if (_capacity - _writeOffset >= _capacity / 4) return;   // a commit emptied it
                    needed = _capacity + 1;                                   // one rung up
                }

                try { GrowTo(needed); }
                catch (Exception ex)
                {
                    lock (_writeLock) _preGrowFailedAt = _capacity;
                    _logger?.LogWarning(ex,
                        "Metric WAL could not grow ahead of need; appends that do not fit will retry the growth " +
                        "themselves and fail if it still cannot be done.");
                }
            }
        }
        finally { Volatile.Write(ref _preGrowQueued, PreGrowNone); }   // the owner's to clear, and only the owner runs this
    }

    /// <summary>
    /// Test hook: drops the mapping the way a failed shrink-and-restore does, leaving the log
    /// alive and refusing appends. Growth can no longer produce that state (see
    /// <see cref="GrowTo"/>), and the dead-log paths it exercises still exist.
    /// </summary>
    internal void LoseMappingForTest()
    {
        lock (_resizeLock)
        lock (_writeLock)
            Unmap();
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
        ReleaseRetired();
    }

    /// <summary>
    /// The mappings <see cref="GrowTo"/> swapped out, still mapped. Unmapping a large dirty view
    /// is the expensive half of a growth — measured 3 ms at 16 MiB rising to 27 ms at 192 MiB —
    /// and it stalls every OTHER thread's page faults in the process for its length, the new
    /// view's included, so doing it at the swap put the whole stall back. They cost address
    /// space and page tables only (their pages ARE the new view's pages: one file), and they go
    /// in <see cref="Unmap"/> — at the commit that empties and shrinks the log, which already
    /// stops ingest to remap, or at disposal. Under <c>_writeLock</c>.
    /// </summary>
    private List<(MemoryMappedFile Mmf, MemoryMappedViewAccessor View)>? _retired;

    private void ReleaseRetired()
    {
        if (_retired is null) return;
        foreach (var (mmf, view) in _retired)
        {
            try { view.SafeMemoryMappedViewHandle.ReleasePointer(); } catch { }
            view.Dispose();
            mmf.Dispose();
        }
        _retired = null;
    }

    // ── Dispose ──────────────────────────────────────────────────────────────

    public void Dispose()
    {
        lock (_resizeLock)   // waits out a growth in flight, which is using the file handle
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

        /// <summary>The string, resolved through <paramref name="interner"/> (which decodes
        /// exactly as <c>Encoding.UTF8.GetString</c> did); <paramref name="id"/> is its interner id.</summary>
        public string ReadString(MetricLabelInterner interner, out int id)
        {
            int n = ReadUInt16();
            if (n == 0 || _pos + n > _data.Length)
            {
                _pos = Math.Min(_pos + n, _data.Length);
                id   = MetricLabelInterner.EmptyStringId;
                return string.Empty;
            }
            id = interner.Intern(_data.Slice(_pos, n), out string s);
            _pos += n;
            return s;
        }
    }
}
