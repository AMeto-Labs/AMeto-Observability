using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.IO.MemoryMappedFiles;
using Microsoft.Extensions.Logging;
using Ameto.Core;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Write-Ahead Log for the span hot tier, backed by a memory-mapped file.
/// Mirrors <c>Ameto.Storage.WriteAheadLog</c> (logs tier) in structure and intent.
///
/// <para>Why it exists: durability for the hot tier used to be provided by writing a
/// <c>.trc</c> segment every 30 seconds regardless of how few spans had arrived. On a
/// low-traffic instance that produced a full segment — sort, LZ4-HC of the blocks and the
/// trace index, four index structures, plus a <c>.stats</c> sidecar — for a handful of
/// spans, roughly 5 800 files a day, which the hourly compaction then rewrote over and
/// over because a merged file stays under the compaction threshold for hours. Appending
/// to this log is a single span copy into an mmap page, so the segment write is now free
/// to wait until a batch is actually worth a file.</para>
///
/// Format (v3):
/// <code>
///   [File Header — 64 bytes; v1's and v2's were the first 32 of them]
///     0   Magic              uint32  "RDSW"
///     4   Version            uint16  3   (2 and 1 are still READ: see <see cref="Open(string, long, ILogger)"/>)
///     6   _pad               uint16
///     8   WriteOffset        int64   next byte to write (absolute, includes this header)
///    16   Generation         uint32  flush generation, see the crash-recovery note below
///    20   _reserved          uint32 + int64
///    32   MoveFrom           int64   the relocation record: see <see cref="RelocateLocked"/>
///    40   MoveLength         int64   0 = no relocation in flight
///    48   MoveDone           int64
///    56   Crc                uint32  CRC32C over bytes [0, 8) and [16, 56): all but the claim
///    60   PendingCrc         uint32  the same, of the state a store in progress is writing
///
///   [Entry 0 …]  — unchanged since v2
///     [Entry Header — 64 bytes, Pack = 1, carries the generation it was written under]
///     [Crc               — uint32, CRC32C over the 64 header bytes + name + service + attrs]
///     [Name UTF-8][ServiceName UTF-8][Attributes msgpack]
/// </code>
///
/// <para><b>v2 was v1 plus a checksum per entry.</b> v1 had none anywhere: the bounds check could
/// only say that the declared lengths FIT, so a torn append — pages of an mmap reaching the disk in
/// whatever order the OS picks — replayed as a span with garbage name, service and attribute bytes
/// straight into the hot tier. That is the shape the metrics WAL was poisoned by on the stand, on
/// the third signal. The CRC is stored after the header and written LAST, over the bytes as they
/// sit in the map, and <see cref="ReadAll"/> stops at the first entry that does not verify —
/// exactly the rule the logs WAL (v4) follows.</para>
///
/// <para><b>v3 is v2 with a header long enough to record a commit's relocation in flight (#103).</b>
/// <see cref="CommitFlush"/> moves the spans appended during a flush to the front of the log. v2 did
/// it with one <c>Buffer.MemoryCopy</c>, and a process killed inside that copy left the copies of
/// the tail up to the copy front, one entry torn by the front, and the untouched originals behind
/// it — under the old header, whose claim still covered all of it. The replay read the copies,
/// stopped at the torn entry (v2's checksum made that certain), and truncated the log there: the
/// originals behind the front — spans acknowledged to the exporter and in no segment — were never
/// replayed again and were overwritten by the next appends. The move is now recorded in the header
/// before its first byte, made in chunks that a redo can repeat exactly, and finished by the next
/// open before anything is replayed (<see cref="RelocateLocked"/>,
/// <see cref="FinishRelocationLocked"/>). The entries did not change: an upgraded v2 log is its
/// entries, byte for byte, behind a longer header.</para>
///
/// <para><b>The v3 header carries its own checksum.</b> One field of it can hide every entry without
/// any entry being wrong: a rotted or copied <see cref="WalFileHeader.Generation"/> that is neither
/// the entries' generation nor its predecessor makes the replay skip them all, silently — and a
/// rotted record would move bytes. A header that does not verify has its generation rebuilt from
/// the entries, which carry checksums of their own, and its record is acted on only in a state a
/// commit can leave (<see cref="SealHeaderLocked"/>, <see cref="FinishRelocationLocked"/>).</para>
///
/// <para><b>Names and services longer than 65 535 bytes are logged EMPTY.</b> v2 kept v1's 16-bit
/// <c>NameLength</c> and <c>ServiceLength</c>, and an append clamps such a field out of the log
/// rather than truncating it mid-rune (see <c>AppendLocked</c>). The hot tier keeps the full text
/// — the ring accepts such a span by parking it — so the span is named while it is live, but a
/// crash before its segment is written replays it with an empty name or service.</para>
///
/// <para><b>Durability, stated because it is a choice.</b> Appends are not fsynced — not per
/// span and not on a timer. There is NO PERIODIC FSYNC BETWEEN SEGMENT FLUSHES: the two
/// flushes in <see cref="CommitFlush"/> are the only points at which this log is forced to
/// the platter (and the one an open makes after finishing an interrupted relocation). The mapping
/// survives the death of this PROCESS (the page cache is the file's and outlives it), and since v3
/// so does a commit killed in the middle of its move: the next open finishes it. The death of the
/// MACHINE loses whatever the OS had not yet written back, which can be every span since the last
/// segment flush. The per-entry checksum is what keeps that loss a clean cut rather than a replay of
/// garbage. What nothing here makes durable is a commit's move across a POWER LOSS: the moved spans
/// and the header that records the move sit on different pages, and the commit's barrier comes
/// after the whole move, so the disk can keep a later chunk of it and lose an earlier one — the
/// spans that arrived during that flush are then lost, as they were before v3. (The logs WAL made
/// the other choice, a timer msync; the trade here was made for the drainer's throughput, and it is
/// this note that makes it a decision rather than an accident.)</para>
///
/// <para><b>Crash recovery.</b> A flush writes the segment first and resets the log second,
/// so a crash between the two would replay spans that are already cold. The flush therefore
/// bumps <see cref="WalFileHeader.Generation"/> BEFORE zeroing the write offset, and recovery
/// keeps only entries stamped with the generation the header now carries. Only a crash
/// landing between the segment write and the moment the commit starts to move the surviving tail
/// can duplicate spans: from the first byte the move overwrites, the flushed generation is gone
/// from the log whether or not the commit lives to stamp the header.</para>
///
/// <para>The generation is assigned by this class under its own write lock, which is what
/// makes the test sound. An earlier design compared each entry's span START TIME against the
/// newest start time in the flushed segment — but that time comes from the instrumented
/// client, not from us, and is not monotonic in append order. An OTLP exporter ships a span
/// when it ENDS, so a long span appended after the flush can easily have started before it;
/// under the old rule such spans were silently dropped on replay. Serialising appends against
/// flushes proves they do not interleave, which is a different and weaker claim than "a span
/// appended after the reset has a later start time" — the latter simply is not true.</para>
///
/// <para>Generation 0 means "never written", so it doubles as the end-of-data marker: a
/// zero-filled region is otherwise indistinguishable from a valid entry with an empty name,
/// an empty service and no attributes. That matters because a span may legitimately carry a
/// zero start time — <c>OtlpTraceStreamParser</c> defaults <c>startTimeUnixNano</c> to 0 when
/// the field is absent and nothing downstream rejects it.</para>
///
/// <para>Spans duplicated by a crash in the window above are replayed into the hot tier
/// while also living in the flushed segment. The read paths that return individual spans —
/// <c>TraceStorageEngine.GetTraceAsync</c> and <c>SearchSpansAsync</c> — drop the repeat by
/// span id, so a waterfall shows it once. The aggregate paths (trace list, per-service
/// stats, service graph, volume) do not: they scan unbounded span volumes, where tracking
/// every id costs far more than the one-span skew it would correct.</para>
/// </summary>
internal sealed unsafe partial class SpanWriteAheadLog : IDisposable
{
    private const uint   MagicNumber     = 0x52_44_53_57; // "RDSW"
    private const ushort WalVersion      = 3;
    private const ushort WalVersionV2    = 2;
    private const ushort WalVersionV1    = 1;

    /// <summary>A v3 file header: v2's 32 bytes, then the relocation record and the header's checksums. See <see cref="WalFileHeader"/>.</summary>
    private const int    FileHeaderSize       = 64;

    /// <summary>A v1 or v2 file header. Its data starts right after it, so such a log's offsets are 32 lower.</summary>
    private const int    FileHeaderSizeLegacy = 32;

    /// <summary>The 64 bytes of <see cref="SpanWalEntryHeader"/> — every field the checksum covers. All of a v1 entry header.</summary>
    private const int    ChecksummedHeaderBytes = 64;

    /// <summary>A v2 (and v3) entry header: the checksummed 64 bytes, then the CRC32C over them and the payload.</summary>
    private const int    EntryHeaderSize   = ChecksummedHeaderBytes + sizeof(uint);

    /// <summary>A v1 entry header: no checksum. Read, and appended only by a log whose upgrade could not commit.</summary>
    private const int    EntryHeaderSizeV1 = ChecksummedHeaderBytes;

    /// <summary>8 MB holds ~12k eight-attribute spans; the log is reset on every flush, so it grows only under a burst.</summary>
    private const long DefaultCapacity = 8 * 1024 * 1024;

    /// <summary>First generation of a fresh log. 0 is reserved for "never written".</summary>
    private const uint FirstGeneration = 1;

    /// <summary>Where a v1 or v2 log is rewritten as v3 before it replaces the original. See <see cref="Open(string, long, ILogger)"/>.</summary>
    internal const string UpgradeSuffix = ".upgrade.tmp";

    /// <summary>
    /// The file header. The first 32 bytes are v1's and v2's, byte for byte; the rest exists in v3
    /// only, and a log that is v1 or v2 on disk never reads or writes past its 32 (see
    /// <see cref="_headerSize"/>).
    ///
    /// <para><b>The relocation record</b> (<see cref="MoveFrom"/>, <see cref="MoveLength"/>,
    /// <see cref="MoveDone"/>) is what makes a commit's move of the surviving tail safe against the
    /// process dying in the middle of it. See <see cref="RelocateLocked"/>.</para>
    ///
    /// <para><b><see cref="Crc"/></b> covers everything but <see cref="WriteOffset"/>: the identity,
    /// the generation and the record, which change a few times per flush, and not the claim, which
    /// every append moves and which the replay checks against the data anyway (it stops at the
    /// first entry that does not verify, and truncates the claim to there). See
    /// <see cref="SealHeaderLocked"/>.</para>
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Size = FileHeaderSize)]
    private struct WalFileHeader
    {
        public uint   Magic;
        public ushort Version;
        private ushort _pad;
        public long   WriteOffset;
        public uint   Generation;
        private uint  _reserved0;
        private long  _reserved1;
        // ── v3 only ──
        public long   MoveFrom;          // logical offset the surviving tail is moved from: the flush boundary
        public long   MoveLength;        // its length; 0 = no relocation in flight
        public long   MoveDone;          // bytes of it already moved, always a chunk boundary
        public uint   Crc;               // CRC32C over bytes [0, 8) and [16, 56): see SealHeaderLocked
        public uint   PendingCrc;        // the same, of the header as the store in progress leaves it
    }

    /// <summary>
    /// Pack = 1 keeps the fields at exactly 64 bytes — without it the 8-byte members would
    /// align and push the tail past the stride, straight into the payload area (the
    /// corruption the logs WAL hit in its v2 layout). <see cref="Generation"/> occupies the
    /// four bytes that were previously padding. In v2 the entry's CRC follows these 64 bytes;
    /// it is not a field here, so a v1 entry and a v2 entry share this struct byte for byte.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = ChecksummedHeaderBytes)]
    private struct SpanWalEntryHeader
    {
        public fixed byte TraceId[16];        // W3C big-endian, written via TraceId.WriteTo
        public ulong  SpanId;
        public ulong  ParentSpanId;
        public long   StartTimeUnixNano;
        public long   DurationNanos;
        public uint   AttrLength;
        public ushort NameLength;
        public ushort ServiceLength;
        public short  HttpStatusCode;
        public byte   Kind;
        public byte   Status;
        public uint   Generation;             // 0 = unwritten; see the class remarks
    }

    private readonly string   _filePath;
    private readonly ILogger? _logger;
    private readonly Lock     _writeLock = new();

    /// <summary>
    /// The most one growth adds. Doubling up to it, a step of it after — see <see cref="NextCapacity"/>.
    /// </summary>
    private readonly long _growthStepCap;

    // Held open for the log's whole life: the mapping is created over it, Grow extends it,
    // and Flush issues FlushFileBuffers through it. On Windows the accessor's flush is
    // FlushViewOfFile, which hands the pages to the filesystem but does NOT wait out the
    // drive cache — without this handle the commit barrier would not actually be durable.
    private FileStream?               _fileStream;
    private MemoryMappedFile?         _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private byte*                     _ptr;
    private long                      _capacity;
    private long                      _writeOffset;        // logical, excludes the file header
    private uint                      _generation;

    /// <summary>
    /// 0 for a v3 log. 1 or 2 when the file on disk is a log of that version: set by
    /// <see cref="OpenOrCreate"/> before the mapping is sized, and never again. <see cref="Open(string, long, ILogger)"/>
    /// upgrades such a log; it stays in its own layout for the life of the process only when that
    /// upgrade cannot commit — appends in its stride, commits with the single copy it always made,
    /// no relocation record, because it has no room for one.
    /// </summary>
    private ushort _legacyVersion;

    /// <summary>
    /// Where the data starts: <see cref="FileHeaderSize"/>, or <see cref="FileHeaderSizeLegacy"/> for a
    /// log that is v1 or v2 on disk. Every offset into the mapping and every file size goes through
    /// it. Set by <see cref="OpenOrCreate"/> before the mapping is sized, and never again.
    /// </summary>
    private int _headerSize = FileHeaderSize;

    /// <summary>
    /// The entry stride this log reads and appends: <see cref="EntryHeaderSize"/> with a checksum
    /// (v2, v3), or — only for a log that is v1 on disk, see <see cref="_legacyVersion"/> —
    /// <see cref="EntryHeaderSizeV1"/> without one.
    /// </summary>
    private int  _entryHeaderSize = EntryHeaderSize;
    private bool _checksummed     = true;

    // ── Open-flush state (see BeginFlush/CommitFlush) ────────────────────────
    private bool _flushOpen;          // one flush at a time — the engine serialises them
    private long _flushBoundary;      // bytes belonging to the generation being flushed
    private bool _generationBumped;   // bump once per COMMIT cycle, not per Begin (retries)

    private static uint Next(uint g) => g == uint.MaxValue ? FirstGeneration : g + 1;

    public string FilePath => _filePath;

    /// <summary>Bytes currently held by the log. Diagnostics only.</summary>
    public long WrittenBytes { get { lock (_writeLock) return _writeOffset; } }

    private SpanWriteAheadLog(string filePath, long growthStepCap, ILogger? logger)
    {
        _filePath      = filePath;
        _growthStepCap = Math.Max(4096, growthStepCap);
        _logger        = logger;
        // Armed before OpenOrCreate, which is where a relocation interrupted by the previous
        // process is finished: the only way a test reaches that relocation and its stores.
        OnRelocationStepForTest = t_relocationStepForNextOpenForTest;
        OnPendingStoredForTest  = t_pendingStoredForNextOpenForTest;
    }

    /// <summary>
    /// Opens the log, creating it if absent, and finishes a commit's relocation that the previous
    /// process did not live to finish (<see cref="FinishRelocationLocked"/>). Growth is bounded by
    /// the traces hot-tier budget (<see cref="MemoryBudgets.TraceHotTierCapBytes"/>) — see
    /// <see cref="NextCapacity"/>.
    ///
    /// <para><b>A v1 OR v2 LOG IS UPGRADED, NOT DISCARDED.</b> Earlier releases wrote v1 and then v2,
    /// and an unknown version is treated as a foreign file and re-initialised — which, for the log a
    /// restart after this upgrade finds, would silently drop every span the previous process
    /// acknowledged and never flushed. So such a file is opened in its own layout (v1: 64-byte
    /// entries without a checksum; v2: v3's entries; both behind a 32-byte header), rewritten as v3
    /// into <c>spans.wal.upgrade.tmp</c> — each entry's bytes verbatim, a v1 entry now with its
    /// checksum, a v2 entry with the one it has — fsynced, and moved over the original. The move is
    /// the commit point: a crash before it leaves the old file untouched and the next start upgrades
    /// it again; a crash after it leaves a complete v3 file. The upgrade copies what the old release
    /// would have replayed, and only that: a v2 log is copied up to the first entry that does not
    /// verify. It cannot repair what a stop under the old release already cost — a v2 log killed
    /// inside the very move this version records has no record of it, and its spans behind the
    /// copy front are lost to the upgrade exactly as they were to the old replay.</para>
    ///
    /// <para><b>AN UPGRADE THAT CANNOT COMPLETE DOES NOT STOP THE SERVER.</b> This runs in the trace
    /// engine's constructor, where a throw fails the host — once, on every existing install, at the
    /// first start after the upgrade. The rename is retried briefly (an antivirus scanner holding the
    /// fresh copy open is the sharing violation seen on Windows); if the copy cannot be written (a
    /// full disk) or the rename still fails, the log is opened in its own version, in place,
    /// untouched: every span it holds replays, new spans are appended in its layout, the error is
    /// logged, and the upgrade is tried again at the next start. That is exactly the log the
    /// previous release ran with — no relocation record, and for v1 no checksum — for one more
    /// process lifetime; the alternatives were refusing to start, or re-initialising a file whose
    /// spans exist nowhere else.</para>
    ///
    /// <para><b>ROLLING BACK is not symmetric.</b> A release older than v3 treats a v3 log as a
    /// foreign file (any version but its own) and re-initialises it in place. That costs nothing
    /// only after a CLEAN stop whose final flush FINISHED — one that logged neither of
    /// <c>TraceStorageEngine.DisposeCoreAsync</c>'s Errors, "The final span flush did not finish
    /// within {Budget}s — the WAL replays the tier on the next start" and "Final span flush failed
    /// — the WAL replays the tier next start". A stop that logged either left the tier in this log,
    /// and so does an UNCLEAN stop; either way the spans the log held and no segment did are lost
    /// by the rollback. Operators are told so, with the text to search for, in
    /// docs/CONFIGURATION.md ("Upgrading and rolling back").</para>
    /// </summary>
    public static SpanWriteAheadLog Open(string filePath, long initialCapacity = DefaultCapacity, ILogger? logger = null) =>
        Open(filePath, initialCapacity, MemoryBudgets.TraceHotTierCapBytes, logger);

    /// <summary>As <see cref="Open(string, long, ILogger)"/>, with the growth step and the upgrade's I/O a test can replace.</summary>
    internal static SpanWriteAheadLog Open(string filePath, long initialCapacity, long growthStepCap,
                                           ILogger? logger = null, UpgradeIo? io = null)
    {
        io ??= UpgradeIo.Default;
        string tmp = filePath + UpgradeSuffix;

        var wal = OpenInstance(filePath, initialCapacity, growthStepCap, logger);
        if (wal._legacyVersion == 0)
        {
            // A STALE COPY from an upgrade that died before its rename. Never the only copy of
            // anything: until the rename the old log is authoritative, and the rename is atomic. An
            // old log re-truncates it below; beside a v3 log it is 8 MB+ of garbage nobody would remove.
            DeleteQuietly(tmp);
            return wal;
        }

        ushort legacy = wal._legacyVersion;
        long   capacity;
        try
        {
            capacity = wal.WriteUpgradedCopy(tmp);
        }
        catch (Exception ex)
        {
            DeleteQuietly(tmp);
            logger?.LogError(ex,
                "The span WAL at {Path} is a v{Version} log and its v3 copy could not be written; it stays "
              + "v{Version} (no relocation record) for this run, every span in it replays, and the upgrade is "
              + "retried at the next start", filePath, legacy, legacy);
            return wal;                                  // already open, in its own layout
        }
        wal.Dispose();                                   // the mapping has to go before the file can

        // THE COMMIT POINT of the upgrade.
        if (TryMoveWithRetry(tmp, filePath, io) is { } moveFailure)
        {
            DeleteQuietly(tmp);
            logger?.LogError(moveFailure,
                "The span WAL at {Path} could not be replaced by its v3 copy after {Attempts} attempts; it "
              + "stays v{Version} (no relocation record) for this run, every span in it replays, and the "
              + "upgrade is retried at the next start", filePath, MoveRetryDelays.Length + 1, legacy);

            // Whatever is at the path now — the rename is atomic, so it is the old log as it was,
            // which opens in its own layout by itself.
            return OpenInstance(filePath, initialCapacity, growthStepCap, logger);
        }

        return OpenInstance(filePath, capacity, growthStepCap, logger);
    }

    private static SpanWriteAheadLog OpenInstance(string filePath, long initialCapacity, long growthStepCap, ILogger? logger)
    {
        var wal = new SpanWriteAheadLog(filePath, growthStepCap, logger);
        try { wal.OpenOrCreate(initialCapacity); }
        catch { wal.Dispose(); throw; }                  // the lifetime handle, not left to the finalizer
        return wal;
    }

    /// <summary>
    /// The pauses between the rename's attempts: six attempts over ~0.8 s. Long enough for a scanner
    /// to let go of a file it opened on creation, short enough that a rename which will never succeed
    /// costs a start less than a second.
    /// </summary>
    private static readonly TimeSpan[] MoveRetryDelays =
    [
        TimeSpan.FromMilliseconds(25), TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(400),
    ];

    /// <summary>The rename, retried on the I/O failures a transient holder causes. Null on success, else the last failure.</summary>
    private static Exception? TryMoveWithRetry(string from, string to, UpgradeIo io)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                io.Move(from, to);
                return null;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                if (attempt >= MoveRetryDelays.Length) return ex;
                io.Wait(MoveRetryDelays[attempt]);
            }
        }
    }

    private static void DeleteQuietly(string path)
    {
        try { File.Delete(path); } catch { /* best-effort: an upgrade truncates it, and it replays nothing */ }
    }

    /// <summary>
    /// The upgrade's rename and the pause between its attempts, as a seam: a test fails the rename
    /// (the sharing violation of production) and waits for nothing. Production uses <see cref="Default"/>.
    /// </summary>
    internal sealed class UpgradeIo
    {
        public static readonly UpgradeIo Default = new();

        public Action<string, string> Move { get; init; } = static (from, to) => File.Move(from, to, overwrite: true);
        public Action<TimeSpan>       Wait { get; init; } = static d => Thread.Sleep(d);
    }

    /// <summary>The format version of the file behind <paramref name="file"/>, or 0 when it is not a span WAL at all.</summary>
    private static ushort VersionOnDisk(FileStream file)
    {
        Span<byte> head = stackalloc byte[6];
        file.Position = 0;
        int read = file.ReadAtLeast(head, head.Length, throwOnEndOfStream: false);
        file.Position = 0;
        if (read < head.Length || BinaryPrimitives.ReadUInt32LittleEndian(head) != MagicNumber) return 0;
        return BinaryPrimitives.ReadUInt16LittleEndian(head[4..]);
    }

    private void OpenOrCreate(long initialCapacity)
    {
        bool exists = File.Exists(_filePath);
        _fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

        // The version decides the header's size, and the header's size decides where the data starts
        // — so it is read off the handle before the mapping is sized.
        ushort onDisk = exists ? VersionOnDisk(_fileStream) : (ushort)0;
        bool   known  = onDisk is WalVersion or WalVersionV2 or WalVersionV1;
        if (onDisk is WalVersionV2 or WalVersionV1)
        {
            // Left in its own layout, and read with it. Open upgrades it once it is open (see there).
            _legacyVersion = onDisk;
            _headerSize    = FileHeaderSizeLegacy;
            if (onDisk == WalVersionV1)
            {
                _entryHeaderSize = EntryHeaderSizeV1;
                _checksummed     = false;
            }
        }

        long fileSize = _headerSize + initialCapacity;
        if (_fileStream.Length < fileSize) _fileStream.SetLength(fileSize);
        else                               fileSize = _fileStream.Length;   // reopen an already-grown log at its size

        _capacity = fileSize - _headerSize;
        Map(fileSize);

        ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
        if (!known)
        {
            // New, foreign or future-versioned file — reinitialise in place, as v3. Anything
            // already there cannot be replayed under a layout we do not know.
            new Span<byte>(_ptr, FileHeaderSize).Clear();
            hdr.Magic       = MagicNumber;
            hdr.Version     = WalVersion;
            hdr.WriteOffset = FileHeaderSize;
            hdr.Generation  = FirstGeneration;
            _writeOffset    = 0;
            _generation     = FirstGeneration;
            SealHeaderLocked();
            return;
        }

        _writeOffset = Math.Max(0, hdr.WriteOffset - _headerSize);
        _generation  = hdr.Generation == 0 ? FirstGeneration : hdr.Generation;
        if (_writeOffset > _capacity) _writeOffset = _capacity;  // truncated file — replay what is mapped
        if (_legacyVersion != 0) return;                         // left untouched: Open upgrades it

        // Decided before anything below stores into the header. A v1 or v2 header has no checksum.
        bool verifies = HeaderVerifies(in hdr);

        // A header that verifies only by its PENDING checksum — the previous process stopped between
        // a field and its seal — is sealed before anything else is stored: the next covered store
        // overwrites PendingCrc first, and a second stop before that store's field would leave a
        // header matching neither slot, a false "does not verify" (the metric WAL's 6327634).
        if (verifies) SealHeaderLocked();

        // Nothing below may make a header that did NOT verify verify again before its generation is
        // rebuilt: an open killed during the finish would otherwise leave the rotted generation
        // sealed, and the next open would skip the rebuild (c4fadf7). Released, and the header
        // sealed once, at the end of the open.
        _sealsHeld = !verifies;

        // A commit's move of the surviving tail that the process did not live to finish is
        // finished here, before anything walks the data. See RelocateLocked.
        FinishRelocationLocked(ref hdr, trusted: verifies);

        // A header whose generation does not verify gets it back from the entries. See there.
        if (!verifies) RebuildGenerationLocked(ref hdr);

        _sealsHeld = false;
        SealHeaderLocked();
    }

    /// <summary>
    /// Rewrites this v1 or v2 log as v3 at <paramref name="tmpPath"/> and fsyncs it. Every entry the
    /// old replay would reach is copied — header bytes and payload verbatim; a v1 entry gets the
    /// checksum computed over them, a v2 entry keeps the one it has (and was verified by) — so the
    /// upgraded log replays exactly what the old log would have, under the same generation rule.
    /// Returns the logical capacity the copy was sized to.
    /// </summary>
    private long WriteUpgradedCopy(string tmpPath)
    {
        lock (_writeLock)
        {
            using var fs = new FileStream(tmpPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None,
                                          bufferSize: 64 * 1024);
            Span<byte> fileHeader = stackalloc byte[FileHeaderSize];
            fileHeader.Clear();
            fs.Write(fileHeader);                        // placeholder; the real one goes in last

            Span<byte> crcBytes = stackalloc byte[sizeof(uint)];
            long pos = 0, written = 0, total;
            while ((total = EntryAt(pos, _writeOffset, _entryHeaderSize, _checksummed)) > 0)
            {
                byte* src     = _ptr + _headerSize + pos;
                var   header  = new ReadOnlySpan<byte>(src, ChecksummedHeaderBytes);
                var   payload = new ReadOnlySpan<byte>(src + _entryHeaderSize, (int)(total - _entryHeaderSize));
                uint  crc     = _checksummed
                    ? Unsafe.ReadUnaligned<uint>(src + ChecksummedHeaderBytes)   // v2: its own, verified by EntryAt
                    : Crc32c.Append(Crc32c.Append(0, header), payload);           // v1: computed now
                BinaryPrimitives.WriteUInt32LittleEndian(crcBytes, crc);

                fs.Write(header);
                fs.Write(crcBytes);
                fs.Write(payload);
                written += EntryHeaderSize + payload.Length;
                pos     += total;
            }

            long capacity = Math.Max(_capacity, written + EntryHeaderSize);
            fs.SetLength(FileHeaderSize + capacity);

            var hdr = new WalFileHeader
            {
                Magic       = MagicNumber,
                Version     = WalVersion,
                WriteOffset = FileHeaderSize + written,
                Generation  = _generation,
            };
            hdr.Crc = hdr.PendingCrc = HeaderChecksum(in hdr);
            MemoryMarshal.Write(fileHeader, in hdr);
            fs.Position = 0;
            fs.Write(fileHeader);
            fs.Flush(flushToDisk: true);
            return capacity;
        }
    }

    private void Map(long fileSize)
    {
        _mmf      = MemoryMappedFile.CreateFromFile(_fileStream!, null, fileSize,
                        MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
        _accessor = _mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.ReadWrite);
        _ptr      = null;
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
    }

    // ── Append ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Appends one span whose text is ALREADY UTF-8 — the shape the raw ingest sink hands over,
    /// so nothing is transcoded here. The name, service and attribute blob are copied verbatim
    /// into the mapped page and checksummed where they land. Nothing is allocated on the managed
    /// heap. A name or service over 65 535 bytes is logged EMPTY (the lengths are 16-bit, and a
    /// truncated length would desynchronise the stride), exactly as v1 did.
    /// </summary>
    public void Append(
        TraceId traceId, SpanId spanId, SpanId parentSpanId, long startTimeUnixNano, long durationNanos,
        SpanKind kind, SpanStatusCode status, short httpStatusCode,
        ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> serviceUtf8, ReadOnlySpan<byte> attrs)
    {
        using var scope = EnterAppendScope();
        scope.Append(traceId, spanId, parentSpanId, startTimeUnixNano, durationNanos,
                     kind, status, httpStatusCode, nameUtf8, serviceUtf8, attrs);
    }

    /// <summary>
    /// ONE hold of the append lock for a run of appends — the engine's write path takes one per
    /// write hold instead of one per span. Dispose releases the lock. A disposed log throws here
    /// rather than answering silently: the engine's shutdown gate refuses spans before they reach
    /// the log, so arriving disposed is a broken gate, and a silent return is the
    /// queryable-but-unrecoverable shape that gate was built to end.
    /// </summary>
    public AppendScope EnterAppendScope()
    {
        if (!_writeLock.TryEnter())
        {
            _appendWaitingForTest?.Invoke();
            _writeLock.Enter();
        }
        return OpenScopeHeld();
    }

    /// <summary>
    /// <see cref="EnterAppendScope"/> without waiting: false when another holder has the log —
    /// in practice <see cref="CommitFlush"/>'s persistence barrier, a drive flush of up to
    /// milliseconds. The engine calls this INSIDE its own write lock and, on false, lets go of
    /// that lock before waiting (<see cref="WaitUntilAppendable"/>): blocking here instead would
    /// hold every reader of the hot tier behind this log's fsync.
    /// </summary>
    public bool TryEnterAppendScope(out AppendScope scope)
    {
        if (!_writeLock.TryEnter()) { scope = default; return false; }
        scope = OpenScopeHeld();
        return true;
    }

    /// <summary>Blocks until the append lock is free — the wait <see cref="TryEnterAppendScope"/> declined — and takes nothing.</summary>
    public void WaitUntilAppendable()
    {
        _appendWaitingForTest?.Invoke();
        lock (_writeLock) { }
    }

    private AppendScope OpenScopeHeld()
    {
        if (_disposed)
        {
            _writeLock.Exit();
            throw new ObjectDisposedException(nameof(SpanWriteAheadLog));
        }
        _scopedAppends = 0;
        return new AppendScope(this);
    }

    /// <summary>Appends made in the current scope — the index <see cref="_beforeBatchEntryForTest"/> is called with. Under the lock.</summary>
    private int _scopedAppends;

    /// <summary>A held append lock. Only <see cref="EnterAppendScope"/> and <see cref="TryEnterAppendScope"/> make one.</summary>
    public readonly ref struct AppendScope
    {
        private readonly SpanWriteAheadLog? _wal;

        internal AppendScope(SpanWriteAheadLog wal) => _wal = wal;

        /// <summary>As <see cref="SpanWriteAheadLog.Append"/>, under the lock this scope holds.</summary>
        public void Append(
            TraceId traceId, SpanId spanId, SpanId parentSpanId, long startTimeUnixNano, long durationNanos,
            SpanKind kind, SpanStatusCode status, short httpStatusCode,
            ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> serviceUtf8, ReadOnlySpan<byte> attrs)
        {
            var wal = _wal ?? throw new InvalidOperationException("an append scope that was never entered");
            wal._beforeBatchEntryForTest?.Invoke(wal._scopedAppends);
            wal.AppendLocked(traceId, spanId, parentSpanId, startTimeUnixNano, durationNanos,
                             kind, status, httpStatusCode, nameUtf8, serviceUtf8, attrs);
            wal._scopedAppends++;
        }

        public void Dispose() => _wal?._writeLock.Exit();
    }

    /// <summary>Test seam: called under the append lock before each append of a scope, with its index in the scope. Throwing from it is a log failure part-way through a batch.</summary>
    internal Action<int>? _beforeBatchEntryForTest;

    /// <summary>Test seam: an append is about to WAIT for the append lock (it was held). Fires from both the blocking entry and <see cref="WaitUntilAppendable"/>.</summary>
    internal Action? _appendWaitingForTest;

    private void AppendLocked(
        TraceId traceId, SpanId spanId, SpanId parentSpanId, long startTimeUnixNano, long durationNanos,
        SpanKind kind, SpanStatusCode status, short httpStatusCode,
        ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> serviceUtf8, ReadOnlySpan<byte> attrs)
    {
        if (_ptr is null)
            throw new InvalidOperationException(
                "Span WAL has no mapping; the log is not accepting appends.");

        // Lengths are stored in 16-bit fields; a pathological name must not silently
        // corrupt the stride, so clamp it out of the log rather than truncate mid-rune.
        if (nameUtf8.Length    > ushort.MaxValue) nameUtf8    = default;
        if (serviceUtf8.Length > ushort.MaxValue) serviceUtf8 = default;

        int  headerSize = _entryHeaderSize;              // v2/v3, unless the log is v1 on disk
        int  payload    = nameUtf8.Length + serviceUtf8.Length + attrs.Length;
        long entrySize  = (long)headerSize + payload;
        EnsureCapacityLocked(_writeOffset + entrySize);

        byte* dest = _ptr + _headerSize + _writeOffset;

        ref var eh = ref Unsafe.AsRef<SpanWalEntryHeader>(dest);
        // TraceId sits at offset 0 of the entry, so the entry pointer addresses it
        // directly — a fixed-size buffer reached through a ref into unmanaged memory
        // would need a `fixed` statement for no benefit.
        traceId.WriteTo(new Span<byte>(dest, 16));
        eh.SpanId            = spanId.RawValue;
        eh.ParentSpanId      = parentSpanId.RawValue;
        eh.StartTimeUnixNano = startTimeUnixNano;
        eh.DurationNanos     = durationNanos;
        eh.AttrLength        = (uint)attrs.Length;
        eh.NameLength        = (ushort)nameUtf8.Length;
        eh.ServiceLength     = (ushort)serviceUtf8.Length;
        eh.HttpStatusCode    = httpStatusCode;
        eh.Kind              = (byte)kind;
        eh.Status            = (byte)status;
        eh.Generation        = _generation;

        byte* p = dest + headerSize;
        nameUtf8.CopyTo(new Span<byte>(p, nameUtf8.Length));       p += nameUtf8.Length;
        serviceUtf8.CopyTo(new Span<byte>(p, serviceUtf8.Length)); p += serviceUtf8.Length;
        attrs.CopyTo(new Span<byte>(p, attrs.Length));

        // THE CHECKSUM LAST, over the bytes where they now sit: the 64 header bytes and the
        // payload after the CRC slot. Computed from the map rather than from the sources, so it
        // vouches for what a replay will read, not for what this method meant to write.
        if (_checksummed)
        {
            uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(dest, ChecksummedHeaderBytes));
            crc      = Crc32c.Append(crc, new ReadOnlySpan<byte>(dest + headerSize, payload));
            Unsafe.WriteUnaligned(dest + ChecksummedHeaderBytes, crc);
        }

        _writeOffset += entrySize;
        Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = _headerSize + _writeOffset;
    }

    // ── Two-phase flush (Begin / Commit / Abandon) ───────────────────────────
    //
    // The engine used to hold its exclusive lock across the WHOLE segment build and reset
    // the log afterwards — correct, and the reason ingest and every query stalled for the
    // build's full duration. The two-phase protocol is what lets the build run OFF the
    // lock: Begin moves in-memory appends to the next generation while the header keeps
    // the one being flushed, so BOTH generations replay after a crash mid-flush (the
    // segment is not durable yet — losing either would be data loss). Commit relocates
    // the new generation's tail to the front and stamps the header, killing the flushed
    // generation; Abandon leaves everything replayable for the retry.

    /// <summary>
    /// Test seam: <see cref="BeginFlush"/> is about to open its window. Throwing from it is
    /// BeginFlush failing before it changed anything — which is also what its "already open"
    /// refusal is. Null in production.
    /// </summary>
    internal Action? _beforeBeginFlushForTest;

    /// <summary>
    /// Opens a flush: appends from here on carry the NEXT generation; the entries being
    /// flushed keep the current one, and recovery accepts both until <see cref="CommitFlush"/>.
    /// The generation is bumped once per commit CYCLE — a Begin after an Abandon reuses the
    /// bumped one, so the replay window never spans more than two generations.
    /// </summary>
    public void BeginFlush()
    {
        _beforeBeginFlushForTest?.Invoke();
        lock (_writeLock)
        {
            if (_flushOpen)
                throw new InvalidOperationException("A span-WAL flush is already open.");
            _flushOpen     = true;
            _flushBoundary = _writeOffset;
            if (!_generationBumped)
            {
                _generation       = Next(_generation);
                _generationBumped = true;
                // The HEADER deliberately keeps the flushed generation until the commit.
            }
        }
    }

    /// <summary>
    /// Ends the open flush: the flushed generation's entries die, the entries appended
    /// during the flush move to the front, and the header commits the new generation.
    /// Call only after the segment carrying the flushed spans is DURABLE on disk.
    ///
    /// <para><b>The order, and what a process killed at each point leaves (#103).</b> The move of the
    /// surviving tail is recorded in the header before its first byte (<see cref="RelocateLocked"/>)
    /// and made in chunks, each recorded as it completes; then the new end is stored, the record
    /// cleared, and the generation-0 terminator planted; then the barrier; then the stamp. Killed
    /// before the record: nothing has moved, and the old header replays both generations — the
    /// flushed one as duplicates of a segment already durable, the one window this protocol keeps.
    /// Killed anywhere from the record to the clear: the next open redoes the chunk the record names
    /// and every one after it, stores the end and clears the record (<see cref="FinishRelocationLocked"/>)
    /// — the surviving tail replays once, the flushed generation not at all. Killed after the clear:
    /// the header's claim is the relocated end, the terminator or the claim stops the walk there,
    /// and the old generation in the header still accepts the tail's. A flushed prefix far shorter
    /// than its tail, provably dead under the stamp, stays where it is and nothing moves
    /// (<see cref="PrefixStaysLocked"/>): killed before the stamp, both generations replay, as before
    /// a record; after it, the tail alone.</para>
    ///
    /// <para><b>What it forces to disk, and where.</b> It used to hand FlushViewOfFile the WHOLE
    /// view, twice, for a commit whose own dirty bytes are the relocated tail and one header page.
    /// Both barriers are now RANGES: the relocated tail with its terminator (which begins at offset
    /// 0, so it takes the header page with it), and then the header page alone. Measured, that is
    /// NOT where a commit's time goes on Windows: the drive flush after the first barrier
    /// (FlushFileBuffers) writes every dirty page of the file, the dead generation's included, and
    /// took 80-300 ms for a 58 MB log either way (SpanWalV2Tests' probe). The ranges keep the
    /// msyncs to what the commit claims; the lock hold is dominated by that drive flush, which is
    /// why the engine never waits for this lock under its own. The drive flush that follows
    /// the second is taken OFF the lock — the header is already stamped and msynced, so an
    /// append racing it cannot make it wrong. The drive flush of the FIRST barrier stays under
    /// the lock, and has to: until it returns, the relocation is not known durable, and an
    /// append admitted in between would either land past the terminator (unreplayable after a
    /// crash of this process) or overwrite the originals of the relocated tail before its copy is
    /// safe (lost to a power cut). The engine keeps that wait away from its readers instead:
    /// its write path never waits for this lock while holding the engine lock
    /// (<see cref="TryEnterAppendScope"/>).</para>
    /// </summary>
    public void CommitFlush()
    {
        FileStream? handle;
        lock (_writeLock)
        {
            if (!_flushOpen) return;
            if (_disposed || _ptr is null)
            {
                // The log is gone (shutdown). Close the cycle but leave _generationBumped
                // set: the header still carries the flushed generation, and a Begin that
                // bumped again would stamp appends two generations ahead of it — which
                // ReadAll drops, silently, with no segment carrying them.
                _flushOpen     = false;
                _flushBoundary = 0;
                return;
            }

            // A replay that truncated the log below the boundary (ReadAll during a flush; the engine
            // never does it) leaves nothing after it: everything still in the log was flushed.
            long boundary = Math.Min(_flushBoundary, _writeOffset);
            long tail     = _writeOffset - boundary;     // appended while the segment was written
            ref var hdr   = ref Unsafe.AsRef<WalFileHeader>(_ptr);

            // A dead prefix far shorter than the tail behind it stays where it is, and so does the
            // tail: nothing moves, the stamp alone kills the prefix. See PrefixStaysLocked.
            bool keeps = tail > 0 && boundary > 0 && PrefixStaysLocked(boundary, tail);
            long end   = keeps ? boundary + tail : tail;

            if (tail > 0 && boundary > 0 && !keeps)
            {
                if (_legacyVersion != 0)
                {
                    // A v1 or v2 log kept in its own layout has no room for the record: it moves the
                    // tail in the one copy it always made, and keeps the #103 window for this run.
                    Buffer.MemoryCopy(_ptr + _headerSize + boundary, _ptr + _headerSize, _capacity, tail);
                }
                else
                {
                    // The claim the record is checked against (FinishRelocationLocked): every append
                    // stored it, so this restates it rather than changes it.
                    Volatile.Write(ref hdr.WriteOffset, _headerSize + boundary + tail);
                    RelocateLocked(ref hdr, boundary, tail, done: 0);
                    Volatile.Write(ref hdr.WriteOffset, _headerSize + tail);
                    OnRelocationStepForTest?.Invoke(RelocationStep.EndStored, tail);
                    ClearRelocationLocked(ref hdr);
                    OnRelocationStepForTest?.Invoke(RelocationStep.Cleared, tail);
                }
            }

            // From here the log ends where the relocated tail does (or, kept in place, where it
            // always did) — in memory and in the header's claim, before the barrier. A barrier that
            // fails below therefore leaves the next append landing over the terminator rather than
            // past it: the version of this path before v2 left the write offset where it was, beyond
            // the generation-0 marker, so every span appended after a failed barrier landed where
            // the next replay stops short of — acknowledged, queryable, and gone at the next restart.
            // The flushed generation's spans are not in the log any more, which is fine: their
            // segment was published before this commit ran.
            _writeOffset    = end;
            hdr.WriteOffset = _headerSize + end;

            // The move does not erase its source, and the old header's generation still accepts the
            // relocated tail's: a generation-0 marker at the new end stops any walk exactly where
            // the data now ends, whatever claim a power loss leaves in the header page — the same
            // trick the metric WAL's Compact uses. Planted AFTER the record is cleared, never before:
            // when the tail is exactly as long as the flushed prefix, the marker's slot is the first
            // source entry, which a finish at open would still read. It always fits after a move
            // (the tail is shorter than the log by at least the flushed prefix, and that prefix
            // holds at least one entry); with nothing flushed before it there was no move, and no
            // room for it means only that the log is full, where the claim alone ends the walk. A
            // kept tail moved nothing, so there is no source behind it to stop a walk short of.
            if (!keeps) PlantEndMarkerLocked(end);

            // ── PERSISTENCE BARRIER. The relocation and the header live on different
            //    pages, and dirty mmap pages reach the platter in whatever order the OS
            //    chooses — program order says nothing across pages. Without this flush a
            //    power loss could persist the committed header FIRST: it would then point
            //    at a front region that still held the old, dead generation, while the
            //    durable originals of the during-flush tail sat beyond the committed
            //    offset, unreachable. Every span appended during the build — tens of
            //    thousands at load — would be lost. Flushed here, a crash before the
            //    header lands leaves the OLD generation over a front that now holds the
            //    relocated tail followed by the generation-0 marker: replay reads the tail
            //    and stops at the marker. Nothing lost, nothing duplicated.
            //
            //    THE RANGE IS [0, relocated tail + terminator), page-aligned outwards. It
            //    starts at the file's first byte, so the header page is in it; the tail is
            //    all the commit dirtied besides the header. Nothing past it is claimed by
            //    anything this commit writes. A kept tail takes its dead prefix with it: the
            //    replay walks through that prefix to reach the tail, and a page of it torn by a
            //    power loss would end the replay in front of every span the commit kept.
            try
            {
                FlushRangeLocked(0, _headerSize + end + EntryHeaderSize);
                FlushHandle(_fileStream!);
            }
            catch (Exception ex)
            {
                // The barrier failed, so the header must NOT claim the new generation: it keeps
                // the flushed one, and the retry's Begin reuses the already-bumped append
                // generation (_generationBumped stays set), so the window never widens. The
                // relocation itself is done and recorded as done (the record is clear), and the
                // log already continues from its end (above).
                _flushOpen     = false;
                _flushBoundary = 0;
                throw new IOException(
                    "span WAL commit barrier failed; the log keeps the flushed generation in its header", ex);
            }

            // THE STAMP, between two checksums like every covered store (StoreCovered): a process
            // killed inside it leaves a header that verifies, as the generation was before it or
            // as it is after, never one that reads as rot.
            StoreCovered(HeaderField.Generation, _generation);

            // Only now is the cycle over: every step that can throw is behind us, so the
            // in-memory generation and the header can no longer drift apart (a Begin that
            // bumped a second time would stamp appends the header's acceptance window does
            // not cover, and ReadAll would drop them).
            _flushBoundary    = 0;
            _flushOpen        = false;
            _generationBumped = false;

            // Commit the header itself, so the dead generation cannot come back after a
            // power loss and be replayed into duplicates of a segment already on disk. The
            // page goes to the filesystem here, under the lock; the drive flush below.
            FlushRangeLocked(0, _headerSize);
            handle = _fileStream;
        }

        // ── OFF THE LOCK: the header page's drive flush. A failure here leaves consistent state
        //    — the old header simply replays both generations — so it is reported, not repaired.
        //    An ObjectDisposedException means Dispose closed the handle meanwhile, and Dispose
        //    fsyncs on its way out.
        if (handle is not null)
        {
            try { FlushHandle(handle); }
            catch (ObjectDisposedException) { }
        }
    }

    /// <summary>
    /// Closes the open flush WITHOUT killing its generation — the segment write failed,
    /// so the log remains the only durable copy. Both generations keep replaying; the
    /// retry's Begin reuses the already-bumped append generation.
    /// </summary>
    public void AbandonFlush()
    {
        lock (_writeLock)
        {
            _flushOpen     = false;
            _flushBoundary = 0;
        }
    }

    // ── The relocation record (v3) ───────────────────────────────────────────

    /// <summary>The points of a relocation <see cref="OnRelocationStepForTest"/> fires at, in order.</summary>
    internal enum RelocationStep : byte
    {
        /// <summary>The record is armed and sealed; no byte has moved.</summary>
        Armed,
        /// <summary>A chunk is moved and its <see cref="WalFileHeader.MoveDone"/> sealed.</summary>
        Chunk,
        /// <summary>The new end is stored; the record is still armed.</summary>
        EndStored,
        /// <summary>The record is cleared and sealed; the end marker is not planted yet.</summary>
        Cleared,
    }

    /// <summary>
    /// Test seam fired at every <see cref="RelocationStep"/> of a relocation — a commit's, or the one
    /// the open finishes — with the bytes moved so far. What the file holds at that instant is what
    /// a process killed there leaves. The states BETWEEN a covered store and its checksum are
    /// <see cref="OnCoveredStoreForTest"/>'s and <see cref="OnPendingStoredForTest"/>'s. Null in
    /// production.
    /// </summary>
    internal Action<RelocationStep, long>? OnRelocationStepForTest;

    /// <summary>
    /// Test seam: <see cref="OnRelocationStepForTest"/> for the logs this THREAD opens next — the only
    /// way to reach a relocation the open itself finishes. Thread-static so that no other test's open
    /// sees it. Null in production.
    /// </summary>
    [ThreadStatic] internal static Action<RelocationStep, long>? t_relocationStepForNextOpenForTest;

    /// <summary>A commit moves its tail only when the flushed prefix is at least 1/n of it — at most n chunks; see <see cref="PrefixStaysLocked"/>.</summary>
    private const long MinPrefixToMoveTail = 8;

    /// <summary>
    /// Whether this commit leaves the flushed prefix <c>[0, boundary)</c> where it is, and with it the
    /// tail behind it, instead of moving the tail to the front. Caller holds the lock.
    ///
    /// <para><b>Why a prefix may stay.</b> The move goes in chunks no longer than the prefix, each
    /// with a covered header store, under the append lock: a one-entry prefix before an 8 MiB tail
    /// of minimal spans is 120 000 of them — measured 10-12.5 ms of commit against 3.3 ms for the
    /// same tail moved in one chunk, where this box's drive flush is 2 ms. Left in place, the prefix
    /// is dead weight the replay skips, and the next commit reclaims it with everything else — its
    /// own prefix is then at least this whole tail, so the move it makes is a few chunks at most.
    /// The metric WAL's rule (61c1f02, d5f7411): the tail moves only when the prefix is at least an
    /// eighth of it, compared multiplied, so at most eight chunks.</para>
    ///
    /// <para><b>Why the span WAL has to check what the metric WAL could assume.</b> There a prefix is
    /// dead by construction: it is at or below the watermark, and the watermark is what the replay
    /// filters on. Here the replay accepts a WINDOW — the header's generation and its successor —
    /// and the prefix is not always outside the window the stamp opens. After an abandoned flush,
    /// the retry's Begin reuses the bumped generation, so the spans appended between the two Begins
    /// sit in the retry's prefix with the very generation this commit stamps; after a restart that
    /// followed a crash mid-flush, the prefix holds the successor's entries from before the crash
    /// behind appends of the header's own. Left in place, either would replay beside the segment
    /// that holds it — on every restart until a later commit moved past it. So a prefix stays only
    /// if the walk over it proves what the replay will do with it under the new header: no entry
    /// of the stamp's generation or its successor. And only if every entry of it verifies: a kept
    /// prefix sits IN FRONT of the tail, and the replay stops at the first entry that does not
    /// verify — a torn one there would cut off every span this commit kept. A walk that does not
    /// end exactly on the boundary proves neither, and the tail moves.</para>
    ///
    /// <para>The walk reads the prefix once, with checksums: at most an eighth of the tail, which
    /// this commit has just been spared moving. v1 and v2 logs kept in their own layout move as they
    /// always did.</para>
    /// </summary>
    private bool PrefixStaysLocked(long boundary, long tail)
    {
        if (_legacyVersion != 0 || boundary * MinPrefixToMoveTail >= tail) return false;

        uint stamp = _generation, successor = Next(stamp);
        long pos = 0, total;
        while (pos < boundary && (total = EntryAt(pos, boundary, _entryHeaderSize, _checksummed)) > 0)
        {
            uint g = Unsafe.AsRef<SpanWalEntryHeader>(_ptr + _headerSize + pos).Generation;
            if (g == stamp || g == successor) return false;
            pos += total;
        }
        return pos == boundary;
    }

    /// <summary>
    /// Moves the surviving tail <c>[from, from + length)</c> to the front, FROM <paramref name="done"/>
    /// on, recording its progress in the header so that a process that dies anywhere inside can be
    /// finished at the next open (<see cref="FinishRelocationLocked"/>). v3 only; caller holds the
    /// lock (or is the open).
    ///
    /// <para><b>Why v2's single <c>Buffer.MemoryCopy</c> was not enough once entries carry
    /// checksums (#103).</b> A process killed in the middle of the copy left the old header over a
    /// front half overwritten: the copies verify, the entry straddling the copy front does not, and
    /// the replay stopped there and truncated the log. The tail entries not yet copied still sat
    /// intact further on — acknowledged spans in no segment — and nothing would ever read them
    /// again. v1 walked on past the front off garbage lengths and replayed them, with duplicates;
    /// v2 made the loss certain. This is the metric WAL's protocol (a057ca6), the same bug in its
    /// Compact.</para>
    ///
    /// <para><b>Chunks no longer than <paramref name="from"/>,</b> so that no chunk's destination
    /// overlaps its own source: a chunk at <c>d</c> writes <c>[d, d + c)</c> and reads
    /// <c>[from + d, from + d + c)</c>, and <c>c ≤ from</c>. The earlier chunks wrote only below
    /// <c>d</c>, so a chunk's source is intact until that chunk is done — which is what makes
    /// redoing the chunk <see cref="WalFileHeader.MoveDone"/> names exact, however far into it the
    /// process got. When the tail fits in the flushed prefix (the usual case: spans arriving during
    /// a build are fewer than the snapshot it writes) this is ONE chunk, the old single copy; a tail
    /// longer than the prefix takes <c>length / from</c> of them, one header store each.</para>
    ///
    /// <para><b>Order.</b> <see cref="WalFileHeader.MoveFrom"/> and <see cref="WalFileHeader.MoveDone"/>
    /// first, then <see cref="WalFileHeader.MoveLength"/>, which arms the record; then each chunk,
    /// then its <see cref="WalFileHeader.MoveDone"/>. The caller stores the new end and only then
    /// clears the record. Program order is what a killed process leaves behind; across a power loss
    /// the header page and the data pages still reach the disk in any order, the residual the class
    /// remarks name.</para>
    ///
    /// <para><b>Program order is pinned, not assumed.</b> Every store of the record is a
    /// <see cref="Volatile.Write(ref long, long)"/> (the covered ones inside
    /// <see cref="StoreCovered"/>): a release, so neither the compiler nor the CPU can let a store the
    /// protocol puts BEFORE it — the record's other fields, a chunk's bytes, a pending checksum —
    /// land after it. A plain store sequence is what the JIT is free to reorder, and a reordered
    /// arming (<c>MoveLength</c> before <c>MoveFrom</c>) is a record a killed process leaves naming
    /// a move that is not the one in flight.</para>
    /// </summary>
    private void RelocateLocked(ref WalFileHeader hdr, long from, long length, long done)
    {
        byte* data = _ptr + _headerSize;
        if (done == 0)
        {
            // MoveFrom and MoveDone count only while MoveLength is set (see HeaderChecksum), so
            // they are written plainly here and the one covered change is the arming.
            Volatile.Write(ref hdr.MoveFrom, from);
            Volatile.Write(ref hdr.MoveDone, 0);
            StoreCovered(HeaderField.MoveLength, (ulong)length);       // armed
            OnRelocationStepForTest?.Invoke(RelocationStep.Armed, 0);
        }

        while (done < length)
        {
            long chunk = Math.Min(from, length - done);
            Buffer.MemoryCopy(data + from + done, data + done, chunk, chunk);
            done += chunk;
            StoreCovered(HeaderField.MoveDone, (ulong)done);
            OnRelocationStepForTest?.Invoke(RelocationStep.Chunk, done);
        }
    }

    /// <summary>Disarms the relocation record: MoveLength first, a covered store; MoveFrom and MoveDone stop counting with it.</summary>
    private void ClearRelocationLocked(ref WalFileHeader hdr)
    {
        StoreCovered(HeaderField.MoveLength, 0);
        Volatile.Write(ref hdr.MoveFrom, 0);
        Volatile.Write(ref hdr.MoveDone, 0);
    }

    /// <summary>
    /// Finishes a relocation the previous process was killed inside (<see cref="RelocateLocked"/>):
    /// redoes the chunk the header's <see cref="WalFileHeader.MoveDone"/> names and every one after
    /// it, stores the end the commit would have stored, clears the record and plants the marker —
    /// the state the commit had reached just before its barrier — and then makes that state durable
    /// with the barrier the commit did not live to issue. Runs at open, before any walk. A record
    /// whose numbers cannot describe a move inside this file is not acted on — moving bytes by it
    /// could only destroy data — and is cleared with an Error; the walk that follows then decides
    /// where the data ends, as it did before the record existed.
    ///
    /// <para>The generation is left as the header has it — the flushed one, which the commit had
    /// not stamped over yet. It accepts the relocated tail's generation as its successor, so the
    /// replay is the same as after a stamp, and the flushed generation's entries are gone from the
    /// log either way: overwritten by the move, or past its end.</para>
    ///
    /// <para><b>A record in a header that did not verify</b> (<paramref name="trusted"/> false) has
    /// to prove it describes the move a commit made, not merely one that would fit: the claim must
    /// be where that commit left it, with the progress that goes with it — the old end,
    /// <c>from + length</c>, with <c>done</c> a chunk boundary (a multiple of <c>from</c>, or all of
    /// it): the move in flight; or the new end, <c>length</c>, with <c>done == length</c>: the move
    /// finished and its end stored, only the clear missing. A new end with less done is no state a
    /// commit leaves, and redoing from there would read source bytes the later chunks have already
    /// overwritten (the metric WAL's 83f849f). Rot that passes, in two fields no store ties
    /// together, is not a case worth moving bytes for.</para>
    /// </summary>
    private void FinishRelocationLocked(ref WalFileHeader hdr, bool trusted)
    {
        long from = hdr.MoveFrom, length = hdr.MoveLength, done = hdr.MoveDone;
        if (length == 0) return;                         // nothing in flight: the normal case

        bool fits    = from > 0 && length > 0 && done >= 0 && done <= length && from <= _capacity - length;
        bool matches = fits
                    && ((_writeOffset == from + length && (done % from == 0 || done == length))
                     || (_writeOffset == length && done == length));
        if (!fits || (!trusted && !matches))
        {
            _logger?.LogError(
                "Span WAL at {Path} records a relocation it cannot vouch for (from {From}, length {Length}, done {Done}, " +
                "claim {Claim}, capacity {Capacity}, header verifies: {Verifies}); ignoring it — the walk decides where " +
                "the data ends.", _filePath, from, length, done, _writeOffset, _capacity, trusted);
            ClearRelocationLocked(ref hdr);
            return;
        }

        _logger?.LogWarning(
            "Span WAL at {Path}: finishing the relocation of {Length} byte(s) of spans appended during a flush, " +
            "which a stop interrupted after {Done} byte(s).", _filePath, length, done);

        RelocateLocked(ref hdr, from, length, done);
        _writeOffset = length;
        Volatile.Write(ref hdr.WriteOffset, _headerSize + length);
        OnRelocationStepForTest?.Invoke(RelocationStep.EndStored, length);
        ClearRelocationLocked(ref hdr);
        OnRelocationStepForTest?.Invoke(RelocationStep.Cleared, length);
        PlantEndMarkerLocked(length);

        // THE COMMIT'S BARRIER, which the stopped process never reached. Without it the finished move
        // lives in the page cache only, and a power loss before the OS writes it back mixes what the
        // killed process moved, what this open redid and what neither did — the residual a commit
        // closes with this same range before anything else may append. A failure is reported, not
        // thrown: the open must not fail the start, and the log is exactly as correct for this run.
        try
        {
            FlushRangeLocked(0, _headerSize + length + EntryHeaderSize);
            FlushHandle(_fileStream!);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex,
                "Span WAL at {Path}: the relocation finished at open could not be forced to disk; it is in the " +
                "page cache, and a power loss before the OS writes it back can still cost the spans it moved.",
                _filePath);
        }
    }

    /// <summary>
    /// Plants the generation-0 end marker at logical offset <paramref name="at"/> when the slot fits
    /// in the mapping — only the entry header's generation field is written. Caller holds the lock.
    /// </summary>
    private void PlantEndMarkerLocked(long at)
    {
        if (at + ChecksummedHeaderBytes <= _capacity)
            Unsafe.AsRef<SpanWalEntryHeader>(_ptr + _headerSize + at).Generation = 0;
    }

    // ── The header's own checksum (v3) ───────────────────────────────────────

    /// <summary>
    /// The header checksum: CRC32C over bytes [0, 8) and [16, 56) of the header — everything but the
    /// claim — with <see cref="WalFileHeader.MoveFrom"/> and <see cref="WalFileHeader.MoveDone"/>
    /// read as 0 while <see cref="WalFileHeader.MoveLength"/> is 0. That last rule is what makes
    /// every covered change ONE field: arming writes the other two first, invisibly, and disarming
    /// clears MoveLength first, after which they are invisible again (see <see cref="StoreCovered"/>).
    /// The metric WAL's header checksum, field for field (29718e4).
    /// </summary>
    private static uint HeaderChecksum(in WalFileHeader header)
    {
        WalFileHeader h = header;
        if (h.MoveLength == 0) { h.MoveFrom = 0; h.MoveDone = 0; }
        var bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<WalFileHeader>(in h));
        return Crc32c.Append(Crc32c.Append(0, bytes[..8]), bytes[16..56]);
    }

    /// <summary>The header verifies: as it is (<see cref="WalFileHeader.Crc"/>) or as the store in flight when the process stopped was leaving it (<see cref="WalFileHeader.PendingCrc"/>).</summary>
    private static bool HeaderVerifies(in WalFileHeader header)
    {
        uint crc = HeaderChecksum(in header);
        return header.Crc == crc || header.PendingCrc == crc;
    }

    /// <summary>The covered header fields that change after the header is first written. See <see cref="StoreCovered"/>.</summary>
    private enum HeaderField : byte { Generation, MoveLength, MoveDone }

    /// <summary>
    /// Test seam fired by <see cref="StoreCovered"/> after the field is stored and BEFORE the checksum
    /// that vouches for it — the state a process killed between the two leaves. Null in production.
    /// </summary>
    internal Action<string, ulong>? OnCoveredStoreForTest;

    /// <summary>
    /// Test seam fired by <see cref="StoreCovered"/> after <see cref="WalFileHeader.PendingCrc"/> is
    /// stored and BEFORE the field — the other state a process killed inside the store leaves.
    /// Null in production.
    /// </summary>
    internal Action<string, ulong>? OnPendingStoredForTest;

    /// <summary>Test seam: <see cref="OnPendingStoredForTest"/> for the logs this THREAD opens next. Null in production.</summary>
    [ThreadStatic] internal static Action<string, ulong>? t_pendingStoredForNextOpenForTest;

    /// <summary>
    /// THE ONE WAY A COVERED HEADER FIELD CHANGES once the header exists: the checksum of the header
    /// AS IT WILL BE goes into <see cref="WalFileHeader.PendingCrc"/>, then the field, then the same
    /// value into <see cref="WalFileHeader.Crc"/>. A process killed anywhere in that sequence leaves
    /// a header that verifies — before the field, by <c>Crc</c>; after it, by <c>PendingCrc</c> — so
    /// a stop is never mistaken for rot (the metric WAL's 1599bf4: its first version stored the
    /// field and then sealed, and a kill in between made the header fail and the rebuild drop a
    /// watermark that was right). Each of the three is a release store, so the order is the one
    /// written here and not the JIT's. v1 and v2 headers have no checksum; a held header (the open
    /// rebuilding one that did not verify) is sealed once, at the end of that open.
    /// </summary>
    private void StoreCovered(HeaderField field, ulong value)
    {
        ref var h = ref Unsafe.AsRef<WalFileHeader>(_ptr);
        if (_legacyVersion != 0 || _sealsHeld)
        {
            Set(ref h, field, value);
            return;
        }

        WalFileHeader next = h;
        Set(ref next, field, value);
        Volatile.Write(ref h.PendingCrc, HeaderChecksum(in next));
        OnPendingStoredForTest?.Invoke(field.ToString(), value);
        Set(ref h, field, value);
        OnCoveredStoreForTest?.Invoke(field.ToString(), value);
        Volatile.Write(ref h.Crc, h.PendingCrc);

        static void Set(ref WalFileHeader h, HeaderField field, ulong value)
        {
            switch (field)
            {
                case HeaderField.Generation: Volatile.Write(ref h.Generation, (uint)value); break;
                case HeaderField.MoveLength: Volatile.Write(ref h.MoveLength, (long)value); break;
                case HeaderField.MoveDone:   Volatile.Write(ref h.MoveDone,   (long)value); break;
            }
        }
    }

    /// <summary>Set by the open while it rewrites a header that did not verify; see <see cref="OpenOrCreate"/>.</summary>
    private bool _sealsHeld;

    /// <summary>
    /// Stores the header's checksum, both slots, over the header as it stands — after the open has
    /// written it whole (a fresh header, a rebuilt one, or one that verified only by its pending
    /// checksum); a single covered field changes through <see cref="StoreCovered"/> instead. v3 only.
    /// Caller holds the lock, or is the open.
    ///
    /// <para><b>Why a checksum on a header the entries already vouch for.</b> One field of it can hide
    /// every entry without any entry being wrong: the replay accepts the header's generation and its
    /// successor, so a rotted <see cref="WalFileHeader.Generation"/> two or more away from the
    /// entries' makes it skip them all — acknowledged spans dropped by four bytes of header,
    /// silently (#103's second half). And a rotted relocation record would move bytes at the next
    /// open. A header that does not verify has its generation rebuilt from the entries instead
    /// (<see cref="RebuildGenerationLocked"/>), and its record is trusted only in a state a commit
    /// leaves (<see cref="FinishRelocationLocked"/>).</para>
    ///
    /// <para><b>Why not the claim.</b> <see cref="WalFileHeader.WriteOffset"/> moves on every append,
    /// and sealing it would put a hash on the append path for a field the replay already checks
    /// against the data: the walk stops at the first entry that does not verify, wherever the claim
    /// says the log ends.</para>
    ///
    /// <para><b>No ordinary stop leaves a header that does not verify.</b> Every covered change after
    /// the first write is one field through <see cref="StoreCovered"/>, whose two checksum slots
    /// vouch for the header before and after it; what remains is rot, a copied or restored file,
    /// and a process killed while the OPEN was rewriting a header it had already found not to
    /// verify — which the next open rebuilds again.</para>
    /// </summary>
    private void SealHeaderLocked()
    {
        if (_legacyVersion != 0 || _sealsHeld) return;
        ref var h = ref Unsafe.AsRef<WalFileHeader>(_ptr);
        h.PendingCrc = h.Crc = HeaderChecksum(in h);
    }

    /// <summary>
    /// The generation of a header that does not verify (<see cref="SealHeaderLocked"/>), taken back
    /// from the entries — which carry their own checksums, and so are the better witness. The walk
    /// over the claimed range verifies every entry and finds the NEWEST generation among them; the
    /// header gets its predecessor, so the replay accepts the newest and the one before it. That
    /// covers every live entry: the live generations are the header's and its successor, and the
    /// newest entry carries one of the two. If it carries the header's (no flush was in flight), the
    /// generation before it is accepted too — a committed generation still in the file, at most,
    /// which replays beside its segment: duplicates, never loss. With no entry at all the
    /// generation starts over.
    ///
    /// <para>NEWEST, NOT LAST. A restart after a crash mid-flush appends under the header's
    /// generation BEHIND entries of its successor, so the last entry can be the older of the two;
    /// taken as the newest, it would have rebuilt a window that drops the successor's entries. The
    /// order is the generation cycle's (<see cref="IsAfter"/>), not the number's, so a wrap past
    /// <see cref="uint.MaxValue"/> does not read as the oldest.</para>
    ///
    /// <para>An Error says it happened; the open seals the result.</para>
    /// </summary>
    private void RebuildGenerationLocked(ref WalFileHeader hdr)
    {
        uint rotted = hdr.Generation, newest = 0;
        for (long pos = 0, total; (total = EntryAt(pos, _writeOffset, _entryHeaderSize, _checksummed)) > 0; pos += total)
        {
            uint g = Unsafe.AsRef<SpanWalEntryHeader>(_ptr + _headerSize + pos).Generation;
            if (newest == 0 || IsAfter(g, newest)) newest = g;
        }

        _generation    = newest == 0 ? FirstGeneration : Prev(newest);
        hdr.Generation = _generation;

        _logger?.LogError(
            "Span WAL at {Path}: the header does not verify (generation {Generation}); its generation was rebuilt " +
            "from the entries: {NewGeneration}, whose spans and its successor's replay.",
            _filePath, rotted, _generation);
    }

    private static uint Prev(uint g) => g == FirstGeneration ? uint.MaxValue : g - 1;

    /// <summary>
    /// <paramref name="a"/> comes after <paramref name="b"/> on the generation cycle — 1, 2, …,
    /// <see cref="uint.MaxValue"/>, 1, … (0 is never a generation), uint.MaxValue values long — by
    /// less than half of it. The generations a log holds at once are a handful of neighbours, so
    /// "less than half the cycle ahead" is "newer".
    /// </summary>
    private static bool IsAfter(uint a, uint b)
    {
        uint x = a - 1, y = b - 1;                       // positions on the cycle, 0-based
        uint d = x >= y ? x - y : x - y - 1;             // (x - y) mod uint.MaxValue: the wrap added 2^32, the cycle is 2^32 - 1
        return d != 0 && d < 0x8000_0000u;
    }

    // ── Reset ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Drops every logged span by opening a new generation — an atomic Begin+Commit with
    /// an empty tail. Only safe when no append can run concurrently (tests, teardown);
    /// the engine's flush path uses the two-phase protocol above instead.
    /// </summary>
    public void Reset()
    {
        lock (_writeLock)
        {
            if (_disposed || _ptr is null) return;

            // Wrap past 0 — it is the "never written" marker recovery relies on.
            _generation       = Next(_generation);
            _generationBumped = false;
            _flushOpen        = false;
            _flushBoundary    = 0;

            StoreCovered(HeaderField.Generation, _generation);   // must land before the offset
            Volatile.Write(ref Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset, _headerSize);
            _writeOffset = 0;
        }
    }

    // ── Recovery ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Replays every complete, verifying entry belonging to the live generations. The first
    /// entry that is short, unwritten or fails its checksum ends the replay: the tail of an
    /// append-only log is the only place a torn write can be, and everything before it is intact.
    /// </summary>
    public List<SpanIngestItem> ReadAll()
    {
        var result = new List<SpanIngestItem>();

        lock (_writeLock)
        {
            if (_ptr is null) return result;

            long pos = 0;
            long end = _writeOffset;
            long total;

            while ((total = EntryAt(pos, end, _entryHeaderSize, _checksummed)) > 0)
            {
                byte* src = _ptr + _headerSize + pos;
                ref var eh = ref Unsafe.AsRef<SpanWalEntryHeader>(src);

                // TWO generations are live, not one: the HEADER's (committed — or, after
                // a crash mid-flush, the generation whose segment never landed) and its
                // successor (appends made while that flush ran). The header, not the
                // in-memory counter, is the truth: on a live log mid-flush the counter
                // already runs one ahead, and judging by it would hide the very entries
                // an abandoned flush needs replayed. Entries from older, committed
                // generations are skipped — their segments hold them.
                uint committed = Unsafe.AsRef<WalFileHeader>(_ptr).Generation;
                if (eh.Generation == committed || eh.Generation == Next(committed))
                {
                    byte* p = src + _entryHeaderSize;
                    string name = eh.NameLength    > 0 ? Encoding.UTF8.GetString(p, eh.NameLength)    : string.Empty;
                    p += eh.NameLength;
                    string svc  = eh.ServiceLength > 0 ? Encoding.UTF8.GetString(p, eh.ServiceLength) : string.Empty;
                    p += eh.ServiceLength;

                    byte[] attrs = [];
                    if (eh.AttrLength > 0)
                    {
                        attrs = new byte[eh.AttrLength];
                        new ReadOnlySpan<byte>(p, (int)eh.AttrLength).CopyTo(attrs);
                    }

                    result.Add(new SpanIngestItem
                    {
                        TraceId           = TraceId.Parse(new ReadOnlySpan<byte>(src, 16)),  // offset 0 of the entry
                        SpanId            = new SpanId(eh.SpanId),
                        ParentSpanId      = new SpanId(eh.ParentSpanId),
                        StartTimeUnixNano = eh.StartTimeUnixNano,
                        DurationNanos     = eh.DurationNanos,
                        Name              = name,
                        ServiceName       = svc,
                        Kind              = (SpanKind)eh.Kind,
                        Status            = (SpanStatusCode)eh.Status,
                        HttpStatusCode    = eh.HttpStatusCode,
                        AttributesBytes   = attrs,
                    });
                }

                pos += total;
            }

            // THE WALK, NOT THE HEADER, IS WHERE THE LOG ENDS. The header's offset can be
            // stale: a power loss can keep an older header page over newer data pages, so the
            // claim may still cover the region a commit's relocation shortened. Replay handles
            // that correctly — it stops at the generation-0 terminator — but Append starts from
            // _writeOffset, so adopting the stale value would place every new entry PAST that
            // terminator, where the NEXT recovery stops before reaching it: everything ingested
            // since this restart, silently dropped, with no segment carrying it. Truncating here
            // is what makes the first append after a recovery land where the data actually ends.
            // The same holds for an entry that fails its checksum: the next append overwrites it,
            // rather than leaving it in front of everything written after it.
            if (pos < _writeOffset)
            {
                _writeOffset = pos;
                Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = _headerSize + pos;
            }
        }

        return result;
    }

    /// <summary>
    /// The entry at logical offset <paramref name="pos"/>: its total length, or 0 where the log
    /// ends — too short for a header, never written (generation 0), longer than what is left, or
    /// (<paramref name="checksummed"/>, v2 and v3) not matching its CRC. Bounds are checked before
    /// the checksum reads anything, so a garbage length cannot walk it off the mapping. Caller holds
    /// the lock.
    /// </summary>
    private long EntryAt(long pos, long end, int headerSize, bool checksummed)
    {
        if (pos + headerSize > end) return 0;

        byte* src = _ptr + _headerSize + pos;
        ref var eh = ref Unsafe.AsRef<SpanWalEntryHeader>(src);

        // Generation 0 is never written by an append, so it marks the end of real
        // data. Nothing about the span's own fields can serve here: an empty name,
        // an empty service, no attributes and a zero start time are all individually
        // legal, which makes a zero-filled region look like a valid entry.
        if (eh.Generation == 0) return 0;

        long total = (long)headerSize + eh.NameLength + eh.ServiceLength + eh.AttrLength;
        if (pos + total > end) return 0;                        // torn tail
        if (total - headerSize > int.MaxValue) return 0;        // a garbage length past what a span can hold

        if (checksummed)
        {
            uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(src, ChecksummedHeaderBytes));
            crc      = Crc32c.Append(crc, new ReadOnlySpan<byte>(src + headerSize, (int)(total - headerSize)));
            if (crc != Unsafe.ReadUnaligned<uint>(src + ChecksummedHeaderBytes)) return 0;
        }
        return total;
    }

    // ── Grow ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The capacity to grow to so that <paramref name="needed"/> logical bytes fit: DOUBLING while
    /// the log is smaller than <paramref name="stepCap"/>, then one step of <paramref name="stepCap"/>
    /// at a time, and never less than what the append in hand needs. Whole pages.
    ///
    /// <para><b>WHY THE STEP IS BOUNDED.</b> The log holds the unflushed tier — at most the hot tier
    /// plus the tail written while a flush builds — and it is mapped for the process's life and
    /// never shrinks. Pure doubling turned a 64 MB log that needed one more span into a 128 MB
    /// mapping: 50 000 eight-attribute spans are ~34 MB of log, so the two generations of one flush
    /// already sit past 64 MB and the next double lands at 128. Capped at
    /// <see cref="MemoryBudgets.TraceHotTierCapBytes"/> (27 MB, a whole hot tier), the same burst
    /// stops at 86 MB. It is a step and not a ceiling: past it the log keeps growing, because
    /// refusing an append here would drop spans the ring already acknowledged; bounding what the
    /// tier (and so the log) may hold is the tier's byte budget, not this file's.</para>
    /// </summary>
    internal static long NextCapacity(long capacity, long needed, long stepCap)
    {
        long step = Math.Clamp(capacity, 4096, Math.Max(4096, stepCap));
        long next = Math.Max(capacity + step, needed);
        return (next + 4095) & ~4095L;
    }

    private void EnsureCapacityLocked(long needed)
    {
        if (needed > _capacity) Grow(NextCapacity(_capacity, needed, _growthStepCap));
    }

    /// <summary>
    /// Grows the mapped capacity to <paramref name="newCapacity"/>. Windows will not resize a
    /// file while it is mapped, so the old mapping has to go first — which means a failure here
    /// (a full disk, i.e. exactly when a log grows) would otherwise leave the object alive with
    /// no mapping, silently refusing every later append. The old mapping is therefore restored
    /// before the exception is allowed out, so a failed growth costs only the append that
    /// triggered it.
    /// </summary>
    private void Grow(long newCapacity)
    {
        long oldFileSize = _headerSize + _capacity;
        long newFileSize = _headerSize + newCapacity;

        Unmap();
        try
        {
            _fileStream!.SetLength(newFileSize);
            Map(newFileSize);
            _capacity = newCapacity;
        }
        catch
        {
            try
            {
                if (_fileStream!.Length < oldFileSize) _fileStream.SetLength(oldFileSize);
                Map(oldFileSize);
            }
            catch { /* nothing left to restore to — the throw below is the honest signal */ }
            throw;
        }
    }

    /// <summary>Drops the mapping only — <see cref="_fileStream"/> outlives it (Grow re-maps over the same handle).</summary>
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

    // ── Durability ───────────────────────────────────────────────────────────

    /// <summary>
    /// msyncs <c>[from, to)</c> of the mapping, page-aligned outwards, plus the first page (the
    /// file header) when the range does not already start in it. Falls back to the whole view
    /// when the platform call fails or there is none — always correct, just the old cost, and
    /// counted in <see cref="RangeFlushFailures"/> so it cannot pass for the new one. The same
    /// shape as the logs WAL's <c>TryFlushRange</c>. Caller holds the lock.
    /// </summary>
    private void FlushRangeLocked(long from, long to)
    {
        if (TryFlushRange(from, to)) return;
        Interlocked.Increment(ref _rangeFlushFailures);
        _accessor?.Flush();
    }

    private bool TryFlushRange(long from, long to)
    {
        if (_ptr is null) return false;

        long pageSize  = Environment.SystemPageSize;
        long fileSize  = _headerSize + _capacity;
        long alignedTo = Math.Min(fileSize, (to + pageSize - 1) / pageSize * pageSize);

        // The header page, unless the range already starts inside it.
        long rangeStart = from / pageSize * pageSize;
        if (rangeStart >= pageSize && !FlushRegion(0, Math.Min(pageSize, fileSize)))
            return false;

        return FlushRegion(rangeStart, alignedTo - rangeStart);
    }

    private bool FlushRegion(long offset, long length)
    {
        if (length <= 0) return true;

        nint addr = (nint)(_ptr + offset);
        bool ok;
        if (OperatingSystem.IsWindows())
            ok = Native.FlushViewOfFile(addr, (nuint)length);
        else if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            ok = Native.Msync(addr, (nuint)length, OperatingSystem.IsMacOS() ? MsyncSyncMacOS : MsyncSyncLinux) == 0;
        else
            return false;   // unknown platform: let the caller flush the whole view

        if (ok)
        {
            Interlocked.Increment(ref _rangeFlushCount);
            RangeFlushedForTest?.Invoke(offset, length);
        }
        return ok;
    }

    /// <summary>FlushFileBuffers / fsync through the handle — the part that waits out the drive cache.</summary>
    private void FlushHandle(FileStream handle)
    {
        if (HandleFlushHookForTest is { } hook) hook(handle);
        else handle.Flush(flushToDisk: true);
        Interlocked.Increment(ref _handleFlushCount);
    }

    // MS_SYNC. Different numbers on the two Unixes, and passing the wrong one makes msync
    // fail with EINVAL rather than do the wrong thing — which the fallback would then cover,
    // silently, with a whole-view flush every commit.
    private const int MsyncSyncLinux = 4;
    private const int MsyncSyncMacOS = 0x0010;

    private static partial class Native
    {
        [LibraryImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static partial bool FlushViewOfFile(nint lpBaseAddress, nuint dwNumberOfBytesToFlush);

        [LibraryImport("libc", EntryPoint = "msync", SetLastError = true)]
        internal static partial int Msync(nint addr, nuint len, int flags);
    }

    // ── Diagnostics (tests) ──────────────────────────────────────────────────

    private long _rangeFlushCount;
    private long _rangeFlushFailures;
    private long _handleFlushCount;

    /// <summary>Successful range msyncs.</summary>
    internal long RangeFlushCount => Interlocked.Read(ref _rangeFlushCount);

    /// <summary>Flushes that fell back to the WHOLE view because the range call failed. Expected to stay 0.</summary>
    internal long RangeFlushFailures => Interlocked.Read(ref _rangeFlushFailures);

    /// <summary>Drive flushes (FlushFileBuffers / fsync) that returned.</summary>
    internal long HandleFlushCount => Interlocked.Read(ref _handleFlushCount);

    /// <summary>Test seam: called with (file offset, length) after every successful range msync, under the lock.</summary>
    internal Action<long, long>? RangeFlushedForTest;

    /// <summary>
    /// Test seam: when set, called INSTEAD of <c>handle.Flush(flushToDisk: true)</c>, so a test can
    /// fail or park a drive flush. Never set in production.
    /// </summary>
    internal Action<FileStream>? HandleFlushHookForTest;

    /// <summary>The logical capacity currently mapped.</summary>
    internal long CapacityForTest { get { lock (_writeLock) return _capacity; } }

    /// <summary>
    /// The file header as it sits in the map, read WITHOUT the lock — so a seam already running
    /// under it (a <see cref="RangeFlushedForTest"/> callback) can see what the commit has stamped.
    /// </summary>
    internal (ushort Version, uint Generation, long WriteOffset) HeaderForTest
    {
        get
        {
            ref var h = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            return (h.Version, h.Generation, h.WriteOffset);
        }
    }

    // ── Dispose ──────────────────────────────────────────────────────────────

    private bool _disposed;

    public void Dispose()
    {
        lock (_writeLock)
        {
            if (_disposed) return;
            _disposed = true;
            Unmap();   // the view flushes its dirty pages as it is disposed
            try { _fileStream?.Flush(flushToDisk: true); } catch { /* best-effort at end of life */ }
            try { _fileStream?.Dispose(); } catch { }
            _fileStream = null;
        }
    }

    /// <summary>Closes and removes the log file. Used by tests and by a data-directory reset.</summary>
    public void Delete()
    {
        Dispose();
        try { if (File.Exists(_filePath)) File.Delete(_filePath); } catch { /* best-effort */ }
    }
}
