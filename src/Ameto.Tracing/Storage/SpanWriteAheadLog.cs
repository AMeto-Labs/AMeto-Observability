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
/// Format (v2):
/// <code>
///   [File Header — 32 bytes]
///     0   Magic              uint32  "RDSW"
///     4   Version            uint16  2   (1 is still READ: see <see cref="Open(string, long)"/>)
///     6   _pad               uint16
///     8   WriteOffset        int64   next byte to write (absolute, includes this header)
///    16   Generation         uint32  flush generation, see the crash-recovery note below
///    20   _reserved          uint32 + int64
///
///   [Entry 0 …]
///     [Entry Header — 64 bytes, Pack = 1, carries the generation it was written under]
///     [Crc               — uint32, CRC32C over the 64 header bytes + name + service + attrs]
///     [Name UTF-8][ServiceName UTF-8][Attributes msgpack]
/// </code>
///
/// <para><b>v2 is v1 plus a checksum per entry, and nothing else.</b> v1 had none anywhere: the
/// bounds check could only say that the declared lengths FIT, so a torn append — pages of an
/// mmap reaching the disk in whatever order the OS picks — replayed as a span with garbage name,
/// service and attribute bytes straight into the hot tier. That is the shape the metrics WAL was
/// poisoned by on the stand, on the third signal. The CRC is stored after the header and written
/// LAST, over the bytes as they sit in the map, and <see cref="ReadAll"/> stops at the first
/// entry that does not verify — exactly the rule the logs WAL (v4) follows. A stride that
/// desynchronises inside a half-relocated front (see <see cref="CommitFlush"/>) now ends the
/// replay cleanly instead of manufacturing spans.</para>
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
/// the platter. The mapping survives the death of this PROCESS (the page cache is the file's
/// and outlives it); the death of the MACHINE loses whatever the OS had not yet written back,
/// which can be every span since the last segment flush. The per-entry checksum is what keeps
/// that loss a clean cut rather than a replay of garbage. (The logs WAL made the other choice,
/// a timer msync; the trade here was made for the drainer's throughput, and it is this note that
/// makes it a decision rather than an accident.)</para>
///
/// <para><b>Crash recovery.</b> A flush writes the segment first and resets the log second,
/// so a crash between the two would replay spans that are already cold. The flush therefore
/// bumps <see cref="WalFileHeader.Generation"/> BEFORE zeroing the write offset, and recovery
/// keeps only entries stamped with the generation the header now carries. Only a crash
/// landing between the segment write and the generation bump can duplicate spans.</para>
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
    private const ushort WalVersion      = 2;
    private const ushort WalVersionV1    = 1;
    private const int    FileHeaderSize  = 32;

    /// <summary>The 64 bytes of <see cref="SpanWalEntryHeader"/> — every field the checksum covers. All of a v1 entry header.</summary>
    private const int    ChecksummedHeaderBytes = 64;

    /// <summary>A v2 entry header: the checksummed 64 bytes, then the CRC32C over them and the payload.</summary>
    private const int    EntryHeaderSize   = ChecksummedHeaderBytes + sizeof(uint);

    /// <summary>A v1 entry header: no checksum. Read only, and only to upgrade the file it is in.</summary>
    private const int    EntryHeaderSizeV1 = ChecksummedHeaderBytes;

    /// <summary>8 MB holds ~12k eight-attribute spans; the log is reset on every flush, so it grows only under a burst.</summary>
    private const long DefaultCapacity = 8 * 1024 * 1024;

    /// <summary>First generation of a fresh log. 0 is reserved for "never written".</summary>
    private const uint FirstGeneration = 1;

    /// <summary>Where a v1 log is rewritten as v2 before it replaces the original. See <see cref="Open(string, long)"/>.</summary>
    internal const string UpgradeSuffix = ".upgrade.tmp";

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

    private readonly string _filePath;
    private readonly Lock   _writeLock = new();

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

    /// <summary>Set by <see cref="OpenOrCreate"/> when the file on disk is a v1 log; <see cref="Open(string, long)"/> upgrades it.</summary>
    private bool _legacyV1;

    /// <summary>
    /// The entry stride this log reads and appends: <see cref="EntryHeaderSize"/> with a checksum
    /// (v2), or — only for a v1 log whose upgrade could not be committed, see
    /// <see cref="StayV1"/> — <see cref="EntryHeaderSizeV1"/> without one.
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

    private SpanWriteAheadLog(string filePath, long growthStepCap)
    {
        _filePath      = filePath;
        _growthStepCap = Math.Max(4096, growthStepCap);
    }

    /// <summary>
    /// Opens the log, creating it if absent. Growth is bounded by the traces hot-tier budget
    /// (<see cref="MemoryBudgets.TraceHotTierCapBytes"/>) — see <see cref="NextCapacity"/>.
    ///
    /// <para><b>A v1 LOG IS UPGRADED, NOT DISCARDED.</b> The release before this one wrote v1, and
    /// until now an unknown version was treated as a foreign file and re-initialised — which, for
    /// the v1 log a restart after this upgrade finds, would have silently dropped every span the
    /// previous process acknowledged and never flushed. So a v1 file is read with the v1 stride,
    /// rewritten entry for entry as v2 into <c>spans.wal.upgrade.tmp</c> (each entry's bytes
    /// verbatim, now with its checksum), fsynced, and moved over the original. The move is the
    /// commit point: a crash before it leaves the v1 file untouched and the next start upgrades it
    /// again; a crash after it leaves a complete v2 file.</para>
    ///
    /// <para><b>AN UPGRADE THAT CANNOT COMPLETE DOES NOT STOP THE SERVER.</b> This runs in the trace
    /// engine's constructor, where a throw fails the host — once, on every existing install, at the
    /// first start after the upgrade. The rename is retried briefly (an antivirus scanner holding the
    /// fresh copy open is the sharing violation seen on Windows); if the copy cannot be written (a
    /// full disk) or the rename still fails, the log is opened AS v1, in place, untouched: every span
    /// it holds replays, new spans are appended in the v1 layout, the error is logged, and the
    /// upgrade is tried again at the next start. That is exactly the log the previous release ran
    /// with — no checksum — for one more process lifetime; the alternatives were refusing to start,
    /// or re-initialising a file whose spans exist nowhere else.</para>
    ///
    /// <para><b>ROLLING BACK is not symmetric.</b> A release older than v2 treats a v2 log as a
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

        var wal = new SpanWriteAheadLog(filePath, growthStepCap);
        try { wal.OpenOrCreate(initialCapacity); }
        catch { wal.Dispose(); throw; }

        if (!wal._legacyV1)
        {
            // A STALE COPY from an upgrade that died before its rename. Never the only copy of
            // anything: until the rename the v1 log is authoritative, and the rename is atomic. A v1
            // log re-truncates it below; beside a v2 log it is 8 MB+ of garbage nobody would remove.
            DeleteQuietly(tmp);
            return wal;
        }

        long capacity;
        try
        {
            capacity = wal.WriteUpgradedCopy(tmp);
        }
        catch (Exception ex)
        {
            DeleteQuietly(tmp);
            logger?.LogError(ex,
                "The span WAL at {Path} is a v1 log and its v2 copy could not be written; it stays v1 "
              + "(no per-entry checksum) for this run, every span in it replays, and the upgrade is "
              + "retried at the next start", filePath);
            return wal.StayV1();
        }
        wal.Dispose();                                   // the mapping has to go before the file can

        // THE COMMIT POINT of the upgrade.
        if (TryMoveWithRetry(tmp, filePath, io) is { } moveFailure)
        {
            DeleteQuietly(tmp);
            logger?.LogError(moveFailure,
                "The span WAL at {Path} could not be replaced by its v2 copy after {Attempts} attempts; "
              + "it stays v1 (no per-entry checksum) for this run, every span in it replays, and the "
              + "upgrade is retried at the next start", filePath, MoveRetryDelays.Length + 1);

            // Whatever is at the path now — the rename is atomic, so it is the v1 log as it was.
            var legacy = new SpanWriteAheadLog(filePath, growthStepCap);
            try { legacy.OpenOrCreate(initialCapacity); }
            catch { legacy.Dispose(); throw; }
            return legacy._legacyV1 ? legacy.StayV1() : legacy;
        }

        var upgraded = new SpanWriteAheadLog(filePath, growthStepCap);
        try { upgraded.OpenOrCreate(capacity); }
        catch { upgraded.Dispose(); throw; }
        return upgraded;
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
    /// Keeps this v1 log as v1 for the life of the process: reads and appends use the v1 stride, no
    /// checksum is written or checked. Everything else — generations, the two-phase flush, the
    /// terminator — is byte-for-byte the same in both versions, which is what makes this safe.
    /// </summary>
    private SpanWriteAheadLog StayV1()
    {
        _entryHeaderSize = EntryHeaderSizeV1;
        _checksummed     = false;
        return this;
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

    private void OpenOrCreate(long initialCapacity)
    {
        bool exists   = File.Exists(_filePath);
        long fileSize = FileHeaderSize + initialCapacity;

        _fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        if (_fileStream.Length < fileSize) _fileStream.SetLength(fileSize);
        else                               fileSize = _fileStream.Length;   // reopen an already-grown log at its size

        _capacity = fileSize - FileHeaderSize;
        Map(fileSize);

        ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
        bool known = exists && hdr.Magic == MagicNumber && (hdr.Version == WalVersion || hdr.Version == WalVersionV1);
        if (!known)
        {
            // New, foreign or future-versioned file — reinitialise in place. Anything
            // already there cannot be replayed under a layout we do not know.
            hdr.Magic       = MagicNumber;
            hdr.Version     = WalVersion;
            hdr.WriteOffset = FileHeaderSize;
            hdr.Generation  = FirstGeneration;
            _writeOffset    = 0;
            _generation     = FirstGeneration;
        }
        else
        {
            _writeOffset = Math.Max(0, hdr.WriteOffset - FileHeaderSize);
            _generation  = hdr.Generation == 0 ? FirstGeneration : hdr.Generation;
            if (_writeOffset > _capacity) _writeOffset = _capacity;  // truncated file — replay what is mapped
            _legacyV1 = hdr.Version == WalVersionV1;                 // left untouched: Open upgrades it
        }
    }

    /// <summary>
    /// Rewrites this (v1) log as v2 at <paramref name="tmpPath"/> and fsyncs it. Every entry the
    /// v1 walk reaches is copied — header bytes and payload verbatim, the checksum computed over
    /// them — so the upgraded log replays exactly what the v1 log would have, under the same
    /// generation rule. Returns the logical capacity the copy was sized to.
    /// </summary>
    private long WriteUpgradedCopy(string tmpPath)
    {
        lock (_writeLock)
        {
            using var fs = new FileStream(tmpPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
            Span<byte> fileHeader = stackalloc byte[FileHeaderSize];
            fileHeader.Clear();
            fs.Write(fileHeader);                        // placeholder; the real one goes in last

            Span<byte> crcBytes = stackalloc byte[sizeof(uint)];
            long pos = 0, written = 0, total;
            while ((total = EntryAt(pos, _writeOffset, EntryHeaderSizeV1, checksummed: false)) > 0)
            {
                byte* src     = _ptr + FileHeaderSize + pos;
                var   header  = new ReadOnlySpan<byte>(src, ChecksummedHeaderBytes);
                var   payload = new ReadOnlySpan<byte>(src + EntryHeaderSizeV1, (int)(total - EntryHeaderSizeV1));
                uint  crc     = Crc32c.Append(Crc32c.Append(0, header), payload);
                MemoryMarshal.Write(crcBytes, in crc);

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

        int  headerSize = _entryHeaderSize;              // v2, unless the upgrade could not commit
        int  payload    = nameUtf8.Length + serviceUtf8.Length + attrs.Length;
        long entrySize  = (long)headerSize + payload;
        EnsureCapacityLocked(_writeOffset + entrySize);

        byte* dest = _ptr + FileHeaderSize + _writeOffset;

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
        Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + _writeOffset;
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

            long boundary = _flushBoundary;
            long tail     = _writeOffset - boundary;     // appended while the segment was written
            if (tail > 0 && boundary > 0)
                Buffer.MemoryCopy(_ptr + FileHeaderSize + boundary,
                                  _ptr + FileHeaderSize, _capacity, tail);

            // The move does not erase its source, and a crash between the stores below
            // would leave the old offset covering both the relocated tail and the stale
            // originals. A generation-0 marker at the new end stops any replay exactly
            // where the data now ends — the same trick the metric WAL's Compact uses.
            // It is not optional: without room for it the commit has no terminator, so
            // rather than proceed unterminated the log grows to make room.
            EnsureCapacityLocked(tail + EntryHeaderSize);
            Unsafe.AsRef<SpanWalEntryHeader>(_ptr + FileHeaderSize + tail).Generation = 0;

            // ── PERSISTENCE BARRIER. The relocation and the header live on different
            //    pages, and dirty mmap pages reach the platter in whatever order the OS
            //    chooses — program order says nothing across pages. Without this flush a
            //    power loss could persist the committed header FIRST: it would then point
            //    at a front region that still held the old, dead generation, while the
            //    durable originals of the during-flush tail sat beyond the committed
            //    offset, unreachable. Every span appended during the build — tens of
            //    thousands at load — would be lost. Flushed here, a crash before the
            //    header lands leaves the OLD header {flushed gen, old offset} over a front
            //    that now holds the relocated tail followed by the generation-0 marker:
            //    replay reads the tail and stops at the marker. Nothing lost, nothing
            //    duplicated.
            //
            //    THE RANGE IS [0, relocated tail + terminator), page-aligned outwards. It
            //    starts at the file's first byte, so the header page is in it; the tail is
            //    all the commit dirtied besides the header. Nothing past it is claimed by
            //    anything this commit writes.
            try
            {
                FlushRangeLocked(0, FileHeaderSize + tail + EntryHeaderSize);
                FlushHandle(_fileStream!);
            }
            catch (Exception ex)
            {
                // The barrier failed, so the header must NOT claim data we could not
                // persist: it keeps the flushed generation, and the retry's Begin reuses the
                // already-bumped append generation (_generationBumped stays set), so the window
                // never widens.
                //
                // THE RELOCATION ITSELF IS DONE, in memory, and the log now continues from its
                // end. The version of this path before v2 left the write offset where it was —
                // past the generation-0 marker just written at the end of the relocated tail —
                // so every span appended after a failed barrier landed beyond a terminator that
                // the next replay stops at: acknowledged, queryable, and gone at the next
                // restart. Pointing the offset at the relocated end puts the next append over
                // that marker instead. The flushed generation's spans are not in the log any
                // more, which is fine: their segment was published before this commit ran.
                _writeOffset = tail;
                Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + tail;
                _flushOpen     = false;
                _flushBoundary = 0;
                throw new IOException(
                    "span WAL commit barrier failed; the log keeps the flushed generation in its header", ex);
            }

            ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            hdr.Generation  = _generation;               // must land before the offset store
            hdr.WriteOffset = FileHeaderSize + tail;

            // Only now is the cycle over: every step that can throw is behind us, so the
            // in-memory generation and the header can no longer drift apart (a Begin that
            // bumped a second time would stamp appends the header's acceptance window does
            // not cover, and ReadAll would drop them).
            _writeOffset      = tail;
            _flushBoundary    = 0;
            _flushOpen        = false;
            _generationBumped = false;

            // Commit the header itself, so the dead generation cannot come back after a
            // power loss and be replayed into duplicates of a segment already on disk. The
            // page goes to the filesystem here, under the lock; the drive flush below.
            FlushRangeLocked(0, FileHeaderSize);
            handle = _fileStream;

            // RESIDUAL WINDOW, now bounded by the checksum: a crash INSIDE the first flush can
            // leave the front half-relocated — some pages the new tail, some still the old
            // generation — under the old header, where a stride can desync mid-region. In v1
            // that replayed garbage; in v2 the first entry that does not verify ends the
            // replay. What it can still cost is the relocated entries past the torn page.
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

            ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            hdr.Generation  = _generation;                // must land before the offset
            hdr.WriteOffset = FileHeaderSize;
            _writeOffset    = 0;
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
                byte* src = _ptr + FileHeaderSize + pos;
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
            // stale by design: a crash after a commit relocated the tail but before its
            // header store leaves an offset covering the region the relocation shortened.
            // Replay handles that correctly — it stops at the generation-0 terminator — but
            // Append starts from _writeOffset, so adopting the stale value would place every
            // new entry PAST that terminator, where the NEXT recovery stops before reaching
            // it: everything ingested since this restart, silently dropped, with no segment
            // carrying it. Truncating here is what makes the first append after a recovery
            // land where the data actually ends. The same holds for an entry that fails its
            // checksum: the next append overwrites it, rather than leaving it in front of
            // everything written after it.
            if (pos < _writeOffset)
            {
                _writeOffset = pos;
                Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + pos;
            }
        }

        return result;
    }

    /// <summary>
    /// The entry at logical offset <paramref name="pos"/>: its total length, or 0 where the log
    /// ends — too short for a header, never written (generation 0), longer than what is left, or
    /// (<paramref name="checksummed"/>, v2) not matching its CRC. Bounds are checked before the
    /// checksum reads anything, so a garbage length cannot walk it off the mapping. Caller holds
    /// the lock.
    /// </summary>
    private long EntryAt(long pos, long end, int headerSize, bool checksummed)
    {
        if (pos + headerSize > end) return 0;

        byte* src = _ptr + FileHeaderSize + pos;
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
        long oldFileSize = FileHeaderSize + _capacity;
        long newFileSize = FileHeaderSize + newCapacity;

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
        long fileSize  = FileHeaderSize + _capacity;
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
