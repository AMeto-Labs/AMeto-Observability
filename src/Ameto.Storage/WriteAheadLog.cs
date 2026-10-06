using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Write-Ahead Log backed by a memory-mapped file.
///
/// Format (v5):
///   [WAL Header   — 32 bytes]
///   [Entry 0 …]
///     [Entry Header — 26 bytes: payloadLen uint32, timestamp int64, level byte, flags byte,
///      templateIndex uint16, exceptionLen uint32, serviceIndex uint16, crc32c uint32]
///     [Entry Payload — raw msgpack bytes][Exception — msgpack ExceptionInfo]
///   [Entry 1 …]
///   ...
///
/// Flags (byte 13 of the entry header, inside the checksummed range):
///   bit 0 — Unpooled: the event's template was outside the template pool (the pool was full,
///           or the index was past its 65 536 ids). templateIndex is then 0 and meaningless,
///           no pool row was written, and the template TEXT is not in the WAL at all, so
///           recovery yields the event with no template. Without the bit, a replay whose pool
///           file held any row resolved index 0 and gave the event index 0's template.
///   bit 1 — Service (v5): serviceIndex is the event's <c>@service</c>, an index into the same
///           intern pool the template's is, and the index's text is a row of the companion pool
///           file, written the first time this WAL logs the index. Without the bit the event has
///           no service and serviceIndex is 0 and meaningless: all 65 536 values are real pool
///           ids, so the index alone cannot say "none" — the same reason the template has a flag.
///   Other bits are reserved and written as zero.
/// The byte was unwritten padding before the Unpooled flag existed, and the format stayed v4 for
/// it: the build before never set it, and the engine opens every WAL as a fresh file named after
/// a newly reserved segment block and extends it with SetLength, which zero-fills. Entries that
/// build wrote therefore read back with no flag and replay exactly as they did. (Only a
/// same-name reopen that reset the write offset over older entries could leave a stale byte
/// there, and the engine never reuses a WAL name.) Append writes the byte explicitly.
///
/// The service needed a FIELD, not only a bit, so it is a new format (see WalVersion). Before it,
/// an event's service lived in its header alone — the hot tier's, and the segment's once flushed —
/// and an event replayed out of a WAL came back with none: an OTLP event's resource service.name,
/// a CLEF event's @service. A v4 file still replays, its events without a service, as they always
/// did; the previous releases cannot read a v5 file (docs/CONFIGURATION.md, Upgrading and rolling
/// back).
///
/// The WAL is append-only. On crash recovery, the storage layer replays complete entries
/// and rebuilds the hot-tier up to the last entry whose checksum verifies.
///
/// Durability: appends land in the OS page cache; <see cref="Flush"/> msyncs the mapping
/// (StorageEngine drives it on a timer) and rotation/dispose flush the view. Per-entry
/// CRC32C means a crash mid-write-back — pages reaching disk in any order — truncates
/// replay at the first torn entry instead of manufacturing garbage events.
/// </summary>
public sealed unsafe partial class WriteAheadLog : IDisposable
{
    // ── WAL file header ──────────────────────────────────────────────────────
    private const uint   MagicNumber    = 0x52_44_57_41; // "RDWA"
    // v3: WalEntryHeader is Pack=1 so the header truly occupies its stride.
    // v2 files had ExceptionLength laid out PAST the 20-byte header (alignment padding
    // before TimestampTicks) where the payload copy immediately overwrote it — recovery
    // read garbage lengths and always bailed with zero entries. v2 files are therefore
    // unrecoverable by construction and are reset on open.
    // v4: appends a CRC32C over header+payload to every entry. mmap dirty pages hit disk
    // in arbitrary order, so after power loss the header's WriteOffset can cover pages
    // that never made it — which parse as garbage (or, worse, as endless zero-length
    // entries). The checksum turns that into a clean stop at the last durable entry.
    // v5: the entry carries the event's service — a 16-bit pool index ahead of the checksum,
    // and the Service flag — so a replayed event gets its @service back (#111). The header
    // grows 24 → 26 bytes; the checksum is still its last field and covers everything before it.
    // v4 and v3 files remain READABLE in recovery (v3 without checksum validation, neither with
    // a service); new files are v5.
    private const ushort WalVersion        = 5;
    private const ushort WalVersionV4      = 4;
    private const ushort WalVersionV3      = 3;
    private const int    FileHeaderSize    = 32;
    private const int    EntryHeaderSize   = 26;
    private const int    EntryHeaderSizeV4 = 24;
    private const int    EntryHeaderSizeV3 = 20;
    // Bytes of the entry header covered by the checksum (everything except the crc itself).
    private const int    ChecksummedHeaderBytes = EntryHeaderSize - 4;
    // WalEntryHeader.Flags (see the class doc): the event's template is outside the pool …
    private const byte   EntryFlagUnpooled = 0x01;
    // … and the event has a service, named by ServiceIndex (v5).
    private const byte   EntryFlagService  = 0x02;

    [StructLayout(LayoutKind.Sequential, Size = FileHeaderSize)]
    private struct WalFileHeader
    {
        public uint   Magic;
        public ushort Version;
        public uint   NodeId;
        public ulong  SegmentId;
        public long   WriteOffset;    // next byte to write (maintained in memory + updated in place)
        private short _pad;
    }

    // Pack = 1 is essential: without it TimestampTicks aligns to 8, pushing
    // ExceptionLength past the intended stride and straight into the payload area
    // (the v2 corruption described at WalVersion). Packed, the fields occupy exactly
    // 4+8+1+1+2+4+2+4 = 26 bytes, with Checksum LAST so the checksum covers [0, 22).
    // v4 and v3 share the first 20 bytes; v4 kept its checksum at [20, 24), where v5 has the
    // service index, and v3 had none — so the parser reads the checksum by offset, not by field.
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = EntryHeaderSize)]
    private struct WalEntryHeader
    {
        public uint   PayloadLength;
        public long   TimestampTicks;
        public byte   Level;
        public byte   Flags;           // EntryFlag* bits; was never-written padding (see the class doc)
        public ushort TemplateIndex;   // index into companion .pool file; 0 and meaningless when Unpooled
        public uint   ExceptionLength; // bytes of msgpack ExceptionInfo appended after payload
        public ushort ServiceIndex;    // v5: @service's index into the same pool; 0 and meaningless without the Service flag
        public uint   Checksum;        // CRC32C over header[0..22) + payload + exception
    }

    /// <summary>The entry layouts recovery reads: this format's and the two before it.</summary>
    private enum EntryLayout : byte { V3, V4, V5 }

    private static int HeaderSizeOf(EntryLayout layout) => layout switch
    {
        EntryLayout.V3 => EntryHeaderSizeV3,
        EntryLayout.V4 => EntryHeaderSizeV4,
        _              => EntryHeaderSize,
    };

    // ── State ────────────────────────────────────────────────────────────────
    private readonly string              _filePath;
    public  string FilePath => _filePath;
    // Held open for the WAL's whole life: the mapping is created over it, Grow extends it,
    // and Flush issues FlushFileBuffers through it — on Windows FlushViewOfFile alone
    // writes pages to the filesystem but does NOT wait out the drive cache, so without
    // this handle the periodic msync would not actually be power-loss durable.
    private          FileStream?         _fileStream;
    private          MemoryMappedFile?   _mmf;
    private          MemoryMappedViewAccessor? _accessor;
    private          byte*               _ptr;
    private          long                _capacity;
    private          long                _writeOffset; // logical, excludes file header
    // Absolute file offset (header included) up to which the mapping has been msynced AND the
    // file handle flushed. Everything past it is not yet durable; when it equals the write
    // offset the tick has nothing to do.
    private          long                _lastFlushedOffset = FileHeaderSize;
    private readonly object              _writeLock = new();
    private          FileStream?          _poolStream;
    private          bool                 _poolDirty;
    // Pool indices this WAL has written a row for — template or service: they share one intern
    // pool, so an index names one string whichever of the two it is, and one row serves both.
    private readonly bool[]               _savedPoolRows = new bool[65536];
    private readonly object               _poolLock = new();

    public string PoolPath => _filePath + ".pool";

    // ── Construction ─────────────────────────────────────────────────────────

    public static WriteAheadLog Open(string filePath, NodeId nodeId, SegmentId segmentId, long initialCapacity = 64 * 1024 * 1024)
    {
        var wal = new WriteAheadLog(filePath);
        wal.OpenOrCreate(nodeId, segmentId, initialCapacity);
        return wal;
    }

    private WriteAheadLog(string filePath) => _filePath = filePath;

    private unsafe void OpenOrCreate(NodeId nodeId, SegmentId segmentId, long initialCapacity)
    {
        bool exists = File.Exists(_filePath);

        // Ensure file exists and has the right size. An existing file may be LARGER than
        // requested (it grew in a previous run) — the mapping must cover the whole file,
        // or a recovered WriteOffset beyond the requested size would walk off the view.
        long fileSize = FileHeaderSize + initialCapacity;
        _fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        if (_fileStream.Length < fileSize)
            _fileStream.SetLength(fileSize);
        else
            fileSize = _fileStream.Length;

        _capacity = fileSize - FileHeaderSize;
        Map(fileSize);

        if (!exists)
        {
            // Write file header
            ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            hdr.Magic       = MagicNumber;
            hdr.Version     = WalVersion;
            hdr.NodeId      = nodeId.Value;
            hdr.SegmentId   = segmentId.Value;
            hdr.WriteOffset = FileHeaderSize;
            _writeOffset    = 0;
        }
        else
        {
            ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            if (hdr.Magic != MagicNumber || hdr.Version != WalVersion)
            {
                // Unknown or older version: live appends need the v5 layout, and pre-v3
                // entries are unreplayable by construction — reinitialise in place.
                // (Orphaned v4 and v3 files are still replayed by ReadForRecovery, which handles
                // their strides; this path is a same-name reopen, which recovery precedes.)
                hdr.Magic       = MagicNumber;
                hdr.Version     = WalVersion;
                hdr.NodeId      = nodeId.Value;
                hdr.SegmentId   = segmentId.Value;
                hdr.WriteOffset = FileHeaderSize;
                _writeOffset    = 0;
            }
            else
            {
                // Recover write position from header. A torn/corrupt header can claim
                // any value — clamp to the mapped range so no append or read walks
                // off the view (unclamped, the first Append after reopen wrote past
                // the mapping: an uncatchable AccessViolation).
                _writeOffset = hdr.WriteOffset - FileHeaderSize;
                if (_writeOffset < 0 || _writeOffset > _capacity)
                {
                    _writeOffset    = 0;
                    hdr.WriteOffset = FileHeaderSize;
                }
            }
        }

        // Whatever came back from the file is already on disk, so the first tick has nothing
        // to msync up to here. A fresh file starts at the header, which Flush always covers.
        _lastFlushedOffset = FileHeaderSize + _writeOffset;

        // Open companion pool file (template index → string) for crash recovery
        _poolStream = new FileStream(_filePath + ".pool",
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _poolStream.Seek(0, SeekOrigin.End);
    }

    // ── Append ───────────────────────────────────────────────────────────────

    // Reused per thread: exception msgpack scratch — the bytes are copied into the mmap
    // below, so nothing outlives the call. Avoids a byte[] per exception-carrying event.
    [ThreadStatic] private static System.Buffers.ArrayBufferWriter<byte>? _tExc;

    /// <summary>
    /// Appends a single event payload to the WAL. Thread-safe via lock.
    /// Fast path: a single Span copy into the mmap region.
    /// </summary>
    /// <param name="templateIndex">
    /// The event's template-pool index. Anything outside <c>[0, 65 535]</c> (-1 once the pool
    /// is full, or a claim past its cap) is logged as UNPOOLED: the entry carries the Unpooled
    /// flag with index 0, and <paramref name="template"/> is ignored, so no pool row is written
    /// for it. Recovery then yields that event with no template.
    /// </param>
    /// <param name="serviceIndex">
    /// The event's <c>@service</c>, as <see cref="LogEventHeader.ServiceNamePoolIndex"/> holds it:
    /// an index into the same pool. -1, or anything else outside <c>[0, 65 535]</c>, logs the event
    /// with no service — the hot tier has nothing else to resolve a service from either.
    /// </param>
    /// <param name="service">
    /// The text at <paramref name="serviceIndex"/>, written as a pool row the first time this WAL
    /// logs that index. Null or empty writes none; recovery then gives the event no service unless
    /// another of this WAL's events wrote the row.
    /// </param>
    public unsafe void Append(long timestampTicks, LogLevel level, int templateIndex, string template, ReadOnlySpan<byte> payload,
                              ExceptionInfo? exception = null, int serviceIndex = -1, string? service = null)
    {
        // One unsigned compare covers both -1 and anything past the pool's 65 536 ids.
        bool   pooled = (uint)templateIndex <= ushort.MaxValue;
        ushort index  = pooled ? (ushort)templateIndex : (ushort)0;
        if (pooled) EnsurePoolRow(index, template);

        // The same compare for the service, whose "outside the pool" is simply "none": a full pool
        // answers -1 at ingest and the header keeps no text for it, unlike the template.
        bool   hasService = (uint)serviceIndex <= ushort.MaxValue;
        ushort svcIndex   = hasService ? (ushort)serviceIndex : (ushort)0;
        if (hasService) EnsurePoolRow(svcIndex, service);

        ReadOnlySpan<byte> excBytes = default;
        if (exception is not null)
        {
            var buf = _tExc ??= new System.Buffers.ArrayBufferWriter<byte>(256);
            buf.Clear();
            var w = new MessagePack.MessagePackWriter(buf);
            exception.Write(ref w);
            w.Flush();
            excBytes = buf.WrittenSpan;
        }
        int entrySize = EntryHeaderSize + payload.Length + excBytes.Length;

        lock (_writeLock)
        {
            // A writer that captured this WAL just before rotation can arrive after the
            // flush thread disposed it. Throwing (not dereferencing a released mapping)
            // is the contract — the caller's event is already in the frozen hot tier.
            ObjectDisposedException.ThrowIf(_disposed, this);
            // Distinct from disposal on purpose: a LIVE WAL left unmapped by a failed Grow
            // (restore path also failed) must not masquerade as the benign rotation race —
            // the engine logs IOException and forces a rotation, but swallows ODE.
            if (_ptr is null)
                throw new IOException("WAL is unmapped after a failed grow — appends unavailable until rotation");

            if (_writeOffset + entrySize > _capacity)
                Grow();

            byte* dest = _ptr + FileHeaderSize + _writeOffset;

            ref var eh = ref Unsafe.AsRef<WalEntryHeader>(dest);
            eh.PayloadLength   = (uint)payload.Length;
            eh.TimestampTicks  = timestampTicks;
            eh.Level           = (byte)level;
            // Written explicitly, set or not, like ServiceIndex below: the bytes under a reset
            // write offset are not guaranteed zero, and the checksum below covers whatever is here.
            eh.Flags           = (byte)((pooled ? 0 : EntryFlagUnpooled) | (hasService ? EntryFlagService : 0));
            eh.TemplateIndex   = index;
            eh.ExceptionLength = (uint)excBytes.Length;
            eh.ServiceIndex    = svcIndex;

            // Checksum the header bytes (crc field excluded — it is the last 4 bytes)
            // plus both data spans, BEFORE copying them: same bytes, and the header part
            // is already in place.
            uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(dest, ChecksummedHeaderBytes));
            crc      = Crc32c.Append(crc, payload);
            crc      = Crc32c.Append(crc, excBytes);
            eh.Checksum = crc;

            if (payload.Length > 0)
                payload.CopyTo(new Span<byte>(dest + EntryHeaderSize, payload.Length));
            if (excBytes.Length > 0)
                excBytes.CopyTo(new Span<byte>(dest + EntryHeaderSize + payload.Length, excBytes.Length));

            _writeOffset += entrySize;

            // Update offset in file header (in-place, no flush)
            ref var fh = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            fh.WriteOffset = FileHeaderSize + _writeOffset;
        }
    }

    /// <summary>
    /// Writes the pool row <paramref name="index"/> → <paramref name="text"/> unless this WAL already
    /// has. The row is in the OS before the entry that needs it is in the mapping, so a process kill
    /// leaves no entry without its row; only the periodic fsync (<see cref="FlushPool"/>) makes it
    /// survive a power loss.
    /// </summary>
    private void EnsurePoolRow(ushort index, string? text)
    {
        if (string.IsNullOrEmpty(text) || _savedPoolRows[index]) return;
        lock (_poolLock)
        {
            if (_savedPoolRows[index]) return;
            _savedPoolRows[index] = true;
            if (_poolStream is null) return;
            var bytes = Encoding.UTF8.GetBytes(text);
            var len   = Math.Min(bytes.Length, ushort.MaxValue);
            Span<byte> hdr = stackalloc byte[4];
            BinaryPrimitives.WriteUInt16LittleEndian(hdr, index);
            BinaryPrimitives.WriteUInt16LittleEndian(hdr[2..], (ushort)len);
            _poolStream.Write(hdr);
            _poolStream.Write(bytes, 0, len);
            _poolStream.Flush();
            _poolDirty = true;
        }
    }

    // ── Durability ───────────────────────────────────────────────────────────

    /// <summary>
    /// msyncs the bytes appended since the previous tick and fsyncs the template pool if it
    /// grew. Called on a timer by the storage engine: acknowledged events are durable within
    /// one interval of power loss instead of "whenever the OS writes back" (up to the tier's
    /// whole life).
    ///
    /// <para>Three things this does NOT do any more. It does not msync the WHOLE mapping —
    /// <c>MemoryMappedViewAccessor.Flush</c> hands FlushViewOfFile/msync the entire 64 MB+
    /// view, and that cost scales with the mapping, not with what was written, so a tick that
    /// appended 40 KB walked 64 MB of page tables. It does not hold <see cref="_writeLock"/>
    /// across the file-handle flush, which is the part that waits out the drive cache and the
    /// part the single appender was stalled behind. And it does no I/O at all on a tick where
    /// nothing was appended since the last SUCCESSFUL flush — the idle case, which used to
    /// msync and fsync regardless. A tick whose handle flush threw is not a success: the
    /// following tick repeats it even if nothing new arrived.</para>
    ///
    /// <para>The first page is always in the range: <see cref="Append"/> updates the file
    /// header's WriteOffset in place, and recovery replays only up to that value, so a durable
    /// tail under a stale header is a tail that is never read back.</para>
    ///
    /// <para>The pool fsync runs even when the WAL half throws. If both fail, the WAL's
    /// exception is the one that propagates (see <see cref="FlushPool"/>).</para>
    /// </summary>
    public void Flush()
    {
        // The pool is fsynced in a finally because the WAL half can throw on EVERY tick: a drive
        // whose FlushFileBuffers keeps failing leaves the watermark where it was, so each tick,
        // idle or not, retries the handle flush and throws again. With the pool after it in
        // straight-line code, a dirty pool on an idle WAL was then never fsynced, and after power
        // loss replay brought the WAL's events back with no template or another event's. (Before
        // the watermark waited for the handle flush, the idle tick after a failure skipped the
        // handle and reached the pool by accident.) The finally runs after FlushTail has released
        // _writeLock, so the pool fsync still never holds up the appender.
        bool walFlushed = false;
        try
        {
            FlushTail();
            walFlushed = true;
        }
        finally
        {
            FlushPool(walFailed: !walFlushed);
        }
    }

    /// <summary>The WAL half of <see cref="Flush"/>: msync the un-synced tail, then flush the file handle.</summary>
    private void FlushTail()
    {
        FileStream? handle   = null;
        long        writeEnd = 0;
        lock (_writeLock)
        {
            if (_disposed || _accessor is null) return;

            writeEnd = FileHeaderSize + _writeOffset;
            if (writeEnd > _lastFlushedOffset)
            {
                if (!TryFlushRange(_lastFlushedOffset, writeEnd))
                {
                    // Correct, just slower — and silent, which is the trap: a platform where
                    // the range call always fails would msync the whole mapping every tick
                    // for ever and look exactly like a working one. The counter records it,
                    // but only tests read it today. The WAL has no logger and no diagnostics
                    // surface exposes the counter, so on a live host this is still invisible.
                    // Surfacing it (a log once, or a StorageEngine diagnostic) is follow-up work.
                    Interlocked.Increment(ref _rangeFlushFailures);
                    _accessor.Flush();     // fallback: whole view, as before
                }

                // The watermark is NOT advanced here. It means "durable up to", and nothing is
                // durable until the handle flush below returns. Advancing it first made a failed
                // FlushFileBuffers permanent while the WAL sat idle: the next tick saw
                // writeEnd == watermark and did nothing, so the acknowledged tail stayed in the
                // drive cache until another event arrived or the WAL rotated.
                handle = _fileStream;
            }
        }

        // Outside the lock on purpose: FlushViewOfFile only queues the pages to the
        // filesystem, FlushFileBuffers is what waits for the drive — milliseconds on a
        // spinning disk, and the appender has no reason to wait with it. On Linux the
        // msync above is already MS_SYNC and this fsync is cheap.
        if (handle is not null)
        {
            // Dispose may have closed the handle between the lock and here (rotation runs
            // on this same thread today, but the flag is the contract, not the thread).
            // An IOException propagates to the flush loop, which logs it. The watermark stays
            // where it was, so the next tick, idle or not, re-issues both flushes; the pool
            // fsync in Flush's finally still runs on every one of those ticks.
            bool flushed = false;
            try
            {
                if (HandleFlushHookForTest is { } hook) hook(handle);
                else handle.Flush(flushToDisk: true);
                flushed = true;
            }
            catch (ObjectDisposedException) { /* Dispose fsyncs it on its way out */ }

            if (flushed)
            {
                Interlocked.Increment(ref _handleFlushCount);
                lock (_writeLock)
                {
                    // Never backwards: a concurrent Flush may already have advanced it further.
                    if (writeEnd > _lastFlushedOffset) _lastFlushedOffset = writeEnd;
                }
            }
        }
    }

    /// <summary>
    /// fsyncs the template pool if a row was written since its last successful fsync. A failed
    /// fsync leaves it dirty, whatever it threw, so the next tick retries it.
    /// </summary>
    /// <param name="walFailed">
    /// The WAL half's exception is already on its way out of <see cref="Flush"/>. That is the
    /// one the flush loop logs: a pool failure thrown from the finally would replace it, and
    /// the log would name the pool while the WAL tail went on failing unreported. So a pool
    /// failure is swallowed here and the pool stays dirty for the next tick. (An IOException
    /// from the pool was already swallowed and retried; this extends the retry to whatever else
    /// it throws, which reaches the loop only on a tick where the WAL half succeeded.)
    /// </param>
    private void FlushPool(bool walFailed)
    {
        lock (_poolLock)
        {
            if (!_poolDirty || _poolStream is null) return;
            try
            {
                if (PoolFlushHookForTest is { } hook) hook(_poolStream);
                else _poolStream.Flush(flushToDisk: true);
                // Cleared only on success. Rows are written under this same lock, so nothing
                // can dirty the pool between the fsync returning and this line.
                _poolDirty = false;
                Interlocked.Increment(ref _poolFlushCount);
            }
            catch (IOException) { /* retried next interval */ }
            catch when (walFailed) { /* retried next interval; the WAL's exception is the one reported */ }
        }
    }

    /// <summary>
    /// msyncs <c>[from, to)</c> of the mapping, page-aligned outwards, plus the first page
    /// (the file header). Returns false if the platform call fails or the platform is one we
    /// have no range call for — the caller then flushes the whole view, which is always
    /// correct, just slower.
    /// </summary>
    private bool TryFlushRange(long from, long to)
    {
        if (_ptr is null) return false;

        long pageSize  = Environment.SystemPageSize;
        long fileSize  = FileHeaderSize + _capacity;
        long alignedTo = Math.Min(fileSize, (to + pageSize - 1) / pageSize * pageSize);

        // The header page, unless the tail range already starts inside it.
        long tailStart = from / pageSize * pageSize;
        if (tailStart >= pageSize && !FlushRegion(0, Math.Min(pageSize, fileSize)))
            return false;

        return FlushRegion(tailStart, alignedTo - tailStart);
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
            Interlocked.Exchange(ref _lastRangeFlushBytes, length);
        }
        return ok;
    }

    // MS_SYNC. Different numbers on the two Unixes, and passing the wrong one makes msync
    // fail with EINVAL rather than do the wrong thing — which the fallback would then cover,
    // silently, with a whole-view flush every tick.
    private const int MsyncSyncLinux = 4;
    private const int MsyncSyncMacOS = 0x0010;

    private static partial class Native
    {
        [System.Runtime.InteropServices.LibraryImport("kernel32.dll", SetLastError = true)]
        [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
        internal static partial bool FlushViewOfFile(nint lpBaseAddress, nuint dwNumberOfBytesToFlush);

        [System.Runtime.InteropServices.LibraryImport("libc", EntryPoint = "msync", SetLastError = true)]
        internal static partial int Msync(nint addr, nuint len, int flags);
    }

    // ── Flush diagnostics (tests) ────────────────────────────────────────────

    private long _rangeFlushCount;
    private long _lastRangeFlushBytes;
    private long _rangeFlushFailures;

    /// <summary>Number of successful range msyncs issued. A clean tick must not raise it.</summary>
    internal long RangeFlushCount => Interlocked.Read(ref _rangeFlushCount);

    /// <summary>
    /// Ticks that fell back to flushing the whole view because the range call failed.
    /// Expected to stay at zero; anything else means every tick is paying the old cost.
    /// </summary>
    internal long RangeFlushFailures => Interlocked.Read(ref _rangeFlushFailures);

    /// <summary>Bytes covered by the most recent range msync.</summary>
    internal long LastRangeFlushBytes => Interlocked.Read(ref _lastRangeFlushBytes);

    private long _handleFlushCount;

    /// <summary>File-handle flushes (FlushFileBuffers / fsync) that returned successfully.</summary>
    internal long HandleFlushCount => Interlocked.Read(ref _handleFlushCount);

    /// <summary>
    /// Test seam: when set, <see cref="Flush"/> calls this instead of
    /// <c>handle.Flush(flushToDisk: true)</c>, so a test can make the drive flush fail.
    /// Never set in production.
    /// </summary>
    internal Action<FileStream>? HandleFlushHookForTest;

    private long _poolFlushCount;

    /// <summary>Template-pool fsyncs that returned successfully.</summary>
    internal long PoolFlushCount => Interlocked.Read(ref _poolFlushCount);

    /// <summary>
    /// Test seam: when set, <see cref="Flush"/> calls this instead of the pool's
    /// <c>Flush(flushToDisk: true)</c>, so a test can watch or fail the pool fsync.
    /// Never set in production.
    /// </summary>
    internal Action<FileStream>? PoolFlushHookForTest;

    /// <summary>
    /// Absolute file offset up to which this WAL is durable: msynced AND its file handle
    /// flushed. It stays behind a failed handle flush, so the next tick retries it.
    /// </summary>
    internal long LastFlushedOffset { get { lock (_writeLock) return _lastFlushedOffset; } }

    /// <summary>Bytes appended so far, excluding the file header.</summary>
    internal long WrittenBytes { get { lock (_writeLock) return _writeOffset; } }

    // ── Recovery ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Replays all complete entries. Used on startup when a cold segment is missing.
    /// </summary>
    public IEnumerable<WalEntry> ReadAll()
    {
        long pos = 0;
        long end = _writeOffset;

        while (pos + EntryHeaderSize <= end)
        {
            if (!TryParseLiveEntry(pos, end, out var entry, out long entrySize))
                yield break;

            yield return entry!;
            pos += entrySize;
        }
    }

    // Iterator bodies cannot touch pointers — this shim reads _ptr outside the iterator.
    private bool TryParseLiveEntry(long pos, long end, out WalEntry? entry, out long entrySize)
        => TryParseEntry(_ptr, pos, end, EntryLayout.V5, out entry, out entrySize);

    /// <summary>
    /// Parses one entry at <paramref name="pos"/>. Bounds are checked and (v4, v5) the CRC is
    /// verified over the in-map spans BEFORE anything is allocated, so a garbage length
    /// field cannot OOM and a torn entry cannot materialise. Returns false at the first
    /// entry that does not verify — everything past it is by definition not durable.
    /// </summary>
    private static unsafe bool TryParseEntry(
        byte* basePtr, long pos, long end, EntryLayout layout,
        out WalEntry? entry, out long entrySize)
    {
        entry     = null;
        entrySize = 0;

        int headerSize = HeaderSizeOf(layout);
        if (pos + headerSize > end) return false;

        byte* src = basePtr + FileHeaderSize + pos;
        // Every layout shares the first 20 bytes; the struct is read past them only for v5.
        ref var eh = ref Unsafe.AsRef<WalEntryHeader>(src);

        long total = (long)headerSize + eh.PayloadLength + eh.ExceptionLength;
        if (pos + total > end)
            return false;
        // Span construction takes int — in a WAL past 2 GiB a garbage length in
        // [2^31, 2^32) passes the long-math bounds check above and the unchecked cast
        // below would go negative: an ArgumentOutOfRangeException instead of the clean
        // stop this parser promises. Such a length is by definition a torn entry.
        if (eh.PayloadLength > int.MaxValue || eh.ExceptionLength > int.MaxValue)
            return false;

        var payloadSpan = new ReadOnlySpan<byte>(src + headerSize, (int)eh.PayloadLength);
        var excSpan     = new ReadOnlySpan<byte>(src + headerSize + (int)eh.PayloadLength, (int)eh.ExceptionLength);

        // v4 and v5 end their header with the checksum, over every header byte before it and both
        // spans — v5's over the service index too, so a torn or rotted service fails the entry
        // like any other field. v3 has none.
        if (layout != EntryLayout.V3)
        {
            int  covered = headerSize - 4;
            uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(src, covered));
            crc      = Crc32c.Append(crc, payloadSpan);
            crc      = Crc32c.Append(crc, excSpan);
            if (crc != Unsafe.ReadUnaligned<uint>(src + covered))
                return false;
        }

        byte[] payload = [];
        if (eh.PayloadLength > 0)
        {
            payload = new byte[eh.PayloadLength];
            payloadSpan.CopyTo(payload);
        }

        ExceptionInfo? exception = null;
        if (eh.ExceptionLength > 0)
        {
            // v3 entries carry no checksum, so garbage here throws deep inside msgpack —
            // treat it as the end of the durable data instead of failing the whole WAL.
            try { exception = ExceptionInfo.FromBytes(excSpan); }
            catch { return false; }
        }

        entry = new WalEntry
        {
            TimestampTicks = eh.TimestampTicks,
            Level          = (LogLevel)eh.Level,
            TemplateIndex  = eh.TemplateIndex,
            // v3 entries predate the flag and their byte 13 was never written: never unpooled.
            Unpooled       = layout != EntryLayout.V3 && (eh.Flags & EntryFlagUnpooled) != 0,
            // Only v5 has the field. A v4 entry's bytes 20-23 are its checksum and its flag bits
            // past bit 0 were reserved, so it names no service whatever they hold.
            ServiceIndex   = layout == EntryLayout.V5 && (eh.Flags & EntryFlagService) != 0 ? eh.ServiceIndex : -1,
            Payload        = payload,
            Exception      = exception,
        };
        entrySize = total;
        return true;
    }

    // ── Grow ─────────────────────────────────────────────────────────────────

    private unsafe void Grow()
    {
        // Release current mapping, extend file, re-map. Runs under _writeLock (called
        // from Append). If extension or re-map fails (disk full is the typical cause —
        // and doubling is exactly when it bites), restore the OLD mapping before
        // rethrowing: leaving _ptr dangling let the next smaller append write through
        // freed memory. Same shape as the metric/span WAL Grow, which were hardened
        // for this first.
        long oldCapacity = _capacity;
        long oldFileSize = FileHeaderSize + oldCapacity;
        long newCapacity = oldCapacity * 2;
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
            // Undo the extension if it landed, then restore the old mapping. If even
            // that fails, _ptr stays null and Append's guard throws instead of faulting.
            try { if (_fileStream!.Length != oldFileSize) _fileStream.SetLength(oldFileSize); }
            catch { /* keep the original exception */ }
            Map(oldFileSize);
            throw;
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

    private void Unmap()
    {
        if (_accessor is not null)
        {
            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            _accessor.Dispose();
        }
        _mmf?.Dispose();
        _accessor = null;
        _mmf      = null;
        _ptr      = null;
    }

    // ── Dispose ──────────────────────────────────────────────────────────────

    private bool _disposed;

    public void Dispose() => DisposeCore(deleteIfEmpty: false);

    /// <summary>
    /// <see cref="Dispose"/>, and then deletes the log and its pool if nothing was ever appended to
    /// it. Decided under the append lock in the same step that marks the log disposed, so there is
    /// no window between the two: an append either reached the log, which is then kept, or arrives
    /// after and throws, exactly as it would after <see cref="Dispose"/>. For the engine's clean
    /// stop (see its DisposeCoreAsync).
    /// </summary>
    /// <returns>True when the log was empty and its file is gone.</returns>
    public bool DisposeDeletingIfEmpty() => DisposeCore(deleteIfEmpty: true);

    private bool DisposeCore(bool deleteIfEmpty)
    {
        bool delete;
        // Under _writeLock: rotation disposes the old WAL from the flush thread while a
        // writer that captured it just before the swap may still be inside Append. The
        // lock makes dispose wait that append out; the _disposed flag makes any later
        // append throw instead of dereferencing the released mapping.
        lock (_writeLock)
        {
            if (_disposed) return false;
            _disposed = true;
            delete    = deleteIfEmpty && _writeOffset == 0;
            Unmap(); // the view flushes dirty pages on dispose
            try { _fileStream?.Flush(flushToDisk: true); } catch { /* best-effort at end of life */ }
            try { _fileStream?.Dispose(); } catch { }
            _fileStream = null;
        }
        lock (_poolLock)
        {
            // fsync, not just OS-buffer: the WAL file outlives this handle until the
            // flush marker lands, and recovery discards a WAL whose pool vanished.
            try { _poolStream?.Flush(flushToDisk: true); _poolStream?.Dispose(); } catch { }
            _poolStream = null;
        }
        if (!delete) return false;

        // A file that will not go (a scanner holding it, on Windows) is left for the next start,
        // which finds an empty log and deletes it; its pool then goes with it, so it stays too.
        try { File.Delete(_filePath); }
        catch { return false; }
        try { File.Delete(PoolPath); } catch { /* rows with no entry to name: harmless */ }
        return true;
    }

    public void Delete()
    {
        string poolPath = _filePath + ".pool";
        Dispose();
        if (File.Exists(_filePath))
            File.Delete(_filePath);
        if (File.Exists(poolPath))
            try { File.Delete(poolPath); } catch { }
    }

    // ── Crash recovery helpers ─────────────────────────────────────────────

    public static Dictionary<ushort, string> LoadPool(string poolPath)
    {
        var dict = new Dictionary<ushort, string>();
        if (!File.Exists(poolPath)) return dict;
        try
        {
            using var fs = new FileStream(poolPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            Span<byte> hdr = stackalloc byte[4];
            while (fs.Read(hdr) == 4)
            {
                ushort index = BinaryPrimitives.ReadUInt16LittleEndian(hdr);
                ushort len   = BinaryPrimitives.ReadUInt16LittleEndian(hdr[2..]);
                var    bytes = new byte[len];
                if (fs.Read(bytes) != len) break;
                dict[index] = Encoding.UTF8.GetString(bytes);
            }
        }
        catch { /* best-effort */ }
        return dict;
    }

    /// <summary>The format this build writes, and the newest it reads (see WalVersion).</summary>
    public const ushort FormatVersion = WalVersion;

    public static (ulong SegmentId, List<WalEntry> Entries) ReadForRecovery(string walPath) =>
        ReadForRecovery(walPath, out _, out _);

    /// <param name="version">
    /// The format the file's header claims, or 0 when the file is not one of these logs. A version
    /// above <see cref="FormatVersion"/> comes back with no entries: a later release wrote the file,
    /// and only it can read it.
    /// </param>
    /// <param name="headerRecordsEntries">
    /// False only when the header's WriteOffset is exactly where the first entry would begin, as in
    /// a log nothing was appended to — what every clean stop before this release left behind, and
    /// what a later release's may. No version so far writes any other value there for an empty log.
    /// Read before the version is looked at, so a later release's file can be told empty too; any
    /// other value says "may hold entries", which is also what a later format that moved the field
    /// would read as.
    /// </param>
    public static unsafe (ulong SegmentId, List<WalEntry> Entries) ReadForRecovery(
        string walPath, out ushort version, out bool headerRecordsEntries)
    {
        version              = 0;
        headerRecordsEntries = false;
        if (!File.Exists(walPath)) return (0, []);
        long fileSize = new FileInfo(walPath).Length;
        if (fileSize < FileHeaderSize) return (0, []);

        using var mmf  = MemoryMappedFile.CreateFromFile(walPath, FileMode.Open, null, fileSize, MemoryMappedFileAccess.Read);
        using var view = mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.Read);
        byte* ptr = null;
        view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        try
        {
            ref var fh = ref Unsafe.AsRef<WalFileHeader>(ptr);
            if (fh.Magic != MagicNumber) return (0, []);
            version              = fh.Version;
            headerRecordsEntries = fh.WriteOffset != FileHeaderSize;
            // v5 = current. v4 = the releases before it: checksummed, no service. v3: replayable,
            // no per-entry validation possible. Anything else is unreplayable by construction.
            EntryLayout layout;
            switch (fh.Version)
            {
                case WalVersion:   layout = EntryLayout.V5; break;
                case WalVersionV4: layout = EntryLayout.V4; break;
                case WalVersionV3: layout = EntryLayout.V3; break;
                default:           return (fh.SegmentId, []);
            }
            ulong segId       = fh.SegmentId;
            long  writeOffset = fh.WriteOffset - FileHeaderSize;
            if (writeOffset <= 0) return (segId, []);

            // The header page and the data pages hit disk independently — a torn header
            // can claim ANY offset. Clamp to the file so the walk stays inside the view
            // (unclamped, this was an uncatchable AccessViolation in the engine
            // constructor: a crash-loop until an operator deleted the file by hand).
            long maxData = fileSize - FileHeaderSize;
            if (writeOffset > maxData) writeOffset = maxData;

            int headerSize = HeaderSizeOf(layout);
            var  entries = new List<WalEntry>();
            long pos     = 0;
            long end     = writeOffset;
            while (pos + headerSize <= end)
            {
                if (!TryParseEntry(ptr, pos, end, layout, out var entry, out long entrySize))
                    break;
                entries.Add(entry!);
                pos += entrySize;
            }
            return (segId, entries);
        }
        finally
        {
            view.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}

public sealed class WalEntry
{
    public long           TimestampTicks { get; init; }
    public LogLevel       Level          { get; init; }
    public ushort         TemplateIndex  { get; init; }

    /// <summary>
    /// The event's template was outside the template pool when it was logged.
    /// <see cref="TemplateIndex"/> is then 0 and names nothing, and the WAL never stored the
    /// template text, so the event is recovered with no template.
    /// </summary>
    public bool           Unpooled       { get; init; }

    /// <summary>
    /// The event's <c>@service</c> as an index into the intern pool, or -1: the event had none, or
    /// the entry predates format v5 and could not say. The index's text is a row of the WAL's pool
    /// file; resolve it through <see cref="ServiceIndexIn"/>, not straight into a live pool.
    /// </summary>
    public int            ServiceIndex   { get; init; } = -1;

    public byte[]         Payload        { get; init; } = [];
    public ExceptionInfo? Exception      { get; init; }

    /// <summary>
    /// <see cref="ServiceIndex"/> when <paramref name="poolRows"/> — the rows of this entry's own pool
    /// file, as <see cref="WriteAheadLog.LoadPool"/> read them — hold its text; otherwise -1, no
    /// service.
    ///
    /// <para>The row is written before the entry, but the two files reach the platter on their own:
    /// a power loss can keep the entry and lose the row. The index then names nothing this WAL can
    /// vouch for. Resolved anyway, it would read whatever the restarted process's pool holds at that
    /// slot — empty, or a string another WAL's replay put there, which is how a stale row becomes a
    /// wrong service in a segment for good.</para>
    /// </summary>
    public int ServiceIndexIn(IReadOnlyDictionary<ushort, string> poolRows) =>
        ServiceIndex >= 0 && poolRows.ContainsKey((ushort)ServiceIndex) ? ServiceIndex : -1;
}
