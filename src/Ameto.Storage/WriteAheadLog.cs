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
/// Format:
///   [WAL Header   — 32 bytes]
///   [Entry 0 …]
///     [Entry Header — 24 bytes: payloadLen uint32, timestamp int64, level byte, pad byte,
///      templateIndex uint16, exceptionLen uint32, crc32c uint32]
///     [Entry Payload — raw msgpack bytes][Exception — msgpack ExceptionInfo]
///   [Entry 1 …]
///   ...
///
/// The WAL is append-only. On crash recovery, the storage layer replays complete entries
/// and rebuilds the hot-tier up to the last entry whose checksum verifies.
///
/// Durability: appends land in the OS page cache; <see cref="Flush"/> msyncs the mapping
/// (StorageEngine drives it on a timer) and rotation/dispose flush the view. Per-entry
/// CRC32C means a crash mid-write-back — pages reaching disk in any order — truncates
/// replay at the first torn entry instead of manufacturing garbage events.
/// </summary>
public sealed unsafe class WriteAheadLog : IDisposable
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
    // v3 files remain READABLE in recovery (no checksum validation); new files are v4.
    private const ushort WalVersion       = 4;
    private const ushort WalVersionV3     = 3;
    private const int    FileHeaderSize   = 32;
    private const int    EntryHeaderSize  = 24;
    private const int    EntryHeaderSizeV3 = 20;
    // Bytes of the entry header covered by the checksum (everything except the crc itself).
    private const int    ChecksummedHeaderBytes = EntryHeaderSize - 4;

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
    // 4+8+1+1+2+4+4 = 24 bytes, with Checksum LAST so the checksum covers [0, 20).
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = EntryHeaderSize)]
    private struct WalEntryHeader
    {
        public uint   PayloadLength;
        public long   TimestampTicks;
        public byte   Level;
        private byte  _pad;
        public ushort TemplateIndex;   // index into companion .pool file
        public uint   ExceptionLength; // bytes of msgpack ExceptionInfo appended after payload
        public uint   Checksum;        // CRC32C over header[0..20) + payload + exception (v4+)
    }

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
    private readonly object              _writeLock = new();
    private          FileStream?          _poolStream;
    private          bool                 _poolDirty;
    private readonly bool[]               _savedTemplateIndices = new bool[65536];
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
                // Unknown or older version: live appends need the v4 layout, and pre-v3
                // entries are unreplayable by construction — reinitialise in place.
                // (Orphaned v3 files are still replayed by ReadForRecovery, which handles
                // the old stride; this path is a same-name reopen, which recovery precedes.)
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

        // Open companion pool file (template index → string) for crash recovery
        _poolStream = new FileStream(_filePath + ".pool",
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _poolStream.Seek(0, SeekOrigin.End);
    }

    // ── Append ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Appends a single event payload to the WAL. Thread-safe via lock.
    /// Fast path: a single Span copy into the mmap region.
    /// </summary>
    // Reused per thread: exception msgpack scratch — the bytes are copied into the mmap
    // below, so nothing outlives the call. Avoids a byte[] per exception-carrying event.
    [ThreadStatic] private static System.Buffers.ArrayBufferWriter<byte>? _tExc;

    public unsafe void Append(long timestampTicks, LogLevel level, ushort templateIndex, string template, ReadOnlySpan<byte> payload, ExceptionInfo? exception = null)
    {
        EnsureTemplateInPool(templateIndex, template);

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
            eh.TemplateIndex   = templateIndex;
            eh.ExceptionLength = (uint)excBytes.Length;

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

    private void EnsureTemplateInPool(ushort index, string template)
    {
        if (string.IsNullOrEmpty(template) || _savedTemplateIndices[index]) return;
        lock (_poolLock)
        {
            if (_savedTemplateIndices[index]) return;
            _savedTemplateIndices[index] = true;
            if (_poolStream is null) return;
            var bytes = Encoding.UTF8.GetBytes(template);
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
    /// msyncs the mapped view and fsyncs the template pool if it grew. Called on a timer
    /// by the storage engine: acknowledged events are durable within one interval of
    /// power loss instead of "whenever the OS writes back" (up to the tier's whole life).
    /// Runs under the write lock — bounded stall for the single writer; the ingest ring
    /// absorbs it.
    /// </summary>
    public void Flush()
    {
        lock (_writeLock)
        {
            if (_disposed || _accessor is null) return;
            _accessor.Flush();
            // FlushViewOfFile (which the accessor flush is on Windows) queues the pages to
            // the filesystem but does not wait for the drive — FlushFileBuffers does. On
            // Linux the accessor flush is already msync(MS_SYNC); the extra fsync is cheap.
            _fileStream?.Flush(flushToDisk: true);
        }
        lock (_poolLock)
        {
            if (!_poolDirty || _poolStream is null) return;
            _poolDirty = false;
            try { _poolStream.Flush(flushToDisk: true); }
            catch (IOException) { _poolDirty = true; } // retried next interval
        }
    }

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
        => TryParseEntry(_ptr, pos, end, validateChecksum: true, v3Layout: false, out entry, out entrySize);

    /// <summary>
    /// Parses one entry at <paramref name="pos"/>. Bounds are checked and (v4) the CRC is
    /// verified over the in-map spans BEFORE anything is allocated, so a garbage length
    /// field cannot OOM and a torn entry cannot materialise. Returns false at the first
    /// entry that does not verify — everything past it is by definition not durable.
    /// </summary>
    private static unsafe bool TryParseEntry(
        byte* basePtr, long pos, long end, bool validateChecksum, bool v3Layout,
        out WalEntry? entry, out long entrySize)
    {
        entry     = null;
        entrySize = 0;

        int headerSize = v3Layout ? EntryHeaderSizeV3 : EntryHeaderSize;
        if (pos + headerSize > end) return false;

        byte* src = basePtr + FileHeaderSize + pos;
        ref var eh = ref Unsafe.AsRef<WalEntryHeader>(src); // v3 shares the first 20 bytes

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

        if (validateChecksum && !v3Layout)
        {
            uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(src, ChecksummedHeaderBytes));
            crc      = Crc32c.Append(crc, payloadSpan);
            crc      = Crc32c.Append(crc, excSpan);
            if (crc != eh.Checksum)
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

    public void Dispose()
    {
        // Under _writeLock: rotation disposes the old WAL from the flush thread while a
        // writer that captured it just before the swap may still be inside Append. The
        // lock makes dispose wait that append out; the _disposed flag makes any later
        // append throw instead of dereferencing the released mapping.
        lock (_writeLock)
        {
            if (_disposed) return;
            _disposed = true;
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

    public static unsafe (ulong SegmentId, List<WalEntry> Entries) ReadForRecovery(string walPath)
    {
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
            // v4 = current (checksummed). v3 = previous release: replayable, no per-entry
            // validation possible. Anything else is unreplayable by construction.
            bool v3 = fh.Version == WalVersionV3;
            if (fh.Version != WalVersion && !v3) return (fh.SegmentId, []);
            ulong segId       = fh.SegmentId;
            long  writeOffset = fh.WriteOffset - FileHeaderSize;
            if (writeOffset <= 0) return (segId, []);

            // The header page and the data pages hit disk independently — a torn header
            // can claim ANY offset. Clamp to the file so the walk stays inside the view
            // (unclamped, this was an uncatchable AccessViolation in the engine
            // constructor: a crash-loop until an operator deleted the file by hand).
            long maxData = fileSize - FileHeaderSize;
            if (writeOffset > maxData) writeOffset = maxData;

            int headerSize = v3 ? EntryHeaderSizeV3 : EntryHeaderSize;
            var  entries = new List<WalEntry>();
            long pos     = 0;
            long end     = writeOffset;
            while (pos + headerSize <= end)
            {
                if (!TryParseEntry(ptr, pos, end, validateChecksum: true, v3Layout: v3, out var entry, out long entrySize))
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
    public byte[]         Payload        { get; init; } = [];
    public ExceptionInfo? Exception      { get; init; }
}
