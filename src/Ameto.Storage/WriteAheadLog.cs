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
///     [Entry Header — 16 bytes: length uint32, timestamp int64, level byte, reserved 3 bytes]
///     [Entry Payload — raw msgpack bytes]
///   [Entry 1 …]
///   ...
///
/// The WAL is append-only. On crash recovery, the storage layer replays incomplete entries
/// and rebuilds the hot-tier up to the last complete entry.
///
/// msync is called asynchronously via a background flush timer — we do NOT fsync per event.
/// </summary>
public sealed unsafe class WriteAheadLog : IDisposable
{
    // ── WAL file header ──────────────────────────────────────────────────────
    private const uint   MagicNumber    = 0x52_44_57_41; // "RDWA"
    private const ushort WalVersion     = 2;
    private const int    FileHeaderSize = 32;
    private const int    EntryHeaderSize = 20;

    [StructLayout(LayoutKind.Sequential, Size = FileHeaderSize)]
    private struct WalFileHeader
    {
        public uint   Magic;
        public ushort Version;
        public uint   NodeId;
        public ulong  SegmentId;
        public long   WriteOffset;    // next byte to write (maintained in memory + flushed on close)
        private short _pad;
    }

    [StructLayout(LayoutKind.Sequential, Size = EntryHeaderSize)]
    private struct WalEntryHeader
    {
        public uint   PayloadLength;
        public long   TimestampTicks;
        public byte   Level;
        private byte  _pad;
        public ushort TemplateIndex;   // index into companion .pool file
        public uint   ExceptionLength; // bytes of msgpack ExceptionInfo appended after payload
    }

    // ── State ────────────────────────────────────────────────────────────────
    private readonly string              _filePath;
    public  string FilePath => _filePath;
    private          MemoryMappedFile?   _mmf;
    private          MemoryMappedViewAccessor? _accessor;
    private          byte*               _ptr;
    private          long                _capacity;
    private          long                _writeOffset; // logical, excludes file header
    private readonly object              _writeLock = new();
    private          FileStream?          _poolStream;
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

        // Ensure file exists and has the right size
        long fileSize = FileHeaderSize + initialCapacity;
        using (var fs = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            if (fs.Length < fileSize)
                fs.SetLength(fileSize);
        }

        _capacity = initialCapacity;
        _mmf      = MemoryMappedFile.CreateFromFile(_filePath, FileMode.Open, null, fileSize, MemoryMappedFileAccess.ReadWrite);
        _accessor = _mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.ReadWrite);
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);

        if (!exists || _ptr == null)
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
            // Recover write position from header
            ref var hdr  = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            _writeOffset = hdr.WriteOffset - FileHeaderSize;
            if (_writeOffset < 0) _writeOffset = 0;
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
    public unsafe void Append(long timestampTicks, LogLevel level, ushort templateIndex, string template, ReadOnlySpan<byte> payload, ExceptionInfo? exception = null)
    {
        EnsureTemplateInPool(templateIndex, template);

        byte[] excBytes = exception is null ? Array.Empty<byte>() : exception.ToBytes();
        int entrySize   = EntryHeaderSize + payload.Length + excBytes.Length;

        lock (_writeLock)
        {
            if (_writeOffset + entrySize > _capacity)
                Grow();

            byte* dest = _ptr + FileHeaderSize + _writeOffset;

            ref var eh = ref Unsafe.AsRef<WalEntryHeader>(dest);
            eh.PayloadLength   = (uint)payload.Length;
            eh.TimestampTicks  = timestampTicks;
            eh.Level           = (byte)level;
            eh.TemplateIndex   = templateIndex;
            eh.ExceptionLength = (uint)excBytes.Length;

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
            if (!TryReadEntry(pos, end, out var entry, out long entrySize))
                yield break;

            yield return entry!;
            pos += entrySize;
        }
    }

    private unsafe bool TryReadEntry(long pos, long end, out WalEntry? entry, out long entrySize)
    {
        entry     = null;
        entrySize = 0;

        byte* src = _ptr + FileHeaderSize + pos;
        ref var eh = ref Unsafe.AsRef<WalEntryHeader>(src);

        long total = (long)EntryHeaderSize + eh.PayloadLength + eh.ExceptionLength;
        if (pos + total > end)
            return false;

        var payload = new byte[eh.PayloadLength];
        if (eh.PayloadLength > 0)
            new ReadOnlySpan<byte>(src + EntryHeaderSize, (int)eh.PayloadLength).CopyTo(payload);

        ExceptionInfo? exception = null;
        if (eh.ExceptionLength > 0)
        {
            var excSpan = new ReadOnlySpan<byte>(src + EntryHeaderSize + (int)eh.PayloadLength, (int)eh.ExceptionLength);
            exception   = ExceptionInfo.FromBytes(excSpan);
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
        // Release current mapping, extend file, re-map
        long newCapacity = _capacity * 2;
        long newFileSize = FileHeaderSize + newCapacity;

        _accessor!.SafeMemoryMappedViewHandle.ReleasePointer();
        _accessor.Dispose();
        _mmf!.Dispose();

        using (var fs = new FileStream(_filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            fs.SetLength(newFileSize);

        _capacity = newCapacity;
        _mmf      = MemoryMappedFile.CreateFromFile(_filePath, FileMode.Open, null, newFileSize, MemoryMappedFileAccess.ReadWrite);
        _accessor = _mmf.CreateViewAccessor(0, newFileSize, MemoryMappedFileAccess.ReadWrite);
        _ptr      = null;
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
    }

    // ── Dispose ──────────────────────────────────────────────────────────────

    private bool _disposed;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try { _poolStream?.Flush(); _poolStream?.Dispose(); } catch { }
        _poolStream = null;

        _accessor?.SafeMemoryMappedViewHandle.ReleasePointer();
        _accessor?.Dispose();
        _mmf?.Dispose();
        _ptr = null;
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
            ulong segId       = fh.SegmentId;
            long  writeOffset = fh.WriteOffset - FileHeaderSize;
            if (writeOffset <= 0) return (segId, []);

            var  entries = new List<WalEntry>();
            long pos     = 0;
            long end     = writeOffset;
            while (pos + EntryHeaderSize <= end)
            {
                byte* src = ptr + FileHeaderSize + pos;
                ref var eh = ref Unsafe.AsRef<WalEntryHeader>(src);
                long total = (long)EntryHeaderSize + eh.PayloadLength + eh.ExceptionLength;
                if (pos + total > end) break;

                var payload = new byte[eh.PayloadLength];
                if (eh.PayloadLength > 0)
                    new ReadOnlySpan<byte>(src + EntryHeaderSize, (int)eh.PayloadLength).CopyTo(payload);

                ExceptionInfo? exception = null;
                if (eh.ExceptionLength > 0)
                {
                    var excSpan = new ReadOnlySpan<byte>(src + EntryHeaderSize + (int)eh.PayloadLength, (int)eh.ExceptionLength);
                    exception   = ExceptionInfo.FromBytes(excSpan);
                }

                entries.Add(new WalEntry
                {
                    TimestampTicks = eh.TimestampTicks,
                    Level          = (LogLevel)eh.Level,
                    TemplateIndex  = eh.TemplateIndex,
                    Payload        = payload,
                    Exception      = exception,
                });
                pos += total;
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
