using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.IO.MemoryMappedFiles;

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
/// Format:
/// <code>
///   [File Header — 32 bytes]
///     0   Magic              uint32  "RDSW"
///     4   Version            uint16  1
///     6   _pad               uint16
///     8   WriteOffset        int64   next byte to write (absolute, includes this header)
///    16   FlushedThroughNano int64   see the crash-recovery note below
///    24   _reserved          int64
///
///   [Entry 0 …]
///     [Entry Header — 64 bytes, Pack = 1]
///     [Name UTF-8][ServiceName UTF-8][Attributes msgpack]
/// </code>
///
/// <para>Append-only, no fsync per span: a span already survived the network hop, and the
/// OS page cache is flushed by the mapping itself. The log is reset once its spans reach a
/// cold segment.</para>
///
/// <para><b>Crash recovery.</b> A flush writes the segment first and resets the log second,
/// so a crash between the two would replay spans that are already cold. To close all but a
/// sliver of that window the flush stamps <c>FlushedThroughNano</c> — the newest start time
/// the segment contains — into the header BEFORE zeroing the write offset, and recovery
/// skips replayed entries at or below it. Appends are serialised by the engine's write lock
/// and cannot interleave with a flush, so no live span is ever skipped by that test. Only a
/// crash landing between the segment write and the header stamp can duplicate spans.</para>
/// </summary>
internal sealed unsafe class SpanWriteAheadLog : IDisposable
{
    private const uint   MagicNumber     = 0x52_44_53_57; // "RDSW"
    private const ushort WalVersion      = 1;
    private const int    FileHeaderSize  = 32;
    private const int    EntryHeaderSize = 64;

    /// <summary>8 MB holds ~40k spans; the log is reset on every flush, so it rarely grows.</summary>
    private const long DefaultCapacity = 8 * 1024 * 1024;

    [StructLayout(LayoutKind.Sequential, Size = FileHeaderSize)]
    private struct WalFileHeader
    {
        public uint   Magic;
        public ushort Version;
        private ushort _pad;
        public long   WriteOffset;
        public long   FlushedThroughNano;
        private long  _reserved;
    }

    /// <summary>
    /// Pack = 1 keeps the fields at exactly 60 bytes inside the 64-byte stride — without it
    /// the 8-byte members would align and push the tail past the stride, straight into the
    /// payload area (the corruption the logs WAL hit in its v2 layout).
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = EntryHeaderSize)]
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
    }

    private readonly string _filePath;
    private readonly Lock   _writeLock = new();

    private MemoryMappedFile?         _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private byte*                     _ptr;
    private long                      _capacity;
    private long                      _writeOffset;        // logical, excludes the file header
    private long                      _flushedThroughNano;

    public string FilePath => _filePath;

    /// <summary>Bytes currently held by the log. Diagnostics only.</summary>
    public long WrittenBytes { get { lock (_writeLock) return _writeOffset; } }

    private SpanWriteAheadLog(string filePath) => _filePath = filePath;

    public static SpanWriteAheadLog Open(string filePath, long initialCapacity = DefaultCapacity)
    {
        var wal = new SpanWriteAheadLog(filePath);
        wal.OpenOrCreate(initialCapacity);
        return wal;
    }

    private void OpenOrCreate(long initialCapacity)
    {
        bool exists   = File.Exists(_filePath);
        long fileSize = FileHeaderSize + initialCapacity;

        using (var fs = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            if (fs.Length < fileSize) fs.SetLength(fileSize);
            else                      fileSize = fs.Length;   // reopen an already-grown log at its size
        }

        _capacity = fileSize - FileHeaderSize;
        Map(fileSize);

        ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
        if (!exists || hdr.Magic != MagicNumber || hdr.Version != WalVersion)
        {
            // New, foreign or future-versioned file — reinitialise in place. Anything
            // already there cannot be replayed under a layout we do not know.
            hdr.Magic              = MagicNumber;
            hdr.Version            = WalVersion;
            hdr.WriteOffset        = FileHeaderSize;
            hdr.FlushedThroughNano = 0;
            _writeOffset           = 0;
            _flushedThroughNano    = 0;
        }
        else
        {
            _writeOffset        = Math.Max(0, hdr.WriteOffset - FileHeaderSize);
            _flushedThroughNano = hdr.FlushedThroughNano;
            if (_writeOffset > _capacity) _writeOffset = _capacity;  // truncated file — replay what is mapped
        }
    }

    private void Map(long fileSize)
    {
        _mmf      = MemoryMappedFile.CreateFromFile(_filePath, FileMode.Open, null, fileSize, MemoryMappedFileAccess.ReadWrite);
        _accessor = _mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.ReadWrite);
        _ptr      = null;
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
    }

    // ── Append ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Appends one span. The name and service are encoded straight into the mapped page —
    /// no intermediate <c>byte[]</c> — and the attributes blob is already msgpack from the
    /// ingest decoder, so it is copied verbatim. Nothing is allocated on the managed heap.
    /// </summary>
    public void Append(SpanIngestItem item)
    {
        string name    = item.Name        ?? string.Empty;
        string service = item.ServiceName ?? string.Empty;

        int nameLen = Encoding.UTF8.GetByteCount(name);
        int svcLen  = Encoding.UTF8.GetByteCount(service);
        int attrLen = item.AttributesBytes.Length;

        // Lengths are stored in 16-bit fields; a pathological name must not silently
        // corrupt the stride, so clamp it out of the log rather than truncate mid-rune.
        if (nameLen > ushort.MaxValue) { name = string.Empty; nameLen = 0; }
        if (svcLen  > ushort.MaxValue) { service = string.Empty; svcLen = 0; }

        int entrySize = EntryHeaderSize + nameLen + svcLen + attrLen;

        lock (_writeLock)
        {
            if (_ptr is null) return;                       // disposed — drop rather than fault
            while (_writeOffset + entrySize > _capacity)
                Grow();

            byte* dest = _ptr + FileHeaderSize + _writeOffset;

            ref var eh = ref Unsafe.AsRef<SpanWalEntryHeader>(dest);
            // TraceId sits at offset 0 of the entry, so the entry pointer addresses it
            // directly — a fixed-size buffer reached through a ref into unmanaged memory
            // would need a `fixed` statement for no benefit.
            item.TraceId.WriteTo(new Span<byte>(dest, 16));
            eh.SpanId            = item.SpanId.RawValue;
            eh.ParentSpanId      = item.ParentSpanId.RawValue;
            eh.StartTimeUnixNano = item.StartTimeUnixNano;
            eh.DurationNanos     = item.DurationNanos;
            eh.AttrLength        = (uint)attrLen;
            eh.NameLength        = (ushort)nameLen;
            eh.ServiceLength     = (ushort)svcLen;
            eh.HttpStatusCode    = item.HttpStatusCode;
            eh.Kind              = (byte)item.Kind;
            eh.Status            = (byte)item.Status;

            byte* p = dest + EntryHeaderSize;
            if (nameLen > 0) { Encoding.UTF8.GetBytes(name,    new Span<byte>(p, nameLen)); p += nameLen; }
            if (svcLen  > 0) { Encoding.UTF8.GetBytes(service, new Span<byte>(p, svcLen));  p += svcLen;  }
            if (attrLen > 0) item.AttributesBytes.AsSpan().CopyTo(new Span<byte>(p, attrLen));

            _writeOffset += entrySize;
            Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + _writeOffset;
        }
    }

    // ── Reset ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Drops every logged span, stamping the watermark first so a crash between the two
    /// stores cannot replay spans the caller has already made cold. Call only after the
    /// segment carrying these spans is on disk.
    /// </summary>
    /// <param name="flushedThroughNano">Newest start time contained in the flushed segment.</param>
    public void Reset(long flushedThroughNano)
    {
        lock (_writeLock)
        {
            if (_ptr is null) return;
            _flushedThroughNano = flushedThroughNano;

            ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            hdr.FlushedThroughNano = flushedThroughNano;   // must land before the offset
            hdr.WriteOffset        = FileHeaderSize;
            _writeOffset           = 0;
        }
    }

    // ── Recovery ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Replays every complete entry that the last flush did not cover. A short or
    /// impossible entry ends the replay: the tail of an append-only log is the only place
    /// a torn write can be, and everything before it is intact.
    /// </summary>
    public List<SpanIngestItem> ReadAll()
    {
        var result = new List<SpanIngestItem>();

        lock (_writeLock)
        {
            if (_ptr is null) return result;

            long pos = 0;
            long end = _writeOffset;

            while (pos + EntryHeaderSize <= end)
            {
                byte* src = _ptr + FileHeaderSize + pos;
                ref var eh = ref Unsafe.AsRef<SpanWalEntryHeader>(src);

                long total = (long)EntryHeaderSize + eh.NameLength + eh.ServiceLength + eh.AttrLength;
                if (total <= 0 || pos + total > end) break;   // torn tail

                // A zero-filled header is a valid-looking 64-byte entry, so bounds alone
                // cannot tell "never written" from "empty name, empty service, no attrs".
                // A span start time is unix nanoseconds and therefore always positive —
                // anything else means we have walked past the data the header claimed.
                if (eh.StartTimeUnixNano <= 0) break;

                if (eh.StartTimeUnixNano > _flushedThroughNano)
                {
                    byte* p = src + EntryHeaderSize;
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
        }

        return result;
    }

    // ── Grow ─────────────────────────────────────────────────────────────────

    private void Grow()
    {
        long newCapacity = _capacity * 2;
        long newFileSize = FileHeaderSize + newCapacity;

        _accessor!.SafeMemoryMappedViewHandle.ReleasePointer();
        _accessor.Dispose();
        _mmf!.Dispose();
        _ptr = null;

        using (var fs = new FileStream(_filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            fs.SetLength(newFileSize);

        _capacity = newCapacity;
        Map(newFileSize);
    }

    // ── Dispose ──────────────────────────────────────────────────────────────

    private bool _disposed;

    public void Dispose()
    {
        lock (_writeLock)
        {
            if (_disposed) return;
            _disposed = true;

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
    }

    /// <summary>Closes and removes the log file. Used by tests and by a data-directory reset.</summary>
    public void Delete()
    {
        Dispose();
        try { if (File.Exists(_filePath)) File.Delete(_filePath); } catch { /* best-effort */ }
    }
}
