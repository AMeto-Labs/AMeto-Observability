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
///    16   Generation         uint32  flush generation, see the crash-recovery note below
///    20   _reserved          uint32 + int64
///
///   [Entry 0 …]
///     [Entry Header — 64 bytes, Pack = 1, carries the generation it was written under]
///     [Name UTF-8][ServiceName UTF-8][Attributes msgpack]
/// </code>
///
/// <para>Append-only, no fsync per span: a span already survived the network hop, and the
/// OS page cache is flushed by the mapping itself. The log is reset once its spans reach a
/// cold segment.</para>
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
internal sealed unsafe class SpanWriteAheadLog : IDisposable
{
    private const uint   MagicNumber     = 0x52_44_53_57; // "RDSW"
    private const ushort WalVersion      = 1;
    private const int    FileHeaderSize  = 32;
    private const int    EntryHeaderSize = 64;

    /// <summary>8 MB holds ~40k spans; the log is reset on every flush, so it rarely grows.</summary>
    private const long DefaultCapacity = 8 * 1024 * 1024;

    /// <summary>First generation of a fresh log. 0 is reserved for "never written".</summary>
    private const uint FirstGeneration = 1;

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
    /// four bytes that were previously padding.
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
        public uint   Generation;             // 0 = unwritten; see the class remarks
    }

    private readonly string _filePath;
    private readonly Lock   _writeLock = new();

    private MemoryMappedFile?         _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private byte*                     _ptr;
    private long                      _capacity;
    private long                      _writeOffset;        // logical, excludes the file header
    private uint                      _generation;

    // ── Open-flush state (see BeginFlush/CommitFlush) ────────────────────────
    private bool _flushOpen;          // one flush at a time — the engine serialises them
    private long _flushBoundary;      // bytes belonging to the generation being flushed
    private bool _generationBumped;   // bump once per COMMIT cycle, not per Begin (retries)

    private static uint Next(uint g) => g == uint.MaxValue ? FirstGeneration : g + 1;

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
            if (_disposed) return;                          // shutdown race — dropping is correct
            if (_ptr is null)
                throw new InvalidOperationException(
                    "Span WAL has no mapping; the log is not accepting appends.");

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
            eh.Generation        = _generation;

            byte* p = dest + EntryHeaderSize;
            if (nameLen > 0) { Encoding.UTF8.GetBytes(name,    new Span<byte>(p, nameLen)); p += nameLen; }
            if (svcLen  > 0) { Encoding.UTF8.GetBytes(service, new Span<byte>(p, svcLen));  p += svcLen;  }
            if (attrLen > 0) item.AttributesBytes.AsSpan().CopyTo(new Span<byte>(p, attrLen));

            _writeOffset += entrySize;
            Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + _writeOffset;
        }
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
    /// Opens a flush: appends from here on carry the NEXT generation; the entries being
    /// flushed keep the current one, and recovery accepts both until <see cref="CommitFlush"/>.
    /// The generation is bumped once per commit CYCLE — a Begin after an Abandon reuses the
    /// bumped one, so the replay window never spans more than two generations.
    /// </summary>
    public void BeginFlush()
    {
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
    /// </summary>
    public void CommitFlush()
    {
        lock (_writeLock)
        {
            if (!_flushOpen) return;
            _flushOpen        = false;
            _generationBumped = false;
            if (_disposed || _ptr is null) return;

            long tail = _writeOffset - _flushBoundary;   // appended while the segment was written
            if (tail > 0 && _flushBoundary > 0)
                Buffer.MemoryCopy(_ptr + FileHeaderSize + _flushBoundary,
                                  _ptr + FileHeaderSize, _capacity, tail);
            _writeOffset   = tail;
            _flushBoundary = 0;

            // The move does not erase its source, and a crash between the stores below
            // would leave the old offset covering both the relocated tail and the stale
            // originals. A generation-0 marker at the new end stops any replay exactly
            // where the data now ends — the same trick the metric WAL's Compact uses.
            if (_writeOffset + EntryHeaderSize <= _capacity)
                Unsafe.AsRef<SpanWalEntryHeader>(_ptr + FileHeaderSize + _writeOffset).Generation = 0;

            ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            hdr.Generation  = _generation;               // must land before the offset store
            hdr.WriteOffset = FileHeaderSize + _writeOffset;
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
    /// Replays every complete entry belonging to the current generation. A short entry ends
    /// the replay: the tail of an append-only log is the only place a torn write can be, and
    /// everything before it is intact.
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

                // Generation 0 is never written by an append, so it marks the end of real
                // data. Nothing about the span's own fields can serve here: an empty name,
                // an empty service, no attributes and a zero start time are all individually
                // legal, which makes a zero-filled region look like a valid entry.
                if (eh.Generation == 0) break;

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

    /// <summary>
    /// Doubles the mapped capacity. Windows will not resize a file while it is mapped, so the
    /// old mapping has to go first — which means a failure here (a full disk, i.e. exactly
    /// when a log grows) would otherwise leave the object alive with no mapping, silently
    /// refusing every later append. The old mapping is therefore restored before the
    /// exception is allowed out, so a failed growth costs only the append that triggered it.
    /// </summary>
    private void Grow()
    {
        long oldFileSize = FileHeaderSize + _capacity;
        long newCapacity = _capacity * 2;
        long newFileSize = FileHeaderSize + newCapacity;

        Unmap();
        try
        {
            using (var fs = new FileStream(_filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                fs.SetLength(newFileSize);

            Map(newFileSize);
            _capacity = newCapacity;
        }
        catch
        {
            try
            {
                using (var fs = new FileStream(_filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                    if (fs.Length < oldFileSize) fs.SetLength(oldFileSize);
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

    private bool _disposed;

    public void Dispose()
    {
        lock (_writeLock)
        {
            if (_disposed) return;
            _disposed = true;
            Unmap();
        }
    }

    /// <summary>Closes and removes the log file. Used by tests and by a data-directory reset.</summary>
    public void Delete()
    {
        Dispose();
        try { if (File.Exists(_filePath)) File.Delete(_filePath); } catch { /* best-effort */ }
    }
}
