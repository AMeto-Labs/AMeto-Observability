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
/// Format (v6):
///   [WAL Header   — 32 bytes]
///   [Entry 0 …]
///     [Entry Header — 26 bytes: payloadLen uint32, timestamp int64, level byte, flags byte,
///      templateIndex uint16, exceptionLen uint32, serviceIndex uint16, crc32c uint32]
///     [Trace context — 24 bytes, only with the Trace flag: traceIdHi uint64, traceIdLo uint64,
///      spanId uint64]
///     [Template — only with the InlineTemplate flag: byteLength uint32, then that many UTF-8 bytes]
///     [Service — only with the InlineService flag: byteLength uint32, then that many UTF-8 bytes]
///     [Entry Payload — raw msgpack bytes][Exception — msgpack ExceptionInfo]
///   [Entry 1 …]
///   ...
///
/// The checksum covers header bytes [0, 22), the trace context, the inline template and service
/// (their lengths included), the payload and the exception.
///
/// Flags (byte 13 of the entry header, inside the checksummed range):
///   bit 0 — Unpooled: the event's template has no pool row (the pool was full, the index was past
///           its 65 536 ids, or — v6 — the text is longer than a row holds). templateIndex is then
///           0 and meaningless and no pool row was written; a v6 entry carries the text itself
///           (InlineTemplate), and an older one does not, so recovery yields that event with no
///           template. Without the bit, a replay whose pool file held any row resolved index 0
///           and gave the event index 0's template.
///   bit 1 — Service (v5): serviceIndex is the event's <c>@service</c>, an index into the same
///           intern pool the template's is, and the index's text is a row of the companion pool
///           file, written the first time this WAL logs the index. Without the bit the event has
///           no service and serviceIndex is 0 and meaningless: all 65 536 values are real pool
///           ids, so the index alone cannot say "none" — the same reason the template has a flag.
///   bit 2 — Trace (v6): the 24-byte trace context follows the header. An event with no trace id
///           and no span id is logged without it, so the field costs nothing where it is empty.
///   bit 3 — InlineTemplate (v6): the event's template TEXT follows the trace context, because no
///           pool row can carry it. With Unpooled, the template is outside the pool, and every
///           such entry carries its own. Without Unpooled, the template is pooled but longer than a
///           row's 65 535 bytes, and the entry is the FIRST of this log to use its index: the later
///           ones carry the index alone, and recovery resolves it through this entry — a log holds
///           such a template once, as it holds a row once. (Carried by every entry, one 70 KB
///           template made a 70 MB log of 1 000 events, and its replay one string per entry.)
///           Without the bit such an event replayed with no template, or a truncated one; for a
///           spilled event, whose only copy is its spill file, that was every such event of every
///           overload, despilled with its message gone for good.
///   bit 4 — InlineService (v6): the event's <c>@service</c> TEXT follows the inline template (byte
///           length uint32, then UTF-8, under the checksum), because no pool row can carry it.
///           Without the Service bit, the service was outside the intern pool — a full pool answers
///           -1 — so no index and no row name it, and every such entry carries its own. With it, the
///           service is pooled but longer than a row's 65 535 bytes, and the entry is the FIRST of
///           this log to use its index, as for a long template (bit 3): the later ones carry the
///           index alone. Templates and services share the pool, so a long text carried once serves
///           both uses of its index. (Carried by every entry, one 70 KB service made a 70 MB log of
///           1 000 events, and 3 500 of them a log whose replay ran a 384 MiB heap out at every
///           start.) Without the bit such an event was logged, replayed and despilled with no service.
///   Other bits are reserved and written as zero.
///
/// <para>v6 exists because a WAL is no longer only a crash net. A log WAL that replays is how a
/// spilled block (see <c>StorageEngine</c>, "Spill") reaches its segments in ordinary operation,
/// and v5 kept no trace context: every replayed event came back with no TraceId and no SpanId,
/// unlinked from its trace for good. That was a crash's price before; it would have been
/// every overloaded minute's.</para>
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
/// Durability: appends land in the OS page cache; <see cref="Flush"/> writes the mapping back and
/// flushes the file handle, both off the appender's lock (StorageEngine drives it on a timer), and
/// rotation/dispose flush the view. Per-entry
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
    // v6: an entry may carry the event's trace context — TraceIdHi, TraceIdLo, SpanId — in 24
    // bytes after its header, said by the Trace flag. The header itself is v5's, byte for byte.
    // v5, v4 and v3 files remain READABLE in recovery (v3 without checksum validation, v3 and v4
    // without a service, none of them with trace context); new files are v6.
    private const ushort WalVersion        = 6;
    private const ushort WalVersionV5      = 5;
    private const ushort WalVersionV4      = 4;
    private const ushort WalVersionV3      = 3;
    private const int    FileHeaderSize    = 32;
    private const int    EntryHeaderSize   = 26;
    private const int    EntryHeaderSizeV4 = 24;
    private const int    EntryHeaderSizeV3 = 20;
    // Bytes of the entry header covered by the checksum (everything except the crc itself).
    private const int    ChecksummedHeaderBytes = EntryHeaderSize - 4;
    // The trace context an entry with the Trace flag carries after its header (v6).
    private const int    TraceContextSize  = 24;
    // WalEntryHeader.Flags (see the class doc): the event's template is outside the pool …
    private const byte   EntryFlagUnpooled = 0x01;
    // … the event has a service, named by ServiceIndex (v5) …
    private const byte   EntryFlagService  = 0x02;
    // … and the entry carries the trace context (v6) …
    private const byte   EntryFlagTrace    = 0x04;
    // … and the template's text, which no pool row can carry (v6; see the class doc).
    private const byte   EntryFlagInlineTemplate = 0x08;
    // … and the service's text, outside the pool (v6; see the class doc).
    private const byte   EntryFlagInlineService  = 0x10;
    // The longest text a pool row holds: its length field is 16 bits.
    private const int    MaxPoolRowBytes   = ushort.MaxValue;

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

    /// <summary>The entry layouts recovery reads: this format's and the three before it.</summary>
    private enum EntryLayout : byte { V3, V4, V5, V6 }

    /// <summary>The fixed header of an entry; a v6 entry's trace context, when it has one, follows it.</summary>
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
    // Pool indices whose template is too long for a row (see InlineTemplate in the class doc):
    // LongTemplateUnlogged once measured, LongTemplateLogged once an entry of this log carries it.
    private readonly byte[]               _longTemplates = new byte[65536];
    private const byte                    LongTemplateUnlogged = 1;
    private const byte                    LongTemplateLogged   = 2;

    public string PoolPath => _filePath + ".pool";

    // ── Construction ─────────────────────────────────────────────────────────

    /// <param name="spill">
    /// A spill target's log (see <c>StorageEngine</c>, "Spill"). It never grows past
    /// <paramref name="initialCapacity"/>, its pool file counted in — <see cref="Append"/> refuses
    /// an entry that does not fit, and the spill rotates to a new file, which the spill's
    /// free-space floor is checked against first; only an empty log takes an entry whatever its
    /// size — and when it is closed the file is cut to what it holds. A growing spill file used to double past the space its
    /// floor check had counted (a storm of large exceptions took one block from ~74 MB to over
    /// 2 GB), and a closed one kept its whole pre-size on NTFS, which allocates it.
    /// </param>
    public static WriteAheadLog Open(string filePath, NodeId nodeId, SegmentId segmentId, long initialCapacity = 64 * 1024 * 1024,
                                     bool spill = false)
    {
        bool fileExisted = File.Exists(filePath);
        bool poolExisted = File.Exists(filePath + ".pool");
        var  wal         = new WriteAheadLog(filePath, spill);
        try
        {
            wal.OpenOrCreate(nodeId, segmentId, initialCapacity);
        }
        catch
        {
            // Nothing is left behind by a log that could not be opened: its handles closed, and the
            // files it created removed. It used to leave a 0-byte file whose FileStream stayed open
            // until finalisation — on a full disk, one per attempt to open one.
            try { wal.Dispose(); } catch { /* the original exception is the one that matters */ }
            if (!fileExisted) try { File.Delete(filePath); } catch { /* best-effort */ }
            if (!poolExisted) try { File.Delete(filePath + ".pool"); } catch { /* best-effort */ }
            throw;
        }
        return wal;
    }

    private WriteAheadLog(string filePath, bool spill)
    {
        _filePath = filePath;
        _spill    = spill;
    }

    /// <summary>A spill target's log: fixed capacity, trimmed when closed (see <see cref="Open"/>).</summary>
    private readonly bool _spill;

    private unsafe void OpenOrCreate(NodeId nodeId, SegmentId segmentId, long initialCapacity)
    {
        bool exists = File.Exists(_filePath);

        // Ensure file exists and has the right size. An existing file may be LARGER than
        // requested (it grew in a previous run) — the mapping must cover the whole file,
        // or a recovered WriteOffset beyond the requested size would walk off the view.
        long fileSize = FileHeaderSize + initialCapacity;
        _fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        long found = _fileStream.Length;
        if (found < fileSize)
        {
            _fileStream.SetLength(fileSize);
            // Its blocks reserved, not left to the first write into each page (round-5 H-F): on Linux
            // SetLength is ftruncate, the file sparse, and that write SIGBUS on a full volume — the
            // process killed where NTFS fails this open with an IOException. See FileSpace.
            FileSpace.Reserve(_fileStream.SafeFileHandle, found, fileSize - found);
        }
        else
            fileSize = found;

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
                // Unknown or older version: live appends need the v6 layout, and pre-v3
                // entries are unreplayable by construction — reinitialise in place.
                // (Orphaned v5, v4 and v3 files are still replayed by ReadForRecovery, which
                // handles their strides; this path is a same-name reopen, which recovery precedes.)
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
        _poolBytes = _poolStream.Length;
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
    /// flag with index 0, no pool row is written for it, and <paramref name="template"/>'s text
    /// goes into the entry itself (InlineTemplate). A pooled template longer than a pool row
    /// holds, which a row used to keep cut at 65 535 bytes, goes into the first entry of this log
    /// that uses its index, and only that one.
    /// </param>
    /// <param name="serviceIndex">
    /// The event's <c>@service</c>, as <see cref="LogEventHeader.ServiceNamePoolIndex"/> holds it:
    /// an index into the same pool. -1, or anything else outside <c>[0, 65 535]</c>, logs the
    /// event's service by its text, in the entry (InlineService) — or, with no text, no service.
    /// </param>
    /// <param name="service">
    /// The text at <paramref name="serviceIndex"/>, written as a pool row the first time this WAL
    /// logs that index — or, when no row can hold it, carried by the first entry of this log that
    /// uses the index, as a long pooled template is; for an index outside the pool, the service's
    /// text itself, which every such entry carries. Null or empty writes none; recovery then gives
    /// the event no service unless another of this WAL's events wrote the row.
    /// </param>
    /// <param name="traceIdHi">The event's trace id, high half. With the other two all zero, no trace context is logged.</param>
    /// <param name="traceIdLo">The event's trace id, low half.</param>
    /// <param name="spanId">The event's span id.</param>
    /// <returns>True once the entry is in the log. False only for a spill target's log (see <see cref="Open"/>) that has no room left for it: nothing was written, and the spill rotates.</returns>
    public unsafe bool Append(long timestampTicks, LogLevel level, int templateIndex, string template, ReadOnlySpan<byte> payload,
                              ExceptionInfo? exception = null, int serviceIndex = -1, string? service = null,
                              ulong traceIdHi = 0, ulong traceIdLo = 0, ulong spanId = 0)
    {
        // One unsigned compare covers both -1 and anything past the pool's 65 536 ids.
        bool   pooled = (uint)templateIndex <= ushort.MaxValue;
        ushort index  = pooled ? (ushort)templateIndex : (ushort)0;

        // The text the entry carries (v6, InlineTemplate). An unpooled template's, in every entry:
        // nothing else names it, and each such event may have its own. A pooled template's only
        // when no row can hold it, and then once per log, in the first entry that uses its index
        // (see the class doc): it used to be marked unpooled and carried by every entry. A row
        // already written says the text fits, and a template measured too long is remembered, so
        // a long one is measured once per log, not once per event.
        bool inlineTemplate;
        if (!pooled)                     inlineTemplate = !string.IsNullOrEmpty(template);
        else if (_savedPoolRows[index])  inlineTemplate = false;
        else
        {
            byte state = Volatile.Read(ref _longTemplates[index]);
            if (state == 0 && FitsPoolRow(template))
            {
                EnsurePoolRow(index, template);
                inlineTemplate = false;
            }
            else
            {
                if (state == 0) Volatile.Write(ref _longTemplates[index], LongTemplateUnlogged);
                // Two first uses at once both carry it, which is harmless; one after an entry that
                // carries it takes the write lock after that entry did, so it lands after it.
                inlineTemplate = state != LongTemplateLogged;
            }
        }

        // Counted without allocating, and copied into the mapping below, under the lock, with no
        // byte[] in between.
        int  templateBytes  = inlineTemplate ? Encoding.UTF8.GetByteCount(template) : 0;
        int  inlineLen      = inlineTemplate ? 4 + templateBytes : 0;

        // The same compare for the service. Outside the pool (a full pool answers -1 at ingest) it
        // has no row to be named by, so its text — given, the only copy of the name, as the tier
        // keeps it — goes into the entry itself (v6 InlineService), in every such entry. With none
        // given, no service.
        bool   hasService    = (uint)serviceIndex <= ushort.MaxValue;
        ushort svcIndex      = hasService ? (ushort)serviceIndex : (ushort)0;
        bool   inlineService;
        if (!hasService)                     inlineService = !string.IsNullOrEmpty(service);
        else if (_savedPoolRows[svcIndex])   inlineService = false;
        else
        {
            // A pooled service no row can hold whole (past 65 535 bytes) is carried as a long pooled
            // template is: by the first entry of this log to use its index — the Service bit with the
            // text — and by the index alone after it. A row used to be written cut, and the service
            // replayed cut; then the entry carried it, but every entry did, as an unpooled one is: one
            // 70 KB service made a 70 MB log of 1 000 events, and its replay one string per event. The
            // state is the template's — templates and services share the pool, an index names one
            // text — so whichever use comes first carries it for both.
            byte state = Volatile.Read(ref _longTemplates[svcIndex]);
            if (state == 0 && FitsPoolRow(service))
            {
                EnsurePoolRow(svcIndex, service);
                inlineService = false;
            }
            else
            {
                if (state == 0) Volatile.Write(ref _longTemplates[svcIndex], LongTemplateUnlogged);
                inlineService = state != LongTemplateLogged && !string.IsNullOrEmpty(service);
            }
        }
        int    serviceBytes  = inlineService ? Encoding.UTF8.GetByteCount(service!) : 0;
        int    inlineSvcLen  = inlineService ? 4 + serviceBytes : 0;

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
        bool hasTrace  = (traceIdHi | traceIdLo | spanId) != 0;
        int  traceLen  = hasTrace ? TraceContextSize : 0;
        int  entrySize = EntryHeaderSize + traceLen + inlineLen + inlineSvcLen + payload.Length + excBytes.Length;

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

            // A spill log's pool file shares its capacity: the floor check counted one file's worth
            // for the block, and rows of up to 64 KB each, written first, were counted by nothing.
            long pool = _spill ? Volatile.Read(ref _poolBytes) : 0;
            if (_writeOffset + entrySize + pool > _capacity)
            {
                // A spill target's log is as large as its floor check counted, and no larger: the
                // spill rotates instead. An empty one takes any entry, or one larger than a whole
                // file would be refused by every file in turn.
                if (_spill && _writeOffset > 0) return false;
                if (_writeOffset + entrySize > _capacity) Grow(_writeOffset + entrySize);
            }

            byte* dest = _ptr + FileHeaderSize + _writeOffset;

            ref var eh = ref Unsafe.AsRef<WalEntryHeader>(dest);
            eh.PayloadLength   = (uint)payload.Length;
            eh.TimestampTicks  = timestampTicks;
            eh.Level           = (byte)level;
            // Written explicitly, set or not, like ServiceIndex below: the bytes under a reset
            // write offset are not guaranteed zero, and the checksum below covers whatever is here.
            eh.Flags           = (byte)((pooled ? 0 : EntryFlagUnpooled)
                                      | (hasService ? EntryFlagService : 0)
                                      | (hasTrace ? EntryFlagTrace : 0)
                                      | (inlineTemplate ? EntryFlagInlineTemplate : 0)
                                      | (inlineService ? EntryFlagInlineService : 0));
            eh.TemplateIndex   = index;
            eh.ExceptionLength = (uint)excBytes.Length;
            eh.ServiceIndex    = svcIndex;

            byte* trace = dest + EntryHeaderSize;
            if (hasTrace)
            {
                Unsafe.WriteUnaligned(trace,      traceIdHi);
                Unsafe.WriteUnaligned(trace + 8,  traceIdLo);
                Unsafe.WriteUnaligned(trace + 16, spanId);
            }

            byte* inline = trace + traceLen;
            if (inlineTemplate)
            {
                Unsafe.WriteUnaligned(inline, (uint)templateBytes);
                Encoding.UTF8.GetBytes(template.AsSpan(), new Span<byte>(inline + 4, templateBytes));
            }

            byte* inlineSvc = inline + inlineLen;
            if (inlineService)
            {
                Unsafe.WriteUnaligned(inlineSvc, (uint)serviceBytes);
                Encoding.UTF8.GetBytes(service.AsSpan(), new Span<byte>(inlineSvc + 4, serviceBytes));
            }

            // Checksum the header bytes (crc field excluded — it is the last 4 bytes), the trace
            // context, the inline template and service and both data spans, BEFORE copying the
            // spans: same bytes, and the header, trace and inline parts are already in place.
            uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(dest, ChecksummedHeaderBytes));
            crc      = Crc32c.Append(crc, new ReadOnlySpan<byte>(trace, traceLen));
            crc      = Crc32c.Append(crc, new ReadOnlySpan<byte>(inline, inlineLen));
            crc      = Crc32c.Append(crc, new ReadOnlySpan<byte>(inlineSvc, inlineSvcLen));
            crc      = Crc32c.Append(crc, payload);
            crc      = Crc32c.Append(crc, excBytes);
            eh.Checksum = crc;

            byte* body = inlineSvc + inlineSvcLen;
            if (payload.Length > 0)
                payload.CopyTo(new Span<byte>(body, payload.Length));
            if (excBytes.Length > 0)
                excBytes.CopyTo(new Span<byte>(body + payload.Length, excBytes.Length));

            _writeOffset += entrySize;

            // Update offset in file header (in-place, no flush)
            ref var fh = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            fh.WriteOffset = FileHeaderSize + _writeOffset;

            // A long pooled template is in the log from here on: the entries after this one name it
            // by index alone. Set only once the entry is in place — a refused or failed append
            // leaves the next one to carry it.
            if (pooled && inlineTemplate)     Volatile.Write(ref _longTemplates[index], LongTemplateLogged);
            if (hasService && inlineService)  Volatile.Write(ref _longTemplates[svcIndex], LongTemplateLogged);
        }
        return true;
    }

    /// <summary>
    /// Whether a pool row holds <paramref name="text"/> whole: a row's length is 16 bits. O(1) for
    /// every text of at most 21 845 UTF-16 units (no unit encodes to more than three bytes); only a
    /// longer one is measured.
    /// </summary>
    private static bool FitsPoolRow(string? text) =>
        text is null || text.Length <= MaxPoolRowBytes / 3 || Encoding.UTF8.GetByteCount(text) <= MaxPoolRowBytes;

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
            Volatile.Write(ref _poolBytes, _poolBytes + 4 + len);
        }
    }

    /// <summary>
    /// Bytes in the pool file. A spill log counts them against its capacity (see <see cref="Append"/>):
    /// its rows are written before the entry that needs them is checked for room, so a refused append
    /// may leave at most two rows — a template's and a service's, 64 KB each — past it.
    /// </summary>
    private long _poolBytes;

    /// <summary>The most a refused spill append can leave in the pool file past the log's capacity: two full rows.</summary>
    internal const long MaxRowsPastCapacity = 2 * (4 + MaxPoolRowBytes);

    // ── Durability ───────────────────────────────────────────────────────────

    /// <summary>
    /// Writes back the bytes appended since the previous tick, fsyncs the file, and fsyncs the
    /// template pool if it grew. Called on a timer by the storage engine: acknowledged events are
    /// durable within one interval of power loss instead of "whenever the OS writes back" (up to
    /// the tier's whole life).
    ///
    /// <para>Three things this does NOT do any more. It does not msync the WHOLE mapping —
    /// <c>MemoryMappedViewAccessor.Flush</c> hands FlushViewOfFile/msync the entire 64 MB+
    /// view, and that cost scales with the mapping, not with what was written, so a tick that
    /// appended 40 KB walked 64 MB of page tables. It does not hold <see cref="_writeLock"/>
    /// across the write-back or the file-handle flush, which are what wait for the disk and what
    /// the single appender was stalled behind (see <see cref="FlushTail"/>; on Linux the tick
    /// issues no msync at all — the fsync writes the mapping's pages). And it does no I/O at all on a tick where
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

    /// <summary>
    /// The WAL half of <see cref="Flush"/>: write the un-synced tail of the mapping back, then flush
    /// the file handle — both OUTSIDE <see cref="_writeLock"/>, which only fixes the range.
    ///
    /// <para>The view's write-back used to run under the lock, and the appender waited it out: on
    /// Linux that was msync(MS_SYNC), which waits for the I/O itself. On Linux it is not issued at
    /// all now — a shared mapping's pages are the file's page cache, and the fsync below writes
    /// them, header page included. Elsewhere (FlushViewOfFile only queues the pages to the file
    /// system; the handle flush then waits for the drive) it runs under the read side of
    /// <see cref="_mapLock"/>, which <see cref="Grow"/> and <see cref="Dispose"/> take for writing
    /// before they unmap: a write-back never runs on a view being torn down, and they wait one out.
    /// An append landing meanwhile is safe to race: the watermark claims only the range fixed under
    /// the lock, whose entries were complete, and a page caught half-written past it is written
    /// again by the next tick — or, after a power loss, cut at its entry's checksum.</para>
    /// </summary>
    private void FlushTail()
    {
        FileStream? handle;
        long        from, writeEnd;
        lock (_writeLock)
        {
            // Not "or the view is gone": a grow whose restoring map failed too leaves the log unmapped
            // until rotation, and its entries before the grow — acknowledged — still need the handle
            // flush. Their pages are the file's (the view was let go of without a write-back, and the
            // handle flush writes them, as it does after every grow); the tick returned here instead,
            // on every tick, and an idle log was never made durable at all.
            if (_disposed) return;

            writeEnd = FileHeaderSize + _writeOffset;
            if (writeEnd <= _lastFlushedOffset) return;

            // The watermark is NOT advanced here. It means "durable up to", and nothing is
            // durable until the handle flush below returns. Advancing it first made a failed
            // FlushFileBuffers permanent while the WAL sat idle: the next tick saw
            // writeEnd == watermark and did nothing, so the acknowledged tail stayed in the
            // drive cache until another event arrived or the WAL rotated.
            from   = _lastFlushedOffset;
            handle = _fileStream;
        }

        if (!OperatingSystem.IsLinux())
        {
            _mapLock.EnterReadLock();
            try
            {
                if (_ptr is not null)   // unmapped since: by Dispose, which writes it back itself, or a failed grow
                {
                    ViewFlushHookForTest?.Invoke();
                    if (!TryFlushRange(from, writeEnd))
                    {
                        // Correct, just slower — and silent, which is the trap: a platform where
                        // the range call always fails would msync the whole mapping every tick
                        // for ever and look exactly like a working one. The counter records it,
                        // but only tests read it today. The WAL has no logger and no diagnostics
                        // surface exposes the counter, so on a live host this is still invisible.
                        // Surfacing it (a log once, or a StorageEngine diagnostic) is follow-up work.
                        Interlocked.Increment(ref _rangeFlushFailures);
                        _accessor?.Flush();     // fallback: whole view, as before
                    }
                }
            }
            finally { _mapLock.ExitReadLock(); }
        }

        // Outside the lock on purpose: FlushViewOfFile only queues the pages to the
        // filesystem, FlushFileBuffers is what waits for the drive — milliseconds on a
        // spinning disk, and the appender has no reason to wait with it. On Linux this fsync
        // is the whole write-back: it writes the mapping's dirty pages and waits for them.
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
        // The fsync runs OFF _poolLock: an append that needs a new row takes that lock with the
        // engine's writer lock held, and it used to wait out the tick's fsync there — every writer
        // with it. Under the lock only the claim: every row so far is in the OS (EnsurePoolRow
        // flushes the stream's buffer under this lock, and the handle's getter flushes it again), so
        // the fsync of the handle makes them durable; a row written meanwhile dirties the pool again
        // for the next tick, and a failed fsync gives the claim back.
        try
        {
            Microsoft.Win32.SafeHandles.SafeFileHandle handle;
            lock (_poolLock)
            {
                if (!_poolDirty || _poolStream is null) return;
                // Inside the try: the getter writes the stream's buffer out first, and a failure there
                // (a row whose own write failed, retried) is the fsync's, kept dirty and retried — it
                // used to leave from outside the try, past the rule below, and replaced the WAL's
                // exception on a tick that had one.
                handle     = PoolHandleHookForTest is { } get ? get(_poolStream) : _poolStream.SafeFileHandle;
                _poolDirty = false;
            }
            if (PoolFlushHookForTest is { } hook) hook(handle);
            else RandomAccess.FlushToDisk(handle);
            Interlocked.Increment(ref _poolFlushCount);
        }
        catch (ObjectDisposedException) { /* disposed meanwhile: Dispose fsyncs the pool itself */ }
        catch (Exception ex)
        {
            lock (_poolLock) _poolDirty = true;
            if (ex is IOException || walFailed) return;   // retried next interval; a WAL failure is the one reported
            throw;
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
    /// Test seam: when set, <see cref="Flush"/> calls this with the pool file's handle instead of
    /// fsyncing it (<see cref="RandomAccess.FlushToDisk"/>), off <see cref="_poolLock"/>, so a test
    /// can watch, hold or fail the pool fsync. Never set in production.
    /// </summary>
    internal Action<Microsoft.Win32.SafeHandles.SafeFileHandle>? PoolFlushHookForTest;

    /// <summary>
    /// Test seam: when set, the tick's claim of the pool file calls this in place of the stream's
    /// <c>SafeFileHandle</c> getter — which writes the stream's buffer out first, and can fail doing
    /// so — under <see cref="_poolLock"/>. Never set in production.
    /// </summary>
    internal Func<FileStream, Microsoft.Win32.SafeHandles.SafeFileHandle>? PoolHandleHookForTest;

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
        var  texts = new WalkTexts();

        while (pos + EntryHeaderSize <= end)
        {
            if (!TryParseLiveEntry(pos, end, texts, out var entry, out long entrySize))
                yield break;

            yield return entry!;
            pos += entrySize;
        }
    }

    // Iterator bodies cannot touch pointers — this shim reads _ptr outside the iterator.
    private bool TryParseLiveEntry(long pos, long end, WalkTexts texts, out WalEntry? entry, out long entrySize)
        => TryParseEntry(_ptr, pos, end, EntryLayout.V6, texts, out entry, out entrySize);

    /// <summary>
    /// Parses one entry at <paramref name="pos"/>. Bounds are checked and (v4 on) the CRC is
    /// verified over the in-map spans BEFORE anything is allocated, so a garbage length
    /// field cannot OOM and a torn entry cannot materialise. Returns false at the first
    /// entry that does not verify — everything past it is by definition not durable.
    /// </summary>
    /// <param name="texts">
    /// What the walk has read of the log's texts so far (see <see cref="WalkTexts"/>). The caller
    /// passes one for the whole walk, in log order.
    /// </param>
    private static unsafe bool TryParseEntry(
        byte* basePtr, long pos, long end, EntryLayout layout, WalkTexts texts,
        out WalEntry? entry, out long entrySize)
    {
        entry = null;
        if (!TryParseRecord(basePtr, pos, end, layout, texts, out var record, out entrySize)) return false;
        entry = record.ToEntry();
        return true;
    }

    /// <summary>
    /// <see cref="TryParseEntry"/> without the copy: the record's payload is a span over the mapping
    /// at <paramref name="basePtr"/>, and lives as long as the mapping does.
    /// </summary>
    private static unsafe bool TryParseRecord(
        byte* basePtr, long pos, long end, EntryLayout layout, WalkTexts texts,
        out WalRecord record, out long entrySize)
    {
        record    = default;
        entrySize = 0;

        int headerSize = HeaderSizeOf(layout);
        if (pos + headerSize > end) return false;

        byte* src = basePtr + FileHeaderSize + pos;
        // Every layout shares the first 20 bytes; the struct is read past them only for v5 on.
        ref var eh = ref Unsafe.AsRef<WalEntryHeader>(src);

        // Only v6 has the trace context and the inline template; their bits were reserved, and
        // written as zero, before it.
        int traceLen = layout == EntryLayout.V6 && (eh.Flags & EntryFlagTrace) != 0 ? TraceContextSize : 0;
        byte* trace  = src + headerSize;

        // The inline template's length is read before the checksum vouches for it, so it is held
        // to the same bounds as every other length here: inside the data, and a span-sized int.
        long inlineLen = 0;
        if (layout == EntryLayout.V6 && (eh.Flags & EntryFlagInlineTemplate) != 0)
        {
            if (pos + headerSize + traceLen + 4 > end) return false;
            uint textLen = Unsafe.ReadUnaligned<uint>(trace + traceLen);
            if (textLen > int.MaxValue - 4) return false;
            inlineLen = 4 + textLen;
        }

        // The inline service (v6, InlineService) follows the inline template, held to the same bounds.
        long inlineSvcLen = 0;
        if (layout == EntryLayout.V6 && (eh.Flags & EntryFlagInlineService) != 0)
        {
            if (pos + headerSize + traceLen + inlineLen + 4 > end) return false;
            uint textLen = Unsafe.ReadUnaligned<uint>(trace + traceLen + inlineLen);
            if (textLen > int.MaxValue - 4) return false;
            inlineSvcLen = 4 + textLen;
        }

        long total = (long)headerSize + traceLen + inlineLen + inlineSvcLen + eh.PayloadLength + eh.ExceptionLength;
        if (pos + total > end)
            return false;
        // Span construction takes int — in a WAL past 2 GiB a garbage length in
        // [2^31, 2^32) passes the long-math bounds check above and the unchecked cast
        // below would go negative: an ArgumentOutOfRangeException instead of the clean
        // stop this parser promises. Such a length is by definition a torn entry.
        if (eh.PayloadLength > int.MaxValue || eh.ExceptionLength > int.MaxValue)
            return false;

        byte* inline    = trace + traceLen;
        byte* inlineSvc = inline + inlineLen;
        byte* body      = inlineSvc + inlineSvcLen;
        var traceSpan   = new ReadOnlySpan<byte>(trace, traceLen);
        var inlineSpan  = new ReadOnlySpan<byte>(inline, (int)inlineLen);
        var svcSpan     = new ReadOnlySpan<byte>(inlineSvc, (int)inlineSvcLen);
        var payloadSpan = new ReadOnlySpan<byte>(body, (int)eh.PayloadLength);
        var excSpan     = new ReadOnlySpan<byte>(body + (int)eh.PayloadLength, (int)eh.ExceptionLength);

        // v4 on end their header with the checksum, over every header byte before it, the trace
        // context, the inline template and service and both spans — v5's over the service index
        // too, v6's over the trace context and the inline texts, so a torn or rotted field fails
        // the entry like any other. v3 has none.
        if (layout != EntryLayout.V3)
        {
            int  covered = headerSize - 4;
            uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(src, covered));
            crc      = Crc32c.Append(crc, traceSpan);
            crc      = Crc32c.Append(crc, inlineSpan);
            crc      = Crc32c.Append(crc, svcSpan);
            crc      = Crc32c.Append(crc, payloadSpan);
            crc      = Crc32c.Append(crc, excSpan);
            if (crc != Unsafe.ReadUnaligned<uint>(src + covered))
                return false;
        }

        ExceptionInfo? exception      = null;
        Exception?     exceptionFault = null;
        if (eh.ExceptionLength > 0)
        {
            // v3 entries carry no checksum, so garbage here throws deep inside msgpack — treat it as
            // the end of the durable data instead of failing the whole WAL.
            //
            // Never an OutOfMemoryException, which says nothing about the bytes. Taken for a torn
            // entry, an out-of-memory decoding a large stack trace ended the replay at that event, and
            // the replay then published the prefix and deleted the log. It propagates: the replay is
            // retried by the despill, which backs off and reads the block again.
            //
            // A v4+ entry's exception is under its checksum, verified above: the bytes are the ones
            // written, and a decode that fails on them fails the same way at every reading. It used to
            // propagate like the out-of-memory, and that entry blocked its whole log for good — the
            // replay failed at every start, the despill retried for ever, and a rollback waiting for a
            // start that logs neither replay line waited with it. The event is read without its
            // exception now, the rest of it whole, and the record says why (ExceptionDecodeError):
            // the replay and the despill name it in an Error.
            try
            {
                BeforeExceptionDecodeForTest.Value?.Invoke();   // stands in for the decode throwing
                exception = ExceptionInfo.FromBytes(excSpan);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                if (layout == EntryLayout.V3) return false;
                exceptionFault = ex;
            }
        }

        // v3 entries predate the flag and their byte 13 was never written: never unpooled.
        bool    unpooled = layout != EntryLayout.V3 && (eh.Flags & EntryFlagUnpooled) != 0;
        string? template = inlineLen != 0 ? texts.Inline(basePtr, inline + 4 - basePtr, (int)inlineLen - 4) : null;
        if (layout == EntryLayout.V6 && !unpooled)
        {
            // A pooled template too long for a row: the first entry of the log that uses the index
            // carries it, and every later one gets the same string. Either way the event has no row
            // to resolve, and is handed on as an unpooled one with its text attached.
            if (template is not null)
            {
                texts.ByIndex[eh.TemplateIndex] = template;
                unpooled = true;
            }
            else if (texts.ByIndex.Count != 0 && texts.ByIndex.TryGetValue(eh.TemplateIndex, out var known))
            {
                template = known;
                unpooled = true;
            }
        }

        // Only v5 on has the field. A v4 entry's bytes 20-23 are its checksum and its flag bits past
        // bit 0 were reserved, so it names no service whatever they hold.
        int     serviceIndex = layout >= EntryLayout.V5 && (eh.Flags & EntryFlagService) != 0 ? eh.ServiceIndex : -1;
        string? service      = inlineSvcLen != 0 ? texts.Inline(basePtr, inlineSvc + 4 - basePtr, (int)inlineSvcLen - 4) : null;
        if (layout == EntryLayout.V6 && serviceIndex >= 0)
        {
            // A pooled service too long for a row, as the template above: carried by the first entry
            // of the log that uses the index (the Service bit WITH the text), named by the index alone
            // after it — or by a template's index, the pool being one. The event has no row to
            // resolve either way, and is handed on with its text and no index, as one outside the pool.
            if (service is not null)
            {
                texts.ByIndex[(ushort)serviceIndex] = service;
                serviceIndex = -1;
            }
            else if (texts.ByIndex.Count != 0 && texts.ByIndex.TryGetValue((ushort)serviceIndex, out var known))
            {
                service      = known;
                serviceIndex = -1;
            }
        }

        record = new WalRecord
        {
            TimestampTicks = eh.TimestampTicks,
            Level          = (LogLevel)eh.Level,
            TemplateIndex  = eh.TemplateIndex,
            Unpooled       = unpooled,
            ServiceIndex   = serviceIndex,
            TraceIdHi      = traceLen != 0 ? Unsafe.ReadUnaligned<ulong>(trace)      : 0,
            TraceIdLo      = traceLen != 0 ? Unsafe.ReadUnaligned<ulong>(trace + 8)  : 0,
            SpanId         = traceLen != 0 ? Unsafe.ReadUnaligned<ulong>(trace + 16) : 0,
            Template       = template,
            Service        = service,
            Payload        = payloadSpan,
            Exception      = exception,
            ExceptionDecodeError = exceptionFault,
        };
        entrySize = total;
        return true;
    }

    /// <summary>
    /// What one walk of a log has read of its texts, in log order.
    ///
    /// <para>The long pooled ones, by index (<see cref="ByIndex"/>; see InlineTemplate and InlineService
    /// in the class doc): the entry that carries one adds it, and a later entry naming its index gets
    /// that same string.</para>
    ///
    /// <para>And ONE string per distinct inline text (<see cref="Inline"/>). A template or service
    /// outside the pool is in every entry that uses it, and the walk used to build a string for each,
    /// which the tier it replayed into then held per event: a log of 3 500 events of one 65 600-character
    /// service (written before such a service was carried once) made 3 500 copies, ~460 MB, and its
    /// replay ran a 384 MiB heap out at every start. A text is looked up by its bytes where they lie,
    /// before a string is made, so a repeat costs a hash and a compare and allocates nothing.</para>
    /// </summary>
    private sealed class WalkTexts
    {
        /// <summary>The long pooled texts met so far, by pool index.</summary>
        internal readonly Dictionary<ushort, string> ByIndex = new();

        // The distinct inline texts so far: a hash of the bytes → the newest of a chain in _seen.
        private readonly Dictionary<int, int> _byHash = new();
        private readonly List<SeenText>       _seen   = new();

        /// <summary>A distinct text, by where its bytes lie (from the walk's base pointer), and the string made for it.</summary>
        private readonly record struct SeenText(long At, int Length, string Text, int Next);

        /// <summary>
        /// The text of the <paramref name="length"/> UTF-8 bytes at <paramref name="at"/> past
        /// <paramref name="basePtr"/>: the string an earlier entry of this walk got for the same bytes,
        /// or a new one. Every call of a walk passes the same mapping, which outlives the walk.
        /// </summary>
        internal unsafe string Inline(byte* basePtr, long at, int length)
        {
            var bytes = new ReadOnlySpan<byte>(basePtr + at, length);
            var hash  = new HashCode();
            hash.AddBytes(bytes);
            int key  = hash.ToHashCode();
            int head = _byHash.TryGetValue(key, out int newest) ? newest : -1;
            for (int i = head; i >= 0; i = _seen[i].Next)
            {
                var seen = _seen[i];
                if (seen.Length == length && new ReadOnlySpan<byte>(basePtr + seen.At, length).SequenceEqual(bytes))
                    return seen.Text;
            }
            string text = Encoding.UTF8.GetString(bytes);
            _seen.Add(new SeenText(at, length, text, head));
            _byHash[key] = _seen.Count - 1;
            return text;
        }
    }

    // ── Grow ─────────────────────────────────────────────────────────────────

    /// <param name="required">The capacity the entry being appended needs. The log doubles, or grows
    /// to this if doubling is not enough: one doubling used to be assumed to be, and an entry larger
    /// than twice the capacity — reachable in a spill log, as small as 1 MB, whose first entry is
    /// taken whatever its size — was copied past the end of the mapping.</param>
    private unsafe void Grow(long required)
    {
        // Release current mapping, extend file, re-map. Runs under _writeLock (called
        // from Append). If extension or re-map fails (disk full is the typical cause —
        // and doubling is exactly when it bites), restore the OLD mapping before
        // rethrowing: leaving _ptr dangling let the next smaller append write through
        // freed memory. Same shape as the metric/span WAL Grow, which were hardened
        // for this first.
        long oldCapacity = _capacity;
        long oldFileSize = FileHeaderSize + oldCapacity;
        // A spill log grows only for its first entry, and only to that entry's size: it is as large
        // as its floor check counted, and a doubling made a log that took a first entry between one
        // and two capacities go on taking entries up to twice what was counted.
        long newCapacity = _spill ? required : Math.Max(oldCapacity * 2, required);
        long newFileSize = FileHeaderSize + newCapacity;

        // Waits out a tick writing the old view back (FlushTail, off the write lock) before the view
        // goes: the write-back must not run on an unmapped range.
        _mapLock.EnterWriteLock();
        try
        {
            // Without writing the old view back: that is a write-back of the whole view, which held the
            // appender (and every writer behind it) 4-30 ms here and far longer on a slow disk. Its
            // pages are the file's — the next view maps the same ones — and the next tick writes back
            // the range from its watermark, from the new view, and fsyncs the file, as it always does.
            Unmap(writeBack: false);
            try
            {
                _fileStream!.SetLength(newFileSize);
                // Reserved, as at the open (round-5 H-F): a full volume fails this grow, which the
                // catch undoes, and not the first append into a new page, with SIGBUS.
                FileSpace.Reserve(_fileStream.SafeFileHandle, oldFileSize, newFileSize - oldFileSize);
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
        finally { _mapLock.ExitWriteLock(); }
    }

    /// <summary>
    /// The view, as the ticks' write-back reads it (FlushTail, off <see cref="_writeLock"/>) against
    /// <see cref="Grow"/> and <see cref="Dispose"/>, which unmap it: they take it for writing, under
    /// the write lock, and the write-back for reading, outside it — so neither ever waits on the
    /// other while holding what the other needs. Never disposed: a tick that raced a dispose to it
    /// would meet an ObjectDisposedException, and it holds no handle until a wait creates one.
    /// </summary>
    private readonly ReaderWriterLockSlim _mapLock = new(LockRecursionPolicy.NoRecursion);

    /// <summary>
    /// Test seam: called by a tick inside the view's write-back, outside <see cref="_writeLock"/> and
    /// under the read side of <see cref="_mapLock"/> — blocking it holds a write-back in flight. Not
    /// reached on Linux, where the tick issues none. Never set in production.
    /// </summary>
    internal Action? ViewFlushHookForTest;

    private void Map(long fileSize)
    {
        MapHookForTest?.Invoke(fileSize);
        _mmf      = MemoryMappedFile.CreateFromFile(_fileStream!, null, fileSize,
                        MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
        _accessor = _mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.ReadWrite);
        _ptr      = null;
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
    }

    /// <param name="writeBack">
    /// Whether to write the view back as it goes — what disposing the accessor does (FlushViewOfFile,
    /// msync), for the whole view. False unmaps it as it is, its dirty pages left to the file's cache.
    /// </param>
    private void Unmap(bool writeBack = true)
    {
        if (_accessor is not null)
        {
            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            if (writeBack) UnmapWriteBackForTest?.Invoke();
            else
            {
                // Closing the view's handle first unmaps it with no write-back: the accessor's dispose
                // writes a view back only while it is still open.
                _accessor.SafeMemoryMappedViewHandle.Dispose();
                UnmapWithoutWriteBackForTest?.Invoke(_accessor.SafeMemoryMappedViewHandle.IsClosed);
            }
            _accessor.Dispose();
        }
        _mmf?.Dispose();
        _accessor = null;
        _mmf      = null;
        _ptr      = null;
    }

    /// <summary>Test seam: runs as an unmap writes the whole view back (a dispose's, not a grow's). Null in production.</summary>
    internal Action? UnmapWriteBackForTest;

    /// <summary>
    /// Test seam: runs in an unmap without write-back (a grow's) just before the accessor is disposed,
    /// with whether the view's handle is closed by then — what makes that dispose skip its write-back
    /// of the whole view. Null in production.
    /// </summary>
    internal Action<bool>? UnmapWithoutWriteBackForTest;

    /// <summary>Test seam: runs as the log maps its file, with the size; throwing from it is a map that failed. Null in production.</summary>
    internal Action<long>? MapHookForTest;

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
            // A tick writing the view back off the write lock is waited out first (see _mapLock).
            _mapLock.EnterWriteLock();
            try { Unmap(); } // the view flushes dirty pages on dispose
            finally { _mapLock.ExitWriteLock(); }
            // A spill file is cut to what it holds once nothing maps it (a mapped file cannot
            // shrink on Windows). It closes long before its pre-size is used — a block closes at the
            // tier's first refusal, ~9 MB of 600 B events in a ~74 MB file — and NTFS allocates the
            // whole pre-size, so the closed files of one long spill held 2 to 8 times the disk their
            // data needed, and reached the spill floor that much sooner. Recovery reads the length
            // and clamps the header's write offset to it; nothing past the offset is lost.
            if (_spill && _fileStream is not null)
                try { _fileStream.SetLength(FileHeaderSize + _writeOffset); } catch { /* keeps its size; still valid */ }
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

    /// <summary>
    /// A log's pool rows. A file that ends part way through a row ends the rows there — the torn
    /// tail of a crash — and so does a row of length 0, the tail a power loss zero-filled. A
    /// failure to read it — out of memory, the file held by another process —
    /// propagates: it used to be swallowed, and the caller went on with the rows read so far, so the
    /// replay resolved every missing row's index through the LIVE pool (another template) and lost
    /// the services those rows named, then deleted the log. The replay now keeps the WAL, and the
    /// despill backs off and reads the block again.
    /// </summary>
    public static Dictionary<ushort, string> LoadPool(string poolPath)
    {
        var dict = new Dictionary<ushort, string>();
        if (!File.Exists(poolPath)) return dict;
        using var fs = new FileStream(poolPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Span<byte> hdr = stackalloc byte[4];
        while (fs.ReadAtLeast(hdr, 4, throwOnEndOfStream: false) == 4)
        {
            ushort index = BinaryPrimitives.ReadUInt16LittleEndian(hdr);
            ushort len   = BinaryPrimitives.ReadUInt16LittleEndian(hdr[2..]);
            // No row is written empty (EnsurePoolRow skips an empty text), so a length of 0 is the torn
            // tail: NTFS zero-fills the part of a file whose size a power loss kept and whose data it
            // did not. Read as rows, that was (0, "") over and over, and the first of them overwrote row
            // 0 — ordinarily the log's first template, which its events then replayed without.
            if (len == 0) break;
            BeforePoolRowForTest.Value?.Invoke(index);
            var    bytes = new byte[len];
            if (fs.ReadAtLeast(bytes, len, throwOnEndOfStream: false) != len) break;
            dict[index] = Encoding.UTF8.GetString(bytes);
        }
        return dict;
    }

    /// <summary>
    /// Test seam: called by the recovery walk before it decodes an entry's exception — throwing from
    /// it is that decode failing. An <see cref="AsyncLocal{T}"/>, so it reaches only the walks of the
    /// test that set it (the replay runs in the engine's constructor). Null in production.
    /// </summary>
    internal static readonly AsyncLocal<Action?> BeforeExceptionDecodeForTest = new();

    /// <summary>Test seam: called by <see cref="LoadPool"/> with each row's index before it reads the row. Null in production.</summary>
    internal static readonly AsyncLocal<Action<ushort>?> BeforePoolRowForTest = new();

    /// <summary>The format this build writes, and the newest it reads (see WalVersion).</summary>
    public const ushort FormatVersion = WalVersion;

    public static (ulong SegmentId, List<WalEntry> Entries) ReadForRecovery(string walPath) =>
        ReadForRecovery(walPath, out _, out _);

    /// <summary>
    /// Every complete entry of a log, materialised — each payload copied into the heap. For a caller
    /// that wants the list; the startup replay and the despill walk the log with
    /// <see cref="RecoveryReader"/> instead, one entry at a time, where it lies.
    /// </summary>
    /// <param name="version">
    /// The format the file's header claims, or 0 when the file is not one of these logs. A version
    /// above <see cref="FormatVersion"/> comes back with no entries: a later release wrote the file,
    /// and only it can read it.
    /// </param>
    /// <param name="headerRecordsEntries">
    /// False only when the header's WriteOffset is exactly where the first entry would begin: the
    /// log every clean stop leaves, which no version so far has written anything past. Read before
    /// the version is looked at, so a later release's file can be told empty too; any other value
    /// says "may hold entries", which is also what a later format that moved the field would read as.
    /// </param>
    public static (ulong SegmentId, List<WalEntry> Entries) ReadForRecovery(
        string walPath, out ushort version, out bool headerRecordsEntries)
    {
        using var reader = RecoveryReader.Open(walPath);
        version              = reader.Version;
        headerRecordsEntries = reader.HeaderRecordsEntries;
        var entries = new List<WalEntry>();
        while (reader.TryReadNext(out var record)) entries.Add(record.ToEntry());
        return (reader.SegmentId, entries);
    }

    /// <summary>
    /// A log's complete entries read one at a time where they lie in its mapping — the recovery walk
    /// with no copy of the log in the heap. <see cref="ReadForRecovery(string, out ushort, out bool)"/>
    /// collected every entry first, each payload copied into a byte array, and a replay then held the
    /// log twice: in that list, and in the tier it filled from it. Read-only; the file is mapped
    /// until <see cref="Dispose"/>, which on Windows is what lets it be deleted.
    /// </summary>
    public sealed unsafe class RecoveryReader : IDisposable
    {
        private MemoryMappedFile?         _mmf;
        private MemoryMappedViewAccessor? _view;
        private byte*                     _ptr;
        private readonly EntryLayout      _layout;
        private readonly bool             _readable;
        private readonly long             _end;
        private long                      _pos;
        private bool                      _stopped;
        private readonly WalkTexts        _texts = new();

        /// <summary>The block the log's header names; 0 when the file is not one of these logs.</summary>
        public ulong SegmentId { get; }

        /// <summary>The format the header claims (see <see cref="ReadForRecovery(string, out ushort, out bool)"/>); 0 for no log.</summary>
        public ushort Version { get; }

        /// <summary>See <see cref="ReadForRecovery(string, out ushort, out bool)"/>.</summary>
        public bool HeaderRecordsEntries { get; }

        /// <summary>
        /// The bytes of the data area the walk covers — the header's write offset, clamped to the
        /// file: an upper bound on what the entries hold (payloads, and events at no fewer than 20
        /// bytes each). 0 when there is nothing to read.
        /// </summary>
        public long DataBytes => _readable ? _end : 0;

        private RecoveryReader() { }

        private RecoveryReader(MemoryMappedFile mmf, MemoryMappedViewAccessor view, byte* ptr, EntryLayout layout, bool readable,
                               ulong segmentId, ushort version, bool recordsEntries, long end)
        {
            _mmf                 = mmf;
            _view                = view;
            _ptr                 = ptr;
            _layout              = layout;
            _readable            = readable;
            _end                 = end;
            SegmentId            = segmentId;
            Version              = version;
            HeaderRecordsEntries = recordsEntries;
        }

        /// <summary>Opens <paramref name="walPath"/>; a missing, short or foreign file reads as a log of no entries.</summary>
        public static RecoveryReader Open(string walPath)
        {
            if (!File.Exists(walPath)) return new RecoveryReader();
            long fileSize = new FileInfo(walPath).Length;
            if (fileSize < FileHeaderSize) return new RecoveryReader();

            var mmf = MemoryMappedFile.CreateFromFile(walPath, FileMode.Open, null, fileSize, MemoryMappedFileAccess.Read);
            MemoryMappedViewAccessor? view = null;
            byte* ptr = null;
            try
            {
                view = mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.Read);
                view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

                ref var fh = ref Unsafe.AsRef<WalFileHeader>(ptr);
                if (fh.Magic != MagicNumber)
                    return new RecoveryReader(mmf, view, ptr, EntryLayout.V6, readable: false, 0, 0, false, 0);

                ushort version        = fh.Version;
                bool   recordsEntries = fh.WriteOffset != FileHeaderSize;
                // v6 = current. v5 = the releases before it: no trace context. v4: checksummed, no
                // service. v3: replayable, no per-entry validation possible. Anything else is
                // unreplayable by construction.
                EntryLayout layout;
                bool        known = true;
                switch (version)
                {
                    case WalVersion:   layout = EntryLayout.V6; break;
                    case WalVersionV5: layout = EntryLayout.V5; break;
                    case WalVersionV4: layout = EntryLayout.V4; break;
                    case WalVersionV3: layout = EntryLayout.V3; break;
                    default:           layout = EntryLayout.V6; known = false; break;
                }

                // The header page and the data pages hit disk independently — a torn header
                // can claim ANY offset. Clamp to the file so the walk stays inside the view
                // (unclamped, this was an uncatchable AccessViolation in the engine
                // constructor: a crash-loop until an operator deleted the file by hand).
                long writeOffset = fh.WriteOffset - FileHeaderSize;
                long maxData     = fileSize - FileHeaderSize;
                if (writeOffset > maxData) writeOffset = maxData;

                return new RecoveryReader(mmf, view, ptr, layout, readable: known && writeOffset > 0,
                                          fh.SegmentId, version, recordsEntries, Math.Max(0, writeOffset));
            }
            catch
            {
                if (ptr != null) view!.SafeMemoryMappedViewHandle.ReleasePointer();
                view?.Dispose();
                mmf.Dispose();
                throw;
            }
        }

        /// <summary>
        /// The next complete entry, or false at the end of the durable data — the first entry that
        /// does not verify, and everything past it. The record's payload is a span over the mapping:
        /// valid until this reader is disposed.
        /// </summary>
        public bool TryReadNext(out WalRecord record)
        {
            record = default;
            if (_stopped || !_readable || _pos + HeaderSizeOf(_layout) > _end) return false;
            if (!TryParseRecord(_ptr, _pos, _end, _layout, _texts, out record, out long size))
            {
                _stopped = true;
                return false;
            }
            _pos += size;
            return true;
        }

        public void Dispose()
        {
            if (_view is not null)
            {
                if (_ptr != null) _view.SafeMemoryMappedViewHandle.ReleasePointer();
                _ptr = null;
                _view.Dispose();
                _view = null;
            }
            _mmf?.Dispose();
            _mmf = null;
        }
    }
}

/// <summary>
/// One entry of a log as <see cref="WriteAheadLog.RecoveryReader"/> reads it: the fields of a
/// <see cref="WalEntry"/>, with its payload a span over the log's mapping rather than a copy.
/// </summary>
public readonly ref struct WalRecord
{
    public long               TimestampTicks { get; init; }
    public LogLevel           Level          { get; init; }
    public ushort             TemplateIndex  { get; init; }
    /// <inheritdoc cref="WalEntry.Unpooled"/>
    public bool               Unpooled       { get; init; }
    /// <inheritdoc cref="WalEntry.ServiceIndex"/>
    public int                ServiceIndex   { get; init; }
    public ulong              TraceIdHi      { get; init; }
    public ulong              TraceIdLo      { get; init; }
    public ulong              SpanId         { get; init; }
    /// <inheritdoc cref="WalEntry.Template"/>
    public string?            Template       { get; init; }
    /// <inheritdoc cref="WalEntry.Service"/>
    public string?            Service        { get; init; }
    /// <summary>The properties payload, over the mapping: valid until the reader is disposed.</summary>
    public ReadOnlySpan<byte> Payload        { get; init; }
    public ExceptionInfo?     Exception      { get; init; }
    /// <inheritdoc cref="WalEntry.ExceptionDecodeError"/>
    public Exception?         ExceptionDecodeError { get; init; }

    /// <inheritdoc cref="WalEntry.ServiceIndexIn"/>
    public int ServiceIndexIn(IReadOnlyDictionary<ushort, string> poolRows) =>
        ServiceIndex >= 0 && poolRows.ContainsKey((ushort)ServiceIndex) ? ServiceIndex : -1;

    /// <summary>The entry, materialised: its payload copied into the heap.</summary>
    public WalEntry ToEntry() => new()
    {
        TimestampTicks = TimestampTicks,
        Level          = Level,
        TemplateIndex  = TemplateIndex,
        Unpooled       = Unpooled,
        ServiceIndex   = ServiceIndex,
        TraceIdHi      = TraceIdHi,
        TraceIdLo      = TraceIdLo,
        SpanId         = SpanId,
        Template       = Template,
        Service        = Service,
        Payload        = Payload.IsEmpty ? [] : Payload.ToArray(),
        Exception      = Exception,
        ExceptionDecodeError = ExceptionDecodeError,
    };
}

public sealed class WalEntry
{
    public long           TimestampTicks { get; init; }
    public LogLevel       Level          { get; init; }
    public ushort         TemplateIndex  { get; init; }

    /// <summary>
    /// The event's template has no pool row to resolve: it was outside the template pool when it was
    /// logged, or (v6) longer than a row holds. <see cref="TemplateIndex"/> then names nothing to
    /// resolve. A v6 log gives the text in <see cref="Template"/>; an older one never stored it, and
    /// the event is recovered with no template.
    /// </summary>
    public bool           Unpooled       { get; init; }

    /// <summary>
    /// The event's <c>@service</c> as an index into the intern pool, or -1: the event had none, or
    /// the entry predates format v5 and could not say. The index's text is a row of the WAL's pool
    /// file; resolve it through <see cref="ServiceIndexIn"/>, not straight into a live pool.
    /// </summary>
    public int            ServiceIndex   { get; init; } = -1;

    /// <summary>The event's trace id, high half; 0 when it had none or the entry predates format v6.</summary>
    public ulong          TraceIdHi      { get; init; }

    /// <summary>The event's trace id, low half; 0 when it had none or the entry predates format v6.</summary>
    public ulong          TraceIdLo      { get; init; }

    /// <summary>The event's span id; 0 when it had none or the entry predates format v6.</summary>
    public ulong          SpanId         { get; init; }

    /// <summary>
    /// The event's template text where no pool row could carry it (v6, InlineTemplate): one outside
    /// the pool, from the entry itself; one longer than a row holds, from the first entry of the log
    /// to use its index — every entry of that index gets the same string. Null otherwise: the
    /// template, if any, is <see cref="TemplateIndex"/>'s pool row.
    /// </summary>
    public string?        Template       { get; init; }

    /// <summary>
    /// The event's <c>@service</c> text where no pool row names it (v6, InlineService): the service
    /// was outside a full intern pool when the event was logged, and the entry carries the name.
    /// Null otherwise: the service, if any, is <see cref="ServiceIndex"/>'s row.
    /// </summary>
    public string?        Service        { get; init; }

    public byte[]         Payload        { get; init; } = [];
    public ExceptionInfo? Exception      { get; init; }

    /// <summary>
    /// Why the event's exception is not in <see cref="Exception"/>: the entry verified — its bytes are
    /// the ones written — and the decode of its exception failed on them, as it will at every reading.
    /// The event is read without it, the rest of it whole; null otherwise. (An out-of-memory decoding
    /// it says nothing about the bytes, and fails the walk instead.)
    /// </summary>
    public Exception?     ExceptionDecodeError { get; init; }

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
