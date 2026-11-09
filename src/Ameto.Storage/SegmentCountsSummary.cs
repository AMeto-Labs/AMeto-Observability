using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// How many events one cold segment holds per <c>(15-minute slot, service, level)</c> — everything
/// <c>GET /api/events/counts</c> needs from a segment that lies wholly inside its window.
///
/// <para>Without it the endpoint LZ4-decodes every block of every segment in the window on every
/// request, so a month-wide Statistics band costs a month of decompression and, on a host whose
/// page cache cannot hold that month, a month of disk reads (2026-11-09: the 1m band on the 512 MB
/// production stand). With it, a segment wholly inside the window costs one small read, or nothing
/// once cached, and only the two boundary segments and the hot tier are still decoded.</para>
///
/// <para><b>Exactness.</b> A summary answers only a grid whose bucket is a whole number of slots
/// (every Statistics preset, 15 min to a day, is), and only for a segment wholly inside the
/// window, where every event counts. Each slot then falls into exactly one bucket: the scan puts an
/// event in <c>unixSeconds / bucketSeconds</c>, the summary puts its slot in
/// <c>(unixSeconds / 900) / (bucketSeconds / 900)</c>, and nested truncating division equals the
/// single one. The service bytes are the column's own, resolved by the aggregator's own UTF-8 road,
/// so case folding, the filter and "(unknown)" behave exactly as in the scan.</para>
///
/// <para><b>Where it lives.</b> New segments carry it in their own file (see
/// <see cref="SegmentWriter"/>: a section referenced from footer slot 1, which v7 readers have
/// always ignored), so it is written and deleted with the segment and needs no lifecycle of its
/// own. Segments written before it existed are summarised the first time a scan reads them whole
/// and kept in memory (<see cref="SegmentCountsCache"/>).</para>
/// </summary>
internal sealed class SegmentCountsSummary
{
    /// <summary>Slot width. Every bucket the summary answers is a whole number of these.</summary>
    public const int SlotSeconds = 900;

    // ── Section format v1 (little-endian) ─────────────────────────────────────────
    //   uint32 magic "RDCS"     uint16 version (1)     uint16 slotSeconds (900)
    //   uint32 eventCount       (= the segment header's EventCount)
    //   uint32 serviceCount     uint32 cellCount       uint32 payloadBytes    uint32 reserved (0)
    //   payload: { uint16 byteLen; bytes[byteLen] } × serviceCount   — the @svc column's own bytes
    //            { int32 slot; uint16 service; uint8 level; uint32 count } × cellCount
    //                                                    — sorted by (slot, service, level), unique
    //   uint32 crc32c over the header and the payload
    internal const uint   SectionMagic   = 0x52_44_43_53;   // "RDCS"
    internal const ushort SectionVersion = 1;
    internal const int    SectionHeaderBytes = 28;
    internal const int    CellBytes      = 11;

    // Mirrors SegmentReader/SegmentWriter: header magic and the fixed footer every version ends with.
    private const uint MagicHeader     = 0x52_44_4C_47;   // "RDLG"
    private const uint MagicFooter     = 0x52_44_46_54;   // "RDFT"
    private const int  FooterSize      = 44;
    private const int  FileHeaderSize  = 46;
    private const ushort FirstVersionWithSection = 7;

    internal readonly struct Cell(int slot, ushort service, byte level, uint count)
    {
        public readonly int    Slot    = slot;
        public readonly uint   Count   = count;
        public readonly ushort Service = service;
        public readonly byte   Level   = level;
    }

    private readonly byte[][] _services;
    private readonly Cell[]   _cells;

    internal SegmentCountsSummary(byte[][] services, Cell[] cells, uint eventCount)
    {
        _services  = services;
        _cells     = cells;
        EventCount = eventCount;
    }

    /// <summary>Events the summary accounts for; equal to the segment header's count.</summary>
    public uint EventCount { get; }

    /// <summary>Distinct <c>(slot, service, level)</c> cells — the summary's size, and its weight in the cache.</summary>
    public int CellCount => _cells.Length;

    internal ReadOnlySpan<Cell> Cells => _cells;
    internal IReadOnlyList<byte[]> Services => _services;

    /// <summary>True when a grid of <paramref name="bucketSeconds"/> can be answered from slots.</summary>
    public static bool Answers(int bucketSeconds) => bucketSeconds > 0 && bucketSeconds % SlotSeconds == 0;

    /// <summary>
    /// Adds every event of the segment to <paramref name="agg"/>, exactly as a header scan of the
    /// whole segment would. The caller has checked that the segment lies wholly inside the
    /// aggregator's window and that <see cref="Answers"/> holds for its grid.
    /// </summary>
    public void AddTo(LogVolumeAggregator agg, int bucketSeconds)
    {
        int slotsPerBucket = bucketSeconds / SlotSeconds;
        int n = _services.Length;
        int[] ids = ArrayPool<int>.Shared.Rent(Math.Max(n, 1));
        try
        {
            // Once per service per segment, not per cell: the same UTF-8 road the scan takes per event.
            for (int i = 0; i < n; i++) ids[i] = agg.ResolveSummaryService(_services[i]);
            foreach (ref readonly var c in _cells.AsSpan())
                agg.AddSummaryCount((long)c.Slot / slotsPerBucket, c.Level, ids[c.Service], c.Count);
        }
        finally { ArrayPool<int>.Shared.Return(ids); }
    }

    // ── Writing ───────────────────────────────────────────────────────────────────

    /// <summary>The section's size in bytes, CRC included.</summary>
    public int SectionBytes
    {
        get
        {
            long bytes = SectionHeaderBytes + 4 + (long)_cells.Length * CellBytes;
            foreach (var s in _services) bytes += 2 + s.Length;
            return checked((int)bytes);
        }
    }

    /// <summary>Writes the section at the stream's position.</summary>
    public void WriteSection(Stream stream)
    {
        int total = SectionBytes;
        byte[] buf = ArrayPool<byte>.Shared.Rent(total);
        try
        {
            var span = buf.AsSpan(0, total);
            int payload = total - SectionHeaderBytes - 4;
            BinaryPrimitives.WriteUInt32LittleEndian(span,        SectionMagic);
            BinaryPrimitives.WriteUInt16LittleEndian(span[4..],   SectionVersion);
            BinaryPrimitives.WriteUInt16LittleEndian(span[6..],   SlotSeconds);
            BinaryPrimitives.WriteUInt32LittleEndian(span[8..],   EventCount);
            BinaryPrimitives.WriteUInt32LittleEndian(span[12..],  (uint)_services.Length);
            BinaryPrimitives.WriteUInt32LittleEndian(span[16..],  (uint)_cells.Length);
            BinaryPrimitives.WriteUInt32LittleEndian(span[20..],  (uint)payload);
            BinaryPrimitives.WriteUInt32LittleEndian(span[24..],  0);

            int pos = SectionHeaderBytes;
            foreach (var s in _services)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(span[pos..], (ushort)s.Length); pos += 2;
                s.CopyTo(span[pos..]);                                                pos += s.Length;
            }
            foreach (ref readonly var c in _cells.AsSpan())
            {
                BinaryPrimitives.WriteInt32LittleEndian (span[pos..], c.Slot);    pos += 4;
                BinaryPrimitives.WriteUInt16LittleEndian(span[pos..], c.Service); pos += 2;
                span[pos] = c.Level;                                              pos += 1;
                BinaryPrimitives.WriteUInt32LittleEndian(span[pos..], c.Count);   pos += 4;
            }
            BinaryPrimitives.WriteUInt32LittleEndian(span[pos..], Crc32c.Append(0, span[..pos]));
            stream.Write(span);
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    // ── Reading ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads the summary a segment file carries, or null when it carries none — a file written
    /// before summaries existed, a summary the writer gave up on, or a section that does not
    /// check out against its CRC, its own counts or the segment's <paramref name="info"/>. Any
    /// failure to read the file at all throws; the caller then takes the scan, whose handling of a
    /// missing or damaged file is the established one.
    ///
    /// <para>Three small reads — header, footer, section — and no block index: a day-scale
    /// segment's index is hundreds of kilobytes that this never needs.</para>
    /// </summary>
    public static SegmentCountsSummary? Read(string filePath, SegmentInfo info)
    {
        FileRefusal.ThrowIfRefused(filePath, FileRefusal.Access.Open);
        using SafeFileHandle handle = File.OpenHandle(
            filePath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);

        long length = RandomAccess.GetLength(handle);
        if (length < FileHeaderSize + FooterSize) return null;

        Span<byte> head = stackalloc byte[6];
        if (!ReadExactly(handle, head, 0)) return null;
        if (BinaryPrimitives.ReadUInt32LittleEndian(head) != MagicHeader) return null;
        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(head[4..]);
        // v4-v6 spend footer slot 1 on the file-level trigram section; only v7 frees it.
        if (version != FirstVersionWithSection) return null;

        Span<byte> footer = stackalloc byte[FooterSize];
        if (!ReadExactly(handle, footer, length - FooterSize)) return null;
        if (BinaryPrimitives.ReadUInt32LittleEndian(footer[40..]) != MagicFooter) return null;
        long offset = BinaryPrimitives.ReadInt64LittleEndian(footer[8..]);
        if (offset == 0) return null;   // written before summaries, or the writer gave one up
        long sectionEnd = length - FooterSize;
        if (offset < FileHeaderSize || offset > sectionEnd - SectionHeaderBytes - 4) return null;

        Span<byte> sh = stackalloc byte[SectionHeaderBytes];
        if (!ReadExactly(handle, sh, offset)) return null;
        if (BinaryPrimitives.ReadUInt32LittleEndian(sh)       != SectionMagic)   return null;
        if (BinaryPrimitives.ReadUInt16LittleEndian(sh[4..])  != SectionVersion) return null;
        if (BinaryPrimitives.ReadUInt16LittleEndian(sh[6..])  != SlotSeconds)    return null;
        if (BinaryPrimitives.ReadUInt32LittleEndian(sh[8..])  != info.EventCount) return null;
        long payload = BinaryPrimitives.ReadUInt32LittleEndian(sh[20..]);
        if (payload > MaxPayloadBytes || offset + SectionHeaderBytes + payload + 4 > sectionEnd) return null;

        int total = (int)(SectionHeaderBytes + payload + 4);
        byte[] buf = ArrayPool<byte>.Shared.Rent(total);
        try
        {
            var span = buf.AsSpan(0, total);
            if (!ReadExactly(handle, span, offset)) return null;
            return Parse(span, info);
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    /// <summary>The largest payload the builder's limits allow; anything above is not a summary it wrote.</summary>
    internal const long MaxPayloadBytes =
        (long)SegmentCountsBuilder.MaxServices * (2 + SegmentCountsBuilder.MaxServiceBytes)
        + (long)SegmentCountsBuilder.MaxCells * CellBytes;

    private static bool ReadExactly(SafeFileHandle handle, Span<byte> into, long offset)
    {
        while (!into.IsEmpty)
        {
            int n = RandomAccess.Read(handle, into, offset);
            if (n <= 0) return false;
            into    = into[n..];
            offset += n;
        }
        return true;
    }

    /// <summary>
    /// Parses and checks a whole section — CRC, bounds, order, references and the total against
    /// the segment's own header — before anything is built, so a summary that comes back is one
    /// that can be added without a further check.
    /// </summary>
    internal static SegmentCountsSummary? Parse(ReadOnlySpan<byte> section, SegmentInfo info)
    {
        if (section.Length < SectionHeaderBytes + 4) return null;
        int crcAt = section.Length - 4;
        if (Crc32c.Append(0, section[..crcAt]) != BinaryPrimitives.ReadUInt32LittleEndian(section[crcAt..])) return null;

        if (BinaryPrimitives.ReadUInt32LittleEndian(section)      != SectionMagic)   return null;
        if (BinaryPrimitives.ReadUInt16LittleEndian(section[4..]) != SectionVersion) return null;
        if (BinaryPrimitives.ReadUInt16LittleEndian(section[6..]) != SlotSeconds)    return null;
        uint eventCount   = BinaryPrimitives.ReadUInt32LittleEndian(section[8..]);
        uint serviceCount = BinaryPrimitives.ReadUInt32LittleEndian(section[12..]);
        uint cellCount    = BinaryPrimitives.ReadUInt32LittleEndian(section[16..]);
        uint payloadBytes = BinaryPrimitives.ReadUInt32LittleEndian(section[20..]);
        if (eventCount != info.EventCount) return null;
        if (payloadBytes != (uint)(crcAt - SectionHeaderBytes)) return null;
        if (serviceCount > SegmentCountsBuilder.MaxServices || cellCount > SegmentCountsBuilder.MaxCells) return null;

        var payload = section[SectionHeaderBytes..crcAt];
        int pos = 0;
        var services = new byte[serviceCount][];
        for (int i = 0; i < services.Length; i++)
        {
            if (payload.Length - pos < 2) return null;
            int len = BinaryPrimitives.ReadUInt16LittleEndian(payload[pos..]); pos += 2;
            if (len > SegmentCountsBuilder.MaxServiceBytes || payload.Length - pos < len) return null;
            services[i] = payload.Slice(pos, len).ToArray();                    pos += len;
        }
        if ((long)(payload.Length - pos) != (long)cellCount * CellBytes) return null;

        var  cells = new Cell[cellCount];
        long sum   = 0;
        long prev  = long.MinValue;
        for (int i = 0; i < cells.Length; i++)
        {
            int    slot    = BinaryPrimitives.ReadInt32LittleEndian (payload[pos..]); pos += 4;
            ushort service = BinaryPrimitives.ReadUInt16LittleEndian(payload[pos..]); pos += 2;
            byte   level   = payload[pos];                                           pos += 1;
            uint   count   = BinaryPrimitives.ReadUInt32LittleEndian(payload[pos..]); pos += 4;
            if (service >= serviceCount || count == 0) return null;
            long key = SortKey(slot, service, level);
            if (key <= prev) return null;   // sorted and unique, as the writer leaves them
            prev = key;
            sum += count;
            cells[i] = new Cell(slot, service, level, count);
        }
        return sum == eventCount ? new SegmentCountsSummary(services, cells, eventCount) : null;
    }

    /// <summary>One number that orders cells by (slot, service, level).</summary>
    internal static long SortKey(int slot, ushort service, byte level) =>
        ((long)slot << 24) | ((long)service << 8) | level;
}

/// <summary>
/// Accumulates a <see cref="SegmentCountsSummary"/> one event at a time: from the writer as it
/// stages a segment's rows, and from a header scan that reads an older segment whole.
///
/// <para>It runs on the flush's and the merge's write loop, so the per-row cost is kept to a
/// range check, a reference check and an array increment: rows arrive in time order, so the
/// open slot's tick range answers "same slot" without dividing; the writer hands in the interned
/// service name, so a small reference-keyed table answers "which service" without hashing bytes;
/// and the open slot's cells are a dense counter array for the first
/// <see cref="DenseServices"/> services and the eight level values a level byte names in
/// practice, with a dictionary behind it for the rest. Nothing is allocated per row.</para>
///
/// <para>Memory is bounded by <see cref="MaxServices"/> and <see cref="MaxCells"/>; past either,
/// the builder gives up — it drops what it holds and every later <see cref="Add"/> is a no-op —
/// and the segment simply has no summary, which costs a scan, never a wrong count.</para>
///
/// <para>A row from an earlier slot than the open one is still counted: its slot is reopened and
/// <see cref="Build"/> sorts and merges the cells when that happened.</para>
/// </summary>
internal sealed class SegmentCountsBuilder
{
    /// <summary>Distinct service spellings one summary may name.</summary>
    internal const int MaxServices = 4_096;

    /// <summary>
    /// The longest service name, in UTF-8 bytes, a summary keeps. Service names are short; a longer
    /// one costs the segment its summary rather than letting one name size the section.
    /// </summary>
    internal const int MaxServiceBytes = 1_024;

    /// <summary>
    /// Cells one summary may hold. A day-scale segment spans 96 slots, so ~40 service/level pairs
    /// make ~4 000 cells; 32 768 leaves an order of magnitude for richer vocabularies and caps the
    /// cells of one summary at 360 KB on disk and 384 KB in memory.
    /// </summary>
    internal const int MaxCells = 32_768;

    /// <summary>Services whose open-slot counters are a dense array (8 KB with <see cref="DenseLevels"/>).</summary>
    private const int DenseServices = 256;
    private const int DenseLevels   = 8;

    private const long UnixEpochTicks = 621_355_968_000_000_000L;   // as LogVolumeAggregator
    private const long TicksPerSecond = TimeSpan.TicksPerSecond;
    private const long SlotTicks      = SegmentCountsSummary.SlotSeconds * TicksPerSecond;

    /// <summary>
    /// The highest slot whose tick range is kept for the no-division check; its end must not
    /// overflow a long. No DateTime reaches it (year 9999 is slot ~2.8e8).
    /// </summary>
    private const long MaxRangedSlot = 900_000_000;

    // Services: by bytes for the scan road, and by the writer's interned name through a small
    // direct-mapped table keyed on the reference.
    private readonly Dictionary<byte[], int> _svcIds = new(ByteSpanComparer.Instance);
    private readonly Dictionary<byte[], int>.AlternateLookup<ReadOnlySpan<byte>> _svcByBytes;
    private readonly List<byte[]> _svcNames = new();
    private readonly object?[] _refKeys = new object?[64];
    private readonly int[]     _refIds  = new int[64];

    // The open slot: its tick range (empty when it has none: at and before 1970 the aggregator's
    // truncating division is not a floor, so those slots always divide), and its cells.
    private long _slot      = long.MinValue;
    private long _slotStart = long.MaxValue;
    private long _slotEnd   = long.MinValue;
    private readonly uint[]   _dense        = new uint[DenseServices * DenseLevels];
    private readonly ushort[] _denseTouched = new ushort[DenseServices * DenseLevels];
    private int _denseTouchedCount;
    private readonly Dictionary<int, uint> _sparse = new();   // (service << 8 | level) beyond the dense block

    private int  _lastClosedSlot = int.MinValue;
    private bool _unsorted;

    private SegmentCountsSummary.Cell[] _cells = [];
    private int  _cellCount;

    private long _events;
    private long _minTs = long.MaxValue;
    private long _maxTs = long.MinValue;
    private bool _abandoned;

    public SegmentCountsBuilder() => _svcByBytes = _svcIds.GetAlternateLookup<ReadOnlySpan<byte>>();

    /// <summary>True once the builder has given up; <see cref="Build"/> then returns null.</summary>
    public bool Abandoned => _abandoned;

    /// <summary>Counts one event.</summary>
    /// <param name="serviceUtf8">The @svc column's bytes for the event; empty for none.</param>
    /// <param name="serviceIdentity">
    /// Optional: an object that is the same reference for every event of the same service (the
    /// writer passes the interned name), which turns a repeat into a reference check.
    /// </param>
    public void Add(long timestampTicks, byte level, ReadOnlySpan<byte> serviceUtf8, object? serviceIdentity = null)
    {
        if (_abandoned) return;
        _events++;
        if (timestampTicks < _minTs) _minTs = timestampTicks;
        if (timestampTicks > _maxTs) _maxTs = timestampTicks;

        if (timestampTicks < _slotStart || timestampTicks >= _slotEnd)
        {
            // The aggregator's own arithmetic (LogVolumeAggregator.BucketOffset), then one more
            // truncating division: see SegmentCountsSummary on why that nests exactly.
            long unixSeconds = (timestampTicks - UnixEpochTicks) / TicksPerSecond;
            long slot        = unixSeconds / SegmentCountsSummary.SlotSeconds;
            if (slot != _slot && !OpenSlot(slot)) return;
        }

        int svc;
        if (serviceIdentity is not null)
        {
            int h = RuntimeHelpers.GetHashCode(serviceIdentity) & (_refKeys.Length - 1);
            if (ReferenceEquals(_refKeys[h], serviceIdentity))
                svc = _refIds[h];
            else
            {
                svc = ResolveService(serviceUtf8);
                if (svc < 0) return;
                _refKeys[h] = serviceIdentity;
                _refIds[h]  = svc;
            }
        }
        else
        {
            svc = ResolveService(serviceUtf8);
            if (svc < 0) return;
        }

        bool added;
        if (svc < DenseServices && level < DenseLevels)
        {
            int i = (svc << 3) | level;
            added = _dense[i]++ == 0;
            if (added) _denseTouched[_denseTouchedCount++] = (ushort)i;
        }
        else
        {
            ref uint count = ref CollectionsMarshal.GetValueRefOrAddDefault(_sparse, (svc << 8) | level, out bool existed);
            count++;
            added = !existed;
        }
        if (added && _cellCount + _denseTouchedCount + _sparse.Count > MaxCells) Abandon();
    }

    /// <summary>
    /// The summary, or null when the builder gave up or what it saw does not match the segment it
    /// is for: a different event count, or an event outside the header's time bounds (a summary is
    /// only ever used for a segment those bounds place wholly inside a window).
    /// </summary>
    public SegmentCountsSummary? Build(uint headerEventCount, long headerMinTicks, long headerMaxTicks)
    {
        if (_abandoned) return null;
        CloseSlot();
        if (_abandoned) return null;
        if (_events != headerEventCount) return null;
        if (_events > 0 && (_minTs < headerMinTicks || _maxTs > headerMaxTicks)) return null;

        var cells = _cells.AsSpan(0, _cellCount);
        if (_unsorted)
        {
            cells.Sort(static (a, b) => SegmentCountsSummary.SortKey(a.Slot, a.Service, a.Level)
                                 .CompareTo(SegmentCountsSummary.SortKey(b.Slot, b.Service, b.Level)));
            int w = 0;
            for (int r = 0; r < cells.Length; r++)
            {
                if (w > 0 && cells[w - 1].Slot == cells[r].Slot && cells[w - 1].Service == cells[r].Service
                          && cells[w - 1].Level == cells[r].Level)
                    cells[w - 1] = new SegmentCountsSummary.Cell(cells[r].Slot, cells[r].Service, cells[r].Level,
                                                                 cells[w - 1].Count + cells[r].Count);
                else
                    cells[w++] = cells[r];
            }
            cells = cells[..w];
        }
        return new SegmentCountsSummary(_svcNames.ToArray(), cells.ToArray(), headerEventCount);
    }

    private bool OpenSlot(long slot)
    {
        CloseSlot();
        if (_abandoned) return false;
        if (slot < int.MinValue || slot > int.MaxValue) { Abandon(); return false; }
        _slot = slot;
        if (slot >= 1 && slot <= MaxRangedSlot)
        {
            _slotStart = UnixEpochTicks + slot * SlotTicks;
            _slotEnd   = _slotStart + SlotTicks;
        }
        else
        {
            _slotStart = long.MaxValue;
            _slotEnd   = long.MinValue;
        }
        return true;
    }

    /// <summary>Moves the open slot's cells, in (service, level) order, onto the list.</summary>
    private void CloseSlot()
    {
        int n = _denseTouchedCount + _sparse.Count;
        if (n == 0) return;
        int slot = (int)_slot;
        if (_cellCount > 0 && slot <= _lastClosedSlot) _unsorted = true;   // an earlier slot, or one reopened
        _lastClosedSlot = slot;

        if (_cells.Length < _cellCount + n)
            Array.Resize(ref _cells, Math.Max(_cellCount + n, Math.Max(64, _cells.Length * 2)));
        int start = _cellCount;
        for (int t = 0; t < _denseTouchedCount; t++)
        {
            int i = _denseTouched[t];
            _cells[_cellCount++] = new SegmentCountsSummary.Cell(slot, (ushort)(i >> 3), (byte)(i & 7), _dense[i]);
            _dense[i] = 0;
        }
        _denseTouchedCount = 0;
        foreach (var (key, count) in _sparse)
            _cells[_cellCount++] = new SegmentCountsSummary.Cell(slot, (ushort)(key >> 8), (byte)key, count);
        _sparse.Clear();
        _cells.AsSpan(start, n).Sort(static (a, b) =>
            ((a.Service << 8) | a.Level).CompareTo((b.Service << 8) | b.Level));
    }

    private int ResolveService(ReadOnlySpan<byte> utf8)
    {
        if (_svcByBytes.TryGetValue(utf8, out int id)) return id;
        if (_svcNames.Count >= MaxServices || utf8.Length > MaxServiceBytes) { Abandon(); return -1; }
        id = _svcNames.Count;
        byte[] key = utf8.ToArray();
        _svcIds.Add(key, id);
        _svcNames.Add(key);
        return id;
    }

    private void Abandon()
    {
        _abandoned = true;
        Array.Clear(_dense);
        _denseTouchedCount = 0;
        _sparse.Clear();
        _svcIds.Clear();
        _svcNames.Clear();
        Array.Clear(_refKeys);
        _cells     = [];
        _cellCount = 0;
    }
}

/// <summary>Byte-for-byte equality over <c>byte[]</c> keys, probe-able with a span.</summary>
internal sealed class ByteSpanComparer : IEqualityComparer<byte[]>, IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
{
    public static readonly ByteSpanComparer Instance = new();
    private ByteSpanComparer() { }

    public bool Equals(byte[]? x, byte[]? y) =>
        ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));
    public int GetHashCode(byte[] obj) => Hash(obj);
    public bool Equals(ReadOnlySpan<byte> alternate, byte[] other) => alternate.SequenceEqual(other);
    public int GetHashCode(ReadOnlySpan<byte> alternate) => Hash(alternate);
    public byte[] Create(ReadOnlySpan<byte> alternate) => alternate.ToArray();

    private static int Hash(ReadOnlySpan<byte> bytes)
    {
        var h = new HashCode();
        h.AddBytes(bytes);
        return h.ToHashCode();
    }
}

/// <summary>
/// Summaries already read or built, by segment, so a segment is summarised once per process and
/// a repeated wide window touches no file at all.
///
/// <para>Bounded by cells (<see cref="DefaultMaxCells"/>, ~6 MB of them): an entry whose segment
/// has left the catalog — merged, deleted, replaced — is dropped the next time the budget is
/// reached, and when the live ones alone fill it nothing more is kept. A segment that is not
/// kept is read or scanned again next time, which is what every segment cost before.</para>
/// </summary>
internal sealed class SegmentCountsCache
{
    internal const long DefaultMaxCells = 524_288;

    private sealed class Item(SegmentInfo info, SegmentCountsSummary summary)
    {
        public readonly SegmentInfo          Info    = info;
        public readonly SegmentCountsSummary Summary = summary;
        public long Weight => Math.Max(1, Summary.CellCount);
    }

    private readonly ConcurrentDictionary<SegmentKey, Item> _map = new();
    private readonly Lock _pruneLock = new();
    private readonly long _maxCells;
    private long _cells;
    private long _lastPruneTicks = -PruneIntervalMs;   // the first prune is never throttled

    /// <summary>
    /// How often a full budget may walk the cache for stale entries. When the live segments alone
    /// fill it, every further add would walk it for nothing; once a second is enough to notice the
    /// merges and deletes that free room.
    /// </summary>
    private const long PruneIntervalMs = 1_000;

    public SegmentCountsCache(long maxCells = DefaultMaxCells) => _maxCells = maxCells;

    /// <summary>Cells held now; for tests.</summary>
    internal long CellsForTest => Interlocked.Read(ref _cells);
    internal int  CountForTest => _map.Count;

    public bool TryGet(SegmentInfo info, [NotNullWhen(true)] out SegmentCountsSummary? summary)
    {
        if (_map.TryGetValue(SegmentKey.Of(info), out var item) && SameFile(item.Info, info))
        {
            summary = item.Summary;
            return true;
        }
        summary = null;
        return false;
    }

    /// <param name="isLive">Whether a cached segment is still the one the catalog serves.</param>
    public void Add(SegmentInfo info, SegmentCountsSummary summary, Func<SegmentInfo, bool> isLive)
    {
        var item = new Item(info, summary);
        long weight = item.Weight;
        if (Interlocked.Add(ref _cells, weight) > _maxCells)
        {
            Interlocked.Add(ref _cells, -weight);
            Prune(isLive);
            if (Interlocked.Add(ref _cells, weight) > _maxCells)
            {
                Interlocked.Add(ref _cells, -weight);
                return;
            }
        }

        var key = SegmentKey.Of(info);
        while (true)
        {
            if (_map.TryGetValue(key, out var old))
            {
                if (_map.TryUpdate(key, item, old)) { Interlocked.Add(ref _cells, -old.Weight); return; }
            }
            else if (_map.TryAdd(key, item)) return;
        }
    }

    private void Prune(Func<SegmentInfo, bool> isLive)
    {
        lock (_pruneLock)
        {
            long now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastPruneTicks) < PruneIntervalMs) return;
            Interlocked.Exchange(ref _lastPruneTicks, now);
            foreach (var kv in _map)
                if (!isLive(kv.Value.Info) && _map.TryRemove(kv))
                    Interlocked.Add(ref _cells, -kv.Value.Weight);
        }
    }

    /// <summary>The same file: the object the catalog holds, or one read off the same file again.</summary>
    internal static bool SameFile(SegmentInfo a, SegmentInfo b) =>
        ReferenceEquals(a, b)
        || (a.EventCount        == b.EventCount
            && a.MinTimestampTicks == b.MinTimestampTicks
            && a.MaxTimestampTicks == b.MaxTimestampTicks
            && a.CompressedBytes   == b.CompressedBytes
            && string.Equals(a.FilePath, b.FilePath, StringComparison.Ordinal));
}
