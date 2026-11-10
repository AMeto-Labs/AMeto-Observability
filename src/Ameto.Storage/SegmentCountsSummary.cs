using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// How many events one cold segment holds per <c>(slot, service, level)</c> — everything
/// <c>GET /api/events/counts</c> needs from a segment that lies wholly inside its window.
///
/// <para>Without it the endpoint LZ4-decodes every block of every segment in the window on every
/// request, so a month-wide Statistics band costs a month of decompression and, on a host whose
/// page cache cannot hold that month, a month of disk reads (2026-11-09: the 1m band on the 512 MB
/// production stand). With it, a segment wholly inside the window costs a small read, and only the
/// two boundary segments and the hot tier are still decoded.</para>
///
/// <para><b>Resolutions.</b> A summary is built at <see cref="SlotSeconds"/> (15 minutes) and can be
/// coarsened to an hour or a day (<see cref="Coarsen"/>): a month-wide window asks for 12-hour
/// buckets and a quarter for days, so the coarse summaries hold 4 and 96 times fewer cells for the
/// same answer. <see cref="ResolutionSeconds"/> says which one an instance is.</para>
///
/// <para><b>Exactness.</b> A summary answers only a grid whose bucket is a whole number of its
/// slots, and only for a segment wholly inside the window, where every event counts. Each slot then
/// falls into exactly one bucket: the scan puts an event in <c>unixSeconds / bucketSeconds</c>, the
/// summary puts its slot in <c>(unixSeconds / resolution) / (bucketSeconds / resolution)</c>, and
/// nested truncating division equals the single one, whatever the sign; coarsening is the same
/// nesting once more. The service bytes are the column's own, resolved by the aggregator's own UTF-8
/// road, so case folding, the filter and "(unknown)" behave exactly as in the scan.</para>
///
/// <para><b>Where it lives.</b> New segments carry the 15-minute summary in their own file (see
/// <see cref="SegmentWriter"/>: a section referenced from footer slot 1, which v7 readers have
/// always ignored). Every segment, old or new, also has a row in <see cref="SegmentCountsStore"/>
/// (<c>counts.db</c>) with all three resolutions: segments written before summaries get theirs from
/// one header scan, by the background backfill or by the first counting request that has to scan
/// them, and keep it across restarts.</para>
/// </summary>
internal sealed class SegmentCountsSummary
{
    /// <summary>The finest slot, the one every summary is built at. Every bucket a summary answers is a whole number of these.</summary>
    public const int SlotSeconds = 900;

    /// <summary>The coarser resolutions the store keeps beside the 15-minute one.</summary>
    public const int HourSeconds = 3_600, DaySeconds = 86_400;

    // ── Section format (little-endian) ────────────────────────────────────────────
    //   uint32 magic "RDCS"     uint16 version          uint16 v1: slotSeconds (900); v2: 0
    //   uint32 eventCount       (= the segment header's EventCount)
    //   uint32 serviceCount     uint32 cellCount       uint32 payloadBytes
    //   uint32 v1: reserved (0); v2: resolutionSeconds (900, 3 600 or 86 400)
    //   payload: { uint16 byteLen; bytes[byteLen] } × serviceCount   — the @svc column's own bytes
    //            { int32 slot; uint16 service; uint8 level; uint32 count } × cellCount
    //                                                    — sorted by (slot, service, level), unique
    //   uint32 crc32c over the header and the payload
    //
    // v1 is what segment files carry (15 minutes only, the field a 16-bit one); v2 moves the
    // resolution into the reserved 32-bit field so a day fits, and is what the store keeps.
    internal const uint   SectionMagic       = 0x52_44_43_53;   // "RDCS"
    internal const ushort FileVersion        = 1;
    internal const ushort StoreVersion       = 2;
    internal const int    SectionHeaderBytes = 28;
    internal const int    CellBytes          = 11;

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

    internal SegmentCountsSummary(byte[][] services, Cell[] cells, uint eventCount, int resolutionSeconds = SlotSeconds)
    {
        _services         = services;
        _cells            = cells;
        EventCount        = eventCount;
        ResolutionSeconds = resolutionSeconds;
    }

    /// <summary>Events the summary accounts for; equal to the segment header's count.</summary>
    public uint EventCount { get; }

    /// <summary>The width of this summary's slots: 900, 3 600 or 86 400 seconds.</summary>
    public int ResolutionSeconds { get; }

    /// <summary>Distinct <c>(slot, service, level)</c> cells.</summary>
    public int CellCount => _cells.Length;

    internal ReadOnlySpan<Cell> Cells => _cells;
    internal IReadOnlyList<byte[]> Services => _services;

    /// <summary>True when a grid of <paramref name="bucketSeconds"/> can be answered from 15-minute slots at all.</summary>
    public static bool Answers(int bucketSeconds) => bucketSeconds > 0 && bucketSeconds % SlotSeconds == 0;

    /// <summary>The coarsest stored resolution a grid of <paramref name="bucketSeconds"/> can be answered from.</summary>
    public static int ResolutionFor(int bucketSeconds) =>
        bucketSeconds % DaySeconds == 0 ? DaySeconds
        : bucketSeconds % HourSeconds == 0 ? HourSeconds
        : SlotSeconds;

    private static bool IsResolution(uint seconds) =>
        seconds is SlotSeconds or HourSeconds or DaySeconds;

    /// <summary>
    /// Adds every event of the segment to <paramref name="agg"/>, exactly as a header scan of the
    /// whole segment would. The caller has checked that the segment lies wholly inside the
    /// aggregator's window and that <paramref name="bucketSeconds"/> is a whole number of this
    /// summary's slots.
    /// </summary>
    public void AddTo(LogVolumeAggregator agg, int bucketSeconds)
    {
        int slotsPerBucket = bucketSeconds / ResolutionSeconds;
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

    /// <summary>
    /// The same counts at a coarser resolution: every slot divided down (the same truncating
    /// nesting), equal cells merged, services as they are.
    /// </summary>
    public SegmentCountsSummary Coarsen(int resolutionSeconds)
    {
        if (resolutionSeconds == ResolutionSeconds) return this;
        if (resolutionSeconds < ResolutionSeconds || resolutionSeconds % ResolutionSeconds != 0)
            throw new ArgumentOutOfRangeException(nameof(resolutionSeconds));
        int factor = resolutionSeconds / ResolutionSeconds;

        var cells = new Cell[_cells.Length];
        for (int i = 0; i < cells.Length; i++)
        {
            ref readonly var c = ref _cells[i];
            cells[i] = new Cell(c.Slot / factor, c.Service, c.Level, c.Count);
        }
        cells.AsSpan().Sort(static (a, b) => SortKey(a.Slot, a.Service, a.Level).CompareTo(SortKey(b.Slot, b.Service, b.Level)));
        int w = 0;
        for (int r = 0; r < cells.Length; r++)
        {
            if (w > 0 && cells[w - 1].Slot == cells[r].Slot && cells[w - 1].Service == cells[r].Service
                      && cells[w - 1].Level == cells[r].Level)
                cells[w - 1] = new Cell(cells[r].Slot, cells[r].Service, cells[r].Level, cells[w - 1].Count + cells[r].Count);
            else
                cells[w++] = cells[r];
        }
        return new SegmentCountsSummary(_services, cells.AsSpan(0, w).ToArray(), EventCount, resolutionSeconds);
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

    /// <summary>Writes the file section (v1) at the stream's position. Only a 15-minute summary goes in a file.</summary>
    public void WriteSection(Stream stream)
    {
        if (ResolutionSeconds != SlotSeconds) throw new InvalidOperationException("A segment file carries the 15-minute summary only.");
        int total = SectionBytes;
        byte[] buf = ArrayPool<byte>.Shared.Rent(total);
        try
        {
            Write(buf.AsSpan(0, total), FileVersion);
            stream.Write(buf, 0, total);
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    /// <summary>The store's form of the summary (v2, any resolution).</summary>
    public byte[] ToStoreBytes()
    {
        var bytes = new byte[SectionBytes];
        Write(bytes, StoreVersion);
        return bytes;
    }

    private void Write(Span<byte> span, ushort version)
    {
        int payload = span.Length - SectionHeaderBytes - 4;
        BinaryPrimitives.WriteUInt32LittleEndian(span,        SectionMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(span[4..],   version);
        BinaryPrimitives.WriteUInt16LittleEndian(span[6..],   version == FileVersion ? (ushort)SlotSeconds : (ushort)0);
        BinaryPrimitives.WriteUInt32LittleEndian(span[8..],   EventCount);
        BinaryPrimitives.WriteUInt32LittleEndian(span[12..],  (uint)_services.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(span[16..],  (uint)_cells.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(span[20..],  (uint)payload);
        BinaryPrimitives.WriteUInt32LittleEndian(span[24..],  version == FileVersion ? 0u : (uint)ResolutionSeconds);

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
    }

    // ── Reading a segment file's section ──────────────────────────────────────────

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
        byte[]? rented = ReadFileSection(filePath, info, out int length);
        if (rented is null) return null;
        try { return Parse(rented.AsSpan(0, length), info); }
        finally { ArrayPool<byte>.Shared.Return(rented); }
    }

    /// <summary>
    /// <see cref="Read"/> and <see cref="AddTo"/> in one, without building the summary: the section
    /// is checked whole and then added straight from its bytes. False when the file carries none.
    /// </summary>
    public static bool TryAddFromFile(string filePath, SegmentInfo info, LogVolumeAggregator agg, int bucketSeconds)
    {
        byte[]? rented = ReadFileSection(filePath, info, out int length);
        if (rented is null) return false;
        try { return TryAddFrom(rented.AsSpan(0, length), info, agg, bucketSeconds); }
        finally { ArrayPool<byte>.Shared.Return(rented); }
    }

    /// <summary>The file's v1 section in a buffer rented from the shared pool (the caller returns it), or null.</summary>
    private static byte[]? ReadFileSection(string filePath, SegmentInfo info, out int length)
    {
        length = 0;
        FileRefusal.ThrowIfRefused(filePath, FileRefusal.Access.Open);
        using SafeFileHandle handle = File.OpenHandle(
            filePath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);

        long fileLength = RandomAccess.GetLength(handle);
        if (fileLength < FileHeaderSize + FooterSize) return null;

        Span<byte> head = stackalloc byte[6];
        if (!ReadExactly(handle, head, 0)) return null;
        if (BinaryPrimitives.ReadUInt32LittleEndian(head) != MagicHeader) return null;
        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(head[4..]);
        // v4-v6 spend footer slot 1 on the file-level trigram section; only v7 frees it.
        if (version != FirstVersionWithSection) return null;

        Span<byte> footer = stackalloc byte[FooterSize];
        if (!ReadExactly(handle, footer, fileLength - FooterSize)) return null;
        if (BinaryPrimitives.ReadUInt32LittleEndian(footer[40..]) != MagicFooter) return null;
        long offset = BinaryPrimitives.ReadInt64LittleEndian(footer[8..]);
        if (offset == 0) return null;   // written before summaries, or the writer gave one up
        long sectionEnd = fileLength - FooterSize;
        if (offset < FileHeaderSize || offset > sectionEnd - SectionHeaderBytes - 4) return null;

        Span<byte> sh = stackalloc byte[SectionHeaderBytes];
        if (!ReadExactly(handle, sh, offset)) return null;
        if (BinaryPrimitives.ReadUInt32LittleEndian(sh)      != SectionMagic) return null;
        if (BinaryPrimitives.ReadUInt16LittleEndian(sh[4..]) != FileVersion)  return null;
        if (BinaryPrimitives.ReadUInt32LittleEndian(sh[8..]) != info.EventCount) return null;
        long payload = BinaryPrimitives.ReadUInt32LittleEndian(sh[20..]);
        if (payload > MaxPayloadBytes || offset + SectionHeaderBytes + payload + 4 > sectionEnd) return null;

        int total = (int)(SectionHeaderBytes + payload + 4);
        byte[] buf = ArrayPool<byte>.Shared.Rent(total);
        if (!ReadExactly(handle, buf.AsSpan(0, total), offset))
        {
            ArrayPool<byte>.Shared.Return(buf);
            return null;
        }
        length = total;
        return buf;
    }

    /// <summary>The largest payload the builder's limits allow; anything above is not a summary it wrote.</summary>
    internal const long MaxPayloadBytes =
        (long)SegmentCountsBuilder.MaxServices * 2 + SegmentCountsBuilder.MaxServiceBytesTotal
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

    // ── Checking and using a section's bytes ──────────────────────────────────────

    /// <summary>Where a checked section's parts lie.</summary>
    private readonly record struct Layout(int Resolution, uint EventCount, int ServiceCount, int CellCount,
                                          int ServicesAt, int CellsAt);

    /// <summary>
    /// Checks a whole section — CRC, version, bounds, order, references, slots within the
    /// segment's own first and last event, and the total against its header — without allocating,
    /// so a section that passes can be used without a further check.
    /// </summary>
    private static bool TryCheck(ReadOnlySpan<byte> section, SegmentInfo info, out Layout layout)
    {
        layout = default;
        if (section.Length < SectionHeaderBytes + 4) return false;
        int crcAt = section.Length - 4;
        if (Crc32c.Append(0, section[..crcAt]) != BinaryPrimitives.ReadUInt32LittleEndian(section[crcAt..])) return false;

        if (BinaryPrimitives.ReadUInt32LittleEndian(section) != SectionMagic) return false;
        ushort version  = BinaryPrimitives.ReadUInt16LittleEndian(section[4..]);
        ushort field6   = BinaryPrimitives.ReadUInt16LittleEndian(section[6..]);
        uint   field24  = BinaryPrimitives.ReadUInt32LittleEndian(section[24..]);
        uint resolution;
        if (version == FileVersion)       { if (field6 != SlotSeconds || field24 != 0) return false; resolution = SlotSeconds; }
        else if (version == StoreVersion) { if (field6 != 0 || !IsResolution(field24)) return false; resolution = field24; }
        else return false;

        uint eventCount   = BinaryPrimitives.ReadUInt32LittleEndian(section[8..]);
        uint serviceCount = BinaryPrimitives.ReadUInt32LittleEndian(section[12..]);
        uint cellCount    = BinaryPrimitives.ReadUInt32LittleEndian(section[16..]);
        uint payloadBytes = BinaryPrimitives.ReadUInt32LittleEndian(section[20..]);
        if (eventCount != info.EventCount) return false;
        if (payloadBytes != (uint)(crcAt - SectionHeaderBytes)) return false;
        if (serviceCount > SegmentCountsBuilder.MaxServices || cellCount > SegmentCountsBuilder.MaxCells) return false;

        var payload = section[SectionHeaderBytes..crcAt];
        int pos = 0;
        long serviceBytes = 0;
        for (uint i = 0; i < serviceCount; i++)
        {
            if (payload.Length - pos < 2) return false;
            int len = BinaryPrimitives.ReadUInt16LittleEndian(payload[pos..]); pos += 2;
            if (len > SegmentCountsBuilder.MaxServiceBytes || payload.Length - pos < len) return false;
            serviceBytes += len;
            if (serviceBytes > SegmentCountsBuilder.MaxServiceBytesTotal) return false;
            pos += len;
        }
        int cellsAt = pos;
        if ((long)(payload.Length - pos) != (long)cellCount * CellBytes) return false;

        // Every cell's slot lies within the slots of the segment's own first and last event: the
        // writer counted those very rows. Only bytes some other writer produced, with a CRC that
        // still checks out, could say otherwise, and they would put events in buckets the segment
        // never reaches.
        long minSlot = SlotOf(info.MinTimestampTicks, (int)resolution);
        long maxSlot = SlotOf(info.MaxTimestampTicks, (int)resolution);
        long sum  = 0;
        long prev = long.MinValue;
        for (uint i = 0; i < cellCount; i++)
        {
            int    slot    = BinaryPrimitives.ReadInt32LittleEndian (payload[pos..]);
            ushort service = BinaryPrimitives.ReadUInt16LittleEndian(payload[(pos + 4)..]);
            byte   level   = payload[pos + 6];
            uint   count   = BinaryPrimitives.ReadUInt32LittleEndian(payload[(pos + 7)..]);
            pos += CellBytes;
            if (service >= serviceCount || count == 0) return false;
            if (slot < minSlot || slot > maxSlot) return false;
            long key = SortKey(slot, service, level);
            if (key <= prev) return false;   // sorted and unique, as the writer leaves them
            prev = key;
            sum += count;
        }
        if (sum != eventCount) return false;

        layout = new Layout((int)resolution, eventCount, (int)serviceCount, (int)cellCount,
                            SectionHeaderBytes, SectionHeaderBytes + cellsAt);
        return true;
    }

    /// <summary>Checks a section and builds the summary it holds, or returns null.</summary>
    internal static SegmentCountsSummary? Parse(ReadOnlySpan<byte> section, SegmentInfo info)
    {
        if (!TryCheck(section, info, out var l)) return null;
        var services = new byte[l.ServiceCount][];
        int pos = l.ServicesAt;
        for (int i = 0; i < services.Length; i++)
        {
            int len = BinaryPrimitives.ReadUInt16LittleEndian(section[pos..]); pos += 2;
            services[i] = section.Slice(pos, len).ToArray();                    pos += len;
        }
        var cells = new Cell[l.CellCount];
        pos = l.CellsAt;
        for (int i = 0; i < cells.Length; i++, pos += CellBytes)
            cells[i] = new Cell(BinaryPrimitives.ReadInt32LittleEndian(section[pos..]),
                                BinaryPrimitives.ReadUInt16LittleEndian(section[(pos + 4)..]),
                                section[pos + 6],
                                BinaryPrimitives.ReadUInt32LittleEndian(section[(pos + 7)..]));
        return new SegmentCountsSummary(services, cells, l.EventCount, l.Resolution);
    }

    /// <summary>
    /// Checks a section and adds it to <paramref name="agg"/> straight from its bytes — no summary
    /// is built, nothing is allocated but what the aggregator allocates for a service it has not
    /// seen. False, having added nothing, when the section does not check out or the grid is not
    /// a whole number of its slots.
    /// </summary>
    internal static bool TryAddFrom(ReadOnlySpan<byte> section, SegmentInfo info, LogVolumeAggregator agg, int bucketSeconds)
    {
        if (!TryCheck(section, info, out var l)) return false;
        if (bucketSeconds <= 0 || bucketSeconds % l.Resolution != 0) return false;
        int slotsPerBucket = bucketSeconds / l.Resolution;

        int[] ids = ArrayPool<int>.Shared.Rent(Math.Max(l.ServiceCount, 1));
        try
        {
            int pos = l.ServicesAt;
            for (int i = 0; i < l.ServiceCount; i++)
            {
                int len = BinaryPrimitives.ReadUInt16LittleEndian(section[pos..]); pos += 2;
                ids[i] = agg.ResolveSummaryService(section.Slice(pos, len));        pos += len;
            }
            pos = l.CellsAt;
            for (int i = 0; i < l.CellCount; i++, pos += CellBytes)
            {
                int    slot    = BinaryPrimitives.ReadInt32LittleEndian (section[pos..]);
                ushort service = BinaryPrimitives.ReadUInt16LittleEndian(section[(pos + 4)..]);
                agg.AddSummaryCount((long)slot / slotsPerBucket, section[pos + 6], ids[service],
                                    BinaryPrimitives.ReadUInt32LittleEndian(section[(pos + 7)..]));
            }
        }
        finally { ArrayPool<int>.Shared.Return(ids); }
        return true;
    }

    /// <summary>One number that orders cells by (slot, service, level).</summary>
    internal static long SortKey(int slot, ushort service, byte level) =>
        ((long)slot << 24) | ((long)service << 8) | level;

    internal const long UnixEpochTicks = 621_355_968_000_000_000L;   // as LogVolumeAggregator
    internal const long TicksPerSecond = TimeSpan.TicksPerSecond;

    /// <summary>
    /// The slot of a timestamp: the aggregator's own arithmetic (LogVolumeAggregator.BucketOffset),
    /// then one more truncating division, which is why a slot's events share every bucket.
    /// </summary>
    internal static long SlotOf(long timestampTicks, int resolutionSeconds = SlotSeconds) =>
        (timestampTicks - UnixEpochTicks) / TicksPerSecond / resolutionSeconds;
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
    /// The longest service name, in UTF-8 bytes, a summary keeps — ingestion's own limit. A longer
    /// one costs the segment its summary rather than letting one name size the section.
    /// </summary>
    internal const int MaxServiceBytes = 1_024;

    /// <summary>
    /// The service names of one summary together, in UTF-8 bytes: 2 048 names of 32 bytes. Past it
    /// the names would outweigh the counts, and a store fed thousands of distinct long names per
    /// segment would hold megabytes of them per summary; that segment is scanned instead.
    /// </summary>
    internal const int MaxServiceBytesTotal = 65_536;

    /// <summary>
    /// Cells one summary may hold. Merges run inside an expiry bucket of a twelfth of the level's
    /// retention, so a 90-day level (the default for most) merges into week-scale segments of up
    /// to 672 slots, and a year-long one into month-scale segments of 2 880. 262 144 cells is 672
    /// slots × 390 service/level pairs, or 2 880 × 91: at most 2.9 MB of section on a segment that
    /// size, and 3 MB held in memory.
    /// </summary>
    internal const int MaxCells = 262_144;

    /// <summary>Services whose open-slot counters are a dense array (8 KB with <see cref="DenseLevels"/>).</summary>
    private const int DenseServices = 256;
    private const int DenseLevels   = 8;

    private const long UnixEpochTicks = SegmentCountsSummary.UnixEpochTicks;
    private const long SlotTicks      = SegmentCountsSummary.SlotSeconds * SegmentCountsSummary.TicksPerSecond;

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
    private int _svcBytesTotal;
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
            // See SegmentCountsSummary on why the slot nests exactly inside every bucket.
            long slot = SegmentCountsSummary.SlotOf(timestampTicks);
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
        if (_svcNames.Count >= MaxServices || utf8.Length > MaxServiceBytes
            || _svcBytesTotal + utf8.Length > MaxServiceBytesTotal) { Abandon(); return -1; }
        _svcBytesTotal += utf8.Length;
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
