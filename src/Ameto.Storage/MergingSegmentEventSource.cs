using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Column extents of one decompressed block — byte offsets into the decoded buffer, parsed once
/// per block instead of once per row.
///
/// <para>Offsets, not spans: a <c>ref struct</c> of spans cannot live in a field, and the block
/// cursor must survive between <see cref="ISegmentEventSource.TryReadNext"/> calls.</para>
/// </summary>
internal struct BlockColumnLayout
{
    public int   EventCount;
    public long  MinTs;
    public ulong MinId;
    public int   TOff, LOff, IOff;
    public int   TrOff, TrLen, SpOff, SpLen;
    public int   MtOff, MtLen, ExOff, ExLen, PrOff, PrLen, SvcOff, SvcLen;

    public static BlockColumnLayout Parse(ReadOnlySpan<byte> span)
    {
        var l = default(BlockColumnLayout);
        int pos      = 0;
        l.EventCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(pos, 4));  pos += 4;
        l.MinTs      = BinaryPrimitives.ReadInt64LittleEndian (span.Slice(pos, 8));       pos += 8;
        l.MinId      = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(pos, 8));       pos += 8;
        byte colCount = span[pos]; pos += 1;

        for (int c = 0; c < colCount; c++)
        {
            byte id      = span[pos]; pos += 1;
            int  byteLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(pos, 4)); pos += 4;
            int  off     = pos; pos += byteLen;
            switch (id)
            {
                case 1: l.TOff   = off; break;
                case 2: l.LOff   = off; break;
                case 3: l.IOff   = off; break;
                case 4: l.MtOff  = off; l.MtLen  = byteLen; break;
                case 5: l.ExOff  = off; l.ExLen  = byteLen; break;
                case 6: l.PrOff  = off; l.PrLen  = byteLen; break;
                case 7: l.TrOff  = off; l.TrLen  = byteLen; break;
                case 8: l.SpOff  = off; l.SpLen  = byteLen; break;
                case 9: l.SvcOff = off; l.SvcLen = byteLen; break;
                default: break;   // unknown columns ignored for forward compat
            }
        }
        return l;
    }
}

/// <summary>
/// A cursor over ONE .seg file's events in file order, decoding a single block at a time.
///
/// <para>Owns its <see cref="SegmentReader"/> and one pooled block buffer. The current row's
/// properties are handed out as a span INTO that buffer, so they are valid exactly until
/// <see cref="MoveNext"/> — which is the lifetime <see cref="ISegmentEventSource"/> promises.</para>
/// </summary>
internal sealed class SegmentEventCursor : IDisposable
{
    private readonly SegmentReader              _reader;
    private readonly Dictionary<string, string> _stringDedup;
    /// <summary>The dedup table probed by UTF-8 bytes — see <see cref="Dedup"/>.</summary>
    private readonly Dictionary<string, string>.AlternateLookup<ReadOnlySpan<byte>> _dedupByUtf8;

    private byte[]            _block = [];
    private int               _blockLen;
    private int               _blockIndex = -1;
    private int               _row        = -1;
    private BlockColumnLayout _layout;

    /// <summary>Sort key of the current row — read eagerly so the heap never materialises a row.</summary>
    public long  TsTicks { get; private set; }
    public ulong Id      { get; private set; }

    public SegmentEventCursor(SegmentReader reader, Dictionary<string, string> stringDedup)
    {
        _reader      = reader;
        _stringDedup = stringDedup;
        _dedupByUtf8 = stringDedup.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    public string FilePath => _reader.Info.FilePath;

    /// <summary>Advances to the next row, loading blocks as needed. False when the file is drained.</summary>
    public bool MoveNext()
    {
        while (true)
        {
            if (_blockIndex >= 0 && _row + 1 < _layout.EventCount)
            {
                _row++;
                var span = _block.AsSpan(0, _blockLen);
                TsTicks  = _layout.MinTs + BinaryPrimitives.ReadInt64LittleEndian(span.Slice(_layout.TOff + _row * 8, 8));
                Id       = _layout.MinId + BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(_layout.IOff + _row * 8, 8));
                return true;
            }
            if (_blockIndex + 1 >= _reader.BlockCount) return false;

            _blockIndex++;
            _blockLen = _reader.ReadBlockInto(_blockIndex, ref _block);
            _layout   = BlockColumnLayout.Parse(_block.AsSpan(0, _blockLen));
            _row      = -1;
        }
    }

    /// <summary>Materialises the current row. Valid until the next <see cref="MoveNext"/>.</summary>
    public SegmentEventRef Current
    {
        get
        {
            var span = _block.AsSpan(0, _blockLen);
            int i    = _row;
            int n    = _layout.EventCount;

            byte level = span[_layout.LOff + i];

            ulong trHi = 0, trLo = 0, spanId = 0;
            if (_layout.TrLen >= (i + 1) * 16)
            {
                trHi = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(_layout.TrOff + i * 16, 8));
                trLo = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(_layout.TrOff + i * 16 + 8, 8));
            }
            if (_layout.SpLen >= (i + 1) * 8)
                spanId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(_layout.SpOff + i * 8, 8));

            string  template = Dedup(RawColumn(span, _layout.MtOff,  _layout.MtLen,  i, n)) ?? string.Empty;
            string? service  = Dedup(RawColumn(span, _layout.SvcOff, _layout.SvcLen, i, n));

            // The exception column travels as BYTES, exactly like properties. Decoding it here
            // and re-encoding it in the writer, per row, cost this merge 11116 B/event against
            // 211 B/event for the same events without exceptions — 53×, on the path that
            // level-split flush sends every Error segment down.
            var excBytes = RawColumn(span, _layout.ExOff, _layout.ExLen, i, n);
            var props    = RawColumn(span, _layout.PrOff, _layout.PrLen, i, n);

            return new SegmentEventRef(Id, TsTicks, (LogLevel)level, trHi, trLo, spanId,
                                       template, service, exception: null, props, excBytes);
        }
    }

    /// <summary>Row <paramref name="i"/> of a string column: (n+1) uint32 offsets, then the payload.</summary>
    private static ReadOnlySpan<byte> RawColumn(ReadOnlySpan<byte> span, int off, int len, int i, int n)
    {
        int offsetsBytes = (n + 1) * 4;
        if (len < offsetsBytes) return default;
        uint start = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(off + i * 4, 4));
        uint end   = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(off + (i + 1) * 4, 4));
        if (end <= start) return default;
        return span.Slice(off + offsetsBytes + (int)start, (int)(end - start));
    }

    /// <summary>
    /// Entries the shared dedup table may hold before it is emptied. Matches
    /// <see cref="StringInternPool"/>'s ceiling, and for the same reason: past it the values are
    /// not a vocabulary any more.
    /// </summary>
    private const int MaxDedupEntries = 65_536;

    /// <summary>
    /// Collapses repeated template / service strings across blocks AND across the merge's source
    /// files. A day's segments share a handful of templates between them, so a merged file's
    /// distinct vocabulary is a rounding error against its event count.
    ///
    /// <para>The lookup is BY UTF-8, straight off the block where the value already lies. This
    /// used to decode first and deduplicate the result, which ran Encoding.UTF8.GetString for
    /// every string of every event and then handed the fresh instance to the GC the moment the
    /// table produced the one already on the heap — two strings per merged event, eight million
    /// Gen0 strings across a four-million-event file, not one of them ever read. Probing through
    /// an alternate comparer over the byte spelling builds a string once per DISTINCT value,
    /// which is what deduplicating was always supposed to mean.</para>
    ///
    /// <para>BOUNDED, because the table is otherwise the one thing in the merge that is not
    /// O(block + group): it retains an entry per DISTINCT template for the whole merge, and a
    /// template built by interpolation is distinct per event — the shape
    /// <see cref="StringInternPool"/>'s exhaustion warning exists for. At ~240 B an entry that
    /// extrapolates to the better part of a gigabyte across a full-sized merged file, which
    /// would silently reintroduce the memory ceiling the streaming merge removed. Dedup is a
    /// pure allocation optimisation — nothing is correct or incorrect about the result — so the
    /// table is simply emptied at the cap and refills with whatever the merge is seeing now.
    /// The dictionary keeps its buckets; what is released is the strings, which are the bulk.</para>
    /// </summary>
    private string? Dedup(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty) return null;
        if (_dedupByUtf8.TryGetValue(utf8, out var pooled)) return pooled;
        if (_stringDedup.Count >= MaxDedupEntries) _stringDedup.Clear();

        // Inserted through the alternate too: keyed by the bytes, the entry costs the one
        // transcode that produced the string rather than a second one to hash it.
        string created = Encoding.UTF8.GetString(utf8);
        _dedupByUtf8[utf8] = created;
        return created;
    }

    public void Dispose()
    {
        if (_block.Length > 0) { ArrayPool<byte>.Shared.Return(_block); _block = []; }
        _reader.Dispose();
    }
}

/// <summary>
/// Ordinal string equality with a UTF-8 alternate, so a string-keyed table can be PROBED with the
/// bytes of a segment column and only build a string when the value turns out to be one it has
/// not seen.
///
/// <para>The two sides agree because they hash the same thing — the UTF-8 spelling. The string
/// side pays a transcode into scratch, which happens once per DISTINCT value on insert (and not
/// even then when the entry goes in through the alternate); the span side, the one running twice
/// per merged event, hashes the block's bytes where they lie.</para>
/// </summary>
internal sealed class Utf8StringComparer : IEqualityComparer<string>,
                                           IAlternateEqualityComparer<ReadOnlySpan<byte>, string>
{
    public static readonly Utf8StringComparer Instance = new();
    private Utf8StringComparer() { }

    /// <summary>Templates and service names sit far inside this; longer values borrow from the pool.</summary>
    private const int StackScratch = 512;

    public bool Equals(string? x, string? y) => string.Equals(x, y, StringComparison.Ordinal);

    public int GetHashCode(string value)
    {
        int needed = Encoding.UTF8.GetByteCount(value);
        byte[]? rented = null;
        Span<byte> scratch = stackalloc byte[StackScratch];
        if (needed > StackScratch) scratch = rented = ArrayPool<byte>.Shared.Rent(needed);
        try
        {
            int written = Encoding.UTF8.GetBytes(value, scratch);
            return HashUtf8(scratch[..written]);
        }
        finally { if (rented is not null) ArrayPool<byte>.Shared.Return(rented); }
    }

    public string Create(ReadOnlySpan<byte> utf8) => Encoding.UTF8.GetString(utf8);

    public int GetHashCode(ReadOnlySpan<byte> utf8) => HashUtf8(utf8);

    public bool Equals(ReadOnlySpan<byte> utf8, string other)
    {
        // A yes here is final, and for the ASCII that a template or a service name almost always
        // is, that settles the comparison against the UTF-16 string with no buffer at all. A no is
        // not an answer — Ascii.Equals also reports false when either side simply is not ASCII —
        // so anything it rejects is decided on the decoded text.
        if (Ascii.Equals(utf8, other.AsSpan())) return true;
        if (other.Length > utf8.Length) return false;   // UTF-8 never spends fewer bytes than UTF-16 units

        char[]? rented = null;
        Span<char> scratch = stackalloc char[StackScratch];
        int needed = Encoding.UTF8.GetMaxCharCount(utf8.Length);
        if (needed > StackScratch) scratch = rented = ArrayPool<char>.Shared.Rent(needed);
        try
        {
            int written = Encoding.UTF8.GetChars(utf8, scratch);
            return other.AsSpan().SequenceEqual(scratch[..written]);
        }
        finally { if (rented is not null) ArrayPool<char>.Shared.Return(rented); }
    }

    private static int HashUtf8(ReadOnlySpan<byte> utf8)
    {
        var hash = new HashCode();
        hash.AddBytes(utf8);
        return hash.ToHashCode();
    }
}

/// <summary>
/// A k-way merge of several .seg files, presented as one <see cref="ISegmentEventSource"/> in
/// (timestamp, id) order.
///
/// <para>This is what makes a merged segment's size a POLICY question instead of a memory one.
/// Compaction used to read every source with <c>ReadAllRaw</c> into a managed
/// <c>List&lt;RawSegmentEvent&gt;</c>, copy that into a <see cref="HotTierSegment"/> and index the
/// result as one batch — peak ≈ 3× the batch, hence the 32 MB / 100k-event caps and the co-fit
/// gate that excluded dense segments from compaction altogether. Here the resident set is one
/// decompressed block PER SOURCE (~64 KB each) plus the writer's own block; the merged file can
/// be a day long and nothing grows with it.</para>
///
/// <para>Sources must already be sorted by (timestamp, id) — every .seg is, because
/// <see cref="SegmentWriter"/> only ever writes in that order. The heap therefore produces a
/// globally sorted stream, which the writer needs for the block zone map to stay monotonic.</para>
///
/// <para>Takes ownership of the readers: <see cref="Dispose"/> closes them, which on Windows is
/// what finally allows the source files to be deleted.</para>
/// </summary>
public sealed class MergingSegmentEventSource : ISegmentEventSource, IDisposable
{
    private readonly SegmentEventCursor[] _cursors;
    private readonly int[]                _heap;      // cursor indices, min-heap on (ts, id, index)
    private          int                  _heapCount;
    private          bool                 _advancePending;
    private          long                 _remaining;

    internal MergingSegmentEventSource(IReadOnlyList<SegmentReader> readers)
    {
        // One dedup table for the whole merge: templates and service names repeat across every
        // source file, so sharing it is what keeps string allocation proportional to distinct
        // values instead of to events. Keyed for probing BY UTF-8 (see SegmentEventCursor.Dedup),
        // which is what makes that sentence true rather than merely intended.
        var dedup = new Dictionary<string, string>(Utf8StringComparer.Instance);

        // Every cursor exists before any I/O happens, so a corrupt first block leaves a fully
        // formed object for Dispose to clean up — no reader is orphaned on a failed open.
        _cursors = new SegmentEventCursor[readers.Count];
        _heap    = new int[readers.Count];
        for (int i = 0; i < readers.Count; i++)
            _cursors[i] = new SegmentEventCursor(readers[i], dedup);

        try
        {
            for (int i = 0; i < readers.Count; i++)
            {
                _remaining += readers[i].Info.EventCount;
                if (_cursors[i].MoveNext()) _heap[_heapCount++] = i;
            }
            for (int i = _heapCount / 2 - 1; i >= 0; i--) SiftDown(i);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>
    /// Opens every source file and merges them; throws (having closed whatever it opened) if any
    /// file cannot be read. The engine's own merge opens its sources during PLANNING instead, so
    /// an unreadable one can be skip-listed before a manifest is written.
    /// </summary>
    public static MergingSegmentEventSource Open(IReadOnlyList<string> filePaths)
    {
        var readers = new List<SegmentReader>(filePaths.Count);
        try
        {
            foreach (var path in filePaths) readers.Add(SegmentReader.Open(path));
            return new MergingSegmentEventSource(readers);
        }
        catch
        {
            foreach (var r in readers) r.Dispose();
            throw;
        }
    }

    /// <summary>Exact, not an estimate: every source's event count is in its file header.</summary>
    public long RemainingEventHint => Math.Max(0, _remaining);

    public bool TryReadNext(out SegmentEventRef ev)
    {
        // The previous event's properties pointed into the winning cursor's block buffer, so the
        // cursor may only advance once the caller has come back for the next event.
        if (_advancePending)
        {
            _advancePending = false;
            if (_cursors[_heap[0]].MoveNext()) SiftDown(0);
            else
            {
                _heap[0] = _heap[--_heapCount];
                if (_heapCount > 0) SiftDown(0);
            }
        }

        if (_heapCount == 0) { ev = default; return false; }

        ev = _cursors[_heap[0]].Current;
        _advancePending = true;
        _remaining--;
        return true;
    }

    /// <summary>(ts, id, cursor) ordering — the cursor index breaks exact ties deterministically.</summary>
    private bool Less(int a, int b)
    {
        var ca = _cursors[a];
        var cb = _cursors[b];
        if (ca.TsTicks != cb.TsTicks) return ca.TsTicks < cb.TsTicks;
        if (ca.Id      != cb.Id)      return ca.Id      < cb.Id;
        return a < b;
    }

    private void SiftDown(int i)
    {
        while (true)
        {
            int l = 2 * i + 1, r = l + 1, m = i;
            if (l < _heapCount && Less(_heap[l], _heap[m])) m = l;
            if (r < _heapCount && Less(_heap[r], _heap[m])) m = r;
            if (m == i) return;
            (_heap[i], _heap[m]) = (_heap[m], _heap[i]);
            i = m;
        }
    }

    public void Dispose()
    {
        foreach (var c in _cursors) c.Dispose();
    }
}
