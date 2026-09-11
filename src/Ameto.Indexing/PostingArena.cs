using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Ameto.Indexing;

/// <summary>
/// Build-phase posting storage shared by the inverted and trigram accumulators: every bucket's
/// ascending offsets, already in the <see cref="SegmentBitmapCodec"/> wire form (varint gaps),
/// appended into fixed 32-byte chunks carved out of pooled 1 MB slabs.
///
/// <para>Why this and not a <c>List&lt;int&gt;</c> per bucket: a 64 MB group holds ~16 M
/// (trigram, offset) pairs in ~300 k buckets. One list object plus a doubling array per bucket
/// cost 4.8 B per pair retained and roughly the same again in growth garbage, the dense
/// buckets' arrays landing on the LOH — which the Workstation GC never compacts. Here a pair
/// costs its varint (one byte for a dense trigram, two or three for a sparse one) plus 4/32 of
/// chunk linkage, a bucket with ONE posting costs no chunk at all (the offset stays inline in
/// its <see cref="Bucket"/>), nothing doubles, and the whole arena is handed back to the pool
/// in one <see cref="Release"/>.</para>
///
/// <para>Serialisation is a copy: the section's posting list is <c>varint(count)</c> followed
/// by exactly the bytes the chunks hold, so <see cref="Write"/> produces the same bytes
/// <see cref="SegmentBitmapCodec.Encode"/> would from the decoded offsets — the parity tests
/// pin that. Out-of-order appends stay lossless (the gap wraps in <c>uint</c> and unwraps in
/// the decoder's <c>int</c> arithmetic), and the owner sorts them out once at serialisation
/// through <see cref="Decode"/>, exactly as the list-based build did.</para>
///
/// <para>Single-threaded by contract: one arena belongs to one index group's builder, which
/// the segment writer drives from one thread.</para>
/// </summary>
internal sealed class PostingArena
{
    private const int ChunkBytes    = 32;
    private const int ChunkHeader   = 4;                       // int32 next chunk id, -1 = last
    private const int Payload       = ChunkBytes - ChunkHeader; // 28
    private const int SlabBytes     = 1 << 20;
    private const int ChunksPerSlab = SlabBytes / ChunkBytes;  // 32768
    private const int ChunkShift    = 5;
    private const int SlabShift     = 15;
    private const int ChunkMask     = ChunksPerSlab - 1;

    /// <summary>
    /// One bucket's posting state. <c>default</c> is a valid empty bucket: <see cref="Count"/>
    /// is 0, and every other field is only read once it is not. <see cref="Last"/> doubles as
    /// the inline home of a single posting, so the ~70 % of buckets that only ever see one
    /// offset (trace ids, span ids, request ids) never touch a chunk.
    /// </summary>
    public struct Bucket
    {
        public int Head;      // first chunk, valid when Count >= 2
        public int Tail;      // chunk being appended to, valid when Count >= 2
        public int Count;     // postings held
        public int Last;      // most recent offset (the only one while Count == 1)
        public int ByteLen;   // varint bytes held in chunks (Count >= 2)
        public int TailUsed;  // payload bytes used in Tail
    }

    private byte[][] _slabs = new byte[4][];
    private int      _slabCount;
    private int      _chunkCount;

    /// <summary>Chunks handed out — for probes that want to know what the arena holds.</summary>
    public int ChunkCount => _chunkCount;

    /// <summary>Bytes the slabs occupy (rented, so retained until <see cref="Release"/>).</summary>
    public long RetainedBytes => (long)_slabCount * SlabBytes;

    /// <summary>
    /// Appends <paramref name="offset"/> to <paramref name="b"/>. Returns false for a repeat of
    /// the bucket's current offset — the same trigram in the template and a property of one
    /// event — which is the only duplicate an in-order build can produce. An offset BELOW the
    /// current one sets <paramref name="unsorted"/> and is stored anyway; the owner restores the
    /// ascending+distinct invariant at serialisation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Append(ref Bucket b, int offset, ref bool unsorted)
    {
        if (b.Count == 0)
        {
            b.Last  = offset;
            b.Count = 1;
            return true;
        }

        int last = b.Last;
        if (last == offset) return false;
        if (last > offset)  unsorted = true;

        if (b.Count == 1)
        {
            // Second posting: the inline first one moves into a fresh chunk as its own gap
            // (prev = -1 ⇒ gap = offset itself).
            int c = NewChunk();
            b.Head = c; b.Tail = c; b.TailUsed = 0; b.ByteLen = 0;
            WriteVarint(ref b, (uint)last);
        }

        WriteVarint(ref b, (uint)(offset - last - 1));
        b.Last = offset;
        b.Count++;
        return true;
    }

    /// <summary>Bytes <see cref="Write"/> will produce for <paramref name="b"/>.</summary>
    public static int SerialisedSize(in Bucket b)
    {
        if (b.Count == 0) return 1;
        if (b.Count == 1) return 1 + VarintLen((uint)b.Last);
        return VarintLen((uint)b.Count) + b.ByteLen;
    }

    /// <summary>
    /// Writes the bucket in <see cref="SegmentBitmapCodec"/> form into <paramref name="dest"/>,
    /// which must hold <see cref="SerialisedSize"/> bytes. Returns the bytes written.
    /// </summary>
    public int Write(in Bucket b, Span<byte> dest)
    {
        int pos = 0;
        PutVarint(dest, ref pos, (uint)b.Count);
        if (b.Count == 0) return pos;
        if (b.Count == 1) { PutVarint(dest, ref pos, (uint)b.Last); return pos; }

        int c = b.Head;
        while (true)
        {
            var slab = _slabs[c >> SlabShift];
            int off  = (c & ChunkMask) << ChunkShift;
            int len  = c == b.Tail ? b.TailUsed : Payload;
            slab.AsSpan(off + ChunkHeader, len).CopyTo(dest.Slice(pos));
            pos += len;
            if (c == b.Tail) break;
            c = BinaryPrimitives.ReadInt32LittleEndian(slab.AsSpan(off, 4));
        }
        return pos;
    }

    /// <summary>
    /// Decodes the bucket's offsets, in append order, into <paramref name="dest"/> (at least
    /// <see cref="Bucket.Count"/> ints). For the build-mode lookups tests use and for the
    /// out-of-order normalisation; the production path never decodes.
    /// </summary>
    public int Decode(in Bucket b, Span<int> dest)
    {
        if (b.Count == 0) return 0;
        if (b.Count == 1) { dest[0] = b.Last; return 1; }

        int n = 0, prev = -1;
        uint v = 0; int shift = 0;
        int c = b.Head;
        while (true)
        {
            var slab = _slabs[c >> SlabShift];
            int off  = (c & ChunkMask) << ChunkShift;
            int len  = c == b.Tail ? b.TailUsed : Payload;
            var span = slab.AsSpan(off + ChunkHeader, len);
            for (int i = 0; i < span.Length; i++)
            {
                byte x = span[i];
                v |= (uint)(x & 0x7F) << shift;
                shift += 7;
                if ((x & 0x80) == 0)
                {
                    int o = prev + 1 + (int)v;
                    dest[n++] = o;
                    prev = o; v = 0; shift = 0;
                }
            }
            if (c == b.Tail) break;
            c = BinaryPrimitives.ReadInt32LittleEndian(slab.AsSpan(off, 4));
        }
        return n;
    }

    /// <summary>Returns every slab to the pool. The arena is empty and reusable afterwards.</summary>
    public void Release()
    {
        for (int i = 0; i < _slabCount; i++)
        {
            ArrayPool<byte>.Shared.Return(_slabs[i]);
            _slabs[i] = null!;
        }
        _slabCount  = 0;
        _chunkCount = 0;
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteVarint(ref Bucket b, uint v)
    {
        while (v >= 0x80)
        {
            PutByte(ref b, (byte)(v | 0x80));
            v >>= 7;
        }
        PutByte(ref b, (byte)v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PutByte(ref Bucket b, byte x)
    {
        if (b.TailUsed == Payload)
        {
            int c = NewChunk();
            var tail = _slabs[b.Tail >> SlabShift];
            BinaryPrimitives.WriteInt32LittleEndian(tail.AsSpan((b.Tail & ChunkMask) << ChunkShift, 4), c);
            b.Tail     = c;
            b.TailUsed = 0;
        }
        _slabs[b.Tail >> SlabShift][((b.Tail & ChunkMask) << ChunkShift) + ChunkHeader + b.TailUsed] = x;
        b.TailUsed++;
        b.ByteLen++;
    }

    private int NewChunk()
    {
        int id = _chunkCount++;
        if ((id & ChunkMask) == 0) AddSlab();
        var slab = _slabs[id >> SlabShift];
        BinaryPrimitives.WriteInt32LittleEndian(slab.AsSpan((id & ChunkMask) << ChunkShift, 4), -1);
        return id;
    }

    private void AddSlab()
    {
        if (_slabCount == _slabs.Length) Array.Resize(ref _slabs, _slabs.Length * 2);
        _slabs[_slabCount++] = ArrayPool<byte>.Shared.Rent(SlabBytes);
    }

    private static int VarintLen(uint v)
        => v < 0x80 ? 1 : v < 0x4000 ? 2 : v < 0x20_0000 ? 3 : v < 0x1000_0000 ? 4 : 5;

    private static void PutVarint(Span<byte> dest, ref int pos, uint v)
    {
        while (v >= 0x80) { dest[pos++] = (byte)(v | 0x80); v >>= 7; }
        dest[pos++] = (byte)v;
    }
}
