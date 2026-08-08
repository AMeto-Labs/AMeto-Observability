using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Ameto.Indexing;

/// <summary>
/// Per-segment XOR/blocked Bloom filter stored on NativeMemory.
///
/// We use a simple blocked Bloom filter (block size = 64 bytes = 512 bits) with
/// 3 independent 32-bit hash functions (MurmurHash3 finaliser mix).
///
/// False-positive rate: ~1% at capacity; false-negative rate: 0% (guaranteed).
///
/// Indexed key: the raw bytes of a UTF-8 serialised property value string.
///
/// Binary format:
///   uint32 bitCount   (must be a multiple of 512)
///   uint32 capacity   (informational; number of items the filter was sized for)
///   byte[] bits       (bitCount / 8 bytes)
/// </summary>
public sealed unsafe class SegmentBloomFilter : IDisposable
{
    // Each block = 64 bytes = 512 bits; we pick a block per hash[0], then set 2 bits inside it.
    private const int BlockBytes = 64;
    private const int BlockBits  = BlockBytes * 8; // 512

    /// <summary>
    /// High bit of the serialised <c>capacity</c> word, set by writers that case-fold on
    /// <see cref="Add(ReadOnlySpan{char})"/>.
    ///
    /// <para>Capacity is informational — it is the segment's expected event count, never
    /// within nine orders of magnitude of 2^31 — so the spare bit costs nothing and saves a
    /// whole-file version bump. A blob without it was written by a build that stored the
    /// ORIGINAL casing, and <see cref="MightContain(ReadOnlySpan{char})"/> must not treat its
    /// silence as proof; see there.</para>
    /// </summary>
    private const uint FoldedMarker = 0x8000_0000u;

    private readonly byte*  _bits;
    private readonly uint   _blockCount;
    private readonly uint   _capacity;
    private readonly bool   _folded;
    private          bool   _disposed;

    private SegmentBloomFilter(byte* bits, uint blockCount, uint capacity, bool folded = true)
    {
        _bits       = bits;
        _blockCount = blockCount;
        _capacity   = capacity;
        _folded     = folded;
    }

    /// <summary>
    /// Largest item count that still sizes honestly. Above it the filter is capped rather
    /// than allowed to wrap: <c>expectedItems * 10</c> in int arithmetic used to overflow
    /// silently and hand back a 64-byte match-everything filter — no crash, just a
    /// prefilter that stops rejecting anything. Callers now size by TERM count, which
    /// reaches this an order of magnitude sooner than event count did.
    /// </summary>
    private const long MaxExpectedItems = (long)int.MaxValue / 10;

    /// <summary>Creates a new empty filter sized for <paramref name="expectedItems"/>.</summary>
    public static SegmentBloomFilter Create(long expectedItems)
    {
        // ~10 bits per item → ~1% FPR; round up to multiple of 512. Long arithmetic
        // throughout, then clamped — the old int product wrapped instead of saturating.
        long clamped   = Math.Clamp(expectedItems, 1, MaxExpectedItems);
        long bitsWanted = Math.Max(clamped * 10, BlockBits);
        uint totalBits  = (uint)Math.Min(bitsWanted, uint.MaxValue - BlockBits);
        totalBits       = (totalBits + (uint)(BlockBits - 1)) & ~(uint)(BlockBits - 1);
        uint blockCount = totalBits / BlockBits;
        uint byteCount  = totalBits / 8;

        var bits = (byte*)NativeMemory.AllocZeroed(byteCount);
        return new SegmentBloomFilter(bits, blockCount, (uint)clamped);
    }

    // ── Write ─────────────────────────────────────────────────────────────────

    public void Add(ReadOnlySpan<byte> key)
    {
        var (h0, h1, h2) = Hash3(key);
        uint blockIdx    = h0 % _blockCount;
        byte* block      = _bits + blockIdx * BlockBytes;

        SetBit(block, h1 % BlockBits);
        SetBit(block, h2 % BlockBits);
    }

    public void Add(string value) => Add((ReadOnlySpan<char>)value);

    /// <summary>
    /// Adds the CASE-FOLDED form of <paramref name="value"/>, encoding into stack/pooled
    /// scratch instead of allocating a byte[] per add.
    ///
    /// <para>Folding is a correctness requirement, not a nicety: the per-event scan compares
    /// strings <see cref="StringComparison.OrdinalIgnoreCase"/>, so a filter keyed on the raw
    /// casing answers "not here" for a value that differs from the query literal only in case
    /// — <c>@l = 'error'</c> against a stored <c>Error</c> — and the whole segment is dropped
    /// unread. Folding also keeps the filter's occupancy where it was; storing both casings
    /// would have raised the false-positive rate for every segment.</para>
    /// </summary>
    public void Add(ReadOnlySpan<char> value)
    {
        char[]? foldRented = value.Length > 256
            ? System.Buffers.ArrayPool<char>.Shared.Rent(value.Length)
            : null;
        Span<char> foldBuf = foldRented ?? stackalloc char[256];
        int foldLen = value.ToLowerInvariant(foldBuf);
        ReadOnlySpan<char> folded = foldLen < 0 ? value : foldBuf[..foldLen];

        AddRaw(folded);

        if (foldRented is not null) System.Buffers.ArrayPool<char>.Shared.Return(foldRented);
    }

    private void AddRaw(ReadOnlySpan<char> value)
    {
        int max = System.Text.Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rented = max > 512 ? System.Buffers.ArrayPool<byte>.Shared.Rent(max) : null;
        Span<byte> buf = rented ?? stackalloc byte[max];
        int n = System.Text.Encoding.UTF8.GetBytes(value, buf);
        Add(buf[..n]);
        if (rented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(rented);
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    public bool MightContain(ReadOnlySpan<byte> key)
    {
        var (h0, h1, h2) = Hash3(key);
        uint blockIdx    = h0 % _blockCount;
        byte* block      = _bits + blockIdx * BlockBytes;

        return TestBit(block, h1 % BlockBits) &&
               TestBit(block, h2 % BlockBits);
    }

    public bool MightContain(string value) => MightContain((ReadOnlySpan<char>)value);

    /// <summary>
    /// Probes the folded form first — that is what <see cref="Add(ReadOnlySpan{char})"/>
    /// stores — then the raw form, which is what a pre-folding writer stored.
    ///
    /// <para>For a segment written BEFORE folding, two probes are still not enough to prove
    /// absence. The filter holds one arbitrary casing and the scan compares
    /// OrdinalIgnoreCase, so <c>@l = 'error'</c> misses a stored <c>Error</c> on the folded
    /// probe AND on the raw probe, and the phase-1 gate drops the segment before the (now
    /// case-insensitive) inverted index can disagree. Folding on the WRITE side fixes nothing
    /// for bytes already on disk. So an unfolded filter may only answer "no" for a value that
    /// has no cased characters — a number, a punctuation-only key — where its one spelling is
    /// the only spelling. Everything else gets "might", which costs those segments the cheap
    /// gate and keeps their rows; the inverted index still prunes them one phase later, and
    /// compaction retires the state as it rewrites them.</para>
    /// </summary>
    public bool MightContain(ReadOnlySpan<char> value)
    {
        char[]? foldRented = value.Length > 256
            ? System.Buffers.ArrayPool<char>.Shared.Rent(value.Length)
            : null;
        Span<char> foldBuf = foldRented ?? stackalloc char[256];
        int foldLen = value.ToLowerInvariant(foldBuf);
        ReadOnlySpan<char> folded = foldLen < 0 ? value : foldBuf[..foldLen];

        bool hit = MightContainRaw(folded);
        if (!hit && !folded.SequenceEqual(value)) hit = MightContainRaw(value);
        if (!hit && !_folded && HasCasedChars(value)) hit = true;   // cannot prove absence

        if (foldRented is not null) System.Buffers.ArrayPool<char>.Shared.Return(foldRented);
        return hit;
    }

    /// <summary>True when some character has a different upper/lower spelling, i.e. when a
    /// pre-folding writer could have stored a casing this probe will not reproduce.</summary>
    private static bool HasCasedChars(ReadOnlySpan<char> value)
    {
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (char.ToLowerInvariant(c) != c || char.ToUpperInvariant(c) != c) return true;
        }
        return false;
    }

    private bool MightContainRaw(ReadOnlySpan<char> value)
    {
        int max = System.Text.Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rented = max > 512 ? System.Buffers.ArrayPool<byte>.Shared.Rent(max) : null;
        Span<byte> buf = rented ?? stackalloc byte[max];
        int n = System.Text.Encoding.UTF8.GetBytes(value, buf);
        bool hit = MightContain(buf[..n]);
        if (rented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        return hit;
    }

    // ── Serialisation ─────────────────────────────────────────────────────────

    public byte[] Serialise()
    {
        uint byteCount = _blockCount * BlockBytes;
        var buf = new byte[4 + 4 + byteCount]; // bitCount + capacity | foldedMarker + bits
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(0), _blockCount * BlockBits);
        // Only claim folded when this instance's contents really were folded — a filter that
        // was read back from a pre-folding blob and re-serialised must keep saying so.
        uint capacityWord = _folded ? (_capacity & ~FoldedMarker) | FoldedMarker : _capacity & ~FoldedMarker;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), capacityWord);
        new Span<byte>(_bits, (int)byteCount).CopyTo(buf.AsSpan(8));
        return buf;
    }

    public static SegmentBloomFilter Deserialise(ReadOnlySpan<byte> data)
    {
        // An absent/empty bloom section means "no index was built" (e.g. a WAL
        // recovery flushed before the index builder was wired) — that is NO
        // information, so the filter must never reject. An all-zero Create(0)
        // would answer "contains nothing" and make the whole segment invisible
        // to equality-hinted queries. All-ones bits ⇒ MightContain always true.
        if (data.Length < 8)
        {
            var matchAll = Create(0);
            new Span<byte>(matchAll._bits, (int)(matchAll._blockCount * BlockBytes)).Fill(0xFF);
            return matchAll;
        }

        uint bitCount     = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data[0..]);
        uint capacityWord = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
        bool folded       = (capacityWord & FoldedMarker) != 0;
        uint capacity     = capacityWord & ~FoldedMarker;
        uint blockCount   = bitCount / BlockBits;
        uint byteCount    = bitCount / 8;

        var bits = (byte*)NativeMemory.AllocZeroed(byteCount);
        data.Slice(8, (int)byteCount).CopyTo(new Span<byte>(bits, (int)byteCount));
        return new SegmentBloomFilter(bits, blockCount, capacity, folded);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SetBit(byte* block, uint bit) =>
        block[bit >> 3] |= (byte)(1 << (int)(bit & 7));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TestBit(byte* block, uint bit) =>
        (block[bit >> 3] & (byte)(1 << (int)(bit & 7))) != 0;

    /// <summary>
    /// Three independent 32-bit hashes via MurmurHash3-style mix of two seeds.
    /// h_i(x) = hash(x, seed_i)  where seed_0=0, seed_1=0x9747b28c, seed_2=0xd4bc5a93
    /// </summary>
    private static (uint h0, uint h1, uint h2) Hash3(ReadOnlySpan<byte> data)
    {
        uint h0 = Murmur32(data, 0);
        uint h1 = Murmur32(data, 0x9747b28c);
        uint h2 = Murmur32(data, 0xd4bc5a93);
        return (h0, h1, h2);
    }

    private static uint Murmur32(ReadOnlySpan<byte> data, uint seed)
    {
        const uint c1 = 0xcc9e2d51u;
        const uint c2 = 0x1b873593u;
        uint h = seed;

        int i = 0;
        while (i + 4 <= data.Length)
        {
            uint k = (uint)(data[i] | (data[i+1] << 8) | (data[i+2] << 16) | (data[i+3] << 24));
            k  *= c1; k  = RotL(k, 15); k *= c2;
            h  ^= k;  h  = RotL(h, 13); h = h * 5 + 0xe6546b64u;
            i  += 4;
        }

        uint tail = 0;
        switch (data.Length - i)
        {
            case 3: tail |= (uint)data[i+2] << 16; goto case 2;
            case 2: tail |= (uint)data[i+1] << 8;  goto case 1;
            case 1: tail |= data[i];
                tail *= c1; tail = RotL(tail, 15); tail *= c2; h ^= tail;
                break;
        }

        h ^= (uint)data.Length;
        // Finalise mix
        h ^= h >> 16; h *= 0x85ebca6bu; h ^= h >> 13; h *= 0xc2b2ae35u; h ^= h >> 16;
        return h;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint RotL(uint x, int r) => (x << r) | (x >> (32 - r));

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        NativeMemory.Free(_bits);
    }
}
