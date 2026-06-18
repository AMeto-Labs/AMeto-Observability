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

    private readonly byte*  _bits;
    private readonly uint   _blockCount;
    private readonly uint   _capacity;
    private          bool   _disposed;

    private SegmentBloomFilter(byte* bits, uint blockCount, uint capacity)
    {
        _bits       = bits;
        _blockCount = blockCount;
        _capacity   = capacity;
    }

    /// <summary>Creates a new empty filter sized for <paramref name="expectedItems"/>.</summary>
    public static SegmentBloomFilter Create(int expectedItems)
    {
        // ~10 bits per item → ~1% FPR; round up to multiple of 512
        uint totalBits  = (uint)Math.Max(expectedItems * 10, BlockBits);
        totalBits       = (totalBits + (uint)(BlockBits - 1)) & ~(uint)(BlockBits - 1);
        uint blockCount = totalBits / BlockBits;
        uint byteCount  = totalBits / 8;

        var bits = (byte*)NativeMemory.AllocZeroed(byteCount);
        return new SegmentBloomFilter(bits, blockCount, (uint)expectedItems);
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

    public void Add(string value) => Add(System.Text.Encoding.UTF8.GetBytes(value));

    // ── Query ─────────────────────────────────────────────────────────────────

    public bool MightContain(ReadOnlySpan<byte> key)
    {
        var (h0, h1, h2) = Hash3(key);
        uint blockIdx    = h0 % _blockCount;
        byte* block      = _bits + blockIdx * BlockBytes;

        return TestBit(block, h1 % BlockBits) &&
               TestBit(block, h2 % BlockBits);
    }

    public bool MightContain(string value) => MightContain(System.Text.Encoding.UTF8.GetBytes(value));

    // ── Serialisation ─────────────────────────────────────────────────────────

    public byte[] Serialise()
    {
        uint byteCount = _blockCount * BlockBytes;
        var buf = new byte[4 + 4 + byteCount]; // bitCount + capacity + bits
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(0), _blockCount * BlockBits);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), _capacity);
        new Span<byte>(_bits, (int)byteCount).CopyTo(buf.AsSpan(8));
        return buf;
    }

    public static SegmentBloomFilter Deserialise(ReadOnlySpan<byte> data)
    {
        if (data.Length < 8)
            return Create(0);

        uint bitCount   = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data[0..]);
        uint capacity   = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
        uint blockCount = bitCount / BlockBits;
        uint byteCount  = bitCount / 8;

        var bits = (byte*)NativeMemory.AllocZeroed(byteCount);
        data.Slice(8, (int)byteCount).CopyTo(new Span<byte>(bits, (int)byteCount));
        return new SegmentBloomFilter(bits, blockCount, capacity);
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
