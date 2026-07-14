using System.Buffers;
using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// Correctness + allocation gate for the zero-alloc segment posting-list codec that replaces
/// per-bucket RoaringBitmap serialisation on the flush hot path.
/// </summary>
public sealed class SegmentBitmapCodecTests
{
    // ── Round-trip correctness ─────────────────────────────────────────────────

    public static IEnumerable<object[]> Cases()
    {
        yield return new object[] { Array.Empty<int>() };                       // empty
        yield return new object[] { new[] { 0 } };                              // single at 0
        yield return new object[] { new[] { 5 } };                              // single sparse
        yield return new object[] { new[] { 0, 1, 2, 3, 4 } };                  // contiguous run
        yield return new object[] { new[] { 0, 2, 4, 6, 8 } };                  // even gaps
        yield return new object[] { new[] { 1, 100, 10_000, 1_000_000 } };      // large gaps
        yield return new object[] { new[] { 0, 1, 1_000_000, 1_000_001 } };     // mixed dense/sparse
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void RoundTrip_Decode_MatchesInput(int[] offsets)
    {
        Span<byte> buf = new byte[SegmentBitmapCodec.MaxEncodedSize(offsets.Length)];
        int written = SegmentBitmapCodec.Encode(offsets, buf);
        Assert.True(written >= 0);

        var encoded = buf[..written];
        Assert.Equal(offsets.Length, SegmentBitmapCodec.Count(encoded));

        var decoded = new int[offsets.Length];
        int n = SegmentBitmapCodec.Decode(encoded, decoded);
        Assert.Equal(offsets.Length, n);
        Assert.Equal(offsets, decoded);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Enumerator_YieldsSameAsDecode(int[] offsets)
    {
        Span<byte> buf = new byte[SegmentBitmapCodec.MaxEncodedSize(offsets.Length)];
        int written = SegmentBitmapCodec.Encode(offsets, buf);

        var got = new List<int>();
        var e = SegmentBitmapCodec.Enumerate(buf[..written]);
        while (e.MoveNext()) got.Add(e.Current);

        Assert.Equal(offsets, got);
    }

    [Fact]
    public void RoundTrip_ManyRandomMonotonicSets()
    {
        var rng = new Random(1234);
        for (int trial = 0; trial < 2_000; trial++)
        {
            int count = rng.Next(0, 400);
            var offsets = new int[count];
            int cur = rng.Next(0, 5);
            for (int i = 0; i < count; i++)
            {
                cur += 1 + rng.Next(0, trial % 3 == 0 ? 1 : 50); // some dense, some sparse
                offsets[i] = cur;
            }

            Span<byte> buf = new byte[SegmentBitmapCodec.MaxEncodedSize(count)];
            int written = SegmentBitmapCodec.Encode(offsets, buf);
            Assert.True(written >= 0);

            var decoded = new int[count];
            SegmentBitmapCodec.Decode(buf[..written], decoded);
            Assert.Equal(offsets, decoded);
        }
    }

    // ── Buffer bounds ──────────────────────────────────────────────────────────

    [Fact]
    public void Encode_ReturnsMinusOne_WhenBufferTooSmall()
    {
        var offsets = new[] { 0, 1_000_000, 2_000_000 };
        Assert.Equal(-1, SegmentBitmapCodec.Encode(offsets, new byte[2]));
    }

    [Fact]
    public void MaxEncodedSize_AlwaysSufficient()
    {
        var rng = new Random(99);
        for (int trial = 0; trial < 500; trial++)
        {
            int count = rng.Next(0, 200);
            var offsets = new int[count];
            int cur = -1;
            for (int i = 0; i < count; i++) { cur += 1 + rng.Next(0, 1_000_000); offsets[i] = cur; }
            int written = SegmentBitmapCodec.Encode(offsets, new byte[SegmentBitmapCodec.MaxEncodedSize(count)]);
            Assert.True(written >= 0, $"MaxEncodedSize insufficient for count={count}");
        }
    }

    // ── Compactness (the point: single/dense buckets are tiny) ─────────────────

    [Fact]
    public void SingleOffset_EncodesToFewBytes()
    {
        Span<byte> buf = new byte[16];
        Assert.True(SegmentBitmapCodec.Encode(new[] { 42 }, buf) <= 3);   // count(1) + one small varint
    }

    [Fact]
    public void ContiguousRun_IsOneBytePerOffset()
    {
        const int n = 1000;
        var offsets = new int[n];
        for (int i = 0; i < n; i++) offsets[i] = i;
        Span<byte> buf = new byte[SegmentBitmapCodec.MaxEncodedSize(n)];
        int written = SegmentBitmapCodec.Encode(offsets, buf);
        // count varint (2 bytes for 1000) + n zero-deltas (1 byte each).
        Assert.True(written <= n + 2, $"contiguous run took {written} bytes for {n} offsets");
    }

    // ── Allocation: encode + decode are heap-free ──────────────────────────────

    [Fact]
    public void EncodeDecode_AllocatesNothing()
    {
        var offsets = new int[200];
        for (int i = 0; i < offsets.Length; i++) offsets[i] = i * 3;
        byte[] buf = ArrayPool<byte>.Shared.Rent(SegmentBitmapCodec.MaxEncodedSize(offsets.Length));
        int[] outBuf = ArrayPool<int>.Shared.Rent(offsets.Length);

        // Warm up hard so tiered JIT recompilation happens BEFORE the measured section.
        for (int i = 0; i < 2_000; i++) { int w = SegmentBitmapCodec.Encode(offsets, buf); SegmentBitmapCodec.Decode(buf.AsSpan(0, w), outBuf); }

        const int iters = 100_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++)
        {
            int w = SegmentBitmapCodec.Encode(offsets, buf);
            long sum = 0;
            var e = SegmentBitmapCodec.Enumerate(buf.AsSpan(0, w));
            while (e.MoveNext()) sum += e.Current;
            GC.KeepAlive(sum);
        }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        ArrayPool<byte>.Shared.Return(buf);
        ArrayPool<int>.Shared.Return(outBuf);
        // Per-call heap allocation would grow with iters (≥ iters bytes). A tiny fixed
        // residue from runtime/JIT noise is fine; the point is it does not scale.
        Assert.True(allocated < iters / 10,
            $"expected no per-call allocation, got {allocated} B over {iters:N0} encode+enumerate ({(double)allocated / iters:F3} B/call)");
    }
}
