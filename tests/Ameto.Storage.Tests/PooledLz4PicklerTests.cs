using Ameto.Tracing.Storage;
using K4os.Compression.LZ4;
using System.Buffers;
using System.Numerics;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE FRAME IS THE PICKLER'S OWN, BYTE FOR BYTE. <see cref="PooledLz4Pickler"/> writes the LZ4 frame
/// <c>LZ4Pickler.Pickle(ReadOnlySpan&lt;byte&gt;, LZ4Level)</c> returns into a buffer it keeps, so a span
/// block costs no array; the format on disk stays the pickler's, which every reader inflates with
/// <c>LZ4Pickler.Unpickle</c>. The golden <c>.trc</c> hashes (<c>TraceFlushProbe</c>) pin the frames a
/// flush of the golden corpus writes; this pins every shape the frame has, so a block that corpus never
/// produces is pinned too: the three header widths (a length difference of one, two and four bytes), the
/// raw store behind a zero byte when LZ4 does not shrink the block, and the one- and four-byte sources
/// (the v4 trace-index stub is four zero bytes). The 70 000-byte shape — a source whose length needs four
/// bytes, shrunk by under 65 536 — is the one the pickler's own <c>IBufferWriter</c> overload gets two
/// bytes wrong, which is why that overload is not what the writer uses.
/// </summary>
public sealed class PooledLz4PicklerTests
{
    public static TheoryData<string, int, byte> Shapes => new()
    {
        { "zeros",      4,         0x00 },   // the v4 trace-index stub: four zero bytes, stored raw
        { "byte",       1,         0x00 },
        { "repetitive", 100,       0x40 },   // shrinks by under 256 bytes: a one-byte difference
        { "repetitive", 1_000,     0x80 },   // by under 65 536: two bytes
        { "mixed",      70_000,    0x80 },   // two bytes, on a source whose length needs four — the writer overload's miss
        { "repetitive", 300_000,   0xC0 },   // four bytes
        { "vocabulary", 2_000_000, 0xC0 },   // a full span block of msgpack-shaped bytes
        { "random",     100,       0x00 },   // random bytes: stored raw behind a zero byte
        { "random",     200_000,   0x00 },
    };

    [Theory]
    [MemberData(nameof(Shapes))]
    public void The_frame_is_what_LZ4Pickler_returns(string shape, int bytes, byte headerByte)
    {
        byte[] raw     = Corpus(shape, bytes);
        var    pickler = new PooledLz4Pickler(scratch: null);
        try
        {
            foreach (var level in new[] { LZ4Level.L00_FAST, SpanWriter.FlushLevel, SpanWriter.MergeLevel })
            {
                byte[] expected = LZ4Pickler.Pickle(raw, level);
                byte[] frame    = pickler.Pickle(raw, level).ToArray();

                Assert.Equal(headerByte, frame[0]);                  // the shape this row is about
                Assert.Equal(expected, frame);
                Assert.Equal(raw, LZ4Pickler.Unpickle(frame));
            }
        }
        finally { pickler.Return(); }
    }

    [Fact]
    public void An_empty_source_is_an_empty_frame_and_rents_nothing()
    {
        var pickler = new PooledLz4Pickler(scratch: null);
        Assert.True(pickler.Pickle(ReadOnlySpan<byte>.Empty, SpanWriter.FlushLevel).IsEmpty);
        Assert.Null(pickler.BufferForTest);
        pickler.Return();
    }

    /// <summary>
    /// THE POINT OF THE CLASS: once the buffer is rented, a segment's blocks allocate nothing on the managed
    /// heap. <c>LZ4Codec.Encode</c> keeps its state off the heap, and the frame lands in the one buffer — where
    /// <c>LZ4Pickler.Pickle</c> returned a fresh array per block, LOH-sized for a span block (8,1 MB over
    /// fifty 1 MiB blocks at L03_HC, measured on 1.3.8). Per-thread, like the flush probe's figure.
    /// </summary>
    [Fact]
    public void Blocks_after_the_first_allocate_nothing()
    {
        byte[] raw     = Corpus("vocabulary", 1 << 20);
        var    pickler = new PooledLz4Pickler(scratch: null);
        try
        {
            pickler.Pickle(raw, SpanWriter.FlushLevel);              // the rental
            var buffer = pickler.BufferForTest;
            Assert.NotNull(buffer);

            long a0     = GC.GetAllocatedBytesForCurrentThread();
            long frames = 0;
            for (int i = 0; i < 50; i++) frames += pickler.Pickle(raw, SpanWriter.FlushLevel).Length;
            long allocated = GC.GetAllocatedBytesForCurrentThread() - a0;

            Assert.Same(buffer, pickler.BufferForTest);
            Assert.True(frames > 0);
            Assert.True(allocated < 1024, $"fifty blocks allocated {allocated:N0} B");
        }
        finally { pickler.Return(); }
    }

    /// <summary>
    /// The buffer follows the largest block seen, and between writes it is the ENGINE'S (#90): the next
    /// write finds it in the scratch whatever thread it runs on, and takes it out while it is in use.
    /// </summary>
    [Fact]
    public void The_buffer_grows_to_the_largest_block_and_is_the_engines_between_writes()
    {
        var scratch = new SpanWriteScratch();

        var first = new PooledLz4Pickler(scratch);
        first.Pickle(Corpus("vocabulary", 100_000), SpanWriter.FlushLevel);
        var small = first.BufferForTest!;
        first.Pickle(Corpus("vocabulary", 1 << 20), SpanWriter.FlushLevel);        // a larger block: a larger rental
        var large = first.BufferForTest!;
        Assert.NotSame(small, large);
        Assert.True(large.Length >= (1 << 20) + PooledLz4Pickler.MaxHeaderBytes);
        first.Return();
        Assert.Null(first.BufferForTest);
        Assert.Same(large, scratch.HeldPickleForTest);                               // kept for the next write
        first.Return();                                                              // idempotent
        Assert.Same(large, scratch.HeldPickleForTest);

        var second = new PooledLz4Pickler(scratch);
        second.Pickle(Corpus("vocabulary", 100_000), SpanWriter.FlushLevel);
        Assert.Same(large, second.BufferForTest);                                    // found again
        Assert.Null(scratch.HeldPickleForTest);                                      // and exclusive while in use
        second.Return();
        Assert.Same(large, scratch.HeldPickleForTest);
    }

    [Fact]
    public void A_buffer_past_the_kept_size_goes_to_the_shared_pool_instead()
    {
        var scratch = new SpanWriteScratch();
        var pickler = new PooledLz4Pickler(scratch);
        pickler.Pickle(Corpus("random", SpanWriteScratch.MaxKeptPickleBytes), SpanWriter.FlushLevel);   // the header puts it past the cap
        Assert.True(pickler.BufferForTest!.Length > SpanWriteScratch.MaxKeptPickleBytes);
        pickler.Return();
        Assert.Null(scratch.HeldPickleForTest);
    }

    /// <summary>
    /// ONE OWNER, WHATEVER THE RENT DOES (#144 review 1). Growing the buffer rents the larger array before it
    /// lets go of the one in hand. The other way round, a Rent that threw — the 8–32 MiB a v3 merge's trace
    /// index asks for, in a container at its heap limit — left the pickler pointing at an array it had already
    /// given back, and the writer's finally gave it back a second time, to the engine's scratch: one array with
    /// two owners, and two writers' LZ4 frames in it later with no error anywhere. Through a pool that refuses
    /// the second rental: nothing is let go of on the failed growth, and after Return() the one array rented
    /// came back exactly once.
    /// </summary>
    [Fact]
    public void A_rent_that_throws_on_growth_leaves_the_buffer_with_one_owner()
    {
        var pool    = new RecordingPool { ThrowOnRent = 2 };
        var pickler = new PooledLz4Pickler(scratch: null, pool);
        pickler.Pickle(Corpus("vocabulary", 1_000), SpanWriter.FlushLevel);                   // the first rental
        var first = Assert.Single(pool.Rented);
        Assert.Same(first, pickler.BufferForTest);

        Assert.Throws<PoolExhaustedException>(() => { pickler.Pickle(Corpus("vocabulary", 100_000), SpanWriter.FlushLevel); });
        Assert.Same(first, pickler.BufferForTest);                                             // still the owner
        Assert.Empty(pool.Returned);                                                           // nothing let go of

        pickler.Return();
        Assert.Null(pickler.BufferForTest);
        Assert.Equal([first], pool.Returned, ReferenceEqualityComparer.Instance);              // once, not twice
    }

    /// <summary>Growth lets go of the outgrown array once, Return() of the larger one once: every rental back exactly once.</summary>
    [Fact]
    public void Every_array_rented_comes_back_exactly_once()
    {
        var pool    = new RecordingPool();
        var pickler = new PooledLz4Pickler(scratch: null, pool);
        pickler.Pickle(Corpus("vocabulary", 1_000), SpanWriter.FlushLevel);
        pickler.Pickle(Corpus("vocabulary", 100_000), SpanWriter.FlushLevel);                  // outgrown: a second rental
        Assert.Equal(2, pool.Rented.Count);
        Assert.Equal([pool.Rented[0]], pool.Returned, ReferenceEqualityComparer.Instance);     // the small one, once
        Assert.Same(pool.Rented[1], pickler.BufferForTest);

        pickler.Return();
        pickler.Return();                                                                      // idempotent
        Assert.Equal(pool.Rented, pool.Returned, ReferenceEqualityComparer.Instance);          // each once, in order
    }

    /// <summary>
    /// THE ENGINE'S BUFFER SURVIVES A WRITE THAT OUTGROWS IT (#144 review 2). A v3 merge starts in the buffer
    /// the last flush left in the slot — 2 MiB — and outgrows it at its trace index: count × 4 + traces × 24
    /// bytes, past the kept size from ~700 000 spans of 10-span traces. The outgrown buffer goes back to the
    /// slot there and then, where the next flush finds it; the index's rental, past the kept size, is the one
    /// that goes to the shared pool at the end. Letting go of the outgrown buffer to the shared pool instead
    /// left the slot empty after every large merge, and the next flush — on another thread — paid that LOH
    /// array again, which is the miss the scratch exists for (#90).
    /// </summary>
    [Fact]
    public void A_write_that_outgrows_the_kept_size_leaves_the_engines_buffer_in_the_slot()
    {
        var scratch = new SpanWriteScratch();
        var kept    = ArrayPool<byte>.Shared.Rent(2 << 20);                                   // what an ordinary flush leaves
        scratch.ReturnPickle(kept);
        Assert.Same(kept, scratch.HeldPickleForTest);

        var pickler = new PooledLz4Pickler(scratch);
        pickler.Pickle(Corpus("vocabulary", 1 << 20), SpanWriter.FlushLevel);                  // a span block: fits
        Assert.Same(kept, pickler.BufferForTest);
        pickler.Pickle(Corpus("zeros", 5 << 20), SpanWriter.FlushLevel);                       // the index: past the kept size
        Assert.NotSame(kept, pickler.BufferForTest);
        Assert.Same(kept, scratch.HeldPickleForTest);                                          // back in the slot already

        pickler.Return();
        Assert.Same(kept, scratch.HeldPickleForTest);                                          // the index's rental did not displace it
    }

    /// <summary>A pool that remembers every array it handed out and took back, and can refuse a rental.</summary>
    private sealed class RecordingPool : ArrayPool<byte>
    {
        public readonly List<byte[]> Rented   = [];
        public readonly List<byte[]> Returned = [];

        /// <summary>The 1-based rental that throws; none by default.</summary>
        public int ThrowOnRent { get; init; } = int.MaxValue;

        public override byte[] Rent(int minimumLength)
        {
            if (Rented.Count + 1 == ThrowOnRent) throw new PoolExhaustedException();
            var a = new byte[(int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, minimumLength))];   // sized as the shared pool sizes
            Rented.Add(a);
            return a;
        }

        public override void Return(byte[] array, bool clearArray = false) => Returned.Add(array);
    }

    private sealed class PoolExhaustedException : Exception { }

    private static byte[] Corpus(string shape, int bytes)
    {
        var b = new byte[bytes];
        switch (shape)
        {
            case "zeros":
                return b;
            case "byte":
                b[0] = 42;
                return b;
            case "random":
                new Random(bytes).NextBytes(b);
                return b;
            case "repetitive":
                for (int i = 0; i < bytes; i++) b[i] = (byte)"wallet-api"[i % 10];
                return b;
            case "mixed":
                // Random bytes LZ4 cannot shrink, then a repetitive tail it can: the whole shrinks by about
                // the tail, which is the two-byte difference on a four-byte length this shape is for.
                new Random(bytes).NextBytes(b.AsSpan(0, bytes - 10_000));
                for (int i = bytes - 10_000; i < bytes; i++) b[i] = (byte)"wallet-api"[i % 10];
                return b;
            default:
            {
                // msgpack-shaped: a small vocabulary of names with a nibble of variation between them
                var      r     = new Random(bytes);
                string[] words = ["wallet-api", "GET /api/v1/orders", "SqlClient.Execute", "http.request.method", "url.path", "service.name"];
                int pos = 0;
                while (pos < bytes)
                {
                    foreach (char c in words[r.Next(words.Length)]) { if (pos >= bytes) break; b[pos++] = (byte)c; }
                    if (pos < bytes) b[pos++] = (byte)r.Next(0, 16);
                }
                return b;
            }
        }
    }
}
