using System.Buffers;
using System.Buffers.Binary;
using Ameto.Core;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// <see cref="SegmentReader.ReadBlockInto"/> grows the caller's block buffer, and the caller is
/// <c>SegmentEventCursor._block</c> — a field handed over by <c>ref</c> and returned to the pool
/// by that cursor's own <c>Dispose</c>. So the order of the two pool calls inside the grow is the
/// whole safety property: return first and the window between the return and the reassignment is
/// one in which the cursor still names an array that is already back in the pool. A throw from the
/// rent in that window (a large block under memory pressure is the realistic way) leaves the
/// cursor to return it a SECOND time.
///
/// <para>A double return does not fault. The pool simply stacks the array twice and later leases
/// it to two renters at once, so the damage surfaces somewhere else entirely — one reader's block
/// bytes appearing inside an unrelated reader's — with nothing linking it back here. That is why
/// it is worth a test rather than a comment: the correct order was restored with no coverage at
/// all, and reverting it left the whole solution green.</para>
///
/// <para>The poisoned size word that once drove <c>Rent</c> to throw is now intercepted a
/// step earlier: <c>ValidateBlockFrame</c> names it corruption before the pool is ever
/// consulted. What this test pins is therefore the STRONGER form of the same property — a
/// poisoned frame reaches neither the return nor the rent, so ownership cannot move and the
/// pool cannot be handed the caller's array. The rent-before-return ordering inside the grow
/// remains for the one thrower validation cannot rule out (a genuine allocation failure on a
/// frame that is large but honest) and rests on the code comment beside it — a file small
/// enough for a test cannot carry an honest frame big enough to fail deterministically. The
/// identical ordering in <c>OtlpLogStreamParser.CopyStringGrow</c> is in the same position.</para>
/// </summary>
public sealed class SegmentBlockBufferGrowthTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-blockbuf-" + Guid.NewGuid().ToString("N"));

    public SegmentBlockBufferGrowthTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>Bucket the probe and the stand-in cursor buffer share.</summary>
    private const int BlockBufferSize = 4096;

    [Fact]
    public void A_failed_grow_leaves_the_cursors_block_buffer_out_of_the_pool()
    {
        // An HONEST file: its first block is a real ~64 KB frame the validator passes, and it
        // is larger than the stand-in buffer below, so ReadBlockInto genuinely enters the
        // grow. The rent inside the grow is then failed through the seam -- the only thrower
        // left, now that the frame validator intercepts every poisoned size before the pool is
        // consulted. (This test's previous vehicle WAS a poisoned size; when the validator
        // arrived, the test kept passing while a return-before-rent mutation survived it --
        // the ordering was covered by nothing.)
        string path = WriteSegment(600);

        var pool = ArrayPool<byte>.Shared;

        // Precondition, not the assertion under test. The pool parks a bucket's most recently
        // returned array in a thread-local slot that the next Rent of that bucket drains first,
        // which is what makes "the pool got this array back" observable at all. Asserted so that
        // a runtime which stopped behaving that way fails HERE rather than passing the real
        // assertion below for a reason that has nothing to do with the code under test.
        byte[] probe = pool.Rent(BlockBufferSize);
        pool.Return(probe);
        Assert.Same(probe, pool.Rent(BlockBufferSize));

        // The rent above left the slot empty and handed us the array. It now stands in for
        // SegmentEventCursor._block: rented, live, owned by the caller across the grow.
        byte[] block = probe;

        using (var reader = SegmentReader.Open(path))
        {
            reader._rentBlockBuffer = static _ => throw new OutOfMemoryException(
                "seam: the grow's rent fails here, as a real allocation failure would");
            Assert.Throws<OutOfMemoryException>(() => { reader.ReadBlockInto(0, ref block); });
        }

        // The grow did not complete, so ownership never moved.
        Assert.Same(probe, block);

        // And therefore the pool must not be holding it either. Returning before renting puts it
        // back in that thread-local slot on the way to the throw, and this rent takes it straight
        // out again — the pool leasing an array its previous owner is still going to return.
        byte[] next = pool.Rent(BlockBufferSize);
        Assert.False(ReferenceEquals(block, next),
            "ReadBlockInto returned the caller's buffer before the rent that failed: the pool has " +
            "leased it to a second renter while SegmentEventCursor still holds it and will return " +
            "it again from Dispose");

        pool.Return(next);
        pool.Return(block);   // the cursor's Dispose, and the only return this array gets
    }

    /// <summary>
    /// Rewrites one block's uncompressed-size word. Walks in from the footer the way the reader
    /// does: 44-byte footer, five int64 slots then the magic, slot 3 naming the block index; the
    /// index is a uint32 count followed by 20-byte entries (since v5) that open with the block's
    /// file offset; a block frame opens with uint32 uncompressed, uint32 compressed.
    /// </summary>
    private static void PatchBlockUncompressedSize(string path, int block, int value)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        Span<byte> slot = stackalloc byte[8];

        fs.Seek(fs.Length - 44 + 24, SeekOrigin.Begin);
        fs.ReadExactly(slot);
        long blockIndexOffset = BinaryPrimitives.ReadInt64LittleEndian(slot);

        fs.Seek(blockIndexOffset + 4 + block * 20, SeekOrigin.Begin);
        fs.ReadExactly(slot);
        long blockOffset = BinaryPrimitives.ReadInt64LittleEndian(slot);

        Span<byte> word = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(word, value);
        fs.Seek(blockOffset, SeekOrigin.Begin);
        fs.Write(word);
    }

    private string WriteSegment(int events)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(events + 1, (long)events * 512 + (8L << 20));
        int tmpl = pool.Intern("block buffer growth {n}");
        int svc  = pool.Intern("Svc.Growth");
        long baseTicks = new DateTimeOffset(2026, 8, 5, 12, 0, 0, TimeSpan.Zero).UtcTicks;

        for (int i = 0; i < events; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = (ulong)(i + 1),
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmpl,
                ServiceNamePoolIndex     = svc,
            }, [], "block buffer growth {n}", null));
        hot.Freeze();

        string path = Path.Combine(_dir, "0-1-0-0.seg");
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool);
        writer.Finalise(new NodeId(0), new SegmentId(1));
        return path;
    }
}
