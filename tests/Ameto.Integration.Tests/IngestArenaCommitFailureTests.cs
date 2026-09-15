using Ameto.Core;
using Ameto.Ingestion;

namespace Ameto.Integration.Tests;

/// <summary>
/// The ingest payload arena is committed as the buffer grows into it, so on Windows a commit
/// can fail at runtime when the host runs out of commit charge. It used to throw
/// OutOfMemoryException out of TryEnqueue — into the ingest HTTP handler, after part of the
/// batch was already in the ring — and the slab it had taken off the free list was never put
/// back, so every failure also shrank the arena for the life of the process.
///
/// <para>The failure is injected through a test hook that makes every commit past the arena's
/// high-water mark fail, which works on every platform (on a plain allocation the hook lowers
/// the mark to zero while it is set).</para>
/// </summary>
public sealed class IngestArenaCommitFailureTests
{
    private const int SlabBytes = 64 * 1024;
    private const int Slabs     = 64;

    private static bool Enqueue(IngestionRingBuffer ring, byte seed)
    {
        Span<byte> payload = stackalloc byte[256];
        payload.Fill(seed);
        return ring.TryEnqueue(DateTimeOffset.UtcNow.UtcTicks, (byte)LogLevel.Information, 0, "t", null, payload);
    }

    [Fact]
    public void A_failed_commit_is_a_counted_refusal_not_an_exception()
    {
        using var ring = new IngestionRingBuffer(1 << 10, SlabBytes, (long)Slabs * SlabBytes);
        Assert.Equal(Slabs, ring.SlabCapacity);

        ring.SimulateArenaCommitFailure(true);
        int refused = 0;
        for (int i = 0; i < 3 * Slabs; i++)
        {
            // An exception here fails the test by itself: that is the HTTP 500 this pins.
            if (!Enqueue(ring, (byte)i)) refused++;
        }

        Assert.Equal(3 * Slabs, refused);
        Assert.Equal(3 * Slabs, ring.DroppedNoCommit);
        Assert.Equal(0, ring.DroppedNoSlab);      // the arena was not full, the host was
        Assert.Equal(0, ring.AcceptedTotal);
    }

    /// <summary>
    /// Once the pressure is over, every slab must still be there. Before the fix each failed
    /// commit leaked the slab it had popped, so 3x the capacity in failures left the arena
    /// with none — back-pressure from then on, silently, for the life of the process.
    /// </summary>
    [Fact]
    public void Every_slab_is_usable_again_once_commits_succeed()
    {
        using var ring = new IngestionRingBuffer(1 << 10, SlabBytes, (long)Slabs * SlabBytes);

        ring.SimulateArenaCommitFailure(true);
        for (int i = 0; i < 3 * Slabs; i++) Enqueue(ring, (byte)i);
        ring.SimulateArenaCommitFailure(false);

        int accepted = 0;
        for (int i = 0; i < Slabs; i++)
            if (Enqueue(ring, (byte)i)) accepted++;

        Assert.Equal(Slabs, accepted);
        Assert.False(Enqueue(ring, 0xFF));         // now it is genuinely full
        Assert.Equal(1, ring.DroppedNoSlab);

        // And the bytes written after recovery round-trip: a slab handed out without its pages
        // would be an access violation, not a wrong byte, but check the contents anyway.
        var buf = new byte[SlabBytes];
        for (int i = 0; i < Slabs; i++)
        {
            Assert.True(ring.TryDequeue(out _, out _, out _, out _, out _, buf, out int len,
                                        out _, out _, out _, out _));
            Assert.Equal(256, len);
            Assert.All(buf.AsSpan(0, len).ToArray(), b => Assert.Equal((byte)i, b));
        }
    }

    /// <summary>
    /// A plain allocation (the arena everywhere but Windows) commits nothing on demand and counts
    /// nothing, so it reports -1 — not its whole size as if it were memory in use, which would show
    /// every idle Linux container holding 512 MB of arena.
    /// </summary>
    [Fact]
    public void A_plain_arena_reports_no_commit_figure_rather_than_its_size()
    {
        using var plain = SlabArena.Create((nuint)(Slabs * SlabBytes), 1 << 20, reserve: false);
        Assert.False(plain.IsCommitOnDemand);
        Assert.Equal(-1, plain.CommittedBytes);

        using var ring = new IngestionRingBuffer(1 << 10, SlabBytes, (long)Slabs * SlabBytes);
        Assert.Equal(ring.ArenaCommitsOnDemand ? 0 : -1, ring.ArenaCommittedBytes);
    }
}
