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
    /// A failing commit must not bury the committed slabs the drainer frees while it fails. The
    /// first fix popped a slab, tried to commit it, and on failure pushed it back on top, so a
    /// committed slab freed during that window ended up UNDER the uncommitted one. The ring then
    /// refused events although committed slabs were free. Under sustained commit exhaustion every
    /// slab freed in such a window was buried, and a server that already had hundreds of MB
    /// committed refused nearly everything until the host recovered.
    ///
    /// <para>The producer is held inside its failing commit while the test frees a slab. The ring
    /// is then drained, and, with commits still failing, it must take one event per committed slab.
    /// Measured before the fix: 15 of 16.</para>
    /// </summary>
    [Fact]
    public void A_slab_freed_while_a_commit_is_failing_is_not_buried_under_the_uncommitted_one()
    {
        const int Committed = 16;                              // one 1 MB commit chunk of 64 KB slabs
        using var ring = new IngestionRingBuffer(1 << 10, SlabBytes, (long)Slabs * SlabBytes);

        for (int i = 0; i < Committed; i++) Assert.True(Enqueue(ring, (byte)i));
        if (ring.ArenaCommitsOnDemand) Assert.Equal((long)Committed * SlabBytes, ring.ArenaCommittedBytes);

        using var parked  = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        int held = 0;
        ring.SimulateArenaCommitFailure(true, plainCommittedBytes: (long)Committed * SlabBytes, onFailure: () =>
        {
            if (Interlocked.Exchange(ref held, 1) != 0) return;   // hold only the first failing commit
            parked.Set();
            release.Wait(TimeSpan.FromSeconds(30));
        });

        // A producer reaches for slab 16, the first uncommitted one, and is held inside the commit.
        bool producerAccepted = false;
        var producer = new Thread(() => producerAccepted = Enqueue(ring, 0xAA)) { IsBackground = true };
        producer.Start();
        Assert.True(parked.Wait(TimeSpan.FromSeconds(30)), "the producer never reached the failing commit");

        // Meanwhile the drainer frees committed slab 0.
        var buf = new byte[SlabBytes];
        Assert.True(Dequeue(ring, buf));
        release.Set();
        Assert.True(producer.Join(TimeSpan.FromSeconds(30)), "the producer never returned");

        while (Dequeue(ring, buf)) { }
        long refusedBeforeRefill = ring.DroppedNoCommit;

        // The ring is empty and every committed slab is free. Commits still fail, so a committed
        // slab buried under an uncommitted one shows up here as a refusal.
        int accepted = 0;
        for (int i = 0; i < Committed; i++)
            if (Enqueue(ring, (byte)i)) accepted++;
        Assert.Equal(Committed, accepted);

        // The next event is the real commit boundary: refused for commit, not for want of a slab.
        Assert.False(Enqueue(ring, 0xFF));
        Assert.Equal(refusedBeforeRefill + 1, ring.DroppedNoCommit);
        Assert.Equal(0, ring.DroppedNoSlab);

        // Slab 0 was already free when the producer's commit failed, so the producer takes it
        // instead of counting a commit refusal.
        Assert.True(producerAccepted, "the producer was refused although a committed slab was free");
        Assert.Equal(0, refusedBeforeRefill);
    }

    private static bool Dequeue(IngestionRingBuffer ring, byte[] buf) =>
        ring.TryDequeue(out _, out _, out _, out _, out _, buf, out _, out _, out _, out _, out _);

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
