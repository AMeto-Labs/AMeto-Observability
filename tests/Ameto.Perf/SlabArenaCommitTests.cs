using Ameto.Core;
using Ameto.Ingestion;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// The ingest payload arena is sized to the back-pressure ceiling — 512 MB by default — on the
/// stated assumption that it is reserved address space whose pages fault in on demand. That held
/// on Linux, where a large NativeMemory.Alloc is an anonymous mapping, and not on Windows, where
/// a block that size is VirtualAlloc(MEM_COMMIT): the server took the whole commit charge at
/// startup, before a single event had arrived.
///
/// <para>These pin the property that replaces it — the arena is paid for as the buffer grows into
/// it — and, more importantly, that payloads still round-trip byte for byte, because a slab handed
/// out before its pages exist is an access violation, not a slow path.</para>
/// </summary>
public sealed class SlabArenaCommitTests
{
    private const long MB = 1024 * 1024;

    private readonly ITestOutputHelper _out;
    public SlabArenaCommitTests(ITestOutputHelper output) => _out = output;

    private static byte[] Payload(int len, byte seed)
    {
        var p = new byte[len];
        for (int i = 0; i < len; i++) p[i] = (byte)(seed + i);
        return p;
    }

    private static bool Enqueue(IngestionRingBuffer ring, byte[] payload) =>
        ring.TryEnqueue(DateTimeOffset.UtcNow.UtcTicks, (byte)LogLevel.Information, 0, "t", null, payload);

    /// <summary>Drains everything and returns the payloads in order.</summary>
    private static List<byte[]> DrainAll(IngestionRingBuffer ring, int maxPayload)
    {
        var got = new List<byte[]>();
        var buf = new byte[maxPayload];
        while (ring.TryDequeue(out _, out _, out _, out _, out _, buf, out int len,
                               out _, out _, out _, out _))
            got.Add(buf.AsSpan(0, len).ToArray());
        return got;
    }

    [Fact]
    public void A_fresh_arena_commits_nothing()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 256 * MB);
        _out.WriteLine($"commit-on-demand={ring.ArenaCommitsOnDemand} committed={ring.ArenaCommittedBytes:N0} B " +
                       $"slabs={ring.SlabCapacity}");

        Assert.True(ring.SlabCapacity > 1000, "the arena must still OFFER its full capacity");
        if (!ring.ArenaCommitsOnDemand) return;   // platforms where the pages were already lazy

        Assert.Equal(0, ring.ArenaCommittedBytes);
    }

    [Fact]
    public void Commit_tracks_the_high_water_mark_not_the_ceiling()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 256 * MB);
        long ceiling = (long)ring.SlabCapacity * 64 * 1024;

        for (int i = 0; i < 64; i++) Assert.True(Enqueue(ring, Payload(100, (byte)i)));
        _out.WriteLine($"64 small events: committed {ring.ArenaCommittedBytes:N0} B of a {ceiling:N0} B ceiling");

        if (!ring.ArenaCommitsOnDemand) return;

        Assert.True(ring.ArenaCommittedBytes > 0, "64 enqueued payloads must have committed something");
        Assert.True(ring.ArenaCommittedBytes < ceiling / 4,
            $"committed {ring.ArenaCommittedBytes} of {ceiling} for 64 small events");
    }

    /// <summary>
    /// The failure this change could introduce is a slab handed out before its pages exist, so
    /// the bytes have to come back exactly — across enough events to cross several commit chunks.
    /// </summary>
    [Fact]
    public void Payloads_round_trip_across_many_commit_chunks()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 256 * MB);

        const int n = 600;
        for (int i = 0; i < n; i++)
            Assert.True(Enqueue(ring, Payload(1024, (byte)i)), $"enqueue {i} was refused");

        var got = DrainAll(ring, 64 * 1024);
        Assert.Equal(n, got.Count);
        for (int i = 0; i < n; i++) Assert.Equal(Payload(1024, (byte)i), got[i]);
    }

    /// <summary>
    /// A LIFO free list re-hands the slabs it already committed, so draining and refilling must
    /// not walk any deeper into the arena.
    /// </summary>
    [Fact]
    public void Reuse_does_not_grow_the_commit()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 256 * MB);

        for (int i = 0; i < 200; i++) Enqueue(ring, Payload(512, (byte)i));
        DrainAll(ring, 64 * 1024);
        long afterFirstBurst = ring.ArenaCommittedBytes;

        for (int round = 0; round < 10; round++)
        {
            for (int i = 0; i < 200; i++) Enqueue(ring, Payload(512, (byte)i));
            DrainAll(ring, 64 * 1024);
        }

        Assert.Equal(afterFirstBurst, ring.ArenaCommittedBytes);
    }

    /// <summary>
    /// Every slab is still reachable and back-pressure still arrives where it did: the
    /// reservation must bound nothing the committed allocation did not.
    /// </summary>
    [Fact]
    public void The_whole_arena_is_still_usable_and_back_pressure_is_unchanged()
    {
        using var ring = new IngestionRingBuffer(1 << 12, 64 * 1024, 16L * 64 * 1024);
        Assert.Equal(16, ring.SlabCapacity);

        for (int i = 0; i < 16; i++)
            Assert.True(Enqueue(ring, Payload(64 * 1024, (byte)i)), $"slab {i} should be available");

        Assert.False(Enqueue(ring, Payload(64 * 1024, 99)));     // no slab left: refused, not crashed
        Assert.Equal(1, ring.DroppedNoSlab);

        var got = DrainAll(ring, 64 * 1024);
        Assert.Equal(16, got.Count);
        for (int i = 0; i < 16; i++) Assert.Equal(Payload(64 * 1024, (byte)i), got[i]);
    }

    /// <summary>
    /// Concurrent producers cross the commit boundary together — the growth path is the only
    /// place in this buffer that takes a lock, and it must be idempotent under a race.
    /// </summary>
    [Fact]
    public void Concurrent_producers_crossing_a_commit_boundary_lose_nothing()
    {
        using var ring = new IngestionRingBuffer(1 << 14, 64 * 1024, 256 * MB);

        const int threads = 8, each = 200;
        var start = new Barrier(threads);
        int accepted = 0;
        var workers = new Thread[threads];
        for (int t = 0; t < threads; t++)
        {
            int id = t;
            workers[t] = new Thread(() =>
            {
                var payload = Payload(4096, (byte)id);
                start.SignalAndWait();
                for (int i = 0; i < each; i++)
                    if (Enqueue(ring, payload)) Interlocked.Increment(ref accepted);
            });
            workers[t].Start();
        }
        foreach (var w in workers) w.Join();

        var got = DrainAll(ring, 64 * 1024);
        _out.WriteLine($"{threads}x{each}: accepted {accepted}, drained {got.Count}, " +
                       $"committed {ring.ArenaCommittedBytes:N0} B");

        Assert.Equal(accepted, got.Count);
        foreach (var p in got)
        {
            Assert.Equal(4096, p.Length);
            Assert.Equal(Payload(4096, p[0]), p);        // the seed is the first byte
        }
    }
}
