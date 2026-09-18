using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What the ingest ring costs a process the moment it is constructed, in PRIVATE bytes — the
/// number that exposes an over-committed native arena, where the working set hides it because
/// nothing has touched the pages yet.
///
/// <para>Prints rather than asserts a size: commit charge on a shared CI box moves for reasons
/// that have nothing to do with this buffer. The assertion is the shape — constructing a
/// half-gigabyte arena must not cost a half-gigabyte of private bytes.</para>
/// </summary>
public sealed class IngestArenaCommitProbe
{
    private const long MB = 1024 * 1024;

    private readonly ITestOutputHelper _out;
    public IngestArenaCommitProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Constructing_the_default_ring_does_not_commit_the_ceiling()
    {
        long before = ProcessMemoryInfo.PrivateBytes;

        using var ring = new IngestionRingBuffer();          // 65536 slots, 512 MB arena ceiling
        long afterCtor = ProcessMemoryInfo.PrivateBytes;

        // A burst the drainer never gets to: 2000 events of 4 KB walks the arena's first slabs.
        var payload = new byte[4096];
        for (int i = 0; i < 2000; i++)
            ring.TryEnqueue(DateTimeOffset.UtcNow.UtcTicks, (byte)LogLevel.Information, 0, "t", null, payload);
        long afterBurst = ProcessMemoryInfo.PrivateBytes;

        long arenaCeiling = (long)ring.SlabCapacity * 64 * 1024;

        _out.WriteLine($"commit-on-demand : {ring.ArenaCommitsOnDemand}");
        _out.WriteLine($"arena ceiling    : {arenaCeiling / MB} MB ({ring.SlabCapacity} slabs)");
        _out.WriteLine($"private bytes    : before {before / MB} MB -> ctor {afterCtor / MB} MB " +
                       $"-> after 2000x4 KB {afterBurst / MB} MB");
        _out.WriteLine($"arena committed  : {ring.ArenaCommittedBytes / MB} MB " +
                       $"({ring.ArenaCommittedBytes:N0} B)");

        Assert.True(arenaCeiling >= 256 * MB, "this probe is about a large arena");

        if (ring.ArenaCommitsOnDemand)
        {
            // The whole point: the construction cost is the ring slots and the free list, not
            // the payload ceiling.
            Assert.True(afterCtor - before < arenaCeiling / 4,
                $"constructing the ring added {(afterCtor - before) / MB} MB of private bytes " +
                $"against a {arenaCeiling / MB} MB arena ceiling");
            Assert.True(ring.ArenaCommittedBytes < arenaCeiling / 4,
                "a 8 MB burst must not commit a quarter of the arena");
        }
    }
}
