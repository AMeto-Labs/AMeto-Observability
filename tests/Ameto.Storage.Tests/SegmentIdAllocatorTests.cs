using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Every id a segment file can carry comes from one counter, and the counter has writers on three
/// different thread models: startup recovery, the flush swap and the merge planner (both under the
/// async flush lock), and <c>ImportSegment</c>, which runs on whatever thread the replication
/// endpoint hands it. That last one used to read and write the counter with no lock at all.
///
/// <para>The consequence is a LOST UPDATE, not a torn read: an import that raises the counter
/// between a flush's read of it and its write is simply overwritten, and the allocator moves
/// backwards over ids that are already on disk. What follows from that is a level block overlapping
/// an imported segment — two files, one id, one catalog slot keyed by it, and whichever registers
/// second evicts the other out of every query and out of retention's enumeration, leaving its bytes
/// on disk forever.</para>
/// </summary>
public sealed class SegmentIdAllocatorTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-segid-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;

    private string SegDir => Path.Combine(_dir, "segments");

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = NewEngine();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private StorageEngine NewEngine() => new(
        Options.Create(new ServerOptions { DataDirectory = _dir }),
        new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance);

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(48);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private void Write(int count, long baseTicks)
    {
        for (int i = 0; i < count; i++)
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
            }, Props(i)));
    }

    /// <summary>
    /// Writes a segment file carrying <paramref name="segId"/>, named the way the replication
    /// endpoint names what it receives — <c>{node}-{id}.seg</c>, with no timestamp range. That
    /// two-part shape matters on its own: it is why an imported id colliding with a flushed one
    /// never trips <c>File.Move(overwrite: false)</c> and never satisfies the
    /// <c>{node}-{id}-*.seg</c> probe recovery uses, so the collision stays silent.
    /// </summary>
    private string WritePeerSegment(ulong segId)
    {
        const string Template = "peer {n}";
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(8, 1L << 20);
        for (int i = 0; i < 4; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(1u, (uint)i).RawValue,
                TimestampUtcTicks        = DateTime.UtcNow.Ticks + i,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = pool.Intern(Template),
            }, Props(i), Template));
        hot.Freeze();

        string path = Path.Combine(SegDir, $"{NodeId.Local.Value}-{segId}.seg");
        using (var writer = new SegmentWriter(path))
        {
            writer.WriteEvents(hot, pool);
            writer.Finalise(NodeId.Local, new SegmentId(segId));
        }
        return path;
    }

    /// <summary>Ids parsed out of the FOUR-part names, i.e. the files this node wrote itself.</summary>
    private HashSet<ulong> LocallyWrittenIds()
    {
        var ids = new HashSet<ulong>();
        foreach (var f in Directory.GetFiles(SegDir, "*.seg"))
        {
            var parts = Path.GetFileNameWithoutExtension(f).Split('-');
            if (parts.Length >= 4 && ulong.TryParse(parts[1], out var id)) ids.Add(id);
        }
        return ids;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The floor semantics, unchanged by the move to <c>AdvanceSegmentIdFloor</c>: an imported id
    /// above the allocator pushes it past, one below leaves it alone. Both directions, because the
    /// helper replaced two differently-spelled comparisons (<c>&gt;=</c> then <c>+1</c> in the
    /// import path, <c>&gt;</c> in the WAL-block path) and collapsing them must not have quietly
    /// changed either.
    /// </summary>
    [Fact]
    public async Task An_import_moves_the_allocator_forward_and_never_backward()
    {
        ulong before = _engine.LiveWalSegmentId;

        _engine.ImportSegment(WritePeerSegment(before + 1000));
        Write(20, DateTime.UtcNow.Ticks);
        await _engine.FlushHotTierAsync();

        ulong afterHigh = _engine.LiveWalSegmentId;
        Assert.True(afterHigh > before + 1000,
            $"live WAL block {afterHigh} did not clear the imported id {before + 1000}");
        foreach (var id in LocallyWrittenIds())
            Assert.NotEqual(before + 1000, id);

        // An id the allocator has already passed must not drag it back.
        _engine.ImportSegment(WritePeerSegment(1));
        Write(20, DateTime.UtcNow.Ticks);
        await _engine.FlushHotTierAsync();

        Assert.True(_engine.LiveWalSegmentId >= afterHigh,
            $"allocator went backwards: {afterHigh} → {_engine.LiveWalSegmentId}");
    }

    /// <summary>
    /// The race, at the counter rather than through the callers. The flush path takes level blocks
    /// and the import path raises the floor; running both from several threads at once is what the
    /// replication endpoint does to a busy node, and the two invariants below are the ones the
    /// whole scheme rests on: an id is handed out ONCE, and a floor the allocator has accepted is
    /// never forgotten.
    ///
    /// <para>Driven directly and not through <see cref="StorageEngine.ImportSegment"/> on purpose.
    /// That path opens and reads a segment file before it reaches the counter, which is orders of
    /// magnitude longer than the read-modify-write it would have to land inside — an end-to-end
    /// version of this test passed repeatedly with the lock removed, so it proved nothing. Here the
    /// unsynchronised counter loses updates on the first run.</para>
    /// </summary>
    [Fact]
    public void Concurrent_reservations_never_reuse_an_id_or_forget_a_floor()
    {
        const int Threads    = 8;
        const int Iterations = 4_000;
        const int BlockWidth = 6;      // LevelSegmentSlots — one segment per level

        var allocated = new ulong[Threads][];
        ulong floorReached = 0;

        var workers = new Thread[Threads];
        for (int t = 0; t < Threads; t++)
        {
            int worker = t;
            allocated[worker] = new ulong[Iterations];
            workers[worker] = new Thread(() =>
            {
                for (int i = 0; i < Iterations; i++)
                {
                    allocated[worker][i] = _engine.AllocateSegmentIdBlock();

                    // Half the threads also behave like an import arriving mid-flight, raising the
                    // floor above whatever the counter has reached.
                    if ((worker & 1) == 0)
                    {
                        ulong floor = allocated[worker][i] + BlockWidth + 1;
                        _engine.AdvanceSegmentIdFloor(floor);
                        if (floor > Volatile.Read(ref floorReached)) Volatile.Write(ref floorReached, floor);
                    }
                }
            });
        }

        foreach (var w in workers) w.Start();
        foreach (var w in workers) w.Join();

        // No id may appear in two blocks. Checking the block STARTS is not enough: two overlapping
        // blocks can start at different ids and still share the four in between, which is exactly
        // the shape a lost update produces.
        var seen = new HashSet<ulong>();
        foreach (var perThread in allocated)
            foreach (ulong start in perThread)
                for (ulong id = start; id < start + BlockWidth; id++)
                    Assert.True(seen.Add(id), $"segment id {id} was reserved twice (block at {start})");

        // And the counter still stands above every floor it accepted — no accepted advance was
        // overwritten by a reservation that had already read the old value.
        ulong next = _engine.AllocateSegmentId();
        Assert.True(next >= Volatile.Read(ref floorReached),
            $"allocator handed out {next} after accepting a floor of {floorReached}");
    }
}
