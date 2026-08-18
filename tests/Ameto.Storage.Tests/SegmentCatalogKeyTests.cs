using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Segment ids are monotonic PER NODE and every node's counter starts at 1, so as soon as a peer
/// replicates anything the two series overlap — collision is the normal state of a cluster, not an
/// edge case. The cold catalog was keyed by the id alone, and both writers into it assign
/// unconditionally, so the second registration EVICTED the first.
///
/// <para>The file did not go anywhere: the names cannot collide (a replica is
/// <c>{node}-{id}.seg</c>, a locally written segment <c>{node}-{id}-{minTs}-{maxTs}.seg</c>), so
/// both stayed on disk while only one was in the catalog. Queries, retention and the merge planner
/// all read the catalog rather than the directory, so the evicted file left all three at once: it
/// was never served, never expired and never compacted, and held disk for the life of the install
/// with nothing logged. <c>LoadSegmentCatalog</c> re-ran the collision on every restart in
/// directory-enumeration order, so which of the two won could change from boot to boot.</para>
/// </summary>
public sealed class SegmentCatalogKeyTests : IAsyncLifetime
{
    /// <summary>A real peer id. The engine under test is NodeId 0, the default.</summary>
    private static readonly NodeId Peer = new(7);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-segkey-" + Guid.NewGuid().ToString("N"));
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
        try { await _engine.DisposeAsync(); } catch { }
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

    private void Write(int count, long baseTicks, LogLevel level = LogLevel.Information)
    {
        for (int i = 0; i < count; i++)
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = level,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("local {n}"),
            }, Props(i)));
    }

    /// <summary>
    /// Writes a replicated segment file exactly as the replication endpoint leaves one: named
    /// <c>{node}-{id}.seg</c>, two name segments, with the peer's own node id in the header —
    /// which is what makes a (node, id) key able to tell it apart from a local segment at all.
    /// </summary>
    private string WritePeerSegment(NodeId node, ulong segId, long baseTicks, int events = 4,
                                    LogLevel level = LogLevel.Information)
    {
        const string Template = "peer {n}";
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(16, 1L << 20);
        for (int i = 0; i < events; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(node.Value, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = level,
                MessageTemplatePoolIndex = pool.Intern(Template),
            }, Props(i), Template));
        hot.Freeze();

        Directory.CreateDirectory(SegDir);
        string path = Path.Combine(SegDir, $"{node.Value}-{segId}.seg");
        using (var writer = new SegmentWriter(path))
        {
            writer.WriteEvents(hot, pool);
            writer.Finalise(node, new SegmentId(segId));
        }
        return path;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The collision itself. A peer's segment carrying an id this node has already written must
    /// not displace the local one — both files exist, so both must be in the catalog, which is
    /// the only thing queries, retention and the merge planner ever consult.
    /// </summary>
    [Fact]
    public async Task A_peer_segment_sharing_a_local_id_does_not_evict_it()
    {
        long now = DateTime.UtcNow.Ticks;
        Write(20, now);
        await _engine.FlushHotTierAsync();

        var local = Assert.Single(_engine.ListSegments());
        ulong shared = local.Id.Value;

        _engine.ImportSegment(WritePeerSegment(Peer, shared, now));

        var all = _engine.ListSegments();
        Assert.Equal(2, all.Count);
        Assert.Contains(all, s => s.NodeId.Value == Peer.Value && s.Id.Value == shared);
        Assert.Contains(all, s => s.NodeId.Value == NodeId.Local.Value && s.Id.Value == shared
                                                           && s.FilePath == local.FilePath);

        // And through the reader path a query actually uses, not just the raw list.
        var window = _engine.GetSegments(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5));
        Assert.Equal(2, window.Count(s => s.Id.Value == shared));
    }

    /// <summary>
    /// What the eviction actually cost, measured rather than argued: retention enumerates the
    /// catalog, so a segment missing from it is never expired. Its bytes then sit in the segments
    /// directory for the life of the install, past every TTL, with nothing logged anywhere.
    /// </summary>
    [Fact]
    public async Task Retention_expires_both_segments_and_leaves_no_file_behind()
    {
        // Well past the 90-day Information TTL, so both are expired the moment retention runs.
        long old = DateTime.UtcNow.AddDays(-200).Ticks;
        Write(20, old);
        await _engine.FlushHotTierAsync();

        var local = Assert.Single(_engine.ListSegments());
        string peerPath = WritePeerSegment(Peer, local.Id.Value, old);
        _engine.ImportSegment(peerPath);

        var result = await _engine.EnforceRetentionAsync();

        Assert.Equal(2, result.DeletedSegments);
        Assert.Empty(_engine.ListSegments());
        Assert.False(File.Exists(local.FilePath), "the local segment's file survived retention");
        Assert.False(File.Exists(peerPath),       "the peer segment's file survived retention");
        Assert.Empty(Directory.GetFiles(SegDir, "*.seg"));
    }

    /// <summary>
    /// A consumer the write-up does not name, and the one that loses events rather than disk. A
    /// flush reserves a block of <c>LevelSegmentSlots</c> ids for the tier it froze and publishes
    /// them as COVERED so a query does not serve the freshly registered per-level segments and the
    /// still-frozen tier both. That set was matched against segment ids alone — so a replicated
    /// peer segment whose own counter happened to land inside the local block was skipped by every
    /// query for the whole duration of the flush. Perfectly readable file, events silently absent
    /// from results.
    ///
    /// <para>Driven at the publish seam, so the tier is provably still frozen while the count is
    /// taken: <c>_afterLevelPublished</c> fires after a level's file is moved into place and
    /// before the frozen tier is dropped from the list the covered set is built from.</para>
    /// </summary>
    [Fact]
    public async Task A_peer_segment_inside_the_live_flush_block_is_still_served()
    {
        long now = DateTime.UtcNow.Ticks;

        // Inside the block the NEXT flush will reserve — the collision that hides a peer's file
        // behind this node's flush.
        ulong inBlock = _engine.LiveWalSegmentId + 2;
        string peerPath = WritePeerSegment(Peer, inBlock, now, events: 4);
        _engine.ImportSegment(peerPath);

        Write(20, now);

        var reached  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        long counted = -1;

        _engine._afterLevelPublished = _ =>
        {
            if (!reached.TrySetResult()) return;
            release.Task.GetAwaiter().GetResult();
        };

        var flushing = Task.Run(() => _engine.FlushHotTierAsync());
        await reached.Task;

        // The tier is frozen, its id block is covered, and the peer's segment shares one of those
        // ids. Its four events must still be counted.
        var counts = await _engine.AggregateLogVolumeAsync(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5),
            minBucket: 0, bucketSeconds: 60, nBuckets: 60, serviceFilter: null);
        counted = counts.Total;

        release.SetResult();
        await flushing;

        // 20 local (in the frozen tier) + 4 peer (in the cold segment the covered set was hiding).
        Assert.Equal(24, counted);
    }
}
