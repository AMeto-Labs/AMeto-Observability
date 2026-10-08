using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// The segment-id allocator restarts from what is on disk, and only two things there say how far
/// it got: segment file NAMES, and the block each orphaned log WAL names in its header — empty
/// logs included, which the replay reserves before it deletes them. The live log a clean stop
/// leaves behind is empty: the boot log of a run that took no event, or the successor its final
/// flush opened. It is therefore the only record of the last block that run handed out, and the
/// next start carries the high-water mark forward from it, one empty log to the next.
///
/// <para>A clean stop that deleted that log (2f15c92, reverted) let the next start seed from the
/// file names alone and hand the same ids out again. Harmless while every earlier id still has its
/// file; but retention empties a quiet node, and then the next segment published under a reused
/// id meets a replica still holding the earlier segment with that id: 409 different-segment,
/// "two nodes appear to be configured with NodeId 0" — a false accusation, and the segment is not
/// replicated.</para>
/// </summary>
public sealed class SegmentIdAcrossRestartTests : IDisposable
{
    private const int Slots = 6;   // StorageEngine.LevelSegmentSlots: a live log's block

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-segid-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort */ }
    }

    private string Dir(string name) => Path.Combine(_root, name);

    private static StorageEngine NewEngine(string dir, uint nodeId = 0)
    {
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions { DataDirectory = dir, NodeId = new NodeId(nodeId) };
        return new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
    }

    private static void Write(StorageEngine engine, long ticks, int count)
    {
        for (int i = 0; i < count; i++)
        {
            var buf = new ArrayBufferWriter<byte>(32);
            var w   = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("n"); w.Write((long)i);
            w.Flush();
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = ticks + i,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
                ServiceNamePoolIndex     = -1,
            }, buf.WrittenSpan, "evt {n}"));
        }
    }

    /// <summary>A run that took no event: its boot log is the whole record of how far it got.</summary>
    [Fact]
    public async Task A_clean_stop_that_took_no_event_does_not_hand_its_block_out_again()
    {
        string dir = Dir("boot");
        ulong handedOut;
        await using (var first = NewEngine(dir))
        {
            await first.CatalogLoaded;
            handedOut = first.LiveWalSegmentId + Slots - 1;     // the last id of its boot block
        }

        await using var next = NewEngine(dir);
        await next.CatalogLoaded;
        Assert.True(next.LiveWalSegmentId > handedOut,
            $"the restart handed out block {next.LiveWalSegmentId} again; the previous run had reached {handedOut}");
        Assert.True(next.AllocateSegmentId() > handedOut);
    }

    /// <summary>
    /// A run that took events: its final flush published them and opened a successor log, which the
    /// stop leaves empty. Its block is above every segment on disk, so the file names alone put the
    /// next start below it.
    /// </summary>
    [Fact]
    public async Task A_clean_stop_after_its_final_flush_does_not_hand_the_successor_block_out_again()
    {
        string dir = Dir("successor");
        ulong flushedBlock, handedOut;
        await using (var first = NewEngine(dir))
        {
            await first.CatalogLoaded;
            flushedBlock = first.LiveWalSegmentId;
            Write(first, DateTime.UtcNow.Ticks, 50);
            await first.DisposeAsync();                         // clean stop: the final flush swaps to a successor
            Assert.True(first.LiveWalSegmentId >= flushedBlock + Slots, "the final flush opened no successor");
            handedOut = first.LiveWalSegmentId + Slots - 1;
        }

        await using var next = NewEngine(dir);
        await next.CatalogLoaded;
        Assert.True(next.LiveWalSegmentId > handedOut,
            $"the restart handed out block {next.LiveWalSegmentId} again; the previous run had reached {handedOut}");
    }

    /// <summary>
    /// The reviewer's case, end to end at the engine: node 0 publishes a segment and a replica (node
    /// 7) takes it; retention later removes node 0's own copy; node 0 stops cleanly and starts again,
    /// takes new events and publishes them. The replica must accept that segment. Handed the earlier
    /// segment's id again, it refuses it as ConflictDifferentSegment — the outcome the replication
    /// endpoint answers 409 different-segment for, telling the sender two nodes share its NodeId.
    /// </summary>
    [Fact]
    public async Task A_segment_published_after_a_clean_restart_is_not_refused_by_a_replica_holding_an_earlier_one()
    {
        string nodeDir = Dir("node0"), replicaDir = Dir("replica7");
        long t = DateTime.UtcNow.Ticks;

        await using var replica = NewEngine(replicaDir, nodeId: 7);
        await replica.CatalogLoaded;

        SegmentInfo earlier;
        await using (var node = NewEngine(nodeDir))
        {
            await node.CatalogLoaded;
            Write(node, t, 20);
            await node.FlushHotTierAsync();
            earlier = Assert.Single(node.ListSegments());
            Assert.Equal(SegmentImportOutcome.Registered, Replicate(earlier, replicaDir, replica));

            await node.DeleteSegmentAsync(SegmentKey.Of(earlier));   // what retention does once its data expires
            Assert.Empty(node.ListSegments());
            Assert.False(File.Exists(earlier.FilePath), "the delete was parked: the file names would still hold the id");
        }                                                              // clean stop

        await using var restarted = NewEngine(nodeDir);
        await restarted.CatalogLoaded;
        Write(restarted, t + TimeSpan.TicksPerHour, 30);
        await restarted.FlushHotTierAsync();
        var later = Assert.Single(restarted.ListSegments());

        Assert.Equal(SegmentImportOutcome.Registered, Replicate(later, replicaDir, replica));
        Assert.True(later.Id.Value > earlier.Id.Value,
            $"the restarted node published segment {later.Id.Value} again — the id of the earlier segment the replica holds");
    }

    /// <summary>What the replication endpoint does with a pushed body: stage it, then let the engine import it.</summary>
    private static SegmentImportOutcome Replicate(SegmentInfo segment, string replicaDir, StorageEngine replica)
    {
        string segDir = Path.Combine(replicaDir, "segments");
        string final  = Path.Combine(segDir, $"{segment.NodeId.Value}-{segment.Id.Value}.seg");
        string staged = Path.Combine(segDir, $"{segment.NodeId.Value}-{segment.Id.Value}.{Guid.NewGuid():N}.seg.tmp");
        File.Copy(segment.FilePath, staged);
        var outcome = replica.ImportSegment(staged, final);
        if (outcome != SegmentImportOutcome.Registered) File.Delete(staged);   // as the endpoint does
        return outcome;
    }
}
