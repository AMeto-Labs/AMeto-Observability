using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// What the log WAL's appender — which the engine calls holding its writer lock, every writer behind
/// it — still waited on after C2 took the tick's view write-back off the write lock (round-3 review
/// R3-1): the tick's fsync of the pool file, under the pool lock an append needing a new row takes;
/// and a grow's write-back of the whole old view, under the write lock.
/// </summary>
public sealed class WalWriterPathTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"Ameto-walwriter-{Guid.NewGuid():N}.wal");

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(60);

    public void Dispose()
    {
        foreach (string p in new[] { _path, _path + ".pool" })
            try { File.Delete(p); } catch { /* best-effort */ }
    }

    /// <summary>
    /// The tick fsyncs the pool off its lock: an append whose template needs a new row goes through
    /// while that fsync is in flight. It waited it out under the pool lock, with the engine's writer
    /// lock held and every writer behind it. The row it wrote is fsynced by the next tick.
    /// </summary>
    [Fact]
    public void An_append_that_needs_a_new_pool_row_goes_through_while_a_tick_fsyncs_the_pool()
    {
        using var wal = WriteAheadLog.Open(_path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024);
        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl-a", new byte[] { 1 });   // a row: the pool is dirty

        using var inFsync = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        wal.PoolFlushHookForTest = _ =>
        {
            inFsync.Set();
            release.Wait(HangGuard);   // the fsync in flight
        };
        var tick = Task.Run(wal.Flush);
        try
        {
            Assert.True(inFsync.Wait(HangGuard), "the tick never reached the pool's fsync");
            var append = Task.Run(() => wal.Append(200, Ameto.Core.LogLevel.Information, 1, "tmpl-b", new byte[] { 2 }));
            Assert.True(append.Wait(TimeSpan.FromSeconds(5)), "the append waited for the tick's pool fsync");
        }
        finally
        {
            release.Set();
            tick.Wait(HangGuard);
        }

        wal.PoolFlushHookForTest = null;
        long fsyncs = wal.PoolFlushCount;
        wal.Flush();                                     // the row written during the fsync is the next tick's
        Assert.Equal(fsyncs + 1, wal.PoolFlushCount);
        Assert.Equal(2, WriteAheadLog.LoadPool(_path + ".pool").Count);
    }

    /// <summary>
    /// The new path the pool fsync off its lock adds: a dispose can now close the pool while a tick's
    /// fsync of it is in flight (it used to wait for the lock the fsync held). The dispose fsyncs the
    /// pool itself, so the tick's fsync finding the handle closed is not an error — the flush loop
    /// would log it as one at every shutdown — and the row is in the file.
    /// </summary>
    [Fact]
    public void A_dispose_while_a_tick_fsyncs_the_pool_is_not_the_ticks_error()
    {
        var wal = WriteAheadLog.Open(_path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024);
        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl-a", new byte[] { 1 });   // a row: the pool is dirty

        using var inFsync  = new ManualResetEventSlim(false);
        using var disposed = new ManualResetEventSlim(false);
        wal.PoolFlushHookForTest = h =>
        {
            inFsync.Set();
            disposed.Wait(HangGuard);       // the dispose closes the pool under the fsync's claim
            RandomAccess.FlushToDisk(h);
        };
        var tick = Task.Run(wal.Flush);
        try
        {
            Assert.True(inFsync.Wait(HangGuard), "the tick never reached the pool's fsync");
            wal.Dispose();
        }
        finally { disposed.Set(); }

        tick.Wait(HangGuard);   // a closed pool is not the tick's failure
        Assert.Single(WriteAheadLog.LoadPool(_path + ".pool"));
    }

    /// <summary>
    /// A grow unmaps the old view without writing it back: that write-back covered the whole view,
    /// under the write lock the appender holds. Its pages are the file's, the new view maps the same
    /// ones, and the next tick writes back the range from its watermark and fsyncs, as it always does:
    /// every entry, before the grow and after, is in the file.
    /// </summary>
    [Fact]
    public void A_grow_unmaps_the_old_view_without_writing_it_back()
    {
        var wal = WriteAheadLog.Open(_path, new NodeId(0), new SegmentId(1UL), initialCapacity: 64 * 1024);
        try
        {
            int writeBacks = 0;
            var closed     = new List<bool>();
            wal.UnmapWriteBackForTest        = () => Interlocked.Increment(ref writeBacks);
            wal.UnmapWithoutWriteBackForTest = closed.Add;

            var payload = new byte[1024];
            for (int i = 0; i < 200; i++)   // ~210 KB: past 64 KB, so it grows (twice)
                wal.Append(1 + i, Ameto.Core.LogLevel.Information, 0, "tmpl", payload);
            Assert.True(wal.WrittenBytes > 128 * 1024);
            Assert.Equal(0, writeBacks);

            // Round-4 L20: the mechanism, not only the branch. The accessor's dispose writes the whole
            // view back while the view is still open; each grow closes the view's handle first, which is
            // what makes that dispose skip it.
            Assert.Equal(2, closed.Count);
            Assert.All(closed, isClosed => Assert.True(isClosed, "the view was still open when its accessor was disposed"));

            wal.Flush();
        }
        finally { wal.Dispose(); }   // the file can be opened to read only once the log lets go of it
        Assert.Equal(200, WriteAheadLog.ReadForRecovery(_path).Entries.Count);
    }

    /// <summary>
    /// Round-4 L17. A grow whose restoring map failed too leaves the log unmapped until rotation, and
    /// the tick still flushes its handle: the entries before the grow — acknowledged — are made
    /// durable. The tick returned before the handle flush whenever the view was gone, on every tick,
    /// so an idle log stayed in the drive's cache.
    /// </summary>
    [Fact]
    public void A_tick_flushes_the_handle_of_a_log_a_failed_grow_left_unmapped()
    {
        var wal = WriteAheadLog.Open(_path, new NodeId(0), new SegmentId(1UL), initialCapacity: 64 * 1024);
        try
        {
            for (int i = 0; i < 10; i++)
                wal.Append(1 + i, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[100]);
            long written = wal.WrittenBytes;

            wal.MapHookForTest = _ => throw new IOException("test: the disk is full");
            Assert.Throws<IOException>(() => wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[128 * 1024]));
            var unmapped = Assert.Throws<IOException>(() => wal.Append(101, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[1]));
            Assert.Contains("unmapped", unmapped.Message);

            long flushes = wal.HandleFlushCount;
            wal.Flush();
            Assert.Equal(flushes + 1, wal.HandleFlushCount);
            Assert.Equal(32 + written, wal.LastFlushedOffset);
        }
        finally { wal.Dispose(); }
        Assert.Equal(10, WriteAheadLog.ReadForRecovery(_path).Entries.Count);
    }

    /// <summary>
    /// Round-4 L19. The tick's claim of the pool reads the stream's handle, whose getter writes the
    /// stream's buffer out first and can fail. That failure is the pool fsync's and follows its rule:
    /// the pool stays dirty for the next tick, an IOException is not the tick's failure, and on a tick
    /// whose WAL half failed it does not replace the WAL's exception. The claim was made outside that
    /// rule's try.
    /// </summary>
    [Fact]
    public void A_pool_claim_that_fails_is_retried_and_never_replaces_the_wals_failure()
    {
        using var wal = WriteAheadLog.Open(_path, new NodeId(0), new SegmentId(1UL), initialCapacity: 64 * 1024);
        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl-a", new byte[] { 1 });   // a row: the pool is dirty

        // The WAL half succeeds: the claim's failure is not the tick's, and the pool stays dirty.
        wal.PoolHandleHookForTest = _ => throw new IOException("test: the pool's buffer could not be written");
        wal.Flush();
        long fsyncs = wal.PoolFlushCount;

        // The WAL half fails too: its exception is the one that leaves the tick.
        wal.Append(200, Ameto.Core.LogLevel.Information, 1, "tmpl-b", new byte[] { 2 });
        wal.HandleFlushHookForTest = _ => throw new IOException("test: the WAL's drive flush failed");
        var failed = Assert.Throws<IOException>(wal.Flush);
        Assert.Contains("WAL's drive flush", failed.Message);

        // And the pool is fsynced once its claim works again.
        wal.PoolHandleHookForTest  = null;
        wal.HandleFlushHookForTest = null;
        wal.Flush();
        Assert.Equal(fsyncs + 1, wal.PoolFlushCount);
    }
}
