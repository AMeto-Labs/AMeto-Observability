using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// The WAL's periodic flush tick. It used to hand the whole mapping to
/// FlushViewOfFile/msync and fsync the file handle every time it ran, whether or not a byte
/// had been appended — a cost that scaled with the 64 MB mapping rather than with the work,
/// and a stall the single appender waited out because the handle flush ran under the write
/// lock.
///
/// <para>What must stay true regardless: a tick after appends makes those appends durable,
/// and recovery reads them back.</para>
/// </summary>
public sealed class WalFlushTickTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"Ameto-walflush-{Guid.NewGuid():N}.wal");

    public void Dispose()
    {
        foreach (string p in new[] { _path, _path + ".pool" })
            if (File.Exists(p)) { try { File.Delete(p); } catch { } }
    }

    private WriteAheadLog Open() =>
        WriteAheadLog.Open(_path, new NodeId(0), new SegmentId(1UL), initialCapacity: 8 * 1024 * 1024);

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(60);

    /// <summary>
    /// What a dirty tick writes the mapping back with: a range msync / FlushViewOfFile — except on
    /// Linux, where it issues none and the fsync writes the mapping's pages (see FlushTail).
    /// </summary>
    private static bool TicksWriteTheViewBack => !OperatingSystem.IsLinux();

    [Fact]
    public void CleanTick_DoesNoIoAtAll()
    {
        using var wal = Open();

        // Nothing appended yet: the very first tick has nothing to make durable.
        wal.Flush();
        Assert.Equal(0, wal.RangeFlushCount);
        Assert.Equal(0, wal.HandleFlushCount);

        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[] { 1, 2, 3 });
        wal.Flush();
        long after   = wal.RangeFlushCount;
        long handles = wal.HandleFlushCount;
        Assert.Equal(1, handles);
        Assert.True(after > 0 == TicksWriteTheViewBack, "a tick after an append must flush");

        // Three more idle ticks — nothing was appended, so nothing may be flushed.
        wal.Flush();
        wal.Flush();
        wal.Flush();
        Assert.Equal(after, wal.RangeFlushCount);
        Assert.Equal(handles, wal.HandleFlushCount);
    }

    [Fact]
    public void DirtyTick_FlushesTheWrittenRange_NotTheWholeMapping()
    {
        using var wal = Open();

        for (int i = 0; i < 50; i++)
            wal.Append(100 + i, Ameto.Core.LogLevel.Information, (ushort)i, "tmpl", new byte[64]);

        wal.Flush();

        if (TicksWriteTheViewBack)
        {
            // 50 small entries is a few KB — a page or two, nowhere near the 8 MB mapping the
            // old whole-view flush walked every tick.
            Assert.True(wal.LastRangeFlushBytes > 0);
            Assert.True(wal.LastRangeFlushBytes <= 64 * 1024,
                $"expected the tick to cover the written range, not the mapping; got {wal.LastRangeFlushBytes:N0} B");
        }
        else Assert.Equal(0, wal.RangeFlushCount);   // Linux: the fsync is the write-back

        // And the watermark advanced to exactly what has been written.
        Assert.Equal(wal.WrittenBytes + 32, wal.LastFlushedOffset);

        // Nothing fell back to the whole-view flush — a platform where the range call always
        // failed would still pass every other assertion here while paying the old cost.
        Assert.Equal(0, wal.RangeFlushFailures);
    }

    [Fact]
    public void TickAfterMoreAppends_FlushesAgain_AndOnlyTheNewTail()
    {
        using var wal = Open();

        wal.Append(1, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[64]);
        wal.Flush();
        long firstWatermark = wal.LastFlushedOffset;
        long ticks          = wal.HandleFlushCount;

        wal.Append(2, Ameto.Core.LogLevel.Information, 1, "tmpl", new byte[64]);
        wal.Flush();

        Assert.True(wal.HandleFlushCount > ticks);
        Assert.True(wal.LastFlushedOffset > firstWatermark);
    }

    /// <summary>
    /// A tick's write-back runs off the appender's lock: an append made while a tick is writing the
    /// mapping back goes through at once. The write-back ran under the lock — on Linux msync MS_SYNC,
    /// which waits for the I/O — and the single appender waited it out every tick. On Linux a tick
    /// issues no msync now, the fsync writing the mapping's pages; elsewhere the view's
    /// FlushViewOfFile runs outside the lock.
    /// </summary>
    [Fact]
    public void An_append_goes_through_while_a_tick_writes_the_mapping_back()
    {
        var wal = Open();
        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[] { 1 });
        long claimed = wal.WrittenBytes + 32;   // what the tick below claims: the file offset written before it
        using var inWriteBack = new ManualResetEventSlim(false);
        using var release     = new ManualResetEventSlim(false);
        if (TicksWriteTheViewBack)
            wal.ViewFlushHookForTest = () => { inWriteBack.Set(); release.Wait(HangGuard); };
        else
            wal.HandleFlushHookForTest = h => { inWriteBack.Set(); release.Wait(HangGuard); h.Flush(flushToDisk: true); };

        var tick = Task.Run(wal.Flush);
        try
        {
            Assert.True(inWriteBack.Wait(HangGuard), "the tick never reached its write-back");
            var append = Task.Run(() => wal.Append(200, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[] { 2 }));
            Assert.True(append.Wait(TimeSpan.FromSeconds(10)), "the append waited for the tick's write-back");
        }
        finally { release.Set(); }
        tick.Wait(HangGuard);

        Assert.Equal(1, wal.HandleFlushCount);
        if (!TicksWriteTheViewBack) Assert.Equal(0, wal.RangeFlushCount);   // no msync on Linux
        wal.Dispose();
        Assert.Equal(new long[] { 100, 200 }, WriteAheadLog.ReadForRecovery(_path).Entries.Select(e => e.TimestampTicks));
    }

    /// <summary>
    /// A grow waits out a tick writing the old view back: the write-back runs off the appender's
    /// lock now, and a grow unmaps the view it is writing back. (On Linux no tick writes the view
    /// back, so there is nothing to wait for.)
    /// </summary>
    [Fact]
    public void A_grow_waits_out_a_tick_writing_the_old_view_back()
    {
        var wal = WriteAheadLog.Open(_path, new NodeId(0), new SegmentId(1UL), initialCapacity: 64 * 1024);
        wal.Append(1, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[512]);
        using var inWriteBack = new ManualResetEventSlim(false);
        using var release     = new ManualResetEventSlim(false);
        wal.ViewFlushHookForTest = () => { inWriteBack.Set(); release.Wait(HangGuard); };

        var tick = Task.Run(wal.Flush);
        try
        {
            if (TicksWriteTheViewBack)
            {
                Assert.True(inWriteBack.Wait(HangGuard), "the tick never reached its write-back");
                var grow = Task.Run(() => wal.Append(2, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[128 * 1024]));
                Assert.False(grow.Wait(TimeSpan.FromMilliseconds(500)), "the grow unmapped the view under a write-back");
                release.Set();
                Assert.True(grow.Wait(HangGuard));
            }
            else wal.Append(2, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[128 * 1024]);
        }
        finally { release.Set(); }
        tick.Wait(HangGuard);

        wal.Flush();
        wal.Dispose();
        Assert.Equal(new long[] { 1, 2 }, WriteAheadLog.ReadForRecovery(_path).Entries.Select(e => e.TimestampTicks));
    }

    /// <summary>The same for a dispose, which unmaps the view too: it waits the write-back out.</summary>
    [Fact]
    public void A_dispose_waits_out_a_tick_writing_the_view_back()
    {
        var wal = Open();
        wal.Append(1, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[64]);
        using var inWriteBack = new ManualResetEventSlim(false);
        using var release     = new ManualResetEventSlim(false);
        wal.ViewFlushHookForTest = () => { inWriteBack.Set(); release.Wait(HangGuard); };

        var tick = Task.Run(wal.Flush);
        try
        {
            if (TicksWriteTheViewBack)
            {
                Assert.True(inWriteBack.Wait(HangGuard), "the tick never reached its write-back");
                var dispose = Task.Run(wal.Dispose);
                Assert.False(dispose.Wait(TimeSpan.FromMilliseconds(500)), "the dispose unmapped the view under a write-back");
                release.Set();
                Assert.True(dispose.Wait(HangGuard));
            }
            else wal.Dispose();
        }
        finally { release.Set(); }
        tick.Wait(HangGuard);

        Assert.Single(WriteAheadLog.ReadForRecovery(_path).Entries);
    }

    /// <summary>
    /// A failed drive flush must be retried by the next tick even when nothing new is appended.
    /// The watermark used to advance under the lock BEFORE the handle flush ran outside it, so
    /// when FlushFileBuffers threw, the next idle tick found writeEnd == watermark and did
    /// nothing: the acknowledged tail stayed in the drive cache until another event arrived
    /// or the WAL rotated.
    /// </summary>
    [Fact]
    public void FailedHandleFlush_IsRetriedByTheNextIdleTick()
    {
        using var wal = Open();
        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[] { 1, 2, 3 });

        bool failNext = true;
        wal.HandleFlushHookForTest = h =>
        {
            if (failNext) { failNext = false; throw new IOException("simulated FlushFileBuffers failure"); }
            h.Flush(flushToDisk: true);
        };

        // Tick 1: the msync lands, the drive flush throws, and the loop would log it.
        Assert.Throws<IOException>(wal.Flush);
        Assert.Equal(0, wal.HandleFlushCount);
        Assert.True(wal.LastFlushedOffset < wal.WrittenBytes + 32,
            "the watermark claimed durability the failed drive flush never delivered");

        // Tick 2: nothing appended, but the tail is still not durable, so the flush is retried.
        wal.Flush();
        Assert.Equal(1, wal.HandleFlushCount);
        Assert.Equal(wal.WrittenBytes + 32, wal.LastFlushedOffset);

        // Tick 3: now it really is idle.
        long ranges = wal.RangeFlushCount;
        wal.Flush();
        Assert.Equal(1, wal.HandleFlushCount);
        Assert.Equal(ranges, wal.RangeFlushCount);
    }

    /// <summary>
    /// A drive flush that fails on every tick must not starve the template pool's fsync. Because
    /// the watermark waits for the handle flush, every tick of such a WAL, idle or not, retries
    /// the handle flush and throws. The pool fsync came after it in straight-line code and was
    /// never reached, so after power loss replay brought the events back with no template or
    /// with another event's.
    /// </summary>
    [Fact]
    public void HandleFlushFailingOnEveryTick_StillFsyncsThePool()
    {
        using var wal = Open();
        wal.HandleFlushHookForTest = static _ => throw new IOException("simulated FlushFileBuffers failure");

        int poolAttempts = 0;
        wal.PoolFlushHookForTest = h =>
        {
            // The pool's own first fsync fails as well (an IOException, which the pool retries
            // quietly), so the idle second tick has to reach the pool again past a handle flush
            // that throws again.
            if (++poolAttempts == 1) throw new IOException("simulated pool fsync failure");
            RandomAccess.FlushToDisk(h);
        };

        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl-a", new byte[] { 1, 2, 3 });

        // Tick 1: the handle flush throws, and the pool fsync is still attempted.
        var ex = Assert.Throws<IOException>(wal.Flush);
        Assert.Equal("simulated FlushFileBuffers failure", ex.Message);
        Assert.Equal(1, poolAttempts);
        Assert.Equal(0, wal.PoolFlushCount);

        // Tick 2: idle, the handle throws again, and the still-dirty pool is fsynced.
        Assert.Throws<IOException>(wal.Flush);
        Assert.Equal(2, poolAttempts);
        Assert.Equal(1, wal.PoolFlushCount);

        // Tick 3: idle and the pool is clean, so there is no pool I/O.
        Assert.Throws<IOException>(wal.Flush);
        Assert.Equal(2, poolAttempts);

        // A new template while the drive is still failing gets its row fsynced on the next tick.
        wal.Append(101, Ameto.Core.LogLevel.Information, 1, "tmpl-b", new byte[] { 4 });
        Assert.Throws<IOException>(wal.Flush);
        Assert.Equal(2, wal.PoolFlushCount);

        // None of that moved the WAL's watermark: its tail is still not durable.
        Assert.Equal(0, wal.HandleFlushCount);
        Assert.True(wal.LastFlushedOffset < wal.WrittenBytes + 32);
    }

    /// <summary>
    /// When the drive flush and the pool fsync fail on the same tick, the drive's exception is
    /// the one that reaches the flush loop's log. A pool exception thrown from the finally would
    /// replace it, and the log would name the pool while the WAL tail went on failing. The pool
    /// must also stay dirty so the next tick fsyncs it. The pool throws a non-IO exception on
    /// purpose, because the pool always swallowed its own IOException.
    /// </summary>
    [Fact]
    public void HandleAndPoolFailingOnTheSameTick_ReportTheHandleFailure_AndBothAreRetried()
    {
        using var wal = Open();
        bool failing = true;
        wal.HandleFlushHookForTest = h =>
        {
            if (failing) throw new IOException("simulated FlushFileBuffers failure");
            h.Flush(flushToDisk: true);
        };
        wal.PoolFlushHookForTest = h =>
        {
            if (failing) throw new UnauthorizedAccessException("simulated pool fsync failure");
            RandomAccess.FlushToDisk(h);
        };

        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[] { 1, 2, 3 });

        var ex = Assert.Throws<IOException>(wal.Flush);
        Assert.Equal("simulated FlushFileBuffers failure", ex.Message);
        Assert.Equal(0, wal.PoolFlushCount);

        // The device recovers. The next tick is idle but has both flushes left to do.
        failing = false;
        wal.Flush();
        Assert.Equal(1, wal.HandleFlushCount);
        Assert.Equal(1, wal.PoolFlushCount);
        Assert.Equal(wal.WrittenBytes + 32, wal.LastFlushedOffset);
    }

    /// <summary>
    /// A pool fsync that fails with something other than an IOException, on a tick where the
    /// WAL half succeeded, still reaches the loop's log. It also leaves the pool dirty: the flag
    /// used to be cleared before the fsync and restored only for IOException, so any other
    /// failure meant the row was never fsynced again.
    /// </summary>
    [Fact]
    public void PoolFsyncFailingAlone_Propagates_AndIsRetriedByTheNextTick()
    {
        using var wal = Open();
        bool failPool = true;
        wal.PoolFlushHookForTest = h =>
        {
            if (failPool) { failPool = false; throw new UnauthorizedAccessException("simulated pool fsync failure"); }
            RandomAccess.FlushToDisk(h);
        };

        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[] { 1, 2, 3 });

        Assert.Throws<UnauthorizedAccessException>(wal.Flush);
        Assert.Equal(1, wal.HandleFlushCount);
        Assert.Equal(0, wal.PoolFlushCount);

        wal.Flush();
        Assert.Equal(1, wal.PoolFlushCount);
    }

    [Fact]
    public void FlushedEntriesSurviveAndReplay()
    {
        var payloads = new List<byte[]>();
        using (var wal = Open())
        {
            for (int i = 0; i < 200; i++)
            {
                var p = new byte[32];
                p[0] = (byte)i;
                payloads.Add(p);
                wal.Append(1000 + i, Ameto.Core.LogLevel.Warning, (ushort)(i % 16), "tmpl-" + (i % 16), p);

                // A tick every so often, as the storage engine's timer does.
                if (i % 37 == 0) wal.Flush();
            }
            wal.Flush();
            wal.Flush();   // idle tick right before close
        }

        var (segId, entries) = WriteAheadLog.ReadForRecovery(_path);
        Assert.Equal(1UL, segId);
        Assert.Equal(200, entries.Count);
        for (int i = 0; i < 200; i++)
        {
            Assert.Equal(1000 + i, entries[i].TimestampTicks);
            Assert.Equal(payloads[i], entries[i].Payload);
        }
    }

    [Fact]
    public void ReopenedWal_DoesNotReflushWhatIsAlreadyOnDisk()
    {
        using (var wal = Open())
        {
            for (int i = 0; i < 20; i++)
                wal.Append(i, Ameto.Core.LogLevel.Information, (ushort)i, "tmpl", new byte[32]);
            wal.Flush();
        }

        using var reopened = Open();

        // Everything in the file came off disk — an idle tick has nothing to do.
        reopened.Flush();
        Assert.Equal(0, reopened.RangeFlushCount);
        Assert.Equal(0, reopened.HandleFlushCount);

        reopened.Append(999, Ameto.Core.LogLevel.Error, 0, "tmpl", new byte[32]);
        reopened.Flush();
        Assert.Equal(1, reopened.HandleFlushCount);
        Assert.Equal(TicksWriteTheViewBack ? 1 : 0, reopened.RangeFlushCount);   // one region: the header page IS the tail page here
    }

    [Fact]
    public void FlushAcrossAGrow_StillReplaysEverything()
    {
        // Capacity small enough that the appends below force at least one Grow (which
        // unmaps and remaps, moving the base pointer the range flush is computed from).
        using (var wal = WriteAheadLog.Open(_path, new NodeId(0), new SegmentId(7UL), initialCapacity: 64 * 1024))
        {
            for (int i = 0; i < 400; i++)
            {
                wal.Append(i, Ameto.Core.LogLevel.Information, (ushort)(i % 8), "tmpl", new byte[512]);
                if (i % 50 == 0) wal.Flush();
            }
            wal.Flush();
        }

        var (segId, entries) = WriteAheadLog.ReadForRecovery(_path);
        Assert.Equal(7UL, segId);
        Assert.Equal(400, entries.Count);
        Assert.Equal(399, entries[^1].TimestampTicks);
    }
}
