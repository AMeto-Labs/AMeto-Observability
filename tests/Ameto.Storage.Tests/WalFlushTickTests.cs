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

    [Fact]
    public void CleanTick_DoesNoIoAtAll()
    {
        using var wal = Open();

        // Nothing appended yet: the very first tick has nothing to make durable.
        wal.Flush();
        Assert.Equal(0, wal.RangeFlushCount);

        wal.Append(100, Ameto.Core.LogLevel.Information, 0, "tmpl", new byte[] { 1, 2, 3 });
        wal.Flush();
        long after = wal.RangeFlushCount;
        Assert.True(after > 0, "a tick after an append must flush");

        // Three more idle ticks — nothing was appended, so nothing may be flushed.
        wal.Flush();
        wal.Flush();
        wal.Flush();
        Assert.Equal(after, wal.RangeFlushCount);
    }

    [Fact]
    public void DirtyTick_FlushesTheWrittenRange_NotTheWholeMapping()
    {
        using var wal = Open();

        for (int i = 0; i < 50; i++)
            wal.Append(100 + i, Ameto.Core.LogLevel.Information, (ushort)i, "tmpl", new byte[64]);

        wal.Flush();

        // 50 small entries is a few KB — a page or two, nowhere near the 8 MB mapping the
        // old whole-view flush walked every tick.
        Assert.True(wal.LastRangeFlushBytes > 0);
        Assert.True(wal.LastRangeFlushBytes <= 64 * 1024,
            $"expected the tick to cover the written range, not the mapping; got {wal.LastRangeFlushBytes:N0} B");

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
        long ticks          = wal.RangeFlushCount;

        wal.Append(2, Ameto.Core.LogLevel.Information, 1, "tmpl", new byte[64]);
        wal.Flush();

        Assert.True(wal.RangeFlushCount > ticks);
        Assert.True(wal.LastFlushedOffset > firstWatermark);
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
            h.Flush(flushToDisk: true);
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
            h.Flush(flushToDisk: true);
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
            h.Flush(flushToDisk: true);
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

        reopened.Append(999, Ameto.Core.LogLevel.Error, 0, "tmpl", new byte[32]);
        reopened.Flush();
        Assert.Equal(1, reopened.RangeFlushCount);   // one region: the header page IS the tail page here
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
