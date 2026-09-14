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
