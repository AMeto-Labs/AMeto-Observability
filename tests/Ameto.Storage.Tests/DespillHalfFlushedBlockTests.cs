using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// A despill the process died in has moved some of its block's levels into place. The next start
/// despills the block again and must not write those levels a second time, whatever has happened to
/// their files meanwhile: the boot scan registers them as ordinary segments, a merge may take one
/// into another segment before the despill — in the background, behind other blocks or a new spill —
/// gets to the block, and a failed despill's retry used to unlink every level file of the block as a
/// leftover. The despill used to ask the disk only when it ran; the start now notes which levels an
/// earlier run published, and the despill and its retry skip exactly those.
/// </summary>
public sealed class DespillHalfFlushedBlockTests : IDisposable
{
    /// <summary>Weeks back: the merge planner merges only inside a sealed bucket.</summary>
    private static readonly long Base = new DateTimeOffset(2026, 8, 20, 9, 0, 0, TimeSpan.Zero).UtcTicks;

    private const ulong Block = 9_000;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(60);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-despill-half-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    /// <summary>A merge takes the earlier run's level, with a neighbour, into another segment before the despill runs.</summary>
    [Fact]
    public async Task A_level_an_earlier_run_published_is_not_written_again_when_a_merge_took_it_first()
    {
        var opts = Options();
        var (spill, infoLevel) = await HalfDespilledBlockAsync(opts);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        await using var run2 = Engine(opts);
        StorageEngine.HoldDespillForTest.Value = null;
        await run2.CatalogLoaded;
        for (int i = 150; i < 250; i++)
            Assert.True(run2.TryWrite(Header(run2, i, LogLevel.Information), Props(i), "event {n}"));
        await run2.FlushHotTierAsync();
        run2._allowIndexlessMerge = true;
        Assert.True(await run2.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the earlier run's level did not merge");
        Assert.False(File.Exists(infoLevel), "setup: the merge left the earlier run's level in place");

        hold.SetResult();
        WaitFor(() => run2.SpillFilesPending == 0 && !File.Exists(spill), "the block to be despilled");

        AssertEveryEventOnce(run2, 250);
    }

    /// <summary>
    /// The despill's own flush fails (after it has moved its Error level): its retry used to delete
    /// every level file of the block as a leftover of the failed attempt — the earlier run's
    /// Information level, a registered segment, among them — and write them all again, while the
    /// catalog still named the deleted file. It keeps that level now, and writes only the rest.
    /// </summary>
    [Fact]
    public async Task The_retry_of_a_failed_despill_keeps_the_levels_an_earlier_run_published()
    {
        var opts = Options();
        var (spill, infoLevel) = await HalfDespilledBlockAsync(opts);
        var infoWritten = File.GetLastWriteTimeUtc(infoLevel);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        await using var run2 = Engine(opts);
        StorageEngine.HoldDespillForTest.Value = null;
        await run2.CatalogLoaded;
        run2._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        int armed = 1;
        run2._afterLevelPublished = _ =>
        {
            if (Interlocked.Exchange(ref armed, 0) == 1) throw new IOException("test: the despill's flush fails after a level");
        };

        hold.SetResult();
        WaitFor(() => run2.SpillFilesPending == 0 && !File.Exists(spill), "the retry to despill the block");

        Assert.True(File.Exists(infoLevel), "the retry deleted the level an earlier run published");
        Assert.Equal(infoWritten, File.GetLastWriteTimeUtc(infoLevel));
        Assert.Equal(50, run2.DespilledEvents);   // the Error level, written by the retry and counted by it
        AssertEveryEventOnce(run2, 150);
    }

    /// <summary>
    /// A level the earlier run published that the boot scan finds torn, and sets aside as
    /// <c>.seg.corrupt</c>, is written again by the despill. The levels to keep were noted before the
    /// scan ran, so the despill skipped it — once at once, before the scan, and once after it, alike —
    /// and its events were left in a file served by nobody. The despill of such a block now waits
    /// for the scan, and keeps only the levels the scan accepted.
    /// </summary>
    [Fact]
    public async Task A_level_the_boot_scan_sets_aside_as_torn_is_written_again_by_the_despill()
    {
        var opts = Options();
        var (spill, infoLevel) = await HalfDespilledBlockAsync(opts);
        using (var torn = new FileStream(infoLevel, FileMode.Open, FileAccess.Write))
            torn.SetLength(16);   // too short for its footer: the scan sets it aside

        var scan = new TaskCompletionSource();
        StorageEngine.HoldCatalogScanForTest.Value = scan.Task;
        StorageEngine run2;
        try { run2 = Engine(opts); }
        finally { StorageEngine.HoldCatalogScanForTest.Value = null; }
        try
        {
            // A despill that did not wait for the scan would be done well within this.
            SpinWait.SpinUntil(() => run2.SpillFilesPending == 0, TimeSpan.FromSeconds(1));

            scan.SetResult();
            await run2.CatalogLoaded;
            Assert.True(File.Exists(infoLevel + ".corrupt"), "setup: the scan did not set the torn level aside");

            WaitFor(() => run2.SpillFilesPending == 0 && !File.Exists(spill), "the block to be despilled");
            AssertEveryEventOnce(run2, 150);
        }
        finally
        {
            scan.TrySetResult();
            await run2.DisposeAsync();
        }
    }

    /// <summary>
    /// Writes the spilled block — events 0..99 at Information, 100..149 at Error — and runs an engine
    /// whose despill moves the Information level into place and then "dies": the throw stands in for
    /// the kill, and the shutdown cancels the retry it schedules. Returns the spill file, still on
    /// disk, and the level file the despill left beside it.
    /// </summary>
    private async Task<(string Spill, string InfoLevel)> HalfDespilledBlockAsync(ServerOptions opts)
    {
        string walDir = Path.Combine(_dir, "wal");
        Directory.CreateDirectory(walDir);
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-{Block}{StorageEngine.SpillExtension}");
        using (var wal = WriteAheadLog.Open(spill, opts.NodeId, new SegmentId(Block), 1 << 20))
            for (int i = 0; i < 150; i++)
                wal.Append(At(i), i < 100 ? LogLevel.Information : LogLevel.Error, 0, "event {n}", Props(i));

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        var run1 = Engine(opts);
        StorageEngine.HoldDespillForTest.Value = null;
        run1._afterLevelPublished = _ => throw new IOException("test: the process dies after its first level's move");
        hold.SetResult();
        WaitFor(() => run1.DespillsCompletedForTest == 1, "run 1's despill to stop after its first level");
        await run1.DisposeAsync();

        string infoLevel = Assert.Single(Directory.GetFiles(Path.Combine(_dir, "segments"), $"{opts.NodeId.Value}-{Block + 2}-*.seg"));
        Assert.True(File.Exists(spill), "the spill file must outlive run 1");
        return (spill, infoLevel);
    }

    private static void AssertEveryEventOnce(StorageEngine engine, int expected)
    {
        var seen  = new HashSet<long>();
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var seg in engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            foreach (var ev in r.ReadAllRaw(dedup))
            {
                long n = (ev.TsTicks - Base) / TimeSpan.TicksPerMillisecond;
                Assert.True(seen.Add(n), $"event {n} is in the segments twice");
            }
        }
        Assert.Equal(expected, seen.Count);
    }

    private ServerOptions Options()
    {
        Directory.CreateDirectory(_dir);
        return new ServerOptions { DataDirectory = _dir };
    }

    private static long At(int i) => Base + i * TimeSpan.TicksPerMillisecond;

    private static LogEventHeader Header(StorageEngine engine, int i, LogLevel level) => new()
    {
        TimestampUtcTicks        = At(i),
        Level                    = level,
        MessageTemplatePoolIndex = engine.TemplatePool.Intern("event {n}"),
    };

    private static byte[] Props(long n)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write(n);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static StorageEngine Engine(ServerOptions opts) => new(
        Microsoft.Extensions.Options.Options.Create(opts),
        new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance);

    private static void WaitFor(Func<bool> condition, string what) =>
        Assert.True(SpinWait.SpinUntil(condition, HangGuard), $"timed out waiting for {what}");
}
