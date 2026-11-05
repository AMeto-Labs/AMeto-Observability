using System.Buffers;
using System.Collections.Concurrent;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Core;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A LOG MERGE ASKS THE GC FOR (#141): the background collection, not the aggressive blocking
/// compacting one it used to force after every merge — a stop-the-world pause every two minutes for
/// as long as merges had a backlog. Pinned where the request is made, in the maintenance loop of a
/// running engine: a pass that merged asks for the background shape, a pass that merged nothing asks
/// for nothing. The gate's own facts are about the gate; the loop could drop the call, make it after
/// every pass, or go back to the aggressive entry point, and every one of them would stay green.
///
/// <para>The gate is process-wide; this assembly runs its classes one at a time, so the seam and the
/// reset below cannot reach another test's merge.</para>
/// </summary>
public sealed class MaintenanceCollectShapeTests : IDisposable
{
    /// <summary>Written on the maintenance loop's thread, read on the test's.</summary>
    private readonly ConcurrentQueue<AggressiveGcGate.Shape> _collected = new();

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-collect-shape-" + Guid.NewGuid().ToString("N"));

    public MaintenanceCollectShapeTests()
    {
        Directory.CreateDirectory(_root);
        AggressiveGcGate.ResetForTest();                     // a merge earlier in the run may hold the slot
        AggressiveGcGate._collectForTest = _collected.Enqueue;
    }

    public void Dispose()
    {
        AggressiveGcGate._collectForTest = null;
        AggressiveGcGate.ResetForTest();
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task A_pass_that_merged_asks_for_a_background_collection()
    {
        // A restart with a backlog: two small files in a sealed bucket are a batch, so the loop's first
        // pass is a merge.
        string dir    = Dir("backlog");
        long   old    = DateTime.UtcNow.AddDays(-20).Date.Ticks;
        var    writer = Engine(dir);
        try
        {
            await FlushAsync(writer, old, from: 0);
            await FlushAsync(writer, old, from: 100);
        }
        finally { await writer.DisposeAsync(); }

        await AfterPassesAsync(dir, passes: 1, engine =>
        {
            Assert.Single(engine.ListSegments());            // setup: that pass merged the two
            Assert.Equal(new[] { AggressiveGcGate.Shape.Background }, _collected);
        });
    }

    [Fact]
    public async Task A_pass_that_merged_nothing_asks_for_nothing()
    {
        // Gated on a merge having happened: an idle server's loop goes round all day, and a collection
        // after each pass is what that gate keeps it from paying for.
        await AfterPassesAsync(Dir("idle"), passes: 3, _ => Assert.Empty(_collected));
    }

    [Fact]
    public void A_merge_right_after_a_pressure_collection_asks_for_nothing()
    {
        // A pressure collection a moment ago compacted the heap: there is nothing left for the merge's
        // sweep to reclaim, so it yields — the one direction the gate lets one shape stand in for another.
        AggressiveGcGate.SetForTest(aggressiveAgo: TimeSpan.FromSeconds(1), backgroundAgo: null);

        StorageEngine.ReleaseMaintenanceMemory();

        Assert.Empty(_collected);
    }

    // ── The engine ──────────────────────────────────────────────────────────

    /// <summary>A data directory: it has to exist before the engine, for its retention store's database.</summary>
    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    /// <summary>
    /// Starts an engine on <paramref name="dir"/> whose maintenance loop goes round every 20 ms, waits
    /// until <paramref name="passes"/> passes have reached the loop's pause — by then the step after
    /// each, the collection request included, is done — and runs <paramref name="check"/> there. The
    /// boot scan is held until the hooks are set: the constructor starts the loop, and the scan it
    /// waits on is over in microseconds on a directory this small.
    /// </summary>
    private static async Task AfterPassesAsync(string dir, int passes, Action<StorageEngine> check)
    {
        var opts   = Options(dir);
        var scan   = new TaskCompletionSource();
        var engine = new StorageEngine(Microsoft.Extensions.Options.Options.Create(opts),
                                       new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                                       NullLogger<StorageEngine>.Instance,
                                       maintenanceStartDelay: TimeSpan.Zero, bootScanHeldUntil: scan.Task);
        int paused = 0;
        engine._allowIndexlessMerge     = true;              // no index builder is wired in a test
        engine._maintenancePauseForTest = TimeSpan.FromMilliseconds(20);
        engine._beforeLoopWaitForTest   = loop => { if (loop == "maintenance") Interlocked.Increment(ref paused); };
        try
        {
            scan.SetResult();
            Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref paused) >= passes, TimeSpan.FromSeconds(15)),
                        $"timed out waiting for {passes} maintenance pass(es)");
            check(engine);
        }
        finally
        {
            engine._beforeLoopWaitForTest = null;
            scan.TrySetResult();
            await engine.DisposeAsync();
        }
    }

    private static ServerOptions Options(string dir) => new()
    {
        DataDirectory = dir,
        HotTier = new HotTierOptions { MaxSizeBytes = 8 * 1024 * 1024, FlushConcurrency = 1, SpillMinFreeDiskBytes = 0 },
    };

    private static StorageEngine Engine(string dir)
    {
        var opts = Options(dir);
        return new StorageEngine(Microsoft.Extensions.Options.Options.Create(opts),
                                 new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                                 NullLogger<StorageEngine>.Instance);
    }

    /// <summary>Ten events from <paramref name="baseTicks"/>, flushed: one small segment.</summary>
    private static async Task FlushAsync(StorageEngine engine, long baseTicks, int from)
    {
        for (int i = from; i < from + 10; i++)
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("event {n}"),
            }, Props(i), "event {n}"));
        await engine.FlushHotTierAsync();
    }

    private static byte[] Props(long n)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write(n);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
