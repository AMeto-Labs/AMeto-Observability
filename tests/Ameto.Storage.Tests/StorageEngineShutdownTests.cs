using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// A hot tier's native memory is freed only once every flush that swapped it has ended and no
/// reader snapshot holds it. <see cref="StorageEngine.DisposeAsync"/> used to assume the first
/// half — "no writes remain" — and ignore the second:
///
/// <list type="bullet">
/// <item>A write arriving after its in-flight snapshot found the tier full, scheduled a flush,
/// and that flush read the tier while shutdown freed it (AccessViolation in
/// <c>HotTierEventSource.EventAt</c>, or a segment written from another tier's rows once the
/// freed memory was reused).</item>
/// <item><see cref="StorageEngine.FlushHotTierAsync"/> was never in the snapshot at all.</item>
/// <item>Retired tiers were freed whatever the reader count said.</item>
/// </list>
///
/// <para>Every test here holds the window open at a seam instead of racing it, and is built so
/// that the engine this replaces fails an assertion rather than faulting the test host: whatever
/// could read freed memory is either finished or aborted before the memory goes.</para>
/// </summary>
public sealed class StorageEngineShutdownTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-shutdown-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort */ }
    }

    private StorageEngine NewEngine(Microsoft.Extensions.Logging.ILogger<StorageEngine>? logger = null) => NewEngine(out _, logger);

    private StorageEngine NewEngine(out string dir, Microsoft.Extensions.Logging.ILogger<StorageEngine>? logger = null)
    {
        dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions
        {
            DataDirectory = dir,
            // One chunk: 16 384 events fill it, so a write loop can reach "tier full".
            HotTier = new HotTierOptions { MaxSizeBytes = 8 * 1024 * 1024 },
        };
        return new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            logger ?? NullLogger<StorageEngine>.Instance);
    }

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static bool TryWrite(StorageEngine engine, int n, LogLevel level) =>
        engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = DateTime.UtcNow.Ticks,
            Level                    = level,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
        }, Props(n));

    private static void Write(StorageEngine engine, int count, LogLevel level)
    {
        for (int i = 0; i < count; i++)
            Assert.True(TryWrite(engine, i, level));
    }

    /// <summary>
    /// Once shutdown has closed the write path, a write is refused and schedules nothing, and by
    /// the time tiers are freed no flush is running. Checked from inside the teardown, at the
    /// seam just before the free, by writing until the engine says no — enough writes to fill
    /// the tier, so an engine that still accepts them also reaches "tier full → schedule a flush".
    /// </summary>
    [Fact]
    public async Task After_the_write_path_closes_no_write_lands_and_no_flush_is_running_when_tiers_are_freed()
    {
        var engine = NewEngine();
        Write(engine, 100, LogLevel.Information);   // the final flush has something to do

        int accepted = -1, runningAtFree = -1, heavyAtFree = -1;
        engine._beforeTiersFreed = () =>
        {
            accepted = 0;
            for (int i = 0; i < 20_000 && TryWrite(engine, i, LogLevel.Information); i++)
                accepted++;

            var flushes   = engine.InFlightFlushTasks();
            runningAtFree = flushes.Count(static t => !t.IsCompleted);
            heavyAtFree   = engine.HeavyPhasesInFlight;

            // An engine without the gate may just have swapped a tier into a flush. Let it end
            // before the teardown frees what it reads: the failure is the numbers above, not a
            // crashed test host.
            try { Task.WaitAll(flushes, TimeSpan.FromSeconds(30)); } catch { /* observed above */ }
            SpinWait.SpinUntil(() => engine.HeavyPhasesInFlight <= 0, TimeSpan.FromSeconds(30));
        };

        await engine.DisposeAsync();

        Assert.Equal(0, accepted);
        Assert.Equal(0, runningAtFree);
        Assert.Equal(0, heavyAtFree);
    }

    /// <summary>
    /// <see cref="StorageEngine.FlushHotTierAsync"/> (the RAM-pressure flush) runs its heavy phase
    /// inline and was never registered where shutdown looked, so shutdown freed the tier it was
    /// writing. Parked between its two level files, the flush must hold shutdown — the first
    /// DisposeAsync and a second, concurrent one alike.
    ///
    /// <para>The "must not complete" side is safe by construction: on a correct engine neither
    /// dispose can finish while this thread sits in the hook, so the bounded wait always runs
    /// out. If one did finish, the hook throws before the flush reads its tier again.</para>
    /// </summary>
    [Fact]
    public async Task Shutdown_waits_for_an_inline_flush_parked_between_two_levels()
    {
        var log    = new CapturingLogger();
        var engine = NewEngine(out string dir, log);
        Write(engine, 50, LogLevel.Information);
        Write(engine, 50, LogLevel.Error);
        string segDir = Path.Combine(dir, "segments");

        Task? first = null, second = null;
        bool firstDoneInside = true, secondDoneInside = true;
        int  calls = 0;
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Increment(ref calls) != 1) return;

            first  = engine.DisposeAsync().AsTask();
            second = engine.DisposeAsync().AsTask();
            Task.WhenAny(Task.WhenAny(first, second), Task.Delay(TimeSpan.FromMilliseconds(500))).Wait();
            firstDoneInside  = first.IsCompleted;
            secondDoneInside = second.IsCompleted;

            if (firstDoneInside || secondDoneInside)
                throw new InvalidOperationException("test: shutdown finished under a running flush — aborting the flush before it reads its freed tier");
        };

        var flush = Task.Run(() => engine.FlushHotTierAsync());
        try { await flush.WaitAsync(TimeSpan.FromSeconds(60)); }
        catch (Exception) when (firstDoneInside || secondDoneInside) { /* reported below */ }

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.False(firstDoneInside,  "DisposeAsync completed while FlushHotTierAsync was between two levels");
        Assert.False(secondDoneInside, "a second DisposeAsync completed while the first was still waiting for the flush");
        Assert.Equal(2, calls);

        await Task.WhenAll(first!, second!).WaitAsync(TimeSpan.FromSeconds(60));

        Assert.DoesNotContain(log.Snapshot(), static l => l.Contains("Segment flush failed", StringComparison.Ordinal));
        Assert.Equal(2, Directory.GetFiles(segDir, "*.seg").Length);   // both levels, then the marker, then the free
    }

    /// <summary>
    /// A tier retired while a query still reads it is freed when the last reader closes — and
    /// shutdown used to free it regardless. Held across DisposeAsync, the reader keeps its tier
    /// alive and readable; shutdown waits for it; closing it frees the tier.
    /// </summary>
    [Fact]
    public async Task A_retired_tier_a_reader_holds_outlives_shutdown_until_the_reader_closes()
    {
        var engine = NewEngine();
        Write(engine, 50, LogLevel.Information);
        var tier   = engine.LiveHotTier;
        var reader = engine.OpenHotTierReader();   // captures `tier` as its current tier

        Task? dispose = null;
        bool freedWhileHeld = true;
        int  readWhileHeld  = -1;
        try
        {
            await engine.FlushHotTierAsync();
            Assert.False(tier.IsDisposed, "precondition: the flush retired the tier without freeing it under the reader");

            dispose = engine.DisposeAsync().AsTask();
            await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromMilliseconds(500)));

            freedWhileHeld = tier.IsDisposed;
            if (!freedWhileHeld)
                readWhileHeld = reader.ReadAll().Count();   // only when it is still there to read
            Assert.False(dispose.IsCompleted, "DisposeAsync did not wait for the open reader");
        }
        finally
        {
            reader.Dispose();
            if (dispose is not null) await dispose.WaitAsync(TimeSpan.FromSeconds(60));
        }

        Assert.False(freedWhileHeld, "shutdown freed a tier an open reader still held");
        Assert.Equal(50, readWhileHeld);
        Assert.True(tier.IsDisposed, "the tier was not freed once its last reader closed");
    }

    /// <summary>
    /// Past the shutdown budget a reader still open does not get its tier freed under it: the
    /// tier stays allocated with an Error, a new snapshot is refused, and the reader's own close
    /// frees it.
    /// </summary>
    [Fact]
    public async Task A_reader_open_past_the_shutdown_budget_keeps_its_tier_and_frees_it_on_close()
    {
        var log    = new CapturingLogger();
        var engine = NewEngine(log);
        engine._shutdownWaitBudget = TimeSpan.FromMilliseconds(200);
        Write(engine, 50, LogLevel.Information);
        var tier   = engine.LiveHotTier;
        var reader = engine.OpenHotTierReader();

        try
        {
            await engine.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(60));

            Assert.False(tier.IsDisposed);
            Assert.Contains(log.Snapshot(), static l => l.StartsWith("Error:", StringComparison.Ordinal) && l.Contains("reader(s) still open", StringComparison.Ordinal));
            Assert.Equal(50, reader.ReadAll().Count());
            Assert.Throws<ObjectDisposedException>(() => engine.OpenHotTierReader());
        }
        finally { reader.Dispose(); }

        Assert.True(tier.IsDisposed);
    }

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private readonly List<string> _lines = [];

        public string[] Snapshot() { lock (_lines) return [.. _lines]; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lines) _lines.Add($"{logLevel}: {formatter(state, exception)}");
        }
    }
}
