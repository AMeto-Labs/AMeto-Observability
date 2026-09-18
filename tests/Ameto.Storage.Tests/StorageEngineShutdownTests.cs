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

    /// <summary>
    /// Bound on a step that, on a correct engine, a signal ends at once. Only a hang reaches it, so
    /// it decides nothing but how long a hung test takes to report.
    /// </summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

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
    ///
    /// <para>Only writes that ARRIVE after the close are covered here: by this seam shutdown already
    /// holds the swap lock and has frozen the live tier, so a flush or a write that got in before
    /// the close cannot race it any more. Those two are the next tests.</para>
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
    /// A flush that took the swap lock and passed its close re-check just before shutdown closed the
    /// write path is committed to swapping the live tier, but has not counted a heavy phase yet.
    /// Shutdown must not read that count until the flush lets go of the lock. If it reads it
    /// sooner, it sees zero, frees the live tier, and the flush then swaps it and writes freed
    /// memory out.
    ///
    /// <para>The flush F starts inside the final flush's heavy phase (swap lock free, writes still
    /// open) and parks at the swap seam, holding the lock. It is released only when shutdown's take
    /// of that lock has to wait for it. An engine that does not take the lock never waits, so it
    /// reaches the free with F still parked. That is recorded there, and then F runs to the end
    /// BEFORE the free, so such an engine fails the assertion instead of faulting the test host.</para>
    /// </summary>
    [Fact]
    public async Task A_flush_holding_the_swap_lock_when_the_write_path_closes_ends_before_its_tier_is_freed()
    {
        var log    = new CapturingLogger();
        var engine = NewEngine(out string dir, log);
        Write(engine, 100, LogLevel.Information);   // tier X, for the final flush
        string segDir = Path.Combine(dir, "segments");

        var fParked  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseF = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        HotTierSegment? y = null;
        Task? f = null;
        int  armF = 0, levels = 0, writtenToY = -1;
        bool fWroteYBeforeFree = false, yFreedDuringItsFlush = true, releasedByLockWait = false;

        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Increment(ref levels) == 1)
            {
                // The final flush's heavy phase: X is swapped out, the lock is free, writes are open.
                y = engine.LiveHotTier;
                writtenToY = 0;
                for (int i = 0; i < 50 && TryWrite(engine, i, LogLevel.Information); i++)
                    writtenToY++;

                Volatile.Write(ref armF, 1);
                f = Task.Run(() => engine.FlushHotTierAsync());
                if (!fParked.Task.Wait(HangGuard))
                    throw new InvalidOperationException("test: the second flush never reached the swap seam");
            }
            else
            {
                // F's heavy phase, just after it wrote Y's level out of Y's memory.
                yFreedDuringItsFlush = y!.IsDisposed;
            }
        };
        engine._beforeSwap = () =>
        {
            if (Interlocked.Exchange(ref armF, 0) == 0) return;   // the final flush's own swap
            fParked.TrySetResult();
            releaseF.Task.Wait(HangGuard);
        };
        engine._onWaitingForFlushLock = () =>
        {
            releasedByLockWait = true;
            releaseF.TrySetResult();
        };
        engine._beforeTiersFreed = () =>
        {
            fWroteYBeforeFree = Volatile.Read(ref levels) >= 2;

            // Only an engine that never waited for F's lock gets here with F still parked. Let F
            // finish before the free, so it never reads the memory this is about to release.
            if (releaseF.TrySetResult())
                try { f?.Wait(HangGuard); } catch { /* the assertions below report it */ }
        };

        await engine.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(60));
        Assert.NotNull(f);
        await f!.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.Equal(50, writtenToY);
        Assert.True(fWroteYBeforeFree,
            "shutdown reached the free while a flush that took the swap lock before the close had not swapped yet — " +
            "it counted nothing, so the tier that flush then swaps and reads would be freed under it");
        Assert.True(releasedByLockWait, "shutdown's take of the swap lock never waited for the flush holding it");
        Assert.False(yFreedDuringItsFlush, "the tier the flush swapped was freed during its heavy phase");
        Assert.DoesNotContain(log.Snapshot(), static l => l.Contains("Segment flush failed", StringComparison.Ordinal));
        Assert.Equal(2, Directory.GetFiles(segDir, "*.seg").Length);   // X's level, then Y's
        Assert.True(y!.IsDisposed, "the tier was not freed once its flush published it");
    }

    /// <summary>
    /// A write that passed the shutdown gate just before the write path closed has not captured its
    /// tier yet. Only the tier's Freeze can stop it landing in memory that shutdown is about to free.
    /// The write parks right after the gate and is released at the last seam before the free, and it
    /// must be refused there. An engine that does not freeze accepts it into the live tier it then
    /// frees. The write finishes before that free, so the failure shows as "accepted", not a crash.
    /// </summary>
    [Fact]
    public async Task A_write_past_the_gate_when_the_write_path_closes_is_refused_by_the_frozen_tier()
    {
        var engine = NewEngine();
        Write(engine, 100, LogLevel.Information);   // the final flush has something to do

        var parked  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int arm = 1;
        engine._afterWriteGate = () =>
        {
            if (Interlocked.Exchange(ref arm, 0) == 0) return;
            parked.TrySetResult();
            release.Task.Wait(HangGuard);
        };
        var write = Task.Run(() => TryWrite(engine, 100, LogLevel.Information));
        await parked.Task.WaitAsync(HangGuard);   // past the gate before shutdown starts

        bool finishedBeforeFree = false, acceptedIntoFreedTier = true;
        engine._beforeTiersFreed = () =>
        {
            release.TrySetResult();
            finishedBeforeFree = write.Wait(HangGuard);
            if (finishedBeforeFree) acceptedIntoFreedTier = write.Result;
        };

        await engine.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(60));

        Assert.True(finishedBeforeFree, "the write parked past the gate did not finish once released");
        Assert.False(acceptedIntoFreedTier,
            "a write that passed the gate before the write path closed was accepted into the live tier shutdown then freed");
    }

    /// <summary>
    /// <see cref="StorageEngine.FlushHotTierAsync"/> (the RAM-pressure flush) runs its heavy phase
    /// inline and was never registered where shutdown looked, so shutdown freed the tier it was
    /// writing. Parked between its two level files, the flush must hold shutdown — the first
    /// DisposeAsync and a second, concurrent one alike.
    ///
    /// <para>The verdict is which happens first: a DisposeAsync completing, or shutdown starting its
    /// wait for heavy phases (a seam that fires only when there is one to wait for). No timer
    /// decides it. On a correct engine the wait always comes first, and while this thread sits in
    /// the hook nothing can complete. An engine that did not count this flush never waits: a
    /// dispose completes having freed the tier, and the hook then throws before the flush reads
    /// that tier again.</para>
    /// </summary>
    [Fact]
    public async Task Shutdown_waits_for_an_inline_flush_parked_between_two_levels()
    {
        var log    = new CapturingLogger();
        var engine = NewEngine(out string dir, log);
        Write(engine, 50, LogLevel.Information);
        Write(engine, 50, LogLevel.Error);
        string segDir = Path.Combine(dir, "segments");

        var waitingForFlush = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine._onWaitingForHeavyPhases = () => waitingForFlush.TrySetResult();

        Task? first = null, second = null;
        bool waitedFirst = false, firstDoneInside = true, secondDoneInside = true;
        int  calls = 0;
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Increment(ref calls) != 1) return;

            first  = engine.DisposeAsync().AsTask();
            second = engine.DisposeAsync().AsTask();
            var winner = Task.WhenAny(first, second, waitingForFlush.Task, Task.Delay(HangGuard)).Result;
            waitedFirst      = ReferenceEquals(winner, waitingForFlush.Task);
            firstDoneInside  = first.IsCompleted;
            secondDoneInside = second.IsCompleted;

            if (!waitedFirst || firstDoneInside || secondDoneInside)
                throw new InvalidOperationException("test: shutdown did not wait for this flush — aborting it before it reads its tier again");
        };

        var flush = Task.Run(() => engine.FlushHotTierAsync());
        try { await flush.WaitAsync(TimeSpan.FromSeconds(60)); }
        catch (Exception) when (!waitedFirst || firstDoneInside || secondDoneInside) { /* reported below */ }

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.False(firstDoneInside,  "DisposeAsync completed while FlushHotTierAsync was between two levels");
        Assert.False(secondDoneInside, "a second DisposeAsync completed while the first was still waiting for the flush");
        Assert.True(waitedFirst, "DisposeAsync neither completed nor started waiting for the flush within the hang guard");
        Assert.Equal(2, calls);

        await Task.WhenAll(first!, second!).WaitAsync(TimeSpan.FromSeconds(60));

        Assert.DoesNotContain(log.Snapshot(), static l => l.Contains("Segment flush failed", StringComparison.Ordinal));
        Assert.Equal(2, Directory.GetFiles(segDir, "*.seg").Length);   // both levels, then the marker, then the free
    }

    /// <summary>
    /// A tier retired while a query still reads it is freed when the last reader closes — and
    /// shutdown used to free it regardless. Held across DisposeAsync, the reader keeps its tier
    /// alive and readable; shutdown waits for it; closing it frees the tier.
    ///
    /// <para>No timer decides the verdict: either DisposeAsync completes first, or its reader wait
    /// starts first (a seam that fires only while a reader is open). The reader is enumerated only
    /// once that wait has started, with dispose still pending and the tier still allocated. At that
    /// point only this reader's close can move shutdown on, so an engine that does not wait fails
    /// before the test reads anything it might have freed.</para>
    /// </summary>
    [Fact]
    public async Task A_retired_tier_a_reader_holds_outlives_shutdown_until_the_reader_closes()
    {
        var engine = NewEngine();
        Write(engine, 50, LogLevel.Information);
        var tier   = engine.LiveHotTier;
        var reader = engine.OpenHotTierReader();   // captures `tier` as its current tier

        var waitingForReaders = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine._onWaitingForReaders = () => waitingForReaders.TrySetResult();

        Task? dispose = null;
        bool disposedFirst = false, waitedFirst = false, doneAtWait = true, freedAtWait = true;
        int  readWhileHeld = -1;
        try
        {
            await engine.FlushHotTierAsync();
            Assert.False(tier.IsDisposed, "precondition: the flush retired the tier without freeing it under the reader");

            dispose = engine.DisposeAsync().AsTask();
            var winner = await Task.WhenAny(dispose, waitingForReaders.Task, Task.Delay(HangGuard));
            disposedFirst = ReferenceEquals(winner, dispose);
            waitedFirst   = ReferenceEquals(winner, waitingForReaders.Task);

            if (waitedFirst)
            {
                doneAtWait  = dispose.IsCompleted;
                freedAtWait = tier.IsDisposed;
                if (!doneAtWait && !freedAtWait)
                    readWhileHeld = reader.ReadAll().Count();   // only while shutdown is provably parked on this reader
            }
        }
        finally
        {
            reader.Dispose();
            if (dispose is not null) await dispose.WaitAsync(TimeSpan.FromSeconds(60));
        }

        Assert.False(disposedFirst, "DisposeAsync did not wait for the open reader");
        Assert.True(waitedFirst, "DisposeAsync neither completed nor started waiting for the reader within the hang guard");
        Assert.False(doneAtWait, "DisposeAsync completed while it was waiting for the open reader");
        Assert.False(freedAtWait, "shutdown freed a tier an open reader still held");
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
