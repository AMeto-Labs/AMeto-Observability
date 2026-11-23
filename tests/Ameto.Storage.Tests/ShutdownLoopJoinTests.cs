using System.Buffers;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Testing;
using MsLogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// The teardown's joins of its four background loops, on the road where a join cannot be set up.
/// <see cref="StorageEngine.DisposeAsync"/> cancels the loops and awaits each in turn, in a try whose catch
/// goes on to the final flush: a loop that had failed must not stop the teardown before it. What the catch
/// also takes is the await's own registration running out of memory — past the owner builder's tries it is
/// thrown into the method with the compiler's state still that suspension's, and from there every finally
/// of the method is skipped until an await suspends and resumes (the builder's remarks). On an idle engine
/// none does: the teardown went on through the writer fence and the two lock statements after it with their
/// exits skipped, returned with no exception and the store Closed, and its only Error blamed the loop. A
/// write from another thread then blocked for good, and so did a reader, and anything else that takes one
/// of those locks.
///
/// <para>The three sections are methods of their own now, whose exits are real finallys, so nothing is left
/// locked. The teardown waits for a loop it could not join by polling, within its budget, and goes on to its
/// end; its line says that the wait could not be set up, and whether the loop then ended — not that the loop
/// had failed.</para>
///
/// <para>How a fact reaches one join and no other. The target loop and every loop joined before it are held
/// in their rounds at seams, so each of those joins suspends, and the first loop still running is the one
/// whose join is suspending. A callback on the shutdown token holds the teardown's <c>CancelAsync</c> until
/// the teardown has suspended on it, so that is its first suspension and every join a later one. The
/// later-suspension seam lets each earlier loop go at its join, which then resumes the teardown, and fails
/// the target's — once the loops after it, told to stop, have ended, so that no join after it suspends and
/// resumes the method: a resumption runs its finallys again and would hide what this is about. (Holding
/// the earlier loops is not for show. Left to end by themselves, one could end between its join's look at
/// it and the seam's, which then took the NEXT join for that one.)</para>
/// </summary>
public sealed class ShutdownLoopJoinTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-loopjoin-" + Guid.NewGuid().ToString("N"));

    /// <summary>
    /// Bound on a step that, on a correct engine, a signal ends at once. Only a hang reaches it, so
    /// it decides nothing but how long a hung test takes to report.
    /// </summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    public void Dispose() => TestDataDirectory.Delete(_root);


    /// <summary>How the loop whose join could not be set up ends while the teardown polls for it.</summary>
    public enum Ending
    {
        /// <summary>Let go 250 ms into the poll: a pause that can only give an engine that does not wait the time to return first.</summary>
        WhilePolled,
        /// <summary>Not within the budget (1 s): the teardown's poll runs out at the budget's deadline, not a moment later.</summary>
        Never,
        /// <summary>Its task faults during the poll: the line carries the loop's own failure, and the join's beside it.</summary>
        FailsWhilePolled,
    }

    /// <summary>
    /// The join of each loop, its registration failed with the loop still in its round and nothing in the live tier
    /// — no final flush comes to resume the method. The teardown goes on to its end: it returns with no exception and
    /// the store Closed, a write is refused and a reader turned away, each on a thread of its own and within the
    /// guard, the retire list's lock is free, and the live WAL and the counts store are closed. Before that it polls
    /// for the loop, as the join would have waited for it: a loop that ends meanwhile has ended when the teardown
    /// returns; one that does not is left behind at the budget's deadline — the shared one, not a budget of its own.
    /// The line says the join could not be set up and how the loop ended; "had failed" only of a loop that had.
    /// </summary>
    [Theory]
    [InlineData(0, "hot-tier age", Ending.WhilePolled)]
    [InlineData(1, "WAL flush",    Ending.WhilePolled)]
    [InlineData(2, "maintenance",  Ending.WhilePolled)]
    [InlineData(3, "despill",      Ending.WhilePolled)]
    [InlineData(0, "hot-tier age", Ending.Never)]
    [InlineData(1, "WAL flush",    Ending.Never)]
    [InlineData(2, "maintenance",  Ending.Never)]
    [InlineData(3, "despill",      Ending.Never)]
    [InlineData(0, "hot-tier age", Ending.FailsWhilePolled)]
    [InlineData(1, "WAL flush",    Ending.FailsWhilePolled)]
    [InlineData(2, "maintenance",  Ending.FailsWhilePolled)]
    [InlineData(3, "despill",      Ending.FailsWhilePolled)]
    public async Task A_join_that_cannot_be_set_up_leaves_nothing_locked_and_the_teardown_goes_on_to_its_end(
        int loop, string named, Ending ending)
    {
        string dir  = NewDir();
        var log     = new CapturingLogger();
        var scan    = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var despill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        StorageEngine engine;
        StorageEngine.HoldDespillForTest.Value = despill.Task;
        try { engine = Open(dir, scan.Task, log); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }

        // The target and the loops joined before it, each held in its round until let go.
        var held    = Enumerable.Range(0, loop + 1).Select(static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        var release = Enumerable.Range(0, loop + 1).Select(static _ => new ManualResetEventSlim()).ToArray();
        using var suspended = new ManualResetEventSlim();
        void Hold(string at)
        {
            int k = Array.IndexOf(Seams, at);
            if (k > loop) return;
            held[k].TrySetResult();
            release[k].Wait(HangGuard);
        }
        engine._beforeLoopWaitForTest       = Hold;                       // the age, WAL and maintenance loops, between two rounds
        engine._despillLoopIterationForTest = () => Hold("despill");     // the despill loop, at the top of a round
        var running = engine.BackgroundLoopsForTest;                     // the engine's own loops, for the way out
        var loops   = running;                                           // what the teardown joins

        // A loop that fails while polled for stands in for the target: a loop's guards keep the engine's own from failing.
        var failure = new InvalidOperationException("test: the loop failed while the teardown polled for it");
        var stand   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // The teardown polls for the loop it could not join.
        int polled = 0;
        long deadline = 0;
        engine._onPollingForLoopForTest = (at, budgetEnds) =>
        {
            if (at != named) return;
            Interlocked.Increment(ref polled);
            Volatile.Write(ref deadline, budgetEnds);
            switch (ending)
            {
                case Ending.WhilePolled:
                    _ = Task.Delay(250).ContinueWith(_ => release[loop].Set(), TaskScheduler.Default);
                    break;
                case Ending.Never:
                    // Here until the deadline: a poll on the shared budget then ends at once, one on a budget of its own does not.
                    Thread.Sleep((int)Math.Max(0, budgetEnds - Environment.TickCount64));
                    break;
                case Ending.FailsWhilePolled:
                    _ = Task.Delay(250).ContinueWith(_ => stand.SetException(failure), TaskScheduler.Default);
                    break;
            }
        };
        if (ending == Ending.Never) engine._shutdownWaitBudget = TimeSpan.FromSeconds(1);

        Task? dispose = null;
        try
        {
            scan.SetResult();
            despill.SetResult();
            await engine.CatalogLoaded.WaitAsync(HangGuard);
            await Task.WhenAll(held.Select(static h => h.Task)).WaitAsync(HangGuard);   // each of their joins suspends. Nothing is written.
            if (ending == Ending.FailsWhilePolled)
            {
                LoopField(loop).SetValue(engine, stand.Task);
                loops = engine.BackgroundLoopsForTest;
            }

            // The teardown's first suspension is its CancelAsync, held until the teardown has suspended on it.
            engine.ShutdownTokenForTest.Register(() => suspended.Wait(HangGuard));
            int failed = 0;
            OwnerAsync.FirstSuspensionFailsForTest.Value = stateMachine =>
            {
                if (IsTeardown(stateMachine)) suspended.Set();
                return false;
            };
            OwnerAsync.LaterSuspensionFailsForTest.Value = (stateMachine, awaiter) =>
            {
                if (!IsTeardown(stateMachine) || awaiter != typeof(TaskAwaiter)) return false;
                int joining = Array.FindIndex(loops, static t => !t.IsCompleted);   // held: only this seam lets it go
                if (joining < 0 || joining > loop) return false;
                if (joining < loop)
                {
                    release[joining].Set();   // an earlier join: its loop ends, and resumes the teardown
                    return false;
                }
                return SpinWait.SpinUntil(() => loops.Skip(loop + 1).All(static t => t.IsCompleted), HangGuard)
                    && Interlocked.Increment(ref failed) == 1;
            };
            try { dispose = engine.DisposeAsync().AsTask(); }
            finally
            {
                OwnerAsync.FirstSuspensionFailsForTest.Value = null;
                OwnerAsync.LaterSuspensionFailsForTest.Value = null;
            }

            // Whether the loop had ended, and when, read as the teardown returns.
            var atReturn = dispose.ContinueWith(_ => (Ended: loops[loop].IsCompleted, At: Environment.TickCount64), CancellationToken.None,
                                                TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            await dispose.WaitAsync(HangGuard);   // rethrows what the teardown threw: nothing, it goes on
            Assert.Equal(1, Volatile.Read(ref failed));   // setup: the target's join was a later suspension, and the seam failed it
            Assert.Equal(QueryAvailability.Closed, engine.Availability);

            // Nothing is left locked.
            Assert.False(OnAThreadOfItsOwn(() => TryWrite(engine), "A write (the writer lock)"));   // refused: the write path is closed
            Assert.IsType<ObjectDisposedException>(OnAThreadOfItsOwn(
                () => Record.Exception(() => engine.OpenHotTierReader().Dispose()), "A reader (the frozen-tier lock)"));   // snapshots are closed
            Assert.True(OnAThreadOfItsOwn(() => { lock (engine.RetireLockForTest) return true; }, "The retire list's lock"));
            // And what the teardown closes last is closed.
            Assert.True(IsDisposed(engine.LiveWalForTest!), "the live WAL is still open");
            Assert.True(IsDisposed(engine.CountsStoreForTest!), "the counts store is still open");

            // The loop it could not join was polled for: waited for — or left behind at the shared deadline, not later.
            Assert.Equal(1, Volatile.Read(ref polled));
            var (ended, returnedAt) = await atReturn;
            Assert.Equal(ending != Ending.Never, ended);
            if (ending == Ending.Never)
                Assert.True(returnedAt - Volatile.Read(ref deadline) < 500,
                    $"the teardown returned {returnedAt - Volatile.Read(ref deadline)} ms past its budget's deadline: it polled on a budget of its own");

            // And the line says what happened.
            var lines = log.Snapshot();
            if (ending == Ending.FailsWhilePolled)
            {
                Assert.Contains(lines, l => l.Level == MsLogLevel.Error && ReferenceEquals(l.Exception, failure)
                                            && l.Message.Contains($"The {named} loop had failed", StringComparison.Ordinal)
                                            && l.Message.Contains("could not set up its wait for the loop either: test: the registration",
                                                                  StringComparison.Ordinal));
                return;
            }
            Assert.Contains(lines, l => l.Level == MsLogLevel.Error
                                        && l.Message.Contains($"could not set up its wait for the {named} loop", StringComparison.Ordinal)
                                        && l.Message.Contains(ending == Ending.WhilePolled ? "has ended since" : "had not ended after",
                                                              StringComparison.Ordinal));
            Assert.DoesNotContain(lines, static l => l.Message.Contains("had failed", StringComparison.Ordinal));
        }
        finally
        {
            suspended.Set();
            foreach (var r in release) r.Set();
            dispose ??= engine.DisposeAsync().AsTask();
            try { await dispose.WaitAsync(HangGuard); }
            catch { /* the teardown's own failure is the fact's, above */ }
            await Task.WhenAny(Task.WhenAll(running), Task.Delay(HangGuard));
            if (!dispose.IsCompletedSuccessfully)
            {
                // A teardown that ended half way — an engine whose joins end it on this road — still holds these.
                engine.LiveWalForTest?.Dispose();
                engine.CountsStoreForTest?.Dispose();
            }
            foreach (var r in release) r.Dispose();
        }
    }

    /// <summary>The seams the loops are held at, by the order the teardown joins them (<see cref="StorageEngine.BackgroundLoopsForTest"/>).</summary>
    private static readonly string[] Seams = ["age", "wal", "maintenance", "despill"];

    /// <summary>The engine's field for each loop, in the same order.</summary>
    private static FieldInfo LoopField(int loop) =>
        typeof(StorageEngine).GetField(LoopFields[loop], BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly string[] LoopFields = ["_flushLoop", "_walFlushLoop", "_maintenanceLoop", "_despillLoop"];

    /// <summary>
    /// The rule behind the theory, for every await of the teardown at once — the joins above, and the waits it
    /// makes best-effort below them (the parked deletes' loop, the flushes in flight, the final flush, the spill
    /// closes), whose catches take a failed registration the same way and go on: no finally in
    /// <c>DisposeCoreAsync</c>'s own body. A lock statement, a using, a foreach over a disposable enumerator and a
    /// try/finally each compile to one, and each is skipped from such a failure until an await suspends and
    /// resumes — a lock's exit with it, for good. Such a section is a method of its own.
    /// </summary>
    [Fact]
    public void The_teardown_holds_no_finally_an_await_could_skip()
    {
        var stateMachine = typeof(StorageEngine).GetNestedTypes(BindingFlags.NonPublic).Single(static t => IsTeardown(t));
        var moveNext     = stateMachine.GetMethod("MoveNext", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;
        var finallys     = moveNext.GetMethodBody()!.ExceptionHandlingClauses
                                   .Where(static c => c.Flags == ExceptionHandlingClauseOptions.Finally);
        Assert.Empty(finallys);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static bool IsTeardown(Type stateMachine) => stateMachine.Name.Contains("<DisposeCoreAsync>", StringComparison.Ordinal);

    /// <summary>Whether a write-ahead log or a counts store has been disposed: its own flag, which neither says out loud.</summary>
    private static bool IsDisposed(object disposable) =>
        (bool)disposable.GetType().GetField("_disposed", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(disposable)!;

    /// <summary>
    /// Runs <paramref name="probe"/> on a thread of its own and returns what it returned; the fact fails if it does
    /// not return within the guard. Not on the pool: a lock the teardown kept belongs to the pool thread that ran the
    /// teardown, and that thread, back in the pool, may be the one handed the probe — which would then enter a lock
    /// it already holds and come back.
    /// </summary>
    private static T OnAThreadOfItsOwn<T>(Func<T> probe, string what)
    {
        T result = default!;
        ExceptionDispatchInfo? failure = null;
        var thread = new Thread(() =>
        {
            try { result = probe(); }
            catch (Exception ex) { failure = ExceptionDispatchInfo.Capture(ex); }
        }) { IsBackground = true, Name = "probe" };
        thread.Start();
        Assert.True(thread.Join(HangGuard), $"{what} did not return: the teardown kept its lock");
        failure?.Throw();
        return result;
    }

    private string NewDir()
    {
        string dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>
    /// An idle engine whose loops come round quickly — the age and WAL loops every 50 ms, maintenance as soon as the
    /// catalog has loaded — and whose boot scan waits for <paramref name="scanHeldUntil"/>, so that a test sets its
    /// seams before the maintenance loop's first round.
    /// </summary>
    private static StorageEngine Open(string dir, Task scanHeldUntil, Microsoft.Extensions.Logging.ILogger<StorageEngine> logger)
    {
        var opts = new ServerOptions
        {
            DataDirectory = dir,
            HotTier = new HotTierOptions
            {
                MaxSizeBytes     = 8 * 1024 * 1024,
                MaxAge           = TimeSpan.FromMilliseconds(50),
                WalFlushInterval = TimeSpan.FromMilliseconds(50),
            },
        };
        return new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            logger, maintenanceStartDelay: TimeSpan.Zero, bootScanHeldUntil: scanHeldUntil);
    }

    private static bool TryWrite(StorageEngine engine) =>
        engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = DateTime.UtcNow.Ticks,
            Level                    = LogLevel.Information,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
        }, Props(0));

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>Every line at every level, formatted, with the exception it carries.</summary>
    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private readonly List<(MsLogLevel Level, string Message, Exception? Exception)> _lines = [];

        public (MsLogLevel Level, string Message, Exception? Exception)[] Snapshot() { lock (_lines) return [.. _lines]; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(MsLogLevel logLevel) => true;

        public void Log<TState>(
            MsLogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            string message = formatter(state, exception);
            lock (_lines) _lines.Add((logLevel, message, exception));
        }
    }
}
