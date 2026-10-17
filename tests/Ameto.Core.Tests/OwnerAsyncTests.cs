using System.Runtime.CompilerServices;

namespace Ameto.Core.Tests;

/// <summary>
/// Round-5 review M-A. <see cref="OwnerAsync"/>'s test seam was an <see cref="AsyncLocal{T}"/> its static
/// initializer created, and the type is initialized at the process's first owner-method suspension — the
/// first swap, spill or despill. An out of memory there failed the type for good: every later first
/// suspension threw <see cref="TypeInitializationException"/>, so no swap, spill or despill ran again and
/// log ingest answered 503 or 500 until a restart. The type has no initializer now: the seam is created
/// by the first test that asks for it, and a suspension reads a field that is null in production.
/// </summary>
public sealed class OwnerAsyncTests
{
    [Fact]
    public void OwnerAsync_has_no_static_initializer_that_could_fail()
    {
        Assert.Null(typeof(OwnerAsync).TypeInitializer);
    }

    /// <summary>The seam still reaches the flow that sets it, and what flows from it; cleared, nothing.</summary>
    [Fact]
    public async Task The_seam_reaches_the_flow_that_sets_it()
    {
        static void Suspend() => OwnerAsync.FirstSuspension(typeof(OwnerAsyncTests));

        OwnerAsync.FirstSuspensionFailsForTest.Value = t => t == typeof(OwnerAsyncTests);
        try
        {
            Assert.Throws<OutOfMemoryException>(Suspend);
            await Task.Run(() => Assert.Throws<OutOfMemoryException>(Suspend));   // flows with the context
            OwnerAsync.FirstSuspension(typeof(string));                          // another state machine: untouched
        }
        finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
        Suspend();
    }

    // ── A later suspension (round-6 review, R6-H3's class) ───────────────────

    /// <summary>What a probe method did on its way out.</summary>
    private sealed class Seen
    {
        public volatile bool Caught, FinallyRan, After;
    }

    /// <summary>
    /// The seam fails a LATER suspension the way a registration that fails does: the method's catch around the
    /// await runs, the finally around it does not (the compiler guards it with the state the suspension set), and
    /// the task faults with what was awaited still pending. The first suspension is not asked: a seam asked there
    /// throws at the yield, outside the try, and nothing is caught. Each builder in turn.
    /// </summary>
    [Fact]
    public async Task A_later_suspension_the_seam_fails_runs_the_catch_and_skips_the_finally()
    {
        await Check(static (s, never) => TaskMethod(s, never));
        await Check(static (s, never) => TaskOfTMethod(s, never));
        await Check(static (s, never) => ValueTaskOfTMethod(s, never).AsTask());

        static async Task Check(Func<Seen, Task, Task> method)
        {
            var never = new TaskCompletionSource();
            int asked = 0;
            OwnerAsync.LaterSuspensionFailsForTest.Value = (sm, awaiter) =>
                sm.Name.Contains("Method>", StringComparison.Ordinal) && Interlocked.Increment(ref asked) == 1;
            var seen = new Seen();
            Task run;
            try { run = method(seen, never.Task); }
            finally { OwnerAsync.LaterSuspensionFailsForTest.Value = null; }

            // The seam flows into the box with the context, as the runtime's registration runs in the method.
            var ex = await Assert.ThrowsAsync<OutOfMemoryException>(() => run.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Contains("later suspension", ex.Message, StringComparison.Ordinal);
            Assert.True(seen.Caught, "the catch around the failed await did not run");
            Assert.False(seen.FinallyRan, "the finally around the failed await ran: the seam threw before the state was set");
            Assert.False(seen.After, "the method went on past the await that failed");
            Assert.Equal(1, Volatile.Read(ref asked));
            Assert.False(never.Task.IsCompleted);
        }
    }

    /// <summary>Cleared, a later suspension is the runtime's: the same methods run to their end.</summary>
    [Fact]
    public async Task Cleared_the_later_suspensions_are_the_runtimes()
    {
        var seen = new Seen();
        var done = new TaskCompletionSource();
        var run  = TaskMethod(seen, done.Task);
        done.SetResult();
        await run.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(seen.After && seen.FinallyRan && !seen.Caught);
    }

    // ── A registration made again (round 7) ──────────────────────────────────

    /// <summary>
    /// A registration on a task that runs out of memory is made again (round 7): the method goes on as if nothing had
    /// failed — no catch, every finally, and a <c>lock</c> after the await gives its monitor back. Let into the method,
    /// such a failure skipped every finally until a suspension resumed, a lock statement's exit with them, and the
    /// monitor stayed with a pool thread for good. Each builder in turn; the third try succeeds.
    /// </summary>
    [Fact]
    public async Task A_registration_on_a_task_that_runs_out_of_memory_is_made_again_and_the_method_runs_on()
    {
        await Check(static (s, wait) => TaskMethod(s, wait));
        await Check(static (s, wait) => TaskOfTMethod(s, wait));
        await Check(static (s, wait) => ValueTaskOfTMethod(s, wait).AsTask());
        await Check(static (s, wait) => LockAfterTheAwait(s, wait));

        static async Task Check(Func<Seen, Task, Task> method)
        {
            var wait  = new TaskCompletionSource();
            int tries = 0;
            OwnerAsync.RegistrationFailsForTest.Value = (sm, awaiter, attempt) =>
            {
                if (!sm.Name.Contains("Method>", StringComparison.Ordinal) && !sm.Name.Contains("LockAfterTheAwait>", StringComparison.Ordinal))
                    return false;
                if (awaiter != typeof(TaskAwaiter)) return false;   // the await of `wait`, not the yield
                Interlocked.Increment(ref tries);
                return attempt < 2;
            };
            var seen = new Seen();
            Task run;
            try { run = method(seen, wait.Task); }
            finally { OwnerAsync.RegistrationFailsForTest.Value = null; }

            // Completed only once the third try is made: completed before, the await would not suspend at all.
            Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref tries) >= 3, TimeSpan.FromSeconds(10)), $"{tries} tries");
            wait.SetResult();
            await run.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(3, Volatile.Read(ref tries));
            Assert.False(seen.Caught, "the failed registration reached the method");
            Assert.True(seen.After && seen.FinallyRan, $"after the await {seen.After}, finally {seen.FinallyRan}");
            Assert.True(Monitor.TryEnter(Gate, TimeSpan.FromSeconds(5)), "the lock after the await kept its monitor");
            Monitor.Exit(Gate);
        }
    }

    /// <summary>
    /// A registration that runs out of memory at every try reaches the method after the last one, as before round 7:
    /// the catch around the await runs, the finally does not. The pauses are cut to nothing here.
    /// </summary>
    [Fact]
    public async Task A_registration_that_fails_at_every_try_reaches_the_method_after_the_last()
    {
        var wait  = new TaskCompletionSource();
        int tries = 0;
        OwnerAsync.RegistrationRetryPauseForTest.Value = static _ => 0;
        OwnerAsync.RegistrationFailsForTest.Value = (sm, awaiter, _) =>
            sm.Name.Contains("TaskMethod>", StringComparison.Ordinal) && awaiter == typeof(TaskAwaiter) && Interlocked.Increment(ref tries) > 0;
        var seen = new Seen();
        Task run;
        try { run = TaskMethod(seen, wait.Task); }
        finally
        {
            OwnerAsync.RegistrationFailsForTest.Value = null;
            OwnerAsync.RegistrationRetryPauseForTest.Value = null;
        }

        var ex = await Assert.ThrowsAsync<OutOfMemoryException>(() => run.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Contains("runtime's registration", ex.Message, StringComparison.Ordinal);
        Assert.Equal(OwnerAsync.RegistrationAttempts, Volatile.Read(ref tries));
        Assert.True(seen.Caught && !seen.FinallyRan && !seen.After, $"caught {seen.Caught}, finally {seen.FinallyRan}, after {seen.After}");
    }

    /// <summary>
    /// Only a task's awaiter is made again. The runtime catches any other awaiter's failure itself and rethrows it on
    /// the pool; one that reaches the builder (its rethrow out of memory too) may have registered first, and made
    /// again it would resume the method twice. So the seam's failure on a ValueTask's awaiter reaches the method at
    /// its first try.
    /// </summary>
    [Fact]
    public async Task A_registration_on_another_awaiter_is_not_made_again()
    {
        var wait  = new TaskCompletionSource();
        int tries = 0;
        OwnerAsync.RegistrationFailsForTest.Value = (sm, awaiter, _) =>
            sm.Name.Contains("ValueTaskAwaitMethod>", StringComparison.Ordinal) && awaiter == typeof(ValueTaskAwaiter)
            && Interlocked.Increment(ref tries) > 0;
        var seen = new Seen();
        Task run;
        try { run = ValueTaskAwaitMethod(seen, wait.Task); }
        finally { OwnerAsync.RegistrationFailsForTest.Value = null; }

        await Assert.ThrowsAsync<OutOfMemoryException>(() => run.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(1, Volatile.Read(ref tries));
        Assert.True(seen.Caught && !seen.FinallyRan, $"caught {seen.Caught}, finally {seen.FinallyRan}");
    }

    private static readonly object Gate = new();

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerTaskMethodBuilder))]
    private static async Task LockAfterTheAwait(Seen seen, Task wait)
    {
        await Task.Yield();
        try
        {
            try { await wait; }
            catch (OutOfMemoryException) { seen.Caught = true; }
            lock (Gate) seen.After = true;
        }
        finally { seen.FinallyRan = true; }
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerTaskMethodBuilder))]
    private static async Task ValueTaskAwaitMethod(Seen seen, Task wait)
    {
        await Task.Yield();
        try
        {
            try { await new ValueTask(wait); }
            catch (OutOfMemoryException) { seen.Caught = true; throw; }
            seen.After = true;
        }
        finally { seen.FinallyRan = true; }
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerTaskMethodBuilder))]
    private static async Task TaskMethod(Seen seen, Task wait)
    {
        await Task.Yield();   // the first suspension
        try
        {
            try { await wait; }   // a later one
            catch (OutOfMemoryException) { seen.Caught = true; throw; }
            seen.After = true;
        }
        finally { seen.FinallyRan = true; }
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerTaskMethodBuilder<>))]
    private static async Task<int> TaskOfTMethod(Seen seen, Task wait)
    {
        await Task.Yield();
        try
        {
            try { await wait; }
            catch (OutOfMemoryException) { seen.Caught = true; throw; }
            seen.After = true;
            return 1;
        }
        finally { seen.FinallyRan = true; }
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerValueTaskMethodBuilder<>))]
    private static async ValueTask<int> ValueTaskOfTMethod(Seen seen, Task wait)
    {
        await Task.Yield();
        try
        {
            try { await wait; }
            catch (OutOfMemoryException) { seen.Caught = true; throw; }
            seen.After = true;
            return 1;
        }
        finally { seen.FinallyRan = true; }
    }
}
