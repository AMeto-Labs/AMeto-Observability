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
