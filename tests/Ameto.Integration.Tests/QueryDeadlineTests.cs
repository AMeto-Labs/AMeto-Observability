using System.Diagnostics;
using System.Reflection;
using Ameto.Server;

namespace Ameto.Integration.Tests;

/// <summary>
/// The live tail re-arms ONE <see cref="QueryDeadline"/> per poll instead of building a
/// linked source, a request-token registration and a timer each time. What that must not
/// change: a poll is still bounded by its own budget, the budget does not run while the
/// tail is parked, and the client going away still cancels the very next poll.
/// </summary>
public sealed class QueryDeadlineTests
{
    /// <summary>
    /// The budget for the tests that watch it fire, on the real clock.
    ///
    /// <para>No fake clock can stand in here: <c>CancellationTokenSource.TryReset</c> refuses any source
    /// whose timer is not the runtime's own TimerQueueTimer, so a deadline on a
    /// <see cref="TimeProvider"/> could never be re-armed, and re-arming is the thing under test.
    /// The margins are therefore built into the numbers instead. Every stretch in which the budget
    /// must NOT fire is two or three adjacent calls, so a false failure needs the runner to
    /// deschedule the test for this whole budget between two statements — four times the longest
    /// stall seen on the two-core CI runner, where the old 100-150 ms budgets failed on one. Every
    /// assertion that the budget DID fire waits for it, bounded far past it.</para>
    /// </summary>
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(2);

    /// <summary>How long a test waits for a budget that must fire before calling it lost.</summary>
    private static readonly TimeSpan FireLimit = Budget + TimeSpan.FromSeconds(15);

    [Fact]
    public void A_rearmed_deadline_is_still_cancelled_by_the_request_going_away()
    {
        using var request  = new CancellationTokenSource();
        using var deadline = new QueryDeadline(request.Token, TimeSpan.FromMinutes(5));
        deadline.Disarm();

        Assert.True(deadline.TryRearm());
        Assert.False(deadline.Token.IsCancellationRequested);
        deadline.Disarm();

        Assert.True(deadline.TryRearm());
        request.Cancel();
        Assert.True(deadline.Token.IsCancellationRequested);
        Assert.False(deadline.TimedOut);          // a disconnect, not a budget
        Assert.False(deadline.TryRearm());        // and it stays that way
    }

    [Fact]
    public async Task The_budget_is_per_poll_and_does_not_run_while_parked()
    {
        using var request  = new CancellationTokenSource();
        using var deadline = new QueryDeadline(request.Token, Budget);

        // A poll that finishes inside its budget, then a park longer than the budget: the clock
        // was stopped, so nothing fires while parked and the next poll re-arms cleanly. A slow
        // runner only lengthens the park, which proves more.
        Assert.True(deadline.TryRearm());
        deadline.Disarm();
        await Task.Delay(Budget + TimeSpan.FromSeconds(1));
        Assert.False(deadline.Token.IsCancellationRequested);
        Assert.True(deadline.TryRearm());

        // A poll that overruns: its own budget expires, is reported as a timeout, and the
        // source cannot be re-armed afterwards — the tail must end, as it does today.
        Assert.True(await FiresAsync(deadline), $"the re-armed budget had not fired {FireLimit.TotalSeconds:0} s later");
        Assert.True(deadline.TimedOut);
        Assert.False(deadline.TryRearm());
    }

    [Fact]
    public async Task Registrations_of_a_finished_poll_do_not_fire_into_the_next()
    {
        using var request  = new CancellationTokenSource();
        using var deadline = new QueryDeadline(request.Token, Budget);

        int firedFromFirstPoll = 0;
        Assert.True(deadline.TryRearm());
        deadline.Token.Register(() => Interlocked.Increment(ref firedFromFirstPoll));
        deadline.Disarm();

        Assert.True(deadline.TryRearm());
        Assert.True(await FiresAsync(deadline), $"the second poll's budget had not fired {FireLimit.TotalSeconds:0} s later");
        Assert.True(deadline.TimedOut);

        // The token reads cancelled before the timer thread has run the callbacks, so give a callback
        // that should not exist the moment it would need. This bounds how surely a regression is
        // caught, never whether a correct deadline passes.
        await Task.Delay(200);
        Assert.Equal(0, Volatile.Read(ref firedFromFirstPoll));   // the first poll's callback was dropped by the re-arm
    }

    /// <summary>
    /// The window a refused re-arm has to cover on its own: the previous poll's budget timer
    /// has been QUEUED to fire (the poll ended right at its budget, the callback waits behind a
    /// starved pool) but has not run. <c>CancellationTokenSource.TryReset</c> refuses such a
    /// source, yet its token is not cancelled at that instant — so a TimedOut that read the
    /// token alone answered "not a timeout", and the tail broke off with no terminal frame.
    /// Reflection because the only production route in is a timer-versus-pool race a test can
    /// provoke but not force: this sets exactly the state that race leaves
    /// (<c>TimerQueueTimer._everQueued</c>, which <c>TryReset</c> reads) on a timer that is
    /// not really going to fire.
    /// </summary>
    [Fact]
    public void A_rearm_refused_by_a_queued_budget_timer_is_a_timeout_before_the_token_says_so()
    {
        using var request  = new CancellationTokenSource();
        using var deadline = new QueryDeadline(request.Token, TimeSpan.FromMinutes(5));
        Assert.True(deadline.TryRearm());

        MarkBudgetTimerQueued(deadline);

        Assert.False(deadline.TryRearm());
        Assert.False(deadline.Token.IsCancellationRequested);   // the callback has not run
        Assert.True(deadline.TimedOut);                         // but the budget is what ended it

        // A client that leaves meanwhile is a disconnect, not a timeout.
        request.Cancel();
        Assert.False(deadline.TimedOut);
    }

    /// <summary>Waits for the armed budget to cancel the source; false if it has not within <see cref="FireLimit"/>.</summary>
    private static async Task<bool> FiresAsync(QueryDeadline deadline)
    {
        var waited = Stopwatch.StartNew();
        while (!deadline.Token.IsCancellationRequested)
        {
            if (waited.Elapsed > FireLimit) return false;
            await Task.Delay(20);
        }
        return true;
    }

    private static void MarkBudgetTimerQueued(QueryDeadline deadline)
    {
        const BindingFlags Instance = BindingFlags.NonPublic | BindingFlags.Instance;

        var cts = typeof(QueryDeadline).GetField("_cts", Instance)?.GetValue(deadline) as CancellationTokenSource;
        Assert.NotNull(cts);
        object? timer = typeof(CancellationTokenSource).GetField("_timer", Instance)?.GetValue(cts);
        Assert.NotNull(timer);
        var everQueued = timer.GetType().GetField("_everQueued", Instance);
        Assert.True(everQueued is not null,
            $"{timer.GetType()} has no _everQueued field: the runtime's TryReset contract changed — re-check QueryDeadline.TryRearm");
        everQueued!.SetValue(timer, true);
    }
}
