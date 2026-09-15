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
        using var deadline = new QueryDeadline(request.Token, TimeSpan.FromMilliseconds(150));

        // A poll that finishes inside its budget, then a long park: the clock was stopped,
        // so nothing fires while parked and the next poll re-arms cleanly.
        Assert.True(deadline.TryRearm());
        deadline.Disarm();
        await Task.Delay(400);
        Assert.False(deadline.Token.IsCancellationRequested);
        Assert.True(deadline.TryRearm());

        // A poll that overruns: its own budget expires, is reported as a timeout, and the
        // source cannot be re-armed afterwards — the tail must end, as it does today.
        await Task.Delay(400);
        Assert.True(deadline.Token.IsCancellationRequested);
        Assert.True(deadline.TimedOut);
        Assert.False(deadline.TryRearm());
    }

    [Fact]
    public async Task Registrations_of_a_finished_poll_do_not_fire_into_the_next()
    {
        using var request  = new CancellationTokenSource();
        using var deadline = new QueryDeadline(request.Token, TimeSpan.FromMilliseconds(100));

        int firedFromFirstPoll = 0;
        Assert.True(deadline.TryRearm());
        deadline.Token.Register(() => firedFromFirstPoll++);
        deadline.Disarm();

        Assert.True(deadline.TryRearm());
        await Task.Delay(300);                    // the second poll's budget expires
        Assert.True(deadline.TimedOut);
        Assert.Equal(0, firedFromFirstPoll);      // the first poll's callback was dropped by the re-arm
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
