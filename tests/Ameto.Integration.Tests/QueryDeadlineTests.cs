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
}
