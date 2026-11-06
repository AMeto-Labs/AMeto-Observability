using System.Diagnostics;
using Ameto.Core;
using Ameto.Server;
using Microsoft.Extensions.Options;

namespace Ameto.Integration.Tests;

/// <summary>
/// #137: <c>QueryGuard.TryEnterAsync</c> waited with <c>SemaphoreSlim.WaitAsync(QueueWait, ct)</c>, which
/// enqueues the waiter before it allocates; an out-of-memory there left a waiter in the queue, the next
/// Release handed the slot to nobody, and one of the 2–16 slots was gone until a restart. The guard
/// now tries the semaphore and pauses in its own frame, so a failed wait costs the request, never a slot.
/// </summary>
public sealed class QueryGuardTests
{
    private static QueryGuard Guard(int slots, TimeSpan queueWait) => new(Options.Create(new ServerOptions
    {
        Query = new QueryOptions { MaxConcurrent = slots, QueueWait = queueWait },
    }));

    [Fact]
    public async Task A_wait_that_fails_never_costs_a_slot()
    {
        var guard = Guard(slots: 1, queueWait: TimeSpan.FromSeconds(2));
        var held  = await guard.TryEnterAsync(default);
        Assert.NotNull(held);

        // The pause of a contended wait could not be set up — the failure that used to leave a dead
        // waiter in the semaphore's queue.
        guard._beforeQueuePollForTest = static () => throw new OutOfMemoryException("test: the pause ran out of memory");
        await Assert.ThrowsAsync<OutOfMemoryException>(async () => await guard.TryEnterAsync(default));
        guard._beforeQueuePollForTest = null;

        // The slot comes back to the next request, not to the failed wait.
        held.Value.Dispose();
        var next = await guard.TryEnterAsync(default);
        Assert.NotNull(next);
        next.Value.Dispose();
    }

    [Fact]
    public async Task A_slot_freed_during_the_wait_is_taken()
    {
        var guard = Guard(slots: 1, queueWait: TimeSpan.FromSeconds(5));
        var held  = await guard.TryEnterAsync(default);
        Assert.NotNull(held);

        var waiting = guard.TryEnterAsync(default).AsTask();
        await Task.Delay(50);
        Assert.False(waiting.IsCompleted, "nothing was free yet");

        held.Value.Dispose();
        var sw   = Stopwatch.StartNew();
        var next = await waiting.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.NotNull(next);
        Assert.True(sw.ElapsedMilliseconds < 1000, $"the freed slot was seen only after {sw.ElapsedMilliseconds} ms");
        next.Value.Dispose();
    }

    [Fact]
    public async Task No_slot_within_QueueWait_is_a_refusal_not_a_wait_forever()
    {
        var guard = Guard(slots: 1, queueWait: TimeSpan.FromMilliseconds(150));
        var held  = await guard.TryEnterAsync(default);
        Assert.NotNull(held);

        var sw     = Stopwatch.StartNew();
        var result = await guard.TryEnterAsync(default);
        Assert.Null(result);
        Assert.InRange(sw.ElapsedMilliseconds, 100, 3000);

        held.Value.Dispose();
    }

    [Fact]
    public async Task A_cancelled_request_stops_waiting()
    {
        var guard = Guard(slots: 1, queueWait: TimeSpan.FromSeconds(10));
        var held  = await guard.TryEnterAsync(default);
        Assert.NotNull(held);

        using var cts = new CancellationTokenSource(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await guard.TryEnterAsync(cts.Token));

        // And the slot it never got is still the holder's to give back.
        held.Value.Dispose();
        var next = await guard.TryEnterAsync(default);
        Assert.NotNull(next);
        next.Value.Dispose();
    }

    [Fact]
    public async Task Unlimited_slots_never_wait()
    {
        var guard = Guard(slots: -1, queueWait: TimeSpan.FromMilliseconds(10));
        var leases = new List<QueryGuard.Lease?>();
        for (int i = 0; i < 64; i++) leases.Add(await guard.TryEnterAsync(default));
        Assert.All(leases, l => Assert.NotNull(l));
    }
}
