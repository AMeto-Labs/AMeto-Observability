using Ameto.Storage;

namespace Ameto.Server;

/// <summary>
/// Wake-up shared by every open live tail. The writer bumps a version; a tail that has
/// nothing left to send parks until that version moves.
///
/// <para>It carries no events. A tail still runs the ordinary query — the same filter, the
/// same level pruning, the same <c>(timestamp, id)</c> cursor — so the query path stays the
/// single source of truth about what a tail may see; all this replaces is the fixed 250 ms
/// sleep between polls. Handing the event itself to subscribers would mean copying its
/// payload out of the arena on the ingest path, which is the one place that cannot afford
/// it.</para>
///
/// <para><b>Ordering.</b> A waiter must read <see cref="Version"/> BEFORE it polls and pass
/// that value to <see cref="WaitAsync"/>. A write that lands during the poll then either
/// appears in the poll's own results or leaves the version changed — never neither. Reading
/// the version afterwards would drop exactly the events that arrived while the poll ran.</para>
/// </summary>
public sealed class LiveEventSignal
{
    private long _version;

    /// <summary>
    /// Non-null only while at least one waiter is parked. Signals do not allocate: the
    /// first one after a park hands this out and stores null, so the cost is one
    /// completion source per WAKE-UP, not per event.
    /// </summary>
    private TaskCompletionSource? _gate;

    /// <summary>Monotonic counter of committed events. Only inequality is meaningful.</summary>
    public long Version => Volatile.Read(ref _version);

    /// <summary>
    /// Called from the storage engine's write hook, once per committed event. Must stay
    /// cheap and must never throw: it runs on the single drain thread, between the hot-tier
    /// publish and the next event.
    /// </summary>
    public void Signal()
    {
        Interlocked.Increment(ref _version);

        // Nobody parked — done. This is the steady state on a busy server, where tails are
        // coalescing rather than sleeping.
        if (Volatile.Read(ref _gate) is null) return;

        Interlocked.Exchange(ref _gate, null)?.TrySetResult();
    }

    /// <summary>
    /// Parks until the version leaves <paramref name="seenVersion"/>, the token fires, or
    /// <paramref name="maxWait"/> elapses. Returns false only for the timeout — the caller
    /// uses that to prove the connection is still alive, and it doubles as a safety-net
    /// re-poll should a wake-up ever be missed.
    /// </summary>
    public async ValueTask<bool> WaitAsync(long seenVersion, TimeSpan maxWait, CancellationToken ct)
    {
        if (Volatile.Read(ref _version) != seenVersion) return true;

        var gate = Volatile.Read(ref _gate);
        if (gate is null)
        {
            var fresh = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            gate = Interlocked.CompareExchange(ref _gate, fresh, null) ?? fresh;
        }

        // The gate is installed; only now may an unchanged version be trusted. Without the
        // fence the store above and the load below could swap places, and a signal racing
        // this park could observe no gate while this park observes no signal — the classic
        // lost wake-up, which here means a tail that stops updating until the next event.
        Interlocked.MemoryBarrier();
        if (Volatile.Read(ref _version) != seenVersion) return true;

        try
        {
            await gate.Task.WaitAsync(maxWait, ct).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }
}

/// <summary>
/// Connects the storage engine's write hook to <see cref="LiveEventSignal"/> at startup —
/// the same wiring-by-hosted-service trick <c>IndexingWiring</c> uses to reach the engine
/// without Ameto.Storage having to know who is listening.
/// </summary>
internal sealed class LiveTailWiring(StorageEngine storage, LiveEventSignal signal)
    : Microsoft.Extensions.Hosting.IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        // CHAINED, not assigned: EventWritten is a single-slot property, and a plain
        // assignment here would silently unsubscribe whoever registered first (or be
        // silently unsubscribed by whoever registers next) depending on hosted-service
        // start order. Composing makes the order irrelevant.
        var previous = storage.EventWritten;
        storage.EventWritten = previous is null
            ? (_, _) => signal.Signal()
            : (h, t) => { previous(h, t); signal.Signal(); };
        return Task.CompletedTask;
    }

    /// <summary>
    /// Left wired on shutdown: unhooking would drop any chained subscriber with it, and the
    /// drain that runs while the host stops still has tails reading from it.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
