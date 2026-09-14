using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Server;

/// <summary>
/// The two bounds every search runs under: how long it may take, and how many may run at
/// once.
///
/// <para>Neither existed. A filter that matches nothing over an unbounded window walks the
/// whole catalog, and it did so for as long as it took — the request outliving the browser
/// tab that started it, because nothing on the server was watching the clock. And since a
/// search memory-maps segments and decompresses blocks in parallel, a few dashboards
/// refreshing together were enough to take the machine, with every one of them getting
/// slower rather than any one of them being told to come back later.</para>
///
/// <para>A refused query is a 503 with <c>Retry-After</c>, which is a much better answer
/// than a queue that never drains.</para>
/// </summary>
internal sealed class QueryGuard
{
    private readonly SemaphoreSlim? _slots;

    public TimeSpan Timeout   { get; }
    public TimeSpan QueueWait { get; }

    public QueryGuard(IOptions<ServerOptions> options)
    {
        var q     = options.Value.Query;
        Timeout   = q.Timeout;
        QueueWait = q.QueueWait > TimeSpan.Zero ? q.QueueWait : TimeSpan.FromSeconds(5);

        int limit = q.MaxConcurrent switch
        {
            0   => Math.Clamp(Environment.ProcessorCount, 2, 16),
            < 0 => 0,                       // explicitly unlimited
            _   => q.MaxConcurrent,
        };
        _slots = limit > 0 ? new SemaphoreSlim(limit, limit) : null;
    }

    /// <summary>
    /// Waits (briefly) for a slot. Null means the server is saturated and the caller should
    /// refuse the request; otherwise dispose the lease when the query is finished.
    /// </summary>
    public async ValueTask<Lease?> TryEnterAsync(CancellationToken ct)
    {
        if (_slots is null) return new Lease(null);
        return await _slots.WaitAsync(QueueWait, ct).ConfigureAwait(false) ? new Lease(_slots) : null;
    }

    /// <summary>
    /// A cancellation token that is also cancelled when the budget runs out. The caller
    /// must tell the two cases apart — a cancelled REQUEST is a client walking away and
    /// needs no message; an expired budget is something the client has to be told.
    /// </summary>
    public QueryDeadline StartDeadline(CancellationToken requestAborted)
        => new(requestAborted, Timeout);

    internal readonly struct Lease(SemaphoreSlim? slots) : IDisposable
    {
        public void Dispose() => slots?.Release();
    }
}

/// <summary>
/// Links the request's cancellation with the query budget; see <see cref="TimedOut"/>.
///
/// <para>One instance serves ONE query, or — for a caller that runs many bounded queries
/// on one request, the live tail — many in turn: <see cref="TryRearm"/> re-arms the same
/// linked source and timer for the next poll instead of building a new linked source, a
/// registration on the request token and a timer per poll, and <see cref="Disarm"/>
/// stops the clock between polls so the budget is per poll, not per connection.</para>
/// </summary>
internal sealed class QueryDeadline : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly CancellationToken       _requestAborted;
    private readonly TimeSpan                _budget;

    public QueryDeadline(CancellationToken requestAborted, TimeSpan budget)
    {
        _requestAborted = requestAborted;
        _budget         = budget;
        _cts            = CancellationTokenSource.CreateLinkedTokenSource(requestAborted);
        if (budget > TimeSpan.Zero) _cts.CancelAfter(budget);
    }

    public CancellationToken Token => _cts.Token;

    /// <summary>True when the budget expired rather than the client disconnecting.</summary>
    public bool TimedOut => _cts.IsCancellationRequested && !_requestAborted.IsCancellationRequested;

    /// <summary>
    /// Stops the budget clock once a query has finished. Without it the timer started for
    /// one poll would keep running while the tail is parked waiting for a write, and fire
    /// into the next poll — or into nothing, cancelling a source the next
    /// <see cref="TryRearm"/> then finds unusable.
    /// </summary>
    public void Disarm()
    {
        if (_budget > TimeSpan.Zero) _cts.CancelAfter(Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// Readies the same source for the next query: drops the previous query's
    /// registrations and starts a fresh budget. False when the source was already
    /// cancelled — the request went away, or the previous budget expired before
    /// <see cref="Disarm"/> ran — in which case the caller must stop, as it would have on
    /// a fresh deadline that was cancelled at birth.
    /// </summary>
    public bool TryRearm()
    {
        if (!_cts.TryReset()) return false;
        if (_budget > TimeSpan.Zero) _cts.CancelAfter(_budget);
        return true;
    }

    public void Dispose() => _cts.Dispose();
}
