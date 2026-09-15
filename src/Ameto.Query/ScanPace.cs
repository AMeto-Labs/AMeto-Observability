using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Ameto.Query;

/// <summary>
/// Hands a query's scan back to its consumer at least every interval of SYNCHRONOUS work, by
/// making the step that crosses the mark go asynchronous.
///
/// <para>Everything under <see cref="QueryExecutor.ExecuteAsync"/> is synchronous in practice.
/// The segment reader decodes blocks inline, the hot tier is RAM, and neither the merge nor the
/// lazy priming ever waits; the async signatures exist to fit the merge. A consumer therefore
/// never saw a <c>MoveNextAsync</c> that was still pending. The events stream's
/// <c>SseJsonWriter.WriteLogEventsAsync</c> sends the rows it has buffered exactly when the
/// scan makes it wait, so on a real scan it had no moment to send in. A sparse cold search that
/// found two rows back to back and its next match thirty seconds of block decode later held the
/// second row for those thirty seconds. Before rows coalesced, each one went out as it was
/// written, so that was a regression the buffer had brought in.</para>
///
/// <para>ONE PER QUERY, shared by every source the merge draws from: the hot tier and each
/// segment scan. The stretch that matters is the one between two rows reaching the consumer,
/// and the merge can spend it in several sources in turn. A page that primes fifty segments with
/// no match between two rows would never yield if each source kept its own clock. The merge
/// steps its sources one at a time, so the fields here see one caller at a time; the thread may
/// change across a yield, the caller does not.</para>
///
/// <para>Cheap by construction: a countdown per event, a clock read every
/// <see cref="EventsPerClockRead"/> events, and a thread-pool hop only when the mark has passed.
/// The hop ignores any synchronization context (<see cref="ConfigureAwaitOptions.ForceYielding"/>),
/// so a caller that blocks on a query from a single-threaded context cannot deadlock on it.</para>
///
/// <para>A BOUND, NOT A DEADLINE. A step that yields and then reaches its row before the consumer
/// has looked at the pending <c>ValueTask</c> is not a wait as far as the consumer can tell; the
/// next mark gives it another chance. On a scan that is actually slow the step after a yield runs
/// on until the next mark, which is exactly the stretch a consumer can use.</para>
/// </summary>
internal sealed class ScanPace
{
    /// <summary>
    /// Half the events stream's 100 ms frame hold: a row left buffered at the end of a burst goes
    /// out within the same bound as one that finds the backlog old when the next row is written.
    /// About twenty hops a second while a scan does nothing but scan.
    /// </summary>
    public static readonly TimeSpan DefaultInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Events between clock reads. Small enough that the time between reads stays far below the
    /// interval at any per-event cost a scan can have; large enough that the clock is not a
    /// per-event cost.
    /// </summary>
    private const int EventsPerClockRead = 64;

    private readonly long _intervalStamps;
    private long _lastYieldStamp;
    private int  _untilClockRead = EventsPerClockRead;

    /// <param name="interval">
    /// Synchronous work allowed between yields. Zero or less makes every clock read a yield.
    /// </param>
    public ScanPace(TimeSpan interval)
    {
        _intervalStamps = interval <= TimeSpan.Zero ? 0 : (long)(interval.TotalSeconds * Stopwatch.Frequency);
        _lastYieldStamp = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Called once for every event a source looks at, match or not. True when the scan has
    /// worked the interval since it last yielded (or since the query started): the source then
    /// awaits <see cref="Yield"/> and carries on with the same event.
    /// </summary>
    public bool Due()
    {
        if (--_untilClockRead > 0) return false;
        _untilClockRead = EventsPerClockRead;

        long now = Stopwatch.GetTimestamp();
        if (now - _lastYieldStamp < _intervalStamps) return false;
        _lastYieldStamp = now;
        return true;
    }

    /// <summary>
    /// A hop to the thread pool that never completes inline and never captures a synchronization
    /// context. Awaiting it allocates nothing beyond the state machine box the iterator creates on
    /// its first pending await, once.
    /// </summary>
    public static ConfiguredTaskAwaitable Yield() =>
        Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
}
