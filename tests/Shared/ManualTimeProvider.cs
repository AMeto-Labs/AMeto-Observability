namespace Ameto.Testing;

/// <summary>
/// A clock that moves only when a test says so. Linked into every test project that drives a
/// production type through its <see cref="TimeProvider"/> seam.
///
/// <para>Why not wall time: the CI runner is a two-core machine running six test projects at once,
/// and a thread there can be descheduled for hundreds of milliseconds. A test that sleeps 20 ms to
/// stay inside a 50 ms window, or races a 150 ms <c>CancelAfter</c>, fails whenever that happens to
/// land in the wrong place. Here time passes only inside <see cref="Advance"/>, and every timer that
/// comes due fires synchronously, in due order, on the thread that advanced — so nothing runs
/// concurrently with the test, and an assertion made after <see cref="Advance"/> returns sees
/// everything that advance caused.</para>
///
/// <para>Timestamps are <see cref="TimeSpan"/> ticks (<see cref="TimestampFrequency"/> =
/// <see cref="TimeSpan.TicksPerSecond"/>), starting well away from zero so a cutoff computed as
/// "now minus an age" is never negative.</para>
/// </summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    private static readonly DateTimeOffset Epoch = new(2026, 9, 1, 0, 0, 0, TimeSpan.Zero);

    private readonly Lock              _lock   = new();
    private readonly List<ManualTimer> _timers = [];
    private long _now = TimeSpan.TicksPerDay;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => Volatile.Read(ref _now);

    public override DateTimeOffset GetUtcNow() => Epoch.AddTicks(GetTimestamp());

    /// <summary>Timers created and not yet disposed.</summary>
    public int ActiveTimers { get { lock (_lock) return _timers.Count; } }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new ManualTimer(this, callback, state);
        lock (_lock)
        {
            _timers.Add(timer);
            timer.ScheduleLocked(dueTime, period);
        }
        return timer;
    }

    /// <summary>
    /// Moves the clock forward by <paramref name="by"/>, stopping at each timer that comes due on
    /// the way to fire it — so a callback reads the time it was due at, and a periodic timer fires
    /// once per period crossed.
    /// </summary>
    public void Advance(TimeSpan by)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(by, TimeSpan.Zero);
        long target;
        lock (_lock) target = _now + by.Ticks;

        while (true)
        {
            ManualTimer? due = null;
            lock (_lock)
            {
                foreach (var t in _timers)
                    if (t.DueAt <= target && (due is null || t.DueAt < due.DueAt)) due = t;

                if (due is null)
                {
                    Volatile.Write(ref _now, target);
                    return;
                }

                if (due.DueAt > _now) Volatile.Write(ref _now, due.DueAt);
                // Rescheduled BEFORE the callback runs, as a real timer is: a callback that changes
                // or disposes its own timer must win over the period.
                due.DueAt = due.PeriodTicks > 0 ? due.DueAt + due.PeriodTicks : long.MaxValue;
            }
            due.Callback(due.State);
        }
    }

    private sealed class ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state) : ITimer
    {
        public readonly TimerCallback Callback = callback;
        public readonly object?       State    = state;
        public long DueAt       = long.MaxValue;   // guarded by the owner's lock
        public long PeriodTicks;                   // 0 = one-shot

        /// <summary>Same reading of the arguments as System.Threading.Timer: -1 ms = never, 0 period = one-shot.</summary>
        public void ScheduleLocked(TimeSpan dueTime, TimeSpan period)
        {
            DueAt       = dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : owner._now + Math.Max(0, dueTime.Ticks);
            PeriodTicks = period == Timeout.InfiniteTimeSpan || period <= TimeSpan.Zero ? 0 : period.Ticks;
        }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner._lock)
            {
                if (!owner._timers.Contains(this)) return false;
                ScheduleLocked(dueTime, period);
                return true;
            }
        }

        public void Dispose()
        {
            lock (owner._lock) owner._timers.Remove(this);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return default;
        }
    }
}
