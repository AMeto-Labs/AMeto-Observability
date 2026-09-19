namespace Ameto.Storage.Tests;

/// <summary>
/// A clock for the metric engine's AGE tests — the hot tier's age, a series' last append, the
/// stale sweep's two-hour bar. Time moves only in <see cref="Advance"/>.
///
/// <para>Local to this assembly rather than <c>tests/Shared/ManualTimeProvider.cs</c>, which is
/// linked into <c>Ameto.Core.Tests</c> and <c>Ameto.Indexing.Tests</c> but not into this project
/// — adding the link is a change to <c>Ameto.Storage.Tests.csproj</c>, which the work package
/// that needed this clock does not own. It is deliberately the smaller thing: only
/// <see cref="GetUtcNow"/> is overridden, so timers and <c>Task.Delay</c> keep running on the
/// system clock and nothing in a test's control flow depends on this class scheduling anything.
/// A test that needs fake TIMERS should link the shared provider rather than grow this one.</para>
/// </summary>
internal sealed class MetricTestClock : TimeProvider
{
    private long _utcTicks;

    public MetricTestClock(DateTimeOffset? start = null) =>
        _utcTicks = (start ?? DateTimeOffset.UtcNow).UtcTicks;

    public override DateTimeOffset GetUtcNow() => new(Volatile.Read(ref _utcTicks), TimeSpan.Zero);

    public void Advance(TimeSpan by)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(by, TimeSpan.Zero);
        Interlocked.Add(ref _utcTicks, by.Ticks);
    }
}
