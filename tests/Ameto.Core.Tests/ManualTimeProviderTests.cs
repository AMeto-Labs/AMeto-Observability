using Ameto.Testing;

namespace Ameto.Core.Tests;

/// <summary>
/// The test clock itself (<c>tests/Shared/ManualTimeProvider.cs</c>). The tests that catch a production
/// conversion made in the wrong units rely on this clock converting RIGHT: if <c>Advance</c> moved its
/// timestamps by TimeSpan ticks at every frequency, a swapped conversion would be exact against it
/// again. Expected values are literals, not the clock's own formula.
/// </summary>
public sealed class ManualTimeProviderTests
{
    [Fact]
    public void The_default_frequency_is_not_a_TimeSpan_tick()
    {
        Assert.Equal(1_000_000_000L, new ManualTimeProvider().TimestampFrequency);
    }

    [Theory]
    [InlineData(ManualTimeProvider.TickFrequency,       1_500_000L)]
    [InlineData(ManualTimeProvider.NanosecondFrequency, 150_000_000L)]
    public void Advance_moves_the_timestamp_by_the_span_at_the_clocks_own_frequency(long frequency, long stampsIn150Ms)
    {
        var clock = new ManualTimeProvider(frequency);
        Assert.Equal(frequency, clock.TimestampFrequency);

        long           start = clock.GetTimestamp();
        DateTimeOffset utc   = clock.GetUtcNow();
        clock.Advance(TimeSpan.FromMilliseconds(150));

        Assert.Equal(stampsIn150Ms, clock.GetTimestamp() - start);
        Assert.Equal(TimeSpan.FromMilliseconds(150), clock.GetUtcNow() - utc);
        Assert.Equal(TimeSpan.FromMilliseconds(150), clock.GetElapsedTime(start));   // TimeProvider's own conversion back
    }

    [Theory]
    [InlineData(ManualTimeProvider.TickFrequency,       1_000_000L)]
    [InlineData(ManualTimeProvider.NanosecondFrequency, 100_000_000L)]
    public void A_timer_fires_at_its_due_time_and_each_period_at_the_clocks_own_frequency(long frequency, long stampsIn100Ms)
    {
        var  clock = new ManualTimeProvider(frequency);
        long start = clock.GetTimestamp();
        var  fired = new List<long>();
        using var timer = clock.CreateTimer(
            _ => fired.Add(clock.GetTimestamp() - start), null, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(50));

        clock.Advance(TimeSpan.FromMilliseconds(100) - TimeSpan.FromTicks(1));
        Assert.Empty(fired);

        clock.Advance(TimeSpan.FromTicks(1));
        Assert.Equal(new[] { stampsIn100Ms }, fired);

        clock.Advance(TimeSpan.FromMilliseconds(100));
        Assert.Equal(new[] { stampsIn100Ms, stampsIn100Ms * 3 / 2, stampsIn100Ms * 2 }, fired);
    }
}
