using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// #130: a retention window of centuries wrapped the trace cutoff into the future (every trace file
/// deleted, every hour) or threw before the prune ran. The arithmetic now saturates and the day
/// counts are bounded at the door.
/// </summary>
public class RetentionMathTests
{
    [Fact]
    public void The_cutoff_is_now_less_the_TTL_and_nothing_past_the_epoch()
    {
        var now = new DateTimeOffset(2026, 11, 7, 0, 0, 0, TimeSpan.Zero);

        Assert.Equal((now.ToUnixTimeMilliseconds() - 30L * 86_400_000) * 1_000_000L,
                     RetentionMath.CutoffNano(now, TimeSpan.FromDays(30)));
        Assert.Equal(now.ToUnixTimeMilliseconds() * 1_000_000L, RetentionMath.CutoffNano(now, TimeSpan.Zero));

        // 200 000 days ≈ 548 years: the multiplied-out form wrapped this into the future.
        Assert.Equal(long.MinValue, RetentionMath.CutoffNano(now, TimeSpan.FromDays(200_000)));
        // 800 000 days: DateTimeOffset.Subtract threw here.
        Assert.Equal(long.MinValue, RetentionMath.CutoffNano(now, TimeSpan.FromDays(800_000)));
        Assert.Equal(long.MinValue, RetentionMath.CutoffNano(now, TimeSpan.MaxValue));
        // A negative TTL is no TTL: the cutoff is now.
        Assert.Equal(now.ToUnixTimeMilliseconds() * 1_000_000L, RetentionMath.CutoffNano(now, TimeSpan.FromDays(-5)));
    }

    [Theory]
    [InlineData(int.MinValue, 1)]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(90, 90)]
    [InlineData(36_500, 36_500)]
    [InlineData(36_501, 36_500)]
    [InlineData(200_000, 36_500)]
    [InlineData(int.MaxValue, 36_500)]
    public void A_day_count_from_anywhere_becomes_a_TTL_inside_the_bounds(int days, int expectedDays)
        => Assert.Equal(TimeSpan.FromDays(expectedDays), RetentionMath.TtlOf(days));

    [Theory]
    [InlineData(0, false)]
    [InlineData(1, true)]
    [InlineData(36_500, true)]
    [InlineData(36_501, false)]
    [InlineData(200_000, false)]
    public void Only_one_to_a_hundred_years_is_a_valid_window(int days, bool valid)
        => Assert.Equal(valid, RetentionMath.IsValidDays(days));

    [Fact]
    public void A_segments_expiry_survives_a_TTL_of_centuries()
    {
        // Before: new DateTimeOffset(max).Add(ttl) — a TTL reaching past year 9999 threw out of
        // EnforceRetentionAsync, and with it the metric and trace prunes that follow it in the loop.
        var now     = new DateTimeOffset(2026, 11, 7, 0, 0, 0, TimeSpan.Zero);
        var segment = new SegmentInfo
        {
            Id = default, NodeId = default, FilePath = "x.seg",
            MinTimestampTicks = now.AddDays(-10).UtcTicks, MaxTimestampTicks = now.AddDays(-9).UtcTicks,
            EventCount = 1, MinLevel = LogLevel.Information, CompressedBytes = 1, UncompressedBytes = 1,
        };

        Assert.True(segment.IsExpired(Policy(TimeSpan.FromDays(7)), now));
        Assert.False(segment.IsExpired(Policy(TimeSpan.FromDays(30)), now));
        Assert.False(segment.IsExpired(Policy(TimeSpan.FromDays(800_000)), now));
        Assert.False(segment.IsExpired(Policy(TimeSpan.MaxValue), now));
    }

    private static RetentionPolicy Policy(TimeSpan ttl) => new(new Dictionary<LogLevel, TimeSpan>
    {
        [LogLevel.Verbose] = ttl, [LogLevel.Debug] = ttl, [LogLevel.Information] = ttl,
        [LogLevel.Warning] = ttl, [LogLevel.Error] = ttl, [LogLevel.Fatal] = ttl,
    });
}
