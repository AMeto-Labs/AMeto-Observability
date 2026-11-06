namespace Ameto.Core;

/// <summary>
/// The arithmetic the three retention paths — log segments, metric files, trace files — share,
/// and the bounds on the day counts a policy may hold.
///
/// <para>A window is 1 to <see cref="MaxDays"/> days (100 years). "Forever" is spelled as the maximum,
/// never as a number of days that overflows the cutoff: 200 000 days (some centuries) used to wrap
/// the trace cutoff into the future, so retention deleted every trace file every hour; 800 000 days
/// (past year 1) made <c>DateTimeOffset.Subtract</c> throw, so retention never ran; 10.7 million
/// days made <c>TimeSpan.FromDays</c> throw in the loop itself (#130, and #125 for metrics).</para>
/// </summary>
public static class RetentionMath
{
    /// <summary>The shortest window: one day.</summary>
    public const int MinDays = 1;

    /// <summary>The longest window: 100 years, the spelling of "forever".</summary>
    public const int MaxDays = 36_500;

    /// <summary>Whether <paramref name="days"/> is a window a policy may hold.</summary>
    public static bool IsValidDays(int days) => days >= MinDays && days <= MaxDays;

    /// <summary>
    /// The TTL of a day count that may come from anywhere — a request, the database, a config file —
    /// clamped into [<see cref="MinDays"/>, <see cref="MaxDays"/>], so that no count can make the
    /// arithmetic below, or <see cref="TimeSpan.FromDays(double)"/>, throw or wrap.
    /// </summary>
    public static TimeSpan TtlOf(int days) => TimeSpan.FromDays(Math.Clamp(days, MinDays, MaxDays));

    /// <summary>
    /// <paramref name="now"/> − <paramref name="ttl"/> in Unix nanoseconds: what retention deletes
    /// before (by a file's newest timestamp) and a rewrite clips at. <see cref="long.MinValue"/> —
    /// nothing is older — when the TTL reaches back past the Unix epoch, which is where computing
    /// it in <see cref="DateTimeOffset"/> and multiplying out went wrong (see the class doc).
    /// Millisecond arithmetic in <see cref="long"/>: a TTL of <see cref="TimeSpan.MaxValue"/> is
    /// 9.2 × 10¹⁴ ms and nothing overflows.
    /// </summary>
    public static long CutoffNano(DateTimeOffset now, TimeSpan ttl)
    {
        long nowMs = now.ToUnixTimeMilliseconds();
        long ttlMs = Math.Max(0, ttl.Ticks / TimeSpan.TicksPerMillisecond);
        return ttlMs >= nowMs ? long.MinValue : (nowMs - ttlMs) * 1_000_000L;
    }
}
