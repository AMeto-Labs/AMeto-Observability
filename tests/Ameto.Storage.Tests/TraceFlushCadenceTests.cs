using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// HOW OFTEN THE HOT TIER BECOMES A SEGMENT (<c>TraceStorageEngine.FlushIfDue</c>).
///
/// <para>Production at ~100 spans/s logged <c>Flushed 3460 spans</c>, <c>Flushed 3120 spans</c>… every
/// 30 seconds: the due check wrote any tier of 500 spans on every tick, 120 segments an hour, and the
/// tier never came near its 20 MB budget. The check now also waits for the tier to be
/// <see cref="TracesOptions.HotTierFlushAge"/> old (five minutes by default); the byte budget, the
/// 50 000-span cap and the one-hour bound flush exactly as they did.</para>
/// </summary>
public sealed class TraceFlushCadenceTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-tcadence-" + Guid.NewGuid().ToString("N"));

    public TraceFlushCadenceTests() => Directory.CreateDirectory(_dir);

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private TraceStorageEngine Engine(TracesOptions? options = null) =>
        new(_dir, NullLogger<TraceStorageEngine>.Instance, options: options);

    private static SpanIngestItem Span(int i) => new()
    {
        TraceId = new TraceId(0xCADE, (ulong)(i / 5 + 1)), SpanId = new SpanId((ulong)(i + 1)),
        StartTimeUnixNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * Ms + i * Ms,
        DurationNanos = Ms, Name = "GET /orders", ServiceName = "billing", Kind = SpanKind.Server,
    };

    private int Segments() => Directory.GetFiles(_dir, "*.trc").Length;

    [Fact]
    public void A_tier_of_thousands_of_spans_waits_for_the_flush_age_before_a_due_check_writes_it()
    {
        using var e = Engine();
        var now = DateTime.UtcNow;
        e._flushClockForTest = () => now;
        for (int i = 0; i < 3_000; i++) e.WriteSpan(Span(i));   // half a minute of a busy install

        for (int tick = 0; tick < 9; tick++)                     // nine 30-second ticks: 4.5 minutes
        {
            now = now.AddSeconds(30);
            e.FlushIfDue();
        }
        e.WaitForFlushForTest();
        Assert.Equal(0, Segments());
        Assert.Equal(3_000, e.HotSpansForTest.Count);

        now = now.AddSeconds(31);                                 // past five minutes
        e.FlushIfDue();
        e.WaitForFlushForTest();
        Assert.Equal(1, Segments());
        Assert.Empty(e.HotSpansForTest);
    }

    [Fact]
    public void Zero_is_the_old_cadence_a_tier_of_500_spans_on_the_next_check()
    {
        using var e = Engine(new TracesOptions { HotTierFlushAge = TimeSpan.Zero });
        for (int i = 0; i < 600; i++) e.WriteSpan(Span(i));
        e.FlushIfDue();
        e.WaitForFlushForTest();
        Assert.Equal(1, Segments());
    }

    [Fact]
    public void A_trickle_still_waits_for_the_hour_and_then_is_written()
    {
        using var e = Engine();
        var now = DateTime.UtcNow;
        e._flushClockForTest = () => now;
        for (int i = 0; i < 40; i++) e.WriteSpan(Span(i));

        now = now.AddMinutes(30);
        e.FlushIfDue();
        e.WaitForFlushForTest();
        Assert.Equal(0, Segments());                              // old enough, not big enough

        now = now.AddMinutes(31);
        e.FlushIfDue();
        e.WaitForFlushForTest();
        Assert.Equal(1, Segments());                              // the hour bound, whatever the size
    }

    [Fact]
    public void A_tier_at_its_byte_budget_is_written_without_waiting_for_the_age()
    {
        // The write path flushes on the budget; the age bar only holds the PERIODIC check back.
        using var e = Engine(new TracesOptions { HotTierMaxBytes = 200_000 });
        for (int i = 0; i < 2_000; i++) e.WriteSpan(Span(i));   // 2 000 × ≥160 B: past the budget
        e.WaitForFlushForTest();
        Assert.True(Segments() >= 1, "a tier past its byte budget waited for the flush age");
    }

    [Fact]
    public void The_flush_age_defaults_to_five_minutes()
    {
        Assert.Equal(TimeSpan.FromMinutes(5), new TracesOptions().EffectiveHotTierFlushAge);
        Assert.Equal(TimeSpan.Zero, new TracesOptions { HotTierFlushAge = TimeSpan.Zero }.EffectiveHotTierFlushAge);
        Assert.Equal(TimeSpan.FromMinutes(5), new TracesOptions { HotTierFlushAge = TimeSpan.FromSeconds(-1) }.EffectiveHotTierFlushAge);
    }
}
