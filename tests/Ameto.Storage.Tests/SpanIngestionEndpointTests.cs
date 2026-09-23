using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Microsoft.Extensions.Logging;

namespace Ameto.Storage.Tests;

/// <summary>
/// TI#11: the span ingest endpoint under a full ring. It wrote one formatted warning PER REFUSED
/// REQUEST into the server's own log storage, under exactly the overload that caused it; and it
/// refused a batch whole at 90 % full although the per-item enqueue already decides. Time is a
/// hand-driven clock: nothing here waits.
/// </summary>
public sealed class SpanIngestionEndpointTests
{
    /// <summary>A clock that moves only when told to.</summary>
    private sealed class StepClock : TimeProvider
    {
        public long Now = 1_000;
        public override long TimestampFrequency => 1_000;          // milliseconds
        public override long GetTimestamp() => Now;
    }

    private sealed class CapturingLogger : ILogger<SpanIngestionEndpoint>
    {
        public readonly List<(LogLevel Level, string Message, IReadOnlyList<KeyValuePair<string, object?>> State)> Entries = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? ex, Func<TState, Exception?, string> fmt)
        {
            var kv = state as IReadOnlyList<KeyValuePair<string, object?>> ?? [];
            lock (Entries) Entries.Add((level, fmt(state, ex), kv));
        }
    }

    private static SpanIngestItem Span(int i) => new()
    {
        TraceId = new TraceId(1, (ulong)(i + 1)), SpanId = new SpanId((ulong)(i + 1)),
        StartTimeUnixNano = 1_000 + i, DurationNanos = 1, Name = "n", ServiceName = "s",
    };

    private static SpanIngestItem[] Spans(int n)
    {
        var a = new SpanIngestItem[n];
        for (int i = 0; i < n; i++) a[i] = Span(i);
        return a;
    }

    private static object? Field(IReadOnlyList<KeyValuePair<string, object?>> state, string key)
    {
        foreach (var kv in state) if (kv.Key == key) return kv.Value;
        return null;
    }

    /// <summary>
    /// A THOUSAND REFUSED REQUESTS IN ONE SECOND ARE ONE WARNING, and the next second's first
    /// refusal is the next one, carrying the counts it summarises. Logging per refusal writes a
    /// thousand.
    /// </summary>
    [Fact]
    public void A_full_ring_is_reported_once_a_second_with_the_counts()
    {
        using var ring = new SpanRingBuffer(capacity: 4);
        var clock  = new StepClock();
        var log    = new CapturingLogger();
        var intake = new SpanIngestionEndpoint(ring, log, clock);

        Assert.True(intake.TryIngest(Spans(4), out int filled));
        Assert.Equal(4, filled);

        var batch = Spans(3);
        for (int i = 0; i < 1_000; i++)
        {
            Assert.False(intake.TryIngest(batch, out int accepted));
            Assert.Equal(0, accepted);
        }

        var first = Assert.Single(log.Entries);
        Assert.Equal(LogLevel.Warning, first.Level);
        Assert.Equal(3L, Field(first.State, "RefusedSpans"));      // the one that won the second
        Assert.Equal(1L, Field(first.State, "RefusedRequests"));

        clock.Now += 999;                                          // still inside the same second
        Assert.False(intake.TryIngest(batch, out _));
        Assert.Single(log.Entries);

        clock.Now += 1;                                            // one second after the first warning
        Assert.False(intake.TryIngest(batch, out _));
        Assert.Equal(2, log.Entries.Count);
        Assert.Equal(1_001L, Field(log.Entries[1].State, "RefusedRequests"));  // 999 + the one past 999 ms + this
        Assert.Equal(3_003L, Field(log.Entries[1].State, "RefusedSpans"));

        Assert.Equal(1_002, intake.RefusedRequests);
        Assert.Equal(3_006, intake.RefusedSpans);
    }

    /// <summary>
    /// NO WHOLE-BATCH REFUSAL AT 90 %. With 15 of 16 slots taken a three-span batch takes the one
    /// slot left and reports it: the per-item result is the decision. The old pre-check refused all
    /// three with room for one, and accepted nothing.
    /// </summary>
    [Fact]
    public void A_nearly_full_ring_takes_what_fits_and_says_how_much()
    {
        using var ring = new SpanRingBuffer(capacity: 16);
        var intake = new SpanIngestionEndpoint(ring, new CapturingLogger(), new StepClock());

        Assert.True(intake.TryIngest(Spans(15), out int pre));
        Assert.Equal(15, pre);
        Assert.True(ring.FillFraction >= 0.9);

        Assert.False(intake.TryIngest(Spans(3), out int accepted));
        Assert.Equal(1, accepted);
        Assert.Equal(2, intake.RefusedSpans);
    }
}
