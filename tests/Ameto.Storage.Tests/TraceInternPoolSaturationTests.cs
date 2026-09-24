using System.Text;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// F4: A FULL INTERN POOL IS SAID OUT LOUD, AND A FULL SERVICE POOL COSTS A STRING PER BLOCK, NOT
/// PER SPAN. The pools never drop a span — past their cap every span keeps its own string — but that
/// happened in silence, and for services it cost one string per span where the pool had cost one per
/// block. Time is a hand-driven clock: nothing here waits.
/// </summary>
public sealed class TraceInternPoolSaturationTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-poolsat-" + Guid.NewGuid().ToString("N"));

    public TraceInternPoolSaturationTests() => Directory.CreateDirectory(_root);

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

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

    private static object? Field(IReadOnlyList<KeyValuePair<string, object?>> state, string key)
    {
        foreach (var kv in state) if (kv.Key == key) return kv.Value;
        return null;
    }

    [Fact]
    public void A_full_pool_is_reported_at_most_once_a_minute_and_counted_where_the_refusals_are()
    {
        var pools = new SpanStringPools(maxNames: 4, maxServices: 2);
        using var ring = new SpanRingBuffer(capacity: 64, maxBytes: 1024 * 1024, pools);
        var clock = new StepClock();
        var log   = new CapturingLogger();
        var sink  = new SpanIngestionEndpoint(ring, log, clock);

        foreach (var s in new[] { "a", "b", "c", "d", "e" }) sink.InternService(Encoding.UTF8.GetBytes(s));
        var first = Assert.Single(log.Entries);                            // the service pool filled: said once
        Assert.Equal(LogLevel.Warning, first.Level);
        Assert.Equal("service-name", Field(first.State, "Pool"));

        clock.Now += 30_000;                                               // inside the minute: the name pool fills
        for (int i = 0; i < 10; i++) pools.Name(Encoding.UTF8.GetBytes($"GET /api/user/{i}"), out _);
        Assert.Single(log.Entries);

        clock.Now += 31_000;                                               // past it: a new tier's pool fills too
        pools.ShedNames();
        for (int i = 0; i < 10; i++) pools.Name(Encoding.UTF8.GetBytes($"GET /api/user/{i}"), out _);
        Assert.Equal(2, log.Entries.Count);
        Assert.Equal("span-name", Field(log.Entries[1].State, "Pool"));
        Assert.Equal(2L, Field(log.Entries[1].State, "Saturations"));      // the one held back, and this one

        Assert.Equal(3, sink.InternPoolSaturations);
        Assert.Equal(12, sink.UnpooledSpanNames);                          // 6 + 6 names past a 4-name pool
    }

    /// <summary>
    /// A service.name per pod fills the service pool for good. Past that point a resource block's
    /// spans used to get a fresh string EACH; the drainer now builds it for the block's first span
    /// and hands it to the rest of the run — one string, charged to the tier once.
    /// </summary>
    [Fact]
    public void A_full_service_pool_costs_one_string_per_block_not_per_span()
    {
        var pools = new SpanStringPools(maxNames: 1_024, maxServices: 1);
        using var ring   = new SpanRingBuffer(capacity: 256, maxBytes: 4 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(_root, NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        var sink    = new SpanIngestionEndpoint(ring, NullLogger<SpanIngestionEndpoint>.Instance);
        var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: false);

        sink.InternService("pod-0"u8);                                      // takes the only place
        byte[] svc = Encoding.UTF8.GetBytes("billing");
        int idx = sink.InternService(svc);
        Assert.Equal(-1, idx);                                             // the pool is full

        for (int i = 0; i < 100; i++)
            Assert.True(sink.TryIngestRaw(new TraceId(5, (ulong)(i + 1)), new SpanId((ulong)(i + 1)), default,
                1_785_000_000_000_000_000L + i, 1_000, "op"u8, idx, svc, SpanKind.Server, SpanStatusCode.Unset, 0, []));
        sink.EndBatch();
        while (drainer.DrainOnce(out _) > 0) { }

        var hot = engine.HotSpansForTest;
        Assert.Equal(100, hot.Count);
        Assert.All(hot, r => Assert.Equal("billing", r.ServiceName));
        Assert.All(hot, r => Assert.Same(hot[0].ServiceName, r.ServiceName));
        Assert.Equal(1, sink.UnpooledServiceNames);
        Assert.Equal(100 * TraceStorageEngine.HotSpanBytes(0) + SpanStringPools.UnpooledStringBytes("billing"),
                     engine.HotBytesForTest);                              // the string charged once
    }
}
