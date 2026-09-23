using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// TS#9: THE SPAN RING REFUSES BY BYTES, NOT ONLY BY SLOTS.
///
/// <para>The ring's only back-pressure was a count — 65 536 slots, or the 90 % pre-check before
/// that — with no idea how big the items were: a full ring of ordinary spans held 35 MB, and 8 192
/// spans carrying 10 KB of attributes held 80 MB (<c>SpanRingBytesProbe</c>), on a host whose hot
/// tier is allowed 20. The ring now reserves a span's bytes before it claims a slot and refuses the
/// span when the reservation would pass its budget.</para>
/// </summary>
public sealed class SpanRingBackPressureTests
{
    private const long MB = 1024 * 1024;

    private readonly ITestOutputHelper _out;
    public SpanRingBackPressureTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public void A_heavy_burst_is_refused_by_bytes_while_slots_are_free()
    {
        using var ring = new SpanRingBuffer(capacity: 1_024, maxBytes: 100_000);
        var heavy = Span(0, blobBytes: 10_000);
        long weight = SpanRingBuffer.PayloadBytes(heavy);

        int accepted = 0;
        for (int i = 0; i < 1_024; i++)
            if (ring.TryEnqueue(Span(i, blobBytes: 10_000))) accepted++;

        _out.WriteLine($"{accepted} of 1 024 slots taken by 10 KB spans under a 100 000 B budget "
                     + $"({ring.RefusedForBytes} refused for bytes)");
        Assert.Equal((int)(100_000 / weight), accepted);
        Assert.True(accepted < ring.Capacity);
        Assert.Equal(1_024 - accepted, ring.RefusedForBytes);
        Assert.True(ring.BytesInFlight <= ring.MaxBytes);
    }

    [Fact]
    public void The_bytes_come_back_as_the_drainer_takes_the_spans()
    {
        using var ring = new SpanRingBuffer(capacity: 1_024, maxBytes: 100_000);
        while (ring.TryEnqueue(Span(0, blobBytes: 10_000))) { }
        Assert.False(ring.TryEnqueue(Span(1, blobBytes: 10_000)));   // spent

        var headers = new SpanHeader[4];
        var apart   = new byte[]?[4];
        int taken   = ring.TryDequeueMany(headers, apart);
        Assert.Equal(4, taken);
        ring.Release(headers);                                       // what the drainer does once it has copied them

        // Four spans' worth came back, and exactly that much fits again.
        for (int i = 0; i < 4; i++) Assert.True(ring.TryEnqueue(Span(2 + i, blobBytes: 10_000)));
        Assert.False(ring.TryEnqueue(Span(9, blobBytes: 10_000)));
    }

    [Fact]
    public void A_span_refused_for_want_of_a_slot_gives_its_bytes_back()
    {
        using var ring = new SpanRingBuffer(capacity: 4, maxBytes: 1024 * 1024);
        var s = Span(0, blobBytes: 1_000);
        for (int i = 0; i < 4; i++) Assert.True(ring.TryEnqueue(s));

        Assert.False(ring.TryEnqueue(s));                             // every slot taken
        Assert.Equal(4 * SpanRingBuffer.PayloadBytes(s), ring.BytesInFlight);
        Assert.Equal(0, ring.RefusedForBytes);
    }

    [Fact]
    public void A_batch_takes_what_fits_by_bytes_and_says_how_much()
    {
        using var ring = new SpanRingBuffer(capacity: 1_024, maxBytes: 100_000);
        var endpoint = new SpanIngestionEndpoint(ring, NullLogger<SpanIngestionEndpoint>.Instance);

        var batch = new SpanIngestItem[20];
        for (int i = 0; i < batch.Length; i++) batch[i] = Span(i, blobBytes: 10_000);

        Assert.False(endpoint.TryIngest(batch, out int accepted));
        Assert.Equal((int)(100_000 / SpanRingBuffer.PayloadBytes(batch[0])), accepted);
        Assert.Equal(batch.Length - accepted, endpoint.RefusedSpans);
    }

    /// <summary>
    /// THE DEFAULT BUDGET IS THE OLD FULL RING, IN BYTES: on a host with room for the caps the ring
    /// still takes every one of its 65 536 slots of ordinary spans — the burst it always absorbed —
    /// and on the stand the same fraction of that its tier gets of its cap.
    /// </summary>
    [Fact]
    public void The_ring_budget_is_the_old_full_ring_in_bytes_scaled_to_the_host()
    {
        var stand = MemoryBudgets.Derive(384 * MB, 512 * MB);
        var large = MemoryBudgets.Derive(64 * 1024 * MB, 64 * 1024 * MB);
        var o = new TracesOptions();

        long largeBudget = o.RingMaxBytesFor(large);
        long standBudget = o.RingMaxBytesFor(stand);
        var ordinary = Span(0, blobBytes: 375);
        _out.WriteLine($"ring budget: large {largeBudget / (double)MB:F1} MB, stand {standBudget / (double)MB:F1} MB; "
                     + $"an ordinary span weighs {SpanRingBuffer.PayloadBytes(ordinary)} B");

        Assert.Equal((long)TracesOptions.DefaultRingCapacity * TracesOptions.OrdinaryRingSpanBytes, largeBudget);
        Assert.True(largeBudget / SpanRingBuffer.PayloadBytes(ordinary) >= TracesOptions.DefaultRingCapacity,
            "a large host's ring no longer holds its 65 536 ordinary spans");
        Assert.InRange(standBudget, 27 * 1000 * 1000, 28 * 1000 * 1000);

        Assert.Equal(3 * MB, new TracesOptions { RingMaxBytes = 3 * MB }.RingMaxBytesFor(stand));

        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        services.AddSingleton(typeof(ILogger<>), typeof(Logger<>));
        services.AddSingleton(new ServerOptions { Traces = new TracesOptions { RingMaxBytes = 7 * MB } });
        services.AddAmetoTracing(Path.Combine(Path.GetTempPath(), "ameto-ringbp-" + Guid.NewGuid().ToString("N")));
        using var sp = services.BuildServiceProvider();
        Assert.Equal(7 * MB, sp.GetRequiredService<SpanRingBuffer>().MaxBytes);
    }

    private static SpanIngestItem Span(int i, int blobBytes) => new()
    {
        TraceId           = new TraceId(0xBAC4UL, (ulong)(i + 1)),
        SpanId            = new SpanId((ulong)(i + 1)),
        StartTimeUnixNano = 1_785_000_000_000_000_000L + i,
        DurationNanos     = 1,
        Name              = "SELECT payments",
        ServiceName       = "billing",
        AttributesBytes   = new byte[blobBytes],
    };
}
