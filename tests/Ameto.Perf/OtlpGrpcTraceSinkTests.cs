using Ameto.Otel;
using Ameto.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// THE gRPC TRACE RECEIVER GOES THROUGH THE RAW SINK (TI#3's tail): the Export handler's decode,
/// <c>OtlpGrpcEndpointMapper.IngestTraces</c>, streams into <see cref="ISpanSink"/> as the HTTP
/// route does, and answers exactly what it answered when it built a list and called
/// <see cref="ISpanIngester.TryIngest"/>: nothing to report for an empty batch, the refused
/// PREFIX as rejected spans with the buffer-full reason otherwise.
/// </summary>
public sealed class OtlpGrpcTraceSinkTests
{
    private readonly ITestOutputHelper _out;
    public OtlpGrpcTraceSinkTests(ITestOutputHelper o) => _out = o;

    [Fact]
    public void The_export_decode_hands_the_sink_what_the_dom_path_would_have_stored()
    {
        byte[] payload = OtlpProtoPayloads.Traces_Realistic(200, nestedAttr: false);
        var sink = new CapturingSpanSink();

        var (ok, rejected, why) = OtlpGrpcEndpointMapper.IngestTraces(payload, sink);

        Assert.True(ok);
        Assert.Equal(0, rejected);
        Assert.NotNull(why);                                              // shown only when something is rejected
        sink.AssertMatches(OtlpTraceMapper.Map(OtlpProtoDecoder.DecodeTraces(payload, payload.Length)));
        Assert.Equal(1, sink.EndBatches);
        Assert.Equal(1, sink.InternCalls);                                // once for the block, not per span
    }

    [Fact]
    public void A_refused_batch_is_reported_as_the_rejected_suffix_with_the_same_reason()
    {
        byte[] payload = OtlpProtoPayloads.Traces_Realistic(20, nestedAttr: false);
        var sink = new CapturingSpanSink { RefuseFrom = 7 };

        var (ok, rejected, why) = OtlpGrpcEndpointMapper.IngestTraces(payload, sink);

        Assert.True(ok);
        Assert.Equal(13, rejected);                                        // 20 offered, the first 7 taken
        Assert.Equal("the ingest buffer was full", why);
        Assert.Equal(7, sink.Spans.Count);
    }

    [Fact]
    public void An_empty_export_has_nothing_to_report()
    {
        Assert.Equal((true, 0, (string?)null),
                     OtlpGrpcEndpointMapper.IngestTraces(OtlpProtoPayloads.EmptyTraces(), new CapturingSpanSink()));
    }

    /// <summary>
    /// And it allocates nothing per span on the way in: the list path it replaced built an item, a
    /// name string and an attribute array for every span (566-622 B/span, <c>OtlpTraceProtoProbe</c>).
    /// Per thread, best of five, into a sink that keeps nothing.
    /// </summary>
    [Fact]
    public void The_export_decode_allocates_nothing_per_span()
    {
        byte[] payload = OtlpProtoPayloads.Traces_Realistic(200, nestedAttr: false);
        var sink = new DiscardingSink();
        for (int i = 0; i < 10; i++) OtlpGrpcEndpointMapper.IngestTraces(payload, sink);

        long best = long.MaxValue;
        for (int r = 0; r < 5; r++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            OtlpGrpcEndpointMapper.IngestTraces(payload, sink);
            best = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - before);
        }
        _out.WriteLine($"gRPC trace decode into the sink: {best:N0} B per 200-span batch");
        Assert.True(best < 200, $"the gRPC trace decode allocated {best:N0} B for 200 spans");
    }

    private sealed class DiscardingSink : ISpanSink
    {
        public int InternService(ReadOnlySpan<byte> serviceUtf8) => 0;
        public bool TryIngestRaw(TraceId traceId, SpanId spanId, SpanId parentSpanId, long startTimeUnixNano,
            long durationNanos, ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8,
            SpanKind kind, SpanStatusCode status, short httpStatusCode, ReadOnlySpan<byte> msgpackAttributes) => true;
        public void EndBatch() { }
    }
}
