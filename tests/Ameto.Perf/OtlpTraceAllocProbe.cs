using System.Text;
using System.Text.Json;
using Ameto.Otel;
using Ameto.Otel.Models;
using Ameto.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Quantifies the allocation win of the streaming OTLP trace parser vs the reflection
/// DOM path (bytes allocated per 200-span batch).
///
/// <para><b>Measured into the raw sink</b> (TI#3), which is what the receiver does now: the parser
/// hands each span to <see cref="ISpanSink"/> as slices of the body and the sink copies them into
/// the ring — so what the parser allocates per span is what a request costs the heap. The sink
/// here keeps nothing (the ring's own copy is native and is measured in
/// <c>SpanRingBytesProbe</c>). The list-returning path it replaced built a
/// <see cref="SpanIngestItem"/>, a name string and an attribute array per span: 330 B/span, and the
/// guard was <c>stream * 3 &lt; dom</c>. It is <c>stream * 20 &lt; dom</c> now.</para>
/// </summary>
public sealed class OtlpTraceAllocProbe
{
    private static readonly JsonSerializerOptions DomOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        AllowTrailingCommas  = true,
    };

    private readonly ITestOutputHelper _out;
    public OtlpTraceAllocProbe(ITestOutputHelper o) => _out = o;

    /// <summary>A sink that takes every span and keeps nothing — the parser's own cost, alone.</summary>
    private sealed class DiscardingSink : ISpanSink
    {
        public static readonly DiscardingSink Instance = new();
        public int InternService(ReadOnlySpan<byte> serviceUtf8) => 0;
        public bool TryIngestRaw(TraceId traceId, SpanId spanId, SpanId parentSpanId, long startTimeUnixNano,
            long durationNanos, ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8,
            SpanKind kind, SpanStatusCode status, short httpStatusCode, ReadOnlySpan<byte> msgpackAttributes) => true;
        public void EndBatch() { }
    }

    [Fact]
    public void StreamingAllocatesFarLessThanDom()
    {
        byte[] utf8  = Encoding.UTF8.GetBytes(BuildBatch(200));
        byte[] proto = OtlpProtoPayloads.Traces_Realistic(200, nestedAttr: false);
        var sink = DiscardingSink.Instance;

        // Warm up JIT + scratch buffers.
        for (int i = 0; i < 10; i++)
        {
            OtlpTraceStreamParser.Parse(utf8, sink);
            OtlpTraceStreamParser.Parse(utf8);
            OtlpTraceProtoParser.Parse(proto, sink);
            OtlpTraceMapper.Map(JsonSerializer.Deserialize<ExportTraceServiceRequest>(utf8, DomOptions)!);
        }

        // Best of five, per thread: a stray GC-visible allocation elsewhere never lands here.
        long stream = Best(() => OtlpTraceStreamParser.Parse(utf8, sink));
        long list   = Best(() => OtlpTraceStreamParser.Parse(utf8));
        long raw    = Best(() => OtlpTraceProtoParser.Parse(proto, sink));
        long dom    = Best(() =>
            OtlpTraceMapper.Map(JsonSerializer.Deserialize<ExportTraceServiceRequest>(utf8, DomOptions)!));

        _out.WriteLine($"batch: 200 spans, {utf8.Length / 1024.0:F1} KB json, {proto.Length / 1024.0:F1} KB protobuf");
        _out.WriteLine($"dom           : {dom / 1024.0,8:F1} KB allocated  ({dom / 200.0,6:F0} B/span)");
        _out.WriteLine($"json -> items : {list / 1024.0,8:F1} KB allocated  ({list / 200.0,6:F0} B/span)  (the list API, for gRPC and the tests)");
        _out.WriteLine($"json -> sink  : {stream / 1024.0,8:F1} KB allocated  ({stream / 200.0,6:F0} B/span)  ({(stream == 0 ? "nothing at all" : $"{(double)dom / stream:F0}x less than dom")})");
        _out.WriteLine($"proto -> sink : {raw / 1024.0,8:F1} KB allocated  ({raw / 200.0,6:F0} B/span)");

        Assert.True(stream * 20 < dom, $"expected ≥20x reduction, got dom={dom} stream={stream}");
    }

    private static long Best(Action a)
    {
        long best = long.MaxValue;
        for (int r = 0; r < 5; r++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            a();
            best = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - before);
        }
        return best;
    }

    private static string BuildBatch(int spans)
    {
        var sb = new StringBuilder(spans * 700);
        sb.Append("""
        {"resourceSpans":[{"resource":{"attributes":[
          {"key":"service.name","value":{"stringValue":"Wallet.API"}},
          {"key":"host.name","value":{"stringValue":"srv-01"}}
        ]},"scopeSpans":[{"scope":{"name":"otel.sdk"},"spans":[
        """);
        for (int i = 0; i < spans; i++)
        {
            if (i > 0) sb.Append(',');
            long start = 1_783_953_780_000_000_000 + i * 1_000_000L;
            sb.Append($$$"""
            {"traceId":"f6f6f098569a7f2ba54f3c734aa5{{{i:x4}}}","spanId":"a1b2c3d4e5f6{{{i:x4}}}",
             "parentSpanId":"0102030405060708","name":"POST /api/pay/{{{i}}}","kind":2,
             "startTimeUnixNano":"{{{start}}}","endTimeUnixNano":"{{{start + 250_000_000}}}",
             "status":{"code":1},
             "attributes":[
               {"key":"http.method","value":{"stringValue":"POST"}},
               {"key":"http.status_code","value":{"intValue":"200"}},
               {"key":"http.route","value":{"stringValue":"/api/pay"}},
               {"key":"net.peer.name","value":{"stringValue":"10.220.0.{{{i % 250}}}"}},
               {"key":"retry","value":{"boolValue":false}},
               {"key":"duration_ms","value":{"doubleValue":{{{i}}}.5}}
             ]}
            """);
        }
        sb.Append("]}]}]}");
        return sb.ToString();
    }
}
