using System.Text;
using System.Text.Json;
using Ameto.Otel;
using Ameto.Otel.Models;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Quantifies the allocation win of the streaming OTLP trace parser vs the reflection
/// DOM path (bytes allocated per 200-span batch).
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

    [Fact]
    public void StreamingAllocatesFarLessThanDom()
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(BuildBatch(200));

        // Warm up JIT + scratch buffers.
        for (int i = 0; i < 10; i++)
        {
            OtlpTraceStreamParser.Parse(utf8);
            OtlpTraceMapper.Map(JsonSerializer.Deserialize<ExportTraceServiceRequest>(utf8, DomOptions)!);
        }

        long stream = Measure(() => OtlpTraceStreamParser.Parse(utf8));
        long dom    = Measure(() =>
            OtlpTraceMapper.Map(JsonSerializer.Deserialize<ExportTraceServiceRequest>(utf8, DomOptions)!));

        _out.WriteLine($"batch: 200 spans, {utf8.Length / 1024.0:F1} KB json");
        _out.WriteLine($"dom      : {dom / 1024.0:F1} KB allocated");
        _out.WriteLine($"streaming: {stream / 1024.0:F1} KB allocated  ({(double)dom / stream:F1}x less)");

        Assert.True(stream * 3 < dom, $"expected ≥3x reduction, got dom={dom} stream={stream}");
    }

    private static long Measure(Action a)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        a();
        return GC.GetAllocatedBytesForCurrentThread() - before;
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
