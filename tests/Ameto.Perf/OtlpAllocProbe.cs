using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Otel.Models;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Quantifies the allocation win of the streaming OTLP parser vs the reflection DOM path
/// (bytes allocated per 100-record batch). Writes the result to a scratchpad file.
/// </summary>
public sealed class OtlpAllocProbe
{
    private sealed class NullSink : IOtlpLogSink
    {
        public int Count;
        public bool TryIngestRaw(long a, byte b, ReadOnlySpan<byte> c, ReadOnlySpan<byte> d,
            ulong e, ulong f, ulong g, ReadOnlySpan<byte> h) { Count++; return true; }
        public void NotifyBatchEnqueued() { }
    }

    private readonly ITestOutputHelper _out;
    public OtlpAllocProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void StreamingAllocatesFarLessThanDom()
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(BuildBatch(100));
        var sink = new NullSink();
        var opts = new JsonSerializerOptions();

        // Warm up JIT + pools.
        for (int i = 0; i < 20; i++)
        {
            OtlpLogStreamParser.Parse(utf8, sink);
            OtlpLogMapper.Map(JsonSerializer.Deserialize<ExportLogsServiceRequest>(utf8, opts)!, NodeId.Local.Value);
        }

        const int iters = 200;

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) OtlpLogStreamParser.Parse(utf8, sink);
        long streamBytes = (GC.GetAllocatedBytesForCurrentThread() - b0) / iters;

        long b1 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++)
            OtlpLogMapper.Map(JsonSerializer.Deserialize<ExportLogsServiceRequest>(utf8, opts)!, NodeId.Local.Value);
        long domBytes = (GC.GetAllocatedBytesForCurrentThread() - b1) / iters;

        string report = $"OTLP 100-record batch — bytes allocated/batch:\n" +
                        $"  DOM (reflect+map): {domBytes:N0} B\n" +
                        $"  streaming        : {streamBytes:N0} B\n" +
                        $"  reduction        : {(domBytes == 0 ? 0 : 100.0 * (domBytes - streamBytes) / domBytes):F1}%  ({(streamBytes == 0 ? 0 : (double)domBytes / streamBytes):F1}x less)";
        _out.WriteLine(report);

        // Guard against re-introducing allocations on the OTLP hot path.
        Assert.True(streamBytes * 8 < domBytes, $"streaming ({streamBytes} B) should allocate far less than DOM ({domBytes} B)");
    }

    private static string BuildBatch(int records)
    {
        var sb = new StringBuilder(records * 400);
        sb.Append("""{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"Etisalat.API"}},{"key":"host.name","value":{"stringValue":"load-gen"}}]},"scopeLogs":[{"scope":{"name":"k6"},"logRecords":[""");
        for (int i = 0; i < records; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append("""{"timeUnixNano":"1783953780000000000","severityNumber":9,"body":{"stringValue":"HTTP request handled"},"attributes":[""");
            sb.Append("""{"key":"orderId","value":{"intValue":"12345"}},{"key":"customerId","value":{"stringValue":"cust-42"}},{"key":"http.method","value":{"stringValue":"GET"}},{"key":"http.route","value":{"stringValue":"/api/pay"}},{"key":"http.status_code","value":{"intValue":"200"}},{"key":"duration_ms","value":{"doubleValue":12.5}},{"key":"region","value":{"stringValue":"ae-dxb"}},{"key":"RequestId","value":{"stringValue":"0HN123abc"}}""");
            sb.Append("]}");
        }
        sb.Append("]}]}]}");
        return sb.ToString();
    }
}
