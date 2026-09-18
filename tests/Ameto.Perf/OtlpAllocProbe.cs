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

    /// <summary>
    /// The streaming parser's own cost per record, on a batch whose attribute values carry JSON
    /// escapes — a quoted message, a Windows path, a newline, an accented name. That is the
    /// ordinary shape of a structured log line, and it is the path that has to unescape into a
    /// scratch buffer before the value can be written as msgpack.
    /// </summary>
    [Fact]
    public void StreamingParseCostPerRecord()
    {
        const int records = 1000;
        byte[] plain   = Encoding.UTF8.GetBytes(BuildBatch(records));
        byte[] escaped = Encoding.UTF8.GetBytes(BuildBatch(records, escapes: true));
        var sink = new NullSink();

        for (int i = 0; i < 20; i++) { OtlpLogStreamParser.Parse(plain, sink); OtlpLogStreamParser.Parse(escaped, sink); }
        Measure(plain, sink, records);                 // a full measured pass of each before
        Measure(escaped, sink, records);               // either counts — tiered JIT otherwise
                                                       // charges the first one for both
        var (plainNs,   plainB)   = Measure(plain, sink, records);
        var (escapedNs, escapedB) = Measure(escaped, sink, records);

        _out.WriteLine($"streaming JSON parse, {records}-record batches (best of 9 rounds):");
        _out.WriteLine($"  plain attributes  : {plainNs:F0} ns/record | {plainB:F1} B/record "
                     + $"| {1_000_000.0 / plainNs:F0} k records/s/core");
        _out.WriteLine($"  escaped attributes: {escapedNs:F0} ns/record | {escapedB:F1} B/record "
                     + $"| {1_000_000.0 / escapedNs:F0} k records/s/core");

        // The scratch is per THREAD, not per record or per escaped value.
        Assert.True(escapedB < 1.0, $"expected no per-record allocation, got {escapedB:F1} B/record");
    }

    /// <summary>
    /// Best of several rounds, not the mean: the interference here is other work on the machine,
    /// which can only ever make a round slower. A mean over a shared box moves by 40 % between
    /// runs and would hide a change this size completely.
    /// </summary>
    private static (double Ns, double Bytes) Measure(byte[] utf8, NullSink sink, int records)
    {
        const int rounds = 9, iters = 30;
        GC.Collect();
        GC.WaitForPendingFinalizers();

        double best = double.MaxValue;
        long bytes  = 0;
        for (int r = 0; r < rounds; r++)
        {
            long b0 = GC.GetAllocatedBytesForCurrentThread();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            for (int i = 0; i < iters; i++) OtlpLogStreamParser.Parse(utf8, sink);
            sw.Stop();
            long allocated = GC.GetAllocatedBytesForCurrentThread() - b0;

            double ns = sw.Elapsed.TotalMilliseconds * 1_000_000.0 / iters / records;
            if (ns < best) { best = ns; bytes = allocated; }
        }
        return (best, bytes / (double)iters / records);
    }

    private static string BuildBatch(int records, bool escapes = false)
    {
        var sb = new StringBuilder(records * 400);
        sb.Append("""{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"Etisalat.API"}},{"key":"host.name","value":{"stringValue":"load-gen"}}]},"scopeLogs":[{"scope":{"name":"k6"},"logRecords":[""");
        for (int i = 0; i < records; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append("""{"timeUnixNano":"1783953780000000000","severityNumber":9,"body":{"stringValue":"HTTP request handled"},"attributes":[""");
            sb.Append("""{"key":"orderId","value":{"intValue":"12345"}},{"key":"customerId","value":{"stringValue":"cust-42"}},{"key":"http.method","value":{"stringValue":"GET"}},{"key":"http.route","value":{"stringValue":"/api/pay"}},{"key":"http.status_code","value":{"intValue":"200"}},{"key":"duration_ms","value":{"doubleValue":12.5}},{"key":"region","value":{"stringValue":"ae-dxb"}},{"key":"RequestId","value":{"stringValue":"0HN123abc"}}""");
            if (escapes)
                // A quoted message, a Windows path, a newline and an accented name: what a
                // structured log line looks like once anyone puts real text in it.
                sb.Append("""
                          ,{"key":"message","value":{"stringValue":"order \"A-42\" accepted"}},{"key":"file","value":{"stringValue":"C:\\svc\\logs\\app.log"}},{"key":"detail","value":{"stringValue":"line one\nline two"}},{"key":"customer","value":{"stringValue":"Beno\u00eet Dupr\u00e9"}}
                          """);
            sb.Append("]}");
        }
        sb.Append("]}]}]}");
        return sb.ToString();
    }
}
