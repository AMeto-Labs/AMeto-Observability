using System.Diagnostics;
using System.Text;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// <c>service.name</c> is a property of the resourceLogs block, identical for every record
/// under it, yet the OTLP JSON parser used to re-intern it once per record: a UTF-8→UTF-16
/// decode plus a Marvin hash plus a dictionary probe, hundreds of times for the same bytes.
/// This measures interning per record against interning once per resource, over a realistic
/// batch, with a real <see cref="StringInternPool"/>.
/// </summary>
public sealed class ServiceInternProbe
{
    private const int Records = 1000;

    /// <summary>Old behaviour: the parser's svcIdx is ignored, so the span is interned per record.</summary>
    private sealed class PerRecordSink : IOtlpLogSink
    {
        public readonly StringInternPool Pool = new();
        public int Interns;
        public int Records;

        public bool TryIngestRaw(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
            ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Records++;
            Interns++;
            Pool.Intern(serviceUtf8);
            return true;
        }

        public void NotifyBatchEnqueued() { }
    }

    /// <summary>New behaviour: interned once per resourceLogs block, reused by index.</summary>
    private sealed class PerResourceSink : IOtlpLogSink
    {
        public readonly StringInternPool Pool = new();
        public int Interns;
        public int Records;

        public int InternService(ReadOnlySpan<byte> serviceUtf8)
        {
            Interns++;
            return Pool.Intern(serviceUtf8);
        }

        public bool TryIngestRaw(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
            ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
            => TryIngestRaw(tsTicks, level, templateUtf8, msgpackProps, traceHi, traceLo, spanId, serviceUtf8, -1);

        public bool TryIngestRaw(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
            ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8, int serviceIdx)
        {
            Records++;
            if (serviceIdx < 0) { Interns++; Pool.Intern(serviceUtf8); }
            return true;
        }

        public void NotifyBatchEnqueued() { }
    }

    private readonly ITestOutputHelper _out;
    public ServiceInternProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void ServiceIsInternedOncePerResource_NotOncePerRecord()
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(BuildBatch(Records));

        var perRecord   = new PerRecordSink();
        var perResource = new PerResourceSink();

        for (int i = 0; i < 10; i++)
        {
            OtlpLogStreamParser.Parse(utf8, perRecord);
            OtlpLogStreamParser.Parse(utf8, perResource);
        }

        // The counts that matter: one intern per resource block instead of one per record.
        perRecord.Interns = perRecord.Records = 0;
        perResource.Interns = perResource.Records = 0;
        OtlpLogStreamParser.Parse(utf8, perRecord);
        OtlpLogStreamParser.Parse(utf8, perResource);

        Assert.Equal(Records, perRecord.Records);
        Assert.Equal(Records, perRecord.Interns);
        Assert.Equal(Records, perResource.Records);
        Assert.Equal(1, perResource.Interns);

        int internsBefore = perRecord.Interns;
        int internsAfter  = perResource.Interns;

        const int iters  = 30;
        const int rounds = 5;
        double beforeNs = double.MaxValue, afterNs = double.MaxValue;
        var sw = new Stopwatch();
        for (int r = 0; r < rounds; r++)
        {
            sw.Restart();
            for (int i = 0; i < iters; i++) OtlpLogStreamParser.Parse(utf8, perRecord);
            sw.Stop();
            beforeNs = Math.Min(beforeNs, sw.Elapsed.TotalNanoseconds / iters);

            sw.Restart();
            for (int i = 0; i < iters; i++) OtlpLogStreamParser.Parse(utf8, perResource);
            sw.Stop();
            afterNs = Math.Min(afterNs, sw.Elapsed.TotalNanoseconds / iters);
        }

        _out.WriteLine(
            $"OTLP JSON service.name interning — {Records:N0} records in one resourceLogs block:\n" +
            $"  per record  (before): {internsBefore / (double)Records,6:F3} interns/record  {beforeNs / Records,7:F0} ns/record (whole parse)\n" +
            $"  per resource (after): {internsAfter / (double)Records,6:F3} interns/record  {afterNs / Records,7:F0} ns/record (whole parse)\n" +
            $"  saved               : {(beforeNs - afterNs) / Records:F0} ns/record");
    }

    /// <summary>
    /// The SAME guarantee on the protobuf road — which is the one production takes, since it is
    /// what SDK exporters and the collector send. The optimisation was wired into the JSON
    /// parser only: <c>OtlpLogProtoParser</c> called the 8-argument overload, so
    /// <c>IngestionEndpoint</c> re-interned the resource's service.name for every record under
    /// it. The two packages that introduced the parser and the interning share no file, so
    /// nothing collided and nothing noticed.
    /// </summary>
    [Fact]
    public void TheProtobufParserAlsoInternsOncePerResource_NotOncePerRecord()
    {
        byte[] payload = OtlpProtoPayloads.Logs_Realistic();
        var    sink    = new PerResourceSink();

        OtlpLogProtoParser.Parse(payload, sink);          // warm the thread-static scratch
        sink.Interns = sink.Records = 0;
        OtlpLogProtoParser.Parse(payload, sink);

        Assert.Equal(OtlpProtoPayloads.LogRecords, sink.Records);
        Assert.Equal(1, sink.Interns);

        _out.WriteLine(
            $"OTLP protobuf service.name interning — {OtlpProtoPayloads.LogRecords:N0} records in one "
          + $"resourceLogs block: {sink.Interns} intern(s), {sink.Interns / (double)sink.Records:F3} per record");
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
