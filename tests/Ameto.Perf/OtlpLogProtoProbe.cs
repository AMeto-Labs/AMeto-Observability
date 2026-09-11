using System.Diagnostics;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Otel;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Cost of the OTLP/protobuf LOGS ingest path — the encoding SDK exporters and the
/// collector send, and therefore the one production runs on.
///
/// <para>Measures decode + map only: everything upstream of the ring enqueue, with a sink
/// that does nothing, so what is left is the parser. The old route allocated a parser
/// object and copied the payload for every nested message (body, and a KeyValue AND an
/// AnyValue per attribute), built a <c>LogEvent</c> object graph that was walked once, and
/// round-tripped the timestamp, every int attribute and both ids through strings.</para>
/// </summary>
public sealed class OtlpLogProtoProbe
{
    private readonly ITestOutputHelper _out;
    public OtlpLogProtoProbe(ITestOutputHelper o) => _out = o;

    /// <summary>Counts and discards — the ring is not what this probe is measuring.</summary>
    private sealed class NullSink : IOtlpLogSink
    {
        public int Count;
        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8)
        { Count++; return true; }
        public void NotifyBatchEnqueued() { }
    }

    [Fact]
    public void SpanParserBeatsDomPath()
    {
        byte[] payload = OtlpProtoPayloads.Logs_Realistic();
        int records = OtlpProtoPayloads.LogRecords;
        var sink = new NullSink();

        for (int i = 0; i < 20; i++)                                  // warm JIT + thread scratch
        {
            OtlpLogMapper.Map(OtlpProtoDecoder.DecodeLogs(payload, payload.Length), NodeId.Local.Value);
            OtlpLogProtoParser.Parse(payload, sink);
        }

        const int iters = 100;

        var (domMs,  domBytes)  = Measure(iters, () =>
            OtlpLogMapper.Map(OtlpProtoDecoder.DecodeLogs(payload, payload.Length), NodeId.Local.Value));
        var (spanMs, spanBytes) = Measure(iters, () => OtlpLogProtoParser.Parse(payload, sink));

        double domNs  = domMs  * 1_000_000.0 / records;
        double spanNs = spanMs * 1_000_000.0 / records;
        double domB   = domBytes  / (double)records;
        double spanB  = spanBytes / (double)records;

        _out.WriteLine($"payload      : {payload.Length / 1024.0:F1} KB protobuf, {records} log records "
                     + "(6 attributes + trace link each, 6 resource attributes)");
        _out.WriteLine($"DOM decode+map : {domMs:F3} ms/batch | {domNs:F0} ns/record | {domB:F0} B/record "
                     + $"| {1_000_000.0 / domNs:F0} k records/s/core");
        _out.WriteLine($"span parser    : {spanMs:F3} ms/batch | {spanNs:F0} ns/record | {spanB:F0} B/record "
                     + $"| {1_000_000.0 / spanNs:F0} k records/s/core");
        _out.WriteLine($"gain           : {domNs / spanNs:F1}x faster, "
                     + $"{(spanBytes == 0 ? double.PositiveInfinity : domB / spanB):F0}x less allocated "
                     + $"({domBytes / 1024.0:F0} KB → {spanBytes / 1024.0:F1} KB per batch)");

        // The parser's own allocation is per BATCH (the thread scratch, grown once), not per
        // record — so this is a guard against the DOM path creeping back onto the hot route,
        // not a tolerance that needs tuning.
        Assert.True(spanBytes * 20 < domBytes,
            $"expected >=20x less allocation, got dom={domBytes} span={spanBytes}");
    }

    private static (double MsPerIter, long Bytes) Measure(int iters, Action body)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iters; i++) body();
        sw.Stop();
        long bytes = GC.GetAllocatedBytesForCurrentThread() - b0;
        return (sw.Elapsed.TotalMilliseconds / iters, bytes / iters);
    }
}
