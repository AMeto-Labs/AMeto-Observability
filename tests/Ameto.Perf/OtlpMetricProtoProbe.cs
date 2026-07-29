using System.Diagnostics;
using Ameto.Otel;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Cost of the OTLP/protobuf metrics ingest path — the shape real OTel SDK exporters
/// send (application/x-protobuf), which is what the sandbox stand receives.
///
/// Measures decode + map only: everything upstream of the WAL append. The point is that
/// this stage was never free — the old route allocated a parser object and copied the
/// payload for every nested message, built a throwaway object graph, and round-tripped
/// wire integers through strings.
/// </summary>
public sealed class OtlpMetricProtoProbe
{
    private readonly ITestOutputHelper _out;
    public OtlpMetricProtoProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void SpanParserBeatsDomPath()
    {
        byte[] payload = OtlpProtoPayloads.Metrics_Realistic();
        int points = OtlpProtoPayloads.Metrics * OtlpProtoPayloads.PointsEach;

        for (int i = 0; i < 20; i++)                                  // warm JIT + pools
        {
            OtlpMetricMapper.Map(OtlpProtoDecoder.DecodeMetrics(payload, payload.Length));
            OtlpMetricProtoParser.Parse(payload);
        }

        const int iters = 200;

        var (domMs, domBytes) = Measure(iters, () =>
            OtlpMetricMapper.Map(OtlpProtoDecoder.DecodeMetrics(payload, payload.Length)));
        var (spanMs, spanBytes) = Measure(iters, () => OtlpMetricProtoParser.Parse(payload));

        double domNs   = domMs  * 1_000_000.0 / points;
        double spanNs  = spanMs * 1_000_000.0 / points;
        double domB    = domBytes  / (double)points;
        double spanB   = spanBytes / (double)points;

        _out.WriteLine($"payload   : {payload.Length / 1024.0:F1} KB protobuf, {points} data points "
                     + $"({OtlpProtoPayloads.Metrics} instruments x {OtlpProtoPayloads.PointsEach} series, "
                     + $"every {OtlpProtoPayloads.HistoEvery}rd a {OtlpProtoPayloads.Buckets}-bucket histogram)");
        _out.WriteLine($"DOM decode+map : {domMs:F3} ms/batch | {domNs:F0} ns/point | {domB:F0} B/point "
                     + $"| {1_000_000.0 / domNs:F0} k points/s/core");
        _out.WriteLine($"span parser    : {spanMs:F3} ms/batch | {spanNs:F0} ns/point | {spanB:F0} B/point "
                     + $"| {1_000_000.0 / spanNs:F0} k points/s/core");
        _out.WriteLine($"gain           : {domNs / spanNs:F1}x faster, {domB / spanB:F1}x less allocated");

        // Guard against the DOM path creeping back onto the protobuf hot path.
        Assert.True(spanBytes * 3 < domBytes,
            $"expected >=3x less allocation, got dom={domBytes} span={spanBytes}");
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
