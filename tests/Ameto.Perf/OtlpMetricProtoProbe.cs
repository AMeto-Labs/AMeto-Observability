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

        // BEST of five runs, each over the same 200 iterations: this box is shared, and the mean
        // of a noisy run measures the neighbours. Allocation is deterministic and takes the
        // minimum too, which is the steady state once every string has been interned.
        var (domMs, domBytes) = Best(iters, () =>
            OtlpMetricMapper.Map(OtlpProtoDecoder.DecodeMetrics(payload, payload.Length)));
        var (spanMs, spanBytes) = Best(iters, () => OtlpMetricProtoParser.Parse(payload));

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

        // Guard against the DOM path creeping back onto the protobuf hot path — and, since label
        // text is interned, against a fresh string per label creeping back into the span parser:
        // that alone put it at 1 199 B/point, 5.6x under the DOM path; interned it is ~174.
        Assert.True(spanBytes * 8 < domBytes,
            $"expected >=8x less allocation, got dom={domBytes} span={spanBytes}");
    }

    private static (double MsPerIter, long Bytes) Best(int iters, Action body)
    {
        double ms    = double.MaxValue;
        long   bytes = long.MaxValue;
        for (int run = 0; run < 5; run++)
        {
            var (m, b) = Measure(iters, body);
            if (m < ms)    ms    = m;
            if (b < bytes) bytes = b;
        }
        return (ms, bytes);
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
