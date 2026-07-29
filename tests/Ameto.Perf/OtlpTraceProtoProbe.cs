using System.Diagnostics;
using Ameto.Otel;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Cost of the OTLP/protobuf trace ingest path, so the decision to rewrite it (or not)
/// rests on a number rather than on symmetry with the metrics path. Decode + map only —
/// everything upstream of the ring buffer.
/// </summary>
public sealed class OtlpTraceProtoProbe
{
    private readonly ITestOutputHelper _out;
    public OtlpTraceProtoProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void DomDecodeCostPerSpan()
    {
        const int spans = 200;
        byte[] payload = OtlpProtoPayloads.Traces_Realistic(spans);

        for (int i = 0; i < 20; i++)
            OtlpTraceMapper.Map(OtlpProtoDecoder.DecodeTraces(payload, payload.Length)!);

        const int iters = 200;
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iters; i++)
            OtlpTraceMapper.Map(OtlpProtoDecoder.DecodeTraces(payload, payload.Length)!);
        sw.Stop();
        long bytes = (GC.GetAllocatedBytesForCurrentThread() - b0) / iters;

        double msPerBatch = sw.Elapsed.TotalMilliseconds / iters;
        double nsPerSpan  = msPerBatch * 1_000_000.0 / spans;

        // The sandbox stand ingests ~4 spans/s (≈500 spans per 2-3 min flush).
        double standCpuPercent = nsPerSpan * 4 / 1_000_000_000.0 * 100.0;

        _out.WriteLine($"payload    : {payload.Length / 1024.0:F1} KB protobuf, {spans} server spans");
        _out.WriteLine($"decode+map : {msPerBatch:F3} ms/batch | {nsPerSpan:F0} ns/span | {bytes / (double)spans:F0} B/span");
        _out.WriteLine($"throughput : {1_000_000.0 / nsPerSpan:F0} k spans/s per core");
        _out.WriteLine($"at the stand's ~4 spans/s this path costs {standCpuPercent:F4} % of one core");
    }
}
