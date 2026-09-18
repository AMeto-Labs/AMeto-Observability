using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Ameto.Otel;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Cost of the OTLP/protobuf TRACE ingest path — the encoding every SDK exporter and the
/// collector send, and therefore the one production runs on. Decode + map only: everything
/// upstream of the ring buffer.
///
/// <para>The old route allocated a parser object and copied the payload for every nested
/// message (ScopeSpans, Span, Status, and a KeyValue AND an AnyValue per attribute), built an
/// OTLP object graph — <c>events[]</c> and <c>links[]</c> included, which the mapper never read
/// — and round-tripped all three ids through lowercase hex and both timestamps and every int
/// attribute through strings.</para>
/// </summary>
public sealed class OtlpTraceProtoProbe
{
    private readonly ITestOutputHelper _out;
    public OtlpTraceProtoProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void SpanParserBeatsDomPath()
    {
        // Two shapes. Both carry what a real SDK sends and the DOM used to materialise and throw
        // away — trace_state, flags, dropped counts, an exception event on every error span and
        // a link on every fourth. "scalar" is the shape the byte-parity test uses; "realistic"
        // adds the array attribute, the one value type on which the two paths disagree by
        // design, so it is the honest per-span cost and the parity payload is the other.
        Run("scalar   ", nestedAttr: false);
        Run("realistic", nestedAttr: true);
    }

    private void Run(string label, bool nestedAttr)
    {
        const int spans = 200;
        byte[] payload = OtlpProtoPayloads.Traces_Realistic(spans, nestedAttr);

        for (int i = 0; i < 20; i++)                                  // warm JIT + thread scratch
        {
            OtlpTraceMapper.Map(OtlpProtoDecoder.DecodeTraces(payload, payload.Length));
            OtlpTraceProtoParser.Parse(payload);
        }

        const int iters = 200;

        var (domMs,  domBytes)  = Measure(iters, () =>
            OtlpTraceMapper.Map(OtlpProtoDecoder.DecodeTraces(payload, payload.Length)));
        var (spanMs, spanBytes) = Measure(iters, () => OtlpTraceProtoParser.Parse(payload));

        double domNs  = domMs  * 1_000_000.0 / spans;
        double spanNs = spanMs * 1_000_000.0 / spans;
        double domB   = domBytes  / (double)spans;
        double spanB  = spanBytes / (double)spans;

        _out.WriteLine($"[{label}] payload   : {payload.Length / 1024.0:F1} KB protobuf, {spans} server spans");
        _out.WriteLine($"[{label}] DOM+map   : {domMs:F3} ms/batch | {domNs:F0} ns/span | {domB:F0} B/span "
                     + $"| {1_000_000.0 / domNs:F0} k spans/s/core");
        _out.WriteLine($"[{label}] parser    : {spanMs:F3} ms/batch | {spanNs:F0} ns/span | {spanB:F0} B/span "
                     + $"| {1_000_000.0 / spanNs:F0} k spans/s/core");
        _out.WriteLine($"[{label}] gain      : {domNs / spanNs:F1}x faster, {domB / spanB:F1}x less allocated "
                     + $"({domBytes / 1024.0:F0} KB → {spanBytes / 1024.0:F0} KB per batch)");

        // What is left per span is the name string, the attribute blob and the item itself —
        // the JSON path's floor, and zero only once the raw span sink lands (WP8). This guards
        // against the DOM path creeping back onto the hot route, not a tolerance to tune.
        // Only allocation is asserted. Wall time swings by 2x between runs on a shared box —
        // it is printed, not gated, because a flaky guard is a guard that gets deleted.
        Assert.True(spanBytes * 4 < domBytes,
            $"{label}: expected >=4x less allocation, got dom={domBytes} span={spanBytes}");
    }

    /// <summary>
    /// The other per-request cost on this route, and the one that did not depend on the encoding:
    /// the decoded-span-count line.
    ///
    /// <para><c>logger.LogDebug("… {SpanCount} spans", spans.Count)</c> binds to the
    /// <c>params object?[]</c> overload, which allocates the array and boxes the int at the call
    /// site — BEFORE <c>IsEnabled</c> is consulted. So it cost on every request at production log
    /// levels, where the line is never written. The pre-compiled
    /// <see cref="LoggerMessage.Define{T}"/> delegate asks first and formats nothing.</para>
    ///
    /// <para>It lives here rather than in an integration test because the difference is
    /// allocation, not behaviour, and measuring it needs no host.</para>
    /// </summary>
    [Fact]
    public void TheDecodedSpanCountLineCostsNothingWhenDebugIsOff()
    {
        var logger = new LevelLogger(enabled: false);
        for (int i = 0; i < 100; i++) OtlpEndpointMapper.LogTracesDecoded(logger, 200);

        const int iters = 10_000;
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) OtlpEndpointMapper.LogTracesDecoded(logger, 200);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - b0;

        _out.WriteLine($"debug off  : {bytes / (double)iters:F1} B per request ({bytes} B over {iters})");
        Assert.Equal(0, bytes);
        Assert.Equal(0, logger.Written);

        // And it still writes the line when Debug IS on — an allocation-free logger that logs
        // nothing would pass the assertion above and lose the diagnostic.
        var on = new LevelLogger(enabled: true);
        OtlpEndpointMapper.LogTracesDecoded(on, 200);
        Assert.Equal(1, on.Written);
        Assert.Contains("200 spans", on.Last);
    }

    /// <summary>An <see cref="ILogger"/> that answers one question and records what it was told.</summary>
    private sealed class LevelLogger(bool enabled) : ILogger
    {
        public int Written;
        public string Last = string.Empty;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => enabled;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, EventId eventId, TState state,
                                Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Written++;
            Last = formatter(state, exception);
        }
    }

    // The closure is built before the byte counter is sampled, so it is not in the measurement.
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
