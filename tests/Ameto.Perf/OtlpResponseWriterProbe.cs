using System.Diagnostics;
using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using Ameto.Otel;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Cost of the reply every ingest call gets: <c>{"ingested":N,"dropped":M}</c>, 28 bytes of
/// fixed-shape JSON.
///
/// <para>It went through a <see cref="Utf8JsonWriter"/> constructed per request — an object,
/// its rented state and its bookkeeping — for two integers. Small next to a batch, but it is
/// paid once per request whether the batch held one record or ten thousand, which is exactly
/// where a small per-request cost matters.</para>
/// </summary>
public sealed class OtlpResponseWriterProbe
{
    private readonly ITestOutputHelper _out;
    public OtlpResponseWriterProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void FormattedReplyAllocatesNothing()
    {
        var sink = PipeWriter.Create(Stream.Null);

        for (int i = 0; i < 50; i++) { ViaJsonWriter(sink, i, 0); ViaFormatter(sink, i, 0); }

        const int iters = 20_000;
        Measure(iters, i => ViaFormatter(sink, i, i % 7));            // both fully warm before either counts
        Measure(iters, i => ViaJsonWriter(sink, i, i % 7));
        var (fmtUs,   fmtBytes)  = Measure(iters, i => ViaFormatter(sink, i, i % 7));
        var (jsonUs,  jsonBytes) = Measure(iters, i => ViaJsonWriter(sink, i, i % 7));

        _out.WriteLine($"Utf8JsonWriter : {jsonUs:F3} us/reply | {jsonBytes} B/reply");
        _out.WriteLine($"Utf8Formatter  : {fmtUs:F3} us/reply | {fmtBytes} B/reply");

        Assert.Equal(0, fmtBytes);
    }

    [Fact]
    public void FormattedReplyIsByteIdenticalToTheJsonWriter()
    {
        // Outside the loop: a stackalloc inside one grows the frame per iteration (CA2014).
        Span<byte> actual = stackalloc byte[OtlpEndpointMapper.JsonOkMaxBytes];

        foreach ((int ingested, int dropped) in
                 new[] { (0, 0), (1, 0), (0, 1), (1000, 17), (int.MaxValue, int.MaxValue) })
        {
            var expected = new ArrayBufferWriter<byte>(64);
            var jw = new Utf8JsonWriter(expected);
            jw.WriteStartObject();
            jw.WriteNumber("ingested"u8, ingested);
            jw.WriteNumber("dropped"u8,  dropped);
            jw.WriteEndObject();
            jw.Flush();

            int n = OtlpEndpointMapper.FormatJsonOk(actual, ingested, dropped);

            Assert.Equal(Encoding.UTF8.GetString(expected.WrittenSpan), Encoding.UTF8.GetString(actual[..n]));
        }
    }

    private static void ViaJsonWriter(PipeWriter writer, int ingested, int dropped)
    {
        var jw = new Utf8JsonWriter(writer);
        jw.WriteStartObject();
        jw.WriteNumber("ingested"u8, ingested);
        jw.WriteNumber("dropped"u8,  dropped);
        jw.WriteEndObject();
        jw.Flush();
        Flush(writer);
    }

    private static void ViaFormatter(PipeWriter writer, int ingested, int dropped)
    {
        writer.Advance(OtlpEndpointMapper.FormatJsonOk(writer.GetSpan(OtlpEndpointMapper.JsonOkMaxBytes),
                                                       ingested, dropped));
        Flush(writer);
    }

    /// <summary>The real handler awaits this; against Stream.Null it always completes inline.</summary>
    private static void Flush(PipeWriter writer)
    {
        var flush = writer.FlushAsync();
        if (!flush.IsCompletedSuccessfully) flush.AsTask().GetAwaiter().GetResult();
    }

    private static (double UsPerIter, long Bytes) Measure(int iters, Action<int> body)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iters; i++) body(i);
        sw.Stop();
        return (sw.Elapsed.TotalMicroseconds / iters,
                (GC.GetAllocatedBytesForCurrentThread() - b0) / iters);
    }
}
