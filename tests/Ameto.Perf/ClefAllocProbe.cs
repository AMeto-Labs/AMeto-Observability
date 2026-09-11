using System.Buffers;
using System.Diagnostics;
using MessagePack;
using Ameto.Core;
using Ameto.Core.Serialization;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Allocation and time per CLEF batch on the <c>/api/events</c> ingest path: the old
/// LogEvent-materialising read (<see cref="LogEventSerializer.DeserializeBatch"/> into a
/// list, exactly what IngestionEndpoint used to do) against the streaming read
/// (<see cref="LogEventSerializer.StreamBatch"/> straight into a sink).
/// </summary>
public sealed class ClefAllocProbe
{
    private const int EventsPerBatch = 1000;

    private sealed class NullSink : LogEventSerializer.IClefBatchSink
    {
        public int Count;
        public long PropBytes;
        public bool TryIngestClef(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ExceptionInfo? exception,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8)
        {
            Count++;
            PropBytes += msgpackProps.Length;   // touch it, as the ring's copy would
            return true;
        }
    }

    private readonly ITestOutputHelper _out;
    public ClefAllocProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void StreamingClefAllocatesFarLessThanLogEventMaterialisation()
    {
        byte[] body = BuildBatch(EventsPerBatch);
        var    sink = new NullSink();

        for (int i = 0; i < 10; i++)
        {
            LogEventSerializer.StreamBatch(body, sink, out _);
            RunLegacy(body);
        }

        const int iters  = 50;
        const int rounds = 5;

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) RunLegacy(body);
        long legacyBytes = (GC.GetAllocatedBytesForCurrentThread() - b0) / iters;

        long b1 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) LogEventSerializer.StreamBatch(body, sink, out _);
        long streamBytes = (GC.GetAllocatedBytesForCurrentThread() - b1) / iters;

        // Wall clock on a shared dev box is noisy (other builds, other agents): take the
        // best round of each, which is the one least disturbed by something else running.
        double legacyNs = double.MaxValue, streamNs = double.MaxValue;
        var sw = new Stopwatch();
        for (int r = 0; r < rounds; r++)
        {
            sw.Restart();
            for (int i = 0; i < iters; i++) RunLegacy(body);
            sw.Stop();
            legacyNs = Math.Min(legacyNs, sw.Elapsed.TotalNanoseconds / iters);

            sw.Restart();
            for (int i = 0; i < iters; i++) LogEventSerializer.StreamBatch(body, sink, out _);
            sw.Stop();
            streamNs = Math.Min(streamNs, sw.Elapsed.TotalNanoseconds / iters);
        }

        _out.WriteLine(
            $"CLEF /api/events — {EventsPerBatch:N0}-event batch ({body.Length:N0} B body):\n" +
            $"  LogEvent materialisation (before): {legacyBytes,10:N0} B/batch  {legacyBytes / (double)EventsPerBatch,7:F1} B/event  {legacyNs / EventsPerBatch,7:F0} ns/event\n" +
            $"  streaming into the sink  (after) : {streamBytes,10:N0} B/batch  {streamBytes / (double)EventsPerBatch,7:F1} B/event  {streamNs / EventsPerBatch,7:F0} ns/event\n" +
            $"  reduction                        : {(legacyBytes == 0 ? 0 : 100.0 * (legacyBytes - streamBytes) / legacyBytes):F1}% bytes, " +
            $"{(streamNs <= 0 ? 0 : legacyNs / streamNs):F2}x faster");

        // Guard against re-introducing per-event objects on the CLEF hot path. This batch
        // has no exceptions, so the streaming path should allocate essentially nothing.
        Assert.True(streamBytes * 10 < legacyBytes,
            $"expected the streaming CLEF read to allocate <1/10 of the LogEvent path, got {streamBytes:N0} vs {legacyBytes:N0} B/batch");
    }


    /// <summary>Exactly what IngestionEndpoint.HandleAsync used to do before streaming.</summary>
    private static int RunLegacy(byte[] body)
    {
        var  events  = new List<LogEvent>(64);
        uint nextSeq = 0;
        LogEventSerializer.DeserializeBatch(
            new ReadOnlySequence<byte>(body), NodeId.Local.Value, ref nextSeq, events);

        int n = 0;
        for (int i = 0; i < events.Count; i++)
            n += events[i].RawProperties.Length;
        return n;
    }

    /// <summary>A batch shaped like a real Serilog/Seq sink post: 8 CLEF fields + 4 props.</summary>
    private static byte[] BuildBatch(int count)
    {
        var buf = new ArrayBufferWriter<byte>(count * 512);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(count);

        var ts = new DateTimeOffset(2024, 3, 1, 10, 20, 30, TimeSpan.Zero);
        for (int i = 0; i < count; i++)
        {
            w.WriteMapHeader(10);
            w.Write("@t");           w.Write(ts.AddMilliseconds(i).UtcDateTime.ToString("O"));
            w.Write("@l");           w.Write((i % 7) == 0 ? "Warning" : "Information");
            w.Write("@mt");          w.Write("Order {OrderId} for {Customer} moved to {State} in {Region}");
            w.Write("@tr");          w.Write("4bf92f3577b34da6a3ce929d0e0e4736");
            w.Write("@sp");          w.Write("00f067aa0ba902b7");
            w.Write("service.name"); w.Write("checkout-api");
            w.Write("OrderId");      w.Write((long)i);
            w.Write("Customer");     w.Write("ACME GmbH (Frankfurt)");
            w.Write("State");        w.Write("AwaitingPayment");
            w.Write("Region");       w.Write("eu-central-1");
        }

        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
