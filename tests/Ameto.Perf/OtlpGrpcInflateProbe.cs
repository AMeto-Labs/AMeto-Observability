using System.Diagnostics;
using System.IO.Compression;
using Ameto.Core;
using Ameto.Otel;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Cost of unframing ONE compressed OTLP/gRPC request — the collector's gRPC exporter
/// defaults to gzip, so this runs on every batch it sends.
///
/// <para>Two costs that have nothing to do with the inflate itself: the output buffer was
/// rented at the full configured ceiling (8 MiB by default) whatever the message actually
/// weighed, and the compressed payload was copied out with <c>ToArray()</c> purely because
/// <c>GZipStream</c> needs a <c>Stream</c>.</para>
/// </summary>
public sealed class OtlpGrpcInflateProbe
{
    private readonly ITestOutputHelper _out;
    public OtlpGrpcInflateProbe(ITestOutputHelper o) => _out = o;

    private const int MaxInflated = 8 * 1024 * 1024;

    [Fact]
    public void UnframeAllocatesPerCall()
    {
        byte[] logs   = OtlpProtoPayloads.Logs_Realistic();
        byte[] framed = GzipFrame(logs);

        for (int i = 0; i < 20; i++) Once(framed);              // warm JIT

        const int iters = 200;
        GC.Collect();
        GC.WaitForPendingFinalizers();
        // After the collection, not before: a gen2 under memory pressure empties
        // IngestBufferPool, and one cold 512 KB inflate buffer spread over 200 iterations is
        // 2.6 KB a call of pure warm-up. What is being measured is the steady state.
        for (int i = 0; i < 5; i++) Once(framed);
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iters; i++) Once(framed);
        sw.Stop();
        long bytes = (GC.GetAllocatedBytesForCurrentThread() - b0) / iters;

        _out.WriteLine($"message : {logs.Length / 1024.0:F1} KB protobuf → {framed.Length / 1024.0:F1} KB gzip frame "
                     + $"(limit {MaxInflated / 1024 / 1024} MB)");
        _out.WriteLine($"unframe : {sw.Elapsed.TotalMicroseconds / iters:F1} us/call | {bytes} B/call allocated");

        // The inflate output and the compressed input are both borrowed, so what is left per
        // call is the GZipStream and its own internal buffer — nothing that scales with the
        // message. The old shape allocated 14 761 B a call copying the compressed payload out
        // with ToArray(), so this has to sit well under that to mean anything: 1 KB does.
        Assert.True(bytes < 1024, $"expected < 1 KB/call, got {bytes} B");
    }

    [Fact]
    public void TheInflateBufferIsSizedFromTheMessageNotTheCeiling()
    {
        byte[] framed = GzipFrame(OtlpProtoPayloads.Logs_Realistic());

        var result = OtlpGrpcFraming.TryUnframe(framed, "gzip", MaxInflated,
                                                out var message, out byte[]? rented, out int rentedLength);
        try
        {
            Assert.Equal(UnframeResult.Ok, result);
            Assert.NotNull(rented);
            Assert.Equal(message.Length, rentedLength);

            // It used to be Rent(maxInflatedBytes) whatever the message weighed — an 8 MiB
            // array for 287 KB of logs, on every compressed request a collector sends, and an
            // 8 MiB array the pool then kept. Under 2x the message is the claim that fails the
            // moment anyone sizes it from the ceiling again.
            _out.WriteLine($"message {message.Length / 1024.0:F1} KB → buffer {rented!.Length / 1024.0:F1} KB "
                         + $"(ceiling would be {MaxInflated / 1024.0:F1} KB)");
            Assert.True(rented.Length < 2 * message.Length,
                $"expected a buffer under 2x the {message.Length} B message, got {rented.Length} B");
        }
        finally
        {
            if (rented is not null) IngestBufferPool.Return(rented);
        }
    }

    private static void Once(byte[] framed)
    {
        var result = OtlpGrpcFraming.TryUnframe(framed, "gzip", MaxInflated,
                                                out var message, out byte[]? rented, out _);
        if (result != UnframeResult.Ok) throw new InvalidOperationException(result.ToString());
        if (message.Length == 0) throw new InvalidOperationException("empty message");
        if (rented is not null) IngestBufferPool.Return(rented);
    }

    /// <summary>The wire shape a gzip-compressed unary gRPC request arrives in.</summary>
    private static byte[] GzipFrame(byte[] message)
    {
        using var compressed = new MemoryStream();
        using (var gz = new GZipStream(compressed, CompressionLevel.Optimal, leaveOpen: true))
            gz.Write(message, 0, message.Length);
        byte[] payload = compressed.ToArray();

        var framed = new byte[5 + payload.Length];
        framed[0] = 1;                                           // compressed
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)payload.Length);
        payload.CopyTo(framed.AsSpan(5));
        return framed;
    }
}
