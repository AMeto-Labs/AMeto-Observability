using System.Buffers;
using System.Text.Encodings.Web;
using System.Text.Json;
using MessagePack;
using Ameto.Tracing;

namespace Ameto.Storage.Tests;

/// <summary>
/// What <c>TraceDetailJson.WriteAttributes</c> does when something goes wrong — in the WRITER
/// rather than in the blob. The blob's own failures are the parity tests' business
/// (<c>TraceDetailTranscodeParityTests</c>); these are about not turning one failure into another.
/// </summary>
public sealed class TraceDetailJsonFaultTests
{
    /// <summary>
    /// An output that hands the writer one buffer and then fails the request for a second — the
    /// shape of a response body whose pipe dies part-way through a span's attribute map.
    /// </summary>
    private sealed class FailingOutput(int budget) : IBufferWriter<byte>
    {
        private int _handedOut;
        public void Advance(int count) { }
        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (_handedOut++ >= budget) throw new IOException("the output failed");
            return new byte[Math.Max(sizeHint, 4096)];
        }
        public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
    }

    /// <summary>Eight pairs of 1 KB strings: more than the writer's first 4 KB buffer holds.</summary>
    private static byte[] BigMap()
    {
        var buf = new ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(8);
        for (int i = 0; i < 8; i++) { w.Write("k" + i); w.Write(new string((char)('a' + i), 1024)); }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    [Fact]
    public void A_writer_that_fails_mid_map_surfaces_its_own_exception_not_a_fallback_state_error()
    {
        // The fast path had ONE catch around the walk AND the write loop. A throw from the writer
        // after WriteStartObject was therefore taken for "the blob will not decode", the span fell
        // back to the dictionary path, and that path's WriteStartObject — a second object opened
        // where the writer expected a property name — threw InvalidOperationException out of the
        // writer's state checks, hiding the real failure and running a full decode for nothing.
        var span = new SpanRecord { AttributesBytes = BigMap() };
        using var w = new Utf8JsonWriter(new FailingOutput(budget: 1),
            new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
        w.WriteStartObject();
        w.WritePropertyName("attributes"u8);

        var ex = Record.Exception(() => TraceDetailJson.WriteAttributes(w, span));

        Assert.NotNull(ex);
        Assert.IsType<IOException>(ex);
        Assert.Equal("the output failed", ex.Message);
    }
}
