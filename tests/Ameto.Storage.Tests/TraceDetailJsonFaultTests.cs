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

    // ── A map that will not decode is decided once ───────────────────────────

    private delegate void BlobWriter(ref MessagePackWriter w);

    private static byte[] Blob(BlobWriter write)
    {
        var buf = new ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    public static TheoryData<string, byte[]> Undecodable() => new()
    {
        { "torn",            Blob(static (ref MessagePackWriter w) => { w.WriteMapHeader(3); w.Write("a"); w.Write("b"); }) },
        { "integer key",     Blob(static (ref MessagePackWriter w) => { w.WriteMapHeader(2); w.Write("a"); w.Write("b"); w.Write(7L); w.Write("c"); }) },
        { "uint64 too big",  Blob(static (ref MessagePackWriter w) => { w.WriteMapHeader(2); w.Write("a"); w.Write("b"); w.Write("big"); w.WriteUInt64(ulong.MaxValue); }) },
        { "not a map",       Blob(static (ref MessagePackWriter w) => { w.WriteArrayHeader(1); w.Write(1L); }) },
    };

    /// <summary>
    /// The walk makes Decode's reader calls in Decode's order, so it throws exactly where Decode
    /// would — and Decode's answer to a throw is null, which the reference path writes as <c>{}</c>.
    /// Declining to that path therefore bought nothing but a SECOND exception per span per request
    /// (the walk's, then Decode's), where the DTO path paid one, memoised on a hot record. The walk's
    /// verdict is now the answer: <c>{}</c>, one exception.
    /// </summary>
    [Theory]
    [MemberData(nameof(Undecodable))]
    public void A_map_that_will_not_decode_is_written_empty_after_one_exception_not_two(string shape, byte[] blob)
    {
        var span   = new SpanRecord { AttributesBytes = blob };
        var buf    = new ArrayBufferWriter<byte>();
        int thread = Environment.CurrentManagedThreadId;
        int thrown = 0;
        EventHandler<System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs> count =
            (_, _) => { if (Environment.CurrentManagedThreadId == thread) Interlocked.Increment(ref thrown); };

        AppDomain.CurrentDomain.FirstChanceException += count;
        try
        {
            using var w = new Utf8JsonWriter(buf, new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
            TraceDetailJson.WriteAttributes(w, span);
        }
        finally { AppDomain.CurrentDomain.FirstChanceException -= count; }

        Assert.Equal("{}", System.Text.Encoding.UTF8.GetString(buf.WrittenSpan));
        Assert.True(thrown == 1, $"{shape}: {thrown} exception(s) thrown to write one empty map");
    }
}
