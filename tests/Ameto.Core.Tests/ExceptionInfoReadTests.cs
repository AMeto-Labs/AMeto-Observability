using System.Buffers;
using Ameto.Core;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Core.Tests;

/// <summary>
/// WHAT READING AN EXCEPTION COSTS. Every exception-bearing row of a cold segment goes through
/// <see cref="ExceptionInfo.FromBytes"/>, and it used to start by copying the whole payload —
/// 1-5 KB of stack trace — because a <see cref="MessagePackReader"/> wants a
/// <see cref="ReadOnlySequence{T}"/> and a span cannot be made into one. It then read every map
/// key as a string in order to switch on it: four short UTF-16 strings per exception per depth,
/// built, compared and dropped.
///
/// <para>Both are gone for callers that hold heap memory. These tests pin the two halves: the
/// results are identical to the byte, and the read does not copy what it was handed.</para>
/// </summary>
public sealed class ExceptionInfoReadTests
{
    private readonly ITestOutputHelper _out;
    public ExceptionInfoReadTests(ITestOutputHelper o) => _out = o;

    /// <summary>BY REF, and it matters: <see cref="MessagePackWriter"/> is a struct that carries
    /// its own position, so an <c>Action&lt;MessagePackWriter&gt;</c> writes into a copy and
    /// <c>Flush</c> then commits nothing — the fixtures come out empty and every parity case
    /// passes vacuously.</summary>
    private delegate void PackAction(ref MessagePackWriter writer);

    private static byte[] Pack(PackAction write)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        var bytes = buf.WrittenSpan.ToArray();
        Assert.NotEmpty(bytes);
        return bytes;
    }

    /// <summary>Every shape the wire format accepts, through both overloads, identical.</summary>
    [Fact]
    public void Both_overloads_agree_on_every_wire_shape()
    {
        var deep = new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "outer",
            StackTrace = "   at A()\n   at B()",
            Inner = new ExceptionInfo
            {
                Type    = "System.FormatException",
                Message = "middle",
                Inner   = new ExceptionInfo { Type = "System.OverflowException", Message = "deepest" },
            },
        };

        foreach (var bytes in new[]
                 {
                     deep.ToBytes(),
                     new ExceptionInfo { Type = "System.Exception" }.ToBytes(),
                     Pack((ref MessagePackWriter w) => w.Write("legacy flat string")),              // legacy CLEF @x
                     Pack((ref MessagePackWriter w) => w.WriteNil()),                               // nil
                     Pack((ref MessagePackWriter w) => w.Write(42)),                                // unknown shape
                     Pack((ref MessagePackWriter w) =>                      // unknown keys, out of order
                     {
                         w.WriteMapHeader(3);
                         w.Write("zzz");  w.Write("ignored");
                         w.Write("msg");  w.Write("only a message");
                         w.Write("type"); w.Write("Some.Type");
                     }),
                     Pack((ref MessagePackWriter w) =>                      // empty map: type defaults
                     {
                         w.WriteMapHeader(0);
                     }),
                 })
        {
            var viaSpan   = ExceptionInfo.FromBytes(bytes.AsSpan());
            var viaMemory = ExceptionInfo.FromBytes(bytes.AsMemory());
            AssertSame(viaSpan, viaMemory);

            // …and the round trip through the writer is unchanged, which is what the segment
            // and WAL formats actually depend on.
            if (viaMemory is not null)
                AssertSame(viaMemory, ExceptionInfo.FromBytes(viaMemory.ToBytes().AsMemory()));
        }

        // The depth cap still truncates at exactly MaxDepth.
        var read = ExceptionInfo.FromBytes(deep.ToBytes().AsMemory())!;
        Assert.Equal("System.OverflowException", read.Inner?.Inner?.Type);
        Assert.Null(read.Inner?.Inner?.Inner);
    }

    private static void AssertSame(ExceptionInfo? a, ExceptionInfo? b)
    {
        if (a is null || b is null) { Assert.Null(a); Assert.Null(b); return; }
        Assert.Equal(a.Type,       b.Type);
        Assert.Equal(a.Message,    b.Message);
        Assert.Equal(a.StackTrace, b.StackTrace);
        AssertSame(a.Inner, b.Inner);
    }

    /// <summary>
    /// The payload is NOT copied. Measured on a map whose bulk is a field the reader skips, so
    /// the copy is the only thing that could account for the bytes: the span overload must pay
    /// for the whole payload, the memory overload for essentially nothing.
    /// </summary>
    [Fact]
    public void Memory_overload_does_not_copy_the_payload()
    {
        const int Pad = 512 * 1024;
        var bytes = Pack((ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(3);
            w.Write("type"); w.Write("System.InvalidOperationException");
            w.Write("msg");  w.Write("boom");
            w.Write("pad");  w.Write(new string('x', Pad));   // unknown key → Skip()
        });

        // Warm: JIT, and the string literals the decode produces.
        _ = ExceptionInfo.FromBytes(bytes.AsMemory());
        _ = ExceptionInfo.FromBytes(bytes.AsSpan());

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var viaMemory = ExceptionInfo.FromBytes(bytes.AsMemory());
        long memBytes = GC.GetAllocatedBytesForCurrentThread() - b0;

        long b1 = GC.GetAllocatedBytesForCurrentThread();
        var viaSpan = ExceptionInfo.FromBytes(bytes.AsSpan());
        long spanBytes = GC.GetAllocatedBytesForCurrentThread() - b1;

        AssertSame(viaSpan, viaMemory);
        _out.WriteLine($"payload {bytes.Length / 1024.0:F0} KB -> memory overload {memBytes} B, span overload {spanBytes} B");

        Assert.True(memBytes < bytes.Length / 8,
            $"the memory overload copied the payload: {memBytes} B for a {bytes.Length} B map");
        Assert.True(spanBytes >= bytes.Length,
            "the span overload is documented as copying — if that changed, update the doc comment");
    }

    /// <summary>
    /// Keys are classified from their bytes, not read as strings. A three-level chain carries
    /// twelve of them; nothing that reads it should be building twelve short strings to switch
    /// on and drop. The budget below is the object graph plus its four VALUE strings with room
    /// to spare, and is comfortably under what a key string apiece used to add.
    /// </summary>
    [Fact]
    public void Reading_a_deep_exception_does_not_allocate_a_string_per_key()
    {
        var bytes = new ExceptionInfo
        {
            Type    = "A",
            Message = "b",
            Inner   = new ExceptionInfo
            {
                Type    = "C",
                Message = "d",
                Inner   = new ExceptionInfo { Type = "E", Message = "f", StackTrace = "g" },
            },
        }.ToBytes().AsMemory();

        for (int i = 0; i < 50; i++) _ = ExceptionInfo.FromBytes(bytes);   // warm

        const int Iterations = 1_000;
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < Iterations; i++) _ = ExceptionInfo.FromBytes(bytes);
        long perRead = (GC.GetAllocatedBytesForCurrentThread() - b0) / Iterations;

        _out.WriteLine($"3-level exception, {bytes.Length} B on the wire: {perRead} B per read");

        // 3 ExceptionInfo objects + 7 tiny value strings. Twelve key strings on top of that is
        // another ~350 B, which this bound excludes.
        Assert.True(perRead < 420, $"{perRead} B per read — a string per key is back");
    }
}
