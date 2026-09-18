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
    /// <see cref="ExceptionInfo.IsPresent"/> must agree with <see cref="ExceptionInfo.FromBytes"/>
    /// on every payload FromBytes ACCEPTS, because <c>LogEvent.HasException</c> is built on it
    /// and that is what answers <c>has(@x)</c>. "Non-empty payload" is not the same question:
    /// nil, an empty legacy string and any other shape all carry bytes and all decode to null.
    ///
    /// <para>A truncated payload, which FromBytes throws on, is NOT covered by that agreement —
    /// IsPresent reads the header only and answers it either way. See
    /// <see cref="IsPresent_answers_a_truncated_payload_from_its_header_alone"/>.</para>
    /// </summary>
    [Fact]
    public void IsPresent_agrees_with_FromBytes_on_every_shape()
    {
        var payloads = new List<byte[]>
        {
            new ExceptionInfo { Type = "System.Exception" }.ToBytes(),
            new ExceptionInfo { Type = "T", Message = "m", StackTrace = "s" }.ToBytes(),
            Pack((ref MessagePackWriter w) => w.WriteMapHeader(0)),          // empty map ⇒ present
            Pack((ref MessagePackWriter w) => w.Write("legacy")),            // non-empty string
            Pack((ref MessagePackWriter w) => w.Write("")),                  // EMPTY string ⇒ null
            Pack((ref MessagePackWriter w) => w.WriteNil()),                 // nil ⇒ null
            Pack((ref MessagePackWriter w) => w.Write(42)),                  // wrong shape ⇒ null
            Pack((ref MessagePackWriter w) => w.Write(true)),                // wrong shape ⇒ null
            Pack((ref MessagePackWriter w) => w.WriteArrayHeader(0)),        // wrong shape ⇒ null
            Pack((ref MessagePackWriter w) => w.Write(new string('x', 40))), // str8
            Pack((ref MessagePackWriter w) => w.Write(new string('x', 400))),// str16
            Pack((ref MessagePackWriter w) => w.Write(new string('x', 70_000))), // str32
        };

        // A map big enough to need a map16 header.
        payloads.Add(Pack((ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(20);
            for (int i = 0; i < 20; i++) { w.Write("k" + i); w.Write(i); }
        }));

        foreach (var bytes in payloads)
        {
            bool decoded = ExceptionInfo.FromBytes(bytes.AsMemory()) is not null;
            Assert.True(decoded == ExceptionInfo.IsPresent(bytes),
                $"IsPresent disagreed with FromBytes on a {bytes.Length} B payload starting 0x{bytes[0]:X2}");
        }

        Assert.False(ExceptionInfo.IsPresent(ReadOnlySpan<byte>.Empty));
    }

    /// <summary>
    /// THE TRUNCATED PAYLOAD, in both directions. IsPresent answers from the msgpack header and
    /// never walks the body, so what decides is the KIND of header rather than how far the cut
    /// went: any map header says present, a string header announcing a non-zero length says
    /// present however little body follows it, and a string header too short to hold its own
    /// length says absent. FromBytes throws on all of them.
    ///
    /// <para>So a corrupt row falls IN to <c>has(@x)</c> when its body is cut — and also when a
    /// map16 or map32 header is cut short of its own entry count, because that branch tests the
    /// lead byte and no length. A cut STRING header is the one shape that falls out. That is a
    /// decision the doc on <see cref="ExceptionInfo.IsPresent"/> states, and this is what fails
    /// if IsPresent is ever made strict — walking the body, or length-checking the map headers
    /// to "match the doc" — or made to throw, any of which would change which corrupt rows a
    /// presence filter returns.</para>
    /// </summary>
    [Fact]
    public void IsPresent_answers_a_truncated_payload_from_its_header_alone()
    {
        (byte[] Bytes, bool Present, string Shape)[] cases =
        [
            ([0xD9, 0x05, 0x61],       true,  "str8 announcing 5 bytes, carrying 1"),
            ([0x81],                   true,  "fixmap announcing 1 entry, carrying none"),
            ([0xDE, 0x00],             true,  "map16 header cut short of its count"),
            ([0xDF, 0x00, 0x00, 0x00], true,  "map32 header cut short of its count"),
            ([0xDB, 0x00, 0x00, 0x00], false, "str32 header cut short of its length"),
        ];

        foreach (var (bytes, present, shape) in cases)
        {
            Assert.True(present == ExceptionInfo.IsPresent(bytes),
                $"{shape}: IsPresent said {!present}, the header says {present}");

            Assert.Throws<EndOfStreamException>(() => ExceptionInfo.FromBytes(bytes.AsMemory()));
            Assert.Throws<EndOfStreamException>(() => ExceptionInfo.FromBytes(bytes.AsSpan()));
        }
    }

    /// <summary>
    /// THE STRING FALLBACK IS REACHABLE. <c>TryReadStringSpan</c> fails when a key straddles
    /// two segments of the sequence, and the classifier then falls back to reading it as a
    /// string — a branch <c>FromBytes</c> can never take, because it hands the reader ONE
    /// contiguous buffer. <see cref="ExceptionInfo.Read"/> is public and takes whatever reader
    /// the caller has, so the branch is not dead: this splits every key down the middle and
    /// asserts the same answer as the contiguous read.
    /// </summary>
    [Fact]
    public void A_key_split_across_buffer_segments_still_classifies()
    {
        var bytes = new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "split me",
            StackTrace = "   at A()",
            Inner      = new ExceptionInfo { Type = "System.FormatException" },
        }.ToBytes();

        var contiguous = ExceptionInfo.FromBytes(bytes.AsMemory());

        // One byte per segment: every multi-byte token, keys included, straddles a boundary.
        var reader = new MessagePackReader(ByteAtATime(bytes));
        var split  = ExceptionInfo.Read(ref reader);

        AssertSame(contiguous, split);
        Assert.Equal("System.InvalidOperationException", split!.Type);
        Assert.Equal("split me", split.Message);
        Assert.Equal("   at A()", split.StackTrace);
        Assert.Equal("System.FormatException", split.Inner?.Type);
    }

    private sealed class Seg : ReadOnlySequenceSegment<byte>
    {
        public Seg(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }
        public Seg Append(ReadOnlyMemory<byte> next)
        {
            var seg = new Seg(next, RunningIndex + Memory.Length);
            Next = seg;
            return seg;
        }
    }

    private static ReadOnlySequence<byte> ByteAtATime(byte[] bytes)
    {
        var first = new Seg(bytes.AsMemory(0, 1), 0);
        var last  = first;
        for (int i = 1; i < bytes.Length; i++) last = last.Append(bytes.AsMemory(i, 1));
        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }

    /// <summary>
    /// A key that is not a string at all is not something the fallback rescues — both
    /// overloads refuse the payload the same way, which is what the segment and WAL readers
    /// already treat as corruption.
    /// </summary>
    [Fact]
    public void A_non_string_key_is_refused_by_both_overloads()
    {
        var bytes = Pack((ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(1);
            w.Write(7);                 // integer key — not a shape the format ever writes
            w.Write("whatever");
        });

        Assert.Throws<MessagePackSerializationException>(() => ExceptionInfo.FromBytes(bytes.AsMemory()));
        Assert.Throws<MessagePackSerializationException>(() => ExceptionInfo.FromBytes(bytes.AsSpan()));
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
