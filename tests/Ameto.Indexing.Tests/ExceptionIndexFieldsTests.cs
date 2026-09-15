using System.Text;
using Ameto.Core;

namespace Ameto.Indexing.Tests;

/// <summary>
/// <see cref="ExceptionInfo.TryReadIndexFields"/> must see exactly what <see cref="ExceptionInfo.FromBytes"/>
/// sees of the three indexed fields, for every shape the wire format accepts, without touching
/// the rest.
/// </summary>
public sealed class ExceptionIndexFieldsTests
{
    private static void AssertSameAsDecode(byte[] bytes)
    {
        var decoded = ExceptionInfo.FromBytes(bytes);
        bool ok = ExceptionInfo.TryReadIndexFields(bytes, out var type, out var msg, out var inner);
        if (decoded is null) { Assert.False(ok); return; }
        Assert.True(ok);
        Assert.Equal(decoded.Type, Encoding.UTF8.GetString(type));
        Assert.Equal(decoded.Message ?? "", Encoding.UTF8.GetString(msg));
        Assert.Equal(decoded.Inner?.Type ?? "", Encoding.UTF8.GetString(inner));
    }

    [Fact]
    public void FullTree_WithStackAndInner()
        => AssertSameAsDecode(new ExceptionInfo
        {
            Type = "System.InvalidOperationException",
            Message = "wallet 7 не в состоянии, допускающем расчёт",
            StackTrace = string.Join('\n', Enumerable.Range(0, 40).Select(f => $"   at Ameto.Payments.Step{f}() in /src/S.cs:line {f}")),
            Inner = new ExceptionInfo { Type = "System.TimeoutException", Message = "ledger", StackTrace = "   at L()",
                                        Inner = new ExceptionInfo { Type = "Deepest", Message = "ignored by the index" } },
        }.ToBytes());

    [Fact]
    public void TypeOnly() => AssertSameAsDecode(new ExceptionInfo { Type = "X" }.ToBytes());

    [Fact]
    public void Nil_IsNoException()
    {
        Assert.Null(ExceptionInfo.FromBytes(new byte[] { 0xC0 }));
        Assert.False(ExceptionInfo.TryReadIndexFields(new byte[] { 0xC0 }, out _, out _, out _));
        Assert.False(ExceptionInfo.TryReadIndexFields(ReadOnlyMemory<byte>.Empty, out _, out _, out _));
    }

    [Fact]
    public void LegacyPlainString_IsTypeExceptionWithMessage()
    {
        var w = new MessagePack.MessagePackWriter(new System.Buffers.ArrayBufferWriter<byte>());
        var buf = new System.Buffers.ArrayBufferWriter<byte>();
        w = new MessagePack.MessagePackWriter(buf);
        w.Write("Something broke");
        w.Flush();
        AssertSameAsDecode(buf.WrittenSpan.ToArray());
    }

    [Fact]
    public void UnknownKeys_AndNonStringValues_AreSkipped()
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new MessagePack.MessagePackWriter(buf);
        w.WriteMapHeader(6);
        w.Write("extra"); w.WriteArrayHeader(2); w.Write(1); w.Write("two");
        w.Write("type");  w.Write("T");
        w.Write("stk");   w.Write(new string('s', 5000));
        w.Write("msg");   w.Write("M");
        w.Write("inner"); w.WriteMapHeader(2); w.Write("stk"); w.Write("x"); w.Write("type"); w.Write("I");
        w.Write("tail");  w.Write(42L);
        w.Flush();
        AssertSameAsDecode(buf.WrittenSpan.ToArray());
    }

    [Theory]
    [InlineData("legacy inner text")]
    [InlineData("")]
    public void LegacyInnerString_ReadsAsDecodeDoes(string inner)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new MessagePack.MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("type");  w.Write("Outer");
        w.Write("inner"); w.Write(inner);
        w.Flush();
        AssertSameAsDecode(buf.WrittenSpan.ToArray());
    }

    [Fact]
    public void NilFields_ReadAsDecodeDoes()
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new MessagePack.MessagePackWriter(buf);
        w.WriteMapHeader(3);
        w.Write("type");  w.WriteNil();
        w.Write("msg");   w.WriteNil();
        w.Write("inner"); w.WriteNil();
        w.Flush();
        AssertSameAsDecode(buf.WrittenSpan.ToArray());
    }

    [Fact]
    public void Truncated_ReturnsFalse_DoesNotThrow()
    {
        var full = new ExceptionInfo { Type = "System.Exception", Message = "hello" }.ToBytes();
        Assert.False(ExceptionInfo.TryReadIndexFields(full.AsMemory(0, full.Length - 3), out _, out _, out _));
    }
}
