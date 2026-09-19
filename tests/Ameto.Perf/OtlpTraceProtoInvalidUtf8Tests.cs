using System.Buffers;
using System.Text;
using Ameto.Otel;
using Google.Protobuf;
using MessagePack;
using Xunit;
using static Ameto.Perf.OtlpProtoPayloads;

namespace Ameto.Perf;

/// <summary>
/// Protobuf strings are not guaranteed to be valid UTF-8, and an exporter sending Latin-1 or
/// cp1251 text sends invalid sequences. The DOM path read every string through
/// <c>CodedInputStream.ReadString()</c>, which replaces each invalid sequence with U+FFFD, so
/// that is what storage held. <see cref="OtlpTraceProtoParser"/> copies keys, string values, the
/// span name and the service name from the wire, and must store the same replaced text rather
/// than the raw invalid bytes: TraceQL's attribute predicates compare bytes, so verbatim copies
/// match differently from main.
///
/// <para>Every attribute comparison here is on msgpack BYTES. Decoding the blob back to strings
/// would replace the invalid sequences on the way out and hide exactly the difference under
/// test.</para>
/// </summary>
public sealed class OtlpTraceProtoInvalidUtf8Tests
{
    // Latin-1 é, a stray continuation byte, a truncated 2-byte lead, a lead byte no sequence
    // starts with (0xF5, 0xFE), a truncated 3-byte sequence and an encoded surrogate.
    private static readonly byte[] BadKey      = [(byte)'a', 0xFF];
    private static readonly byte[] BadValue    = [(byte)'c', (byte)'a', (byte)'f', 0xE9];
    private static readonly byte[] BadValue2   = [(byte)'v', 0x80, (byte)'w', 0xC3];
    private static readonly byte[] BadService  = [(byte)'s', (byte)'v', (byte)'c', 0xFE];
    private static readonly byte[] BadName     = [(byte)'G', 0xE0, 0x80, (byte)'T'];
    private static readonly byte[] BadInnerKey = [(byte)'i', (byte)'n', 0xF5];
    private static readonly byte[] BadInnerVal = [(byte)'y', 0xED, 0xA0, 0x80];     // encoded surrogate

    [Fact]
    public void InvalidUtf8_InKeysValuesNameAndService_IsStoredAsTheDomPathStoredIt()
    {
        byte[] payload = Msg(c =>
            Nested(c, 1, Msg(rs =>
            {
                Nested(rs, 1, Msg(res =>
                {
                    Nested(res, 1, RawStringAttr("service.name"u8.ToArray(), BadService));
                    Nested(res, 1, RawStringAttr(BadKey, "resource"u8.ToArray()));
                }));
                Nested(rs, 2, Msg(ss => Nested(ss, 2, Msg(sp =>
                {
                    sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
                    sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
                    sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
                    sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("b7ad6b7169203331")));
                    RawString(sp, 5, BadName);
                    sp.WriteTag(7, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_000_000_000UL);
                    sp.WriteTag(8, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_100_000_000UL);
                    Nested(sp, 9, RawStringAttr(BadKey, "x"u8.ToArray()));
                    Nested(sp, 9, RawStringAttr("latin1"u8.ToArray(), BadValue));
                    Nested(sp, 9, RawStringAttr("mixed"u8.ToArray(), BadValue2));
                }))));
            })));

        var dom    = OtlpTraceMapper.Map(OtlpProtoDecoder.DecodeTraces(payload, payload.Length));
        var parsed = OtlpTraceProtoParser.Parse(payload);

        var d = Assert.Single(dom);
        var s = Assert.Single(parsed);
        Assert.Equal(d.Name,        s.Name);
        Assert.Equal(d.ServiceName, s.ServiceName);
        Assert.True(d.AttributesBytes.AsSpan().SequenceEqual(s.AttributesBytes),
            $"attribute bytes differ\n  dom: {Convert.ToHexString(d.AttributesBytes)}\n  new: {Convert.ToHexString(s.AttributesBytes)}");

        // And the replacement really happened, rather than both paths agreeing on raw bytes.
        Assert.Contains('�', s.Name);
        Assert.Contains('�', s.ServiceName);
    }

    /// <summary>
    /// Nested kvlist and array strings go through the same writers. The DOM path cannot be the
    /// oracle here (it wrote every nested value as nil), so the expected bytes are what it would
    /// have written for the replaced strings.
    /// </summary>
    [Fact]
    public void InvalidUtf8_InNestedKvlistAndArrayStrings_IsReplacedAsReadStringReplacedIt()
    {
        byte[] payload = Msg(c =>
            Nested(c, 1, Msg(rs =>
                Nested(rs, 2, Msg(ss => Nested(ss, 2, Msg(sp =>
                {
                    sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
                    sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
                    sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
                    sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("b7ad6b7169203331")));
                    Nested(sp, 9, Msg(kv =>
                    {
                        RawString(kv, 1, "ctx"u8.ToArray());
                        Nested(kv, 2, Msg(v => Nested(v, 6, Msg(kvl =>
                        {
                            Nested(kvl, 1, RawStringAttr(BadInnerKey, BadInnerVal));
                            Nested(kvl, 1, Msg(nest =>
                            {
                                RawString(nest, 1, "arr"u8.ToArray());
                                Nested(nest, 2, Msg(v2 => Nested(v2, 5, Msg(arr =>
                                    Nested(arr, 1, Msg(e => RawString(e, 1, BadValue)))))));
                            }));
                        }))));
                    }));
                })))))));

        var span = Assert.Single(OtlpTraceProtoParser.Parse(payload));

        var buf = new ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("ctx");
        w.WriteMapHeader(2);
        w.Write(Encoding.UTF8.GetString(BadInnerKey));
        w.Write(Encoding.UTF8.GetString(BadInnerVal));
        w.Write("arr");
        w.WriteArrayHeader(1);
        w.Write(Encoding.UTF8.GetString(BadValue));
        w.Flush();

        Assert.True(buf.WrittenSpan.SequenceEqual(span.AttributesBytes),
            $"attribute bytes differ\n  expected: {Convert.ToHexString(buf.WrittenSpan)}\n  actual  : {Convert.ToHexString(span.AttributesBytes)}");
    }

    /// <summary>
    /// The check that keeps invalid input correct must not cost valid input anything: after the
    /// thread scratch has grown, a realistic batch parses with nothing but the per-span name
    /// string, attribute blob and item — the shape the JSON path already had, and the floor
    /// until the raw span sink lands.
    /// </summary>
    [Fact]
    public void ValidInput_AllocatesOnlyThePerSpanItem()
    {
        byte[] payload = Traces_Realistic();
        for (int i = 0; i < 20; i++) OtlpTraceProtoParser.Parse(payload);

        const int iters = 50;
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) OtlpTraceProtoParser.Parse(payload);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - b0;

        // The DOM path allocated 10 245 B/span on this payload. What is left here is the ~400 B
        // attribute blob, the name string and the item — nothing per attribute, nothing per
        // nested message, and nothing at all for the events and links it no longer materialises.
        double perSpan = bytes / (double)iters / 200;
        Assert.True(perSpan < 700, $"expected under 700 B/span, got {perSpan:F0} B/span ({bytes} B total)");
    }

    /// <summary>A length-delimited field holding arbitrary bytes: WriteString cannot produce invalid UTF-8.</summary>
    private static void RawString(CodedOutputStream c, int field, byte[] bytes)
    {
        c.WriteTag(field, WireFormat.WireType.LengthDelimited);
        c.WriteBytes(ByteString.CopyFrom(bytes));
    }

    private static byte[] RawStringAttr(byte[] key, byte[] value) => Msg(c =>
    {
        RawString(c, 1, key);
        Nested(c, 2, Msg(v => RawString(v, 1, value)));
    });
}
