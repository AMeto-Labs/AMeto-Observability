using System.Buffers;
using System.Text;
using Ameto.Core;
using Ameto.Ingestion;
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
/// that is what storage held. <see cref="OtlpLogProtoParser"/> copies keys and string values
/// from the wire, and must store the same replaced text rather than the raw invalid bytes: the
/// filters and the trigram index compare bytes, so verbatim copies match differently from main.
///
/// <para>Every comparison here is on msgpack BYTES. Decoding the blob back to strings would
/// replace the invalid sequences on the way out and hide exactly the difference under test.</para>
/// </summary>
public sealed class OtlpLogProtoInvalidUtf8Tests
{
    // Latin-1 é, a stray continuation byte, a truncated 2-byte lead, a lead byte no sequence starts
    // with (0xF5, 0xFE, 0xFF), a truncated 3-byte sequence and an encoded surrogate.
    private static readonly byte[] BadKey      = [(byte)'a', 0xFF];
    private static readonly byte[] BadValue    = [(byte)'c', (byte)'a', (byte)'f', 0xE9];
    private static readonly byte[] BadValue2   = [(byte)'v', 0x80, (byte)'w', 0xC3];
    private static readonly byte[] BadService  = [(byte)'s', (byte)'v', (byte)'c', 0xFE];
    private static readonly byte[] BadBody     = [(byte)'m', 0xE0, 0x80, (byte)' ', (byte)'{', (byte)'X', (byte)'}'];
    private static readonly byte[] BadInnerKey = [(byte)'i', (byte)'n', 0xF5];
    private static readonly byte[] BadInnerVal = [(byte)'y', 0xED, 0xA0, 0x80];     // encoded surrogate

    [Fact]
    public void InvalidUtf8_InKeysValuesBodyAndService_IsStoredAsTheDomPathStoredIt()
    {
        byte[] payload = Msg(c =>
            Nested(c, 1, Msg(rl =>
            {
                Nested(rl, 1, Msg(res =>
                {
                    Nested(res, 1, RawStringAttr("service.name"u8.ToArray(), BadService));
                    Nested(res, 1, RawStringAttr(BadKey, "resource"u8.ToArray()));
                }));
                Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
                {
                    lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_060_000_000_000UL);
                    lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                    Nested(lr, 5, Msg(b => RawString(b, 1, BadBody)));
                    Nested(lr, 6, RawStringAttr(BadKey, "x"u8.ToArray()));
                    Nested(lr, 6, RawStringAttr("latin1"u8.ToArray(), BadValue));
                    Nested(lr, 6, RawStringAttr("mixed"u8.ToArray(), BadValue2));
                }))));
            })));

        var dom = OtlpLogMapper.Map(OtlpProtoDecoder.DecodeLogs(payload, payload.Length), NodeId.Local.Value);
        var sink = new OtlpLogProtoParityTests.CapturingSink();
        OtlpLogProtoParser.Parse(payload, sink);

        var d   = Assert.Single(dom);
        var rec = Assert.Single(sink.Records);
        Assert.Equal(d.MessageTemplate, rec.Tmpl);          // through the intern pool: replaced
        Assert.Equal(d.ServiceName,     rec.Svc);
        Assert.True(d.RawProperties.Span.SequenceEqual(rec.Props),
            $"property bytes differ\n  dom: {Convert.ToHexString(d.RawProperties.Span)}\n  new: {Convert.ToHexString(rec.Props)}");
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
            Nested(c, 1, Msg(rl =>
                Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
                {
                    lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_060_000_000_000UL);
                    Nested(lr, 6, Msg(kv =>
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

        var sink = new OtlpLogProtoParityTests.CapturingSink();
        OtlpLogProtoParser.Parse(payload, sink);
        var rec = Assert.Single(sink.Records);

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

        Assert.True(buf.WrittenSpan.SequenceEqual(rec.Props),
            $"property bytes differ\n  expected: {Convert.ToHexString(buf.WrittenSpan)}\n  actual  : {Convert.ToHexString(rec.Props)}");
    }

    /// <summary>
    /// The check that keeps invalid input correct must not cost valid input anything: after
    /// the thread scratch has grown, a realistic batch parses with no allocation at all.
    /// </summary>
    [Fact]
    public void ValidInput_ParsesWithNoPerRecordAllocation()
    {
        byte[] payload = Logs_Realistic();
        var sink = new NullSink();
        for (int i = 0; i < 20; i++) OtlpLogProtoParser.Parse(payload, sink);

        const int iters = 50;
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iters; i++) OtlpLogProtoParser.Parse(payload, sink);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - b0;

        double perRecord = bytes / (double)iters / LogRecords;
        Assert.True(perRecord < 1.0, $"expected 0 B/record on valid input, got {perRecord:F2} B/record ({bytes} B total)");
    }

    private sealed class NullSink : IOtlpLogSink
    {
        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8) => true;
        public void NotifyBatchEnqueued() { }
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
