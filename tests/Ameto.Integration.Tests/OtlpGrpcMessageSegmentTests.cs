using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using Google.Protobuf;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Otel;
using Xunit;

namespace Ameto.Integration.Tests;

/// <summary>
/// The gRPC receiver's one remaining per-signal difference: where each decoder reads the
/// request message from.
///
/// <para>An inflated message is alone in its own buffer. An uncompressed one sits five bytes
/// into the request buffer, behind the frame header — which the span parsers take in their
/// stride, and which the trace DOM decoder, taking (buffer, length) and reading from index 0,
/// cannot. So the trace path memmoves the message down and the others do not, and getting
/// that backwards hands a decoder five bytes of frame header and then truncates the tail.
/// On protobuf that is not a parse error: it is a batch that silently loses its last record,
/// or a span whose last attribute vanishes.</para>
///
/// <para><b>What these tests prove:</b> that a real framed body — identity and gzip, for all
/// three signals — reaches each signal's real decoder as exactly the bytes the client sent,
/// and that the records come out. They run the same <c>TryUnframe</c> →
/// <c>MessageSegment</c> → decoder sequence the route handler runs, over the same buffer
/// shape the body reader produces.</para>
///
/// <para><b>What they do NOT prove:</b> anything about the transport — routing, the API-key
/// check, HTTP/2 negotiation, or the grpc-status trailer. TestServer cannot honestly test
/// those here: it supplies a response-trailers feature Kestrel does not and reports HTTP/2 for
/// what is not an HTTP/2 connection, so a green test there can assert behaviour the real
/// server does not have — which is exactly what once hid a success path that answered with no
/// grpc-status at all. The transport is verified against a real Kestrel instead (docs/API.md
/// carries the curl).</para>
///
/// <para>They also do not prove the WIRING — which route passes
/// <c>decodeReadsFromZero: true</c> — because that is inside <c>MapOtlpGrpcEndpoints</c> and
/// needs a host to reach. Worth knowing that both ways of getting it wrong are loud rather
/// than silent: a trace route without the memmove refuses every batch with "invalid tag
/// (zero)" (the last test here), and a log or metric route WITH it is merely a redundant copy
/// of bytes the span parsers would have read where they lay.</para>
/// </summary>
public sealed class OtlpGrpcMessageSegmentTests
{
    private const int Limit = 8 * 1024 * 1024;

    private sealed class CapturingSink : IOtlpLogSink
    {
        public readonly List<(string Tmpl, string? Svc, ulong TrHi)> Records = [];
        public bool TryIngestRaw(long ts, byte level, ReadOnlySpan<byte> tmpl, ReadOnlySpan<byte> props,
            ulong trHi, ulong trLo, ulong sp, ReadOnlySpan<byte> svc)
        {
            Records.Add((Encoding.UTF8.GetString(tmpl), svc.IsEmpty ? null : Encoding.UTF8.GetString(svc), trHi));
            return true;
        }
        public void NotifyBatchEnqueued() { }
    }

    // ── The sequence the route handler runs ───────────────────────────────────

    /// <summary>
    /// Frames a message the way a client does, into a buffer shaped like the one the body
    /// reader hands over (rented, so longer than the request), then unframes and places it.
    /// </summary>
    private static ArraySegment<byte> Deliver(byte[] message, bool gzip, bool decodeReadsFromZero,
                                              out byte[]? inflated)
    {
        byte[] payload = gzip ? Gzip(message) : message;

        // A pooled buffer is bigger than the body it holds, and holds rubbish past it — if a
        // decoder is handed a length rather than a slice, that rubbish is what it reads.
        byte[] body = new byte[payload.Length + 5 + 97];
        body.AsSpan().Fill(0xCC);
        body[0] = gzip ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(1), (uint)payload.Length);
        payload.CopyTo(body.AsSpan(5));
        int bodyLen = payload.Length + 5;

        var result = OtlpGrpcFraming.TryUnframe(body.AsMemory(0, bodyLen), gzip ? "gzip" : null, Limit,
                                                out var unframed, out inflated, out int inflatedLen);
        Assert.Equal(UnframeResult.Ok, result);

        return OtlpGrpcEndpointMapper.MessageSegment(
            body, unframed.Length, inflated, inflatedLen, decodeReadsFromZero);
    }

    private static byte[] Gzip(byte[] data)
    {
        using var ms = new MemoryStream();
        using (var z = new GZipStream(ms, CompressionMode.Compress, leaveOpen: true)) z.Write(data);
        return ms.ToArray();
    }

    // ── Logs: the span parser, at offset 5 ────────────────────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Logs_arrive_whole_at_the_offset_the_parser_is_given(bool gzip)
    {
        byte[] message = LogsRequest(records: 5);
        var segment = Deliver(message, gzip, decodeReadsFromZero: false, out byte[]? inflated);
        try
        {
            if (!gzip) Assert.Equal(OtlpGrpcFraming.HeaderBytes, segment.Offset);   // NOT memmoved
            Assert.Equal(message.Length, segment.Count);

            var sink = new CapturingSink();
            var (ingested, dropped) = OtlpLogProtoParser.Parse(segment.AsSpan(), sink);

            Assert.Equal(5, ingested);
            Assert.Equal(0, dropped);
            Assert.Equal(5, sink.Records.Count);
            // The LAST record is the one a five-byte offset error truncates away.
            Assert.Equal("record 4", sink.Records[4].Tmpl);
            Assert.All(sink.Records, r => Assert.Equal("Etisalat.API", r.Svc));
            Assert.Equal(0x0af7651916cd43ddUL, sink.Records[0].TrHi);
        }
        finally
        {
            if (inflated is not null) IngestBufferPool.Return(inflated);
        }
    }

    // ── Traces: the DOM decoder, memmoved to index 0 ──────────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Traces_arrive_whole_at_index_zero(bool gzip)
    {
        byte[] message = TracesRequest(spans: 4);
        var segment = Deliver(message, gzip, decodeReadsFromZero: true, out byte[]? inflated);
        try
        {
            Assert.Equal(0, segment.Offset);                                        // memmoved down
            Assert.Equal(message.Length, segment.Count);

            // Exactly the call the route's decode lambda makes.
            var request = OtlpProtoDecoder.DecodeTraces(segment.Array!, segment.Offset + segment.Count);
            var spans   = OtlpTraceMapper.Map(request);

            Assert.Equal(4, spans.Count);
            Assert.Equal("span 3", spans[3].Name);                                  // the truncated one
            Assert.Equal("Etisalat.API", spans[0].ServiceName);
        }
        finally
        {
            if (inflated is not null) IngestBufferPool.Return(inflated);
        }
    }

    // ── Metrics: the span parser, at offset 5 ─────────────────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Metrics_arrive_whole_at_the_offset_the_parser_is_given(bool gzip)
    {
        byte[] message = MetricsRequest();
        var segment = Deliver(message, gzip, decodeReadsFromZero: false, out byte[]? inflated);
        try
        {
            if (!gzip) Assert.Equal(OtlpGrpcFraming.HeaderBytes, segment.Offset);
            var points = OtlpMetricProtoParser.Parse(segment.AsSpan());
            Assert.Equal(2, points.Count);
            Assert.Equal("queue.depth", points[0].Name);
            Assert.Equal(17, points[1].ScalarValue);
        }
        finally
        {
            if (inflated is not null) IngestBufferPool.Return(inflated);
        }
    }

    /// <summary>
    /// The reason the memmove is still there, stated as a test: hand the trace DOM decoder the
    /// segment the SPAN parsers get — offset 5, header still in front — and it reads the frame's
    /// compression flag as a field tag and refuses the whole batch. So the flag on
    /// <c>HandleAsync</c> is load-bearing, and this is what happens if it is set wrong.
    /// </summary>
    [Fact]
    public void The_trace_decoder_cannot_read_an_unmoved_frame()
    {
        byte[] message = TracesRequest(spans: 4);
        var wrong = Deliver(message, gzip: false, decodeReadsFromZero: false, out _);

        // (buffer, length) from index 0 over a body that still carries its 5-byte header: the
        // identity flag is 0x00, and a zero tag is not a field number.
        Assert.Throws<InvalidProtocolBufferException>(
            () => OtlpProtoDecoder.DecodeTraces(wrong.Array!, wrong.Offset + wrong.Count));
    }

    // ── Payloads ──────────────────────────────────────────────────────────────

    private static byte[] Msg(Action<CodedOutputStream> body)
    {
        using var ms = new MemoryStream();
        var o = new CodedOutputStream(ms);
        body(o);
        o.Flush();
        return ms.ToArray();
    }

    private static void Sub(CodedOutputStream o, int field, byte[] payload)
    {
        o.WriteTag(field, WireFormat.WireType.LengthDelimited);
        o.WriteBytes(ByteString.CopyFrom(payload));
    }

    private static byte[] StringAttr(string key, string value) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        Sub(c, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString(value); }));
    });

    private static byte[] Resource() =>
        Msg(res => Sub(res, 1, StringAttr("service.name", "Etisalat.API")));

    private static byte[] LogsRequest(int records) => Msg(c =>
        Sub(c, 1, Msg(rl =>
        {
            Sub(rl, 1, Resource());
            Sub(rl, 2, Msg(sl =>
            {
                for (int i = 0; i < records; i++)
                {
                    int ii = i;
                    Sub(sl, 2, Msg(lr =>
                    {
                        lr.WriteTag(1, WireFormat.WireType.Fixed64);
                        lr.WriteFixed64(1_785_300_060_000_000_000UL + (ulong)ii);
                        lr.WriteTag(2, WireFormat.WireType.Varint); lr.WriteEnum(9);
                        Sub(lr, 5, Msg(b =>
                        {
                            b.WriteTag(1, WireFormat.WireType.LengthDelimited);
                            b.WriteString($"record {ii}");
                        }));
                        Sub(lr, 6, StringAttr("index", ii.ToString()));
                        lr.WriteTag(9, WireFormat.WireType.LengthDelimited);
                        lr.WriteBytes(ByteString.CopyFrom(
                            Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
                    }));
                }
            }));
        })));

    private static byte[] TracesRequest(int spans) => Msg(c =>
        Sub(c, 1, Msg(rs =>
        {
            Sub(rs, 1, Resource());
            Sub(rs, 2, Msg(ss =>
            {
                for (int i = 0; i < spans; i++)
                {
                    int ii = i;
                    Sub(ss, 2, Msg(sp =>
                    {
                        sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
                        sp.WriteBytes(ByteString.CopyFrom(
                            Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
                        sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
                        sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString($"b7ad6b716920330{ii}")));
                        sp.WriteTag(5, WireFormat.WireType.LengthDelimited); sp.WriteString($"span {ii}");
                        sp.WriteTag(6, WireFormat.WireType.Varint);          sp.WriteEnum(2);
                        sp.WriteTag(7, WireFormat.WireType.Fixed64);         sp.WriteFixed64(1_785_300_000_000_000_000UL);
                        sp.WriteTag(8, WireFormat.WireType.Fixed64);         sp.WriteFixed64(1_785_300_000_012_000_000UL);
                    }));
                }
            }));
        })));

    private static byte[] MetricsRequest() => Msg(c =>
        Sub(c, 1, Msg(rm =>
        {
            Sub(rm, 1, Resource());
            Sub(rm, 2, Msg(sm => Sub(sm, 2, Msg(metric =>
            {
                metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString("queue.depth");
                Sub(metric, 7, Msg(s =>                                    // sum
                {
                    for (int i = 0; i < 2; i++)
                    {
                        int ii = i;
                        Sub(s, 1, Msg(dp =>
                        {
                            dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                            dp.WriteTag(6, WireFormat.WireType.Fixed64); dp.WriteSFixed64(16 + ii);
                            Sub(dp, 7, StringAttr("queue", $"q{ii}"));
                        }));
                    }
                }));
            }))));
        })));
}
