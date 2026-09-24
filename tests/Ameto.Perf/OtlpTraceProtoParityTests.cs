using Ameto.Core.Serialization;
using Ameto.Otel;
using Ameto.Tracing;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// Pins <see cref="OtlpTraceProtoParser"/> to the decode-to-DOM-then-map path it replaces on
/// the OTLP/protobuf traces route: same payload in, the same <see cref="SpanIngestItem"/>s out
/// — every field, the drop rules, and the attribute blob BYTE FOR BYTE.
///
/// <para>Hand-rolled wire parsing earns this. A mis-read field does not throw; it lands the
/// wrong bytes in storage and nothing notices. The attribute map is compared as bytes, not as a
/// decoded dictionary, because key order and msgpack encoding are both part of what goes into
/// a <c>.trc</c> segment.</para>
///
/// <para>Two divergences are asserted deliberately rather than papered over, and both move the
/// protobuf path onto the JSON path's behaviour: <c>array_value</c> / <c>kvlist_value</c>
/// attributes are encoded instead of being written as nil (the DOM decoder never modelled those
/// AnyValue cases, so protobuf clients silently lost them), and a self-ingest URL over 512
/// bytes stops being dropped. Both have a test of their own below.</para>
/// </summary>
public sealed class OtlpTraceProtoParityTests
{
    [Fact]
    public void MatchesDomPath_OnRealisticBatch()
    {
        // nestedAttr off: the array_value attribute is the one value the two paths disagree on
        // by design, and it has its own test below.
        byte[] payload = OtlpProtoPayloads.Traces_Realistic(200, nestedAttr: false);
        var spans = AssertSame(payload);

        Assert.Equal(200, spans.Count);

        // Guard the mapping itself, not only that the two paths agree.
        var first = spans[0];
        Assert.Equal("Etisalat.API", first.ServiceName);
        Assert.Equal("GET /api/v1/resource/0", first.Name);
        Assert.Equal(SpanKind.Server, first.Kind);
        Assert.Equal(SpanStatusCode.Error, first.Status);          // span 0: status code 2
        Assert.Equal(SpanStatusCode.Ok, spans[1].Status);
        Assert.Equal((short)500, first.HttpStatusCode);
        Assert.Equal((short)200, spans[1].HttpStatusCode);
        Assert.Equal(12_000_000L, first.DurationNanos);
        Assert.Equal(0x0af7651916cd43ddUL, TraceHi(first.TraceId));
        Assert.Equal(new SpanId(0xb7ad6b7169203300UL), first.SpanId);
        Assert.Equal(new SpanId(0x00f067aa0ba902b7UL), first.ParentSpanId);

        var attrs = Attrs(first.AttributesBytes);
        // Resource attributes lead the map — minus service.name, which is a column here.
        Assert.False(attrs.ContainsKey("service.name"));
        Assert.Equal("Test", attrs["deployment.environment"]);
        Assert.Equal("sandbox-kz02", attrs["host.name"]);
        Assert.Equal("GET", attrs["http.request.method"]);
        Assert.Equal(500L, attrs["http.response.status_code"]);    // promoted AND kept in the map
        // events[] and links[] are never materialised, so nothing of theirs reaches the map.
        Assert.False(attrs.ContainsKey("exception.type"));
        Assert.False(attrs.ContainsKey("messaging.operation"));
    }

    /// <summary>
    /// THE SERVICE IS INTERNED ONCE PER RESOURCE BLOCK (TI#5): two hundred spans under one resource,
    /// one call — the log parser's <c>InternService</c> / <c>ServiceIdx</c> shape — and every span
    /// carries the index of exactly its own service bytes (the capturing sink checks that per span).
    /// </summary>
    [Fact]
    public void RawSink_InternsTheServiceOncePerResourceBlock()
    {
        var sink = new CapturingSpanSink();
        var (ingested, _) = OtlpTraceProtoParser.Parse(OtlpProtoPayloads.Traces_Realistic(200, nestedAttr: false), sink);

        Assert.Equal(200, ingested);
        Assert.Equal(1, sink.InternCalls);
        Assert.All(sink.Spans, static s => Assert.Equal(0, s.ServiceIdx));
    }

    /// <summary>
    /// A RESOURCE BLOCK WITH NO SPANS TAKES NO SERVICE-POOL SLOT (PR #84 review, #6). The service pool
    /// holds 4 096 entries for the life of the process; an exporter that sends a block per pod, most
    /// of them empty, filled it over protobuf — interned per block, before any span was seen — and
    /// never over JSON, which interns at the block's first span. Five blocks with a service and no
    /// spans (one with an empty ScopeSpans), then one block with a span: ONE intern call, for the
    /// block that had a span, on both encodings.
    ///
    /// <para>Reverted (interned per block): 7 calls over protobuf, one per block.</para>
    /// </summary>
    [Fact]
    public void RawSink_AResourceBlockWithNoSpansInternsNothing()
    {
        static byte[] Resource(string service) => OtlpProtoPayloads.Msg(res =>
            OtlpProtoPayloads.Nested(res, 1, RawStringAttr("service.name"u8.ToArray(), System.Text.Encoding.UTF8.GetBytes(service))));

        byte[] payload = OtlpProtoPayloads.Msg(c =>
        {
            for (int i = 0; i < 5; i++)                                          // no ScopeSpans at all
                OtlpProtoPayloads.Nested(c, 1, OtlpProtoPayloads.Msg(rs => OtlpProtoPayloads.Nested(rs, 1, Resource($"pod-{i}"))));
            OtlpProtoPayloads.Nested(c, 1, OtlpProtoPayloads.Msg(rs =>          // a ScopeSpans with no span
            {
                OtlpProtoPayloads.Nested(rs, 1, Resource("pod-empty-scope"));
                OtlpProtoPayloads.Nested(rs, 2, OtlpProtoPayloads.Msg(static _ => { }));
            }));
            OtlpProtoPayloads.Nested(c, 1, OtlpProtoPayloads.Msg(rs =>
            {
                OtlpProtoPayloads.Nested(rs, 1, Resource("checkout"));
                OtlpProtoPayloads.Nested(rs, 2, OtlpProtoPayloads.Msg(ss => OtlpProtoPayloads.Nested(ss, 2, OtlpProtoPayloads.Msg(sp =>
                {
                    sp.WriteTag(1, Google.Protobuf.WireFormat.WireType.LengthDelimited);
                    sp.WriteBytes(Google.Protobuf.ByteString.CopyFrom(Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
                    sp.WriteTag(2, Google.Protobuf.WireFormat.WireType.LengthDelimited);
                    sp.WriteBytes(Google.Protobuf.ByteString.CopyFrom(Convert.FromHexString("b7ad6b7169203331")));
                    RawString(sp, 5, "GET /cart"u8.ToArray());
                }))));
            }));
        });

        var proto = new CapturingSpanSink();
        var (ingested, _) = OtlpTraceProtoParser.Parse(payload, proto);
        Assert.Equal(1, ingested);
        Assert.Equal(1, proto.InternCalls);
        Assert.Equal("checkout"u8.ToArray(), Assert.Single(proto.Interned));

        // The JSON parser, on the same blocks: the behaviour the protobuf route now matches.
        string empty = string.Concat(Enumerable.Range(0, 5).Select(i =>
            $"{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"pod-{i}\"}}}}]}}}},"));
        string json = "{\"resourceSpans\":[" + empty
            + "{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"pod-empty-scope\"}}]},\"scopeSpans\":[{\"spans\":[]}]},"
            + "{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"checkout\"}}]},"
            + "\"scopeSpans\":[{\"spans\":[{\"traceId\":\"0af7651916cd43dd8448eb211c80319c\",\"spanId\":\"b7ad6b7169203331\",\"name\":\"GET /cart\"}]}]}]}";
        var viaJson = new CapturingSpanSink();
        var (jsonIngested, _) = OtlpTraceStreamParser.Parse(System.Text.Encoding.UTF8.GetBytes(json), viaJson);
        Assert.Equal(1, jsonIngested);
        Assert.Equal(1, viaJson.InternCalls);
        Assert.Equal("checkout"u8.ToArray(), Assert.Single(viaJson.Interned));
    }

    /// <summary>
    /// THE SINK IS ONLY EVER HANDED VALID UTF-8. A name and a service carrying invalid sequences
    /// reach the sink as the bytes of the text the DOM path stored (U+FFFD per invalid sequence) —
    /// the capturing sink asserts validity on every call, and the bytes are compared with the DOM's.
    /// </summary>
    [Fact]
    public void RawSink_GetsValidUtf8_ForAnInvalidNameAndService()
    {
        byte[] badService = [(byte)'s', (byte)'v', (byte)'c', 0xFE];
        byte[] badName    = [(byte)'G', 0xE0, 0x80, (byte)'T'];
        byte[] payload = OtlpProtoPayloads.Msg(c =>
            OtlpProtoPayloads.Nested(c, 1, OtlpProtoPayloads.Msg(rs =>
            {
                OtlpProtoPayloads.Nested(rs, 1, OtlpProtoPayloads.Msg(res =>
                    OtlpProtoPayloads.Nested(res, 1, RawStringAttr("service.name"u8.ToArray(), badService))));
                OtlpProtoPayloads.Nested(rs, 2, OtlpProtoPayloads.Msg(ss => OtlpProtoPayloads.Nested(ss, 2, OtlpProtoPayloads.Msg(sp =>
                {
                    sp.WriteTag(1, Google.Protobuf.WireFormat.WireType.LengthDelimited);
                    sp.WriteBytes(Google.Protobuf.ByteString.CopyFrom(Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
                    sp.WriteTag(2, Google.Protobuf.WireFormat.WireType.LengthDelimited);
                    sp.WriteBytes(Google.Protobuf.ByteString.CopyFrom(Convert.FromHexString("b7ad6b7169203331")));
                    RawString(sp, 5, badName);
                }))));
            })));

        var sink = new CapturingSpanSink();
        OtlpTraceProtoParser.Parse(payload, sink);
        var d = Assert.Single(ViaDom(payload));
        sink.AssertMatches([d]);
        Assert.Contains('�', d.Name);
        Assert.Contains('�', d.ServiceName);
    }

    [Fact]
    public void MatchesDomPath_OnEmptyBatch()
    {
        byte[] empty = OtlpProtoPayloads.EmptyTraces();
        Assert.Empty(AssertSame(empty));
    }

    [Fact]
    public void MatchesDomPath_OnScalarAttributeTypes()
    {
        var spans = AssertSame(OtlpProtoPayloads.Traces_ScalarAttributeTypes());
        var span  = Assert.Single(spans);

        Assert.Equal("unknown", span.ServiceName);                 // no service.name on this resource
        var attrs = Attrs(span.AttributesBytes);
        Assert.Equal(42L,   attrs["int.attr"]);
        Assert.Equal(-7L,   attrs["negative.attr"]);
        Assert.Equal(true,  attrs["bool.attr"]);
        Assert.Equal(false, attrs["false.attr"]);
        Assert.Equal(1.5,   attrs["double.attr"]);
        Assert.Equal("",    attrs["empty.attr"]);
        Assert.Null(attrs["no.value"]);                            // key present, value message absent
        Assert.Null(attrs["empty.value"]);                         // AnyValue with no case set
        Assert.Null(attrs["blob"]);                                // bytes_value: nil on both paths
        Assert.False(attrs.ContainsKey("orphan"));                 // value with no key is dropped entirely
        Assert.Equal(8L, attrs["host.cpu.count"]);                 // resource attributes lead the map
        Assert.Equal(SpanStatusCode.Unset, span.Status);
        Assert.True(span.ParentSpanId.IsEmpty);
    }

    [Fact]
    public void MatchesDomPath_OnDropRules()
    {
        var spans = AssertSame(OtlpProtoPayloads.Traces_DropRules());

        // short trace id, missing span id, short span id and the self-ingest CLIENT span are gone.
        Assert.Equal(4, spans.Count);
        Assert.Equal("self-ingest-server", spans[0].Name);          // same URL, but kind SERVER
        Assert.Equal("kind-eleven",        spans[1].Name);          // masks to CLIENT, but kind != 3
        Assert.Equal(SpanKind.Client,      spans[1].Kind);
        Assert.Equal("short-parent",       spans[2].Name);
        Assert.True(spans[2].ParentSpanId.IsEmpty);                 // 4 bytes is not a parent id
        Assert.Equal("ordinary",           spans[3].Name);
        Assert.Equal(new SpanId(0x00f067aa0ba902b7UL), spans[3].ParentSpanId);
    }

    [Fact]
    public void MatchesDomPath_OnOutOfOrderAndAmbiguousWireShapes()
    {
        var spans = AssertSame(OtlpProtoPayloads.Traces_OutOfOrderAndAmbiguous());
        Assert.Equal(2, spans.Count);

        var amb = spans[0];
        // The resource is read in a pass of its own, so scope_spans written first still sees it.
        // Three service.name entries: an int, then two strings. The mapper skips the non-string
        // one and keeps the FIRST string — so this pins both halves of the rule, and a parser
        // that took the last string would answer "Loses.Third" here.
        Assert.Equal("Wins.Second", amb.ServiceName);
        Assert.Equal("Wins.Second", ViaDom(OtlpProtoPayloads.Traces_OutOfOrderAndAmbiguous())[0].ServiceName);
        Assert.Equal(SpanKind.Client, amb.Kind);                    // raw 11 masks to 3…
        Assert.Equal(0L, amb.StartTimeUnixNano);                    // fixed64 past long.MaxValue
        Assert.Equal(1_785_300_060_000_000_000L, amb.DurationNanos);
        Assert.Equal((short)200, amb.HttpStatusCode);               // the new semconv key wins
        Assert.Equal(SpanStatusCode.Unset, amb.Status);             // an unknown status code

        var attrs = Attrs(amb.AttributesBytes);
        Assert.Equal("backwards",     attrs["reversed"]);           // value before key
        Assert.Equal("string wins",   attrs["oneof"]);              // string beats int/double/bool
        Assert.Equal("last key wins", attrs["repeated"]);
        Assert.False(attrs.ContainsKey("ignored"));
        Assert.Equal("Test", attrs["deployment.environment"]);

        // Second block: no resource at all, so nothing carries over from the first.
        Assert.Equal("unknown", spans[1].ServiceName);
        Assert.Empty(spans[1].AttributesBytes);
    }

    /// <summary>
    /// The one attribute shape where the paths differ: the DOM decoder had no case for AnyValue
    /// fields 5 and 6, so an array or a kvlist reached storage as nil. Encoding them is a
    /// deliberate divergence in the client's favour, and the same one the logs round took.
    /// </summary>
    [Fact]
    public void EncodesArrayValues_TheDomPathDroppedToNil()
    {
        byte[] payload = OtlpProtoPayloads.Traces_Realistic(1, nestedAttr: true);

        var parsed = OtlpTraceProtoParser.Parse(payload);
        var dom    = ViaDom(payload);

        var accept = Assert.IsType<object[]>(Attrs(Assert.Single(parsed).AttributesBytes)["http.request.header.accept"]);
        Assert.Equal(new object?[] { "application/json", "text/html" }, accept);

        Assert.Null(Attrs(Assert.Single(dom).AttributesBytes)["http.request.header.accept"]);
    }

    /// <summary>
    /// The self-ingest guard now runs on UTF-8, which refuses a URL over 512 bytes rather than
    /// comparing it — the JSON parser's behaviour, and the reason a span the DOM path dropped is
    /// kept here. Recorded, not fixed: closing the hole means raising the cap in both streaming
    /// parsers at once, which is its own change.
    /// </summary>
    [Fact]
    public void AnOversizedSelfIngestUrl_IsKeptHere_AndWasDroppedByTheDomPath()
    {
        byte[] payload = OtlpProtoPayloads.Traces_OversizedSelfIngestUrl();

        Assert.Empty(ViaDom(payload));
        Assert.Equal("oversized-self-ingest", Assert.Single(OtlpTraceProtoParser.Parse(payload)).Name);
    }

    /// <summary>
    /// A truncated upload is refused by the wire reader, which is what the receivers turn into
    /// 400 / INVALID_ARGUMENT. The DOM decoder refuses it too, with its own exception type.
    /// </summary>
    [Fact]
    public void ATruncatedPayload_IsRefused()
    {
        byte[] payload = OtlpProtoPayloads.Traces_TruncatedLengthPrefix();
        Assert.ThrowsAny<Exception>(() => OtlpTraceProtoParser.Parse(payload));
        Assert.ThrowsAny<Exception>(() => ViaDom(payload));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static List<SpanIngestItem> ViaDom(byte[] payload)
        => OtlpTraceMapper.Map(OtlpProtoDecoder.DecodeTraces(payload, payload.Length));

    private static Dictionary<string, object?> Attrs(byte[] msgpack)
        => msgpack.Length == 0 ? [] : LogEventSerializer.DeserializePropertiesMap(msgpack) ?? [];

    private static ulong TraceHi(TraceId id) => id.High;

    /// <summary>
    /// RETARGETED AT THE RAW SINK (TI#3): every payload is parsed TWICE — into the
    /// <see cref="CapturingSpanSink"/>, which records the bytes the ring would be handed and compares
    /// them with the DOM path byte for byte, and through the list-returning overload the gRPC
    /// receiver uses — and both must match the DOM.
    /// </summary>
    private static List<SpanIngestItem> AssertSame(byte[] payload)
    {
        var dom  = ViaDom(payload);
        var sink = new CapturingSpanSink();
        var (ingested, refused) = OtlpTraceProtoParser.Parse(payload, sink);
        var raw = sink.AssertMatches(dom);
        Assert.Equal((dom.Count, 0), (ingested, refused));
        Assert.Equal(1, sink.EndBatches);

        var parsed = OtlpTraceProtoParser.Parse(payload);

        Assert.Equal(dom.Count, parsed.Count);
        for (int i = 0; i < dom.Count; i++)
        {
            var d = dom[i];
            var s = parsed[i];

            Assert.Equal(d.TraceId,           s.TraceId);
            Assert.Equal(d.SpanId,            s.SpanId);
            Assert.Equal(d.ParentSpanId,      s.ParentSpanId);
            Assert.Equal(d.StartTimeUnixNano, s.StartTimeUnixNano);
            Assert.Equal(d.DurationNanos,     s.DurationNanos);
            Assert.Equal(d.Name,              s.Name);
            Assert.Equal(d.ServiceName,       s.ServiceName);
            Assert.Equal(d.Kind,              s.Kind);
            Assert.Equal(d.Status,            s.Status);
            Assert.Equal(d.HttpStatusCode,    s.HttpStatusCode);
            Assert.True(d.AttributesBytes.AsSpan().SequenceEqual(s.AttributesBytes),
                $"span {i} ({d.Name}): attribute bytes differ\n"
              + $"  dom: {Convert.ToHexString(d.AttributesBytes)}\n"
              + $"  new: {Convert.ToHexString(s.AttributesBytes)}");
        }
        return parsed;
    }

    /// <summary>A length-delimited field holding arbitrary bytes: WriteString cannot produce invalid UTF-8.</summary>
    private static void RawString(Google.Protobuf.CodedOutputStream c, int field, byte[] bytes)
    {
        c.WriteTag(field, Google.Protobuf.WireFormat.WireType.LengthDelimited);
        c.WriteBytes(Google.Protobuf.ByteString.CopyFrom(bytes));
    }

    private static byte[] RawStringAttr(byte[] key, byte[] value) => OtlpProtoPayloads.Msg(c =>
    {
        RawString(c, 1, key);
        OtlpProtoPayloads.Nested(c, 2, OtlpProtoPayloads.Msg(v => RawString(v, 1, value)));
    });
}
