using System.Text;
using System.Text.Json;
using Google.Protobuf;
using MessagePack;
using Ameto.Otel;
using Ameto.Otel.Models;
using Ameto.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// THE TWO STREAMING TRACE PARSERS, OVER THE SAME LOGICAL PAYLOAD, MUST AGREE.
///
/// <para>Each is pinned to the reflection DOM path by a parity suite of its own —
/// <c>OtlpTraceProtoParityTests</c> (protobuf vs <c>OtlpProtoDecoder</c>) and
/// <c>OtlpTraceStreamingParityTests</c> (JSON vs <c>JsonSerializer</c>) — and NEITHER of those
/// compares the two parsers. That left the promotion rules, which both parsers implement by hand
/// and neither DOM oracle exercises in the shapes below, free to drift apart on the road nobody
/// tests: protobuf is what every SDK exporter and the collector send, JSON is what a curl and
/// the docs use.</para>
///
/// <para><b>It had drifted, twice</b>, and this file is written around the two cases:</para>
/// <list type="number">
/// <item><b>The status latch.</b> <c>OtlpTraceMapper.ExtractHttpStatusCode</c> <c>break</c>s at
/// the first <c>http.response.status_code</c> that parses, so the FIRST one wins. The protobuf
/// parser latched; the JSON parser re-tested a <c>httpFromNew</c> flag but never blocked on it,
/// so a span carrying the key twice came back 500 over protobuf and 503 over JSON.</item>
/// <item><b>The status parse.</b> <c>short.TryParse</c> takes the whole string or nothing. The
/// protobuf parser checked <c>consumed == length</c>; the JSON parser discarded the count, so
/// <c>"200abc"</c> promoted as 200 there and as nothing everywhere else.</item>
/// </list>
///
/// <para>The comparison is field by field and the attribute map is compared as BYTES, because key
/// order and msgpack encoding are both part of what lands in a <c>.trc</c> segment. Both payloads
/// are written out in full in this file, side by side, so that changing one and not the other is
/// visible in the diff rather than only in a failure.</para>
/// </summary>
public sealed class OtlpTraceRouteParityTests
{
    private readonly ITestOutputHelper _out;
    public OtlpTraceRouteParityTests(ITestOutputHelper o) => _out = o;

    [Fact]
    public void The_two_streaming_parsers_agree_on_one_payload()
    {
        var fromProto = OtlpTraceProtoParser.Parse(ProtoPayload());
        var fromJson  = OtlpTraceStreamParser.Parse(Encoding.UTF8.GetBytes(JsonPayload));

        _out.WriteLine($"protobuf {fromProto.Count} span(s), JSON {fromJson.Count} span(s)");
        for (int i = 0; i < Math.Min(fromProto.Count, fromJson.Count); i++)
            _out.WriteLine($"  [{i}] {fromProto[i].Name,-22} http={fromProto[i].HttpStatusCode,-4} "
                         + $"svc={fromProto[i].ServiceName,-12} attrs={fromProto[i].AttributesBytes.Length} B");

        Assert.Equal(fromProto.Count, fromJson.Count);

        for (int i = 0; i < fromProto.Count; i++)
        {
            var p = fromProto[i];
            var j = fromJson[i];

            Assert.Equal(p.TraceId,           j.TraceId);
            Assert.Equal(p.SpanId,            j.SpanId);
            Assert.Equal(p.ParentSpanId,      j.ParentSpanId);
            Assert.Equal(p.StartTimeUnixNano, j.StartTimeUnixNano);
            Assert.Equal(p.DurationNanos,     j.DurationNanos);
            Assert.Equal(p.Name,              j.Name);
            Assert.Equal(p.ServiceName,       j.ServiceName);
            Assert.Equal(p.Kind,              j.Kind);
            Assert.Equal(p.Status,            j.Status);
            Assert.Equal(p.HttpStatusCode,    j.HttpStatusCode);
            Assert.True(p.AttributesBytes.AsSpan().SequenceEqual(j.AttributesBytes),
                $"attribute msgpack differs on span #{i} ({p.Name}):\n"
              + $"  protobuf {Convert.ToHexString(p.AttributesBytes)}\n"
              + $"  json     {Convert.ToHexString(j.AttributesBytes)}");
        }

        // AND THE ANSWERS THEMSELVES, so a parser that has drifted into agreement with the other
        // rather than with the DOM mapper is still caught. Both payloads carry four spans; the
        // CLIENT span pointing at this server's own receiver is dropped by both, leaving three.
        Assert.Equal(3, fromProto.Count);

        // The latch: the FIRST http.response.status_code wins, and the second is only an
        // attribute. This is OtlpTraceMapper's `break`.
        Assert.Equal("latched",     fromProto[0].Name);
        Assert.Equal((short)500,    fromProto[0].HttpStatusCode);

        // The whole string or nothing: "200abc" is not a status on any route.
        Assert.Equal("partial",     fromProto[1].Name);
        Assert.Equal((short)0,      fromProto[1].HttpStatusCode);

        // A duplicate resource service.name: the FIRST string-valued one is the service, and
        // NEITHER copy reaches the attribute map.
        Assert.Equal("last",        fromProto[2].Name);
        Assert.Equal("Wallet.API",  fromProto[2].ServiceName);
        Assert.Equal((short)404,    fromProto[2].HttpStatusCode);   // a new-key status as a string

        var attrs = Attrs(fromProto[2].AttributesBytes);
        Assert.False(attrs.ContainsKey("service.name"));
        Assert.Equal("srv-01", attrs["host.name"]);
        Assert.Equal(true,     attrs["retry"]);
        Assert.Equal(12.75,    attrs["duration_ms"]);
        Assert.Equal("404",    attrs["http.response.status_code"]);  // promoted AND kept
    }

    /// <summary>
    /// The same three promotion facts stated against the DOM mapper as well, so "the two agree"
    /// cannot become "the two are wrong together". <c>ExtractHttpStatusCode</c> is the definition
    /// both parsers reproduce and it is nine lines long; asserting it here is what stops a future
    /// edit from moving all three at once.
    /// </summary>
    [Theory]
    [InlineData(500, 503, 500)]   // first new-key value wins — the mapper breaks
    [InlineData(200, 404, 200)]
    public void The_first_parsed_new_key_status_wins_on_both_routes(int first, int second, int expected)
    {
        byte[] proto = Request(Resource(StringAttr("service.name", "svc")), Span(
            name: "dup", kind: 2, traceHex: "aa000000000000000000000000000001", spanHex: "bb00000000000001",
            attrs: [IntAttr("http.response.status_code", first), IntAttr("http.response.status_code", second)]));

        string json = $$"""
        { "resourceSpans": [ { "resource": { "attributes": [
            { "key": "service.name", "value": { "stringValue": "svc" } } ] },
          "scopeSpans": [ { "spans": [ {
            "traceId": "aa000000000000000000000000000001", "spanId": "bb00000000000001",
            "name": "dup", "kind": 2,
            "startTimeUnixNano": "1785300000000000000", "endTimeUnixNano": "1785300000001000000",
            "attributes": [
              { "key": "http.response.status_code", "value": { "intValue": "{{first}}" } },
              { "key": "http.response.status_code", "value": { "intValue": "{{second}}" } } ]
          } ] } ] } ] }
        """;

        Assert.Equal((short)expected, Assert.Single(OtlpTraceProtoParser.Parse(proto)).HttpStatusCode);
        Assert.Equal((short)expected, Assert.Single(OtlpTraceStreamParser.Parse(Encoding.UTF8.GetBytes(json))).HttpStatusCode);
    }

    /// <summary>
    /// AN <c>intValue</c> THAT IS NOT AN INT, which is a JSON-only shape: protobuf's
    /// <c>int_value</c> is a varint and cannot carry one, so the oracle here is the DOM mapper
    /// alone — <c>JsonSerializer</c> + <c>OtlpTraceMapper.Map</c>, the path OTLP/JSON is held to.
    ///
    /// <para>proto3 JSON writes every 64-bit field as a QUOTED decimal, so the value under
    /// <c>intValue</c> is a string on the ordinary road and whatever the exporter put there.
    /// <c>Utf8Parser</c> answers such a string twice — a success flag and a consumed count
    /// — and the JSON parser used to discard both and capture the <c>out</c> value anyway. Under
    /// <c>http.response.status_code</c> that promoted 0 for <c>"abc"</c> and 200 for
    /// <c>"200abc"</c>, and because the key is the NEW one it also LATCHED: the valid status
    /// beside it could no longer be promoted, while the DOM's <c>short.TryParse</c> — which takes
    /// the whole string or nothing — returned it.</para>
    ///
    /// <para>The attribute blob is compared as well, because the same answer decides what the map
    /// carries: the mapper falls through to nil when <c>long.TryParse</c> says no, and a parser
    /// that wrote the 0 or the 200 it had just refused would put a number in a <c>.trc</c> segment
    /// that the span never sent.</para>
    /// </summary>
    [Theory]
    [InlineData("abc")]                    // TryParse says false and hands back 0
    [InlineData("200abc")]                 // it says true and stops after three bytes
    [InlineData("")]                       // nothing at all
    [InlineData("99999999999999999999")]   // past int64
    [InlineData("\\u0032\\u0030\\u0030")]  // escaped "200": a whole int64 the DOM does read
    public void A_non_numeric_int_value_neither_promotes_nor_blocks_the_next_status(string text)
    {
        // The garbage FIRST, so a parser that latched on it cannot reach the 503 behind it.
        byte[] utf8 = Encoding.UTF8.GetBytes($$"""
        { "resourceSpans": [ { "resource": { "attributes": [
            { "key": "service.name", "value": { "stringValue": "svc" } } ] },
          "scopeSpans": [ { "spans": [ {
            "traceId": "aa000000000000000000000000000002", "spanId": "bb00000000000002",
            "name": "not-an-int", "kind": 2,
            "startTimeUnixNano": "1785300000000000000", "endTimeUnixNano": "1785300000001000000",
            "attributes": [
              { "key": "http.response.status_code", "value": { "intValue": "{{text}}" } },
              { "key": "http.response.status_code", "value": { "intValue": "503" } } ]
          } ] } ] } ] }
        """);

        var streamed = Assert.Single(OtlpTraceStreamParser.Parse(utf8));
        var dom      = Assert.Single(OtlpTraceMapper.Map(
            JsonSerializer.Deserialize<ExportTraceServiceRequest>(utf8, DomOptions)!));

        // The literal first — "the two agree" is not enough when one of them is the subject.
        // An escaped "200" IS an int64 once unescaped, and the latch then belongs to it.
        short expected = text.StartsWith("\\u", StringComparison.Ordinal) ? (short)200 : (short)503;
        Assert.Equal(expected, dom.HttpStatusCode);
        Assert.Equal(expected, streamed.HttpStatusCode);

        Assert.True(dom.AttributesBytes.AsSpan().SequenceEqual(streamed.AttributesBytes),
            $"attribute msgpack differs for intValue \"{text}\":\n"
          + $"  dom  {Convert.ToHexString(dom.AttributesBytes)}\n"
          + $"  json {Convert.ToHexString(streamed.AttributesBytes)}");
    }

    /// <summary>Camel case and nothing else — <c>OtlpTraceStreamingParityTests</c>' options.</summary>
    private static readonly JsonSerializerOptions DomOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        AllowTrailingCommas  = true,
    };

    // ── The payload, in both encodings ────────────────────────────────────────

    /// <summary>
    /// Four spans under one resource that names itself twice:
    /// <list type="number">
    ///   <item><c>latched</c> — <c>http.response.status_code</c> twice (500, then 503).</item>
    ///   <item><c>partial</c> — <c>http.status_code</c> as the string <c>"200abc"</c>.</item>
    ///   <item><c>self-ingest</c> — a CLIENT span whose <c>url.full</c> is this server's own OTLP
    ///   receiver. Dropped by both parsers, which is what makes the URL check part of this
    ///   parity rather than a separate test.</item>
    ///   <item><c>last</c> — a scalar of every kind, an array attribute, and a new-key status as
    ///   a STRING (the other half of the promotion, which reads text rather than a varint).</item>
    /// </list>
    /// </summary>
    private static byte[] ProtoPayload() => Request(
        Resource(
            StringAttr("service.name", "Wallet.API"),
            StringAttr("service.name", "Ignored.Second"),
            StringAttr("host.name", "srv-01")),
        Span("latched", 2, "11111111111111111111111111111111", "1111111111111111",
             [IntAttr("http.response.status_code", 500), IntAttr("http.response.status_code", 503)]),
        Span("partial", 2, "22222222222222222222222222222222", "2222222222222222",
             [StringAttr("http.status_code", "200abc")]),
        Span("self-ingest", 3, "33333333333333333333333333333333", "3333333333333333",
             [StringAttr("url.full", "http://AMETO-HOST:8555/OTLP/v1/traces")]),
        Span("last", 2, "44444444444444444444444444444444", "4444444444444444",
             [StringAttr("http.response.status_code", "404"),
              BoolAttr("retry", true),
              DoubleAttr("duration_ms", 12.75),
              ArrayAttr("tags", "a", "b")]));

    private const string JsonPayload = """
    {
      "resourceSpans": [
        {
          "resource": {
            "attributes": [
              { "key": "service.name", "value": { "stringValue": "Wallet.API" } },
              { "key": "service.name", "value": { "stringValue": "Ignored.Second" } },
              { "key": "host.name",    "value": { "stringValue": "srv-01" } }
            ]
          },
          "scopeSpans": [
            {
              "spans": [
                {
                  "traceId": "11111111111111111111111111111111",
                  "spanId":  "1111111111111111",
                  "name": "latched", "kind": 2,
                  "startTimeUnixNano": "1785300000000000000",
                  "endTimeUnixNano":   "1785300000001000000",
                  "attributes": [
                    { "key": "http.response.status_code", "value": { "intValue": "500" } },
                    { "key": "http.response.status_code", "value": { "intValue": "503" } }
                  ]
                },
                {
                  "traceId": "22222222222222222222222222222222",
                  "spanId":  "2222222222222222",
                  "name": "partial", "kind": 2,
                  "startTimeUnixNano": "1785300000000000000",
                  "endTimeUnixNano":   "1785300000001000000",
                  "attributes": [
                    { "key": "http.status_code", "value": { "stringValue": "200abc" } }
                  ]
                },
                {
                  "traceId": "33333333333333333333333333333333",
                  "spanId":  "3333333333333333",
                  "name": "self-ingest", "kind": 3,
                  "startTimeUnixNano": "1785300000000000000",
                  "endTimeUnixNano":   "1785300000001000000",
                  "attributes": [
                    { "key": "url.full", "value": { "stringValue": "http://AMETO-HOST:8555/OTLP/v1/traces" } }
                  ]
                },
                {
                  "traceId": "44444444444444444444444444444444",
                  "spanId":  "4444444444444444",
                  "name": "last", "kind": 2,
                  "startTimeUnixNano": "1785300000000000000",
                  "endTimeUnixNano":   "1785300000001000000",
                  "attributes": [
                    { "key": "http.response.status_code", "value": { "stringValue": "404" } },
                    { "key": "retry",       "value": { "boolValue": true } },
                    { "key": "duration_ms", "value": { "doubleValue": 12.75 } },
                    { "key": "tags", "value": { "arrayValue": { "values": [
                        { "stringValue": "a" }, { "stringValue": "b" } ] } } }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
    """;

    // ── protobuf builders ─────────────────────────────────────────────────────

    private static byte[] Request(byte[] resource, params byte[][] spans) => OtlpProtoPayloads.Msg(c =>
        OtlpProtoPayloads.Nested(c, 1, OtlpProtoPayloads.Msg(rs =>
        {
            OtlpProtoPayloads.Nested(rs, 1, resource);
            OtlpProtoPayloads.Nested(rs, 2, OtlpProtoPayloads.Msg(ss =>
            {
                foreach (byte[] s in spans) OtlpProtoPayloads.Nested(ss, 2, s);
            }));
        })));

    private static byte[] Resource(params byte[][] attrs) => OtlpProtoPayloads.Msg(r =>
    {
        foreach (byte[] a in attrs) OtlpProtoPayloads.Nested(r, 1, a);
    });

    private static byte[] Span(string name, int kind, string traceHex, string spanHex, byte[][] attrs) =>
        OtlpProtoPayloads.Msg(c =>
        {
            c.WriteTag(1, WireFormat.WireType.LengthDelimited);
            c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString(traceHex)));
            c.WriteTag(2, WireFormat.WireType.LengthDelimited);
            c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString(spanHex)));
            c.WriteTag(5, WireFormat.WireType.LengthDelimited); c.WriteString(name);
            c.WriteTag(6, WireFormat.WireType.Varint);          c.WriteEnum(kind);
            c.WriteTag(7, WireFormat.WireType.Fixed64);         c.WriteFixed64(1_785_300_000_000_000_000UL);
            c.WriteTag(8, WireFormat.WireType.Fixed64);         c.WriteFixed64(1_785_300_000_001_000_000UL);
            foreach (byte[] a in attrs) OtlpProtoPayloads.Nested(c, 9, a);
        });

    private static byte[] StringAttr(string key, string value) => OtlpProtoPayloads.Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        OtlpProtoPayloads.Nested(c, 2, OtlpProtoPayloads.Msg(
            v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString(value); }));
    });

    private static byte[] IntAttr(string key, long value) => OtlpProtoPayloads.Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        OtlpProtoPayloads.Nested(c, 2, OtlpProtoPayloads.Msg(
            v => { v.WriteTag(3, WireFormat.WireType.Varint); v.WriteInt64(value); }));
    });

    private static byte[] BoolAttr(string key, bool value) => OtlpProtoPayloads.Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        OtlpProtoPayloads.Nested(c, 2, OtlpProtoPayloads.Msg(
            v => { v.WriteTag(2, WireFormat.WireType.Varint); v.WriteBool(value); }));
    });

    private static byte[] DoubleAttr(string key, double value) => OtlpProtoPayloads.Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        OtlpProtoPayloads.Nested(c, 2, OtlpProtoPayloads.Msg(
            v => { v.WriteTag(4, WireFormat.WireType.Fixed64); v.WriteDouble(value); }));
    });

    private static byte[] ArrayAttr(string key, params string[] values) => OtlpProtoPayloads.Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        OtlpProtoPayloads.Nested(c, 2, OtlpProtoPayloads.Msg(
            v => OtlpProtoPayloads.Nested(v, 5, OtlpProtoPayloads.Msg(arr =>
            {
                foreach (string s in values)
                {
                    string t = s;
                    OtlpProtoPayloads.Nested(arr, 1, OtlpProtoPayloads.Msg(
                        e => { e.WriteTag(1, WireFormat.WireType.LengthDelimited); e.WriteString(t); }));
                }
            }))));
    });

    private static Dictionary<string, object?> Attrs(byte[] msgpack)
    {
        var reader = new MessagePackReader(msgpack);
        int count  = reader.ReadMapHeader();
        var d      = new Dictionary<string, object?>(count, StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            string key = reader.ReadString() ?? string.Empty;
            d[key] = reader.NextMessagePackType switch
            {
                MessagePackType.String  => reader.ReadString(),
                MessagePackType.Integer => reader.ReadInt64(),
                MessagePackType.Float   => reader.ReadDouble(),
                MessagePackType.Boolean => reader.ReadBoolean(),
                _                       => Skip(ref reader),
            };
        }
        return d;

        static object? Skip(ref MessagePackReader r) { r.Skip(); return null; }
    }
}
