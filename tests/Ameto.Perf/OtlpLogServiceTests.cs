using System.Buffers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Otel.Models;
using Google.Protobuf;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// An OTLP log's service: the resource attribute <c>service.name</c> is still what the service
/// is READ from (it is the semantic convention), and it becomes the event's <c>@service</c> header
/// field. Every OTLP event used to carry it twice — the header, and the same attribute left in the
/// property map — and now carries it once: the attribute that became the header leaves the map.
///
/// <para>The rule, on all three roads (the mapper they are pinned to, the protobuf parser, the
/// JSON streaming parser): the FIRST <c>service.name</c> resource attribute decides; a non-empty
/// string becomes the header and is not written to the map; anything else gives no header and
/// stays an ordinary property, as does every later <c>service.name</c>. Each shape below is put
/// through all three, which must agree on the header and on the property bytes.</para>
/// </summary>
public sealed class OtlpLogServiceTests
{
    private const string Svc = "Etisalat.API";

    // ── Shapes ───────────────────────────────────────────────────────────────

    [Fact]
    public void The_service_is_the_header_and_not_a_property()
    {
        var r = AllThree(resource: [("service.name", Svc), ("host.name", "load-gen")],
                         record:   [("orderId", 42L)]);

        Assert.Equal(Svc, r.Service);
        Assert.False(r.Props.ContainsKey("service.name"));
        Assert.False(r.Props.ContainsKey("@service"));       // nothing is written for it at all
        Assert.Equal("load-gen", r.Props["host.name"]);
        Assert.Equal(42L, r.Props["orderId"]);
        Assert.Equal(2, r.Props.Count);
    }

    [Fact]
    public void A_service_that_is_the_only_resource_attribute_leaves_no_resource_properties()
    {
        var r = AllThree(resource: [("service.name", Svc)], record: []);
        Assert.Equal(Svc, r.Service);
        Assert.Empty(r.Props);
    }

    [Fact]
    public void The_first_service_name_decides_and_a_later_one_is_an_ordinary_property()
    {
        var r = AllThree(resource: [("service.name", Svc), ("service.name", "Second.API")], record: []);
        Assert.Equal(Svc, r.Service);
        Assert.Equal("Second.API", Assert.Single(r.Props).Value);
    }

    [Fact]
    public void A_first_service_name_that_is_not_a_string_gives_no_service_and_stays()
    {
        var r = AllThree(resource: [("service.name", 7L), ("service.name", "Too.Late")], record: []);
        Assert.Null(r.Service);
        Assert.Equal(2, r.KeyCount);                         // both still in the map, duplicate key and all
    }

    [Fact]
    public void An_empty_service_name_gives_no_service_and_stays()
    {
        var r = AllThree(resource: [("service.name", "")], record: []);
        Assert.Null(r.Service);
        Assert.Equal("", r.Props["service.name"]);
    }

    [Fact]
    public void A_record_attribute_named_service_name_is_an_ordinary_property()
    {
        // The service is a property of the resource; a record that carries the same key is just
        // a record with an attribute, as it always was.
        var r = AllThree(resource: [("service.name", Svc)], record: [("service.name", "From.Record")]);
        Assert.Equal(Svc, r.Service);
        Assert.Equal("From.Record", r.Props["service.name"]);
    }

    [Fact]
    public void A_resource_without_a_service_has_none()
    {
        var r = AllThree(resource: [("host.name", "load-gen")], record: [("orderId", 1L)]);
        Assert.Null(r.Service);
        Assert.Equal(2, r.Props.Count);
    }

    [Theory]
    [InlineData(255)]
    [InlineData(256)]      // the JSON parser's starting buffer
    [InlineData(257)]      // one past it: the buffer grows rather than losing the service
    [InlineData(5_000)]
    public void A_long_service_name_is_still_the_service(int length)
    {
        string longName = new('s', length);
        var r = AllThree(resource: [("service.name", longName)], record: []);
        Assert.Equal(longName, r.Service);
        Assert.Empty(r.Props);
    }

    [Fact]
    public void An_escaped_key_is_the_same_key_to_the_json_parser()
    {
        // "service.name" is "service.name" to every JSON reader, the DOM one included.
        string json = Json([("service.name", Svc), ("host.name", "h")], [])
            .Replace("\"key\":\"service.name\"", "\"key\":\"service\\u002Ename\"", StringComparison.Ordinal);
        Assert.Contains("service\\u002Ename", json);

        var (svc, props) = ViaJsonStream(json);
        var dom          = ViaDom(JsonSerializer.Deserialize<ExportLogsServiceRequest>(json)!);
        Assert.Equal(Svc, svc);
        Assert.Equal(dom.ServiceName, svc);
        Assert.True(dom.RawProperties.Span.SequenceEqual(props));
        Assert.False(Decode(props).ContainsKey("service.name"));
    }

    // ── The three roads ──────────────────────────────────────────────────────

    private sealed record Outcome(string? Service, Dictionary<string, object?> Props, int KeyCount);

    /// <summary>
    /// One record under one resource, through the mapper (from JSON and from protobuf), the JSON
    /// streaming parser and the protobuf parser; asserts they agree and returns what they said.
    /// </summary>
    private static Outcome AllThree((string Key, object Value)[] resource, (string Key, object Value)[] record)
    {
        string json  = Json(resource, record);
        byte[] proto = Proto(resource, record);

        var domJson  = ViaDom(JsonSerializer.Deserialize<ExportLogsServiceRequest>(json)!);
        var domProto = ViaDom(OtlpProtoDecoder.DecodeLogs(proto, proto.Length));
        var (jsonSvc, jsonProps)   = ViaJsonStream(json);
        var (protoSvc, protoProps) = ViaProto(proto);

        Assert.Equal(domJson.ServiceName, domProto.ServiceName);
        Assert.Equal(domJson.ServiceName, jsonSvc);
        Assert.Equal(domJson.ServiceName, protoSvc);

        // Byte for byte against the mapper — except that both parsers write an empty map where
        // the mapper writes no bytes at all (a documented difference, and the same thing to a reader).
        AssertSameProps(domJson.RawProperties.Span, jsonProps);
        AssertSameProps(domProto.RawProperties.Span, protoProps);
        AssertSameProps(domJson.RawProperties.Span, protoProps);

        return new Outcome(jsonSvc, Decode(jsonProps), KeyCount(jsonProps));
    }

    private static void AssertSameProps(ReadOnlySpan<byte> dom, byte[] parsed)
    {
        if (dom.IsEmpty) Assert.Equal(0, KeyCount(parsed));
        else Assert.True(dom.SequenceEqual(parsed),
            $"dom: {Convert.ToHexString(dom)}\nnew: {Convert.ToHexString(parsed)}");
    }

    private static LogEvent ViaDom(ExportLogsServiceRequest req) =>
        Assert.Single(OtlpLogMapper.Map(req, NodeId.Local.Value));

    private static (string? Service, byte[] Props) ViaJsonStream(string json)
    {
        var sink = new CapturingSink();
        Assert.Equal((1, 0), OtlpLogStreamParser.Parse(Encoding.UTF8.GetBytes(json), sink));
        return Assert.Single(sink.Records);
    }

    private static (string? Service, byte[] Props) ViaProto(byte[] proto)
    {
        var sink = new CapturingSink();
        Assert.Equal((1, 0), OtlpLogProtoParser.Parse(proto, sink));
        return Assert.Single(sink.Records);
    }

    private static Dictionary<string, object?> Decode(byte[] msgpack) =>
        msgpack.Length == 0 ? new() : LogEventSerializer.DeserializePropertiesMap(msgpack) ?? new();

    /// <summary>Keys as written — a dictionary would collapse a repeated one.</summary>
    private static int KeyCount(byte[] msgpack)
    {
        if (msgpack.Length == 0) return 0;
        var reader = new MessagePack.MessagePackReader(msgpack);
        return reader.ReadMapHeader();
    }

    private sealed class CapturingSink : IOtlpLogSink
    {
        public readonly List<(string? Service, byte[] Props)> Records = [];

        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
            ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Records.Add((serviceUtf8.IsEmpty ? null : Encoding.UTF8.GetString(serviceUtf8), msgpackProps.ToArray()));
            return true;
        }

        public void NotifyBatchEnqueued() { }
    }

    // ── Payloads ─────────────────────────────────────────────────────────────

    private static string Json((string Key, object Value)[] resource, (string Key, object Value)[] record)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        using (var w = new Utf8JsonWriter(buf))
        {
            w.WriteStartObject();
            w.WriteStartArray("resourceLogs");
            w.WriteStartObject();
            w.WriteStartObject("resource");
            WriteJsonAttributes(w, resource);
            w.WriteEndObject();
            w.WriteStartArray("scopeLogs");
            w.WriteStartObject();
            w.WriteStartArray("logRecords");
            w.WriteStartObject();
            w.WriteString("timeUnixNano", "1783953780000000000");
            w.WriteNumber("severityNumber", 9);
            w.WriteStartObject("body");
            w.WriteString("stringValue", "handled");
            w.WriteEndObject();
            WriteJsonAttributes(w, record);
            w.WriteEndObject();
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndArray();
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    }

    private static void WriteJsonAttributes(Utf8JsonWriter w, (string Key, object Value)[] attrs)
    {
        w.WriteStartArray("attributes");
        foreach (var (key, value) in attrs)
        {
            w.WriteStartObject();
            w.WriteString("key", key);
            w.WriteStartObject("value");
            if (value is string s) w.WriteString("stringValue", s);
            else w.WriteString("intValue", ((long)value).ToString(System.Globalization.CultureInfo.InvariantCulture));
            w.WriteEndObject();
            w.WriteEndObject();
        }
        w.WriteEndArray();
    }

    private static byte[] Proto((string Key, object Value)[] resource, (string Key, object Value)[] record) =>
        OtlpProtoPayloads.Msg(c => OtlpProtoPayloads.Nested(c, 1, OtlpProtoPayloads.Msg(rl =>
        {
            OtlpProtoPayloads.Nested(rl, 1, OtlpProtoPayloads.Msg(res =>
            {
                foreach (var a in resource) OtlpProtoPayloads.Nested(res, 1, ProtoAttr(a.Key, a.Value));
            }));
            OtlpProtoPayloads.Nested(rl, 2, OtlpProtoPayloads.Msg(sl => OtlpProtoPayloads.Nested(sl, 2, OtlpProtoPayloads.Msg(lr =>
            {
                lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_783_953_780_000_000_000UL);
                lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                OtlpProtoPayloads.Nested(lr, 5, OtlpProtoPayloads.Msg(b =>
                {
                    b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("handled");
                }));
                foreach (var a in record) OtlpProtoPayloads.Nested(lr, 6, ProtoAttr(a.Key, a.Value));
            }))));
        })));

    private static byte[] ProtoAttr(string key, object value) => OtlpProtoPayloads.Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        OtlpProtoPayloads.Nested(c, 2, OtlpProtoPayloads.Msg(v =>
        {
            if (value is string s) { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString(s); }
            else                   { v.WriteTag(3, WireFormat.WireType.Varint);          v.WriteInt64((long)value); }
        }));
    });
}
