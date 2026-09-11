using System.Text;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Ingestion;
using Ameto.Otel;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// Pins <see cref="OtlpLogProtoParser"/> to the decode-to-DOM-then-map path it replaces on
/// the OTLP/protobuf logs route: same payload in, the same ring entry out — timestamp,
/// level, template, service, trace/span ids, and the properties blob byte for byte.
///
/// <para>Hand-rolled wire parsing earns this. A mis-read field does not throw; it lands the
/// wrong bytes in storage and nothing notices. The properties map is compared as BYTES, not
/// as a decoded dictionary, because key order and msgpack encoding are both part of what
/// goes on disk.</para>
///
/// <para>Two divergences are asserted deliberately rather than papered over, and both move
/// the protobuf path onto the JSON path's behaviour:
/// a record with no properties at all gets an empty msgpack map instead of zero bytes
/// (normalised away below — the two decode identically), and array / kvlist attribute
/// values are encoded instead of being written as nil, because the DOM decoder never
/// modelled those AnyValue cases and protobuf clients silently lost them.</para>
/// </summary>
public sealed class OtlpLogProtoParityTests
{
    internal sealed class CapturingSink : IOtlpLogSink
    {
        public readonly List<Record> Records = [];
        public int Batches;

        internal readonly record struct Record(
            long Ts, byte Level, string Tmpl, byte[] Props, ulong TrHi, ulong TrLo, ulong Sp, string? Svc);

        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8)
        {
            Records.Add(new Record(tsTicks, level,
                Encoding.UTF8.GetString(templateUtf8),
                msgpackProps.ToArray(),
                traceHi, traceLo, spanId,
                serviceUtf8.IsEmpty ? null : Encoding.UTF8.GetString(serviceUtf8)));
            return true;
        }

        public void NotifyBatchEnqueued() => Batches++;
    }

    [Fact]
    public void MatchesDomPath_OnRealisticBatch()
    {
        byte[] payload = OtlpProtoPayloads.Logs_Realistic();
        var sink = AssertSame(payload);
        Assert.Equal(OtlpProtoPayloads.LogRecords, sink.Records.Count);
        Assert.Equal(1, sink.Batches);                       // one drainer wake per batch, not per record

        // Guard the mapping itself, not only that the two paths agree.
        var first = sink.Records[0];
        Assert.Equal("Etisalat.API", first.Svc);
        Assert.Equal((byte)LogLevel.Error, first.Level);     // record 0: severity_number 17
        Assert.Equal((byte)LogLevel.Information, sink.Records[1].Level);
        Assert.StartsWith("HTTP {Method}", first.Tmpl);
        Assert.Equal(0x0af7651916cd43ddUL, first.TrHi);
        Assert.Equal(0x8448eb211c80009cUL, first.TrLo);
        Assert.Equal(0xb7ad6b7169203300UL, first.Sp);

        var props = Props(first.Props);
        Assert.Equal("Etisalat.API", props["service.name"]);  // stays in the map as well
        Assert.Equal("0af7651916cd43dd8448eb211c80009c", props["@tr"]);
        Assert.Equal("b7ad6b7169203300", props["@sp"]);
        Assert.Equal(500L, props["http.response.status_code"]);
        Assert.Equal(true, props["cache_hit"]);
    }

    [Fact]
    public void MatchesDomPath_OnEmptyBatch()
    {
        var sink = new CapturingSink();
        byte[] empty = OtlpProtoPayloads.EmptyLogs();
        var (ingested, dropped) = OtlpLogProtoParser.Parse(empty, sink);

        Assert.Equal(0, ingested);
        Assert.Equal(0, dropped);
        Assert.Empty(sink.Records);
        Assert.Equal(0, sink.Batches);                       // nothing ingested ⇒ the drainer is left alone
        Assert.Empty(ViaDom(empty));
    }

    [Fact]
    public void MatchesDomPath_OnScalarAttributeTypes()
    {
        byte[] payload = OtlpProtoPayloads.Logs_ScalarAttributeTypes();
        var sink = AssertSame(payload);

        var props = Props(sink.Records[0].Props);
        Assert.Equal(42L,   props["int.attr"]);
        Assert.Equal(-7L,   props["negative.attr"]);
        Assert.Equal(true,  props["bool.attr"]);
        Assert.Equal(false, props["false.attr"]);
        Assert.Equal(1.5,   props["double.attr"]);
        Assert.Equal("",    props["empty.attr"]);
        Assert.Null(props["no.value"]);                      // key present, value message absent
        Assert.Null(props["empty.value"]);                   // AnyValue with no case set
        Assert.False(props.ContainsKey("orphan"));           // value with no key is dropped entirely
        Assert.Equal(8L, props["host.cpu.count"]);           // resource attributes lead the map
        Assert.Null(sink.Records[0].Svc);                    // no service.name on this resource
    }

    [Fact]
    public void MatchesDomPath_OnBareRecord()
    {
        // No timestamp, severity, body, attributes or trace id anywhere.
        byte[] payload = OtlpProtoPayloads.Logs_BareRecord();
        var sink = AssertSame(payload);

        var rec = Assert.Single(sink.Records);
        Assert.Equal(string.Empty, rec.Tmpl);
        Assert.Null(rec.Svc);
        Assert.Equal(0UL, rec.TrHi);
        Assert.Equal(0UL, rec.TrLo);
        Assert.Equal(0UL, rec.Sp);
        Assert.Equal((byte)LogLevel.Information, rec.Level);
        Assert.Empty(Props(rec.Props));
        // A zero timestamp means "now" on both paths.
        Assert.InRange(rec.Ts, DateTimeOffset.UtcNow.UtcTicks - TimeSpan.TicksPerMinute,
                               DateTimeOffset.UtcNow.UtcTicks + TimeSpan.TicksPerMinute);
    }

    [Fact]
    public void MatchesDomPath_OnSeverityTextFallback()
    {
        // severity_number absent for all of these, so the text decides — and the body is an
        // int_value, which has no string to be a template.
        byte[] payload = OtlpProtoPayloads.Logs_SeverityTextOnly();
        var sink = AssertSame(payload);

        Assert.Equal(OtlpProtoPayloads.SeverityTexts.Length, sink.Records.Count);
        byte[] expected =
        [
            (byte)LogLevel.Fatal, (byte)LogLevel.Error, (byte)LogLevel.Warning, (byte)LogLevel.Warning,
            (byte)LogLevel.Information, (byte)LogLevel.Information, (byte)LogLevel.Debug,
            (byte)LogLevel.Verbose, (byte)LogLevel.Information, (byte)LogLevel.Information,
        ];
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], sink.Records[i].Level);
        Assert.All(sink.Records, r => Assert.Equal(string.Empty, r.Tmpl));
    }

    /// <summary>
    /// Nested array / kvlist / bytes attribute values, and a trace id of a length no
    /// conformant client sends. This is the ONE payload where the paths differ: the DOM
    /// decoder had no case for AnyValue fields 5, 6 and 7, so all three collapsed to nil.
    /// </summary>
    [Fact]
    public void EncodesNestedValues_TheDomPathDroppedToNil()
    {
        byte[] payload = OtlpProtoPayloads.Logs_NestedAndBytes();

        var sink = new CapturingSink();
        OtlpLogProtoParser.Parse(payload, sink);
        var rec   = Assert.Single(sink.Records);
        var props = Props(rec.Props);

        var tags = Assert.IsType<object[]>(props["tags"]);
        Assert.Equal(new object?[] { "a", 2L, true }, tags);

        var ctx = Assert.IsType<Dictionary<string, object?>>(props["ctx"]);
        Assert.Equal(3, ctx.Count);                          // the keyless entry is dropped
        Assert.Equal("x", ctx["inner"]);
        Assert.Equal(2L,  ctx["depth"]);
        Assert.Equal(new object?[] { 0.5 }, Assert.IsType<object[]>(ctx["list"]));

        Assert.Null(props["blob"]);                          // bytes_value: nil on both paths

        // The short trace id is hex-encoded into @tr exactly as the mapper did, but does not
        // parse into the correlation columns — only a 16-byte id does.
        Assert.Equal("0af7651916cd43dd", props["@tr"]);
        Assert.False(props.ContainsKey("@sp"));
        Assert.Equal(0UL, rec.TrHi);
        Assert.Equal(0UL, rec.TrLo);

        // What the DOM path made of the same three attributes.
        var domProps = Props(ViaDom(payload)[0].RawProperties.ToArray());
        Assert.Null(domProps["tags"]);
        Assert.Null(domProps["ctx"]);
        Assert.Null(domProps["blob"]);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static List<LogEvent> ViaDom(byte[] payload)
        => OtlpLogMapper.Map(OtlpProtoDecoder.DecodeLogs(payload, payload.Length), NodeId.Local.Value);

    private static Dictionary<string, object?> Props(byte[] msgpack)
        => msgpack.Length == 0 ? [] : LogEventSerializer.DeserializePropertiesMap(msgpack) ?? [];

    /// <summary>
    /// An empty property map and no property bytes at all decode the same; the mapper wrote
    /// the second, the parser writes the first (as the JSON path does).
    /// </summary>
    private static ReadOnlySpan<byte> Normalise(ReadOnlySpan<byte> props)
        => props.Length == 1 && props[0] == 0x80 ? default : props;

    private static CapturingSink AssertSame(byte[] payload)
    {
        var dom  = ViaDom(payload);
        var sink = new CapturingSink();
        var (ingested, dropped) = OtlpLogProtoParser.Parse(payload, sink);

        Assert.Equal(0, dropped);
        Assert.Equal(dom.Count, ingested);
        Assert.Equal(dom.Count, sink.Records.Count);

        for (int i = 0; i < dom.Count; i++)
        {
            var d = dom[i];
            var s = sink.Records[i];

            // A record with no timestamp gets "now" from each path independently.
            if (d.Timestamp.UtcTicks != s.Ts)
                Assert.InRange(s.Ts, d.Timestamp.UtcTicks - TimeSpan.TicksPerMinute,
                                     d.Timestamp.UtcTicks + TimeSpan.TicksPerMinute);

            Assert.Equal((byte)d.Level,     s.Level);
            Assert.Equal(d.MessageTemplate, s.Tmpl);
            Assert.Equal(d.ServiceName,     s.Svc);
            Assert.Equal(d.TraceIdHi,       s.TrHi);
            Assert.Equal(d.TraceIdLo,       s.TrLo);
            Assert.Equal(d.SpanId,          s.Sp);
            Assert.True(Normalise(d.RawProperties.Span).SequenceEqual(Normalise(s.Props)),
                $"record {i}: property bytes differ\n  dom: {Convert.ToHexString(d.RawProperties.Span)}\n  new: {Convert.ToHexString(s.Props)}");
        }
        return sink;
    }
}
