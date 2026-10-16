using System.Buffers;
using System.Text;
using Ameto.Ingestion;
using Ameto.Otel;
using MessagePack;

namespace Ameto.Integration.Tests;

/// <summary>
/// Round-5 review H-E (round 3's H3, not closed for OTLP/JSON), with L-10 and part of L-17: what the
/// JSON log parser holds while it decodes a record, and the text it stores. The protobuf parser counts a
/// record's property map before it writes it and stops writing once it is past the sink's limit; the
/// JSON parser wrote every attribute whole into its scratch — an escaped one first unescaped into a rent
/// as long as the value — and measured the map only at assembly. One 7.9 MB attribute held 7.9 MB
/// (16.3 MB with one escape) against a 2.1 MB charge, sixteen such requests 233 MiB that the in-flight
/// bound never saw.
/// </summary>
public sealed class OtlpJsonRoomTests
{
    private const int Limit = 64 * 1024;

    /// <summary>A sink that keeps what the parser hands it.</summary>
    private sealed class Sink(int limit = Limit, int maxService = int.MaxValue) : IOtlpLogSink
    {
        public readonly List<byte[]> Maps      = [];
        public readonly List<long>   Refused   = [];
        public readonly List<byte[]> Templates = [];
        public readonly List<byte[]> Services  = [];
        public long Charged, MostCharged;

        public int MaxEventPayloadBytes => limit;
        public int MaxServiceNameBytes  => maxService;

        public void ChargeScratch(long bytes)
        {
            Charged    += bytes;
            MostCharged = Math.Max(MostCharged, Charged);
        }

        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
                                 ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Maps.Add(msgpackProps.ToArray());
            Templates.Add(templateUtf8.ToArray());
            Services.Add(serviceUtf8.ToArray());
            return true;
        }

        public bool TryIngestOversized(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, long payloadBytes,
                                       ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8, int serviceIdx)
        {
            Refused.Add(payloadBytes);
            Templates.Add(templateUtf8.ToArray());
            Services.Add(serviceUtf8.ToArray());
            return false;
        }

        public void NotifyBatchEnqueued() { }
    }

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static byte[] Cat(params byte[][] parts) => parts.SelectMany(p => p).ToArray();

    /// <summary>One record with the given JSON attribute list (and body), under a resource with the given attribute list.</summary>
    private static byte[] Body(string recordAttributes, string resourceAttributes = "", string body = "") =>
        Utf8("{\"resourceLogs\":[{\"resource\":{\"attributes\":[" + resourceAttributes + "]},\"scopeLogs\":[{\"logRecords\":[{" +
             (body.Length > 0 ? "\"body\":{\"stringValue\":\"" + body + "\"}," : "") +
             "\"attributes\":[" + recordAttributes + "]}]}]}]}");

    private static string StringAttr(string key, string jsonText) => "{\"key\":\"" + key + "\",\"value\":{\"stringValue\":\"" + jsonText + "\"}}";

    /// <summary>The map the sink got, as key → value bytes (the value's msgpack).</summary>
    private static Dictionary<string, byte[]> Pairs(byte[] map)
    {
        var r = new MessagePackReader(map);
        int n = r.ReadMapHeader();
        var pairs = new Dictionary<string, byte[]>();
        for (int i = 0; i < n; i++)
        {
            string key = r.ReadString()!;
            var start = r.Position;
            r.Skip();
            pairs[key] = r.Sequence.Slice(start, r.Position).ToArray();
        }
        Assert.True(r.End, "the map has bytes past its pairs");
        return pairs;
    }

    private static byte[] MsgpackString(byte[] utf8)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);
        w.WriteString(utf8);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    // ── H-E: a value past the limit is counted, not written ───────────────────

    /// <summary>
    /// The review's attribute: 7.9 MB in one record's attribute — plain, with one escape, and on the
    /// resource. Counted, not written: the record is refused with the size its map would have had, and the
    /// parse allocates next to nothing. It used to grow the record (or resource) scratch to the value, and
    /// rent a buffer as long again for its escape.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true,  false)]
    [InlineData(false, true)]
    [InlineData(true,  true)]
    public void An_attribute_past_the_event_limit_is_counted_not_written(bool escaped, bool onResource)
    {
        const int N = 7_900_000;
        string text  = new('x', N);
        string json  = escaped ? text[..(N / 2)] + "\\n" + text[(N / 2)..] : text;
        long   bytes = escaped ? N + 1 : N;
        byte[] body  = onResource ? Body("", StringAttr("big", json)) : Body(StringAttr("big", json));

        var sink = new Sink();
        OtlpLogStreamParser.Parse(Body(StringAttr("warm", "w\\n"), StringAttr("warm", "w\\n")), new Sink());   // this thread's scratch
        long before = GC.GetAllocatedBytesForCurrentThread();
        var (ingested, dropped) = OtlpLogStreamParser.Parse(body, sink);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal((0, 1), (ingested, dropped));
        Assert.Equal(1 + 4 + 5 + bytes, Assert.Single(sink.Refused));   // map header, "big", str32 header, the text
        Assert.True(allocated < 256 * 1024, $"the parse allocated {allocated:N0} B for a {bytes:N0} B attribute");
    }

    /// <summary>
    /// A template carries escapes: unescaped into a copy of its own, which can be as long as the body — and
    /// so is counted against the batch before it is taken (IOtlpLogSink.ChargeScratch), and given back at the
    /// parse's end. It was a body-sized rent nothing counted.
    /// </summary>
    [Fact]
    public void An_escaped_template_is_counted_against_the_batch_before_it_is_taken()
    {
        string unit = "line \\\"quoted\\\"\\n";            // 17 bytes in JSON, 14 unescaped
        string json = string.Concat(Enumerable.Repeat(unit, 150_000));
        var sink = new Sink();

        var (ingested, _) = OtlpLogStreamParser.Parse(Body("", body: json), sink);

        Assert.Equal(1, ingested);
        byte[] expected = Utf8(string.Concat(Enumerable.Repeat("line \"quoted\"\n", 150_000)));
        Assert.Equal(expected, sink.Templates[0]);
        Assert.True(sink.MostCharged >= expected.Length, $"the template's copy of {expected.Length:N0} B was counted at {sink.MostCharged:N0} B");
        Assert.Equal(0, sink.Charged);   // given back
    }

    /// <summary>
    /// A <c>service.name</c> is copied no further than the sink keeps it (four bytes past, for its cut at a
    /// character): the sink stores the same name, and a 7.9 MB one is not copied whole — escaped or not.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_service_name_is_copied_no_further_than_the_sink_keeps_it(bool escaped)
    {
        const int N = 7_900_000;
        string name = new string('s', 1022) + "€" + new string('t', N);      // a character across the 1 024-byte cut
        string json = escaped ? "\\u0073" + name[1..] : name;                // s is 's'
        byte[] body = Body(StringAttr("k", "v"), "{\"key\":\"service.name\",\"value\":{\"stringValue\":\"" + json + "\"}}");
        var sink = new Sink(maxService: 1024);
        OtlpLogStreamParser.Parse(Body(StringAttr("k", "v"), StringAttr("service.name", "warm")), new Sink(maxService: 1024));   // this thread's scratch

        long before = GC.GetAllocatedBytesForCurrentThread();
        OtlpLogStreamParser.Parse(body, sink);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        byte[] got = Assert.Single(sink.Services);
        Assert.True(got.Length <= 1024 + 4, $"{got.Length:N0} B of the service name copied for a sink that keeps 1 024");
        Assert.Equal(IngestionEndpoint.ServiceName(Utf8(name)).ToArray(), IngestionEndpoint.ServiceName(got).ToArray());
        Assert.True(allocated < 256 * 1024, $"the parse allocated {allocated:N0} B for a {N:N0} B service name");
    }

    /// <summary>
    /// What the JSON parser counts is what it would write, to the byte, for every kind of value: a map of
    /// size S is taken at a limit of S and refused at S - 1, with S as the size it would have had.
    /// </summary>
    [Theory]
    [InlineData("plain ascii")]
    [InlineData("quote \\\" backslash \\\\ slash \\/ ctl \\b\\f\\n\\r\\t")]
    [InlineData("é \\u00e9 € \\u20ac 😀 \\ud83d\\ude00 \\u0000")]
    [InlineData("lone \\ud800 and \\udc00 surrogates")]
    [InlineData("31 chars xxxxxxxxxxxxxxxxxxxxxx")]
    [InlineData("32 chars xxxxxxxxxxxxxxxxxxxxxxx")]
    public void A_json_map_is_counted_to_the_byte(string jsonText)
    {
        string attributes =
            StringAttr("s", jsonText) + "," +
            "{\"key\":\"i\",\"value\":{\"intValue\":\"-12345678901\"}}," +
            "{\"key\":\"b\",\"value\":{\"boolValue\":true}}," +
            "{\"key\":\"d\",\"value\":{\"doubleValue\":1.5}}," +
            "{\"key\":\"a\",\"value\":{\"arrayValue\":{\"values\":[{\"stringValue\":\"" + jsonText + "\"},{\"intValue\":\"7\"},{\"kvlistValue\":{\"values\":[" + StringAttr("n", jsonText) + "]}}]}}}," +
            "{\"key\":\"e\",\"value\":{\"arrayValue\":{}}}," +
            "{\"key\":\"none\"}";
        byte[] body = Utf8("{\"resourceLogs\":[{\"resource\":{\"attributes\":[" + StringAttr("r", jsonText) + "]},\"scopeLogs\":[{\"logRecords\":[{" +
                           "\"traceId\":\"0123456789abcdef0123456789abcdef\",\"spanId\":\"0123456789abcdef\",\"attributes\":[" + attributes + "]}]}]}]}");

        var whole = new Sink(limit: int.MaxValue);
        OtlpLogStreamParser.Parse(body, whole);
        int size = Assert.Single(whole.Maps).Length;

        var at = new Sink(limit: size);
        Assert.Equal((1, 0), OtlpLogStreamParser.Parse(body, at));
        Assert.Equal(whole.Maps[0], at.Maps[0]);

        var under = new Sink(limit: size - 1);
        Assert.Equal((0, 1), OtlpLogStreamParser.Parse(body, under));
        Assert.Equal(size, Assert.Single(under.Refused));
    }

    // ── R5-M8: a "resource" key given twice ───────────────────────────────────

    /// <summary>
    /// Round-6 review R5-M8. A <c>"resource"</c> key given more than once in one resourceLogs element (JSON
    /// allows it; OTLP does not send it) adds its attributes to the first's, as the protobuf parser merges a
    /// resource field given twice: the scratch holds both. The budget was reset for each, so it counted only
    /// the last: a record whose map passed the limit was assembled past it, and one under a first resource
    /// cut part way was assembled with that cut map — answered 200, stored, and given back with no
    /// properties (the review's P7M). One budget for the block now: what JSON makes of it is what protobuf
    /// makes of its twin, map for map and size for size.
    /// </summary>
    [Theory]
    [InlineData("a:60000,b:10000 | c:1")]            // the first past the limit part way, then one under it (P7M)
    [InlineData("a:30000 | b:30000 | c:30000")]      // each under the limit, together past it
    [InlineData("a:10 | b:20")]                      // together under it: one map with both
    public void A_resource_given_twice_is_one_map_as_protobuf_makes_it(string resources)
    {
        // Each resource's attributes, as key:length (a string of that many 'v').
        var attrs = resources.Split('|', StringSplitOptions.TrimEntries)
                             .Select(r => r.Split(',').Select(p => (Key: p.Split(':')[0], Len: int.Parse(p.Split(':')[1]))).ToArray()).ToArray();

        string json = "{\"resourceLogs\":[{" +
                      string.Concat(attrs.Select(a => "\"resource\":{\"attributes\":[" + string.Join(",", a.Select(kv => StringAttr(kv.Key, new string('v', kv.Len)))) + "]},")) +
                      "\"scopeLogs\":[{\"logRecords\":[{\"body\":{\"stringValue\":\"rec\"},\"attributes\":[" + StringAttr("d", "7") + "]}]}]}]}";
        byte[] proto = Pb(1, Cat(
            Cat(attrs.Select(a => Pb(1, Cat(a.Select(kv => Pb(1, PbKv(kv.Key, new string('v', kv.Len)))).ToArray()))).ToArray()),
            Pb(2, Pb(2, Cat(Pb(5, Pb(1, Utf8("rec"))), Pb(6, PbKv("d", "7")))))));

        var fromJson  = new Sink();
        var fromProto = new Sink();
        var counts    = OtlpLogStreamParser.Parse(Utf8(json), fromJson);
        Assert.Equal(OtlpLogProtoParser.Parse(proto, fromProto), counts);

        Assert.Equal(fromProto.Refused, fromJson.Refused);
        Assert.Equal(fromProto.Maps.Count, fromJson.Maps.Count);
        for (int i = 0; i < fromProto.Maps.Count; i++) Assert.Equal(fromProto.Maps[i], fromJson.Maps[i]);
    }

    /// <summary>
    /// R5-M8, its scratch: a resource given many times was written whole each time, past the limit — 130
    /// resources of 60 KB took 16.6 MB at the stage against a 2.1 MB charge (the review's P3D). Past the
    /// block's one budget nothing more is written.
    /// </summary>
    [Fact]
    public void A_resource_given_many_times_takes_no_more_scratch_than_one_map()
    {
        var sb = new StringBuilder("{\"resourceLogs\":[{");
        for (int i = 0; i < 64; i++) sb.Append("\"resource\":{\"attributes\":[").Append(StringAttr("r" + i, new string('v', 60_000))).Append("]},");
        sb.Append("\"scopeLogs\":[{\"logRecords\":[{\"attributes\":[").Append(StringAttr("a", "x")).Append("]}]}]}]}");
        byte[] body = Utf8(sb.ToString());

        var sink = new Sink();
        OtlpLogStreamParser.Parse(Body(StringAttr("warm", "w"), StringAttr("warm", "w")), new Sink());   // this thread's scratch
        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal((0, 1), OtlpLogStreamParser.Parse(body, sink));
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // map16 header, 64 × (the key "rN", str16 header, the text), then "a" and "x": the map it would have been.
        Assert.Equal(3 + Enumerable.Range(0, 64).Sum(i => 1 + ("r" + i).Length + 3 + 60_000) + 2 + 2, Assert.Single(sink.Refused));
        Assert.True(allocated < 512 * 1024, $"the parse allocated {allocated:N0} B for 64 resources of 60 KB, against a {Limit:N0} B limit");
    }

    /// <summary>A protobuf length-delimited field: tag, varint length, payload.</summary>
    private static byte[] Pb(int field, byte[] payload)
    {
        var o = new List<byte> { (byte)(field << 3 | 2) };
        for (ulong v = (ulong)payload.Length; ; v >>= 7) { if (v < 0x80) { o.Add((byte)v); break; } o.Add((byte)(v | 0x80)); }
        o.AddRange(payload);
        return o.ToArray();
    }

    /// <summary>A protobuf KeyValue with a string value.</summary>
    private static byte[] PbKv(string key, string value) => Cat(Pb(1, Utf8(key)), Pb(2, Pb(1, Utf8(value))));

    // ── L-10: text that is not UTF-8 ──────────────────────────────────────────

    /// <summary>
    /// Round-5 review L-10. A JSON string the parse could not transcode — a lone surrogate escape, or a byte
    /// that is not UTF-8 beside an escape — threw out of the parse part way through the batch (its prefix
    /// already staged, the answer 400); and one with no escape was stored as it came, where the protobuf
    /// parser stores U+FFFD. Each sequence that is not UTF-8 is U+FFFD now, as protobuf stores it.
    /// </summary>
    [Theory]
    [InlineData(new byte[] { (byte)'a', (byte)'\\', (byte)'u', (byte)'d', (byte)'8', (byte)'0', (byte)'0', (byte)'b' },
                new byte[] { (byte)'a', 0xEF, 0xBF, 0xBD, (byte)'b' })]
    [InlineData(new byte[] { (byte)'a', 0xFF, (byte)'\\', (byte)'n' },
                new byte[] { (byte)'a', 0xEF, 0xBF, 0xBD, (byte)'\n' })]
    [InlineData(new byte[] { (byte)'a', 0xFF, (byte)'b' },
                new byte[] { (byte)'a', 0xEF, 0xBF, 0xBD, (byte)'b' })]
    [InlineData(new byte[] { (byte)'a', 0xE2, 0x82, (byte)'b' },
                new byte[] { (byte)'a', 0xEF, 0xBF, 0xBD, (byte)'b' })]
    public void Json_text_that_is_not_utf8_is_stored_as_protobuf_stores_it(byte[] jsonText, byte[] stored)
    {
        byte[] body = Cat(Utf8("{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{\"attributes\":[{\"key\":\"v\",\"value\":{\"stringValue\":\""),
                          jsonText, Utf8("\"}}]},{\"body\":{\"stringValue\":\"second\"}}]}]}]}"));
        var sink = new Sink();

        Assert.Equal((2, 0), OtlpLogStreamParser.Parse(body, sink));
        Assert.Equal(MsgpackString(stored), Pairs(sink.Maps[0])["v"]);
    }

    // ── L-17: nested values, written in place ─────────────────────────────────

    /// <summary>
    /// A nested array or kvlist was buffered in a writer of its own at every level and copied into the
    /// one above: a 4 MB leaf fifteen levels deep allocated 131 MB. Counted on a copy of the reader and
    /// written in place now — and past the limit not written at all.
    /// </summary>
    [Fact]
    public void A_nested_value_is_written_in_place()
    {
        const int Depth = 15;
        string leaf = "{\"stringValue\":\"" + new string('x', 4_000_000) + "\"}";
        string value = leaf;
        for (int i = 0; i < Depth; i++) value = "{\"arrayValue\":{\"values\":[" + value + "]}}";
        byte[] body = Body("{\"key\":\"deep\",\"value\":" + value + "}");
        var sink = new Sink();

        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal((0, 1), OtlpLogStreamParser.Parse(body, sink));
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(1 + 5 + Depth * 1 + 5 + 4_000_000, Assert.Single(sink.Refused));   // header, "deep", 15 array headers, str32, the leaf
        Assert.True(allocated < 256 * 1024, $"the parse allocated {allocated:N0} B for a {Depth}-level 4 MB value");
    }

    /// <summary>
    /// An array or kvlist value with no <c>values</c> is an empty one. It was no value at all: its key was
    /// followed by the next pair's, and the map no longer read as the pairs it counted.
    /// </summary>
    [Fact]
    public void An_array_or_kvlist_value_with_no_values_is_an_empty_one()
    {
        var sink = new Sink();
        OtlpLogStreamParser.Parse(Body("{\"key\":\"a\",\"value\":{\"arrayValue\":{}}},{\"key\":\"k\",\"value\":{\"kvlistValue\":{}}}," + StringAttr("after", "x")), sink);

        var pairs = Pairs(Assert.Single(sink.Maps));
        Assert.Equal(new byte[] { 0x90 }, pairs["a"]);   // fixarray, 0
        Assert.Equal(new byte[] { 0x80 }, pairs["k"]);   // fixmap, 0
        Assert.Equal(MsgpackString(Utf8("x")), pairs["after"]);
    }
}
