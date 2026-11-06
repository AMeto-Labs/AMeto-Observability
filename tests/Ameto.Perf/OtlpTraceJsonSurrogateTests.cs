using System.Text;
using Ameto.Otel;
using MessagePack;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// #138 item 5: the JSON trace parser compared property names with <c>ValueTextEquals</c> and read values
/// with <c>GetString</c>/<c>CopyString</c>, which throw on a <c>\u</c> escape of a lone surrogate
/// (<c>"\ud800"</c>). The throw left the receiver's parse: 400, with the batch's prefix already in the
/// ring and acknowledged — and OTLP does not retry a 400. The log parser was given U+FFFD for such text in
/// round 5; the trace parser now goes through the same helper.
/// </summary>
public sealed class OtlpTraceJsonSurrogateTests
{
    private static byte[] Json(string spanName, string attrKey, string attrValue, string service) => Encoding.UTF8.GetBytes(
        "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"" + service + "\"}}," +
        "{\"key\":\"host\\ud800\",\"value\":{\"stringValue\":\"h\"}}]}," +
        "\"scopeSpans\":[{\"spans\":[{\"traceId\":\"0af7651916cd43dd8448eb211c80319c\",\"spanId\":\"b7ad6b7169203331\"," +
        "\"name\":\"" + spanName + "\",\"kind\":2,\"startTimeUnixNano\":\"1785300000000000000\",\"endTimeUnixNano\":\"1785300000012000000\"," +
        "\"attributes\":[{\"key\":\"" + attrKey + "\",\"value\":{\"stringValue\":\"" + attrValue + "\"}}," +
        "{\"key\":\"http.response.status_code\",\"value\":{\"intValue\":\"2\\ud80000\"}}]}]}]}]}");

    private static Dictionary<string, string?> Attributes(byte[] msgpack)
    {
        var r = new MessagePackReader(msgpack);
        int n = r.ReadMapHeader();
        var d = new Dictionary<string, string?>();
        for (int i = 0; i < n; i++)
        {
            string key = r.ReadString()!;
            d[key] = r.NextMessagePackType == MessagePackType.String ? r.ReadString() : SkipToNull(ref r);
        }
        return d;

        static string? SkipToNull(ref MessagePackReader r) { r.Skip(); return null; }
    }

    [Fact]
    public void A_lone_surrogate_escape_is_stored_as_U_FFFD_instead_of_refusing_the_batch()
    {
        var sink = new CapturingSpanSink();

        var (ingested, refused, _) = OtlpTraceStreamParser.Parse(
            Json(spanName: "GET \\ud800/x", attrKey: "k\\ud800", attrValue: "v\\udc00", service: "svc\\ud800"), sink);

        Assert.Equal((1, 0), (ingested, refused));
        var span = Assert.Single(sink.Spans);
        Assert.Equal("GET \uFFFD/x", Encoding.UTF8.GetString(span.Name));
        Assert.Equal("svc\uFFFD",    Encoding.UTF8.GetString(span.Service));

        var attrs = Attributes(span.Attrs);
        Assert.Equal("v\uFFFD", attrs["k\uFFFD"]);
        Assert.Equal("h",       attrs["host\uFFFD"]);                        // the resource attribute, spliced in
        Assert.True(attrs.ContainsKey("http.response.status_code"));          // the unparsable escaped int is nil, not a throw
        Assert.Equal(0, span.Http);                                            // and promoted nothing
    }

    /// <summary>
    /// An escape beside bytes that are not UTF-8: the reader takes such a token (it checks escapes, not
    /// UTF-8), each byte is stored as U+FFFD — three for one — and the text was unescaped into a buffer sized
    /// at its ESCAPED length, so it was cut wherever the rent rounded to: "\n" and 65 535 bytes of 0xFF are
    /// 65 537 bytes escaped and 196 606 stored, and the 131 072-byte rent kept 131 071. On main the parse
    /// threw (CopyString), a 400 with the batch's prefix already in the ring.
    /// </summary>
    [Fact]
    public void An_escaped_value_beside_bytes_that_are_not_utf8_is_stored_whole()
    {
        byte[] value = new byte[2 + 65_535];
        value[0] = (byte)'\\'; value[1] = (byte)'n'; value.AsSpan(2).Fill(0xFF);
        byte[] body = [.. Encoding.UTF8.GetBytes(
            "{\"resourceSpans\":[{\"scopeSpans\":[{\"spans\":[{\"traceId\":\"0af7651916cd43dd8448eb211c80319c\"," +
            "\"spanId\":\"b7ad6b7169203331\",\"name\":\"x\",\"attributes\":[{\"key\":\"k\",\"value\":{\"stringValue\":\""),
            .. value, .. Encoding.UTF8.GetBytes("\"}}]}]}]}]}")];
        var sink = new CapturingSpanSink();

        var (ingested, refused, _) = OtlpTraceStreamParser.Parse(body, sink);

        Assert.Equal((1, 0), (ingested, refused));
        Assert.Equal(1 + 65_535, Attributes(Assert.Single(sink.Spans).Attrs)["k"]!.Length);   // "\n" and one U+FFFD a byte
    }

    [Fact]
    public void Ordinary_escapes_still_read_as_before()
    {
        var sink = new CapturingSpanSink();

        var (ingested, _, _) = OtlpTraceStreamParser.Parse(
            Json(spanName: "GET \\u002fapi", attrKey: "http.url", attrValue: "http:\\/\\/h\\/p", service: "svc"), sink);

        Assert.Equal(1, ingested);
        var span = Assert.Single(sink.Spans);
        Assert.Equal("GET /api", Encoding.UTF8.GetString(span.Name));
        Assert.Equal("http://h/p", Attributes(span.Attrs)["http.url"]);
    }
}
