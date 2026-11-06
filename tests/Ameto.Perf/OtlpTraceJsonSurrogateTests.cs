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

        var (ingested, refused) = OtlpTraceStreamParser.Parse(
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

    [Fact]
    public void Ordinary_escapes_still_read_as_before()
    {
        var sink = new CapturingSpanSink();

        var (ingested, _) = OtlpTraceStreamParser.Parse(
            Json(spanName: "GET \\u002fapi", attrKey: "http.url", attrValue: "http:\\/\\/h\\/p", service: "svc"), sink);

        Assert.Equal(1, ingested);
        var span = Assert.Single(sink.Spans);
        Assert.Equal("GET /api", Encoding.UTF8.GetString(span.Name));
        Assert.Equal("http://h/p", Attributes(span.Attrs)["http.url"]);
    }
}
