using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using MessagePack;

namespace Ameto.Integration.Tests;

/// <summary>
/// The <c>/api/events</c> reply is written straight into the response buffer with
/// Utf8Formatter instead of an interpolated string. Its bytes are a contract — clients parse
/// them — so they are pinned here character for character, including the boundary shapes an
/// off-by-one in the hand-written writer would break.
/// </summary>
public sealed class IngestResponseBodyTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public IngestResponseBodyTests(AmetoWebAppFactory factory) => _factory = factory;

    private static ByteArrayContent Batch(int events)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt"); w.Write("response body probe {i}");
            w.Write("i");   w.Write((long)i);
        }
        w.Flush();

        var content = new ByteArrayContent(buf.WrittenSpan.ToArray());
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return content;
    }

    [Theory]
    [InlineData(0)]    // both counts single digit
    [InlineData(1)]
    [InlineData(9)]
    [InlineData(10)]   // two digits — the carry an off-by-one in the writer would eat
    [InlineData(100)]
    public async Task ReplyIsExactlyTheCountsJson(int events)
    {
        var resp = await _factory.CreateClient().PostAsync("/api/events", Batch(events));

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("application/json", resp.Content.Headers.ContentType?.MediaType);

        string body = await resp.Content.ReadAsStringAsync();
        Assert.Equal($"{{\"ingested\":{events},\"dropped\":0}}", body);
    }
}
