using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Ameto.Core;

namespace Ameto.Integration.Tests;

/// <summary>
/// The status an OTLP/HTTP receiver answers a body that is too big — over the wire, through the
/// real pipeline.
///
/// <para>The hole this fills: <c>OtlpEndpointMapper.ReadBodyAsync</c> sets 413 when the shared
/// body reader hands back a null buffer, and all three HTTP handlers return on null — but nothing
/// posted an oversized body to <c>/otlp/v1/*</c>. <see cref="OtlpOversizedBodyTests"/> covers the
/// READER (what it rents, what it gives back) by driving it directly, which is the right way to
/// cover both receivers at once; the consequence is that it never reaches the mapping from "null
/// buffer" to "413". Deleting or inverting that branch left every suite green while the receiver
/// answered <b>200 with an empty body</b> — a collector would record the batch as delivered and
/// drop it, which is the one failure a sender cannot detect.</para>
///
/// <para>The ceiling is configured down to a few kilobytes rather than posting 8 MiB per case:
/// what is under test is the refusal, and the reader's own behaviour at the real default is
/// already measured next door.</para>
/// </summary>
public sealed class OtlpHttpOversizedBodyStatusTests : IClassFixture<OtlpHttpOversizedBodyStatusTests.Factory>
{
    /// <summary>Small enough to cross in one allocation, large enough to be a plausible body.</summary>
    private const int MaxOtlp = 4096;

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { MaxOtlpBatchBytes = MaxOtlp };
    }

    private readonly HttpClient _client;

    public OtlpHttpOversizedBodyStatusTests(Factory factory) => _client = factory.CreateClient();

    private static ByteArrayContent Json(byte[] bytes)
    {
        var content = new ByteArrayContent(bytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        return content;
    }

    /// <summary>
    /// A body one byte over the ceiling, with the length declared — the road where the reader
    /// refuses before renting anything at all.
    /// </summary>
    [Theory]
    [InlineData("/otlp/v1/logs")]
    [InlineData("/v1/logs")]
    [InlineData("/otlp/v1/traces")]
    [InlineData("/otlp/v1/metrics")]
    public async Task A_declared_body_over_the_ceiling_is_refused_with_413(string path)
    {
        var response = await _client.PostAsync(path, Json(new byte[MaxOtlp + 1]));

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
    }

    /// <summary>
    /// The same refusal when the body only PROVES it is too big by arriving: no Content-Length,
    /// so the reader discovers the excess mid-read and must still answer 413 rather than fall
    /// through to a handler with a null buffer.
    /// </summary>
    [Fact]
    public async Task A_chunked_body_over_the_ceiling_is_refused_with_413()
    {
        var content = new StreamContent(new UndeclaredLengthStream(new byte[MaxOtlp + 1]));
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

        var response = await _client.PostAsync("/otlp/v1/logs", content);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
    }

    /// <summary>
    /// The control, and the reason the assertions above are about SIZE: the same route, the same
    /// key, a well-formed body under the ceiling — 200. Without this, a receiver that answered
    /// 413 to everything (a broken key, a missing route) would pass the tests above.
    /// </summary>
    [Theory]
    [InlineData("/otlp/v1/logs",    "{\"resourceLogs\":[]}")]
    [InlineData("/v1/logs",         "{\"resourceLogs\":[]}")]
    [InlineData("/otlp/v1/traces",  "{\"resourceSpans\":[]}")]
    [InlineData("/otlp/v1/metrics", "{\"resourceMetrics\":[]}")]
    public async Task A_body_under_the_ceiling_is_accepted(string path, string body)
    {
        var response = await _client.PostAsync(path, Json(Encoding.UTF8.GetBytes(body)));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    /// <summary>
    /// A stream that refuses to state its length, so <see cref="HttpClient"/> sends the body
    /// chunked. <see cref="StreamContent"/> over a plain <see cref="MemoryStream"/> would declare
    /// one and take the other road.
    /// </summary>
    private sealed class UndeclaredLengthStream : Stream
    {
        private readonly byte[] _bytes;
        private          int    _position;

        public UndeclaredLengthStream(byte[] bytes) => _bytes = bytes;

        public override bool CanRead  => true;
        public override bool CanSeek  => false;
        public override bool CanWrite => false;
        public override long Length   => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int n = Math.Min(count, _bytes.Length - _position);
            Array.Copy(_bytes, _position, buffer, offset, n);
            _position += n;
            return n;
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value)                => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
