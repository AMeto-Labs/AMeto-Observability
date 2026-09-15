using System.Buffers;
using MessagePack;
using Ameto.Ingestion;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// A client that aborts mid-send leaves a body whose prefix parses perfectly up to the cut.
/// Streaming that prefix into the ring and then answering 400 is the worst of both: Seq
/// clients treat a non-2xx as a failed batch and resend it (Serilog.Sinks.Seq throws, and its
/// batching sink retries), so the events that DID land arrive again on every retry.
///
/// <para>Content-Length is the promise. Short of it, the batch is refused whole, before a
/// single event reaches the ring — which is what these tests measure, on the ring's own
/// accepted counter rather than on a query that would have to wait for the drainer.</para>
/// </summary>
public sealed class IngestTruncatedBodyTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public IngestTruncatedBodyTests(AmetoWebAppFactory factory) => _factory = factory;

    private static byte[] Batch(int events)
    {
        var buf = new ArrayBufferWriter<byte>(1024);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(3);
            w.Write("@t");  w.Write("2024-03-01T10:20:30.1234567Z");
            w.Write("@mt"); w.Write("truncation probe {i}");
            w.Write("i");   w.Write((long)i);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Drives the endpoint through TestServer with a hand-built request, which is the only
    /// way to promise more bytes in Content-Length than the body actually carries — an
    /// HttpClient refuses to send that.
    /// </summary>
    private async Task<(int Status, long Accepted, string? ContentType, string Body)> PostAsync(
        byte[] body, long declaredLength)
    {
        _factory.CreateClient().Dispose();   // makes the factory seed its API key
        var ring     = _factory.Services.GetRequiredService<IngestionRingBuffer>();
        long before  = ring.AcceptedTotal;

        var response = await _factory.Server.SendAsync(ctx =>
        {
            ctx.Request.Method        = "POST";
            ctx.Request.Path          = "/api/events";
            ctx.Request.ContentType   = "application/octet-stream";
            ctx.Request.ContentLength = declaredLength;
            ctx.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
            ctx.Request.Body          = new MemoryStream(body);
        });

        using var reader = new StreamReader(response.Response.Body);
        return (response.Response.StatusCode, ring.AcceptedTotal - before,
                response.Response.ContentType, await reader.ReadToEndAsync());
    }

    /// <summary>
    /// A refused truncated body answers in the same shape as every other /api/events reply —
    /// counts, as JSON — so a sender parsing the 400 is not handed an empty body on this one path.
    /// </summary>
    private static void AssertNothingLandedBody(string? contentType, string body)
    {
        Assert.Equal("application/json", contentType);
        using var doc = System.Text.Json.JsonDocument.Parse(body);
        Assert.Equal(0, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("dropped").GetInt32());
        Assert.False(doc.RootElement.TryGetProperty("failedAtElement", out _));
    }

    [Fact]
    public async Task TruncatedBody_IngestsNothing_AndIsRefused()
    {
        byte[] full = Batch(20);

        // Half the promised bytes arrive — the array header and the first events are intact.
        var (status, accepted, contentType, body) = await PostAsync(full[..(full.Length / 2)], full.Length);

        Assert.Equal(StatusCodes.Status400BadRequest, status);
        Assert.Equal(0, accepted);
        AssertNothingLandedBody(contentType, body);
    }

    [Fact]
    public async Task BodyOneByteShort_IngestsNothing()
    {
        byte[] full = Batch(20);

        var (status, accepted, contentType, body) = await PostAsync(full[..^1], full.Length);

        Assert.Equal(StatusCodes.Status400BadRequest, status);
        Assert.Equal(0, accepted);
        AssertNothingLandedBody(contentType, body);
    }

    [Fact]
    public async Task CompleteBody_StillIngestsEveryEvent()
    {
        byte[] full = Batch(20);

        var (status, accepted, _, _) = await PostAsync(full, full.Length);

        Assert.Equal(StatusCodes.Status200OK, status);
        Assert.Equal(20, accepted);
    }
}
