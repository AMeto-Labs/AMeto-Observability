using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using MessagePack;
using Ameto.Ingestion;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// A batch that arrives in full and turns malformed part way through leaves its intact prefix
/// in the ring. Two things then have to happen, and neither used to:
///
/// <list type="bullet">
/// <item>the drainer is woken for that prefix — otherwise it sits in the ring until the drain
/// loop's 1 s missed-signal timeout notices it;</item>
/// <item>the 400 says how much landed and where it stopped, in the log at Warning and in the
/// response body, so an operator does not have to guess whether a refused batch was a no-op.</item>
/// </list>
/// </summary>
public sealed class IngestMalformedBatchReportingTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public IngestMalformedBatchReportingTests(AmetoWebAppFactory factory) => _factory = factory;

    /// <summary>
    /// A well-formed array header and <paramref name="good"/> complete events, followed by an
    /// element whose value is cut short — the body is entirely present, so the
    /// content-length guard does not fire and the reader throws mid-array.
    /// </summary>
    private static byte[] BatchWithBadTail(int good)
    {
        var buf = new ArrayBufferWriter<byte>(1024);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(good + 1);
        for (int i = 0; i < good; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt"); w.Write("reporting probe {i}");
            w.Write("i");   w.Write((long)i);
        }
        // The last element claims two pairs and supplies one — a malformed middle, not a
        // truncated transfer: every byte the Content-Length promises is here.
        w.WriteMapHeader(2);
        w.Write("@mt"); w.Write("never completed");
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static ByteArrayContent Content(byte[] body)
    {
        var c = new ByteArrayContent(body);
        c.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return c;
    }

    [Fact]
    public async Task MalformedTail_Answers400_WithWhatLandedAndWhereItStopped()
    {
        const int good = 5;
        var resp = await _factory.CreateClient().PostAsync("/api/events", Content(BatchWithBadTail(good)));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Equal("application/json", resp.Content.Headers.ContentType?.MediaType);

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        Assert.Equal(good, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(0,    doc.RootElement.GetProperty("dropped").GetInt32());
        Assert.Equal(good, doc.RootElement.GetProperty("failedAtElement").GetInt32());
    }

    [Fact]
    public async Task MalformedTail_WakesTheDrainerForThePrefix()
    {
        _factory.CreateClient().Dispose();
        var drainer = _factory.Services.GetRequiredService<IngestionDrainer>();

        long before = drainer.NotifyCount;
        var resp = await _factory.CreateClient().PostAsync("/api/events", Content(BatchWithBadTail(5)));
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);

        Assert.True(drainer.NotifyCount > before,
            "the prefix is in the ring; the drainer must be woken for it rather than left to time out");
    }

    /// <summary>
    /// A body that fails before any element is read still reports nothing ingested — and no
    /// failedAtElement beyond element zero.
    /// </summary>
    [Fact]
    public async Task NotAnArray_Answers400_WithNothingIngested()
    {
        var resp = await _factory.CreateClient().PostAsync(
            "/api/events", Content([0xa5, 0x68, 0x65, 0x6c, 0x6c, 0x6f]));   // fixstr "hello"

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        Assert.Equal(0, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("dropped").GetInt32());
    }

    // ── The narrowed catch ───────────────────────────────────────────────────

    /// <summary>
    /// The catch around StreamBatch used to be catch(Exception), so a fault in the SINK — the
    /// ring, the intern pool, the logger, or the ObjectDisposedException a shutdown raises
    /// mid-batch — was reported to the client as a malformed payload and hidden behind a 400.
    /// The predicate is the narrowing; these pin it.
    /// </summary>
    [Theory]
    [InlineData(typeof(MessagePackSerializationException))]
    [InlineData(typeof(EndOfStreamException))]
    public void BadBodyShapes_AreTreatedAsMalformed(Type exceptionType)
    {
        var ex = (Exception)Activator.CreateInstance(exceptionType)!;
        Assert.True(IngestionEndpoint.IsMalformedPayload(ex));
    }

    [Theory]
    [InlineData(typeof(ObjectDisposedException))]
    [InlineData(typeof(InvalidOperationException))]
    [InlineData(typeof(NullReferenceException))]
    [InlineData(typeof(OutOfMemoryException))]
    [InlineData(typeof(IOException))]
    public void ServerSideFailures_AreNotTreatedAsMalformed(Type exceptionType)
    {
        var ex = exceptionType == typeof(ObjectDisposedException)
            ? new ObjectDisposedException("ring")
            : (Exception)Activator.CreateInstance(exceptionType)!;
        Assert.False(IngestionEndpoint.IsMalformedPayload(ex),
            $"{exceptionType.Name} is the server's problem and must surface as 500, not 400");
    }
}
