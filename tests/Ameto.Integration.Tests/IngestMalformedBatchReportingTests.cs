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
    /// A body that fails before any element is read reports nothing ingested — and NO
    /// failedAtElement. It used to say failedAtElement 0, the very body a failure inside
    /// element 0 produces, so a sender could not tell "not an array" from "first event bad".
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
        Assert.False(doc.RootElement.TryGetProperty("failedAtElement", out _),
            "the array header itself failed; no element was reached");
    }

    /// <summary>A failure INSIDE element 0 still names it — the case the header failure must not look like.</summary>
    [Fact]
    public async Task FirstElementBad_Answers400_WithFailedAtElementZero()
    {
        var resp = await _factory.CreateClient().PostAsync("/api/events", Content(BatchWithBadTail(0)));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        Assert.Equal(0, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("failedAtElement").GetInt32());
    }

    // ── Client-built bodies the reader rejects with an unlisted type ──────────

    /// <summary>
    /// A 32-bit length prefix of 2^31 or more makes MessagePackReader throw OverflowException —
    /// a checked uint-to-int conversion in TryReadStringSpan and TrySkip — not the
    /// MessagePackSerializationException / EndOfStreamException pair the catch used to list. The
    /// body is entirely the client's, so it answers 400 with the counts, not a 500 with a stack
    /// trace in the server log and the prefix's drainer left unwoken.
    ///
    /// <para>Both bodies are a good event (<c>{"@mt":"ok"}</c>) followed by one carrying the
    /// overflowing prefix, so the reply also has to say that element 0 landed.</para>
    ///
    /// <para>Only rows that answered 500 under the old type list are kept. A str32/ext32 VALUE
    /// under an unknown key (Skip) and a str32 <c>@x</c> (ExceptionInfo.Read) were probed too:
    /// on a body this short they end as EndOfStreamException, which the old list already
    /// covered, so they would pass either way and prove nothing.</para>
    /// </summary>
    [Theory]
    // [{"@mt":"ok"}, {"@t": str32 len 0xffffffff, "@mt":"x"}] — a value, read by TryReadStringSpan
    [InlineData("92" + "81a3406d74a26f6b" + "82a24074dbffffffffa3406d74a178")]
    // [{"@mt":"ok"}, {<str32 key, len 0xffffffff>: …}] — a key, read by TryReadStringSpan
    [InlineData("92" + "81a3406d74a26f6b" + "81dbffffffff")]
    public async Task LengthPrefixPastInt32_Answers400_WithWhatLanded(string hex)
    {
        var resp = await _factory.CreateClient().PostAsync("/api/events", Content(Convert.FromHexString(hex)));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Equal("application/json", resp.Content.Headers.ContentType?.MediaType);

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        Assert.Equal(1, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("dropped").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("failedAtElement").GetInt32());
    }
}
