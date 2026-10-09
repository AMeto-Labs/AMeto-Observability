using System.Buffers;
using System.Text.Json;
using MessagePack;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// A request is answered once its events are in the store; when the store could take none of them
/// — no room within <c>Ingestion.BackPressureWait</c>, or the engine shutting down — the request is
/// answered 503 with <c>Retry-After</c> and the counts, never a 200 with a "dropped" count no client
/// retries. Nothing was written, so the retry the 503 asks for writes the batch once.
///
/// <para>Driven with a closed engine: the deterministic way to an engine that takes nothing. The
/// road through a full tier with every flush slot busy is <c>IngestWriteAndSpillTests</c>'s.</para>
/// </summary>
public sealed class IngestStoreBusyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-ingest-busy-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task A_batch_the_store_takes_none_of_is_answered_503_with_retry_after_and_the_counts()
    {
        Directory.CreateDirectory(_dir);
        var opts    = new ServerOptions { DataDirectory = _dir };
        var storage = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        await storage.DisposeAsync();

        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        var ctx      = Post(Clef(3));

        await endpoint.HandleAsync(ctx);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.Equal(IngestionEndpoint.RetryAfterSeconds, ctx.Response.Headers.RetryAfter.ToString());
        ctx.Response.Body.Position = 0;
        using var doc = JsonDocument.Parse(ctx.Response.Body);
        Assert.Equal(0, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(3, doc.RootElement.GetProperty("dropped").GetInt32());
        Assert.Equal(0, endpoint.AcceptedTotal);
    }

    private static DefaultHttpContext Post(byte[] body)
    {
        var ctx = new DefaultHttpContext();
        ctx.Request.Method        = "POST";
        ctx.Request.ContentType   = "application/octet-stream";
        ctx.Request.ContentLength = body.Length;
        ctx.Request.Body          = new MemoryStream(body);
        ctx.Response.Body         = new MemoryStream();
        return ctx;
    }

    private static byte[] Clef(int events)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt"); w.Write("busy probe {i}");
            w.Write("i");   w.Write((long)i);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
