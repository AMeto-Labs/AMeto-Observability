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

    /// <summary>
    /// "Nothing written" is about the CLIENT's events. A batch whose first event is over the size
    /// limit has a drop marker staged in its place, and the marker is written like any event; when
    /// the store then takes nothing more, r.Written was 1 and the batch was answered 200 with every
    /// one of the client's records dropped — a partial success no client retries. It is a 503.
    /// </summary>
    [Fact]
    public async Task A_batch_of_which_only_a_drop_marker_was_written_is_answered_503_not_200()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions
        {
            DataDirectory = _dir,
            Ingestion     = new IngestionOptions { MaxEventPayloadBytes = 64, BackPressureWait = TimeSpan.FromMilliseconds(100) },
        };
        await using var storage = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        int writes = 0;
        storage._afterWriteGate = () =>
        {
            // The store takes the marker, then nothing: each later write fails, and is retried until the wait runs out.
            if (Interlocked.Increment(ref writes) >= 2) throw new OutOfMemoryException("test: no room after the marker");
        };
        try
        {
            var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
            var ctx      = Post(ClefOversizedFirst(3));

            await endpoint.HandleAsync(ctx);

            Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
            ctx.Response.Body.Position = 0;
            using var doc = JsonDocument.Parse(ctx.Response.Body);
            Assert.Equal(0, doc.RootElement.GetProperty("ingested").GetInt32());
            Assert.Equal(4, doc.RootElement.GetProperty("dropped").GetInt32());   // the oversized one, and the three not written
            Assert.Equal(1, endpoint.AcceptedTotal);                               // the marker
        }
        finally { storage._afterWriteGate = null; }
    }

    /// <summary>
    /// An event over the size limit is counted once, by the answer that ends its request — not once
    /// per attempt. A batch answered 503 is staged again by every retry, so one oversized event in a
    /// batch retried each second for three minutes read as ~180 drops. (Its Warning, which names the
    /// sender, is still logged as each attempt is read: a log call that runs out of memory there is
    /// answered 503 before anything is written, which OtlpOutOfMemoryTests pins.)
    /// </summary>
    [Fact]
    public async Task An_event_over_the_size_limit_is_counted_by_the_answer_that_ends_its_request_not_by_each_retry()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions
        {
            DataDirectory = _dir,
            Ingestion     = new IngestionOptions { MaxEventPayloadBytes = 64 },
        };
        var storage = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        await storage.DisposeAsync();   // takes nothing: every attempt is answered 503

        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        for (int attempt = 0; attempt < 3; attempt++)
        {
            var ctx = Post(ClefOversizedFirst(2));
            await endpoint.HandleAsync(ctx);
            Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        }
        Assert.Equal(0, endpoint.DroppedOversized);

        // The attempt that is answered for good counts it, once.
        await using var open = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        var accepting = new IngestionEndpoint(open, open.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        var last      = Post(ClefOversizedFirst(2));
        await accepting.HandleAsync(last);
        Assert.Equal(StatusCodes.Status200OK, last.Response.StatusCode);
        Assert.Equal(1, accepting.DroppedOversized);
    }

    /// <summary>A CLEF batch whose first event's properties are over a 64-byte limit, then <paramref name="events"/> ordinary ones.</summary>
    private static byte[] ClefOversizedFirst(int events)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events + 1);
        w.WriteMapHeader(2);
        w.Write("@mt");  w.Write("oversized probe");
        w.Write("Blob"); w.Write(new string('x', 200));
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt"); w.Write("busy probe {i}");
            w.Write("i");   w.Write((long)i);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
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
