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
    /// The drop markers are written AFTER the client's events. A batch whose first event is over the
    /// size limit used to have its marker staged in that event's place, so a store with room for one
    /// event took the marker and none of the client's records: the request was answered 503 although
    /// something was written, and the retry wrote the Error marker again. The store's one event is
    /// now the client's first record, and the answer a partial 200, as for any batch the store ran
    /// out of room part way through.
    /// </summary>
    [Fact]
    public async Task A_store_with_room_for_one_event_takes_the_clients_first_record_not_its_drop_marker()
    {
        var opts = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var written = CaptureWrites(storage);
        TakeOnly(storage, events: 1);   // then each write fails, and is retried until the wait runs out
        try
        {
            var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
            var ctx      = Post(ClefOversizedFirst(3));

            await endpoint.HandleAsync(ctx);

            Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
            var (ingested, dropped) = Counts(ctx);
            Assert.Equal(1, ingested);
            Assert.Equal(3, dropped);                     // the oversized one, and the two the store had no room for
            Assert.Equal(new[] { "busy probe {i}" }, written);   // the client's record — no drop marker
            Assert.Equal(1, endpoint.DroppedOversized);   // a 200 ends the request: counted
        }
        finally { storage._afterWriteGate = null; }
    }

    /// <summary>
    /// A batch of nothing but events over the size limit is answered for good — a 200 with every
    /// one dropped — however much room the store had. No retry could store any of its events, only
    /// send them again, each over the limit, to write the markers. It was a 503 whenever a marker
    /// was left unwritten, the one already written included (then written again by the retry).
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task A_batch_of_nothing_but_events_over_the_size_limit_is_answered_200_whatever_the_store_took(int room)
    {
        var opts = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var written = CaptureWrites(storage);
        TakeOnly(storage, events: room);
        try
        {
            var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
            var ctx      = Post(ClefOversizedOnly(2));

            await endpoint.HandleAsync(ctx);

            Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
            var (ingested, dropped) = Counts(ctx);
            Assert.Equal(0, ingested);
            Assert.Equal(2, dropped);
            Assert.Equal(room, written.Count);            // the markers there was room for
            Assert.Equal(2, endpoint.DroppedOversized);
        }
        finally { storage._afterWriteGate = null; }
    }

    /// <summary>
    /// The promise end to end: a store that takes none of a batch with an event over the size limit
    /// writes nothing of it — not its drop marker either — and answers 503; the retry, with room,
    /// writes the client's records and the marker once each.
    /// </summary>
    [Fact]
    public async Task A_503_wrote_nothing_and_its_retry_writes_the_drop_marker_once()
    {
        var opts = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var written  = CaptureWrites(storage);
        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);

        TakeOnly(storage, events: 0);
        var refused = Post(ClefOversizedFirst(2));
        try { await endpoint.HandleAsync(refused); }
        finally { storage._afterWriteGate = null; }

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, refused.Response.StatusCode);
        Assert.Empty(written);

        var retried = Post(ClefOversizedFirst(2));
        await endpoint.HandleAsync(retried);

        Assert.Equal(StatusCodes.Status200OK, retried.Response.StatusCode);
        Assert.Equal((2, 1), Counts(retried));
        Assert.Equal(1, written.Count(t => t.Contains("dropped an oversized log event", StringComparison.Ordinal)));
        Assert.Equal(3, written.Count);
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

    /// <summary>
    /// The Debug line that says what the store did not write runs after the write, and is no reason
    /// to fail the request: one that ran out of memory there (its arguments box) made the answer a
    /// 500 — for a batch the store had partly written, with the count of that part lost, and a Seq
    /// client's retry writing it twice. Here the store takes nothing, so the answer is the 503.
    /// </summary>
    [Fact]
    public async Task A_log_line_that_runs_out_of_memory_after_the_write_does_not_cost_the_answer()
    {
        var opts    = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        var storage = Engine(opts);
        await storage.DisposeAsync();   // takes nothing

        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, new OutOfMemoryOnDebug());
        var ctx      = Post(Clef(3));

        await endpoint.HandleAsync(ctx);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.Equal((0, 3), Counts(ctx));
    }

    /// <summary>Throws an <see cref="OutOfMemoryException"/> from every Debug line.</summary>
    private sealed class OutOfMemoryOnDebug : Microsoft.Extensions.Logging.ILogger<IngestionEndpoint>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == Microsoft.Extensions.Logging.LogLevel.Debug)
                throw new OutOfMemoryException("test: the Debug line ran out of memory");
        }
    }

    private ServerOptions Options(TimeSpan backPressureWait)
    {
        Directory.CreateDirectory(_dir);
        return new ServerOptions
        {
            DataDirectory = _dir,
            Ingestion     = new IngestionOptions { MaxEventPayloadBytes = 64, BackPressureWait = backPressureWait },
        };
    }

    private static StorageEngine Engine(ServerOptions opts) => new(
        Microsoft.Extensions.Options.Options.Create(opts),
        new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance);

    /// <summary>The templates of the events the store writes, in the order it writes them.</summary>
    private static List<string> CaptureWrites(StorageEngine storage)
    {
        var written = new List<string>();
        storage.EventWritten = (_, template) => { lock (written) written.Add(template); };
        return written;
    }

    /// <summary>The store takes <paramref name="events"/> more events, then fails every write — retried until the wait runs out.</summary>
    private static void TakeOnly(StorageEngine storage, int events)
    {
        int writes = 0;
        storage._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref writes) > events) throw new OutOfMemoryException("test: no room");
        };
    }

    private static (int Ingested, int Dropped) Counts(DefaultHttpContext ctx)
    {
        ctx.Response.Body.Position = 0;
        using var doc = JsonDocument.Parse(ctx.Response.Body);
        return (doc.RootElement.GetProperty("ingested").GetInt32(), doc.RootElement.GetProperty("dropped").GetInt32());
    }

    /// <summary>A CLEF batch of <paramref name="events"/> events, each with properties over a 64-byte limit.</summary>
    private static byte[] ClefOversizedOnly(int events)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt");  w.Write("oversized probe");
            w.Write("Blob"); w.Write(new string('x', 200));
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
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
