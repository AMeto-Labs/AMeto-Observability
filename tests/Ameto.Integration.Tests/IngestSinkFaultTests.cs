using System.Buffers;
using System.Text.Json;
using MessagePack;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// The catch around StreamBatch decides whose fault a failed batch was. A body the reader
/// rejects is the client's: 400, with the counts. A throw out of the SINK underneath (the ring,
/// the intern pool, the logger, or the ObjectDisposedException a shutdown mid-batch raises) is
/// the server's. It must leave the handler, so hosting answers 500 and logs it, rather than be
/// reported to the client as a "malformed payload".
///
/// <para>These drive <see cref="IngestionEndpoint.HandleAsync"/> itself, not the predicate:
/// widening the catch back to <c>catch (Exception)</c> makes the sink-fault test fail, which
/// a predicate-only pin never did.</para>
///
/// <para>The sink fault is a logger that throws on its first call, reached through the
/// oversized-event path inside TryIngestClef. A logger is the one sink dependency that can be
/// faulted safely: a disposed ring does not throw, it reads freed native memory.</para>
/// </summary>
public sealed class IngestSinkFaultTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public IngestSinkFaultTests(AmetoWebAppFactory factory) => _factory = factory;

    [Fact]
    public async Task SinkFault_EscapesTheHandler_InsteadOfAnswering400()
    {
        var logger   = new ThrowOnFirstLogCall();
        var endpoint = EndpointWith(logger);
        var ctx      = Post(OversizedEventBatch());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => endpoint.HandleAsync(ctx));

        Assert.Same(logger.Thrown, ex);
        Assert.NotEqual(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.Equal(0, ctx.Response.Body.Length);   // no "malformed payload" counts body
    }

    /// <summary>The same harness with a body that is the client's fault: handled, 400, counts.</summary>
    [Fact]
    public async Task ReaderFault_IsHandled_As400WithCounts()
    {
        var endpoint = EndpointWith(Microsoft.Extensions.Logging.Abstractions.NullLogger<IngestionEndpoint>.Instance);
        // [{"@mt":"ok"}, {<str32 key, len 0xffffffff>: …}] — OverflowException from the reader.
        var ctx = Post(Convert.FromHexString("92" + "81a3406d74a26f6b" + "81dbffffffff"));

        await endpoint.HandleAsync(ctx);

        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        ctx.Response.Body.Position = 0;
        using var doc = JsonDocument.Parse(ctx.Response.Body);
        Assert.Equal(1, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("failedAtElement").GetInt32());
    }

    private IngestionEndpoint EndpointWith(Microsoft.Extensions.Logging.ILogger<IngestionEndpoint> logger)
    {
        var sp = _factory.Services;
        return new IngestionEndpoint(
            sp.GetRequiredService<IngestionRingBuffer>(),
            sp.GetRequiredService<StringInternPool>(),
            sp.GetRequiredService<IngestionDrainer>(),
            sp.GetRequiredService<ServerOptions>(),
            logger);
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

    /// <summary>One event whose ~100 KB property is over the 64 KB per-event limit.</summary>
    private static byte[] OversizedEventBatch()
    {
        var buf = new ArrayBufferWriter<byte>(128 * 1024);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(1);
        w.WriteMapHeader(2);
        w.Write("@mt"); w.Write("sink fault probe");
        w.Write("Big"); w.Write(new string('x', 100_000));
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Throws from its FIRST call only, which is the oversized-drop warning inside the sink. Any
    /// later call succeeds, so a catch that swallowed the fault could log its own warning and
    /// answer 400 — which is exactly what the test must be able to see.
    /// </summary>
    private sealed class ThrowOnFirstLogCall : Microsoft.Extensions.Logging.ILogger<IngestionEndpoint>
    {
        private int _calls;
        public InvalidOperationException? Thrown { get; private set; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (Interlocked.Increment(ref _calls) != 1) return;
            Thrown = new InvalidOperationException("sink fault: the logger under TryIngestClef failed");
            throw Thrown;
        }
    }
}
