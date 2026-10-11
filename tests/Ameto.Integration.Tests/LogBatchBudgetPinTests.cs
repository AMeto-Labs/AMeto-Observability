using System.Buffers;
using System.Text;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Ameto.Integration.Tests;

/// <summary>
/// Round-4 review L8: three parts of the decoded-batch bound no test reached — with all three mutated
/// away, every test passed. The payload array sized from what the limit leaves, rather than doubled
/// past it; and the mid-decode 503 of the OTLP/HTTP and CLEF receivers, when a batch's reservation
/// cannot grow while other batches hold the rest of the bound.
/// </summary>
public sealed class LogBatchBudgetPinTests : IClassFixture<LogBatchBudgetPinTests.Factory>, IAsyncLifetime
{
    private const long InFlight = 4L * 1024 * 1024;

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { MaxDecodedLogBytesInFlight = InFlight };
    }

    private readonly Factory _factory;
    private readonly string  _dir = Path.Combine(Path.GetTempPath(), "ameto-budget-pin-" + Guid.NewGuid().ToString("N"));
    private StorageEngine    _storage = null!;

    public LogBatchBudgetPinTests(Factory factory)
    {
        _factory = factory;
        using var seeded = factory.CreateClient();   // creating a client is what seeds the test API key
    }

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _storage = new StorageEngine(Options.Create(opts), new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                                     NullLogger<StorageEngine>.Instance);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _storage.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private IngestionEndpoint Endpoint(long limit, long inFlight) => new(
        _storage, _storage.TemplatePool,
        new ServerOptions
        {
            DataDirectory = _dir,
            Ingestion     = new IngestionOptions { MaxDecodedLogBatchBytes = limit, MaxDecodedLogBytesInFlight = inFlight },
        },
        NullLogger<IngestionEndpoint>.Instance);

    /// <summary>
    /// The payload array grows from what the batch's limit leaves, not by doubling past it: a batch of
    /// 9 MiB of payloads under a 10 MiB limit, its array first taken at 4 MiB, doubled to 8 and then to
    /// 16 — refused as too heavy at 8, with the 9 it needed well inside its limit.
    /// </summary>
    [Fact]
    public void A_batch_near_its_limit_grows_its_payloads_to_what_the_limit_leaves_and_is_taken()
    {
        var endpoint = Endpoint(limit: 10L * 1024 * 1024, inFlight: 64L * 1024 * 1024);
        using var batch = endpoint.TryBeginBatch(payloadSizeHint: 4 * 1024 * 1024, decodedWeight: 1024 * 1024)!;
        var payload = new byte[60 * 1024];
        payload[0] = 0x80;   // an empty msgpack map, then padding the batch keeps as it is
        for (int i = 0; i < 9 * 1024 / 60; i++)
            Assert.True(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)LogLevel.Information, "headroom {N}"u8, payload, 0, 0, 0, "svc"u8));
        Assert.True(batch.Staged.PayloadCapacity <= 10L * 1024 * 1024, $"the payload array is {batch.Staged.PayloadCapacity:N0} B");
    }

    /// <summary>
    /// CLEF: a batch whose reservation cannot grow mid-decode — other batches hold the rest of the bound
    /// — is answered 503 with Retry-After and nothing written; the exception came out of the sink, and
    /// without its catch it was a 500 (a Seq client's retry is the same either way, but an operator's
    /// alert is not).
    /// </summary>
    [Fact]
    public async Task A_clef_batch_whose_reservation_cannot_grow_mid_decode_is_503()
    {
        var endpoint = Endpoint(limit: 64L * 1024 * 1024, inFlight: InFlight);
        byte[] body  = ClefWithExceptions(events: 40, stackChars: 6_000);   // ~0.25 MB, decoding to several times that
        long weight  = IngestionEndpoint.WeighClef(body, out _);
        using var others = endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: InFlight - weight - 16 * 1024)!;

        var ctx = new DefaultHttpContext();
        ctx.Request.Method        = "POST";
        ctx.Request.ContentType   = "application/octet-stream";
        ctx.Request.ContentLength = body.Length;
        ctx.Request.Body          = new MemoryStream(body);
        ctx.Response.Body         = new MemoryStream();
        await endpoint.HandleAsync(ctx);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.Equal(IngestionEndpoint.RetryAfterSeconds, ctx.Response.Headers.RetryAfter.ToString());
        Assert.Equal(0, endpoint.AcceptedTotal);
    }

    /// <summary>OTLP/HTTP: the same, 503 with the "holding as many decoded log batches" Status — without its catch, a 400.</summary>
    [Fact]
    public async Task An_otlp_http_batch_whose_reservation_cannot_grow_mid_decode_is_503()
    {
        var endpoint = _factory.Services.GetRequiredService<IngestionEndpoint>();
        var logs     = ((RouteEndpoint)_factory.Services.GetRequiredService<EndpointDataSource>().Endpoints
                           .Single(e => e is RouteEndpoint r && r.RoutePattern.RawText == "/v1/logs")).RequestDelegate!;
        var sb = new StringBuilder("{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[");
        for (int i = 0; i < 400; i++)
            sb.Append(i == 0 ? "" : ",").Append("{\"attributes\":[{\"key\":\"pad\",\"value\":{\"stringValue\":\"")
              .Append('p', 3_000).Append("\"}}]}");
        sb.Append("]}]}]}");
        byte[] body = Encoding.UTF8.GetBytes(sb.ToString());   // weighs 400 x 128 B, decodes to ~1.2 MB
        long accepted = endpoint.AcceptedTotal;

        using (endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: InFlight - 256 * 1024)!)   // others hold all but 256 KB
        {
            var ctx = new DefaultHttpContext { RequestServices = _factory.Services };
            ctx.Request.Method        = "POST";
            ctx.Request.ContentType   = "application/json";
            ctx.Request.ContentLength = body.Length;
            ctx.Request.Body          = new MemoryStream(body);
            ctx.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
            ctx.Response.Body         = new MemoryStream();
            await logs(ctx);

            Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
            await ctx.Response.BodyWriter.FlushAsync();   // the host completes the writer; a bare context does not
            ctx.Response.Body.Position = 0;
            Assert.Contains("holding as many decoded log batches", new StreamReader(ctx.Response.Body).ReadToEnd());
        }
        Assert.Equal(accepted, endpoint.AcceptedTotal);
    }

    /// <summary>CLEF events whose <c>@x</c> carries a stack of <paramref name="stackChars"/> characters.</summary>
    private static byte[] ClefWithExceptions(int events, int stackChars)
    {
        string stack = new('s', stackChars);
        var buf = new ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(3);
            w.Write("@mt"); w.Write("failed {N}");
            w.Write("@t");  w.Write(DateTime.UtcNow.ToString("O"));
            w.Write("@x");
            w.WriteMapHeader(3);
            w.Write("type"); w.Write("System.InvalidOperationException");
            w.Write("msg");  w.Write("failed " + i);
            w.Write("stk");  w.Write(stack);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
