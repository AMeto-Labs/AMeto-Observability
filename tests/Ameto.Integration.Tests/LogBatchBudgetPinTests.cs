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

    // ── L-13: a record refused unassembled ────────────────────────────────────

    /// <summary>
    /// Round-5 review L-13. A record refused unassembled has its template decoded for its drop marker
    /// (which keeps 256 units of it) and its drop line (120): the whole template, through
    /// Encoding.UTF8's replacement fallback — ~35 B for each byte that is not UTF-8, 140 MB for one record
    /// of a 4 MB body. Only the start the marker keeps is decoded now, U+FFFD for what is not UTF-8.
    /// </summary>
    [Fact]
    public void A_record_refused_unassembled_decodes_only_the_start_of_its_template()
    {
        var endpoint = Endpoint(limit: 64L * 1024 * 1024, inFlight: 64L * 1024 * 1024);
        using var batch = endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: 1024 * 1024)!;
        byte[] template = new byte[4_000_000];
        template.AsSpan().Fill(0xFF);
        long ts = DateTimeOffset.UtcNow.UtcTicks;
        batch.TryIngestOversized(ts, (byte)LogLevel.Information, template, 1_000_000, 0, 0, 0, default, -1);   // warm

        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.False(batch.TryIngestOversized(ts, (byte)LogLevel.Information, template, 1_000_000, 0, 0, 0, default, -1));
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < 64 * 1024, $"refusing a record of a 4 MB template that is not UTF-8 allocated {allocated:N0} B");
        Assert.Equal(new string('�', 120) + "…", batch.DropAt(1).Template);   // what its drop line names, as before
    }

    // ── L-7: a refused batch goes back before its answer ──────────────────────

    /// <summary>A response body that notes what was held decoded at once when the answer was first written.</summary>
    private sealed class NotingStream(Func<long> inFlight) : MemoryStream
    {
        public long? AtAnswer;
        private void Note() => AtAnswer ??= inFlight();
        public override void Write(byte[] buffer, int offset, int count) { Note(); base.Write(buffer, offset, count); }
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) { Note(); return base.WriteAsync(buffer, ct); }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) { Note(); return base.WriteAsync(buffer, offset, count, ct); }
        public override Task FlushAsync(CancellationToken ct) { Note(); return base.FlushAsync(ct); }
    }

    /// <summary>A response that notes what was held decoded at once when it was completed (a gRPC answer has no body).</summary>
    private sealed class NotingResponse(Func<long> inFlight) : Microsoft.AspNetCore.Http.Features.IHttpResponseBodyFeature
    {
        private readonly Microsoft.AspNetCore.Http.Features.IHttpResponseBodyFeature _inner =
            new StreamResponseBodyFeature(new MemoryStream());
        public long? AtAnswer;
        public Stream Stream => _inner.Stream;
        public System.IO.Pipelines.PipeWriter Writer => _inner.Writer;
        public void DisableBuffering() => _inner.DisableBuffering();
        public Task StartAsync(CancellationToken ct = default) { AtAnswer ??= inFlight(); return _inner.StartAsync(ct); }
        public Task SendFileAsync(string path, long offset, long? count, CancellationToken ct = default) => _inner.SendFileAsync(path, offset, count, ct);
        public Task CompleteAsync() { AtAnswer ??= inFlight(); return _inner.CompleteAsync(); }
    }

    /// <summary>
    /// Round-5 review L-7, CLEF. A batch refused mid-decode for the budget held its reservation while its
    /// 503 was written: the finally gave it back after the answer, and an await whose registration fails
    /// skips that finally — the reservation lost for good. It goes back before the answer is awaited: when
    /// the answer is written, what is held decoded is the other batches' alone.
    /// </summary>
    [Fact]
    public async Task A_clef_batch_refused_mid_decode_is_given_back_before_its_answer()
    {
        var endpoint = Endpoint(limit: 64L * 1024 * 1024, inFlight: InFlight);
        byte[] body  = ClefWithExceptions(events: 40, stackChars: 6_000);
        long weight  = IngestionEndpoint.WeighClef(body, out _);
        using var others = endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: InFlight - weight - 16 * 1024)!;

        var answer = new NotingStream(() => endpoint.DecodedBytesInFlight);
        var ctx = new DefaultHttpContext();
        ctx.Request.Method        = "POST";
        ctx.Request.ContentType   = "application/octet-stream";
        ctx.Request.ContentLength = body.Length;
        ctx.Request.Body          = new MemoryStream(body);
        ctx.Response.Body         = answer;
        await endpoint.HandleAsync(ctx);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.Equal(others.ReservedBytes, answer.AtAnswer);
    }

    /// <summary>
    /// Round-5 review L-7, gRPC: the same for a logs batch refused mid-decode for the budget — its bytes
    /// that are not UTF-8 decode to three times their weight — answered UNAVAILABLE.
    /// </summary>
    [Fact]
    public async Task A_grpc_batch_refused_mid_decode_is_given_back_before_its_answer()
    {
        var endpoint = _factory.Services.GetRequiredService<IngestionEndpoint>();
        using var others = endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: InFlight - 300 * 1024)!;   // all but 300 KB
        var answer = new NotingResponse(() => endpoint.DecodedBytesInFlight);
        var ctx    = GrpcLogsRequest(_factory.Services, answer);
        await HandleGrpcLogsAsync(ctx);

        Assert.True(ctx.Response.Headers["grpc-status"].ToString() == "14",
                    $"grpc-status {ctx.Response.Headers["grpc-status"]}: {ctx.Response.Headers["grpc-message"]}");
        Assert.Equal(others.ReservedBytes, answer.AtAnswer);
    }

    /// <summary>Round-5 review L-7, gRPC: the same for a logs batch that decodes past its limit, answered RESOURCE_EXHAUSTED.</summary>
    [Fact]
    public async Task A_grpc_batch_too_heavy_mid_decode_is_given_back_before_its_answer()
    {
        var endpoint = Endpoint(limit: 400 * 1024, inFlight: 64L * 1024 * 1024);   // weighs ~205 KB, decodes to ~600 KB
        var answer   = new NotingResponse(() => endpoint.DecodedBytesInFlight);
        var ctx      = GrpcLogsRequest(new WithEndpoint(_factory.Services, endpoint), answer);
        await HandleGrpcLogsAsync(ctx);

        Assert.True(ctx.Response.Headers["grpc-status"].ToString() == "8",
                    $"grpc-status {ctx.Response.Headers["grpc-status"]}: {ctx.Response.Headers["grpc-message"]}");
        Assert.Equal(0, answer.AtAnswer);
    }

    /// <summary>Round-5 review L-7, gRPC: the same for a logs batch whose decode runs out of memory, answered UNAVAILABLE.</summary>
    [Fact]
    public async Task A_grpc_batch_whose_decode_runs_out_of_memory_is_given_back_before_its_answer()
    {
        var endpoint = Endpoint(limit: 64L * 1024 * 1024, inFlight: 64L * 1024 * 1024);
        int staged   = 0;
        endpoint.BeforeStageForTest = () => { if (++staged == 2) throw new OutOfMemoryException("test: the decode ran out of memory"); };
        var answer = new NotingResponse(() => endpoint.DecodedBytesInFlight);
        var ctx    = GrpcLogsRequest(new WithEndpoint(_factory.Services, endpoint), answer);
        await HandleGrpcLogsAsync(ctx);

        Assert.True(ctx.Response.Headers["grpc-status"].ToString() == "14",
                    $"grpc-status {ctx.Response.Headers["grpc-status"]}: {ctx.Response.Headers["grpc-message"]}");
        Assert.Equal(0, answer.AtAnswer);
    }

    /// <summary>The request services, but for <see cref="IngestionEndpoint"/>: one of the test's own.</summary>
    private sealed class WithEndpoint(IServiceProvider inner, IngestionEndpoint endpoint) : IServiceProvider
    {
        public object? GetService(Type type) => type == typeof(IngestionEndpoint) ? endpoint : inner.GetService(type);
    }

    /// <summary>
    /// A gRPC logs request of ten records, each one attribute of 20 KB that is not UTF-8 (~60 KB decoded),
    /// answered into <paramref name="answer"/>.
    /// </summary>
    private static HttpContext GrpcLogsRequest(IServiceProvider services, NotingResponse answer)
    {
        byte[] invalid = new byte[20_000];
        invalid.AsSpan().Fill(0xFF);
        byte[] record  = Proto(6, Concat(Proto(1, Encoding.UTF8.GetBytes("pad")), Proto(2, Proto(1, invalid))));
        byte[] message = Proto(1, Proto(2, Concat(Enumerable.Range(0, 10).Select(_ => Proto(2, record)).ToArray())));
        byte[] framed  = new byte[5 + message.Length];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)message.Length);
        message.CopyTo(framed, 5);

        var ctx = new DefaultHttpContext { RequestServices = services };
        ctx.Request.Protocol      = "HTTP/2";
        ctx.Request.Method        = "POST";
        ctx.Request.ContentType   = "application/grpc";
        ctx.Request.ContentLength = framed.Length;
        ctx.Request.Body          = new MemoryStream(framed);
        ctx.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
        ctx.Features.Set<Microsoft.AspNetCore.Http.Features.IHttpResponseBodyFeature>(answer);
        return ctx;
    }

    private static Task HandleGrpcLogsAsync(HttpContext ctx) =>
        Ameto.Otel.OtlpGrpcEndpointMapper.HandleLogsAsync(ctx, new Ameto.Otel.OtlpInflateGate(1, TimeSpan.Zero),
            new Ameto.Otel.OtlpGzipTooLargeLog(NullLogger.Instance, TimeProvider.System),
            new Ameto.Otel.OtlpOutOfMemoryLog(NullLogger.Instance, TimeProvider.System));

    private static byte[] Concat(params byte[][] parts) => parts.SelectMany(p => p).ToArray();

    /// <summary>A length-delimited protobuf field.</summary>
    private static byte[] Proto(int field, byte[] payload)
    {
        var o = new List<byte> { (byte)(field << 3 | 2) };
        for (ulong v = (ulong)payload.Length; ; v >>= 7)
        {
            if (v < 0x80) { o.Add((byte)v); break; }
            o.Add((byte)(v | 0x80));
        }
        o.AddRange(payload);
        return o.ToArray();
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
