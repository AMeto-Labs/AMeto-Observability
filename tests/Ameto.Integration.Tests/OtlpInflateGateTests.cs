using System.Buffers.Binary;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Otel;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// <see cref="OtlpInflateGate"/>: at most N gzip bodies held inflated at once, across both OTLP
/// receivers — PR #96 review, finding 1.
///
/// <para>Each inflate was bounded and nothing bounded how many ran together: ~30 concurrent
/// ~12 KB bombs, with any ingest key, held 8–12 MiB each and reached the stand's 384 MB heap
/// limit. What is pinned here: the gate's size is the rule argued in its summary, computed from
/// the real memory model; with every slot taken a compressed body waits briefly and is then
/// refused with the answer OTLP exporters RETRY (HTTP 503 + <c>Retry-After</c>, gRPC
/// <c>UNAVAILABLE</c>), releasing no slot it did not take and keeping no buffer; an uncompressed
/// body never touches the gate; and a freed slot lets the same body through and comes back.</para>
///
/// <para>The slots are held by the test itself — the seam is the gate, registered small (two
/// slots, 50 ms of patience) in this host in place of the process-sized one.</para>
/// </summary>
public sealed class OtlpInflateGateTests : IClassFixture<OtlpInflateGateTests.Factory>
{
    private const int Slots = 2;

    public sealed class Factory : AmetoWebAppFactory
    {
        internal OtlpInflateGate Gate { get; } = new(Slots, TimeSpan.FromMilliseconds(50));

        protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureServices(services => services.AddSingleton(Gate));
        }
    }

    private readonly Factory    _factory;
    private readonly HttpClient _client;

    public OtlpInflateGateTests(Factory factory)
    {
        _factory = factory;
        factory.Server.PreserveExecutionContext = true;                  // the pool ledger is an AsyncLocal
        _client = factory.CreateClient();
    }

    // ── The size ──────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>min(cores, IngestBufferBytes / MaxOtlpBatchBytes)</c>, at least one — with the budget
    /// taken from <see cref="MemoryBudgets.Derive"/>, not restated. The stand: a 512 MiB container,
    /// the runtime's 75 % heap limit (384 MiB), 5 % of it for request bodies ≈ 20.1 MB, over the
    /// 8 MiB default = 2 — however many cores the host lends it.
    /// </summary>
    [Theory]
    [InlineData(512L * 1024 * 1024 * 3 / 4, 512L * 1024 * 1024, 8 * 1024 * 1024, 16, 2)]   // the stand
    [InlineData(512L * 1024 * 1024 * 3 / 4, 512L * 1024 * 1024, 8 * 1024 * 1024, 1,  1)]   // the stand on one core
    [InlineData(64L  << 30,                 64L  << 30,         8 * 1024 * 1024, 32, 16)]  // the 128 MiB cap binds
    [InlineData(64L  << 30,                 64L  << 30,         8 * 1024 * 1024, 4,  4)]   // the cores bind
    [InlineData(512L * 1024 * 1024 * 3 / 4, 512L * 1024 * 1024, 64 * 1024 * 1024, 16, 1)] // a limit past the budget: still one
    public void The_gate_is_as_wide_as_the_cores_and_the_body_budget_allow(
        long managedLimit, long physicalLimit, int maxOtlpBatchBytes, int cores, int expected)
    {
        long budget = MemoryBudgets.Derive(managedLimit, physicalLimit).IngestBufferBytes;
        Assert.Equal(expected, OtlpInflateGate.CapacityFor(budget, maxOtlpBatchBytes, cores));
    }

    [Fact]
    public async Task A_full_gate_says_no_after_its_patience_and_a_freed_slot_says_yes()
    {
        var gate = new OtlpInflateGate(2, TimeSpan.Zero);
        Assert.True(await gate.TryEnterAsync(default));
        Assert.True(await gate.TryEnterAsync(default));
        Assert.False(await gate.TryEnterAsync(default));
        Assert.Equal(0, gate.Available);

        gate.Exit();
        Assert.True(await gate.TryEnterAsync(default));
    }

    // ── OTLP/HTTP: 503 + Retry-After ──────────────────────────────────────────

    [Theory]
    [InlineData("/v1/logs",    true)]
    [InlineData("/v1/traces",  false)]
    [InlineData("/v1/metrics", true)]
    public async Task With_every_slot_taken_a_gzip_body_is_503_and_takes_nothing(string route, bool protobuf)
    {
        var gate = _factory.Gate;
        Assert.Equal(Slots, gate.Available);
        byte[] message = EmptyExport(route, protobuf);

        for (int i = 0; i < Slots; i++) Assert.True(await gate.TryEnterAsync(default));
        try
        {
            using (var ledger = IngestBufferPoolLedger.Open())
            {
                using var refused = await PostAsync(route, OtlpGzipTests.Gzip(message), protobuf, gzip: true);

                Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
                Assert.Equal(TimeSpan.FromSeconds(1), refused.Headers.RetryAfter?.Delta);
                Assert.Equal("the server is inflating as many gzip batches as it can hold; retry",
                             StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf));
                Assert.Equal(0, gate.Available);                              // released nothing it did not take
                ledger.AssertEveryBufferCameBackOnce(minRents: 1);            // the compressed body, back
                Assert.Equal(1, ledger.Rents);                                // and no inflate buffer
            }

            // Uncompressed bodies never touch the gate: full, it still lets them through.
            using var plain = await PostAsync(route, message, protobuf, gzip: false);
            Assert.Equal(HttpStatusCode.OK, plain.StatusCode);
        }
        finally
        {
            for (int i = 0; i < Slots; i++) gate.Exit();
        }

        // A free slot: the same compressed body goes through — and gives its slot back.
        using var accepted = await PostAsync(route, OtlpGzipTests.Gzip(message), protobuf, gzip: true);
        Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
        Assert.Equal(Slots, gate.Available);
    }

    [Fact]
    public async Task Every_way_out_of_a_gzip_request_gives_its_slot_back()
    {
        // Accepted, inflated past the limit, not gzip, and a parse failure after a good inflate:
        // four ways a slot is taken, four ways it has to come back.
        var gate = _factory.Gate;
        byte[] logs = EmptyExport("/v1/logs", protobuf: true);

        (byte[] Body, HttpStatusCode Expected)[] cases =
        [
            (OtlpGzipTests.Gzip(logs),                                   HttpStatusCode.OK),
            (GzipBomb.Payload,                                           HttpStatusCode.RequestEntityTooLarge),
            ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], HttpStatusCode.BadRequest),
            (OtlpGzipTests.Gzip([0x0A, 0x7F]),                           HttpStatusCode.BadRequest),   // inflates; field 1 claims 127 bytes it lacks
        ];
        foreach (var (body, expected) in cases)
        {
            using var response = await PostAsync("/v1/logs", body, protobuf: true, gzip: true);
            Assert.Equal(expected, response.StatusCode);
            Assert.Equal(Slots, gate.Available);
        }
    }

    /// <summary>
    /// PR #96 review, finding 2, through the receiver: an inflate that runs out of memory answers
    /// the retryable 503 — not the 400 the grow used to earn, nor the 500 the first rent used to
    /// escape as — and still gives back its slot and every buffer. Rent 1 is the compressed body;
    /// rent 2 the inflate's first buffer; rent 3, with the trailer lying low, its grow.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task An_inflate_that_runs_out_of_memory_is_503_not_400_and_gives_everything_back(int failingRent)
    {
        byte[] body = (byte[])GzipBomb.Payload.Clone();
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(body.Length - 4), 1);

        using var ledger = IngestBufferPoolLedger.Open();
        ledger.FailRent(failingRent, new OutOfMemoryException());
        using var response = await PostAsync("/v1/logs", body, protobuf: true, gzip: true);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(TimeSpan.FromSeconds(1), response.Headers.RetryAfter?.Delta);
        Assert.Equal("the server ran short of memory inflating this batch; retry",
                     StatusMessage(await response.Content.ReadAsByteArrayAsync(), protobuf: true));
        Assert.Equal(Slots, _factory.Gate.Available);
        ledger.AssertEveryBufferCameBackOnce(minRents: failingRent - 1);
    }

    // ── OTLP/gRPC: UNAVAILABLE ────────────────────────────────────────────────

    /// <summary>
    /// The gRPC receiver's side of the same gate, driven over a plain context: TestServer cannot
    /// honestly carry gRPC (see <see cref="OtlpGrpcFramingTests"/>), and what is under test is the
    /// status the handler chooses, which with nothing written yet goes out in the headers.
    /// </summary>
    [Fact]
    public async Task With_every_slot_taken_a_compressed_grpc_frame_is_UNAVAILABLE_and_an_identity_one_is_served()
    {
        var gate = new OtlpInflateGate(1, TimeSpan.Zero);
        byte[] message = EmptyExport("/v1/logs", protobuf: true);
        int decoded = 0;
        Func<HttpContext, ArraySegment<byte>, (bool, int, string?)> decode = (_, _) => { decoded++; return (true, 0, null); };

        Assert.True(await gate.TryEnterAsync(default));
        var full = GrpcCall(Frame(OtlpGzipTests.Gzip(message), compressed: true));
        await OtlpGrpcEndpointMapper.HandleAsync(full, ApiKeyPermissions.Logs, gate, decode);

        Assert.Equal("14", full.Response.Headers["grpc-status"].ToString());
        Assert.Equal(OtlpGrpcEndpointMapper.GateFullMessage, full.Response.Headers["grpc-message"].ToString());
        Assert.Equal(0, decoded);
        Assert.Equal(0, gate.Available);                                       // released nothing it did not take

        var identity = GrpcCall(Frame(message, compressed: false));
        await OtlpGrpcEndpointMapper.HandleAsync(identity, ApiKeyPermissions.Logs, gate, decode);
        Assert.Equal("0", identity.Response.Headers["grpc-status"].ToString());
        Assert.Equal(1, decoded);

        gate.Exit();
        var freed = GrpcCall(Frame(OtlpGzipTests.Gzip(message), compressed: true));
        await OtlpGrpcEndpointMapper.HandleAsync(freed, ApiKeyPermissions.Logs, gate, decode);
        Assert.Equal("0", freed.Response.Headers["grpc-status"].ToString());
        Assert.Equal(2, decoded);
        Assert.Equal(1, gate.Available);                                       // and the slot came back
    }

    [Fact]
    public async Task A_grpc_inflate_that_runs_out_of_memory_is_UNAVAILABLE_not_INVALID_ARGUMENT()
    {
        // Rent 1 is the framed body, rent 2 the inflate's first buffer. It used to escape the
        // handler (the first rent was outside the inflate's try); a failed grow was INVALID_ARGUMENT,
        // which an exporter never retries.
        var gate = new OtlpInflateGate(1, TimeSpan.Zero);
        var call = GrpcCall(Frame(OtlpGzipTests.Gzip(EmptyExport("/v1/logs", protobuf: true)), compressed: true));

        using var ledger = IngestBufferPoolLedger.Open();
        ledger.FailRent(2, new OutOfMemoryException());
        await OtlpGrpcEndpointMapper.HandleAsync(call, ApiKeyPermissions.Logs, gate,
            static (_, _) => throw new InvalidOperationException("nothing should reach the decoder"));

        Assert.Equal("14", call.Response.Headers["grpc-status"].ToString());
        Assert.Equal(OtlpGrpcEndpointMapper.MemoryShortMessage, call.Response.Headers["grpc-message"].ToString());
        Assert.Equal(1, gate.Available);
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    // ── Harness ───────────────────────────────────────────────────────────────

    /// <summary>
    /// A valid export carrying nothing, for the route's signal: one empty resource entry in protobuf
    /// (field 1, length 0 — not zero bytes, which gzip compresses to NO bytes and so to no inflate at
    /// all), the empty resource list in JSON.
    /// </summary>
    private static byte[] EmptyExport(string route, bool protobuf)
    {
        if (protobuf) return [0x0A, 0x00];
        string field = route.EndsWith("logs", StringComparison.Ordinal) ? "resourceLogs"
                     : route.EndsWith("traces", StringComparison.Ordinal) ? "resourceSpans"
                     : "resourceMetrics";
        return Encoding.UTF8.GetBytes("{\"" + field + "\":[]}");
    }

    private Task<HttpResponseMessage> PostAsync(string route, byte[] body, bool protobuf, bool gzip)
    {
        var content = new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue(protobuf ? "application/x-protobuf" : "application/json");
        if (gzip) content.Headers.ContentEncoding.Add("gzip");
        return _client.PostAsync(route, content);
    }

    /// <summary>The <c>message</c> of an OTLP Status body, in either encoding.</summary>
    private static string StatusMessage(byte[] body, bool protobuf)
    {
        if (!protobuf) return JsonDocument.Parse(body).RootElement.GetProperty("message").GetString()!;
        Assert.Equal(0x12, body[0]);
        Assert.Equal(body.Length - 2, (int)body[1]);
        return Encoding.UTF8.GetString(body, 2, body.Length - 2);
    }

    private static byte[] Frame(byte[] payload, bool compressed)
    {
        var framed = new byte[OtlpGrpcFraming.HeaderBytes + payload.Length];
        framed[0] = compressed ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)payload.Length);
        payload.CopyTo(framed.AsSpan(OtlpGrpcFraming.HeaderBytes));
        return framed;
    }

    private DefaultHttpContext GrpcCall(byte[] framed)
    {
        var ctx = new DefaultHttpContext { RequestServices = _factory.Services };
        ctx.Request.Method        = "POST";
        ctx.Request.Protocol      = "HTTP/2";
        ctx.Request.ContentType   = "application/grpc";
        ctx.Request.ContentLength = framed.Length;
        ctx.Request.Body          = new MemoryStream(framed);
        ctx.Request.Headers["X-Seq-ApiKey"]  = AmetoWebAppFactory.TestApiKey;
        ctx.Request.Headers["grpc-encoding"] = "gzip";
        ctx.Response.Body = new MemoryStream();
        return ctx;
    }
}
