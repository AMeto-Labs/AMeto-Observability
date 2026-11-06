using System.Buffers.Binary;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Tracing;
using Google.Protobuf;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// #138: a trace batch the span ring had no room for was answered <c>200 {"ingested":0,"dropped":N}</c>
/// over HTTP and OK with every span rejected over gRPC — OTLP's partial success, which exporters do not
/// retry: the batch was acknowledged and gone, and a drainer holding a tail the store would not take
/// (<c>SpanDrainerRetryTests</c>) only moved the loss to the receiver. Nothing of such a batch was taken,
/// so it is now answered 503 with <c>Retry-After</c> / UNAVAILABLE, which exporters retry and which
/// duplicates nothing — the log road's rule for a store with no room. A batch the ring took part of
/// keeps the partial-success answer.
/// </summary>
public sealed class TraceRingFullAnswerTests : IClassFixture<TraceRingFullAnswerTests.RefusingSinkFactory>
{
    /// <summary>A span sink that takes the first <see cref="TakeFirst"/> spans of each request and refuses the rest.</summary>
    public sealed class RefusingSink : ISpanSink
    {
        public volatile int TakeFirst;
        private int _offered;

        public int InternService(ReadOnlySpan<byte> serviceUtf8) => 0;

        public bool TryIngestRaw(TraceId traceId, SpanId spanId, SpanId parentSpanId, long startTimeUnixNano,
            long durationNanos, ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8,
            SpanKind kind, SpanStatusCode status, short httpStatusCode, ReadOnlySpan<byte> msgpackAttributes)
            => Interlocked.Increment(ref _offered) <= TakeFirst;

        public void EndBatch() => Interlocked.Exchange(ref _offered, 0);
    }

    public sealed class RefusingSinkFactory : AmetoWebAppFactory
    {
        public RefusingSink Sink { get; } = new();

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureTestServices(services => services.AddSingleton<ISpanSink>(Sink));
        }
    }

    private readonly RefusingSinkFactory _factory;
    private readonly HttpClient          _client;

    public TraceRingFullAnswerTests(RefusingSinkFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-Seq-ApiKey", AmetoWebAppFactory.TestApiKey);
    }

    // ── HTTP ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_batch_the_ring_had_no_room_for_is_503_with_Retry_After()
    {
        _factory.Sink.TakeFirst = 0;

        using var resp = await PostJson(TwoSpansJson());

        Assert.Equal(HttpStatusCode.ServiceUnavailable, resp.StatusCode);
        Assert.Equal("1", resp.Headers.RetryAfter?.Delta?.TotalSeconds.ToString() ?? resp.Headers.GetValues("Retry-After").Single());
        var body = JsonDocument.Parse(await resp.Content.ReadAsStringAsync()).RootElement;
        Assert.Equal("the trace ingest buffer is full and had no room for this batch; retry", body.GetProperty("message").GetString());
    }

    [Fact]
    public async Task A_batch_the_ring_took_part_of_is_200_with_the_rest_dropped()
    {
        _factory.Sink.TakeFirst = 1;

        using var resp = await PostJson(TwoSpansJson());

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("{\"ingested\":1,\"dropped\":1}", await resp.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task A_batch_the_ring_took_whole_is_200()
    {
        _factory.Sink.TakeFirst = int.MaxValue;

        using var resp = await PostJson(TwoSpansJson());

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("{\"ingested\":2,\"dropped\":0}", await resp.Content.ReadAsStringAsync());
    }

    // ── gRPC ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Over_grpc_a_batch_the_ring_had_no_room_for_is_UNAVAILABLE()
    {
        _factory.Sink.TakeFirst = 0;
        var call = GrpcCall(Frame(TwoSpansProto()));

        await Export(call);

        Assert.Equal("14", call.Response.Headers["grpc-status"].ToString());
        Assert.Equal(OtlpGrpcEndpointMapper.TraceBufferFullMessage, call.Response.Headers["grpc-message"].ToString());
    }

    [Fact]
    public async Task Over_grpc_a_batch_the_ring_took_part_of_is_OK_with_a_partial_success()
    {
        _factory.Sink.TakeFirst = 1;
        var call = GrpcCall(Frame(TwoSpansProto()));

        await Export(call);

        Assert.Equal("0", call.Response.Headers["grpc-status"].ToString());
        Assert.Equal(OtlpGrpcFraming.Frame(OtlpGrpcFraming.ExportResponse(1, "the ingest buffer was full")),
                     ((MemoryStream)call.Response.Body).ToArray());
    }

    // ── Harness ───────────────────────────────────────────────────────────────

    private Task<HttpResponseMessage> PostJson(string json)
    {
        var content = new StringContent(json, Encoding.UTF8);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        return _client.PostAsync("/v1/traces", content);
    }

    private static string TwoSpansJson() =>
        "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"ring-full\"}}]}," +
        "\"scopeSpans\":[{\"spans\":[" +
        Span("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331") + "," +
        Span("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203332") +
        "]}]}]}";

    private static string Span(string traceId, string spanId) =>
        "{\"traceId\":\"" + traceId + "\",\"spanId\":\"" + spanId + "\",\"name\":\"GET /ring\",\"kind\":2," +
        "\"startTimeUnixNano\":\"1785300000000000000\",\"endTimeUnixNano\":\"1785300000012000000\"}";

    private static byte[] TwoSpansProto() => Msg(c => Sub(c, 1, Msg(rs =>
    {
        Sub(rs, 1, Msg(res => Sub(res, 1, Msg(kv =>
        {
            kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("service.name");
            Sub(kv, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("ring-full"); }));
        }))));
        Sub(rs, 2, Msg(ss =>
        {
            Sub(ss, 2, ProtoSpan("b7ad6b7169203331"));
            Sub(ss, 2, ProtoSpan("b7ad6b7169203332"));
        }));
    })));

    private static byte[] ProtoSpan(string spanId) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
        c.WriteTag(2, WireFormat.WireType.LengthDelimited); c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString(spanId)));
        c.WriteTag(5, WireFormat.WireType.LengthDelimited); c.WriteString("GET /ring");
        c.WriteTag(6, WireFormat.WireType.Varint);          c.WriteEnum(2);
        c.WriteTag(7, WireFormat.WireType.Fixed64);         c.WriteFixed64(1_785_300_000_000_000_000UL);
        c.WriteTag(8, WireFormat.WireType.Fixed64);         c.WriteFixed64(1_785_300_000_012_000_000UL);
    });

    private static byte[] Msg(Action<CodedOutputStream> body)
    {
        using var ms = new MemoryStream();
        var o = new CodedOutputStream(ms);
        body(o);
        o.Flush();
        return ms.ToArray();
    }

    private static void Sub(CodedOutputStream o, int field, byte[] payload)
    {
        o.WriteTag(field, WireFormat.WireType.LengthDelimited);
        o.WriteBytes(ByteString.CopyFrom(payload));
    }

    private static byte[] Frame(byte[] payload)
    {
        var framed = new byte[OtlpGrpcFraming.HeaderBytes + payload.Length];
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
        ctx.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
        ctx.Response.Body         = new MemoryStream();
        return ctx;
    }

    private static Task Export(DefaultHttpContext call)
    {
        var gate        = new OtlpInflateGate(1, TimeSpan.Zero);
        var noLog       = new OtlpGzipTooLargeLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        var noMemoryLog = new OtlpOutOfMemoryLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        return OtlpGrpcEndpointMapper.HandleAsync(call, ApiKeyPermissions.Traces, gate, noLog, noMemoryLog,
            static (c, msg) => OtlpGrpcEndpointMapper.IngestTraces(msg.AsSpan(), c.RequestServices.GetRequiredService<ISpanSink>()));
    }
}
