using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Otel;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// A LOG BATCH IS WEIGHED BEFORE IT IS DECODED, and the batches held decoded at once are bounded.
///
/// <para>A log batch is decoded whole before any of it is written — the request is answered only
/// once its events are in the store — and a small record decodes to many times its wire size: a
/// staged record costs ~80 B besides its properties, an OTLP record's properties begin with a copy
/// of its resource's attributes, and an empty LogRecord is two bytes. An 8 MiB body of them decoded
/// to ~4 M records, ~540 MB, past a 512 MB container's heap, with nothing to stop it but the byte
/// limit. Now every log receiver weighs the body first, by a walk that allocates nothing, refuses
/// one heavier than <c>Ingestion.MaxDecodedLogBatchBytes</c> with 413 (gRPC: RESOURCE_EXHAUSTED),
/// nothing of it ingested, and reserves the weight of one it takes against twice that for all the
/// batches held at once — past it, 503 with Retry-After (gRPC: UNAVAILABLE), nothing decoded.</para>
///
/// <para>This host runs at 1 MB a batch, 2 MB held at once.</para>
/// </summary>
public sealed class LogBatchDecodedBudgetTests : IClassFixture<LogBatchDecodedBudgetTests.Factory>
{
    private const long Limit = 1_000_000;

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { MaxDecodedLogBatchBytes = Limit };
    }

    private static readonly OtlpGzipTooLargeLog NoLog       = new(NullLogger.Instance, TimeProvider.System);
    private static readonly OtlpOutOfMemoryLog  NoMemoryLog = new(NullLogger.Instance, TimeProvider.System);

    private readonly Factory           _factory;
    private readonly HttpClient        _client;
    private readonly IngestionEndpoint _endpoint;

    public LogBatchDecodedBudgetTests(Factory factory)
    {
        _factory  = factory;
        _client   = factory.CreateClient();
        _endpoint = factory.Services.GetRequiredService<IngestionEndpoint>();
    }

    // ── The weight ────────────────────────────────────────────────────────────

    /// <summary>
    /// The review's case, weighed: an 8 MiB body of empty LogRecords is ~4.2 M records, ~512 MiB
    /// decoded — and the walk that says so allocates nothing.
    /// </summary>
    [Fact]
    public void An_8_MiB_body_of_empty_log_records_weighs_half_a_gigabyte_and_the_walk_allocates_nothing()
    {
        byte[] body = EmptyRecords(MaxEmptyRecords);
        Assert.True(body.Length <= new IngestionOptions().MaxOtlpBatchBytes);
        OtlpLogBatchWeight.WeighProto(body);   // warm

        long before = GC.GetAllocatedBytesForCurrentThread();
        long weight = OtlpLogBatchWeight.WeighProto(body);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal((long)MaxEmptyRecords * IngestionOptions.DecodedLogRecordBytes, weight);
        Assert.True(weight > 500L * 1024 * 1024);
        Assert.True(allocated < 1024, $"weighing allocated {allocated:N0} B");
    }

    /// <summary>A resource's attributes count once per record under it: they are copied into each.</summary>
    [Fact]
    public void A_resources_attributes_weigh_once_per_record_in_protobuf_and_json()
    {
        byte[] attribute = Attribute("k8s.pod.name", new string('p', 200));
        byte[] record    = Record("hello {n}");
        byte[] proto     = Request(Block(resourceAttributes: [attribute], records: [record, record, record]));
        long   expected  = 3 * (IngestionOptions.DecodedLogRecordBytes + attribute.Length) + 3 * record.Length;
        Assert.Equal(expected, OtlpLogBatchWeight.WeighProto(proto));

        string json = """
            {"resourceLogs":[{"resource":{"attributes":[{"key":"k8s.pod.name","value":{"stringValue":"PPP"}}]},
              "scopeLogs":[{"logRecords":[{"body":{"stringValue":"a"}},{"body":{"stringValue":"b"}}]}]}]}
            """.Replace("PPP", new string('p', 200));
        int attributesBytes = json.IndexOf("]},", StringComparison.Ordinal) + 1 - json.IndexOf("[{\"key\"", StringComparison.Ordinal);
        long jsonWeight = OtlpLogBatchWeight.WeighJson(Encoding.UTF8.GetBytes(json));
        Assert.Equal(2 * (IngestionOptions.DecodedLogRecordBytes + attributesBytes) + 2 * "{\"body\":{\"stringValue\":\"a\"}}".Length,
                     jsonWeight);
    }

    // ── One batch over the limit: 413, nothing ingested ───────────────────────

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task An_otlp_log_batch_heavier_than_the_limit_is_refused_413_with_nothing_ingested(bool protobuf)
    {
        long accepted = _endpoint.AcceptedTotal;
        byte[] body   = protobuf ? EmptyRecords(20_000) : JsonRecords(10_000);   // ~2.6 MB and ~1.3 MB decoded

        using var response = await PostOtlpAsync(body, protobuf);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Contains("split it", await response.Content.ReadAsStringAsync());
        Assert.Equal(accepted, _endpoint.AcceptedTotal);
        Assert.Equal(0, _endpoint.DecodedBytesInFlight);
    }

    [Fact]
    public async Task A_clef_batch_heavier_than_the_limit_is_refused_413_with_nothing_ingested()
    {
        long accepted = _endpoint.AcceptedTotal;

        using var response = await PostClefAsync(ClefEmptyEvents(20_000));   // a 20 KB body, ~2.6 MB decoded

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Equal(accepted, _endpoint.AcceptedTotal);
    }

    [Fact]
    public async Task A_grpc_log_batch_heavier_than_the_limit_is_resource_exhausted_with_nothing_ingested()
    {
        long accepted = _endpoint.AcceptedTotal;
        var  call     = GrpcCall(Frame(EmptyRecords(20_000)));

        await OtlpGrpcEndpointMapper.HandleLogsAsync(call, new OtlpInflateGate(1, TimeSpan.Zero), NoLog, NoMemoryLog);

        Assert.Equal("8", call.Response.Headers["grpc-status"].ToString());   // RESOURCE_EXHAUSTED
        Assert.Equal(accepted, _endpoint.AcceptedTotal);
    }

    /// <summary>A batch under the limit goes through as it did.</summary>
    [Fact]
    public async Task A_log_batch_under_the_limit_is_taken()
    {
        using var response = await PostOtlpAsync(EmptyRecords(1_000), protobuf: true);   // ~128 KB decoded

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(1_000, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(0, _endpoint.DecodedBytesInFlight);
    }

    // ── The batches held at once: 503, nothing decoded ────────────────────────

    /// <summary>
    /// The batches held decoded at once are bounded at twice the per-batch limit: the store behind,
    /// requests waiting out BackPressureWait with their batches decoded, a new one is asked to retry
    /// before it decodes anything. Once the others are done it is taken. Over HTTP, CLEF and gRPC.
    /// </summary>
    [Fact]
    public async Task A_batch_that_would_take_the_held_total_past_the_bound_is_asked_to_retry_until_room_comes()
    {
        long accepted = _endpoint.AcceptedTotal;
        var  held     = _endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: 1_500_000);   // what others hold
        Assert.NotNull(held);
        try
        {
            byte[] otlp = EmptyRecords(5_000);   // 640 KB decoded: under the limit, over what is left
            using (var refused = await PostOtlpAsync(otlp, protobuf: true))
            {
                Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
                Assert.Equal(TimeSpan.FromSeconds(1), refused.Headers.RetryAfter?.Delta);
                Assert.Contains("retry", await refused.Content.ReadAsStringAsync());
            }

            using (var refused = await PostClefAsync(ClefEmptyEvents(5_000)))
            {
                Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
                Assert.Equal(TimeSpan.FromSeconds(1), refused.Headers.RetryAfter?.Delta);
                using var doc = JsonDocument.Parse(await refused.Content.ReadAsStringAsync());
                Assert.Equal(0,     doc.RootElement.GetProperty("ingested").GetInt32());
                Assert.Equal(5_000, doc.RootElement.GetProperty("dropped").GetInt32());
            }

            var call = GrpcCall(Frame(otlp));
            await OtlpGrpcEndpointMapper.HandleLogsAsync(call, new OtlpInflateGate(1, TimeSpan.Zero), NoLog, NoMemoryLog);
            Assert.Equal("14", call.Response.Headers["grpc-status"].ToString());   // UNAVAILABLE

            Assert.Equal(accepted, _endpoint.AcceptedTotal);
            Assert.Equal(1_500_000, _endpoint.DecodedBytesInFlight);   // the refusals reserved nothing

            held!.Dispose();
            held = null;
            using var taken = await PostOtlpAsync(otlp, protobuf: true);
            Assert.Equal(HttpStatusCode.OK, taken.StatusCode);
            Assert.Equal(accepted + 5_000, _endpoint.AcceptedTotal);
            Assert.Equal(0, _endpoint.DecodedBytesInFlight);
        }
        finally { held?.Dispose(); }
    }

    // ── Bodies ────────────────────────────────────────────────────────────────

    /// <summary>The most empty records an 8 MiB OTLP body holds, in one resource and one scope.</summary>
    internal const int MaxEmptyRecords = (8 * 1024 * 1024 - 16) / 2;

    /// <summary>An ExportLogsServiceRequest of one resource and one scope holding <paramref name="records"/> empty LogRecords, two bytes each.</summary>
    internal static byte[] EmptyRecords(int records)
    {
        int scopeLen    = 2 * records;
        int resourceLen = 1 + VarintLength(scopeLen) + scopeLen;
        var body        = new byte[1 + VarintLength(resourceLen) + resourceLen];
        int at = 0;
        body[at++] = 0x0A; at += WriteVarint(body.AsSpan(at), resourceLen);   // resource_logs (1)
        body[at++] = 0x12; at += WriteVarint(body.AsSpan(at), scopeLen);      // scope_logs (2)
        for (int i = 0; i < records; i++)
        {
            body[at++] = 0x12;                                                // log_records (2)
            body[at++] = 0x00;                                                // of length 0
        }
        return body;
    }

    private static byte[] JsonRecords(int records)
    {
        var sb = new StringBuilder("{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[");
        for (int i = 0; i < records; i++) sb.Append(i == 0 ? "{}" : ",{}");
        sb.Append("]}]}]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private static byte[] ClefEmptyEvents(int events)
    {
        var buf = new ArrayBufferWriter<byte>(events + 8);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++) w.WriteMapHeader(0);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static byte[] Attribute(string key, string value) =>
        LengthDelimited(1, Encoding.UTF8.GetBytes(key))
            .Concat(LengthDelimited(2, LengthDelimited(1, Encoding.UTF8.GetBytes(value)))).ToArray();

    private static byte[] Record(string body) => LengthDelimited(5, LengthDelimited(1, Encoding.UTF8.GetBytes(body)));

    private static byte[] Block(byte[][] resourceAttributes, byte[][] records)
    {
        var resource = resourceAttributes.SelectMany(a => LengthDelimited(1, a)).ToArray();
        var scope    = records.SelectMany(r => LengthDelimited(2, r)).ToArray();
        return LengthDelimited(1, resource).Concat(LengthDelimited(2, scope)).ToArray();
    }

    private static byte[] Request(byte[] block) => LengthDelimited(1, block);

    private static byte[] LengthDelimited(int field, byte[] payload)
    {
        var out_ = new byte[1 + VarintLength(payload.Length) + payload.Length];
        out_[0] = (byte)(field << 3 | 2);
        int at = 1 + WriteVarint(out_.AsSpan(1), payload.Length);
        payload.CopyTo(out_, at);
        return out_;
    }

    private static int VarintLength(int value)
    {
        int n = 1;
        while ((value >>= 7) != 0) n++;
        return n;
    }

    private static int WriteVarint(Span<byte> to, int value)
    {
        int n = 0;
        uint v = (uint)value;
        while (v >= 0x80) { to[n++] = (byte)(v | 0x80); v >>= 7; }
        to[n++] = (byte)v;
        return n;
    }

    // ── Calls ─────────────────────────────────────────────────────────────────

    private Task<HttpResponseMessage> PostOtlpAsync(byte[] body, bool protobuf)
    {
        var content = new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue(protobuf ? "application/x-protobuf" : "application/json");
        return _client.PostAsync("/v1/logs", content);
    }

    private Task<HttpResponseMessage> PostClefAsync(byte[] body)
    {
        var content = new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return _client.PostAsync("/api/events", content);
    }

    private static byte[] Frame(byte[] message)
    {
        var framed = new byte[OtlpGrpcFraming.HeaderBytes + message.Length];
        BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)message.Length);
        message.CopyTo(framed.AsSpan(OtlpGrpcFraming.HeaderBytes));
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
        ctx.Response.Body = new MemoryStream();
        return ctx;
    }
}

/// <summary>
/// The review's deterministic case, end to end on a host with the DEFAULT limit: one 8 MiB OTLP body
/// of empty LogRecords — ~4.2 M records, ~512 MiB decoded, more than the default allows anywhere — is
/// refused 413 before a record is staged. It was decoded whole, and on a 512 MB container that ran
/// the heap out.
/// </summary>
public sealed class LogBatchDefaultBudgetTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    private readonly HttpClient         _client;

    public LogBatchDefaultBudgetTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
    }

    [Fact]
    public async Task An_8_MiB_otlp_body_of_empty_log_records_is_refused_413_before_it_is_decoded()
    {
        var  endpoint = _factory.Services.GetRequiredService<IngestionEndpoint>();
        long accepted = endpoint.AcceptedTotal;
        var  content  = new ByteArrayContent(LogBatchDecodedBudgetTests.EmptyRecords(LogBatchDecodedBudgetTests.MaxEmptyRecords));
        content.Headers.ContentType = new MediaTypeHeaderValue("application/x-protobuf");

        using var response = await _client.PostAsync("/v1/logs", content);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Equal(accepted, endpoint.AcceptedTotal);
        Assert.Equal(0, endpoint.DecodedBytesInFlight);
    }
}
