using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// WHAT A LOG BATCH HOLDS DECODED IS COUNTED AS IT IS TAKEN, and the per-request limit is that count,
/// not the weight put on the body before decoding (round-3 review H1, Mb, R1b-4, R1a-2, R1b-3).
///
/// <para>The weight was an estimate, and bodies built to beat it beat it: a resource attribute of
/// bytes that are not UTF-8 is stored as U+FFFD, three bytes for each, in every record under it (a
/// 25 KB body weighed 33 MB and staged ~100 MB, and ran a 384 MiB heap out); past a full intern pool
/// every record's template and service are strings of its own, and a CLEF event's <c>@x</c> a tree of
/// them, none of which the weight saw (~2.8x). Each body below is weighed under this host's limit,
/// decodes past it, and is refused 413 with nothing written.</para>
///
/// <para>This host runs at 4 MiB a batch.</para>
/// </summary>
public sealed class LogBatchHeldMemoryTests : IClassFixture<LogBatchHeldMemoryTests.Factory>
{
    private const long Limit = 4L * 1024 * 1024;

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { MaxDecodedLogBatchBytes = Limit };
    }

    private readonly Factory           _factory;
    private readonly HttpClient        _client;
    private readonly IngestionEndpoint _endpoint;

    public LogBatchHeldMemoryTests(Factory factory)
    {
        _factory  = factory;
        _client   = factory.CreateClient();
        _endpoint = factory.Services.GetRequiredService<IngestionEndpoint>();
    }

    // ── H1: bytes that decode three times their size, in every record ─────────

    [Fact]
    public async Task A_body_whose_resource_decodes_to_three_times_its_bytes_in_every_record_is_refused_413()
    {
        byte[] body   = InvalidUtf8ResourceBody(attributeBytes: 2_000, records: 1_800);   // ~3.6 MB of 0xFF... once
        long   weight = Ameto.Otel.OtlpLogBatchWeight.WeighProto(body);
        Assert.InRange(weight, Limit / 2, Limit);   // weighed under the limit: the weight lets it in
        long accepted = _endpoint.AcceptedTotal;

        using var response = await PostOtlpAsync(body, protobuf: true);   // ~10.8 MB of U+FFFD staged

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Equal(accepted, _endpoint.AcceptedTotal);
        Assert.Equal(0, _endpoint.DecodedBytesInFlight);
    }

    /// <summary>The same body over gRPC: RESOURCE_EXHAUSTED once the decode passes the limit, nothing written.</summary>
    [Fact]
    public async Task The_same_body_over_grpc_is_resource_exhausted_with_nothing_written()
    {
        byte[] body     = InvalidUtf8ResourceBody(attributeBytes: 2_000, records: 1_800);
        long   accepted = _endpoint.AcceptedTotal;
        var    call     = GrpcCall(Frame(body));
        var    noLog    = new Ameto.Otel.OtlpGzipTooLargeLog(NullLogger.Instance, TimeProvider.System);
        var    noMemory = new Ameto.Otel.OtlpOutOfMemoryLog(NullLogger.Instance, TimeProvider.System);

        await Ameto.Otel.OtlpGrpcEndpointMapper.HandleLogsAsync(call, new Ameto.Otel.OtlpInflateGate(1, TimeSpan.Zero), noLog, noMemory);

        Assert.Equal("8", call.Response.Headers["grpc-status"].ToString());   // RESOURCE_EXHAUSTED
        Assert.Equal(accepted, _endpoint.AcceptedTotal);
        Assert.Equal(0, _endpoint.DecodedBytesInFlight);
    }

    // ── Mb: the strings the decode makes past a full pool ─────────────────────

    [Fact]
    public async Task Otlp_records_whose_templates_and_services_are_strings_of_their_own_past_a_full_pool_are_refused_413()
    {
        FillPool();
        string body1k = new('m', 1_024);
        byte[] body   = Request(Block([Attribute("service.name", "svc.past.full")],
                                      Enumerable.Repeat(Record(body1k), 2_500).ToArray()));
        Assert.InRange(Ameto.Otel.OtlpLogBatchWeight.WeighProto(body), Limit / 2, Limit);
        long accepted = _endpoint.AcceptedTotal;

        using var response = await PostOtlpAsync(body, protobuf: true);   // ~5.3 MB of UTF-16 templates

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Equal(accepted, _endpoint.AcceptedTotal);
        Assert.Equal(0, _endpoint.DecodedBytesInFlight);
    }

    [Fact]
    public async Task Clef_events_whose_exceptions_decode_to_trees_of_strings_are_refused_413()
    {
        byte[] body = ClefEventsWithExceptions(events: 1_000, stackChars: 1_000);   // ~3 MB of UTF-8 stacks
        long   weight = IngestionEndpoint.WeighClef(body, out _);
        Assert.InRange(weight, Limit / 2, Limit);
        long accepted = _endpoint.AcceptedTotal;

        using var response = await PostClefAsync(body);   // ~6 MB of UTF-16 exception text, besides the payloads

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Equal(accepted, _endpoint.AcceptedTotal);
        Assert.Equal(0, _endpoint.DecodedBytesInFlight);
    }

    // ── R1b-4: OTLP/JSON is not refused for the length of its text ────────────

    /// <summary>
    /// JSON spells a record out at many times what it decodes to — sixty int attributes are ~2.7 KB of
    /// text and ~0.8 KB of msgpack. Weighed by its text, this batch was refused 413 at half what a
    /// protobuf batch carrying the same records is let through with; it decodes to under a megabyte.
    /// </summary>
    [Fact]
    public async Task An_otlp_json_batch_whose_text_is_over_the_limit_but_decodes_under_it_is_taken()
    {
        byte[] body = JsonRecordsWithIntAttributes(records: 2_000, attributes: 60);
        Assert.True(body.Length > Limit, $"the text is {body.Length:N0} B");
        long accepted = _endpoint.AcceptedTotal;

        using var response = await PostOtlpAsync(body, protobuf: false);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(2_000, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(accepted + 2_000, _endpoint.AcceptedTotal);
    }

    // ── R1a-2: the commit cannot run out of memory adding the drop markers ────

    /// <summary>
    /// The markers go in after the client's events when the batch is committed — outside every
    /// receiver's decode and its out-of-memory answer. A marker that needed the batch to grow there
    /// ran out of memory into a 500 (gRPC UNKNOWN), which exporters drop, for a batch nothing of which
    /// was written. Room for each marker is now kept as it is staged: the commit grows nothing, and
    /// here any growth past the staging throws.
    /// </summary>
    [Fact]
    public async Task Committing_a_batch_adds_its_drop_markers_without_growing_it()
    {
        var  options = _factory.Services.GetRequiredService<ServerOptions>();
        var  props   = new byte[options.Ingestion.MaxEventPayloadBytes + 1];
        using var batch = _endpoint.BeginBatch();
        for (int i = 0; i < 256; i++)   // fills the first array of records exactly
            Assert.True(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information,
                                           "commit probe {N}"u8, [0x80], 0, 0, 0, "commit.probe"u8));
        Assert.False(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information,
                                        "oversized {N}"u8, props, 0, 0, 0, "commit.probe"u8));

        LogWriteBatch.BeforeGrowForTest.Value = () => throw new OutOfMemoryException("test: growing the batch at its commit");
        LogIngestResult result;
        try { result = await batch.CommitAsync(); }
        finally { LogWriteBatch.BeforeGrowForTest.Value = null; }

        Assert.Equal(256, result.Ingested);
        Assert.Equal(1,   result.Dropped);
        Assert.False(result.Busy);
    }

    // ── R1b-3: a huge configured limit does not overflow the bound on what is held ──

    [Fact]
    public void A_huge_decoded_batch_limit_still_lets_two_batches_be_held_at_once()
    {
        var sp       = _factory.Services;
        var options  = new ServerOptions { Ingestion = new IngestionOptions { MaxDecodedLogBatchBytes = long.MaxValue } };
        var endpoint = new IngestionEndpoint(sp.GetRequiredService<StorageEngine>(), sp.GetRequiredService<StringInternPool>(),
                                             options, NullLogger<IngestionEndpoint>.Instance);

        using var first  = endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: 1_000);
        using var second = endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: 1_000);

        Assert.NotNull(first);
        Assert.NotNull(second);   // twice long.MaxValue had wrapped negative: one batch at a time
    }

    // ── Bodies ────────────────────────────────────────────────────────────────

    /// <summary>One resource whose only attribute is <paramref name="attributeBytes"/> bytes of 0xFF, over that many empty records.</summary>
    internal static byte[] InvalidUtf8ResourceBody(int attributeBytes, int records)
    {
        var value = new byte[attributeBytes];
        value.AsSpan().Fill(0xFF);
        byte[] attribute = LengthDelimited(1, Encoding.UTF8.GetBytes("blob")).Concat(LengthDelimited(2, LengthDelimited(1, value))).ToArray();
        return Request(Block([attribute], Enumerable.Repeat(Array.Empty<byte>(), records).ToArray()));
    }

    /// <summary>CLEF events whose <c>@x</c> is three levels deep, each with a stack of <paramref name="stackChars"/> characters.</summary>
    internal static byte[] ClefEventsWithExceptions(int events, int stackChars)
    {
        string stack = new('s', stackChars);
        var buf = new ArrayBufferWriter<byte>(events * (3 * stackChars + 256));
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(3);
            w.Write("@t");  w.Write(DateTime.UtcNow.ToString("O"));
            w.Write("@mt"); w.Write("failed {N}");
            w.Write("@x");
            WriteException(ref w, depth: 3, stack);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();

        static void WriteException(ref MessagePackWriter w, int depth, string stack)
        {
            w.WriteMapHeader(depth > 1 ? 4 : 3);
            w.Write("type"); w.Write("System.InvalidOperationException");
            w.Write("msg");  w.Write("failed at depth " + depth);
            w.Write("stk");  w.Write(stack);
            if (depth > 1)
            {
                w.Write("inner");
                WriteException(ref w, depth - 1, stack);
            }
        }
    }

    internal static byte[] JsonRecordsWithIntAttributes(int records, int attributes)
    {
        var record = new StringBuilder("{\"attributes\":[");
        for (int a = 0; a < attributes; a++)
            record.Append(a == 0 ? "" : ",").Append("{\"key\":\"attribute.").Append(a).Append("\",\"value\":{\"intValue\":\"").Append(a).Append("\"}}");
        record.Append("]}");
        var sb = new StringBuilder("{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[");
        for (int i = 0; i < records; i++) sb.Append(i == 0 ? "" : ",").Append(record);
        sb.Append("]}]}]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private void FillPool()
    {
        var pool = _factory.Services.GetRequiredService<StringInternPool>();
        for (int i = 0; pool.Intern("filler " + i) >= 0; i++) { }
    }

    internal static byte[] Attribute(string key, string value) =>
        LengthDelimited(1, Encoding.UTF8.GetBytes(key))
            .Concat(LengthDelimited(2, LengthDelimited(1, Encoding.UTF8.GetBytes(value)))).ToArray();

    internal static byte[] Record(string body) => LengthDelimited(5, LengthDelimited(1, Encoding.UTF8.GetBytes(body)));

    internal static byte[] Block(byte[][] resourceAttributes, byte[][] records)
    {
        var resource = resourceAttributes.SelectMany(a => LengthDelimited(1, a)).ToArray();
        var scope    = records.SelectMany(r => LengthDelimited(2, r)).ToArray();
        return LengthDelimited(1, resource).Concat(LengthDelimited(2, scope)).ToArray();
    }

    internal static byte[] Request(byte[] block) => LengthDelimited(1, block);

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
        var framed = new byte[Ameto.Otel.OtlpGrpcFraming.HeaderBytes + message.Length];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)message.Length);
        message.CopyTo(framed.AsSpan(Ameto.Otel.OtlpGrpcFraming.HeaderBytes));
        return framed;
    }

    private Microsoft.AspNetCore.Http.DefaultHttpContext GrpcCall(byte[] framed)
    {
        var ctx = new Microsoft.AspNetCore.Http.DefaultHttpContext { RequestServices = _factory.Services };
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
