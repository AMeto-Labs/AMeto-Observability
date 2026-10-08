using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Metrics;
using Ameto.Otel;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// A METRICS BATCH IS COUNTED BEFORE IT IS DECODED (#126 review F2).
///
/// <para>A metric data point can be two bytes on the wire and ~125 decoded (~200 from JSON): 200 000
/// empty gauge points — a 400 KB protobuf body, 1.8 KB gzipped — allocated 25 MB to decode, 62.5×.
/// At the 8 MiB body limit that is ~500 MiB, past the 512 MB stand's whole heap: the decode ran out
/// of memory, which since #125 is answered 503, and the exporter retried the same batch for minutes.
/// Now the points are counted by a walk that allocates nothing, and a batch over
/// <c>Ingestion.MaxOtlpMetricPoints</c> is refused 413 (gRPC: RESOURCE_EXHAUSTED) before one is
/// built. This host runs at 100 000.</para>
/// </summary>
public sealed class OtlpMetricPointBudgetTests : IClassFixture<OtlpMetricPointBudgetTests.Factory>
{
    private const int Limit = 100_000;

    private const string Refusal = "the batch holds more data points than this server decodes in one request; split it";

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { MaxOtlpMetricPoints = Limit };
    }

    private readonly Factory    _factory;
    private readonly HttpClient _client;

    public OtlpMetricPointBudgetTests(Factory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
    }

    // ── The counts ────────────────────────────────────────────────────────────

    [Fact]
    public void The_counts_allocate_nothing_and_count_every_point()
    {
        byte[] proto = EmptyPoints("budget.count", 200_000, protobuf: true);
        byte[] json  = EmptyPoints("budget.count", 200_000, protobuf: false);
        Assert.Equal(200_000, OtlpMetricPointBudget.CountProto(proto));         // warm
        Assert.Equal(200_000, OtlpMetricPointBudget.CountJson(json));

        long before = GC.GetAllocatedBytesForCurrentThread();
        int  p = OtlpMetricPointBudget.CountProto(proto);
        int  j = OtlpMetricPointBudget.CountJson(json);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(200_000, p);
        Assert.Equal(200_000, j);
        Assert.True(allocated < 1024, $"counting 400 000 points allocated {allocated:N0} B");
    }

    /// <summary>Every point the parsers build is counted: gauges, sums and histograms, across resources and scopes.</summary>
    [Fact]
    public void The_proto_count_matches_what_the_parser_builds()
    {
        byte[] message = Msg(c =>
        {
            for (int resource = 0; resource < 2; resource++)
                Sub(c, 1, Msg(rm => Sub(rm, 2, Msg(sm =>
                {
                    Sub(sm, 2, Metric("budget.gauge", 5, points: 3, histogram: false));
                    Sub(sm, 2, Metric("budget.sum", 7, points: 2, histogram: false));
                    Sub(sm, 2, Metric("budget.histogram", 9, points: 4, histogram: true));
                }))));
        });

        Assert.Equal(OtlpMetricProtoParser.Parse(message).Count, OtlpMetricPointBudget.CountProto(message));
        Assert.Equal(18, OtlpMetricPointBudget.CountProto(message));
    }

    // ── OTLP/HTTP: 413 ────────────────────────────────────────────────────────

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_batch_over_the_point_limit_is_413_and_nothing_of_it_is_ingested(bool protobuf)
    {
        // 200 000 EMPTY points: 400 KB of protobuf, 600 KB of JSON — well inside the 8 MiB body limit.
        string name = "budget.over." + (protobuf ? "proto" : "json");
        using var refused = await PostAsync(EmptyPoints(name, 200_000, protobuf), protobuf);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, refused.StatusCode);
        Assert.Equal(protobuf ? "application/x-protobuf" : "application/json", refused.Content.Headers.ContentType?.MediaType);
        Assert.Equal(Refusal, StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf));
        Assert.Empty(QueryNames(name));                                         // the ingest is synchronous: nothing landed

        // Within the limit it is decoded and taken as before — and the same query sees it.
        using var accepted = await PostAsync(EmptyPoints(name, 1_000, protobuf, stamped: true), protobuf);
        Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
        Assert.Equal("{\"ingested\":1000,\"dropped\":0}", await accepted.Content.ReadAsStringAsync());
        Assert.Contains(name, QueryNames(name));
    }

    // ── OTLP/gRPC: RESOURCE_EXHAUSTED ─────────────────────────────────────────

    [Fact]
    public async Task A_grpc_batch_over_the_point_limit_is_RESOURCE_EXHAUSTED()
    {
        var gate = new OtlpInflateGate(1, TimeSpan.Zero);
        var noLog = new OtlpGzipTooLargeLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        var noMemoryLog = new OtlpOutOfMemoryLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        var call = GrpcCall(Frame(EmptyPoints("budget.grpc", 200_000, protobuf: true)));

        await OtlpGrpcEndpointMapper.HandleAsync(call, ApiKeyPermissions.Metrics, gate, noLog, noMemoryLog,
            (c, msg) => OtlpGrpcEndpointMapper.DecodeMetrics(c, msg.AsSpan(), Limit));

        Assert.Equal("8", call.Response.Headers["grpc-status"].ToString());
        Assert.Equal(Refusal, call.Response.Headers["grpc-message"].ToString());
        Assert.Empty(QueryNames("budget.grpc"));
        Assert.Equal(1, gate.Available);
    }

    // ── Harness ───────────────────────────────────────────────────────────────

    private IEnumerable<string> QueryNames(string name) =>
        _factory.Services.GetRequiredService<IMetricQuery>().GetMetricNames(name);

    /// <summary>
    /// One gauge of <paramref name="points"/> data points: empty (two bytes each on the wire), or
    /// stamped with a recent time and a value, as an exporter's would be.
    /// </summary>
    private static byte[] EmptyPoints(string name, int points, bool protobuf, bool stamped = false)
    {
        long now = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000) * 1_000_000L;
        if (!protobuf)
        {
            var sb = new StringBuilder("{\"resourceMetrics\":[{\"scopeMetrics\":[{\"metrics\":[{\"name\":\"")
                .Append(name).Append("\",\"gauge\":{\"dataPoints\":[");
            for (int i = 0; i < points; i++)
            {
                if (i > 0) sb.Append(',');
                if (stamped)
                    sb.Append("{\"timeUnixNano\":\"").Append((now + i * 1_000_000L).ToString(CultureInfo.InvariantCulture))
                      .Append("\",\"asDouble\":1.5}");
                else sb.Append("{}");
            }
            return Encoding.UTF8.GetBytes(sb.Append("]}}]}]}]}").ToString());
        }

        return Msg(c => Sub(c, 1, Msg(rm => Sub(rm, 2, Msg(sm => Sub(sm, 2, Msg(metric =>
        {
            metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString(name);
            Sub(metric, 5, Msg(gauge =>
            {
                for (int i = 0; i < points; i++)
                {
                    long ts = now + i * 1_000_000L;
                    Sub(gauge, 1, stamped
                        ? Msg(dp =>
                        {
                            dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64((ulong)ts);
                            dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(1.5);
                        })
                        : []);
                }
            }));
        })))))));
    }

    /// <summary>A metric of <paramref name="points"/> data points under field <paramref name="kindField"/> (5 gauge, 7 sum, 9 histogram).</summary>
    private static byte[] Metric(string name, int kindField, int points, bool histogram) => Msg(metric =>
    {
        metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString(name);
        Sub(metric, kindField, Msg(data =>
        {
            for (int i = 0; i < points; i++)
                Sub(data, 1, Msg(dp =>
                {
                    dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64((ulong)(1_784_800_000_000_000_000L + i));
                    if (histogram)
                    {
                        dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteFixed64(3);
                    }
                    else
                    {
                        dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(i);
                    }
                }));
        }));
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

    private Task<HttpResponseMessage> PostAsync(byte[] body, bool protobuf)
    {
        var content = new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue(protobuf ? "application/x-protobuf" : "application/json");
        return _client.PostAsync("/v1/metrics", content);
    }

    /// <summary>The <c>message</c> of an OTLP Status body, in either encoding.</summary>
    private static string StatusMessage(byte[] body, bool protobuf)
    {
        if (!protobuf) return JsonDocument.Parse(body).RootElement.GetProperty("message").GetString()!;
        Assert.Equal(0x12, body[0]);
        Assert.Equal(body.Length - 2, (int)body[1]);
        return Encoding.UTF8.GetString(body, 2, body.Length - 2);
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
        ctx.Response.Body = new MemoryStream();
        return ctx;
    }
}
