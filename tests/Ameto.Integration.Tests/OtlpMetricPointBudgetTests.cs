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
using Microsoft.Extensions.Logging;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// A METRICS BATCH IS WEIGHED BEFORE IT IS DECODED (#126 review F2, NEW-1).
///
/// <para>A metric data point can be two bytes on the wire and ~125 decoded (~200 from JSON): 200 000
/// empty gauge points — a 400 KB protobuf body, 1.8 KB gzipped — allocated 25 MB to decode, 62.5×.
/// At the 8 MiB body limit that is ~500 MiB, past the 512 MB stand's whole heap: the decode ran out
/// of memory, which since #125 is answered 503, and the exporter retried the same batch for minutes.
/// A histogram bucket count is one byte on the wire and 24 decoded. Now the batch is weighed by a
/// walk that allocates nothing — its points, and its histogram points' buckets, bounds and
/// exemplars — and one that would decode past <c>Ingestion.MaxOtlpMetricPoints</c> points' worth is
/// refused 413 (gRPC: RESOURCE_EXHAUSTED) before a point is built. This host runs at 100 000 points,
/// 12.8 MB.</para>
/// </summary>
public sealed class OtlpMetricPointBudgetTests : IClassFixture<OtlpMetricPointBudgetTests.Factory>
{
    private const int Limit = 100_000;

    private const string Refusal = "the batch decodes to more data points and buckets than this server takes in one request; split it";

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { MaxOtlpMetricPoints = Limit };

        /// <summary>What the host logs — for the refusal's warning (#126 review NEW-2).</summary>
        public OtlpHttpGzipTests.CapturedLog Log { get; } = new();

        /// <summary>The clock the refusal's warning is throttled by: it moves only when a test says so.</summary>
        internal Ameto.Testing.ManualTimeProvider Clock { get; } = new();

        protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<ILoggerProvider>(Log);
                services.AddSingleton(sp => new OtlpMetricBudgetLog(
                    sp.GetRequiredService<ILoggerFactory>().CreateLogger("Ameto.Otel"), Clock));
            });
        }
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

    /// <summary>
    /// The JSON count is the points the model builds: those of a gauge, sum or histogram, where the
    /// deserializer binds them. An exponential histogram's or a summary's points, which the model does
    /// not have and the protobuf count skips too, are not counted, nor is a <c>dataPoints</c> array
    /// anywhere else (#126 review NEW-6: 3 + 2 such points counted 5, and decoded 0).
    /// </summary>
    [Fact]
    public void The_json_count_is_the_points_the_model_builds()
    {
        byte[] json = Encoding.UTF8.GetBytes("""
            {"resourceMetrics":[{"resource":{"dataPoints":[{}]},"scopeMetrics":[{"metrics":[
              {"name":"budget.exp","exponentialHistogram":{"dataPoints":[{},{},{}]}},
              {"name":"budget.summary","summary":{"dataPoints":[{},{}]}},
              {"name":"budget.gauge","gauge":{"dataPoints":[{"asDouble":1}]}},
              {"name":"budget.sum","sum":{"dataPoints":[{"asDouble":1},{"asDouble":2},],"isMonotonic":true}},
              {"name":"budget.histogram","histogram":{"dataPoints":[{"count":"1"}]},"dataPoints":[{}]}
            ]}]}]}
            """);

        var request = JsonSerializer.Deserialize<Ameto.Otel.Models.ExportMetricsServiceRequest>(
            json, new JsonSerializerOptions { AllowTrailingCommas = true });
        Assert.Equal(4, OtlpMetricMapper.Map(request!).Count);
        Assert.Equal(4, OtlpMetricPointBudget.CountJson(json));
    }

    // ── The limit ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The default limit is the request-body share over 128 B, and never below 131 040: the share is
    /// floored at 16 MiB − 4 KiB, so the rule's own floor of 65 536 never binds (#126 review NEW-7).
    /// The heaps are the review's table: 288 MiB, the stand's 384 MiB, 768 MiB, and 3 GiB where the
    /// share's 128 MiB cap applies.
    /// </summary>
    [Theory]
    [InlineData(288L << 20,  131_040)]
    [InlineData(384L << 20,  157_286)]
    [InlineData(768L << 20,  314_572)]
    [InlineData(3L   << 30, 1_048_576)]
    public void The_default_limit_is_the_body_share_over_128_bytes_and_its_own_floor_never_binds(long managedHeap, int expected)
    {
        int derived = IngestionOptions.DefaultMaxOtlpMetricPointsFor(MemoryBudgets.Derive(managedHeap, managedHeap * 4 / 3));
        Assert.Equal(expected, derived);
        Assert.True(derived > IngestionOptions.MinOtlpMetricPoints);
    }

    /// <summary>0 or below, configured, means the default rule — as unset does (#126 review NEW-7).</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void A_limit_of_zero_or_below_means_the_default_rule(int configured)
    {
        Assert.Equal(new IngestionOptions().EffectiveMaxOtlpMetricPoints,
                     new IngestionOptions { MaxOtlpMetricPoints = configured }.EffectiveMaxOtlpMetricPoints);
        Assert.Equal(7, new IngestionOptions { MaxOtlpMetricPoints = 7 }.EffectiveMaxOtlpMetricPoints);
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

    // ── A refusal is logged, not only answered (#126 review NEW-2) ───────────

    /// <summary>
    /// The 413 is logged: the Collector's exporter does not retry it and does not split the batch, so
    /// without a line only the exporter's own log said a batch was lost. At most one Warning a second,
    /// with the count since the last, the latest batch's points and decoded size, the limit, and the
    /// sender — the shape of the gzip over-limit warning.
    /// </summary>
    [Fact]
    public async Task A_refused_batch_is_a_warning_once_a_second_with_the_count_the_points_and_the_limit()
    {
        byte[] over = EmptyPoints("budget.warned", 200_000, protobuf: true);
        const string Event = "OtlpMetricsOverBudget";

        // A new second: the first refusal is written at once (with whatever earlier tests left pending).
        _factory.Clock.Advance(OtlpMetricBudgetLog.Interval);
        int warned = _factory.Log.Count(Event, LogLevel.Warning);
        using (var first = await PostAsync(over, protobuf: true))
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, first.StatusCode);
        Assert.Equal(warned + 1, _factory.Log.Count(Event, LogLevel.Warning));

        // A second in the same second: refused and counted, not written.
        using (var second = await PostAsync(over, protobuf: true))
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, second.StatusCode);
        Assert.Equal(warned + 1, _factory.Log.Count(Event, LogLevel.Warning));

        // The next second: one line, for it and the one that trips it.
        _factory.Clock.Advance(OtlpMetricBudgetLog.Interval);
        using (var third = await PostAsync(over, protobuf: true))
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, third.StatusCode);
        Assert.Equal(warned + 2, _factory.Log.Count(Event, LogLevel.Warning));

        var line = _factory.Log.Last(Event);
        Assert.Equal(2L, line["Count"]);
        Assert.Equal(200_000L, line["Points"]);
        Assert.Equal(Limit, line["Limit"]);
        Assert.Equal(200_000L * OtlpMetricPointBudget.PointBytes, line["DecodedBytes"]);
        Assert.Equal(KeyListPreview(AmetoWebAppFactory.TestApiKey), line["KeyPreview"]);
    }

    /// <summary>The same warning from the gRPC receiver, with the caller's address.</summary>
    [Fact]
    public async Task A_grpc_refusal_is_the_same_warning()
    {
        var clock     = new Ameto.Testing.ManualTimeProvider();
        var log       = new OtlpHttpGzipTests.CapturedLog();
        var budgetLog = new OtlpMetricBudgetLog(log.CreateLogger("Ameto.Otel"), clock);
        var gate = new OtlpInflateGate(1, TimeSpan.Zero);
        var noLog = new OtlpGzipTooLargeLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        var noMemoryLog = new OtlpOutOfMemoryLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        byte[] frame = Frame(EmptyPoints("budget.grpc.warned", 200_000, protobuf: true));

        for (int i = 0; i < 3; i++)
        {
            if (i == 2) clock.Advance(OtlpMetricBudgetLog.Interval);
            var call = GrpcCall(frame);
            call.Connection.RemoteIpAddress = IPAddress.Parse("203.0.113.9");
            await OtlpGrpcEndpointMapper.HandleAsync(call, ApiKeyPermissions.Metrics, gate, noLog, noMemoryLog,
                (c, msg) => OtlpGrpcEndpointMapper.DecodeMetrics(c, msg.AsSpan(), Limit, budgetLog));
            Assert.Equal("8", call.Response.Headers["grpc-status"].ToString());
        }

        Assert.Equal(2, log.Count("OtlpMetricsOverBudget", LogLevel.Warning));     // the first at once, then one for two
        var line = log.Last("OtlpMetricsOverBudget");
        Assert.Equal(2L, line["Count"]);
        Assert.Equal(200_000L, line["Points"]);
        Assert.Equal(Limit, line["Limit"]);
        Assert.Equal(KeyListPreview(AmetoWebAppFactory.TestApiKey), line["KeyPreview"]);
        Assert.Equal("203.0.113.9", line["RemoteAddress"]);
    }

    /// <summary>What <c>GET /api/auth/keys</c> shows for a key: <c>KeyHash[..8]</c>, the hash being AuthStore's.</summary>
    private static string KeyListPreview(string key)
        => Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(key))).ToLowerInvariant()[..8];

    // ── A histogram point's arrays are weighed too (#126 review NEW-1) ────────

    /// <summary>
    /// The protobuf weight of a histogram point's arrays is what the parser builds from them, in every
    /// encoding the parser reads: packed varint and packed fixed64 counts, unpacked varint and fixed64
    /// counts, packed and unpacked bounds, and exemplars.
    /// </summary>
    [Fact]
    public void The_proto_weight_counts_a_histogram_points_arrays_as_the_parser_reads_them()
    {
        byte[] message = Msg(c => Sub(c, 1, Msg(rm => Sub(rm, 2, Msg(sm =>
        {
            Sub(sm, 2, Msg(metric =>
            {
                metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString("weight.histogram");
                Sub(metric, 9, Msg(h =>
                {
                    Sub(h, 1, Msg(dp =>                                         // packed varints (13 B), packed bounds, exemplars
                    {
                        Sub(dp, 6, Enumerable.Range(1, 13).Select(i => (byte)i).ToArray());
                        Sub(dp, 7, Doubles(12));
                        Sub(dp, 8, Exemplar());
                        Sub(dp, 8, Exemplar());
                    }));
                    Sub(h, 1, Msg(dp =>                                         // packed fixed64 (32 B), unpacked bounds
                    {
                        Sub(dp, 6, Doubles(4));
                        for (int i = 0; i < 3; i++) { dp.WriteTag(7, WireFormat.WireType.Fixed64); dp.WriteDouble(i); }
                    }));
                    Sub(h, 1, Msg(dp =>                                         // unpacked varint and fixed64 counts
                    {
                        for (int i = 0; i < 3; i++) { dp.WriteTag(6, WireFormat.WireType.Varint);  dp.WriteUInt64(5); }
                        for (int i = 0; i < 2; i++) { dp.WriteTag(6, WireFormat.WireType.Fixed64); dp.WriteFixed64(7); }
                    }));
                }));
            }));
            Sub(sm, 2, Metric("weight.gauge", 5, points: 2, histogram: false));
        })))));

        var items  = OtlpMetricProtoParser.Parse(message);
        var weight = OtlpMetricPointBudget.WeighProto(message);

        Assert.Equal(items.Count, weight.Points);
        Assert.Equal(items.Sum(i => i.BucketCounts?.Length ?? 0), weight.Buckets);
        Assert.Equal(items.Sum(i => i.BucketBounds?.Length ?? 0), weight.Bounds);
        Assert.Equal(items.Sum(i => i.Exemplars?.Length ?? 0), weight.Exemplars);
        Assert.Equal((5, 22, 15, 2), (weight.Points, weight.Buckets, weight.Bounds, weight.Exemplars));

        static byte[] Doubles(int n) => Msg(o => { for (int i = 0; i < n; i++) o.WriteDouble(i + 0.5); });
        static byte[] Exemplar() => Msg(e =>
        {
            e.WriteTag(3, WireFormat.WireType.Fixed64); e.WriteDouble(1.5);
            e.WriteTag(5, WireFormat.WireType.LengthDelimited); e.WriteBytes(ByteString.CopyFrom(new byte[16] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }));
        });
    }

    /// <summary>The JSON weight is what the mapper builds: bucket counts, bounds and exemplars of histogram points only.</summary>
    [Fact]
    public void The_json_weight_counts_a_histogram_points_arrays_as_the_model_binds_them()
    {
        byte[] json = Encoding.UTF8.GetBytes("""
            {"resourceMetrics":[{"scopeMetrics":[{"metrics":[
              {"name":"weight.histogram","histogram":{"dataPoints":[
                {"count":"3","bucketCounts":["1","1","1"],"explicitBounds":[1,2],
                 "exemplars":[{"asDouble":1.5,"traceId":"0102030405060708090a0b0c0d0e0f10"}]},
                {"count":"1","bucketCounts":["1"],"explicitBounds":[]}
              ]}},
              {"name":"weight.gauge","gauge":{"dataPoints":[{"asDouble":1,"bucketCounts":["9","9"],"exemplars":[{}]}]}}
            ]}]}]}
            """);

        var items  = OtlpMetricMapper.Map(JsonSerializer.Deserialize<Ameto.Otel.Models.ExportMetricsServiceRequest>(json)!);
        var weight = OtlpMetricPointBudget.WeighJson(json);

        Assert.Equal(items.Count, weight.Points);
        Assert.Equal(items.Sum(i => i.BucketCounts?.Length ?? 0), weight.Buckets);
        Assert.Equal(items.Sum(i => i.BucketBounds?.Length ?? 0), weight.Bounds);
        Assert.Equal(items.Sum(i => i.Exemplars?.Length ?? 0), weight.Exemplars);
        Assert.Equal((3, 4, 2, 1), (weight.Points, weight.Buckets, weight.Bounds, weight.Exemplars));
    }

    /// <summary>
    /// ONE POINT OF 8 300 001 PACKED BUCKETS IS 413, AND NEVER PARSED. An 8.3 MB body inside the byte
    /// limit, counted as one point, allocated 200 MB to parse and kept a 66 MB array; two of them at
    /// once, about 8 KB each as gzip uploads, would have passed the 512 MB stand's heap. Weighed, it is
    /// refused by a walk that allocates nothing, over HTTP and over gRPC.
    /// </summary>
    [Fact]
    public async Task One_point_of_8_300_001_packed_buckets_is_refused_by_a_walk_that_allocates_nothing()
    {
        byte[] body = HistogramBeside("weight.wide", 8_300_001, "weight.wide.beside", protobuf: true);
        Assert.True(body.Length < 8 * 1024 * 1024, "setup: inside the byte limit");

        OtlpMetricPointBudget.WeighProto(body);                                // warm
        long before = GC.GetAllocatedBytesForCurrentThread();
        var weight  = OtlpMetricPointBudget.WeighProto(body);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(8_300_001, weight.Buckets);
        Assert.True(allocated < 1024, $"weighing allocated {allocated:N0} B");

        using var refused = await PostAsync(body, protobuf: true);
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, refused.StatusCode);
        Assert.Equal(Refusal, StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf: true));

        var gate = new OtlpInflateGate(1, TimeSpan.Zero);
        var noLog = new OtlpGzipTooLargeLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        var noMemoryLog = new OtlpOutOfMemoryLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        var call = GrpcCall(Frame(body));
        await OtlpGrpcEndpointMapper.HandleAsync(call, ApiKeyPermissions.Metrics, gate, noLog, noMemoryLog,
            (c, msg) => OtlpGrpcEndpointMapper.DecodeMetrics(c, msg.AsSpan(), Limit));
        Assert.Equal("8", call.Response.Headers["grpc-status"].ToString());
        Assert.Equal(Refusal, call.Response.Headers["grpc-message"].ToString());

        Assert.Empty(QueryNames("weight.wide"));                               // neither the point nor the gauge beside it
    }

    /// <summary>JSON's bucket counts are weighed the same: 600 000 of them, a 2.4 MB body, are 413.</summary>
    [Fact]
    public async Task A_json_point_of_600_000_bucket_counts_is_413()
    {
        using var refused = await PostAsync(HistogramBeside("weight.json.wide", 600_001, "weight.json.beside", protobuf: false),
                                            protobuf: false);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, refused.StatusCode);
        Assert.Equal(Refusal, StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf: false));
        Assert.Empty(QueryNames("weight.json"));
    }

    /// <summary>
    /// What an exporter sends is still taken: 8 192 points — one OpenTelemetry Collector batch at its
    /// default size — of 16 buckets and 15 bounds each weigh 6.2 MB, inside this host's 12.8 MB and
    /// inside even the 8.4 MB of the default rule's 65 536-point floor.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_batch_of_8192_histogram_points_of_16_buckets_is_taken(bool protobuf)
    {
        string name = "weight.batch." + (protobuf ? "proto" : "json");
        byte[] body = HistogramPoints(name, points: 8_192, buckets: 16, protobuf);

        var weight = protobuf ? OtlpMetricPointBudget.WeighProto(body) : OtlpMetricPointBudget.WeighJson(body);
        Assert.Equal((8_192, 8_192 * 16, 8_192 * 15), (weight.Points, weight.Buckets, weight.Bounds));
        Assert.True(OtlpMetricPointBudget.Fits(weight, IngestionOptions.DefaultMaxOtlpMetricPointsFor(default)),
                    "the smallest derived limit refuses a Collector batch");

        using var accepted = await PostAsync(body, protobuf);
        Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
        Assert.Equal("{\"ingested\":8192,\"dropped\":0}", await accepted.Content.ReadAsStringAsync());
        Assert.Contains(name, QueryNames(name));
    }

    // ── A histogram point of more than 65 535 buckets (#126 review NEW-0) ──────

    /// <summary>
    /// A histogram point of more than <see cref="MetricIngestItem.MaxBucketCounts"/> buckets is refused
    /// at decode and reported as dropped; the rest of its batch is ingested. One of 7.5 million buckets,
    /// about 7 KB as a gzip upload, was answered <c>{"ingested":1}</c> and then stopped every metric
    /// flush until a restart: no block a reader opens can hold it, and the metric WAL keeps only its
    /// first 65 535 buckets. A point AT the cap is taken as before.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_histogram_point_of_more_than_65535_buckets_is_dropped_and_the_rest_of_its_batch_ingested(bool protobuf)
    {
        string tag = protobuf ? "proto" : "json";
        string wide = "buckets.wide." + tag, beside = "buckets.beside." + tag;

        using var refused = await PostAsync(HistogramBeside(wide, MetricIngestItem.MaxBucketCounts + 2, beside, protobuf), protobuf);

        Assert.Equal(HttpStatusCode.OK, refused.StatusCode);
        Assert.Equal("{\"ingested\":1,\"dropped\":1}", await refused.Content.ReadAsStringAsync());
        Assert.Empty(QueryNames(wide));
        Assert.Contains(beside, QueryNames(beside));

        string atCap = "buckets.at-cap." + tag, beside2 = "buckets.beside2." + tag;
        using var accepted = await PostAsync(HistogramBeside(atCap, MetricIngestItem.MaxBucketCounts, beside2, protobuf), protobuf);

        Assert.Equal("{\"ingested\":2,\"dropped\":0}", await accepted.Content.ReadAsStringAsync());
        Assert.Contains(atCap, QueryNames(atCap));
    }

    /// <summary>The same over gRPC: OK, with the point in <c>partial_success</c> and the reason.</summary>
    [Fact]
    public async Task A_grpc_histogram_point_of_more_than_65535_buckets_is_a_rejected_point_with_its_reason()
    {
        var gate = new OtlpInflateGate(1, TimeSpan.Zero);
        var noLog = new OtlpGzipTooLargeLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        var noMemoryLog = new OtlpOutOfMemoryLog(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);
        var call = GrpcCall(Frame(HistogramBeside("buckets.wide.grpc", MetricIngestItem.MaxBucketCounts + 2,
                                                  "buckets.beside.grpc", protobuf: true)));

        await OtlpGrpcEndpointMapper.HandleAsync(call, ApiKeyPermissions.Metrics, gate, noLog, noMemoryLog,
            (c, msg) => OtlpGrpcEndpointMapper.DecodeMetrics(c, msg.AsSpan(), Limit));

        Assert.Equal("0", call.Response.Headers["grpc-status"].ToString());
        Assert.Equal(OtlpGrpcFraming.Frame(OtlpGrpcFraming.ExportResponse(1, "histogram points of more than 65535 buckets were refused")),
                     ((MemoryStream)call.Response.Body).ToArray());
        Assert.Empty(QueryNames("buckets.wide.grpc"));
        Assert.Contains("buckets.beside.grpc", QueryNames("buckets.beside.grpc"));
    }

    // ── Harness ───────────────────────────────────────────────────────────────

    /// <summary>
    /// One batch: a histogram point of <paramref name="buckets"/> zero counts — packed varints, one byte
    /// each, or JSON strings — and a gauge point beside it, both stamped a minute ago. The bucket
    /// counts used here are never a multiple of 8 bytes, which the parser would read as fixed64.
    /// </summary>
    private static byte[] HistogramBeside(string histogram, int buckets, string gauge, bool protobuf)
    {
        Assert.NotEqual(0, buckets % 8);
        long now = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000) * 1_000_000L;
        string ts = now.ToString(CultureInfo.InvariantCulture);
        if (!protobuf)
        {
            var sb = new StringBuilder("{\"resourceMetrics\":[{\"scopeMetrics\":[{\"metrics\":[{\"name\":\"")
                .Append(histogram).Append("\",\"histogram\":{\"dataPoints\":[{\"timeUnixNano\":\"").Append(ts)
                .Append("\",\"count\":\"1\",\"bucketCounts\":[");
            for (int i = 0; i < buckets; i++) sb.Append(i == 0 ? "\"0\"" : ",\"0\"");
            sb.Append("]}]}},{\"name\":\"").Append(gauge).Append("\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"")
              .Append(ts).Append("\",\"asDouble\":1.5}]}}]}]}]}");
            return Encoding.UTF8.GetBytes(sb.ToString());
        }

        return Msg(c => Sub(c, 1, Msg(rm => Sub(rm, 2, Msg(sm =>
        {
            Sub(sm, 2, Msg(metric =>
            {
                metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString(histogram);
                Sub(metric, 9, Msg(h => Sub(h, 1, Msg(dp =>
                {
                    dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64((ulong)now);
                    dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteFixed64(1);
                    Sub(dp, 6, new byte[buckets]);                              // packed varint zeros
                }))));
            }));
            Sub(sm, 2, Msg(metric =>
            {
                metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString(gauge);
                Sub(metric, 5, Msg(g => Sub(g, 1, Msg(dp =>
                {
                    dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64((ulong)now);
                    dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(1.5);
                }))));
            }));
        })))));
    }

    /// <summary>
    /// One histogram of <paramref name="points"/> points as an exporter sends them: stamped a minute
    /// ago and a millisecond apart, <paramref name="buckets"/> counts as packed fixed64 (the type the
    /// .proto declares) and one bound fewer as packed doubles; in JSON, counts as strings.
    /// </summary>
    private static byte[] HistogramPoints(string name, int points, int buckets, bool protobuf)
    {
        long now = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000) * 1_000_000L;
        if (!protobuf)
        {
            var sb = new StringBuilder("{\"resourceMetrics\":[{\"scopeMetrics\":[{\"metrics\":[{\"name\":\"")
                .Append(name).Append("\",\"histogram\":{\"dataPoints\":[");
            for (int i = 0; i < points; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append("{\"timeUnixNano\":\"").Append((now + i * 1_000_000L).ToString(CultureInfo.InvariantCulture))
                  .Append("\",\"count\":\"").Append(buckets).Append("\",\"sum\":1.5,\"bucketCounts\":[");
                for (int b = 0; b < buckets; b++) sb.Append(b == 0 ? "\"1\"" : ",\"1\"");
                sb.Append("],\"explicitBounds\":[");
                for (int b = 0; b < buckets - 1; b++) sb.Append(b == 0 ? "" : ",").Append(b + 1);
                sb.Append("]}");
            }
            return Encoding.UTF8.GetBytes(sb.Append("]}}]}]}]}").ToString());
        }

        byte[] counts = Msg(o => { for (int b = 0; b < buckets; b++) o.WriteFixed64(1); });
        byte[] bounds = Msg(o => { for (int b = 0; b < buckets - 1; b++) o.WriteDouble(b + 1); });
        return Msg(c => Sub(c, 1, Msg(rm => Sub(rm, 2, Msg(sm => Sub(sm, 2, Msg(metric =>
        {
            metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString(name);
            Sub(metric, 9, Msg(h =>
            {
                for (int i = 0; i < points; i++)
                    Sub(h, 1, Msg(dp =>
                    {
                        dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64((ulong)(now + i * 1_000_000L));
                        dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteFixed64((ulong)buckets);
                        dp.WriteTag(5, WireFormat.WireType.Fixed64); dp.WriteDouble(1.5);
                        Sub(dp, 6, counts);
                        Sub(dp, 7, bounds);
                    }));
            }));
        })))))));
    }

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
