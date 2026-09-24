using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Metrics;
using Xunit.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// EVERY METRICS ENDPOINT, BYTE FOR BYTE, as it answered before the JSON switch (issue #83 WP7,
/// M#9). Captured on the unchanged code (<c>db5cdd1</c>), where every endpoint was
/// <c>Results.Json(&lt;object&gt;)</c> through reflection with ASP.NET Core's web defaults.
///
/// <para>What a source-generated context or a hand-written <c>Utf8JsonWriter</c> could move
/// without anybody noticing, and what the fixture below is shaped to catch: property names and
/// their camelCase (the Angular client reads <c>ts</c>, <c>value</c>, <c>count</c>, <c>sum</c>,
/// <c>labels</c>, <c>bounds</c>, <c>columns</c>); property ORDER; dictionary keys, which the
/// naming policy does NOT touch; number text — 0.1, 1e300, 5e-324, -0, 2^53 + 1, long.MaxValue;
/// string escaping; empty arrays versus absent properties; the empty label set; request binding
/// (case-insensitive names, numbers sent as strings); the refusals; the Content-Type; and what a
/// NaN, an infinity or a repeated label key does to a request (it fails it, 500, before a byte is
/// sent — kept, not fixed, by the switch).</para>
///
/// <para><b>Changed on purpose since, and only for those series (#92):</b> a repeated label key is
/// written once, with the last value of its run — <c>raw-dup-key</c>, <c>query-dup-key</c> and
/// <c>exemplars-dup-key</c> were 500 and are 200 — and a NaN or an infinity is written as
/// <c>null</c> — <c>raw-nan</c>, <c>raw-infinity</c>, <c>query-nan</c>, <c>expr-sub-default-name</c>
/// and <c>expr-unknown-op</c> (whose arithmetic overflows) were 500 and are 200, as are the new
/// <c>exemplars-nan</c> and <c>heatmap-infinite-bound</c>. And a group with a NaN member is reduced
/// over its finite members — the new <c>query-nan-fleet-sum</c> / <c>-last</c> answered null where a
/// member was NaN. Nothing else moved.</para>
///
/// <para><b>The escaping is not STJ's default, and this test is how that was found.</b> ASP.NET
/// Core's <c>JsonOptions</c> sets <c>JavaScriptEncoder.UnsafeRelaxedJsonEscaping</c>: <c>&lt;</c>,
/// <c>&amp;</c>, <c>'</c>, <c>+</c> and Cyrillic go out raw, and only quotes, backslashes, control
/// characters and characters outside the BMP (the emoji, as a surrogate pair) are escaped. A
/// source-generated context carries its OWN options, whose encoder is the strict default — so a
/// switch that took <c>MetricJson.Default</c> as it comes would have changed every label value
/// with a <c>&lt;</c> or a non-Latin letter in it.</para>
///
/// <para>The storage behind the endpoints is a fixed fake, so the bytes are a statement about
/// the ENDPOINTS: the aggregator is the real one, over that fake. Culture: the serializer is
/// culture-invariant and the only dates in play carry an explicit offset — a local-time date
/// would be a golden of the machine's time zone.</para>
/// </summary>
public sealed class MetricResponseShapeTests : IClassFixture<MetricResponseShapeTests.ShapeFactory>
{
    private readonly HttpClient _client;
    private readonly ITestOutputHelper _out;

    public MetricResponseShapeTests(ShapeFactory factory, ITestOutputHelper output)
    {
        _client = factory.CreateClient();
        _out    = output;
    }

    /// <summary>The app, with the metric storage swapped for <see cref="ShapeStore"/>.</summary>
    public sealed class ShapeFactory : AmetoWebAppFactory
    {
        protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureTestServices(static services =>
            {
                var store = new ShapeStore();
                services.AddSingleton<IMetricQuery>(store);
                services.AddSingleton<IMetricCatalog>(store);
                services.AddSingleton<IMetricExemplars>(store);
                services.AddSingleton<IMetricAggregator>(new MetricAggregator(store));
            });
        }
    }

    // ── The requests ──────────────────────────────────────────────────────────

    public static TheoryData<string, string, string, string?> Cases() => new()
    {
        // name,                    method, url,                                                            body
        { "names",                  "GET",  "/api/metrics/names",                                           null },
        { "names-prefix",           "GET",  "/api/metrics/names?prefix=shape.",                             null },
        { "names-prefix-none",      "GET",  "/api/metrics/names?prefix=zzz",                                null },
        { "catalog",                "GET",  "/api/metrics/catalog",                                         null },
        { "catalog-search",         "GET",  "/api/metrics/catalog?search=GAU",                              null },
        { "catalog-search-none",    "GET",  "/api/metrics/catalog?search=zzz",                              null },
        { "labels",                 "GET",  "/api/metrics/shape.gauge/labels",                              null },
        { "labels-unknown",         "GET",  "/api/metrics/nope/labels",                                     null },
        { "label-values",           "GET",  "/api/metrics/shape.gauge/labels/route/values",                 null },
        { "label-values-unknown",   "GET",  "/api/metrics/shape.gauge/labels/nope/values",                  null },

        { "raw-gauge",              "GET",  "/api/metrics/shape.gauge",                                     null },
        { "raw-hist",               "GET",  "/api/metrics/shape.hist",                                      null },
        { "raw-counter",            "GET",  "/api/metrics/shape.counter?from=2026-07-23T10:00:00Z&to=2026-07-23T11:00:00.5%2B05:00&step=15s", null },
        { "raw-unknown",            "GET",  "/api/metrics/nope",                                            null },
        { "raw-nan",                "GET",  "/api/metrics/shape.nan",                                       null },
        { "raw-infinity",           "GET",  "/api/metrics/shape.inf",                                       null },
        { "raw-dup-key",            "GET",  "/api/metrics/shape.dup",                                       null },

        { "query-none",             "POST", "/api/metrics/query", """{"metric":"shape.gauge"}""" },
        { "query-rate-by-service",  "POST", "/api/metrics/query", """{"metric":"shape.counter","aggregation":"rate","groupBy":["service.name"]}""" },
        { "query-increase",         "POST", "/api/metrics/query", """{"metric":"shape.counter","aggregation":"increase"}""" },
        { "query-sum",              "POST", "/api/metrics/query", """{"metric":"shape.counter","aggregation":"sum","groupBy":["route"]}""" },
        { "query-avg-topk",         "POST", "/api/metrics/query", """{"metric":"shape.gauge","aggregation":"avg","groupBy":["service.name"],"topk":1}""" },
        { "query-min",              "POST", "/api/metrics/query", """{"metric":"shape.gauge","aggregation":"MIN","groupBy":[]}""" },
        { "query-max-missing-key",  "POST", "/api/metrics/query", """{"metric":"shape.gauge","aggregation":"max","groupBy":["nope"]}""" },
        { "query-last",             "POST", "/api/metrics/query", """{"metric":"shape.counter","aggregation":"last","groupBy":["service.name"]}""" },
        { "query-quantile",         "POST", "/api/metrics/query", """{"metric":"shape.hist","aggregation":"quantile","quantile":0.95}""" },
        { "query-quantile-string",  "POST", "/api/metrics/query", """{"metric":"shape.hist","aggregation":"quantile","quantile":"0.5","groupBy":["service.name"]}""" },
        { "query-quantile-default", "POST", "/api/metrics/query", """{"metric":"shape.hist","aggregation":"quantile"}""" },
        { "query-unknown-agg",      "POST", "/api/metrics/query", """{"metric":"shape.gauge","aggregation":"bogus"}""" },
        { "query-echo",             "POST", "/api/metrics/query", """{"metric":"shape.echo","from":"2026-07-23T10:00:00Z","to":"2026-07-23T11:30:00.123+05:00","step":"5m","filters":{"service.name":"Svc","route":"/a|/b"}}""" },
        { "query-echo-pascal",      "POST", "/api/metrics/query", """{"Metric":"shape.echo","FROM":"2026-07-23T10:00:00.1234567Z","Step":"90","Filters":{},"TopK":"3"}""" },
        { "query-echo-steps",       "POST", "/api/metrics/query", """{"metric":"shape.echo","step":"1.5h","to":"2026-07-23T11:00:00-03:30"}""" },
        { "query-echo-timespan",    "POST", "/api/metrics/query", """{"metric":"shape.echo","step":"00:00:15","from":"not a date"}""" },
        { "query-unknown-metric",   "POST", "/api/metrics/query", """{"metric":"nope"}""" },
        { "query-nan",              "POST", "/api/metrics/query", """{"metric":"shape.nan"}""" },
        { "query-dup-key",          "POST", "/api/metrics/query", """{"metric":"shape.dup"}""" },
        { "query-nan-fleet-sum",    "POST", "/api/metrics/query", """{"metric":"shape.nanfleet","aggregation":"sum","groupBy":["service.name"]}""" },
        { "query-nan-fleet-last",   "POST", "/api/metrics/query", """{"metric":"shape.nanfleet","aggregation":"last","groupBy":["service.name"]}""" },
        { "query-bad-json",         "POST", "/api/metrics/query", """{"metric":""" },
        { "query-no-metric",        "POST", "/api/metrics/query", """{"aggregation":"rate"}""" },
        { "query-blank-metric",     "POST", "/api/metrics/query", """{"metric":"  "}""" },
        { "query-null",             "POST", "/api/metrics/query", """null""" },
        { "query-no-content-type",  "POST", "/api/metrics/query", "!raw{\"metric\":\"shape.gauge\"}" },

        { "expr-div",               "POST", "/api/metrics/expr",  """{"left":{"metric":"shape.counter","aggregation":"rate"},"right":{"metric":"shape.counter","aggregation":"increase"},"op":"div","scale":100,"name":"ratio"}""" },
        { "expr-sub-default-name",  "POST", "/api/metrics/expr",  """{"left":{"metric":"shape.gauge"},"right":{"metric":"shape.gauge","aggregation":"max"},"op":"SUB","scale":-2}""" },
        { "expr-unknown-op",        "POST", "/api/metrics/expr",  """{"left":{"metric":"shape.gauge"},"right":{"metric":"shape.counter"},"op":"pow"}""" },
        { "expr-no-right",          "POST", "/api/metrics/expr",  """{"left":{"metric":"shape.gauge"}}""" },
        { "expr-bad-json",          "POST", "/api/metrics/expr",  """[1,2""" },

        { "heatmap",                "GET",  "/api/metrics/shape.hist/heatmap",                              null },
        { "heatmap-echo",           "GET",  "/api/metrics/shape.echo.hist/heatmap?from=2026-07-23T10:00:00Z&step=1m&filters=service.name:Svc,route:/a,bad,:x,k:", null },
        { "heatmap-not-histogram",  "GET",  "/api/metrics/shape.gauge/heatmap",                             null },
        { "heatmap-infinite-bound", "GET",  "/api/metrics/shape.hist.inf/heatmap",                          null },

        { "exemplars",              "GET",  "/api/metrics/shape.gauge/exemplars",                           null },
        { "exemplars-echo",         "GET",  "/api/metrics/shape.echo/exemplars?from=2026-07-23T10:00:00Z&to=2026-07-23T12:00:00%2B01:00&filters=a:b&limit=5000", null },
        { "exemplars-limit-bad",    "GET",  "/api/metrics/shape.echo/exemplars?limit=abc",                  null },
        { "exemplars-none",         "GET",  "/api/metrics/nope/exemplars",                                  null },
        { "exemplars-dup-key",      "GET",  "/api/metrics/shape.dup/exemplars",                             null },
        { "exemplars-nan",          "GET",  "/api/metrics/shape.nan/exemplars",                             null },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Every_metrics_endpoint_answers_the_bytes_it_answered(string name, string method, string url, string? body)
    {
        string actual = await Exchange(method, url, body);
        _out.WriteLine(Literal(name, actual));
        Assert.True(Expected.TryGetValue(name, out var expected), $"no golden for {name}:{Environment.NewLine}{Literal(name, actual)}");
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Status, Content-Type, and the body verbatim. A 5xx body is the developer exception page in
    /// this environment — a stack trace, whose line numbers move with every edit — so for those
    /// the outcome is the status alone; a failure that escapes to the client is its type.
    /// </summary>
    private async Task<string> Exchange(string method, string url, string? body)
    {
        using var req = new HttpRequestMessage(new HttpMethod(method), url);
        if (body is not null)
        {
            if (body.StartsWith("!raw", StringComparison.Ordinal))
                req.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body[4..]));   // no Content-Type at all
            else
                req.Content = new StringContent(body, Encoding.UTF8, "application/json");
        }

        HttpResponseMessage resp;
        try { resp = await _client.SendAsync(req); }
        catch (Exception ex) { return "EXCEPTION " + ex.GetType().Name; }

        using (resp)
        {
            int status = (int)resp.StatusCode;
            string type = resp.Content.Headers.ContentType?.ToString() ?? "-";
            if (status >= 500) return $"{status.ToString(CultureInfo.InvariantCulture)} {type}";

            string text;
            try { text = Encoding.UTF8.GetString(await resp.Content.ReadAsByteArrayAsync()); }
            catch (Exception ex) { return $"{status.ToString(CultureInfo.InvariantCulture)} {type} BODY-EXCEPTION {ex.GetType().Name}"; }
            return $"{status.ToString(CultureInfo.InvariantCulture)} {type}\n{text}";
        }
    }

    /// <summary>
    /// An answer far past the writer's flush threshold (~14.7 KB): 600 series x 40 points, some
    /// 1.7 MB of JSON, so the raw endpoint sends it in many flushes while it is still reading
    /// series, and the query endpoint writes it from a list. Its bytes must be what the old path —
    /// DTOs through reflection over ASP.NET Core's JsonOptions — wrote; that path is rebuilt here
    /// as the oracle, since a golden string of 1.7 MB would say nothing a reader could check.
    /// </summary>
    private static IEnumerable<MetricSeries> BigAnswer()
    {
        for (int s = 0; s < 600; s++)
        {
            var pts = new MetricDataPoint[40];
            for (int p = 0; p < pts.Length; p++)
                pts[p] = P(T0 + p * 15 * S, s * 0.1 + p / 3.0, count: p, sum: p * 0.7);
            yield return Series("shape.big", s % 3 == 0 ? MetricKind.Histogram : MetricKind.Gauge, "ms",
                L("service.name", "svc-" + (s % 7).ToString(CultureInfo.InvariantCulture),
                  "route", "/r/" + s.ToString(CultureInfo.InvariantCulture) + "?q=<x>&y='z'",
                  "note", "Сервис " + (s % 5).ToString(CultureInfo.InvariantCulture)),
                null, pts);
        }
    }

    private static string OracleOf(IEnumerable<MetricSeries> series)
    {
        var options = new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web)
        {
            Encoder          = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            TypeInfoResolver = new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver(),
        };
        var dtos = series.Select(s => new MetricSeriesDto
        {
            Name   = s.Name,
            Kind   = s.Kind.ToString(),
            Unit   = s.Unit,
            Labels = LastValueWins(s.Labels),
            Points = s.Points.Select(p => new MetricPointDto { Ts = p.TimestampUnixNano, Value = p.Value, Count = p.Count, Sum = p.Sum }).ToList(),
        }).ToList();
        return System.Text.Json.JsonSerializer.Serialize(dtos, options);
    }

    /// <summary>
    /// The DTO's label dictionary as the old endpoints built it (<c>Pairs.ToDictionary</c>), except
    /// that a repeated key keeps the last value of its run where ToDictionary threw (#92) — for a
    /// valid set, the same dictionary in the same order.
    /// </summary>
    private static Dictionary<string, string> LastValueWins(LabelSet labels)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (k, v) in labels.Pairs) dict[k] = v;
        return dict;
    }

    /// <summary>
    /// A repeated label key far past the first flush (WP7 review, F4; #92). It used to fail the
    /// request: a clean 500 from the list answers, which checked every series before the first
    /// byte, and a DROPPED CONNECTION from the streamed raw answer once its first ~14.7 KB had gone —
    /// the browser saw status 0. The key is now written once, last value of its run, so both
    /// complete, and every series around it is written exactly as before.
    /// </summary>
    [Theory]
    [InlineData("POST", "/api/metrics/query", """{"metric":"shape.big.dup"}""")]
    [InlineData("GET",  "/api/metrics/shape.big.dup", null)]
    public async Task A_repeated_label_key_past_the_first_flush_no_longer_fails_the_answer(string method, string url, string? body)
    {
        string actual   = await Exchange(method, url, body);
        string expected = "200 application/json; charset=utf-8\n" + OracleOf(BigAnswer().Append(BigDup));
        Assert.Contains("\"labels\":{\"k\":\"v2\"}", expected);
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected, actual);
    }

    /// <summary>The series with a repeated label key that <c>shape.big.dup</c> answers after <see cref="BigAnswer"/>.</summary>
    private static readonly MetricSeries BigDup =
        Series("shape.big", MetricKind.Gauge, "", L("k", "v1", "k", "v2"), null, P(T0, 1));

    /// <summary>
    /// A NaN and both infinities far past the first flush (#92). They used to fail the request as a
    /// repeated key did: a 500 from the list answer, a dropped connection from the streamed one. They
    /// are written as <c>null</c> now and the answer completes; every other byte is the oracle's.
    /// </summary>
    [Theory]
    [InlineData("POST", "/api/metrics/query", """{"metric":"shape.big.nan"}""")]
    [InlineData("GET",  "/api/metrics/shape.big.nan", null)]
    public async Task A_non_finite_value_past_the_first_flush_no_longer_fails_the_answer(string method, string url, string? body)
    {
        string actual = await Exchange(method, url, body);
        string big    = OracleOf(BigAnswer());
        string expected = "200 application/json; charset=utf-8\n" + big[..^1] + ","
            + """{"name":"shape.big","kind":"Gauge","unit":"","labels":{"k":"nan"},"points":["""
            + """{"ts":1784800800000000000,"value":1,"count":0,"sum":0},"""
            + """{"ts":1784800801000000000,"value":null,"count":2,"sum":null},"""
            + """{"ts":1784800802000000000,"value":null,"count":0,"sum":0.5}]}]""";
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected, actual);
    }

    /// <summary>The series with NaN and ±Infinity that <c>shape.big.nan</c> answers after <see cref="BigAnswer"/>.</summary>
    private static readonly MetricSeries BigNaN =
        Series("shape.big", MetricKind.Gauge, "", L("k", "nan"), null,
               P(T0, 1), P(T0 + S, double.NaN, count: 2, sum: double.PositiveInfinity), P(T0 + 2 * S, double.NegativeInfinity, sum: 0.5));

    [Theory]
    [InlineData("GET",  "/api/metrics/shape.big", null)]
    [InlineData("POST", "/api/metrics/query",     """{"metric":"shape.big"}""")]
    public async Task A_large_answer_streams_the_bytes_the_serializer_wrote(string method, string url, string? body)
    {
        string actual   = await Exchange(method, url, body);
        string expected = "200 application/json; charset=utf-8\n" + OracleOf(BigAnswer());
        Assert.True(expected.Length > 1_000_000, $"the fixture is only {expected.Length} chars — no longer past many flushes");
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected, actual);
    }

    private static string Literal(string name, string actual) =>
        "[\"" + name + "\"] = \"" + actual.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n") + "\",";

    // ── The goldens (captured on db5cdd1) ─────────────────────────────────────

    private static readonly Dictionary<string, string> Expected = new(StringComparer.Ordinal)
    {
        ["catalog"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.gauge\",\"type\":\"Gauge\",\"unit\":\"By\",\"labelKeys\":[\"route\",\"service.name\"],\"cardinality\":3,\"lastSeenMs\":1784800800123},{\"name\":\"shape.hist\",\"type\":\"Histogram\",\"unit\":\"s\",\"labelKeys\":[],\"cardinality\":0,\"lastSeenMs\":0},{\"name\":\"Шкала.метрика\",\"type\":\"Counter\",\"unit\":\"\",\"labelKeys\":[\"ключ\"],\"cardinality\":2147483647,\"lastSeenMs\":9223372036854775807}]",
        ["catalog-search"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.gauge\",\"type\":\"Gauge\",\"unit\":\"By\",\"labelKeys\":[\"route\",\"service.name\"],\"cardinality\":3,\"lastSeenMs\":1784800800123}]",
        ["catalog-search-none"] = "200 application/json; charset=utf-8\n[]",
        ["exemplars"] = "200 application/json; charset=utf-8\n[{\"ts\":1784800800123456789,\"value\":0.1,\"traceId\":\"0af7651916cd43dd8448eb211c80319c\",\"spanId\":\"b7ad6b7169203331\",\"labels\":{\"route\":\"/a\",\"service.name\":\"Svc\"}},{\"ts\":9223372036854775807,\"value\":-0,\"traceId\":\"\",\"spanId\":\"\",\"labels\":{}}]",
        ["exemplars-echo"] = "200 application/json; charset=utf-8\n[{\"ts\":1784800800000000000,\"value\":1000,\"traceId\":\"from=2026-07-23T10:00:00.0000000+00:00;to=2026-07-23T12:00:00.0000000+01:00;step=null;filters=[a=b]\",\"spanId\":\"1000\",\"labels\":{\"k\":\"v\"}}]",
        ["exemplars-limit-bad"] = "200 application/json; charset=utf-8\n[{\"ts\":1784800800000000000,\"value\":200,\"traceId\":\"from=null;to=null;step=null;filters=null\",\"spanId\":\"200\",\"labels\":{\"k\":\"v\"}}]",
        ["exemplars-nan"] = "200 application/json; charset=utf-8\n[{\"ts\":1784800800000000000,\"value\":null,\"traceId\":\"\",\"spanId\":\"\",\"labels\":{\"k\":\"v\"}},{\"ts\":1784800801000000000,\"value\":null,\"traceId\":\"\",\"spanId\":\"\",\"labels\":{\"k\":\"v\"}},{\"ts\":1784800802000000000,\"value\":0.25,\"traceId\":\"\",\"spanId\":\"\",\"labels\":{\"k\":\"v\"}}]",   // 500 before (#92)
        ["exemplars-none"] = "200 application/json; charset=utf-8\n[]",
        ["exemplars-dup-key"] = "200 application/json; charset=utf-8\n[{\"ts\":1784800800000000000,\"value\":2,\"traceId\":\"0af7651916cd43dd8448eb211c80319c\",\"spanId\":\"b7ad6b7169203331\",\"labels\":{\"a\":\"first\",\"k\":\"v2\",\"z\":\"last\"}}]",   // was 500 (#92)
        ["expr-bad-json"] = "400 application/json; charset=utf-8\n\"Invalid JSON\"",
        ["expr-div"] = "200 application/json; charset=utf-8\n{\"name\":\"ratio\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{},\"points\":[{\"ts\":1784800815000000000,\"value\":6.666666666666667,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":6.666666666666667,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":6.666666666666667,\"count\":0,\"sum\":0},{\"ts\":1784800861000000000,\"value\":6.25,\"count\":0,\"sum\":0},{\"ts\":1784800890000000000,\"value\":3.4482758620689653,\"count\":0,\"sum\":0}]}",
        ["expr-no-right"] = "400 application/json; charset=utf-8\n\"'left' and 'right' are required\"",
        ["expr-sub-default-name"] = "200 application/json; charset=utf-8\n{\"name\":\"expr\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{},\"points\":[{\"ts\":1784800800000000000,\"value\":0,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":null,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":0,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":0,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":0,\"count\":0,\"sum\":0},{\"ts\":1784800875000000000,\"value\":0,\"count\":0,\"sum\":0},{\"ts\":9223372036854775807,\"value\":0,\"count\":0,\"sum\":0}]}",   // was 500: the sum overflows to -Infinity (#92)
        ["expr-unknown-op"] = "200 application/json; charset=utf-8\n{\"name\":\"expr\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{},\"points\":[{\"ts\":1784800800000000000,\"value\":0.023423423423423424,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":null,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":0.047619047619047616,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":0,\"count\":0,\"sum\":0}]}",   // was 500: the sum overflows to +Infinity (#92)
        ["heatmap"] = "200 application/json; charset=utf-8\n{\"bounds\":[0.005,0.01,0.025,0.1,1],\"unit\":\"s\",\"columns\":[{\"ts\":1784800815000,\"counts\":[1,2,3,3,1,0]},{\"ts\":1784800830000,\"counts\":[0,0,0,1,0,1]},{\"ts\":1784800860000,\"counts\":[0,1,1,1,0,1]}]}",
        ["heatmap-echo"] = "200 application/json; charset=utf-8\n{\"bounds\":[1,2],\"unit\":\"from=2026-07-23T10:00:00.0000000+00:00;to=null;step=600000000;filters=[service.name=Svc][route=/a][k=]\",\"columns\":[{\"ts\":1784800860000,\"counts\":[1,1,0]}]}",
        ["heatmap-infinite-bound"] = "200 application/json; charset=utf-8\n{\"bounds\":[0.5,null],\"unit\":\"s\",\"columns\":[{\"ts\":1784800815000,\"counts\":[1,1,0]}]}",   // 500 before (#92)
        ["heatmap-not-histogram"] = "200 application/json; charset=utf-8\n{\"bounds\":[],\"unit\":\"\",\"columns\":[]}",
        ["label-values"] = "200 application/json; charset=utf-8\n[\"/a\",\"/b?x=<1>&y='2'\"]",
        ["label-values-unknown"] = "200 application/json; charset=utf-8\n[]",
        ["labels"] = "200 application/json; charset=utf-8\n[\"route\",\"service.name\",\"ünï\"]",
        ["labels-unknown"] = "200 application/json; charset=utf-8\n[]",
        ["names"] = "200 application/json; charset=utf-8\n[\"a\\\"quote<tag>\",\"shape.counter\",\"shape.gauge\",\"shape.hist\",\"Шкала.метрика\"]",
        ["names-prefix"] = "200 application/json; charset=utf-8\n[\"shape.counter\",\"shape.gauge\",\"shape.hist\"]",
        ["names-prefix-none"] = "200 application/json; charset=utf-8\n[]",
        ["query-avg-topk"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"service.name\":\"Сервис\"},\"points\":[{\"ts\":1784800800000000000,\"value\":2.5,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":1.7976931348623157E+308,\"count\":0,\"sum\":0}]}]",
        ["query-bad-json"] = "400 application/json; charset=utf-8\n\"Invalid JSON\"",
        ["query-blank-metric"] = "400 application/json; charset=utf-8\n\"'metric' is required\"",
        ["query-echo"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.echo\",\"kind\":\"Gauge\",\"unit\":\"echo\",\"labels\":{\"echo\":\"from=2026-07-23T10:00:00.0000000+00:00;to=2026-07-23T11:30:00.1230000+05:00;step=3000000000;filters=[service.name=Svc][route=/a|/b]\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["query-echo-pascal"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.echo\",\"kind\":\"Gauge\",\"unit\":\"echo\",\"labels\":{\"echo\":\"from=2026-07-23T10:00:00.1234567+00:00;to=null;step=900000000;filters=\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["query-echo-steps"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.echo\",\"kind\":\"Gauge\",\"unit\":\"echo\",\"labels\":{\"echo\":\"from=null;to=2026-07-23T11:00:00.0000000-03:30;step=54000000000;filters=null\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["query-echo-timespan"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.echo\",\"kind\":\"Gauge\",\"unit\":\"echo\",\"labels\":{\"echo\":\"from=null;to=null;step=150000000;filters=null\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["query-increase"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/x\",\"service.name\":\"a\"},\"points\":[{\"ts\":1784800815000000000,\"value\":15,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":5,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":17,\"count\":0,\"sum\":0},{\"ts\":1784800861000000000,\"value\":21.5,\"count\":0,\"sum\":0},{\"ts\":1784800890000000000,\"value\":18.5,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/x\",\"service.name\":\"b\"},\"points\":[{\"ts\":1784800815000000000,\"value\":0,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":60,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/y\",\"service.name\":\"a\"},\"points\":[{\"ts\":1784800830000000000,\"value\":2,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":0.5,\"count\":0,\"sum\":0}]}]",
        ["query-last"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"service.name\":\"a\"},\"points\":[{\"ts\":1784800890000000000,\"value\":43.5,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"service.name\":\"b\"},\"points\":[{\"ts\":1784800830000000000,\"value\":160,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"service.name\":\"c\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["query-max-missing-key"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{},\"points\":[{\"ts\":1784800800000000000,\"value\":2.5,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":1.7976931348623157E+308,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":8,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":5E-324,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":123456789.123,\"count\":0,\"sum\":0},{\"ts\":1784800875000000000,\"value\":-42,\"count\":0,\"sum\":0},{\"ts\":9223372036854775807,\"value\":1.5,\"count\":0,\"sum\":0}]}]",
        ["query-min"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"route\":\"/a?b=<c>&d='e'\",\"service.name\":\"Svc\"},\"points\":[{\"ts\":1784800800000000000,\"value\":0.1,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":1E+300,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":-0,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":5E-324,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":123456789.123,\"count\":0,\"sum\":0},{\"ts\":1784800875000000000,\"value\":-42,\"count\":0,\"sum\":0},{\"ts\":9223372036854775807,\"value\":1.5,\"count\":9223372036854775807,\"sum\":-1.25}]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"emoji\":\"\\uD83D\\uDE00 \\u0001 \\t \\\\ /\",\"service.name\":\"Сервис\"},\"points\":[{\"ts\":1784800800000000000,\"value\":2.5,\"count\":9007199254740993,\"sum\":0.30000000000000004},{\"ts\":1784800815000000000,\"value\":1.7976931348623157E+308,\"count\":0,\"sum\":0}]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"service.name\":\"Empty\"},\"points\":[]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{},\"points\":[{\"ts\":1784800815000000000,\"value\":7,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":8,\"count\":0,\"sum\":0}]}]",
        ["query-nan"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.nan\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"k\":\"v\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0},{\"ts\":1784800801000000000,\"value\":null,\"count\":0,\"sum\":0}]}]",   // was 500 (#92)
        ["query-nan-fleet-sum"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.nanfleet\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"service.name\":\"fleet\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":2,\"count\":0,\"sum\":0}]}]",   // the NaN member skipped; was null, null (#92)
        ["query-nan-fleet-last"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.nanfleet\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"service.name\":\"fleet\"},\"points\":[{\"ts\":1784800815000000000,\"value\":3,\"count\":0,\"sum\":0}]}]",   // each pod's last finite value; was null (#92)
        ["query-dup-key"] ="200 application/json; charset=utf-8\n[{\"name\":\"shape.dup\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"k\":\"v2\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",   // was 500 (#92)
        ["query-no-content-type"] = "400 application/json; charset=utf-8\n\"Invalid JSON\"",
        ["query-no-metric"] = "400 application/json; charset=utf-8\n\"'metric' is required\"",
        ["query-none"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"route\":\"/a?b=<c>&d='e'\",\"service.name\":\"Svc\"},\"points\":[{\"ts\":1784800800000000000,\"value\":0.1,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":1E+300,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":-0,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":5E-324,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":123456789.123,\"count\":0,\"sum\":0},{\"ts\":1784800875000000000,\"value\":-42,\"count\":0,\"sum\":0},{\"ts\":9223372036854775807,\"value\":1.5,\"count\":9223372036854775807,\"sum\":-1.25}]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"emoji\":\"\\uD83D\\uDE00 \\u0001 \\t \\\\ /\",\"service.name\":\"Сервис\"},\"points\":[{\"ts\":1784800800000000000,\"value\":2.5,\"count\":9007199254740993,\"sum\":0.30000000000000004},{\"ts\":1784800815000000000,\"value\":1.7976931348623157E+308,\"count\":0,\"sum\":0}]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"service.name\":\"Empty\"},\"points\":[]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{},\"points\":[{\"ts\":1784800815000000000,\"value\":7,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":8,\"count\":0,\"sum\":0}]}]",
        ["query-null"] = "400 application/json; charset=utf-8\n\"'metric' is required\"",
        ["query-quantile"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"a\"},\"points\":[{\"ts\":1784800815000000000,\"value\":0.55,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":1,\"count\":0,\"sum\":0}]},{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"b\"},\"points\":[{\"ts\":1784800830000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["query-quantile-default"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"a\"},\"points\":[{\"ts\":1784800815000000000,\"value\":0.55,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":1,\"count\":0,\"sum\":0}]},{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"b\"},\"points\":[{\"ts\":1784800830000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["query-quantile-string"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"a\"},\"points\":[{\"ts\":1784800815000000000,\"value\":0.02,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":0.025,\"count\":0,\"sum\":0}]},{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"b\"},\"points\":[{\"ts\":1784800830000000000,\"value\":0.1,\"count\":0,\"sum\":0}]}]",
        ["query-rate-by-service"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"service.name\":\"a\"},\"points\":[{\"ts\":1784800815000000000,\"value\":1,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":0.4666666666666667,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":1.1666666666666667,\"count\":0,\"sum\":0},{\"ts\":1784800861000000000,\"value\":1.34375,\"count\":0,\"sum\":0},{\"ts\":1784800890000000000,\"value\":0.6379310344827587,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"service.name\":\"b\"},\"points\":[{\"ts\":1784800815000000000,\"value\":0,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":4,\"count\":0,\"sum\":0}]}]",
        ["query-sum"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/x\"},\"points\":[{\"ts\":1784800800000000000,\"value\":110,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":125,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":165,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":22,\"count\":0,\"sum\":0},{\"ts\":1784800861000000000,\"value\":21.5,\"count\":0,\"sum\":0},{\"ts\":1784800890000000000,\"value\":40,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/y\"},\"points\":[{\"ts\":1784800815000000000,\"value\":1,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":3,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":3.5,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["query-unknown-agg"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"route\":\"/a?b=<c>&d='e'\",\"service.name\":\"Svc\"},\"points\":[{\"ts\":1784800800000000000,\"value\":0.1,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":1E+300,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":-0,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":5E-324,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":123456789.123,\"count\":0,\"sum\":0},{\"ts\":1784800875000000000,\"value\":-42,\"count\":0,\"sum\":0},{\"ts\":9223372036854775807,\"value\":1.5,\"count\":9223372036854775807,\"sum\":-1.25}]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"emoji\":\"\\uD83D\\uDE00 \\u0001 \\t \\\\ /\",\"service.name\":\"Сервис\"},\"points\":[{\"ts\":1784800800000000000,\"value\":2.5,\"count\":9007199254740993,\"sum\":0.30000000000000004},{\"ts\":1784800815000000000,\"value\":1.7976931348623157E+308,\"count\":0,\"sum\":0}]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"service.name\":\"Empty\"},\"points\":[]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{},\"points\":[{\"ts\":1784800815000000000,\"value\":7,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":8,\"count\":0,\"sum\":0}]}]",
        ["query-unknown-metric"] = "200 application/json; charset=utf-8\n[]",
        ["raw-counter"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/x\",\"service.name\":\"a\"},\"points\":[{\"ts\":1784800800000000000,\"value\":10,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":25,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":5,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":20,\"count\":0,\"sum\":0},{\"ts\":1784800861000000000,\"value\":21.5,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/x\",\"service.name\":\"b\"},\"points\":[{\"ts\":1784800800000000000,\"value\":100,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":100,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":160,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/y\",\"service.name\":\"a\"},\"points\":[{\"ts\":1784800815000000000,\"value\":1,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":3,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":3.5,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"route\":\"/x\",\"service.name\":\"a\"},\"points\":[{\"ts\":1784800845000000000,\"value\":22,\"count\":0,\"sum\":0},{\"ts\":1784800890000000000,\"value\":40,\"count\":0,\"sum\":0}]},{\"name\":\"shape.counter\",\"kind\":\"Counter\",\"unit\":\"{req}\",\"labels\":{\"service.name\":\"c\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",
        ["raw-dup-key"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.dup\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"k\":\"v2\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0}]}]",   // was 500 (#92)
        ["raw-gauge"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"route\":\"/a?b=<c>&d='e'\",\"service.name\":\"Svc\"},\"points\":[{\"ts\":1784800800000000000,\"value\":0.1,\"count\":0,\"sum\":0},{\"ts\":1784800815000000000,\"value\":1E+300,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":-0,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":5E-324,\"count\":0,\"sum\":0},{\"ts\":1784800860000000000,\"value\":123456789.123,\"count\":0,\"sum\":0},{\"ts\":1784800875000000000,\"value\":-42,\"count\":0,\"sum\":0},{\"ts\":9223372036854775807,\"value\":1.5,\"count\":9223372036854775807,\"sum\":-1.25}]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{\"emoji\":\"\\uD83D\\uDE00 \\u0001 \\t \\\\ /\",\"service.name\":\"Сервис\"},\"points\":[{\"ts\":1784800800000000000,\"value\":2.5,\"count\":9007199254740993,\"sum\":0.30000000000000004},{\"ts\":1784800815000000000,\"value\":1.7976931348623157E+308,\"count\":0,\"sum\":0}]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"service.name\":\"Empty\"},\"points\":[]},{\"name\":\"shape.gauge\",\"kind\":\"Gauge\",\"unit\":\"By\",\"labels\":{},\"points\":[{\"ts\":1784800815000000000,\"value\":7,\"count\":0,\"sum\":0},{\"ts\":1784800830000000000,\"value\":8,\"count\":0,\"sum\":0}]}]",
        ["raw-hist"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"a\"},\"points\":[{\"ts\":1784800800000000000,\"value\":0.02,\"count\":10,\"sum\":0.2},{\"ts\":1784800815000000000,\"value\":0.03,\"count\":20,\"sum\":0.6},{\"ts\":1784800830000000000,\"value\":0,\"count\":0,\"sum\":0},{\"ts\":1784800845000000000,\"value\":0.05,\"count\":5,\"sum\":0.25},{\"ts\":1784800860000000000,\"value\":0.1,\"count\":9007199254740993,\"sum\":1000000000000000}]},{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"b\"},\"points\":[{\"ts\":1784800815000000000,\"value\":0.5,\"count\":2,\"sum\":1},{\"ts\":1784800830000000000,\"value\":0.9,\"count\":4,\"sum\":3.6}]},{\"name\":\"shape.hist\",\"kind\":\"Histogram\",\"unit\":\"s\",\"labels\":{\"service.name\":\"no-bounds\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":1,\"sum\":1}]}]",
        ["raw-infinity"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.inf\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"k\":\"v\"},\"points\":[{\"ts\":1784800800000000000,\"value\":null,\"count\":0,\"sum\":0}]}]",   // was 500 (#92)
        ["raw-nan"] = "200 application/json; charset=utf-8\n[{\"name\":\"shape.nan\",\"kind\":\"Gauge\",\"unit\":\"\",\"labels\":{\"k\":\"v\"},\"points\":[{\"ts\":1784800800000000000,\"value\":1,\"count\":0,\"sum\":0},{\"ts\":1784800801000000000,\"value\":null,\"count\":0,\"sum\":0}]}]",   // was 500 (#92)
        ["raw-unknown"] = "200 application/json; charset=utf-8\n[]",
    };

    // ── The storage behind the endpoints ──────────────────────────────────────

    private const long T0 = 1_784_800_800_000_000_000L;   // 2026-07-23T10:00:00Z
    private const long S  = 1_000_000_000L;

    private static MetricDataPoint P(long ts, double v, long count = 0, double sum = 0, long[]? b = null) =>
        new() { TimestampUnixNano = ts, Value = v, Count = count, Sum = sum, BucketCounts = b };

    private static LabelSet L(params string[] kv)
    {
        var pairs = new List<KeyValuePair<string, string>>();
        for (int i = 0; i < kv.Length; i += 2) pairs.Add(new(kv[i], kv[i + 1]));
        return new LabelSet(pairs);
    }

    private static MetricSeries Series(string name, MetricKind kind, string unit, LabelSet labels,
                                       double[]? bounds, params MetricDataPoint[] points) =>
        new() { Name = name, Kind = kind, Unit = unit, Labels = labels, BucketBounds = bounds, Points = points };

    private static readonly double[] HistBounds = [0.005, 0.01, 0.025, 0.1, 1];

    /// <summary>
    /// The fixed storage. Two metrics echo what they were asked (<c>shape.echo</c>,
    /// <c>shape.echo.hist</c>) so the bytes also pin how each endpoint BOUND its parameters —
    /// dates, steps, filters, limits — not only how it wrote its answer.
    /// </summary>
    private sealed class ShapeStore : IMetricQuery, IMetricCatalog, IMetricExemplars
    {
        private static readonly string[] Names = ["a\"quote<tag>", "shape.counter", "shape.gauge", "shape.hist", "Шкала.метрика"];

        public IEnumerable<string> GetMetricNames(string? prefix = null)
        {
            var list = new List<string>();
            foreach (var n in Names)
                if (prefix is null || n.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) list.Add(n);
            return list;
        }

        public async IAsyncEnumerable<MetricSeries> QueryAsync(
            string metricName, DateTimeOffset? from = null, DateTimeOffset? to = null, TimeSpan? step = null,
            IReadOnlyDictionary<string, string>? labelMatchers = null, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            foreach (var s in Answer(metricName, from, to, step, labelMatchers)) yield return s;
        }

        private static IEnumerable<MetricSeries> Answer(
            string metric, DateTimeOffset? from, DateTimeOffset? to, TimeSpan? step, IReadOnlyDictionary<string, string>? filters)
        {
            switch (metric.ToLowerInvariant())
            {
                case "shape.gauge":
                    yield return Series("shape.gauge", MetricKind.Gauge, "By",
                        L("service.name", "Svc", "route", "/a?b=<c>&d='e'"), null,
                        P(T0, 0.1), P(T0 + 15 * S, 1e300), P(T0 + 30 * S, -0.0), P(T0 + 45 * S, 5e-324),
                        P(T0 + 60 * S, 123456789.123), P(T0 + 75 * S, -42), P(long.MaxValue, 1.5, count: long.MaxValue, sum: -1.25));
                    yield return Series("shape.gauge", MetricKind.Gauge, "By",
                        L("service.name", "Сервис", "emoji", "😀 \u0001 \t \\ /"), null,
                        P(T0, 2.5, count: 9_007_199_254_740_993, sum: 0.30000000000000004), P(T0 + 15 * S, double.MaxValue));
                    yield return Series("shape.gauge", MetricKind.Gauge, "", L("service.name", "Empty"), null);
                    yield return Series("shape.gauge", MetricKind.Gauge, "By", LabelSet.Empty, null,
                        P(T0 + 15 * S, 7), P(T0 + 30 * S, 8));
                    break;

                case "shape.counter":
                    yield return Series("shape.counter", MetricKind.Counter, "{req}", L("service.name", "a", "route", "/x"), null,
                        P(T0, 10), P(T0 + 15 * S, 25), P(T0 + 30 * S, 5), P(T0 + 45 * S, 20), P(T0 + 61 * S, 21.5));
                    yield return Series("shape.counter", MetricKind.Counter, "{req}", L("service.name", "b", "route", "/x"), null,
                        P(T0, 100), P(T0 + 15 * S, 100), P(T0 + 30 * S, 160));
                    yield return Series("shape.counter", MetricKind.Counter, "{req}", L("service.name", "a", "route", "/y"), null,
                        P(T0 + 15 * S, 1), P(T0 + 30 * S, 3), P(T0 + 45 * S, 3.5));
                    // A second fragment of the first series: overlaps it at 45 s (it wins there).
                    yield return Series("shape.counter", MetricKind.Counter, "{req}", L("route", "/x", "service.name", "a"), null,
                        P(T0 + 45 * S, 22), P(T0 + 90 * S, 40));
                    yield return Series("shape.counter", MetricKind.Counter, "{req}", L("service.name", "c"), null, P(T0, 1));
                    break;

                case "shape.hist":
                    yield return Series("shape.hist", MetricKind.Histogram, "s", L("service.name", "a"), HistBounds,
                        P(T0,          0.02,  count: 10, sum: 0.2,  b: [1, 2, 3, 4, 0, 0]),
                        P(T0 + 15 * S, 0.03,  count: 20, sum: 0.6,  b: [2, 4, 6, 7, 1, 0]),
                        P(T0 + 30 * S, 0,     count: 0,  sum: 0),
                        P(T0 + 45 * S, 0.05,  count: 5,  sum: 0.25, b: [0, 1, 1, 2, 1, 0]),
                        P(T0 + 60 * S, 0.1,   count: 9_007_199_254_740_993, sum: 1e15, b: [0, 2, 2, 3, 1, 1]));
                    yield return Series("shape.hist", MetricKind.Histogram, "s", L("service.name", "b"), HistBounds,
                        P(T0 + 15 * S, 0.5, count: 2, sum: 1, b: [0, 0, 0, 0, 2, 0]),
                        P(T0 + 30 * S, 0.9, count: 4, sum: 3.6, b: [0, 0, 0, 1, 2, 1]));
                    yield return Series("shape.hist", MetricKind.Histogram, "s", L("service.name", "no-bounds"), null,
                        P(T0, 1, count: 1, sum: 1, b: [1]));
                    break;

                case "shape.echo":
                    yield return Series("shape.echo", MetricKind.Gauge, "echo", Echo(from, to, step, filters), null, P(T0, 1));
                    break;

                case "shape.echo.hist":
                    yield return Series("shape.echo.hist", MetricKind.Histogram, EchoText(from, to, step, filters), L("k", "v"), [1, 2],
                        P(T0, 1, count: 1, sum: 1, b: [1, 0, 0]), P(T0 + 60 * S, 1, count: 3, sum: 3, b: [2, 1, 0]));
                    break;

                case "shape.nan":
                    yield return Series("shape.nan", MetricKind.Gauge, "", L("k", "v"), null, P(T0, 1), P(T0 + S, double.NaN));
                    break;

                case "shape.inf":
                    yield return Series("shape.inf", MetricKind.Gauge, "", L("k", "v"), null, P(T0, double.PositiveInfinity));
                    break;

                case "shape.nanfleet":
                    // Two pods of one service, each with a NaN where the other has a value: the group
                    // is answered from the finite member at every timestamp.
                    yield return Series("shape.nanfleet", MetricKind.Gauge, "", L("service.name", "fleet", "pod", "a"), null,
                        P(T0, 1), P(T0 + 15 * S, double.NaN));
                    yield return Series("shape.nanfleet", MetricKind.Gauge, "", L("service.name", "fleet", "pod", "b"), null,
                        P(T0, double.NaN), P(T0 + 15 * S, 2));
                    break;

                case "shape.dup":
                    yield return Series("shape.dup", MetricKind.Gauge, "", L("k", "v1", "k", "v2"), null, P(T0, 1));
                    break;

                case "shape.big":
                    foreach (var s in BigAnswer()) yield return s;
                    break;

                case "shape.big.dup":
                    // The large answer, then a series with a repeated label key: the failure lies far past
                    // the first flush.
                    foreach (var s in BigAnswer()) yield return s;
                    yield return BigDup;
                    break;

                case "shape.big.nan":
                    foreach (var s in BigAnswer()) yield return s;
                    yield return BigNaN;
                    break;

                case "shape.hist.inf":
                    // An exporter that sends +Inf as an explicit bound (OTLP leaves it implicit).
                    yield return Series("shape.hist.inf", MetricKind.Histogram, "s", L("service.name", "a"), [0.5, double.PositiveInfinity],
                        P(T0,          0.5, count: 1, sum: 0.5, b: [1, 0, 0]),
                        P(T0 + 15 * S, 1,   count: 3, sum: 3,   b: [2, 1, 0]));
                    break;
            }
        }

        private static string Date(DateTimeOffset? d) => d is { } v ? v.ToString("O", CultureInfo.InvariantCulture) : "null";

        private static string EchoText(DateTimeOffset? from, DateTimeOffset? to, TimeSpan? step, IReadOnlyDictionary<string, string>? filters)
        {
            var sb = new StringBuilder();
            sb.Append("from=").Append(Date(from)).Append(";to=").Append(Date(to))
              .Append(";step=").Append(step is { } s ? s.Ticks.ToString(CultureInfo.InvariantCulture) : "null")
              .Append(";filters=");
            if (filters is null) sb.Append("null");
            else foreach (var (k, v) in filters) sb.Append('[').Append(k).Append('=').Append(v).Append(']');
            return sb.ToString();
        }

        private static LabelSet Echo(DateTimeOffset? from, DateTimeOffset? to, TimeSpan? step, IReadOnlyDictionary<string, string>? filters) =>
            L("echo", EchoText(from, to, step, filters));

        public async IAsyncEnumerable<MetricSeries> GetLatestAsync(
            string metricName, IReadOnlyDictionary<string, string>? labelMatchers = null, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            yield break;
        }

        public IReadOnlyList<MetricCatalogEntry> GetCatalog(string? search = null)
        {
            var all = new List<MetricCatalogEntry>
            {
                new() { Name = "shape.gauge", Kind = MetricKind.Gauge, Unit = "By", LabelKeys = ["route", "service.name"],
                        Cardinality = 3, LastSeenMs = 1_784_800_800_123 },
                new() { Name = "shape.hist", Kind = MetricKind.Histogram, Unit = "s", LabelKeys = [], Cardinality = 0, LastSeenMs = 0 },
                new() { Name = "Шкала.метрика", Kind = MetricKind.Counter, Unit = "", LabelKeys = ["ключ"],
                        Cardinality = int.MaxValue, LastSeenMs = long.MaxValue },
            };
            if (search is null) return all;
            return all.FindAll(e => e.Name.Contains(search, StringComparison.OrdinalIgnoreCase));
        }

        public IReadOnlyList<string> GetLabelKeys(string metricName) =>
            metricName == "shape.gauge" ? ["route", "service.name", "ünï"] : [];

        public IReadOnlyList<string> GetLabelValues(string metricName, string labelKey) =>
            metricName == "shape.gauge" && labelKey == "route" ? ["/a", "/b?x=<1>&y='2'"] : [];

        public IReadOnlyList<ExemplarSample> GetExemplars(
            string metricName, DateTimeOffset? from, DateTimeOffset? to, IReadOnlyDictionary<string, string>? filters, int limit = 200)
        {
            switch (metricName)
            {
                case "shape.gauge":
                    return
                    [
                        new() { TimestampUnixNano = T0 + 123_456_789, Value = 0.1, TraceId = "0af7651916cd43dd8448eb211c80319c",
                                SpanId = "b7ad6b7169203331", Labels = L("service.name", "Svc", "route", "/a") },
                        new() { TimestampUnixNano = long.MaxValue, Value = -0.0, Labels = LabelSet.Empty },
                    ];
                case "shape.echo":
                    return
                    [
                        new() { TimestampUnixNano = T0, Value = limit,
                                TraceId = EchoText(from, to, null, filters), SpanId = limit.ToString(CultureInfo.InvariantCulture),
                                Labels = L("k", "v") },
                    ];
                case "shape.nan":
                    return
                    [
                        new() { TimestampUnixNano = T0, Value = double.NaN, Labels = L("k", "v") },
                        new() { TimestampUnixNano = T0 + S, Value = double.NegativeInfinity, Labels = L("k", "v") },
                        new() { TimestampUnixNano = T0 + 2 * S, Value = 0.25, Labels = L("k", "v") },
                    ];
                case "shape.dup":
                    return
                    [
                        new() { TimestampUnixNano = T0, Value = 2, TraceId = "0af7651916cd43dd8448eb211c80319c",
                                SpanId = "b7ad6b7169203331", Labels = L("z", "last", "k", "v2", "a", "first", "k", "v1") },
                    ];
                default:
                    return [];
            }
        }
    }
}
