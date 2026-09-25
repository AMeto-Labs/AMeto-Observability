using System.Net;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Alerts;
using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// A SERIES STORED BEFORE INGEST COLLAPSED REPEATED LABEL KEYS (#92) STILL ANSWERS EVERY FILTER.
///
/// <para>Ingest no longer builds a label set with a key twice, but the WAL and the <c>.mts</c> files of
/// a server upgraded in place hold the ones it built before. The response writer already wrote such a
/// key once; the label MATCHER — the one scan behind the cold reader, the hot tier and the exemplar
/// ring — still threw on it, so any filter that touched the series was a 500 on /query, /heatmap and
/// /exemplars, and a metric alert rule with labels failed every tick while the series was in its
/// window. Now the key is matched on its ordinal-greatest value (the last of its sorted run): the
/// value the answer writes.</para>
///
/// <para>The series is filed through the real engine exactly as the pre-fix parser left it: a label
/// set built with the key twice.</para>
/// </summary>
public sealed class MetricStoredRepeatedKeyTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    private readonly HttpClient         _client;

    public MetricStoredRepeatedKeyTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
    }

    /// <summary>What an exporter sending <c>k</c> twice left in storage before the fix.</summary>
    private static readonly LabelSet Legacy =
        new([new("service.name", "legacy"), new("k", "v1"), new("k", "v2")]);

    private static long NowNanos => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

    private void Seed(string gauge, string hist)
    {
        var engine = _factory.Services.GetRequiredService<MetricStorageEngine>();
        long now = NowNanos;
        engine.Ingest(
        [
            new MetricIngestItem { Name = gauge, Kind = MetricKind.Gauge, Unit = "1", Labels = Legacy,
                                   TimestampUnixNano = now - 60_000_000_000L, ScalarValue = 5 },
            new MetricIngestItem { Name = gauge, Kind = MetricKind.Gauge, Unit = "1", Labels = Legacy,
                                   TimestampUnixNano = now - 30_000_000_000L, ScalarValue = 7,
                                   Exemplars = [new MetricExemplar { TimestampUnixNano = now - 30_000_000_000L, Value = 7,
                                                                     TraceId = "0af7651916cd43dd8448eb211c80319c", SpanId = "b7ad6b7169203331" }] },
            new MetricIngestItem { Name = hist, Kind = MetricKind.Histogram, Unit = "s", Labels = Legacy,
                                   TimestampUnixNano = now - 60_000_000_000L, HistogramCount = 1, HistogramSum = 0.5,
                                   BucketBounds = [1, 2], BucketCounts = [1, 0, 0] },
            new MetricIngestItem { Name = hist, Kind = MetricKind.Histogram, Unit = "s", Labels = Legacy,
                                   TimestampUnixNano = now - 30_000_000_000L, HistogramCount = 3, HistogramSum = 2.5,
                                   BucketBounds = [1, 2], BucketCounts = [2, 1, 0] },
        ]);
    }

    private async Task<(HttpStatusCode Status, string Body)> Get(string url)
    {
        using var resp = await _client.GetAsync(url);
        return (resp.StatusCode, await resp.Content.ReadAsStringAsync());
    }

    private async Task<(HttpStatusCode Status, string Body)> Post(string url, string json)
    {
        using var resp = await _client.PostAsync(url, new StringContent(json, Encoding.UTF8, "application/json"));
        return (resp.StatusCode, await resp.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Every_filtered_endpoint_matches_the_last_value_of_a_repeated_key()
    {
        Seed("legacy.dup.gauge", "legacy.dup.hist");

        // POST /api/metrics/query: the key's written value matches, the value it was overwritten by does not.
        var hit = await Post("/api/metrics/query", """{"metric":"legacy.dup.gauge","filters":{"k":"v2"}}""");
        Assert.Equal(HttpStatusCode.OK, hit.Status);
        Assert.Contains("\"labels\":{\"k\":\"v2\",\"service.name\":\"legacy\"}", hit.Body);
        Assert.Contains("\"value\":7", hit.Body);
        var miss = await Post("/api/metrics/query", """{"metric":"legacy.dup.gauge","filters":{"k":"v1"}}""");
        Assert.Equal((HttpStatusCode.OK, "[]"), miss);

        // Grouped by the repeated key: one group, under the written value.
        var grouped = await Post("/api/metrics/query", """{"metric":"legacy.dup.gauge","aggregation":"max","groupBy":["k"]}""");
        Assert.Equal(HttpStatusCode.OK, grouped.Status);
        Assert.Contains("\"labels\":{\"k\":\"v2\"}", grouped.Body);

        // GET /api/metrics/{name}/heatmap with a filter.
        var heat = await Get("/api/metrics/legacy.dup.hist/heatmap?filters=k:v2");
        Assert.Equal(HttpStatusCode.OK, heat.Status);
        Assert.StartsWith("{\"bounds\":[1,2]", heat.Body);
        Assert.Contains("\"counts\":[1,1,0]", heat.Body);

        // GET /api/metrics/{name}/exemplars with a filter: the ring holds the series' own label set.
        var ex = await Get("/api/metrics/legacy.dup.gauge/exemplars?filters=k:v2");
        Assert.Equal(HttpStatusCode.OK, ex.Status);
        Assert.Contains("\"traceId\":\"0af7651916cd43dd8448eb211c80319c\"", ex.Body);
        Assert.Contains("\"labels\":{\"k\":\"v2\",\"service.name\":\"legacy\"}", ex.Body);
        Assert.Equal((HttpStatusCode.OK, "[]"), await Get("/api/metrics/legacy.dup.gauge/exemplars?filters=k:v1"));
    }

    [Fact]
    public async Task A_metric_rule_with_labels_evaluates_over_such_a_series()
    {
        Seed("legacy.alert.gauge", "legacy.alert.hist");
        var evaluator = _factory.Services.GetRequiredService<AlertEvaluator>();
        var store     = _factory.Services.GetRequiredService<AlertRuleStore>();
        var rule = new AlertRule
        {
            Id          = "legacy-dup",
            Name        = "legacy dup",
            Source      = AlertSource.Metric,
            Metric      = "legacy.alert.gauge",
            Aggregation = "max",
            Labels      = new Dictionary<string, string> { ["k"] = "v2" },
            Comparator  = AlertComparator.GreaterThan,
            Threshold   = 6,
            Window      = TimeSpan.FromHours(1),
            For         = TimeSpan.Zero,
            Cooldown    = TimeSpan.Zero,
        };
        store.Upsert(rule);
        try
        {
            await evaluator.EvaluateOnceAsync();

            // Before: the filtered query threw inside the evaluation, the rule was logged as failing
            // and never got a state at all.
            var state = evaluator.GetStates().Single(s => s.RuleId == rule.Id);
            Assert.Equal(AlertState.Firing, state.State);
            Assert.Equal(7, state.LastValue);
        }
        finally { store.Delete(rule.Id); }
    }
}
