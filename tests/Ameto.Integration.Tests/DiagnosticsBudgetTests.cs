using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Tracing;

namespace Ameto.Integration.Tests;

/// <summary>
/// THE DIAGNOSTICS THE DOCS PROMISE (PR #84 review, #7). config.yml says the effective Metrics
/// budgets "are printed at startup and in GET /api/diagnostics", and CONFIGURATION.md says
/// /api/diagnostics "counts the refusals" of <c>MaxExemplarMetrics</c>; neither existed, and nor
/// did any trace budget or any of the span ring's refusal counters. An operator tuning
/// <c>HotTierBytes</c> or <c>RingMaxBytes</c> on the 512 MB stand, as those comments instruct, had
/// nothing to read.
///
/// <para>The host is built with figures no derivation produces (a 12 345 678-byte metric tier, a
/// one-chunk ring, one exemplar ring), so a field that reported a fresh derivation instead of what
/// the engines enforce would be caught; and the counters are made to MOVE, through the routes that
/// move them in production: an OTLP span heavier than the ring budget, and a second metric name
/// asking for an exemplar ring past the cap.</para>
/// </summary>
public sealed class DiagnosticsBudgetTests : IClassFixture<DiagnosticsBudgetTests.Factory>
{
    /// <summary>A metric tier no derivation produces.</summary>
    public const long MetricHotTierBytes = 12_345_678;

    /// <summary>One arena chunk: a 100 KB span is refused for bytes whatever slots are free.</summary>
    public const long RingMaxBytes = 64 * 1024;

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override MetricsOptions ConfiguredMetrics =>
            new() { HotTierBytes = MetricHotTierBytes, MaxExemplarMetrics = 1 };

        protected override TracesOptions ConfiguredTraces => new() { RingMaxBytes = RingMaxBytes };
    }

    private readonly Factory    _factory;
    private readonly HttpClient _client;

    public DiagnosticsBudgetTests(Factory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
    }

    private async Task<JsonElement> Diagnostics() =>
        await _client.GetFromJsonAsync<JsonElement>("/api/diagnostics");

    private static long Long(JsonElement json, string name) => json.GetProperty(name).GetInt64();

    [Fact]
    public async Task Diagnostics_reports_the_effective_metric_and_trace_budgets()
    {
        var json    = await Diagnostics();
        var metrics = _factory.Services.GetRequiredService<MetricStorageEngine>();
        var traces  = _factory.Services.GetRequiredService<TraceDiagnostics>();

        // Metrics: the explicit tier wins; the unset bars derive from IT, not from the host.
        Assert.Equal(MetricHotTierBytes,      Long(json, "metricsHotTierBudgetBytes"));
        Assert.Equal(MetricHotTierBytes / 10, Long(json, "metricsMinFlushBytes"));      // a tenth of the tier
        Assert.Equal(8L * 1024 * 1024,        Long(json, "metricsWalInitialBytes"));    // the tier, capped at 8 MB
        Assert.Equal(metrics.ExemplarsPerMetric, json.GetProperty("metricsExemplarsPerMetric").GetInt32());
        Assert.True(metrics.ExemplarsPerMetric > 0);
        Assert.Equal(1, json.GetProperty("metricsMaxExemplarMetrics").GetInt32());

        // Traces: what the engine and the ring were built with.
        Assert.Equal(RingMaxBytes,                      Long(json, "tracesRingMaxBytes"));
        Assert.Equal(TracesOptions.DefaultRingCapacity, json.GetProperty("tracesRingCapacity").GetInt32());
        Assert.Equal(traces.HotTierBudgetBytes,         Long(json, "tracesHotTierBudgetBytes"));
        Assert.Equal(traces.MergeBudgetBytes,           Long(json, "tracesMergeBudgetBytes"));
        Assert.True(traces.HotTierBudgetBytes > 0 && traces.MergeBudgetBytes > 0);

        foreach (string counter in new[]
                 {
                     "metricsExemplarMetricsRefused", "tracesRingBytesInFlight",
                     "tracesRingRefusedForBytes", "tracesRingRefusedNoSlot", "tracesRingRefusedNoArena",
                     "tracesUnpooledSpanNames", "tracesUnpooledServiceNames", "tracesInternPoolSaturations",
                     "metricsLabelPoolStrings", "metricsLabelPoolSaturations", "metricsLabelPoolResets",
                 })
            Assert.True(Long(json, counter) >= 0, counter);

        // The metric label pool (#88): the process-wide one the OTLP parsers intern into.
        Assert.Equal(MetricLabelInterner.DefaultMaxStrings, json.GetProperty("metricsLabelPoolMaxStrings").GetInt32());
        Assert.True(Long(json, "metricsLabelPoolStrings") <= MetricLabelInterner.DefaultMaxStrings);

        // Additive: the fields the client reads are all still there.
        foreach (string existing in new[] { "diskFreeBytes", "processWorkingSetBytes", "segmentCount",
                                            "metricsStorageBytes", "tracesStorageBytes", "tracesSegmentCount" })
            Assert.True(json.TryGetProperty(existing, out _), existing);
    }

    [Fact]
    public async Task A_refused_span_and_a_refused_exemplar_move_their_counters()
    {
        var before = await Diagnostics();

        // A span heavier than the whole ring budget: refused for BYTES — not for a slot, not for arena.
        string heavy = new('x', 100_000);
        string body  =
            "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"diag-svc\"}}]},"
          + "\"scopeSpans\":[{\"spans\":[{\"traceId\":\"0af7651916cd43dd8448eb211c80319c\",\"spanId\":\"b7ad6b7169203331\","
          + "\"name\":\"heavy\",\"kind\":2,\"startTimeUnixNano\":\"1727000000000000000\",\"endTimeUnixNano\":\"1727000000001000000\","
          + "\"attributes\":[{\"key\":\"db.statement\",\"value\":{\"stringValue\":\"" + heavy + "\"}}]}]}]}]}";
        var content = new ByteArrayContent(Encoding.UTF8.GetBytes(body));
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        var response = await _client.PostAsync("/otlp/v1/traces", content);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var reply = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(1, reply.GetProperty("dropped").GetInt32());

        // Two metric names asking for an exemplar ring under a one-ring cap: the second is refused.
        // Nothing else in this class ingests exemplars, so the first takes the only ring.
        var metrics = _factory.Services.GetRequiredService<MetricStorageEngine>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        metrics.Ingest([WithExemplar("diag.exemplar.first", now), WithExemplar("diag.exemplar.second", now)]);

        var after = await Diagnostics();
        Assert.Equal(Long(before, "tracesRingRefusedForBytes") + 1, Long(after, "tracesRingRefusedForBytes"));
        Assert.Equal(Long(before, "tracesRingRefusedNoSlot"),       Long(after, "tracesRingRefusedNoSlot"));
        Assert.Equal(Long(before, "tracesRingRefusedNoArena"),      Long(after, "tracesRingRefusedNoArena"));
        Assert.Equal(Long(before, "metricsExemplarMetricsRefused") + 1, Long(after, "metricsExemplarMetricsRefused"));
    }

    private static MetricIngestItem WithExemplar(string name, long nano) => new()
    {
        Name              = name,
        Kind              = MetricKind.Gauge,
        Unit              = "ms",
        Labels            = LabelSet.Empty,
        TimestampUnixNano = nano,
        ScalarValue       = 1,
        Exemplars         =
        [
            new MetricExemplar
            {
                TimestampUnixNano = nano,
                Value             = 1,
                TraceId           = "0af7651916cd43dd8448eb211c80319c",
                SpanId            = "b7ad6b7169203331",
            },
        ],
    };
}
