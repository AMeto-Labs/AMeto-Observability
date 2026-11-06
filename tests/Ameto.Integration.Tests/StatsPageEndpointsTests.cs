using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Storage;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// The three aggregates the Statistics page draws: the log store's counters with the span they
/// cover (<c>GET /api/stats</c>), the trace stats with a chosen bucket count, the merged duration
/// histogram and the store's counters (<c>GET /api/traces/stats</c>), and the metric store's
/// catalogue bucketed over a window (<c>GET /api/metrics/stats</c>). Each over the real engine in
/// the real host; the metric one is also refused once its store has closed, as its siblings are.
/// </summary>
public sealed class StatsPageEndpointsTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    // ── Logs ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Stats_report_the_catalogue_and_the_span_of_time_it_covers()
    {
        using var factory = new AmetoWebAppFactory();
        var client  = factory.CreateClient();
        var storage = factory.Services.GetRequiredService<StorageEngine>();
        await storage.CatalogLoaded.WaitAsync(Patience);

        var empty = await client.GetFromJsonAsync<JsonElement>("/api/stats");
        Assert.Equal(0, empty.GetProperty("segments").GetInt32());
        Assert.Equal(0, empty.GetProperty("uncompressedBytes").GetInt64());
        Assert.Equal(JsonValueKind.Null, empty.GetProperty("oldestTimestamp").ValueKind);
        Assert.Equal(JsonValueKind.Null, empty.GetProperty("newestTimestamp").ValueKind);

        var oldest = DateTimeOffset.UtcNow.AddDays(-2);
        var newest = DateTimeOffset.UtcNow.AddMinutes(-1);
        await TestHelpers.IngestAsync(client,
            TestHelpers.MakeEvent("two days ago", ts: oldest),
            TestHelpers.MakeEvent("a minute ago", ts: newest));
        await TestHelpers.WaitForEventsAsync(client, 2);
        await storage.FlushHotTierAsync();

        // The flush publishes its segment a moment after it returns; the catalogue is read until it shows.
        JsonElement stats = default;
        var deadline = DateTimeOffset.UtcNow + Patience;
        while (true)
        {
            stats = await client.GetFromJsonAsync<JsonElement>("/api/stats");
            if (stats.GetProperty("segments").GetInt32() >= 1 || DateTimeOffset.UtcNow > deadline) break;
            await Task.Delay(50);
        }

        Assert.True(stats.GetProperty("segments").GetInt32() >= 1, stats.GetRawText());
        Assert.Equal(2, stats.GetProperty("totalEvents").GetInt64());
        Assert.True(stats.GetProperty("compressedBytes").GetInt64() > 0);
        Assert.True(stats.GetProperty("uncompressedBytes").GetInt64() > 0);
        var reportedOldest = DateTimeOffset.Parse(stats.GetProperty("oldestTimestamp").GetString()!);
        var reportedNewest = DateTimeOffset.Parse(stats.GetProperty("newestTimestamp").GetString()!);
        Assert.True((reportedOldest - oldest).Duration() < TimeSpan.FromSeconds(1), $"{reportedOldest} vs {oldest}");
        Assert.True((reportedNewest - newest).Duration() < TimeSpan.FromSeconds(1), $"{reportedNewest} vs {newest}");
    }

    // ── Traces ────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Trace_stats_take_the_bucket_count_and_carry_the_histogram_and_the_store()
    {
        using var factory = new AmetoWebAppFactory();
        var client = factory.CreateClient();
        var engine = factory.Services.GetRequiredService<TraceStorageEngine>();
        await engine.ColdLoadCompleted.WaitAsync(Patience);

        // Twenty spans of one service in the last 20 s: 1 ms each, one trace apiece.
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < 20; i++)
            Assert.True(engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0, (ulong)i + 1),
                SpanId            = new SpanId((ulong)i + 1),
                StartTimeUnixNano = now - (i + 1) * 1_000_000_000L,
                DurationNanos     = 1_000_000,
                Name              = "GET /orders",
                ServiceName       = "checkout",
                Kind              = SpanKind.Server,
            }));

        var stats = await client.GetFromJsonAsync<JsonElement>("/api/traces/stats?buckets=48");

        Assert.Equal(48, stats.GetProperty("totalSparkline").GetArrayLength());
        Assert.Equal(48, stats.GetProperty("errorSparkline").GetArrayLength());
        Assert.Equal(20, stats.GetProperty("totalTraces").GetInt32());
        Assert.Equal(20, stats.GetProperty("spanCount").GetInt64());
        Assert.Equal(1, stats.GetProperty("serviceCount").GetInt32());
        Assert.Equal(1.0, stats.GetProperty("maxLatencyMs").GetDouble());
        Assert.False(string.IsNullOrEmpty(stats.GetProperty("from").GetString()));

        var histogram = stats.GetProperty("histogram");
        Assert.Equal(HistogramBuckets.Count, histogram.GetProperty("counts").GetArrayLength());
        Assert.Equal(HistogramBuckets.Count - 1, histogram.GetProperty("boundsMs").GetArrayLength());
        Assert.Equal(1.0, histogram.GetProperty("boundsMs")[0].GetDouble());
        Assert.Equal(20, histogram.GetProperty("counts").EnumerateArray().Sum(c => c.GetInt64()));

        // Nothing has been flushed: the store is the hot tier, and its services are the window's.
        var store = stats.GetProperty("store");
        Assert.Equal(20, store.GetProperty("spans").GetInt64());
        Assert.Equal(20, store.GetProperty("hotSpans").GetInt32());
        Assert.Equal(0, store.GetProperty("segments").GetInt32());
        Assert.Equal(0, store.GetProperty("fileBytes").GetInt64());
        Assert.Equal(1, store.GetProperty("services").GetInt32());
        Assert.Equal(JsonValueKind.Null, store.GetProperty("oldestStartMs").ValueKind);

        // The column count is bounded, and the default is the Traces page's twenty.
        var capped = await client.GetFromJsonAsync<JsonElement>("/api/traces/stats?buckets=5000");
        Assert.Equal(2000, capped.GetProperty("totalSparkline").GetArrayLength());
        var plain = await client.GetFromJsonAsync<JsonElement>("/api/traces/stats");
        Assert.Equal(20, plain.GetProperty("totalSparkline").GetArrayLength());
    }

    // ── Metrics ───────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Metric_stats_aggregate_the_catalogue_the_segments_and_the_hot_tier()
    {
        using var factory = new AmetoWebAppFactory();
        var client = factory.CreateClient();
        var engine = factory.Services.GetRequiredService<MetricStorageEngine>();
        await engine.ColdLoadCompleted.WaitAsync(Patience);

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        long Ago(int seconds) => now - seconds * 1_000_000_000L;
        MetricIngestItem[] points =
        [
            new() { Name = "cpu.load", Kind = MetricKind.Gauge, TimestampUnixNano = Ago(1800), ScalarValue = 0.4 },
            new() { Name = "cpu.load", Kind = MetricKind.Gauge, TimestampUnixNano = Ago(1200), ScalarValue = 0.5 },
            new() { Name = "cpu.load", Kind = MetricKind.Gauge, TimestampUnixNano = Ago(600),  ScalarValue = 0.6 },
            new() { Name = "http.requests", Kind = MetricKind.Counter, Labels = new LabelSet([KeyValuePair.Create("env", "a")]), TimestampUnixNano = Ago(1500), ScalarValue = 10 },
            new() { Name = "http.requests", Kind = MetricKind.Counter, Labels = new LabelSet([KeyValuePair.Create("env", "a")]), TimestampUnixNano = Ago(900),  ScalarValue = 20 },
            new() { Name = "http.requests", Kind = MetricKind.Counter, Labels = new LabelSet([KeyValuePair.Create("env", "b")]), TimestampUnixNano = Ago(1500), ScalarValue = 1 },
            new() { Name = "http.requests", Kind = MetricKind.Counter, Labels = new LabelSet([KeyValuePair.Create("env", "b")]), TimestampUnixNano = Ago(900),  ScalarValue = 2 },
        ];
        Assert.Equal(0, engine.Ingest(points));
        await engine.ScheduleThresholdFlushForTest();   // the hot tier becomes cold segments
        Assert.Equal(0, engine.HotPointCount);
        // One more point stays hot, so the answer has both tiers to count.
        Assert.Equal(0, engine.Ingest([new MetricIngestItem { Name = "cpu.load", Kind = MetricKind.Gauge, TimestampUnixNano = Ago(10), ScalarValue = 0.7 }]));

        var from = DateTimeOffset.UtcNow.AddHours(-1);
        var to   = DateTimeOffset.UtcNow;
        var dto  = await client.GetFromJsonAsync<JsonElement>(
            $"/api/metrics/stats?from={Uri.EscapeDataString(from.ToString("O"))}&to={Uri.EscapeDataString(to.ToString("O"))}&bucket=600");

        Assert.Equal(600, dto.GetProperty("bucketSeconds").GetInt32());
        long expectedColumns = to.ToUnixTimeSeconds() / 600 - from.ToUnixTimeSeconds() / 600 + 1;
        Assert.Equal(expectedColumns, dto.GetProperty("buckets").GetArrayLength());

        var store = dto.GetProperty("store");
        Assert.Equal(2, store.GetProperty("metrics").GetInt32());
        Assert.Equal(3, store.GetProperty("series").GetInt64());          // cpu.load + two http.requests series
        Assert.True(store.GetProperty("segments").GetInt32() >= 1, dto.GetRawText());
        Assert.Equal(store.GetProperty("segments").GetInt32(), store.GetProperty("segmentsRaw").GetInt32());
        Assert.True(store.GetProperty("bytes").GetInt64() > 0);
        Assert.Equal(1, store.GetProperty("hotPoints").GetInt32());
        Assert.True(store.GetProperty("hotSeries").GetInt32() >= 1);
        Assert.NotEqual(JsonValueKind.Null, store.GetProperty("oldestMs").ValueKind);

        var kinds = dto.GetProperty("kinds").EnumerateArray().ToDictionary(k => k.GetProperty("kind").GetString()!);
        Assert.Equal(1, kinds["Gauge"].GetProperty("metrics").GetInt32());
        Assert.Equal(2, kinds["Counter"].GetProperty("series").GetInt64());

        var window = dto.GetProperty("window");
        Assert.Equal(2, window.GetProperty("metrics").GetInt32());
        long written = window.GetProperty("bytesWritten").GetInt64();
        Assert.True(written > 0);
        Assert.Equal(written, window.GetProperty("rawBytes").GetInt64());

        // The per-metric columns and `other` add up to exactly what the window says was written.
        var byMetric = dto.GetProperty("byMetric").EnumerateArray().ToList();
        Assert.Equal(["cpu.load", "http.requests"], byMetric.Select(m => m.GetProperty("name").GetString()!).Order().ToArray());
        long columns = byMetric.Sum(m => m.GetProperty("points").EnumerateArray().Sum(p => p.GetInt64()))
                     + dto.GetProperty("otherPoints").EnumerateArray().Sum(p => p.GetInt64());
        Assert.Equal(written, columns);
        Assert.Equal(written, byMetric.Sum(m => m.GetProperty("bytes").GetInt64()) + dto.GetProperty("otherBytes").GetInt64());
        Assert.Equal("Counter", byMetric.Single(m => m.GetProperty("name").GetString() == "http.requests").GetProperty("kind").GetString());
    }

    [Fact]
    public async Task Metric_stats_refuse_a_malformed_or_inverted_window()
    {
        using var factory = new AmetoWebAppFactory();
        var client = factory.CreateClient();

        using var inverted = await client.GetAsync("/api/metrics/stats?from=2026-11-06T00:00:00Z&to=2026-11-05T00:00:00Z");
        Assert.Equal(HttpStatusCode.BadRequest, inverted.StatusCode);
        using var malformed = await client.GetAsync("/api/metrics/stats?from=yesterday");
        Assert.Equal(HttpStatusCode.BadRequest, malformed.StatusCode);
    }

    /// <summary>The store's sentence once it has closed (#95), as every other metric read gives it.</summary>
    [Fact]
    public async Task Metric_stats_answer_503_once_the_store_has_closed()
    {
        using var factory = new AmetoWebAppFactory();
        var client = factory.CreateClient();
        var engine = factory.Services.GetRequiredService<MetricStorageEngine>();
        await engine.ColdLoadCompleted.WaitAsync(Patience);

        using var open = await client.GetAsync("/api/metrics/stats");
        Assert.Equal(HttpStatusCode.OK, open.StatusCode);

        await engine.DisposeAsync();   // the store closes; the host is not stopping
        Assert.Equal(QueryAvailability.Closed, engine.Availability);

        using var closed = await client.GetAsync("/api/metrics/stats");
        string body = await closed.Content.ReadAsStringAsync();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, closed.StatusCode);
        Assert.Contains("metric store has shut down", body);
    }
}
