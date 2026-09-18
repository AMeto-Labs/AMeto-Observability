using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// <c>GET /api/metrics/names</c> is the Explore page's first request, and it used to read
/// <c>_hot.Keys</c> — which on a <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/>
/// acquires EVERY lock in the table and materialises a list of every SERIES. At the sandbox's
/// 38 741 series that is ~1.2 MB allocated and every ingest thread blocked for the duration, per
/// page load, to produce a few dozen distinct names.
/// </summary>
public sealed class MetricCatalogNamesTests
{
    private readonly ITestOutputHelper _out;
    public MetricCatalogNamesTests(ITestOutputHelper o) => _out = o;

    [Fact]
    public async Task The_names_are_the_same_set_hot_cold_and_after_a_drain()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mnames-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            engine.Ingest([Point("http.server.duration", baseNano),
                           Point("http.client.duration", baseNano),
                           Point("process.cpu.time",     baseNano)]);

            Assert.Equal(["http.client.duration", "http.server.duration", "process.cpu.time"],
                         engine.GetMetricNames().ToArray());

            // Prefix filtering is case-insensitive and the order is the sorted one, both as before.
            Assert.Equal(["http.client.duration", "http.server.duration"],
                         engine.GetMetricNames("HTTP.").ToArray());
            Assert.Empty(engine.GetMetricNames("nothing."));

            // After the drain the hot tier holds no points and — past the stale bar — would hold
            // no series either. The names must survive both, which is what _meta is for.
            await engine.ScheduleThresholdFlushForTest();
            Assert.Equal(0, engine.HotPointCount);
            Assert.Equal(["http.client.duration", "http.server.duration", "process.cpu.time"],
                         engine.GetMetricNames().ToArray());

            // And a name that exists ONLY on disk — a restart, where the catalog is seeded from
            // the cold segments — is still listed.
            await engine.DisposeAsync();
            await using var reopened = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            await reopened.ColdLoadCompleted;
            Assert.Equal(["http.client.duration", "http.server.duration", "process.cpu.time"],
                         reopened.GetMetricNames().ToArray());
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// The cost, at a cardinality the sandbox actually reaches. Two names across 20 000 series:
    /// the answer is two strings, and what it takes to produce them must not scale with the
    /// series count.
    /// </summary>
    [Fact]
    public async Task Listing_the_names_does_not_walk_the_hot_tier()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mnamescost-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            const int series = 20_000;
            var batch = new MetricIngestItem[1_000];
            for (int done = 0; done < series; done += batch.Length)
            {
                for (int i = 0; i < batch.Length; i++)
                    batch[i] = Point(((done + i) & 1) == 0 ? "http.server.duration" : "process.cpu.time",
                                     baseNano, "series-" + (done + i));
                engine.Ingest(batch);
            }
            Assert.Equal(series, engine.HotSeriesCount);

            // Warm the path, then measure it: the first call JITs and grows nothing after.
            _ = engine.GetMetricNames().ToArray();

            long before = GC.GetAllocatedBytesForCurrentThread();
            var  names  = engine.GetMetricNames().ToArray();
            long cost   = GC.GetAllocatedBytesForCurrentThread() - before;

            _out.WriteLine($"{series:N0} series, {names.Length} names: {cost:N0} B allocated per call");
            Assert.Equal(2, names.Length);
            Assert.True(cost < 64 * 1024,
                $"listing {names.Length} names over {series:N0} series allocated {cost:N0} B — "
              + "the hot tier is being walked");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// The catalog walk runs once per SERIES, not once per point. It used to do
    /// <c>_meta.GetOrAdd</c> plus a nested <c>GetOrAdd</c> and a <c>ContainsKey</c> per label per
    /// data point — four to eight concurrent-dictionary lookups on the ingest hot path, in a
    /// state where nothing can have changed: the label set IS the series identity.
    /// </summary>
    [Fact]
    public async Task The_catalog_is_walked_once_per_series_and_not_once_per_point()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mmeta-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            const int series = 100, rounds = 100;
            var batch = new MetricIngestItem[series];
            for (int r = 0; r < rounds; r++)
            {
                for (int s = 0; s < series; s++)
                    batch[s] = Point("meta.metric", baseNano + r * 1_000_000L, "series-" + s);
                engine.Ingest(batch);
            }

            _out.WriteLine($"{series * rounds:N0} points over {series} series: {engine.MetaRegistrations} catalog walks");
            Assert.Equal(series, engine.MetaRegistrations);

            // And the catalog still says everything it said before.
            var entry = engine.GetCatalog().Single(e => e.Name == "meta.metric");
            Assert.Equal(MetricKind.Gauge, entry.Kind);
            Assert.Equal(series, entry.Cardinality);
            Assert.Equal(["series"], entry.LabelKeys);
            Assert.Equal(series, engine.GetLabelValues("meta.metric", "series").Count);

            // LastSeenMs is the one field that genuinely moves, and it moved.
            Assert.Equal((baseNano + (rounds - 1) * 1_000_000L) / 1_000_000L, entry.LastSeenMs);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// Exemplars are optional in OTLP and most exporters send none, but the pass that files them
    /// re-walked the WHOLE batch regardless — re-testing every item's timestamp against the
    /// future limit a second time, to discover item by item that there was nothing there.
    /// </summary>
    [Fact]
    public async Task A_batch_that_carries_no_exemplars_is_not_walked_a_second_time()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mexem-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            var plain = new MetricIngestItem[500];
            for (int i = 0; i < plain.Length; i++) plain[i] = Point("exemplar.metric", baseNano + i, "s" + i);
            engine.Ingest(plain);
            engine.Ingest(plain);
            Assert.Equal(0, engine.ExemplarPasses);

            // One item with an exemplar is enough to earn the pass — and the exemplar lands.
            var withOne = new MetricIngestItem[3];
            for (int i = 0; i < withOne.Length; i++) withOne[i] = Point("exemplar.metric", baseNano + i, "s" + i);
            withOne[1] = new MetricIngestItem
            {
                Name              = "exemplar.metric",
                Kind              = MetricKind.Gauge,
                Labels            = new LabelSet(new Dictionary<string, string> { ["series"] = "s1" }),
                TimestampUnixNano = baseNano + 1,
                ScalarValue       = 1.0,
                Exemplars         = [new MetricExemplar { TimestampUnixNano = baseNano + 1, Value = 7.0, TraceId = "abc", SpanId = "def" }],
            };
            engine.Ingest(withOne);

            Assert.Equal(1, engine.ExemplarPasses);
            var exemplars = engine.GetExemplars("exemplar.metric", null, null, null);
            Assert.Equal(7.0, Assert.Single(exemplars).Value);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    private static MetricIngestItem Point(string name, long nano, string series = "s") => new()
    {
        Name              = name,
        Kind              = MetricKind.Gauge,
        Labels            = new LabelSet(new Dictionary<string, string> { ["series"] = series }),
        TimestampUnixNano = nano,
        ScalarValue       = 1.0,
    };
}
