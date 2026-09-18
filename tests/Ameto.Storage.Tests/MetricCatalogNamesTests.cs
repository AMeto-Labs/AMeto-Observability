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

    private static MetricIngestItem Point(string name, long nano, string series = "s") => new()
    {
        Name              = name,
        Kind              = MetricKind.Gauge,
        Labels            = new LabelSet(new Dictionary<string, string> { ["series"] = series }),
        TimestampUnixNano = nano,
        ScalarValue       = 1.0,
    };
}
