using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE CATALOG OFFERS ONLY THE VALUE A FILTER MATCHES (#92, PR #101 review F4).
///
/// <para>A series stored before ingest collapsed repeated label keys carries a key twice. The answers
/// and every filter use its ordinal-greatest value; the catalog recorded BOTH, so
/// <c>/labels/{key}/values</c> offered a value that selects nothing — pick it in the Explore filter
/// and the panel is empty. Both catalog walks — a series first seen live (<c>RegisterMeta</c>) and
/// the seed from cold files at startup (<c>SeedCatalogFromCold</c>) — now record the last of the
/// key's run only.</para>
/// </summary>
public sealed class MetricCatalogRepeatedKeyTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mcatdup-" + Guid.NewGuid().ToString("N"));
    private MetricStorageEngine? _engine;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_engine is not null) await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>What an exporter sending <c>k</c> twice left in storage before the fix.</summary>
    private static readonly LabelSet Legacy = new([new("k", "v1"), new("k", "v2"), new("z", "1")]);

    [Fact]
    public void A_series_first_seen_live_offers_the_value_a_filter_matches()
    {
        _engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance, new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });
        _engine.Ingest([new MetricIngestItem { Name = "cat.dup", Kind = MetricKind.Gauge, Labels = Legacy,
                                               TimestampUnixNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L, ScalarValue = 1 }]);

        Assert.Equal(["v2"], _engine.GetLabelValues("cat.dup", "k"));
        Assert.Equal(["1"],  _engine.GetLabelValues("cat.dup", "z"));
        Assert.Equal(["k", "z"], _engine.GetLabelKeys("cat.dup"));
    }

    [Fact]
    public async Task A_series_seeded_from_a_cold_file_offers_the_value_a_filter_matches()
    {
        MetricWriter.Write(_dir, [(new SeriesKey("cat.cold.dup", MetricKind.Gauge, "", Legacy),
                                   new HotSeries([new MetricDataPoint { TimestampUnixNano = 1_784_800_020_000_000_000L, Value = 1 }]))],
                           MetricGranularity.Raw);

        _engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        await _engine.ColdLoadCompleted;

        Assert.Equal(["v2"], _engine.GetLabelValues("cat.cold.dup", "k"));
        Assert.Equal(["1"],  _engine.GetLabelValues("cat.cold.dup", "z"));
    }
}
