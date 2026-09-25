using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// POST /api/alerts/preview FOR A WINDOW WITH NO FINITE VALUE CLAIMS NO VERDICT (#92).
///
/// <para>The evaluator leaves a metric rule's state alone when every point of its window is NaN or
/// infinite. The preview used to answer 0 for that window and compute <c>wouldFire</c> from it, so a
/// "&lt; 5" rule previewed as WOULD FIRE while the evaluator would never fire it. It answers
/// <c>value: null, wouldFire: false</c> now — the editor prints "—".</para>
/// </summary>
public sealed class AlertPreviewNonFiniteTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public AlertPreviewNonFiniteTests(AmetoWebAppFactory factory) => _factory = factory;

    private void Seed(string metric, params double[] values)
    {
        var engine = _factory.Services.GetRequiredService<MetricStorageEngine>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var items = new MetricIngestItem[values.Length];
        for (int i = 0; i < values.Length; i++)
            items[i] = new MetricIngestItem
            {
                Name = metric, Kind = MetricKind.Gauge, Unit = "1", Labels = new LabelSet([new("service.name", "preview")]),
                TimestampUnixNano = now - (values.Length - i) * 10_000_000_000L, ScalarValue = values[i],
            };
        engine.Ingest(items);
    }

    private async Task<string> Preview(string metric)
    {
        var admin = _factory.CreateClient();
        admin.DefaultRequestHeaders.Add(TestAuthHandler.RoleHeader, "admin");
        using var resp = await admin.PostAsJsonAsync("/api/alerts/preview", new
        {
            name = "preview", source = "Metric", metric, aggregation = "max",
            comparator = "LessThan", threshold = 5.0, windowSeconds = 600,
        });
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        return await resp.Content.ReadAsStringAsync();
    }

    [Fact]
    public async Task A_window_of_NaN_previews_no_value_and_no_verdict()
    {
        Seed("preview.nan.gauge", double.NaN, double.PositiveInfinity);
        Assert.Equal("""{"value":null,"threshold":5,"wouldFire":false}""", await Preview("preview.nan.gauge"));
    }

    [Fact]
    public async Task A_finite_window_previews_its_value_and_verdict_as_before()
    {
        Seed("preview.finite.gauge", 3, double.NaN);
        Assert.Equal("""{"value":3,"threshold":5,"wouldFire":true}""", await Preview("preview.finite.gauge"));
    }
}
