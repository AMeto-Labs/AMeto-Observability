using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Ameto.Indexing;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>indexCacheBudgetBytes</c> in /api/diagnostics was recomputed from configuration on every
/// poll — <c>QueryOptions.EffectiveIndexCacheBytes</c>, a fresh <c>MemoryBudgets.Current()</c>
/// each time — rather than read from the cache. That cost a <c>GC.GetGCMemoryInfo</c> per poll and
/// could disagree with the budget the cache was actually built with, e.g. after
/// <c>GC.RefreshMemoryLimit</c> following a container resize. The endpoint now reports
/// <see cref="SegmentIndexCache.BudgetBytes"/>.
///
/// <para>To tell the two apart, this host's cache is built with a budget that neither
/// configuration nor this machine's memory would ever derive.</para>
/// </summary>
public sealed class DiagnosticsIndexCacheBudgetTests : IClassFixture<DiagnosticsIndexCacheBudgetTests.Factory>
{
    /// <summary>A budget no derivation produces: not a share of any real limit, not a configured default.</summary>
    public const long DistinctBudget = 12_345_678;

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureTestServices(static services =>
            {
                services.RemoveAll<SegmentIndexCache>();
                services.AddSingleton(new SegmentIndexCache(DistinctBudget, TimeSpan.Zero));
            });
        }
    }

    private readonly HttpClient _client;

    public DiagnosticsIndexCacheBudgetTests(Factory factory) => _client = factory.CreateClient();

    [Fact]
    public async Task Diagnostics_reports_the_budget_the_cache_enforces_not_a_fresh_derivation()
    {
        var json = await _client.GetFromJsonAsync<JsonElement>("/api/diagnostics");

        Assert.Equal(DistinctBudget, json.GetProperty("indexCacheBudgetBytes").GetInt64());
    }
}
