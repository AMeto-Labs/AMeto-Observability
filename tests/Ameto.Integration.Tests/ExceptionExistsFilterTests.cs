using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Storage;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>@x.exists</c> end to end. Two events go in over HTTP — one with <c>@x</c>, one without —
/// and are filtered both ways twice: while they sit in the hot tier, and again once the tier
/// has been flushed and the same filters run against a segment and its index.
///
/// <para>The filter reference has always listed the field, and the server evaluated it as a
/// user property no event has, so every one of these filters read zero events in both tiers.
/// Each spelling is therefore checked beside the <c>has(@x)</c> form that always worked: the
/// two must select the same events.</para>
///
/// <para>Both events are logged at the SAME level on purpose. A flush splits by level, and
/// only two rows in one index group can show the group being dropped for the row without an
/// exception — which is what an index hint for this field would do to
/// <c>@x.exists = false</c>.</para>
/// </summary>
public sealed class ExceptionExistsFilterTests : IClassFixture<AmetoWebAppFactory>
{
    private const string ProbePrefix   = "x-exists probe:";
    private const string Failed        = ProbePrefix + " settle failed";
    private const string Refused       = ProbePrefix + " settle refused";
    private const string ExceptionType = "System.InvalidOperationException";
    private const string InnerType     = "System.IO.IOException";

    private static readonly (string Filter, string Expected)[] Cases =
    [
        ("@x.exists = true",                                       Failed),
        ("has(@x)",                                                Failed),
        ("['@x.exists'] = true",                                   Failed),
        // The example in docs/FILTER_EXPRESSIONS.md.
        ($"@x.exists = true and @x.inner.type = '{InnerType}'",    Failed),
        ("@x.exists = false",                                      Refused),
        ("@x.exists != true",                                      Refused),
        ("not has(@x)",                                            Refused),
        ("@x.exists = false and @l = 'Error'",                     Refused),
    ];

    private readonly AmetoWebAppFactory _factory;
    private readonly HttpClient         _client;

    public ExceptionExistsFilterTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
    }

    [Fact]
    public async Task Exists_filters_both_ways_in_the_hot_tier_and_in_a_flushed_segment()
    {
        var now    = DateTimeOffset.UtcNow;
        var result = await TestHelpers.IngestAsync(_client,
            new LogEvent
            {
                Id              = new EventId(0, 0),
                Timestamp       = now,
                Level           = LogLevel.Error,
                MessageTemplate = Failed,
                Exception       = new ExceptionInfo
                {
                    Type       = ExceptionType,
                    Message    = "gateway timeout on settle",
                    StackTrace = "   at Billing.Settle()",
                    Inner      = new ExceptionInfo { Type = InnerType, Message = "disk" },
                },
            },
            TestHelpers.MakeEvent(Refused, LogLevel.Error, now.AddMilliseconds(1)));
        Assert.Equal(2, result.GetProperty("ingested").GetInt32());

        await TestHelpers.WaitForEventsAsync(_client, expectedCount: 2, filter: $"@mt like '{ProbePrefix}%'");

        // Every read is checked before anything is asserted, so a failure names all of them.
        var failures = new List<string>();
        var engine   = _factory.Services.GetRequiredService<StorageEngine>();

        Assert.True(engine.LiveHotTier.Count >= 2, "both events should be in the hot tier before the flush");
        await CheckAsync("hot tier", failures);

        await engine.FlushHotTierAsync();
        Assert.Equal(0, engine.LiveHotTier.Count);
        Assert.NotEmpty(engine.ListSegments());
        await CheckAsync("flushed segment", failures);

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    private async Task CheckAsync(string where, List<string> failures)
    {
        foreach (var (filter, expected) in Cases)
        {
            string[] got = await ProbeTemplatesAsync(filter);
            if (got is not [var only] || only != expected)
                failures.Add($"{where}: `{filter}` should select [{expected}], selected [{string.Join(", ", got)}]");
        }

        // The query the gap was found with: it read 0 events while `has(@x)` read the one.
        var byType = await AggregateRowsAsync("select count(*) where @x.exists = true group by @x.type");
        if (byType is not [var typed] || typed.Key != ExceptionType || typed.Count != 1)
            failures.Add($"{where}: counting `@x.exists = true` by @x.type should give [{ExceptionType}: 1], gave [{Describe(byType)}]");

        // A group key reads the field the way a predicate does, so the two cannot disagree.
        var byExists = await AggregateRowsAsync(
            $"select count(*) where @mt like '{ProbePrefix}%' group by @x.exists");
        Array.Sort(byExists, static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        if (byExists is not [("false", 1), ("true", 1)])
            failures.Add($"{where}: grouping the two events by @x.exists should give [false: 1, true: 1], gave [{Describe(byExists)}]");
    }

    /// <summary>
    /// The templates a filter selects, narrowed HERE to this test's two events rather than in
    /// the filter: what is under test is the predicate standing alone, with no other conjunct
    /// handing the index something to narrow by.
    /// </summary>
    private async Task<string[]> ProbeTemplatesAsync(string filter)
    {
        var events    = await TestHelpers.StreamEventsAsync(_client, TestHelpers.EventsUrl(filter: filter));
        var templates = new List<string>();
        foreach (var ev in events)
            if (ev.GetProperty("@mt").GetString() is { } mt && mt.StartsWith(ProbePrefix, StringComparison.Ordinal))
                templates.Add(mt);
        templates.Sort(StringComparer.Ordinal);
        return [.. templates];
    }

    private async Task<(string? Key, double Count)[]> AggregateRowsAsync(string query)
    {
        var resp = await _client.GetAsync("/api/events/aggregate?filter=" + Uri.EscapeDataString(query));
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(json.GetProperty("partial").GetBoolean(), $"`{query}` came back partial");

        var rows = new List<(string?, double)>();
        foreach (var row in json.GetProperty("rows").EnumerateArray())
            rows.Add((row.GetProperty("key")[0].GetString(), row.GetProperty("values")[0].GetDouble()));
        return [.. rows];
    }

    private static string Describe((string? Key, double Count)[] rows) =>
        string.Join(", ", rows.Select(static r => $"{r.Key ?? "null"}: {r.Count}"));
}
