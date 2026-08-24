using System.Buffers;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Storage;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>GET /api/events/aggregate</c> answers with a table, which is why it is not the SSE
/// endpoint the search uses. These cover the wire shape, the refusals, and the one property
/// that matters most about an aggregate: that a partial answer says so, because unlike a
/// truncated list of events a wrong total looks exactly like a right one.
/// </summary>
public sealed class AggregationEndpointTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    private readonly HttpClient _client;

    public AggregationEndpointTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
        Seed();
    }

    private static bool _seeded;
    private static readonly object _seedLock = new();

    /// <summary>Two services at three levels, written once for the whole class.</summary>
    private void Seed()
    {
        lock (_seedLock)
        {
            if (_seeded) return;
            _seeded = true;

            var engine = _factory.Services.GetRequiredService<StorageEngine>();
            var buf    = new ArrayBufferWriter<byte>(64);

            for (int i = 0; i < 60; i++)
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(1);
                w.Write("Elapsed");
                w.Write((double)(i % 10));
                w.Flush();

                Assert.True(engine.TryWrite(new LogEventHeader
                {
                    TimestampUtcTicks        = DateTimeOffset.UtcNow.UtcTicks - (60 - i) * TimeSpan.TicksPerSecond,
                    Level                    = i % 3 == 0 ? LogLevel.Error : LogLevel.Information,
                    MessageTemplatePoolIndex = engine.TemplatePool.Intern("agg {n}"),
                    ServiceNamePoolIndex     = engine.TemplatePool.Intern(i % 2 == 0 ? "checkout" : "billing"),
                }, buf.WrittenSpan, "agg {n}"));
            }
        }
    }

    private async Task<JsonElement> AggregateAsync(string query, HttpStatusCode expected = HttpStatusCode.OK)
    {
        var resp = await _client.GetAsync("/api/events/aggregate?filter=" + Uri.EscapeDataString(query));
        Assert.Equal(expected, resp.StatusCode);
        return await resp.Content.ReadFromJsonAsync<JsonElement>();
    }

    [Fact]
    public async Task A_scalar_count_comes_back_as_one_row_with_one_column()
    {
        var json = await AggregateAsync("select count(*)");

        Assert.Empty(json.GetProperty("keyColumns").EnumerateArray());
        Assert.Equal("count", json.GetProperty("valueColumns")[0].GetString());

        var rows = json.GetProperty("rows").EnumerateArray().ToArray();
        Assert.Single(rows);
        Assert.True(rows[0].GetProperty("values")[0].GetDouble() >= 60);
        Assert.False(json.GetProperty("partial").GetBoolean());
    }

    [Fact]
    public async Task Grouping_names_its_key_column_and_returns_a_row_per_group()
    {
        var json = await AggregateAsync("select count(*) group by ['service.name'] as service");

        Assert.Equal("service", json.GetProperty("keyColumns")[0].GetString());

        var rows = json.GetProperty("rows").EnumerateArray().ToArray();
        Assert.Contains(rows, r => r.GetProperty("key")[0].GetString() == "checkout");
        Assert.Contains(rows, r => r.GetProperty("key")[0].GetString() == "billing");
    }

    [Fact]
    public async Task A_where_clause_narrows_it()
    {
        var all      = await AggregateAsync("select count(*)");
        var filtered = await AggregateAsync("select count(*) where @l = 'Error'");

        double total = all.GetProperty("rows")[0].GetProperty("values")[0].GetDouble();
        double errs  = filtered.GetProperty("rows")[0].GetProperty("values")[0].GetDouble();

        Assert.True(errs > 0);
        Assert.True(errs < total);
    }

    [Fact]
    public async Task An_aggregate_with_no_numbers_serialises_as_null_not_zero()
    {
        var json = await AggregateAsync("select avg(Nonexistent)");
        Assert.Equal(JsonValueKind.Null, json.GetProperty("rows")[0].GetProperty("values")[0].ValueKind);
    }

    [Fact]
    public async Task A_limited_answer_still_reports_how_many_groups_there_were()
    {
        var json = await AggregateAsync("select count(*) group by ['service.name'] limit 1");

        Assert.Single(json.GetProperty("rows").EnumerateArray());
        Assert.Equal(2, json.GetProperty("groupsFound").GetInt32());
    }

    // ── Refusals ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_filter_that_is_not_an_aggregation_is_turned_away_with_an_example()
    {
        var json = await AggregateAsync("@l = 'Error'", HttpStatusCode.BadRequest);
        Assert.Contains("select", json.GetProperty("error").GetString(), StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("select nonsense(x)")]
    [InlineData("select count(*) group")]
    [InlineData("select count(*) where Level = ")]
    public async Task A_malformed_aggregation_is_a_400_with_the_reason(string query)
    {
        var json = await AggregateAsync(query, HttpStatusCode.BadRequest);
        Assert.Contains("Invalid aggregation", json.GetProperty("error").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_search_endpoint_points_an_aggregation_at_the_right_door()
    {
        // Typing `select …` into the box hits /api/events first. The parser's own report would
        // be about a `select` it has never heard of; this says where the query belongs.
        var resp = await _client.GetAsync(
            "/api/events?filter=" + Uri.EscapeDataString("select count(*) group by @l"));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Contains("/api/events/aggregate", json.GetProperty("error").GetString(), StringComparison.Ordinal);
    }
}
