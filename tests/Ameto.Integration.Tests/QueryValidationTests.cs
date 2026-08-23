using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Ameto.Integration.Tests;

/// <summary>
/// A search that cannot run has to say so. Both stream endpoints used to set the SSE
/// headers and flush BEFORE parsing anything, so a malformed date or a filter with a
/// syntax error arrived as 200 OK followed by a stream that stopped — indistinguishable
/// from "no results" — while the parser's own diagnostic was right there. Everything that
/// can be rejected is now checked before the response is committed.
/// </summary>
public sealed class QueryValidationTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly HttpClient _client;

    public QueryValidationTests(AmetoWebAppFactory factory) => _client = factory.CreateClient();

    private static async Task<string> ErrorOf(HttpResponseMessage resp)
    {
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return json.GetProperty("error").GetString() ?? "";
    }

    /// <summary>An unclosed group — the parser's own <c>Expected RParen</c>.</summary>
    private const string BadFilter = "(@l = 'Error'";

    [Fact]
    public async Task A_filter_with_a_syntax_error_is_400_with_the_parser_message()
    {
        var resp = await _client.GetAsync("/api/events?filter=" + Uri.EscapeDataString(BadFilter));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("Invalid filter", await ErrorOf(resp), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task An_invalid_regex_is_reported_as_a_filter_error_not_a_dead_stream()
    {
        var resp = await _client.GetAsync(
            "/api/events?filter=" + Uri.EscapeDataString("regexMatch(Region, '([unclosed')"));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("Invalid filter", await ErrorOf(resp), StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("from=not-a-date", "from")]
    [InlineData("to=yesterday",    "to")]
    public async Task A_malformed_timestamp_names_the_parameter(string query, string expectedName)
    {
        var resp = await _client.GetAsync($"/api/events?{query}");

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains(expectedName, await ErrorOf(resp), StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_inverted_window_is_rejected_rather_than_silently_returning_nothing()
    {
        var resp = await _client.GetAsync(
            "/api/events?from=2026-08-02T00:00:00Z&to=2026-08-01T00:00:00Z");

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("later than", await ErrorOf(resp), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task The_live_tail_validates_before_it_opens_the_stream()
    {
        var resp = await _client.GetAsync("/api/events/live?filter=" + Uri.EscapeDataString(BadFilter));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("Invalid filter", await ErrorOf(resp), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task A_valid_query_still_streams()
    {
        var resp = await _client.GetAsync("/api/events?count=1&filter=" + Uri.EscapeDataString("@l = 'Error'"));

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("text/event-stream", resp.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task The_counts_chart_rejects_an_inverted_window_like_every_other_endpoint()
    {
        // It used to swap them silently, answering a different question than the one asked.
        var resp = await _client.GetAsync(
            "/api/events/counts?from=2026-08-02T00:00:00Z&to=2026-08-01T00:00:00Z");

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("later than", await ErrorOf(resp), StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("/api/spans/00000000000000ff/logs?from=not-a-date")]
    [InlineData("/api/traces/000000000000000000000000000000ff/logs?to=whenever")]
    [InlineData("/api/events/counts?from=nope")]
    public async Task Every_query_endpoint_names_a_malformed_timestamp(string url)
    {
        var resp = await _client.GetAsync(url);

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("ISO-8601", await ErrorOf(resp), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Diagnostics_reports_what_ingest_accepted_and_why_it_dropped()
    {
        // Overload used to be invisible server-side: a client saw its own "dropped" count
        // and nobody could see the ring filling, let alone which limit was hit.
        var resp = await _client.GetAsync("/api/diagnostics");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        foreach (var field in new[]
                 {
                     "ingestAcceptedTotal", "ingestDrainedTotal", "ingestPending", "ingestCapacity",
                     "ingestSlabCapacity",
                     "ingestDroppedOversized", "ingestDroppedNoSlab", "ingestDroppedRingFull",
                     "ingestWriteErrorDrops",
                 })
        {
            Assert.True(json.TryGetProperty(field, out var value), $"missing '{field}'");
            Assert.True(value.GetInt64() >= 0, $"'{field}' should be a non-negative counter");
        }
        Assert.True(json.GetProperty("ingestCapacity").GetInt64() > 0);

        // Saturation is measured against the SLABS, which are what a burst runs out of —
        // there are fewer of them than ring slots, so a full buffer must not read as idle.
        Assert.True(json.GetProperty("ingestSlabCapacity").GetInt64() > 0);
        Assert.True(json.GetProperty("ingestSlabCapacity").GetInt64() <= json.GetProperty("ingestCapacity").GetInt64());
        Assert.True(json.TryGetProperty("ingestSaturationPercent", out var sat));
        Assert.True(sat.GetDouble() >= 0);
    }

    // ── The validation endpoint the browser falls back to ─────────────────────
    // EventSource cannot read the body of a non-200, so the UI asks here when a stream
    // dies before delivering anything.

    [Fact]
    public async Task Validate_reports_ok_for_a_usable_query()
    {
        var resp = await _client.GetAsync(
            "/api/events/validate?filter=" + Uri.EscapeDataString("@l = 'Error' and contains(@mt, 'x')"));

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(json.GetProperty("ok").GetBoolean());
    }

    [Fact]
    public async Task Validate_returns_the_same_diagnostic_the_stream_refused_with()
    {
        string bad = Uri.EscapeDataString(BadFilter);
        var stream   = await _client.GetAsync($"/api/events?filter={bad}");
        var validate = await _client.GetAsync($"/api/events/validate?filter={bad}");

        Assert.Equal(HttpStatusCode.BadRequest, stream.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, validate.StatusCode);
        Assert.Equal(await ErrorOf(stream), await ErrorOf(validate));
    }
}
