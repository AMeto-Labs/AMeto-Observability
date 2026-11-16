using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Ameto.Query.Filtering;

namespace Ameto.Integration.Tests;

/// <summary>
/// A filter that nests too deeply is a 400, on every endpoint that takes one.
///
/// <para>It used to be the end of the process. The parser reads a filter by recursion and
/// nothing counted the levels, so a few hundred opening parentheses — a filter of four hundred
/// to twelve hundred characters depending on the build, typed by any signed-in user into the
/// search box — overflowed the stack of the request thread. A stack overflow is not an
/// exception; nothing catches it, and the server was gone for everybody.</para>
///
/// <para>The limit lives in the parser and surfaces as its ordinary <see cref="FormatException"/>,
/// which each of these endpoints already turned into a 400 carrying the message. Nothing in the
/// endpoints changed; these hold them to that. A run that dies here with "Stack overflow"
/// instead of failing is the defect itself.</para>
///
/// <para>The test server has no request line limit, which is why ten thousand parentheses reach
/// the endpoint here. Kestrel answers a request line over 8 192 bytes with a 414 of its own, so
/// what a real client can deliver is about four thousand levels — the 250 below is that case.</para>
/// </summary>
public sealed class FilterNestingEndpointTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly HttpClient _client;

    public FilterNestingEndpointTests(AmetoWebAppFactory factory) => _client = factory.CreateClient();

    private static readonly string TooDeep = $"more than {FilterParser.MaxNestingDepth} levels deep";

    /// <summary>
    /// <paramref name="inner"/> inside <paramref name="depth"/> groups, as a query-string value.
    /// The parentheses go in as they are — they are legal there, and escaping twenty thousand of
    /// them would only test the length of a URL.
    /// </summary>
    private static string Nested(int depth, string inner)
        => new string('(', depth) + Uri.EscapeDataString(inner) + new string(')', depth);

    private static async Task<string> ErrorOf(HttpResponseMessage resp)
    {
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return json.GetProperty("error").GetString() ?? "";
    }

    // ── Search, live tail, validation ─────────────────────────────────────────

    [Theory]
    // 250 levels is the realistic one: 505 characters, well inside a request line, and past
    // the point where a Debug build's stack ran out. 10 000 is the one nobody could miss.
    [InlineData("/api/events", 250)]
    [InlineData("/api/events", 10_000)]
    [InlineData("/api/events/live", 250)]
    [InlineData("/api/events/live", 10_000)]
    [InlineData("/api/events/validate", 250)]
    [InlineData("/api/events/validate", 10_000)]
    public async Task A_filter_nested_too_deeply_is_a_400_that_says_so(string endpoint, int depth)
    {
        using var resp = await _client.GetAsync($"{endpoint}?filter={Nested(depth, "@l = 'Error'")}");

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        string error = await ErrorOf(resp);
        Assert.StartsWith("Invalid filter:", error, StringComparison.Ordinal);
        Assert.Contains(TooDeep, error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("/api/events")]
    [InlineData("/api/events/live")]
    [InlineData("/api/events/validate")]
    public async Task Words_inside_ten_thousand_parentheses_are_refused_not_searched_for(string endpoint)
    {
        // No comparison in it, so by the search box's rule this is free text — and free text
        // swallows the parser's refusals. Not this one.
        using var resp = await _client.GetAsync($"{endpoint}?filter={Nested(10_000, "connection timeout")}");

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains(TooDeep, await ErrorOf(resp), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("/api/events")]
    [InlineData("/api/events/validate")]
    public async Task A_run_of_not_is_held_to_the_same_limit(string endpoint)
    {
        string filter = string.Concat(Enumerable.Repeat("not ", 10_000)) + "@l = 'Error'";
        using var resp = await _client.GetAsync($"{endpoint}?filter={Uri.EscapeDataString(filter)}");

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains(TooDeep, await ErrorOf(resp), StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_deepest_filter_the_limit_allows_still_runs()
    {
        string filter = Nested(FilterParser.MaxNestingDepth, "@l = 'Error'");

        using var validate = await _client.GetAsync($"/api/events/validate?filter={filter}");
        Assert.Equal(HttpStatusCode.OK, validate.StatusCode);

        using var search = await _client.GetAsync($"/api/events?count=1&filter={filter}");
        Assert.Equal(HttpStatusCode.OK, search.StatusCode);
        Assert.Equal("text/event-stream", search.Content.Headers.ContentType?.MediaType);
    }

    // ── Aggregation ───────────────────────────────────────────────────────────

    [Theory]
    [InlineData(250)]
    [InlineData(10_000)]
    public async Task The_where_clause_of_an_aggregation_is_held_to_it_too(int depth)
    {
        string query = Uri.EscapeDataString("select count(*) where ")
                     + Nested(depth, "@l = 'Error'")
                     + Uri.EscapeDataString(" group by @service");

        // Asked where it belongs…
        using var aggregate = await _client.GetAsync($"/api/events/aggregate?filter={query}");
        Assert.Equal(HttpStatusCode.BadRequest, aggregate.StatusCode);
        string error = await ErrorOf(aggregate);
        Assert.StartsWith("Invalid aggregation:", error, StringComparison.Ordinal);
        Assert.Contains(TooDeep, error, StringComparison.Ordinal);

        // …and where it does not: the search endpoint parses it far enough to say what it is.
        using var search = await _client.GetAsync($"/api/events?filter={query}");
        Assert.Equal(HttpStatusCode.BadRequest, search.StatusCode);
        Assert.Contains(TooDeep, await ErrorOf(search), StringComparison.Ordinal);
    }

    // ── Alert rules ───────────────────────────────────────────────────────────
    // The one place a filter arrives in a request BODY, where no request line bounds its length.

    private static object Rule(string filter) => new
    {
        name      = "nested-too-deeply",
        source    = "Log",
        threshold = 1.0,
        filter,
        channels  = Array.Empty<object>(),
    };

    private static string NestedText(int depth, string inner)
        => new string('(', depth) + inner + new string(')', depth);

    [Theory]
    [InlineData("/api/alerts")]
    [InlineData("/api/alerts/preview")]
    public async Task An_alert_rule_cannot_carry_a_filter_nested_too_deeply(string endpoint)
    {
        using var resp = await _client.PostAsJsonAsync(endpoint, Rule(NestedText(10_000, "@l = 'Error'")));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        string error = await ErrorOf(resp);
        Assert.StartsWith("Invalid filter:", error, StringComparison.Ordinal);
        Assert.Contains(TooDeep, error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Changing_a_saved_rule_to_such_a_filter_is_refused_and_the_rule_keeps_the_one_it_had()
    {
        using var created = await _client.PostAsJsonAsync("/api/alerts", Rule("@l = 'Error'"));
        Assert.Equal(HttpStatusCode.Created, created.StatusCode);
        string id = (await created.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetString()!;
        try
        {
            using var updated = await _client.PutAsJsonAsync($"/api/alerts/{id}", Rule(NestedText(10_000, "@l = 'Error'")));
            Assert.Equal(HttpStatusCode.BadRequest, updated.StatusCode);
            Assert.Contains(TooDeep, await ErrorOf(updated), StringComparison.Ordinal);

            var stored = await _client.GetFromJsonAsync<JsonElement>($"/api/alerts/{id}");
            Assert.Equal("@l = 'Error'", stored.GetProperty("filter").GetString());
        }
        finally
        {
            using var _ = await _client.DeleteAsync($"/api/alerts/{id}");
        }
    }

    // ── A long chain is not nesting ───────────────────────────────────────────

    [Fact]
    public async Task The_longest_chain_a_request_line_can_carry_is_searched_not_refused()
    {
        // `''or''or''…`: four characters a clause, 2 040 of them in the 8 192 bytes Kestrel
        // allows a request line. Nothing about it is nested, so it is not refused — and the
        // tree it compiles to is balanced, so it does not cost a stack frame per clause either.
        string filter = string.Join("or", Enumerable.Repeat("''", 2_040));

        using var validate = await _client.GetAsync($"/api/events/validate?filter={Uri.EscapeDataString(filter)}");
        Assert.Equal(HttpStatusCode.OK, validate.StatusCode);

        using var search = await _client.GetAsync($"/api/events?count=1&filter={Uri.EscapeDataString(filter)}");
        Assert.Equal(HttpStatusCode.OK, search.StatusCode);
    }

    [Fact]
    public async Task An_alert_rule_whose_filter_is_a_chain_of_fifty_thousand_clauses_previews()
    {
        // Half a megabyte of filter in a body: more clauses than any stack could follow one
        // frame at a time. The preview compiles it and runs it against the store.
        string filter = string.Join(" or ", Enumerable.Range(0, 50_000).Select(i => $"Code = {i}"));

        using var resp = await _client.PostAsJsonAsync("/api/alerts/preview", Rule(filter));

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(json.GetProperty("wouldFire").GetBoolean());
    }
}
