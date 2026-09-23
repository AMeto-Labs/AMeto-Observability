using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// The label filter every metric read applies per series (issue #83 WP7, M#4(c)): the cold reader,
/// the hot tier and the exemplar ring share <see cref="MetricReader.MatchesLabels"/>. It used to
/// build a dictionary of the series' pairs per series per query; it now walks the sorted pairs.
/// What it answers is pinned by <c>MetricQueryGoldenTests</c>; this pins what it costs and the
/// corners of the '|' option list in words.
/// </summary>
public sealed class MetricLabelMatcherTests
{
    private static LabelSet L(params string[] kv)
    {
        var pairs = new List<KeyValuePair<string, string>>();
        for (int i = 0; i < kv.Length; i += 2) pairs.Add(new(kv[i], kv[i + 1]));
        return new LabelSet(pairs);
    }

    private static readonly LabelSet Http = L(
        "service.name", "Checkout", "http.route", "/orders/{id}", "http.request.method", "GET",
        "http.response.status_code", "200", "server.address", "host-7");

    [Fact]
    public void A_filtered_series_costs_no_allocation()
    {
        var matchers = new Dictionary<string, string>
        {
            ["http.request.method"] = "GET|POST",
            ["service.name"]        = "Checkout",
            ["server.address"]      = "host-1|host-7",
        };
        IReadOnlyDictionary<string, string> asInterface = matchers;

        Assert.True(MetricReader.MatchesLabels(Http, asInterface));   // warm, and the answer

        long before = GC.GetAllocatedBytesForCurrentThread();
        int hits = 0;
        for (int i = 0; i < 10_000; i++)
            if (MetricReader.MatchesLabels(Http, asInterface)) hits++;
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(10_000, hits);
        // The dictionary of pairs per call was ~560 B; Split('|') another array and string per option.
        Assert.True(bytes < 1_024, $"10 000 filtered series allocated {bytes} B — a per-series allocation is back");
    }

    [Theory]
    [InlineData("GET",        true)]
    [InlineData("get",        false)]    // ordinal
    [InlineData("POST|GET",   true)]
    [InlineData("POST|PUT",   false)]
    [InlineData("GET|",       true)]
    [InlineData("|GET",       true)]
    [InlineData("POST||PUT",  false)]
    [InlineData("",           false)]
    public void A_value_matches_exactly_or_one_of_its_options(string matcher, bool expected) =>
        Assert.Equal(expected, MetricReader.MatchesLabels(Http, new Dictionary<string, string> { ["http.request.method"] = matcher }));

    [Fact]
    public void An_empty_option_accepts_an_empty_value_and_a_bar_in_the_value_is_not_an_option()
    {
        var empty = L("k", "");
        Assert.True(MetricReader.MatchesLabels(empty,  new Dictionary<string, string> { ["k"] = "" }));
        Assert.True(MetricReader.MatchesLabels(empty,  new Dictionary<string, string> { ["k"] = "a||b" }));
        Assert.True(MetricReader.MatchesLabels(empty,  new Dictionary<string, string> { ["k"] = "a|" }));
        Assert.False(MetricReader.MatchesLabels(empty, new Dictionary<string, string> { ["k"] = "a|b" }));

        // A matcher with a bar is an option list, so a value that itself contains the bar can only
        // be matched by an exact matcher without one — as Split('|') had it.
        var barred = L("k", "a|b");
        Assert.False(MetricReader.MatchesLabels(barred, new Dictionary<string, string> { ["k"] = "a|b" }));
        Assert.False(MetricReader.MatchesLabels(barred, new Dictionary<string, string> { ["k"] = "a" }));
    }

    [Fact]
    public void Every_matcher_must_hold_and_an_absent_key_fails()
    {
        Assert.True(MetricReader.MatchesLabels(Http, new Dictionary<string, string>()));
        Assert.False(MetricReader.MatchesLabels(Http, new Dictionary<string, string> { ["zzz"] = "x" }));      // past every key
        Assert.False(MetricReader.MatchesLabels(Http, new Dictionary<string, string> { ["aaa"] = "x" }));      // before every key
        Assert.False(MetricReader.MatchesLabels(Http, new Dictionary<string, string> { ["http.routE"] = "/orders/{id}" }));
        Assert.False(MetricReader.MatchesLabels(Http, new Dictionary<string, string>
        {
            ["service.name"] = "Checkout", ["http.response.status_code"] = "500",
        }));
        Assert.False(MetricReader.MatchesLabels(LabelSet.Empty, new Dictionary<string, string> { ["k"] = "" }));

        // Through the interface, a non-Dictionary implementation takes the same road.
        IReadOnlyDictionary<string, string> sorted = new SortedDictionary<string, string> { ["http.route"] = "/orders/{id}" };
        Assert.True(MetricReader.MatchesLabels(Http, sorted));
    }
}
