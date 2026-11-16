using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// A filter is text from outside, and the parser reads it by recursion: a level of stack for
/// every parenthesis that is open and every <c>not</c>. With nothing counting them the text
/// chose the depth, and a few hundred opening parentheses — a filter shorter than this
/// paragraph — overflowed the stack. That cannot be caught; it ends the process, and on the
/// server the process is everybody's.
///
/// <para>So the depth is counted, and one level past <see cref="FilterParser.MaxNestingDepth"/>
/// is a <see cref="FormatException"/> like every other thing the parser refuses, which the
/// endpoints already answer with a 400.</para>
///
/// <para>Every test below that goes past the limit used to end the test host rather than fail.
/// If one of them takes the whole run down with "Stack overflow", that is this limit not
/// being applied somewhere the parser recurses.</para>
/// </summary>
public sealed class FilterNestingLimitTests
{
    private const int Limit = FilterParser.MaxNestingDepth;

    private static string Open(int n)  => new('(', n);
    private static string Close(int n) => new(')', n);

    private static string Times(string s, int n) => string.Concat(Enumerable.Repeat(s, n));

    private static FormatException Rejects(string filter)
        => Assert.Throws<FormatException>(() => CompiledFilter.Compile(filter));

    private static LogEvent Event(params (string Key, object Value)[] props)
    {
        var map = new Dictionary<string, object?>(props.Length);
        foreach (var (k, v) in props) map[k] = v;
        return new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 12, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Error,
            MessageTemplate = "something happened",
            Properties      = map,
        };
    }

    // ── The limit itself ──────────────────────────────────────────────────────

    [Fact]
    public void The_last_level_under_the_limit_parses_and_means_what_it_says()
    {
        var f = CompiledFilter.Compile(Open(Limit) + "A = 1" + Close(Limit));

        Assert.True(f.Matches(Event(("A", 1L))));
        Assert.False(f.Matches(Event(("A", 2L))));
    }

    [Fact]
    public void One_level_past_the_limit_is_refused_and_the_sentence_says_what_to_do()
    {
        var ex = Rejects(Open(Limit + 1) + "A = 1" + Close(Limit + 1));

        // How deep is too deep, where the level that broke it opens, what counts, what to do.
        Assert.Contains($"more than {Limit} levels deep", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"at pos {Limit}", ex.Message, StringComparison.Ordinal);
        Assert.Contains("each '(' and each 'not' is a level", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Simplify the grouping", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_not_is_a_level_too()
    {
        // `not not not …` recurses in the parser exactly as a parenthesis does, and each one is
        // a level of the tree that everything downstream then descends.
        var f = CompiledFilter.Compile(Times("not ", Limit) + "A = 1");
        Assert.True(f.Matches(Event(("A", 1L))));          // an even number of negations

        var ex = Rejects(Times("not ", Limit + 1) + "A = 1");
        Assert.Contains($"more than {Limit} levels deep", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"at pos {4 * Limit}", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Groups_and_nots_share_the_one_count()
    {
        // `not (` is two levels.
        string ok = Times("not (", Limit / 2) + "A = 1" + Close(Limit / 2);
        Assert.True(CompiledFilter.Compile(ok).Matches(Event(("A", 1L))));

        Rejects(Times("not (", Limit / 2) + "not A = 1" + Close(Limit / 2));
    }

    // ── Why the limit is where it is ──────────────────────────────────────────

    [Fact]
    public void A_filter_grown_a_click_at_a_time_nests_once_per_click_and_a_hundred_clicks_fit()
    {
        // What the events page does when a condition is added from an event: "exclude these"
        // writes `(current filter) and @mt <> '…'`. Nobody types a hundred levels, but a
        // session of clicking arrives at one a level at a time — so the limit has to be past
        // anything a person would click through, and it is about what a request line holds.
        string filter = "@l = 'Error'";
        for (int click = 1; click <= Limit; click++)
            filter = $"({filter}) and @mt <> 'Noise {click}'";

        var f = CompiledFilter.Compile(filter);

        static LogEvent Templated(string template) => new()
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 12, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Error,
            MessageTemplate = template,
        };
        Assert.True(f.Matches(Templated("Order {Id} shipped")));
        Assert.False(f.Matches(Templated("Noise 1")));
        Assert.False(f.Matches(Templated($"Noise {Limit}")));

        // The click after that is refused, and says why.
        var ex = Rejects($"({filter}) and @mt <> 'One more'");
        Assert.Contains($"more than {Limit} levels deep", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void So_do_a_hundred_clicks_that_alternate_and_with_or()
    {
        // The property menu offers both joiners. Alternating them is the one way clicking
        // makes the TREE deep as well as the text: a level per click, a hundred in all.
        string filter = "P0 = 1";
        for (int click = 1; click <= Limit; click++)
            filter = $"({filter}) {(click % 2 == 0 ? "and" : "or")} P{click} = 1";

        var f = CompiledFilter.Compile(filter);

        // The last two clicks were `… or P99 = 1` and then `… and P100 = 1`.
        Assert.True(f.Matches(Event(($"P{Limit - 1}", 1L), ($"P{Limit}", 1L))));
        Assert.False(f.Matches(Event(($"P{Limit}", 1L))));
        Assert.False(f.Matches(Event(($"P{Limit - 1}", 1L))));
    }

    // ── What is counted is what is OPEN ───────────────────────────────────────

    [Fact]
    public void A_closed_group_gives_its_level_back()
    {
        // Three hundred groups, none inside another: the depth is one throughout. A counter
        // that only ever went up would refuse the sixty-fifth.
        string filter = string.Join(" and ", Enumerable.Repeat("(A = 1)", 300));
        Assert.True(CompiledFilter.Compile(filter).Matches(Event(("A", 1L))));

        // …and the same for a group that went all the way down first.
        string deepTwice = Open(Limit) + "A = 1" + Close(Limit) + " and " + Open(Limit) + "B = 2" + Close(Limit);
        var f = CompiledFilter.Compile(deepTwice);
        Assert.True(f.Matches(Event(("A", 1L), ("B", 2L))));
        Assert.False(f.Matches(Event(("A", 1L), ("B", 3L))));
    }

    [Fact]
    public void A_not_gives_its_level_back_when_its_operand_ends()
    {
        string filter = string.Join(" and ", Enumerable.Repeat("not A = 2", 300));
        Assert.True(CompiledFilter.Compile(filter).Matches(Event(("A", 1L))));
    }

    [Fact]
    public void The_parentheses_of_a_function_and_the_brackets_of_a_path_are_not_levels()
    {
        // Only a GROUP re-enters the grammar. `has(…)`, `in […]`, `A['k'][0]` and the infix
        // `not in` are read where they stand, so a filter at the limit can still hold them.
        string inner = "has(A) and A in [1, 2, 3] and B['k'][0] = 5 and C not in ['x'] and length(S) > 2";
        var f = CompiledFilter.Compile(Open(Limit) + inner + Close(Limit));

        var ev = Event(
            ("A", 1L),
            ("B", new Dictionary<string, object?> { ["k"] = new List<object?> { 5L } }),
            ("C", "y"),
            ("S", "abc"));
        Assert.True(f.Matches(ev));
    }

    [Fact]
    public void Parentheses_inside_a_string_literal_are_text()
    {
        // The count is of tokens the parser descends into, not of characters. Five hundred
        // parentheses in a VALUE are a value.
        string value = Open(500) + "x" + Close(500);

        var equal = CompiledFilter.Compile($"Path = '{value}'");
        Assert.True(equal.Matches(Event(("Path", value))));
        Assert.False(equal.Matches(Event(("Path", "x"))));

        var like = CompiledFilter.Compile($"Path like '%{Open(500)}%'");
        Assert.True(like.Matches(Event(("Path", value))));

        // A pattern has a grammar of its own and the regex engine looks after its own depth.
        var regex = CompiledFilter.Compile($"regexMatch(Path, '{Open(500)}x{Close(500)}')");
        Assert.True(regex.Matches(Event(("Path", "x"))));
    }

    // ── What used to end the process ──────────────────────────────────────────

    public static TheoryData<string, string> FarTooDeep()
    {
        const int n = 10_000;
        return new TheoryData<string, string>
        {
            { "balanced groups round a comparison", Open(n) + "A = 1" + Close(n) },
            { "groups that are never closed",       Open(n) + "A = 1" },
            { "nothing but opening parentheses",    Open(n) },
            { "a run of not",                       Times("not ", n) + "A = 1" },
            { "not and groups, alternating",        Times("not (", n) + "A = 1" + Close(n) },
            { "a level-only filter",                Open(n) + "@l = 'Error'" + Close(n) },
            { "inside an in-list filter",           Open(n) + "@l in ['Error']" + Close(n) },
            { "after a clause that parsed",         "A = 1 and " + Open(n) + "B = 2" + Close(n) },
        };
    }

    [Theory]
    [MemberData(nameof(FarTooDeep))]
    public void Ten_thousand_levels_are_refused_like_any_other_filter_that_cannot_run(string what, string filter)
    {
        _ = what;
        var ex = Rejects(filter);
        Assert.Contains($"more than {Limit} levels deep", ex.Message, StringComparison.Ordinal);

        // The parser's own entry point answers the same way: nothing above it adds the limit.
        Assert.Throws<FormatException>(() => FilterParser.Parse(filter));
    }

    // ── The search box must not swallow it ────────────────────────────────────

    [Theory]
    // No comparison, no in-list, no function call: by the search box's rule these are prose,
    // and a FormatException raised while reading prose is discarded in favour of a text search
    // for every token. For this one refusal that would be a search for ten thousand
    // parentheses — a different answer, to a question nobody asked, and no word about why.
    [InlineData("timeout")]
    [InlineData("could not connect")]
    [InlineData("")]
    public void Free_text_is_no_way_round_the_limit(string words)
    {
        string filter = Open(10_000) + words + Close(10_000);

        var ex = Rejects(filter);
        Assert.Contains($"more than {Limit} levels deep", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_run_of_not_in_front_of_prose_is_refused_too()
        => Assert.Contains($"more than {Limit} levels deep",
                           Rejects(Times("not ", 10_000) + "timeout").Message, StringComparison.Ordinal);

    [Theory]
    // …and prose that is merely full of parentheses is still prose. The limit must not turn the
    // search box into something that refuses a paste.
    [InlineData("at Ameto.Query.FilterParser.Parse(String filter)")]
    [InlineData("f(g(h(x)))")]
    [InlineData("(unbalanced (on purpose")]
    [InlineData("not found (yet)")]
    public void Ordinary_prose_with_parentheses_is_still_searched_for(string text)
        => Assert.False(CompiledFilter.Compile(text).IsMatchAll);

    [Fact]
    public void Prose_nested_right_up_to_the_limit_is_still_a_search()
    {
        var f = CompiledFilter.Compile(Open(Limit) + "timeout" + Close(Limit));

        Assert.True(f.Matches(Event(("Detail", "the request hit a timeout"))));
        Assert.False(f.Matches(Event(("Detail", "all good"))));
    }

    // ── The aggregation grammar reads its where-clause through the same parser ─

    [Fact]
    public void A_where_clause_is_held_to_the_same_limit()
    {
        string tooDeep = "select count(*) where " + Open(10_000) + "A = 1" + Close(10_000) + " group by @service";

        // The aggregation endpoint's road.
        var parse = Assert.Throws<FormatException>(() => AggregationParser.Parse(tooDeep));
        Assert.Contains($"more than {Limit} levels deep", parse.Message, StringComparison.Ordinal);

        // The search endpoints' road: the text HAS claimed to be an aggregation, so its mistake
        // is reported rather than declined.
        Assert.Throws<FormatException>(() => AggregationParser.TryParse(tooDeep, out _));

        // At the limit it is an aggregation like any other, clause heads and all.
        var q = AggregationParser.Parse("select count(*) where " + Open(Limit) + "A = 1" + Close(Limit) + " group by @service limit 5");
        Assert.Equal(5, q.Limit);
        Assert.Single(q.Keys);
    }
}
