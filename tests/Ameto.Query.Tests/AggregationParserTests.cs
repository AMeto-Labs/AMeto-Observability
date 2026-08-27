using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// The aggregation grammar sits on top of a language whose keyword table is matched on whole
/// identifiers, case-insensitively, in every position — so a word promoted there stops being
/// available as a property name everywhere (which is why <c>Values = 5</c> already cannot be
/// written without the bracket escape). <c>Count</c>, <c>Min</c> and <c>Max</c> are ordinary
/// names for a property to have, so none of these words is a lexer keyword: they are recognised
/// by POSITION. These pin both halves of that — the grammar, and everything it must not eat.
/// </summary>
public sealed class AggregationParserTests
{
    private static AggregationQuery Parse(string text) => AggregationParser.Parse(text);

    private static FormatException Rejects(string text)
        => Assert.Throws<FormatException>(() => AggregationParser.Parse(text));

    // ── Not an aggregation ────────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("@l = 'Error'")]
    [InlineData("Error")]
    [InlineData("selected = true")]            // starts with the letters, is not the word
    [InlineData("select_count = 5")]           // one identifier, not two
    [InlineData("user selected a plan")]       // free text that merely contains it
    // The word itself is not enough. `select` is not a lexer keyword and never was, and the
    // search box's contract is that anything which is not an expression is free text — so
    // claiming these would turn an ordinary search into a parse error.
    [InlineData("select the cheapest plan")]
    [InlineData("select count")]               // no parenthesis: two ordinary words
    [InlineData("select nonsense(x)")]         // not an aggregate name
    [InlineData("select")]
    public void Only_an_unmistakable_aggregation_is_claimed_from_the_search_path(string? text)
    {
        Assert.False(AggregationParser.LooksLikeAggregation(text));
        Assert.False(AggregationParser.TryParse(text, out _));
    }

    [Theory]
    [InlineData("select count(*)")]
    [InlineData("SELECT Count(*) group by @l")]
    [InlineData("select avg(Elapsed) where @l = 'Error'")]
    public void An_unmistakable_aggregation_is_claimed(string text)
    {
        Assert.True(AggregationParser.LooksLikeAggregation(text));
        Assert.True(AggregationParser.TryParse(text, out var q));
        Assert.NotNull(q);
    }

    [Theory]
    // The same text asked of the endpoint that exists to answer aggregations: there, not being
    // one is the caller's mistake and gets named rather than shrugged at.
    [InlineData("@l = 'Error'",       "start with 'select'")]
    [InlineData("select the plan",    "count, sum, min, max or avg")]
    [InlineData("select nonsense(x)", "count, sum, min, max or avg")]
    public void The_endpoint_entry_point_names_what_is_wrong(string text, string expected)
        => Assert.Contains(expected, Rejects(text).Message, StringComparison.OrdinalIgnoreCase);

    [Theory]
    // The words the grammar reserves BY POSITION must stay usable as property names, because
    // promoting them in the lexer would take them away from every filter ever written.
    [InlineData("Count = 5")]
    [InlineData("Min > 1 and Max < 100")]
    [InlineData("Sum = 42")]
    [InlineData("['group'] = 'a'")]
    [InlineData("Limit = 10")]
    [InlineData("By = 'someone'")]
    public void A_property_may_still_be_named_after_a_clause_word(string filter)
        => CompiledFilter.Compile(filter);

    // ── The shape ─────────────────────────────────────────────────────────────

    [Fact]
    public void A_bare_count_is_one_column_and_no_keys()
    {
        var q = Parse("select count(*)");

        Assert.True(q.IsScalar);
        Assert.Empty(q.Keys);
        var agg = Assert.Single(q.Aggregates);
        Assert.Equal(AggregateKind.Count, agg.Kind);
        Assert.Null(agg.Property);
        Assert.Equal("count", agg.Alias);
        Assert.Null(q.FilterText);
    }

    [Fact]
    public void The_star_is_optional_because_the_lexer_cannot_see_it()
    {
        // '*' is one of the characters the lexer drops, so by the time the token list exists
        // count(*) and count() are the same call. Accepted rather than pretended otherwise.
        Assert.Equal(AggregateKind.Count, Assert.Single(Parse("select count()").Aggregates).Kind);
    }

    [Fact]
    public void Group_by_names_the_key_columns()
    {
        var q = Parse("select count(*) group by ['service.name'], @l");

        Assert.False(q.IsScalar);
        Assert.Equal(2, q.Keys.Count);
        Assert.Equal("service.name", q.Keys[0].Alias);
        Assert.Equal("@l", q.Keys[1].Alias);
    }

    [Fact]
    public void The_where_clause_is_kept_verbatim_for_the_ordinary_scan()
    {
        // Not re-serialised from a tree: the text goes to the executor as-is, so the
        // aggregation inherits index hints and time-bound folding rather than a second
        // rendering of the same filter that could drift from the first.
        var q = Parse("select count(*) where @l = 'Error' and Region = 'eu' group by @l");
        Assert.Equal("@l = 'Error' and Region = 'eu'", q.FilterText);
    }

    [Fact]
    public void A_where_clause_may_run_to_the_end()
        => Assert.Equal("@l = 'Error'", Parse("select count(*) where @l = 'Error'").FilterText);

    [Fact]
    public void Clause_words_inside_the_where_clause_are_found_at_the_top_level_only()
    {
        // `group` appears inside a bracketed property name here; cutting the where-clause at
        // the first occurrence of the word would slice through the middle of the filter.
        var q = Parse("select count(*) where ['group'] = 'a' group by @l");
        Assert.Equal("['group'] = 'a'", q.FilterText);
        Assert.Single(q.Keys);
    }

    [Theory]
    [InlineData("select sum(Elapsed)",  AggregateKind.Sum)]
    [InlineData("select min(Elapsed)",  AggregateKind.Min)]
    [InlineData("select max(Elapsed)",  AggregateKind.Max)]
    [InlineData("select avg(Elapsed)",  AggregateKind.Avg)]
    [InlineData("select COUNT(Region)", AggregateKind.Count)]
    public void Every_aggregate_is_recognised_in_any_case(string text, AggregateKind expected)
        => Assert.Equal(expected, Assert.Single(Parse(text).Aggregates).Kind);

    [Fact]
    public void Several_aggregates_keep_their_order()
    {
        var q = Parse("select count(*), avg(Elapsed), max(Elapsed) group by @l");

        Assert.Equal(3, q.Aggregates.Count);
        Assert.Equal(["count", "avg(Elapsed)", "max(Elapsed)"], q.Aggregates.Select(a => a.Alias));
    }

    [Fact]
    public void As_renames_a_column()
    {
        var q = Parse("select count(*) as events group by ['service.name'] as service");

        Assert.Equal("events",  Assert.Single(q.Aggregates).Alias);
        Assert.Equal("service", Assert.Single(q.Keys).Alias);
    }

    [Theory]
    [InlineData("select count(*) limit 5", 5)]
    [InlineData("select count(*)", AggregationQuery.DefaultLimit)]
    public void Limit_bounds_the_rows_returned(string text, int expected)
        => Assert.Equal(expected, Parse(text).Limit);

    [Fact]
    public void Limit_cannot_be_raised_past_the_group_cap()
        => Assert.Equal(AggregationParser.MaxGroups, Parse("select count(*) limit 999999").Limit);

    // ── Refusals ──────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("select sum()",                "needs a property")]
    [InlineData("select count(*) group",       "Expected 'by'")]
    [InlineData("select count(*) group by",    "property to group by")]
    [InlineData("select count(*) limit",       "positive whole number")]
    [InlineData("select count(*) limit 0",     "positive whole number")]
    [InlineData("select count(*) limit -3",    "positive whole number")]
    [InlineData("select count(*) where",       "needs a filter expression")]
    [InlineData("select count(*) as",          "name after 'as'")]
    [InlineData("select count(*) rubbish",     "expected 'where', 'group by' or 'limit'")]
    public void A_malformed_aggregation_says_what_is_wrong(string text, string expected)
        => Assert.Contains(expected, Rejects(text).Message, StringComparison.OrdinalIgnoreCase);

    [Theory]
    // `limit` and `group` are plausible property names and ordinary English words. They head a
    // clause only where they can BE one — `limit` in front of a number, `group` in front of
    // `by` — so a where-clause that merely mentions them stays a filter.
    [InlineData("select count(*) where limit exceeded",        "limit exceeded")]
    [InlineData("select count(*) where Limit = 10",            "Limit = 10")]
    [InlineData("select count(*) where Limit = 10 limit 5",    "Limit = 10")]
    [InlineData("select count(*) where ['group'] = 'a'",       "['group'] = 'a'")]
    public void A_clause_word_is_only_a_clause_where_it_can_be_one(string text, string expectedFilter)
        => Assert.Equal(expectedFilter, Parse(text).FilterText);

    [Fact]
    public void An_unmatched_closer_cannot_hide_the_clauses_after_it()
    {
        // The depth counter used to go negative on a stray ')', after which every later clause
        // head was skipped and the whole tail — group by and all — was swallowed into the
        // filter text, where bare words read as free text and nothing errored.
        var ex = Assert.Throws<FormatException>(
            () => Parse("select count(*) where (@l = 'Error')) group by @l"));
        Assert.NotNull(ex.Message);
    }

    [Fact]
    public void A_property_named_after_a_function_is_told_how_to_escape_itself()
    {
        // `Bucket`, `Values`, `Keys`, `Length` and forty-odd others lex as their keyword
        // wherever they appear — the same wart that stops `Values = 5` being written. Being
        // told the bracket form beats discovering that a column is simply unusable.
        var ex = Rejects("select count(*) group by Bucket");
        Assert.Contains("['Bucket']", ex.Message, StringComparison.Ordinal);

        // …and the escape it names actually works.
        Assert.Equal("Bucket", Assert.Single(Parse("select count(*) group by ['Bucket']").Keys).Alias);
    }

    [Fact]
    public void The_number_of_columns_is_bounded()
    {
        // Every group allocates four arrays sized by the column count and there may be 10 000
        // groups, so the two multiply — and a query string holds about a thousand columns.
        string wide = "select " + string.Join(", ", Enumerable.Repeat("sum(A)", AggregationParser.MaxAggregates + 1));
        Assert.Contains("columns can be selected", Rejects(wide).Message);

        string manyKeys = "select count(*) group by " +
                          string.Join(", ", Enumerable.Range(0, AggregationParser.MaxGroupKeys + 1).Select(i => $"K{i}"));
        Assert.Contains("keys can be grouped by", Rejects(manyKeys).Message);
    }

    [Fact]
    public void A_broken_where_clause_is_reported_here_rather_than_mid_scan()
    {
        // The filter is compiled while parsing the aggregation, so the diagnostic arrives with
        // the 400 instead of surfacing from inside a result stream.
        Assert.Contains("Expected a value", Rejects("select count(*) where Level = group by @l").Message);
    }
}
