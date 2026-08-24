using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// Every failure in this language used to be silent. An unreadable filter was not refused —
/// it was reinterpreted, and the reinterpretation ran: a typo past a valid prefix widened the
/// result set, a mis-spelled operator narrowed it to nothing, a function nobody implemented
/// became a text search for its own name. The user saw a page of results or an empty one,
/// with no way to tell either from an honest answer.
///
/// <para>These pin the refusals. A <see cref="FormatException"/> here reaches the browser as
/// a 400 carrying the parser's own sentence, because both stream endpoints compile the filter
/// before committing the response.</para>
/// </summary>
public sealed class FilterRefusesNonsenseTests
{
    private static FormatException Rejects(string filter)
        => Assert.Throws<FormatException>(() => CompiledFilter.Compile(filter));

    // ── The one that was costing results ──────────────────────────────────────

    [Fact]
    public void The_SQL_spelling_of_not_equal_means_not_equal()
    {
        // The events page writes this itself: deselect exactly one of the six levels and
        // setLevelsClause emits `@l <> 'Debug'`. There was no <> operator, so it parsed as
        // '<' then '>', the value came back null, and the node became an is-absent test on a
        // field every event carries — zero rows, from a click in the level selector.
        var f = CompiledFilter.Compile("@l <> 'Debug'");

        Assert.True(f.Matches(Event(LogLevel.Error)));
        Assert.True(f.Matches(Event(LogLevel.Information)));
        Assert.False(f.Matches(Event(LogLevel.Debug)));
    }

    [Fact]
    public void It_is_the_same_operator_as_the_other_spelling()
    {
        var angle = CompiledFilter.Compile("@l <> 'Debug'");
        var bang  = CompiledFilter.Compile("@l != 'Debug'");

        foreach (var level in new[] { LogLevel.Verbose, LogLevel.Debug, LogLevel.Information,
                                      LogLevel.Warning, LogLevel.Error, LogLevel.Fatal })
            Assert.Equal(bang.Matches(Event(level)), angle.Matches(Event(level)));
    }

    [Fact]
    public void Less_than_and_greater_than_still_parse_on_their_own()
    {
        // The new two-character operator must not eat either single-character one.
        Assert.True(CompiledFilter.Compile("Elapsed > 5").Matches(Event(LogLevel.Error, ("Elapsed", 9L))));
        Assert.False(CompiledFilter.Compile("Elapsed > 5").Matches(Event(LogLevel.Error, ("Elapsed", 1L))));
        Assert.True(CompiledFilter.Compile("Elapsed < 5").Matches(Event(LogLevel.Error, ("Elapsed", 1L))));
        Assert.True(CompiledFilter.Compile("Elapsed <= 5").Matches(Event(LogLevel.Error, ("Elapsed", 5L))));
        Assert.True(CompiledFilter.Compile("Elapsed >= 5").Matches(Event(LogLevel.Error, ("Elapsed", 5L))));
    }

    // ── not in / not like ─────────────────────────────────────────────────────

    [Fact]
    public void Not_in_negates_the_set_instead_of_searching_for_the_field_name()
    {
        // `@l not in [...]` fell through every structured branch and landed in the free-text
        // run, which searched the message text for the literal "@l" and discarded the list.
        var f = CompiledFilter.Compile("@l not in ['Debug', 'Verbose']");

        Assert.True(f.Matches(Event(LogLevel.Error)));
        Assert.False(f.Matches(Event(LogLevel.Debug)));
        Assert.False(f.Matches(Event(LogLevel.Verbose)));
    }

    [Fact]
    public void Not_like_negates_the_pattern()
    {
        var f = CompiledFilter.Compile("Region not like 'eu-%'");

        Assert.True(f.Matches(Event(LogLevel.Error, ("Region", "us-east"))));
        Assert.False(f.Matches(Event(LogLevel.Error, ("Region", "eu-west"))));
    }

    [Fact]
    public void The_parenthesised_negation_still_means_the_same_thing()
    {
        var infix   = CompiledFilter.Compile("@l not in ['Debug']");
        var grouped = CompiledFilter.Compile("not (@l in ['Debug'])");

        foreach (var level in new[] { LogLevel.Debug, LogLevel.Error })
            Assert.Equal(grouped.Matches(Event(level)), infix.Matches(Event(level)));
    }

    [Fact]
    public void A_not_with_nothing_to_negate_is_refused()
        => Assert.Contains("'in' or 'like'", Rejects("@l = 'Error' and Region not 'eu-west'").Message);

    // ── Reading stops early ───────────────────────────────────────────────────

    [Theory]
    // Each of these parsed to the part before the junk, and ran it. The user got results —
    // just not for the query they wrote.
    [InlineData("@l = 'Error' Region = 'eu'")]      // two clauses, no 'and'
    [InlineData("@l = 'Error' and")]                // half-typed
    [InlineData("@l = 'Error' select count(*)")]    // a language this one does not speak
    [InlineData("(@l = 'Error') junk")]
    public void A_filter_that_cannot_be_read_to_the_end_is_refused(string filter)
        => Rejects(filter);

    [Fact]
    public void The_message_says_where_reading_stopped()
    {
        var ex = Rejects("@l = 'Error' Region = 'eu'");
        Assert.Contains("Region", ex.Message, StringComparison.Ordinal);
        Assert.Contains("and", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── A comparison with nothing to compare against ──────────────────────────

    [Theory]
    [InlineData("Level = ")]        // the wart: became "Level is absent", matching nothing
    [InlineData("@l = ")]
    [InlineData("Elapsed > ")]
    [InlineData("@l = and Region = 'eu'")]
    public void A_comparison_missing_its_value_is_refused(string filter)
        => Assert.Contains("Expected a value", Rejects(filter).Message);

    // ── Characters the language has no meaning for ────────────────────────────

    [Fact]
    public void Punctuation_inside_a_quoted_value_is_still_just_text()
    {
        // The refusals are about the GRAMMAR, not about the data: values carry whatever they carry.
        Assert.True(CompiledFilter.Compile("Path = '/api/users?x=1'")
                                  .Matches(Event(LogLevel.Error, ("Path", "/api/users?x=1"))));
        Assert.True(CompiledFilter.Compile("Url like '%*%'")
                                  .Matches(Event(LogLevel.Error, ("Url", "a*b"))));
    }

    // ── The box is also the search box ────────────────────────────────────────
    // Refusing an unreadable EXPRESSION is the point; refusing a paste is not. The rule is
    // that a filter with nothing structural in it — no comparison, no in/like, no bracket —
    // is prose, and every word in it is a search term.

    [Theory]
    [InlineData("GET /api/orders/123",                          new[] { "GET", "api", "orders", "123" })]
    [InlineData("NullReferenceException: Object reference",     new[] { "NullReferenceException", "Object", "reference" })]
    [InlineData("user not found",                               new[] { "user", "not", "found" })]
    [InlineData("could not connect",                            new[] { "could", "not", "connect" })]
    [InlineData("Error timeout",                                new[] { "Error", "timeout" })]
    [InlineData("fatal crash in checkout",                      new[] { "fatal", "crash", "in", "checkout" })]
    public void Prose_is_searched_for_rather_than_refused(string filter, string[] expectedTerms)
    {
        var node = FilterParser.Parse(filter);

        var free = Assert.IsType<FreeTextNode>(node);
        Assert.Equal(expectedTerms, free.Terms);
    }

    [Theory]
    // A Windows path, a JSON fragment, a stack frame. The lexer drops the punctuation and the
    // words are what remain — the behaviour the filter-expression documentation promises.
    [InlineData(@"C:\Users\ruslan\app.log")]
    [InlineData("{\"orderId\":42}")]
    [InlineData("at Ameto.Query.FilterParser.Parse(String filter)")]
    [InlineData("50% failed")]
    [InlineData("order#4711")]
    // Punctuation that happens to BE an operator. A comparison only claims the input when it
    // has whitespace on both sides — otherwise a pasted query string, a logfmt line and a
    // generic type would each be refused, which is the search box's commonest input.
    [InlineData("GET /api/orders?status=active")]
    [InlineData("level=error msg=timeout retries=3")]
    [InlineData("List<String> at Program.cs:42")]
    [InlineData("{\"tags\":[\"eu\",\"west\"]}")]
    [InlineData("a<b and b>c")]
    public void A_paste_still_compiles(string filter)
        => CompiledFilter.Compile(filter);

    [Fact]
    public void A_bare_level_word_alone_is_still_a_level_filter()
    {
        // The shortcut has to survive: `Error` on its own means the level, and the toolbar
        // and the alert rules rely on it. It is only a following bare word that turns the
        // same token into the first word of a phrase.
        Assert.IsType<LevelNode>(FilterParser.Parse("Error"));
        Assert.IsType<LevelNode>(FilterParser.Parse("Fatal"));
        Assert.IsType<OrNode>(FilterParser.Parse("Error or Fatal"));
        Assert.IsType<FreeTextNode>(FilterParser.Parse("Error timeout"));
    }

    [Fact]
    public void Structure_anywhere_in_the_input_brings_the_refusals_back()
    {
        // One comparison is enough to say this was meant as an expression, and from there a
        // half-written one is an error rather than a bag of words.
        Rejects("@l = 'Error' Region = 'eu'");
        Rejects("Level = ");
    }

    // ── Functions that do not exist ───────────────────────────────────────────

    [Theory]
    [InlineData("sum(Elapsed) > 5")]
    [InlineData("avg(Elapsed) > 5")]
    [InlineData("count(Region) > 5")]
    public void An_unknown_function_is_named_rather_than_searched_for(string filter)
    {
        // These became a free-text search for the function's own name with the arguments
        // dropped — `sum(Elapsed) > 5` looked for the word "sum" in the message text.
        var ex = Rejects(filter);
        Assert.Contains("Unknown function", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_functions_that_do_exist_still_work()
    {
        Assert.True(CompiledFilter.Compile("contains(Region, 'eu')")
                                  .Matches(Event(LogLevel.Error, ("Region", "eu-west"))));
        Assert.True(CompiledFilter.Compile("length(Region) > 3")
                                  .Matches(Event(LogLevel.Error, ("Region", "eu-west"))));
        Assert.True(CompiledFilter.Compile("has(Region)")
                                  .Matches(Event(LogLevel.Error, ("Region", "eu-west"))));
    }

    // ── A literal where a property belongs ────────────────────────────────────

    [Fact]
    public void An_in_list_needs_a_property_on_its_left()
    {
        // `'Error' in [...]` silently became `@l in [...]` — a guess that answers a different
        // question and looks right often enough to be believed.
        Assert.Contains("'in' needs a property", Rejects("'Error' in ['a', 'b']").Message);
    }

    [Fact]
    public void A_like_needs_a_property_on_its_left()
        => Assert.Contains("'like' needs a property",
                           Rejects("@l = 'Error' and 'eu-west' like 'eu-%'").Message);

    [Theory]
    // Kept on the theory that it was "simply false" — which holds only for '='. Compare()
    // answers `op is Ne` when an operand resolves to nothing, so these matched EVERY event.
    [InlineData("'a' != 'b'")]
    [InlineData("'Error' <> 'Debug'")]
    [InlineData("'a' = 'b'")]
    public void A_comparison_with_no_property_at_all_is_refused(string filter)
        => Assert.Contains("no property to test", Rejects(filter).Message);

    // ── Quotes inside values ──────────────────────────────────────────────────

    [Fact]
    public void A_doubled_quote_is_one_quote_inside_the_value()
    {
        // "Filter by this value" writes the SQL escape, so a value carrying an apostrophe
        // closed the string early: the predicate compared against the text up to the
        // apostrophe and the rest was dropped.
        var f = CompiledFilter.Compile("Detail = 'can''t connect'");

        Assert.True(f.Matches(Event(LogLevel.Error, ("Detail", "can't connect"))));
        Assert.False(f.Matches(Event(LogLevel.Error, ("Detail", "can"))));
    }

    [Fact]
    public void The_backslash_escape_the_grammar_documents_still_works()
    {
        var f = CompiledFilter.Compile(@"Detail = 'can\'t connect'");
        Assert.True(f.Matches(Event(LogLevel.Error, ("Detail", "can't connect"))));
    }

    // ── Values in a list ──────────────────────────────────────────────────────

    [Theory]
    [InlineData("@l in [Error, Fatal]")]
    [InlineData("Region in [eu]")]
    public void An_unquoted_item_in_a_list_is_named_rather_than_read_as_nothing(string filter)
    {
        // ParseLiteral had no identifier case, so each bare word entered the list as null:
        // the test matched neither Error nor Fatal, and — since a null item stringifies to
        // "" — it DID match anything whose property was empty.
        Assert.Contains("property name", Rejects(filter).Message);
    }

    [Fact]
    public void A_decimal_in_a_list_is_a_number()
    {
        // ParseLiteral only tried long, so 1.5 entered the list as the string "1.5".
        Assert.True(CompiledFilter.Compile("Ratio in [1.5, 2.5]")
                                  .Matches(Event(LogLevel.Error, ("Ratio", 1.5d))));
    }

    [Fact]
    public void A_list_with_a_missing_separator_is_refused()
        => Rejects("@l in ['Error' 'Fatal']");

    // ── What must keep working ────────────────────────────────────────────────

    [Theory]
    // Everything the events page itself generates, plus the shapes the docs advertise.
    [InlineData("")]
    [InlineData("@l = 'Error'")]
    [InlineData("@l <> 'Debug'")]
    [InlineData("@l in ['Error', 'Fatal']")]
    [InlineData("@l not in ['Debug']")]
    [InlineData("['service.name'] = 'checkout'")]
    [InlineData("['service.name'] in ['checkout', 'billing']")]
    [InlineData("@l = 'Error' and ['service.name'] = 'checkout'")]
    [InlineData("@l = 'Error' and ['service.name'] = 'checkout' and Region = 'eu'")]
    [InlineData("Error")]
    [InlineData("timeout")]
    [InlineData("user balance")]
    [InlineData("not (@l = 'Error')")]
    [InlineData("@t >= '2026-08-01T00:00:00Z' and @t < '2026-08-02T00:00:00Z'")]
    [InlineData("contains(@mt, 'boom') or startsWith(Region, 'eu')")]
    [InlineData("regexMatch(Region, '[0-9]+')")]
    [InlineData("Headers['Api-Request-Id'] = 'x'")]
    [InlineData("coalesce(A, B) = 'x'")]
    [InlineData("fromJson(Payload).user.id = 7")]
    [InlineData("@x.type = 'System.Exception'")]
    public void A_filter_that_was_always_valid_still_compiles(string filter)
        => CompiledFilter.Compile(filter);

    // ── Corpus ────────────────────────────────────────────────────────────────

    private static LogEvent Event(LogLevel level, params (string Key, object Value)[] props)
    {
        var map = new Dictionary<string, object?>(props.Length);
        foreach (var (k, v) in props) map[k] = v;
        return new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 12, 0, 0, TimeSpan.Zero),
            Level           = level,
            MessageTemplate = "something happened",
            Properties      = map,
        };
    }
}
