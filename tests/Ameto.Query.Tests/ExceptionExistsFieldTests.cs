using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Query.Tests;

/// <summary>
/// <c>@x.exists</c> is the built-in "this event carries an exception": a bool on every event.
/// The filter reference has documented it since the first commit and the segment index has
/// kept a bucket of that name just as long, but no row of <see cref="BuiltinFields"/> named it.
/// The evaluator therefore read <c>@x.exists</c> as a nested user property — <c>@x</c>, then
/// <c>exists</c> — which no event has, and the predicate matched nothing in either tier.
///
/// <para>Pinned here: the table row (every spelling, and no VALUE hint); the evaluator's answer
/// (<see cref="LogEvent.HasException"/>, so the same one <c>has(@x)</c> gives and with nothing
/// decoded); the one question the index IS asked — "which rows carry an exception", for the
/// predicates that are exactly that question and for no other; and how far the index's marker
/// may answer it, over indexes the builder really wrote. The spellings also run through the
/// production prefilter in <c>IndexHintKeyTests</c>, every payload shape through the
/// object-against-bytes oracle in <c>LazyExceptionEvaluationTests</c>, and whole stores through
/// the executor in <c>ExceptionPresenceIndexTests</c>.</para>
/// </summary>
public sealed class ExceptionExistsFieldTests
{
    // ── The table row ─────────────────────────────────────────────────────────

    [Theory]
    [InlineData("@x\u0001exists")]            // @x.exists — the parser's encoded path
    [InlineData("Exception\u0001Exists")]     // Exception.Exists
    [InlineData("@x.exists")]                 // ['@x.exists'] — the bracket escape keeps the dot
    public void Every_spelling_resolves_to_the_field(string alias)
    {
        Assert.True(BuiltinFields.TryResolve(alias, out var field));
        Assert.Equal(BuiltinField.ExceptionExists, field);
    }

    /// <summary>
    /// The aliases above are only worth anything if they are what the parser really produces
    /// for the spellings a person types — so each is taken from the parser, not from a constant.
    /// </summary>
    [Theory]
    [InlineData("@x.exists = true",        "@x\u0001exists")]
    [InlineData("@x['exists'] = true",     "@x\u0001exists")]
    [InlineData("['@x.exists'] = true",    "@x.exists")]
    [InlineData("Exception.Exists = true", "Exception\u0001Exists")]
    public void The_parser_hands_over_a_spelling_the_table_knows(string filter, string property)
    {
        var node = Assert.IsType<CompareNode>(FilterParser.Parse(filter));
        Assert.Equal(property, node.Property);
        Assert.True(BuiltinFields.TryResolve(node.Property, out var field));
        Assert.Equal(BuiltinField.ExceptionExists, field);
    }

    [Theory]
    [InlineData("@x\u0001Exists")]            // aliases are ordinal, like every other built-in's
    [InlineData("@X\u0001exists")]
    [InlineData("Exception\u0001exists")]
    [InlineData("@x\u0001exist")]
    [InlineData("exists")]
    [InlineData("@x\u0001exists\u0001value")]
    public void Near_misses_stay_user_properties(string name) =>
        Assert.False(BuiltinFields.TryResolve(name, out _));

    /// <summary>
    /// The row gives the field nothing a hint BY VALUE could use: the index's bucket of the same
    /// name holds "true" and nothing else, so it cannot be asked about <c>false</c>, and the
    /// bloom holds the bucket's name rather than a value.
    /// </summary>
    [Fact]
    public void The_row_gives_the_field_no_value_hint()
    {
        Assert.Equal("@x.exists", ClefFields.ExceptionExists);
        Assert.Null(BuiltinFields.InvertedKey(BuiltinField.ExceptionExists));
        Assert.False(BuiltinFields.IsBloomIndexed(BuiltinField.ExceptionExists));
        Assert.False(BuiltinFields.IsTrigramIndexed(BuiltinField.ExceptionExists));
    }

    // ── The one question the index is asked ───────────────────────────────────

    /// <summary>
    /// "Which rows carry an exception" is the one thing the index's marker records, so it is
    /// asked for exactly the predicates that are that question — and each row here is first
    /// shown to BE that question: it matches an event if and only if the event has an exception.
    /// </summary>
    [Theory]
    [InlineData("has(@x)")]
    [InlineData("isDefined(@x)")]
    [InlineData("has(Exception)")]
    [InlineData("has(@x.type)")]              // the type defaults when absent: present exactly when the exception is
    [InlineData("has(Exception.Type)")]
    [InlineData("@x.exists = true")]
    [InlineData("@x.exists = 'true'")]
    [InlineData("@x.exists = 'TRUE'")]
    [InlineData("@x.exists != false")]
    [InlineData("@x.exists <> false")]
    [InlineData("@x.exists != 'no'")]         // a bool reads any text but "true" as false
    [InlineData("@x.exists in [true]")]
    [InlineData("@x.exists in ['true', 'TRUE']")]
    [InlineData("true = @x.exists")]
    [InlineData("['@x.exists'] = true")]
    [InlineData("Exception.Exists = true")]
    public void A_predicate_that_selects_the_events_with_an_exception_asks_the_marker(string expression)
    {
        var filter = CompiledFilter.Compile(expression);
        Assert.All(Shapes(), ev => Assert.Equal(ev.HasException, filter.Matches(ev)));

        var hint = Assert.Single(filter.GetInvertedHints());
        Assert.Equal(ClefFields.ExceptionExists, hint.property);
        Assert.Equal("true", hint.value);

        // Nothing for the bloom gate — it holds the bucket's name, not a value — and no text.
        Assert.False(filter.TryGetIndexHint(out _, out _));
        Assert.Empty(filter.GetTrigramHints());
    }

    /// <summary>
    /// Everything else about the field scans, and the first assertion is why: each of these
    /// admits an event without an exception, or refuses one with — so a list of the rows that
    /// carry one is not its answer.
    /// </summary>
    [Theory]
    [InlineData("@x.exists = false")]
    [InlineData("@x.exists != true")]
    [InlineData("@x.exists <> true")]
    [InlineData("@x.exists = 'false'")]
    [InlineData("@x.exists = 'no'")]
    [InlineData("@x.exists = 1")]
    [InlineData("@x.exists in [false]")]
    [InlineData("@x.exists in [true, false]")]
    [InlineData("@x.exists = null")]
    [InlineData("@x.exists != null")]
    [InlineData("@x.exists > false")]           // ordering a bool matches nothing
    [InlineData("contains(@x.exists, 'true')")] // case-sensitive against "True": matches nothing
    [InlineData("has(@x.exists)")]              // true of every event
    [InlineData("has(@x.message)")]             // a field of the exception, not its presence
    [InlineData("has(@x.inner.type)")]
    [InlineData("not has(@x)")]
    [InlineData("not @x.exists = true")]
    [InlineData("has(@x) or Region = 'ae-dxb'")]   // an OR branch is not a conjunct
    public void Anything_else_about_it_asks_the_index_nothing(string expression)
    {
        var filter = CompiledFilter.Compile(expression);
        Assert.Contains(Shapes(), ev => filter.Matches(ev) != ev.HasException);

        Assert.Empty(filter.GetInvertedHints());
        Assert.Empty(filter.GetTrigramHints());
        Assert.False(filter.TryGetIndexHint(out _, out _));
    }

    /// <summary>
    /// The hint is given to the shapes it recognises, not to everything that happens to select
    /// the same events. These do select them — a double negation, a pattern only "True" fits —
    /// and still ask the index nothing: an unrecognised spelling is scanned, which costs time
    /// and never a row.
    /// </summary>
    [Theory]
    [InlineData("not @x.exists = false")]
    [InlineData("not not has(@x)")]
    [InlineData("@x.exists like 'tru%'")]
    public void A_spelling_the_hint_does_not_recognise_is_scanned(string expression)
    {
        var filter = CompiledFilter.Compile(expression);
        Assert.All(Shapes(), ev => Assert.Equal(ev.HasException, filter.Matches(ev)));

        Assert.Empty(filter.GetInvertedHints());
        Assert.Empty(filter.GetTrigramHints());
        Assert.False(filter.TryGetIndexHint(out _, out _));
    }

    [Fact]
    public void A_conjunct_beside_it_keeps_its_own_hint()
    {
        // The example in docs/FILTER_EXPRESSIONS.md: the marker and the inner type both narrow.
        var filter = CompiledFilter.Compile($"@x.exists = true and @x.inner.type = '{InnerType}'");

        var hints = filter.GetInvertedHints();
        Assert.Equal(2, hints.Count);
        Assert.Equal((ClefFields.ExceptionExists, (object?)"true"), hints[0]);
        Assert.Equal((ClefFields.ExceptionInnerType, (object?)InnerType), hints[1]);

        // The bloom gate is the inner type's alone.
        Assert.True(filter.TryGetIndexHint(out string key, out object? value));
        Assert.Equal(ClefFields.ExceptionInnerType, key);
        Assert.Equal(InnerType, value);
    }

    /// <summary>
    /// One event of every kind the theories above have to tell apart: an exception as a
    /// decoded object and as raw bytes, one with nothing but a type, no exception at all, an
    /// exception column holding nil, and an event the OR's other branch matches.
    /// </summary>
    private static LogEvent[] Shapes() =>
    [
        Event(exception: Boom),
        Event(rawException: Boom.ToBytes()),
        Event(rawException: new ExceptionInfo { Type = "System.TimeoutException" }.ToBytes()),
        Event(),
        Event(rawException: NilPayload),
        Event(props: new() { ["Region"] = "ae-dxb" }),
    ];

    // ── The evaluator ─────────────────────────────────────────────────────────

    private const string InnerType = "System.IO.IOException";

    private static readonly ExceptionInfo Boom = new()
    {
        Type       = "System.InvalidOperationException",
        Message    = "gateway timeout on settle",
        StackTrace = "   at Billing.Settle()\n   at Billing.Run()",
        Inner      = new ExceptionInfo { Type = InnerType, Message = "disk" },
    };

    /// <summary>msgpack nil: an exception column that is not empty and still holds no exception.</summary>
    private static readonly byte[] NilPayload = [0xC0];

    private static LogEvent Event(
        ExceptionInfo? exception = null, ReadOnlyMemory<byte> rawException = default,
        Dictionary<string, object?>? props = null, uint id = 1) => new()
    {
        Id              = new EventId(0u, id),
        Timestamp       = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero),
        Level           = LogLevel.Error,
        MessageTemplate = "settle {OrderId}",
        Exception       = exception,
        RawException    = rawException,
        Properties      = props,
    };

    private static bool Eval(string filter, LogEvent ev) => CompiledFilter.Compile(filter).Matches(ev);

    /// <summary>
    /// Every row is asked of an event with an exception and of one without, in each shape a
    /// tier hands the evaluator: the decoded object (hand-built events) and the raw bytes (the
    /// hot tier's arena view, a segment's column) — and, for "without", a column holding nil.
    /// </summary>
    [Theory]
    // The spellings the filter reference promises.
    [InlineData("@x.exists = true",           true,  false)]
    [InlineData("@x.exists = false",          false, true)]
    [InlineData("@x.exists != true",          false, true)]
    [InlineData("@x.exists <> true",          false, true)]
    [InlineData("@x.exists != false",         true,  false)]
    [InlineData("not @x.exists = true",       false, true)]
    [InlineData("true = @x.exists",           true,  false)]   // literal on the left
    [InlineData("['@x.exists'] = true",       true,  false)]
    [InlineData("Exception.Exists = true",    true,  false)]
    [InlineData("Exception.Exists = false",   false, true)]
    // A bool compares against the literal's text, exactly as a bool property does.
    [InlineData("@x.exists = 'true'",         true,  false)]
    [InlineData("@x.exists = 'TRUE'",         true,  false)]
    [InlineData("@x.exists = 'false'",        false, true)]
    [InlineData("@x.exists in [true]",        true,  false)]
    [InlineData("@x.exists in [false]",       false, true)]
    [InlineData("@x.exists not in [true]",    false, true)]
    // A bool on EVERY event: never null, which is what makes `= false` mean "has none".
    [InlineData("has(@x.exists)",             true,  true)]
    [InlineData("@x.exists = null",           false, false)]
    [InlineData("@x.exists != null",          true,  true)]
    [InlineData("typeOf(@x.exists) = 'bool'", true,  true)]
    // The example in docs/FILTER_EXPRESSIONS.md.
    [InlineData("@x.exists = true and @x.inner.type = 'System.IO.IOException'", true, false)]
    public void The_evaluator_answers_from_whether_the_event_has_an_exception(
        string filter, bool withException, bool withoutException)
    {
        Assert.Equal(withException, Eval(filter, Event(exception: Boom)));
        Assert.Equal(withException, Eval(filter, Event(rawException: Boom.ToBytes())));

        Assert.Equal(withoutException, Eval(filter, Event()));
        Assert.Equal(withoutException, Eval(filter, Event(rawException: NilPayload)));
    }

    /// <summary>
    /// A built-in SHADOWS a user property of the same spelling, as every row of the table does.
    /// These two events are the only ones the old reading — a nested <c>@x</c> map, then the
    /// flat key — could ever have matched; neither has an exception.
    /// </summary>
    [Fact]
    public void A_user_property_of_the_same_name_is_shadowed_by_the_header_fact()
    {
        var nested = Event(props: new() { ["@x"] = new Dictionary<string, object?> { ["exists"] = true } });
        var flat   = Event(props: new() { ["@x.exists"] = true });

        foreach (var ev in new[] { nested, flat })
        {
            Assert.False(Eval("@x.exists = true", ev));
            Assert.True(Eval("@x.exists = false", ev));
            Assert.False(Eval("['@x.exists'] = true", ev));
        }
    }

    /// <summary>
    /// Per candidate event the field costs a table probe and the payload's lead byte. The
    /// evaluator's value road is <c>object?</c>, so this is what fails if the answer is ever
    /// boxed where it is read instead of once for the process.
    /// </summary>
    [Theory]
    [InlineData("@x.exists = true")]
    [InlineData("@x.exists = false")]
    [InlineData("@x.exists != true")]
    [InlineData("@x.exists = 'true'")]
    public void Evaluating_it_allocates_nothing_and_decodes_nothing(string expression)
    {
        var filter = CompiledFilter.Compile(expression);
        var raw    = Event(rawException: Boom.ToBytes());
        LogEvent[] events = [raw, Event(exception: Boom), Event(), Event(rawException: NilPayload)];

        for (int i = 0; i < 100; i++)
            for (int e = 0; e < events.Length; e++) filter.Matches(events[e]);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
            for (int e = 0; e < events.Length; e++) filter.Matches(events[e]);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(bytes == 0, $"expected zero allocation over 4000 evaluations, saw {bytes} B");
        Assert.False(raw.ExceptionMaterialised, $"`{expression}` built the ExceptionInfo tree");
    }

    // ── How far the index's marker may answer ─────────────────────────────────

    /// <summary>Five rows, one of every kind the marker and the scan could come to disagree on.</summary>
    private static readonly byte[][] Rows =
    [
        Boom.ToBytes(),                                                                    // 0: readable
        [],                                                                                // 1: no exception
        Pack(static (ref MessagePackWriter w) => w.Write("System.Exception: plain text")),  // 2: legacy @x, a plain string
        // 3: a map whose `type` is not a string. LogEvent.HasException answers from the lead
        //    byte and says yes; the index cannot read its fields.
        Pack(static (ref MessagePackWriter w) => { w.WriteMapHeader(1); w.Write("type"); w.Write(7); }),
        NilPayload,                                                                        // 4: a column holding nil
    ];

    private static readonly uint[] RowsWithAnException = [0, 2, 3];
    private static readonly uint[] RowsWithout         = [1, 4];

    /// <summary>
    /// The marker's list IS the scan's answer to "which rows carry an exception" — the row
    /// whose map the index could not read included — and that is the only thing it can be
    /// asked. It holds "true" and nothing else, so asked for <c>false</c> it says "no such row"
    /// about a group where two rows match: the reason the field gets no hint by value, and the
    /// positive question a hint of its own.
    /// </summary>
    [Fact]
    public void The_marker_lists_what_the_scan_matches_and_knows_nothing_about_false()
    {
        var events = Rows.Select(static (payload, i) => Event(rawException: payload, id: (uint)i)).ToArray();
        Assert.Equal(RowsWithAnException, Scan(events, "@x.exists = true"));
        Assert.Equal(RowsWithAnException, Scan(events, "has(@x)"));
        Assert.Equal(RowsWithout,         Scan(events, "@x.exists = false"));
        Assert.Equal(RowsWithout,         Scan(events, "@x.exists != true"));
        Assert.Equal(RowsWithout,         Scan(events, "not has(@x)"));

        using var builder = Filled(new SegmentIndexBuilder(Rows.Length), Rows);
        using var index   = Index(builder);
        Assert.Equal(1, builder.MalformedExceptionPayloads);     // row 3: counted, and still marked

        uint[]? forTrue = index.LookupIntersect([(ClefFields.ExceptionExists, (object?)"true")]);
        Assert.NotNull(forTrue);
        Assert.Equal(RowsWithAnException, forTrue);

        uint[]? forFalse = index.LookupIntersect([(ClefFields.ExceptionExists, (object?)false)]);
        Assert.NotNull(forFalse);                     // not "no information": the bucket is known…
        Assert.Empty(forFalse);                       // …and proves no row matches, where two do
    }

    /// <summary>
    /// The production narrowing, over a group of each kind. Written by this build: the marker's
    /// rows, and a group that holds no exception is dropped unread. Written before it — a marker
    /// that leaves out the unreadable row, and no word that it is complete: no information, so
    /// the group is scanned and the row is still found.
    /// </summary>
    [Theory]
    [InlineData("has(@x)")]
    [InlineData("isDefined(@x)")]
    [InlineData("@x.exists = true")]
    [InlineData("@x.exists != false")]
    public void The_narrowing_follows_the_marker_only_as_far_as_its_builder_vouched(string expression)
    {
        var filter = CompiledFilter.Compile(expression);

        byte[][] noException = [[], NilPayload];

        using (var builder = Filled(new SegmentIndexBuilder(Rows.Length), Rows))
        using (var vouched = Index(builder))
        {
            Assert.True(QueryExecutor.TryNarrowWithIndex(filter, vouched, out uint[]? candidates));
            Assert.NotNull(candidates);
            Assert.Equal(RowsWithAnException, candidates);
        }

        using (var builder = Filled(new SegmentIndexBuilder(noException.Length), noException))
        using (var none = Index(builder))
            Assert.False(QueryExecutor.TryNarrowWithIndex(filter, none, out _), "a vouched group with no exception must be dropped");

        using (var older = Filled(new PreVouchingIndexSink(Rows.Length, 0), Rows))
        using (var index = Index(older))
        {
            // What narrowing by an older marker would have cost: read straight off the section,
            // past the readers' rule, it lists rows 0 and 2 and not 3.
            byte[] section = Sections(older).Inverted;
            var    catalog = SegmentInvertedIndex.ReadCatalog(section);
            Assert.False(catalog.VouchesForExceptionMarker);
            int[]? listed = SegmentInvertedIndex.ScanBucket(
                section, catalog.Codec, catalog.Properties[ClefFields.ExceptionExists].Runs, "true");
            Assert.NotNull(listed);
            Assert.Equal(new[] { 0, 2 }, listed);

            Assert.True(QueryExecutor.TryNarrowWithIndex(filter, index, out uint[]? candidates));
            Assert.Null(candidates);
        }

        using (var older = Filled(new PreVouchingIndexSink(noException.Length, 0), noException))
        using (var index = Index(older))
        {
            Assert.True(QueryExecutor.TryNarrowWithIndex(filter, index, out uint[]? candidates),
                "an older group without a marker proves nothing: it must be scanned, not dropped");
            Assert.Null(candidates);
        }
    }

    /// <summary>
    /// Every row of a level-split Error group carries an exception, so the marker's list is the
    /// whole group. That is the third narrowing state — every row, named by nothing — rather
    /// than a group-sized array to select all of them, and rather than "no information", which
    /// would send the rest of the segment to a full scan with it.
    /// </summary>
    [Fact]
    public void A_group_where_every_row_has_an_exception_contributes_every_row_without_naming_them()
    {
        byte[][] everyRow = [Boom.ToBytes(), Rows[2], Rows[3], Boom.ToBytes()];
        var filter = CompiledFilter.Compile("has(@x)");

        using var builder = Filled(new SegmentIndexBuilder(everyRow.Length), everyRow);
        using var index   = Index(builder);

        Assert.True(QueryExecutor.TryNarrowWithIndex(filter, index, null, (uint)everyRow.Length, out uint[]? candidates, out bool whole));
        Assert.Null(candidates);
        Assert.True(whole);

        // With the group's size unknown the same rows arrive named — the state is about how the
        // answer is carried, not which rows it is.
        Assert.True(QueryExecutor.TryNarrowWithIndex(filter, index, null, 0, out uint[]? named, out whole));
        Assert.False(whole);
        Assert.NotNull(named);
        Assert.Equal(new uint[] { 0, 1, 2, 3 }, named);

        // In a group one row larger the list is a genuine narrowing again.
        Assert.True(QueryExecutor.TryNarrowWithIndex(filter, index, null, (uint)everyRow.Length + 1, out named, out whole));
        Assert.False(whole);
        Assert.NotNull(named);
        Assert.Equal(new uint[] { 0, 1, 2, 3 }, named);
    }

    /// <summary>
    /// Where the line is: the marker's list stands for its group when it leaves out at most one
    /// row in sixteen. The count it is judged by is an upper bound, so it may exceed the group;
    /// a group whose size is unknown is never "nearly all"; and one of fewer than sixteen rows
    /// only when the list is the whole of it.
    /// </summary>
    [Theory]
    [InlineData(16u,            16,           true)]
    [InlineData(16u,            15,           true)]
    [InlineData(16u,            14,           false)]
    [InlineData(32u,            30,           true)]
    [InlineData(32u,            29,           false)]
    [InlineData(15u,            15,           true)]
    [InlineData(15u,            14,           false)]
    [InlineData(1u,             1,            true)]
    [InlineData(1u,             0,            false)]
    [InlineData(10_000u,        9_375,        true)]
    [InlineData(10_000u,        9_374,        false)]
    [InlineData(16u,            40,           true)]
    [InlineData(0u,             0,            false)]
    [InlineData(0u,             5,            false)]
    [InlineData(4_000_000_000u, int.MaxValue, false)]   // compared as wide as the wider of the two
    public void A_list_stands_for_its_group_when_it_leaves_out_at_most_one_row_in_sixteen(uint group, int rows, bool whole) =>
        Assert.Equal(whole, QueryExecutor.NamesNearlyAllOf(group, rows));

    /// <summary>
    /// A marker that names nearly all of its group is not built and then discarded — it is never
    /// ASKED for. The narrowing asks the index how many rows the marker lists, which costs one
    /// read of the list's leading count, and stops there: alone, the group is carried as "every
    /// row"; beside a hint that does narrow, that hint narrows by itself. The rows the list
    /// would have left out arrive with the rest and are the scan's to reject.
    ///
    /// <para>What it replaced decoded the list, copied it and kept it, in every group of every
    /// segment a first page touched, to find that it named the group.</para>
    /// </summary>
    [Fact]
    public void A_marker_that_names_nearly_all_of_its_group_is_counted_and_never_listed()
    {
        const int Rows = 32;

        // One row in thirty-two has no exception: inside the one in sixteen a list may leave out.
        using var dense = Sharded(Rows, static row => row != 9);
        var asked = new RecordingIndex(dense);

        Assert.True(QueryExecutor.TryNarrowWithIndex(CompiledFilter.Compile("has(@x)"), asked, null, Rows, out uint[]? candidates, out bool whole));
        Assert.True(whole);
        Assert.Null(candidates);
        Assert.Equal([ClefFields.ExceptionExists], asked.Counted);
        Assert.Empty(asked.Listed);

        asked = new RecordingIndex(dense);
        Assert.True(QueryExecutor.TryNarrowWithIndex(CompiledFilter.Compile("has(@x) and Shard = 'a'"), asked, null, Rows, out candidates, out whole));
        Assert.False(whole);
        Assert.NotNull(candidates);
        Assert.Equal(new uint[] { 8, 9, 10, 11 }, candidates);      // row 9 has none: the scan rejects it
        Assert.Equal([ClefFields.ExceptionExists], asked.Counted);
        Assert.Equal(["Shard"], asked.Listed);

        // Four rows in thirty-two without one is past the line: the list is worth having, and is
        // asked for with the rest, as it always was.
        using var sparser = Sharded(Rows, static row => row % 8 != 1);
        asked = new RecordingIndex(sparser);
        Assert.True(QueryExecutor.TryNarrowWithIndex(CompiledFilter.Compile("has(@x)"), asked, null, Rows, out candidates, out whole));
        Assert.False(whole);
        Assert.NotNull(candidates);
        Assert.Equal(28, candidates.Length);
        Assert.Equal([ClefFields.ExceptionExists], asked.Listed);

        asked = new RecordingIndex(sparser);
        Assert.True(QueryExecutor.TryNarrowWithIndex(CompiledFilter.Compile("has(@x) and Shard = 'a'"), asked, null, Rows, out candidates, out whole));
        Assert.NotNull(candidates);
        Assert.Equal(new uint[] { 8, 10, 11 }, candidates);         // row 9 has none, and the list says so
        Assert.Equal([ClefFields.ExceptionExists + " + Shard"], asked.Listed);

        // A filter that does not ask for an exception counts nothing: the question is the marker's alone.
        asked = new RecordingIndex(dense);
        Assert.True(QueryExecutor.TryNarrowWithIndex(CompiledFilter.Compile("Shard = 'a'"), asked, null, Rows, out candidates, out _));
        Assert.Equal(new uint[] { 8, 9, 10, 11 }, candidates!);
        Assert.Empty(asked.Counted);

        // Any other key has to be listed to be recognised — and once it is, a list as long as
        // the group is the group, here every row being an Error: carried whole all the same.
        asked = new RecordingIndex(dense);
        Assert.True(QueryExecutor.TryNarrowWithIndex(CompiledFilter.Compile("@l = 'Error'"), asked, null, Rows, out candidates, out whole));
        Assert.True(whole);
        Assert.Null(candidates);
        Assert.Empty(asked.Counted);
        Assert.Equal([ClefFields.Level], asked.Listed);

        // Both at once: the marker counted and left out, the level listed and recognised.
        asked = new RecordingIndex(dense);
        Assert.True(QueryExecutor.TryNarrowWithIndex(CompiledFilter.Compile("has(@x) and @l = 'Error'"), asked, null, Rows, out candidates, out whole));
        Assert.True(whole);
        Assert.Null(candidates);
        Assert.Equal([ClefFields.ExceptionExists], asked.Counted);
        Assert.Equal([ClefFields.Level], asked.Listed);
    }

    /// <summary>
    /// What the narrowing is told about a filter, once, when it is compiled: whether the marker's
    /// hint is among its inverted hints — only then may a group whose marker lists no row be
    /// dropped — and which hints stand beside it, in their order, to narrow alone where the
    /// marker's list is not worth building.
    /// </summary>
    [Theory]
    [InlineData("has(@x)",                                  true,  "")]
    [InlineData("@x.exists = true",                         true,  "")]
    [InlineData("has(@x) and @x.exists != false",           true,  "")]
    [InlineData("has(@x) and Shard = 'a'",                  true,  "Shard")]
    [InlineData("Shard = 'a' and has(@x) and @l = 'Error'", true,  "Shard,@l")]
    [InlineData("Shard = 'a'",                              false, "Shard")]
    [InlineData("@x.exists = false and Shard = 'a'",        false, "Shard")]
    [InlineData("not has(@x)",                              false, "")]
    [InlineData("has(@x) or Shard = 'a'",                   false, "")]
    public void The_filter_says_whether_it_asks_for_an_exception_and_what_it_hints_beside(
        string expression, bool asks, string beside)
    {
        var filter = CompiledFilter.Compile(expression);
        var rest   = filter.GetInvertedHintsBesideExceptionPresence();

        Assert.Equal(asks, filter.HintsExceptionPresence);
        Assert.Equal(asks, filter.GetInvertedHints().Contains(CompiledFilter.ExceptionPresentHint));
        Assert.Equal(beside, string.Join(",", rest.Select(static hint => hint.property)));
        Assert.DoesNotContain(CompiledFilter.ExceptionPresentHint, rest);
        if (!asks) Assert.Same(filter.GetInvertedHints(), rest);     // nothing to leave out, nothing copied
    }

    /// <summary>
    /// One index group of <paramref name="rows"/> Error rows, as a query reads it: rows 8 to 11
    /// carry <c>Shard = 'a'</c> and the rest <c>'b'</c>, and a row carries an exception where
    /// <paramref name="hasException"/> says so.
    /// </summary>
    private static SegmentIndexReader Sharded(int rows, Func<int, bool> hasException)
    {
        byte[] a = Pack(static (ref MessagePackWriter w) => { w.WriteMapHeader(1); w.Write("Shard"); w.Write("a"); });
        byte[] b = Pack(static (ref MessagePackWriter w) => { w.WriteMapHeader(1); w.Write("Shard"); w.Write("b"); });
        byte[] boom = Boom.ToBytes();
        long ticks = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero).UtcTicks;

        using var builder = new SegmentIndexBuilder(rows);
        for (int i = 0; i < rows; i++)
        {
            var ev = new SegmentEventRef(new EventId(0u, (uint)i).RawValue, ticks + i, LogLevel.Error,
                                         0, 0, 0, "settle {OrderId}", "Billing.API",
                                         i is >= 8 and < 12 ? a : b, hasException(i) ? boom : []);
            builder.Add((uint)i, in ev);
        }
        return Index(builder);
    }

    /// <summary>An index that notes which keys it was asked to count and which to list.</summary>
    private sealed class RecordingIndex(ISegmentIndex inner) : ISegmentIndex
    {
        public List<string> Counted { get; } = [];
        public List<string> Listed  { get; } = [];

        public uint[]? Lookup(string propertyName, object? value)
        {
            Listed.Add(propertyName);
            return inner.Lookup(propertyName, value);
        }

        public bool MightContain(string propertyName, object? value) => inner.MightContain(propertyName, value);

        public uint[]? LookupTrigram(ReadOnlySpan<char> text) => inner.LookupTrigram(text);

        public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates)
        {
            Listed.Add(string.Join(" + ", predicates.Select(static p => p.property)));
            return inner.LookupIntersect(predicates);
        }

        public int? CountAtMost(string propertyName, object? value)
        {
            Counted.Add(propertyName);
            return inner.CountAtMost(propertyName, value);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>The ordinals of the events a filter matches — the scan, which is the answer owed.</summary>
    private static uint[] Scan(LogEvent[] events, string expression)
    {
        var filter = CompiledFilter.Compile(expression);
        var hits   = new List<uint>();
        foreach (var ev in events)
            if (filter.Matches(ev)) hits.Add((uint)ev.Id.RawValue);
        return [.. hits];
    }

    /// <summary>One index group pushed into <paramref name="sink"/>: a row per exception payload, at Error level.</summary>
    private static TSink Filled<TSink>(TSink sink, byte[][] exceptions) where TSink : ISegmentIndexSink
    {
        long ticks = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero).UtcTicks;
        for (int i = 0; i < exceptions.Length; i++)
        {
            var ev = new SegmentEventRef(new EventId(0u, (uint)i).RawValue, ticks + i, LogLevel.Error,
                                         0, 0, 0, "settle {OrderId}", "Billing.API", EmptyProps, exceptions[i]);
            sink.Add((uint)i, in ev);
        }
        return sink;
    }

    private static (byte[] Inverted, byte[] Trigram, byte[] Bloom) Sections(ISegmentIndexSink sink) => sink.Serialise();

    /// <summary>The group's index as a query reads it: the packed reader over the sink's sections.</summary>
    private static SegmentIndexReader Index(ISegmentIndexSink sink)
    {
        var (inverted, trigram, bloom) = Sections(sink);
        return SegmentIndexReader.Load(inverted, trigram, bloom);
    }

    /// <summary>msgpack fixmap of zero entries — an event with no properties.</summary>
    private static readonly byte[] EmptyProps = [0x80];

    private delegate void PackAction(ref MessagePackWriter writer);

    private static byte[] Pack(PackAction write)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
