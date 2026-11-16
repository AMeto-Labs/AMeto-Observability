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
/// <para>Pinned here: the table row (every spelling, and deliberately NO index hint), the
/// evaluator's answer (<see cref="LogEvent.HasException"/>, so the same one <c>has(@x)</c>
/// gives and with nothing decoded), and why the index's own <c>@x.exists</c> bucket is not
/// asked — shown over a real index rather than asserted. The spellings also run through the
/// production prefilter in <c>IndexHintKeyTests</c>, and every payload shape through the
/// object-against-bytes oracle in <c>LazyExceptionEvaluationTests</c>.</para>
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

    [Fact]
    public void The_row_names_no_index_structure_a_hint_could_use()
    {
        Assert.Equal("@x.exists", ClefFields.ExceptionExists);
        Assert.Null(BuiltinFields.InvertedKey(BuiltinField.ExceptionExists));
        Assert.False(BuiltinFields.IsBloomIndexed(BuiltinField.ExceptionExists));
        Assert.False(BuiltinFields.IsTrigramIndexed(BuiltinField.ExceptionExists));
    }

    [Theory]
    [InlineData("@x.exists = true")]
    [InlineData("@x.exists = false")]
    [InlineData("@x.exists != true")]
    [InlineData("@x.exists = 'true'")]
    [InlineData("['@x.exists'] = true")]
    [InlineData("Exception.Exists = false")]
    [InlineData("@x.exists in [true]")]
    [InlineData("@x.exists in [false]")]
    [InlineData("@x.exists like 'tru%'")]
    [InlineData("contains(@x.exists, 'true')")]
    [InlineData("startsWith(@x.exists, 'tru')")]
    public void No_predicate_on_it_asks_the_index_anything(string expression)
    {
        var filter = CompiledFilter.Compile(expression);
        Assert.Empty(filter.GetInvertedHints());
        Assert.Empty(filter.GetTrigramHints());
        Assert.False(filter.TryGetIndexHint(out _, out _));
    }

    [Fact]
    public void A_conjunct_beside_it_keeps_its_own_hint()
    {
        // The example in docs/FILTER_EXPRESSIONS.md. `@x.exists` gives the index nothing to
        // answer and takes nothing away: the inner type still narrows the segment.
        var filter = CompiledFilter.Compile($"@x.exists = true and @x.inner.type = '{InnerType}'");

        var hint = Assert.Single(filter.GetInvertedHints());
        Assert.Equal(ClefFields.ExceptionInnerType, hint.property);
        Assert.Equal(InnerType, hint.value);

        Assert.True(filter.TryGetIndexHint(out string key, out object? value));
        Assert.Equal(ClefFields.ExceptionInnerType, key);
        Assert.Equal(InnerType, value);
    }

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
        Dictionary<string, object?>? props = null) => new()
    {
        Id              = new EventId(0u, 1u),
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

    // ── Why the index's own bucket is not asked ───────────────────────────────

    /// <summary>
    /// <c>SegmentIndexBuilder</c> files an <c>@x.exists</c> bucket, and the row gives the field
    /// no hint all the same. This is the reason, over an index the builder really wrote: the
    /// bucket is a presence MARKER, not the field's values.
    ///
    /// <para>It holds "true" for the rows whose exception the builder could READ — so a hint
    /// for <c>@x.exists = true</c> would lose the row whose map it could not read, which the
    /// evaluator (and <c>has(@x)</c>) counts. And it holds nothing else — so a hint for
    /// <c>@x.exists = false</c> would meet "known bucket, no such value", which the executor
    /// takes as proof and drops the group on, where two rows match. Either hint costs the query
    /// rows; scanning costs it only time, and no more than <c>has(@x)</c> has always cost.</para>
    /// </summary>
    [Fact]
    public void The_index_bucket_of_the_same_name_cannot_answer_for_the_field()
    {
        const ulong WellFormed = 7001, NoException = 7002, LegacyString = 7003, UnreadableMap = 7004, Nil = 7005;

        using var hot  = new HotTierSegment(16, 64 * 1024);
        var       pool = new StringInternPool();

        Write(hot, pool, WellFormed,    Boom.ToBytes());
        Write(hot, pool, NoException,   default);
        // Legacy @x: a plain string. An exception to the evaluator and to the builder alike.
        Write(hot, pool, LegacyString,  Pack(static (ref MessagePackWriter w) => w.Write("System.Exception: plain text")));
        // A map whose `type` is not a string. LogEvent.HasException answers from the lead byte
        // and says yes; the builder cannot read its index fields and files the row nowhere.
        Write(hot, pool, UnreadableMap, Pack(static (ref MessagePackWriter w) => { w.WriteMapHeader(1); w.Write("type"); w.Write(7); }));
        Write(hot, pool, Nil,           NilPayload);

        hot.Freeze();
        List<LogEvent> events = [.. hot.ReadAll(pool)];

        var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool);
        var index = SegmentIndexReader.Load(
            builder.SerialisedInvertedIndex, builder.SerialisedTrigramIndex, builder.SerialisedBloomFilter);

        // What the field answers, and that it is has(@x)'s answer.
        ulong[] with    = [WellFormed, LegacyString, UnreadableMap];
        ulong[] without = [NoException, Nil];
        Assert.Equal(with,    Scan(events, "@x.exists = true"));
        Assert.Equal(with,    Scan(events, "has(@x)"));
        Assert.Equal(without, Scan(events, "@x.exists = false"));
        Assert.Equal(without, Scan(events, "@x.exists != true"));
        Assert.Equal(without, Scan(events, "not has(@x)"));

        // What the bucket would have answered. Offsets are write order: 0..4.
        Assert.Equal(1, builder.MalformedExceptionPayloads);

        uint[]? forTrue = index.LookupIntersect([(ClefFields.ExceptionExists, (object?)true)]);
        Assert.NotNull(forTrue);
        Assert.Equal([0u, 2u], forTrue);              // offset 3, the unreadable map, is not there

        uint[]? forFalse = index.LookupIntersect([(ClefFields.ExceptionExists, (object?)false)]);
        Assert.NotNull(forFalse);                     // not "no information": the bucket is known…
        Assert.Empty(forFalse);                       // …and proves no row matches, where two do
    }

    private static ulong[] Scan(List<LogEvent> events, string expression)
    {
        var filter = CompiledFilter.Compile(expression);
        var hits   = new List<ulong>();
        foreach (var ev in events)
            if (filter.Matches(ev)) hits.Add(ev.Id.RawValue);
        return [.. hits];
    }

    private static void Write(HotTierSegment hot, StringInternPool pool, ulong id, ReadOnlySpan<byte> exception)
    {
        const string Template = "settle {OrderId}";
        var header = new LogEventHeader
        {
            Id                       = id,
            TimestampUtcTicks        = new DateTimeOffset(2026, 11, 16, 9, 0, (int)(id % 60), TimeSpan.Zero).UtcTicks,
            MessageTemplatePoolIndex = pool.Intern(Template),
            ServiceNamePoolIndex     = pool.Intern("Billing.API"),
            Level                    = LogLevel.Error,
        };
        Assert.True(hot.TryWrite(header, EmptyProps, Template, exception));
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
