using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// A text predicate on a plain top-level property is answered straight out of the event's
/// msgpack, into a stack buffer: <c>TryReadPropertyText</c> instead of
/// <c>TryReadProperty</c> plus the string it boxed and threw away per candidate event.
///
/// <para>Two roads now exist to the same value — the unboxed one, and the dictionary once
/// something has materialised it — so what has to be pinned is that they AGREE: same answer
/// for every predicate, including the last-wins duplicate-key rule that
/// <c>PropertyProbeTests</c> fixes for the boxing road. Everything the unboxed probe declines
/// (arrays, nested maps, numbers, values past the scratch ceiling, dotted paths, built-in
/// fields) must fall through and behave exactly as before, which is the other half of the
/// sweep below.</para>
/// </summary>
public sealed class UnboxedPropertyTextTests
{
    private delegate void WriteProps(ref MessagePackWriter w);

    private static LogEvent Event(WriteProps writeProps)
    {
        var buf = new ArrayBufferWriter<byte>(4096);
        var w   = new MessagePackWriter(buf);
        writeProps(ref w);
        w.Flush();

        return new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 10, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Information,
            MessageTemplate = "request handled",
            RawProperties   = buf.WrittenMemory,
        };
    }

    private static readonly string LongValue = new('x', 700);   // past the 512-char scratch

    private static LogEvent Sample() => Event(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(8);
        w.Write("RequestPath");         w.Write("/api/v1/users/42");
        w.Write("Unicode");             w.Write("ключ 😀 straße");
        w.Write("Elapsed");             w.Write(125L);
        w.Write("Flag");                w.Write(true);
        w.Write("Nil");                 w.WriteNil();
        w.Write("Tags");                w.WriteArrayHeader(2); w.Write("red"); w.Write("blue");
        w.Write("Nested");              w.WriteMapHeader(1); w.Write("inner"); w.Write("deep");
        w.Write("Long");                w.Write(new string('x', 700));
    });

    private static bool Matches(LogEvent ev, string filter) => CompiledFilter.Compile(filter).Matches(ev);

    // ── the two roads agree ───────────────────────────────────────────────────

    /// <summary>
    /// Every predicate answered twice: once off the raw msgpack (the unboxed road) and once
    /// against an event whose dictionary is already built (the old road). A disagreement here
    /// is a filter whose answer depends on whether something else happened to materialise the
    /// map first — the exact failure the probe's last-wins rule exists to prevent.
    /// </summary>
    [Theory]
    [InlineData("RequestPath like '%users%'",        true)]
    [InlineData("RequestPath like '%orders%'",       false)]
    [InlineData("RequestPath like '/api%'",          true)]
    [InlineData("RequestPath like '%42'",            true)]
    [InlineData("RequestPath like '/api/v1/users/42'", true)]
    [InlineData("Unicode like '%Straße%'",           true)]   // non-ASCII: the folding matcher
    [InlineData("Unicode like '%STRASSE%'",          false)]  // ß does not fold to ss here
    [InlineData("Unicode like '%СТРА%'",             false)]  // Cyrillic С is not Latin S
    [InlineData("Unicode like '%ключ%'",             true)]
    [InlineData("Unicode like '%КЛЮЧ%'",             true)]
    [InlineData("Unicode like '%nope%'",             false)]
    [InlineData("contains(RequestPath, 'v1/users')", true)]
    [InlineData("contains(RequestPath, 'nope')",     false)]
    [InlineData("startsWith(RequestPath, '/api')",   true)]
    [InlineData("startsWith(RequestPath, 'api')",    false)]
    [InlineData("endsWith(RequestPath, '/42')",      true)]
    [InlineData("endsWith(RequestPath, '/43')",      false)]
    [InlineData("Missing like '%x%'",                false)]  // absent key
    [InlineData("contains(Missing, 'x')",            false)]
    [InlineData("Tags like '%blue%'",                true)]   // array: declines, falls through
    [InlineData("Tags like '%green%'",               false)]
    [InlineData("Elapsed like '12%'",                true)]   // number: declines, falls through
    [InlineData("Nil like '%x%'",                    false)]
    [InlineData("Flag like 'Tr%'",                   true)]
    [InlineData("Long like '%xxx%'",                 true)]   // past the scratch: falls through
    [InlineData("Long like 'x%'",                    true)]
    [InlineData("@mt like '%handled%'",              true)]   // built-in field: untouched road
    public void The_unboxed_road_and_the_dictionary_road_agree(string filter, bool expected)
    {
        var raw = Sample();
        Assert.Equal(expected, Matches(raw, filter));

        var materialised = Sample();
        _ = materialised.Properties;                       // force the dictionary first
        Assert.True(materialised.PropertiesMaterialised);
        Assert.Equal(expected, Matches(materialised, filter));
    }

    /// <summary>
    /// The map is still not built for a text predicate — that is the whole point of the probe,
    /// and it is also what makes the agreement above meaningful rather than accidental.
    /// </summary>
    [Theory]
    [InlineData("RequestPath like '%users%'")]
    [InlineData("contains(RequestPath, 'users')")]
    [InlineData("startsWith(RequestPath, '/api')")]
    [InlineData("endsWith(RequestPath, '42')")]
    [InlineData("Missing like '%x%'")]
    public void A_text_predicate_never_materialises_the_map(string filter)
    {
        var ev = Sample();
        Matches(ev, filter);
        Assert.False(ev.PropertiesMaterialised,
            "a text predicate read one value — the whole map must not have been deserialised");
    }

    // ── the probe itself ──────────────────────────────────────────────────────

    [Fact]
    public void Reads_a_string_value_into_the_caller_s_buffer()
    {
        var ev = Sample();
        Span<char> buf = stackalloc char[512];

        Assert.True(LogEventSerializer.TryReadPropertyText(ev.RawProperties, "RequestPath", buf, out int n, out bool present));
        Assert.True(present);
        Assert.Equal("/api/v1/users/42", buf[..n].ToString());

        Assert.True(LogEventSerializer.TryReadPropertyText(ev.RawProperties, "Unicode", buf, out n, out present));
        Assert.Equal("ключ 😀 straße", buf[..n].ToString());
    }

    /// <summary>
    /// An absent key says so POSITIVELY — <c>keyPresent</c> false — because that is what lets
    /// the evaluator answer null without a second walk. A key that IS there but holds something
    /// the probe does not decode must NOT look the same, or the caller would stop instead of
    /// falling through and an array-valued property would quietly stop matching.
    /// </summary>
    [Theory]
    [InlineData("Missing", false, false)]
    [InlineData("Tags",    false, true)]
    [InlineData("Nested",  false, true)]
    [InlineData("Elapsed", false, true)]
    [InlineData("Flag",    false, true)]
    [InlineData("Nil",     false, true)]
    [InlineData("Long",    false, true)]   // present, a string, but past the buffer
    [InlineData("Unicode", true,  true)]
    public void Declines_with_the_reason_the_caller_needs(string key, bool answered, bool present)
    {
        var ev = Sample();
        Span<char> buf = stackalloc char[512];
        Assert.Equal(answered, LogEventSerializer.TryReadPropertyText(ev.RawProperties, key, buf, out _, out bool p));
        Assert.Equal(present, p);
    }

    /// <summary>
    /// LAST occurrence wins, as it does for every other probe and for the dictionary itself.
    /// OTLP flattens resource and record attributes into one map with no dedup, so a repeated
    /// key is ordinary traffic, not a malformed payload.
    /// </summary>
    [Fact]
    public void Duplicate_keys_keep_the_last_value()
    {
        var ev = Event(static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(3);
            w.Write("service.env"); w.Write("resource-level");
            w.Write("other");       w.Write("x");
            w.Write("service.env"); w.Write("record-level");
        });

        Span<char> buf = stackalloc char[64];
        Assert.True(LogEventSerializer.TryReadPropertyText(ev.RawProperties, "service.env", buf, out int n, out _));
        Assert.Equal("record-level", buf[..n].ToString());

        // …and the predicate that reads it agrees with the dictionary that would be built.
        Assert.True(Matches(ev, "['service.env'] like 'record%'"));
        Assert.False(Matches(ev, "['service.env'] like 'resource%'"));
    }

    /// <summary>A duplicate whose LAST occurrence is not a plain string must decline, not
    /// answer with the earlier string — the two roads would disagree otherwise.</summary>
    [Fact]
    public void Duplicate_keys_are_judged_on_the_last_value_s_type()
    {
        var ev = Event(static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(2);
            w.Write("dup"); w.Write("a-string");
            w.Write("dup"); w.WriteArrayHeader(1); w.Write("in-an-array");
        });

        Span<char> buf = stackalloc char[64];
        Assert.False(LogEventSerializer.TryReadPropertyText(ev.RawProperties, "dup", buf, out _, out bool present));
        Assert.True(present);
        Assert.True(Matches(ev, "dup like '%in-an-array%'"));
        Assert.False(Matches(ev, "dup like '%a-string%'"));
    }

    // ── the numeric road ──────────────────────────────────────────────────────

    private static LogEvent Numbers() => Event(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(10);
        w.Write("i32");     w.Write(125);
        w.Write("i64");     w.Write(9_000_000_000L);
        w.Write("u64");     w.Write(ulong.MaxValue);
        w.Write("dbl");     w.Write(1.5d);
        w.Write("flt");     w.Write(2.5f);
        w.Write("neg");     w.Write(-17);
        w.Write("zero");    w.Write(0);
        w.Write("notNum");  w.Write("NaN");           // a STRING that looks numeric
        w.Write("textNum"); w.Write("125");           // a STRING that IS numeric
        w.Write("arr");     w.WriteArrayHeader(2); w.Write(1); w.Write(2);
    });

    /// <summary>
    /// A numeric comparison against a plain top-level key reads the number straight out of the
    /// msgpack instead of boxing it. The road is narrow — numeric literal, numeric value — and
    /// everything outside it must keep the semantics it had, including the ones that look
    /// surprising: a STRING-valued property compared with a number goes down the string road
    /// and compares textually, and it still must.
    /// </summary>
    [Theory]
    [InlineData("i32 = 125",        true)]
    [InlineData("i32 <> 125",       false)]
    [InlineData("i32 > 100",        true)]
    [InlineData("i32 >= 125",       true)]
    [InlineData("i32 < 100",        false)]
    [InlineData("i64 = 9000000000", true)]
    [InlineData("i64 > 8999999999", true)]
    [InlineData("u64 > 0",          true)]
    [InlineData("dbl = 1.5",        true)]
    [InlineData("dbl > 1.4",        true)]
    [InlineData("flt = 2.5",        true)]
    [InlineData("neg = -17",        true)]
    [InlineData("neg < 0",          true)]
    [InlineData("zero = 0",         true)]
    [InlineData("zero <> 0",        false)]
    [InlineData("missing = 1",      false)]   // absent: false for everything…
    [InlineData("missing > 1",      false)]
    [InlineData("missing < 1",      false)]
    [InlineData("missing <> 1",     true)]    // …except Ne, which is what Compare(null, …) says
    [InlineData("notNum = 0",       false)]   // string value vs number: the string road
    [InlineData("notNum <> 0",      true)]
    [InlineData("textNum = 125",    true)]    // "125" coerces on the string road's numeric tail
    [InlineData("i32 = '125'",      true)]    // number value vs STRING literal: not the fast road
    [InlineData("i32 = 'x'",        false)]
    [InlineData("arr = 1",          true)]    // array value: match-any, not the fast road
    [InlineData("arr = 3",          false)]
    public void The_numeric_road_and_the_dictionary_road_agree(string filter, bool expected)
    {
        var raw = Numbers();
        Assert.Equal(expected, Matches(raw, filter));

        var materialised = Numbers();
        _ = materialised.Properties;
        Assert.True(materialised.PropertiesMaterialised);
        Assert.Equal(expected, Matches(materialised, filter));
    }

    /// <summary>
    /// A duplicate key whose LAST value is not a number must decline, not answer with the
    /// earlier number — the two roads would disagree about the same event otherwise.
    /// </summary>
    [Fact]
    public void A_duplicate_whose_last_value_is_not_a_number_declines()
    {
        var ev = Event(static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(2);
            w.Write("dup"); w.Write(125);
            w.Write("dup"); w.Write("not a number");
        });

        Assert.False(LogEventSerializer.TryReadPropertyNumber(ev.RawProperties, "dup", out _, out bool present));
        Assert.True(present);

        // …and the predicate agrees with the dictionary that would be built.
        Assert.False(Matches(ev, "dup = 125"));
        Assert.True(Matches(ev, "dup <> 125"));
    }

    [Fact]
    public void A_numeric_predicate_never_materialises_the_map_and_allocates_nothing()
    {
        var ev     = Numbers();
        var filter = CompiledFilter.Compile("i32 > 100");

        for (int i = 0; i < 100; i++) filter.Matches(ev);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++) filter.Matches(ev);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.False(ev.PropertiesMaterialised);
        Assert.True(bytes == 0, $"expected zero allocation over 1000 evaluations, saw {bytes} B");
    }

    [Fact]
    public void Numbers_are_read_without_a_box()
    {
        var ev = Sample();
        Assert.True(LogEventSerializer.TryReadPropertyNumber(ev.RawProperties, "Elapsed", out double n, out bool present));
        Assert.Equal(125d, n);
        Assert.True(present);

        Assert.False(LogEventSerializer.TryReadPropertyNumber(ev.RawProperties, "RequestPath", out _, out present));
        Assert.True(present);

        Assert.False(LogEventSerializer.TryReadPropertyNumber(ev.RawProperties, "Missing", out _, out present));
        Assert.False(present);
    }

    [Fact]
    public void A_text_predicate_allocates_nothing_per_event()
    {
        var ev     = Sample();
        var filter = CompiledFilter.Compile("RequestPath like '%users%'");

        for (int i = 0; i < 100; i++) filter.Matches(ev);   // warm up, and prove the map stays raw

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++) filter.Matches(ev);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.False(ev.PropertiesMaterialised);
        Assert.True(bytes == 0, $"expected zero allocation over 1000 evaluations, saw {bytes} B");
    }
}
