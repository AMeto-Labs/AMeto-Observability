using System.Buffers;
using System.Globalization;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// A filter returns the same rows while its events sit in the hot tier and after they are flushed
/// into a segment, whose index groups the query then skips or narrows by value. Four ways that
/// broke, each found by the review of #163 and reproduced here through the real
/// <see cref="StorageEngine"/> and <see cref="QueryExecutor"/>: write rows, query, flush, query
/// again — uncached and through a <see cref="SegmentIndexCache"/>.
///
/// <list type="number">
/// <item>An integer past 2^53. The scan compared numbers as doubles, so every integer that rounds
/// to the same double was "equal" — up to 1 024 neighbouring ids at a time — and a quoted literal
/// past 2^53 was asked of the index in no integer form at all.</item>
/// <item>The scan's 1e-15 tolerance: <c>Ratio = 0.3</c> took a stored <c>0.1 + 0.2</c>, and no
/// index can list every double within a tolerance of another. <c>Delta = 0</c> against a stored
/// <c>-0.0</c> is the other half: IEEE equality, which the index was not asked about.</item>
/// <item>A stored <c>false</c> equalled any literal whose text is not <c>true</c> — <c>'banana'</c>,
/// <c>0</c>, <c>1</c>; the inverted index was asked for the <c>false</c> bucket, the keyless bloom
/// gate in front of it was not, and could not be without passing every group that holds a false
/// anywhere.</item>
/// <item>The host culture: the scan rendered a stored number with it (on <c>ru-KZ</c> 12.5 is
/// <c>12,5</c>), the index with the invariant culture.</item>
/// </list>
///
/// <para>Each test pins the rows themselves, not only that the tiers agree: where the fix changed
/// what the SCAN accepts — the big integers and the tolerance of 1 and 2, the bool rule of 3, all
/// of 4 — the old hot-tier answer is the one that changed, and the expectation says which rows a
/// filter means now.</para>
/// </summary>
public sealed class ValueFormsAcrossTheFlushTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-valueforms-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];
    private int _stores;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    private delegate void Value(ref MessagePackWriter w);

    /// <summary>One event: its name (the <c>Row</c> property the assertions read) and one more
    /// property, <paramref name="Property"/>, holding what <paramref name="Write"/> writes.</summary>
    private sealed record Row(string Name, string Property, Value Write);

    private static Row Long(string name, string property, long v)     => new(name, property, (ref MessagePackWriter w) => w.Write(v));
    private static Row ULong(string name, string property, ulong v)   => new(name, property, (ref MessagePackWriter w) => w.Write(v));
    private static Row Double(string name, string property, double v) => new(name, property, (ref MessagePackWriter w) => w.Write(v));
    private static Row Bool(string name, string property, bool v)     => new(name, property, (ref MessagePackWriter w) => w.Write(v));
    private static Row Text(string name, string property, string v)   => new(name, property, (ref MessagePackWriter w) => w.Write(v));

    private (StorageEngine Engine, QueryExecutor Query) Store()
    {
        string dir = Path.Combine(_dir, "s" + Interlocked.Increment(ref _stores));
        Directory.CreateDirectory(dir);
        var options = new ServerOptions { DataDirectory = dir };
        var engine  = new StorageEngine(
            Options.Create(options),
            new RetentionStore(options, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engines.Add(engine);
        engine.IndexSinkFactory = static (events, terms) => new SegmentIndexBuilder(events, 5, terms);
        return (engine, new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance));
    }

    private static void Write(StorageEngine engine, int ordinal, Row row)
    {
        var buf = new ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("Row"); w.Write(row.Name);
        w.Write(row.Property);
        row.Write(ref w);
        w.Flush();

        const string template = "settle {Row}";
        long baseTicks = new DateTimeOffset(2026, 11, 17, 9, 0, 0, TimeSpan.Zero).UtcTicks;
        Assert.True(engine.TryWrite(new LogEventHeader
        {
            Id                       = new EventId(0u, (uint)ordinal).RawValue,
            TimestampUtcTicks        = baseTicks + ordinal * TimeSpan.TicksPerSecond,
            Level                    = LogLevel.Warning,   // one level: a flush splits by level, and the rows must share a group
            MessageTemplatePoolIndex = engine.TemplatePool.Intern(template),
            ServiceNamePoolIndex     = engine.TemplatePool.Intern("Ledger.API"),
        }, buf.WrittenSpan, template, default));
    }

    private static async Task<string[]> RowsAsync(QueryExecutor query, string? filter)
    {
        var events = await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true);
        return [.. events.Select(static ev => (string)ev.Properties!["Row"]!)];
    }

    /// <summary>
    /// The rows <paramref name="filter"/> returns from the hot tier, then from the one index group
    /// the flush makes of them — read uncached and through the index cache — each equal to
    /// <paramref name="expected"/>, in write order.
    /// </summary>
    private Task AssertRowsAsync(string filter, Row[] rows, params string[] expected)
        => AssertRowsAsync(filter, rows, expected, [CultureInfo.CurrentCulture.Name]);

    /// <summary>
    /// <see cref="AssertRowsAsync(string, Row[], string[])"/>, asked under each of
    /// <paramref name="cultures"/> ("" is the invariant culture) — every one before the flush and
    /// every one after it, of one store. The culture is async-local, so it reaches the prefilter's
    /// parallel bodies too.
    /// </summary>
    private async Task AssertRowsAsync(string filter, Row[] rows, string[] expected, string[] cultures)
    {
        var (engine, query) = Store();
        for (int i = 0; i < rows.Length; i++) Write(engine, i, rows[i]);

        Assert.Empty(engine.ListSegments());
        var hot = new string[cultures.Length][];
        for (int c = 0; c < cultures.Length; c++)
            hot[c] = await InCultureAsync(cultures[c], () => RowsAsync(query, filter));

        await engine.FlushHotTierAsync();
        var segment = Assert.Single(engine.ListSegments());
        using (var reader = SegmentReader.Open(segment.FilePath))
        {
            Assert.Equal(1, reader.Groups.Length);
            Assert.Equal((uint)rows.Length, reader.Groups[0].EventCount);
        }

        using var cache = new SegmentIndexCache(16L * 1024 * 1024);
        var cachedQuery = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);
        for (int c = 0; c < cultures.Length; c++)
        {
            string[] flushed = await InCultureAsync(cultures[c], () => RowsAsync(query, filter));
            string[] cached  = await InCultureAsync(cultures[c], () => RowsAsync(cachedQuery, filter));
            string[] again   = await InCultureAsync(cultures[c], () => RowsAsync(cachedQuery, filter));   // the cache's memo

            string report = $"`{filter}` under '{cultures[c]}': expected [{string.Join(", ", expected)}], " +
                            $"hot [{string.Join(", ", hot[c])}], flushed [{string.Join(", ", flushed)}], " +
                            $"cached [{string.Join(", ", cached)}], cached again [{string.Join(", ", again)}]";
            Assert.True(expected.AsSpan().SequenceEqual(hot[c]),  "hot tier — " + report);
            Assert.True(expected.AsSpan().SequenceEqual(flushed), "flushed segment — " + report);
            Assert.True(expected.AsSpan().SequenceEqual(cached),  "flushed segment through the index cache — " + report);
            Assert.True(expected.AsSpan().SequenceEqual(again),   "the index cache's memo — " + report);
        }
    }

    private static async Task<T> InCultureAsync<T>(string culture, Func<Task<T>> body)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try { return await body(); }
        finally { CultureInfo.CurrentCulture = saved; }
    }

    // ── 1. An integer past 2^53 ───────────────────────────────────────────────

    /// <summary>
    /// <c>big</c> and <c>next</c> are neighbours that round to the same double,
    /// 1 234 567 890 123 456 768. Compared as doubles they were one value: <c>OrderNo =
    /// 1234567890123456789</c> returned order …790 too, and only while it was hot — the index files
    /// each long under its own digits, so the flushed group answered for exactly one of them.
    /// </summary>
    private static Row[] BigOrders() =>
    [
        Long("big",   "OrderNo", 1234567890123456789L),
        Long("next",  "OrderNo", 1234567890123456790L),
        Long("small", "OrderNo", 42L),
    ];

    [Theory]
    [InlineData("OrderNo = 1234567890123456789",        "big")]
    [InlineData("OrderNo = 1234567890123456790",        "next")]
    [InlineData("OrderNo = '1234567890123456789'",      "big")]   // the index had no integer form of a quoted literal past 2^53
    [InlineData("OrderNo = '1234567890123456789.0'",    "big")]
    [InlineData("OrderNo in ['1234567890123456789']",   "big")]
    [InlineData("OrderNo in [1234567890123456789]",     "big")]
    [InlineData("OrderNo <> 1234567890123456789",       "next", "small")]
    [InlineData("OrderNo > 1234567890123456789",        "next")]
    [InlineData("OrderNo >= 1234567890123456790",       "next")]
    [InlineData("OrderNo < 1234567890123456790",        "big", "small")]
    public Task An_integer_past_2_to_the_53_is_compared_exactly(string filter, params string[] expected)
        => AssertRowsAsync(filter, BigOrders(), expected);

    /// <summary>
    /// The number as a browser rounds the stored …789 — what the event view's "filter by this value"
    /// writes, since rows reach the client through <c>JSON.parse</c>. It is a different integer, so
    /// it matches neither order: it used to match both while they were hot and neither once they were
    /// flushed. Finding the row from the client needs its exact digits (a client change).
    /// </summary>
    [Theory]
    [InlineData("OrderNo = 1234567890123456800")]
    [InlineData("OrderNo = 1234567890123456768.0")]   // the double both orders round to, exactly
    [InlineData("OrderNo = '1.2345678901234568E+18'")]
    public Task A_neighbouring_integer_matches_no_order(string filter)
        => AssertRowsAsync(filter, BigOrders());

    /// <summary>
    /// A DOUBLE property is compared as a double, the literal read as the double nearest it — so the
    /// digits a browser shows for a stored 1234567890123456768.0, <c>1234567890123456800</c>, do find
    /// it, while beside it an integer property holding a neighbouring value is compared exactly.
    /// </summary>
    [Theory]
    [InlineData("Ts = 1234567890123456800",        "dbl")]
    [InlineData("Ts = 1234567890123456789",        "dbl", "lng")]
    [InlineData("Ts = '1.2345678901234568E+18'",   "dbl")]
    [InlineData("Ts in ['1.2345678901234568E+18']", "dbl")]
    [InlineData("Ts > 1234567890123456768",        "lng")]
    public Task A_double_past_2_to_the_53_is_read_as_a_double(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            Double("dbl", "Ts", 1234567890123456768d),
            Long("lng",   "Ts", 1234567890123456789L),
            Long("small", "Ts", 42L),
        ], expected);

    /// <summary>The same rules reach an array's elements and a nested property.</summary>
    [Theory]
    [InlineData("Ids = 1234567890123456789",         "arr")]
    [InlineData("Ids = '1234567890123456789'",       "arr")]
    [InlineData("Ids = 1234567890123456790")]
    [InlineData("Ids in ['1234567890123456789']",    "arr")]
    [InlineData("Order.No = 1234567890123456789",    "nested")]
    [InlineData("Order.No = '1234567890123456789'",  "nested")]
    [InlineData("Order.No = 1234567890123456800")]
    [InlineData("Order.Ratio = 0.3")]
    [InlineData("Order.Ratio = 0.30000000000000004", "nested")]
    [InlineData("Order.Zero = 0",                    "nested")]
    [InlineData("contains(Order.Ratio, '0.300')",    "nested")]
    public Task Arrays_and_nested_values_follow_the_same_rules(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            new("arr", "Ids", static (ref MessagePackWriter w) =>
            {
                w.WriteArrayHeader(2);
                w.Write(1234567890123456789L);
                w.Write(42L);
            }),
            new("nested", "Order", static (ref MessagePackWriter w) =>
            {
                w.WriteMapHeader(3);
                w.Write("No");    w.Write(1234567890123456789L);
                w.Write("Ratio"); w.Write(0.1 + 0.2);
                w.Write("Zero");  w.Write(-0.0);
            }),
            Long("plain", "Other", 7L),
        ], expected);

    /// <summary>2^53 + 1 is the first long a double cannot hold; it rounds to 2^53.</summary>
    [Theory]
    [InlineData("OrderNo = '9007199254740993'", "edge")]
    [InlineData("OrderNo = 9007199254740993",   "edge")]
    [InlineData("OrderNo = 9007199254740992",   "pow")]
    [InlineData("OrderNo = '9007199254740992'", "pow")]
    [InlineData("OrderNo = 9007199254740992.0", "pow")]
    public Task The_first_integer_a_double_cannot_hold(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            Long("pow",  "OrderNo", 9007199254740992L),
            Long("edge", "OrderNo", 9007199254740993L),
        ], expected);

    /// <summary>A uint64 above long.MaxValue is filed under its bare digits; a literal that big does
    /// not fit the parser's long and arrives as text.</summary>
    [Theory]
    [InlineData("OrderNo = 18446744073709551615",   "max")]
    [InlineData("OrderNo = '18446744073709551615'", "max")]
    [InlineData("OrderNo = 18446744073709551614",   "below")]
    [InlineData("OrderNo in ['18446744073709551614']", "below")]
    public Task An_unsigned_integer_past_long_max(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            ULong("max",   "OrderNo", ulong.MaxValue),
            ULong("below", "OrderNo", ulong.MaxValue - 1),
            Long("small",  "OrderNo", 42L),
        ], expected);

    // ── 2. The scan's tolerance, and IEEE zero ─────────────────────────────────

    /// <summary>
    /// <c>0.1 + 0.2</c> is 0.30000000000000004 — and that is what the event view shows and what
    /// "filter by this value" writes. A typed <c>0.3</c> means 0.3: the 1e-15 tolerance that also
    /// matched the sum was a band no index can list, and it held only while the rows were hot.
    /// </summary>
    [Theory]
    [InlineData("Ratio = 0.3",                 "third")]
    [InlineData("Ratio = '0.3'",               "third")]
    [InlineData("Ratio = 0.30000000000000004", "sum")]
    [InlineData("Ratio <> 0.3",                "sum", "other")]
    [InlineData("Ratio in [0.30000000000000004]", "sum")]
    public Task A_double_is_compared_exactly(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            Double("sum",   "Ratio", 0.1 + 0.2),
            Double("third", "Ratio", 0.3),
            Double("other", "Ratio", 0.75),
        ], expected);

    /// <summary>
    /// -0.0 equals 0 (IEEE, and the event view shows a stored -0.0 as <c>0</c>, so "filter by this
    /// value" writes <c>Delta = 0</c>). The index files it under its own spelling, <c>-0</c>, which
    /// no form of a zero literal named.
    /// </summary>
    [Theory]
    [InlineData("Delta = 0",      "neg", "pos", "long")]
    [InlineData("Delta = -0",     "neg", "pos", "long")]
    [InlineData("Delta = 0.0",    "neg", "pos", "long")]
    [InlineData("Delta = -0.0",   "neg", "pos", "long")]
    [InlineData("Delta = '-0'",   "neg", "pos", "long")]
    [InlineData("Delta <> 0",     "other")]
    [InlineData("Delta in ['-0']", "neg")]                 // `in` compares text, and -0.0 reads "-0"
    [InlineData("Delta in [0]",   "pos", "long")]
    public Task Negative_zero_equals_zero(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            Double("neg",   "Delta", -0.0),
            Double("pos",   "Delta", 0.0),
            Long("long",    "Delta", 0L),
            Double("other", "Delta", 7.5),
        ], expected);

    // ── 3. A stored bool against a literal that is not one ─────────────────────

    /// <summary>
    /// A stored bool equals a bool, or the text of one — <c>'true'</c> / <c>'false'</c> in any case.
    /// It used to read every other text as false, so a stored <c>false</c> equalled <c>'banana'</c>,
    /// <c>0</c> and <c>1</c> alike while hot; the bloom gate, asked for the literal's own text, then
    /// dropped the flushed group. The other literals now equal no bool, so <c>!=</c> holds for them.
    /// </summary>
    [Theory]
    [InlineData("P = 'banana'")]
    [InlineData("P = 0")]
    [InlineData("P = 1")]
    [InlineData("P = 'no'")]
    [InlineData("P = 2.5")]
    [InlineData("P = ' false'")]                           // the text is " false", not "false"
    [InlineData("P = 'false'",     "f")]
    [InlineData("P = 'FALSE'",     "f")]
    [InlineData("P = false",       "f")]
    [InlineData("P = 'TRUE'",      "t")]
    [InlineData("P = true",        "t")]
    [InlineData("P <> 'banana'",   "f", "t")]
    [InlineData("P <> 'false'",    "t")]
    [InlineData("P <> 0",          "f", "t")]
    [InlineData("P in ['false']",  "f")]
    [InlineData("P in ['banana']")]                        // `in` compares text: "False" is not "banana"
    public Task A_stored_bool_against_any_literal(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            Bool("f", "P", false),
            Bool("t", "P", true),
        ], expected);

    /// <summary>The other direction: a bool literal against a stored STRING compares as text.</summary>
    [Theory]
    [InlineData("Flag = true",  "lower", "upper")]
    [InlineData("Flag = false", "no")]
    public Task A_bool_literal_against_stored_text(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            Text("lower", "Flag", "true"),
            Text("upper", "Flag", "TRUE"),
            Text("no",    "Flag", "False"),
            Text("other", "Flag", "banana"),
        ], expected);

    // ── 4. The host culture ────────────────────────────────────────────────────

    /// <summary>
    /// A filter means the same thing on every host. The scan rendered a stored number in the host's
    /// culture for the text predicates — <c>12,5</c> on <c>ru-KZ</c> and <c>sv-SE</c>, whose minus is
    /// also U+2212 — while the index holds the invariant text. So <c>contains(Ratio, '2,5')</c>
    /// matched hot rows that a flush then lost, and <c>'2.5'</c> matched in neither tier.
    /// </summary>
    [Theory]
    [InlineData("contains(Ratio, '2.5')",   "num")]
    [InlineData("contains(Ratio, '2,5')")]
    [InlineData("startsWith(Ratio, '12.')", "num")]
    [InlineData("endsWith(Ratio, '.5')",    "num")]
    [InlineData("Ratio like '12.5'",        "num")]
    [InlineData("Ratio like '12,5'")]
    [InlineData("Ratio like '%2.5'",        "num")]
    [InlineData("Ratio in ['12.5']",        "num")]
    [InlineData("Ratio in ['12,5']")]
    [InlineData("Ratio = '12.5'",           "num")]
    [InlineData("regexMatch(Ratio, '^12\\\\.5$')", "num")]   // the lexer unescapes \\ to \, so the regex is ^12\.5$
    [InlineData("toLower(Ratio) = '12.5'",  "num")]
    [InlineData("contains(Ratio, '-3.25')", "neg")]
    [InlineData("Ratio in ['-3.25']",       "neg")]
    [InlineData("Ratio like '-3%'",         "neg")]
    public Task A_number_reads_the_same_on_every_host(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            Double("num",   "Ratio", 12.5),
            Double("neg",   "Ratio", -3.25),
            Double("other", "Ratio", 7.75),
        ], expected, Hosts);

    /// <summary>The invariant culture, two that write a decimal comma (sv-SE also a U+2212 minus), and en-US.</summary>
    private static readonly string[] Hosts = ["", "ru-KZ", "sv-SE", "en-US"];

    /// <summary>The other direction: a stored STRING against a numeric literal, which the scan
    /// rendered in the host culture to compare as text.</summary>
    [Theory]
    [InlineData("Code = 2.5", "dot")]
    [InlineData("Code = -5",  "minus")]
    public Task A_numeric_literal_reads_the_same_on_every_host(string filter, params string[] expected)
        => AssertRowsAsync(filter,
        [
            Text("dot",   "Code", "2.5"),
            Text("comma", "Code", "2,5"),
            Text("minus", "Code", "-5"),
        ], expected, Hosts);
}
