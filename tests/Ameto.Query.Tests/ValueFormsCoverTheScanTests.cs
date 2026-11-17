using System.Buffers;
using System.Globalization;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The law the segment index lives by — it may cost a query its acceleration, never its rows —
/// asked of every pairing of a stored value and a literal at once, rather than of the few a bug
/// report happened to name.
///
/// <para>For each stored value one event is indexed by the real <see cref="SegmentIndexBuilder"/>.
/// For each literal, whenever the scan pairs the two — <c>P = literal</c> or <c>P in [literal]</c>,
/// through the evaluator's own comparison — the group's index has to find the event: the bloom
/// gate and the inverted check (<see cref="SegmentIndexReader.MightContain"/>) and the posting list
/// (<see cref="SegmentIndexReader.Lookup"/>). And the scan has to give the same answers on every
/// host culture, because the index's bytes do not depend on one.</para>
///
/// <para>The values are the ones that broke it: integers past 2^53 and past long.MaxValue, a
/// double's noise, both zeros, infinities and NaN, bools against text and numbers, and numerals
/// spelled every way the grammar accepts.</para>
/// </summary>
public sealed class ValueFormsCoverTheScanTests
{
    /// <summary>What a property can hold once decoded: msgpack integers arrive as long (or ulong
    /// past long.MaxValue), floats as double.</summary>
    private static readonly object?[] Stored =
    [
        0L, 5L, -5L, 42L, 12345L, 9007199254740991L, 9007199254740992L, 9007199254740993L,
        1234567890123456789L, 1234567890123456790L, long.MaxValue, long.MinValue,
        9223372036854775808UL, ulong.MaxValue - 1, ulong.MaxValue,
        0.0, -0.0, 0.3, 0.1 + 0.2, 12.5, -2.5, -3.25, 5.0, 0.75, 1e20, 1.2345678901234568E18,
        Math.Pow(2, 60), 9007199254740992.0, double.Epsilon, 1e300,
        double.PositiveInfinity, double.NegativeInfinity, double.NaN,
        true, false,
        "5", "05", "12.5", "12,5", "2.5", "-5", "true", "TRUE", "false", "banana", "", "0", "-0",
        "1234567890123456789", "Infinity", "NaN", "1e3", "True",
    ];

    /// <summary>What a filter literal can be: the parser makes a long, a double (when the token
    /// has a point), a bool, or text — a quoted value, or a numeral too big for a long.</summary>
    private static readonly object[] Literals =
    [
        0L, 5L, -5L, 1L, 42L, 12345L, 9007199254740992L, 9007199254740993L,
        1234567890123456789L, 1234567890123456790L, 1234567890123456800L, long.MaxValue, long.MinValue,
        0.0, -0.0, 0.3, 0.30000000000000004, 12.5, -2.5, 5.0, 2.5, 1234567890123456768.0, 1e20,
        true, false,
        "5", "05", " 5 ", "5.0", "+5", "(5)", "1e3", "1,000", "12.5", "12,5", "2.5", "-2.5", "-5",
        "0", "-0", "0.0", "true", "TRUE", "True", "false", "False", " false", "no", "banana", "",
        "1234567890123456789", "1234567890123456789.0", "1.2345678901234568E+18",
        "9223372036854775808", "18446744073709551614", "18446744073709551615",
        "9007199254740993", "0.3", "0.30000000000000004", "Infinity", "-Infinity", "NaN", "1e300",
    ];

    private static readonly string[] Cultures = ["", "ru-KZ", "sv-SE", "de-DE", "en-US"];

    [Fact]
    public void Every_value_the_scan_pairs_with_a_literal_is_found_by_the_index()
    {
        var misses = new List<string>();
        foreach (object? stored in Stored)
        {
            using var index = IndexOf(stored);
            foreach (object literal in Literals)
            {
                // The answer the scan gives on an invariant host; the next test holds every other
                // host to the same answer, so the index (written invariantly) owes only this one.
                var (eq, ne, inList) = InCulture("", () => Scan(stored, literal));
                Assert.False(eq && ne, $"{Show(stored)} is both = and != {Show(literal)}");
                if (!eq && !inList) continue;

                bool might     = index.MightContain("P", literal);
                uint[]? posted = index.Lookup("P", literal);
                if (!might || posted is null || !posted.Contains(0u))
                    misses.Add($"{Show(stored)} {(eq ? "=" : "in")} {Show(literal)}: " +
                               $"bloom+inverted gate {might}, postings [{string.Join(",", posted ?? [])}]");
            }
        }
        Assert.True(misses.Count == 0, "the index lost rows the scan matches:\n" + string.Join("\n", misses));
    }

    [Fact]
    public void The_scan_gives_the_same_answer_on_every_host()
    {
        var differ = new List<string>();
        foreach (object? stored in Stored)
            foreach (object literal in Literals)
            {
                var invariant = InCulture("", () => Scan(stored, literal));
                foreach (string culture in Cultures)
                {
                    var here = InCulture(culture, () => Scan(stored, literal));
                    if (here != invariant)
                        differ.Add($"{Show(stored)} vs {Show(literal)} under {culture}: (=, !=, in) {here}, invariant {invariant}");
                }
            }
        Assert.True(differ.Count == 0, string.Join("\n", differ));
    }

    /// <summary>
    /// A stored scalar's text — what <c>contains</c>, <c>like</c> and <c>in</c> read — is the text
    /// the index trigrammed and put in the bloom, on every host.
    /// </summary>
    [Fact]
    public void A_stored_scalar_reads_as_the_text_the_index_holds()
    {
        foreach (string culture in Cultures)
            InCulture(culture, () =>
            {
                foreach (object? stored in Stored)
                {
                    if (stored is null) continue;
                    string text = IndexValueForms.PlainText(stored);
                    Assert.True(FilterEvaluator.InValues(stored, [text]), $"{Show(stored)} does not read as \"{text}\" under '{culture}'");

                    if (text.Length < 3 || stored is string) continue;
                    using var index = IndexOf(stored);
                    uint[]? found = index.LookupTrigram(text);
                    Assert.True(found is not null && found.Contains(0u), $"the trigram index has no \"{text}\" for {Show(stored)}");
                }
                return 0;
            });
    }

    /// <summary>
    /// The form lists stay inside <see cref="IndexValueForms.MaxForms"/> — running out throws —
    /// with the margin its comment claims, in the cultures that add spellings.
    /// </summary>
    [Fact]
    public void The_form_lists_fit_their_buffer()
    {
        int most = 0, mostPlain = 0;
        foreach (string culture in Cultures)
            InCulture(culture, () =>
            {
                foreach (object literal in Literals)
                {
                    IndexValueForms.Buffer buf = default;
                    most      = Math.Max(most, IndexValueForms.Serialised(literal, buf));
                    mostPlain = Math.Max(mostPlain, IndexValueForms.Plain(literal, buf));
                }
                return 0;
            });
        Assert.Equal(8, most);       // a zero literal where the culture spells -0 its own way
        Assert.True(mostPlain <= 4, $"Plain built {mostPlain} forms");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (bool Eq, bool Ne, bool In) Scan(object? stored, object literal) =>
        (FilterEvaluator.CompareValues(stored, literal, CompareOp.Eq),
         FilterEvaluator.CompareValues(stored, literal, CompareOp.Ne),
         FilterEvaluator.InValues(stored, [literal]));

    private static T InCulture<T>(string culture, Func<T> body)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try { return body(); }
        finally { CultureInfo.CurrentCulture = saved; }
    }

    private static string Show(object? v) => v switch
    {
        null     => "null",
        string s => $"'{s}'",
        double d => d.ToString("R", CultureInfo.InvariantCulture) + "d",
        _        => $"{v} ({v.GetType().Name})",
    };

    /// <summary>One event carrying <c>P</c> = <paramref name="value"/>, indexed the way a flush
    /// indexes a group, and read back the way a query reads one.</summary>
    private static SegmentIndexReader IndexOf(object? value)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("P");
        switch (value)
        {
            case null:     w.WriteNil(); break;
            case long l:   w.Write(l);   break;
            case ulong u:  w.Write(u);   break;
            case double d: w.Write(d);   break;
            case bool b:   w.Write(b);   break;
            case string s: w.Write(s);   break;
            default: throw new ArgumentException(value.GetType().Name);
        }
        w.Flush();

        var pool = new StringInternPool();
        using var hot = new HotTierSegment(4, 1024 * 1024);
        Assert.True(hot.TryWrite(new LogEventHeader
        {
            Id                       = new EventId(0u, 0u).RawValue,
            TimestampUtcTicks        = new DateTimeOffset(2026, 11, 17, 9, 0, 0, TimeSpan.Zero).UtcTicks,
            Level                    = LogLevel.Warning,
            MessageTemplatePoolIndex = pool.Intern("settle"),
            ServiceNamePoolIndex     = pool.Intern("Ledger.API"),
        }, buf.WrittenSpan, "settle", default));

        using var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool);
        return SegmentIndexReader.Load(builder.SerialisedInvertedIndex, builder.SerialisedTrigramIndex, builder.SerialisedBloomFilter);
    }
}
