using System.Buffers;
using System.Diagnostics;
using MessagePack;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// The two per-event text predicates. LIKE matched by recursing at every position after
/// every wildcard — exponential in the number of <c>%</c> — and copied a lowercased value
/// per event; regex ran the interpreted engine per event with no timeout at all, so one
/// pattern typed into the search box could hold a core indefinitely. These tests pin the
/// semantics that must NOT change, and that the pathological shapes now finish.
/// </summary>
public sealed class LikeAndRegexTests
{
    private delegate void WriteProps(ref MessagePackWriter w);

    private static LogEvent Event(WriteProps writeProps)
    {
        var buf = new ArrayBufferWriter<byte>(256);
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

    private static LogEvent WithValue(string value) => Event((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(1);
        w.Write("Region"); w.Write(value);
    });

    private static bool Matches(LogEvent ev, string filter) => CompiledFilter.Compile(filter).Matches(ev);

    // ── LIKE semantics ────────────────────────────────────────────────────────

    [Theory]
    [InlineData("ae-dxb", "ae-dxb",   true)]    // literal
    [InlineData("ae-dxb", "AE-DXB",   true)]    // case-insensitive
    [InlineData("ae-dxb", "ae%",      true)]
    [InlineData("ae-dxb", "%dxb",     true)]
    [InlineData("ae-dxb", "%e-d%",    true)]
    [InlineData("ae-dxb", "ae_dxb",   true)]    // _ is exactly one char
    [InlineData("ae-dxb", "ae__dxb",  false)]
    [InlineData("ae-dxb", "%",        true)]
    [InlineData("ae-dxb", "%%",       true)]
    [InlineData("ae-dxb", "a%b",      true)]
    [InlineData("ae-dxb", "a%x",      false)]   // must end at x
    [InlineData("ae-dxb", "dxb",      false)]   // LIKE is anchored at both ends
    [InlineData("",       "%",        true)]
    [InlineData("",       "_",        false)]
    [InlineData("aaa",    "%a",       true)]
    [InlineData("aaa",    "a%a%a",    true)]
    [InlineData("aaa",    "a%a%a%a",  false)]
    public void Like_matches_exactly_as_specified(string value, string pattern, bool expected)
    {
        Assert.Equal(expected, Matches(WithValue(value), $"Region like '{pattern}'"));
    }

    [Fact]
    public void Like_survives_the_pattern_that_used_to_go_exponential()
    {
        // Six wildcards over a 40-char non-matching value: the recursive matcher branched
        // at every position after every one of them.
        var ev = WithValue(new string('a', 40) + "z");
        var sw = Stopwatch.StartNew();
        Assert.False(Matches(ev, "Region like '%a%a%a%a%a%a%q'"));
        Assert.True(sw.ElapsedMilliseconds < 1000, $"LIKE took {sw.ElapsedMilliseconds} ms — it should be linear");
    }

    // ── Regex ─────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("ae-dxb", "^ae",        true)]
    [InlineData("ae-dxb", "dxb$",       true)]
    [InlineData("ae-dxb", "^AE",        false)]   // case-sensitive by default
    [InlineData("ae-dxb", "[a-z]{2}-",  true)]
    [InlineData("ae-dxb", "^[0-9]+$",   false)]
    public void RegexMatch_keeps_its_semantics(string value, string pattern, bool expected)
    {
        Assert.Equal(expected, Matches(WithValue(value), $"regexMatch(Region, '{pattern}')"));
    }

    [Fact]
    public void RegexExtract_still_extracts_and_compares()
    {
        // Character classes rather than \d: the filter grammar consumes backslashes inside
        // single-quoted strings, which is its own (documented) wart, not this one.
        var ev = WithValue("order-4711");
        Assert.True(Matches(ev,  "regexExtract(Region, 'order-([0-9]+)', 1) = '4711'"));
        Assert.False(Matches(ev, "regexExtract(Region, 'order-([0-9]+)', 1) = '9999'"));
    }

    [Fact]
    public void A_catastrophic_pattern_no_longer_runs_away()
    {
        // (a+)+$ against a long non-matching value is the textbook ReDoS. The linear engine
        // takes it in its stride; a pattern it refuses would be bounded by the timeout.
        var ev = WithValue(new string('a', 32) + "!");
        var sw = Stopwatch.StartNew();
        Assert.False(Matches(ev, "regexMatch(Region, '^(a+)+$')"));
        Assert.True(sw.ElapsedMilliseconds < 2000, $"regex took {sw.ElapsedMilliseconds} ms — the ReDoS guard did not hold");
    }

    [Fact]
    public void A_lookahead_pattern_still_works_through_the_fallback_engine()
    {
        // Lookaround is exactly what the non-backtracking engine refuses at construction,
        // so this exercises the fallback path (backtracking + match timeout).
        Assert.True(Matches(WithValue("abab"),  "regexMatch(Region, '^(?=ab)abab$')"));
        Assert.False(Matches(WithValue("abcd"), "regexMatch(Region, '^(?=ab)abab$')"));
    }

    [Fact]
    public void An_invalid_pattern_is_reported_when_the_filter_is_parsed()
    {
        // Not per event, from the middle of a result stream, which is where the old static
        // Regex.IsMatch call threw.
        Assert.ThrowsAny<Exception>(() => CompiledFilter.Compile("regexMatch(Region, '([unclosed')"));
    }
}
