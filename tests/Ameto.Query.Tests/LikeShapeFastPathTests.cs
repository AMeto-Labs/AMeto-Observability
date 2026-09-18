using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// The ASCII shortcut in <c>LikeMatchFast</c>: <c>%lit%</c>, <c>lit%</c>, <c>%lit</c> and a
/// bare literal are answered by a vectorised <c>Contains/StartsWith/EndsWith/Equals</c>
/// under <c>OrdinalIgnoreCase</c> instead of the per-character folding matcher.
///
/// <para>Two things have to hold and both are pinned here. First the REDUCTION: which patterns
/// count as which shape, and — just as important — which do not, because a pattern with a
/// wildcard left inside the literal (<c>%a%b%</c>, <c>%a_b%</c>) answered by a plain Contains
/// would match strings LIKE never matched. Second the EQUIVALENCE: over ASCII the shortcut and
/// the matcher must give the same answer for every pattern and every value, and outside ASCII
/// the shortcut must not be taken at all — <c>OrdinalIgnoreCase</c> and the
/// <c>ToLowerInvariant</c> folding the matcher uses disagree there (the Kelvin sign, the
/// supplementary-plane letters), and those cases are the reason the guard exists.</para>
/// </summary>
public sealed class LikeShapeFastPathTests
{
    // ── the reduction ─────────────────────────────────────────────────────────

    [Fact]
    public void Reduces_the_vectorisable_shapes()
    {
        AssertShape("%timeout%", LikeShape.Contains,   "timeout");
        AssertShape("Handled%",  LikeShape.StartsWith, "handled");   // lowered once, at compile time
        AssertShape("%request",  LikeShape.EndsWith,   "request");
        AssertShape("exact",     LikeShape.Equals,     "exact");
        AssertShape("%%",        LikeShape.Contains,   "");          // matches everything, as the matcher does
        AssertShape("%A%",       LikeShape.Contains,   "a");
        AssertShape("a%",        LikeShape.StartsWith, "a");

        static void AssertShape(string pattern, LikeShape shape, string affix)
        {
            var node = new LikeNode("Region", pattern);
            Assert.Equal(shape, node.Shape);
            Assert.Equal(affix, node.Affix);
            Assert.True(node.AffixAscii);
        }
    }

    [Fact]
    public void Leaves_the_general_shapes_to_the_matcher()
    {
        foreach (var pattern in new[] { "%a%b%", "%a_b%", "a%b", "ae_dxb", "%_%", "a_%" })
            Assert.Equal(LikeShape.None, new LikeNode("Region", pattern).Shape);
    }

    [Fact]
    public void MatchAll_is_not_given_a_shape()
    {
        var node = new LikeNode("Region", "%");
        Assert.True(node.IsMatchAll);
        Assert.Equal(LikeShape.None, node.Shape);
    }

    /// <summary>
    /// A pattern that is not ASCII but LOWERS to ASCII is still judged after lowering — the
    /// comparison uses the lowered form, so that is the form the guard has to look at.
    /// </summary>
    [Fact]
    public void Ascii_is_judged_on_the_lowered_pattern()
    {
        var dotted = new LikeNode("Region", "%İ%");        // LATIN CAPITAL I WITH DOT ABOVE
        Assert.Equal(LikeShape.Contains, dotted.Shape);
        Assert.Equal(System.Text.Ascii.IsValid(dotted.Affix), dotted.AffixAscii);

        var cyrillic = new LikeNode("Region", "%ключ%");   // ключ
        Assert.Equal(LikeShape.Contains, cyrillic.Shape);
        Assert.False(cyrillic.AffixAscii);                      // stays on the matcher
    }

    // ── the equivalence ───────────────────────────────────────────────────────

    /// <summary>
    /// Every pattern against every value, compared with an independent reference implementation
    /// of LIKE — no shapes, no shortcut, a plain recursive walk under the same
    /// <c>ToLowerInvariant</c> folding. If the shortcut ever answers differently for ASCII,
    /// this is what says so.
    /// </summary>
    [Fact]
    public void Fast_path_agrees_with_a_reference_matcher_over_ascii()
    {
        string[] values =
        [
            "", "a", "A", "ab", "AB", "abc", "aBc", "timeout", "TIMEOUT",
            "hit a timeout after 30s", "Handled; response returned",
            "%2fapi%2fusers", "a%b", "__", "request", "REQUEST", "prerequest",
            "timeouttimeout", "xtimeout", "timeoutx", "tim", "  timeout  ",
        ];
        string[] patterns =
        [
            "", "a", "A", "%a%", "%A%", "a%", "A%", "%a", "%A",
            "%timeout%", "%TIMEOUT%", "timeout", "Handled%", "%request",
            "%%", "%2f%", "%b%", "%x%", "%  %", "%a%b%", "a_c",
        ];

        AssertAgreement(patterns, values);
    }

    /// <summary>
    /// The same sweep over the values and patterns the shortcut must REFUSE. These are the
    /// cases where the two foldings differ, so a shortcut taken here would change answers.
    /// </summary>
    [Fact]
    public void Non_ascii_keeps_the_folding_semantics()
    {
        string[] values =
        [
            "K",                       // KELVIN SIGN
            "k", "K",
            "straße", "STRASSE",
            "\U00010400", "\U00010428",     // DESERET capital / small LONG I
            // MIXED: an ASCII window at one end, non-ASCII at the other. The shortcut is scoped
            // to the window an anchored pattern actually reads, so these are the values that say
            // whether that scoping kept the folding semantics where it does NOT read.
            "abcK", "Kabc", "handled;ключ",
            "ключ",     // ключ
            "КЛЮЧ",     // КЛЮЧ
            "aKb",
        ];
        string[] patterns =
        [
            "K", "k", "K", "%K%", "%k%", "%K%",
            "%\U00010400%", "%\U00010428%",
            "abc%", "%abc", "ABC%", "%ABC", "handled%", "%handled", "abcK", "Kabc",
            "%ключ%", "%КЛЮЧ%", "%стр%",
        ];

        AssertAgreement(patterns, values);
    }

    private static void AssertAgreement(string[] patterns, string[] values)
    {
        foreach (var pattern in patterns)
        {
            foreach (var value in values)
            {
                bool expected = ReferenceLike(value, pattern);
                bool actual   = Matches(WithValue(value), $"Region like '{pattern}'");
                Assert.True(expected == actual,
                    $"pattern '{pattern}' vs value '{value}': reference={expected}, evaluator={actual}");
            }
        }
    }

    // ── reference implementation ──────────────────────────────────────────────

    /// <summary>
    /// LIKE spelled out the slow, obvious way: lower both sides with the SAME
    /// <c>ToLowerInvariant</c> the evaluator lowers its pattern with, then recurse.
    /// Deliberately not a copy of the production matcher — a reference that shares the
    /// implementation cannot catch the implementation being wrong.
    /// </summary>
    private static bool ReferenceLike(string value, string pattern)
        => Walk(value.ToLowerInvariant(), 0, pattern.ToLowerInvariant(), 0);

    private static bool Walk(string v, int vi, string p, int pi)
    {
        if (pi == p.Length) return vi == v.Length;
        if (p[pi] == '%')
        {
            for (int k = vi; k <= v.Length; k++)
                if (Walk(v, k, p, pi + 1)) return true;
            return false;
        }
        if (vi == v.Length) return false;
        if (p[pi] == '_') return Walk(v, vi + 1, p, pi + 1);
        return p[pi] == v[vi] && Walk(v, vi + 1, p, pi + 1);
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    private static LogEvent WithValue(string value)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("Region"); w.Write(value);
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

    private static bool Matches(LogEvent ev, string filter) => CompiledFilter.Compile(filter).Matches(ev);
}
