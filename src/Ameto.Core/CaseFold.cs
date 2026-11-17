using System.Runtime.CompilerServices;
using System.Text;

namespace Ameto.Core;

/// <summary>
/// THE CANONICAL CASE FOLD — one character out for every character in, from a table COMPILED INTO
/// THE BINARY rather than asked of the host. Every index that answers for a case-insensitive
/// comparison keys on it: the span attribute blooms (<c>SpanBloom</c>, issue #86) and the log
/// segment's bloom filter and trigram index (<see cref="SegmentBloomFilter"/>,
/// <c>SegmentTrigramIndex</c>).
///
/// <para>WHAT AN INDEX NEEDS FROM A FOLD. An index keyed on <c>fold(text)</c> may answer "not here"
/// for a literal only if every text the scan would accept for it folds the same. The scan has two
/// equivalences: <see cref="StringComparison.OrdinalIgnoreCase"/> (<c>=</c>, <c>in</c>, free text, the
/// <c>ci</c> functions) — a UTF-16 unit's simple uppercase, a surrogate pair's code point's — and
/// LIKE, which lowercases one character at a time. So the fold must be constant on every class of
/// both. Lowercase is not: it keeps <c>ς</c> apart from <c>σ</c> (both <c>Σ</c> to the comparer),
/// <c>µ</c> from <c>μ</c> (both <c>Μ</c>), and the archaic Cyrillic U+1C80–U+1C88 from
/// <c>в д о с т ъ ѣ ꙋ</c> — 44 BMP pairs the comparer calls equal and a lowercase index called
/// different, so a row the hot tier returned for <c>Place = 'ΟΔΟΣ'</c> was dropped unread once it
/// was flushed. Upper-of-lower is constant on both: equal lowercases have equal upper-of-lowers, and
/// the comparer's pairs are merged by it, which <c>SpanBloomCanonicalTests</c> and
/// <c>CaseFoldTests</c> prove over every code point rather than over a list of letters.</para>
///
/// <para>WHY A TABLE. <c>ToUpperInvariant(ToLowerInvariant(c))</c> asked at run time depends on
/// WHERE it runs: under ICU it is the host's ICU, under invariant globalization (the Docker image —
/// <c>install/docker/Dockerfile</c>) it is .NET's own Unicode data. On the dev box the two disagree
/// on five BMP pairs from Unicode 16 (U+A7CB/U+0264, U+A7CC/U+A7CD, U+A7DA/U+A7DB, U+A7DC/U+019B,
/// U+1C89/U+1C8A): bits the container set for "ɤ" folded to U+A7CB would be probed by a Windows host
/// as "ɤ", and the answer skipped with the value byte-identical on both sides. An ICU update, or a
/// .NET release with newer Unicode data, would do the same to any pair it adds. A table makes what a
/// writer stores a function of the BUILD alone: the same on every host, in both globalization modes,
/// and with no ICU call per character on the flush path.</para>
///
/// <para>WHAT IS IN IT. For every BMP scalar <c>c</c>: <c>ToUpperInvariant(ToLowerInvariant(c))</c>
/// under .NET 10's invariant-globalization casing data (Unicode 16), except U+017F (ſ), which
/// <c>OrdinalIgnoreCase</c> refuses to fold and so folds to itself. Stored as runs
/// <c>(first, last, delta, stride)</c>: every <c>c</c> from <c>first</c> to <c>last</c> in steps of
/// <c>stride</c> folds to <c>c + delta</c>; anything not listed folds to itself. 1 198 scalars in
/// 197 runs. <c>SpanBloomCanonicalTests.The_embedded_fold_is_the_runtimes_invariant_fold</c>
/// regenerates it in memory under invariant globalization (CI runs it so, on Linux) and fails on any
/// difference — which is the day to regenerate this table, and the <see cref="Fingerprint"/> moves
/// with it.</para>
///
/// <para>EVERY SCALAR OUTSIDE THE BMP, AND EVERY LONE SURROGATE, FOLDS TO U+FFFD (<see cref="Scalar"/>,
/// <see cref="FoldUtf16"/>, <see cref="FoldUtf8"/>). <c>OrdinalIgnoreCase</c> folds astral letters from
/// .NET's own Unicode data while ICU answers from the host's: measured on the dev box, the 22 Garay
/// case pairs (U+10D50–U+10D65 against U+10D70–U+10D85) are equal under the comparer and unmapped by
/// ICU. Collapsing them costs selectivity only (an index cannot tell 👍 from 👎 and keeps a group too
/// many), never a row, and needs no table. A lone surrogate — which a filter literal can hold and a
/// value decoded from UTF-8 cannot — folds the same way as the pair it may be half of, so a substring
/// that starts or ends inside a pair still finds its trigrams. Folded text is therefore one BMP unit
/// per scalar and never contains a surrogate: never longer than its UTF-16 source, and never longer
/// than its UTF-8 source counted in bytes.</para>
///
/// <para>A HOST THAT KNOWS MORE THAN THE TABLE. The scan still compares with the QUERY host's
/// <c>OrdinalIgnoreCase</c> and lowercase, so a host with newer casing (ICU with Unicode 17, a later
/// .NET) can call equal two characters the table keeps apart. <see cref="HostDisagrees"/> finds
/// those characters once, from the host's own comparer, and a reader does not trust an index's
/// silence about a literal holding one. On the hosts this build runs on — ICU on the dev box,
/// invariant in the container — the set is empty.</para>
/// </summary>
internal static class CaseFold
{
    /// <summary>
    /// Folded into <see cref="Fingerprint"/>, so a change of rule outside the table (the astral
    /// collapse, say) changes it too. Bump it with any such change.
    ///
    /// <para>ONE THING NO TABLE AND NO RULE MAY CHANGE: ASCII folds to upper case (a-z to A-Z,
    /// nothing else moves). A reader that meets a section folded by another fingerprint trusts it
    /// for ASCII literals by folding them with ITS OWN table — sound only while every table agrees
    /// on ASCII. A rule that folds ASCII any other way needs a new section marker (the log bloom's
    /// <c>LBC1</c>, the log trigram index's <c>LTC1</c>, the span bloom index's <c>RDB3</c>), not just
    /// a new fingerprint, or a build rolled back from it would probe its ASCII wrongly.</para>
    /// </summary>
    private const ulong RuleVersion = 1;

    /// <summary>(first, last, delta, stride) — see the type docstring. Generated; do not hand-edit.</summary>
    private static ReadOnlySpan<int> Runs =>
    [
        0x0061, 0x007A,    -32, 1,
        0x00B5, 0x00B5,    743, 1,
        0x00E0, 0x00F6,    -32, 1,
        0x00F8, 0x00FE,    -32, 1,
        0x00FF, 0x00FF,    121, 1,
        0x0101, 0x012F,     -1, 2,
        0x0133, 0x0137,     -1, 2,
        0x013A, 0x0148,     -1, 2,
        0x014B, 0x0177,     -1, 2,
        0x017A, 0x017E,     -1, 2,
        0x0180, 0x0180,    195, 1,
        0x0183, 0x0185,     -1, 2,
        0x0188, 0x0188,     -1, 1,
        0x018C, 0x018C,     -1, 1,
        0x0192, 0x0192,     -1, 1,
        0x0195, 0x0195,     97, 1,
        0x0199, 0x0199,     -1, 1,
        0x019A, 0x019A,    163, 1,
        0x019B, 0x019B,  42561, 1,
        0x019E, 0x019E,    130, 1,
        0x01A1, 0x01A5,     -1, 2,
        0x01A8, 0x01A8,     -1, 1,
        0x01AD, 0x01AD,     -1, 1,
        0x01B0, 0x01B0,     -1, 1,
        0x01B4, 0x01B6,     -1, 2,
        0x01B9, 0x01B9,     -1, 1,
        0x01BD, 0x01BD,     -1, 1,
        0x01BF, 0x01BF,     56, 1,
        0x01C5, 0x01C5,     -1, 1,
        0x01C6, 0x01C6,     -2, 1,
        0x01C8, 0x01C8,     -1, 1,
        0x01C9, 0x01C9,     -2, 1,
        0x01CB, 0x01CB,     -1, 1,
        0x01CC, 0x01CC,     -2, 1,
        0x01CE, 0x01DC,     -1, 2,
        0x01DD, 0x01DD,    -79, 1,
        0x01DF, 0x01EF,     -1, 2,
        0x01F2, 0x01F2,     -1, 1,
        0x01F3, 0x01F3,     -2, 1,
        0x01F5, 0x01F5,     -1, 1,
        0x01F9, 0x021F,     -1, 2,
        0x0223, 0x0233,     -1, 2,
        0x023C, 0x023C,     -1, 1,
        0x023F, 0x0240,  10815, 1,
        0x0242, 0x0242,     -1, 1,
        0x0247, 0x024F,     -1, 2,
        0x0250, 0x0250,  10783, 1,
        0x0251, 0x0251,  10780, 1,
        0x0252, 0x0252,  10782, 1,
        0x0253, 0x0253,   -210, 1,
        0x0254, 0x0254,   -206, 1,
        0x0256, 0x0257,   -205, 1,
        0x0259, 0x0259,   -202, 1,
        0x025B, 0x025B,   -203, 1,
        0x025C, 0x025C,  42319, 1,
        0x0260, 0x0260,   -205, 1,
        0x0261, 0x0261,  42315, 1,
        0x0263, 0x0263,   -207, 1,
        0x0264, 0x0264,  42343, 1,
        0x0265, 0x0265,  42280, 1,
        0x0266, 0x0266,  42308, 1,
        0x0268, 0x0268,   -209, 1,
        0x0269, 0x0269,   -211, 1,
        0x026A, 0x026A,  42308, 1,
        0x026B, 0x026B,  10743, 1,
        0x026C, 0x026C,  42305, 1,
        0x026F, 0x026F,   -211, 1,
        0x0271, 0x0271,  10749, 1,
        0x0272, 0x0272,   -213, 1,
        0x0275, 0x0275,   -214, 1,
        0x027D, 0x027D,  10727, 1,
        0x0280, 0x0280,   -218, 1,
        0x0282, 0x0282,  42307, 1,
        0x0283, 0x0283,   -218, 1,
        0x0287, 0x0287,  42282, 1,
        0x0288, 0x0288,   -218, 1,
        0x0289, 0x0289,    -69, 1,
        0x028A, 0x028B,   -217, 1,
        0x028C, 0x028C,    -71, 1,
        0x0292, 0x0292,   -219, 1,
        0x029D, 0x029D,  42261, 1,
        0x029E, 0x029E,  42258, 1,
        0x0345, 0x0345,     84, 1,
        0x0371, 0x0373,     -1, 2,
        0x0377, 0x0377,     -1, 1,
        0x037B, 0x037D,    130, 1,
        0x03AC, 0x03AC,    -38, 1,
        0x03AD, 0x03AF,    -37, 1,
        0x03B1, 0x03C1,    -32, 1,
        0x03C2, 0x03C2,    -31, 1,
        0x03C3, 0x03CB,    -32, 1,
        0x03CC, 0x03CC,    -64, 1,
        0x03CD, 0x03CE,    -63, 1,
        0x03D0, 0x03D0,    -62, 1,
        0x03D1, 0x03D1,    -57, 1,
        0x03D5, 0x03D5,    -47, 1,
        0x03D6, 0x03D6,    -54, 1,
        0x03D7, 0x03D7,     -8, 1,
        0x03D9, 0x03EF,     -1, 2,
        0x03F0, 0x03F0,    -86, 1,
        0x03F1, 0x03F1,    -80, 1,
        0x03F2, 0x03F2,      7, 1,
        0x03F3, 0x03F3,   -116, 1,
        0x03F4, 0x03F4,    -92, 1,
        0x03F5, 0x03F5,    -96, 1,
        0x03F8, 0x03F8,     -1, 1,
        0x03FB, 0x03FB,     -1, 1,
        0x0430, 0x044F,    -32, 1,
        0x0450, 0x045F,    -80, 1,
        0x0461, 0x0481,     -1, 2,
        0x048B, 0x04BF,     -1, 2,
        0x04C2, 0x04CE,     -1, 2,
        0x04CF, 0x04CF,    -15, 1,
        0x04D1, 0x052F,     -1, 2,
        0x0561, 0x0586,    -48, 1,
        0x10D0, 0x10FA,   3008, 1,
        0x10FD, 0x10FF,   3008, 1,
        0x13F8, 0x13FD,     -8, 1,
        0x1C80, 0x1C80,  -6254, 1,
        0x1C81, 0x1C81,  -6253, 1,
        0x1C82, 0x1C82,  -6244, 1,
        0x1C83, 0x1C84,  -6242, 1,
        0x1C85, 0x1C85,  -6243, 1,
        0x1C86, 0x1C86,  -6236, 1,
        0x1C87, 0x1C87,  -6181, 1,
        0x1C88, 0x1C88,  35266, 1,
        0x1C8A, 0x1C8A,     -1, 1,
        0x1D79, 0x1D79,  35332, 1,
        0x1D7D, 0x1D7D,   3814, 1,
        0x1D8E, 0x1D8E,  35384, 1,
        0x1E01, 0x1E95,     -1, 2,
        0x1E9B, 0x1E9B,    -59, 1,
        0x1E9E, 0x1E9E,  -7615, 1,
        0x1EA1, 0x1EFF,     -1, 2,
        0x1F00, 0x1F07,      8, 1,
        0x1F10, 0x1F15,      8, 1,
        0x1F20, 0x1F27,      8, 1,
        0x1F30, 0x1F37,      8, 1,
        0x1F40, 0x1F45,      8, 1,
        0x1F51, 0x1F57,      8, 2,
        0x1F60, 0x1F67,      8, 1,
        0x1F70, 0x1F71,     74, 1,
        0x1F72, 0x1F75,     86, 1,
        0x1F76, 0x1F77,    100, 1,
        0x1F78, 0x1F79,    128, 1,
        0x1F7A, 0x1F7B,    112, 1,
        0x1F7C, 0x1F7D,    126, 1,
        0x1F80, 0x1F87,      8, 1,
        0x1F90, 0x1F97,      8, 1,
        0x1FA0, 0x1FA7,      8, 1,
        0x1FB0, 0x1FB1,      8, 1,
        0x1FB3, 0x1FB3,      9, 1,
        0x1FBE, 0x1FBE,  -7205, 1,
        0x1FC3, 0x1FC3,      9, 1,
        0x1FD0, 0x1FD1,      8, 1,
        0x1FE0, 0x1FE1,      8, 1,
        0x1FE5, 0x1FE5,      7, 1,
        0x1FF3, 0x1FF3,      9, 1,
        0x2126, 0x2126,  -7549, 1,
        0x212A, 0x212A,  -8415, 1,
        0x212B, 0x212B,  -8294, 1,
        0x214E, 0x214E,    -28, 1,
        0x2170, 0x217F,    -16, 1,
        0x2184, 0x2184,     -1, 1,
        0x24D0, 0x24E9,    -26, 1,
        0x2C30, 0x2C5F,    -48, 1,
        0x2C61, 0x2C61,     -1, 1,
        0x2C65, 0x2C65, -10795, 1,
        0x2C66, 0x2C66, -10792, 1,
        0x2C68, 0x2C6C,     -1, 2,
        0x2C73, 0x2C73,     -1, 1,
        0x2C76, 0x2C76,     -1, 1,
        0x2C81, 0x2CE3,     -1, 2,
        0x2CEC, 0x2CEE,     -1, 2,
        0x2CF3, 0x2CF3,     -1, 1,
        0x2D00, 0x2D25,  -7264, 1,
        0x2D27, 0x2D27,  -7264, 1,
        0x2D2D, 0x2D2D,  -7264, 1,
        0xA641, 0xA66D,     -1, 2,
        0xA681, 0xA69B,     -1, 2,
        0xA723, 0xA72F,     -1, 2,
        0xA733, 0xA76F,     -1, 2,
        0xA77A, 0xA77C,     -1, 2,
        0xA77F, 0xA787,     -1, 2,
        0xA78C, 0xA78C,     -1, 1,
        0xA791, 0xA793,     -1, 2,
        0xA794, 0xA794,     48, 1,
        0xA797, 0xA7A9,     -1, 2,
        0xA7B5, 0xA7C3,     -1, 2,
        0xA7C8, 0xA7CA,     -1, 2,
        0xA7CD, 0xA7CD,     -1, 1,
        0xA7D1, 0xA7D1,     -1, 1,
        0xA7D7, 0xA7DB,     -1, 2,
        0xA7F6, 0xA7F6,     -1, 1,
        0xAB53, 0xAB53,   -928, 1,
        0xAB70, 0xABBF, -38864, 1,
        0xFF41, 0xFF5A,    -32, 1,
    ];

    private static char[] s_table       = Expand(Runs);
    private static ulong  s_fingerprint = FingerprintOf(s_table);

    /// <summary>What every folded astral scalar and lone surrogate becomes.</summary>
    internal const char Collapsed = '�';

    // ── The fold ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The fold of one BMP UTF-16 unit. Every surrogate maps to itself in the table — a caller
    /// holding text, which may hold pairs, folds it through <see cref="FoldUtf16"/> instead.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static char Bmp(char c) => s_table[c];

    /// <summary>The fold of one scalar: a BMP scalar through the table, any other <see cref="Collapsed"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static char Scalar(Rune r) => r.IsBmp ? s_table[r.Value] : Collapsed;

    /// <summary>
    /// Folds UTF-16 text into <paramref name="destination"/>, ONE unit per scalar: a BMP unit through
    /// the table, a surrogate pair — or a surrogate that is not half of one — to
    /// <see cref="Collapsed"/>. Writes at most <paramref name="source"/>.Length units and returns
    /// how many. The ASCII prefix — usually the whole text — goes through the vectorised
    /// <see cref="Ascii.ToUpper(ReadOnlySpan{char}, Span{char}, out int)"/>, which is this table
    /// over ASCII.
    /// </summary>
    internal static int FoldUtf16(ReadOnlySpan<char> source, Span<char> destination)
    {
        Ascii.ToUpper(source, destination[..source.Length], out int written);
        int i = written;
        char[] table = s_table;
        while (i < source.Length)
        {
            char c = source[i];
            if (!char.IsSurrogate(c))
            {
                destination[written++] = table[c];
                i++;
                continue;
            }
            i += char.IsHighSurrogate(c) && i + 1 < source.Length && char.IsLowSurrogate(source[i + 1]) ? 2 : 1;
            destination[written++] = Collapsed;
        }
        return written;
    }

    /// <summary>
    /// Folds UTF-8 text into UTF-16 units in <paramref name="destination"/>, one per scalar, an
    /// ill-formed sequence one <see cref="Collapsed"/> per maximal invalid subsequence — the rule of
    /// <see cref="Rune.DecodeFromUtf8"/>, which is also the one <c>Encoding.UTF8</c> applies when the
    /// scan decodes the same bytes, so what is folded here is what the scan compares. Writes at most
    /// <paramref name="source"/>.Length units and returns how many.
    /// </summary>
    internal static int FoldUtf8(ReadOnlySpan<byte> source, Span<char> destination)
    {
        Ascii.ToUpper(source, destination[..source.Length], out int written);
        int i = written;
        char[] table = s_table;
        while (i < source.Length)
        {
            byte b = source[i];
            if (b < 0x80)
            {
                destination[written++] = table[b];
                i++;
                continue;
            }
            Rune.DecodeFromUtf8(source[i..], out Rune r, out int used);
            destination[written++] = r.IsBmp ? table[r.Value] : Collapsed;
            i += used;
        }
        return written;
    }

    /// <summary>
    /// 64-bit FNV-1a over <see cref="RuleVersion"/> and all 65 536 entries, written into every section
    /// folded by this table — a span segment's canonical bloom index, a log index group's bloom and
    /// trigram sections. A reader whose fingerprint differs was built with another fold, and trusts
    /// such a section for all-ASCII literals only: ASCII folds the same under every table there has
    /// been.
    /// </summary>
    internal static ulong Fingerprint => s_fingerprint;

    // ── The host against the table ────────────────────────────────────────────

    /// <summary>
    /// True for a character this host's <c>OrdinalIgnoreCase</c> or <c>ToLowerInvariant</c> treats
    /// differently from the table; an index's silence about a literal holding one is no proof.
    /// Computed once, on first use, from the host's own comparer: every BMP scalar bucketed by
    /// <c>string.GetHashCode(s, OrdinalIgnoreCase)</c> (equal under the comparer ⇒ equal hash), every
    /// comparer-equal pair the table folds apart marked together with its lowercases, every scalar
    /// whose host lowercase the table folds elsewhere marked with that lowercase — and then every
    /// scalar whose host lowercase is marked. That last pass is for LIKE as the log index meets it:
    /// a pattern run arrives as TYPED, and a typed <c>Ω</c> lowercases to the same <c>ω</c> as a
    /// stored character the table may not fold with either (the Ohm sign, on a table that predates
    /// the mapping), so the typed character is marked through its lowercase.
    /// </summary>
    internal static bool HostDisagrees(char c) => (Drift.Bits[c >> 6] & (1UL << (c & 63))) != 0;

    /// <summary>True when <see cref="HostDisagrees"/> any character of <paramref name="text"/>.</summary>
    internal static bool HostDisagreesWithAny(ReadOnlySpan<char> text)
    {
        if (Drift.Count == 0) return false;
        foreach (char c in text)
            if (HostDisagrees(c)) return true;
        return false;
    }

    /// <summary>How many characters <see cref="HostDisagrees"/> — printed by the tests.</summary>
    internal static int HostDisagreementCount => Drift.Count;

    /// <summary>
    /// True when this host treats some ASCII character differently from the table — a comparer that
    /// equated <c>ſ</c> with <c>s</c>, or <c>ı</c> with <c>I</c>, would mark <c>s</c> or <c>i</c>. The
    /// readers' shortcut "ASCII folds the same everywhere" is then false, so EVERY literal is
    /// answered without the index's silence (review F-A of #86). No host this build runs on does it;
    /// the flag exists so that a host that ever does is answered completely rather than quickly.
    /// </summary>
    internal static bool HostDisagreesOnAscii => Drift.Ascii;

    /// <summary>
    /// TEST SEAM: behave as a host that treats exactly <paramref name="marked"/> differently from the
    /// table, until disposed.
    /// </summary>
    internal static DriftSwap UseHostDriftForTest(string marked)
    {
        var bits = new ulong[65_536 / 64];
        foreach (char c in marked) Mark(bits, c);
        var saved = Volatile.Read(ref s_drift);
        Volatile.Write(ref s_drift, NewDriftSet(bits));
        return new DriftSwap(saved);
    }

    /// <summary>
    /// TEST SEAM: the host-drift set this host would have against <paramref name="table"/> as the
    /// compiled one, until disposed. With <see cref="UseTableForTest"/> over the same table it is a
    /// build whose table knows less casing than this host — the case <see cref="HostDisagrees"/>
    /// exists for, which no host this build runs on presents.
    /// </summary>
    internal static DriftSwap UseHostDriftAgainstTableForTest(char[] table)
    {
        ArgumentOutOfRangeException.ThrowIfNotEqual(table.Length, 65_536);
        var saved = Volatile.Read(ref s_drift);
        Volatile.Write(ref s_drift, ComputeDrift(table));
        return new DriftSwap(saved);
    }

    internal readonly struct DriftSwap(object? saved) : IDisposable
    {
        public void Dispose() => Volatile.Write(ref s_drift, (DriftSet?)saved);
    }

    /// <summary>
    /// TEST SEAM: fold with <paramref name="table"/> (65 536 entries) until disposed — the shape of a
    /// section written by a build whose fold differs. The test assemblies here run serially.
    /// </summary>
    internal static TableSwap UseTableForTest(char[] table)
    {
        ArgumentOutOfRangeException.ThrowIfNotEqual(table.Length, 65_536);
        var swap = new TableSwap(s_table, s_fingerprint);
        s_table       = table;
        s_fingerprint = FingerprintOf(table);
        return swap;
    }

    /// <summary>A copy of the compiled table, for building a test's variant of it.</summary>
    internal static char[] CopyOfTable() => (char[])s_table.Clone();

    internal readonly struct TableSwap(char[] table, ulong fingerprint) : IDisposable
    {
        public void Dispose()
        {
            s_table       = table;
            s_fingerprint = fingerprint;
        }
    }

    private static char[] Expand(ReadOnlySpan<int> runs)
    {
        var t = new char[65_536];
        for (int c = 0; c < t.Length; c++) t[c] = (char)c;
        for (int i = 0; i + 3 < runs.Length; i += 4)
            for (int c = runs[i]; c <= runs[i + 1]; c += runs[i + 3])
                t[c] = (char)(c + runs[i + 2]);
        return t;
    }

    private static ulong FingerprintOf(char[] table)
    {
        const ulong Prime = 1099511628211UL;
        ulong h = 14695981039346656037UL;
        h = (h ^ RuleVersion) * Prime;
        foreach (char c in table)
        {
            h = (h ^ (byte)c)        * Prime;
            h = (h ^ (byte)(c >> 8)) * Prime;
        }
        return h;
    }

    /// <summary>
    /// The host-drift set: which characters this host treats differently from the COMPILED table.
    /// Built on first use and kept for the process. It is a property of the host and the build, so it
    /// is computed from <see cref="Runs"/> itself and never from <c>s_table</c>, which
    /// <see cref="UseTableForTest"/> swaps: a set first built under a swap would describe the
    /// substitute, and every later probe in the process would be judged against the wrong table
    /// (review F-B of #86).
    /// </summary>
    private sealed class DriftSet(ulong[] bits, int count, bool ascii)
    {
        public readonly ulong[] Bits  = bits;
        public readonly int     Count = count;
        public readonly bool    Ascii = ascii;
    }

    private static DriftSet? s_drift;

    private static DriftSet Drift => Volatile.Read(ref s_drift) ?? BuildDrift();

    private static DriftSet BuildDrift()
    {
        var built = ComputeDrift(Expand(Runs));
        return Interlocked.CompareExchange(ref s_drift, built, null) ?? built;
    }

    /// <summary>TEST SEAM: forget the host-drift set, so the next use computes it again.</summary>
    internal static void ForgetHostDriftForTest() => Volatile.Write(ref s_drift, null);

    private static DriftSet ComputeDrift(char[] table)
    {
        var bits = new ulong[65_536 / 64];

        // Scalars whose host lowercase the table folds elsewhere: LIKE lowercases, and a reader may
        // be handed a literal already lowercased (the span hints are).
        for (int c = 0; c < 65_536; c++)
        {
            if (c is >= 0xD800 and <= 0xDFFF) continue;
            char l = char.ToLowerInvariant((char)c);
            if (table[l] != table[c]) { Mark(bits, (char)c); Mark(bits, l); }
        }

        // Pairs the host's comparer calls equal and the table keeps apart.
        var keyed = new List<ulong>(65_536);
        Span<char> one = stackalloc char[1];
        for (int c = 0; c < 65_536; c++)
        {
            if (c is >= 0xD800 and <= 0xDFFF) continue;
            one[0] = (char)c;
            uint h = (uint)string.GetHashCode(one, StringComparison.OrdinalIgnoreCase);
            keyed.Add(((ulong)h << 32) | (uint)c);
        }
        keyed.Sort();
        for (int i = 0; i < keyed.Count; )
        {
            int j = i + 1;
            while (j < keyed.Count && keyed[j] >> 32 == keyed[i] >> 32) j++;
            for (int a = i; a < j; a++)
            for (int b = a + 1; b < j; b++)
            {
                char ca = (char)keyed[a], cb = (char)keyed[b];
                if (table[ca] == table[cb]) continue;
                if (!string.Equals(ca.ToString(), cb.ToString(), StringComparison.OrdinalIgnoreCase)) continue;
                Mark(bits, ca); Mark(bits, cb);
                Mark(bits, char.ToLowerInvariant(ca)); Mark(bits, char.ToLowerInvariant(cb));
            }
            i = j;
        }

        // Scalars whose host lowercase is marked. LIKE equates a typed character with a stored one
        // whenever their host lowercases agree; when the table folds them apart, the first pass
        // marked one of the two and their common lowercase, so the other is caught here, through
        // that lowercase. Until nothing more is marked — lowercase is idempotent, so in practice once.
        for (bool more = true; more; )
        {
            more = false;
            for (int c = 0; c < 65_536; c++)
            {
                if (c is >= 0xD800 and <= 0xDFFF || IsMarked(bits, (char)c)) continue;
                char l = char.ToLowerInvariant((char)c);
                if (l != c && IsMarked(bits, l)) { Mark(bits, (char)c); more = true; }
            }
        }

        return NewDriftSet(bits);
    }

    private static bool IsMarked(ulong[] bits, char c) => (bits[c >> 6] & (1UL << (c & 63))) != 0;

    /// <summary>Wraps a marked set with its count and whether it touches ASCII (U+0000–U+007F, the first two words).</summary>
    private static DriftSet NewDriftSet(ulong[] bits)
    {
        int count = 0;
        foreach (ulong w in bits) count += System.Numerics.BitOperations.PopCount(w);
        return new DriftSet(bits, count, ascii: (bits[0] | bits[1]) != 0);
    }

    private static void Mark(ulong[] bits, char c) => bits[c >> 6] |= 1UL << (c & 63);
}
