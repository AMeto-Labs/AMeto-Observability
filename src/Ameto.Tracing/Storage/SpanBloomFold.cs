namespace Ameto.Tracing.Storage;

/// <summary>
/// THE SPAN BLOOM'S CASE FOLD, AS DATA COMPILED INTO THE BINARY — not asked of the host.
///
/// <para>WHY A TABLE. The fold used to be <c>Rune.ToUpperInvariant(Rune.ToLowerInvariant(r))</c>,
/// and that answer depends on WHERE it runs: under ICU it is the host's ICU, under invariant
/// globalization (the Docker image — <c>install/docker/Dockerfile</c>) it is .NET's own Unicode
/// data. On the dev box the two disagree on five BMP pairs from Unicode 16 (U+A7CB/U+0264,
/// U+A7CC/U+A7CD, U+A7DA/U+A7DB, U+A7DC/U+019B, U+1C89/U+1C8A): a segment the container flushed
/// with "ɤ" folded to U+A7CB would be probed by a Windows host as "ɤ", and the block holding the
/// answer skipped — the value byte-identical on both sides. An ICU update, or a .NET release with
/// newer Unicode data, would do the same to any pair it adds. A table in the binary makes the bits
/// the writer sets a function of the BUILD alone: the same on every host, in both globalization
/// modes, and with no ICU call per character on the flush path.</para>
///
/// <para>WHAT IS IN IT. For every BMP scalar <c>c</c>: <c>ToUpperInvariant(ToLowerInvariant(c))</c>
/// under .NET 10's invariant-globalization casing data (Unicode 16), except U+017F (ſ), which
/// <c>OrdinalIgnoreCase</c> refuses to fold and so folds to itself. Stored as runs
/// <c>(first, last, delta, stride)</c>: every <c>c</c> from <c>first</c> to <c>last</c> in steps of
/// <c>stride</c> folds to <c>c + delta</c>; anything not listed folds to itself. 1 198 scalars in
/// 197 runs. Astral scalars are not in it — they all fold to U+FFFD (see <c>SpanBloom.Fold</c>).
/// <c>SpanBloomCanonicalTests.The_embedded_fold_is_the_runtimes_invariant_fold</c> regenerates it in
/// memory under invariant globalization (CI runs it so, on Linux) and fails on any difference —
/// which is the day to regenerate this table, and the <see cref="Fingerprint"/> moves with it.</para>
///
/// <para>A HOST THAT KNOWS MORE THAN THE TABLE. The evaluator still compares with the QUERY host's
/// <c>OrdinalIgnoreCase</c>, so a host with newer casing (ICU with Unicode 17, a later .NET) can call
/// equal two characters the table keeps apart. <see cref="HostDisagrees"/> finds those characters
/// once, from the host's own comparer, and the reader probes only the key for a literal holding one.
/// On the hosts this build runs on — ICU on the dev box, invariant in the container — the set is
/// empty.</para>
/// </summary>
internal static class SpanBloomFold
{
    /// <summary>Folded into <see cref="Fingerprint"/>, so a change of rule outside the table (the
    /// astral collapse, say) changes it too. Bump it with any such change.</summary>
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

    /// <summary>
    /// The fold of one BMP scalar. A lone surrogate never reaches here — the decoders hand those on
    /// as U+FFFD — and the table maps every surrogate to itself anyway.
    /// </summary>
    internal static char Bmp(char c) => s_table[c];

    /// <summary>
    /// 64-bit FNV-1a over <see cref="RuleVersion"/> and all 65 536 entries, written into every
    /// segment next to <c>SpanBloom.CanonicalMarker</c>. A reader whose fingerprint differs was built
    /// with another fold, and trusts a value probe only for an all-ASCII literal — ASCII folds the
    /// same under every table there has been.
    /// </summary>
    internal static ulong Fingerprint => s_fingerprint;

    /// <summary>
    /// True for a character this host's <c>OrdinalIgnoreCase</c> or <c>ToLowerInvariant</c> treats
    /// differently from the table; a literal holding one cannot be probed by value. Computed once, on
    /// first use, from the host's own comparer: every BMP scalar bucketed by
    /// <c>string.GetHashCode(s, OrdinalIgnoreCase)</c> (equal under the comparer ⇒ equal hash), every
    /// comparer-equal pair the table folds apart marked together with its lowercases, and every
    /// scalar whose host lowercase the table folds elsewhere marked with that lowercase.
    /// </summary>
    internal static bool HostDisagrees(char c) => (Drift.Bits[c >> 6] & (1UL << (c & 63))) != 0;

    /// <summary>How many characters <see cref="HostDisagrees"/> — printed by the tests.</summary>
    internal static int HostDisagreementCount => Drift.Count;

    /// <summary>
    /// TEST SEAM: fold with <paramref name="table"/> (65 536 entries) until disposed — the shape of a
    /// segment written by a build whose fold differs. The test assemblies here run serially.
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
    private sealed class DriftSet(ulong[] bits, int count)
    {
        public readonly ulong[] Bits  = bits;
        public readonly int     Count = count;
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

        // Scalars whose host lowercase the table folds elsewhere: the reader only ever sees the
        // host-lowercased literal (AttrHint.LowerValue).
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

        int count = 0;
        foreach (ulong w in bits) count += System.Numerics.BitOperations.PopCount(w);
        return new DriftSet(bits, count);
    }

    private static void Mark(ulong[] bits, char c) => bits[c >> 6] |= 1UL << (c & 63);
}
