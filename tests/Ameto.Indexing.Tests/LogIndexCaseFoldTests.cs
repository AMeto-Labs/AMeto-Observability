using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Indexing;
using Xunit.Abstractions;

namespace Ameto.Indexing.Tests;

/// <summary>
/// THE LOG INDEX FOLDS THE WAY THE SCAN COMPARES — and where a section on disk was folded some
/// other way, it does not pretend otherwise.
///
/// <para>The scan has two equivalences: <see cref="StringComparison.OrdinalIgnoreCase"/> for
/// <c>=</c>, <c>in</c>, free text and the <c>ci</c> functions, and lowercase char by char for LIKE.
/// The segment's bloom filter and trigram index may only answer "not here" where every text either
/// one would accept was stored under the key asked. What is proved here, over every code point
/// rather than a list of letters:</para>
/// <list type="bullet">
///   <item>the canonical fold (<see cref="CaseFold"/>) gives every pair either relation calls equal
///     ONE key, so a section this build writes is exact for every literal;</item>
///   <item>a section an older build wrote — lowercased by its host — is never trusted for a literal
///     whose lowercase could differ from what it stored, and still prunes ASCII ones;</item>
///   <item>a section folded by another build's table, and a host that compares a character
///     differently from the table, cost pruning and never rows;</item>
///   <item>an older build reading a section this build wrote finds a bloom that never says no and a
///     trigram index that holds nothing — slower, and complete;</item>
///   <item>the fold itself: one unit per scalar, the same from UTF-16 and from UTF-8, ASCII upper.</item>
/// </list>
/// </summary>
public sealed class LogIndexCaseFoldTests
{
    private readonly ITestOutputHelper _out;

    public LogIndexCaseFoldTests(ITestOutputHelper output) => _out = output;

    // ── The fold ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Moving the table from the span code into <see cref="CaseFold"/> must not move the fingerprint
    /// every span segment written since #86 carries — a different one would make this build read
    /// all of them as folded by another table. This is the value <c>SpanBloomFold.Fingerprint</c>
    /// had before the move; it changes only with the table, deliberately.
    /// </summary>
    [Fact]
    public void The_fingerprint_is_the_one_span_segments_already_carry() =>
        Assert.Equal(0xE8EC90FE163C9514UL, CaseFold.Fingerprint);

    [Fact]
    public void Over_ASCII_the_fold_is_ASCII_upper_case()
    {
        for (int c = 0; c < 128; c++)
            Assert.Equal(char.ToUpperInvariant((char)c), CaseFold.Bmp((char)c));
    }

    /// <summary>
    /// <see cref="CaseFold.FoldUtf16"/> is the table applied one scalar at a time — a pair, and a
    /// surrogate that is half of none, each one <see cref="CaseFold.Collapsed"/> — whatever mix of
    /// ASCII (the vectorised prefix), BMP, pairs and lone halves it is handed.
    /// </summary>
    [Fact]
    public void FoldUtf16_is_the_table_one_scalar_at_a_time()
    {
        var rng = new Random(160);
        string[] pieces = ["a", "Z", "0", " ", "ς", "Σ", "µ", "μ", "ᲀ", "K", "ẞ", "ſ", "ı", "İ", "ж", "Ω",
                           "\U0001F525", "\U00010D50", "\U00010428", "\uD83D", "\uDE00", "�", "ǅ"];
        for (int round = 0; round < 5_000; round++)
        {
            var sb = new StringBuilder();
            int n = rng.Next(0, 24);
            for (int i = 0; i < n; i++) sb.Append(pieces[rng.Next(pieces.Length)]);
            string text = sb.ToString();

            var expected = new StringBuilder();
            for (int i = 0; i < text.Length; )
            {
                Rune.DecodeFromUtf16(text.AsSpan(i), out Rune r, out int used);
                expected.Append(CaseFold.Scalar(r));
                i += used;
            }

            var dest = new char[text.Length];
            int written = CaseFold.FoldUtf16(text, dest);
            Assert.Equal(expected.ToString(), new string(dest, 0, written));
        }
    }

    /// <summary>
    /// Folding the UTF-8 the builder reads gives what folding the text the SCAN decodes from those
    /// bytes gives — ill-formed sequences included, one U+FFFD per maximal invalid subsequence on
    /// both sides.
    /// </summary>
    [Fact]
    public void FoldUtf8_folds_the_text_the_scan_decodes()
    {
        var rng = new Random(161);
        byte[][] pieces = [.. new[] { "a", "Z", "ς", "µ", "ᲀ", "\U0001F525", "\U00010D50", "ж" }.Select(Encoding.UTF8.GetBytes),
                          [0xFF], [0xC0], [0xE2, 0x82], [0xED, 0xA0, 0x80], [0xF4, 0x90, 0x80, 0x80], [0x80]];
        for (int round = 0; round < 5_000; round++)
        {
            var bytes = new List<byte>();
            int n = rng.Next(0, 20);
            for (int i = 0; i < n; i++) bytes.AddRange(pieces[rng.Next(pieces.Length)]);
            byte[] utf8 = [.. bytes];

            string decoded = Encoding.UTF8.GetString(utf8);
            var viaText = new char[decoded.Length];
            int expected = CaseFold.FoldUtf16(decoded, viaText);

            var viaBytes = new char[utf8.Length];
            int written = CaseFold.FoldUtf8(utf8, viaBytes);
            Assert.Equal(new string(viaText, 0, expected), new string(viaBytes, 0, written));
        }
    }

    // ── The scan's equivalences, over every code point ────────────────────────

    /// <summary>
    /// Every pair of scalars the SCAN can call equal, found through the comparers themselves: every
    /// scalar is bucketed by <c>string.GetHashCode(s, OrdinalIgnoreCase)</c> (equal under a comparer
    /// ⇒ equal hash) and every pair in a bucket the comparer calls equal is kept; and every scalar is
    /// paired with any other that lowercases to the same thing, which is LIKE's equivalence.
    /// </summary>
    private static List<(string A, string B, bool Ordinal)> ScanPairs() => s_scanPairs.Value;

    /// <summary>Computed once: it walks every code point twice, and five tests ask.</summary>
    private static readonly Lazy<List<(string A, string B, bool Ordinal)>> s_scanPairs = new(ComputeScanPairs);

    private static List<(string A, string B, bool Ordinal)> ComputeScanPairs()
    {
        var pairs = new List<(string, string, bool)>();
        Span<char> u16 = stackalloc char[2];

        var keyed = new List<ulong>(1_112_064);
        for (int cp = 0; cp <= 0x10FFFF; cp++)
        {
            if (cp is >= 0xD800 and <= 0xDFFF) continue;
            int n = new Rune(cp).EncodeToUtf16(u16);
            keyed.Add(((ulong)(uint)string.GetHashCode(u16[..n], StringComparison.OrdinalIgnoreCase) << 32) | (uint)cp);
        }
        keyed.Sort();
        for (int i = 0; i < keyed.Count; )
        {
            int j = i + 1;
            while (j < keyed.Count && keyed[j] >> 32 == keyed[i] >> 32) j++;
            for (int a = i; a < j; a++)
            for (int b = a + 1; b < j; b++)
            {
                string sa = char.ConvertFromUtf32((int)(uint)keyed[a]), sb = char.ConvertFromUtf32((int)(uint)keyed[b]);
                if (string.Equals(sa, sb, StringComparison.OrdinalIgnoreCase)) pairs.Add((sa, sb, true));
            }
            i = j;
        }

        var byLower = new Dictionary<int, List<int>>();
        for (int cp = 0; cp <= 0x10FFFF; cp++)
        {
            if (cp is >= 0xD800 and <= 0xDFFF) continue;
            int lower = Rune.ToLowerInvariant(new Rune(cp)).Value;
            if (lower == cp && !byLower.ContainsKey(cp)) continue;
            if (!byLower.TryGetValue(lower, out var list)) byLower[lower] = list = [lower];
            if (lower != cp) list.Add(cp);
        }
        foreach (var members in byLower.Values)
            for (int a = 0; a < members.Count; a++)
            for (int b = a + 1; b < members.Count; b++)
                pairs.Add((char.ConvertFromUtf32(members[a]), char.ConvertFromUtf32(members[b]), false));
        return pairs;
    }

    private static string Fold(string s)
    {
        var d = new char[s.Length];
        return new string(d, 0, CaseFold.FoldUtf16(s, d));
    }

    /// <summary>
    /// THE CORE CLAIM. Every pair the scan calls equal folds to one key, both ways round. Make the
    /// fold a lowercase again and this fails on ς/σ first.
    ///
    /// <para>No pair is excused for the host comparing a character differently from the table
    /// (<see cref="CaseFold.HostDisagrees"/>), although the readers would then refuse to trust it
    /// and lose no row: that set is computed AGAINST the table, so a wrong table marks its own
    /// mistakes and an excuse here would hide every one of them. A host newer than the table fails
    /// here by name — the day to regenerate it, exactly as
    /// <c>SpanBloomCanonicalTests.The_embedded_fold_is_the_runtimes_invariant_fold</c> says.</para>
    /// </summary>
    [Fact]
    public void Every_pair_the_scan_calls_equal_folds_to_one_key()
    {
        var pairs = ScanPairs();
        int ordinal = 0, like = 0;
        var failures = new List<string>();
        foreach (var (a, b, isOrdinal) in pairs)
        {
            if (isOrdinal) ordinal++; else like++;
            if (Fold(a) == Fold(b)) continue;
            failures.Add($"{(isOrdinal ? "OrdinalIgnoreCase" : "lowercase")}: U+{char.ConvertToUtf32(a, 0):X4} / U+{char.ConvertToUtf32(b, 0):X4}"
                       + (Disagrees(a) || Disagrees(b) ? " — this host compares it differently from the table" : ""));
        }

        _out.WriteLine($"{ordinal:N0} OrdinalIgnoreCase pairs, {like:N0} lowercase pairs, "
                     + $"{CaseFold.HostDisagreementCount} characters this host folds differently from the table");
        foreach (var f in failures.Take(40)) _out.WriteLine(f);
        Assert.True(ordinal > 1_200, $"only {ordinal} OrdinalIgnoreCase pairs — the bucketing stopped seeing them");
        Assert.True(like > 1_200, $"only {like} lowercase pairs");
        Assert.Empty(failures);

        static bool Disagrees(string s) => s.Length == 1 && CaseFold.HostDisagrees(s[0]);
    }

    // ── The bloom ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A canonical bloom holding one side of every pair the comparer calls equal answers "might"
    /// for the other — the equality gate's half of the core claim, through the real filter.
    /// </summary>
    [Fact]
    public void A_canonical_bloom_answers_for_every_spelling_the_comparer_accepts()
    {
        var pairs = ScanPairs().Where(static p => p.Ordinal).ToList();
        using var written = SegmentBloomFilter.Create(4 * pairs.Count);
        foreach (var (a, _, _) in pairs) written.Add("v" + a);
        using var read = SegmentBloomFilter.Deserialise(written.Serialise());

        Assert.Equal(SegmentBloomFilter.TextFold.Canonical, read.Fold);
        Assert.Equal(CaseFold.Fingerprint, read.FoldFingerprint);
        foreach (var (a, b, _) in pairs)
            Assert.True(read.MightContain("V" + b), $"value U+{char.ConvertToUtf32(a, 0):X4}, literal U+{char.ConvertToUtf32(b, 0):X4}");
    }

    /// <summary>
    /// A bloom as every build before the canonical fold wrote it — each value lowercased by the
    /// writer, the folded marker set — never says "no" for a literal the comparer matches with a
    /// value it holds. The previous reader probed the literal's lowercase and its raw spelling, and
    /// neither is the stored one for ς against Σ, µ against μ and Μ, ᲀ against В and в, … — the
    /// counter below, so the test shows it is looking.
    /// </summary>
    [Fact]
    public void A_lowercase_bloom_never_proves_absence_for_a_value_the_comparer_matches()
    {
        var pairs = ScanPairs().Where(static p => p.Ordinal).ToList();
        int oldReaderMissed = 0;
        foreach (var (a, b, _) in pairs)
        foreach (var (value, literal) in new[] { (a, b), (b, a) })
        {
            // A filter holding just this value: nothing else can answer for the literal.
            using var read = SegmentBloomFilter.Deserialise(LowercaseBloom([value]));
            Assert.Equal(SegmentBloomFilter.TextFold.LowerInvariant, read.Fold);
            Assert.True(read.MightContain(literal), $"value U+{char.ConvertToUtf32(value, 0):X4}, literal U+{char.ConvertToUtf32(literal, 0):X4}");

            string stored = value.ToLowerInvariant();
            if (literal.ToLowerInvariant() != stored && literal != stored) oldReaderMissed++;
        }
        _out.WriteLine($"{oldReaderMissed} directed pairs the old reader's lowercase probe did not reproduce");
        Assert.True(oldReaderMissed > 0, "lowercase agrees with the comparer everywhere — then this test is not looking");
    }

    /// <summary>An ASCII literal keeps its pruning in a lowercase bloom, and only an ASCII one does.</summary>
    [Fact]
    public void A_lowercase_bloom_still_prunes_ascii_literals_and_only_those()
    {
        using var read = SegmentBloomFilter.Deserialise(LowercaseBloom(["Connection refused", "Error", "οδος", "Ошибка"]));

        Assert.True(read.MightContain("connection REFUSED"));
        Assert.True(read.MightContain("ERROR"));
        Assert.False(read.MightContain("Warning"));
        Assert.False(read.MightContain("nothing here"));

        // Present, and absent: neither can be proved from these bits.
        Assert.True(read.MightContain("ΟΔΟΣ"));
        Assert.True(read.MightContain("ОШИБКА"));
        Assert.True(read.MightContain("ΑΘΗΝΑ"));
        Assert.True(read.MightContain("платёж"));
    }

    /// <summary>
    /// A canonical bloom folded by ANOTHER build's table — the day the table is regenerated — is
    /// probed by value only for ASCII literals, which every table folds alike.
    /// </summary>
    [Fact]
    public void A_bloom_another_table_folded_is_trusted_for_ascii_literals_only()
    {
        var table = CaseFold.CopyOfTable();
        table['ω'] = 'ω';                                   // a table that no longer folds ω
        byte[] section;
        using (CaseFold.UseTableForTest(table))
        {
            using var written = SegmentBloomFilter.Create(64);
            written.Add("Error");
            written.Add("ωμέγα");
            section = written.Serialise();
        }

        using var read = SegmentBloomFilter.Deserialise(section);
        Assert.Equal(SegmentBloomFilter.TextFold.Canonical, read.Fold);
        Assert.NotEqual(CaseFold.Fingerprint, read.FoldFingerprint);

        Assert.True(read.MightContain("error"));
        Assert.False(read.MightContain("Warning"));
        Assert.True(read.MightContain("ΩΜΈΓΑ"));            // folded differently there: not provable here
        Assert.True(read.MightContain("ΑΘΗΝΑ"));
    }

    /// <summary>
    /// A host whose comparer treats a character differently from the table — newer casing than the
    /// build's — gets "might" for a literal holding it, and full pruning for every other.
    /// </summary>
    [Fact]
    public void A_host_that_compares_a_character_differently_cannot_prune_a_literal_holding_it()
    {
        using var written = SegmentBloomFilter.Create(64);
        written.Add("αθηνα");
        using var read = SegmentBloomFilter.Deserialise(written.Serialise());

        Assert.False(read.MightContain("ΟΔΟΣ"));
        using (CaseFold.UseHostDriftForTest("Σσς"))
        {
            Assert.True(read.MightContain("ΟΔΟΣ"));
            Assert.False(read.MightContain("ΚΑΛΗ"));
        }
        Assert.False(read.MightContain("ΟΔΟΣ"));
    }

    /// <summary>
    /// A host whose comparer equates a non-ASCII character with an ASCII one breaks every ASCII
    /// premise the readers rely on: nothing is pruned, in any kind of section.
    /// </summary>
    [Fact]
    public void A_host_that_folds_into_ascii_prunes_nothing()
    {
        using var canonical = SegmentBloomFilter.Deserialise(SerialiseNew("Error"));
        using var lowercase = SegmentBloomFilter.Deserialise(LowercaseBloom(["Error"]));
        using (CaseFold.UseHostDriftForTest("sſ"))
        {
            Assert.True(CaseFold.HostDisagreesOnAscii);
            Assert.True(canonical.MightContain("Warning"));
            Assert.True(lowercase.MightContain("Warning"));
            Assert.Equal(0, SegmentTrigramIndex.QueryKeys("timeout", SegmentTrigramIndex.TrigramFold.Canonical, CaseFold.Fingerprint, new long[16]));
        }
        Assert.False(canonical.MightContain("Warning"));
    }

    /// <summary>
    /// ROLLBACK. An older build reads a bloom section as <c>[bitCount][capacity][bits]</c> and takes
    /// the first <c>bitCount / 8</c> bytes after the header; it probes the lowercase and the raw
    /// spelling of a literal. In a section this build wrote, that is one block with every bit set —
    /// a filter that answers "might" to any probe — so the older build reads such a group through
    /// its inverted index and its rows, never past them.
    /// </summary>
    [Fact]
    public void An_older_build_reads_a_canonical_bloom_as_one_that_never_says_no()
    {
        byte[] section = SerialiseNew("Error", "οδος");

        // bc182d60's Deserialise: bitCount, a capacity word whose bit 31 says "lowercased", and
        // bitCount / 8 bytes of bits after the header, once the section is at least that long.
        uint bitCount = BinaryPrimitives.ReadUInt32LittleEndian(section);
        uint capacity = BinaryPrimitives.ReadUInt32LittleEndian(section.AsSpan(4));
        Assert.Equal(512u, bitCount);                                          // one block
        Assert.True(section.Length >= 8 + bitCount / 8);                      // its length check passes
        Assert.Equal(0u, capacity & 0x8000_0000u);                            // a filter it does not trust for cased text either
        Assert.All(section.AsSpan(8, (int)(bitCount / 8)).ToArray(), static b => Assert.Equal(0xFF, b));
        // Its probe picks a block (there is one) and tests two of its bits: with every bit set,
        // every probe — lowercase, raw, any value — answers "might".
    }

    /// <summary>
    /// The trace-id index's filter holds eight-byte ids: it keeps the legacy layout, byte for byte —
    /// the folded marker included — and refuses text, which it could only store under a header that
    /// says something else.
    /// </summary>
    [Fact]
    public void A_binary_keyed_filter_keeps_the_legacy_layout_and_takes_no_text()
    {
        byte[] key = [1, 2, 3, 4, 5, 6, 7, 8];
        using var binary = SegmentBloomFilter.CreateForBinaryKeys(64);
        binary.Add((ReadOnlySpan<byte>)key);
        byte[] section = binary.Serialise();

        Assert.Equal(8 + binary.RetainedBytes, section.Length);
        Assert.Equal((uint)(binary.RetainedBytes * 8), BinaryPrimitives.ReadUInt32LittleEndian(section));
        Assert.Equal(0x8000_0000u | (uint)binary.Capacity, BinaryPrimitives.ReadUInt32LittleEndian(section.AsSpan(4)));
        Assert.Throws<InvalidOperationException>(() => binary.Add("text"));
        Assert.Throws<InvalidOperationException>(() => binary.AddUtf8("text"u8));

        using var read = SegmentBloomFilter.Deserialise(section);
        Assert.True(read.MightContain((ReadOnlySpan<byte>)key));
        Assert.Throws<InvalidOperationException>(() => read.Add("text"));      // a section read back is not canonical
    }

    /// <summary>A section that carries the marker and not what the marker promises is refused —
    /// never read as the all-ones block in front of it.</summary>
    [Fact]
    public void A_torn_canonical_bloom_is_refused()
    {
        byte[] section = SerialiseNew("Error");
        Assert.Throws<InvalidDataException>(() => SegmentBloomFilter.Deserialise(section.AsSpan(0, 80)));
        Assert.Throws<InvalidDataException>(() => SegmentBloomFilter.Deserialise(section.AsSpan(0, section.Length - 1)));
    }

    [Fact]
    public void WriteTo_writes_what_Serialise_returns()
    {
        using var filter = SegmentBloomFilter.Create(1_000);
        filter.Add("ΟΔΟΣ");
        filter.AddUtf8("µs"u8);
        var ms = new MemoryStream();
        filter.WriteTo(ms);
        Assert.Equal(filter.Serialise(), ms.ToArray());
        Assert.Equal(filter.SerialisedLength, ms.Length);
    }

    // ── The trigram index ─────────────────────────────────────────────────────

    /// <summary>
    /// Every pair EITHER relation calls equal — OrdinalIgnoreCase for free text and <c>contains</c>,
    /// lowercase for LIKE — is one trigram in a canonical section, so a substring the scan matches
    /// is never ruled out.
    /// </summary>
    [Fact]
    public void A_canonical_trigram_section_finds_every_spelling_the_scan_accepts()
    {
        var pairs = ScanPairs();
        var built = new SegmentTrigramIndex();
        for (int i = 0; i < pairs.Count; i++) built.Add((uint)i, "q" + pairs[i].A + "z");
        byte[] section = built.Serialise();
        var oracle = SegmentTrigramIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load([], section, []);
        Assert.Equal(SegmentTrigramIndex.TrigramFold.Canonical, oracle.Fold);

        for (int i = 0; i < pairs.Count; i++)
        {
            string search = "Q" + pairs[i].B + "Z";
            var hits = oracle.Lookup(search);
            Assert.True(hits is null || Array.IndexOf(hits, (uint)i) >= 0,
                $"{(pairs[i].Ordinal ? "OrdinalIgnoreCase" : "lowercase")} pair U+{char.ConvertToUtf32(pairs[i].A, 0):X4} / U+{char.ConvertToUtf32(pairs[i].B, 0):X4}");
            Assert.Equal(hits, lazy.LookupTrigram(search));
        }
    }

    /// <summary>
    /// A trigram section every build before the canonical fold wrote — V2, keys lowercased by the
    /// writer — never rules out a substring the scan matches: a trigram holding anything but ASCII is
    /// not asked of it, and every one that is asked was stored as asked.
    /// </summary>
    [Fact]
    public void A_lowercase_trigram_section_never_rules_out_a_substring_the_scan_matches()
    {
        var pairs = ScanPairs();
        var adds = new List<(uint, string)>();
        for (int i = 0; i < pairs.Count; i++) adds.Add(((uint)i, "q" + pairs[i].A + "z"));
        byte[] section = LowercaseTrigrams(adds);
        var oracle = SegmentTrigramIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load([], section, []);
        Assert.Equal(SegmentTrigramIndex.TrigramFold.Lower, oracle.Fold);

        for (int i = 0; i < pairs.Count; i++)
        {
            string search = "Q" + pairs[i].B + "Z";
            var hits = oracle.Lookup(search);
            Assert.True(hits is null || Array.IndexOf(hits, (uint)i) >= 0,
                $"{(pairs[i].Ordinal ? "OrdinalIgnoreCase" : "lowercase")} pair U+{char.ConvertToUtf32(pairs[i].A, 0):X4} / U+{char.ConvertToUtf32(pairs[i].B, 0):X4}");
            Assert.Equal(hits, lazy.LookupTrigram(search));
        }
    }

    /// <summary>
    /// What a lowercase section still prunes: an ASCII term fully, a mixed term by its all-ASCII
    /// windows, a term with none of those not at all ("no information", never "absent").
    /// </summary>
    [Theory]
    [InlineData("REFUSED",              "1")]        // exact
    [InlineData("nothing",              "")]         // absent, and provably so
    [InlineData("ΟΔΟΣ.EXCEPTION",       "3")]        // ".ex", "exc", … — the Greek half asks nothing
    [InlineData("ΟΔΟΣ",                 null)]       // nothing to ask
    [InlineData("ОШИБКА",               null)]
    [InlineData("Ошибка платежа",       null)]
    [InlineData("\U0001F525 hot",        "4")]        // " ho", "hot"
    public void A_lowercase_section_answers_what_its_ascii_trigrams_can(string search, string? expected)
    {
        byte[] section = LowercaseTrigrams(
        [
            (0, "Ошибка платежа"),
            (1, "Connection refused"),
            (2, "οδος"),
            (3, "System.Οδος.Exception"),
            (4, "\U0001F525 hot"),
        ]);
        var oracle = SegmentTrigramIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load([], section, []);

        uint[]? hits = oracle.Lookup(search);
        Assert.Equal(hits, lazy.LookupTrigram(search));
        if (expected is null) Assert.Null(hits);
        else Assert.Equal(expected.Length == 0 ? [] : expected.Split(',').Select(uint.Parse).ToArray(), hits);
    }

    /// <summary>
    /// ROLLBACK. An older build reads a trigram section's first eight bytes as the V2 magic and a
    /// bucket count; in a section this build wrote the count is zero, which it treats as an index
    /// that holds nothing — "no information" — and so reads such a group instead of looking up
    /// canonical keys with its own lowercase.
    /// </summary>
    [Fact]
    public void An_older_build_reads_a_canonical_trigram_section_as_holding_nothing()
    {
        var built = new SegmentTrigramIndex();
        built.Add(0, "Failed to process payment");
        built.Add(1, "Ошибка платежа");
        byte[] section = built.Serialise();

        Assert.Equal(0xFFFFFFFEu, BinaryPrimitives.ReadUInt32LittleEndian(section));
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(section.AsSpan(4)));

        // This build reads the real buckets behind them.
        var read = SegmentTrigramIndex.Deserialise(section);
        Assert.Equal([0u], read.Lookup("PAYMENT")!);
        Assert.Equal([1u], read.Lookup("ПЛАТЕЖА")!);

        // And the eight bytes on their own are what every build reads as "never built".
        Assert.Null(SegmentTrigramIndex.Deserialise(section.AsSpan(0, 8)).Lookup("payment"));
        using var lazy = SegmentIndexReader.Load([], section.AsSpan(0, 8), []);
        Assert.Null(lazy.LookupTrigram("payment"));
    }

    /// <summary>
    /// A trigram section folded by ANOTHER build's table is asked only its ASCII windows: a
    /// non-ASCII trigram may be stored under a key this build's table would not produce.
    /// </summary>
    [Fact]
    public void A_trigram_section_another_table_folded_is_asked_only_its_ascii_windows()
    {
        var table = CaseFold.CopyOfTable();
        table['ω'] = 'ω';                                   // a table that no longer folds ω
        byte[] section;
        using (CaseFold.UseTableForTest(table))
        {
            var built = new SegmentTrigramIndex();
            built.Add(0, "ωμέγα point");
            built.Add(1, "another point");
            section = built.Serialise();
        }
        var read = SegmentTrigramIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load([], section, []);
        Assert.Equal(SegmentTrigramIndex.TrigramFold.Canonical, read.Fold);

        foreach (var (search, expected) in new (string, uint[]?)[]
        {
            ("ΩΜΈΓΑ",       null),      // stored (ω, Μ, Έ) there; this build would ask (Ω, Μ, Έ)
            ("POINT",       [0, 1]),
            ("another",     [1]),
            ("absent",      []),
            ("ΩΜΈΓΑ POINT", [0, 1]),    // " po", "poi", … only
        })
        {
            Assert.Equal(expected, read.Lookup(search));
            Assert.Equal(expected, lazy.LookupTrigram(search));
        }
    }

    /// <summary>
    /// A host whose comparer treats a character differently from the table: a trigram holding it is
    /// not asked, and the term narrows by the rest of its windows — never to proof of absence by it.
    /// </summary>
    [Fact]
    public void A_host_that_compares_a_character_differently_does_not_ask_a_trigram_holding_it()
    {
        var built = new SegmentTrigramIndex();
        built.Add(0, "οδος");
        built.Add(1, "οδοx");
        byte[] section = built.Serialise();
        var read = SegmentTrigramIndex.Deserialise(section);

        Assert.Equal([0u], read.Lookup("ΟΔΟΣ")!);         // both windows asked
        using (CaseFold.UseHostDriftForTest("Σσς"))
        {
            using var lazy = SegmentIndexReader.Load([], section, []);
            Assert.Equal([0u, 1u], read.Lookup("ΟΔΟΣ")!);  // "ΔΟΣ" not asked; "ΟΔΟ" narrows
            Assert.Equal([0u, 1u], lazy.LookupTrigram("ΟΔΟΣ")!);
            Assert.Null(read.Lookup("ΔΟΣ"));               // its one window holds Σ
            Assert.Null(lazy.LookupTrigram("ΔΟΣ"));
        }
    }

    /// <summary>
    /// LIKE MEETS THE TRIGRAM INDEX WITH THE PATTERN AS TYPED. On a host whose casing is newer than
    /// the table, a typed character can share its host lowercase with a stored one the table does
    /// not fold it with — here a build whose table predates the Ohm sign's lowercase (U+2126 → ω):
    /// LIKE takes a typed Ω (U+03A9) and a stored Ohm sign both to ω. The host-drift set marked
    /// the Ohm sign and ω, but not Ω, so <c>'%Ωyz%'</c> asked (Ω, Y, Z) of a group that stored
    /// (U+2126, Y, Z) and dropped it. Ω is now marked through its lowercase (review of #168).
    /// </summary>
    [Fact]
    public void A_pattern_typed_in_capitals_is_not_trusted_where_the_host_lowercases_what_the_table_does_not_fold()
    {
        Assert.True(char.ToLowerInvariant('Ω') == 'ω', "this host does not lowercase the Ohm sign — the premise is gone");

        var table = CaseFold.CopyOfTable();
        table['Ω'] = 'Ω';
        using var fold  = CaseFold.UseTableForTest(table);
        using var drift = CaseFold.UseHostDriftAgainstTableForTest(table);
        Assert.True(CaseFold.HostDisagrees('Ω'));
        Assert.True(CaseFold.HostDisagrees('ω'));
        Assert.True(CaseFold.HostDisagrees('Ω'), "the typed capital is not marked through its lowercase");

        var built = new SegmentTrigramIndex();
        built.Add(0, "xΩyz");
        byte[] section = built.Serialise();
        var read = SegmentTrigramIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load([], section, []);
        foreach (string search in new[] { "Ωyz", "ωyz", "Ωyz", "xΩy" })
        {
            var hits = read.Lookup(search);
            Assert.True(hits is null || Array.IndexOf(hits, 0u) >= 0, $"'{search}' ruled out the row LIKE matches");
            Assert.Equal(hits, lazy.LookupTrigram(search));
        }
    }

    /// <summary>
    /// A canonical trigram section cut short anywhere is either unreadable — which the executor
    /// answers by reading the group — or "no information"; it never proves a term it holds absent.
    /// </summary>
    [Fact]
    public void A_torn_canonical_trigram_section_never_proves_absence()
    {
        var built = new SegmentTrigramIndex();
        built.Add(0, "Connection refused");
        built.Add(1, "Ошибка платежа");
        byte[] section = built.Serialise();

        for (int length = 0; length < section.Length; length++)
        {
            byte[] torn = section.AsSpan(0, length).ToArray();
            foreach (var (search, row) in new (string, uint)[] { ("refused", 0), ("ПЛАТЕЖА", 1) })
            foreach (var ask in new Func<uint[]?>[]
            {
                () => SegmentTrigramIndex.Deserialise(torn).Lookup(search),
                () => { using var lazy = SegmentIndexReader.Load([], torn, []); return lazy.LookupTrigram(search); },
            })
            {
                uint[]? hits;
                try { hits = ask(); }
                catch (Exception) { continue; }            // unreadable: QueryExecutor reads the group
                Assert.True(hits is null || Array.IndexOf(hits, row) >= 0,
                    $"'{search}' lost its row in the section cut at {length} of {section.Length} bytes");
            }
        }
    }

    /// <summary>
    /// A surrogate that is half of a pair in the stored text is half of the same collapsed scalar in
    /// the search: a substring that starts or ends inside a pair still finds its trigrams.
    /// </summary>
    [Fact]
    public void A_search_that_cuts_a_surrogate_pair_still_finds_its_row()
    {
        var built = new SegmentTrigramIndex();
        built.Add(7, "ab\U0001F600cd");
        var read = SegmentTrigramIndex.Deserialise(built.Serialise());

        Assert.Equal([7u], read.Lookup("AB\U0001F600CD")!);
        Assert.Equal([7u], read.Lookup("ab\uD83D")!);                 // ends inside the pair
        Assert.Equal([7u], read.Lookup("\uDE00cd")!);                 // starts inside it
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static byte[] SerialiseNew(params string[] values)
    {
        using var filter = SegmentBloomFilter.Create(64);
        foreach (var v in values) filter.Add(v);
        return filter.Serialise();
    }

    /// <summary>
    /// A bloom section as every build from folding up to the canonical fold wrote it: each value's
    /// <c>ToLowerInvariant</c>, encoded UTF-8, in the legacy layout with the folded marker — which is
    /// what a binary-keyed filter still writes.
    /// </summary>
    internal static byte[] LowercaseBloom(IReadOnlyCollection<string> values)
    {
        using var filter = SegmentBloomFilter.CreateForBinaryKeys(Math.Max(64, values.Count * 4));
        foreach (var v in values) filter.Add(Encoding.UTF8.GetBytes(v.ToLowerInvariant()));
        return filter.Serialise();
    }

    /// <summary>
    /// A trigram section as every build from the V2 layout up to the canonical fold wrote it: keys
    /// lowercased with <c>ToLowerInvariant</c>, buckets in first-seen order, postings ascending.
    /// </summary>
    internal static byte[] LowercaseTrigrams(IEnumerable<(uint Offset, string Text)> adds)
    {
        var sets = new Dictionary<(char, char, char), List<int>>();
        foreach (var (offset, text) in adds)
        {
            if (text.Length < 3) continue;
            string lower = text.ToLowerInvariant();
            for (int i = 0; i <= lower.Length - 3; i++)
            {
                var key = (lower[i], lower[i + 1], lower[i + 2]);
                if (!sets.TryGetValue(key, out var list)) sets[key] = list = [];
                if (list.Count == 0 || list[^1] < (int)offset) list.Add((int)offset);
            }
        }

        var ms = new MemoryStream();
        Span<byte> tmp = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, 0xFFFFFFFEu);       ms.Write(tmp);
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, (uint)sets.Count); ms.Write(tmp);
        foreach (var ((c0, c1, c2), list) in sets)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(tmp, c0); ms.Write(tmp[..2]);
            BinaryPrimitives.WriteUInt16LittleEndian(tmp, c1); ms.Write(tmp[..2]);
            BinaryPrimitives.WriteUInt16LittleEndian(tmp, c2); ms.Write(tmp[..2]);
            var enc = new byte[SegmentBitmapCodec.MaxEncodedSize(list.Count)];
            int n   = SegmentBitmapCodec.Encode(list.ToArray(), enc);
            BinaryPrimitives.WriteUInt32LittleEndian(tmp, (uint)n); ms.Write(tmp);
            ms.Write(enc, 0, n);
        }
        return ms.ToArray();
    }
}
