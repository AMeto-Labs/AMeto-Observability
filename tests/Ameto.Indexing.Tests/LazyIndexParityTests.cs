using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// <see cref="SegmentIndexReader"/> answers from the PACKED sections, lazily, and remembers what
/// it worked out; <see cref="SegmentInvertedIndex.Deserialise"/> and
/// <see cref="SegmentTrigramIndex.Deserialise"/> decode everything up front. They must give the
/// same answer to every question — the decoded pair is the reference, and every rule it has
/// accumulated gets a shape here: values compared OrdinalIgnoreCase with case-collisions unioned,
/// names and values decoded with replacement (two invalid names that decode alike are ONE
/// property), the dotted spelling of an encoded path, every numeric and bool encoding of a
/// literal, values long enough to leave the stack scratch, legacy layouts, and a legacy trigram
/// section that repeats a key (the last bucket wins).
///
/// <para>Every question is asked twice: once from the section, once from the memo that the first
/// asking filled. And answers are handed out as copies — a caller that writes into one must not
/// change the next.</para>
/// </summary>
public sealed class LazyIndexParityTests
{
    // ── Inverted ──────────────────────────────────────────────────────────────

    private static readonly byte[] InvalidA = [0x61, 0xFF];   // "a" + invalid byte
    private static readonly byte[] InvalidB = [0x61, 0xFE];   // decodes to the same "a\uFFFD"

    /// <summary>A section with one bucket of every shape the decoded reader has a rule for.</summary>
    private static SegmentInvertedIndex BuildRich()
    {
        var b = new SegmentInvertedIndex();
        uint o = 0;
        // Case collisions — one bucket to a query.
        b.Add(o++, "@l", "Error");
        b.Add(o++, "@l", "error");
        b.Add(o++, "@l", "ERROR");
        b.Add(o++, "@l", "Information");
        // Numbers and bools in every stored type, and digits stored as text.
        b.Add(o++, "Count", 5);
        b.Add(o++, "Count", 5L);
        b.Add(o++, "Count", 5.0d);
        b.Add(o++, "Count", "5");
        b.Add(o++, "Count", 7L);
        b.Add(o++, "Ratio", 2.5d);
        b.Add(o++, "Ratio", 2.5f);
        b.Add(o++, "Enabled", true);
        b.Add(o++, "Enabled", false);
        b.Add(o++, "Missing", null);
        b.Add(o++, "Empty", "");
        // Non-ASCII values, and characters OrdinalIgnoreCase folds onto ASCII ones.
        b.Add(o++, "Msg", "Ошибка");
        b.Add(o++, "Msg", "ОШИБКА");
        b.Add(o++, "Msg", "ſ");            // long s
        b.Add(o++, "Msg", "K");            // Kelvin sign
        b.Add(o++, "Msg", "ı");            // dotless i
        b.Add(o++, "Msg", "straße");
        b.Add(o++, "Msg", "plain");
        b.Add(o++, "Msg", new string('x', 300));
        b.Add(o++, "Msg", new string('X', 300));
        b.Add(o++, "Msg", new string('y', 90));
        // A nested map key and a flat attribute that spell the same path.
        b.Add(o++, "http\u0001method", "GET");
        b.Add(o++, "http.method", "get");
        b.Add(o++, "http.method", "POST");
        b.Add(o++, "only.flat", "v");
        b.Add(o++, "arr\u0001x\u0002", "z");
        // Invalid UTF-8 in names and values: two byte strings, one decoded string.
        b.AddUtf8(o++, InvalidA, "v1"u8);
        b.AddUtf8(o++, InvalidB, "v2"u8);
        b.AddUtf8(o++, InvalidB, "v1"u8);
        b.AddUtf8(o++, "Bytes"u8, InvalidA);
        b.AddUtf8(o++, "Bytes"u8, InvalidB);
        // A value repeated across many events — a real posting list.
        for (uint i = 0; i < 50; i++) b.Add(o++, "@l", i % 3 == 0 ? "Warning" : "warning");
        return b;
    }

    /// <summary>Every question the rich section has a rule for, and some it has no bucket for.</summary>
    private static readonly (string Property, object? Value)[] RichQuestions =
    [
        ("@l", "Error"), ("@l", "error"), ("@l", "eRRoR"), ("@l", "Information"), ("@l", "Warning"),
        ("@l", "Fatal"),
        ("Count", 5L), ("Count", 5), ("Count", "5"), ("Count", 5.0d), ("Count", 7L), ("Count", "7"),
        ("Count", 6L), ("Count", "five"),
        ("Ratio", 2.5d), ("Ratio", "2.5"), ("Ratio", 2.5f), ("Ratio", 3L),
        ("Enabled", true), ("Enabled", false), ("Enabled", "true"), ("Enabled", "TRUE"), ("Enabled", "no"),
        ("Missing", null), ("Count", null), ("Empty", ""), ("Empty", "x"),
        ("Msg", "ошибка"), ("Msg", "Ошибка"), ("Msg", "s"), ("Msg", "S"), ("Msg", "k"), ("Msg", "K"),
        ("Msg", "i"), ("Msg", "I"), ("Msg", "STRASSE"), ("Msg", "STRAßE"), ("Msg", "PLAIN"),
        ("Msg", new string('x', 300)), ("Msg", new string('x', 299)), ("Msg", new string('Y', 90)),
        ("Msg", new string('y', 100)),
        ("http\u0001method", "GET"), ("http\u0001method", "get"), ("http\u0001method", "POST"),
        ("http\u0001method", "PUT"), ("http.method", "GET"), ("only\u0001flat", "v"),
        ("only\u0001flat", "w"), ("arr\u0001x\u0002", "z"), ("arr.x\u0002", "z"),
        ("a\uFFFD", "v1"), ("a\uFFFD", "v2"), ("a\uFFFD", "v3"), ("Bytes", "a\uFFFD"), ("Bytes", "A\uFFFD"),
        ("NoSuchProperty", "x"), ("", ""),
    ];

    public static TheoryData<string> Layouts => new() { "codec", "roaring" };

    private static byte[] Serialise(SegmentInvertedIndex b, string layout) =>
        layout == "codec" ? b.Serialise() : b.SerialiseRoaringV1();

    [Theory]
    [MemberData(nameof(Layouts))]
    public void EveryInvertedQuestion_GetsTheDecodedAnswer_FromTheSectionAndFromTheMemo(string layout)
    {
        byte[] section = Serialise(BuildRich(), layout);
        var oracle = SegmentInvertedIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load(section, [], []);

        for (int pass = 0; pass < 2; pass++)
        {
            foreach (var (p, v) in RichQuestions)
            {
                string what = $"pass {pass}, {Show(p)} = {Show(v)}";
                Assert.True(Same(oracle.Lookup(p, v), lazy.Lookup(p, v)), "Lookup: " + what);
                Assert.True(oracle.MightContain(p, v) == lazy.MightContain(p, v), "MightContain: " + what);
                Assert.True(Same(oracle.LookupIntersect([(p, v)]), lazy.LookupIntersect([(p, v)])), "LookupIntersect: " + what);
            }

            // Conjunctions: every pair, including known-with-unknown and absent-with-present.
            for (int i = 0; i < RichQuestions.Length; i += 3)
            for (int j = 1; j < RichQuestions.Length; j += 4)
            {
                (string, object?)[] both = [RichQuestions[i], RichQuestions[j]];
                Assert.True(Same(oracle.LookupIntersect(both), lazy.LookupIntersect(both)),
                    $"pass {pass}, LookupIntersect of {Show(both[0].Item1)} and {Show(both[1].Item1)}");
            }
        }
    }

    [Fact]
    public void AnEmptyInvertedSection_AnswersNoInformation_LikeTheDecodedOne()
    {
        var oracle = SegmentInvertedIndex.Deserialise([]);
        using var lazy = SegmentIndexReader.Load([], [], []);

        Assert.Null(lazy.Lookup("@l", "Error"));
        Assert.Null(lazy.LookupIntersect([("@l", "Error")]));
        Assert.True(lazy.MightContain("@l", "Error"));
        Assert.Equal(oracle.MightContain("@l", "Error"), lazy.MightContain("@l", "Error"));
    }

    [Fact]
    public void RandomSections_AnswerRandomQuestions_LikeTheDecodedOne()
    {
        var rng = new Random(80);
        string[] props  = ["A", "b", "B", "c.d", "c\u0001d", "Ω", "e"];
        string[] values = ["x", "X", "xy", "XY", "Ωmega", "ΩMEGA", "1", "01", "true", "", "long-" + new string('q', 40)];

        for (int round = 0; round < 40; round++)
        {
            var b = new SegmentInvertedIndex();
            int events = rng.Next(1, 300);
            for (uint o = 0; o < events; o++)
            {
                int k = rng.Next(1, 4);
                for (int j = 0; j < k; j++)
                {
                    string p = props[rng.Next(props.Length)];
                    object? v = rng.Next(5) switch
                    {
                        0 => (long)rng.Next(3),
                        1 => rng.Next(2) == 0,
                        2 => rng.Next(3) + 0.5d,
                        _ => values[rng.Next(values.Length)],
                    };
                    b.Add(o, p, v);
                }
            }
            byte[] section = rng.Next(4) == 0 ? b.SerialiseRoaringV1() : b.Serialise();
            var oracle = SegmentInvertedIndex.Deserialise(section);
            using var lazy = SegmentIndexReader.Load(section, [], []);

            for (int q = 0; q < 60; q++)
            {
                string p = props[rng.Next(props.Length)];
                object? v = rng.Next(4) switch
                {
                    0 => (long)rng.Next(3),
                    1 => rng.Next(2) == 0 ? "TRUE" : false,
                    2 => (rng.Next(3) + 0.5d).ToString(System.Globalization.CultureInfo.InvariantCulture),
                    _ => values[rng.Next(values.Length)],
                };
                string what = $"round {round}, {Show(p)} = {Show(v)}";
                Assert.True(Same(oracle.Lookup(p, v), lazy.Lookup(p, v)), "Lookup: " + what);
                Assert.True(oracle.MightContain(p, v) == lazy.MightContain(p, v), "MightContain: " + what);
                (string, object?)[] pair = [(p, v), (props[rng.Next(props.Length)], values[rng.Next(values.Length)])];
                Assert.True(Same(oracle.LookupIntersect(pair), lazy.LookupIntersect(pair)), "LookupIntersect: " + what);
            }
        }
    }

    [Fact]
    public void AnAnswerIsACopy_WritingIntoItChangesNothing()
    {
        byte[] section = BuildRich().Serialise();
        using var lazy = SegmentIndexReader.Load(section, [], []);

        var first = lazy.LookupIntersect([("@l", "Warning")])!;
        var expected = (uint[])first.Clone();
        Array.Fill(first, 99u);

        Assert.Equal(expected, lazy.LookupIntersect([("@l", "Warning")]));
        Assert.Equal(expected, lazy.Lookup("@l", "warning"));
    }

    // ── Laziness: what a question costs is what it asks for ─────────────────

    /// <summary>
    /// The point of the change. A group with one high-cardinality property (a request id per
    /// event) and one small one: asking about the small one must not decode the big one. The
    /// decoded reader held a string and an array per request id — several times the section —
    /// and allocated all of it before answering anything.
    /// </summary>
    [Fact]
    public void AskingOneProperty_DecodesNeitherTheOthersNorTheSection()
    {
        var b = new SegmentInvertedIndex();
        for (uint o = 0; o < 20_000; o++)
        {
            b.Add(o, "RequestId", $"req-{o:x8}");
            b.Add(o, "Provider", o % 3 == 0 ? "UnionPay" : "Visa");
        }
        byte[] section = b.Serialise();

        long a0 = GC.GetAllocatedBytesForCurrentThread();
        using var lazy = SegmentIndexReader.Load(section, [], []);
        var hits = lazy.LookupIntersect([("Provider", "UnionPay")]);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - a0;

        Assert.Equal(SegmentInvertedIndex.Deserialise(section).LookupIntersect([("Provider", "UnionPay")]), hits);

        // The section copy the loaded reader owns, one posting list and the answer — well under
        // twice the section. Decoding everything is ~8x it for this shape.
        Assert.True(allocated < 2L * section.Length,
            $"a one-property question allocated {allocated} B against a {section.Length} B section");
        Assert.True(lazy.ApproxManagedBytes < 3L * section.Length / 2,
            $"the reader retains {lazy.ApproxManagedBytes} B for a {section.Length} B section");
    }

    // ── Counted before it is named ────────────────────────────────────────────

    /// <summary>
    /// <see cref="ISegmentIndex.CountAtMost"/> is <see cref="ISegmentIndex.LookupIntersect"/>
    /// asked about one predicate, with a number in place of the list — and the narrowing decides
    /// by it whether the list is built at all. So the two must never disagree about which of the
    /// three answers a question has: a count of "no information" over a list of "nothing matches"
    /// would scan a group the index had proved empty, and the reverse would DROP one it knew
    /// nothing about.
    ///
    /// <para>Every row of the rich section is filed once, so here the number is also exact — for
    /// the decoded reader, for a packed reader asked only to count (which reads each list's
    /// leading count, or decodes a legacy bitmap that has none), and for one that had already
    /// decoded the list. Both passes: the second is answered from the memo.</para>
    /// </summary>
    [Theory]
    [MemberData(nameof(Layouts))]
    public void EveryCount_IsTheAnswerTheListGives_FromTheSectionAndFromTheMemo(string layout)
    {
        byte[] section = Serialise(BuildRich(), layout);
        var oracle = SegmentInvertedIndex.Deserialise(section);
        using var counting = SegmentIndexReader.Load(section, [], []);   // asked to count, never to list
        using var listing  = SegmentIndexReader.Load(section, [], []);   // asked to list, then to count

        for (int pass = 0; pass < 2; pass++)
        foreach (var (p, v) in RichQuestions)
        {
            string what = $"pass {pass}, {Show(p)} = {Show(v)}";
            uint[]? list = oracle.LookupIntersect([(p, v)]);
            int?    rows = list?.Length;

            Assert.True(rows == oracle.CountAtMost(p, v),   $"decoded: {what}");
            Assert.True(rows == counting.CountAtMost(p, v), $"counted off the section: {what}");
            Assert.True(Same(list, listing.LookupIntersect([(p, v)])), $"list: {what}");
            Assert.True(rows == listing.CountAtMost(p, v),  $"counted off the decoded list: {what}");
        }

        // A reader that was only ever asked to count still has every list to give.
        foreach (var (p, v) in RichQuestions)
        {
            Assert.True(Same(oracle.LookupIntersect([(p, v)]), counting.LookupIntersect([(p, v)])), $"list after count: {Show(p)} = {Show(v)}");
            Assert.True(oracle.MightContain(p, v) == counting.MightContain(p, v), $"MightContain after count: {Show(p)} = {Show(v)}");
        }
    }

    /// <summary>
    /// The same, where the bound is NOT exact: random sections file one row under several
    /// spellings of a property and several casings of a value, which the list unions and the
    /// packed count sums. What must hold is the contract, whichever was asked first.
    /// </summary>
    [Fact]
    public void RandomSections_CountNoFewerRowsThanTheyList_AndGiveTheSameKindOfAnswer()
    {
        var rng = new Random(161);
        string[] props  = ["A", "b", "B", "c.d", "c\u0001d", "Ω", "e"];
        string[] values = ["x", "X", "xy", "XY", "Ωmega", "ΩMEGA", "1", "01", "true", "", "long-" + new string('q', 40)];
        bool boundWasLoose = false;

        for (int round = 0; round < 40; round++)
        {
            var b = new SegmentInvertedIndex();
            int events = rng.Next(1, 300);
            for (uint o = 0; o < events; o++)
            {
                int k = rng.Next(1, 4);
                for (int j = 0; j < k; j++)
                    b.Add(o, props[rng.Next(props.Length)], rng.Next(4) switch
                    {
                        0 => (long)rng.Next(3),
                        1 => rng.Next(2) == 0,
                        _ => values[rng.Next(values.Length)],
                    });
            }
            byte[] section = rng.Next(4) == 0 ? b.SerialiseRoaringV1() : b.Serialise();
            var oracle = SegmentInvertedIndex.Deserialise(section);
            using var lazy = SegmentIndexReader.Load(section, [], []);

            for (int q = 0; q < 60; q++)
            {
                string  p = props[rng.Next(props.Length)];
                object? v = rng.Next(3) switch
                {
                    0 => (long)rng.Next(3),
                    1 => rng.Next(2) == 0 ? "TRUE" : false,
                    _ => values[rng.Next(values.Length)],
                };
                string what = $"round {round}, {Show(p)} = {Show(v)}";

                // Either order: the count before its list is decoded, or after.
                if (rng.Next(2) == 0) lazy.LookupIntersect([(p, v)]);
                int?    counted = lazy.CountAtMost(p, v);
                uint[]? list    = lazy.LookupIntersect([(p, v)]);

                Assert.True(Same(oracle.LookupIntersect([(p, v)]), list), "list: " + what);
                AssertCounts(list, counted, "packed: " + what);
                Assert.True(list?.Length == oracle.CountAtMost(p, v), "decoded: " + what);   // exact: it has the union
                if (list is not null && counted > list.Length) boundWasLoose = true;
            }
        }

        // Without a loose bound somewhere, this proved no more than the rich section does.
        Assert.True(boundWasLoose, "no question was counted above its list");
    }

    /// <summary>
    /// What the count is FOR. The posting list worth not building is the big one — a value most
    /// of a group carries — and it is told apart from the rest by its length, which the section
    /// states in the list's first bytes. Reading that must cost nothing like the list: it used to
    /// be decoded, copied and kept in order to learn that it should not have been.
    /// </summary>
    [Fact]
    public void CountingABucket_DecodesNoPostingList_AndKeepsNone()
    {
        const int Rows = 20_000, Listed = Rows - (Rows + 2) / 3;
        var b = new SegmentInvertedIndex();
        for (uint o = 0; o < Rows; o++)
        {
            b.Add(o, "RequestId", $"req-{o:x8}");
            b.Add(o, "Provider", o % 3 == 0 ? "UnionPay" : "Visa");
        }
        byte[] section = b.Serialise();

        using var lazy = SegmentIndexReader.Load(section, [], []);
        Assert.Null(lazy.CountAtMost("NoSuchProperty", "x"));        // the catalog is read here, once
        long kept = lazy.ApproxManagedBytes;

        long a0 = GC.GetAllocatedBytesForCurrentThread();
        int? counted = lazy.CountAtMost("Provider", "Visa");
        long allocated = GC.GetAllocatedBytesForCurrentThread() - a0;

        Assert.Equal(Listed, counted);
        // Decoded, the list is 4 bytes a row: 53 KB. The count is a memo entry — and, the first
        // time, the tables it and the absent encodings of the value are remembered in.
        Assert.True(allocated < 2048, $"counting a {Listed}-row bucket allocated {allocated} B");
        Assert.True(lazy.ApproxManagedBytes - kept < 2048,
            $"counting a {Listed}-row bucket left {lazy.ApproxManagedBytes - kept} B in the memo");

        // Asked again it is the memo's, and the memo's answer is free.
        a0 = GC.GetAllocatedBytesForCurrentThread();
        counted = lazy.CountAtMost("Provider", "Visa");
        allocated = GC.GetAllocatedBytesForCurrentThread() - a0;
        Assert.Equal(Listed, counted);
        Assert.Equal(0, allocated);

        // The list is still there for a caller who does want it, and from then on the count is
        // the list's own length: the number kept in its place is dropped.
        long counting = lazy.ApproxManagedBytes;
        Assert.Equal(Listed, lazy.LookupIntersect([("Provider", "Visa")])!.Length);
        Assert.True(lazy.ApproxManagedBytes - counting >= 4L * Listed - 512,
            $"the decoded list added {lazy.ApproxManagedBytes - counting} B to the memo");
        Assert.Equal(Listed, lazy.CountAtMost("Provider", "Visa"));
    }

    /// <summary>
    /// A value that is in the section and lists no row is not an absent value: the decoded
    /// reader holds an empty list for it and <c>MightContain</c> says so. Both count 0, and a
    /// packed reader that counted first must not go on to remember the value as absent — its
    /// answers would then depend on which question it was asked first.
    /// </summary>
    [Fact]
    public void AValueFiledWithNoRow_IsStillAValue_WhicheverQuestionComesFirst()
    {
        var b = new SegmentInvertedIndex();
        b.Add(3, "P", "hollow");
        b.Add(7, "P", "one");
        byte[] section = b.Serialise();

        // No builder files a value without a row, so the list is emptied by hand: its leading
        // count, which follows the value's text and the list's own byte length.
        int count = section.AsSpan().IndexOf("hollow"u8) + "hollow".Length + sizeof(uint);
        Assert.Equal(1, section[count]);
        section[count] = 0;

        var oracle = SegmentInvertedIndex.Deserialise(section);
        (string, object?)[] hollow = [("P", "hollow")];
        Assert.True(oracle.LookupIntersect(hollow) is { Length: 0 });
        Assert.True(oracle.MightContain("P", "hollow"));
        Assert.Equal(0, oracle.CountAtMost("P", "hollow"));

        foreach (bool countFirst in (bool[])[true, false])
        {
            using var lazy = SegmentIndexReader.Load(section, [], []);
            if (countFirst) Assert.Equal(0, lazy.CountAtMost("P", "hollow"));

            Assert.True(lazy.LookupIntersect(hollow) is { Length: 0 }, $"list, count first: {countFirst}");
            Assert.True(lazy.Lookup("P", "hollow") is { Length: 0 }, $"lookup, count first: {countFirst}");
            Assert.True(lazy.MightContain("P", "hollow"), $"MightContain, count first: {countFirst}");
            Assert.Equal(0, lazy.CountAtMost("P", "hollow"));

            // And the value beside it is untouched by any of this.
            Assert.Equal(1, lazy.CountAtMost("P", "one"));
            Assert.Equal([7u], lazy.LookupIntersect([("P", "one")])!);
        }
    }

    /// <summary>
    /// An index that knows no cheaper way counts by building the list: the interface's default,
    /// which is what any other implementation gets, has to give the same three answers.
    /// </summary>
    [Fact]
    public void AnIndexWithNoCountOfItsOwn_CountsByItsList()
    {
        var b = new SegmentInvertedIndex();
        b.Add(0, "P", "v");
        b.Add(4, "P", "v");
        ISegmentIndex index = new ListsOnly(SegmentInvertedIndex.Deserialise(b.Serialise()));

        Assert.Equal(2, index.CountAtMost("P", "v"));
        Assert.Equal(0, index.CountAtMost("P", "absent"));
        Assert.Null(index.CountAtMost("NoSuchProperty", "v"));
    }

    /// <summary>The members <see cref="ISegmentIndex"/> requires, and none of the optional ones.</summary>
    private sealed class ListsOnly(SegmentInvertedIndex inner) : ISegmentIndex
    {
        public uint[]? Lookup(string propertyName, object? value) => inner.Lookup(propertyName, value);
        public bool MightContain(string propertyName, object? value) => inner.MightContain(propertyName, value);
        public uint[]? LookupTrigram(ReadOnlySpan<char> text) => null;
        public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates) =>
            inner.LookupIntersect(predicates);
    }

    /// <summary>
    /// Null and 0 are different answers here too: "no information, scan" and "nothing matches,
    /// drop the group". The count may exceed the list and may not fall short of it.
    /// </summary>
    private static void AssertCounts(uint[]? list, int? counted, string what)
    {
        if (list is null) { Assert.True(counted is null, $"a count of {counted} over no information: {what}"); return; }

        Assert.True(counted is not null, $"no count over a list of {list.Length}: {what}");
        Assert.True(counted >= list.Length, $"a count of {counted} under a list of {list.Length}: {what}");
        // Zero is proof, and the only count an empty list has.
        Assert.True((counted == 0) == (list.Length == 0), $"a count of {counted} over a list of {list.Length}: {what}");
    }

    // ── Trigram ───────────────────────────────────────────────────────────────

    private static readonly string[] Texts =
    [
        "Failed to process payment {PaymentId}: gateway timeout after {Timeout} ms",
        "HTTP {Method} {Path} responded {StatusCode} in {Elapsed} ms",
        "System.TimeoutException",
        "Ошибка оплаты: превышено время ожидания",
        "req-3f2a9c01be77",
        "wallet:1234567:balance",
        "aaaaaaaaaaaaaaaa",
        "MiXeD CaSe TeXt",
    ];

    private static readonly string[] Searches =
    [
        "timeout", "TIMEOUT", "Timeout after", "payment", "gateway timeout after", "responded",
        "ошибка", "ОПЛАТЫ", "время", "req-3f2a", "3F2A9C", "wallet:", ":balance", "aaaa", "aaa",
        "mixed case", "absent", "zzz", "tim", "ti", "", "ms", "xyzzy-not-there",
        new string('a', 1500), "a" + new string('b', 1100),
    ];

    private static SegmentTrigramIndex BuildTrigrams()
    {
        var t = new SegmentTrigramIndex();
        for (uint o = 0; o < 64; o++)
            t.Add(o, Texts[o % Texts.Length]);
        t.Add(64, new string('a', 1500));
        return t;
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void EveryTrigramSearch_GetsTheDecodedAnswer_FromTheSectionAndFromTheMemo(string layout)
    {
        var built = BuildTrigrams();
        byte[] section = layout == "codec" ? built.Serialise() : built.SerialiseRoaringV1();
        var oracle = SegmentTrigramIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load([], section, []);

        for (int pass = 0; pass < 2; pass++)
            foreach (var s in Searches)
                Assert.True(Same(oracle.Lookup(s), lazy.LookupTrigram(s)), $"pass {pass}, '{Show(s)}'");
    }

    [Fact]
    public void AnEmptyTrigramSection_IsNoInformation_NotNoMatches()
    {
        using var lazy = SegmentIndexReader.Load([], [], []);
        Assert.Null(lazy.LookupTrigram("timeout"));
        Assert.Equal(SegmentTrigramIndex.Deserialise([]).Lookup("timeout"), lazy.LookupTrigram("timeout"));
    }

    /// <summary>
    /// The single-byte-key layouts (V1 codec, and the older Roaring one) could repeat a key —
    /// non-ASCII truncated onto '?' — and reading them assigned into a dictionary, so the LAST
    /// bucket won. A lazy reader that stopped at the first would answer differently.
    /// </summary>
    [Fact]
    public void ARepeatedLegacyKey_LastBucketWins()
    {
        // V1 codec: magic, count, then (3 key bytes, u32 length, varint postings) per bucket.
        var blob = new List<byte>();
        void U32(uint v) { var b = new byte[4]; BinaryPrimitives.WriteUInt32LittleEndian(b, v); blob.AddRange(b); }
        void Bucket(string key, int[] offsets)
        {
            blob.AddRange(Encoding.ASCII.GetBytes(key));
            var enc = new byte[64];
            int n = SegmentBitmapCodec.Encode(offsets, enc);
            U32((uint)n);
            blob.AddRange(enc.AsSpan(0, n).ToArray());
        }
        U32(0xFFFFFFFFu);
        U32(3);
        Bucket("abc", [1, 2, 3]);
        Bucket("?bc", [4]);
        Bucket("abc", [7, 9]);
        byte[] section = [.. blob];

        var oracle = SegmentTrigramIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load([], section, []);

        Assert.Equal([7u, 9u], oracle.Lookup("abc")!);
        Assert.Equal(oracle.Lookup("abc"), lazy.LookupTrigram("abc"));
        Assert.Equal(oracle.Lookup("?bc"), lazy.LookupTrigram("?bc"));
        Assert.Equal(oracle.Lookup("пbc"), lazy.LookupTrigram("пbc"));   // truncated on write: no match
    }

    [Fact]
    public void RandomTexts_AnswerRandomSearches_LikeTheDecodedOne()
    {
        var rng = new Random(80);
        const string Alphabet = "abcAB-: Ωж1";
        for (int round = 0; round < 30; round++)
        {
            var t = new SegmentTrigramIndex();
            int events = rng.Next(1, 200);
            for (uint o = 0; o < events; o++) t.Add(o, RandomText(rng, Alphabet, rng.Next(0, 30)));
            byte[] section = rng.Next(4) == 0 ? t.SerialiseRoaringV1() : t.Serialise();
            var oracle = SegmentTrigramIndex.Deserialise(section);
            using var lazy = SegmentIndexReader.Load([], section, []);

            for (int q = 0; q < 50; q++)
            {
                string s = RandomText(rng, Alphabet, rng.Next(0, 8));
                Assert.True(Same(oracle.Lookup(s), lazy.LookupTrigram(s)), $"round {round}, '{Show(s)}'");
            }
        }
    }

    private static string RandomText(Random rng, string alphabet, int length)
    {
        var chars = new char[length];
        for (int i = 0; i < length; i++) chars[i] = alphabet[rng.Next(alphabet.Length)];
        return new string(chars);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Null and empty are different answers ("scan" and "nothing matches"), so they
    /// must match exactly — not merely both be falsy.</summary>
    private static bool Same(uint[]? a, uint[]? b) =>
        a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);

    private static string Show(object? v) => v switch
    {
        null     => "null",
        string s => s.Length > 24 ? $"\"{s[..10]}…\"({s.Length})" : "\"" + s.Replace("\u0001", "\\u1").Replace("\u0002", "\\u2") + "\"",
        _        => $"{v} ({v.GetType().Name})",
    };
}
