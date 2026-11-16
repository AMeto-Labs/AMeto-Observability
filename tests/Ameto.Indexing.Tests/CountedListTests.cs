using Ameto.Core;
using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// A posting list is COUNTED before it is built, by one walk of its property's entries that
/// decodes the list there and then only when it is short
/// (<see cref="SegmentInvertedIndex.ProbeBucket"/>, <see cref="SegmentIndexReader.ShortListPostings"/>).
///
/// <para>The query's narrowing asks every equality how many rows its list holds before it names
/// one, and what it does next depends on the answer: a long list it may never ask for at all (it
/// names nearly the whole group, or the page ends before the scan reaches the group). So the
/// count must not cost what the list costs — and for the lists a selective lookup is made of,
/// which it always goes on to ask for, counting must not cost a SECOND walk of a property that
/// has an entry per row. Pinned here: what the walk hands back at each length, that asking
/// whether a value is there decodes nothing long, that a short list counted is a list already
/// had, that a long one counted is later decoded from its own bytes — and, throughout, that
/// every answer is still the one the decoded reference gives, whichever question came first.</para>
/// </summary>
public sealed class CountedListTests
{
    private const int Rows = 3_000;

    /// <summary>
    /// One group of <see cref="Rows"/> rows with a list of every kind: two long ones that split
    /// the group (<c>Lane</c>), a long one that is two entries differing only in case
    /// (<c>Case</c> = Loud / LOUD), a short one beside one that is nearly the group
    /// (<c>Few</c>), and one row to a value (<c>Id</c>).
    /// </summary>
    private static SegmentInvertedIndex Build()
    {
        var b = new SegmentInvertedIndex();
        for (uint o = 0; o < Rows; o++)
        {
            b.Add(o, "Id",   $"id-{o:D5}");
            b.Add(o, "Lane", o % 2 == 0 ? "even" : "odd");
            b.Add(o, "Case", (o % 3) switch { 0 => "Loud", 1 => "LOUD", _ => "quiet" });
            b.Add(o, "Few",  o % 40 == 0 ? "rare" : "common");
        }
        return b;
    }

    private static readonly (string Property, object? Value)[] Questions =
    [
        ("Lane", "even"), ("Lane", "ODD"), ("Lane", "nobody"),
        ("Case", "loud"), ("Case", "quiet"),
        ("Few", "rare"), ("Few", "common"),
        ("Id", "id-00017"), ("Id", "id-99999"),
        ("NoSuchProperty", "even"),
    ];

    public static TheoryData<string> Layouts => new() { "codec", "roaring" };

    private static byte[] Serialise(SegmentInvertedIndex b, string layout) =>
        layout == "codec" ? b.Serialise() : b.SerialiseRoaringV1();

    // ── The walk ──────────────────────────────────────────────────────────────

    /// <summary>
    /// What one walk hands back, by the list's length against the limit: the count always; the
    /// list when it is no longer than the limit; and otherwise the place its bytes lie, from
    /// which it decodes to exactly what a scan for it gives.
    /// </summary>
    [Fact]
    public void TheWalkCountsEveryList_DecodesAShortOne_AndSaysWhereALongOneLies()
    {
        byte[] section = Build().Serialise();
        var catalog = SegmentInvertedIndex.ReadCatalog(section);
        Assert.True(catalog.Codec);
        var lane = catalog.Properties["Lane"].Runs;

        // Long: counted, not decoded, located.
        int count = SegmentInvertedIndex.ProbeBucket(section, true, lane, "even", 256, out int[]? postings, out var where);
        Assert.Equal(Rows / 2, count);
        Assert.Null(postings);
        Assert.True(where.IsOne);
        Assert.Equal(SegmentInvertedIndex.ScanBucket(section, true, lane, "even"),
                     SegmentInvertedIndex.DecodeAt(section.AsSpan(where.Offset, where.Length)));

        // The limit is inclusive: a list of exactly that many rows is decoded, one more is not.
        Assert.Equal(Rows / 2, SegmentInvertedIndex.ProbeBucket(section, true, lane, "even", Rows / 2, out postings, out where));
        Assert.NotNull(postings);
        Assert.Equal(Rows / 2, postings.Length);
        Assert.False(where.IsOne);
        Assert.Equal(Rows / 2, SegmentInvertedIndex.ProbeBucket(section, true, lane, "even", Rows / 2 - 1, out postings, out _));
        Assert.Null(postings);

        // Short: the list comes with the count.
        var few = catalog.Properties["Few"].Runs;
        Assert.Equal(Rows / 40, SegmentInvertedIndex.ProbeBucket(section, true, few, "rare", 256, out postings, out where));
        Assert.Equal([.. Enumerable.Range(0, Rows).Where(static o => o % 40 == 0)], postings);
        Assert.False(where.IsOne);

        // No such value: −1, which is not the 0 of a value that lists no row.
        Assert.Equal(-1, SegmentInvertedIndex.ProbeBucket(section, true, lane, "nobody", 256, out postings, out where));
        Assert.Null(postings);
        Assert.False(where.IsOne);

        // The two ends of the walk: a count that decodes nothing, a scan that decodes whatever is there.
        Assert.Equal(1, SegmentInvertedIndex.CountBucket(section, true, catalog.Properties["Id"].Runs, "id-00017"));
        Assert.Equal(Rows / 2, SegmentInvertedIndex.ScanBucket(section, true, lane, "odd")!.Length);
    }

    /// <summary>
    /// A value filed under two spellings that differ only in case is two entries and one bucket:
    /// the count sums them, a short one is their union, and a long one has no single place to be
    /// decoded from — the caller walks again for it, which is the price of a shape no ordinary
    /// property has.
    /// </summary>
    [Fact]
    public void ABucketOfSeveralEntries_IsCountedAcrossThem_AndIsNotLocated()
    {
        byte[] section = Build().Serialise();
        var runs = SegmentInvertedIndex.ReadCatalog(section).Properties["Case"].Runs;
        int both = Rows - Rows / 3;                                   // Loud and LOUD together

        Assert.Equal(both, SegmentInvertedIndex.ProbeBucket(section, true, runs, "loud", 256, out int[]? postings, out var where));
        Assert.Null(postings);
        Assert.False(where.IsOne);

        Assert.Equal(both, SegmentInvertedIndex.ProbeBucket(section, true, runs, "loud", both, out postings, out _));
        Assert.Equal([.. Enumerable.Range(0, Rows).Where(static o => o % 3 != 2)], postings);
    }

    /// <summary>
    /// A legacy section's bitmaps carry no leading count, so each is decoded to be counted — and
    /// what was decoded is handed back whatever the limit, there being nothing left to save.
    /// </summary>
    [Fact]
    public void ALegacyListIsDecodedToBeCounted_AndHandedBack()
    {
        byte[] section = Build().SerialiseRoaringV1();
        var catalog = SegmentInvertedIndex.ReadCatalog(section);
        Assert.False(catalog.Codec);

        int count = SegmentInvertedIndex.ProbeBucket(section, false, catalog.Properties["Lane"].Runs, "even", 0, out int[]? postings, out var where);
        Assert.Equal(Rows / 2, count);
        Assert.NotNull(postings);
        Assert.Equal(Rows / 2, postings.Length);
        Assert.False(where.IsOne);
    }

    // ── The memo ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Whether a value is THERE is not a question about its rows. <c>MightContain</c> is the
    /// first thing a narrowing asks of a group, and it used to take the posting list to see
    /// whether there was one — decoding, and keeping, the list of a value most of the group
    /// carries, in every group asked.
    /// </summary>
    [Fact]
    public void AskingWhetherAValueIsThere_DecodesNoLongList_AndKeepsNone()
    {
        byte[] section = Build().Serialise();
        using var lazy = SegmentIndexReader.Load(section, [], []);
        Assert.Null(lazy.CountAtMost("NoSuchProperty", "x"));          // the catalog is read here, once
        long kept = lazy.ApproxManagedBytes;

        Assert.True(lazy.MightContain("Few", "common"));
        Assert.True(lazy.MightContain("Lane", "even"));
        Assert.False(lazy.MightContain("Lane", "nobody"));

        // Decoded, the two lists are 4 bytes a row: ~17.7 KB. What is kept is two counts, an
        // absence and the tables they are remembered in.
        Assert.True(lazy.ApproxManagedBytes - kept < 2048,
            $"asking whether two values are there left {lazy.ApproxManagedBytes - kept} B in the memo");

        // The lists are still there for a caller who wants them.
        Assert.Equal(Rows / 2, lazy.LookupIntersect([("Lane", "even")])!.Length);
    }

    /// <summary>
    /// The selective lookup's path: it counts, then asks for the list. Counting a short list
    /// leaves the list in the memo, so the asking that follows reads nothing of the section —
    /// one walk of a property that has an entry per row, where a count kept as a bare number
    /// would make it two.
    /// </summary>
    [Fact]
    public void AShortListCounted_IsAListAlreadyHad_SoTheLookupThatFollowsReadsNothing()
    {
        byte[] section = Build().Serialise();
        using var cache = new SegmentIndexCache(1 << 20);
        (string, object?)[] one = [("Id", "id-00017")];

        using (var first = SegmentIndexView.OverSections(cache, "a.seg", 0, section, default, default))
        {
            Assert.Equal(1, first.CountAtMost("Id", "id-00017"));
            Assert.True(first.ReadSections);
        }
        using (var second = SegmentIndexView.OverSections(cache, "a.seg", 0, section, default, default))
        {
            Assert.Equal([17u], second.LookupIntersect(one)!);
            Assert.True(second.TryLookupIntersect(one, out var lent) && lent.Length == 1);
            Assert.Equal(1, second.CountAtMost("Id", "id-00017"));
            Assert.False(second.ReadSections, "the lookup after the count went back to the section");
        }
        Assert.Equal(1, cache.MissCount);
        Assert.Equal(1, cache.HitCount);

        // A LONG list counted is only a count: its list is read when it is asked for, once.
        (string, object?)[] many = [("Lane", "even")];
        using (var counting = SegmentIndexView.OverSections(cache, "a.seg", 0, section, default, default))
            Assert.Equal(Rows / 2, counting.CountAtMost("Lane", "even"));
        using (var listing = SegmentIndexView.OverSections(cache, "a.seg", 0, section, default, default))
        {
            Assert.Equal(Rows / 2, listing.LookupIntersect(many)!.Length);
            Assert.True(listing.ReadSections);
        }
        using (var again = SegmentIndexView.OverSections(cache, "a.seg", 0, section, default, default))
        {
            Assert.Equal(Rows / 2, again.LookupIntersect(many)!.Length);
            Assert.Equal(Rows / 2, again.CountAtMost("Lane", "even"));
            Assert.False(again.ReadSections);
        }
    }

    /// <summary>
    /// Whatever was asked first — the count, whether the value is there, or the list — every
    /// answer is the decoded reference's, in both layouts, for lists short and long, single and
    /// several-entried; and the count never falls short of the list it stands for.
    /// </summary>
    [Theory]
    [MemberData(nameof(Layouts))]
    public void WhicheverQuestionComesFirst_EveryAnswerIsTheDecodedOne(string layout)
    {
        byte[] section = Serialise(Build(), layout);
        var oracle = SegmentInvertedIndex.Deserialise(section);

        foreach (int firstQuestion in (int[])[0, 1, 2, 3])
        {
            using var lazy = SegmentIndexReader.Load(section, [], []);
            for (int pass = 0; pass < 2; pass++)
            foreach (var (p, v) in Questions)
            {
                string what = $"{layout}, first question {firstQuestion}, pass {pass}, {p} = {v}";
                (string, object?)[] q = [(p, v)];

                switch (firstQuestion)
                {
                    case 0: lazy.CountAtMost(p, v); break;
                    case 1: lazy.MightContain(p, v); break;
                    case 2: lazy.LookupIntersect(q); break;
                    default: lazy.TryLookupIntersect(q, out _); break;
                }

                uint[]? list = oracle.LookupIntersect(q);
                Assert.True(Same(list, lazy.LookupIntersect(q)), "list: " + what);
                Assert.True(Same(list, lazy.Lookup(p, v)) || list is { Length: 0 }, "lookup: " + what);
                Assert.True(oracle.MightContain(p, v) == lazy.MightContain(p, v), "MightContain: " + what);

                bool answered = lazy.TryLookupIntersect(q, out var lent);
                Assert.True(answered == (list is not null), "an answer at all: " + what);
                if (list is not null) Assert.True(lent.Span.SequenceEqual(list), "lent: " + what);

                int? counted = lazy.CountAtMost(p, v);
                Assert.True((counted is null) == (list is null), "a count at all: " + what);
                if (list is not null)
                {
                    Assert.True(counted >= list.Length, $"counted {counted} under a list of {list.Length}: " + what);
                    Assert.True((counted == 0) == (list.Length == 0), "none counted and none listed: " + what);
                }
            }

            // And together: two long lists, a long and a short one, a short one and no information.
            (string, object?)[][] together =
            [
                [("Lane", "even"), ("Case", "loud")],
                [("Lane", "odd"), ("Id", "id-00017")],
                [("Lane", "even"), ("Id", "id-00017")],
                [("Few", "rare"), ("NoSuchProperty", "x")],
                [("Lane", "even"), ("Lane", "odd")],
            ];
            foreach (var q in together)
            {
                uint[]? list = oracle.LookupIntersect(q);
                Assert.True(Same(list, lazy.LookupIntersect(q)), $"{layout}: {q[0].Item1} with {q[1].Item1}");
                Assert.True(lazy.TryLookupIntersect(q, out var lent) && lent.Span.SequenceEqual(list));
            }
        }
    }

    // ── What is lent ──────────────────────────────────────────────────────────

    /// <summary>
    /// <c>TryLookupIntersect</c> lends the list the memo keeps — the same memory to every caller,
    /// with no copy — while <c>LookupIntersect</c> still hands each caller an array of its own.
    /// So a caller that writes into its own array changes nobody's answer, and a caller that was
    /// lent the memo's cannot write at all.
    /// </summary>
    [Fact]
    public void TheListIsLentUncopied_AndAnArrayIsStillTheCallersOwn()
    {
        byte[] section = Build().Serialise();
        using var lazy = SegmentIndexReader.Load(section, [], []);
        (string, object?)[] q = [("Lane", "even")];

        Assert.True(lazy.TryLookupIntersect(q, out var first));
        Assert.True(lazy.TryLookupIntersect(q, out var again));
        Assert.Equal(Rows / 2, first.Length);
        Assert.True(first.Span == again.Span, "the list was copied on its way out of the memo");

        uint[] mine = lazy.LookupIntersect(q)!;
        Assert.False((ReadOnlySpan<uint>)mine == first.Span);
        Array.Fill(mine, 7u);
        Assert.True(lazy.TryLookupIntersect(q, out var after));
        Assert.Equal([.. Enumerable.Range(0, Rows).Where(static o => o % 2 == 0).Select(static o => (uint)o)], after.ToArray());

        // An intersection is worked out afresh each time, and is its own array either way.
        (string, object?)[] two = [("Lane", "even"), ("Few", "rare")];
        Assert.True(lazy.TryLookupIntersect(two, out var a));
        Assert.True(lazy.TryLookupIntersect(two, out var b));
        Assert.False(a.Span == b.Span);
        Assert.True(a.Span.SequenceEqual(b.Span));
        Assert.Equal(Rows / 40, a.Length);
    }

    /// <summary>
    /// An index that knows no better answers the read-only question from its list: the
    /// interface's default, with the same three answers.
    /// </summary>
    [Fact]
    public void AnIndexWithNothingToLend_AnswersFromItsList()
    {
        ISegmentIndex index = new ListsOnly(SegmentInvertedIndex.Deserialise(Build().Serialise()));

        Assert.True(index.TryLookupIntersect([("Few", "rare")], out var rows));
        Assert.Equal(Rows / 40, rows.Length);
        Assert.True(index.TryLookupIntersect([("Few", "nobody")], out rows));
        Assert.True(rows.IsEmpty);
        Assert.False(index.TryLookupIntersect([("NoSuchProperty", "x")], out rows));
        Assert.True(rows.IsEmpty);
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

    private static bool Same(uint[]? a, uint[]? b) =>
        a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);
}
