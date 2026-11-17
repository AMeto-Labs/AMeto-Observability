using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Ameto.Testing;
using MessagePack;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The <c>@x.exists</c> bucket marks "this row carries an exception", and a search for
/// exceptions — <c>has(@x)</c>, <c>@x.exists = true</c> — is narrowed by it. That is only safe
/// if the bucket names EXACTLY the rows the scan would match, and it did not: the marker was
/// filed after the exception map had been read, so a row whose map the builder could not read
/// was left out, while <c>LogEvent.HasException</c> — the payload's lead byte — still counts it.
///
/// <para>Three things make it safe, and each is pinned here. The builder files the marker on
/// <see cref="ExceptionInfo.IsPresent"/> alone, the probe the scan uses, over the bytes the
/// writer stores. It leaves its word that it did so in every group it seals: an inverted bucket
/// named <see cref="ClefFields.ExceptionExistsComplete"/> with NO value in it, which is
/// something only a builder writes. And both readers of a section let the marker answer only
/// where they find that word — there, a missing marker is proof that the group holds no
/// exception; anywhere else the key is "no information" and the group is scanned, as every group
/// written before this was and still must be.</para>
/// </summary>
public sealed class ExceptionMarkerTests
{
    private static readonly byte[] Readable =
        new ExceptionInfo { Type = "System.TimeoutException", Message = "ledger did not answer" }.ToBytes();

    /// <summary>A map whose <c>type</c> is not a string: present to the scan, unreadable to the index.</summary>
    private static readonly byte[] NonStringType =
        ExceptionShapes.Pack(static (ref MessagePackWriter w) => { w.WriteMapHeader(1); w.Write("type"); w.Write(42); });

    private static readonly (string Property, object? Value) Present = (ClefFields.ExceptionExists, "true");

    // ── What the builder files ────────────────────────────────────────────────

    /// <summary>
    /// Every shape a client sends, the shapes that decode to no exception, and the ones no
    /// decoder reads at all — a truncated map, a non-string type, counts and lengths that lie.
    /// The marker's posting list must be the rows <see cref="LogEvent.HasException"/> is true
    /// for, no more and no fewer, because that property is what the scan answers
    /// <c>has(@x)</c> from.
    /// </summary>
    [Fact]
    public void TheMarkerNamesEveryRowTheScanCountsAsHavingAnException_AndNoOther()
    {
        byte[][] payloads =
        [
            .. ExceptionShapes.Payloads.Values,
            [],                                                   // no exception column at all
            Readable.AsSpan(0, Readable.Length - 4).ToArray(),    // a map cut short
            NonStringType,
            [0xDF, 0x80, 0x00, 0x00, 0x00],                       // map32 announcing 2^31 entries
            [0x81, 0xA3, (byte)'s', (byte)'t', (byte)'k', 0xDD, 0x80, 0x00, 0x00, 0x00],
            [0xDB, 0x80, 0x00, 0x00, 0x00],                       // legacy str32 claiming 2^31 bytes
            [0xD9, 0x05, 0x61],                                   // str8 of five bytes carrying one
            [0xD9],                                               // string headers too short to hold
            [0xDA, 0x00],                                         // their own length: absent
            [0xDB, 0x00, 0x00, 0x00],
            [0xCC],
        ];

        var expected   = new List<uint>();
        int unreadable = 0;
        for (int i = 0; i < payloads.Length; i++)
        {
            bool scanCountsIt = EventWith(payloads[i]).HasException;
            Assert.Equal(ExceptionInfo.IsPresent(payloads[i]), scanCountsIt);
            if (!scanCountsIt) continue;

            expected.Add((uint)i);
            if (!ExceptionInfo.TryReadIndexFields(payloads[i], out _, out _, out _)) unreadable++;
        }
        // The rows the marker used to leave out. Without some, this test would prove nothing new.
        Assert.True(unreadable >= 4, $"only {unreadable} present-but-unreadable payload(s) in the set");

        Group  group  = Build(payloads);
        uint[] marked = [.. expected];

        foreach (var index in Readers(group))
        {
            uint[]? listed = index.Lookup(Present.Property, Present.Value);
            Assert.NotNull(listed);
            Assert.Equal(marked, listed);
            Assert.Equal(marked.Length, index.CountAtMost(Present.Property, Present.Value));
        }
    }

    [Fact]
    public void EveryGroupCarriesTheBuildersWord_AsABucketWithNoValue()
    {
        byte[] section = Build([[], Readable]).Inverted;
        var catalog = SegmentInvertedIndex.ReadCatalog(section);

        Assert.True(catalog.VouchesForExceptionMarker);
        var word = catalog.Properties[ClefFields.ExceptionExistsComplete];
        Assert.All(word.Runs, static run => Assert.Equal(0, run.ValueCount));

        // The marker itself is filed under the name the readers ask for.
        Assert.True(catalog.Properties.ContainsKey(ClefFields.ExceptionExists));

        // And the decoded reader sees the same empty bucket: known, and holding nothing.
        AssertRows([], SegmentInvertedIndex.Deserialise(section)
                                           .LookupIntersect([(ClefFields.ExceptionExistsComplete, "anything")]));
    }

    [Fact]
    public void ABuilderThatSawNoEventVouchesForNothing()
    {
        using var builder = new SegmentIndexBuilder(4);
        var catalog = SegmentInvertedIndex.ReadCatalog(builder.SerialisedInvertedIndex);

        Assert.Empty(catalog.Properties);
        Assert.False(catalog.VouchesForExceptionMarker);
    }

    // ── What the readers make of it ───────────────────────────────────────────

    /// <summary>
    /// In a vouched group the marker bucket exists only once a row is filed in it, so a group
    /// with no exception has none. For any other key a missing bucket means "never indexed here,
    /// scan". Here it is proof, and the group is dropped unread — which is most of what the
    /// marker is worth to a search for exceptions.
    /// </summary>
    [Fact]
    public void AVouchedGroupWithNoException_ProvesThereIsNone()
    {
        Group group = Build([[], [0xC0], []]);
        Assert.False(SegmentInvertedIndex.ReadCatalog(group.Inverted).Properties.ContainsKey(ClefFields.ExceptionExists));

        foreach (var index in Readers(group))
        {
            AssertRows([], index.LookupIntersect([Present]));
            Assert.Equal(0, index.CountAtMost(Present.Property, Present.Value));
            Assert.False(index.MightContain(Present.Property, Present.Value));
            // Proof from one conjunct is proof for the conjunction.
            AssertRows([], index.LookupIntersect([("@l", "Error"), Present]));
        }
    }

    [Fact]
    public void AVouchedGroup_NarrowsToItsMarkedRows()
    {
        Group group = Build([Readable, [], NonStringType, [], Readable]);

        foreach (var index in Readers(group))
        {
            AssertRows([0u, 2u, 4u], index.LookupIntersect([Present]));
            Assert.Equal(3, index.CountAtMost(Present.Property, Present.Value));
            Assert.True(index.MightContain(Present.Property, Present.Value));
        }
    }

    /// <summary>
    /// The bloom is handed the marker's NAME — its bucket exists — and never the value filed in
    /// it, so asked about <c>"true"</c> it says no for a group whose marker lists rows. The
    /// packed reader gates <c>MightContain</c> on the bloom for every other key, and used to for
    /// this one: it answered "definitely not here" about a group with exceptions, where the
    /// decoded reader, which has no bloom to ask, answered "might". Nothing in the query path
    /// sends the marker through <c>MightContain</c> today; the two must not disagree on the day
    /// something does.
    /// </summary>
    [Fact]
    public void TheBloomHoldsTheMarkersName_NotItsValue_AndIsNotAskedAboutTheMarker()
    {
        Group group = Build([Readable, [], NonStringType]);

        // The premise, or this proves nothing: asked for the value, this group's bloom says no.
        using (var bloom = SegmentBloomFilter.Deserialise(group.Bloom))
        {
            Assert.True(bloom.MightContain(ClefFields.ExceptionExists));
            Assert.False(SegmentIndexReader.MightContainValue(bloom, Present.Value));
        }

        var decoded = SegmentInvertedIndex.Deserialise(group.Inverted);
        using var packed = SegmentIndexReader.Load(group.Inverted, group.Trigram, group.Bloom);
        using var cache  = new SegmentIndexCache(1 << 20);

        foreach (object? value in (object?[])["true", true, "TRUE", "false", false, null])
        {
            bool expected = decoded.MightContain(ClefFields.ExceptionExists, value);
            Assert.True(expected == packed.MightContain(ClefFields.ExceptionExists, value), $"packed, {value ?? "null"}");

            // And through a view over a cached memo, which is how a query asks: from the
            // sections the first time, from the memo the second.
            for (int ask = 0; ask < 2; ask++)
            {
                using var view = SegmentIndexView.OverSections(cache, "errors.seg", 0, group.Inverted, group.Trigram, group.Bloom);
                Assert.True(expected == view.MightContain(ClefFields.ExceptionExists, value), $"view, ask {ask}, {value ?? "null"}");
            }
        }
        Assert.True(packed.MightContain(Present.Property, Present.Value));
    }

    /// <summary>
    /// The count is what the narrowing asks first, in every group of every segment a search for
    /// exceptions touches. It is read off the section once — the marker's list opens with its own
    /// length — and off the cached memo after that, without the section; the list is not decoded
    /// by either.
    /// </summary>
    [Fact]
    public void TheMarkersCount_IsReadOffTheSectionOnce_AndOffTheCachedMemoAfter()
    {
        Group group = Build([Readable, Readable, [], NonStringType]);
        using var cache = new SegmentIndexCache(1 << 20);

        using (var first = SegmentIndexView.OverSections(cache, "errors.seg", 0, group.Inverted, group.Trigram, group.Bloom))
        {
            Assert.Equal(3, first.CountAtMost(Present.Property, Present.Value));
            Assert.True(first.ReadSections);
        }
        long learned = cache.TotalBytes;

        using (var again = SegmentIndexView.OverSections(cache, "errors.seg", 0, group.Inverted, group.Trigram, group.Bloom))
        {
            Assert.Equal(3, again.CountAtMost(Present.Property, Present.Value));
            Assert.False(again.ReadSections);
        }
        Assert.Equal((1L, 1L), (cache.HitCount, cache.MissCount));
        Assert.Equal(learned, cache.TotalBytes);                 // the second asking taught it nothing

        // A caller that goes on to want the rows gets them, from the same memo.
        using var listing = SegmentIndexView.OverSections(cache, "errors.seg", 0, group.Inverted, group.Trigram, group.Bloom);
        AssertRows([0u, 1u, 3u], listing.LookupIntersect([Present]));
        Assert.Equal(3, listing.CountAtMost(Present.Property, Present.Value));
    }

    /// <summary>
    /// A group as every build before this one wrote it: a marker, and no word. Its list leaves
    /// out the rows whose map could not be read, so it must not narrow and its absence must not
    /// prove anything — the key is unknown here, whatever the bucket holds.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AGroupWithoutTheWord_IsNoInformation_WhateverItsMarkerHolds(bool markerPresent)
    {
        var older = new SegmentInvertedIndex();
        older.Add(0, "@l", "Error");
        older.Add(1, "@l", "Error");
        older.Add(2, "@l", "Error");
        if (markerPresent) older.Add(0, ClefFields.ExceptionExists, "true");   // row 2's map was unreadable
        Group group = Older(older.Serialise(), markerPresent ? ["Error", ClefFields.ExceptionExists] : ["Error"]);
        Assert.False(SegmentInvertedIndex.ReadCatalog(group.Inverted).VouchesForExceptionMarker);

        foreach (var index in Readers(group))
        {
            Assert.Null(index.Lookup(Present.Property, Present.Value));
            Assert.Null(index.LookupIntersect([Present]));
            Assert.Null(index.CountAtMost(Present.Property, Present.Value));
            Assert.True(index.MightContain(Present.Property, Present.Value));

            // It does not veto a conjunct the group CAN answer, and adds nothing to it.
            AssertRows([0u, 1u, 2u], index.LookupIntersect([("@l", "Error"), Present]));
        }
    }

    /// <summary>
    /// The word is the bucket's EMPTINESS, not its name. A client that sends a property called
    /// <c>@x.exists.complete</c> files a value into that very bucket, and a bucket with a value in
    /// it is what any property looks like — in this group and in one written years ago. So it
    /// vouches for nothing and the group is scanned: the safe way to be wrong.
    /// </summary>
    [Fact]
    public void APropertyOfTheWordsName_WithAValueInIt_VouchesForNothing()
    {
        byte[] props = ExceptionShapes.Pack(static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(1);
            w.Write(ClefFields.ExceptionExistsComplete);
            w.Write("a client's own value");
        });
        Group group = Build([Readable, []], props);

        var word = SegmentInvertedIndex.ReadCatalog(group.Inverted).Properties[ClefFields.ExceptionExistsComplete];
        Assert.Contains(word.Runs, static run => run.ValueCount > 0);
        Assert.False(SegmentInvertedIndex.ReadCatalog(group.Inverted).VouchesForExceptionMarker);

        foreach (var index in Readers(group))
        {
            Assert.Null(index.LookupIntersect([Present]));
            Assert.Null(index.CountAtMost(Present.Property, Present.Value));
            Assert.True(index.MightContain(Present.Property, Present.Value));
        }
    }

    /// <summary>
    /// The packed reader and the decoded one are two implementations of one rule, and the marker
    /// gave that rule two cases no other key has. Every question, over every kind of group,
    /// asked of both — and twice of the packed one, whose second answer comes from its memo.
    ///
    /// <para>The packed reader is given NO bloom here, on purpose: a bloom is a second source of
    /// "definitely not" that the decoded reader does not have, so with one the two may differ on
    /// any key but the marker (<see cref="TheBloomHoldsTheMarkersName_NotItsValue_AndIsNotAskedAboutTheMarker"/>
    /// is where the marker meets a real one). Without it, they are the same rule twice.</para>
    /// </summary>
    [Fact]
    public void ThePackedReaderAndTheDecodedOneAgreeAboutTheMarker()
    {
        var olderWithMarker = new SegmentInvertedIndex();
        olderWithMarker.Add(0, "@l", "Error");
        olderWithMarker.Add(0, ClefFields.ExceptionExists, "true");

        byte[] wordWithValue = Build([Readable, []], ExceptionShapes.Pack(static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(1);
            w.Write(ClefFields.ExceptionExistsComplete);
            w.Write(true);
        })).Inverted;

        byte[][] sections =
        [
            Build([Readable, [], NonStringType]).Inverted,   // vouched, marked rows
            Build([[], [0xC0]]).Inverted,                    // vouched, no exception
            olderWithMarker.Serialise(),                     // an older group
            olderWithMarker.SerialiseRoaringV1(),            // …and an older layout
            wordWithValue,                                   // the word's name, taken by a client
        ];
        (string, object?)[] questions =
        [
            Present, (ClefFields.ExceptionExists, true), (ClefFields.ExceptionExists, "TRUE"),
            (ClefFields.ExceptionExists, false), (ClefFields.ExceptionExists, "false"),
            (ClefFields.ExceptionExists, null), (ClefFields.ExceptionExistsComplete, "x"),
            ("@l", "Error"), ("@l", "Fatal"), ("NoSuchProperty", "x"),
        ];

        for (int s = 0; s < sections.Length; s++)
        {
            var oracle = SegmentInvertedIndex.Deserialise(sections[s]);
            using var lazy = SegmentIndexReader.Load(sections[s], [], []);

            for (int pass = 0; pass < 2; pass++)
            foreach (var q in questions)
            {
                string what = $"section {s}, pass {pass}, {q.Item1} = {q.Item2 ?? "null"}";
                Assert.True(Same(oracle.Lookup(q.Item1, q.Item2), lazy.Lookup(q.Item1, q.Item2)), "Lookup: " + what);
                Assert.True(oracle.MightContain(q.Item1, q.Item2) == lazy.MightContain(q.Item1, q.Item2), "MightContain: " + what);
                Assert.True(Same(oracle.LookupIntersect([q]), lazy.LookupIntersect([q])), "LookupIntersect: " + what);
                // The count is the list's answer with a number for the rows, and no row is filed
                // twice in these groups, so the packed count is exact as the decoded one is.
                Assert.True(oracle.LookupIntersect([q])?.Length == oracle.CountAtMost(q.Item1, q.Item2), "CountAtMost, decoded: " + what);
                Assert.True(oracle.LookupIntersect([q])?.Length == lazy.CountAtMost(q.Item1, q.Item2), "CountAtMost, packed: " + what);

                foreach (var other in questions)
                    Assert.True(Same(oracle.LookupIntersect([q, other]), lazy.LookupIntersect([q, other])),
                        $"LookupIntersect: {what} and {other.Item1} = {other.Item2 ?? "null"}");
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>One index group as a reader is given it: the three sections its builder sealed.</summary>
    private readonly record struct Group(byte[] Inverted, byte[] Trigram, byte[] Bloom);

    /// <summary>
    /// One group holding a row per payload, at Error level — ALL of what the builder wrote for
    /// it. The bloom matters: a reader handed an empty one treats it as "no bloom was built" and
    /// never asks it, and the packed reader's answers about the marker were once right only for
    /// as long as nothing did.
    /// </summary>
    private static Group Build(byte[][] exceptions, byte[]? props = null)
    {
        props ??= EmptyProps;
        using var builder = new SegmentIndexBuilder(Math.Max(1, exceptions.Length));
        for (int i = 0; i < exceptions.Length; i++)
        {
            var ev = new SegmentEventRef(new EventId(0u, (uint)i).RawValue, Ticks + i, LogLevel.Error,
                                         0, 0, 0, "settlement failed", "Billing", props, exceptions[i]);
            builder.Add((uint)i, in ev);
        }
        return new Group(builder.SerialisedInvertedIndex, builder.SerialisedTrigramIndex, builder.SerialisedBloomFilter);
    }

    /// <summary>
    /// A group written before the builder vouched for its marker: a hand-made inverted section,
    /// and the bloom such a builder left beside it — the level's text and the NAME of each
    /// bucket it filed, the marker's included.
    /// </summary>
    private static Group Older(byte[] inverted, string[] bloomTerms)
    {
        using var bloom = SegmentBloomFilter.Create(16);
        foreach (string term in bloomTerms) bloom.Add(term);
        return new Group(inverted, [], bloom.Serialise());
    }

    /// <summary>Both readers of one group; the packed one disposed with the enumeration.</summary>
    private static IEnumerable<ISegmentIndex> Readers(Group group)
    {
        yield return SegmentInvertedIndex.Deserialise(group.Inverted);
        using var lazy = SegmentIndexReader.Load(group.Inverted, group.Trigram, group.Bloom);
        yield return lazy;
    }

    /// <summary>An event as a cold row hands it to the evaluator: the column's bytes, undecoded.</summary>
    private static LogEvent EventWith(byte[] exception) => new()
    {
        Id              = new EventId(0u, 1u),
        Timestamp       = new DateTimeOffset(Ticks, TimeSpan.Zero),
        Level           = LogLevel.Error,
        MessageTemplate = "settlement failed",
        RawException    = exception,
    };

    private static readonly long   Ticks      = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero).UtcTicks;
    private static readonly byte[] EmptyProps = [0x80];

    /// <summary>
    /// An answer that names rows — none included. Null is a different answer ("no information,
    /// scan"), and an equality assertion against an empty list would let it pass for "nothing
    /// matches", which is exactly the confusion these tests exist to rule out.
    /// </summary>
    private static void AssertRows(uint[] expected, uint[]? answer)
    {
        Assert.NotNull(answer);
        Assert.Equal(expected, answer);
    }

    /// <summary>Null and empty are different answers ("scan" and "nothing matches").</summary>
    private static bool Same(uint[]? a, uint[]? b) =>
        a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);
}
