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

        byte[] section = Build(payloads);
        uint[] marked  = [.. expected];

        foreach (var index in Readers(section))
        {
            uint[]? listed = index.Lookup(Present.Property, Present.Value);
            Assert.NotNull(listed);
            Assert.Equal(marked, listed);
        }
    }

    [Fact]
    public void EveryGroupCarriesTheBuildersWord_AsABucketWithNoValue()
    {
        var catalog = SegmentInvertedIndex.ReadCatalog(Build([[], Readable]));

        Assert.True(catalog.VouchesForExceptionMarker);
        var word = catalog.Properties[ClefFields.ExceptionExistsComplete];
        Assert.All(word.Runs, static run => Assert.Equal(0, run.ValueCount));

        // The marker itself is filed under the name the readers ask for.
        Assert.True(catalog.Properties.ContainsKey(ClefFields.ExceptionExists));

        // And the decoded reader sees the same empty bucket: known, and holding nothing.
        AssertRows([], SegmentInvertedIndex.Deserialise(Build([[], Readable]))
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
        byte[] section = Build([[], [0xC0], []]);
        Assert.False(SegmentInvertedIndex.ReadCatalog(section).Properties.ContainsKey(ClefFields.ExceptionExists));

        foreach (var index in Readers(section))
        {
            AssertRows([], index.LookupIntersect([Present]));
            Assert.False(index.MightContain(Present.Property, Present.Value));
            // Proof from one conjunct is proof for the conjunction.
            AssertRows([], index.LookupIntersect([("@l", "Error"), Present]));
        }
    }

    [Fact]
    public void AVouchedGroup_NarrowsToItsMarkedRows()
    {
        byte[] section = Build([Readable, [], NonStringType, [], Readable]);

        foreach (var index in Readers(section))
        {
            AssertRows([0u, 2u, 4u], index.LookupIntersect([Present]));
            Assert.True(index.MightContain(Present.Property, Present.Value));
        }
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
        byte[] section = older.Serialise();
        Assert.False(SegmentInvertedIndex.ReadCatalog(section).VouchesForExceptionMarker);

        foreach (var index in Readers(section))
        {
            Assert.Null(index.Lookup(Present.Property, Present.Value));
            Assert.Null(index.LookupIntersect([Present]));
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
        byte[] section = Build([Readable, []], props);

        var word = SegmentInvertedIndex.ReadCatalog(section).Properties[ClefFields.ExceptionExistsComplete];
        Assert.Contains(word.Runs, static run => run.ValueCount > 0);
        Assert.False(SegmentInvertedIndex.ReadCatalog(section).VouchesForExceptionMarker);

        foreach (var index in Readers(section))
        {
            Assert.Null(index.LookupIntersect([Present]));
            Assert.True(index.MightContain(Present.Property, Present.Value));
        }
    }

    /// <summary>
    /// The packed reader and the decoded one are two implementations of one rule, and the marker
    /// gave that rule two cases no other key has. Every question, over every kind of group,
    /// asked of both — and twice of the packed one, whose second answer comes from its memo.
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
        }));

        byte[][] sections =
        [
            Build([Readable, [], NonStringType]),     // vouched, marked rows
            Build([[], [0xC0]]),                      // vouched, no exception
            olderWithMarker.Serialise(),              // an older group
            olderWithMarker.SerialiseRoaringV1(),     // …and an older layout
            wordWithValue,                            // the word's name, taken by a client
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

                foreach (var other in questions)
                    Assert.True(Same(oracle.LookupIntersect([q, other]), lazy.LookupIntersect([q, other])),
                        $"LookupIntersect: {what} and {other.Item1} = {other.Item2 ?? "null"}");
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>The inverted section of one group holding a row per payload, at Error level.</summary>
    private static byte[] Build(byte[][] exceptions, byte[]? props = null)
    {
        props ??= EmptyProps;
        using var builder = new SegmentIndexBuilder(Math.Max(1, exceptions.Length));
        for (int i = 0; i < exceptions.Length; i++)
        {
            var ev = new SegmentEventRef(new EventId(0u, (uint)i).RawValue, Ticks + i, LogLevel.Error,
                                         0, 0, 0, "settlement failed", "Billing", props, exceptions[i]);
            builder.Add((uint)i, in ev);
        }
        return builder.SerialisedInvertedIndex;
    }

    /// <summary>Both readers of one section; the packed one disposed with the enumeration.</summary>
    private static IEnumerable<ISegmentIndex> Readers(byte[] section)
    {
        yield return SegmentInvertedIndex.Deserialise(section);
        using var lazy = SegmentIndexReader.Load(section, [], []);
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
