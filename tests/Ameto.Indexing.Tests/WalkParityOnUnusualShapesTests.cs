using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The streaming property walk against the reference build, on the four shapes of value it used
/// to file otherwise than the scan reads them (and than the reference build, which reads the
/// properties through <c>LogEventSerializer</c> as the scan does, files them):
///
/// <list type="bullet">
/// <item>a map key that is not a string — the pair is skipped, at any depth, and the walk carries
/// on (it used to throw, and the rest of the row went unindexed);</item>
/// <item>a nil element of an array — the string <c>"&lt;null&gt;"</c>, as
/// <c>LogEventSerializer.ReadArray</c> reads it (a nil map value stays a nil);</item>
/// <item>an extension element of an array — <c>"&lt;null&gt;"</c> too; an extension map value
/// is a nil, as <c>ReadDynamic</c> reads it;</item>
/// <item>a binary value — the text <c>"System.Byte[]"</c>, what the scan's text predicates read.</item>
/// </list>
///
/// <para>Byte-identical sections and the same presented-term count are the statement that the walk
/// files exactly what the reference files; the lookups after them say what that is.</para>
/// </summary>
public sealed class WalkParityOnUnusualShapesTests
{
    [Fact]
    public void StreamingWalk_MatchesReferenceBuild_OnUnusualShapes()
    {
        var pool = new StringInternPool();
        using var hot = Tier(pool, Rows);

        using var streaming = new SegmentIndexBuilder(hot.Count);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(hot.Count);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedInvertedIndex, streaming.SerialisedInvertedIndex);
        Assert.Equal(reference.SerialisedTrigramIndex,  streaming.SerialisedTrigramIndex);
        Assert.Equal(reference.SerialisedBloomFilter,   streaming.SerialisedBloomFilter);
        Assert.Equal(reference.BloomTermsAdded,         streaming.BloomTermsAdded);
    }

    /// <summary>
    /// What the walk files, said outright: the properties after a key that is not a string, and
    /// nothing of the skipped pair's value; the reader's text for a nil or extension element and
    /// for a binary value, under the property's own bucket — the term a client string of that text
    /// files, which is why a row holding the real string shares it.
    /// </summary>
    [Fact]
    public void TheWalk_FilesWhatTheScanReads()
    {
        var pool = new StringInternPool();
        using var hot = Tier(pool, Rows);

        using var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool);
        using var index = SegmentIndexReader.Load(builder.SerialisedInvertedIndex, builder.SerialisedTrigramIndex, builder.SerialisedBloomFilter);

        // Each row is written twice, at i and i + Rows.Length.
        uint[] Of(params string[] names) =>
        [
            .. names.Select(n => (uint)Array.FindIndex(Rows, r => r.Name == n))
                    .SelectMany(i => new[] { i, i + (uint)Rows.Length })
                    .Order(),
        ];

        // 1. The pairs after a key that is not a string.
        Assert.Equal(Of("int-key"),        index.Lookup("P", "needle"));
        Assert.Equal(Of("int-key"),        index.Lookup("N", 4242L));
        Assert.Equal(Of("nested-int-key"), index.Lookup("R", "needle"));
        Assert.Equal(Of("nested-int-key"), index.Lookup("S", 4242L));
        Assert.Equal(Of("every-key-kind"), index.Lookup("P", "kinds"));
        Assert.Equal(Of("nested-int-key"), index.Lookup(Flat("Counts", "Kept"), "yes"));
        Assert.Equal(Of("map-under-a-key"), index.Lookup("P", "after-map"));
        // ... and nothing of a skipped pair's value, however deep it goes.
        Assert.Equal(Array.Empty<uint>(), index.LookupTrigram("hidden"));

        // 2 and 3. A nil or extension element: the reader's "<null>", beside the real string.
        Assert.Equal(Of("nil-elements", "ext-elements", "the-real-strings"), index.Lookup("Tags", "<null>"));
        Assert.Equal(Of("nil-elements"), index.Lookup(Flat("Ctx", "Tags"), "<null>"));
        Assert.Equal(Of("nil-elements"), index.Lookup("M", "<null>"));
        Assert.Equal(Of("ext-elements"), index.Lookup("Marks", "<null>"));
        Assert.Equal(Of("maps-in-an-array"), index.Lookup("List", "<null>"));
        // A nil or extension MAP value stays a nil, inside an array's map as anywhere else.
        Assert.Equal(Of("ext-elements"), index.Lookup("Stamp", null));
        Assert.Equal(Of("maps-in-an-array"), index.Lookup(Flat("List", "a"), null));
        Assert.Equal(Of("maps-in-an-array"), index.Lookup(Flat("List", "b"), null));
        Assert.Equal(Of("ext-elements"), index.Lookup("After", "later"));

        // 4. A binary value: the reader's text.
        Assert.Equal(Of("binary"), index.Lookup("Bin", "System.Byte[]"));
        Assert.Equal(Of("binary"), index.Lookup("Bins", "System.Byte[]"));
        Assert.Equal(Of("binary"), index.Lookup(Flat("Ctx", "Bin"), "System.Byte[]"));
        Assert.Equal(Of("binary", "the-real-strings"), index.LookupTrigram("system.byte[]"));

        // The texts are in the bloom and the trigram index, which is what the gates ask.
        Assert.True(index.Bloom.MightContain("<null>"));
        Assert.True(index.Bloom.MightContain("system.byte[]"));
        Assert.Equal(Of("nil-elements", "ext-elements", "the-real-strings", "maps-in-an-array"), index.LookupTrigram("<null>"));
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

    /// <summary>A nested property's bucket: its path joined as the walk joins it.</summary>
    private static string Flat(params string[] path) => string.Join(ClefFields.PropertyPathSeparator, path);

    private delegate void PackPairs(ref MessagePackWriter writer);

    /// <summary>One row: its name (the <c>Row</c> property, written first) and the pairs after it.</summary>
    private sealed record Row(string Name, int Pairs, PackPairs Rest);

    /// <summary>A msgpack timestamp (fixext 4, type -1) and an ext 8 of three bytes, type 7.</summary>
    private static readonly byte[] TimestampExt = [0xD6, 0xFF, 0x00, 0x00, 0x00, 0x01];
    private static readonly byte[] Ext8         = [0xC7, 0x03, 0x07, 0x01, 0x02, 0x03];

    private static readonly Row[] Rows =
    [
        new("int-key", 3, static (ref MessagePackWriter w) => { w.Write(1); w.Write("x"); w.Write("P"); w.Write("needle"); w.Write("N"); w.Write(4242L); }),
        new("nested-int-key", 4, static (ref MessagePackWriter w) =>
        {
            w.Write("Before"); w.Write("early");
            w.Write("Counts"); w.WriteMapHeader(3); w.Write(500); w.Write(3); w.Write("Kept"); w.Write("yes"); w.Write(404); w.Write(1);
            w.Write("R");      w.Write("needle");
            w.Write("S");      w.Write(4242L);
        }),
        new("nil-key", 2, static (ref MessagePackWriter w) => { w.WriteNil(); w.Write("x"); w.Write("P"); w.Write("nil-key"); }),
        new("every-key-kind", 8, static (ref MessagePackWriter w) =>
        {
            w.Write(true);                                           w.Write("x");
            w.Write(-40);                                            w.Write("x");
            w.Write(ulong.MaxValue);                                 w.Write("x");
            w.Write(2.5d);                                           w.Write("x");
            w.Write(new byte[] { 9, 9 });                            w.Write("x");
            w.WriteArrayHeader(2); w.Write(1); w.Write("k");         w.Write("x");
            w.WriteRaw(TimestampExt);                                w.Write("x");
            w.Write("P");                                            w.Write("kinds");
        }),
        new("map-under-a-key", 2, static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(1); w.Write("k"); w.Write(1);
            w.WriteMapHeader(2); w.Write("Deep"); w.Write("hidden"); w.Write("More"); w.WriteArrayHeader(2); w.Write("hidden"); w.WriteNil();
            w.Write("P"); w.Write("after-map");
        }),
        new("nil-elements", 3, static (ref MessagePackWriter w) =>
        {
            w.Write("Tags"); w.WriteArrayHeader(2); w.Write("a"); w.WriteNil();
            w.Write("Ctx");  w.WriteMapHeader(2); w.Write("Tags"); w.WriteArrayHeader(1); w.WriteNil(); w.Write("Nil"); w.WriteNil();
            w.Write("M");    w.WriteArrayHeader(1); w.WriteArrayHeader(2); w.WriteNil(); w.Write("z");
        }),
        new("ext-elements", 4, static (ref MessagePackWriter w) =>
        {
            w.Write("Tags");  w.WriteArrayHeader(1); w.WriteRaw(TimestampExt);
            w.Write("Marks"); w.WriteArrayHeader(2); w.Write("m"); w.WriteRaw(Ext8);
            w.Write("Stamp"); w.WriteRaw(TimestampExt);
            w.Write("After"); w.Write("later");
        }),
        new("binary", 3, static (ref MessagePackWriter w) =>
        {
            w.Write("Bin");  w.Write(new byte[] { 1, 2, 3 });
            w.Write("Bins"); w.WriteArrayHeader(2); w.Write("x"); w.Write(new byte[] { 4 });
            w.Write("Ctx");  w.WriteMapHeader(1); w.Write("Bin"); w.Write(new byte[] { 5, 6 });
        }),
        new("the-real-strings", 2, static (ref MessagePackWriter w) => { w.Write("Tags"); w.WriteArrayHeader(1); w.Write("<null>"); w.Write("Text"); w.Write("System.Byte[]"); }),
        new("maps-in-an-array", 1, static (ref MessagePackWriter w) =>
        {
            w.Write("List"); w.WriteArrayHeader(2);
            w.WriteMapHeader(2); w.Write("a"); w.WriteNil(); w.Write("b"); w.WriteRaw(Ext8);
            w.WriteNil();
        }),
        new("plain", 2, static (ref MessagePackWriter w) => { w.Write("P"); w.Write("other"); w.Write("Tags"); w.WriteArrayHeader(1); w.Write("b"); }),
    ];

    /// <summary>Every row twice, in order, so the second pass meets every term as a repeat.</summary>
    private static HotTierSegment Tier(StringInternPool pool, Row[] rows)
    {
        const string Template = "settle {Row}";
        var hot = new HotTierSegment(rows.Length * 2 + 1, 1024 * 1024);
        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < rows.Length * 2; i++)
        {
            var row = rows[i % rows.Length];
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1 + row.Pairs);
            w.Write("Row"); w.Write(row.Name);
            row.Rest(ref w);
            w.Flush();

            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = new DateTimeOffset(2026, 11, 17, 9, 0, 0, TimeSpan.Zero).UtcTicks + i,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = pool.Intern(Template),
                ServiceNamePoolIndex     = pool.Intern("Ledger.API"),
            }, buf.WrittenSpan, Template));
        }
        return hot;
    }
}
