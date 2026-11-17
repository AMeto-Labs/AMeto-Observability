using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The builder adds a term to the bloom filter the first time it sees it and not again. The
/// filter is a set, so the section must be byte-identical to adding on every event (the
/// reference build still does that), and the count the writer sizes the next group from must
/// still be the number of terms PRESENTED — repeats included — or every forecast after the
/// first group would silently shrink.
///
/// <para>Byte-identical for the inputs tried here, that is. One input is known not to be, and
/// no query can tell: the KEY of a client property named like a bucket the header files is
/// never added once the header has made the bucket (see the note on first sight in
/// <see cref="SegmentIndexBuilder"/>).</para>
/// </summary>
public sealed class BloomFirstSightTests
{
    private static HotTierSegment Tier(int events, StringInternPool pool, int svcIdx, int tmplIdx, string tmpl)
    {
        var hot = new HotTierSegment(events + 1, (long)events * 512 + 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(4);
            w.Write("region");  w.Write("ae-dxb");                 // repeats every event
            w.Write("attempt"); w.Write((long)(i % 3));             // three distinct values
            w.Write("ok");      w.Write(i % 2 == 0);                // both bools
            w.Write("user");    w.Write("u-" + i);                  // unique per event
            w.Flush();
            hot.TryWrite(new LogEventHeader
            {
                Id = new EventId(0u, (uint)i).RawValue, TimestampUtcTicks = baseTicks + i,
                Level = LogLevel.Warning, MessageTemplatePoolIndex = tmplIdx, ServiceNamePoolIndex = svcIdx,
                TraceIdHi = 0x11UL, TraceIdLo = (ulong)(i % 5) + 1, SpanId = 0x22UL,
            }, buf.WrittenSpan, tmpl);
        }
        return hot;
    }

    [Fact]
    public void FirstSightBloom_IsByteIdenticalToAddOnEveryEvent_AndCountsPresentedTerms()
    {
        const int events = 1_000;
        var pool   = new StringInternPool();
        int svcIdx = pool.Intern("Wallet.API");
        int tmplIdx = pool.Intern("settlement {Id} done");
        using var hot = Tier(events, pool, svcIdx, tmplIdx, pool.Get(tmplIdx));

        using var streaming = new SegmentIndexBuilder(events);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(events);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedBloomFilter, streaming.SerialisedBloomFilter);

        // Per event: level, template, trace, span, service, and 4 keys + 4 values = 13 presented.
        Assert.Equal(13L * events, streaming.BloomTermsAdded);

        using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
        foreach (var term in new[] { "warning", "settlement {id} done", "wallet.api", "ae-dxb", "region", "attempt", "2", "ok", "true", "false", "user", "u-999", "0000000000000011" + "0000000000000003" })
            Assert.True(bloom.MightContain(term), term);
    }

    // ── The one term whose first filer gives the bloom a different text ───────

    /// <summary>
    /// "New to the inverted index" stands for "new to the bloom" only where whoever filed the
    /// term first gave the bloom the same text. The exception marker does not: it files
    /// <c>"true"</c> in the bucket <c>@x.exists</c> and gives the bloom the bucket's NAME. A
    /// client property of that literal name holding the string <c>"true"</c> is the same term,
    /// so after the marker it read as a repeat and its text never reached the bloom — which a
    /// filter on the value then asked, and was told the group held no such thing.
    ///
    /// <para>The reference build adds every term on every event and so never had the gap; the
    /// streaming build has to come out byte-identical to it in whichever order the two meet —
    /// rows apart, or the marker and the property on one row, where the marker is always first.
    /// The property is tried as a scalar and as an array, which the walk files alike.</para>
    /// </summary>
    [Theory]
    [InlineData("exception,client")]
    [InlineData("client,exception")]
    [InlineData("both")]
    [InlineData("plain,exception,plain,client,client,exception,both")]
    [InlineData("client,client,exception,client")]
    public void AClientValueInTheMarkersTerm_ReachesTheBloom_WhicheverWasFiledFirst(string order)
    {
        foreach (bool asArray in (bool[])[false, true])
        {
            var pool = new StringInternPool();
            using var hot = MarkerTier(order.Split(','), pool, asArray);

            using var streaming = new SegmentIndexBuilder(hot.Count);
            streaming.Build(hot, pool);
            using var reference = new SegmentIndexBuilder(hot.Count);
            reference.BuildReference(hot, pool);

            Assert.Equal(reference.SerialisedBloomFilter,   streaming.SerialisedBloomFilter);
            Assert.Equal(reference.SerialisedInvertedIndex, streaming.SerialisedInvertedIndex);
            Assert.Equal(reference.BloomTermsAdded,         streaming.BloomTermsAdded);

            // Said outright as well: the value's text, and the bucket's name beside it.
            using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
            Assert.True(bloom.MightContain("true"), $"`{order}`: the client's value is not in the bloom");
            Assert.True(bloom.MightContain("@x.exists"));
        }
    }

    /// <summary>
    /// The fix adds the one term that was missing and nothing else. In a group where no client
    /// files the marker's value the bloom is given the bucket's name and never <c>"true"</c> —
    /// which is what the reference build does, adding the name for every exception and the value
    /// for none, so its bytes are the statement. (Giving the bloom the value in the marker's own
    /// add would have closed the gap too, and let every group that holds an exception pass the
    /// gate for <c>true</c>.)
    /// </summary>
    [Fact]
    public void WithoutAClientValue_TheMarkerStillGivesTheBloomItsNameAlone()
    {
        var pool = new StringInternPool();
        using var hot = MarkerTier(["exception", "plain", "exception"], pool, asArray: false);

        using var streaming = new SegmentIndexBuilder(hot.Count);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(hot.Count);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedBloomFilter, streaming.SerialisedBloomFilter);
        Assert.Equal(reference.BloomTermsAdded,       streaming.BloomTermsAdded);

        using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
        Assert.True(bloom.MightContain("@x.exists"));
    }

    // ── A string that spells a typed value's term ─────────────────────────────

    /// <summary>
    /// The same meeting between two scalars. The inverted index files a typed value under a
    /// type-tagged form — the long 5 as <c>\0l5</c> — and a string under its own bytes, so the
    /// string <c>"\0l5"</c> under the same property IS the long's term, and the two give the
    /// bloom different texts: <c>\0l5</c> and <c>5</c>. Whichever was filed second used to be
    /// taken for a repeat and kept out of the filter.
    ///
    /// <para>Tried for every typed form the walk files, in rows apart either way round, as the
    /// two elements of one array either way round, and repeated. The streaming build has to come
    /// out byte-identical to the reference build, which adds every text every time — and both
    /// texts have to be in it, said outright.</para>
    /// </summary>
    [Theory]
    [MemberData(nameof(TypedKindsAndOrders))]
    public void AStringSpellingATypedTerm_AndTheTypedValue_BothReachTheBloom(string kind, string order)
    {
        var (spelling, text, _) = TypedKinds[kind];
        var pool = new StringInternPool();
        using var hot = TaggedTier(kind, order, pool);

        using var streaming = new SegmentIndexBuilder(hot.Count);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(hot.Count);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedBloomFilter,   streaming.SerialisedBloomFilter);
        Assert.Equal(reference.SerialisedInvertedIndex, streaming.SerialisedInvertedIndex);
        Assert.Equal(reference.BloomTermsAdded,         streaming.BloomTermsAdded);

        using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
        Assert.True(bloom.MightContain(spelling), $"{kind}, `{order}`: the string's text is not in the bloom");
        Assert.True(bloom.MightContain(text),     $"{kind}, `{order}`: the typed value's text is not in the bloom");
    }

    public static TheoryData<string, string> TypedKindsAndOrders
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (string kind in TypedKinds.Keys)
            foreach (string order in (string[])["string,typed", "typed,string", "[string,typed]", "[typed,string]", "typed,typed,string,typed,string"])
            {
                // A nil INSIDE an array is no comparison with the reference: the reference reads
                // the element as the string "<null>" (LogEventSerializer.ReadArray — and the scan
                // reads it so too), where the walk files a nil. That disagreement is older than
                // this rule and untouched by it; the walk's side of the meeting is still asked of
                // the engine, in StringSpellingATypedTermTests.
                if (kind == "nil" && order.StartsWith('[')) continue;
                data.Add(kind, order);
            }
            return data;
        }
    }

    /// <summary>
    /// What that rule stands on, for the terms production files: every term the streaming walk
    /// files under a property either begins with NUL or is the very text the bloom holds for it.
    /// So only a string that begins with NUL can share a typed value's term, and those are what
    /// the builder watches for.
    ///
    /// <para>Asked of the walk itself, which spells its forms inline (<c>AddScalar</c>,
    /// <c>AddLong</c>, <c>AddNumeric</c>) and never calls <see cref="IndexValueForms.Serialise"/>:
    /// one row per msgpack shape a property can hold, every integer encoding, both float widths,
    /// and the binary and extension shapes the walk does not file today. A shape it starts to file
    /// under anything else would let an ordinary string take a typed value's term unseen, and
    /// fails here first.</para>
    /// </summary>
    [Fact]
    public void EveryTermTheWalkFiles_BeginsWithNul_OrIsTheTextItGivesTheBloom()
    {
        (string Shape, PackValue Write)[] shapes =
        [
            ("nil",                 static (ref MessagePackWriter w) => w.WriteNil()),
            ("true",                static (ref MessagePackWriter w) => w.Write(true)),
            ("false",               static (ref MessagePackWriter w) => w.Write(false)),
            ("positive fixint",     static (ref MessagePackWriter w) => w.Write(5)),
            ("negative fixint",     static (ref MessagePackWriter w) => w.Write(-3)),
            ("uint8",               static (ref MessagePackWriter w) => w.WriteUInt8(200)),
            ("uint16",              static (ref MessagePackWriter w) => w.WriteUInt16(60_000)),
            ("uint32",              static (ref MessagePackWriter w) => w.WriteUInt32(4_000_000_000)),
            ("uint64",              static (ref MessagePackWriter w) => w.WriteUInt64(5)),
            ("uint64 past a long",  static (ref MessagePackWriter w) => w.WriteUInt64(ulong.MaxValue)),
            ("int8",                static (ref MessagePackWriter w) => w.WriteInt8(-100)),
            ("int16",               static (ref MessagePackWriter w) => w.WriteInt16(-30_000)),
            ("int32",               static (ref MessagePackWriter w) => w.WriteInt32(-2_000_000_000)),
            ("int64",               static (ref MessagePackWriter w) => w.WriteInt64(long.MinValue)),
            ("float32",             static (ref MessagePackWriter w) => w.Write(2.5f)),
            ("float64",             static (ref MessagePackWriter w) => w.Write(2.5d)),
            ("string",              static (ref MessagePackWriter w) => w.Write("plain")),
            ("bin",                 static (ref MessagePackWriter w) => w.Write((ReadOnlySpan<byte>)[1, 2, 3])),
            ("ext",                 static (ref MessagePackWriter w) => w.WriteExtensionFormat(new ExtensionResult(5, new byte[] { 1 }))),
            ("timestamp ext",       static (ref MessagePackWriter w) => w.Write(new DateTime(2026, 11, 17, 9, 0, 0, DateTimeKind.Utc))),
        ];

        foreach (var (shape, write) in shapes)
        {
            var pool = new StringInternPool();
            using var hot = TierOfRows(pool, [("P", write)]);
            using var streaming = new SegmentIndexBuilder(hot.Count);
            streaming.Build(hot, pool);

            using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
            foreach (string term in TermsUnder(streaming.SerialisedInvertedIndex, "P"))
                Assert.True(term.StartsWith('\0') || bloom.MightContain(term),
                    $"{shape}: filed as `{term.Replace("\0", "\\0")}`, which neither begins with NUL nor is text the bloom holds");
        }
    }

    /// <summary>
    /// The same rule for the forms <see cref="IndexValueForms.Serialise"/> writes — the reference
    /// build's, and <c>SegmentInvertedIndex.Add</c>'s: every value under its own text (a string,
    /// a number written without a tag) or under a form that begins with NUL.
    /// </summary>
    [Fact]
    public void EveryValueIsFiledUnderItsOwnText_OrUnderAFormThatBeginsWithNul()
    {
        object?[] values = [null, true, false, 5, 5L, -7L, 2.5d, 2.5f, (short)3, (byte)4, 5UL, ulong.MaxValue, "plain", ""];
        foreach (var value in values)
        {
            string form = IndexValueForms.Serialise(value);
            Assert.True(form == IndexValueForms.PlainText(value) || form.StartsWith('\0'),
                $"{value?.GetType().Name ?? "null"} {value}: filed as `{form.Replace("\0", "\\0")}`, which is neither its own text nor NUL-led");
        }
    }

    /// <summary>
    /// The marks are a bitset by property id, grown as ids turn up. Here the strings that spell
    /// typed terms sit at ids in the bitset's first word, its second and its fourth — so it is
    /// made, grown, and grown past a word nothing marks — and every typed value beside them must
    /// still reach the bloom, as the reference build has it.
    /// </summary>
    [Fact]
    public void StringsSpellingTypedTerms_PastTheFirst64Properties_LeaveTheTypedValuesTheirText()
    {
        var properties = new List<(string, PackValue)> { ("L", static (ref MessagePackWriter w) => w.Write("\0true")) };
        for (int i = 0; i < 70; i++)  properties.Add(($"F{i:D3}", static (ref MessagePackWriter w) => w.Write("filler")));
        properties.Add(("P", static (ref MessagePackWriter w) => w.Write("\0l5")));
        for (int i = 70; i < 200; i++) properties.Add(($"F{i:D3}", static (ref MessagePackWriter w) => w.Write("filler")));
        properties.Add(("Q", static (ref MessagePackWriter w) => w.Write("\0d2.5")));

        var pool = new StringInternPool();
        using var hot = TierOfRows(pool, [.. properties],
        [
            ("L", static (ref MessagePackWriter w) => w.Write(true)),
            ("P", static (ref MessagePackWriter w) => w.Write(5L)),
            ("Q", static (ref MessagePackWriter w) => w.Write(2.5d)),
        ]);

        // Sized for the fillers, or the filter saturates and says "maybe" to everything.
        using var streaming = new SegmentIndexBuilder(hot.Count, estimatedTermsPerEvent: 1024);
        streaming.Build(hot, pool);
        using var reference = new SegmentIndexBuilder(hot.Count, estimatedTermsPerEvent: 1024);
        reference.BuildReference(hot, pool);

        // The premise: the section lists properties in id order, and the three marked ones fall
        // in the bitset's first, second and fourth words.
        var order = PropertyOrder(streaming.SerialisedInvertedIndex);
        Assert.True(order.IndexOf("L") < 64 && order.IndexOf("P") is >= 64 and < 128 && order.IndexOf("Q") >= 192,
            $"ids: L {order.IndexOf("L")}, P {order.IndexOf("P")}, Q {order.IndexOf("Q")}");

        Assert.Equal(reference.SerialisedBloomFilter,   streaming.SerialisedBloomFilter);
        Assert.Equal(reference.SerialisedInvertedIndex, streaming.SerialisedInvertedIndex);

        using var bloom = SegmentBloomFilter.Deserialise(streaming.SerialisedBloomFilter);
        foreach (string text in (string[])["true", "5", "2.5", "\0true", "\0l5", "\0d2.5"])
            Assert.True(bloom.MightContain(text), $"`{text.Replace("\0", "\\0")}` is not in the bloom");
    }

    /// <summary>
    /// Every value filed under <paramref name="property"/> in an inverted section, as text — read
    /// off the section's own layout (<c>SegmentInvertedIndex.WriteTo</c>): per entry a 16-bit
    /// length, the value, a 32-bit length and the posting list.
    /// </summary>
    private static List<string> TermsUnder(byte[] section, string property)
    {
        var terms   = new List<string>();
        var catalog = SegmentInvertedIndex.ReadCatalog(section);
        if (!catalog.Properties.TryGetValue(property, out var prop)) return terms;
        foreach (var run in prop.Runs)
        {
            int pos = run.Offset;
            for (int v = 0; v < run.ValueCount; v++)
            {
                int len = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(section.AsSpan(pos)); pos += 2;
                terms.Add(System.Text.Encoding.UTF8.GetString(section, pos, len));                            pos += len;
                pos += 4 + (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(section.AsSpan(pos));
            }
        }
        return terms;
    }

    /// <summary>The property names of an inverted section in the order it lists them — first
    /// seen first, which is property-id order.</summary>
    private static List<string> PropertyOrder(byte[] section)
    {
        var names = new List<string>();
        int pos   = 4;                                                       // the codec marker
        int count = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(section.AsSpan(pos)); pos += 4;
        for (int p = 0; p < count; p++)
        {
            int len = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(section.AsSpan(pos)); pos += 2;
            names.Add(System.Text.Encoding.UTF8.GetString(section, pos, len));                            pos += len;
            int values = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(section.AsSpan(pos)); pos += 4;
            for (int v = 0; v < values; v++)
            {
                pos += 2 + System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(section.AsSpan(pos));
                pos += 4 + (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(section.AsSpan(pos));
            }
        }
        return names;
    }

    /// <summary>A tier of one row per argument, each row the properties given, in order.</summary>
    private static HotTierSegment TierOfRows(StringInternPool pool, params (string Name, PackValue Write)[][] rows)
    {
        const string Template = "settle {Row}";
        var hot = new HotTierSegment(rows.Length + 1, 1024 * 1024);
        var buf = new ArrayBufferWriter<byte>(4096);
        for (int i = 0; i < rows.Length; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(rows[i].Length);
            foreach (var (name, write) in rows[i])
            {
                w.Write(name);
                write(ref w);
            }
            w.Flush();

            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = new DateTimeOffset(2026, 11, 17, 9, 0, 0, TimeSpan.Zero).UtcTicks + i,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = pool.Intern(Template),
                ServiceNamePoolIndex     = pool.Intern("Billing.API"),
            }, buf.WrittenSpan, Template));
        }
        return hot;
    }

    /// <summary>
    /// The typed values the walk files with a tag, each with the string that spells its term and
    /// the text it gives the bloom.
    /// </summary>
    private static readonly Dictionary<string, (string Spelling, string Text, PackValue Typed)> TypedKinds = new()
    {
        ["long"]   = ("\0l5",    "5",     static (ref MessagePackWriter w) => w.Write(5L)),
        ["double"] = ("\0d2.5",  "2.5",   static (ref MessagePackWriter w) => w.Write(2.5d)),
        ["true"]   = ("\0true",  "true",  static (ref MessagePackWriter w) => w.Write(true)),
        ["false"]  = ("\0false", "false", static (ref MessagePackWriter w) => w.Write(false)),
        ["nil"]    = ("\0null",  "",      static (ref MessagePackWriter w) => w.WriteNil()),
    };

    private delegate void PackValue(ref MessagePackWriter writer);

    /// <summary>
    /// Rows in <paramref name="order"/>: <c>string</c> holds the kind's spelling under <c>P</c>,
    /// <c>typed</c> its typed value; a bracketed pair is one row whose <c>P</c> is an array of the
    /// two. Every row also carries a <c>Row</c> of its own.
    /// </summary>
    private static HotTierSegment TaggedTier(string kind, string order, StringInternPool pool)
    {
        const string Template = "settle {Row}";
        var (spelling, _, typed) = TypedKinds[kind];
        string[][] rows = order.StartsWith('[') ? [order.Trim('[', ']').Split(',')] : [.. order.Split(',').Select(static v => new[] { v })];

        var hot = new HotTierSegment(rows.Length + 1, 1024 * 1024);
        var buf = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < rows.Length; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("Row"); w.Write("r-" + i);
            w.Write("P");
            if (rows[i].Length > 1) w.WriteArrayHeader(rows[i].Length);
            foreach (string value in rows[i])
            {
                if (value == "string") w.Write(spelling);
                else                   typed(ref w);
            }
            w.Flush();

            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = new DateTimeOffset(2026, 11, 17, 9, 0, 0, TimeSpan.Zero).UtcTicks + i,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = pool.Intern(Template),
                ServiceNamePoolIndex     = pool.Intern("Billing.API"),
            }, buf.WrittenSpan, Template));
        }
        return hot;
    }

    /// <summary>
    /// Rows by kind: <c>exception</c> carries one, <c>client</c> the property
    /// <c>@x.exists = "true"</c> (or <c>["true"]</c>), <c>both</c> both, <c>plain</c> neither.
    /// </summary>
    private static HotTierSegment MarkerTier(string[] rows, StringInternPool pool, bool asArray)
    {
        const string Template = "settle {Row}";
        byte[] exception = new ExceptionInfo { Type = "System.InvalidOperationException", Message = "gateway timeout" }.ToBytes();

        var hot = new HotTierSegment(rows.Length + 1, 1024 * 1024);
        var buf = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < rows.Length; i++)
        {
            bool client = rows[i] is "client" or "both";
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(client ? 2 : 1);
            w.Write("Row"); w.Write("r-" + i);
            if (client)
            {
                w.Write("@x.exists");
                if (asArray) w.WriteArrayHeader(1);
                w.Write("true");
            }
            w.Flush();

            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero).UtcTicks + i,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = pool.Intern(Template),
                ServiceNamePoolIndex     = pool.Intern("Billing.API"),
            }, buf.WrittenSpan, Template, rows[i] is "exception" or "both" ? exception : default));
        }
        return hot;
    }
}
